"""
Módulo 6 — Orchestrator & Error Handling
Coordina todo el pipeline y maneja excepciones.
"""

import os
import json
import signal
import logging
import queue
import threading
from datetime import datetime

# Colored console output
try:
    from colorama import init as colorama_init, Fore, Style
    colorama_init(autoreset=True)

    def _c(msg: str, color: str = Fore.RESET) -> str:  # helper to colorize
        return f"{color}{msg}{Style.RESET_ALL}"

except ImportError:  # colorama not available – gracefully degrade
    class _Dummy:
        RESET = ""
        RED = ""
        GREEN = ""
        YELLOW = ""
        CYAN = ""
        MAGENTA = ""
        BLUE = ""

    Fore = Style = _Dummy()  # type: ignore

    def _c(msg: str, color: str = "") -> str:
        return msg
from typing import Dict, Any, Optional
from prometheus_client import Counter, Histogram, Gauge, start_http_server
from dotenv import load_dotenv

from .fetcher import SnowflakeFetcher
from .assistant_bridge import OpenAIAssistantBridge
from .drive_documenter import GoogleDriveDocumenter
from .email_dispatcher import EmailDispatcher
from .listener import listener_thread

load_dotenv()

class Orchestrator:
    def __init__(self):
        self.event_queue = queue.Queue()
        self.running = False
        self.max_retries = int(os.getenv('MAX_RETRIES', 3))
        
        # Initialize modules
        self.fetcher = SnowflakeFetcher()
        self.assistant = OpenAIAssistantBridge()
        self.drive_documenter = GoogleDriveDocumenter()
        self.email_dispatcher = EmailDispatcher()
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Setup file handler for structured JSON logs
        json_handler = logging.FileHandler('logs/orchestrator.log')
        json_handler.setFormatter(logging.Formatter('%(message)s'))
        self.json_logger = logging.getLogger('json_logger')
        self.json_logger.addHandler(json_handler)
        self.json_logger.setLevel(logging.INFO)
        
        # Prometheus metrics
        self.success_counter = Counter('pipeline_success_total', 'Total successful pipeline executions')
        self.error_counter = Counter('pipeline_error_total', 'Total pipeline errors', ['module', 'error_type'])
        self.processing_time = Histogram('pipeline_processing_seconds', 'Time spent processing each transcript')
        self.in_flight_gauge = Gauge('pipeline_in_flight', 'Number of transcripts currently being processed')
        self.queue_size_gauge = Gauge('pipeline_queue_size', 'Size of the processing queue')
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self.logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.running = False
    
    def _log_structured(self, transcript_id: str, status: str, module: str = None, error: str = None, **kwargs):
        """Log structured JSON data"""
        log_data = {
            'timestamp': datetime.now().isoformat(),
            'transcript_id': transcript_id,
            'status': status,
            **kwargs
        }
        
        if module:
            log_data['module'] = module
        if error:
            log_data['error'] = error
            
        self.json_logger.info(json.dumps(log_data))
    
    def _should_retry(self, transcript_id: str, attempt: int) -> bool:
        """Determine if we should retry processing"""
        return attempt < self.max_retries
    
    def _requeue_transcript(self, transcript_id: str):
        """Put transcript back in queue for retry"""
        self.event_queue.put(transcript_id)
        self.logger.info(f"Requeued transcript {transcript_id} for retry")
    
    def process_transcript(self, transcript_id: str, attempt: int = 1) -> bool:
        """Process a single transcript through the entire pipeline"""
        start_time = datetime.now()
        
        try:
            self.in_flight_gauge.inc()
            self._log_structured(transcript_id, "PROCESSING_STARTED", attempt=attempt)
            
            # Step 1: Fetch data from Snowflake
            self.logger.info(f"Fetching data for transcript {transcript_id}")
            print(_c(f"[FETCHER] Fetching data for transcript {transcript_id}", Fore.CYAN))
            data = self.fetcher.fetch_data(transcript_id)
            if not data:
                self._log_structured(transcript_id, "FAILED_FETCH", module="fetcher")
                self.error_counter.labels(module='fetcher', error_type='no_data').inc()
                return False
            
            self._log_structured(transcript_id, "FETCH_SUCCESS", module="fetcher", 
                               account_name=data.account_name)
            print(_c(f"[FETCHER] Fetched data for account '{data.account_name}'", Fore.GREEN))
            
            # Step 2: Get summary from OpenAI Assistant
            self.logger.info(f"Getting summary for transcript {transcript_id}")
            print(_c("[ASSISTANT] Sending transcript to OpenAI Assistant…", Fore.CYAN))
            summary = self.assistant.get_summary(data)
            if not summary:
                self._log_structured(transcript_id, "FAILED_OAI", module="assistant")
                self.error_counter.labels(module='assistant', error_type='no_summary').inc()
                return False
            
            self._log_structured(transcript_id, "SUMMARY_SUCCESS", module="assistant")
            print(_c("[ASSISTANT] Received summary from OpenAI Assistant", Fore.GREEN))
            
            # Step 3: Update Google Drive documents
            self.logger.info(f"Updating Drive docs for transcript {transcript_id}")
            print(_c(f"[DRIVE] Finding/creating documents for account '{data.account_name}'", Fore.CYAN))
            doc_links = self.drive_documenter.update_drive_docs(data.account_name, summary)
            
            self._log_structured(transcript_id, "DRIVE_SUCCESS", module="drive_documenter",
                               call_doc_url=doc_links[0], log_doc_url=doc_links[1])
            print(_c("[DRIVE] Documents updated", Fore.GREEN))
            
            # Step 4: Send emails
            self.logger.info(f"Sending emails for transcript {transcript_id}")
            print(_c("[EMAIL] Sending CS & AM notification emails…", Fore.CYAN))
            email_status = self.email_dispatcher.send_emails(data, summary, doc_links)
            
            self._log_structured(transcript_id, "EMAIL_SUCCESS", module="email_dispatcher",
                               cs_status=email_status[0], am_status=email_status[1])
            print(_c("[EMAIL] Emails sent", Fore.GREEN))
            
            # Mark as successful
            processing_time = (datetime.now() - start_time).total_seconds()
            self.processing_time.observe(processing_time)
            self.success_counter.inc()
            
            self._log_structured(transcript_id, "PIPELINE_SUCCESS", 
                               processing_time_seconds=processing_time)
            
            self.logger.info(f"Successfully processed transcript {transcript_id} in {processing_time:.2f}s")
            print(_c(f"[PIPELINE] Successfully processed transcript {transcript_id} in {processing_time:.1f}s", Fore.MAGENTA))
            return True
            
        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"Error processing transcript {transcript_id} (attempt {attempt}): {error_msg}")

            print(_c(f"[ERROR] Transcript {transcript_id}: {error_msg}", Fore.RED))
            
            self._log_structured(transcript_id, "PIPELINE_ERROR", 
                               error=error_msg, attempt=attempt)
            
            self.error_counter.labels(module='orchestrator', error_type='general').inc()
            
            # Decide whether to retry
            if self._should_retry(transcript_id, attempt):
                self.logger.info(f"Will retry transcript {transcript_id} (attempt {attempt + 1})")
                self._requeue_transcript(transcript_id)
            else:
                self.logger.error(f"Max retries exceeded for transcript {transcript_id}")
                self._log_structured(transcript_id, "PIPELINE_FAILED_FINAL", 
                                   max_attempts=attempt)
            
            return False
            
        finally:
            self.in_flight_gauge.dec()
    
    def run_pipeline_worker(self):
        """Main worker thread that processes transcripts from the queue"""
        self.logger.info("Starting pipeline worker...")
        
        while self.running:
            try:
                # Update queue size metric
                self.queue_size_gauge.set(self.event_queue.qsize())
                
                # Get transcript from queue (blocking with timeout)
                transcript_id = self.event_queue.get(timeout=5)
                
                self.logger.info(f"Processing transcript: {transcript_id}")
                self.process_transcript(transcript_id)
                
                # Mark task as done
                self.event_queue.task_done()
                
            except queue.Empty:
                # No items in queue, continue loop
                continue
            except Exception as e:
                self.logger.error(f"Unexpected error in pipeline worker: {e}")
    
    def start(self):
        """Start the orchestrator"""
        self.running = True
        
        # Start Prometheus metrics server
        metrics_port = int(os.getenv('METRICS_PORT', 8000))
        start_http_server(metrics_port)
        self.logger.info(f"Metrics server started on port {metrics_port}")
        
        # Start listener thread
        listener_thread_obj = threading.Thread(
            target=listener_thread, 
            args=(self.event_queue,),
            daemon=True
        )
        listener_thread_obj.start()
        self.logger.info("Listener thread started")
        
        # Start pipeline worker
        self.logger.info("Starting orchestrator...")
        self.run_pipeline_worker()
    
    def stop(self):
        """Stop the orchestrator gracefully"""
        self.running = False
        
        # Wait for queue to be processed
        self.logger.info("Waiting for queue to be processed...")
        self.event_queue.join()
        
        self.logger.info("Orchestrator stopped")

def main():
    """Main entry point"""
    orchestrator = Orchestrator()
    try:
        orchestrator.start()
    except KeyboardInterrupt:
        orchestrator.logger.info("Received keyboard interrupt")
    finally:
        orchestrator.stop()

if __name__ == "__main__":
    main()