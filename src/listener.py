"""
Módulo 1 — Gong Webhook / Listener
Escucha cuando se inserta una nueva fila en la tabla transcripts de Gong.
"""

import os
import time
import logging
import queue
import threading
from typing import Set, List, Dict, Any
from datetime import datetime
import requests
from dotenv import load_dotenv

load_dotenv()

class GongListener:
    def __init__(self, event_queue: queue.Queue):
        self.event_queue = event_queue
        self.processed_ids: Set[str] = set()
        # Daily scheduled execution time (defaults to 07:00 AM) can be changed
        # via environment variables RUN_HOUR and RUN_MINUTE. The previous
        # POLL_INTERVAL setting (every N seconds) is no longer used but kept as
        # a fallback if someone still sets it.
        self.run_hour = int(os.getenv('RUN_HOUR', 7))  # 0-23
        self.run_minute = int(os.getenv('RUN_MINUTE', 0))  # 0-59

        # Retain the original POLL_INTERVAL for backward compatibility when
        # sleeping while waiting for the next scheduled run. This value is not
        # used to trigger polling anymore; it only defines the maximum sleep
        # chunk so the thread can react to shutdown requests promptly.
        self._idle_sleep_seconds = int(os.getenv('POLL_INTERVAL', 300))

        self.gong_api_url = os.getenv('GONG_API_URL')
        self.gong_api_key = os.getenv('GONG_API_KEY')
        self.running = False
        
        logging.basicConfig(
            filename='logs/event_listener.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
    def poll_gong_transcripts(self) -> List[Dict[str, Any]]:
        """Poll Gong API for new transcripts"""
        try:
            headers = {
                'Authorization': f'Bearer {self.gong_api_key}',
                'Content-Type': 'application/json'
            }
            
            # Get transcripts from the last 24 hours. Because the polling now
            # runs once per day, we widen the window to ensure we pick up all
            # transcripts generated since the previous run.
            response = requests.get(
                f"{self.gong_api_url}/v2/calls/transcripts",
                headers=headers,
                params={
                    'fromDateTime': (datetime.now().timestamp() - 86_400) * 1000,  # 24 hours ago
                    'toDateTime': datetime.now().timestamp() * 1000
                }
            )
            response.raise_for_status()
            
            data = response.json()
            return data.get('transcripts', [])
            
        except requests.RequestException as e:
            self.logger.error(f"Error polling Gong API: {e}")
            return []
    
    def start_listening(self):
        """Start the polling loop"""
        self.running = True
        self.logger.info("Starting Gong listener...")
        
        from datetime import time as dt_time, timedelta

        # Determine the next run time first.
        now = datetime.now()
        scheduled_today = datetime.combine(now.date(), dt_time(self.run_hour, self.run_minute))
        next_run = scheduled_today if now < scheduled_today else scheduled_today + timedelta(days=1)

        while self.running:
            try:
                now = datetime.now()

                # If it's not yet time, sleep in small increments so we can
                # catch shutdown signals without significant delay.
                if now < next_run:
                    sleep_seconds = min((next_run - now).total_seconds(), self._idle_sleep_seconds)
                    time.sleep(max(sleep_seconds, 1))
                    continue

                # It's time to poll.
                new_rows = self.poll_gong_transcripts()

                for row in new_rows:
                    transcript_id = row.get('transcript_id')

                    if transcript_id and transcript_id not in self.processed_ids:
                        # Add to queue and mark as processed
                        self.event_queue.put(transcript_id)
                        self.processed_ids.add(transcript_id)

                        self.logger.info(
                            f"New transcript detected - ID: {transcript_id}, "
                            f"Timestamp: {datetime.now()}, Status: NEW"
                        )

                # After running, calculate next day's run time.
                next_run += timedelta(days=1)

            except Exception as e:
                self.logger.error(f"Error in listener loop: {e}")
                # Wait a bit before the next check to avoid tight error loop.
                time.sleep(30)
    
    def stop_listening(self):
        """Stop the polling loop"""
        self.running = False
        self.logger.info("Stopping Gong listener...")

def listener_thread(event_queue: queue.Queue):
    """Thread function for running the listener"""
    listener = GongListener(event_queue)
    listener.start_listening()