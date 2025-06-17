"""
Módulo 3 — OpenAI Assistant Bridge
Envía la info al Assistant y obtiene la respuesta resumida.
"""

import os
import time
import logging
from typing import Optional
from openai import OpenAI
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv
from .fetcher import TranscriptData

load_dotenv()

class OpenAIAssistantBridge:
    def __init__(self):
        self.client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        self.assistant_id = os.getenv('ASSISTANT_ID')
        self.timeout = int(os.getenv('TIMEOUT', 120))
        self.logger = logging.getLogger(__name__)
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def get_summary(self, data: TranscriptData) -> Optional[str]:
        """Get summary from OpenAI Assistant"""
        try:
            # Create thread
            thread = self.client.beta.threads.create()
            
            # Prepare message content with transcript and metadata
            message_content = f"""
            Please analyze this call transcript and provide a comprehensive summary.
            
            Account Information:
            - Account Name: {data.account_name}
            - Account Number: {data.account_number}
            - Speaker: {data.speaker_name} ({data.speaker_email})
            
            Transcript:
            {data.transcript_text}
            
            Please provide a detailed summary including:
            1. Key discussion points
            2. Action items
            3. Customer concerns or requests
            4. Next steps
            """
            
            # Add message to thread
            self.client.beta.threads.messages.create(
                thread_id=thread.id,
                role="user",
                content=message_content
            )
            
            # Run assistant
            run = self.client.beta.threads.runs.create(
                thread_id=thread.id,
                assistant_id=self.assistant_id
            )
            
            # Wait for completion with timeout
            start_time = time.time()
            while time.time() - start_time < self.timeout:
                run_status = self.client.beta.threads.runs.retrieve(
                    thread_id=thread.id,
                    run_id=run.id
                )
                
                if run_status.status == "completed":
                    # Get messages
                    messages = self.client.beta.threads.messages.list(
                        thread_id=thread.id
                    )
                    
                    # Get the assistant's response
                    assistant_response = messages.data[0].content[0].text.value
                    
                    if assistant_response and assistant_response.strip():
                        self.logger.info(f"Summary generated for transcript {data.transcript_id}")
                        return assistant_response.strip()
                    else:
                        self.logger.error(f"Empty response from assistant for transcript {data.transcript_id}")
                        return None
                
                elif run_status.status in ["failed", "cancelled", "expired"]:
                    self.logger.error(f"Assistant run failed with status: {run_status.status}")
                    return None
                
                time.sleep(2)  # Check every 2 seconds
            
            # Timeout reached
            self.logger.error(f"Timeout waiting for assistant response for transcript {data.transcript_id}")
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting summary from OpenAI: {e}")
            raise