"""
Módulo 2 — Snowflake Data Fetcher
Ejecuta consulta en Snowflake y devuelve datos del transcript.
"""

import os
import logging
from dataclasses import dataclass
from typing import Optional
import snowflake.connector
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv

load_dotenv()

@dataclass
class TranscriptData:
    transcript_id: str
    account_name: str
    account_number: str
    speaker_name: str
    speaker_email: str
    cs_email: str
    am_email: str
    transcript_text: str

class SnowflakeFetcher:
    def __init__(self):
        self.connection_params = {
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA')
        }
        
        self.logger = logging.getLogger(__name__)
        
        # Hard-coded query as specified
        self.query = """
        SELECT 
            t.transcript_id,
            a.account_name,
            a.account_number,
            c.speaker_name,
            c.speaker_email,
            a.cs_email,
            a.am_email,
            t.transcript_text
        FROM transcripts t
        JOIN calls c ON t.call_id = c.call_id
        JOIN accounts a ON c.account_id = a.account_id
        WHERE t.transcript_id = %s
        """
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def fetch_data(self, transcript_id: str) -> Optional[TranscriptData]:
        """Fetch transcript data from Snowflake"""
        try:
            with snowflake.connector.connect(**self.connection_params) as cnx:
                cursor = cnx.cursor()
                cursor.execute(self.query, (transcript_id,))
                row = cursor.fetchone()
                
                if not row:
                    self.logger.error(f"No data found for transcript_id: {transcript_id}")
                    return None
                
                # Validate critical fields
                if not all([row[1], row[2], row[3], row[4], row[5], row[6], row[7]]):
                    self.logger.error(f"Missing critical fields for transcript_id: {transcript_id}")
                    return None
                
                return TranscriptData(
                    transcript_id=row[0],
                    account_name=row[1],
                    account_number=row[2],
                    speaker_name=row[3],
                    speaker_email=row[4],
                    cs_email=row[5],
                    am_email=row[6],
                    transcript_text=row[7]
                )
                
        except Exception as e:
            self.logger.error(f"Error fetching data for transcript_id {transcript_id}: {e}")
            raise