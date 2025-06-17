"""
Módulo 5 — Email Dispatcher
Envía correos templados al CS y AM con resumen y links.
"""

import os
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Tuple
from jinja2 import Environment, FileSystemLoader
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv
from .fetcher import TranscriptData

load_dotenv()

class EmailDispatcher:
    def __init__(self):
        self.smtp_host = os.getenv('SMTP_HOST')
        self.smtp_port = int(os.getenv('SMTP_PORT', 587))
        self.smtp_user = os.getenv('SMTP_USER')
        self.smtp_password = os.getenv('SMTP_PASSWORD')
        self.from_email = os.getenv('FROM_EMAIL')
        
        # Setup Jinja2 environment
        template_dir = os.path.join(os.path.dirname(__file__), 'templates')
        self.jinja_env = Environment(loader=FileSystemLoader(template_dir))
        
        self.logger = logging.getLogger(__name__)
        
        # Setup logging for email dispatcher
        email_handler = logging.FileHandler('logs/email_dispatcher.log')
        email_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        )
        self.logger.addHandler(email_handler)
    
    def render_template(self, template_name: str, **kwargs) -> str:
        """Render email template with provided data"""
        template = self.jinja_env.get_template(template_name)
        return template.render(**kwargs)
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def send_email(self, to_email: str, subject: str, html_content: str) -> str:
        """Send individual email"""
        try:
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.from_email
            msg['To'] = to_email
            
            # Add HTML content
            html_part = MIMEText(html_content, 'html', 'utf-8')
            msg.attach(html_part)
            
            # Send email
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.send_message(msg)
            
            self.logger.info(f"Email sent successfully to {to_email}")
            return f"EMAIL_SENT_{to_email}"
            
        except Exception as e:
            self.logger.error(f"Error sending email to {to_email}: {e}")
            raise
    
    def send_emails(
        self, 
        data: TranscriptData, 
        summary: str, 
        doc_links: Tuple[str, str]
    ) -> Tuple[str, str]:
        """Send emails to both CS and AM"""
        call_doc_url, log_doc_url = doc_links
        
        # Template data
        template_data = {
            'account_name': data.account_name,
            'account_number': data.account_number,
            'speaker_name': data.speaker_name,
            'speaker_email': data.speaker_email,
            'cs_email': data.cs_email,
            'am_email': data.am_email,
            'summary': summary,
            'call_doc_url': call_doc_url,
            'log_doc_url': log_doc_url
        }
        
        # Send CS email
        cs_subject = f"Resumen de Llamada - {data.account_name}"
        cs_content = self.render_template('cs_template.html', **template_data)
        cs_status = self.send_email(data.cs_email, cs_subject, cs_content)
        
        # Send AM email
        am_subject = f"Resumen para AM - {data.account_name}"
        am_content = self.render_template('am_template.html', **template_data)
        am_status = self.send_email(data.am_email, am_subject, am_content)
        
        self.logger.info(
            f"Emails sent for transcript {data.transcript_id}: "
            f"CS={cs_status}, AM={am_status}"
        )
        
        return cs_status, am_status