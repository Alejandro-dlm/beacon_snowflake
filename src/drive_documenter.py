"""
Módulo 4 — Google Drive Documenter
Crea/actualiza documentos en Google Drive por cliente.
"""

import os
import logging
from datetime import datetime
from typing import Tuple, Optional
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv()

class GoogleDriveDocumenter:
    def __init__(self):
        self.creds_path = os.getenv('GOOGLE_CREDS_JSON')
        self.scopes = [
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/documents'
        ]
        
        self.credentials = Credentials.from_service_account_file(
            self.creds_path, scopes=self.scopes
        )
        
        self.drive_service = build('drive', 'v3', credentials=self.credentials)
        self.docs_service = build('docs', 'v1', credentials=self.credentials)
        
        self.logger = logging.getLogger(__name__)
        self.clients_folder_name = "Clientes"
    
    def get_or_create_clients_folder(self) -> str:
        """Get or create the main 'Clientes' folder"""
        query = f"name='{self.clients_folder_name}' and mimeType='application/vnd.google-apps.folder'"
        results = self.drive_service.files().list(q=query).execute()
        items = results.get('files', [])
        
        if items:
            return items[0]['id']
        
        # Create folder
        folder_metadata = {
            'name': self.clients_folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        folder = self.drive_service.files().create(body=folder_metadata).execute()
        self.logger.info(f"Created main folder: {self.clients_folder_name}")
        return folder['id']
    
    def get_or_create_account_folder(self, account_name: str) -> str:
        """Get or create folder for specific account"""
        clients_folder_id = self.get_or_create_clients_folder()
        
        query = f"name='{account_name}' and mimeType='application/vnd.google-apps.folder' and '{clients_folder_id}' in parents"
        results = self.drive_service.files().list(q=query).execute()
        items = results.get('files', [])
        
        if items:
            return items[0]['id']
        
        # Create account folder
        folder_metadata = {
            'name': account_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [clients_folder_id]
        }
        folder = self.drive_service.files().create(body=folder_metadata).execute()
        self.logger.info(f"Created account folder: {account_name}")
        return folder['id']
    
    def get_or_create_doc(self, folder_id: str, doc_name: str) -> str:
        """Get or create a Google Doc in the specified folder"""
        query = f"name='{doc_name}' and mimeType='application/vnd.google-apps.document' and '{folder_id}' in parents"
        results = self.drive_service.files().list(q=query).execute()
        items = results.get('files', [])
        
        if items:
            return items[0]['id']
        
        # Create document
        doc_metadata = {
            'name': doc_name,
            'mimeType': 'application/vnd.google-apps.document',
            'parents': [folder_id]
        }
        doc = self.drive_service.files().create(body=doc_metadata).execute()
        self.logger.info(f"Created document: {doc_name}")
        return doc['id']
    
    def overwrite_doc(self, doc_id: str, content: str):
        """Completely replace document content"""
        # Get current document to find content length
        doc = self.docs_service.documents().get(documentId=doc_id).execute()
        
        # Delete all content first
        if doc['body']['content']:
            end_index = doc['body']['content'][-1]['endIndex'] - 1
            if end_index > 1:
                requests = [{
                    'deleteContentRange': {
                        'range': {
                            'startIndex': 1,
                            'endIndex': end_index
                        }
                    }
                }]
                self.docs_service.documents().batchUpdate(
                    documentId=doc_id, body={'requests': requests}
                ).execute()
        
        # Insert new content
        requests = [{
            'insertText': {
                'location': {'index': 1},
                'text': content
            }
        }]
        self.docs_service.documents().batchUpdate(
            documentId=doc_id, body={'requests': requests}
        ).execute()
    
    def append_to_doc(self, doc_id: str, content: str):
        """Append content to the end of document"""
        doc = self.docs_service.documents().get(documentId=doc_id).execute()
        end_index = doc['body']['content'][-1]['endIndex'] - 1
        
        requests = [{
            'insertText': {
                'location': {'index': end_index},
                'text': content
            }
        }]
        self.docs_service.documents().batchUpdate(
            documentId=doc_id, body={'requests': requests}
        ).execute()
    
    def get_doc_url(self, doc_id: str) -> str:
        """Get shareable URL for document"""
        return f"https://docs.google.com/document/d/{doc_id}/edit"
    
    def update_drive_docs(self, account_name: str, summary: str) -> Tuple[str, str]:
        """Main function to update both documents for an account"""
        try:
            # Get or create account folder
            folder_id = self.get_or_create_account_folder(account_name)
            
            # Handle Call Summary document
            call_doc_id = self.get_or_create_doc(folder_id, "Call Summary")
            self.overwrite_doc(call_doc_id, summary)
            call_doc_url = self.get_doc_url(call_doc_id)
            
            # Handle Summary Log document
            log_doc_id = self.get_or_create_doc(folder_id, "Summary Log")
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
            log_entry = f"\n--- {timestamp} ---\n{summary}\n"
            self.append_to_doc(log_doc_id, log_entry)
            log_doc_url = self.get_doc_url(log_doc_id)
            
            self.logger.info(f"Updated Drive documents for account: {account_name}")
            return call_doc_url, log_doc_url
            
        except Exception as e:
            self.logger.error(f"Error updating Drive documents for {account_name}: {e}")
            raise