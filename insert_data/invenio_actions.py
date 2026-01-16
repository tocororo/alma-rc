"""InvenioRDM API client with improved error handling and logging."""
from columns import *
import json
import logging
import os
from typing import Dict, List, Optional, Any
import requests

from columns import INVENIO_API_CONFIG

logger = logging.getLogger(__name__)


class InvenioClient:
    """Client for interacting with InvenioRDM API."""
    
    def __init__(self, base_url: Optional[str] = None, token: Optional[str] = None):
        """Initialize Invenio client.
        
        Args:
            base_url: Invenio API base URL
            token: API authentication token
        """
        self.base_url = base_url or INVENIO_API_CONFIG['base_url']
        self.token = token or INVENIO_API_CONFIG['token']
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.token}'
        }
    
    def _make_request(self, method: str, endpoint: str, **kwargs) -> Optional[requests.Response]:
        """Make HTTP request with error handling."""
        url = f"{self.base_url}{endpoint}"
        
        try:
            response = requests.request(method, url, headers=self.headers, verify=False, **kwargs)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            logger.error(f"HTTP error {method} {url}: {e}")
            if hasattr(e, 'response') and e.response:
                logger.error(f"Response: {e.response.text}")
            return None
    
    def _ensure_draft_exists(self, record_id: str) -> Optional[str]:
        """Get the ID of a draft for the given record. Creates one if it doesn't exist.
        
        Returns:
            The draft record ID, or None on failure.
        """
        # First, try to get the existing draft
        draft_response = self._make_request('GET', f'/api/records/{record_id}/draft')
        if draft_response and draft_response.status_code == 200:
            # Draft exists, return its ID (same as the parent record)
            logger.info(f"Found existing draft for record {record_id}")
            return record_id
        
        # If no draft (likely a published record), create one
        logger.info(f"No existing draft for {record_id}. Creating a new draft...")
        draft_create_response = self._make_request('POST', f'/api/records/{record_id}/draft')
        
        if draft_create_response and draft_create_response.status_code in (201, 200):
            # A 201 means a new draft was created. A 200 might mean one was already created.
            logger.info(f"Successfully created draft for record {record_id}")
            return record_id
        
        # Handle specific case: draft may already exist but with a different ID? (Edge case)
        # The API typically reuses the same ID for the draft of a published record.
        logger.error(f"Failed to create or retrieve draft for record {record_id}")
        return None

    def delete_all_records_and_drafts(self) -> None:
        """Delete all records and drafts from InvenioRDM instance."""
        logger.warning("Starting deletion of all records and drafts")
        
        records = self._fetch_all_records()
        if not records:
            logger.info("No records found to delete")
            return
        
        logger.info(f"Found {len(records)} records to delete")
        
        for record in records:
            self._delete_record(record)
        
        logger.info("Deletion process completed")
    
    def _fetch_all_records(self) -> List[Dict]:
        """Fetch all records from InvenioRDM."""
        response = self._make_request('GET', '/api/records', params={'size': 1000})
        if not response:
            return []
        return response.json().get('hits', {}).get('hits', [])
    
    def _delete_record(self, record: Dict) -> None:
        """Delete a single record or draft."""
        record_id = record['id']
        is_draft = record.get('is_draft', False)
        
        logger.info(f"Processing record {record_id} (draft: {is_draft})")
        
        if is_draft:
            self._delete_draft(record_id)
        else:
            self._delete_published_record(record_id)
    
    def _delete_draft(self, record_id: str) -> None:
        """Delete a draft record."""
        response = self._make_request('DELETE', f'/api/records/{record_id}/draft')
        if response and response.status_code == 204:
            logger.info(f"Deleted draft {record_id}")
        else:
            logger.error(f"Failed to delete draft {record_id}")
    
    def _delete_published_record(self, record_id: str) -> None:
        """Delete a published record by creating and deleting a draft."""
        # Create draft from published record
        response = self._make_request('POST', f'/api/records/{record_id}/draft')
        if not response or response.status_code not in (201, 400):
            logger.error(f"Failed to create draft for published record {record_id}")
            return
        
        # Delete the draft
        self._delete_draft(record_id)
    
    def search_record_exact(self, field: str, value: str) -> Optional[str]:
        """Search for record with exact field value match.
        
        Args:
            field: Field path in metadata schema
            value: Exact value to search for
            
        Returns:
            Record ID if found, None otherwise
        """
        query = f'{field}:"{value}"'
        response = self._make_request('GET', '/api/records', params={'q': query, 'size': 1})
        
        if not response:
            return None
        
        hits = response.json().get('hits', {}).get('hits', [])
        if hits:
            record_id = hits[0]['id']
            logger.info(f"Found record with {field}='{value}': {record_id}")
            return record_id
        
        logger.debug(f"No record found with {field}='{value}'")
        return None
    
    def create_or_update_record(self, record: Dict, record_id: Optional[str] = None,
                              bitstreams: Optional[List[str]] = None) -> Optional[str]:
        """Create or update a record in InvenioRDM.
        
        Args:
            record: Record metadata as dictionary
            record_id: Existing record ID for updates
            bitstreams: List of bitstream file paths
            
        Returns:
            Record ID if successful, None otherwise
        """
        if record_id is None:
            # CREATE: A brand new record
            return self._create_record(record, bitstreams)
        else:
            # UPDATE: An existing record (published or draft)
            return self._update_record(record_id, record, bitstreams)
           
    def _create_record(self, record: Dict, bitstreams: Optional[List[str]]) -> Optional[str]:
        """Create a new record."""
        response = self._make_request('POST', '/api/records', data=json.dumps(record))
        
        if not response or response.status_code != 201:
            logger.error(f"Failed to create record")
            return None
        
        record_id = response.json()["id"]
        logger.info(f"Created record: {record_id}")
        
        if bitstreams:
            self._upload_and_publish_files(record_id, bitstreams)
        
        return record_id
    
    def _update_record(self, record_id: str, record: Dict, bitstreams: Optional[List[str]] = None) -> Optional[str]:
        """Update an existing record by working on its draft, then publishing.
        
        This workflow updates the record WITHOUT creating a new version number.
        """
        # STEP 1: Ensure we have a draft to work with
        draft_id = self._ensure_draft_exists(record_id)
        if not draft_id:
            logger.error(f"Cannot proceed with update. Could not get/create draft for {record_id}")
            return None
        
        # STEP 2: Update the draft with new metadata
        logger.info(f"Updating draft metadata for {draft_id}")
        update_response = self._make_request('PUT', f'/api/records/{draft_id}/draft',
                                             data=json.dumps(record))
        
        if not update_response or update_response.status_code != 200:
            logger.error(f"Failed to update draft metadata for {draft_id}")
            return None
        
        # STEP 3: Handle file uploads if any
        if bitstreams:
            self._upload_and_publish_files(draft_id, bitstreams, publish_after=False)
        
        # STEP 4: Publish the draft to finalize the update
        logger.info(f"Publishing updated draft {draft_id}")
        publish_response = self._make_request('POST', f'/api/records/{draft_id}/draft/actions/publish')
        
        if publish_response and publish_response.status_code == 202:
            logger.info(f"Successfully updated and published record {draft_id}")
            return draft_id
        else:
            logger.error(f"Failed to publish draft {draft_id}. Record is left in draft state.")
            return None
    
    def _create_new_version(self, record_id: str, record: Dict) -> Optional[str]:
        """Create a new version of a published record."""
        response = self._make_request('POST', f'/api/records/{record_id}/versions')
        
        if not response or response.status_code != 201:
            logger.error(f"Failed to create new version for {record_id}")
            return None
        
        new_draft_id = response.json()["id"]
        logger.info(f"Created new version: {new_draft_id}")
        
        # Update the new draft
        return self._update_record(new_draft_id, record)
    
    def _upload_and_publish_files(self, record_id: str, file_paths: List[str], publish_after: bool = True) -> None:
        """Upload files to a record's draft.
        
        Args:
            record_id: The record (or draft) ID.
            file_paths: List of file paths to upload.
            publish_after: If True, publish the record after uploading files.
        """
        for file_path in file_paths:
            if self._upload_file(record_id, file_path):
                if publish_after:
                    self._publish_record(record_id)
    
    def _upload_file(self, record_id: str, file_path: str) -> Optional[str]:
        """Upload a single file to a record."""
        file_name = os.path.basename(file_path)
        
        # Initiate file upload
        upload_data = [{'key': file_name}]
        response = self._make_request('POST', f'/api/records/{record_id}/draft/files',
                                     data=json.dumps(upload_data))
        
        if not response or response.status_code != 201:
            logger.error(f"Failed to initiate upload for {file_name}")
            return None
        
        # Upload file content
        content_headers = {
            'Content-Type': 'application/octet-stream',
            'Authorization': f'Bearer {self.token}'
        }
        
        with open(file_path, 'rb') as file:
            upload_response = requests.put(
                f'{self.base_url}/api/records/{record_id}/draft/files/{file_name}/content',
                headers=content_headers,
                data=file,
                verify=False
            )
        
        if upload_response.status_code == 200:
            logger.info(f"Uploaded file {file_name}")
            self._commit_file(record_id, file_name)
            return file_name
        
        logger.error(f"Failed to upload file {file_name}")
        return None
    
    def _commit_file(self, record_id: str, file_name: str) -> None:
        """Commit an uploaded file."""
        response = self._make_request('POST', 
                                     f'/api/records/{record_id}/draft/files/{file_name}/commit')
        if response and response.status_code == 200:
            logger.info(f"Committed file {file_name}")
    
    def _publish_record(self, record_id: str) -> None:
        """Publish a record draft."""
        response = self._make_request('POST', 
                                     f'/api/records/{record_id}/draft/actions/publish')
        if response and response.status_code == 202:
            logger.info(f"Published record {record_id}")
        else:
            logger.error(f"Failed to publish record {record_id}")


# Global client instance for backward compatibility
_client = InvenioClient()


def delete_all_records_and_drafts():
    """Backward compatibility wrapper."""
    _client.delete_all_records_and_drafts()


def search_record_exact(field: str, value: str) -> Optional[str]:
    """Backward compatibility wrapper."""
    return _client.search_record_exact(field, value)


def create_or_update_record(record: Dict, record_id: Optional[str] = None,
                          subfolder_path: str = '', 
                          bitstreams: Optional[List[str]] = None) -> Optional[str]:
    """Backward compatibility wrapper."""
    return _client.create_or_update_record(record, record_id, bitstreams)


def create_record(record: Dict, subfolder_path: str = '', 
                 bitstreams: Optional[List[str]] = None) -> Optional[str]:
    """Backward compatibility wrapper."""
    return _client.create_or_update_record(record, bitstreams=bitstreams)


def upload_files(record_id: str, files: List[str]) -> Optional[str]:
    """Backward compatibility wrapper.
    
    Note: This function has different behavior than the refactored version.
    It returns the first successfully uploaded filename.
    """
    for file_path in files:
        file_name = _client._upload_file(record_id, file_path)
        if file_name:
            return file_name
    return None


def commit_files(record_id: str, file_name: str) -> None:
    """Backward compatibility wrapper."""
    _client._commit_file(record_id, file_name)


def publish_record(record_id: str) -> None:
    """Backward compatibility wrapper."""
    _client._publish_record(record_id)