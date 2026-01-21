"""DSpace bitstream harvester with improved logging and error handling."""

import logging
import os
import re
import time
import random
import urllib.parse
from datetime import datetime
from typing import Dict, Optional
from lxml import html
import requests

from alma_rc.config import Config

logger = logging.getLogger(__name__)


class DSpaceHarvester:
    """Harvester for DSpace bitstreams with rate limiting and error handling."""
    
    # Constants for bitstream processing
    BITSTREAM_PATH_PATTERN = '/bitstream/'
    SKIP_LINK_PATTERNS = ['open', 'view', 'show', 'display', 'accesar', 'ver', 'abrir']
    LARGE_FILE_THRESHOLD = 100 * 1024 * 1024  # 100MB
    CHUNK_SIZE = 8192
    
    def __init__(self, timeout: int = 30, delay: float = 1.0):
        """Initialize the DSpace harvester.
        
        Args:
            timeout: HTTP request timeout in seconds
            delay: Base delay between requests in seconds
        """
        self.timeout = timeout or Config.REQUEST_TIMEOUT
        self.delay = delay or Config.REQUEST_DELAY
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': Config.DEFAULT_USER_AGENT})
        
    def random_sleep(self) -> None:
        """Add random delay between requests to avoid being blocked."""
        random_delay = self.delay + (0.5 - random.random())
        time.sleep(random_delay)
        
    @staticmethod
    def clean_filename(filename: str) -> str:
        """Clean filename to be filesystem safe.
        
        Args:
            filename: Original filename
            
        Returns:
            Cleaned filename
        """
        # Remove invalid characters
        filename = re.sub(r'[<>:"/\\|?*]', '', filename)
        # Replace spaces with underscores
        filename = filename.replace(' ', '_')
        # Limit length
        if len(filename) > Config.MAX_FILENAME_LENGTH:
            name, ext = os.path.splitext(filename)
            truncated_name = name[:150] + ext
            return truncated_name.strip()
        return filename.strip()
    
    def get_bitstream_urls(self, handle_url: str) -> Dict[str, str]:
        """Extract bitstream URLs from DSpace handle page.
        
        Args:
            handle_url: DSpace handle URL
            
        Returns:
            Dictionary mapping filenames to URLs
        """
        try:
            self.random_sleep()
            response = self.session.get(handle_url, timeout=self.timeout)
            response.raise_for_status()
            
            tree = html.fromstring(response.content)
            bitstream_links = tree.xpath(f'//a[contains(@href, "{self.BITSTREAM_PATH_PATTERN}")]')
            
            urls = {}
            for link in bitstream_links:
                href = link.get('href', '')
                if not href:
                    continue
                    
                full_url = self._construct_full_url(href, handle_url)
                filename = self._extract_filename(link, full_url)
                
                if not self._should_process_link(link, filename):
                    continue
                    
                urls[filename] = full_url
                
            logger.debug(f"Found {len(urls)} bitstream URLs from {handle_url}")
            return urls
            
        except requests.RequestException as e:
            logger.error(f"HTTP error extracting bitstreams from {handle_url}: {e}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error extracting bitstreams from {handle_url}: {e}")
            return {}
    
    def _construct_full_url(self, href: str, base_url: str) -> str:
        """Construct full URL from relative or absolute href."""
        if href.startswith('/'):
            parsed_url = urllib.parse.urlparse(base_url)
            return f"{parsed_url.scheme}://{parsed_url.netloc}{href}"
        elif not href.startswith('http'):
            return urllib.parse.urljoin(base_url, href)
        return href
    
    def _extract_filename(self, link, url: str) -> str:
        """Extract filename from link text or URL path."""
        filename = link.text_content().strip() or os.path.basename(urllib.parse.urlparse(url).path)
        return self.clean_filename(filename)
    
    def _should_process_link(self, link, filename: str) -> bool:
        """Determine if a link should be processed as a file download."""
        # Skip if filename doesn't have an extension
        if '.' not in filename or filename.split('.')[-1].strip() == '':
            logger.debug(f"Skipping link without extension: {filename}")
            return False
            
        # Skip common non-file link texts
        link_text_lower = link.text_content().lower().strip()
        if any(pattern in link_text_lower for pattern in self.SKIP_LINK_PATTERNS):
            logger.debug(f"Skipping likely Open/View link: {link_text_lower}")
            return False
            
        return True
    
    def download_bitstream(self, url: str, output_path: str) -> bool:
        """Download a single bitstream file.
        
        Args:
            url: Bitstream URL
            output_path: Local path to save the file
            
        Returns:
            True if download successful, False otherwise
        """
        try:
            logger.info(f"Downloading bitstream from {url}")
            self.random_sleep()
            
            response = self.session.get(url, timeout=self.timeout, stream=True)
            response.raise_for_status()
            
            self._check_file_size(response)
            self._save_file(response, output_path)
            
            file_size = os.path.getsize(output_path)
            logger.info(f"Successfully downloaded {output_path} ({file_size} bytes)")
            return True
            
        except Exception as e:
            self._cleanup_partial_download(output_path)
            logger.error(f"HTTP error downloading {url}: {e}")
            
        return False
    
    def _check_file_size(self, response: requests.Response) -> None:
        """Check if file size exceeds threshold and log warning."""
        content_length = response.headers.get('content-length')
        if content_length and int(content_length) > self.LARGE_FILE_THRESHOLD:
            logger.warning(f"Large file detected: {content_length} bytes")
    
    def _save_file(self, response: requests.Response, output_path: str) -> None:
        """Save file in chunks to handle large files."""
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=self.CHUNK_SIZE):
                if chunk:
                    f.write(chunk)
    
    def _cleanup_partial_download(self, output_path: str) -> None:
        """Clean up partial download if it exists."""
        if os.path.exists(output_path):
            try:
                os.remove(output_path)
                logger.debug(f"Cleaned up partial download: {output_path}")
            except OSError as e:
                logger.warning(f"Failed to clean up partial download {output_path}: {e}")
    
    def get_and_save_bitstreams(self, handle_url: str, output_base_dir: str, 
                               record_id: str) -> Dict[str, str]:
        """Get bitstream URLs and save files in organized directories.
        
        Args:
            handle_url: DSpace handle URL
            output_base_dir: Base directory for saving files
            record_id: Record identifier for directory naming
            
        Returns:
            Dictionary mapping filenames to saved paths
        """
        try:
            safe_record_id = self._sanitize_record_id(record_id)
            bitstream_urls = self.get_bitstream_urls(handle_url)
            
            if not bitstream_urls:
                logger.info(f"No bitstreams found for {handle_url}")
                return {}
            
            logger.info(f"Found {len(bitstream_urls)} bitstreams for {handle_url}")
            
            saved_files = self._process_bitstreams(bitstream_urls, output_base_dir, safe_record_id)
            self._log_processing_summary(saved_files, record_id)
            
            return saved_files
            
        except Exception as e:
            logger.error(f"Error in get_and_save_bitstreams for {handle_url}: {e}")
            return {}
    
    @staticmethod
    def _sanitize_record_id(record_id: str) -> str:
        """Sanitize record ID for use in directory names."""
        return re.sub(r'[^\w\-_\.]', '_', str(record_id))
    
    def _process_bitstreams(self, bitstream_urls: Dict[str, str], 
                           output_base_dir: str, safe_record_id: str) -> Dict[str, str]:
        """Process and download all bitstreams."""
        saved_files = {}
        bitstream_dir = os.path.join(output_base_dir, safe_record_id)
        os.makedirs(bitstream_dir, exist_ok=True)
        
        for i, (filename, url) in enumerate(bitstream_urls.items(), 1):
            try:
                file_path = os.path.join(bitstream_dir, filename)
                if self.download_bitstream(url, file_path):
                    saved_files[filename] = file_path
                    logger.info(f"Saved bitstream {i}/{len(bitstream_urls)}: {file_path}")
            except Exception as e:
                logger.error(f"Error processing bitstream {filename}: {e}")
                continue
                
        return saved_files
    
    def _log_processing_summary(self, saved_files: Dict[str, str], record_id: str) -> None:
        """Log summary of bitstream processing."""
        if saved_files:
            logger.info(f"Successfully saved {len(saved_files)} bitstreams for record {record_id}")
        else:
            logger.warning(f"No bitstreams were successfully saved for record {record_id}")
    
    def process_record_with_bitstreams(self, record_id: str, handle_url: str, 
                                      output_dir: str) -> Dict[str, str]:
        """Process a DSpace record including downloading bitstreams.
        
        Args:
            record_id: Record identifier
            handle_url: DSpace handle URL
            output_dir: Output directory
            
        Returns:
            Dictionary of bitstream results
        """
        try:
            if not handle_url:
                logger.warning(f"No handle URL found for record {record_id}")
                return {}
            
            logger.info(f"Processing bitstreams for record {record_id} from {handle_url}")
            bitstream_results = self.get_and_save_bitstreams(
                handle_url, 
                os.path.join(output_dir, 'bitstreams'), 
                record_id
            )
            
            return bitstream_results
            
        except Exception as e:
            logger.error(f"Error processing record with bitstreams: {e}")
            return {}