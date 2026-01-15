
import logging
import time
import requests
import random
import re

import os
from typing import Dict
import urllib
from lxml import html
from datetime import datetime

class DSpaceHarvester:
    def __init__(self, timeout: int = 30, delay: float = 1.0):
        self.timeout = timeout
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO)

    def random_sleep(self):
        """Random delay between requests to avoid being blocked"""
        time.sleep(self.delay + (0.5 - random.random()))

    def clean_filename(self, filename: str) -> str:
        """Clean filename to be filesystem safe"""
        # Remove invalid characters
        filename = re.sub(r'[<>:"/\\|?*]', '', filename)
        # Replace spaces with underscores
        filename = filename.replace(' ', '_')
        # Limit length
        if len(filename) > 200:
            name, ext = os.path.splitext(filename)
            filename = name[:150] + ext
        return filename.strip()

    def get_bitstream_urls(self, handle_url: str) -> Dict[str, str]:
        """Extract bitstream URLs - only files with extensions (skip Open/View links)"""
        try:
            self.random_sleep()
            response = self.session.get(handle_url, timeout=self.timeout)
            response.raise_for_status()
            
            tree = html.fromstring(response.content)
            bitstream_links = tree.xpath('//a[contains(@href, "/bitstream/")]')
            
            urls = {}
            for link in bitstream_links:
                href = link.get('href', '')
                if not href:
                    continue
                    
                # Construct full URL
                if href.startswith('/'):
                    parsed_url = urllib.parse.urlparse(handle_url)
                    href = f"{parsed_url.scheme}://{parsed_url.netloc}{href}"
                elif not href.startswith('http'):
                    href = urllib.parse.urljoin(handle_url, href)
                
                # Get filename from link text or URL path
                filename = link.text_content().strip() or os.path.basename(urllib.parse.urlparse(href).path)
                filename = self.clean_filename(filename)
                
                # Skip if filename doesn't have an extension (likely "Open/View" links)
                if '.' not in filename or filename.split('.')[-1].strip() == '':
                    self.logger.debug(f"Skipping link without extension: {filename}")
                    continue
                    
                # Additional check: skip common non-file link texts
                link_text_lower = link.text_content().lower().strip()
                skip_patterns = ['open', 'view', 'show', 'display', 'accesar', 'ver', 'abrir']
                if any(pattern in link_text_lower for pattern in skip_patterns):
                    self.logger.debug(f"Skipping likely Open/View link: {link_text_lower}")
                    continue
                
                urls[filename] = href
                
            return urls
            
        except Exception as e:
            self.logger.error(f"Error extracting bitstreams from {handle_url}: {e}")
            return {}

    def download_bitstream(self, url: str, output_path: str) -> bool:
        """Download a single bitstream file"""
        try:
            self.logger.info(f"Downloading: {url}")
            self.random_sleep()
            
            response = self.session.get(url, timeout=self.timeout, stream=True)
            response.raise_for_status()
            
            # Get content length if available
            content_length = response.headers.get('content-length')
            if content_length and int(content_length) > 100 * 1024 * 1024:  # 100MB
                self.logger.warning(f"Large file detected: {content_length} bytes")
            
            # Write file in chunks to handle large files
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            file_size = os.path.getsize(output_path)
            self.logger.info(f"Downloaded {output_path} ({file_size} bytes)")
            return True
            
        except Exception as e:
            self.logger.error(f"Error downloading {url}: {e}")
            if os.path.exists(output_path):
                os.remove(output_path)  # Clean up partial downloads
            return False

    def get_and_save_bitstreams(self, handle_url: str, output_base_dir: str, record_id: str) -> Dict:
        """
        Get bitstream URLs and save the files in separate directories
        
        Args:
            handle_url: The DSpace handle URL
            output_base_dir: Base directory where files will be saved
            record_id: Record identifier for directory naming
            
        Returns:
            Dictionary mapping filenames to their saved paths
        """
        try:
            # Clean record_id for directory name
            safe_record_id = re.sub(r'[^\w\-_\.]', '_', str(record_id))
            
            # Get bitstream URLs
            bitstream_urls = self.get_bitstream_urls(handle_url)
            
            if not bitstream_urls:
                self.logger.info(f"No bitstreams found for {handle_url}")
                return {}
            
            self.logger.info(f"Found {len(bitstream_urls)} bitstreams for {handle_url}")
            
            saved_files = {}
            
            # Process each bitstream
            bitstream_dir = os.path.join(
                output_base_dir, 
                safe_record_id
            )
            os.makedirs(bitstream_dir, exist_ok=True)
            for i, (filename, url) in enumerate(bitstream_urls.items(), 1):
                try:
                    # Create separate directory for each bitstream
                    # Format: {output_base_dir}/{record_id}/bitstream_{index}_{filename}/
                    
                    
                    
                    
                    # Save file in the bitstream directory
                    file_path = os.path.join(bitstream_dir, filename)
                    
                    # Download the file
                    if self.download_bitstream(url, file_path):
                        saved_files[filename] = file_path
                        self.logger.info(f"Saved bitstream {i}/{len(bitstream_urls)}: {file_path}")
                        
                        # Save metadata about the bitstream
                        metadata_path = os.path.join(bitstream_dir, 'bitstream_metadata.json')
                        metadata = {
                            'filename': filename,
                            'original_url': url,
                            'download_path': file_path,
                            'record_id': record_id,
                            'handle_url': handle_url,
                            'download_time': datetime.now().isoformat()
                        }
                        
                        # with open(metadata_path, 'w', encoding='utf-8') as f:
                        #     json.dump(metadata, f, indent=2, ensure_ascii=False)
                    
                except Exception as e:
                    self.logger.error(f"Error processing bitstream {filename}: {e}")
                    continue
            
            if saved_files:
                self.logger.info(f"Successfully saved {len(saved_files)} bitstreams for record {record_id}")
            else:
                self.logger.warning(f"No bitstreams were successfully saved for record {record_id}")
            
            return saved_files
            
        except Exception as e:
            self.logger.error(f"Error in get_and_save_bitstreams for {handle_url}: {e}")
            import traceback
            traceback.print_exc()
            return {}

    def process_record_with_bitstreams(self, record_id, handle_url: str, output_dir: str) -> Dict:
        """
        Process a single DSpace record including downloading bitstreams
        
        Args:
            row: Pandas Series containing DSpace record data
            output_dir: Base output directory
            
        Returns:
            Tuple of (record_id, invenio_record, bitstream_results)
        """
        try:

            bitstream_results = {}
            if handle_url:
                self.logger.info(f"Processing bitstreams for record {record_id} from {handle_url}")
                bitstream_results = self.get_and_save_bitstreams(
                    handle_url, 
                    os.path.join(output_dir, 'bitstreams'), 
                    record_id
                )
            else:
                self.logger.warning(f"No handle URL found for record {record_id}")
            
            # Save bitstream information to the record
            # if bitstream_results:
            #     if not hasattr(invenio_record, 'custom_fields'):
            #         invenio_record.custom_fields = {}
                
            #     bitstream_info = []
            #     for filename, file_path in bitstream_results.items():
            #         bitstream_info.append({
            #             'filename': filename,
            #             'path': file_path,
            #             'size': os.path.getsize(file_path) if os.path.exists(file_path) else 0
            #         })
                
            #     invenio_record.custom_fields['bitstreams'] = bitstream_info
            
            return bitstream_results
            
        except Exception as e:
            self.logger.error(f"Error processing record with bitstreams: {e}")
            import traceback
            traceback.print_exc()
            return None, None, {}
