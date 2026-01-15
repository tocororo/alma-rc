from codecs import ignore_errors
import os
import logging
import time
import random
import json
import signal
import sys
from typing import Dict, List, Tuple, Optional
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, asdict
import hashlib

from sickle import Sickle
from sickle.oaiexceptions import BadArgument, CannotDisseminateFormat, NoRecordsMatch
import requests
from lxml import html
import urllib.parse
import urllib3
from urllib3.exceptions import InsecureRequestWarning

# Suppress insecure request warnings
urllib3.disable_warnings(InsecureRequestWarning)

@dataclass
class HarvestStats:
    """Statistics for the harvest process"""
    total_documents: int = 0
    successful_documents: int = 0
    failed_documents: int = 0
    files_downloaded: int = 0
    total_bytes_downloaded: int = 0
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    formats_available: List[str] = None
    failed_records: Dict[str, List[str]] = None
    partial_failures: Dict[str, List[str]] = None
    download_errors: List[str] = None
    processed_identifiers: List[str] = None  # Track processed IDs for resume
    
    def __post_init__(self):
        if self.formats_available is None:
            self.formats_available = []
        if self.failed_records is None:
            self.failed_records = {}
        if self.partial_failures is None:
            self.partial_failures = {}
        if self.download_errors is None:
            self.download_errors = []
        if self.processed_identifiers is None:
            self.processed_identifiers = []

class DSpaceHarvester:
    def __init__(self, config: Dict):
        """
        Initialize the DSpace harvester with configuration
        """
        self.oai_endpoint = config.get('oai_endpoint', "https://rc.upr.edu.cu/oai/request")
        self.output_dir = Path(config.get('output_dir', ".data/metadatos_dspace"))
        self.log_file = config.get('log_file', "harvest.log")
        self.base_handle_url = config.get('base_handle_url', "https://rc.upr.edu.cu/handle/")
        self.min_delay = config.get('min_delay', 1)
        self.max_delay = config.get('max_delay', 5)
        self.batch_size = config.get('batch_size', 10)
        self.max_retries = config.get('max_retries', 3)
        self.timeout = config.get('timeout', 30)
        self.user_agent = config.get('user_agent', 
            'DSpace-Harvester/1.0 (+https://github.com/your-repo)')
        self.checkpoint_interval = config.get('checkpoint_interval', 10)  # Save checkpoint every N records
        
        # Initialize statistics
        self.stats = HarvestStats()
        self.stats.start_time = datetime.now().isoformat()
        
        # Resume tracking
        self.checkpoint_file = self.output_dir / "checkpoint.json"
        self.processed_file = self.output_dir / "processed_ids.json"
        self.state_file = self.output_dir / "harvest_state.json"
        
        # Setup
        self.setup_logging()
        self.setup_signal_handlers()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Session
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': self.user_agent})
        self.session.verify = False
        
        # OAI client
        self.sickle = Sickle(self.oai_endpoint)
        
        # Control flags
        self.should_stop = False
        
    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
    def signal_handler(self, signum, frame):
        """Handle interrupt signals"""
        self.logger.info(f"\nReceived signal {signum}, initiating graceful shutdown...")
        self.should_stop = True
        
    def setup_logging(self):
        """Configure logging"""
        detailed_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s"
        )
        simple_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s"
        )
        
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        logger.handlers.clear()
        
        # File handler
        file_handler = logging.FileHandler(self.log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(detailed_formatter)
        logger.addHandler(file_handler)
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(simple_formatter)
        logger.addHandler(console_handler)
        
        self.logger = logging.getLogger(__name__)
    
    def save_state(self):
        """Save complete harvest state for resuming"""
        state = {
            'config': {
                'oai_endpoint': self.oai_endpoint,
                'output_dir': str(self.output_dir),
                'base_handle_url': self.base_handle_url,
            },
            'stats': asdict(self.stats),
            'timestamp': datetime.now().isoformat()
        }
        
        try:
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(state, f, indent=2, ensure_ascii=False)
            self.logger.debug("Harvest state saved")
        except Exception as e:
            self.logger.error(f"Failed to save state: {e}")
    
    def load_state(self) -> bool:
        """Load harvest state if exists"""
        if not self.state_file.exists():
            return False
            
        try:
            with open(self.state_file, 'r', encoding='utf-8') as f:
                state = json.load(f)
            
            # Restore statistics
            stats_dict = state['stats']
            self.stats = HarvestStats(**stats_dict)
            
            # Restore processed identifiers from backup file if available
            if self.processed_file.exists():
                with open(self.processed_file, 'r', encoding='utf-8') as f:
                    processed = json.load(f)
                    self.stats.processed_identifiers = processed
            
            self.logger.info(f"Loaded harvest state with {len(self.stats.processed_identifiers)} processed records")
            return True
        except Exception as e:
            self.logger.error(f"Error loading state: {e}")
            return False
    
    def save_processed_ids(self):
        """Save processed identifiers to file"""
        try:
            with open(self.processed_file, 'w', encoding='utf-8') as f:
                json.dump(self.stats.processed_identifiers, f, indent=2)
        except Exception as e:
            self.logger.error(f"Failed to save processed IDs: {e}")
    
    def save_checkpoint(self, identifier: str, force: bool = False):
        """
        Save checkpoint for resuming
        
        Args:
            identifier: Last successfully processed identifier
            force: Force save even if not at interval
        """
        if not force and self.stats.total_documents % self.checkpoint_interval != 0:
            return
            
        try:
            # Save state
            self.save_state()
            
            # Save checkpoint marker
            checkpoint = {
                'last_identifier': identifier,
                'timestamp': datetime.now().isoformat(),
                'stats_summary': {
                    'total_documents': self.stats.total_documents,
                    'successful_documents': self.stats.successful_documents,
                    'files_downloaded': self.stats.files_downloaded
                }
            }
            
            with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(checkpoint, f, indent=2)
            
            # Save processed IDs
            self.save_processed_ids()
            
            self.logger.debug(f"Checkpoint saved: {identifier}")
            
        except Exception as e:
            self.logger.error(f"Failed to save checkpoint: {e}")
    
    def load_checkpoint(self) -> Optional[str]:
        """Load last checkpoint identifier"""
        if not self.checkpoint_file.exists():
            return None
            
        try:
            with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                checkpoint = json.load(f)
            return checkpoint['last_identifier']
        except Exception as e:
            self.logger.error(f"Error loading checkpoint: {e}")
            return None
    
    def clean_incomplete_record(self, doc_id: str):
        """Clean incomplete record data if resuming"""
        doc_dir = self.output_dir / doc_id
        if doc_dir.exists():
            # Check if record was completed (has metadata files)
            metadata_files = list(doc_dir.glob("metadata_*.xml"))
            if not metadata_files:
                # Incomplete record, clean it
                import shutil
                try:
                    shutil.rmtree(doc_dir)
                    self.logger.info(f"Cleaned incomplete record: {doc_id}")
                except Exception as e:
                    self.logger.error(f"Failed to clean record {doc_id}: {e}")
    
    def random_sleep(self, base_delay: float = None):
        """Random sleep to avoid overwhelming server"""
        if base_delay:
            delay = base_delay + random.uniform(0, 1)
        else:
            delay = random.uniform(self.min_delay, self.max_delay)
        
        self.logger.debug(f"Sleeping for {delay:.2f} seconds")
        time.sleep(delay)
    
    def get_skip_list(self) -> List[str]:
        """Get list of already processed document IDs"""
        skip_list = []
        
        # Check output directory for existing records
        for item in self.output_dir.iterdir():
            if item.is_dir():
                # Check if directory has metadata files
                metadata_files = list(item.glob("metadata_*.xml"))
                if metadata_files:
                    skip_list.append(item.name)
        
        return skip_list
    
    def harvest_all(self, max_records: Optional[int] = None, 
                    resume: bool = True, 
                    skip_existing: bool = True):
        """
        Harvest all records with resume capability
        
        Args:
            max_records: Maximum number of records to harvest
            resume: Resume from last checkpoint if available
            skip_existing: Skip records that already exist in output directory
        """
        self.logger.info("Starting DSpace harvest")
        
        # Load previous state if resuming
        if resume:
            if self.load_state():
                self.logger.info(f"Resumed from saved state with {len(self.stats.processed_identifiers)} processed records")
            else:
                self.logger.info("No previous state found, starting fresh harvest")
        
        try:
            # Get available metadata formats
            formats = [fmt.metadataPrefix for fmt in self.sickle.ListMetadataFormats()]
            self.stats.formats_available = formats
            self.logger.info(f"Available metadata formats: {formats}")
            
            # Get all identifiers
            self.logger.info("Fetching record identifiers...")
            identifiers = list(self.sickle.ListIdentifiers(ignore_deleted=False, metadataPrefix='oai_dc'))
            
            if max_records:
                identifiers = identifiers[:max_records]
            
            total_identifiers = len(identifiers)
            self.logger.info(f"Total records to process: {total_identifiers}")
            
            # Get skip list
            skip_list = self.get_skip_list() if skip_existing else []
            if skip_list:
                self.logger.info(f"Skipping {len(skip_list)} already processed records")
            
            # Also skip from processed_identifiers if resuming
            if resume and self.stats.processed_identifiers:
                skip_list.extend(self.stats.processed_identifiers)
                skip_list = list(set(skip_list))  # Remove duplicates
            
            # Process records
            processed_count = 0
            for idx, identifier in enumerate(identifiers, 1):
                # Check if we should stop
                if self.should_stop:
                    self.logger.info("Stop signal received, saving state...")
                    self.save_checkpoint(identifier.identifier, force=True)
                    break
                
                doc_id = identifier.identifier.split(":")[-1]
                
                # Skip if already processed
                if doc_id in skip_list:
                    self.logger.debug(f"Skipping already processed record: {doc_id}")
                    continue
                
                # Clean incomplete record if exists
                self.clean_incomplete_record(doc_id)
                
                # Harvest record
                try:
                    self.logger.info(f"[{idx}/{total_identifiers}] Processing: {doc_id}")
                    
                    # Harvest metadata
                    doc_dir = self.output_dir / doc_id
                    doc_dir.mkdir(parents=True, exist_ok=True)
                    
                    metadata_success = False
                    for fmt in formats:
                        try:
                            metadata = self.sickle.GetRecord(
                                identifier=identifier.identifier, 
                                metadataPrefix=fmt
                            )
                            metadata_file = doc_dir / f"metadata_{fmt}.xml"
                            with open(metadata_file, "w", encoding="utf-8") as f:
                                f.write(metadata.raw)
                            metadata_success = True
                            self.logger.debug(f"  - Metadata {fmt}: OK")
                        except Exception as e:
                            self.logger.warning(f"  - Metadata {fmt}: Failed - {e}")
                    
                    # Download bitstreams if metadata was successful
                    if metadata_success:
                        handle_url = f"{self.base_handle_url.rstrip('/')}/{doc_id}"
                        bitstreams = self.get_bitstream_urls(handle_url)
                        
                        if bitstreams:
                            files_dir = doc_dir / "files"
                            files_dir.mkdir(exist_ok=True)
                            
                            files_downloaded = 0
                            total_bytes = 0
                            
                            for filename, url in bitstreams.items():
                                save_path = files_dir / filename
                                success, file_size = self.download_with_retry(url, save_path)
                                if success:
                                    files_downloaded += 1
                                    total_bytes += file_size
                                    self.random_sleep()
                            
                            self.stats.files_downloaded += files_downloaded
                            self.stats.total_bytes_downloaded += total_bytes
                            self.logger.info(f"  - Downloaded {files_downloaded} files ({total_bytes:,} bytes)")
                        
                        # Record as successful
                        self.stats.successful_documents += 1
                        self.stats.processed_identifiers.append(doc_id)
                    else:
                        self.stats.failed_documents += 1
                        self.stats.failed_records[doc_id] = ["All metadata formats failed"]
                    
                    self.stats.total_documents += 1
                    processed_count += 1
                    
                    # Save checkpoint periodically
                    if processed_count % self.checkpoint_interval == 0:
                        self.save_checkpoint(identifier.identifier)
                        self.logger.info(f"Checkpoint saved. Progress: {idx}/{total_identifiers}")
                    
                    # Random sleep between records
                    self.random_sleep()
                    
                except Exception as e:
                    self.stats.failed_documents += 1
                    self.stats.failed_records[doc_id] = [str(e)]
                    self.logger.error(f"Error processing {doc_id}: {e}")
                    continue
            
            # Final save
            self.save_state()
            
            # Remove checkpoint file if completed
            if not self.should_stop and processed_count == len(identifiers):
                if self.checkpoint_file.exists():
                    self.checkpoint_file.unlink()
                self.logger.info("Harvest completed successfully")
            
            # Generate report
            self.generate_report()
            
        except NoRecordsMatch:
            self.logger.error("No records match the specified criteria")
        except Exception as e:
            self.logger.error(f"Harvest failed: {e}")
            raise
    
    def get_bitstream_urls(self, handle_url: str) -> Dict[str, str]:
        """Extract bitstream URLs"""
        try:
            self.random_sleep()
            response = self.session.get(handle_url, timeout=self.timeout)
            response.raise_for_status()
            
            tree = html.fromstring(response.content)
            bitstream_links = tree.xpath('//a[contains(@href, "/bitstream/")]')
            
            urls = {}
            for link in bitstream_links:
                href = link.get('href', '')
                if href:
                    if href.startswith('/'):
                        parsed_url = urllib.parse.urlparse(handle_url)
                        href = f"{parsed_url.scheme}://{parsed_url.netloc}{href}"
                    elif not href.startswith('http'):
                        href = urllib.parse.urljoin(handle_url, href)
                    
                    filename = link.text_content().strip() or os.path.basename(urllib.parse.urlparse(href).path)
                    filename = self.clean_filename(filename)
                    urls[filename] = href
            
            return urls
            
        except Exception as e:
            self.logger.error(f"Error extracting bitstreams: {e}")
            return {}
    
    def download_with_retry(self, url: str, save_path: Path, retry_count: int = 0) -> Tuple[bool, int]:
        """Download with retry logic"""
        try:
            response = self.session.get(url, timeout=self.timeout, stream=True)
            response.raise_for_status()
            
            file_size = int(response.headers.get('content-length', 0))
            downloaded = 0
            
            with open(save_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
            
            actual_size = save_path.stat().st_size
            return True, actual_size
            
        except Exception as e:
            if retry_count < self.max_retries:
                self.logger.warning(f"Retry {retry_count + 1} for {url}")
                time.sleep(2 ** retry_count)
                return self.download_with_retry(url, save_path, retry_count + 1)
            else:
                return False, 0
    
    def clean_filename(self, filename: str) -> str:
        """Clean filename"""
        invalid_chars = '<>:"/\\|?*\'"'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        return filename[:200] if len(filename) > 200 else filename
    
    def generate_report(self):
        """Generate harvest report"""
        report = {
            'summary': asdict(self.stats),
            'timestamp': datetime.now().isoformat()
        }
        
        report_file = self.output_dir / f"harvest_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        # Also generate text report
        txt_file = self.output_dir / f"harvest_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(txt_file, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write("DSpace Harvest Report\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Total Documents: {self.stats.total_documents}\n")
            f.write(f"Successful: {self.stats.successful_documents}\n")
            f.write(f"Failed: {self.stats.failed_documents}\n")
            f.write(f"Files Downloaded: {self.stats.files_downloaded}\n")
            f.write(f"Total Data: {self.stats.total_bytes_downloaded:,} bytes\n")
            
            if self.stats.failed_records:
                f.write("\nFailed Records:\n")
                for doc_id, errors in self.stats.failed_records.items():
                    f.write(f"  - {doc_id}: {', '.join(errors)}\n")
        
        self.logger.info(f"Report saved to: {report_file}")

def main():
    """Main execution"""
    config = {
        'oai_endpoint': "https://rc.upr.edu.cu/oai/request",
        'output_dir': ".data/harvest",
        'log_file': ".data/harvest/harvest.log",
        'base_handle_url': "https://rc.upr.edu.cu/handle/",
        'min_delay': 1,
        'max_delay': 5,
        'batch_size': 20,
        'max_retries': 3,
        'timeout': 30,
        'checkpoint_interval': 10,  # Save checkpoint every 10 records
        'user_agent': 'DSpace-Harvester/1.0 (+https://github.com/tocororo/alma)'
    }
    
    # Create harvester
    harvester = DSpaceHarvester(config)
    
    try:
        # Start harvest (will auto-resume if state exists)
        harvester.harvest_all(
            max_records=None,  # All records
            resume=True,       # Resume from previous state
            skip_existing=True # Skip already processed records
        )
        
    except KeyboardInterrupt:
        harvester.logger.info("Harvest interrupted by user")
        harvester.save_state()
        harvester.logger.info("State saved for resuming")
        
    except Exception as e:
        harvester.logger.error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()