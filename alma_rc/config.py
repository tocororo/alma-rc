# config.py
import os
import json
from typing import Any
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def _parse_json_env(var_name: str, default: Any = None) -> Any:
    """Parse JSON encoded environment variable."""
    value = os.getenv(var_name)
    if value:
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            print(f"Warning: Could not parse JSON for {var_name}")
            return default
    return default

class Config:
    """Application configuration from environment variables."""
    
    # API Configuration
    INVENIO_API_BASE_URL = os.getenv('INVENIO_API_BASE_URL', 'https://alma.upr.edu.cu')
    INVENIO_API_TOKEN = os.getenv('INVENIO_API_TOKEN')
    
    # Application Configuration
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    DATA_DIR = os.getenv('DATA_DIR', '/data')
    CSV_SAMPLE_SIZE = int(os.getenv('CSV_SAMPLE_SIZE', '100'))
    REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', '30'))
    REQUEST_DELAY = float(os.getenv('REQUEST_DELAY', '2.0'))
    
    # DSpace Configuration
    DEFAULT_PUBLISHER = os.getenv('DEFAULT_PUBLISHER', 'Universidad de Pinar del Río "Hermanos Saíz Montes de Oca"')
    DEFAULT_USER_AGENT = os.getenv('DEFAULT_USER_AGENT', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
    MAX_FILENAME_LENGTH = int(os.getenv('MAX_FILENAME_LENGTH', '200'))
    
    # Field Separators
    MULTI_VALUE_SEPARATOR = os.getenv('MULTI_VALUE_SEPARATOR', '||')
    LANGUAGE_SEPARATOR = os.getenv('LANGUAGE_SEPARATOR', ';')
    
    
    # DSpace CSV Columns (parsed from JSON)
    DSPACE_CSV_COLUMNS = _parse_json_env('DSPACE_CSV_COLUMNS', [])
    
    # Resource Type Mappings
    RESOURCE_TYPE_MAP = _parse_json_env('RESOURCE_TYPE_MAP', {})
    
    # Thesis Collection Mappings
    THESIS_COLLECTION_MAP = _parse_json_env('THESIS_COLLECTION_MAP', {})
    
    # External Program Collections
    EXTERNAL_PROGRAM_COLLECTIONS = _parse_json_env('EXTERNAL_PROGRAM_COLLECTIONS', {})
    
    # University Subject Mappings
    UPR_SUBJECT_MAP = _parse_json_env('UPR_SUBJECT_MAP', {})
    
    # University Entities
    UPR_ENTITIES = _parse_json_env('UPR_ENTITIES', {})
    
    # Language Variants
    LANGUAGE_VARIANTS = _parse_json_env('LANGUAGE_VARIANTS', {})
    
    # Date Type Mapping
    DATE_TYPE_MAP = _parse_json_env('DATE_TYPE_MAP', {})
    
    # Validate required environment variables
    @classmethod
    def validate(cls) -> None:
        """Validate required environment variables."""
        required_vars = ['INVENIO_API_TOKEN']
        missing = [var for var in required_vars if not getattr(cls, var)]
        
        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}\n"
                "Please set them in your .env file or environment."
            )
        
        # Validate that we have DSpace CSV columns
        if not cls.DSPACE_CSV_COLUMNS:
            print("Warning: DSPACE_CSV_COLUMNS not configured or empty")
    
    @classmethod
    def get_invenio_api_config(cls) -> dict:
        """Get Invenio API configuration dictionary."""
        return {
            'base_url': cls.INVENIO_API_BASE_URL,
            'token': cls.INVENIO_API_TOKEN
        }
    
    @classmethod
    def get_headers(cls) -> dict:
        """Get default HTTP headers for API requests."""
        return {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {cls.INVENIO_API_TOKEN}'
        }
    
    @classmethod
    def get_bitstream_config(cls) -> dict:
        """Get bitstream download configuration."""
        return {
            'timeout': cls.REQUEST_TIMEOUT,
            'delay': cls.REQUEST_DELAY
        }
    
    @classmethod
    def get_field_separators(cls) -> dict:
        """Get field separators dictionary."""
        return {
            'multi_value': cls.MULTI_VALUE_SEPARATOR,
            'language': cls.LANGUAGE_SEPARATOR
        }
    
    @classmethod
    def print_config_summary(cls) -> None:
        """Print configuration summary."""
        print("=" * 60)
        print("Configuration Summary")
        print("=" * 60)
        print(f"API Base URL: {cls.INVENIO_API_BASE_URL}")
        print(f"API Token: {'Set' if cls.INVENIO_API_TOKEN else 'Not Set'}")
        print(f"Log Level: {cls.LOG_LEVEL}")
        print(f"Default Publisher: {cls.DEFAULT_PUBLISHER}")
        print(f"DSpace CSV Columns: {len(cls.DSPACE_CSV_COLUMNS)} columns")
        print(f"Resource Type Mappings: {len(cls.RESOURCE_TYPE_MAP)} items")
        print(f"Thesis Collection Mappings: {len(cls.THESIS_COLLECTION_MAP)} items")
        print(f"University Entities: {len(cls.UPR_ENTITIES)} items")
        print("=" * 60)

# Validate configuration on import
try:
    Config.validate()
    Config.print_config_summary()
except ValueError as e:
    print(f"Configuration Error: {e}")
    # Don't raise here to allow for dynamic configuration later