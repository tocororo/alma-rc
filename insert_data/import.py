"""Main import script for transforming DSpace CSV to Invenio records."""
from datetime import datetime
import traceback
from columns import *

import csv
import json
import logging
import os
from typing import List, Optional, Tuple
import pandas as pd

from dspace_bitstream import DSpaceHarvester
from invenio_mapping import process_dspace_row
from invenio_actions import create_or_update_record, search_record_exact

logger = logging.getLogger(__name__)

def load_invenio_from_dspace_csv(csv_path: str, output_dir: Optional[str] = None, 
                                sample: int = 100) -> List[Tuple[str, str]]:
    """Load and transform DSpace CSV export to Invenio records.
    
    Args:
        csv_path: Path to DSpace CSV export file
        output_dir: Optional directory to save JSON records
        sample: Number of records to process (for testing)
        
    Returns:
        List of tuples (rc_handle, alma_record_id)
    """
    try:
        df = pd.read_csv(csv_path, sep=',', quoting=csv.QUOTE_ALL, dtype=str)
        logger.info(f"Loaded CSV with {len(df)} records")
    except Exception as e:
        logger.error(f"Failed to load CSV file {csv_path}: {e}")
        return []
    
    # Ensure output_dir exists for error logging
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    
    harvester = DSpaceHarvester(timeout=60, delay=2.0)
    records = []
    errors = []
    
    for idx, row in df.iterrows():
        if idx >= sample:
            break
            
        try:
            rc_record_id, handle, invenio_record = process_dspace_row(row)
            
            # Save JSON record if output_dir provided and record created
            if output_dir and invenio_record is not None:
                save_invenio_record_to_json(invenio_record, os.path.join(output_dir, rc_record_id))
            
            logger.debug(f"--------------------------------------------- {handle}")
            logger.debug(f"--------------------------------------------- {invenio_record}")
            
            if invenio_record is None:
                error_msg = f"Skipping row {idx}: Could not create Invenio record"
                logger.warning(error_msg)
                errors.append({
                    'row_index': idx,
                    'row_data': row.to_dict(),
                    'error_type': 'Invenio record creation failed',
                    'error_message': error_msg,
                    'handle': handle if 'handle' in locals() else None
                })
                continue
            bitstream_results = None
            # bitstream_results = harvester.process_record_with_bitstreams(
            #     rc_record_id, handle, output_dir
            # )
            logger.debug(f"--------------------------------------------- {bitstream_results}")
            
            record_id = search_record_exact('metadata.identifiers.identifier', handle)
            logger.debug(f"--------------------------------------------- {record_id}")

            alma_record_id = create_or_update_record(
                record=json.loads(invenio_record.json()),
                record_id=record_id,
                bitstreams=list(bitstream_results.values()) if bitstream_results else None
            )
            logger.debug(f"--------------------------------------------- {alma_record_id}")

            if alma_record_id:
                records.append((handle, alma_record_id))
                _save_progress(records, output_dir)
            else:
                # Record creation failed in create_or_update_record
                error_msg = f"Failed to create/update record in Invenio for row {idx}"
                logger.warning(error_msg)
                errors.append({
                    'row_index': idx,
                    'row_data': row.to_dict(),
                    'error_type': 'Invenio API failure',
                    'error_message': error_msg,
                    'handle': handle
                })
                
        except Exception as e:
            error_msg = f"Error processing row {idx}: {e}"
            logger.error(error_msg)
            # Save error details for this row
            errors.append({
                'row_index': idx,
                'row_data': row.to_dict(),
                'error_type': type(e).__name__,
                'error_message': str(e),
                'traceback': traceback.format_exc(),  # Full stack trace
                'handle': handle if 'handle' in locals() else None
            })
            continue
    
    # Save errors to CSV if any occurred
    if errors and output_dir:
        try:
            error_file_path = os.path.join(output_dir, f"errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
            
            # Flatten the row_data dictionary for better CSV structure
            error_rows = []
            for error in errors:
                flat_error = {
                    'row_index': error['row_index'],
                    'row_data': error['row_data'],
                    'error_type': error['error_type'],
                    'error_message': error['error_message'],
                    'traceback': error['traceback'] if 'traceback' in error else '',
                    'handle': error['handle']
                }
                
                # Add all row data columns
                for key, value in error['row_data'].items():
                    # Ensure column names don't have invalid characters
                    clean_key = str(key).replace('\n', ' ').replace('\r', ' ')
                    flat_error[f'data_{clean_key}'] = value
                
                error_rows.append(flat_error)
            
            # Create DataFrame and save to CSV
            error_df = pd.DataFrame(error_rows)
            error_df.to_csv(error_file_path, index=False, encoding='utf-8')
            logger.info(f"Saved {len(errors)} error rows to: {error_file_path}")
            
        except Exception as e:
            logger.error(f"Failed to save error CSV: {e}")
    
    # Also save a summary of processed vs error counts
    logger.info(f"Successfully processed {len(records)} records, encountered {len(errors)} errors")
    
    return records

def _save_progress(records: List[Tuple[str, str]], output_dir: Optional[str]) -> None:
    """Save processing progress to CSV file."""
    if output_dir and records:
        df_output = pd.DataFrame(records, columns=['rc_handle', 'alma_record_id'])
        output_path = os.path.join(output_dir, 'records_output.csv')
        df_output.to_csv(output_path, index=False)
        logger.debug(f"Saved progress to {output_path}")

        
def save_invenio_record_to_json(record, output_path: str) -> bool:
    """Properly serialize and save a Pydantic BaseModel to JSON file.
    
    Args:
        record: InveniordmRecordSchemaV600 instance
        output_path: Path to save JSON file
        
    Returns:
        True if successful, False otherwise
    """
    try:
        json_str = record.json(
            indent=2,
            exclude_none=True,
            by_alias=True
        )
        
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(json_str)
            
        logger.info(f"Successfully saved record to: {output_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error saving record to JSON {output_path}: {e}")
        return False


def setup_logging(log_level: str = "INFO") -> None:
    """Configure logging for the application."""
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('import.log')
        ]
    )


def main() -> None:
    """Main entry point for the script."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Transform DSpace CSV export to Invenio records'
    )
    parser.add_argument('csv_file', help='Path to DSpace CSV export file')
    parser.add_argument('--output-dir', help='Directory to save JSON records', 
                       default='invenio_records')
    parser.add_argument('--sample', type=int, help='Process only first N records', 
                       default=100)
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       default='DEBUG', help='Set logging level')
    
    args = parser.parse_args()
    setup_logging(args.log_level)
    
    logger.info(f"Processing DSpace CSV file: {args.csv_file}")
    
    records = load_invenio_from_dspace_csv(
        args.csv_file, 
        args.output_dir, 
        args.sample
    )
    
    logger.info(f"Successfully processed {len(records)} records")
    logger.info(f"Records saved to: {args.output_dir}")


if __name__ == "__main__":
    main()