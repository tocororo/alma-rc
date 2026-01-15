import os
import pandas as pd
import csv
import json


from typing import List


from dspace_bitstream import DSpaceHarvester
from invenio_record import InveniordmRecordSchemaV600
from invenio_mapping import process_dspace_row
from invenio_actions import create_or_update_record, search_record_exact


def load_invenio_from_dspace_csv(csv_path: str, output_dir: str = None, sample: int=100) -> List[tuple]:
    """
    Load and transform all records from a DSpace CSV export to Invenio records.
    
    Args:
        csv_path: Path to the DSpace CSV export file
        output_dir: Optional directory to save JSON records
    
    Returns:
        List of tuples (record_id, invenio_record)
    """
    # Read the CSV file
    df = pd.read_csv(csv_path, sep=',', quoting=csv.QUOTE_ALL, dtype=str)
    
    harvester = DSpaceHarvester(timeout=60, delay=2.0)

    records = []
    i = 0
    for idx, row in df.iterrows() :
        i+=1
        if i < sample:
            try:
                rc_record_id, handle, invenio_record = process_dspace_row(row)
                if invenio_record is not None:

                    bitstream_results = harvester.process_record_with_bitstreams(rc_record_id, handle, output_dir)
                    
                    record_id = None
                    record_id = search_record_exact('metadata.identifiers.identifier', handle)

                    alma_record_id = create_or_update_record(record=json.loads(invenio_record.json()), record_id=record_id, bitstreams=bitstream_results.values())
                    # alma_record_id = create_or_update_record(record=json.loads(invenio_record.json()), record_id=record_id)
                    if alma_record_id: 
                        records.append((handle, alma_record_id))

                    # Save to JSON if output directory is specified
                    if output_dir:
                        df_output = pd.DataFrame(records, columns=['rc_handle', 'alma_record_id'])
                        output_path = os.path.join(output_dir, 'records_output.csv')
                        df_output.to_csv(output_path, index=False)

                        print(f"Saved {len(records)} records to 'records_output.csv'")
                            
            except Exception as e:
                print(f"Error processing row {idx}: {e}")
                continue
    
    return records

def save_invenio_record_to_json(record: InveniordmRecordSchemaV600, output_path: str):
    """
    Properly serialize and save a Pydantic BaseModel to JSON file.
    Uses model_dump_json() for correct serialization.
    """
    try:
        # Convert Pydantic model to JSON string
        json_str = record.json(
            indent=2,           # Pretty formatting
            exclude_none=True,  # Exclude None values
            by_alias=True       # Use field aliases (like $schema)
        )
        
        # Save to JSON file with proper encoding
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(json_str)
            
        print(f"Successfully saved record to: {output_path}")
        return True
        
    except Exception as e:
        print(f"Error saving record to JSON: {e}")
        print(f"Record type: {type(record)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """
    Example usage of the script.
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Transform DSpace CSV export to Invenio records')
    parser.add_argument('csv_file', help='Path to DSpace CSV export file')
    parser.add_argument('--output-dir', help='Directory to save JSON records', default='invenio_records')
    parser.add_argument('--sample', type=int, help='Process only first N records', default=None)
    
    args = parser.parse_args()
    
    print(f"Processing DSpace CSV file: {args.csv_file}")
    
    records = load_invenio_from_dspace_csv(args.csv_file, args.output_dir, args.sample)
    
    if args.sample:
        records = records[:args.sample]
    
    print(f"Successfully processed {len(records)} records")
    print(f"Records saved to: {args.output_dir}")

if __name__ == "__main__":
    main()