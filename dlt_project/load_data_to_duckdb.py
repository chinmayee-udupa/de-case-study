import os
import pathlib
import dlt
from dlt.sources.filesystem import filesystem, read_csv
from datetime import date
import re
import argparse
import sys

def load_data(load_type="full", load_date=None, base_input_path="../input_files"):
    """
    Load CSV files from specified directory with configurable parameters
    
    Args:
        load_type (str): "full" for full load, "incremental" for incremental load
        load_date: Specific date for load_date column (default: today)
        base_input_path: Base path for input files
    """
    # Set default load_date to today if not provided
    if load_date is None:
        load_date = date.today()
    
    # Determine input directory based on load type
    if load_type.lower() == "incremental":
        INPUT_DIR = pathlib.Path(f"{base_input_path}/incremental_load").absolute()
        write_disposition = "append"
    else:
        INPUT_DIR = pathlib.Path(f"{base_input_path}/full_load").absolute()
        write_disposition = "replace"
    
    DB_PATH = os.path.abspath("../dbt_project/reports/sources/final/dev.duckdb")

    print(f"Load type: {load_type}")
    print(f"Input directory: {INPUT_DIR}")
    print(f"Database path: {DB_PATH}")
    print(f"Load date: {load_date}")
    print(f"Write disposition: {write_disposition}")

    # Check if input directory exists
    if not INPUT_DIR.exists():
        raise ValueError(f"Input directory does not exist: {INPUT_DIR}")

    # Check if CSV files exist
    csv_files = list(INPUT_DIR.glob("*.csv"))
    print(f"Found {len(csv_files)} CSV files:")
    for file in csv_files:
        print(f"  - {file.name}")

    if not csv_files:
        print("No CSV files found to process!")
        return

    # Configure destination
    dlt.config["DESTINATION__DUCKDB__CREDENTIALS"] = DB_PATH

    # Create pipeline
    pipeline = dlt.pipeline(
        pipeline_name="de_case_study",
        dataset_name="raw",
        destination=dlt.destinations.duckdb(credentials=DB_PATH),
    )

    # Process each CSV file as a separate resource/table
    for file in csv_files:
        # Extract table name using regex to handle various filename patterns
        filename = file.stem
        table_name = extract_table_name(filename)
        print(f"Processing: {filename} -> Table: {table_name}")
        
        # Create filesystem source for this specific file
        source = filesystem(
            bucket_url=str(INPUT_DIR),
            file_glob=file.name
        ).with_name(table_name)
        
        # Read CSV and add load date
        source = source | read_csv().add_map(lambda row: {**row, "load_date": load_date})
        
        # Run pipeline for this resource
        info = pipeline.run(
            source, 
            table_name=table_name, 
            write_disposition=write_disposition,
            columns={
                "load_date": {
                    "data_type": "date",
                    "nullable": False
                }
            }
        )
        print(f"Loaded {table_name}: {info}")

def extract_table_name(filename):
    """
    Extract clean table name from various filename patterns
    Handles: DE_casestudy_charges_1.csv, DE_casestudy_charges.csv, charges_1.csv, etc.
    """
    # Remove common prefixes and suffixes
    patterns_to_remove = [
        r'^DE_',
        r'^DE_casestudy_',
        r'^casestudy_',
        r'_\d+$',  # Remove trailing numbers like _1, _2, etc.
    ]
    
    table_name = filename
    for pattern in patterns_to_remove:
        table_name = re.sub(pattern, '', table_name)
    
    # Ensure table name is valid (no special characters, etc.)
    table_name = re.sub(r'[^a-zA-Z0-9_]', '_', table_name)
    table_name = table_name.lower()
    
    return table_name

def parse_date(date_str):
    """Parse date string in YYYY-MM-DD format"""
    try:
        year, month, day = map(int, date_str.split('-'))
        return date(year, month, day)
    except ValueError:
        raise ValueError("Date must be in YYYY-MM-DD format")

def main():
    parser = argparse.ArgumentParser(description="Load CSV files to DuckDB with dlt")
    parser.add_argument("--load-type", "-t", choices=["full", "incremental"], 
                       default="full", help="Load type: full or incremental")
    parser.add_argument("--load-date", "-d", 
                       help="Load date in YYYY-MM-DD format (default: today)")
    parser.add_argument("--input-path", "-i", default="../input_files",
                       help="Base input path (default: ../input_files)")
    
    args = parser.parse_args()
    
    # Parse load date if provided
    load_date_obj = None
    if args.load_date:
        load_date_obj = parse_date(args.load_date)
    
    # Execute the load
    load_data(
        load_type=args.load_type,
        load_date=load_date_obj,
        base_input_path=args.input_path
    )

if __name__ == "__main__":
    main()
