#!/usr/bin/env python3
"""
Commercial Log Copy Script - Production Version
Copies the "Commercials" sheet from K: drive to Crossings TV Dropbox
"""

import pandas as pd
import os
from datetime import datetime
import logging
import sys
from pathlib import Path

# Create logs directory if it doesn't exist
log_dir = Path.home() / 'logs'
log_dir.mkdir(exist_ok=True)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / 'commercial_log_copy.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

def copy_commercial_log():
    """Copy the Commercials sheet to a new dated file"""
    
    # Production file paths
    source_file = "/mnt/k/Traffic/Media Library/Commercial Log.xlsx"
    destination_dir = "/mnt/c/Users/Kurt/Crossings TV Dropbox/kurt olmstead/Financial/Sales/WeeklyReports/ctv-bookedbiz-db/ctv-bookedbiz-db/data/raw/daily"
    
    # Generate filename with current date
    today = datetime.now()
    year_suffix = str(today.year)[-2:]  # Get last 2 digits of year
    month = f"{today.month:02d}"
    day = f"{today.day:02d}"
    
    destination_filename = f"Commercial Log {year_suffix}{month}{day}.xlsx"
    destination_path = os.path.join(destination_dir, destination_filename)
    
    try:
        # Check if source file exists
        if not os.path.exists(source_file):
            logging.error(f"Source file not found: {source_file}")
            return False
        
        # Check if destination directory exists, create if not
        if not os.path.exists(destination_dir):
            logging.info(f"Creating destination directory: {destination_dir}")
            os.makedirs(destination_dir, exist_ok=True)
        
        # Check if destination file already exists
        if os.path.exists(destination_path):
            logging.warning(f"Destination file already exists: {destination_filename}")
            logging.info("Proceeding with overwrite...")
        
        # Read the source Excel file
        logging.info(f"Reading source file: {source_file}")
        
        # Read all sheets to check which ones exist
        xl_file = pd.ExcelFile(source_file)
        available_sheets = xl_file.sheet_names
        logging.info(f"Available sheets: {available_sheets}")
        
        # Check if "Commercials" sheet exists
        if "Commercials" not in available_sheets:
            logging.error("'Commercials' sheet not found in source file")
            return False
        
        # Read the Commercials sheet
        df_commercials = pd.read_excel(source_file, sheet_name="Commercials")
        logging.info(f"Successfully read Commercials sheet with {len(df_commercials)} rows and {len(df_commercials.columns)} columns")
        
        # Show sample of data being processed (first 3 rows, key columns only)
        if len(df_commercials) > 0:
            sample_cols = ['Bill Code', 'Start Date', 'End Date'] if 'Bill Code' in df_commercials.columns else df_commercials.columns[:3]
            logging.info("Sample data being processed:")
            logging.info(f"\n{df_commercials[sample_cols].head(3)}")
        
        # Write to destination file
        logging.info(f"Writing to destination: {destination_path}")
        with pd.ExcelWriter(destination_path, engine='openpyxl') as writer:
            df_commercials.to_excel(writer, sheet_name="Commercials", index=False)
        
        # Verify the file was created successfully
        if os.path.exists(destination_path):
            file_size = os.path.getsize(destination_path)
            logging.info(f"Successfully created: {destination_filename}")
            logging.info(f"File size: {file_size:,} bytes")
            logging.info(f"Data rows processed: {len(df_commercials):,}")
            return True
        else:
            logging.error("File creation failed - destination file not found")
            return False
        
    except FileNotFoundError as e:
        logging.error(f"File not found: {e}")
        return False
    except PermissionError as e:
        logging.error(f"Permission denied: {e}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return False

def main():
    """Main function"""
    logging.info("Starting Commercial Log copy process - PRODUCTION MODE")
    logging.info("Source: K:/Traffic/Media Library/Commercial Log.xlsx")
    logging.info("Destination: Crossings TV Dropbox/...data/raw/daily/")
    
    success = copy_commercial_log()
    
    if success:
        logging.info("Commercial Log copy completed successfully")
        sys.exit(0)
    else:
        logging.error("Commercial Log copy failed")
        sys.exit(1)

if __name__ == "__main__":
    main()