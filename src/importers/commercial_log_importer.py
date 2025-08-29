#!/usr/bin/env python3
"""
Commercial Log Copy Script - Improved Version
Maintains original functionality while fixing key style violations
"""

import pandas as pd
import os
from datetime import datetime
import logging
import sys
from pathlib import Path
from typing import Tuple, Optional


def setup_logging() -> logging.Logger:
    """Configure logging with proper separation"""
    # Create logs directory if it doesn't exist
    log_dir = Path.home() / 'logs'
    log_dir.mkdir(exist_ok=True)
    
    # Set up logging
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    
    # Avoid duplicate handlers
    if not logger.handlers:
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        # File handler
        file_handler = logging.FileHandler(log_dir / 'commercial_log_copy.log')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        # Console handler  
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    return logger


def generate_filename() -> str:
    """Generate standardized filename with date suffix"""
    today = datetime.now()
    year_suffix = str(today.year)[-2:]  # Get last 2 digits of year
    month = f"{today.month:02d}"
    day = f"{today.day:02d}"
    
    return f"Commercial Log {year_suffix}{month}{day}.xlsx"


def validate_source_file(source_path: str, logger: logging.Logger) -> bool:
    """Validate source file exists and is accessible"""
    if not os.path.exists(source_path):
        logger.error(f"Source file not found: {source_path}")
        return False
    return True


def ensure_destination_directory(dest_dir: str, logger: logging.Logger) -> bool:
    """Ensure destination directory exists"""
    if not os.path.exists(dest_dir):
        logger.info(f"Creating destination directory: {dest_dir}")
        try:
            os.makedirs(dest_dir, exist_ok=True)
            return True
        except OSError as e:
            logger.error(f"Failed to create directory: {e}")
            return False
    return True


def read_commercials_sheet(source_path: str, logger: logging.Logger) -> Optional[pd.DataFrame]:
    """Read the Commercials sheet from source Excel file"""
    try:
        logger.info(f"Reading source file: {source_path}")
        
        # Use context manager for proper resource cleanup
        with pd.ExcelFile(source_path) as xl_file:
            available_sheets = xl_file.sheet_names
            logger.info(f"Available sheets: {available_sheets}")
            
            if "Commercials" not in available_sheets:
                logger.error("'Commercials' sheet not found in source file")
                return None
            
            df = pd.read_excel(xl_file, sheet_name="Commercials")
            logger.info(f"Successfully read Commercials sheet with {len(df)} rows and {len(df.columns)} columns")
            
            # Log sample data if available
            if len(df) > 0:
                sample_cols = ['Bill Code', 'Start Date', 'End Date'] if 'Bill Code' in df.columns else df.columns[:3]
                logger.info("Sample data being processed:")
                logger.info(f"\n{df[sample_cols].head(3)}")
            
            return df
            
    except Exception as e:
        logger.error(f"Failed to read Excel file: {e}")
        return None


def write_excel_file(df: pd.DataFrame, destination_path: str, logger: logging.Logger) -> bool:
    """Write DataFrame to Excel file"""
    try:
        logger.info(f"Writing to destination: {destination_path}")
        
        with pd.ExcelWriter(destination_path, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name="Commercials", index=False)
        
        # Verify file was created
        if os.path.exists(destination_path):
            file_size = os.path.getsize(destination_path)
            filename = Path(destination_path).name
            logger.info(f"Successfully created: {filename}")
            logger.info(f"File size: {file_size:,} bytes")
            logger.info(f"Data rows processed: {len(df):,}")
            return True
        else:
            logger.error("File creation failed - destination file not found")
            return False
            
    except Exception as e:
        logger.error(f"Failed to write Excel file: {e}")
        return False


def copy_commercial_log() -> bool:
    """
    Copy the Commercials sheet from K: drive to local Pi project folder
    
    Returns:
        bool: True if successful, False otherwise
    """
    logger = setup_logging()
    
    # Configuration - local to raspberry pi project
    source_file = "/mnt/k-drive/Traffic/Media Library/Commercial Log.xlsx"
    destination_dir = "/opt/apps/ctv-bookedbiz-db/data/raw/daily"
    
    # Generate destination path
    filename = generate_filename()
    destination_path = os.path.join(destination_dir, filename)
    
    # Log if file will be overwritten
    if os.path.exists(destination_path):
        logger.warning(f"Destination file already exists: {filename}")
        logger.info("Proceeding with overwrite...")
    
    # Validate inputs
    if not validate_source_file(source_file, logger):
        return False
    
    if not ensure_destination_directory(destination_dir, logger):
        return False
    
    # Read source data
    df = read_commercials_sheet(source_file, logger)
    if df is None:
        return False
    
    # Write destination file
    return write_excel_file(df, destination_path, logger)


def main() -> None:
    """Main function with proper error handling"""
    logger = setup_logging()
    logger.info("Starting Commercial Log copy process - PRODUCTION MODE")
    logger.info("Source: K-Drive:/Traffic/Media Library/Commercial Log.xlsx")
    logger.info("Destination: /opt/apps/ctv-bookedbiz-db/data/raw/daily/")
    
    success = copy_commercial_log()
    
    if success:
        logger.info("Commercial Log copy completed successfully")
        sys.exit(0)
    else:
        logger.error("Commercial Log copy failed")
        sys.exit(1)


if __name__ == "__main__":
    main()