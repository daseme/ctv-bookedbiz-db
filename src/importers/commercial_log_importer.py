#!/usr/bin/env python3
"""
Commercial Log Copy Script - Enhanced with WorldLink Lines Support
Maintains original functionality while adding multi-sheet processing.
Following Clean Architecture principles with proper separation of concerns.
"""

import pandas as pd
import os
from datetime import datetime
import logging
import sys
from pathlib import Path
from typing import Tuple, Optional, List, Dict
from dataclasses import dataclass


# ============================================================================
# Domain Models
# ============================================================================

@dataclass
class SheetProcessingResult:
    sheet_name: str
    rows_read: int
    columns_read: int
    success: bool
    error_message: Optional[str] = None
    
    @property
    def summary(self) -> str:
        if self.success:
            return f"{self.sheet_name}: {self.rows_read:,} rows, {self.columns_read} columns"
        else:
            return f"{self.sheet_name}: FAILED - {self.error_message}"


@dataclass 
class MultiSheetImportResult:
    total_rows: int
    sheets_processed: List[SheetProcessingResult]
    destination_file: str
    success: bool
    error_message: Optional[str] = None
    
    @property
    def summary(self) -> str:
        if self.success:
            sheet_summaries = [result.summary for result in self.sheets_processed]
            return f"Combined {self.total_rows:,} total rows from {len(self.sheets_processed)} sheets: {'; '.join(sheet_summaries)}"
        else:
            return f"Import failed: {self.error_message}"


# ============================================================================
# Value Objects and Business Rules
# ============================================================================

class CommercialLogConfig:
    """Configuration for commercial log processing"""
    
    # Required sheets to process (in order)
    REQUIRED_SHEETS = ["Commercials", "Worldlink Lines", "Add to booked business"]
    
    # Source and destination paths
    SOURCE_FILE = "/mnt/k-drive/Traffic/Media Library/Commercial Log.xlsx"
    DESTINATION_DIR = "/opt/apps/ctv-bookedbiz-db/data/raw/daily"
    
    @staticmethod
    def generate_filename() -> str:
        """Generate standardized filename with date suffix"""
        today = datetime.now()
        year_suffix = str(today.year)[-2:]  # Get last 2 digits of year
        month = f"{today.month:02d}"
        day = f"{today.day:02d}"
        
        return f"Commercial Log {year_suffix}{month}{day}.xlsx"


# ============================================================================
# Data Access Layer
# ============================================================================

class ExcelSheetReader:
    """Repository for reading Excel sheets with proper resource management"""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
    
    def read_multiple_sheets(self, source_path: str, sheet_names: List[str]) -> Dict[str, SheetProcessingResult]:
        """Read multiple sheets from Excel file with proper error handling"""
        results = {}
        
        try:
            self.logger.info(f"Reading source file: {source_path}")
            
            # Use context manager for proper resource cleanup
            with pd.ExcelFile(source_path) as xl_file:
                available_sheets = xl_file.sheet_names
                self.logger.info(f"Available sheets: {available_sheets}")
                
                # Process each required sheet
                for sheet_name in sheet_names:
                    results[sheet_name] = self._read_single_sheet(xl_file, sheet_name, available_sheets)
                
            return results
            
        except Exception as e:
            self.logger.error(f"Failed to open Excel file: {e}")
            # Return failed results for all requested sheets
            return {
                sheet_name: SheetProcessingResult(
                    sheet_name=sheet_name,
                    rows_read=0,
                    columns_read=0,
                    success=False,
                    error_message=str(e)
                )
                for sheet_name in sheet_names
            }
    
    def _read_single_sheet(self, xl_file, sheet_name: str, available_sheets: List[str]) -> SheetProcessingResult:
        """Read a single sheet with error handling"""
        try:
            if sheet_name not in available_sheets:
                return SheetProcessingResult(
                    sheet_name=sheet_name,
                    rows_read=0,
                    columns_read=0,
                    success=False,
                    error_message=f"Sheet '{sheet_name}' not found in workbook"
                )
            
            df = pd.read_excel(xl_file, sheet_name=sheet_name)
            
            # Log sample data if available
            if len(df) > 0:
                self.logger.info(f"Successfully read '{sheet_name}' sheet: {len(df)} rows, {len(df.columns)} columns")
                
                # Log sample data for verification
                sample_cols = self._get_sample_columns(df)
                if len(df) >= 3:
                    self.logger.info(f"Sample data from {sheet_name}:")
                    self.logger.info(f"\n{df[sample_cols].head(3)}")
            else:
                self.logger.warning(f"Sheet '{sheet_name}' is empty")
            
            return SheetProcessingResult(
                sheet_name=sheet_name,
                rows_read=len(df),
                columns_read=len(df.columns),
                success=True
            )
            
        except Exception as e:
            error_msg = f"Failed to read sheet '{sheet_name}': {e}"
            self.logger.error(error_msg)
            return SheetProcessingResult(
                sheet_name=sheet_name,
                rows_read=0,
                columns_read=0,
                success=False,
                error_message=error_msg
            )
    
    def _get_sample_columns(self, df: pd.DataFrame) -> List[str]:
        """Get sample columns for logging, prefer key business columns"""
        priority_columns = ['Bill Code', 'Start Date', 'End Date', 'Customer', 'Market']
        available_priority = [col for col in priority_columns if col in df.columns]
        
        if available_priority:
            return available_priority[:3]
        else:
            return df.columns[:3].tolist()


class ExcelDataCombiner:
    """Service for combining multiple sheets into a single dataset"""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
    
    def combine_sheets(self, sheet_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Combine multiple sheets with sheet source tracking"""
        combined_dataframes = []
        
        for sheet_name, df in sheet_data.items():
            if len(df) > 0:
                # Add sheet source column for tracking
                df_copy = df.copy()
                df_copy['sheet_source'] = sheet_name
                combined_dataframes.append(df_copy)
                
                self.logger.info(f"Added {len(df):,} rows from '{sheet_name}' sheet")
        
        if not combined_dataframes:
            self.logger.warning("No data found in any sheets")
            return pd.DataFrame()
        
        # Combine all sheets
        combined_df = pd.concat(combined_dataframes, ignore_index=True)
        
        self.logger.info(f"Combined dataset: {len(combined_df):,} total rows from {len(combined_dataframes)} sheets")
        return combined_df


class ExcelFileWriter:
    """Service for writing Excel files with proper error handling"""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
    
    def write_combined_data(self, df: pd.DataFrame, destination_path: str) -> bool:
        """Write combined DataFrame to Excel file"""
        try:
            self.logger.info(f"Writing to destination: {destination_path}")
            
            with pd.ExcelWriter(destination_path, engine='openpyxl') as writer:
                # Write to 'Commercials' sheet for backward compatibility
                # But now contains data from both original sheets
                df.to_excel(writer, sheet_name="Commercials", index=False)
            
            # Verify file was created
            if os.path.exists(destination_path):
                file_size = os.path.getsize(destination_path)
                filename = Path(destination_path).name
                self.logger.info(f"Successfully created: {filename}")
                self.logger.info(f"File size: {file_size:,} bytes")
                self.logger.info(f"Data rows written: {len(df):,}")
                
                # Log sheet source breakdown
                if 'sheet_source' in df.columns:
                    source_counts = df['sheet_source'].value_counts()
                    self.logger.info(f"Sheet breakdown: {dict(source_counts)}")
                
                return True
            else:
                self.logger.error("File creation failed - destination file not found")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to write Excel file: {e}")
            return False


# ============================================================================
# Business Logic Layer
# ============================================================================

class MultiSheetCommercialLogService:
    """Service for processing multiple sheets from Commercial Log Excel file"""
    
    def __init__(self, 
                 sheet_reader: ExcelSheetReader,
                 data_combiner: ExcelDataCombiner, 
                 file_writer: ExcelFileWriter,
                 logger: logging.Logger):
        self.sheet_reader = sheet_reader
        self.data_combiner = data_combiner
        self.file_writer = file_writer
        self.logger = logger
    
    def process_commercial_log_file(self, source_path: str, destination_path: str) -> MultiSheetImportResult:
        """Process Commercial Log file with multiple sheets"""
        
        # Step 1: Read all required sheets
        sheet_results = self.sheet_reader.read_multiple_sheets(source_path, CommercialLogConfig.REQUIRED_SHEETS)
        
        # Check if any sheets failed
        failed_sheets = [result for result in sheet_results.values() if not result.success]
        if failed_sheets:
            error_msg = f"Failed to read required sheets: {[s.sheet_name for s in failed_sheets]}"
            return MultiSheetImportResult(
                total_rows=0,
                sheets_processed=list(sheet_results.values()),
                destination_file=destination_path,
                success=False,
                error_message=error_msg
            )
        
        # Step 2: Extract successful DataFrames
        sheet_dataframes = {}
        for sheet_name, result in sheet_results.items():
            if result.success:
                # Re-read the successful sheets (we only got metadata in step 1)
                with pd.ExcelFile(source_path) as xl_file:
                    sheet_dataframes[sheet_name] = pd.read_excel(xl_file, sheet_name=sheet_name)
        
        # Step 3: Combine sheets
        combined_df = self.data_combiner.combine_sheets(sheet_dataframes)
        
        if len(combined_df) == 0:
            return MultiSheetImportResult(
                total_rows=0,
                sheets_processed=list(sheet_results.values()),
                destination_file=destination_path,
                success=False,
                error_message="No data found in any sheets"
            )
        
        # Step 4: Write combined file
        write_success = self.file_writer.write_combined_data(combined_df, destination_path)
        
        return MultiSheetImportResult(
            total_rows=len(combined_df),
            sheets_processed=list(sheet_results.values()),
            destination_file=destination_path,
            success=write_success,
            error_message=None if write_success else "Failed to write destination file"
        )


# ============================================================================
# Infrastructure Layer
# ============================================================================

class FileSystemService:
    """Service for file system operations"""
    
    @staticmethod
    def validate_source_file(source_path: str, logger: logging.Logger) -> bool:
        """Validate source file exists and is accessible"""
        if not os.path.exists(source_path):
            logger.error(f"Source file not found: {source_path}")
            return False
        return True
    
    @staticmethod
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


class LoggingService:
    """Service for logging configuration with proper separation"""
    
    @staticmethod
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


# ============================================================================
# Application Layer (Facade)
# ============================================================================

class CommercialLogImportFacade:
    """Main facade for commercial log import operations"""
    
    def __init__(self):
        self.logger = LoggingService.setup_logging()
        
        # Initialize services with dependency injection
        self.sheet_reader = ExcelSheetReader(self.logger)
        self.data_combiner = ExcelDataCombiner(self.logger)
        self.file_writer = ExcelFileWriter(self.logger)
        self.commercial_log_service = MultiSheetCommercialLogService(
            self.sheet_reader,
            self.data_combiner, 
            self.file_writer,
            self.logger
        )
    
    def copy_commercial_log(self) -> bool:
        """
        Copy and combine Commercials and WorldLink Lines sheets from K: drive to local Pi project folder
        
        Returns:
            bool: True if successful, False otherwise
        """
        self.logger.info("Starting Multi-Sheet Commercial Log copy process - PRODUCTION MODE")
        self.logger.info("Source: K-Drive:/Traffic/Media Library/Commercial Log.xlsx")
        self.logger.info("Destination: /opt/apps/ctv-bookedbiz-db/data/raw/daily/")
        self.logger.info(f"Processing sheets: {CommercialLogConfig.REQUIRED_SHEETS}")
        
        # Generate destination path
        filename = CommercialLogConfig.generate_filename()
        destination_path = os.path.join(CommercialLogConfig.DESTINATION_DIR, filename)
        
        # Log if file will be overwritten
        if os.path.exists(destination_path):
            self.logger.warning(f"Destination file already exists: {filename}")
            self.logger.info("Proceeding with overwrite...")
        
        # Validate inputs
        if not FileSystemService.validate_source_file(CommercialLogConfig.SOURCE_FILE, self.logger):
            return False
        
        if not FileSystemService.ensure_destination_directory(CommercialLogConfig.DESTINATION_DIR, self.logger):
            return False
        
        # Process multi-sheet file
        result = self.commercial_log_service.process_commercial_log_file(
            CommercialLogConfig.SOURCE_FILE,
            destination_path
        )
        
        # Log detailed results
        if result.success:
            self.logger.info("Multi-sheet import completed successfully")
            self.logger.info(result.summary)
            
            # Log sheet-by-sheet breakdown
            for sheet_result in result.sheets_processed:
                self.logger.info(f"  └─ {sheet_result.summary}")
            
        else:
            self.logger.error("Multi-sheet import failed")
            self.logger.error(result.error_message)
            
            # Log individual sheet failures
            for sheet_result in result.sheets_processed:
                if not sheet_result.success:
                    self.logger.error(f"  └─ {sheet_result.summary}")
        
        return result.success


# ============================================================================
# Legacy Functions (Maintained for Backward Compatibility)
# ============================================================================

def setup_logging() -> logging.Logger:
    """Legacy function - maintained for compatibility"""
    return LoggingService.setup_logging()


def generate_filename() -> str:
    """Legacy function - maintained for compatibility"""
    return CommercialLogConfig.generate_filename()


def validate_source_file(source_path: str, logger: logging.Logger) -> bool:
    """Legacy function - maintained for compatibility"""
    return FileSystemService.validate_source_file(source_path, logger)


def ensure_destination_directory(dest_dir: str, logger: logging.Logger) -> bool:
    """Legacy function - maintained for compatibility"""
    return FileSystemService.ensure_destination_directory(dest_dir, logger)


def read_commercials_sheet(source_path: str, logger: logging.Logger) -> Optional[pd.DataFrame]:
    """
    Legacy function - now reads and combines both sheets
    Maintained for backward compatibility with existing scripts
    """
    facade = CommercialLogImportFacade()
    
    # Read both sheets
    sheet_results = facade.sheet_reader.read_multiple_sheets(source_path, CommercialLogConfig.REQUIRED_SHEETS)
    
    # Check if both sheets were read successfully
    failed_sheets = [result for result in sheet_results.values() if not result.success]
    if failed_sheets:
        logger.error(f"Failed to read required sheets: {[s.sheet_name for s in failed_sheets]}")
        return None
    
    # Re-read and combine sheets
    try:
        with pd.ExcelFile(source_path) as xl_file:
            sheet_dataframes = {}
            for sheet_name in CommercialLogConfig.REQUIRED_SHEETS:
                sheet_dataframes[sheet_name] = pd.read_excel(xl_file, sheet_name=sheet_name)
        
        combined_df = facade.data_combiner.combine_sheets(sheet_dataframes)
        logger.info(f"Combined legacy read: {len(combined_df)} total rows")
        return combined_df
        
    except Exception as e:
        logger.error(f"Failed to combine sheets: {e}")
        return None


def write_excel_file(df: pd.DataFrame, destination_path: str, logger: logging.Logger) -> bool:
    """Legacy function - maintained for compatibility"""
    writer = ExcelFileWriter(logger)
    return writer.write_combined_data(df, destination_path)


def copy_commercial_log() -> bool:
    """
    Legacy function - now uses new multi-sheet architecture
    Maintained for backward compatibility with existing automation
    """
    facade = CommercialLogImportFacade()
    return facade.copy_commercial_log()


# ============================================================================
# Main Entry Point
# ============================================================================

def main() -> None:
    """Main function with proper error handling and new multi-sheet support"""
    facade = CommercialLogImportFacade()
    success = facade.copy_commercial_log()
    
    if success:
        facade.logger.info("Enhanced Commercial Log copy completed successfully")
        sys.exit(0)
    else:
        facade.logger.error("Enhanced Commercial Log copy failed")
        sys.exit(1)


if __name__ == "__main__":
    main()