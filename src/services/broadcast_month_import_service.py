#!/usr/bin/env python3
"""
Broadcast month import service for managing month-based data imports.
Orchestrates the complete import workflow with validation, deletion, and import.
"""

import sys
import logging
import uuid
from pathlib import Path
from datetime import datetime, date
from typing import List, Set, Optional, Dict, Any
from dataclasses import dataclass

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from database.connection import DatabaseConnection
from services.month_closure_service import MonthClosureService, ValidationResult
from services.import_integration_utilities import extract_display_months_from_excel, validate_excel_for_import
from utils.broadcast_month_utils import BroadcastMonthParser, extract_broadcast_months_from_excel

logger = logging.getLogger(__name__)


@dataclass
class ImportResult:
    """Comprehensive result of import operation."""
    success: bool
    batch_id: str
    import_mode: str
    broadcast_months_affected: List[str]
    records_deleted: int
    records_imported: int
    duration_seconds: float
    error_messages: List[str]
    closed_months: List[str]  # For historical mode
    
    def __post_init__(self):
        """Ensure lists are not None."""
        if self.error_messages is None:
            self.error_messages = []
        if self.closed_months is None:
            self.closed_months = []


class BroadcastMonthImportError(Exception):
    """Raised when there's an error with import operations."""
    pass


class BroadcastMonthImportService:
    """Service for managing month-based data imports with validation and safety."""
    
    def __init__(self, db_connection: DatabaseConnection):
        """
        Initialize the broadcast month import service.
        
        Args:
            db_connection: Database connection instance
        """
        self.db = db_connection
        self.closure_service = MonthClosureService(db_connection)
        self.parser = BroadcastMonthParser()
    
    def validate_import(self, excel_file: str, import_mode: str) -> ValidationResult:
        """
        Validate Excel file for import based on mode.
        
        Args:
            excel_file: Path to Excel file
            import_mode: 'HISTORICAL', 'WEEKLY_UPDATE', or 'MANUAL'
            
        Returns:
            ValidationResult with detailed validation info
            
        Raises:
            BroadcastMonthImportError: If validation cannot be performed
        """
        logger.info(f"Validating {excel_file} for {import_mode} import")
        
        try:
            # Extract broadcast months from Excel in display format
            display_months = list(extract_display_months_from_excel(excel_file))
            
            if not display_months:
                raise BroadcastMonthImportError("No broadcast months found in Excel file")
            
            logger.info(f"Found {len(display_months)} months in Excel: {sorted(display_months)}")
            
            # Validate against closed months
            return self.closure_service.validate_months_for_import(display_months, import_mode)
            
        except Exception as e:
            error_msg = f"Failed to validate import: {str(e)}"
            logger.error(error_msg)
            raise BroadcastMonthImportError(error_msg)
    
    def execute_month_replacement(self, 
                                excel_file: str, 
                                import_mode: str, 
                                closed_by: str = None,
                                dry_run: bool = False) -> ImportResult:
        """
        Execute complete month replacement workflow.
        
        Args:
            excel_file: Path to Excel file
            import_mode: 'HISTORICAL', 'WEEKLY_UPDATE', or 'MANUAL'
            closed_by: Required for HISTORICAL mode (who is closing the months)
            dry_run: If True, validate and preview without making changes
            
        Returns:
            ImportResult with comprehensive results
            
        Raises:
            BroadcastMonthImportError: If import fails
        """
        start_time = datetime.now()
        batch_id = f"{import_mode.lower()}_{int(start_time.timestamp())}"
        
        logger.info(f"Starting {import_mode} import from {excel_file} (batch: {batch_id})")
        
        result = ImportResult(
            success=False,
            batch_id=batch_id,
            import_mode=import_mode,
            broadcast_months_affected=[],
            records_deleted=0,
            records_imported=0,
            duration_seconds=0.0,
            error_messages=[],
            closed_months=[]
        )
        
        try:
            # Step 1: Validate import
            validation = self.validate_import(excel_file, import_mode)
            
            if not validation.is_valid:
                result.error_messages.append(validation.error_message)
                logger.error(f"Import validation failed: {validation.error_message}")
                return result
            
            result.broadcast_months_affected = validation.open_months_found + validation.closed_months_found
            
            logger.info(f"Validation passed for {len(result.broadcast_months_affected)} months")
            
            if dry_run:
                logger.info("DRY RUN - No changes will be made")
                result.success = True
                return result
            
            # Step 2: Validate HISTORICAL mode requirements
            if import_mode == 'HISTORICAL':
                if not closed_by:
                    raise BroadcastMonthImportError("HISTORICAL mode requires --closed-by parameter")
            
            # Step 3: Create import batch record
            batch_record_id = self._create_import_batch(batch_id, import_mode, excel_file, result.broadcast_months_affected)
            
            # Step 4: Execute the import in transaction
            with self.db.transaction() as conn:
                # Delete existing data for affected months
                deleted_count = self._delete_broadcast_month_data(result.broadcast_months_affected, conn)
                result.records_deleted = deleted_count
                
                logger.info(f"Deleted {deleted_count} existing records for {len(result.broadcast_months_affected)} months")
                
                # Import new data from Excel
                imported_count = self._import_excel_data(excel_file, batch_id, conn)
                result.records_imported = imported_count
                
                logger.info(f"Imported {imported_count} new records")
                
                # Step 5: Handle HISTORICAL mode - close all months
                if import_mode == 'HISTORICAL':
                    for month in result.broadcast_months_affected:
                        if not self.closure_service.is_month_closed(month):
                            self.closure_service.close_broadcast_month(month, closed_by, f"Auto-closed by historical import {batch_id}")
                            result.closed_months.append(month)
                            logger.info(f"Closed month {month} as part of historical import")
                
                # Step 6: Update batch record as completed
                self._complete_import_batch(batch_record_id, result, conn)
            
            # Calculate final statistics
            end_time = datetime.now()
            result.duration_seconds = (end_time - start_time).total_seconds()
            result.success = True
            
            logger.info(f"Import completed successfully in {result.duration_seconds:.2f} seconds")
            
            return result
            
        except Exception as e:
            error_msg = f"Import failed: {str(e)}"
            result.error_messages.append(error_msg)
            logger.error(error_msg, exc_info=True)
            
            # Mark batch as failed if it was created
            try:
                self._fail_import_batch(batch_id, error_msg)
            except:
                pass  # Don't fail on cleanup failure
            
            return result
    
    def _create_import_batch(self, batch_id: str, import_mode: str, source_file: str, months: List[str]) -> str:
        """Create import batch record for audit trail."""
        try:
            with self.db.transaction() as conn:
                conn.execute("""
                    INSERT INTO import_batches (
                        batch_id, import_mode, source_file, broadcast_months_affected,
                        status, started_by
                    ) VALUES (?, ?, ?, ?, 'RUNNING', ?)
                """, (batch_id, import_mode, source_file, str(months), 'system'))
                
                logger.info(f"Created import batch record: {batch_id}")
                return batch_id
                
        except Exception as e:
            logger.error(f"Failed to create import batch record: {e}")
            raise BroadcastMonthImportError(f"Failed to create import batch: {e}")
    
    def _delete_broadcast_month_data(self, display_months: List[str], conn) -> int:
        """
        Delete existing data for broadcast months.
        Works with datetime format in database.
        """
        total_deleted = 0
        
        for display_month in display_months:
            # Get all datetime values for this display month
            datetime_values = self.closure_service._get_datetime_values_for_month(display_month)
            
            if datetime_values:
                # Delete spots for all datetime values in this month
                placeholders = ', '.join(['?' for _ in datetime_values])
                cursor = conn.execute(
                    f"DELETE FROM spots WHERE broadcast_month IN ({placeholders}) AND is_historical = 0",
                    datetime_values
                )
                
                deleted_count = cursor.rowcount
                total_deleted += deleted_count
                
                logger.debug(f"Deleted {deleted_count} spots for {display_month} ({len(datetime_values)} datetime values)")
        
        return total_deleted
    
    def _import_excel_data(self, excel_file: str, batch_id: str, conn) -> int:
        """
        Import Excel data using enhanced production importer.
        Integrates with the existing production_importer.py functionality.
        """
        try:
            # Import using the enhanced production importer with batch tracking
            from importers.enhanced_production_importer import import_excel_with_batch
            
            imported_count = import_excel_with_batch(
                excel_file_path=excel_file,
                database_path=self.db.db_path,
                batch_id=batch_id
            )
            
            logger.info(f"Successfully imported {imported_count} records with batch ID {batch_id}")
            return imported_count
            
        except Exception as e:
            error_msg = f"Excel import failed: {str(e)}"
            logger.error(error_msg)
            raise BroadcastMonthImportError(error_msg)
    
    def _complete_import_batch(self, batch_id: str, result: ImportResult, conn):
        """Mark import batch as completed with statistics."""
        try:
            conn.execute("""
                UPDATE import_batches 
                SET status = 'COMPLETED',
                    completed_at = CURRENT_TIMESTAMP,
                    records_imported = ?,
                    records_deleted = ?
                WHERE batch_id = ?
            """, (result.records_imported, result.records_deleted, batch_id))
            
            logger.debug(f"Marked batch {batch_id} as completed")
            
        except Exception as e:
            logger.error(f"Failed to update batch completion: {e}")
    
    def _fail_import_batch(self, batch_id: str, error_message: str):
        """Mark import batch as failed."""
        try:
            with self.db.transaction() as conn:
                conn.execute("""
                    UPDATE import_batches 
                    SET status = 'FAILED',
                        completed_at = CURRENT_TIMESTAMP,
                        error_summary = ?
                    WHERE batch_id = ?
                """, (error_message, batch_id))
                
                logger.info(f"Marked batch {batch_id} as failed")
                
        except Exception as e:
            logger.error(f"Failed to mark batch as failed: {e}")
    
    def get_import_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent import history."""
        try:
            conn = self.db.connect()
            cursor = conn.execute("""
                SELECT batch_id, import_mode, source_file, import_date, status,
                       records_imported, records_deleted, broadcast_months_affected
                FROM import_batches 
                ORDER BY import_date DESC 
                LIMIT ?
            """, (limit,))
            
            history = []
            for row in cursor.fetchall():
                history.append({
                    'batch_id': row[0],
                    'import_mode': row[1],
                    'source_file': row[2],
                    'import_date': row[3],
                    'status': row[4],
                    'records_imported': row[5] or 0,
                    'records_deleted': row[6] or 0,
                    'months_affected': eval(row[7]) if row[7] else []  # Convert string back to list
                })
            
            return history
            
        except Exception as e:
            logger.error(f"Failed to get import history: {e}")
            return []
    
    def cleanup_failed_imports(self) -> int:
        """Clean up any failed or stuck import batches."""
        try:
            with self.db.transaction() as conn:
                # Mark old running batches as failed (older than 1 hour)
                cursor = conn.execute("""
                    UPDATE import_batches 
                    SET status = 'FAILED',
                        error_summary = 'Import timed out or was interrupted',
                        completed_at = CURRENT_TIMESTAMP
                    WHERE status = 'RUNNING' 
                      AND import_date < datetime('now', '-1 hour')
                """)
                
                cleaned_count = cursor.rowcount
                
                if cleaned_count > 0:
                    logger.info(f"Cleaned up {cleaned_count} failed import batches")
                
                return cleaned_count
                
        except Exception as e:
            logger.error(f"Failed to cleanup imports: {e}")
            return 0


# Convenience functions for simple usage
def execute_import(excel_file: str, 
                  import_mode: str, 
                  db_path: str,
                  closed_by: str = None,
                  dry_run: bool = False) -> ImportResult:
    """Simple function to execute an import."""
    db_connection = DatabaseConnection(db_path)
    service = BroadcastMonthImportService(db_connection)
    
    try:
        return service.execute_month_replacement(excel_file, import_mode, closed_by, dry_run)
    finally:
        db_connection.close()


# Test and example usage
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Test broadcast month import service")
    parser.add_argument("excel_file", help="Excel file to import")
    parser.add_argument("--db-path", default="data/database/test.db", help="Database path")
    parser.add_argument("--mode", choices=['WEEKLY_UPDATE', 'HISTORICAL', 'MANUAL'],
                       default='WEEKLY_UPDATE', help="Import mode")
    parser.add_argument("--closed-by", help="Required for HISTORICAL mode")
    parser.add_argument("--dry-run", action="store_true", help="Validate and preview without importing")
    parser.add_argument("--validate-only", action="store_true", help="Only validate the import")
    parser.add_argument("--history", action="store_true", help="Show import history")
    parser.add_argument("--cleanup", action="store_true", help="Cleanup failed imports")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    # Setup logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format='%(levelname)s: %(message)s')
    
    if not Path(args.db_path).exists():
        print(f"❌ Database not found: {args.db_path}")
        sys.exit(1)
    
    # Create service
    db_connection = DatabaseConnection(args.db_path)
    service = BroadcastMonthImportService(db_connection)
    
    try:
        if args.history:
            history = service.get_import_history()
            if history:
                print(f"Recent import history:")
                for record in history:
                    print(f"  {record['batch_id']}: {record['import_mode']} - {record['status']} - {record['records_imported']} imported")
            else:
                print("No import history found")
        
        elif args.cleanup:
            cleaned = service.cleanup_failed_imports()
            print(f"Cleaned up {cleaned} failed import batches")
        
        elif args.validate_only:
            validation = service.validate_import(args.excel_file, args.mode)
            if validation.is_valid:
                print(f"✅ Validation passed for {args.mode} mode")
                if validation.closed_months_found:
                    print(f"⚠️  Includes closed months: {validation.closed_months_found}")
            else:
                print(f"❌ Validation failed: {validation.error_message}")
                print(f"Solution: {validation.suggested_action}")
        
        else:
            # Execute import
            result = service.execute_month_replacement(
                args.excel_file, 
                args.mode, 
                args.closed_by,
                args.dry_run
            )
            
            print(f"\nImport Results:")
            print(f"  Success: {result.success}")
            print(f"  Batch ID: {result.batch_id}")
            print(f"  Mode: {result.import_mode}")
            print(f"  Duration: {result.duration_seconds:.2f} seconds")
            print(f"  Months affected: {result.broadcast_months_affected}")
            print(f"  Records deleted: {result.records_deleted:,}")
            print(f"  Records imported: {result.records_imported:,}")
            
            if result.closed_months:
                print(f"  Months closed: {result.closed_months}")
            
            if result.error_messages:
                print(f"  Errors:")
                for error in result.error_messages:
                    print(f"    - {error}")
            
            if not result.success:
                sys.exit(1)
    
    finally:
        db_connection.close()