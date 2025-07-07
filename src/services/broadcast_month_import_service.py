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
from services.month_closure_service import MonthClosureService, ValidationResult, MonthClosureError
from services.import_integration_utilities import extract_display_months_from_excel, validate_excel_for_import
from utils.broadcast_month_utils import BroadcastMonthParser, extract_broadcast_months_from_excel
from services.base_service import BaseService
from utils.broadcast_month_utils import normalize_broadcast_day



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


class BroadcastMonthImportService(BaseService):
    def __init__(self, db_connection: DatabaseConnection):
        super().__init__(db_connection)  # ADD THIS LINE
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
    
    def _lookup_market_id(self, market_name: str, conn) -> Optional[int]:
        """Look up market_id from market_name/market_code"""
        if not market_name:
            return None
            
        cursor = conn.execute("""
            SELECT market_id FROM markets 
            WHERE market_code = ? 
            OR market_name = ? 
            OR (? = 'Admin' AND market_code = 'ADMIN')
            OR (? = 'Admin' AND market_name = 'ADMINISTRATIVE')
        """, (market_name, market_name, market_name, market_name))
        
        result = cursor.fetchone()
        return result[0] if result else None

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
            with self.safe_transaction() as conn:
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
                        try:
                            # Skip the is_month_closed check since we're in a transaction
                            # Just try to close it and handle the error if already closed
                            self.closure_service.close_broadcast_month_with_connection(
                                month, closed_by, conn, f"Auto-closed by historical import {batch_id}"
                            )
                            result.closed_months.append(month)
                            logger.info(f"Closed month {month} as part of historical import")
                            
                        except MonthClosureError as e:
                            if "already closed" in str(e):
                                logger.info(f"Month {month} already closed, skipping")
                            else:
                                logger.error(f"Failed to close month {month}: {e}")
                                result.error_messages.append(f"Failed to close month {month}: {e}")
                
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
        """FIXED: Create import batch record without nested transaction."""
        try:
            # Use existing connection if in transaction, otherwise create safe transaction
            if self.in_transaction:
                conn = self.get_connection()
                conn.execute("""
                    INSERT INTO import_batches (
                        batch_id, import_mode, source_file, broadcast_months_affected,
                        status, started_by
                    ) VALUES (?, ?, ?, ?, 'RUNNING', ?)
                """, (batch_id, import_mode, source_file, str(months), 'system'))
            else:
                with self.safe_transaction() as conn:
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
        """Delete all non-historical data for specified broadcast months."""
        total_deleted = 0

        for display_month in display_months:
            try:
                normalized_month = datetime.strptime(display_month, "%b-%y").strftime("%Y-%m")
            except ValueError:
                normalized_month = display_month  # Assume already in YYYY-MM if parsing fails

            cursor = conn.execute(
                """
                DELETE FROM spots
                WHERE strftime('%Y-%m', broadcast_month) = ?
                AND is_historical = 0
                """,
                (normalized_month,)
            )
            deleted = cursor.rowcount
            print(f"   🗑️  Deleted {deleted:,} spots for {display_month} ({normalized_month})")
            total_deleted += deleted

        return total_deleted


    def _capture_broadcast_month(self, raw_value):
        """Capture normalized broadcast month into stats."""
        if raw_value:
            try:
                dt = normalize_broadcast_day(raw_value)
                label = dt.strftime("%b-%y")
                self.stats["broadcast_months_found"].add(label)
            except Exception:
                pass

   
    def _import_excel_data(self, excel_file: str, batch_id: str, conn) -> int:
        """
        CRITICAL FIX: Import Excel data using a transaction-compatible approach.
        
        The issue was that this method was being called WITHIN a transaction,
        but then trying to create its own transaction, causing a deadlock.
        """
        try:
            print(f"🚀 Starting transaction-compatible Excel import...")
            
            # Instead of using the enhanced importer which creates its own transaction,
            # we'll process the Excel file directly within the existing transaction
            
            # Step 1: Load and parse Excel file
            from openpyxl import load_workbook
            from pathlib import Path
            from datetime import datetime
            
            workbook = load_workbook(excel_file, data_only=True)
            worksheet = workbook.active
            
            # Step 2: Parse headers to understand column structure
            header_row = list(worksheet.iter_rows(min_row=1, max_row=1, values_only=True))[0]
            
            # Complete comprehensive column mapping from enhanced importer
            column_mapping = {
                # ===================================================================
                # CORE FIELDS - Same in both 2024 and 2025 formats
                # ===================================================================
                'Bill Code': 'bill_code',
                'Start Date': 'air_date',
                'End Date': 'end_date',
                'Time In': 'time_in',
                'Time out': 'time_out',
                'Length': 'length_seconds',
                'Comments': 'program',
                'Language': 'language_code',
                'Line': 'line_number',
                'Type': 'spot_type',
                'Make Good': 'make_good',
                'Spot Value': 'spot_value',
                'Month': 'broadcast_month',
                'Broker Fees': 'broker_fees',
                'Revenue Type': 'revenue_type',
                'Billing Type': 'billing_type',
                'Market': 'market_name',
                
                # ===================================================================
                # 2024 FORMAT COLUMNS
                # ===================================================================
                'Day(s)': 'day_of_week',
                'Media/Name/Program': 'media',
                'Format': 'format',
                'Units-Spot count': 'sequence_number',
                'Agency/Episode# or cut number': 'estimate',
                'Unit rate Gross': 'gross_rate',
                'Sales/rep com: revenue sharing': 'priority',
                'Station Net': 'station_net',
                'Sales Person': 'sales_person',
                'Agency?': 'agency_flag',
                'Affidavit?': 'affidavit_flag',
                'Notarize?': 'contract',
                
                # ===================================================================
                # 2025 FORMAT COLUMNS (Alternative mappings)
                # ===================================================================
                'Day': 'day_of_week',              # Alternative to 'Day(s)'
                'Show Name': 'media',              # Alternative to 'Media/Name/Program'
                'Show': 'format',                  # Alternative to 'Format'
                'Spots': 'sequence_number',        # Alternative to 'Units-Spot count'
                'Estimate': 'estimate',            # Alternative to 'Agency/Episode# or cut number'
                'Gross': 'gross_rate',             # Alternative to 'Unit rate Gross'
                ' Gross ': 'gross_rate',           # Handle potential spacing variations
                'Priority': 'priority',            # Alternative to 'Sales/rep com: revenue sharing'
                'Net': 'station_net',              # Alternative to 'Station Net'
                ' Net ': 'station_net',            # Handle potential spacing variations
                'AE': 'sales_person',              # Alternative to 'Sales Person'
                'Agency': 'agency_flag',           # Alternative to 'Agency?'
                'Affidavit': 'affidavit_flag',     # Alternative to 'Affidavit?'
                'Notarize': 'contract',            # Alternative to 'Notarize?'
                
                # ===================================================================
                # ADDITIONAL VARIATIONS (Common alternatives)
                # ===================================================================
                'Account Executive': 'sales_person',   # Another common variation
                'Salesperson': 'sales_person',         # Another common variation
                'Gross Rate': 'gross_rate',            # Another common variation
                'Net Rate': 'station_net',             # Another common variation
                'Air Date': 'air_date',                # Alternative to 'Start Date'
                'Date': 'air_date',                    # Simple date column
                'Program': 'media',                    # Alternative to show/media fields
                'Episode': 'estimate',                 # Alternative to estimate field
            }
            
            # Find column indexes
            column_indexes = {}
            for col_idx, header in enumerate(header_row):
                if header and str(header).strip() in column_mapping:
                    field_name = column_mapping[str(header).strip()]
                    column_indexes[field_name] = col_idx
            
            print(f"📋 Mapped {len(column_indexes)} columns for direct import")
            
            # Step 3: Process rows within the existing transaction
            total_rows = worksheet.max_row - 1
            imported_count = 0
            skipped_count = 0
            
            print(f"📈 Processing {total_rows:,} records within existing transaction...")
            
            # Pre-load some lookup data to avoid repeated queries
            agency_cache = {}
            customer_cache = {}
            market_cache = {}
            
            for row_num, row in enumerate(worksheet.iter_rows(min_row=2, values_only=True), start=2):
                if not any(cell for cell in row):
                    continue
                
                try:
                    # Extract basic required fields
                    bill_code = None
                    air_date = None
                    
                    if 'bill_code' in column_indexes:
                        bill_code = row[column_indexes['bill_code']]
                    if 'air_date' in column_indexes:
                        air_date = row[column_indexes['air_date']]
                    
                    if not bill_code or not air_date:
                        skipped_count += 1
                        continue
                    
                    # Parse bill code for agency/customer
                    agency_name = None
                    customer_name = str(bill_code).strip()
                    
                    if ':' in str(bill_code):
                        parts = str(bill_code).split(':', 1)
                        agency_name = parts[0].strip()
                        customer_name = parts[1].strip()
                    
                    # Remove PRODUCTION suffixes
                    for suffix in [' PRODUCTION', ' Production', ' PROD']:
                        if customer_name.endswith(suffix):
                            customer_name = customer_name[:-len(suffix)].strip()
                            break
                    
                    # Get or create agency
                    agency_id = None
                    if agency_name:
                        if agency_name in agency_cache:
                            agency_id = agency_cache[agency_name]
                        else:
                            cursor = conn.execute("SELECT agency_id FROM agencies WHERE agency_name = ?", (agency_name,))
                            row_result = cursor.fetchone()
                            if row_result:
                                agency_id = row_result[0]
                            else:
                                cursor = conn.execute("INSERT INTO agencies (agency_name) VALUES (?)", (agency_name,))
                                agency_id = cursor.lastrowid
                            agency_cache[agency_name] = agency_id
                    
                    # Get or create customer
                    customer_id = None
                    if customer_name in customer_cache:
                        customer_id = customer_cache[customer_name]
                    else:
                        cursor = conn.execute("SELECT customer_id FROM customers WHERE normalized_name = ?", (customer_name,))
                        row_result = cursor.fetchone()
                        if row_result:
                            customer_id = row_result[0]
                        else:
                            cursor = conn.execute("INSERT INTO customers (normalized_name) VALUES (?)", (customer_name,))
                            customer_id = cursor.lastrowid
                        customer_cache[customer_name] = customer_id
                    
                    # Build spot data dictionary
                    spot_data = {
                        'bill_code': str(bill_code).strip(),
                        'customer_id': customer_id,
                        'agency_id': agency_id,
                        'source_file': Path(excel_file).name,
                        'load_date': datetime.now(),
                        'is_historical': 0,
                        'import_batch_id': batch_id
                    }
                    # ADD THIS BLOCK - Market ID lookup
                    market_name = None
                    if 'market_name' in column_indexes and column_indexes['market_name'] < len(row):
                        market_name = row[column_indexes['market_name']]
                        
                    if market_name:
                        market_name = str(market_name).strip()
                        
                        # Use cache for market lookups
                        if market_name in market_cache:
                            market_id = market_cache[market_name]
                        else:
                            market_id = self._lookup_market_id(market_name, conn)
                            market_cache[market_name] = market_id
                            
                        # Add to spot_data
                        spot_data['market_name'] = market_name
                        spot_data['market_id'] = market_id
                        
                        if not market_id:
                            print(f"⚠️ Could not find market_id for market_name: {market_name}")
                    # END ADD BLOCK
                    # Add other fields from Excel
                    for field_name, col_idx in column_indexes.items():
                        if field_name not in ['bill_code', 'market_name'] and col_idx < len(row):
                            raw_value = row[col_idx]
                            if raw_value is not None:
                                # Basic type conversion
                                if field_name == 'air_date':
                                    if hasattr(raw_value, 'date'):
                                        spot_data[field_name] = raw_value.date()
                                    else:
                                        spot_data[field_name] = raw_value
                                elif field_name in ['gross_rate', 'station_net', 'spot_value', 'broker_fees']:
                                    try:
                                        spot_data[field_name] = float(raw_value) if raw_value else None
                                    except:
                                        spot_data[field_name] = None
                                elif field_name == 'broadcast_month':
                                    try:
                                        raw_date = raw_value.date() if hasattr(raw_value, 'date') else raw_value
                                        spot_data[field_name] = normalize_broadcast_day(raw_date)
                                    except Exception as e:
                                        print(f"⚠️ Failed to normalize broadcast_month on row {row_num}: {e}")
                                        skipped_count += 1
                                        continue
                                else:
                                    spot_data[field_name] = str(raw_value).strip() if raw_value else None

                    
                    # Insert spot record
                    fields = list(spot_data.keys())
                    placeholders = ', '.join(['?' for _ in fields])
                    field_names = ', '.join(fields)
                    values = [spot_data[field] for field in fields]
                    
                    query = f"INSERT INTO spots ({field_names}) VALUES ({placeholders})"
                    conn.execute(query, values)
                    
                    imported_count += 1
                    
                    # Progress update every 10000 records
                    if imported_count % 10000 == 0:
                        print(f"📊 Imported {imported_count:,} records...")
                    
                except Exception as e:
                    print(f"⚠️  Error processing row {row_num}: {e}")
                    skipped_count += 1
                    continue
            
            workbook.close()
            
            print(f"✅ Direct import completed: {imported_count:,} imported, {skipped_count:,} skipped")
            return imported_count
            
        except Exception as e:
            error_msg = f"Direct Excel import failed: {str(e)}"
            print(f"❌ {error_msg}")
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
        """FIXED: Mark import batch as failed without nested transaction."""
        try:
            # Use existing connection if in transaction, otherwise create safe transaction
            if self.in_transaction:
                conn = self.get_connection()
                conn.execute("""
                    UPDATE import_batches 
                    SET status = 'FAILED',
                        completed_at = CURRENT_TIMESTAMP,
                        error_summary = ?
                    WHERE batch_id = ?
                """, (error_message, batch_id))
            else:
                with self.safe_transaction() as conn:
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
            with self.safe_connection() as conn:  # ✅ BaseService connection management
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
                        'months_affected': eval(row[7]) if row[7] else []
                    })
                
                return history
                
        except Exception as e:
            logger.error(f"Failed to get import history: {e}")
            return []
    
    def cleanup_failed_imports(self) -> int:
        """Clean up any failed or stuck import batches."""
        try:
            with self.safe_transaction() as conn:
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
    parser.add_argument("--db-path", default="data/database/production.db", help="Database path")
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