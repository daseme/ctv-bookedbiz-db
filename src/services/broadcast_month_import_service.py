#!/usr/bin/env python3
"""
Broadcast month import service for managing month-based data imports.
Orchestrates the complete import workflow with validation, deletion, and import.
Enhanced with progress bars and clean output.
"""

import re
import sys
import logging
import uuid
import contextlib
import io
from pathlib import Path
from datetime import datetime, date
from typing import List, Set, Optional, Dict, Any, Callable
from dataclasses import dataclass

# Add tqdm for progress bars
from tqdm import tqdm

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import DatabaseConnection
from src.services.month_closure_service import MonthClosureService, ValidationResult, MonthClosureError
from src.services.import_integration_utilities import extract_display_months_from_excel, validate_excel_for_import
from src.utils.broadcast_month_utils import BroadcastMonthParser, extract_broadcast_months_from_excel
from src.services.base_service import BaseService
from src.utils.broadcast_month_utils import normalize_broadcast_day
from src.services.entity_alias_service import EntityAliasService

logger = logging.getLogger(__name__)

EXCEL_COLUMN_POSITIONS = {
    0:  'bill_code',           # Bill Code
    1:  'air_date',            # Start Date  
    2:  'end_date',            # End Date
    3:  'day_of_week',         # Day(s)
    4:  'time_in',             # Time In
    5:  'time_out',            # Time Out
    6:  'length_seconds',      # Length
    7:  'media',               # Media/Name/Program (was "program", now renamed)
    8:  'comments',            # Comments (if we want to preserve both, otherwise make this `None`)
    9:  'language_code',       # Language
    10: 'format',              # Format
    11: 'sequence_number',     # Units-Spot count
    12: 'line_number',         # Line
    13: 'spot_type',           # Type
    14: 'estimate',            # Agency/Episode# or cut number
    15: 'gross_rate',          # Unit rate Gross
    16: 'make_good',           # Make Good
    17: 'spot_value',          # Spot Value
    18: 'broadcast_month',     # Month
    19: 'broker_fees',         # Broker Fees
    20: 'priority',            # Sales/rep com: revenue sharing
    21: 'station_net',         # Station Net
    22: 'sales_person',        # Sales Person
    23: 'revenue_type',        # Revenue Type
    24: 'billing_type',        # Billing Type
    25: 'agency_flag',         # Agency?
    26: 'affidavit_flag',      # Affidavit?
    27: 'contract',            # Notarize?
    28: 'market_name',         # Market
}



@contextlib.contextmanager
def suppress_verbose_logging():
    """Context manager to suppress verbose logging during import operations."""
    # Get all loggers and save their levels
    root_logger = logging.getLogger()
    original_level = root_logger.level
    
    # Set root logger to only show WARNINGS and above
    root_logger.setLevel(logging.WARNING)
    
    # Suppress specific noisy loggers
    noisy_loggers = [
        'openpyxl',
        'xlrd',
        'pandas',
        'services',
        'utils',
        '__main__'
    ]
    
    original_levels = {}
    for logger_name in noisy_loggers:
        logger_obj = logging.getLogger(logger_name)
        original_levels[logger_name] = logger_obj.level
        logger_obj.setLevel(logging.WARNING)
    
    try:
        yield
    finally:
        # Restore original logging levels
        root_logger.setLevel(original_level)
        for logger_name, level in original_levels.items():
            logging.getLogger(logger_name).setLevel(level)


@contextlib.contextmanager
def suppress_stdout_stderr():
    """Context manager to suppress stdout/stderr during noisy operations."""
    old_stdout = sys.stdout
    old_stderr = sys.stderr
    
    try:
        sys.stdout = io.StringIO()
        sys.stderr = io.StringIO()
        yield
    finally:
        sys.stdout = old_stdout
        sys.stderr = old_stderr


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


def build_revenue_type_normalizer() -> Callable[[Optional[str]], Optional[str]]:
    """Pure normalizer: A&O variants → 'Internal Ad Sales'; else passthrough."""
    def _norm(v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        key = re.sub(r'[\s\./-]+', '', v.lower()).replace('&', 'and')
        return 'Internal Ad Sales' if key in {'ao', 'aando'} else v
    return _norm

def build_spot_type_normalizer() -> Callable[[Optional[str]], str]:
    """Pure normalizer for CHECK constraint on spots.spot_type."""
    allowed = {'AV','BB','BNS','COM','CRD','PKG','PRD','PRG','SVC',''}
    aliases = {
        'SPOT': 'COM',
        'COMMERCIAL': 'COM',
        'BONUS': 'BNS',
        'PKG': 'PKG',
        'PROD': 'PRD',
        'PROGRAM': 'PRG',
        'SERVICE': 'SVC',
    }
    def _norm(v: Optional[str]) -> str:
        x = (v or '').strip().upper()
        x = aliases.get(x, x)
        return x if x in allowed else ''
    return _norm


class BroadcastMonthImportError(Exception):
    """Raised when there's an error with import operations."""
    pass


class BroadcastMonthImportService(BaseService):
    def __init__(self, db_connection: DatabaseConnection):
        super().__init__(db_connection)
        self.closure_service = MonthClosureService(db_connection)
        self.parser = BroadcastMonthParser()
        self.alias_service = EntityAliasService(db_connection)
        self.normalize_revenue_type = build_revenue_type_normalizer()
        self.normalize_spot_type = build_spot_type_normalizer()
    
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
            with suppress_verbose_logging():
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

    def _lookup_entities_exact(self, bill_code: str, conn) -> Dict[str, Optional[int]]:
        """
        Phase 1: Exact string matching for agencies and customers.
        Bill code format: "Agency:Customer Name"
        """
        result = {'agency_id': None, 'customer_id': None}
        
        if not bill_code or ':' not in bill_code:
            return result
        
        try:
            # Split on ':' to get agency and customer
            parts = bill_code.split(':', 1)
            if len(parts) != 2:
                return result
                
            agency_name = parts[0].strip()
            customer_name = parts[1].strip()
            
            # Exact agency lookup
            cursor = conn.execute("""
                SELECT agency_id FROM agencies 
                WHERE agency_name = ? AND is_active = 1
            """, (agency_name,))
            
            agency_result = cursor.fetchone()
            if agency_result:
                result['agency_id'] = agency_result[0]
            
            # Exact customer lookup  
            cursor = conn.execute("""
                SELECT customer_id FROM customers 
                WHERE normalized_name = ? AND is_active = 1
            """, (customer_name,))
            
            customer_result = cursor.fetchone()
            if customer_result:
                result['customer_id'] = customer_result[0]
                
            return result
            
        except Exception as e:
            logger.warning(f"Failed to lookup entities for bill_code '{bill_code}': {e}")
            return result

    def _lookup_direct_customer(self, bill_code: str, conn) -> Optional[int]:
        """
        Phase 1.5: Handle direct billing customers (no agency in bill_code).
        For entities like "Sacramento County Water Agency" that are customers, not "Agency:Customer".
        """
        if not bill_code:
            return None
            
        try:
            # Direct exact lookup for customer
            cursor = conn.execute("""
                SELECT customer_id FROM customers 
                WHERE normalized_name = ? AND is_active = 1
            """, (bill_code.strip(),))
            
            result = cursor.fetchone()
            return result[0] if result else None
            
        except Exception as e:
            logger.warning(f"Failed direct customer lookup for '{bill_code}': {e}")
            return None

    def execute_month_replacement(self, 
                                excel_file: str, 
                                import_mode: str, 
                                closed_by: str = None,
                                dry_run: bool = False) -> ImportResult:
        """
        Execute complete month replacement workflow with progress tracking.
        """
        start_time = datetime.now()
        batch_id = f"{import_mode.lower()}_{int(start_time.timestamp())}"
        
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
            # Step 1: Extract months from Excel (with progress)
            with suppress_verbose_logging():
                tqdm.write(f"🔍 Analyzing Excel file: {Path(excel_file).name}")
                display_months = list(extract_display_months_from_excel(excel_file))
            
            if not display_months:
                raise BroadcastMonthImportError("No broadcast months found in Excel file")
            
            tqdm.write(f"📅 Found {len(display_months)} months: {', '.join(sorted(display_months))}")
            
            # Step 2: Check which months are closed
            closed_months = self.closure_service.get_closed_months(display_months)
            open_months = [month for month in display_months if month not in closed_months]
            
            if closed_months:
                tqdm.write(f"🔒 Closed months: {len(closed_months)} ({', '.join(sorted(closed_months))})")
            if open_months:
                tqdm.write(f"📂 Open months: {len(open_months)} ({', '.join(sorted(open_months))})")
            
            # Step 3: Handle different import modes
            if import_mode == 'WEEKLY_UPDATE':
                if closed_months:
                    tqdm.write(f"✅ WEEKLY_UPDATE: Auto-skipping {len(closed_months)} closed months")
                
                if not open_months:
                    tqdm.write("✅ No open months to import - all months are closed")
                    result.success = True
                    return result
                
                # Only process open months
                result.broadcast_months_affected = open_months
                
            elif import_mode == 'HISTORICAL':
                # Historical imports can process all months (will close them afterwards)
                if not closed_by:
                    raise BroadcastMonthImportError("HISTORICAL mode requires --closed-by parameter")
                result.broadcast_months_affected = display_months
                
            else:  # MANUAL mode
                # Manual imports require explicit handling of closed months
                if closed_months:
                    validation_error = f"Manual import contains closed months: {closed_months}. Use --force to override or filter the Excel file."
                    result.error_messages.append(validation_error)
                    tqdm.write(f"❌ {validation_error}")
                    return result
                result.broadcast_months_affected = display_months
            
            tqdm.write(f"🎯 Processing {len(result.broadcast_months_affected)} months: {', '.join(result.broadcast_months_affected)}")
            
            if dry_run:
                tqdm.write("🔍 DRY RUN - No changes will be made")
                result.success = True
                return result
            
            # Step 4: Create import batch record
            batch_record_id = self._create_import_batch(batch_id, import_mode, excel_file, result.broadcast_months_affected)
            
            # Step 5: Execute the import in transaction with progress tracking
            try:
                with self.safe_transaction() as conn:
                    # Delete existing data with progress
                    deleted_count = self._delete_broadcast_month_data_with_progress(result.broadcast_months_affected, conn)
                    result.records_deleted = deleted_count
                    
                    # Import filtered data with comprehensive progress tracking
                    imported_count = self._import_excel_data_with_progress(
                        excel_file, batch_id, conn, result.broadcast_months_affected
                    )
                    result.records_imported = imported_count
                    
                    # Clean summary instead of verbose logging
                    if import_mode == 'WEEKLY_UPDATE' and closed_months:
                        net_change = imported_count - deleted_count
                        tqdm.write(f"📊 Import complete: {imported_count:,} imported, {deleted_count:,} deleted (net: {net_change:+,})")
                    
                    # Step 6: Handle HISTORICAL mode - close all months (atomic, same transaction)
                    if import_mode == 'HISTORICAL':
                        with tqdm(total=len(result.broadcast_months_affected), desc="🔒 Closing months", unit=" months") as pbar:
                            for month in result.broadcast_months_affected:
                                try:
                                    # NOTE: use the "with_connection" variant to reuse this txn/conn
                                    self.closure_service.close_broadcast_month_with_connection(month, closed_by, conn)
                                    result.closed_months.append(month)
                                    pbar.update(1)
                                    pbar.set_description(f"🔒 Closed {month}")
                                except MonthClosureError as e:
                                    tqdm.write(f"⚠️ Failed to close month {month}: {e}")
                                    pbar.update(1)
                        tqdm.write(f"✅ Closed {len(result.closed_months)} months")

                    # Step 7: Complete the batch record
                    self._complete_import_batch(batch_id, result, conn)
                    
                    # If we get here, the transaction was successful
                    result.success = True
                    duration = (datetime.now() - start_time).total_seconds()
                    tqdm.write(f"✅ Import completed successfully in {duration:.1f} seconds")
                    
            except Exception as transaction_error:
                error_msg = f"Transaction failed: {str(transaction_error)}"
                tqdm.write(f"❌ {error_msg}")
                result.error_messages.append(error_msg)
                self._fail_import_batch(batch_id, error_msg)
                raise BroadcastMonthImportError(error_msg)
                
        except Exception as e:
            error_msg = f"Import failed: {str(e)}"
            tqdm.write(f"❌ {error_msg}")
            result.error_messages.append(error_msg)
            if 'batch_id' in locals():
                self._fail_import_batch(batch_id, error_msg)
        
        # Calculate total duration
        result.duration_seconds = (datetime.now() - start_time).total_seconds()
        
        return result
    
    def _create_import_batch(self, batch_id: str, import_mode: str, source_file: str, months: List[str]) -> int:
        """Create import batch record."""
        try:
            with self.safe_transaction() as conn:
                cursor = conn.execute("""
                    INSERT INTO import_batches 
                    (batch_id, import_mode, source_file, import_date, status, broadcast_months_affected)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP, 'RUNNING', ?)
                """, (batch_id, import_mode, source_file, str(months)))
                
                batch_record_id = cursor.lastrowid
                return batch_record_id
                
        except Exception as e:
            error_msg = f"Failed to create import batch: {str(e)}"
            tqdm.write(f"❌ {error_msg}")
            raise BroadcastMonthImportError(error_msg)
    
    def _delete_broadcast_month_data_with_progress(self, months: List[str], conn) -> int:
        """Delete existing data for specified broadcast months with progress tracking."""
        if not months:
            return 0
        
        total_deleted = 0
        
        # First, get counts for progress bar
        month_counts = {}
        for month in months:
            cursor = conn.execute("SELECT COUNT(*) FROM spots WHERE broadcast_month = ?", (month,))
            count = cursor.fetchone()[0]
            month_counts[month] = count
            total_deleted += count
        
        if total_deleted == 0:
            tqdm.write("📭 No existing records to delete")
            return 0
        
        # Delete with progress tracking
        deleted_so_far = 0
        with tqdm(total=total_deleted, desc="🗑️  Deleting existing records", unit=" records") as pbar:
            for month in months:
                count = month_counts[month]
                if count > 0:
                    cursor = conn.execute("DELETE FROM spots WHERE broadcast_month = ?", (month,))
                    actual_deleted = cursor.rowcount
                    deleted_so_far += actual_deleted
                    pbar.update(actual_deleted)
                    pbar.set_description(f"🗑️  Deleted {deleted_so_far:,}/{total_deleted:,}")
        
        tqdm.write(f"✅ Deleted {total_deleted:,} existing records")
        return total_deleted
    
    def _import_excel_data_with_progress(self, excel_file: str, batch_id: str, conn, allowed_months: List[str]) -> int:
        """
        Import Excel data using fixed column positions with comprehensive progress tracking.
        """
        from openpyxl import load_workbook
        from datetime import datetime
        import re

        # Verify batch exists
        cursor = conn.execute("SELECT COUNT(*) FROM import_batches WHERE batch_id = ?", (batch_id,))
        if cursor.fetchone()[0] == 0:
            raise BroadcastMonthImportError(f"batch_id {batch_id} not found in import_batches table")

        try:
            with suppress_verbose_logging(), suppress_stdout_stderr():
                workbook = load_workbook(excel_file, read_only=True, data_only=True)
                worksheet = workbook.active

            total_records = worksheet.max_row - 1
            imported_count = 0
            skipped_count = 0
            filtered_count = 0
            unmatched_customers = set()
            unmatched_agencies = set()

            with tqdm(total=total_records, desc="📦 Processing Excel rows", unit=" rows") as pbar:
                for row_num, row in enumerate(worksheet.iter_rows(min_row=2, values_only=True), start=2):
                    pbar.update(1)

                    try:
                        if not any(row):
                            continue

                        # Use fixed position for broadcast_month
                        month_col_index = [k for k, v in EXCEL_COLUMN_POSITIONS.items() if v == 'broadcast_month']
                        if not month_col_index:
                            skipped_count += 1
                            continue
                        month_value = row[month_col_index[0]]

                        if not month_value:
                            skipped_count += 1
                            continue

                        # Format broadcast_month
                        try:
                            if hasattr(month_value, 'date'):
                                bm_date = month_value.date()
                            elif isinstance(month_value, str):
                                for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S"]:
                                    try:
                                        bm_date = datetime.strptime(month_value.strip(), fmt).date()
                                        break
                                    except:
                                        continue
                                else:
                                    bm_date = datetime.strptime(str(month_value), "%Y-%m-%d").date()
                            else:
                                bm_date = month_value
                            broadcast_month_display = bm_date.strftime("%b-%y")
                        except:
                            skipped_count += 1
                            continue

                        if broadcast_month_display not in allowed_months:
                            filtered_count += 1
                            continue

                        spot_data = {
                            'import_batch_id': batch_id,
                            'broadcast_month': broadcast_month_display,
                        }
                        
                        for col_idx, field_name in EXCEL_COLUMN_POSITIONS.items():
                            if field_name and col_idx < len(row):
                                val = row[col_idx]
                                if val is None or val == '':
                                    continue

                                if field_name == 'bill_code':
                                    spot_data[field_name] = str(val).strip()

                                elif field_name == 'air_date':
                                    try:
                                        spot_data[field_name] = val.date().isoformat() if hasattr(val, 'date') else str(val).strip()
                                    except:
                                        spot_data[field_name] = str(val).strip()

                                elif field_name in ['gross_rate', 'station_net', 'spot_value', 'broker_fees']:
                                    try:
                                        spot_data[field_name] = float(val)
                                    except:
                                        spot_data[field_name] = None

                                elif field_name == 'day_of_week':
                                    try:
                                        spot_data[field_name] = normalize_broadcast_day(str(val).strip())
                                    except:
                                        spot_data[field_name] = str(val).strip()

                                elif field_name == 'revenue_type':  # NEW: normalize A&O → Internal Ad Sales
                                    spot_data[field_name] = self.normalize_revenue_type(str(val).strip())

                                elif field_name == 'spot_type':     # NEW: enforce CHECK constraint
                                    spot_data[field_name] = self.normalize_spot_type(str(val).strip())

                                elif field_name != 'broadcast_month':  # already handled above
                                    spot_data[field_name] = str(val).strip()


                        if 'market_name' in spot_data:
                            market_id = self._lookup_market_id(spot_data['market_name'], conn)
                            if market_id:
                                spot_data['market_id'] = market_id

                        if 'bill_code' in spot_data:
                            with suppress_verbose_logging():
                                entities = self._lookup_entities_with_aliases(spot_data['bill_code'], conn)
                            if entities['customer_id']:
                                spot_data['customer_id'] = entities['customer_id']
                            else:
                                unmatched_customers.add(spot_data['bill_code'])
                            if entities['agency_id']:
                                spot_data['agency_id'] = entities['agency_id']
                            else:
                                if ':' in spot_data['bill_code']:
                                    unmatched_agencies.add(spot_data['bill_code'].split(':', 1)[0].strip())

                        if 'language_id' not in spot_data:
                            spot_data['language_id'] = 1

                        if not spot_data.get('bill_code') or not spot_data.get('air_date'):
                            skipped_count += 1
                            continue

                        with suppress_verbose_logging():
                            fields = list(spot_data.keys())
                            placeholders = ', '.join(['?'] * len(fields))
                            field_names = ', '.join(fields)
                            values = [spot_data[field] for field in fields]

                            query = f"INSERT INTO spots ({field_names}) VALUES ({placeholders})"
                            conn.execute(query, values)
                            imported_count += 1

                        if imported_count % 1000 == 0:
                            pbar.set_description(f"📦 Imported {imported_count:,} records")

                    except Exception as row_error:
                        skipped_count += 1
                        if skipped_count <= 5:
                            tqdm.write(f"⚠️ Row {row_num} error: {str(row_error)[:100]}...")
                        continue

            workbook.close()
            tqdm.write(f"✅ Import complete: {imported_count:,} records imported")
            if skipped_count:
                tqdm.write(f"⏭️ Skipped: {skipped_count:,} records (errors)")
            if filtered_count:
                tqdm.write(f"🚫 Filtered: {filtered_count:,} records (closed months)")
            if unmatched_customers:
                tqdm.write(f"⚠️ Unmatched customers: {len(unmatched_customers)}")
            if unmatched_agencies:
                tqdm.write(f"⚠️ Unmatched agencies: {len(unmatched_agencies)}")

            return imported_count

        except Exception as e:
            raise BroadcastMonthImportError(f"Excel import failed: {e}")


    def _lookup_entities_with_aliases(self, bill_code: str, conn) -> dict:
        """
        Enhanced entity lookup with alias support.
        Priority: exact match → alias match → None
        """
        result = {'agency_id': None, 'customer_id': None, 'used_alias': False}
        
        if not bill_code:
            return result
        
        if ':' in bill_code:
            # Standard "Agency:Customer" format
            agency_part, customer_part = bill_code.split(':', 1)
            agency_name = agency_part.strip()
            customer_name = customer_part.strip()
            
            # Customer lookup: exact then alias
            result['customer_id'] = self._lookup_customer_id(customer_name, conn)
            if not result['customer_id']:
                result['customer_id'] = self.alias_service.lookup_customer_by_alias(customer_name, conn)
                if result['customer_id']:
                    result['used_alias'] = True
            
            # Agency lookup: exact then alias  
            result['agency_id'] = self._lookup_agency_id(agency_name, conn)
            if not result['agency_id']: 
                result['agency_id'] = self.alias_service.lookup_agency_by_alias(agency_name, conn)
                if result['agency_id']:
                    result['used_alias'] = True
                
        else:
            # Direct billing format
            result['customer_id'] = self._lookup_direct_customer(bill_code, conn)
            if not result['customer_id']:
                result['customer_id'] = self.alias_service.lookup_customer_by_alias(bill_code.strip(), conn)
                if result['customer_id']:
                    result['used_alias'] = True
        
        return result

    def _lookup_customer_id(self, customer_name: str, conn) -> Optional[int]:
        """Direct customer lookup (no alias)."""
        try:
            cursor = conn.execute("""
                SELECT customer_id FROM customers 
                WHERE normalized_name = ? AND is_active = 1
            """, (customer_name,))
            
            result = cursor.fetchone()
            return result[0] if result else None
        except:
            return None

    def _lookup_agency_id(self, agency_name: str, conn) -> Optional[int]:
        """Direct agency lookup (no alias)."""
        try:
            cursor = conn.execute("""
                SELECT agency_id FROM agencies 
                WHERE agency_name = ? AND is_active = 1
            """, (agency_name,))
            
            result = cursor.fetchone()
            return result[0] if result else None
        except:
            return None
    
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
            
        except Exception as e:
            tqdm.write(f"⚠️ Failed to update batch completion: {e}")
    
    def _fail_import_batch(self, batch_id: str, error_message: str):
        """Mark import batch as failed."""
        try:
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
            
        except Exception as e:
            logger.error(f"Failed to mark batch as failed: {e}")
    
    def get_import_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent import history."""
        try:
            with self.safe_connection() as conn:
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
                    tqdm.write(f"🧹 Cleaned up {cleaned_count} failed import batches")
                
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
    
    # Setup logging - default to WARNING to reduce noise
    level = logging.DEBUG if args.verbose else logging.WARNING
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
            # Execute import with clean output
            result = service.execute_month_replacement(
                args.excel_file, 
                args.mode, 
                args.closed_by,
                args.dry_run
            )
            
            print(f"\n📊 Import Results:")
            print(f"  Status: {'✅ Success' if result.success else '❌ Failed'}")
            print(f"  Batch ID: {result.batch_id}")
            print(f"  Mode: {result.import_mode}")
            print(f"  Duration: {result.duration_seconds:.1f} seconds")
            print(f"  Months: {', '.join(result.broadcast_months_affected)}")
            print(f"  Records deleted: {result.records_deleted:,}")
            print(f"  Records imported: {result.records_imported:,}")
            
            if result.closed_months:
                print(f"  Months closed: {', '.join(result.closed_months)}")
            
            if result.error_messages:
                print(f"  Errors:")
                for error in result.error_messages:
                    print(f"    - {error}")
            
            if not result.success:
                sys.exit(1)
    
    finally:
        db_connection.close()