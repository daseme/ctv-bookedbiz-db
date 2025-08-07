#!/usr/bin/env python3
"""
Broadcast month import service for managing month-based data imports.
Orchestrates the complete import workflow with validation, deletion, and import.
"""

import re
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
from services.entity_alias_service import EntityAliasService

logger = logging.getLogger(__name__)

EXCEL_COLUMN_POSITIONS = {
    0: 'bill_code',           # Bill Code
    1: 'air_date',            # Start Date  
    2: 'end_date',            # End Date
    3: 'day_of_week',         # Day(s)
    4: 'time_in',             # Time In
    5: 'time_out',            # Time out
    6: 'length_seconds',      # Length
    7: 'program',             # Media/Name/Program
    8: None,                  # Comments (ignore - not in schema)
    9: 'language_code',       # Language
    10: 'format',             # Format
    11: 'sequence_number',    # Units-Spot count
    12: 'line_number',        # Line
    13: 'spot_type',          # Type
    14: 'estimate',           # Agency/Episode# or cut number
    15: 'gross_rate',         # Unit rate Gross
    16: 'make_good',          # Make Good
    17: 'spot_value',         # Spot Value
    18: 'broadcast_month',    # Month
    19: 'broker_fees',        # Broker Fees
    20: 'priority',           # Sales/rep com: revenue sharing
    21: 'station_net',        # Station Net
    22: 'sales_person',       # Sales Person
    23: 'revenue_type',       # Revenue Type
    24: 'billing_type',       # Billing Type
    25: 'agency_flag',        # Agency?
    26: 'affidavit_flag',     # Affidavit?
    27: 'contract',           # Notarize?
    28: 'market_name',        # Market
}


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
        super().__init__(db_connection)
        self.closure_service = MonthClosureService(db_connection)
        self.parser = BroadcastMonthParser()
        self.alias_service = EntityAliasService(db_connection)
    
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
        Execute complete month replacement workflow.
        FIXED: Proper error handling and transaction management.
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
            # Step 1: Extract months from Excel
            display_months = list(extract_display_months_from_excel(excel_file))
            
            if not display_months:
                raise BroadcastMonthImportError("No broadcast months found in Excel file")
            
            logger.info(f"Found {len(display_months)} months in Excel: {sorted(display_months)}")
            
            # Step 2: Check which months are closed
            closed_months = self.closure_service.get_closed_months(display_months)
            open_months = [month for month in display_months if month not in closed_months]
            
            logger.info(f"Closed months: {len(closed_months)} {sorted(closed_months)}")
            logger.info(f"Open months: {len(open_months)} {sorted(open_months)}")
            
            # Step 3: Handle different import modes
            if import_mode == 'WEEKLY_UPDATE':
                if closed_months:
                    logger.info(f"🚫 WEEKLY_UPDATE: Auto-skipping {len(closed_months)} closed months: {closed_months}")
                    logger.info(f"✅ WEEKLY_UPDATE: Will process {len(open_months)} open months: {open_months}")
                
                if not open_months:
                    logger.info("No open months to import - all months are closed")
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
                    logger.error(validation_error)
                    return result
                result.broadcast_months_affected = display_months
            
            logger.info(f"Will process {len(result.broadcast_months_affected)} months: {result.broadcast_months_affected}")
            
            if dry_run:
                logger.info("DRY RUN - No changes will be made")
                result.success = True
                return result
            
            # Step 4: Create import batch record
            batch_record_id = self._create_import_batch(batch_id, import_mode, excel_file, result.broadcast_months_affected)
            
            # Step 5: Execute the import in transaction with proper error handling
            try:
                with self.safe_transaction() as conn:
                    # Delete existing data for months we're processing
                    deleted_count = self._delete_broadcast_month_data(result.broadcast_months_affected, conn)
                    result.records_deleted = deleted_count
                    
                    logger.info(f"Deleted {deleted_count} existing records for {len(result.broadcast_months_affected)} months")
                    
                    # Import filtered data (only for months we're processing)
                    imported_count = self._import_excel_data_filtered(
                        excel_file, batch_id, conn, result.broadcast_months_affected
                    )
                    result.records_imported = imported_count
                    
                    logger.info(f"Imported {imported_count} new records")
                    
                    if import_mode == 'WEEKLY_UPDATE' and closed_months:
                        logger.info(f"📊 Import summary:")
                        logger.info(f"   ✅ Processed: {result.broadcast_months_affected}")
                        logger.info(f"   🚫 Skipped (closed): {closed_months}")
                        logger.info(f"   📝 Records imported: {imported_count:,}")
                    
                    # Step 6: Handle HISTORICAL mode - close all months
                    if import_mode == 'HISTORICAL':
                        for month in result.broadcast_months_affected:
                            try:
                                self.closure_service.close_month(month, closed_by, conn)
                                result.closed_months.append(month)
                                logger.info(f"Closed month: {month}")
                            except MonthClosureError as e:
                                logger.warning(f"Failed to close month {month}: {e}")
                    
                    # Step 7: Complete the batch record
                    self._complete_import_batch(batch_id, result, conn)
                    
                    # If we get here, the transaction was successful
                    result.success = True
                    logger.info(f"Import completed successfully in {(datetime.now() - start_time).total_seconds():.2f} seconds")
                    
            except Exception as transaction_error:
                error_msg = f"Transaction failed: {str(transaction_error)}"
                logger.error(error_msg)
                result.error_messages.append(error_msg)
                self._fail_import_batch(batch_id, error_msg)
                raise BroadcastMonthImportError(error_msg)
                
        except Exception as e:
            error_msg = f"Import failed: {str(e)}"
            logger.error(error_msg)
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
                logger.info(f"Created import batch record: {batch_id}")
                return batch_record_id
                
        except Exception as e:
            error_msg = f"Failed to create import batch: {str(e)}"
            logger.error(error_msg)
            raise BroadcastMonthImportError(error_msg)
    
    def _delete_broadcast_month_data(self, months: List[str], conn) -> int:
        """Delete existing data for specified broadcast months."""
        if not months:
            return 0
        
        total_deleted = 0
        
        for month in months:
            cursor = conn.execute("""
                DELETE FROM spots WHERE broadcast_month = ?
            """, (month,))
            
            deleted = cursor.rowcount
            total_deleted += deleted
            
            print(f"   🗑️  Deleted {deleted} spots for {month}")
        
        return total_deleted
    
    def _import_excel_data_filtered(self, excel_file: str, batch_id: str, conn, allowed_months: List[str]) -> int:
        """
        Import Excel data with month filtering and robust broadcast_month conversion.
        Uses the working conversion logic from the original method.
        """
        print(f"🚀 Starting filtered Excel import for months: {allowed_months}")
        
        # Verify batch exists
        cursor = conn.execute("SELECT COUNT(*) FROM import_batches WHERE batch_id = ?", (batch_id,))
        if cursor.fetchone()[0] == 0:
            raise BroadcastMonthImportError(f"❌ batch_id {batch_id} not found in import_batches table")
        else:
            print(f"✅ batch_id {batch_id} exists in import_batches table")
        
        try:
            from openpyxl import load_workbook
            from datetime import datetime
            import re
            
            workbook = load_workbook(excel_file, read_only=True, data_only=True)
            worksheet = workbook.active
            
            # Get header row and build column mapping using position-based approach
            header_row = next(worksheet.iter_rows(min_row=1, max_row=1, values_only=True))
            
            # Use the comprehensive column mapping from the original working method
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
            }

            # Build header index mapping
            field_indices = {}
            for i, header in enumerate(header_row):
                if header and str(header).strip() in column_mapping:
                    field_name = column_mapping[str(header).strip()]
                    field_indices[field_name] = i

            print(f"📋 Found {len(field_indices)} mapped columns from Excel headers")

            # Validate required columns
            required_fields = ['bill_code', 'broadcast_month']
            missing_fields = [field for field in required_fields if field not in field_indices]
            if missing_fields:
                raise BroadcastMonthImportError(f"Missing required columns: {missing_fields}")

            # Count total records for progress
            total_records = sum(1 for _ in worksheet.iter_rows(min_row=2))
            print(f"📈 Processing {total_records:,} records with month filtering...")

            imported_count = 0
            skipped_count = 0
            filtered_count = 0

            # Process each data row
            for row_num, row in enumerate(worksheet.iter_rows(min_row=2, values_only=True), start=2):
                try:
                    if not any(cell for cell in row):
                        continue

                    # Extract broadcast month first to check if we should process this row
                    month_col_index = field_indices.get('broadcast_month')
                    if month_col_index is None or month_col_index >= len(row):
                        skipped_count += 1
                        continue

                    broadcast_month_raw = row[month_col_index]
                    if not broadcast_month_raw:
                        skipped_count += 1
                        continue

                    # ROBUST BROADCAST MONTH CONVERSION (from working original method)
                    try:
                        raw_date = broadcast_month_raw.date() if hasattr(broadcast_month_raw, 'date') else broadcast_month_raw
                        # Convert to mmm-yy format instead of storing as date
                        if hasattr(raw_date, 'strftime'):
                            broadcast_month_display = raw_date.strftime("%b-%y")
                        else:
                            # Handle string dates - MORE ROBUST VERSION
                            if isinstance(raw_date, str):
                                # Try common date formats
                                for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S"]:
                                    try:
                                        parsed_date = datetime.strptime(raw_date.strip(), fmt)
                                        broadcast_month_display = parsed_date.strftime("%b-%y")
                                        break
                                    except:
                                        continue
                                else:
                                    # If no format worked, keep as-is
                                    broadcast_month_display = raw_date.strip()
                            else:
                                # Not a string, try to convert directly
                                parsed_date = datetime.strptime(str(raw_date), "%Y-%m-%d").date()
                                broadcast_month_display = parsed_date.strftime("%b-%y")
                    except Exception as e:
                        print(f"⚠️ Failed to format broadcast_month on row {row_num}: {e}")
                        skipped_count += 1
                        continue

                    # Validate broadcast_month format using original validation
                    if not re.match(r'^[A-Z][a-z]{2}-\d{2}$', broadcast_month_display):
                        print(f"❌ Invalid broadcast_month format: {broadcast_month_display}")
                        skipped_count += 1
                        continue

                    # Filter: Skip if not in allowed months
                    if broadcast_month_display not in allowed_months:
                        if filtered_count < 10:  # Only show first 10 filtered messages
                            print(f"   🚫 Filtered out record for closed month: {broadcast_month_display}")
                        filtered_count += 1
                        continue

                    # Build spot data record using the working approach
                    spot_data = {
                        'import_batch_id': batch_id,
                        'broadcast_month': broadcast_month_display,  # Already converted to proper format
                        # load_date will be auto-generated by database
                    }

                    # Extract all available fields from Excel row
                    for field_name, col_index in field_indices.items():
                        if col_index < len(row):
                            raw_value = row[col_index]
                            
                            if raw_value is not None and raw_value != '':
                                # Process different field types using original working logic
                                if field_name == 'bill_code':
                                    spot_data[field_name] = str(raw_value).strip()
                                elif field_name == 'air_date':
                                    try:
                                        if hasattr(raw_value, 'date'):
                                            spot_data[field_name] = raw_value.date().isoformat()
                                        else:
                                            spot_data[field_name] = str(raw_value).strip()
                                    except:
                                        spot_data[field_name] = str(raw_value).strip()
                                elif field_name in ['gross_rate', 'station_net', 'spot_value', 'broker_fees']:
                                    try:
                                        spot_data[field_name] = float(raw_value) if raw_value else None
                                    except:
                                        spot_data[field_name] = None
                                elif field_name == 'broadcast_month':
                                    # Already processed above, skip
                                    pass
                                elif field_name == 'day_of_week':
                                    try:
                                        if raw_value:
                                            spot_data[field_name] = normalize_broadcast_day(str(raw_value).strip())
                                    except:
                                        spot_data[field_name] = str(raw_value).strip() if raw_value else None
                                else:
                                    spot_data[field_name] = str(raw_value).strip() if raw_value else None

                    # Lookup market_id if market_name provided
                    if 'market_name' in spot_data and spot_data['market_name']:
                        market_id = self._lookup_market_id(spot_data['market_name'], conn)
                        if market_id:
                            spot_data['market_id'] = market_id

                    # ENHANCED: Lookup entities with alias support
                    if 'bill_code' in spot_data and spot_data['bill_code']:
                        entities = self._lookup_entities_with_aliases(spot_data['bill_code'], conn)
                        
                        if entities['customer_id']:
                            spot_data['customer_id'] = entities['customer_id']
                        
                        if entities['agency_id']:
                            spot_data['agency_id'] = entities['agency_id']
                        
                        # Log successful alias resolutions for monitoring
                        if entities.get('used_alias'):
                            if imported_count < 5:  # Only log first 5 for debugging
                                print(f"   ✅ Alias resolved: {spot_data['bill_code']}")
                        
                        # Log remaining unmatched (should be minimal now)
                        if not entities['customer_id'] and imported_count < 5:
                            print(f"   ⚠️  No customer match: {spot_data['bill_code']}")
                        if not entities['agency_id'] and imported_count < 5:
                            print(f"   ⚠️  No agency match: {spot_data['bill_code']}")
                        
                        # Log unmatched entities for Phase 2 analysis (only show first 10)
                        if not entities['customer_id'] and imported_count < 10:
                            print(f"   ⚠️  No customer match: {spot_data['bill_code']}")
                        if not entities['agency_id'] and imported_count < 10:
                            print(f"   ⚠️  No agency match: {spot_data['bill_code']}")

                    # Set default language_id if not specified
                    if 'language_id' not in spot_data:
                        spot_data['language_id'] = 1  # Default to English

                    # Validate required fields
                    if not spot_data.get('bill_code'):
                        skipped_count += 1
                        continue

                    if not spot_data.get('air_date'):
                        skipped_count += 1
                        continue

                    # Insert the record with detailed error handling
                    try:
                        fields = list(spot_data.keys())
                        placeholders = ', '.join(['?' for _ in fields])
                        field_names = ', '.join(fields)
                        values = [spot_data[field] for field in fields]

                        query = f"INSERT INTO spots ({field_names}) VALUES ({placeholders})"
                        
                        cursor = conn.execute(query, values)
                        imported_count += 1
                        
                        # Progress update every 5000 records
                        if imported_count % 5000 == 0:
                            print(f"📊 Imported {imported_count:,} records (filtered out {filtered_count:,})...")
                        
                    except Exception as insert_error:
                        print(f"❌ INSERT failed for row {row_num}: {insert_error}")
                        print(f"   Bill code: {spot_data.get('bill_code', 'N/A')}")
                        print(f"   Month: {spot_data.get('broadcast_month', 'N/A')}")
                        print(f"   Query: {query}")
                        print(f"   Values: {values[:5]}...")  # Show first 5 values
                        
                        skipped_count += 1
                        
                        # Stop after 5 INSERT errors to avoid spam
                        if skipped_count > 5:
                            raise BroadcastMonthImportError(f"Too many INSERT errors, stopping import")
                        
                        continue
                
                except Exception as row_error:
                    print(f"⚠️  Error processing row {row_num}: {row_error}")
                    skipped_count += 1
                    continue
            
            workbook.close()
            
            print(f"✅ Filtered import completed:")
            print(f"   📊 {imported_count:,} records imported")
            print(f"   ⏭️  {skipped_count:,} records skipped (errors)")
            print(f"   🚫 {filtered_count:,} records filtered out (closed months)")
            
            return imported_count
            
        except Exception as e:
            error_msg = f"Filtered Excel import failed: {str(e)}"
            print(f"❌ {error_msg}")
            raise BroadcastMonthImportError(error_msg)

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
        except Exception as e:
            logger.warning(f"Failed customer lookup for '{customer_name}': {e}")
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
        except Exception as e:
            logger.warning(f"Failed agency lookup for '{agency_name}': {e}")
            return None
    
    def _build_column_mapping(self, header_row) -> Dict[str, int]:
        """
        Build position-based mapping - column names can vary but positions stay constant.
        This is much more reliable than name-based matching.
        """
        print(f"📋 Using position-based column mapping (reliable for consistent Excel structure)")
        
        # Use fixed position mapping
        column_mapping = {}
        
        for position, db_field in EXCEL_COLUMN_POSITIONS.items():
            if db_field is not None:  # Skip ignored columns (like Comments)
                column_mapping[db_field] = position
        
        # Log the mapping for debugging
        print(f"   📊 Mapped {len(column_mapping)} columns using fixed positions")
        
        # Optional: Log any position mismatches for future reference
        if len(header_row) != len(EXCEL_COLUMN_POSITIONS):
            print(f"   ⚠️  Excel has {len(header_row)} columns, expected {len(EXCEL_COLUMN_POSITIONS)}")
        
        return column_mapping
    
    def _extract_spot_data(self, row, column_mapping, batch_id, broadcast_month_display, conn) -> Optional[Dict]:
        """
        Extract spot data using position-based mapping.
        Much more reliable since positions don't change.
        """
        try:
            # Build base spot data
            spot_data = {
                'import_batch_id': batch_id,
                'broadcast_month': broadcast_month_display,
                # load_date is auto-generated by database
            }
            
            # Extract fields using position-based mapping
            for field, position in column_mapping.items():
                if position < len(row) and row[position] is not None:
                    value = row[position]
                    
                    # Skip empty values
                    if value == '' or value is None:
                        continue
                    
                    # Process specific field types
                    if field == 'air_date':
                        if hasattr(value, 'date'):
                            spot_data[field] = value.date().isoformat()
                        else:
                            try:
                                from datetime import datetime
                                if isinstance(value, str):
                                    # Try common date formats
                                    for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%m-%d-%Y']:
                                        try:
                                            parsed_date = datetime.strptime(value.strip(), fmt)
                                            spot_data[field] = parsed_date.date().isoformat()
                                            break
                                        except:
                                            continue
                                    else:
                                        # If no format worked, keep as string
                                        spot_data[field] = str(value)
                                else:
                                    spot_data[field] = str(value)
                            except:
                                spot_data[field] = str(value)
                    
                    elif field == 'end_date':
                        if hasattr(value, 'date'):
                            spot_data[field] = value.date().isoformat()
                        elif value:
                            spot_data[field] = str(value).strip()
                    
                    elif field in ['time_in', 'time_out']:
                        if hasattr(value, 'time'):
                            spot_data[field] = value.time().isoformat()
                        elif value:
                            time_str = str(value).strip()
                            # Handle various time formats
                            if ':' in time_str:
                                spot_data[field] = time_str
                            else:
                                spot_data[field] = time_str
                    
                    elif field in ['gross_rate', 'spot_value', 'broker_fees', 'station_net']:
                        try:
                            # Handle currency formatting
                            if isinstance(value, str):
                                # Remove currency symbols and commas
                                clean_value = value.replace('$', '').replace(',', '').strip()
                                spot_data[field] = float(clean_value) if clean_value else 0.0
                            else:
                                spot_data[field] = float(value) if value else 0.0
                        except:
                            spot_data[field] = 0.0
                    
                    elif field in ['sequence_number', 'line_number', 'priority']:
                        try:
                            spot_data[field] = int(float(value)) if value else None
                        except:
                            spot_data[field] = None
                    
                    elif field in ['agency_flag', 'affidavit_flag']:
                        # Handle Y/N flags
                        if value:
                            val_str = str(value).strip().upper()
                            spot_data[field] = 'Y' if val_str in ['Y', 'YES', '1', 'TRUE'] else 'N'
                    
                    else:
                        # All other fields - convert to string
                        if value:
                            spot_data[field] = str(value).strip()
            
            # REQUIRED FIELDS VALIDATION
            if not spot_data.get('bill_code'):
                print(f"   ⚠️  Skipping row: missing bill_code")
                return None
                
            if not spot_data.get('air_date'):
                print(f"   ⚠️  Skipping row: missing air_date for bill_code {spot_data.get('bill_code', 'unknown')}")
                return None
            
            # Lookup market_id from market_name (position 28)
            if 'market_name' in spot_data and spot_data['market_name']:
                market_id = self._lookup_market_id(spot_data['market_name'], conn)
                if market_id:
                    spot_data['market_id'] = market_id
                # Keep market_name for debugging, will be filtered out later
            
            # Set default language_id if not specified
            if 'language_id' not in spot_data:
                spot_data['language_id'] = 1  # Default to English
            
            # Filter out fields not in actual database schema
            valid_fields = {
                'bill_code', 'air_date', 'end_date', 'day_of_week', 
                'time_in', 'time_out', 'length_seconds', 'media', 'program', 
                'language_code', 'format', 'sequence_number', 'line_number', 
                'spot_type', 'estimate', 'gross_rate', 'make_good', 'spot_value', 
                'broadcast_month', 'broker_fees', 'priority', 'station_net', 
                'sales_person', 'revenue_type', 'billing_type', 'agency_flag', 
                'affidavit_flag', 'contract', 'customer_id', 'agency_id', 
                'market_id', 'language_id', 'source_file', 
                'is_historical', 'effective_date', 'import_batch_id', 'spot_category'
            }
            
            # Only include valid fields
            filtered_spot_data = {k: v for k, v in spot_data.items() if k in valid_fields}
            
            return filtered_spot_data
            
        except Exception as e:
            logger.warning(f"Failed to extract spot data: {e}")
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
            
            logger.debug(f"Marked batch {batch_id} as completed")
            
        except Exception as e:
            logger.error(f"Failed to update batch completion: {e}")
    
    def _fail_import_batch(self, batch_id: str, error_message: str):
        """Mark import batch as failed without nested transaction."""
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