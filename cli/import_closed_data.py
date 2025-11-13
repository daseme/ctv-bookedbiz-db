#!/usr/bin/env python3
"""
Production Sales Data Import Tool

Imports sales data from Excel files into SQLite database with automatic market setup
and month closure protection. Supports both historical imports (replaces all data)
and weekly updates (skips closed months).

CORE FUNCTIONALITY:
- Imports Excel data using fixed column positions (29 columns expected)
- Two import modes: HISTORICAL (processes all months) or WEEKLY_UPDATE (skips closed months)
- Auto-creates missing markets and schedule assignments from Excel data
- Uses language data directly from Excel Language column
- Closes imported months permanently to prevent future modifications
- Progress tracking with tqdm progress bars

COMMAND LINE OPTIONS:
- excel_file         - Path to Excel file (required)
- --year            - Expected year for validation (required)
- --closed-by       - Name/ID of person performing import (required)
- --skip-closed     - Skip closed months (WEEKLY_UPDATE mode), default processes all (HISTORICAL mode)
- --auto-setup      - Auto-create missing markets and schedule assignments
- --dry-run         - Preview changes without executing import
- --force           - Skip confirmation prompts
- --verbose         - Enable detailed logging
- --db-path         - Database path (default: data/database/production.db)

IMPORT MODES:
- HISTORICAL: Processes all months, replaces existing data, closes open months
- WEEKLY_UPDATE: Only processes open months, skips closed months, leaves months open

USAGE EXAMPLES:
# Historical import (processes all months, closes them)
python import_closed_data.py data/2025-complete.xlsx --year 2025 --closed-by "Kurt"

# Weekly update (skips closed months)
python import_closed_data.py data/current.xlsx --year 2025 --closed-by "Kurt" --skip-closed

# Initial setup with market creation
python import_closed_data.py data/2024-annual.xlsx --year 2024 --closed-by "Kurt" --auto-setup

# Preview without changes
python import_closed_data.py data/march.xlsx --year 2025 --closed-by "Kurt" --dry-run

# Unattended execution
python import_closed_data.py data/data.xlsx --year 2025 --closed-by "System" --force

EXCEL FILE REQUIREMENTS:
- 29 columns in fixed positions (see EXCEL_COLUMN_POSITIONS in broadcast_month_import_service.py)
- Header row required
- Market, air date, and broadcast month columns required
- Language column used directly (column 9)

DEPENDENCIES: openpyxl, tqdm, psutil, sqlite3
"""

import sys
import argparse
import gc
import psutil
import os
from pathlib import Path
from datetime import datetime
from typing import Optional, Generator, Set, Dict, List, Tuple
import pandas as pd
import sqlite3

# Add tqdm for progress bars
from tqdm import tqdm

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))  # Add cli dir
sys.path.insert(0, str(Path(__file__).parent.parent))  # Add root dir
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))  # Add src dir

from src.services.broadcast_month_import_service import BroadcastMonthImportService
from src.services.import_integration_utilities import get_excel_import_summary
from src.utils.broadcast_month_utils import BroadcastMonthParser
from src.database.connection import DatabaseConnection


class EnhancedMarketSetupManager:
    """Enhanced market setup manager with tqdm progress bars."""

    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection
        self.parser = BroadcastMonthParser()

    def scan_excel_for_markets(self, excel_file: str) -> Dict[str, Dict]:
        """
        Scan Excel file to extract all market codes with tqdm progress bar.
        """
        print(f"🔍 Scanning Excel file for market codes: {excel_file}")

        try:
            from src.services.import_integration_utilities import get_excel_worksheet_flexible

            worksheet, sheet_name, workbook = get_excel_worksheet_flexible(excel_file)
            print(f"📄 Market scan using sheet: {sheet_name}")

            # Find required columns
            header_row = next(
                worksheet.iter_rows(min_row=1, max_row=1, values_only=True)
            )
            market_col_index = None
            air_date_col_index = None

            for i, header in enumerate(header_row):
                if header:
                    header_str = str(header).strip().lower()
                    if header_str in [
                        "market_name",
                        "market",
                        "market_code",
                        "market name",
                    ]:
                        market_col_index = i
                    elif header_str in [
                        "start date",
                        "air date",
                        "date",
                        "airdate",
                        "air_date",
                    ]:
                        air_date_col_index = i

            if market_col_index is None:
                raise ValueError("Market column not found in Excel file")
            if air_date_col_index is None:
                raise ValueError("Air date column not found in Excel file")

            print(f"📍 Found Market column at index {market_col_index}")
            print(f"📍 Found Air Date column at index {air_date_col_index}")

            # Get total row count for tqdm
            total_rows = worksheet.max_row - 1  # Subtract header row

            markets_data = {}

            # Use tqdm for progress tracking
            with tqdm(
                total=total_rows, desc="📊 Scanning Excel rows", unit=" rows"
            ) as pbar:
                for row_num, row in enumerate(
                    worksheet.iter_rows(min_row=2, values_only=True), start=2
                ):
                    pbar.update(1)

                    if not any(cell for cell in row):
                        continue

                    if market_col_index < len(row) and air_date_col_index < len(row):
                        market_value = row[market_col_index]
                        air_date_value = row[air_date_col_index]

                        if market_value and air_date_value:
                            market_code = str(market_value).strip()

                            # Parse air date
                            try:
                                if isinstance(air_date_value, datetime):
                                    air_date = air_date_value.date()
                                else:
                                    air_date = datetime.strptime(
                                        str(air_date_value), "%Y-%m-%d"
                                    ).date()
                            except:
                                continue

                            # Track market data
                            if market_code not in markets_data:
                                markets_data[market_code] = {
                                    "earliest_date": air_date,
                                    "latest_date": air_date,
                                    "spot_count": 0,
                                }
                            else:
                                if (
                                    air_date
                                    < markets_data[market_code]["earliest_date"]
                                ):
                                    markets_data[market_code]["earliest_date"] = (
                                        air_date
                                    )
                                if air_date > markets_data[market_code]["latest_date"]:
                                    markets_data[market_code]["latest_date"] = air_date

                            markets_data[market_code]["spot_count"] += 1

                    # Update progress description periodically
                    if row_num % 5000 == 0:
                        pbar.set_description(
                            f"📊 Scanning ({len(markets_data)} markets found)"
                        )

            workbook.close()

            print(f"\n✅ Excel scan complete:")
            print(f"   📊 {total_rows:,} total spots analyzed")
            print(f"   🎯 {len(markets_data)} unique markets found")

            # Display market summary
            print(f"\n📋 Market Summary:")
            for market_code, data in tqdm(
                sorted(markets_data.items()), desc="📋 Displaying markets", leave=False
            ):
                print(
                    f"   📋 {market_code}: {data['spot_count']:,} spots ({data['earliest_date']} to {data['latest_date']})"
                )

            return markets_data

        except Exception as e:
            raise RuntimeError(f"Failed to scan Excel for markets: {str(e)}")

    def create_missing_markets(self, excel_markets: Dict[str, Dict]) -> Dict[str, int]:
        """Create missing markets with tqdm progress bar."""
        existing_markets = self.get_existing_markets()
        missing_markets = set(excel_markets.keys()) - set(existing_markets.keys())

        if not missing_markets:
            print("✅ All markets already exist in database")
            return existing_markets

        print(f"🏗️  Creating {len(missing_markets)} missing markets...")

        with self.db.transaction() as conn:
            # Use tqdm for market creation
            for market_code in tqdm(
                sorted(missing_markets), desc="🏗️  Creating markets", unit=" markets"
            ):
                market_name = self._generate_market_name(market_code)

                cursor = conn.execute(
                    """
                    INSERT INTO markets (market_code, market_name) 
                    VALUES (?, ?)
                """,
                    (market_code, market_name),
                )

                market_id = cursor.lastrowid
                existing_markets[market_code] = market_id

                tqdm.write(
                    f"   ✅ Created market: {market_code} ({market_name}) - ID: {market_id}"
                )

        print(f"✅ Market creation complete")
        return existing_markets

    def setup_schedule_assignments(
        self, excel_markets: Dict[str, Dict], market_mapping: Dict[str, int]
    ) -> int:
        """Create schedule assignments with tqdm progress bar."""
        markets_with_schedules = self.get_markets_with_schedules()
        assignments_created = 0

        print(f"🗓️  Setting up schedule assignments...")

        # Filter markets that need assignments
        markets_needing_assignments = [
            (market_code, market_data)
            for market_code, market_data in excel_markets.items()
            if market_mapping[market_code] not in markets_with_schedules
        ]

        if not markets_needing_assignments:
            print("✅ All markets already have schedule assignments")
            return 0

        with self.db.transaction() as conn:
            # Use tqdm for schedule assignments
            for market_code, market_data in tqdm(
                markets_needing_assignments,
                desc="🗓️  Creating assignments",
                unit=" assignments",
            ):
                market_id = market_mapping[market_code]
                earliest_date = market_data["earliest_date"]

                cursor = conn.execute(
                    """
                    INSERT INTO schedule_market_assignments 
                    (schedule_id, market_id, effective_start_date, assignment_priority)
                    VALUES (1, ?, ?, 1)
                """,
                    (market_id, earliest_date),
                )

                assignments_created += 1
                tqdm.write(
                    f"   ✅ {market_code}: Created schedule assignment (effective from {earliest_date})"
                )

        print(
            f"✅ Schedule assignment setup complete: {assignments_created} assignments created"
        )
        return assignments_created

    def execute_market_setup(self, excel_file: str) -> Dict:
        """Execute complete market setup process with enhanced progress tracking."""
        print(f"🚀 Starting automatic market setup...")
        start_time = datetime.now()

        # Step 1: Scan Excel for markets
        excel_markets = self.scan_excel_for_markets(excel_file)

        # Step 2: Create missing markets
        market_mapping = self.create_missing_markets(excel_markets)

        # Step 3: Setup schedule assignments
        assignments_created = self.setup_schedule_assignments(
            excel_markets, market_mapping
        )

        # Step 4: Update existing schedule dates if needed (placeholder)
        dates_updated = 0  # Not implemented yet

        duration = (datetime.now() - start_time).total_seconds()

        summary = {
            "markets_found": len(excel_markets),
            "markets_created": len(
                [
                    m
                    for m in excel_markets.keys()
                    if m not in self.get_existing_markets()
                ]
            ),
            "assignments_created": assignments_created,
            "dates_updated": dates_updated,
            "duration_seconds": duration,
            "excel_markets": excel_markets,
        }

        print(f"✅ Market setup complete in {duration:.2f} seconds!")
        return summary

    def get_existing_markets(self) -> Dict[str, int]:
        """Get existing markets from database."""
        with self.db.transaction() as conn:
            cursor = conn.execute("SELECT market_code, market_id FROM markets")
            return {row[0]: row[1] for row in cursor.fetchall()}

    def get_markets_with_schedules(self) -> Set[int]:
        """Get market IDs that already have schedule assignments."""
        with self.db.transaction() as conn:
            cursor = conn.execute(
                "SELECT DISTINCT market_id FROM schedule_market_assignments"
            )
            return {row[0] for row in cursor.fetchall()}

    def _generate_market_name(self, market_code: str) -> str:
        """Generate a proper market name from market code."""
        name_mappings = {
            "NYC": "NEW YORK",
            "LAX": "LOS ANGELES",
            "SFO": "SAN FRANCISCO",
            "SEA": "SEATTLE",
            "CHI": "CHICAGO",
            "MSP": "MINNEAPOLIS",
            "DAL": "DALLAS",
            "HOU": "HOUSTON",
            "WDC": "WASHINGTON DC",
            "CVC": "CENTRAL VALLEY",
            "CMP": "CHI MSP",
            "MMT": "MAMMOTH",
            "ADMIN": "ADMINISTRATIVE",
        }

        return name_mappings.get(market_code, market_code.upper().replace("_", " "))


class SimpleHistoricalImporter:
    """Simple historical importer - no complex language processing needed."""

    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection
        self.market_manager = EnhancedMarketSetupManager(db_connection)
        self.import_service = BroadcastMonthImportService(db_connection)
        self.process = psutil.Process(os.getpid())

    def _ensure_bill_codes_in_raw_inputs(self, excel_file: str) -> None:
        """
        Ensure all bill_code values from Excel are added to raw_customer_inputs
        This prevents normalization gaps from occurring.
        """
        try:
            import pandas as pd
            
            # Try to read the main data sheet - handle multiple possible sheet names
            sheet_names_to_try = ["Commercials", "Commercial Lines", "Sheet1", 0]  # 0 = first sheet
            df = None
            sheet_used = None
            
            for sheet_name in sheet_names_to_try:
                try:
                    df = pd.read_excel(excel_file, sheet_name=sheet_name)
                    sheet_used = sheet_name
                    print(f"   📄 Reading bill codes from sheet: {sheet_name}")
                    break
                except Exception:
                    continue
            
            if df is None:
                print("   ⚠️ Warning: Could not read any sheet from Excel file")
                return

            # Get unique bill codes - try multiple possible column names
            bill_code_columns = ['bill_code', 'Bill Code', 'Customer', 'Client', 'Advertiser', 'customer']
            bill_codes = []
            column_used = None
            
            for col in bill_code_columns:
                if col in df.columns:
                    bill_codes = df[col].dropna().unique().tolist()
                    column_used = col
                    break
            
            if not bill_codes:
                print("   ⚠️ Warning: No bill code column found in Excel file")
                available_columns = list(df.columns)[:10]  # Show first 10 columns
                print(f"   Available columns: {available_columns}")
                return
            
            # Clean and filter bill codes
            clean_bill_codes = []
            for bill_code in bill_codes:
                if bill_code and str(bill_code).strip() and str(bill_code).strip().upper() != 'NAN':
                    clean_bill_codes.append(str(bill_code).strip())
            
            if clean_bill_codes:
                # Add to raw_customer_inputs using tqdm for progress
                current_time = datetime.now().isoformat()
                added_count = 0
                
                with self.db.transaction() as conn:
                    with tqdm(clean_bill_codes, desc="   🔧 Adding bill codes", unit=" codes") as pbar:
                        for bill_code in pbar:
                            try:
                                cursor = conn.execute("""
                                    INSERT OR IGNORE INTO raw_customer_inputs (raw_text, created_at)
                                    VALUES (?, ?)
                                """, (bill_code, current_time))
                                
                                if cursor.rowcount > 0:
                                    added_count += 1
                                    
                            except Exception as e:
                                # Continue with other bill codes if one fails
                                continue
                
                print(f"   ✅ Normalization system updated:")
                print(f"      Sheet: {sheet_used}, Column: {column_used}")
                print(f"      Total bill codes found: {len(clean_bill_codes):,}")
                print(f"      New bill codes added: {added_count:,}")
                
                if added_count == 0:
                    print(f"      (All bill codes were already in system)")
            else:
                print("   ⚠️ Warning: No valid bill codes found after cleaning")
        
        except Exception as e:
            print(f"   ⚠️ Warning: Could not update raw_customer_inputs: {e}")
            # Don't fail the entire import if this step fails

    def execute_simple_import(
        self,
        excel_file: str,
        expected_year: int,
        closed_by: str,
        auto_setup_markets: bool = True,
        dry_run: bool = False,
        skip_closed_months: bool = False,
    ) -> Dict:
        """
        Execute simple historical import - language comes directly from Excel.
        """
        start_time = datetime.now()
        batch_id = f"simple_historical_{int(start_time.timestamp())}"

        print(f"Production Sales Data Import Starting...")
        print(f"Excel file: {excel_file}")
        print(f"Expected year: {expected_year}")
        print(f"Closed by: {closed_by}")
        print(f"Auto-setup markets: {auto_setup_markets}")
        print(f"Dry run: {dry_run}")
        print(f"Batch ID: {batch_id}")
        print("=" * 70)

        results = {
            "success": False,
            "batch_id": batch_id,
            "market_setup": None,
            "import_result": None,
            "duration_seconds": 0,
            "error_messages": [],
        }

        try:
            # Step 1: Market setup (if enabled)
            if auto_setup_markets and not dry_run:
                print(f"🏗️  STEP 1: Automatic Market Setup")
                market_setup_result = self.market_manager.execute_market_setup(
                    excel_file
                )
                results["market_setup"] = market_setup_result

                print(f"📊 Market Setup Results:")
                print(
                    f"   🎯 Markets found in Excel: {market_setup_result['markets_found']}"
                )
                print(
                    f"   🏗️  New markets created: {market_setup_result.get('markets_created', 0)}"
                )
                print(
                    f"   🗓️  Schedule assignments created: {market_setup_result['assignments_created']}"
                )
                print(
                    f"   📅 Schedule dates updated: {market_setup_result['dates_updated']}"
                )
                print()

            # Step 1.5: CRITICAL FIX - Update normalization system
            if not dry_run:
                print(f"🔧 STEP 1.5: Updating normalization system with new bill codes...")
                self._ensure_bill_codes_in_raw_inputs(excel_file)
                print()

            # Step 2: Historical data import (includes language from Excel)
            print(f"📦 STEP 2: Historical Data Import")
            print(
                f"   🔤 Language data will be read directly from Excel Language column"
            )

            if dry_run:
                print(f"🔍 DRY RUN - No changes would be made")
                summary = get_excel_import_summary(excel_file, self.db.db_path)
                results["import_result"] = {
                    "would_import": True,
                    "months_found": len(summary["months_in_excel"]),
                    "total_spots": summary["total_existing_spots_affected"],
                }
            else:
                # Execute actual import with progress tracking
                # The BroadcastMonthImportService already handles language_code from Excel
                import_mode = "WEEKLY_UPDATE" if skip_closed_months else "HISTORICAL"
                import_result = self.import_service.execute_month_replacement(
                    excel_file,
                    import_mode,  # Use the variable
                    closed_by,
                    dry_run=False,
                )
                results["import_result"] = import_result

            results["success"] = True

        except Exception as e:
            error_msg = f"Production import failed: {str(e)}"
            results["error_messages"].append(error_msg)
            print(f"❌ {error_msg}")

        # Calculate total duration
        results["duration_seconds"] = (datetime.now() - start_time).total_seconds()

        return results


def display_production_preview(
    excel_file: str,
    expected_year: int,
    db_path: str,
    auto_setup: bool,
    skip_closed: bool = False,
):
    """Display comprehensive preview."""
    print(f"Production Import Preview")
    print(f"=" * 70)
    print(f"Excel file: {excel_file}")
    print(f"Expected year: {expected_year}")
    print(f"Auto-setup markets: {auto_setup}")
    print()

    try:
        db_connection = DatabaseConnection(db_path)

        # Preview with progress estimation
        print("🔍 Analyzing Excel file for preview...")

        if auto_setup:
            # Market setup preview
            market_manager = EnhancedMarketSetupManager(db_connection)

            # Quick scan for preview (flexible sheet selection)
            from src.services.import_integration_utilities import get_excel_worksheet_flexible
            worksheet, sheet_name, workbook = get_excel_worksheet_flexible(excel_file)
            print(f"📄 Preview using sheet: {sheet_name}")

            total_rows = max(0, (worksheet.max_row or 1) - 1)  # exclude header
            print(f"📊 File contains ~{total_rows:,} rows to analyze")

            # Quick market analysis
            excel_markets = market_manager.scan_excel_for_markets(excel_file)
            existing_markets = market_manager.get_existing_markets()
            missing_markets = set(excel_markets.keys()) - set(existing_markets.keys())

            print(f"🏗️  Market Setup Preview:")
            print(f"   📊 Markets in Excel: {len(excel_markets)}")
            print(f"   ✅ Already exist: {len(existing_markets)}")
            print(f"   🏗️  Will create: {len(missing_markets)}")
            if missing_markets:
                print(f"   📋 New markets to create: {sorted(missing_markets)}")
            print()

            workbook.close()

        # Show import preview
        summary = get_excel_import_summary(excel_file, db_path)

        print(f"📦 Import Preview:")
        print(f"   📅 Months found: {len(summary['months_in_excel'])}")
        print(f"   📊 Existing DB records to replace: {summary['total_existing_spots_affected']:,}")
        print(f"   📄 Rows in Excel (est.): {summary.get('total_rows_in_excel', 0):,}")
        print(f"   🔒 Closed months: {len(summary['closed_months'])}")
        print(f"   📂 Open months: {len(summary['open_months'])}")

        if skip_closed:
            print(f"   🎯 Mode: WEEKLY_UPDATE (will skip {len(summary['closed_months'])} closed months)")
        else:
            print(f"   🎯 Mode: HISTORICAL (will process all months, replace existing data)")

        print("")
        print(f"🔤 Language Handling:")
        print(f"   📋 Language data will be read directly from Excel Language column")
        print(f"   🎯 No complex processing needed - simple and reliable")
        print(f"   ✅ Language assignments handled automatically during import")

        db_connection.close()

    except Exception as e:
        print(f"❌ Error generating preview: {e}")

def main():
    """Simple main function - language comes from Excel directly."""
    parser = argparse.ArgumentParser(
        description="Production Sales Data Import Tool - SIMPLE VERSION",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
 uv run python ./cli/import_closed_data.py data/raw/2025.xlsx --year 2
025 --closed-by "Kurt"
  python import_closed_data.py data/historical/2024-complete.xlsx --year 2024 --closed-by "Kurt" --auto-setup
  python import_closed_data.py data/March-2025.xlsx --year 2025 --closed-by "Kurt" --dry-run

Features:
  - Real-time progress tracking
  - Automatic market detection and creation  
  - Uses language column directly from Excel (simple and reliable)
  - Permanent month closure protection
  - Comprehensive audit trail
        """,
    )

    parser.add_argument("excel_file", help="Path to Excel file to import")
    parser.add_argument(
        "--year", type=int, required=True, help="Expected year for validation"
    )
    parser.add_argument(
        "--closed-by", required=True, help="Name/ID of person performing the import"
    )
    parser.add_argument(
        "--skip-closed",
        action="store_true",
        help="Skip already closed months (use WEEKLY_UPDATE mode instead of HISTORICAL)",
    )
    parser.add_argument(
        "--auto-setup",
        action="store_true",
        help="Automatically create missing markets and schedule assignments",
    )
    parser.add_argument(
        "--db-path", default="data/database/production.db", help="Database path"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Preview import without making changes"
    )
    parser.add_argument(
        "--force", action="store_true", help="Skip confirmation prompts"
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")

    args = parser.parse_args()

    # Validation
    excel_path = Path(args.excel_file)
    if not excel_path.exists():
        print(f"❌ Excel file not found: {args.excel_file}")
        sys.exit(1)

    db_path = Path(args.db_path)
    if not db_path.exists():
        print(f"❌ Database not found: {args.db_path}")
        sys.exit(1)

    try:
        print(f"Production Sales Data Import Tool")
        print(f"=" * 60)

        # Display production preview
        display_production_preview(
            args.excel_file, args.year, args.db_path, args.auto_setup, args.skip_closed
        )

        # Get confirmation unless forced or dry run
        if not args.dry_run and not args.force:
            print(f"\n🚨 CONFIRMATION REQUIRED")
            action_list = [
                "Import historical data and close months",
                "Use language data directly from Excel",
            ]
            if args.auto_setup:
                action_list.insert(0, "Create missing markets and schedule assignments")

            print(f"This will:")
            for i, action in enumerate(action_list, 1):
                print(f"  {i}. {action}")

            response = (
                input(f"\nProceed with import? (type 'yes' to confirm): ")
                .strip()
                .lower()
            )
            if response != "yes":
                print(f"❌ Import cancelled by user")
                sys.exit(0)

        # Execute simple import
        db_connection = DatabaseConnection(args.db_path)
        importer = SimpleHistoricalImporter(db_connection)

        try:
            results = importer.execute_simple_import(
                args.excel_file,
                args.year,
                args.closed_by,
                args.auto_setup,
                args.dry_run,
                args.skip_closed,  # Add this line
            )

            # Display results
            print(f"\n" + "=" * 70)
            print(f"PRODUCTION IMPORT {'PREVIEW' if args.dry_run else 'COMPLETED'}")
            print(f"=" * 70)

            print(f"📊 Overall Results:")
            print(f"  Success: {'✅' if results['success'] else '❌'}")
            print(f"  Duration: {results['duration_seconds']:.2f} seconds")
            print(f"  Batch ID: {results['batch_id']}")

            # Market setup results
            if results["market_setup"]:
                setup = results["market_setup"]
                print(f"\n🏗️  Market Setup Results:")
                print(f"  Markets found: {setup['markets_found']}")
                print(f"  New markets created: {setup.get('markets_created', 0)}")
                print(f"  Schedule assignments created: {setup['assignments_created']}")
                print(f"  Schedule dates updated: {setup['dates_updated']}")

            # Import results
            if results["import_result"]:
                import_res = results["import_result"]
                print(f"\n📦 Import Results:")
                if not args.dry_run and hasattr(import_res, "success"):
                    print(f"  Records deleted: {import_res.records_deleted:,}")
                    print(f"  Records imported: {import_res.records_imported:,}")
                    print(
                        f"  Months affected: {len(import_res.broadcast_months_affected)}"
                    )
                    if (
                        hasattr(import_res, "closed_months")
                        and import_res.closed_months
                    ):
                        print(f"  Months closed: {import_res.closed_months}")
                else:
                    print(f"  Would import: {import_res.get('total_spots', 0):,} spots")
                    print(f"  Months found: {import_res.get('months_found', 0)}")

            if results["error_messages"]:
                print(f"\n❌ Errors:")
                for error in results["error_messages"]:
                    print(f"  • {error}")
                sys.exit(1)

            if results["success"] and not args.dry_run:
                print(f"\nClosed data import completed successfully.")
                print(f"Next steps:")
                print(f"  - All closed data is imported and protected")
                print(f"  - Markets and schedules are configured")
                print(f"  - Language data imported directly from Excel")
                print(f"  - Database is ready for reporting and analysis")

        finally:
            db_connection.close()

    except Exception as e:
        print(f"❌ Production import error: {e}")
        if args.verbose:
            import traceback

            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
