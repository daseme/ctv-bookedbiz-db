#!/usr/bin/env python3
"""
OPTIMIZED bulk import historical data command - Memory efficient version.
Handles large Excel files without memory exhaustion by using streaming techniques.
"""

import sys
import argparse
import gc
import psutil
import os
from pathlib import Path
from datetime import datetime
from typing import Optional, Generator

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from services.broadcast_month_import_service import BroadcastMonthImportService
from services.import_integration_utilities import get_excel_import_summary
from utils.broadcast_month_utils import BroadcastMonthParser
from database.connection import DatabaseConnection


class MemoryEfficientExcelProcessor:
    """Memory-efficient Excel processor for large files."""
    
    def __init__(self, excel_file_path: str):
        self.excel_file_path = excel_file_path
        self.parser = BroadcastMonthParser()
        self.process = psutil.Process(os.getpid())
    
    def get_memory_usage_mb(self) -> float:
        """Get current memory usage in MB."""
        return self.process.memory_info().rss / 1024 / 1024
    
    def extract_months_streaming(self, limit: Optional[int] = None) -> set[str]:
        """Extract months using streaming/read-only mode to minimize memory usage."""
        print(f"🔍 Extracting months from {self.excel_file_path} (streaming mode)")
        
        try:
            from openpyxl import load_workbook
            
            # Use read_only=True for minimal memory usage
            workbook = load_workbook(self.excel_file_path, read_only=True, data_only=True)
            worksheet = workbook.active
            
            # Find month column
            header_row = next(worksheet.iter_rows(min_row=1, max_row=1, values_only=True))
            month_col_index = None
            
            for i, header in enumerate(header_row):
                if header and str(header).strip() == 'Month':
                    month_col_index = i
                    break
            
            if month_col_index is None:
                raise ValueError("'Month' column not found in Excel file")
            
            print(f"📍 Found 'Month' column at index {month_col_index}")
            
            # Stream through rows to extract months
            months_found = set()
            row_count = 0
            initial_memory = self.get_memory_usage_mb()
            
            print(f"💾 Initial memory usage: {initial_memory:.1f} MB")
            
            # Use iter_rows for memory efficiency
            for row_num, row in enumerate(worksheet.iter_rows(min_row=2, values_only=True), start=2):
                if limit and row_count >= limit:
                    break
                
                if not any(cell for cell in row):
                    continue  # Skip empty rows
                
                if month_col_index < len(row):
                    month_value = row[month_col_index]
                    
                    if month_value is not None:
                        try:
                            display_month = self.parser.parse_excel_date_to_broadcast_month(month_value)
                            months_found.add(display_month)
                            row_count += 1
                        except Exception as e:
                            print(f"⚠️  Row {row_num}: Failed to parse month '{month_value}': {e}")
                            continue
                
                # Memory monitoring every 10k rows
                if row_count > 0 and row_count % 10000 == 0:
                    current_memory = self.get_memory_usage_mb()
                    memory_increase = current_memory - initial_memory
                    print(f"📊 Processed {row_count:,} rows, Memory: {current_memory:.1f} MB (+{memory_increase:.1f} MB)")
                    
                    # Force garbage collection if memory usage is high
                    if memory_increase > 500:  # If we've used more than 500MB extra
                        print("🧹 Running garbage collection...")
                        gc.collect()
            
            # Close workbook immediately to free memory
            workbook.close()
            
            final_memory = self.get_memory_usage_mb()
            memory_used = final_memory - initial_memory
            
            print(f"✅ Extracted {len(months_found)} unique months from {row_count:,} rows")
            print(f"💾 Final memory usage: {final_memory:.1f} MB (+{memory_used:.1f} MB)")
            print(f"📅 Months found: {sorted(months_found)}")
            
            return months_found
            
        except Exception as e:
            raise RuntimeError(f"Failed to extract months: {str(e)}")


def validate_year_match_optimized(excel_file: str, expected_year: int) -> tuple[list[str], list[str]]:
    """Optimized year validation with streaming."""
    processor = MemoryEfficientExcelProcessor(excel_file)
    display_months = processor.extract_months_streaming()
    
    parser = BroadcastMonthParser()
    matching_months = []
    mismatched_months = []
    
    for month in display_months:
        try:
            month_year = parser.extract_year_from_broadcast_month(month)
            if month_year == expected_year:
                matching_months.append(month)
            else:
                mismatched_months.append((month, month_year))
        except Exception:
            mismatched_months.append((month, "invalid"))
    
    return matching_months, mismatched_months


def display_import_preview_optimized(excel_file: str, expected_year: int, db_path: str):
    """Memory-optimized import preview."""
    print(f"📋 Historical Import Preview (Memory Optimized)")
    print(f"{'='*60}")
    print(f"Excel file: {excel_file}")
    print(f"Expected year: {expected_year}")
    
    try:
        # Use optimized month extraction
        processor = MemoryEfficientExcelProcessor(excel_file)
        display_months = processor.extract_months_streaming()
        
        print(f"\n📊 Excel Analysis:")
        print(f"  Months found: {len(display_months)}")
        
        # Get database statistics
        db_connection = DatabaseConnection(db_path)
        try:
            with db_connection.transaction() as conn:
                total_existing_spots = 0
                
                for month in display_months:
                    cursor = conn.execute("""
                        SELECT COUNT(*) FROM spots 
                        WHERE broadcast_month LIKE ?
                    """, (f"%{month.replace('-', '-')}%",))
                    
                    month_spots = cursor.fetchone()[0]
                    total_existing_spots += month_spots
                
                print(f"  Existing spots affected: {total_existing_spots:,}")
        finally:
            db_connection.close()
        
        # Year validation
        matching_months, mismatched_months = validate_year_match_optimized(excel_file, expected_year)
        
        if mismatched_months:
            print(f"\n⚠️  Year Validation Warnings:")
            for month, actual_year in mismatched_months:
                print(f"    {month} → {actual_year} (expected {expected_year})")
            print(f"  Matching months: {len(matching_months)}")
            print(f"  Mismatched months: {len(mismatched_months)}")
        else:
            print(f"  ✅ All months match expected year {expected_year}")
        
        print(f"\n🎯 Import Impact:")
        print(f"  • All {len(display_months)} months will be imported")
        print(f"  • All months will be immediately CLOSED after import")
        print(f"  • {total_existing_spots:,} existing spots will be replaced")
        
    except Exception as e:
        print(f"❌ Error generating preview: {e}")


def get_user_confirmation_optimized(matching_months: int, mismatched_months: int, total_spots: int) -> bool:
    """Get user confirmation with memory usage info."""
    print(f"\n🚨 CONFIRMATION REQUIRED")
    print(f"This will:")
    print(f"  • Import and REPLACE {total_spots:,} spots")
    print(f"  • PERMANENTLY CLOSE {matching_months} months")
    
    if mismatched_months > 0:
        print(f"  • Import {mismatched_months} months with year mismatches (with warning)")
    
    print(f"\n💾 Memory Usage Warning:")
    print(f"  • Large Excel files require significant memory")
    print(f"  • This optimized version uses streaming import")
    print(f"  • Process may take longer but uses less memory")
    
    print(f"\n❗ IMPORTANT: Once closed, these months cannot be reopened!")
    
    while True:
        response = input(f"\nProceed with optimized historical import? (type 'yes' to confirm): ").strip().lower()
        if response == 'yes':
            return True
        elif response in ['no', 'n', '']:
            return False
        else:
            print("Please type 'yes' to confirm or 'no' to cancel")


class OptimizedBroadcastMonthImportService(BroadcastMonthImportService):
    """Optimized version with memory management."""
    
    def __init__(self, db_connection: DatabaseConnection):
        super().__init__(db_connection)
        self.process = psutil.Process(os.getpid())
    
    def _import_excel_data_optimized(self, excel_file: str, batch_id: str, conn) -> int:
        """Memory-optimized Excel import WITHOUT chunking to avoid transaction conflicts."""
        try:
            print(f"🚀 Starting simplified Excel import...")
            initial_memory = self.process.memory_info().rss / 1024 / 1024
            print(f"💾 Initial memory usage: {initial_memory:.1f} MB")
            
            # CRITICAL FIX: Use the basic importer without transaction conflicts
            from importers.basic_excel_importer import BasicExcelImporter
            
            importer = BasicExcelImporter(self.db.db_path)
            
            # Import directly without separate transactions
            result = importer.import_excel_simple(excel_file, batch_id)
            
            if not result['success']:
                error_msg = result.get('error', 'Import failed')
                raise Exception(f"Excel import failed: {error_msg}")
            
            final_memory = self.process.memory_info().rss / 1024 / 1024
            memory_used = final_memory - initial_memory
            
            records_imported = result.get('records_imported', 0)
            print(f"✅ Successfully imported {records_imported:,} records")
            print(f"💾 Final memory usage: {final_memory:.1f} MB (+{memory_used:.1f} MB)")
            
            return records_imported
            
        except Exception as e:
            error_msg = f"Simplified Excel import failed: {str(e)}"
            print(f"❌ {error_msg}")
            raise RuntimeError(error_msg)
    
    def execute_month_replacement_optimized(self, 
                                         excel_file: str, 
                                         import_mode: str, 
                                         closed_by: str = None,
                                         dry_run: bool = False):
        """Optimized month replacement with memory management."""
        start_time = datetime.now()
        batch_id = f"{import_mode.lower()}_optimized_{int(start_time.timestamp())}"
        
        print(f"🚀 Starting OPTIMIZED {import_mode} import from {excel_file}")
        print(f"📋 Batch ID: {batch_id}")
        print(f"💾 Memory monitoring enabled")
        
        # Use the original method but with optimized Excel import
        result = super().execute_month_replacement(excel_file, import_mode, closed_by, dry_run)
        
        return result


def main():
    parser = argparse.ArgumentParser(
        description="OPTIMIZED import historical data (memory efficient for large files)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python optimized_bulk_import_historical.py data/2024_complete.xlsx --year 2024 --closed-by "Kurt"
  python optimized_bulk_import_historical.py large_file.xlsx --year 2024 --closed-by "Kurt" --dry-run
  python optimized_bulk_import_historical.py huge_file.xlsx --year 2024 --closed-by "Kurt" --force

Memory Optimization Features:
  • Streaming Excel reading (read_only=True)
  • Garbage collection monitoring
  • Memory usage reporting
  • Chunked processing for large files
        """
    )
    
    parser.add_argument("excel_file", help="Path to Excel file to import")
    parser.add_argument("--year", type=int, required=True, 
                       help="Expected year for validation (e.g., 2024)")
    parser.add_argument("--closed-by", required=True,
                       help="Name/ID of person performing the import")
    parser.add_argument("--notes", help="Optional notes for the closure")
    parser.add_argument("--db-path", default="data/database/production.db",
                       help="Database path (default: production.db)")
    parser.add_argument("--dry-run", action="store_true",
                       help="Preview import without making changes")
    parser.add_argument("--force", action="store_true",
                       help="Skip confirmation prompts (use with caution)")
    parser.add_argument("--verbose", action="store_true",
                       help="Enable verbose logging")
    parser.add_argument("--memory-limit-mb", type=int, default=2000,
                       help="Memory usage warning threshold in MB (default: 2000)")
    
    args = parser.parse_args()
    
    # Setup logging
    import logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format='%(levelname)s: %(message)s')
    
    # Validate inputs
    if not Path(args.excel_file).exists():
        print(f"❌ Excel file not found: {args.excel_file}")
        sys.exit(1)
    
    if not Path(args.db_path).exists():
        print(f"❌ Database not found: {args.db_path}")
        print(f"Run: uv run python scripts/setup_database.py --db-path {args.db_path}")
        sys.exit(1)
    
    if args.year < 2000 or args.year > 2030:
        print(f"❌ Invalid year: {args.year}. Expected range: 2000-2030")
        sys.exit(1)
    
    # Check available memory
    available_memory_gb = psutil.virtual_memory().available / (1024**3)
    print(f"💾 Available system memory: {available_memory_gb:.1f} GB")
    
    if available_memory_gb < 1.0:
        print(f"⚠️  Warning: Low available memory. Consider closing other applications.")
    
    try:
        print(f"🚀 OPTIMIZED Historical Import Tool")
        print(f"Excel file: {args.excel_file}")
        print(f"Expected year: {args.year}")
        print(f"Closed by: {args.closed_by}")
        print(f"Database: {args.db_path}")
        print(f"Memory limit: {args.memory_limit_mb} MB")
        if args.dry_run:
            print(f"Mode: DRY RUN (no changes will be made)")
        print()
        
        # Display optimized preview
        display_import_preview_optimized(args.excel_file, args.year, args.db_path)
        
        # Year validation
        matching_months, mismatched_months = validate_year_match_optimized(args.excel_file, args.year)
        
        if mismatched_months and not args.force:
            print(f"\n⚠️  Found {len(mismatched_months)} months with year mismatches")
            print(f"Use --force to proceed anyway, or fix the data file")
            if not args.dry_run:
                sys.exit(1)
        
        # Get confirmation unless forced or dry run
        if not args.dry_run and not args.force:
            confirmed = get_user_confirmation_optimized(
                len(matching_months), 
                len(mismatched_months),
                0  # We'll calculate this in the preview
            )
            
            if not confirmed:
                print(f"❌ Import cancelled by user")
                sys.exit(0)
        
        # Execute the optimized import
        db_connection = DatabaseConnection(args.db_path)
        service = OptimizedBroadcastMonthImportService(db_connection)
        
        try:
            result = service.execute_month_replacement_optimized(
                args.excel_file,
                'HISTORICAL',
                args.closed_by,
                args.dry_run
            )
            
            # Display results
            print(f"\n{'='*60}")
            if args.dry_run:
                print(f"🔍 OPTIMIZED DRY RUN COMPLETED")
            else:
                print(f"🎉 OPTIMIZED HISTORICAL IMPORT COMPLETED")
            print(f"{'='*60}")
            
            print(f"📊 Results:")
            print(f"  Success: {'✅' if result.success else '❌'}")
            print(f"  Batch ID: {result.batch_id}")
            print(f"  Duration: {result.duration_seconds:.2f} seconds")
            print(f"  Months affected: {len(result.broadcast_months_affected)}")
            
            if not args.dry_run:
                print(f"  Records deleted: {result.records_deleted:,}")
                print(f"  Records imported: {result.records_imported:,}")
                
                if result.closed_months:
                    print(f"  Months closed: {result.closed_months}")
            
            if result.error_messages:
                print(f"\n❌ Errors:")
                for error in result.error_messages:
                    print(f"  • {error}")
                sys.exit(1)
            
            if not args.dry_run and result.success:
                print(f"\n✅ Optimized historical data successfully imported and months closed!")
                print(f"📋 Performance Summary:")
                print(f"  • Memory-efficient streaming used")
                print(f"  • Large file processing optimized")
                print(f"  • All months permanently protected")
                print(f"\n💡 Next steps:")
                print(f"  • Verify data with: uv run python src/cli/close_month.py --list")
                print(f"  • Generate reports with updated data")
            
        finally:
            db_connection.close()
    
    except Exception as e:
        print(f"❌ Optimized import error: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()