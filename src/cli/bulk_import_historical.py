#!/usr/bin/env python3
"""
Bulk import historical data command.
Imports Excel data and immediately closes all months for historical preservation.
"""

import sys
import argparse
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from services.broadcast_month_import_service import BroadcastMonthImportService, BroadcastMonthImportError
from services.import_integration_utilities import extract_display_months_from_excel, get_excel_import_summary
from utils.broadcast_month_utils import BroadcastMonthParser
from database.connection import DatabaseConnection


def validate_year_match(excel_file: str, expected_year: int) -> tuple[list[str], list[str]]:
    """
    Validate that Excel months match expected year.
    
    Returns:
        Tuple of (matching_months, mismatched_months)
    """
    display_months = list(extract_display_months_from_excel(excel_file))
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


def display_import_preview(excel_file: str, expected_year: int, db_path: str):
    """Display what the historical import would do."""
    print(f"📋 Historical Import Preview")
    print(f"={'='*60}")
    print(f"Excel file: {excel_file}")
    print(f"Expected year: {expected_year}")
    
    try:
        # Get Excel summary
        summary = get_excel_import_summary(excel_file, db_path)
        
        print(f"\n📊 Excel Analysis:")
        print(f"  Months found: {len(summary['months_in_excel'])}")
        print(f"  Existing spots affected: {summary['total_existing_spots_affected']:,}")
        
        # Year validation
        matching_months, mismatched_months = validate_year_match(excel_file, expected_year)
        
        if mismatched_months:
            print(f"\n⚠️  Year Validation Warnings:")
            for month, actual_year in mismatched_months:
                print(f"    {month} → {actual_year} (expected {expected_year})")
            print(f"  Matching months: {len(matching_months)}")
            print(f"  Mismatched months: {len(mismatched_months)}")
        else:
            print(f"  ✅ All months match expected year {expected_year}")
        
        # Show month details
        print(f"\n📅 Month Details:")
        for month_info in summary['month_details']:
            status = "CLOSED" if month_info['is_closed'] else "OPEN"
            spots = month_info['existing_spots']
            revenue = month_info['existing_revenue']
            print(f"  {month_info['month']}: {status} - {spots:,} spots (${revenue:,.2f})")
        
        # Import impact
        print(f"\n🎯 Import Impact:")
        print(f"  • All {len(summary['months_in_excel'])} months will be imported")
        print(f"  • All months will be immediately CLOSED after import")
        print(f"  • {summary['total_existing_spots_affected']:,} existing spots will be replaced")
        
        if summary['closed_months']:
            print(f"  • {len(summary['closed_months'])} months are already closed: {summary['closed_months']}")
        
    except Exception as e:
        print(f"❌ Error generating preview: {e}")


def get_user_confirmation(matching_months: int, mismatched_months: int, total_spots: int) -> bool:
    """Get user confirmation for the import."""
    print(f"\n🚨 CONFIRMATION REQUIRED")
    print(f"This will:")
    print(f"  • Import and REPLACE {total_spots:,} spots")
    print(f"  • PERMANENTLY CLOSE {matching_months} months")
    
    if mismatched_months > 0:
        print(f"  • Import {mismatched_months} months with year mismatches (with warning)")
    
    print(f"\n❗ IMPORTANT: Once closed, these months cannot be reopened!")
    
    while True:
        response = input(f"\nProceed with historical import? (type 'yes' to confirm): ").strip().lower()
        if response == 'yes':
            return True
        elif response in ['no', 'n', '']:
            return False
        else:
            print("Please type 'yes' to confirm or 'no' to cancel")


def main():
    parser = argparse.ArgumentParser(
        description="Import historical data and immediately close all months",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python src/cli/bulk_import_historical.py data/2024_complete.xlsx --year 2024 --closed-by "Kurt"
  python src/cli/bulk_import_historical.py data/2023_data.xlsx --year 2023 --closed-by "Kurt" --notes "Q4 setup"
  python src/cli/bulk_import_historical.py data/2024_complete.xlsx --year 2024 --dry-run
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
    
    try:
        print(f"🚀 Historical Import Tool")
        print(f"Excel file: {args.excel_file}")
        print(f"Expected year: {args.year}")
        print(f"Closed by: {args.closed_by}")
        print(f"Database: {args.db_path}")
        if args.dry_run:
            print(f"Mode: DRY RUN (no changes will be made)")
        print()
        
        # Display preview
        display_import_preview(args.excel_file, args.year, args.db_path)
        
        # Year validation
        matching_months, mismatched_months = validate_year_match(args.excel_file, args.year)
        
        if mismatched_months and not args.force:
            print(f"\n⚠️  Found {len(mismatched_months)} months with year mismatches")
            print(f"Use --force to proceed anyway, or fix the data file")
            if not args.dry_run:
                sys.exit(1)
        
        # Get confirmation unless forced or dry run
        if not args.dry_run and not args.force:
            summary = get_excel_import_summary(args.excel_file, args.db_path)
            confirmed = get_user_confirmation(
                len(matching_months), 
                len(mismatched_months), 
                summary['total_existing_spots_affected']
            )
            
            if not confirmed:
                print(f"❌ Import cancelled by user")
                sys.exit(0)
        
        # Execute the import
        db_connection = DatabaseConnection(args.db_path)
        service = BroadcastMonthImportService(db_connection)
        
        try:
            result = service.execute_month_replacement(
                args.excel_file,
                'HISTORICAL',
                args.closed_by,
                args.dry_run
            )
            
            # Display results
            print(f"\n{'='*60}")
            if args.dry_run:
                print(f"🔍 DRY RUN COMPLETED")
            else:
                print(f"🎉 HISTORICAL IMPORT COMPLETED")
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
                print(f"\n✅ Historical data successfully imported and months closed!")
                print(f"📋 Next steps:")
                print(f"  • Verify data with: uv run python src/cli/close_month.py --list")
                print(f"  • Import next historical year if needed")
            
        finally:
            db_connection.close()
    
    except BroadcastMonthImportError as e:
        print(f"❌ Import error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print(f"\n❌ Import cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()