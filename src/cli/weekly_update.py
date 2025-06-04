#!/usr/bin/env python3
"""
Weekly update command for standard booked business imports.
Replaces open month data while protecting closed historical months.
"""

import sys
import argparse
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from services.broadcast_month_import_service import BroadcastMonthImportService, BroadcastMonthImportError
from services.import_integration_utilities import get_excel_import_summary, validate_excel_for_import
from database.connection import DatabaseConnection


def display_weekly_update_preview(excel_file: str, db_path: str):
    """Display what the weekly update would do."""
    print(f"📋 Weekly Update Preview")
    print(f"={'='*60}")
    print(f"Excel file: {excel_file}")
    
    try:
        # Get Excel summary
        summary = get_excel_import_summary(excel_file, db_path)
        
        print(f"\n📊 Excel Analysis:")
        print(f"  Months found: {len(summary['months_in_excel'])}")
        print(f"  Total existing spots: {summary['total_existing_spots_affected']:,}")
        
        # Validation check
        validation = validate_excel_for_import(excel_file, 'WEEKLY_UPDATE', db_path)
        
        if validation.is_valid:
            print(f"  ✅ Weekly update allowed")
        else:
            print(f"  ❌ Weekly update BLOCKED")
            print(f"     Reason: {validation.error_message}")
            print(f"     Solution: {validation.suggested_action}")
            return False
        
        # Show month details
        print(f"\n📅 Month Details:")
        for month_info in summary['month_details']:
            status = "CLOSED" if month_info['is_closed'] else "OPEN"
            spots = month_info['existing_spots']
            revenue = month_info['existing_revenue']
            status_icon = "🔒" if month_info['is_closed'] else "📂"
            print(f"  {status_icon} {month_info['month']}: {status} - {spots:,} spots (${revenue:,.2f})")
        
        # Import impact
        open_months = summary['open_months']
        print(f"\n🎯 Import Impact:")
        print(f"  • {len(open_months)} open months will be updated")
        print(f"  • {summary['total_existing_spots_affected']:,} existing spots will be replaced")
        print(f"  • Closed months will be protected (no changes)")
        
        if summary['closed_months']:
            print(f"  • {len(summary['closed_months'])} closed months: {summary['closed_months']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error generating preview: {e}")
        return False


def get_user_confirmation(total_spots: int, open_months: int, force: bool = False) -> bool:
    """Get user confirmation for the update."""
    if force:
        return True
    
    print(f"\n🚨 CONFIRMATION REQUIRED")
    print(f"This will:")
    print(f"  • REPLACE {total_spots:,} existing spots")
    print(f"  • Update {open_months} open months")
    print(f"  • Preserve all closed/historical months")
    
    while True:
        response = input(f"\nProceed with weekly update? (yes/no): ").strip().lower()
        if response in ['yes', 'y']:
            return True
        elif response in ['no', 'n', '']:
            return False
        else:
            print("Please enter 'yes' or 'no'")


def main():
    parser = argparse.ArgumentParser(
        description="Weekly update for booked business data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python src/cli/weekly_update.py data/booked_business_current.xlsx
  python src/cli/weekly_update.py data/weekly_data.xlsx --dry-run
  python src/cli/weekly_update.py data/weekly_data.xlsx --force
        """
    )
    
    # Define arguments - NO year or closed-by required for weekly updates
    parser.add_argument("excel_file", help="Path to Excel file to import")
    parser.add_argument("--db-path", default="data/database/production.db",
                       help="Database path (default: production.db)")
    parser.add_argument("--dry-run", action="store_true",
                       help="Preview import without making changes")
    parser.add_argument("--force", action="store_true",
                       help="Skip confirmation prompts")
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
    
    try:
        print(f"🔄 Weekly Update Tool")
        print(f"Excel file: {args.excel_file}")
        print(f"Database: {args.db_path}")
        if args.dry_run:
            print(f"Mode: DRY RUN (no changes will be made)")
        print()
        
        # Display preview and validate
        can_proceed = display_weekly_update_preview(args.excel_file, args.db_path)
        
        if not can_proceed:
            print(f"\n❌ Weekly update cannot proceed due to validation errors")
            print(f"💡 Common solutions:")
            print(f"  • Remove closed month data from Excel file")
            print(f"  • Use historical import mode for closed months")
            print(f"  • Check if months need to be manually closed first")
            sys.exit(1)
        
        # Get confirmation unless forced or dry run
        if not args.dry_run and not args.force:
            summary = get_excel_import_summary(args.excel_file, args.db_path)
            confirmed = get_user_confirmation(
                summary['total_existing_spots_affected'],
                len(summary['open_months']),
                args.force
            )
            
            if not confirmed:
                print(f"❌ Weekly update cancelled by user")
                sys.exit(0)
        
        # Execute the update
        db_connection = DatabaseConnection(args.db_path)
        service = BroadcastMonthImportService(db_connection)
        
        try:
            result = service.execute_month_replacement(
                args.excel_file,
                'WEEKLY_UPDATE',
                closed_by=None,  # Not needed for weekly updates
                dry_run=args.dry_run
            )
            
            # Display results
            print(f"\n{'='*60}")
            if args.dry_run:
                print(f"🔍 DRY RUN COMPLETED")
            else:
                print(f"🎉 WEEKLY UPDATE COMPLETED")
            print(f"{'='*60}")
            
            print(f"📊 Results:")
            print(f"  Success: {'✅' if result.success else '❌'}")
            print(f"  Batch ID: {result.batch_id}")
            print(f"  Duration: {result.duration_seconds:.2f} seconds")
            print(f"  Months updated: {len(result.broadcast_months_affected)}")
            
            if not args.dry_run:
                print(f"  Records deleted: {result.records_deleted:,}")
                print(f"  Records imported: {result.records_imported:,}")
                print(f"  Net change: {result.records_imported - result.records_deleted:+,}")
            
            if result.error_messages:
                print(f"\n❌ Errors:")
                for error in result.error_messages:
                    print(f"  • {error}")
                sys.exit(1)
            
            if not args.dry_run and result.success:
                print(f"\n✅ Weekly update completed successfully!")
                print(f"📋 Summary:")
                print(f"  • Updated {len(result.broadcast_months_affected)} open months")
                print(f"  • Historical/closed months preserved")
                print(f"  • Database ready for reporting")
                
                print(f"\n💡 Next steps:")
                print(f"  • Generate reports with updated data")
                print(f"  • Close current month when ready: uv run python src/cli/close_month.py")
            
        finally:
            db_connection.close()
    
    except BroadcastMonthImportError as e:
        print(f"❌ Import error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print(f"\n❌ Weekly update cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()