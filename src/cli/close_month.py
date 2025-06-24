#!/usr/bin/env python3
"""
Close month command for manually closing broadcast months.
Marks months as permanently historical and read-only.
"""

import sys
import argparse
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from services.month_closure_service import MonthClosureService, MonthClosureError
from utils.broadcast_month_utils import BroadcastMonthParser
from database.connection import DatabaseConnection


def display_month_status(month: str, service: MonthClosureService):
    """Display detailed status for a specific month."""
    print(f"📊 Month Status: {month}")
    print(f"{'='*40}")
    
    try:
        stats = service.get_month_statistics(month)
        
        if 'error' in stats:
            print(f"❌ Error: {stats['error']}")
            return
        
        # Basic information
        status = "CLOSED" if stats['is_closed'] else "OPEN"
        status_icon = "🔒" if stats['is_closed'] else "📂"
        print(f"Status: {status_icon} {status}")
        
        # Data statistics
        print(f"\nData Statistics:")
        print(f"  Total spots: {stats['total_spots']:,}")
        print(f"  Spots with revenue: {stats['spots_with_revenue']:,}")
        print(f"  Unique customers: {stats['unique_customers']}")
        
        if stats['total_revenue'] > 0:
            print(f"  Total revenue: ${stats['total_revenue']:,.2f}")
            print(f"  Average revenue: ${stats['avg_revenue']:,.2f}")
        
        # Closure information
        if stats['closure_info']:
            info = stats['closure_info']
            print(f"\nClosure Details:")
            print(f"  Closed on: {info['closed_date']}")
            print(f"  Closed by: {info['closed_by']}")
            if info['notes']:
                print(f"  Notes: {info['notes']}")
        
        # Database technical details
        if 'datetime_values_count' in stats:
            print(f"\nTechnical Details:")
            print(f"  Database datetime values: {stats['datetime_values_count']}")
    
    except Exception as e:
        print(f"❌ Error getting month status: {e}")


def display_all_closed_months(service: MonthClosureService):
    """Display all closed months with details."""
    print(f"🔒 All Closed Months")
    print(f"{'='*50}")
    
    try:
        closed_months = service.get_all_closed_months()
        
        if not closed_months:
            print("No closed months found")
            return
        
        print(f"Found {len(closed_months)} closed months:\n")
        
        for info in closed_months:
            parser = BroadcastMonthParser()
            display_name = parser.format_broadcast_month_for_display(info['broadcast_month'])
            
            print(f"🔒 {info['broadcast_month']} ({display_name})")
            print(f"   Closed: {info['closed_date']} by {info['closed_by']}")
            if info['notes']:
                print(f"   Notes: {info['notes']}")
            print()
    
    except Exception as e:
        print(f"❌ Error listing closed months: {e}")


def get_closure_confirmation(month: str, stats: dict) -> bool:
    """Get user confirmation for month closure."""
    print(f"\n🚨 CONFIRMATION REQUIRED")
    print(f"About to PERMANENTLY close month: {month}")
    
    parser = BroadcastMonthParser()
    display_name = parser.format_broadcast_month_for_display(month)
    print(f"Display name: {display_name}")
    
    print(f"\nThis will:")
    print(f"  • Mark {stats['total_spots']:,} spots as historical")
    print(f"  • Prevent future modifications to this month")
    print(f"  • Make closure PERMANENT (cannot be undone)")
    
    if stats['total_revenue'] > 0:
        print(f"  • Protect ${stats['total_revenue']:,.2f} in revenue data")
    
    print(f"\n❗ IMPORTANT: This action cannot be reversed!")
    
    while True:
        response = input(f"\nPermanently close {month}? (type 'CLOSE' to confirm): ").strip()
        if response == 'CLOSE':
            return True
        elif response.lower() in ['no', 'n', 'cancel', '']:
            return False
        else:
            print("Type 'CLOSE' to confirm or 'no' to cancel")


def main():
    parser = argparse.ArgumentParser(
        description="Close broadcast months to make them permanently historical",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python src/cli/close_month.py "May-25" --closed-by "Kurt"
  python src/cli/close_month.py "Dec-24" --closed-by "Kurt" --notes "End of year closing"
  python src/cli/close_month.py --list
  python src/cli/close_month.py --status "May-25"
        """
    )
    
    parser.add_argument("broadcast_month", nargs='?', 
                       help="Month to close in 'Mmm-YY' format (e.g., 'May-25')")
    parser.add_argument("--closed-by", 
                       help="Name/ID of person closing the month (required for closure)")
    parser.add_argument("--notes", 
                       help="Optional notes about the closure")
    parser.add_argument("--db-path", default="data/database/production.db",
                       help="Database path (default: production.db)")
    parser.add_argument("--list", action="store_true",
                       help="List all currently closed months")
    parser.add_argument("--status", 
                       help="Show detailed status for a specific month")
    parser.add_argument("--force", action="store_true",
                       help="Skip confirmation prompt (use with extreme caution)")
    parser.add_argument("--verbose", action="store_true",
                       help="Enable verbose logging")
    
    args = parser.parse_args()
    
    # Setup logging
    import logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format='%(levelname)s: %(message)s')
    
    # Validate database
    if not Path(args.db_path).exists():
        print(f"❌ Database not found: {args.db_path}")
        print(f"Run: uv run python scripts/setup_database.py --db-path {args.db_path}")
        sys.exit(1)
    
    # Create service
    db_connection = DatabaseConnection(args.db_path)
    service = MonthClosureService(db_connection)
    
    try:
        if args.list:
            # List all closed months
            display_all_closed_months(service)
        
        elif args.status:
            # Show status for specific month
            display_month_status(args.status, service)
        
        elif args.broadcast_month:
            # Close a specific month
            month = args.broadcast_month
            
            # Validate month format
            parser = BroadcastMonthParser()
            if not parser.validate_broadcast_month_format(month):
                print(f"❌ Invalid month format: '{month}'")
                print(f"Expected format: 'Mmm-YY' (e.g., 'May-25', 'Dec-24')")
                sys.exit(1)
            
            # Require closed-by for actual closure
            if not args.closed_by:
                print(f"❌ --closed-by is required when closing a month")
                print(f"Example: --closed-by 'Kurt'")
                sys.exit(1)
            
            print(f"🔒 Month Closure Tool")
            print(f"Month: {month}")
            print(f"Closed by: {args.closed_by}")
            if args.notes:
                print(f"Notes: {args.notes}")
            print()
            
            # Check if already closed
            if service.is_month_closed(month):
                print(f"⚠️  Month '{month}' is already closed")
                
                # Show closure details
                closure_info = service.get_month_closure_info(month)
                if closure_info:
                    print(f"Closed on: {closure_info['closed_date']} by {closure_info['closed_by']}")
                    if closure_info['notes']:
                        print(f"Notes: {closure_info['notes']}")
                
                sys.exit(0)
            
            # Get month statistics
            stats = service.get_month_statistics(month)
            if 'error' in stats:
                print(f"❌ Error: {stats['error']}")
                sys.exit(1)
            
            if stats['total_spots'] == 0:
                print(f"❌ Cannot close '{month}': No data exists for this month")
                print(f"Import data first, then close the month")
                sys.exit(1)
            
            # Show month details
            display_month_status(month, service)
            
            # Get confirmation unless forced
            if not args.force:
                confirmed = get_closure_confirmation(month, stats)
                if not confirmed:
                    print(f"❌ Month closure cancelled by user")
                    sys.exit(0)
            
            # Execute the closure
            try:
                success = service.close_broadcast_month(month, args.closed_by, args.notes)
                
                if success:
                    print(f"\n✅ Month '{month}' successfully closed!")
                    
                    # Show updated statistics
                    updated_stats = service.get_month_statistics(month)
                    print(f"\n📊 Final Status:")
                    print(f"  Status: 🔒 CLOSED")
                    print(f"  Spots protected: {updated_stats['total_spots']:,}")
                    print(f"  Revenue protected: ${updated_stats['total_revenue']:,.2f}")
                    
                    print(f"\n💡 Next steps:")
                    print(f"  • Month is now permanently protected from changes")
                    print(f"  • Continue with weekly updates as normal")
                    print(f"  • View all closed months: uv run python src/cli/close_month.py --list")
                else:
                    print(f"❌ Failed to close month '{month}'")
                    sys.exit(1)
            
            except MonthClosureError as e:
                print(f"❌ Closure error: {e}")
                sys.exit(1)
        
        else:
            # No action specified, show help
            parser.print_help()
            print(f"\nQuick commands:")
            print(f"  List closed months:    uv run python src/cli/close_month.py --list")
            print(f"  Check month status:    uv run python src/cli/close_month.py --status 'May-25'")
            print(f"  Close a month:         uv run python src/cli/close_month.py 'May-25' --closed-by 'Kurt'")
    
    except KeyboardInterrupt:
        print(f"\n❌ Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)
    
    finally:
        db_connection.close()


if __name__ == "__main__":
    main()