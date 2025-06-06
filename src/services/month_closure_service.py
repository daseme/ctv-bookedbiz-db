#!/usr/bin/env python3
"""
Month closure service for managing broadcast month closures and validation.
Provides the core logic for protecting historical data from modification.
"""

import sys
import sqlite3
import logging
from pathlib import Path
from datetime import datetime, date
from typing import List, Set, Optional
from dataclasses import dataclass

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from database.connection import DatabaseConnection
from utils.broadcast_month_utils import BroadcastMonthParser, BroadcastMonthParseError

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of month validation for import operations."""
    is_valid: bool
    closed_months_found: List[str]
    open_months_found: List[str]
    error_message: str
    suggested_action: str
    
    def __post_init__(self):
        """Ensure lists are not None."""
        if self.closed_months_found is None:
            self.closed_months_found = []
        if self.open_months_found is None:
            self.open_months_found = []


class MonthClosureError(Exception):
    """Raised when there's an error with month closure operations."""
    pass


class MonthClosureService:
    """Service for managing broadcast month closures and validation."""
    
    def __init__(self, db_connection: DatabaseConnection):
        """
        Initialize the month closure service.
        
        Args:
            db_connection: Database connection instance
        """
        self.db = db_connection
        self.parser = BroadcastMonthParser()
    
    def _get_datetime_values_for_month(self, broadcast_month_display: str) -> List[str]:
        """
        Get all datetime values in database that match a display format month.
        
        Args:
            broadcast_month_display: Display format like 'Nov-24'
            
        Returns:
            List of actual datetime strings from database for that month
            
        Example:
            'Nov-24' -> ['2024-11-01 00:00:00', '2024-11-15 00:00:00', ...]
        """
        if not self.parser.validate_broadcast_month_format(broadcast_month_display):
            return []
        
        try:
            # Extract year and month from display format
            year = self.parser.extract_year_from_broadcast_month(broadcast_month_display)
            month_name = broadcast_month_display.split('-')[0]
            month_num = self.parser.MONTH_NAME_TO_NUM[month_name]
            
            # Find all datetime values in database for this year/month
            conn = self.db.connect()
            cursor = conn.execute("""
                SELECT DISTINCT broadcast_month 
                FROM spots 
                WHERE broadcast_month IS NOT NULL
                  AND strftime('%Y', broadcast_month) = ?
                  AND strftime('%m', broadcast_month) = ?
            """, (str(year), f"{month_num:02d}"))
            
            datetime_values = [row[0] for row in cursor.fetchall()]
            logger.debug(f"Found {len(datetime_values)} datetime values for {broadcast_month_display}")
            return datetime_values
            
        except Exception as e:
            logger.error(f"Error finding datetime values for {broadcast_month_display}: {e}")
            return []
    
    # CRITICAL FIX: Add this method to MonthClosureService class in month_closure_service.py

    def close_broadcast_month_with_connection(self, broadcast_month_display: str, closed_by: str, conn, notes: str = None) -> bool:
        """
        CRITICAL FIX: Mark a broadcast month as permanently closed using existing connection.
        This version works within an existing transaction.
        
        Args:
            broadcast_month_display: Month to close in 'Mmm-YY' format (e.g., 'Nov-24')
            closed_by: Name/ID of person closing the month
            conn: Existing database connection with active transaction
            notes: Optional notes about the closure
            
        Returns:
            True if month was successfully closed
            
        Raises:
            MonthClosureError: If month cannot be closed
        """
        logger.info(f"Attempting to close broadcast month: {broadcast_month_display}")
        
        # Validate broadcast month format
        if not self.parser.validate_broadcast_month_format(broadcast_month_display):
            raise MonthClosureError(f"Invalid broadcast month format: '{broadcast_month_display}'. Expected format: 'Mmm-YY'")
        
        # Check if month already closed
        if self.is_month_closed(broadcast_month_display):
            raise MonthClosureError(f"Month '{broadcast_month_display}' is already closed")
        
        # Get all datetime values for this display month
        datetime_values = self._get_datetime_values_for_month(broadcast_month_display)
        if not datetime_values:
            raise MonthClosureError(f"Cannot close '{broadcast_month_display}': No data exists for this month. Import data first, then close the month.")
        
        try:
            # CRITICAL FIX: Use the existing connection instead of creating a new transaction
            
            # Insert closure record (using display format for user interface)
            conn.execute("""
                INSERT INTO month_closures (broadcast_month, closed_date, closed_by, notes)
                VALUES (?, ?, ?, ?)
            """, (broadcast_month_display, date.today(), closed_by, notes))
            
            # Mark all spots for this month as historical (using actual datetime values)
            updated_count = 0
            for datetime_value in datetime_values:
                cursor = conn.execute("""
                    UPDATE spots 
                    SET is_historical = 1 
                    WHERE broadcast_month = ?
                """, (datetime_value,))
                updated_count += cursor.rowcount
            
            logger.info(f"Successfully closed '{broadcast_month_display}': {updated_count} spots marked as historical from {len(datetime_values)} datetime values")
            
            return True
            
        except Exception as e:
            error_msg = f"Failed to close month '{broadcast_month_display}': {str(e)}"
            logger.error(error_msg)
            raise MonthClosureError(error_msg)

    def _get_display_format_for_datetime(self, datetime_value: str) -> str:
        """
        Convert database datetime value to display format.
        
        Args:
            datetime_value: Database datetime string like '2024-11-15 00:00:00'
            
        Returns:
            Display format like 'Nov-24'
        """
        try:
            return self.parser.parse_excel_date_to_broadcast_month(datetime_value)
        except BroadcastMonthParseError:
            logger.warning(f"Could not convert datetime '{datetime_value}' to display format")
            return datetime_value  # Return as-is if conversion fails
    
    def close_broadcast_month(self, broadcast_month_display: str, closed_by: str, notes: str = None) -> bool:
        """
        UPDATED: Mark a broadcast month as permanently closed.
        This version handles both standalone and within-transaction usage.
        
        Args:
            broadcast_month_display: Month to close in 'Mmm-YY' format (e.g., 'Nov-24')
            closed_by: Name/ID of person closing the month
            notes: Optional notes about the closure
            
        Returns:
            True if month was successfully closed
            
        Raises:
            MonthClosureError: If month cannot be closed
        """
        logger.info(f"Attempting to close broadcast month: {broadcast_month_display}")
        
        # Validate broadcast month format
        if not self.parser.validate_broadcast_month_format(broadcast_month_display):
            raise MonthClosureError(f"Invalid broadcast month format: '{broadcast_month_display}'. Expected format: 'Mmm-YY'")
        
        # Check if month already closed
        if self.is_month_closed(broadcast_month_display):
            raise MonthClosureError(f"Month '{broadcast_month_display}' is already closed")
        
        # Get all datetime values for this display month
        datetime_values = self._get_datetime_values_for_month(broadcast_month_display)
        if not datetime_values:
            raise MonthClosureError(f"Cannot close '{broadcast_month_display}': No data exists for this month. Import data first, then close the month.")
        
        try:
            # CRITICAL FIX: Check if we're already in a transaction
            # If so, don't create a new one
            conn = self.db.connect()
            
            # Try to detect if we're already in a transaction
            try:
                conn.execute("BEGIN IMMEDIATE")
                in_transaction = False
            except sqlite3.OperationalError as e:
                if "cannot start a transaction within a transaction" in str(e):
                    in_transaction = True
                else:
                    raise
            
            try:
                # Insert closure record (using display format for user interface)
                conn.execute("""
                    INSERT INTO month_closures (broadcast_month, closed_date, closed_by, notes)
                    VALUES (?, ?, ?, ?)
                """, (broadcast_month_display, date.today(), closed_by, notes))
                
                # Mark all spots for this month as historical (using actual datetime values)
                updated_count = 0
                for datetime_value in datetime_values:
                    cursor = conn.execute("""
                        UPDATE spots 
                        SET is_historical = 1 
                        WHERE broadcast_month = ?
                    """, (datetime_value,))
                    updated_count += cursor.rowcount
                
                # Only commit if we started the transaction
                if not in_transaction:
                    conn.commit()
                
                logger.info(f"Successfully closed '{broadcast_month_display}': {updated_count} spots marked as historical from {len(datetime_values)} datetime values")
                
                return True
                
            except Exception as e:
                # Only rollback if we started the transaction
                if not in_transaction:
                    conn.rollback()
                raise
            finally:
                if not in_transaction:
                    conn.close()
                    
        except Exception as e:
            error_msg = f"Failed to close month '{broadcast_month_display}': {str(e)}"
            logger.error(error_msg)
            raise MonthClosureError(error_msg)
    

    
    def is_month_closed(self, broadcast_month_display: str) -> bool:
        """
        Check if a broadcast month is closed.
        
        Args:
            broadcast_month_display: Month to check in 'Mmm-YY' format
            
        Returns:
            True if month is closed, False otherwise
        """
        try:
            conn = self.db.connect()
            cursor = conn.execute(
                "SELECT 1 FROM month_closures WHERE broadcast_month = ?",
                (broadcast_month_display,)
            )
            return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Error checking if month '{broadcast_month_display}' is closed: {e}")
            return False
    
    def get_closed_months(self, broadcast_months_display: List[str]) -> List[str]:
        """
        Filter list to return only closed months.
        
        Args:
            broadcast_months_display: List of broadcast months in display format to check
            
        Returns:
            List of months that are closed (in display format)
        """
        if not broadcast_months_display:
            return []
        
        try:
            conn = self.db.connect()
            placeholders = ', '.join(['?' for _ in broadcast_months_display])
            cursor = conn.execute(
                f"SELECT broadcast_month FROM month_closures WHERE broadcast_month IN ({placeholders})",
                broadcast_months_display
            )
            
            closed_months = [row[0] for row in cursor.fetchall()]
            logger.debug(f"Found {len(closed_months)} closed months out of {len(broadcast_months_display)} checked")
            return closed_months
            
        except Exception as e:
            logger.error(f"Error getting closed months: {e}")
            return []
    
    def get_all_closed_months(self) -> List[str]:
        """
        Get all closed broadcast months.
        
        Returns:
            List of all closed months, sorted chronologically
        """
        try:
            conn = self.db.connect()
            cursor = conn.execute("""
                SELECT broadcast_month, closed_date, closed_by, notes
                FROM month_closures 
                ORDER BY broadcast_month
            """)
            
            closed_months = []
            for row in cursor.fetchall():
                closed_months.append({
                    'broadcast_month': row[0],
                    'closed_date': row[1],
                    'closed_by': row[2],
                    'notes': row[3]
                })
            
            return closed_months
            
        except Exception as e:
            logger.error(f"Error getting all closed months: {e}")
            return []
    
    def get_month_closure_info(self, broadcast_month: str) -> Optional[dict]:
        """
        Get detailed closure information for a specific month.
        
        Args:
            broadcast_month: Month to get info for
            
        Returns:
            Dictionary with closure info, or None if not closed
        """
        try:
            conn = self.db.connect()
            cursor = conn.execute("""
                SELECT broadcast_month, closed_date, closed_by, notes, created_date
                FROM month_closures 
                WHERE broadcast_month = ?
            """, (broadcast_month,))
            
            row = cursor.fetchone()
            if row:
                return {
                    'broadcast_month': row[0],
                    'closed_date': row[1],
                    'closed_by': row[2],
                    'notes': row[3],
                    'created_date': row[4]
                }
            return None
            
        except Exception as e:
            logger.error(f"Error getting closure info for '{broadcast_month}': {e}")
            return None
    
    def validate_months_for_import(self, broadcast_months_display: List[str], import_mode: str) -> ValidationResult:
        """
        Validate months can be imported based on mode.
        
        Args:
            broadcast_months_display: List of broadcast months in display format to validate
            import_mode: Import mode ('HISTORICAL', 'WEEKLY_UPDATE', 'MANUAL')
            
        Returns:
            ValidationResult with detailed validation info
        """
        logger.info(f"Validating {len(broadcast_months_display)} months for {import_mode} import")
        
        # Get closed and open months
        closed_months = self.get_closed_months(broadcast_months_display)
        open_months = [month for month in broadcast_months_display if month not in closed_months]
        
        # Validation logic based on import mode
        if import_mode == 'WEEKLY_UPDATE':
            # Weekly updates cannot touch any closed months
            if closed_months:
                error_msg = (
                    f"Cannot import: data contains closed months {closed_months} "
                    f"and open months {open_months}."
                )
                suggested_action = (
                    f"Remove rows with broadcast_month in {closed_months} from Excel file and retry weekly update."
                )
                
                return ValidationResult(
                    is_valid=False,
                    closed_months_found=closed_months,
                    open_months_found=open_months,
                    error_message=error_msg,
                    suggested_action=suggested_action
                )
        
        elif import_mode == 'HISTORICAL':
            # Historical imports can import into any months (will be closed immediately)
            if closed_months:
                logger.warning(f"Historical import includes already closed months: {closed_months}")
        
        elif import_mode == 'MANUAL':
            # Manual imports are allowed but we warn about closed months
            if closed_months:
                logger.warning(f"Manual import includes closed months: {closed_months}")
        
        # All validations passed
        return ValidationResult(
            is_valid=True,
            closed_months_found=closed_months,
            open_months_found=open_months,
            error_message="",
            suggested_action=""
        )
    
    def bulk_close_months(self, broadcast_months: List[str], closed_by: str, notes: str = None) -> dict:
        """
        Close multiple broadcast months in a single operation.
        
        Args:
            broadcast_months: List of months to close
            closed_by: Name/ID of person closing the months
            notes: Optional notes for all closures
            
        Returns:
            Dictionary with results: {'success': [...], 'failed': [...], 'already_closed': [...]}
        """
        logger.info(f"Bulk closing {len(broadcast_months)} months")
        
        results = {
            'success': [],
            'failed': [],
            'already_closed': []
        }
        
        for broadcast_month in broadcast_months:
            try:
                if self.is_month_closed(broadcast_month):
                    results['already_closed'].append(broadcast_month)
                    logger.info(f"Month '{broadcast_month}' already closed, skipping")
                else:
                    self.close_broadcast_month(broadcast_month, closed_by, notes)
                    results['success'].append(broadcast_month)
                    logger.info(f"Successfully closed '{broadcast_month}'")
                    
            except MonthClosureError as e:
                results['failed'].append({'month': broadcast_month, 'error': str(e)})
                logger.error(f"Failed to close '{broadcast_month}': {e}")
        
        logger.info(f"Bulk closure completed: {len(results['success'])} success, {len(results['failed'])} failed, {len(results['already_closed'])} already closed")
        return results
    
    def _get_spots_count_for_month(self, broadcast_month: str) -> int:
        """Get count of spots for a specific broadcast month."""
        try:
            conn = self.db.connect()
            cursor = conn.execute(
                "SELECT COUNT(*) FROM spots WHERE broadcast_month = ?",
                (broadcast_month,)
            )
            return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Error counting spots for '{broadcast_month}': {e}")
            return 0
    
    def get_month_statistics(self, broadcast_month_display: str) -> dict:
        """
        Get detailed statistics for a broadcast month.
        
        Args:
            broadcast_month_display: Month to get statistics for in 'Mmm-YY' format
            
        Returns:
            Dictionary with month statistics
        """
        try:
            # Get all datetime values for this display month
            datetime_values = self._get_datetime_values_for_month(broadcast_month_display)
            
            if not datetime_values:
                return {
                    'broadcast_month': broadcast_month_display,
                    'total_spots': 0,
                    'spots_with_revenue': 0,
                    'total_revenue': 0.0,
                    'avg_revenue': 0.0,
                    'min_revenue': 0.0,
                    'max_revenue': 0.0,
                    'unique_customers': 0,
                    'is_closed': self.is_month_closed(broadcast_month_display),
                    'closure_info': self.get_month_closure_info(broadcast_month_display)
                }
            
            conn = self.db.connect()
            
            # Basic spot count for all datetime values in this month
            placeholders = ', '.join(['?' for _ in datetime_values])
            cursor = conn.execute(
                f"SELECT COUNT(*) FROM spots WHERE broadcast_month IN ({placeholders})",
                datetime_values
            )
            total_spots = cursor.fetchone()[0]
            
            # Revenue statistics
            cursor = conn.execute(f"""
                SELECT 
                    COUNT(*) as spots_with_revenue,
                    SUM(gross_rate) as total_revenue,
                    AVG(gross_rate) as avg_revenue,
                    MIN(gross_rate) as min_revenue,
                    MAX(gross_rate) as max_revenue
                FROM spots 
                WHERE broadcast_month IN ({placeholders}) AND gross_rate IS NOT NULL
            """, datetime_values)
            
            revenue_row = cursor.fetchone()
            
            # Customer count
            cursor = conn.execute(f"""
                SELECT COUNT(DISTINCT customer_id) 
                FROM spots 
                WHERE broadcast_month IN ({placeholders}) AND customer_id IS NOT NULL
            """, datetime_values)
            
            unique_customers = cursor.fetchone()[0]
            
            # Closure info
            closure_info = self.get_month_closure_info(broadcast_month_display)
            
            return {
                'broadcast_month': broadcast_month_display,
                'total_spots': total_spots,
                'spots_with_revenue': revenue_row[0] if revenue_row[0] else 0,
                'total_revenue': float(revenue_row[1]) if revenue_row[1] else 0.0,
                'avg_revenue': float(revenue_row[2]) if revenue_row[2] else 0.0,
                'min_revenue': float(revenue_row[3]) if revenue_row[3] else 0.0,
                'max_revenue': float(revenue_row[4]) if revenue_row[4] else 0.0,
                'unique_customers': unique_customers,
                'datetime_values_count': len(datetime_values),
                'is_closed': closure_info is not None,
                'closure_info': closure_info
            }
            
        except Exception as e:
            logger.error(f"Error getting statistics for '{broadcast_month_display}': {e}")
            return {
                'broadcast_month': broadcast_month_display,
                'error': str(e)
            }


# Convenience functions for simple usage
def close_month(db_path: str, broadcast_month: str, closed_by: str, notes: str = None) -> bool:
    """Simple function to close a single month."""
    db_connection = DatabaseConnection(db_path)
    service = MonthClosureService(db_connection)
    
    try:
        result = service.close_broadcast_month(broadcast_month, closed_by, notes)
        return result
    finally:
        db_connection.close()


def is_month_closed(db_path: str, broadcast_month: str) -> bool:
    """Simple function to check if a month is closed."""
    db_connection = DatabaseConnection(db_path)
    service = MonthClosureService(db_connection)
    
    try:
        return service.is_month_closed(broadcast_month)
    finally:
        db_connection.close()


def get_all_closed_months(db_path: str) -> List[str]:
    """Simple function to get all closed months."""
    db_connection = DatabaseConnection(db_path)
    service = MonthClosureService(db_connection)
    
    try:
        return service.get_all_closed_months()
    finally:
        db_connection.close()


# Test and example usage
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Test month closure service")
    parser.add_argument("--db-path", default="data/database/test.db", help="Database path")
    parser.add_argument("--close-month", help="Close a specific month (e.g., 'Nov-24')")
    parser.add_argument("--closed-by", help="Name of person closing the month")
    parser.add_argument("--notes", help="Optional notes for closure")
    parser.add_argument("--check-month", help="Check if a month is closed")
    parser.add_argument("--list-closed", action="store_true", help="List all closed months")
    parser.add_argument("--test-validation", action="store_true", help="Test validation logic")
    parser.add_argument("--month-stats", help="Get statistics for a specific month")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    # Setup logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format='%(levelname)s: %(message)s')
    
    if not Path(args.db_path).exists():
        print(f"❌ Database not found: {args.db_path}")
        print("Run: uv run python scripts/setup_database.py --db-path {args.db_path}")
        sys.exit(1)
    
    # Create service
    db_connection = DatabaseConnection(args.db_path)
    service = MonthClosureService(db_connection)
    
    try:
        if args.close_month:
            if not args.closed_by:
                print("❌ --closed-by is required when closing a month")
                sys.exit(1)
            
            try:
                result = service.close_broadcast_month(args.close_month, args.closed_by, args.notes)
                if result:
                    print(f"✅ Successfully closed month '{args.close_month}'")
                else:
                    print(f"❌ Failed to close month '{args.close_month}'")
            except MonthClosureError as e:
                print(f"❌ Error: {e}")
        
        elif args.check_month:
            is_closed = service.is_month_closed(args.check_month)
            status = "CLOSED" if is_closed else "OPEN"
            print(f"Month '{args.check_month}' status: {status}")
            
            if is_closed:
                info = service.get_month_closure_info(args.check_month)
                if info:
                    print(f"  Closed on: {info['closed_date']}")
                    print(f"  Closed by: {info['closed_by']}")
                    if info['notes']:
                        print(f"  Notes: {info['notes']}")
        
        elif args.list_closed:
            closed_months = service.get_all_closed_months()
            if closed_months:
                print(f"Found {len(closed_months)} closed months:")
                for info in closed_months:
                    print(f"  {info['broadcast_month']} - Closed {info['closed_date']} by {info['closed_by']}")
                    if info['notes']:
                        print(f"    Notes: {info['notes']}")
            else:
                print("No closed months found")
        
        elif args.month_stats:
            stats = service.get_month_statistics(args.month_stats)
            if 'error' in stats:
                print(f"❌ Error getting statistics: {stats['error']}")
            else:
                print(f"Statistics for {stats['broadcast_month']}:")
                print(f"  Total spots: {stats['total_spots']:,}")
                print(f"  Spots with revenue: {stats['spots_with_revenue']:,}")
                print(f"  Total revenue: ${stats['total_revenue']:,.2f}")
                print(f"  Average revenue: ${stats['avg_revenue']:,.2f}")
                print(f"  Unique customers: {stats['unique_customers']}")
                print(f"  Status: {'CLOSED' if stats['is_closed'] else 'OPEN'}")
                
                if stats['closure_info']:
                    info = stats['closure_info']
                    print(f"  Closed on: {info['closed_date']} by {info['closed_by']}")
        
        elif args.test_validation:
            print("Testing validation logic:")
            print("=" * 50)
            
            # Test with some sample months (mix of potentially closed and open)
            test_months = ['Jan-24', 'Feb-24', 'Nov-24', 'Dec-24', 'Jan-25', 'Feb-25']
            
            print(f"Testing validation with months: {test_months}")
            
            for import_mode in ['WEEKLY_UPDATE', 'HISTORICAL', 'MANUAL']:
                result = service.validate_months_for_import(test_months, import_mode)
                print(f"\n{import_mode} mode:")
                print(f"  Valid: {result.is_valid}")
                print(f"  Closed months: {result.closed_months_found}")
                print(f"  Open months: {result.open_months_found}")
                if result.error_message:
                    print(f"  Error: {result.error_message}")
                    print(f"  Suggestion: {result.suggested_action}")
        
        else:
            print("Use one of the available options:")
            print("  --close-month 'Nov-24' --closed-by 'Kurt'")
            print("  --check-month 'Nov-24'")
            print("  --list-closed")
            print("  --month-stats 'Nov-24'")
            print("  --test-validation")
    
    finally:
        db_connection.close()