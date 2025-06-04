#!/usr/bin/env python3
"""
Broadcast month utilities for handling Excel date format conversion and month extraction.
Converts Excel dates (11/15/2024) to broadcast month format (Nov-24).
"""

import sys
import logging
from pathlib import Path
from datetime import datetime, date
from typing import Set, List, Optional, Union
import re

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from openpyxl import load_workbook
except ImportError:
    print("❌ openpyxl not available. Run: uv add openpyxl")
    sys.exit(1)

logger = logging.getLogger(__name__)


class BroadcastMonthParseError(Exception):
    """Raised when there's an error parsing broadcast month data."""
    pass


class BroadcastMonthParser:
    """Handles conversion between Excel dates and broadcast month format."""
    
    # Month name mapping for consistent format
    MONTH_NAMES = {
        1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
        7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'
    }
    
    # Reverse mapping for parsing
    MONTH_NAME_TO_NUM = {name: num for num, name in MONTH_NAMES.items()}
    
    def __init__(self):
        """Initialize the broadcast month parser."""
        self.parse_stats = {
            'total_parsed': 0,
            'successful_conversions': 0,
            'errors': 0,
            'unique_months_found': set()
        }
    
    def parse_excel_date_to_broadcast_month(self, excel_date: Union[datetime, date, str]) -> str:
        """
        Convert Excel date to broadcast month format.
        
        Examples:
        - datetime(2024, 11, 15) -> 'Nov-24'
        - date(2024, 5, 1) -> 'May-24'
        - '11/15/2024' -> 'Nov-24'
        - '2024-11-15' -> 'Nov-24'
        
        Args:
            excel_date: Date from Excel (datetime, date, or string)
            
        Returns:
            Broadcast month in 'Mmm-YY' format
            
        Raises:
            BroadcastMonthParseError: If date cannot be parsed
        """
        self.parse_stats['total_parsed'] += 1
        
        try:
            # Handle datetime and date objects
            if isinstance(excel_date, (datetime, date)):
                target_date = excel_date
            
            # Handle string dates
            elif isinstance(excel_date, str):
                target_date = self._parse_date_string(excel_date.strip())
            
            # Handle None or other types
            else:
                raise BroadcastMonthParseError(f"Unsupported date type: {type(excel_date)}")
            
            # Convert to broadcast month format
            month_name = self.MONTH_NAMES[target_date.month]
            year_suffix = str(target_date.year)[2:]  # Last 2 digits
            broadcast_month = f"{month_name}-{year_suffix}"
            
            self.parse_stats['successful_conversions'] += 1
            self.parse_stats['unique_months_found'].add(broadcast_month)
            
            logger.debug(f"Converted {excel_date} -> {broadcast_month}")
            return broadcast_month
            
        except Exception as e:
            self.parse_stats['errors'] += 1
            error_msg = f"Failed to convert '{excel_date}' to broadcast month: {str(e)}"
            logger.warning(error_msg)
            raise BroadcastMonthParseError(error_msg)
    
    def _parse_date_string(self, date_str: str) -> date:
        """Parse various string date formats to date object."""
        if not date_str:
            raise ValueError("Empty date string")
        
        # Try common date formats
        formats_to_try = [
            '%m/%d/%Y',      # 11/15/2024
            '%m/%d/%y',      # 11/15/24
            '%Y-%m-%d',      # 2024-11-15
            '%d/%m/%Y',      # 15/11/2024 (international)
            '%Y/%m/%d',      # 2024/11/15
            '%m-%d-%Y',      # 11-15-2024
            '%m-%d-%y',      # 11-15-24
        ]
        
        for fmt in formats_to_try:
            try:
                parsed_date = datetime.strptime(date_str, fmt).date()
                logger.debug(f"Parsed '{date_str}' using format '{fmt}'")
                return parsed_date
            except ValueError:
                continue
        
        # If all formats fail
        raise ValueError(f"Could not parse date string '{date_str}' with any known format")
    
    def validate_broadcast_month_format(self, broadcast_month: str) -> bool:
        """
        Validate if a string matches broadcast month format.
        
        Args:
            broadcast_month: String to validate
            
        Returns:
            True if valid format (Mmm-YY), False otherwise
            
        Examples:
            'Nov-24' -> True
            'Dec-25' -> True
            'November-24' -> False
            'Nov24' -> False
            'nov-24' -> False (case sensitive)
        """
        if not isinstance(broadcast_month, str):
            return False
        
        # Pattern: 3-letter month name, dash, 2-digit year
        pattern = r'^[A-Z][a-z]{2}-\d{2}$'
        
        if not re.match(pattern, broadcast_month):
            return False
        
        # Check if month name is valid
        month_part = broadcast_month.split('-')[0]
        return month_part in self.MONTH_NAME_TO_NUM
    
    def extract_year_from_broadcast_month(self, broadcast_month: str) -> int:
        """
        Extract 4-digit year from broadcast month.
        
        Args:
            broadcast_month: Broadcast month in 'Mmm-YY' format
            
        Returns:
            4-digit year
            
        Examples:
            'Nov-24' -> 2024
            'Dec-25' -> 2025
            'Jan-30' -> 2030
            
        Raises:
            BroadcastMonthParseError: If format is invalid
        """
        if not self.validate_broadcast_month_format(broadcast_month):
            raise BroadcastMonthParseError(f"Invalid broadcast month format: '{broadcast_month}'")
        
        year_suffix = broadcast_month.split('-')[1]
        year_num = int(year_suffix)
        
        # Convert 2-digit year to 4-digit year
        # Assume 00-30 means 2000-2030, 31-99 means 1931-1999
        if year_num <= 30:
            return 2000 + year_num
        else:
            return 1900 + year_num
    
    def get_broadcast_months_in_year(self, year: int) -> List[str]:
        """
        Get all 12 broadcast months for a given year.
        
        Args:
            year: 4-digit year
            
        Returns:
            List of all months in broadcast format for that year
            
        Example:
            get_broadcast_months_in_year(2024) -> ['Jan-24', 'Feb-24', ..., 'Dec-24']
        """
        year_suffix = str(year)[2:]  # Last 2 digits
        return [f"{month_name}-{year_suffix}" for month_name in self.MONTH_NAMES.values()]
    
    def format_broadcast_month_for_display(self, broadcast_month: str) -> str:
        """
        Format broadcast month for user-friendly display.
        
        Args:
            broadcast_month: Broadcast month in 'Mmm-YY' format
            
        Returns:
            Formatted string for display
            
        Examples:
            'Nov-24' -> 'November 2024'
            'Dec-25' -> 'December 2025'
        """
        if not self.validate_broadcast_month_format(broadcast_month):
            return broadcast_month  # Return as-is if invalid
        
        month_part, year_part = broadcast_month.split('-')
        
        # Full month names
        full_month_names = {
            'Jan': 'January', 'Feb': 'February', 'Mar': 'March', 'Apr': 'April',
            'May': 'May', 'Jun': 'June', 'Jul': 'July', 'Aug': 'August',
            'Sep': 'September', 'Oct': 'October', 'Nov': 'November', 'Dec': 'December'
        }
        
        full_month = full_month_names.get(month_part, month_part)
        full_year = self.extract_year_from_broadcast_month(broadcast_month)
        
        return f"{full_month} {full_year}"
    
    def get_statistics(self) -> dict:
        """Get parsing statistics."""
        stats = self.parse_stats.copy()
        stats['unique_months_found'] = sorted(list(stats['unique_months_found']))
        if stats['total_parsed'] > 0:
            stats['success_rate'] = (stats['successful_conversions'] / stats['total_parsed']) * 100
        else:
            stats['success_rate'] = 0
        return stats
    
    def reset_statistics(self):
        """Reset parsing statistics."""
        self.parse_stats = {
            'total_parsed': 0,
            'successful_conversions': 0,
            'errors': 0,
            'unique_months_found': set()
        }


def extract_broadcast_months_from_excel(excel_file_path: str, 
                                      month_column: str = 'Month',
                                      limit: Optional[int] = None,
                                      return_display_format: bool = True) -> Set[str]:
    """
    Extract all unique broadcast months from an Excel file.
    
    Args:
        excel_file_path: Path to Excel file
        month_column: Name of column containing month data (default: 'Month')
        limit: Optional limit on rows to process (for testing)
        return_display_format: If True, return 'Nov-24' format; if False, return datetime format
        
    Returns:
        Set of unique broadcast months found in file
        
    Raises:
        BroadcastMonthParseError: If file cannot be read or processed
    """
    print(f"Extracting broadcast months from: {excel_file_path}")
    
    if not Path(excel_file_path).exists():
        raise BroadcastMonthParseError(f"Excel file not found: {excel_file_path}")
    
    parser = BroadcastMonthParser()
    broadcast_months = set()
    
    try:
        workbook = load_workbook(excel_file_path, read_only=True, data_only=True)
        worksheet = workbook.active
        
        # Find the month column
        header_row = list(worksheet.iter_rows(min_row=1, max_row=1, values_only=True))[0]
        month_col_index = None
        
        for i, header in enumerate(header_row):
            if header and str(header).strip() == month_column:
                month_col_index = i
                break
        
        if month_col_index is None:
            raise BroadcastMonthParseError(f"Column '{month_column}' not found in Excel file")
        
        print(f"Found '{month_column}' column at index {month_col_index}")
        
        # Process rows
        row_count = 0
        for row_num, row in enumerate(worksheet.iter_rows(min_row=2, values_only=True), start=2):
            if limit and row_count >= limit:
                break
            
            if not any(cell for cell in row):
                continue  # Skip empty rows
            
            if month_col_index < len(row):
                month_value = row[month_col_index]
                
                if month_value is not None:
                    try:
                        if return_display_format:
                            broadcast_month = parser.parse_excel_date_to_broadcast_month(month_value)
                        else:
                            # Return the raw datetime value (for database compatibility)
                            if isinstance(month_value, str):
                                broadcast_month = month_value
                            else:
                                broadcast_month = str(month_value)
                        
                        broadcast_months.add(broadcast_month)
                        row_count += 1
                    except BroadcastMonthParseError as e:
                        logger.warning(f"Row {row_num}: {e}")
                        continue
        
        workbook.close()
        
        stats = parser.get_statistics()
        print(f"Processed {stats['total_parsed']} month values")
        print(f"Successfully converted {stats['successful_conversions']} ({stats['success_rate']:.1f}%)")
        
        if return_display_format:
            print(f"Found {len(broadcast_months)} unique broadcast months: {sorted(broadcast_months)}")
        else:
            print(f"Found {len(broadcast_months)} unique datetime values")
        
        if stats['errors'] > 0:
            print(f"⚠️  {stats['errors']} parsing errors encountered (check logs)")
        
        return broadcast_months
        
    except Exception as e:
        raise BroadcastMonthParseError(f"Failed to process Excel file: {str(e)}")


def validate_broadcast_months_for_year(broadcast_months: Set[str], expected_year: int) -> tuple[Set[str], Set[str]]:
    """
    Validate that broadcast months belong to expected year.
    
    Args:
        broadcast_months: Set of broadcast months to validate
        expected_year: Expected 4-digit year
        
    Returns:
        Tuple of (matching_months, mismatched_months)
    """
    parser = BroadcastMonthParser()
    matching_months = set()
    mismatched_months = set()
    
    for broadcast_month in broadcast_months:
        try:
            year = parser.extract_year_from_broadcast_month(broadcast_month)
            if year == expected_year:
                matching_months.add(broadcast_month)
            else:
                mismatched_months.add(broadcast_month)
        except BroadcastMonthParseError:
            mismatched_months.add(broadcast_month)  # Invalid format
    
    return matching_months, mismatched_months


# Convenience functions for simple usage
def parse_excel_date(excel_date: Union[datetime, date, str]) -> str:
    """Simple function to parse a single Excel date."""
    parser = BroadcastMonthParser()
    return parser.parse_excel_date_to_broadcast_month(excel_date)


def is_valid_broadcast_month(broadcast_month: str) -> bool:
    """Simple function to validate broadcast month format."""
    parser = BroadcastMonthParser()
    return parser.validate_broadcast_month_format(broadcast_month)


# Test and example usage
if __name__ == "__main__":
    import argparse
    
    parser_cli = argparse.ArgumentParser(description="Test broadcast month utilities")
    parser_cli.add_argument("--test-dates", action="store_true", help="Test date conversion")
    parser_cli.add_argument("--extract-from-excel", help="Excel file to extract months from")
    parser_cli.add_argument("--limit", type=int, help="Limit rows processed (for testing)")
    parser_cli.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    
    args = parser_cli.parse_args()
    
    # Setup logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format='%(levelname)s: %(message)s')
    
    if args.test_dates:
        print("Testing Broadcast Month Parser:")
        print("=" * 50)
        
        parser = BroadcastMonthParser()
        
        # Test various date formats
        test_dates = [
            datetime(2024, 11, 15),      # datetime object
            date(2024, 5, 1),            # date object
            '11/15/2024',                # MM/DD/YYYY
            '5/1/24',                    # M/D/YY
            '2024-11-15',                # ISO format
            'invalid_date',              # Should fail
            None,                        # Should fail
        ]
        
        for test_date in test_dates:
            try:
                result = parser.parse_excel_date_to_broadcast_month(test_date)
                print(f"✓ {test_date} -> {result}")
            except BroadcastMonthParseError as e:
                print(f"✗ {test_date} -> Error: {e}")
        
        print(f"\nStatistics:")
        stats = parser.get_statistics()
        for key, value in stats.items():
            print(f"  {key}: {value}")
    
    if args.extract_from_excel:
        try:
            broadcast_months = extract_broadcast_months_from_excel(
                args.extract_from_excel, 
                limit=args.limit
            )
            
            if broadcast_months:
                print(f"\n✅ Successfully extracted {len(broadcast_months)} unique broadcast months")
                
                # Group by year for display
                years = {}
                parser = BroadcastMonthParser()
                for month in sorted(broadcast_months):
                    try:
                        year = parser.extract_year_from_broadcast_month(month)
                        if year not in years:
                            years[year] = []
                        years[year].append(month)
                    except BroadcastMonthParseError:
                        continue
                
                print("\nMonths by year:")
                for year in sorted(years.keys()):
                    print(f"  {year}: {', '.join(sorted(years[year]))}")
            else:
                print("❌ No broadcast months found in file")
                
        except BroadcastMonthParseError as e:
            print(f"❌ Error: {e}")
            sys.exit(1)
    
    if not args.test_dates and not args.extract_from_excel:
        print("Use --test-dates or --extract-from-excel to test functionality")
        print("Example: python src/utils/broadcast_month_utils.py --test-dates")
        print("Example: python src/utils/broadcast_month_utils.py --extract-from-excel data/sample.xlsx")