#!/usr/bin/env python3
"""
Integration utilities for import processes.
Handles conversion between Excel data and month closure system formats.
"""

import sys
from pathlib import Path
from typing import Set, List, Dict, Any
from datetime import datetime, date

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.broadcast_month_utils import extract_broadcast_months_from_excel, BroadcastMonthParser
from src.services.month_closure_service import MonthClosureService, ValidationResult
from src.database.connection import DatabaseConnection


def extract_display_months_from_excel(excel_file_path: str, limit: int = None) -> Set[str]:
    """
    Extract broadcast months from Excel file in display format ('Nov-24').
    This is the format used by the month closure system.
    
    Args:
        excel_file_path: Path to Excel file
        limit: Optional limit for testing
        
    Returns:
        Set of broadcast months in display format
    """
    return extract_broadcast_months_from_excel(
        excel_file_path, 
        month_column='Month',
        limit=limit,
        return_display_format=True
    )


def validate_excel_for_import(excel_file_path: str, 
                            import_mode: str, 
                            db_path: str,
                            limit: int = None) -> ValidationResult:
    """
    Validate an Excel file for import against closed months.
    
    Args:
        excel_file_path: Path to Excel file
        import_mode: 'WEEKLY_UPDATE', 'HISTORICAL', or 'MANUAL'
        db_path: Database path for month closure checking
        limit: Optional limit for testing
        
    Returns:
        ValidationResult indicating if import is allowed
    """
    # Extract months from Excel in display format
    display_months = list(extract_display_months_from_excel(excel_file_path, limit))
    
    # Validate against closed months
    db_connection = DatabaseConnection(db_path)
    try:
        closure_service = MonthClosureService(db_connection)
        return closure_service.validate_months_for_import(display_months, import_mode)
    finally:
        db_connection.close()


def get_excel_import_summary(excel_file_path: str, db_path: str, limit: int = None) -> Dict[str, Any]:
    """
    Get comprehensive summary of what an Excel import would affect.
    
    Args:
        excel_file_path: Path to Excel file
        db_path: Database path
        limit: Optional limit for testing
        
    Returns:
        Dictionary with import summary information
    """
    display_months = list(extract_display_months_from_excel(excel_file_path, limit))
    
    db_connection = DatabaseConnection(db_path)
    try:
        closure_service = MonthClosureService(db_connection)
        
        # Get closure status for each month
        month_details = []
        total_spots_affected = 0
        
        for month in sorted(display_months):
            stats = closure_service.get_month_statistics(month)
            month_details.append({
                'month': month,
                'is_closed': stats.get('is_closed', False),
                'existing_spots': stats.get('total_spots', 0),
                'existing_revenue': stats.get('total_revenue', 0.0)
            })
            total_spots_affected += stats.get('total_spots', 0)
        
        closed_months = closure_service.get_closed_months(display_months)
        open_months = [m for m in display_months if m not in closed_months]
        
        return {
            'excel_file': excel_file_path,
            'months_in_excel': display_months,
            'closed_months': closed_months,
            'open_months': open_months,
            'month_details': month_details,
            'total_existing_spots_affected': total_spots_affected,
            'can_weekly_update': len(closed_months) == 0,
            'can_historical_import': True,  # Always allowed
            'can_manual_import': True       # Always allowed with warnings
        }
        
    finally:
        db_connection.close()


def preview_import_impact(excel_file_path: str, 
                         import_mode: str,
                         db_path: str,
                         limit: int = None) -> Dict[str, Any]:
    """
    Preview what an import would do without executing it.
    
    Args:
        excel_file_path: Path to Excel file
        import_mode: Import mode to simulate
        db_path: Database path
        limit: Optional limit for testing
        
    Returns:
        Dictionary with detailed preview information
    """
    summary = get_excel_import_summary(excel_file_path, db_path, limit)
    validation = validate_excel_for_import(excel_file_path, import_mode, db_path, limit)
    
    # Count records in Excel (estimated)
    try:
        from openpyxl import load_workbook
        workbook = load_workbook(excel_file_path, read_only=True)
        worksheet = workbook.active
        excel_rows = worksheet.max_row - 1  # Subtract header row
        if limit:
            excel_rows = min(excel_rows, limit)
        workbook.close()
    except Exception:
        excel_rows = 0
    
    return {
        'import_mode': import_mode,
        'validation': {
            'is_valid': validation.is_valid,
            'error_message': validation.error_message,
            'suggested_action': validation.suggested_action
        },
        'excel_summary': summary,
        'estimated_new_records': excel_rows,
        'impact_summary': {
            'months_affected': len(summary['months_in_excel']),
            'existing_spots_replaced': summary['total_existing_spots_affected'],
            'new_spots_imported': excel_rows,
            'net_change': excel_rows - summary['total_existing_spots_affected']
        }
    }


# Convenience functions for CLI usage
def quick_validate_excel(excel_file_path: str, import_mode: str, db_path: str) -> bool:
    """
    Quick validation - returns True if import is allowed, False otherwise.
    Prints user-friendly messages.
    """
    try:
        validation = validate_excel_for_import(excel_file_path, import_mode, db_path)
        
        if validation.is_valid:
            print(f"✅ Excel file validated for {import_mode} import")
            if validation.closed_months_found:
                print(f"⚠️  Warning: Includes closed months {validation.closed_months_found}")
            return True
        else:
            print(f"❌ Excel file cannot be imported in {import_mode} mode")
            print(f"Error: {validation.error_message}")
            print(f"Solution: {validation.suggested_action}")
            return False
            
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        return False


def display_import_preview(excel_file_path: str, import_mode: str, db_path: str):
    """Display a comprehensive import preview."""
    try:
        preview = preview_import_impact(excel_file_path, import_mode, db_path)
        
        print(f"Import Preview: {excel_file_path}")
        print(f"Mode: {import_mode}")
        print("=" * 60)
        
        # Validation status
        if preview['validation']['is_valid']:
            print(f"✅ Import allowed")
        else:
            print(f"❌ Import blocked: {preview['validation']['error_message']}")
            print(f"Solution: {preview['validation']['suggested_action']}")
            return
        
        # Impact summary
        impact = preview['impact_summary']
        print(f"\nImpact Summary:")
        print(f"  Months affected: {impact['months_affected']}")
        print(f"  Existing spots to replace: {impact['existing_spots_replaced']:,}")
        print(f"  New spots to import: {impact['new_spots_imported']:,}")
        print(f"  Net change: {impact['net_change']:+,}")
        
        # Month details
        excel_summary = preview['excel_summary']
        print(f"\nMonth Details:")
        for month_info in excel_summary['month_details']:
            status = "CLOSED" if month_info['is_closed'] else "OPEN"
            print(f"  {month_info['month']}: {status} - {month_info['existing_spots']:,} existing spots")
        
        if excel_summary['closed_months']:
            print(f"\n⚠️  Closed months: {excel_summary['closed_months']}")
        
    except Exception as e:
        print(f"❌ Preview failed: {e}")


# Test and example usage
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Test import integration utilities")
    parser.add_argument("excel_file", help="Excel file to analyze")
    parser.add_argument("--db-path", default="data/database/production.db", help="Database path")
    parser.add_argument("--mode", choices=['WEEKLY_UPDATE', 'HISTORICAL', 'MANUAL'], 
                       default='WEEKLY_UPDATE', help="Import mode to test")
    parser.add_argument("--limit", type=int, help="Limit rows processed (for testing)")
    parser.add_argument("--quick-validate", action="store_true", help="Quick validation only")
    parser.add_argument("--preview", action="store_true", help="Show detailed preview")
    parser.add_argument("--summary", action="store_true", help="Show Excel summary")
    
    args = parser.parse_args()
    
    if args.quick_validate:
        success = quick_validate_excel(args.excel_file, args.mode, args.db_path)
        sys.exit(0 if success else 1)
    
    elif args.preview:
        display_import_preview(args.excel_file, args.mode, args.db_path)
    
    elif args.summary:
        summary = get_excel_import_summary(args.excel_file, args.db_path, args.limit)
        print(f"Excel Import Summary:")
        print(f"  File: {summary['excel_file']}")
        print(f"  Months: {summary['months_in_excel']}")
        print(f"  Closed months: {summary['closed_months']}")
        print(f"  Open months: {summary['open_months']}")
        print(f"  Total existing spots affected: {summary['total_existing_spots_affected']:,}")
        print(f"  Can weekly update: {'Yes' if summary['can_weekly_update'] else 'No'}")
    
    else:
        print("Use --quick-validate, --preview, or --summary")
        print("Example: python import_integration_utilities.py data/test.xlsx --preview --mode WEEKLY_UPDATE")