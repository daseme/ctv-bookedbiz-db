#!/usr/bin/env python3
"""
Identify and analyze administrative/internal entries that shouldn't be used for customer matching.

This script will help clean your data by finding entries with administrative patterns
like "DO NOT INVOIC", "Broker Fees", "Internal", "Test", etc.

Usage:
  python -m src.cli.identify_admin_entries --db-path data/database/production.db
  python -m src.cli.identify_admin_entries --db-path data/database/production.db --export-csv admin_entries.csv
"""

from __future__ import annotations

import argparse
import re
import sqlite3
from pathlib import Path
from typing import List, Tuple, Dict
import csv

# Administrative patterns that indicate internal/non-customer entries
ADMIN_PATTERNS = [
    r"DO NOT INVOIC",
    r"DO NOT BILL",
    r"BROKER FEES?",
    r"INTERNAL",
    r"ADMIN",
    r"TEST",
    r"PLACEHOLDER",
    r"TEMP",
    r"DUMMY",
    r"SAMPLE",
    r"DELETE",
    r"REMOVE",
    r"IGNORE",
    r"TRAINING",
    r"DEMO",
    r"EXAMPLE",
    r"MISC",
    r"VARIOUS",
    r"UNKNOWN CLIENT",
    r"TBD",
    r"TO BE DETERMINED",
    r"HOUSE ACCOUNT",
    r"CONTRA",
    r"ADJUSTMENT",
    r"CREDIT",
    r"REFUND",
]

# Compile patterns for efficiency
ADMIN_REGEX = re.compile(
    r"\b(?:" + "|".join(ADMIN_PATTERNS) + r")\b", 
    re.IGNORECASE
)

def get_all_bill_codes(db_path: str) -> List[Tuple[str, float, int]]:
    """Get all bill codes with revenue and spot count."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Query to get bill codes with their revenue and spot totals
    # Using the actual schema: spots table with gross_rate and station_net
    query = """
    SELECT 
        bill_code,
        SUM(COALESCE(station_net, COALESCE(gross_rate, 0))) as total_revenue,
        COUNT(*) as total_spots
    FROM spots 
    WHERE bill_code IS NOT NULL 
        AND bill_code != ''
        AND (revenue_type != 'Trade' OR revenue_type IS NULL)
    GROUP BY bill_code
    ORDER BY total_revenue DESC
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()
    
    return [(row[0], row[1], row[2]) for row in results]

def identify_admin_entries(bill_codes: List[Tuple[str, float, int]]) -> Dict[str, List[Tuple[str, float, int]]]:
    """Categorize entries into admin vs regular."""
    admin_entries = []
    suspicious_entries = []
    clean_entries = []
    
    for bill_code, revenue, spots in bill_codes:
        if ADMIN_REGEX.search(bill_code):
            admin_entries.append((bill_code, revenue, spots))
        elif is_suspicious_pattern(bill_code):
            suspicious_entries.append((bill_code, revenue, spots))
        else:
            clean_entries.append((bill_code, revenue, spots))
    
    return {
        'admin': admin_entries,
        'suspicious': suspicious_entries,
        'clean': clean_entries
    }

def is_suspicious_pattern(bill_code: str) -> bool:
    """Check for suspicious patterns that might indicate admin entries."""
    bill_code_lower = bill_code.lower()
    
    # Suspicious patterns
    suspicious_indicators = [
        # Too many special characters
        len(re.findall(r'[^a-zA-Z0-9\s]', bill_code)) > len(bill_code) * 0.3,
        # All caps abbreviations with no real words
        len(bill_code) < 10 and bill_code.isupper() and not any(word.lower() in ['inc', 'llc', 'corp', 'ltd'] for word in bill_code.split()),
        # Repeated characters or obvious placeholders
        'xxx' in bill_code_lower,
        'zzz' in bill_code_lower,
        '...' in bill_code,
        # Generic entries
        bill_code_lower in ['tbd', 'tba', 'n/a', 'na', 'none', 'null', 'empty'],
        # Suspiciously short
        len(bill_code.strip()) < 3,
    ]
    
    return any(suspicious_indicators)

def print_analysis(categorized: Dict[str, List[Tuple[str, float, int]]]):
    """Print analysis of the categorized entries."""
    admin_entries = categorized['admin']
    suspicious_entries = categorized['suspicious']
    clean_entries = categorized['clean']
    
    total_entries = len(admin_entries) + len(suspicious_entries) + len(clean_entries)
    admin_revenue = sum(revenue for _, revenue, _ in admin_entries)
    suspicious_revenue = sum(revenue for _, revenue, _ in suspicious_entries)
    clean_revenue = sum(revenue for _, revenue, _ in clean_entries)
    total_revenue = admin_revenue + suspicious_revenue + clean_revenue
    
    print("=" * 80)
    print("ADMINISTRATIVE ENTRY ANALYSIS")
    print("=" * 80)
    print(f"Total entries:        {total_entries:,}")
    print(f"Admin entries:        {len(admin_entries):,} ({len(admin_entries)/total_entries*100:.1f}%)")
    print(f"Suspicious entries:   {len(suspicious_entries):,} ({len(suspicious_entries)/total_entries*100:.1f}%)")
    print(f"Clean entries:        {len(clean_entries):,} ({len(clean_entries)/total_entries*100:.1f}%)")
    print()
    print(f"Total revenue:        ${total_revenue:,.2f}")
    print(f"Admin revenue:        ${admin_revenue:,.2f} ({admin_revenue/total_revenue*100:.1f}%)")
    print(f"Suspicious revenue:   ${suspicious_revenue:,.2f} ({suspicious_revenue/total_revenue*100:.1f}%)")
    print(f"Clean revenue:        ${clean_revenue:,.2f} ({clean_revenue/total_revenue*100:.1f}%)")
    
    # Show top admin entries
    print("\nTOP ADMIN ENTRIES (by revenue)")
    print("-" * 80)
    print(f"{'Bill Code':<50} | {'Revenue':>12} | {'Spots':>8}")
    print("-" * 80)
    for bill_code, revenue, spots in sorted(admin_entries, key=lambda x: x[1], reverse=True)[:20]:
        print(f"{bill_code[:49]:<50} | ${revenue:>10,.0f} | {spots:>8,}")
    
    # Show top suspicious entries  
    if suspicious_entries:
        print("\nTOP SUSPICIOUS ENTRIES (by revenue)")
        print("-" * 80)
        print(f"{'Bill Code':<50} | {'Revenue':>12} | {'Spots':>8}")
        print("-" * 80)
        for bill_code, revenue, spots in sorted(suspicious_entries, key=lambda x: x[1], reverse=True)[:20]:
            print(f"{bill_code[:49]:<50} | ${revenue:>10,.0f} | {spots:>8,}")

def export_to_csv(categorized: Dict[str, List[Tuple[str, float, int]]], filename: str):
    """Export the analysis to CSV."""
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Category', 'Bill_Code', 'Revenue', 'Spots'])
        
        for category, entries in categorized.items():
            for bill_code, revenue, spots in entries:
                writer.writerow([category, bill_code, revenue, spots])
    
    print(f"\nExported analysis to: {filename}")

def main():
    parser = argparse.ArgumentParser(
        description="Identify administrative/internal entries in bill codes"
    )
    parser.add_argument(
        "--db-path", 
        required=True, 
        help="Path to SQLite database"
    )
    parser.add_argument(
        "--export-csv", 
        help="Export results to CSV file"
    )
    parser.add_argument(
        "--min-revenue",
        type=float,
        default=0,
        help="Only show entries with revenue >= this amount (default: 0)"
    )
    
    args = parser.parse_args()
    
    if not Path(args.db_path).exists():
        print(f"ERROR: Database not found: {args.db_path}")
        return 1
    
    print(f"Analyzing bill codes from: {args.db_path}")
    print("Loading data...")
    
    # Get all bill codes
    bill_codes = get_all_bill_codes(args.db_path)
    
    # Filter by minimum revenue if specified
    if args.min_revenue > 0:
        bill_codes = [(bc, rev, spots) for bc, rev, spots in bill_codes if rev >= args.min_revenue]
        print(f"Filtered to entries with revenue >= ${args.min_revenue:,.2f}")
    
    # Categorize entries
    categorized = identify_admin_entries(bill_codes)
    
    # Print analysis
    print_analysis(categorized)
    
    # Export if requested
    if args.export_csv:
        export_to_csv(categorized, args.export_csv)
    
    print("\nRECOMMENDATIONS:")
    print("1. Review the 'admin' entries - these likely should be excluded from customer matching")
    print("2. Check 'suspicious' entries manually - some may be legitimate, others may be admin")
    print("3. Consider adding a filter to exclude admin entries from your matching algorithm")
    
    return 0

if __name__ == "__main__":
    exit(main())