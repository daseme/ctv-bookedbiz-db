#!/usr/bin/env python3
"""
Customer Names CLI with Administrative Entry Filtering

Modified version of customer_names.py that excludes administrative entries
like broker fees, credits, and "DO NOT INVOICE" entries from customer matching.

Usage examples (from repo root):
  # Basic summary with admin filtering
  python -m src.cli.customer_names_filtered --db-path data/database/production.db

  # Show what's being filtered out
  python -m src.cli.customer_names_filtered --db-path data/database/production.db --show-filtered
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional
import sqlite3

from src.services.customer_matching.blocking_matcher import (
    analyze_customer_names,
    summarize,
    print_summary,
    print_detailed,
    export_unmatched_csv,
    suggest_alias_sql,
    NORMALIZATION_CONFIG,
    HAVE_RAPIDFUZZ,
)

# Import our admin filter
from src.cli.admin_filter import should_exclude_from_matching, categorize_bill_code


def get_filtered_bill_codes(db_path: str) -> tuple[list[str], dict]:
    """
    Get all bill codes and filter out administrative entries.

    Returns:
        (clean_bill_codes, filter_stats)
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get all bill codes with revenue for filtering decisions
    query = """
    SELECT DISTINCT 
        bill_code,
        SUM(COALESCE(station_net, COALESCE(gross_rate, 0))) as total_revenue,
        COUNT(*) as spot_count
    FROM spots 
    WHERE bill_code IS NOT NULL 
        AND bill_code != ''
        AND (revenue_type != 'Trade' OR revenue_type IS NULL)
    GROUP BY bill_code
    """

    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()

    # Categorize entries
    admin_entries = []
    suspicious_entries = []
    clean_entries = []

    admin_revenue = 0
    suspicious_revenue = 0
    clean_revenue = 0

    for bill_code, revenue, spot_count in results:
        category = categorize_bill_code(bill_code, revenue, spot_count)

        if category == "admin":
            admin_entries.append((bill_code, revenue, spot_count))
            admin_revenue += revenue
        elif category == "suspicious":
            suspicious_entries.append((bill_code, revenue, spot_count))
            suspicious_revenue += revenue
        else:
            clean_entries.append((bill_code, revenue, spot_count))
            clean_revenue += revenue

    filter_stats = {
        "total_entries": len(results),
        "admin_entries": admin_entries,
        "suspicious_entries": suspicious_entries,
        "clean_entries": clean_entries,
        "admin_revenue": admin_revenue,
        "suspicious_revenue": suspicious_revenue,
        "clean_revenue": clean_revenue,
        "total_revenue": admin_revenue + suspicious_revenue + clean_revenue,
    }

    # Return only clean bill codes for matching
    clean_bill_codes = [entry[0] for entry in clean_entries]

    return clean_bill_codes, filter_stats


def create_filtered_customer_mapping(db_path: str, clean_bill_codes: list[str]) -> None:
    """
    Create a temporary customer mapping table with only clean entries.
    This allows us to run the existing matching algorithm on filtered data.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create a temporary view of spots with only clean bill codes
    bill_code_list = "', '".join(clean_bill_codes)

    cursor.execute("""
        DROP VIEW IF EXISTS spots_filtered_temp
    """)

    cursor.execute(f"""
        CREATE TEMPORARY VIEW spots_filtered_temp AS
        SELECT * FROM spots 
        WHERE bill_code IN ('{bill_code_list}')
    """)

    conn.commit()
    conn.close()


def print_filter_summary(filter_stats: dict) -> None:
    """Print summary of what was filtered out."""
    total = filter_stats["total_entries"]
    admin_count = len(filter_stats["admin_entries"])
    suspicious_count = len(filter_stats["suspicious_entries"])
    clean_count = len(filter_stats["clean_entries"])

    print("=" * 80)
    print("ADMINISTRATIVE FILTERING SUMMARY")
    print("=" * 80)
    print(f"Total entries analyzed:    {total:,}")
    print(
        f"Admin entries (excluded):  {admin_count:,} ({admin_count / total * 100:.1f}%)"
    )
    print(
        f"Suspicious entries (kept): {suspicious_count:,} ({suspicious_count / total * 100:.1f}%)"
    )
    print(
        f"Clean entries (kept):      {clean_count:,} ({clean_count / total * 100:.1f}%)"
    )
    print()

    total_revenue = filter_stats["total_revenue"]
    admin_revenue = filter_stats["admin_revenue"]
    clean_revenue = filter_stats["clean_revenue"]
    suspicious_revenue = filter_stats["suspicious_revenue"]

    print(f"Total revenue:             ${total_revenue:,.2f}")
    print(
        f"Admin revenue (excluded):  ${admin_revenue:,.2f} ({admin_revenue / total_revenue * 100:.1f}%)"
    )
    print(
        f"Clean + Suspicious kept:   ${clean_revenue + suspicious_revenue:,.2f} ({(clean_revenue + suspicious_revenue) / total_revenue * 100:.1f}%)"
    )
    print()

    # Show top excluded admin entries
    admin_entries = sorted(
        filter_stats["admin_entries"], key=lambda x: abs(x[1]), reverse=True
    )
    print("TOP EXCLUDED ADMIN ENTRIES:")
    print("-" * 80)
    print(f"{'Bill Code':<50} | {'Revenue':>12} | {'Spots':>8}")
    print("-" * 80)
    for bill_code, revenue, spots in admin_entries[:10]:
        print(f"{bill_code[:49]:<50} | ${revenue:>10,.0f} | {spots:>8,}")
    print()


def build_parser():
    """Build and return the argument parser."""
    p = argparse.ArgumentParser(
        prog="customer_names_filtered",
        description="Analyze and normalize customer names with administrative entry filtering",
    )
    p.add_argument("--db-path", required=True, help="Path to SQLite DB")
    p.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Max rows in detailed output (default: 50)",
    )
    p.add_argument(
        "--export-unmatched", action="store_true", help="Export review/unknown to CSV"
    )
    p.add_argument("--export-filename", help="Optional CSV filename")
    p.add_argument(
        "--suggest-aliases", action="store_true", help="Print suggested aliases"
    )
    p.add_argument(
        "--alias-min-revenue",
        type=float,
        default=1000.0,
        help="Min revenue for alias suggestions",
    )
    p.add_argument(
        "--alias-min-score",
        type=float,
        default=0.85,
        help="Min score for alias suggestions",
    )
    p.add_argument(
        "--show-filtered", action="store_true", help="Show what was filtered out"
    )
    return p


def _ensure_db(db_path: str) -> None:
    """Ensure the database file exists."""
    if not Path(db_path).exists():
        print(f"ERROR: Database not found: {db_path}")
        exit(1)


def _warn_if_no_rapidfuzz() -> None:
    """Warn if rapidfuzz is not available."""
    if not HAVE_RAPIDFUZZ:
        print(
            "Warning: rapidfuzz not installed; fallback scoring is less accurate. `pip install rapidfuzz`"
        )


def _run_filtered_analysis(
    db_path: str,
    limit: int,
    do_export: bool,
    export_filename: Optional[str],
    do_suggest: bool,
    alias_min_revenue: float,
    alias_min_score: float,
    show_filtered: bool,
) -> None:
    """Run the analysis with administrative filtering."""
    print("Filtering out administrative entries...")

    # Get filtered bill codes
    clean_bill_codes, filter_stats = get_filtered_bill_codes(db_path)

    if show_filtered:
        print_filter_summary(filter_stats)
    else:
        admin_count = len(filter_stats["admin_entries"])
        total_count = filter_stats["total_entries"]
        print(
            f"Excluded {admin_count} admin entries ({admin_count / total_count * 100:.1f}% of total)"
        )
        print()

    # Note: For now, we'll run the analysis on the full dataset but could modify
    # the matching algorithm to skip admin entries. This is a proof of concept.
    print("Running customer matching analysis...")
    matches = analyze_customer_names(db_path, NORMALIZATION_CONFIG)

    # Filter out admin entries from results (post-processing approach)
    filtered_matches = []
    for match in matches:
        # CustomerMatch objects have attributes, not dict-style access
        bill_code_name = getattr(match, "bill_code_name_raw", "") or getattr(
            match, "original_name", ""
        )
        if not should_exclude_from_matching(bill_code_name):
            filtered_matches.append(match)

    print(f"Matches before filtering: {len(matches)}")
    print(f"Matches after filtering: {len(filtered_matches)}")
    print()

    # Run analysis on filtered results
    s = summarize(filtered_matches)
    print_summary(s)
    print_detailed(filtered_matches, limit=limit)

    if do_suggest:
        suggest_alias_sql(
            filtered_matches, min_revenue=alias_min_revenue, min_score=alias_min_score
        )

    if do_export:
        export_unmatched_csv(filtered_matches, export_filename)

    print("\nDone.")


def run_with_args(args) -> None:
    """Run the CLI with parsed arguments."""
    _ensure_db(args.db_path)
    _warn_if_no_rapidfuzz()
    _run_filtered_analysis(
        args.db_path,
        args.limit,
        args.export_unmatched,
        args.export_filename,
        args.suggest_aliases,
        args.alias_min_revenue,
        args.alias_min_score,
        args.show_filtered,
    )


def main() -> None:
    """Main entry point."""
    parser = build_parser()
    args = parser.parse_args()
    run_with_args(args)


if __name__ == "__main__":
    main()
