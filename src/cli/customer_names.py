#!/usr/bin/env python3
"""
Customer Names CLI
Thin wrapper around services.customer_matching.blocking_matcher.
Keeps CLI concerns here; leaves matching logic in the service.
Windows-friendly, no sys.path hacks (module lives under src/).
Usage examples (from repo root):
  # Basic summary + top review/unknown
  python -m src.cli.customer_names --db-path data/database/production.db
  # Export review/unknown to CSV + print alias suggestions
  python -m src.cli.customer_names --db-path data/database/production.db --export-unmatched --suggest-aliases
  # Filter detail list size
  python -m src.cli.customer_names --db-path data/database/production.db --limit 100
  # Tighter alias suggestion thresholds
  python -m src.cli.customer_names --db-path data/database/production.db --alias-min-revenue 2500 --alias-min-score 0.9
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

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


def build_parser():
    """Build and return the argument parser."""
    p = argparse.ArgumentParser(
        prog="customer_names",
        description="Analyze and normalize customer names from bill_code with blocking + fuzzy matching",
    )
    p.add_argument(
        "--db-path",
        required=True,
        help="Path to SQLite DB (e.g., data/database/production.db)",
    )
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
        "--suggest-aliases",
        action="store_true",
        help="Print suggested aliases (no writes)",
    )
    p.add_argument(
        "--alias-min-revenue",
        type=float,
        default=1000.0,
        help="Min revenue to suggest alias (default: 1000)",
    )
    p.add_argument(
        "--alias-min-score",
        type=float,
        default=0.85,
        help="Min score to suggest alias (default: 0.85)",
    )
    return p


def _ensure_db(db_path: str) -> None:
    """Ensure the database file exists."""
    if not Path(db_path).exists():
        _die(f"Database not found: {db_path}")


def _warn_if_no_rapidfuzz() -> None:
    """Warn if rapidfuzz is not available."""
    if not HAVE_RAPIDFUZZ:
        print(
            "Warning: rapidfuzz not installed; fallback scoring is less accurate. `pip install rapidfuzz`"
        )


def _run_analysis(
    db_path: str,
    limit: int,
    do_export: bool,
    export_filename: Optional[str],
    do_suggest: bool,
    alias_min_revenue: float,
    alias_min_score: float,
) -> None:
    """Run the main analysis workflow."""
    matches = analyze_customer_names(db_path, NORMALIZATION_CONFIG)
    s = summarize(matches)
    print_summary(s)
    print_detailed(matches, limit=limit)

    if do_suggest:
        suggest_alias_sql(
            matches, min_revenue=alias_min_revenue, min_score=alias_min_score
        )

    if do_export:
        export_unmatched_csv(matches, export_filename)

    print("\nDone.")


def _die(msg: str) -> None:
    """Print error message and exit."""
    _print_err(msg)
    exit(1)


def _print_err(msg: str) -> None:
    """Print error message to stderr."""
    print(f"ERROR: {msg}")


def run_with_args(args) -> None:
    """Run the CLI with parsed arguments."""
    _ensure_db(args.db_path)
    _warn_if_no_rapidfuzz()
    _run_analysis(
        args.db_path,
        args.limit,
        args.export_unmatched,
        args.export_filename,
        args.suggest_aliases,
        args.alias_min_revenue,
        args.alias_min_score,
    )


def main() -> None:
    """Main entry point."""
    parser = build_parser()
    args = parser.parse_args()
    run_with_args(args)


if __name__ == "__main__":
    main()
