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

from services.customer_matching.blocking_matcher import (
    analyze_customer_names,
    summarize,
    print_summary,
    print_detailed,
    export_unmatched_csv,
    suggest_alias_sql,
    NORMALIZATION_CONFIG,
    HAVE_RAPIDFUZZ,
)


build_parser = lambda: (
    (p := argparse.ArgumentParser(
        prog="customer_names",
        description="Analyze and normalize customer names from bill_code with blocking + fuzzy matching",
    ))
    .add_argument("--db-path", required=True, help="Path to SQLite DB (e.g., data/database/production.db)") or
    p.add_argument("--limit", type=int, default=50, help="Max rows in detailed output (default: 50)") or
    p.add_argument("--export-unmatched", action="store_true", help="Export review/unknown to CSV") or
    p.add_argument("--export-filename", help="Optional CSV filename") or
    p.add_argument("--suggest-aliases", action="store_true", help="Print suggested aliases (no writes)") or
    p.add_argument("--alias-min-revenue", type=float, default=1000.0, help="Min revenue to suggest alias (default: 1000)") or
    p.add_argument("--alias-min-score", type=float, default=0.85, help="Min score to suggest alias (default: 0.85)") or
    p
)


run_with_args = lambda args: (
    _ensure_db(args.db_path),
    _warn_if_no_rapidfuzz(),
    _run_analysis(args.db_path, args.limit, args.export_unmatched, args.export_filename,
                  args.suggest_aliases, args.alias_min_revenue, args.alias_min_score)
)


_ensure_db = lambda db: (
    (None if Path(db).exists() else _die(f"Database not found: {db}"))
)

_warn_if_no_rapidfuzz = lambda: (
    print("Warning: rapidfuzz not installed; fallback scoring is less accurate. `pip install rapidfuzz`") if not HAVE_RAPIDFUZZ else None
)


def _run_analysis(db_path: str,
                  limit: int,
                  do_export: bool,
                  export_filename: Optional[str],
                  do_suggest: bool,
                  alias_min_revenue: float,
                  alias_min_score: float) -> None:
    matches = analyze_customer_names(db_path, NORMALIZATION_CONFIG)
    s = summarize(matches)
    print_summary(s)
    print_detailed(matches, limit=limit)

    if do_suggest:
        suggest_alias_sql(matches, min_revenue=alias_min_revenue, min_score=alias_min_score)

    if do_export:
        export_unmatched_csv(matches, export_filename)

    print("\nDone.")


_die = lambda msg: (_print_err(msg), exit(1))
_print_err = lambda msg: print(f"ERROR: {msg}")


main = lambda: run_with_args(build_parser().parse_args())


if __name__ == "__main__":
    main()
