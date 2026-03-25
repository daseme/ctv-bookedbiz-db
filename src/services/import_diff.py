"""Diff-based import utilities for contract-group fingerprinting.

Compares (bill_code, contract, broadcast_month) groups between
Excel source data and the SQLite database to identify what actually
changed, avoiding unnecessary delete-and-reinsert cycles.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import Dict, List, Tuple

# Type aliases
GroupKey = Tuple[str, str, str]  # (bill_code, contract, broadcast_month)
Fingerprint = Tuple[int, int]   # (sum_cents, row_count)


def build_db_fingerprints(
    months: List[str], conn: sqlite3.Connection
) -> Dict[GroupKey, Fingerprint]:
    """Build fingerprints from DB spots grouped by (bill_code, contract, month).

    Returns a dict mapping each group key to (sum_cents, row_count).
    """
    if not months:
        return {}

    placeholders = ",".join("?" * len(months))
    sql = f"""
        SELECT
            bill_code,
            CASE WHEN contract IS NULL OR contract = '' OR contract = '0' THEN '' ELSE contract END AS contract,
            broadcast_month,
            CAST(SUM(CAST(ROUND(COALESCE(spot_value, 0) * 100, 0) AS INTEGER)) AS INTEGER) AS sum_cents,
            COUNT(*) AS row_count
        FROM spots
        WHERE broadcast_month IN ({placeholders})
        GROUP BY bill_code, CASE WHEN contract IS NULL OR contract = '' OR contract = '0' THEN '' ELSE contract END, broadcast_month
    """
    cursor = conn.execute(sql, months)
    return {
        (row[0], row[1], row[2]): (row[3], row[4])
        for row in cursor.fetchall()
    }


# Column indices matching EXCEL_COLUMN_POSITIONS
_COL_BILL_CODE = 0
_COL_SPOT_VALUE = 17
_COL_BROADCAST_MONTH = 18
_COL_REVENUE_TYPE = 23
_COL_CONTRACT = 27


def build_excel_fingerprints(
    rows: List[Tuple],
) -> Tuple[Dict[GroupKey, Fingerprint], Dict[GroupKey, List[Tuple]], set]:
    """Build fingerprints from raw Excel rows.

    Returns:
        (fingerprints, grouped_rows, months_found)
        - fingerprints: GroupKey → (sum_cents, row_count)
        - grouped_rows: GroupKey → list of raw rows (preserving sheet tag)
        - months_found: set of broadcast_month strings seen
    """
    from collections import defaultdict
    from src.services.import_integration_utilities import _parse_month_value

    grouped_rows: Dict[GroupKey, List[Tuple]] = defaultdict(list)
    sums: Dict[GroupKey, int] = defaultdict(int)
    counts: Dict[GroupKey, int] = defaultdict(int)
    months_found: set = set()

    for raw_row in rows:
        # Strip sheet-name tag for column access, but preserve full row
        row = raw_row[:30] if len(raw_row) > 30 else raw_row

        # Parse broadcast month — skip rows without one
        month_val = row[_COL_BROADCAST_MONTH] if len(row) > _COL_BROADCAST_MONTH else None
        month = _parse_month_value(month_val)
        if not month:
            continue

        # Skip Trade rows — excluded by CHECK constraint on spots table
        rev_type = row[_COL_REVENUE_TYPE] if len(row) > _COL_REVENUE_TYPE else None
        if rev_type and str(rev_type).strip() == "Trade":
            continue

        bill_code = str(row[_COL_BILL_CODE]).strip() if row[_COL_BILL_CODE] else ""
        contract = row[_COL_CONTRACT] if len(row) > _COL_CONTRACT else None
        contract = str(contract).strip() if contract else ""
        if contract == "0":
            contract = ""

        # Convert spot_value to integer cents
        raw_value = row[_COL_SPOT_VALUE] if len(row) > _COL_SPOT_VALUE else None
        cents = round(float(raw_value) * 100) if raw_value is not None else 0

        key: GroupKey = (bill_code, contract, month)
        grouped_rows[key].append(raw_row)
        sums[key] += cents
        counts[key] += 1
        months_found.add(month)

    fingerprints = {key: (sums[key], counts[key]) for key in sums}
    return dict(fingerprints), dict(grouped_rows), months_found


@dataclass
class DiffResult:
    """Result of comparing Excel vs DB fingerprints."""

    unchanged: set = field(default_factory=set)  # GroupKeys that match
    changed: set = field(default_factory=set)     # Different fingerprint
    added: set = field(default_factory=set)        # Only in Excel
    removed: set = field(default_factory=set)      # Only in DB
    should_fallback: bool = False                  # >threshold changed


def compare_fingerprints(
    excel_fps: Dict[GroupKey, Fingerprint],
    db_fps: Dict[GroupKey, Fingerprint],
    fallback_threshold: float = 0.80,
) -> DiffResult:
    """Compare Excel and DB fingerprints to find what changed.

    The fallback_threshold is evaluated against groups present in BOTH
    sides (unchanged + changed), not purely added/removed groups.
    """
    excel_keys = set(excel_fps.keys())
    db_keys = set(db_fps.keys())

    added = excel_keys - db_keys
    removed = db_keys - excel_keys
    common = excel_keys & db_keys

    unchanged = set()
    changed = set()

    # Tolerance: 10 cents ($0.10) — floating-point rounding between
    # Python and SQLite can produce small differences that aren't real changes
    CENTS_TOLERANCE = 10

    for key in common:
        excel_cents, excel_count = excel_fps[key]
        db_cents, db_count = db_fps[key]
        if excel_count == db_count and abs(excel_cents - db_cents) <= CENTS_TOLERANCE:
            unchanged.add(key)
        else:
            changed.add(key)

    # Fallback: if most overlapping groups changed, something systemic happened
    overlap_count = len(unchanged) + len(changed)
    if overlap_count > 0 and len(changed) / overlap_count > fallback_threshold:
        should_fallback = True
    else:
        should_fallback = False

    return DiffResult(
        unchanged=unchanged,
        changed=changed,
        added=added,
        removed=removed,
        should_fallback=should_fallback,
    )
