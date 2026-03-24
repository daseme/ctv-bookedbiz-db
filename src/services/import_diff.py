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
            COALESCE(contract, '') AS contract,
            broadcast_month,
            CAST(ROUND(SUM(COALESCE(spot_value, 0)) * 100, 0) AS INTEGER) AS sum_cents,
            COUNT(*) AS row_count
        FROM spots
        WHERE broadcast_month IN ({placeholders})
        GROUP BY bill_code, COALESCE(contract, ''), broadcast_month
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

        bill_code = row[_COL_BILL_CODE] or ""
        contract = row[_COL_CONTRACT] if len(row) > _COL_CONTRACT else None
        contract = contract or ""

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
