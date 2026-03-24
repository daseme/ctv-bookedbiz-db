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
