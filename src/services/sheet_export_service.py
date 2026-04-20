"""
Service for the /api/revenue/sheet-export endpoint.

Emits long-format revenue rows grouped by the display tuple
(customer, market, revenue_class, ae1, agency_flag, sector) × broadcast_month.

See docs/superpowers/specs/2026-04-20-revenue-sheet-export-design.md §5
for the spec and §6.6 for the hash version compatibility contract.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

HASH_VERSION = "v1"

# Mmm -> month number, for Mmm-YY -> ISO conversion.
_MONTH_MAP = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}


def _broadcast_month_to_iso(bm: str) -> str:
    """Convert 'Jan-25' to '2025-01-01'. Raises on malformed input."""
    if not bm or len(bm) != 6 or bm[3] != "-":
        raise ValueError(f"Malformed broadcast_month: {bm!r}")
    month_name, year_suffix = bm[:3], bm[4:]
    month_num = _MONTH_MAP[month_name]
    # 2-digit years: assume 2000s (earliest data is 2022 per design doc §5).
    year = 2000 + int(year_suffix)
    return f"{year:04d}-{month_num:02d}-01"


class SheetExportService:
    """Pulls the sheet-export dataset from the DB and shapes it for JSON."""

    def __init__(self, database_connection):
        self._db = database_connection

    def get_rows(
        self,
        start_month: Optional[str] = None,
        end_month: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Return {"metadata": {...}, "rows": [...]}. See spec §5."""
        rows = self._query(start_month, end_month)
        return {
            "metadata": {
                "generated_at": datetime.now(timezone.utc).isoformat(
                    timespec="seconds"
                ).replace("+00:00", "Z"),
                "start_month": start_month,
                "end_month": end_month,
                "hash_version": HASH_VERSION,
                "row_count": len(rows),
            },
            "rows": rows,
        }

    def _query(
        self,
        start_month: Optional[str],
        end_month: Optional[str],
    ) -> List[Dict[str, Any]]:
        """Run the GROUP BY aggregation and shape each row.

        Bypasses spots_reporting view because it doesn't expose agency_flag
        (see 2026-04-20-db-schema-audit.md, Schema Surprise #1).
        """
        sql = """
            SELECT
              s.bill_code                                                AS customer,
              m.market_code                                              AS market,
              s.revenue_type                                             AS revenue_class,
              s.sales_person                                             AS ae1,
              CASE WHEN s.agency_flag = 'Agency' THEN 'Y' ELSE 'N' END   AS agency_flag,
              sect.sector_name                                           AS sector,
              s.broadcast_month                                          AS broadcast_month_raw,
              SUM(s.gross_rate)                                          AS gross_rate,
              SUM(s.station_net)                                         AS station_net,
              SUM(s.broker_fees)                                         AS broker_fees
            FROM spots s
            LEFT JOIN customers c   ON s.customer_id = c.customer_id
            LEFT JOIN sectors   sect ON c.sector_id = sect.sector_id
            LEFT JOIN markets   m   ON s.market_id = m.market_id
            WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
            GROUP BY 1, 2, 3, 4, 5, 6, 7
            HAVING COALESCE(SUM(s.gross_rate),0) <> 0
                OR COALESCE(SUM(s.station_net),0) <> 0
                OR COALESCE(SUM(s.broker_fees),0) <> 0
            ORDER BY 1, 2, 3, 4, 5, 6,
              CASE SUBSTR(s.broadcast_month, 1, 3)
                WHEN 'Jan' THEN 1  WHEN 'Feb' THEN 2  WHEN 'Mar' THEN 3
                WHEN 'Apr' THEN 4  WHEN 'May' THEN 5  WHEN 'Jun' THEN 6
                WHEN 'Jul' THEN 7  WHEN 'Aug' THEN 8  WHEN 'Sep' THEN 9
                WHEN 'Oct' THEN 10 WHEN 'Nov' THEN 11 WHEN 'Dec' THEN 12
              END,
              SUBSTR(s.broadcast_month, 5, 2)
        """
        with self._db.connection() as conn:
            cursor = conn.execute(sql)
            out: List[Dict[str, Any]] = []
            for r in cursor.fetchall():
                out.append({
                    "customer":        r["customer"],
                    "market":          r["market"],
                    "revenue_class":   r["revenue_class"],
                    "ae1":             r["ae1"],
                    "agency_flag":     r["agency_flag"],
                    "sector":          r["sector"],
                    "broadcast_month": _broadcast_month_to_iso(
                        r["broadcast_month_raw"]
                    ),
                    "gross_rate":      float(r["gross_rate"] or 0),
                    "station_net":     float(r["station_net"] or 0),
                    "broker_fees":     float(r["broker_fees"] or 0),
                })
            return out
