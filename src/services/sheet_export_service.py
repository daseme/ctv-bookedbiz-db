"""
Service for the /api/revenue/sheet-export endpoint.

Emits long-format revenue rows grouped by the display tuple
(customer, market, revenue_class, ae1, agency_flag, sector) × broadcast_month.

See docs/superpowers/specs/2026-04-20-revenue-sheet-export-design.md §5
for the spec and §6.6 for the hash version compatibility contract.
See docs/sheet-export-client-contract.md for the v1.1 client contract.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

HASH_VERSION = "v1"
SCHEMA_VERSION = "1.1"

# Contract values that indicate "no contract" and must be excluded from the
# representative-spot selection. See client contract §5.
_SENTINEL_CONTRACTS = (None, "", "N")

# Mmm -> month number, for Mmm-YY -> ISO conversion and for ORDER BY chronological.
_MONTH_MAP = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}

_UNIT_SEPARATOR = "\x1f"


def row_hash(
    customer: Any,
    market: Any,
    revenue_class: Any,
    ae1: Any,
    agency_flag: Any,
    sector: Any,
) -> str:
    """Compute the v1 row identity hash.

    Must match the client-side Power Query implementation in fnRowHash.pq.
    See client contract §3 for the full algorithm and §4 for pinned vectors.
    """

    def norm(x: Any) -> str:
        return "" if x is None else str(x).strip().lower()

    joined = _UNIT_SEPARATOR.join(
        norm(f) for f in (customer, market, revenue_class, ae1, agency_flag, sector)
    )
    return hashlib.sha1(joined.encode("utf-8")).hexdigest()


def _broadcast_month_to_iso(bm: str) -> str:
    """Convert 'Jan-25' to '2025-01-01'. Raises ValueError on any malformed input.

    All corruption paths (wrong length, unknown month name, non-numeric year)
    converge to a single ValueError with a consistent message so callers can
    catch one exception type.
    """
    if not bm or len(bm) != 6 or bm[3] != "-":
        raise ValueError(f"Malformed broadcast_month: {bm!r}")
    month_name, year_suffix = bm[:3], bm[4:]
    try:
        month_num = _MONTH_MAP[month_name]
        # 2-digit years: assume 2000s (earliest data is 2022 per design doc §5).
        year = 2000 + int(year_suffix)
    except (KeyError, ValueError) as e:
        raise ValueError(f"Malformed broadcast_month: {bm!r}") from e
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
        """Return {"metadata": {...}, "rows": [...]}. See client contract §2/§6."""
        rows = self._query(start_month, end_month)
        return {
            "metadata": {
                "generated_at": datetime.now(timezone.utc).isoformat(
                    timespec="seconds"
                ).replace("+00:00", "Z"),
                "start_month": start_month,
                "end_month": end_month,
                "hash_version": HASH_VERSION,
                "schema_version": SCHEMA_VERSION,
                "row_hash_source": "server",
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

        For v1.1, also selects one "representative spot" per 6-field tuple —
        the non-sentinel-contract spot with the latest broadcast_month
        (tiebreak spot_id DESC) — and carries its broker_fees / gross_rate
        onto every monthly row of the tuple. Tuples with only sentinel
        contracts emit null broker fields. See client contract §5.
        """
        # Group/partition on NORMALIZED identity keys (lower+trim, matching
        # the row_hash function) so case or whitespace drift in the source
        # data (e.g., "iGRAPHIX" vs "iGraphix", "Riley Van Patten" vs
        # "Riley van Patten") collapses to one emitted row. Display casing
        # is resolved deterministically at the tuple level in tuple_display
        # so it stays consistent across months of the same row_hash.
        sql = """
            WITH base AS (
                SELECT
                    s.spot_id                                                  AS spot_id,
                    s.bill_code                                                AS bill_code,
                    m.market_code                                              AS market_code,
                    s.revenue_type                                             AS revenue_type,
                    s.sales_person                                             AS sales_person,
                    CASE WHEN s.agency_flag = 'Agency' THEN 'Y' ELSE 'N' END   AS agency_flag,
                    sect.sector_name                                           AS sector_name,
                    s.broadcast_month                                          AS broadcast_month_raw,
                    s.contract                                                 AS contract,
                    s.gross_rate                                               AS gross_rate,
                    s.station_net                                              AS station_net,
                    s.broker_fees                                              AS broker_fees,
                    -- Normalized keys. Null → '' to match the Python
                    -- row_hash rule (see row_hash() above).
                    COALESCE(LOWER(TRIM(s.bill_code)), '')                     AS customer_key,
                    COALESCE(LOWER(TRIM(m.market_code)), '')                   AS market_key,
                    COALESCE(LOWER(TRIM(s.revenue_type)), '')                  AS revenue_class_key,
                    COALESCE(LOWER(TRIM(s.sales_person)), '')                  AS ae1_key,
                    COALESCE(LOWER(TRIM(sect.sector_name)), '')                AS sector_key
                FROM spots s
                LEFT JOIN customers c    ON s.customer_id = c.customer_id
                LEFT JOIN sectors   sect ON c.sector_id = sect.sector_id
                LEFT JOIN markets   m    ON s.market_id = m.market_id
                WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
            ),
            tuple_display AS (
                -- One canonical display casing per normalized tuple.
                -- MIN() is deterministic across refreshes. In ASCII,
                -- uppercase sorts before lowercase, so "iGRAPHIX" wins
                -- over "iGraphix" when both appear for the same tuple.
                SELECT
                    customer_key, market_key, revenue_class_key,
                    ae1_key, agency_flag, sector_key,
                    MIN(bill_code)     AS customer,
                    MIN(market_code)   AS market,
                    MIN(revenue_type)  AS revenue_class,
                    MIN(sales_person)  AS ae1,
                    MIN(sector_name)   AS sector
                FROM base
                GROUP BY customer_key, market_key, revenue_class_key,
                         ae1_key, agency_flag, sector_key
            ),
            representative AS (
                SELECT
                    customer_key, market_key, revenue_class_key,
                    ae1_key, agency_flag, sector_key,
                    broker_fees AS rep_broker_fees,
                    gross_rate  AS rep_gross_rate
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY customer_key, market_key, revenue_class_key,
                                         ae1_key, agency_flag, sector_key
                            ORDER BY
                                SUBSTR(broadcast_month_raw, 5, 2) DESC,
                                CASE SUBSTR(broadcast_month_raw, 1, 3)
                                    WHEN 'Jan' THEN  1 WHEN 'Feb' THEN  2
                                    WHEN 'Mar' THEN  3 WHEN 'Apr' THEN  4
                                    WHEN 'May' THEN  5 WHEN 'Jun' THEN  6
                                    WHEN 'Jul' THEN  7 WHEN 'Aug' THEN  8
                                    WHEN 'Sep' THEN  9 WHEN 'Oct' THEN 10
                                    WHEN 'Nov' THEN 11 WHEN 'Dec' THEN 12
                                END DESC,
                                spot_id DESC
                        ) AS rn
                    FROM base
                    WHERE contract IS NOT NULL
                      AND contract != ''
                      AND contract != 'N'
                )
                WHERE rn = 1
            ),
            aggregated AS (
                SELECT
                    customer_key, market_key, revenue_class_key,
                    ae1_key, agency_flag, sector_key,
                    broadcast_month_raw,
                    SUM(gross_rate)  AS gross_rate,
                    SUM(station_net) AS station_net,
                    SUM(broker_fees) AS broker_fees
                FROM base
                GROUP BY customer_key, market_key, revenue_class_key,
                         ae1_key, agency_flag, sector_key,
                         broadcast_month_raw
                HAVING COALESCE(SUM(gross_rate), 0)  <> 0
                    OR COALESCE(SUM(station_net), 0) <> 0
                    OR COALESCE(SUM(broker_fees), 0) <> 0
            )
            SELECT
                d.customer, d.market, d.revenue_class, d.ae1,
                a.agency_flag, d.sector,
                a.broadcast_month_raw,
                a.gross_rate, a.station_net, a.broker_fees,
                r.rep_broker_fees, r.rep_gross_rate
            FROM aggregated a
            JOIN tuple_display d
              USING (customer_key, market_key, revenue_class_key,
                     ae1_key, agency_flag, sector_key)
            LEFT JOIN representative r
              USING (customer_key, market_key, revenue_class_key,
                     ae1_key, agency_flag, sector_key)
            ORDER BY d.customer, d.market, d.revenue_class, d.ae1,
                a.agency_flag, d.sector,
                CASE SUBSTR(a.broadcast_month_raw, 1, 3)
                    WHEN 'Jan' THEN  1 WHEN 'Feb' THEN  2 WHEN 'Mar' THEN  3
                    WHEN 'Apr' THEN  4 WHEN 'May' THEN  5 WHEN 'Jun' THEN  6
                    WHEN 'Jul' THEN  7 WHEN 'Aug' THEN  8 WHEN 'Sep' THEN  9
                    WHEN 'Oct' THEN 10 WHEN 'Nov' THEN 11 WHEN 'Dec' THEN 12
                END,
                SUBSTR(a.broadcast_month_raw, 5, 2)
        """
        with self._db.connection() as conn:
            cursor = conn.execute(sql)
            out: List[Dict[str, Any]] = []
            for r in cursor.fetchall():
                rep_broker_fees = r["rep_broker_fees"]
                rep_gross_rate = r["rep_gross_rate"]
                if rep_broker_fees is None:
                    # Tuple has only sentinel contracts — no attributable broker data.
                    broker_yn: Optional[str] = None
                    broker_pct: Optional[float] = None
                else:
                    broker_yn = "Y" if rep_broker_fees > 0 else "N"
                    if rep_gross_rate and rep_gross_rate > 0:
                        broker_pct = float(rep_broker_fees) / float(rep_gross_rate)
                    else:
                        broker_pct = None
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
                    "broker_yn":       broker_yn,
                    "broker_pct":      broker_pct,
                    "row_hash": row_hash(
                        r["customer"], r["market"], r["revenue_class"],
                        r["ae1"], r["agency_flag"], r["sector"],
                    ),
                })
            return out
