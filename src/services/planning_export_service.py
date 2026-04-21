"""
Service for the /api/revenue/planning-export endpoint.

Emits one row per (ae1, broadcast_month) for the requested year, carrying
budget, forecast, and booked amounts plus the three derived fields
(expected, pipeline, vs_budget) the workbook needs to render its
planning view without re-implementing Kurt's semantic.

See docs/planning-export-client-contract.md for the client contract,
and §8 of that doc for the four places this intentionally diverges
from the in-app /planning/ page.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

SCHEMA_VERSION = "1.0"

# Mmm abbrev → month number. Used for Mmm-YY → ISO conversion.
_MONTH_MAP = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}


# ---------------------------------------------------------------------------
# Derivation helpers — pure, null-handling per client contract §3.
# ---------------------------------------------------------------------------


def compute_expected(
    booked: float,
    forecast: Optional[float],
    budget: Optional[float],
) -> float:
    """max(booked, COALESCE(forecast, budget)). booked is always numeric."""
    plan = forecast if forecast is not None else budget
    if plan is None:
        return booked
    return max(booked, plan)


def compute_pipeline(
    booked: float,
    forecast: Optional[float],
    budget: Optional[float],
) -> Optional[float]:
    """max(0, COALESCE(forecast, budget) − booked). Floored. Null if no plan."""
    plan = forecast if forecast is not None else budget
    if plan is None:
        return None
    return max(0.0, plan - booked)


def compute_vs_budget(
    booked: float,
    budget: Optional[float],
) -> Optional[float]:
    """booked − budget, signed. Null when budget is null."""
    if budget is None:
        return None
    return booked - budget


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class PlanningExportService:
    """Shapes budget/forecast/booked data for the workbook's planning view."""

    def __init__(self, database_connection):
        self._db = database_connection

    def get_rows(self, year: Optional[int] = None) -> Dict[str, Any]:
        """Return {"metadata": {...}, "rows": [...]} for the given year."""
        if year is None:
            year = date.today().year
        rows = self._query(year)
        return {
            "metadata": {
                "generated_at": datetime.now(timezone.utc).isoformat(
                    timespec="seconds"
                ).replace("+00:00", "Z"),
                "schema_version": SCHEMA_VERSION,
                "year": year,
                "row_count": len(rows),
            },
            "rows": rows,
        }

    # ---- internals ----

    def _query(self, year: int) -> List[Dict[str, Any]]:
        """Build the planning-export rows for a single year."""
        with self._db.connection() as conn:
            entities = self._fetch_active_entities(conn)
            budgets = self._fetch_budgets(conn, year)
            forecasts = self._fetch_forecasts(conn, year)
            booked = self._fetch_booked(conn, year)

        rows: List[Dict[str, Any]] = []
        year_suffix = str(year)[2:]

        for ae_name in entities:
            for month in range(1, 13):
                budget_val = budgets.get((ae_name, month))
                forecast_row = forecasts.get((ae_name, month))
                booked_val = booked.get((ae_name, month), 0.0)

                # Emission rule: at least one of budget row, forecast row,
                # booked > 0 must be present.
                has_activity = (
                    budget_val is not None
                    or forecast_row is not None
                    or booked_val > 0
                )
                if not has_activity:
                    continue

                forecast_val = (
                    forecast_row["amount"] if forecast_row is not None else None
                )
                new_accts = (
                    forecast_row["new_accts"] if forecast_row is not None else None
                )
                new_dollars = (
                    forecast_row["new_dollars"] if forecast_row is not None else None
                )

                # Sanity check: broadcast_month in the emitted row uses ISO
                # first-of-month, consistent with sheet-export.
                iso_month = f"{year:04d}-{month:02d}-01"

                rows.append({
                    "ae1": ae_name,
                    "broadcast_month": iso_month,
                    "budget": budget_val,
                    "forecast": forecast_val,
                    "booked": booked_val,
                    "new_accts": new_accts,
                    "new_dollars": new_dollars,
                    "expected": compute_expected(booked_val, forecast_val, budget_val),
                    "pipeline": compute_pipeline(booked_val, forecast_val, budget_val),
                    "vs_budget": compute_vs_budget(booked_val, budget_val),
                })

        # Unused to silence linter if the helper isn't exercised.
        _ = year_suffix
        return rows

    # ---- DB reads ----

    def _fetch_active_entities(self, conn) -> List[str]:
        """Ordered list of active revenue entity names."""
        cursor = conn.execute(
            """
            SELECT entity_name
            FROM revenue_entities
            WHERE is_active = 1
            ORDER BY entity_name
            """
        )
        return [r["entity_name"] for r in cursor.fetchall()]

    def _fetch_budgets(
        self, conn, year: int
    ) -> Dict[Tuple[str, int], float]:
        """{(ae_name, month): budget_amount} for the year."""
        cursor = conn.execute(
            """
            SELECT ae_name, month, budget_amount
            FROM budget
            WHERE year = ?
            """,
            (year,),
        )
        return {
            (r["ae_name"], r["month"]): float(r["budget_amount"])
            for r in cursor.fetchall()
        }

    def _fetch_forecasts(
        self, conn, year: int
    ) -> Dict[Tuple[str, int], Dict[str, Any]]:
        """{(ae_name, month): {amount, new_accts, new_dollars}} for the year."""
        cursor = conn.execute(
            """
            SELECT ae_name, month, forecast_amount,
                   new_accounts_forecast, new_dollars_forecast
            FROM forecast
            WHERE year = ?
            """,
            (year,),
        )
        out: Dict[Tuple[str, int], Dict[str, Any]] = {}
        for r in cursor.fetchall():
            new_dollars = r["new_dollars_forecast"]
            out[(r["ae_name"], r["month"])] = {
                "amount": float(r["forecast_amount"]),
                "new_accts": r["new_accounts_forecast"],
                "new_dollars": (
                    float(new_dollars) if new_dollars is not None else None
                ),
            }
        return out

    def _fetch_booked(
        self, conn, year: int
    ) -> Dict[Tuple[str, int], float]:
        """{(ae_name, month_number): booked} covering WorldLink/House/regular.

        Mirrors planning_repository.get_booked_revenue (src/repositories/
        planning_repository.py:435-548):
          - WorldLink:  bill_code LIKE 'WorldLink%'  (ignores sales_person)
          - House:      sales_person='House' AND bill_code NOT LIKE 'WorldLink%'
          - Regular AE: sales_person = :ae (no bill_code filter)

        One UNION query returns (ae_name, broadcast_month_raw, booked) for
        every AE. Python then indexes by (ae_name, month_number).
        """
        year_suffix = str(year)[2:]
        sql = """
            SELECT 'WorldLink' AS ae_name, broadcast_month AS bm,
                   COALESCE(SUM(gross_rate), 0) AS booked
            FROM spots
            WHERE bill_code LIKE 'WorldLink%'
              AND SUBSTR(broadcast_month, 5, 2) = ?
              AND (revenue_type != 'Trade' OR revenue_type IS NULL)
            GROUP BY broadcast_month

            UNION ALL

            SELECT 'House' AS ae_name, broadcast_month AS bm,
                   COALESCE(SUM(gross_rate), 0) AS booked
            FROM spots
            WHERE sales_person = 'House'
              AND bill_code NOT LIKE 'WorldLink%'
              AND SUBSTR(broadcast_month, 5, 2) = ?
              AND (revenue_type != 'Trade' OR revenue_type IS NULL)
            GROUP BY broadcast_month

            UNION ALL

            SELECT sales_person AS ae_name, broadcast_month AS bm,
                   COALESCE(SUM(gross_rate), 0) AS booked
            FROM spots
            WHERE sales_person IS NOT NULL
              AND sales_person NOT IN ('House', 'WorldLink')
              AND SUBSTR(broadcast_month, 5, 2) = ?
              AND (revenue_type != 'Trade' OR revenue_type IS NULL)
            GROUP BY sales_person, broadcast_month
        """
        cursor = conn.execute(sql, (year_suffix, year_suffix, year_suffix))
        out: Dict[Tuple[str, int], float] = {}
        for r in cursor.fetchall():
            bm = r["bm"]
            month_abbrev = bm[:3] if bm and len(bm) >= 3 else None
            month_num = _MONTH_MAP.get(month_abbrev)
            if month_num is None:
                # Defensive: skip malformed broadcast_month rather than blow up
                # the whole endpoint. Importer triggers should prevent this.
                logger.warning("Skipping spot with malformed broadcast_month: %r", bm)
                continue
            # Aggregate in case both regular-AE and House/WorldLink branches
            # ever hit the same (ae_name, month) — shouldn't, but defensive.
            key = (r["ae_name"], month_num)
            out[key] = out.get(key, 0.0) + float(r["booked"])
        return out
