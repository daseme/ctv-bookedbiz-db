"""
Planning Repository - Data access layer for forecast planning.

Handles all database operations for:
- Revenue entities (AEs, House, Agencies)
- Forecast data (adjusted expectations)
- Booked revenue (from spots)
- Budget data
"""

import sqlite3
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime
from decimal import Decimal

from src.database.connection import DatabaseConnection
from src.services.base_service import BaseService
from src.models.planning import (
    RevenueEntity,
    EntityType,
    PlanningPeriod,
    PlanningRow,
    Money,
    ForecastUpdate,
    ForecastChange,
)

logger = logging.getLogger(__name__)


class PlanningRepository(BaseService):
    """Repository for planning-related data access."""

    def __init__(self, db_connection: DatabaseConnection):
        super().__init__(db_connection)

    # =========================================================================
    # Revenue Entities
    # =========================================================================

    def get_active_revenue_entities(self) -> List[RevenueEntity]:
        """Get all active revenue entities."""
        with self.safe_connection() as conn:
            cursor = conn.execute("""
                SELECT entity_id, entity_name, entity_type, is_active, notes
                FROM revenue_entities
                WHERE is_active = 1
                ORDER BY 
                    CASE entity_type 
                        WHEN 'AE' THEN 1 
                        WHEN 'House' THEN 2 
                        WHEN 'Agency' THEN 3 
                    END,
                    entity_name
            """)

            return [self._row_to_revenue_entity(row) for row in cursor.fetchall()]

    def get_revenue_entity_by_name(self, name: str) -> Optional[RevenueEntity]:
        """Get revenue entity by name."""
        with self.safe_connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM revenue_entities WHERE entity_name = ?", (name,)
            )
            row = cursor.fetchone()
            return self._row_to_revenue_entity(row) if row else None

    def upsert_revenue_entity(
        self, entity_name: str, entity_type: EntityType = EntityType.AE
    ) -> RevenueEntity:
        """Insert or update a revenue entity."""
        with self.safe_transaction() as conn:
            cursor = conn.execute(
                """
                INSERT INTO revenue_entities (entity_name, entity_type)
                VALUES (?, ?)
                ON CONFLICT(entity_name) DO UPDATE SET
                    entity_type = excluded.entity_type,
                    is_active = 1
                RETURNING entity_id, entity_name, entity_type, is_active, notes
            """,
                (entity_name, entity_type.value),
            )

            row = cursor.fetchone()
            return self._row_to_revenue_entity(row)

    def get_unmatched_revenue(self, year: int) -> List[Dict[str, Any]]:
        """Get revenue from sales_person values not in revenue_entities."""
        with self.safe_connection() as conn:
            year_suffix = str(year)[2:]
            cursor = conn.execute(
                """
                SELECT 
                    s.sales_person,
                    COUNT(*) AS spot_count,
                    SUM(s.gross_rate) AS total_revenue
                FROM spots s
                LEFT JOIN revenue_entities re ON re.entity_name = s.sales_person
                WHERE re.entity_id IS NULL
                  AND s.broadcast_month LIKE ?
                  AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
                  AND s.sales_person IS NOT NULL 
                  AND s.sales_person != ''
                GROUP BY s.sales_person
                ORDER BY total_revenue DESC
            """,
                (f"%-{year_suffix}",),
            )

            return [dict(row) for row in cursor.fetchall()]

    # =========================================================================
    # Budget Data
    # =========================================================================

    def get_budget(self, ae_name: str, year: int, month: int) -> Optional[Decimal]:
        """Get budget amount for an AE/year/month."""
        with self.safe_connection() as conn:
            cursor = conn.execute(
                """
                SELECT budget_amount
                FROM budget
                WHERE ae_name = ? AND year = ? AND month = ?
            """,
                (ae_name, year, month),
            )

            row = cursor.fetchone()
            return Decimal(str(row["budget_amount"])) if row else None

    def get_budgets_for_periods(
        self, ae_name: str, periods: List[PlanningPeriod]
    ) -> Dict[PlanningPeriod, Decimal]:
        """Get budget amounts for multiple periods."""
        if not periods:
            return {}

        with self.safe_connection() as conn:
            placeholders = ",".join(["(?, ?)" for _ in periods])
            params = []
            for p in periods:
                params.extend([p.year, p.month])

            cursor = conn.execute(
                f"""
                SELECT year, month, budget_amount
                FROM budget
                WHERE ae_name = ? AND (year, month) IN ({placeholders})
            """,
                [ae_name] + params,
            )

            result = {}
            for row in cursor.fetchall():
                period = PlanningPeriod(year=row["year"], month=row["month"])
                result[period] = Decimal(str(row["budget_amount"]))

            return result

    # =========================================================================
    # Forecast Data
    # =========================================================================

    def get_forecast(self, ae_name: str, year: int, month: int) -> Optional[Decimal]:
        """Get forecast amount for an AE/year/month."""
        with self.safe_connection() as conn:
            cursor = conn.execute(
                """
                SELECT forecast_amount, updated_date, updated_by
                FROM forecast
                WHERE ae_name = ? AND year = ? AND month = ?
            """,
                (ae_name, year, month),
            )

            row = cursor.fetchone()
            return Decimal(str(row["forecast_amount"])) if row else None

    def get_forecast_with_metadata(
        self, ae_name: str, year: int, month: int
    ) -> Optional[Dict[str, Any]]:
        """Get forecast with update metadata."""
        with self.safe_connection() as conn:
            cursor = conn.execute(
                """
                SELECT forecast_amount, updated_date, updated_by, notes
                FROM forecast
                WHERE ae_name = ? AND year = ? AND month = ?
            """,
                (ae_name, year, month),
            )

            row = cursor.fetchone()
            if not row:
                return None

            return {
                "amount": Decimal(str(row["forecast_amount"])),
                "updated_date": datetime.fromisoformat(row["updated_date"])
                if row["updated_date"]
                else None,
                "updated_by": row["updated_by"],
                "notes": row["notes"],
            }

    def save_forecast(self, update: ForecastUpdate) -> ForecastChange:
        """Save a forecast update and record history."""
        with self.safe_transaction() as conn:
            # Get previous amount for history
            cursor = conn.execute(
                """
                SELECT forecast_amount
                FROM forecast
                WHERE ae_name = ? AND year = ? AND month = ?
            """,
                (update.ae_name, update.year, update.month),
            )

            row = cursor.fetchone()
            previous_amount = Decimal(str(row["forecast_amount"])) if row else None

            # Upsert forecast
            conn.execute(
                """
                INSERT INTO forecast (ae_name, year, month, forecast_amount, updated_by, notes)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(ae_name, year, month) DO UPDATE SET
                    forecast_amount = excluded.forecast_amount,
                    updated_date = CURRENT_TIMESTAMP,
                    updated_by = excluded.updated_by,
                    notes = excluded.notes
            """,
                (
                    update.ae_name,
                    update.year,
                    update.month,
                    float(update.new_amount),
                    update.updated_by,
                    update.notes,
                ),
            )

            # Record history
            conn.execute(
                """
                INSERT INTO forecast_history 
                (ae_name, year, month, previous_amount, new_amount, changed_by, session_notes)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    update.ae_name,
                    update.year,
                    update.month,
                    float(previous_amount) if previous_amount else None,
                    float(update.new_amount),
                    update.updated_by,
                    update.notes,
                ),
            )

            logger.info(
                f"Forecast updated: {update.ae_name} {update.year}-{update.month:02d} "
                f"from {previous_amount} to {update.new_amount}"
            )

            return ForecastChange(
                ae_name=update.ae_name,
                period=update.period,
                previous_amount=Money(previous_amount) if previous_amount else None,
                new_amount=Money(update.new_amount),
                changed_date=datetime.now(),
                changed_by=update.updated_by,
                session_notes=update.notes,
            )

    def delete_forecast(self, ae_name: str, year: int, month: int) -> bool:
        """Delete a forecast override (reverts to budget)."""
        with self.safe_transaction() as conn:
            cursor = conn.execute(
                """
                DELETE FROM forecast
                WHERE ae_name = ? AND year = ? AND month = ?
            """,
                (ae_name, year, month),
            )

            deleted = cursor.rowcount > 0
            if deleted:
                logger.info(f"Forecast deleted: {ae_name} {year}-{month:02d}")

            return deleted

    # =========================================================================
    # Booked Revenue (from spots) - Updated for WorldLink handling
    # =========================================================================

    def get_booked_revenue(self, ae_name: str, year: int, month: int) -> Decimal:
        """Get booked revenue for an AE/year/month from spots.

        Special handling:
        - WorldLink: Match on bill_code LIKE 'WorldLink:%'
        - House: Exclude WorldLink bill_codes (they belong to WorldLink entity)
        """
        with self.safe_connection() as conn:
            period = PlanningPeriod(year=year, month=month)

            if ae_name == "WorldLink":
                # WorldLink revenue is identified by bill_code prefix, not sales_person
                cursor = conn.execute(
                    """
                    SELECT COALESCE(SUM(gross_rate), 0) AS booked
                    FROM spots
                    WHERE bill_code LIKE 'WorldLink:%'
                      AND broadcast_month = ?
                      AND (revenue_type != 'Trade' OR revenue_type IS NULL)
                """,
                    (period.broadcast_month,),
                )
            elif ae_name == "House":
                # House revenue excludes WorldLink bill_codes
                cursor = conn.execute(
                    """
                    SELECT COALESCE(SUM(gross_rate), 0) AS booked
                    FROM spots
                    WHERE sales_person = ?
                      AND broadcast_month = ?
                      AND (revenue_type != 'Trade' OR revenue_type IS NULL)
                      AND bill_code NOT LIKE 'WorldLink:%'
                """,
                    (ae_name, period.broadcast_month),
                )
            else:
                # Standard AE lookup by sales_person
                cursor = conn.execute(
                    """
                    SELECT COALESCE(SUM(gross_rate), 0) AS booked
                    FROM spots
                    WHERE sales_person = ?
                      AND broadcast_month = ?
                      AND (revenue_type != 'Trade' OR revenue_type IS NULL)
                """,
                    (ae_name, period.broadcast_month),
                )

            row = cursor.fetchone()
            return Decimal(str(row["booked"]))

    def get_booked_revenue_for_periods(
        self, ae_name: str, periods: List[PlanningPeriod]
    ) -> Dict[PlanningPeriod, Decimal]:
        """Get booked revenue for multiple periods.

        Special handling for WorldLink and House entities.
        """
        if not periods:
            return {}

        with self.safe_connection() as conn:
            broadcast_months = [p.broadcast_month for p in periods]
            placeholders = ",".join(["?" for _ in broadcast_months])

            if ae_name == "WorldLink":
                cursor = conn.execute(
                    f"""
                    SELECT broadcast_month, COALESCE(SUM(gross_rate), 0) AS booked
                    FROM spots
                    WHERE bill_code LIKE 'WorldLink:%'
                      AND broadcast_month IN ({placeholders})
                      AND (revenue_type != 'Trade' OR revenue_type IS NULL)
                    GROUP BY broadcast_month
                """,
                    broadcast_months,
                )
            elif ae_name == "House":
                cursor = conn.execute(
                    f"""
                    SELECT broadcast_month, COALESCE(SUM(gross_rate), 0) AS booked
                    FROM spots
                    WHERE sales_person = ?
                      AND broadcast_month IN ({placeholders})
                      AND (revenue_type != 'Trade' OR revenue_type IS NULL)
                      AND bill_code NOT LIKE 'WorldLink:%'
                    GROUP BY broadcast_month
                """,
                    [ae_name] + broadcast_months,
                )
            else:
                cursor = conn.execute(
                    f"""
                    SELECT broadcast_month, COALESCE(SUM(gross_rate), 0) AS booked
                    FROM spots
                    WHERE sales_person = ?
                      AND broadcast_month IN ({placeholders})
                      AND (revenue_type != 'Trade' OR revenue_type IS NULL)
                    GROUP BY broadcast_month
                """,
                    [ae_name] + broadcast_months,
                )

            result = {}
            for row in cursor.fetchall():
                period = PlanningPeriod.from_broadcast_month(row["broadcast_month"])
                result[period] = Decimal(str(row["booked"]))

            # Fill in zeros for periods with no spots
            for p in periods:
                if p not in result:
                    result[p] = Decimal("0")

            return result

    def get_all_booked_revenue(
        self, periods: List[PlanningPeriod]
    ) -> Dict[str, Dict[PlanningPeriod, Decimal]]:
        """Get booked revenue for all AEs across periods.

        This method handles the special cases:
        - WorldLink: Aggregated from bill_code LIKE 'WorldLink:%'
        - House: Excludes WorldLink bill_codes
        - All others: Standard sales_person matching
        """
        if not periods:
            return {}

        with self.safe_connection() as conn:
            broadcast_months = [p.broadcast_month for p in periods]
            placeholders = ",".join(["?" for _ in broadcast_months])

            result: Dict[str, Dict[PlanningPeriod, Decimal]] = {}

            # 1. Get standard AE revenue (excluding House for now)
            cursor = conn.execute(
                f"""
                SELECT 
                    sales_person,
                    broadcast_month, 
                    COALESCE(SUM(gross_rate), 0) AS booked
                FROM spots
                WHERE broadcast_month IN ({placeholders})
                  AND (revenue_type != 'Trade' OR revenue_type IS NULL)
                  AND sales_person IS NOT NULL
                  AND sales_person != 'House'
                GROUP BY sales_person, broadcast_month
            """,
                broadcast_months,
            )

            for row in cursor.fetchall():
                ae_name = row["sales_person"]
                period = PlanningPeriod.from_broadcast_month(row["broadcast_month"])

                if ae_name not in result:
                    result[ae_name] = {}
                result[ae_name][period] = Decimal(str(row["booked"]))

            # 2. Get House revenue (excluding WorldLink bill_codes)
            cursor = conn.execute(
                f"""
                SELECT 
                    broadcast_month, 
                    COALESCE(SUM(gross_rate), 0) AS booked
                FROM spots
                WHERE broadcast_month IN ({placeholders})
                  AND (revenue_type != 'Trade' OR revenue_type IS NULL)
                  AND sales_person = 'House'
                  AND bill_code NOT LIKE 'WorldLink:%'
                GROUP BY broadcast_month
            """,
                broadcast_months,
            )

            for row in cursor.fetchall():
                period = PlanningPeriod.from_broadcast_month(row["broadcast_month"])
                if "House" not in result:
                    result["House"] = {}
                result["House"][period] = Decimal(str(row["booked"]))

            # 3. Get WorldLink revenue (from bill_code prefix)
            cursor = conn.execute(
                f"""
                SELECT 
                    broadcast_month, 
                    COALESCE(SUM(gross_rate), 0) AS booked
                FROM spots
                WHERE broadcast_month IN ({placeholders})
                  AND (revenue_type != 'Trade' OR revenue_type IS NULL)
                  AND bill_code LIKE 'WorldLink:%'
                GROUP BY broadcast_month
            """,
                broadcast_months,
            )

            for row in cursor.fetchall():
                period = PlanningPeriod.from_broadcast_month(row["broadcast_month"])
                if "WorldLink" not in result:
                    result["WorldLink"] = {}
                result["WorldLink"][period] = Decimal(str(row["booked"]))

            return result

    def get_booked_detail(
        self, entity: RevenueEntity, period: PlanningPeriod, limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Get detailed breakdown of booked revenue by customer for an entity/period.
        Args:
            entity: Revenue entity (AE, House, WorldLink, etc.)
            period: Planning period (year/month)
            limit: Max rows to return
        Returns:
            List of dicts with customer_id, customer_name, agency_name, revenue, spot_count
        """
        # Build WHERE clause based on entity type
        if entity.entity_type == EntityType.AGENCY and entity.entity_name == "WorldLink":
            entity_filter = """
                (s.bill_code LIKE 'WL:%' OR s.bill_code LIKE 'WORLDLINK:%')
            """
            params = [period.broadcast_month, limit]
        elif entity.entity_type == EntityType.HOUSE:
            entity_filter = "UPPER(TRIM(s.sales_person)) = 'HOUSE'"
            params = [period.broadcast_month, limit]
        else:
            entity_filter = "UPPER(TRIM(s.sales_person)) = UPPER(TRIM(?))"
            params = [entity.entity_name, period.broadcast_month, limit]
        
        query = f"""
            SELECT
                s.customer_id,
                COALESCE(c.normalized_name, s.bill_code) as customer_name,
                a.agency_name,
                sec.sector_code,
                sec.sector_name,
                SUM(s.gross_rate) as revenue,
                COUNT(*) as spot_count
            FROM spots s
            LEFT JOIN customers c ON s.customer_id = c.customer_id
            LEFT JOIN agencies a ON s.agency_id = a.agency_id
            LEFT JOIN sectors sec ON c.sector_id = sec.sector_id
            WHERE {entity_filter}
            AND s.broadcast_month = ?
            AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
            GROUP BY s.customer_id, COALESCE(c.normalized_name, s.bill_code), a.agency_name, sec.sector_code, sec.sector_name
            ORDER BY customer_name ASC
            LIMIT ?
        """
        
        with self.safe_connection() as conn:
            cursor = conn.execute(query, params)
            results = []
            for row in cursor.fetchall():
                results.append({
                    "customer_id": row[0],
                    "customer_name": row[1] or "Unknown",
                    "agency_name": row[2],
                    "sector_code": row[3],
                    "sector_name": row[4],
                    "revenue": float(row[5] or 0),
                    "spot_count": int(row[6] or 0)
                })
            return results

    def get_booked_detail_annual(
        self, entity: RevenueEntity, year: int, limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Get detailed breakdown of booked revenue by customer for a full year."""
        periods = PlanningPeriod.full_year(year)
        broadcast_months = [p.broadcast_month for p in periods]
        month_placeholders = ",".join(["?" for _ in broadcast_months])

        if entity.entity_type == EntityType.AGENCY and entity.entity_name == "WorldLink":
            entity_filter = """
                (s.bill_code LIKE 'WL:%' OR s.bill_code LIKE 'WORLDLINK:%')
            """
            params = broadcast_months + [limit]
        elif entity.entity_type == EntityType.HOUSE:
            entity_filter = "UPPER(TRIM(s.sales_person)) = 'HOUSE'"
            params = broadcast_months + [limit]
        else:
            entity_filter = "UPPER(TRIM(s.sales_person)) = UPPER(TRIM(?))"
            params = [entity.entity_name] + broadcast_months + [limit]

        query = f"""
            SELECT
                s.customer_id,
                COALESCE(c.normalized_name, s.bill_code) as customer_name,
                a.agency_name,
                sec.sector_code,
                sec.sector_name,
                SUM(s.gross_rate) as revenue,
                COUNT(*) as spot_count
            FROM spots s
            LEFT JOIN customers c ON s.customer_id = c.customer_id
            LEFT JOIN agencies a ON s.agency_id = a.agency_id
            LEFT JOIN sectors sec ON c.sector_id = sec.sector_id
            WHERE {entity_filter}
            AND s.broadcast_month IN ({month_placeholders})
            AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
            GROUP BY s.customer_id, COALESCE(c.normalized_name, s.bill_code), a.agency_name, sec.sector_code, sec.sector_name
            ORDER BY customer_name ASC
            LIMIT ?
        """

        with self.safe_connection() as conn:
            cursor = conn.execute(query, params)
            results = []
            for row in cursor.fetchall():
                results.append({
                    "customer_id": row[0],
                    "customer_name": row[1] or "Unknown",
                    "agency_name": row[2],
                    "sector_code": row[3],
                    "sector_name": row[4],
                    "revenue": float(row[5] or 0),
                    "spot_count": int(row[6] or 0)
                })
            return results

    # =========================================================================
    # Combined Planning Data
    # =========================================================================

    def get_planning_row(
        self, entity: RevenueEntity, period: PlanningPeriod
    ) -> PlanningRow:
        """Get complete planning data for one entity and period."""
        budget = self.get_budget(entity.entity_name, period.year, period.month)
        forecast_data = self.get_forecast_with_metadata(
            entity.entity_name, period.year, period.month
        )
        booked = self.get_booked_revenue(entity.entity_name, period.year, period.month)

        # Forecast defaults to budget if not overridden
        if forecast_data:
            forecast_amount = forecast_data["amount"]
            forecast_updated = forecast_data["updated_date"]
            forecast_updated_by = forecast_data["updated_by"]
        else:
            forecast_amount = budget or Decimal("0")
            forecast_updated = None
            forecast_updated_by = None

        return PlanningRow(
            entity=entity,
            period=period,
            budget=Money(budget or Decimal("0")),
            forecast_entered=Money(forecast_amount),  # ← CHANGED from forecast=
            booked=Money(booked),
            forecast_updated=forecast_updated,
            forecast_updated_by=forecast_updated_by,
        )

    def get_forecast_history(
        self, ae_name: str, year: int, month: int, limit: int = 10
    ) -> List[ForecastChange]:
        """Get forecast change history for an AE/period."""
        with self.safe_connection() as conn:
            cursor = conn.execute(
                """
                SELECT 
                    ae_name, year, month, 
                    previous_amount, new_amount,
                    changed_date, changed_by, session_notes
                FROM forecast_history
                WHERE ae_name = ? AND year = ? AND month = ?
                ORDER BY changed_date DESC
                LIMIT ?
            """,
                (ae_name, year, month, limit),
            )

            result = []
            for row in cursor.fetchall():
                result.append(
                    ForecastChange(
                        ae_name=row["ae_name"],
                        period=PlanningPeriod(year=row["year"], month=row["month"]),
                        previous_amount=Money(Decimal(str(row["previous_amount"])))
                        if row["previous_amount"]
                        else None,
                        new_amount=Money(Decimal(str(row["new_amount"]))),
                        changed_date=datetime.fromisoformat(row["changed_date"]),
                        changed_by=row["changed_by"],
                        session_notes=row["session_notes"],
                    )
                )

            return result

    def get_company_totals_for_period(
        self, period: "PlanningPeriod"
    ) -> Dict[str, Decimal]:
        """
        Get company-wide totals for a period.

        Returns:
            Dict with 'budget', 'forecast', 'booked' totals
        """
        with self.safe_connection() as conn:
            # Budget total
            cursor = conn.execute(
                """
                SELECT COALESCE(SUM(budget_amount), 0) AS total
                FROM budget
                WHERE year = ? AND month = ?
            """,
                (period.year, period.month),
            )
            budget_total = Decimal(str(cursor.fetchone()["total"]))

            # Forecast total (with fallback to budget per AE)
            cursor = conn.execute(
                """
                SELECT COALESCE(SUM(
                    COALESCE(f.forecast_amount, b.budget_amount)
                ), 0) AS total
                FROM budget b
                LEFT JOIN forecast f 
                    ON f.ae_name = b.ae_name 
                    AND f.year = b.year 
                    AND f.month = b.month
                WHERE b.year = ? AND b.month = ?
            """,
                (period.year, period.month),
            )
            forecast_total = Decimal(str(cursor.fetchone()["total"]))

            # Booked total (all revenue for the broadcast month)
            cursor = conn.execute(
                """
                SELECT COALESCE(SUM(gross_rate), 0) AS total
                FROM spots
                WHERE broadcast_month = ?
                  AND (revenue_type != 'Trade' OR revenue_type IS NULL)
            """,
                (period.broadcast_month,),
            )
            booked_total = Decimal(str(cursor.fetchone()["total"]))

            return {
                "budget": budget_total,
                "forecast": forecast_total,
                "booked": booked_total,
            }

    # =========================================================================
    # Helpers
    # =========================================================================

    def _row_to_revenue_entity(self, row: sqlite3.Row) -> RevenueEntity:
        """Convert database row to RevenueEntity."""
        return RevenueEntity(
            entity_id=row["entity_id"],
            entity_name=row["entity_name"],
            entity_type=EntityType(row["entity_type"]),
            is_active=bool(row["is_active"]),
            notes=row["notes"],
        )
