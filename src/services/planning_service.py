"""
Planning Service - Business logic for forecast planning sessions.

Orchestrates planning operations:
- Loading planning data for sessions (full year + active window)
- Updating forecasts
- Calculating summaries and variances
- Validating business rules
"""

import logging
from typing import List, Optional, Dict, Any
from decimal import Decimal
from datetime import date


from src.utils.calendar import SellableDaysCalendar
from src.models.planning import BurnDownMetrics

from src.database.connection import DatabaseConnection
from src.services.base_service import BaseService
from src.repositories.sector_expectation_repository import SectorExpectationRepository
from src.repositories.planning_repository import PlanningRepository
from src.models.planning import (
    RevenueEntity,
    EntityType,
    PlanningPeriod,
    PlanningRow,
    EntityPlanningData,
    PlanningSummary,
    Money,
    ForecastUpdate,
    ForecastChange,
    SectorExpectation,
    EntitySectorExpectations,
    SectorExpectationValidation,
)

logger = logging.getLogger(__name__)


class PlanningService(BaseService):
    """Service for planning session operations."""

    def __init__(self, db_connection: DatabaseConnection):
        super().__init__(db_connection)
        self.repository = PlanningRepository(db_connection)
        self.sector_repo = SectorExpectationRepository(db_connection)

    # =========================================================================
    # Planning Data Retrieval
    # =========================================================================

    def get_planning_summary(
        self,
        months_ahead: int = 2,
        include_inactive: bool = False,
        planning_year: Optional[int] = None,
    ) -> PlanningSummary:
        """
        Get complete planning summary for all entities.

        Args:
            months_ahead: Number of months beyond current to include (default 2)
            include_inactive: Include inactive revenue entities
            planning_year: Year to plan for (default: current year)

        Returns:
            PlanningSummary with all entities and periods
        """
        if planning_year is None:
            planning_year = date.today().year

        # Get all periods for the full year and active window
        all_periods = PlanningPeriod.full_year(planning_year)
        active_periods = PlanningPeriod.planning_window(months_ahead)
        past_periods = PlanningPeriod.past_periods(planning_year)

        entities = self.repository.get_active_revenue_entities()

        entity_data_list = []
        for entity in entities:
            entity_data = self._build_entity_planning_data(entity, all_periods)
            entity_data_list.append(entity_data)

        return PlanningSummary(
            planning_year=planning_year,
            all_periods=all_periods,
            active_periods=active_periods,
            past_periods=past_periods,
            entity_data=entity_data_list,
        )

    def get_full_year_planning_summary(
        self, year: int, active_window_months: int = 2
    ) -> PlanningSummary:
        """
        Get planning summary showing all 12 months with active window highlighted.

        Args:
            year: Planning year
            active_window_months: Number of months in active planning window

        Returns:
            PlanningSummary with full year data
        """
        return self.get_planning_summary(
            months_ahead=active_window_months, planning_year=year
        )

    def get_entity_planning_data(
        self,
        entity_name: str,
        months_ahead: int = 2,
        planning_year: Optional[int] = None,
    ) -> Optional[EntityPlanningData]:
        """Get planning data for a single entity."""
        entity = self.repository.get_revenue_entity_by_name(entity_name)
        if not entity:
            logger.warning(f"Revenue entity not found: {entity_name}")
            return None

        if planning_year is None:
            planning_year = date.today().year

        all_periods = PlanningPeriod.full_year(planning_year)
        return self._build_entity_planning_data(entity, all_periods)

    def get_planning_row(
        self, entity_name: str, year: int, month: int
    ) -> Optional[PlanningRow]:
        """Get planning data for a single entity and period."""
        entity = self.repository.get_revenue_entity_by_name(entity_name)
        if not entity:
            return None

        period = PlanningPeriod(year=year, month=month)
        return self.repository.get_planning_row(entity, period)

    def _build_entity_planning_data(
        self, entity: RevenueEntity, periods: List[PlanningPeriod]
    ) -> EntityPlanningData:
        """Build complete planning data for an entity across periods."""
        rows = []
        for period in periods:
            row = self.repository.get_planning_row(entity, period)
            rows.append(row)

        return EntityPlanningData(entity=entity, rows=rows)

    # =========================================================================
    # Forecast Updates
    # =========================================================================

    def update_forecast(
        self,
        ae_name: str,
        year: int,
        month: int,
        new_amount: Decimal,
        updated_by: str,
        notes: Optional[str] = None,
    ) -> ForecastChange:
        """
        Update forecast for an AE/period.

        Args:
            ae_name: Revenue entity name
            year: Year
            month: Month (1-12)
            new_amount: New forecast amount
            updated_by: User making the change
            notes: Optional session notes

        Returns:
            ForecastChange record
        """
        # Validate entity exists
        entity = self.repository.get_revenue_entity_by_name(ae_name)
        if not entity:
            raise ValueError(f"Unknown revenue entity: {ae_name}")

        # Validate period is not in the past (closed months)
        period = PlanningPeriod(year=year, month=month)
        if period.is_past:
            raise ValueError(
                f"Cannot update forecast for closed period: {period.display}"
            )

        # Create and save update
        update = ForecastUpdate(
            ae_name=ae_name,
            year=year,
            month=month,
            new_amount=new_amount,
            updated_by=updated_by,
            notes=notes,
        )

        return self.repository.save_forecast(update)

    def bulk_update_forecasts(
        self,
        updates: List[Dict[str, Any]],
        updated_by: str,
        session_notes: Optional[str] = None,
    ) -> List[ForecastChange]:
        """
        Update multiple forecasts at once.

        Args:
            updates: List of dicts with ae_name, year, month, amount
            updated_by: User making the changes
            session_notes: Notes for all changes in this session

        Returns:
            List of ForecastChange records
        """
        changes = []
        errors = []

        for u in updates:
            try:
                change = self.update_forecast(
                    ae_name=u["ae_name"],
                    year=u["year"],
                    month=u["month"],
                    new_amount=Decimal(str(u["amount"])),
                    updated_by=updated_by,
                    notes=session_notes,
                )
                changes.append(change)
            except ValueError as e:
                # Skip past periods but log them
                logger.warning(f"Skipped forecast update: {e}")
                errors.append({"update": u, "error": str(e)})
            except Exception as e:
                logger.error(f"Failed to update forecast for {u}: {e}")
                raise

        if errors:
            logger.info(f"Bulk update completed with {len(errors)} skipped updates")

        return changes

    def reset_forecast_to_budget(self, ae_name: str, year: int, month: int) -> bool:
        """Reset forecast to budget by removing the override."""
        period = PlanningPeriod(year=year, month=month)
        if period.is_past:
            raise ValueError(
                f"Cannot reset forecast for closed period: {period.display}"
            )
        return self.repository.delete_forecast(ae_name, year, month)

    # =========================================================================
    # Summary and Analysis
    # =========================================================================

    def get_period_totals(self, period: PlanningPeriod) -> Dict[str, Money]:
        """Get totals across all entities for a period."""
        entities = self.repository.get_active_revenue_entities()

        total_budget = Money.zero()
        total_forecast = Money.zero()
        total_booked = Money.zero()

        for entity in entities:
            row = self.repository.get_planning_row(entity, period)
            total_budget = total_budget + row.budget
            total_forecast = total_forecast + row.forecast
            total_booked = total_booked + row.booked

        return {
            "budget": total_budget,
            "forecast": total_forecast,
            "booked": total_booked,
            "pipeline": total_forecast - total_booked,
            "variance": total_forecast - total_budget,
        }

    def get_company_summary(
        self, months_ahead: int = 2, planning_year: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Get high-level company summary for planning window.

        Now returns full year data with active window indicated.
        """
        if planning_year is None:
            planning_year = date.today().year

        all_periods = PlanningPeriod.full_year(planning_year)
        active_periods = PlanningPeriod.planning_window(months_ahead)
        past_periods = PlanningPeriod.past_periods(planning_year)

        total_budget = Money.zero()
        total_forecast = Money.zero()
        total_booked = Money.zero()

        period_details = []
        periods_by_key = {}

        for period in all_periods:
            totals = self.get_period_totals(period)
            total_budget = total_budget + totals["budget"]
            total_forecast = total_forecast + totals["forecast"]
            total_booked = total_booked + totals["booked"]

            period_data = {
                "period": period,
                "budget": totals["budget"],
                "forecast": totals["forecast"],
                "booked": totals["booked"],
                "pipeline": totals["pipeline"],
                "variance": totals["variance"],
                "pct_booked": (
                    float(totals["booked"].amount / totals["forecast"].amount * 100)
                    if totals["forecast"].amount > 0
                    else 0
                ),
                "is_active": period in active_periods,
                "is_past": period in past_periods,
            }
            period_details.append(period_data)
            periods_by_key[period.key] = period_data

        return {
            "planning_year": planning_year,
            "total_budget": total_budget,
            "total_forecast": total_forecast,
            "total_booked": total_booked,
            "total_pipeline": total_forecast - total_booked,
            "total_variance": total_forecast - total_budget,
            "periods": period_details,
            "periods_by_key": periods_by_key,
            "all_periods": all_periods,
            "active_periods": active_periods,
            "past_periods": past_periods,
        }

    def get_forecast_history(
        self, ae_name: str, year: int, month: int, limit: int = 10
    ) -> List[ForecastChange]:
        """Get history of forecast changes for an entity/period."""
        return self.repository.get_forecast_history(ae_name, year, month, limit)

    # =========================================================================
    # Revenue Entity Management
    # =========================================================================

    def get_revenue_entities(self) -> List[RevenueEntity]:
        """Get all active revenue entities."""
        return self.repository.get_active_revenue_entities()

    def sync_revenue_entities(self, year: int) -> Dict[str, Any]:
        """
        Sync revenue entities from budget and spots data.

        Creates entries for any AEs that appear in budget or spots
        but are not yet in revenue_entities.

        Returns:
            Summary of sync operation
        """
        added = []

        # Get unmatched revenue (AEs in spots but not in revenue_entities)
        unmatched = self.repository.get_unmatched_revenue(year)

        for record in unmatched:
            ae_name = record["sales_person"]

            # Determine entity type
            if ae_name.lower() == "house":
                entity_type = EntityType.HOUSE
            elif ae_name.lower() == "worldlink":
                entity_type = EntityType.AGENCY
            else:
                entity_type = EntityType.AE

            entity = self.repository.upsert_revenue_entity(ae_name, entity_type)
            added.append(
                {
                    "name": ae_name,
                    "type": entity_type.value,
                    "revenue": float(record["total_revenue"]),
                }
            )
            logger.info(f"Added revenue entity: {ae_name} ({entity_type.value})")

        return {"added_count": len(added), "added_entities": added}

    # =========================================================================
    # Sector Expectations
    # =========================================================================

    def get_sector_expectations_for_entity(
        self, ae_name: str, year: int
    ) -> EntitySectorExpectations:
        """
        Get all sector expectations for an entity/year.

        Args:
            ae_name: Entity name (AE or House)
            year: Planning year

        Returns:
            EntitySectorExpectations with all sector detail
        """
        entity = self.repository.get_revenue_entity_by_name(ae_name)
        if not entity:
            raise ValueError(f"Unknown entity: {ae_name}")

        expectations = self.sector_repo.get_by_entity_year(ae_name, year)

        return EntitySectorExpectations(
            entity_name=ae_name,
            entity_type=entity.entity_type,
            year=year,
            expectations=expectations,
        )

    def get_all_sector_expectations(
        self, year: int
    ) -> Dict[str, EntitySectorExpectations]:
        """
        Get sector expectations for all entities.

        Args:
            year: Planning year

        Returns:
            Dict mapping entity_name -> EntitySectorExpectations
        """
        return self.sector_repo.get_all_for_year(year)

    def get_budget_with_sectors(self, year: int) -> List[Dict[str, Any]]:
        """
        Get budget data with sector expectations for Budget Entry UI.

        Returns data structured for the expandable budget table.

        Args:
            year: Planning year

        Returns:
            List of entity data with budget and sector breakdown
        """
        entities = self.repository.get_active_revenue_entities()
        all_expectations = self.sector_repo.get_all_for_year(year)
        all_sectors = self.sector_repo.get_all_sectors()

        result = []

        for entity in entities:
            # Skip WorldLink - no sector breakdown
            if entity.entity_name == "WorldLink":
                entity_data = {
                    "entity": entity,
                    "has_sector_detail": False,
                    "budgets": {},
                    "sectors": [],
                    "sector_totals": {},
                    "is_balanced": {},
                }
            else:
                # Get budgets for each month
                periods = PlanningPeriod.full_year(year)
                budgets = {}
                for period in periods:
                    budget = self.repository.get_budget(
                        entity.entity_name, period.year, period.month
                    )
                    budgets[period.month] = budget or Decimal("0")

                # Get sector expectations
                entity_expectations = all_expectations.get(entity.entity_name)

                # Build sector grid for template
                sectors = []
                sector_totals = {m: Decimal("0") for m in range(1, 13)}

                if entity_expectations:
                    grid = entity_expectations.monthly_grid()
                    for (
                        sector_id,
                        sector_code,
                        sector_name,
                    ) in entity_expectations.sectors_used():
                        sector_data = {
                            "sector_id": sector_id,
                            "sector_code": sector_code,
                            "sector_name": sector_name,
                            "amounts": {},
                        }
                        for month in range(1, 13):
                            amount = grid.get(sector_id, {}).get(month, Decimal("0"))
                            sector_data["amounts"][month] = amount
                            sector_totals[month] += amount
                        sectors.append(sector_data)

                # Check balance for each month
                is_balanced = {}
                for month in range(1, 13):
                    is_balanced[month] = sector_totals[month] == budgets[month]

                entity_data = {
                    "entity": entity,
                    "has_sector_detail": True,
                    "budgets": budgets,
                    "sectors": sectors,
                    "sector_totals": sector_totals,
                    "is_balanced": is_balanced,
                }

            result.append(entity_data)

        return result

    def validate_sector_expectations(
        self, ae_name: str, year: int
    ) -> SectorExpectationValidation:
        """
        Validate that sector expectations sum to budget for each month.

        Args:
            ae_name: Entity name
            year: Planning year

        Returns:
            SectorExpectationValidation with results
        """
        validation = SectorExpectationValidation(
            entity_name=ae_name, year=year, is_valid=True
        )

        # Get budget for each month
        for month in range(1, 13):
            budget = self.repository.get_budget(ae_name, year, month)
            budget_amount = budget or Decimal("0")

            # Get sector total for this month
            expectations = self.sector_repo.get_by_entity_month(ae_name, year, month)
            sector_total = sum((e.expected_amount for e in expectations), Decimal("0"))

            if sector_total != budget_amount:
                validation.add_month_mismatch(month, budget_amount, sector_total)
            else:
                validation.add_month_balanced(month, budget_amount)

        return validation

    def save_sector_expectations(
        self,
        ae_name: str,
        year: int,
        expectations: List[Dict[str, Any]],
        updated_by: str,
    ) -> Dict[str, Any]:
        """
        Save sector expectations with validation.

        Args:
            ae_name: Entity name
            year: Planning year
            expectations: List of {sector_id, month, amount} dicts
            updated_by: User making the change

        Returns:
            Dict with success status and any validation errors
        """
        # Convert to SectorExpectation objects
        sector_expectations = []
        all_sectors = {s["sector_id"]: s for s in self.sector_repo.get_all_sectors()}

        for exp in expectations:
            sector = all_sectors.get(exp["sector_id"])
            if not sector:
                return {
                    "success": False,
                    "error": f"Unknown sector_id: {exp['sector_id']}",
                }

            sector_expectations.append(
                SectorExpectation(
                    ae_name=ae_name,
                    sector_id=exp["sector_id"],
                    sector_code=sector["sector_code"],
                    sector_name=sector["sector_name"],
                    year=year,
                    month=exp["month"],
                    expected_amount=Decimal(str(exp["amount"])),
                    notes=exp.get("notes"),
                )
            )

        # Save
        saved_count = self.sector_repo.save_expectations(
            ae_name, year, sector_expectations, updated_by
        )

        # Validate against budget
        validation = self.validate_sector_expectations(ae_name, year)

        return {
            "success": True,
            "saved_count": saved_count,
            "validation": {
                "is_valid": validation.is_valid,
                "errors": validation.errors,
                "month_details": validation.month_details,
            },
        }

    def add_sector_to_entity(
        self, ae_name: str, sector_id: int, year: int, updated_by: str
    ) -> Dict[str, Any]:
        """
        Add a new sector to an entity's expectations (initialized to zero).

        Args:
            ae_name: Entity name
            sector_id: Sector to add
            year: Planning year
            updated_by: User making the change

        Returns:
            Dict with the new sector info
        """
        # Verify sector exists
        all_sectors = {s["sector_id"]: s for s in self.sector_repo.get_all_sectors()}
        sector = all_sectors.get(sector_id)
        if not sector:
            raise ValueError(f"Unknown sector_id: {sector_id}")

        # Check if already exists
        existing = self.sector_repo.get_by_entity_year(ae_name, year)
        if any(e.sector_id == sector_id for e in existing):
            raise ValueError(
                f"Sector {sector['sector_name']} already exists for {ae_name}"
            )

        # Add expectations with zero amounts for all 12 months
        new_expectations = []
        for month in range(1, 13):
            exp = SectorExpectation(
                ae_name=ae_name,
                sector_id=sector_id,
                sector_code=sector["sector_code"],
                sector_name=sector["sector_name"],
                year=year,
                month=month,
                expected_amount=Decimal("0"),
            )
            self.sector_repo.save_single_expectation(exp, updated_by)
            new_expectations.append(exp)

        logger.info(f"Added sector {sector['sector_name']} to {ae_name} for {year}")

        return {
            "success": True,
            "sector_id": sector_id,
            "sector_code": sector["sector_code"],
            "sector_name": sector["sector_name"],
            "amounts": {m: 0 for m in range(1, 13)},
        }

    def remove_sector_from_entity(
        self, ae_name: str, sector_id: int, year: int
    ) -> bool:
        """
        Remove a sector from an entity's expectations.

        Args:
            ae_name: Entity name
            sector_id: Sector to remove
            year: Planning year

        Returns:
            True if deleted, False if not found
        """
        deleted = self.sector_repo.delete_sector_for_entity(ae_name, sector_id, year)
        return deleted > 0

    def get_available_sectors(self, ae_name: str, year: int) -> List[Dict[str, Any]]:
        """
        Get sectors available to add (not already in use).

        Args:
            ae_name: Entity name
            year: Planning year

        Returns:
            List of sector dicts
        """
        return self.sector_repo.get_available_sectors_for_entity(ae_name, year)

    def get_all_sectors(self) -> List[Dict[str, Any]]:
        """Get all active sectors."""
        return self.sector_repo.get_all_sectors()

    # ============================================================================
    # Add this method to PlanningService class
    # ============================================================================

    def get_burn_down_metrics(
        self, periods: List[PlanningPeriod], as_of: Optional[date] = None
    ) -> Dict[PlanningPeriod, BurnDownMetrics]:
        """
        Calculate burn-down metrics for each period.

        These are company-wide totals showing the "physics" of hitting forecast.

        Key insight: For future months, "days left" should be CUMULATIVE from today.
        - January (current): remaining days in Jan
        - February: remaining in Jan + all of Feb
        - March: remaining in Jan + all of Feb + all of Mar

        Args:
            periods: List of planning periods (typically active window)
            as_of: Calculate as of this date (default: today)

        Returns:
            Dict mapping period -> BurnDownMetrics
        """
        if as_of is None:
            as_of = date.today()

        # Create calendar with adjustment lookup from DB
        def adjustment_lookup(year: int, month: int) -> Optional[tuple]:
            return self.repository.get_sellable_days_adjustment(year, month)

        cal = SellableDaysCalendar(adjustment_lookup)

        # Sort periods chronologically to compute cumulative runway
        sorted_periods = sorted(periods, key=lambda p: (p.year, p.month))

        # First pass: get sellable days info for each period
        days_by_period = {}
        for period in sorted_periods:
            days_by_period[period] = cal.get_sellable_days(
                period.year, period.month, as_of
            )

        # Second pass: compute cumulative runway for each period
        cumulative_runway = {}
        running_total = 0

        for period in sorted_periods:
            days_info = days_by_period[period]

            # Determine if this is a current/past month or future month
            period_start = date(period.year, period.month, 1)

            if as_of.year == period.year and as_of.month == period.month:
                # Current month: use remaining days, start accumulating
                running_total = days_info.sellable_remaining
            elif period_start > as_of:
                # Future month: add ALL sellable days to running total
                running_total += days_info.sellable_total
            else:
                # Past month: no runway (already closed)
                running_total = 0

            cumulative_runway[period] = running_total

        # Third pass: build metrics with cumulative runway
        result = {}

        for period in periods:
            days_info = days_by_period[period]

            # Get company totals
            totals = self.repository.get_company_totals_for_period(period)

            # Get booked MTD by effective_date (for pace calculation)
            booked_mtd = self.repository.get_booked_mtd_by_effective_date(
                period.broadcast_month, as_of
            )

            # Build metrics object with CUMULATIVE runway
            metrics = BurnDownMetrics(
                period=period,
                sellable_days_total=days_info.sellable_total,
                sellable_days_elapsed=days_info.sellable_elapsed,
                sellable_days_remaining=cumulative_runway[period],  # <-- CUMULATIVE
                adjustment=days_info.adjustment,
                adjustment_reason=days_info.adjustment_reason,
                forecast_total=Money(totals["forecast"]),
                booked_total=Money(totals["booked"]),
                booked_mtd=Money(booked_mtd),
            )

            result[period] = metrics

        return result

    def get_booked_detail(
        self, entity_name: str, year: int, month: int, limit: int = 50
    ) -> Optional[Dict[str, Any]]:
        """
        Get detailed breakdown of booked revenue for an entity/period.

        Args:
            entity_name: Name of the revenue entity
            year: Year
            month: Month (1-12)
            limit: Max customers to return

        Returns:
            Dict with entity info, period info, total, and customer breakdown
        """
        entity = self.repository.get_revenue_entity_by_name(entity_name)
        if not entity:
            logger.warning(f"Revenue entity not found: {entity_name}")
            return None

        period = PlanningPeriod(year=year, month=month)

        # Get the detail breakdown
        customers = self.repository.get_booked_detail(entity, period, limit)

        # Calculate total from the breakdown
        total_revenue = sum(c["revenue"] for c in customers)
        total_spots = sum(c["spot_count"] for c in customers)

        return {
            "entity_name": entity.entity_name,
            "entity_type": entity.entity_type.value,
            "period": {
                "year": year,
                "month": month,
                "display": period.display,
                "broadcast_month": period.broadcast_month,
            },
            "total_revenue": total_revenue,
            "total_spots": total_spots,
            "customer_count": len(customers),
            "customers": customers,
        }

    # =========================================================================
    # Validation
    # =========================================================================

    def validate_planning_data(self, year: int) -> Dict[str, Any]:
        """
        Validate planning data integrity.

        Checks:
        - All active entities have budget data
        - No unmatched revenue in spots
        - Forecast values are reasonable
        """
        issues = []
        warnings = []

        entities = self.repository.get_active_revenue_entities()
        all_periods = PlanningPeriod.full_year(year)

        for entity in entities:
            for period in all_periods:
                budget = self.repository.get_budget(
                    entity.entity_name, period.year, period.month
                )

                if budget is None:
                    issues.append(
                        f"Missing budget: {entity.entity_name} for {period.display}"
                    )

        # Check for unmatched revenue
        unmatched = self.repository.get_unmatched_revenue(year)
        if unmatched:
            for record in unmatched:
                warnings.append(
                    f"Unmatched revenue: {record['sales_person']} "
                    f"(${record['total_revenue']:,.0f})"
                )

        return {
            "is_valid": len(issues) == 0,
            "issues": issues,
            "warnings": warnings,
            "entities_checked": len(entities),
            "periods_checked": len(all_periods),
        }
