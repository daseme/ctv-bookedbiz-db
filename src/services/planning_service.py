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


from src.utils.calendar import SellableDaysCalendar, SellableDaysInfo
from src.models.planning import BurnDownMetrics, PaceStatus

from src.database.connection import DatabaseConnection
from src.services.base_service import BaseService
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
)

logger = logging.getLogger(__name__)


class PlanningService(BaseService):
    """Service for planning session operations."""

    def __init__(self, db_connection: DatabaseConnection):
        super().__init__(db_connection)
        self.repository = PlanningRepository(db_connection)

    # =========================================================================
    # Planning Data Retrieval
    # =========================================================================

    def get_planning_summary(
        self, 
        months_ahead: int = 2,
        include_inactive: bool = False,
        planning_year: Optional[int] = None
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
            entity_data=entity_data_list
        )

    def get_full_year_planning_summary(
        self,
        year: int,
        active_window_months: int = 2
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
            months_ahead=active_window_months,
            planning_year=year
        )

    def get_entity_planning_data(
        self, 
        entity_name: str,
        months_ahead: int = 2,
        planning_year: Optional[int] = None
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
        self, 
        entity_name: str, 
        year: int, 
        month: int
    ) -> Optional[PlanningRow]:
        """Get planning data for a single entity and period."""
        entity = self.repository.get_revenue_entity_by_name(entity_name)
        if not entity:
            return None
        
        period = PlanningPeriod(year=year, month=month)
        return self.repository.get_planning_row(entity, period)

    def _build_entity_planning_data(
        self, 
        entity: RevenueEntity, 
        periods: List[PlanningPeriod]
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
        notes: Optional[str] = None
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
            notes=notes
        )
        
        return self.repository.save_forecast(update)

    def bulk_update_forecasts(
        self,
        updates: List[Dict[str, Any]],
        updated_by: str,
        session_notes: Optional[str] = None
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
                    notes=session_notes
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

    def reset_forecast_to_budget(
        self,
        ae_name: str,
        year: int,
        month: int
    ) -> bool:
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

    def get_period_totals(
        self, 
        period: PlanningPeriod
    ) -> Dict[str, Money]:
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
            "variance": total_forecast - total_budget
        }

    def get_company_summary(
        self, 
        months_ahead: int = 2,
        planning_year: Optional[int] = None
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
                    if totals["forecast"].amount > 0 else 0
                ),
                "is_active": period in active_periods,
                "is_past": period in past_periods
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
            "past_periods": past_periods
        }

    def get_forecast_history(
        self,
        ae_name: str,
        year: int,
        month: int,
        limit: int = 10
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
            added.append({
                "name": ae_name,
                "type": entity_type.value,
                "revenue": float(record["total_revenue"])
            })
            logger.info(f"Added revenue entity: {ae_name} ({entity_type.value})")
        
        return {
            "added_count": len(added),
            "added_entities": added
        }

# ============================================================================
# Add this method to PlanningService class
# ============================================================================

    def get_burn_down_metrics(
        self,
        periods: List[PlanningPeriod],
        as_of: Optional[date] = None
    ) -> Dict[PlanningPeriod, BurnDownMetrics]:
        """
        Calculate burn-down metrics for each period.
        
        These are company-wide totals showing the "physics" of hitting forecast.
        
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
        
        calendar = SellableDaysCalendar(adjustment_lookup)
        
        result = {}
        
        for period in periods:
            # Get sellable days info
            days_info = calendar.get_sellable_days(period.year, period.month, as_of)
            
            # Get company totals
            totals = self.repository.get_company_totals_for_period(period)
            
            # Get booked MTD by effective_date (for pace calculation)
            booked_mtd = self.repository.get_booked_mtd_by_effective_date(
                period.broadcast_month,
                as_of
            )
            
            # Build metrics object
            metrics = BurnDownMetrics(
                period=period,
                sellable_days_total=days_info.sellable_total,
                sellable_days_elapsed=days_info.sellable_elapsed,
                sellable_days_remaining=days_info.sellable_remaining,
                adjustment=days_info.adjustment,
                adjustment_reason=days_info.adjustment_reason,
                forecast_total=Money(totals["forecast"]),
                booked_total=Money(totals["booked"]),
                booked_mtd=Money(booked_mtd)
            )
            
            result[period] = metrics
        
        return result

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
            "periods_checked": len(all_periods)
        }