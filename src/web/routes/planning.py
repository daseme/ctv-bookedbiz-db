"""
Planning session routes - Forecast management UI and API.

Provides:
- Planning session main page (full year view with active window)
- API endpoints for forecast updates
- Real-time data refresh
"""

import logging
from flask import Blueprint, render_template, request, jsonify
from datetime import date
from decimal import Decimal
from enum import Enum
from dataclasses import dataclass
from typing import Optional, Dict, List, Any


from src.repositories.sector_expectation_repository import SectorExpectationRepository
from src.services.sector_planning_service import SectorPlanningService
from src.repositories.sector_planning_repository import SectorPlanningRepository
from src.services.container import get_container
from src.web.utils.request_helpers import (
    handle_request_errors,
    log_requests,
    safe_get_service,
    create_success_response,
    create_error_response,
)
from src.models.planning import PlanningPeriod, EntityType

logger = logging.getLogger(__name__)

# Create blueprint
planning_bp = Blueprint("planning", __name__, url_prefix="/planning")


# ============================================================================
# Template Helper Classes
# ============================================================================

class CompanySummaryWrapper:
    """Wrapper to make company summary dict accessible with dot notation in templates."""
    
    def __init__(self, data: dict):
        self._data = data
        self._periods_by_key = {}
        for p in data.get("periods", []):
            period = p["period"]
            self._periods_by_key[period.key] = PeriodDataWrapper(p)
    
    @property
    def total_budget(self):
        return self._data.get("total_budget")
    
    @property
    def total_forecast(self):
        return self._data.get("total_forecast")
    
    @property
    def total_booked(self):
        return self._data.get("total_booked")
    
    @property
    def total_pipeline(self):
        return self._data.get("total_pipeline")
    
    @property
    def total_variance(self):
        return self._data.get("total_variance")
    
    @property
    def periods(self):
        return [PeriodDataWrapper(p) for p in self._data.get("periods", [])]
    
    @property
    def periods_by_key(self):
        return self._periods_by_key


class PeriodDataWrapper:
    """Wrapper for period data to support dot notation in templates."""
    
    def __init__(self, data: dict):
        self._data = data
    
    @property
    def period(self):
        return self._data["period"]
    
    @property
    def budget(self):
        return self._data["budget"]
    
    @property
    def forecast(self):
        return self._data["forecast"]
    
    @property
    def booked(self):
        return self._data["booked"]
    
    @property
    def pipeline(self):
        return self._data["pipeline"]
    
    @property
    def variance(self):
        return self._data["variance"]
    
    @property
    def pct_booked(self):
        return self._data.get("pct_booked", 0)
    
    @property
    def is_active(self):
        return self._data.get("is_active", False)
    
    @property
    def is_past(self):
        return self._data.get("is_past", False)


# ============================================================================
# Main UI Routes
# ============================================================================

@planning_bp.route("/")
def planning_session():
    """Main planning session view."""
    try:
        container = get_container()
        planning_service = container.get("planning_service")
        
        planning_year = request.args.get("year", date.today().year, type=int)
        months_ahead = request.args.get("months_ahead", 2, type=int)
        
        summary = planning_service.get_planning_summary(
            months_ahead=months_ahead,
            planning_year=planning_year
        )
        
        company_summary = planning_service.get_company_summary(
            months_ahead=months_ahead,
            planning_year=planning_year
        )
        
        burn_down_metrics = planning_service.get_burn_down_metrics(
            periods=summary.active_periods
        )
        
        template_data = {
            "planning_year": planning_year,
            "current_date": date.today().strftime("%B %d, %Y"),
            "all_periods": summary.all_periods,
            "active_periods": summary.active_periods,
            "past_periods": summary.past_periods,
            "entity_data": summary.entity_data,
            "company": CompanySummaryWrapper(company_summary),
            "burn_down": burn_down_metrics,
        }
        
        return render_template("planning_session.html", **template_data)
        
    except Exception as e:
        logger.error(f"Error loading planning session: {e}", exc_info=True)
        return render_template("error.html", error=str(e)), 500


@planning_bp.route("/history/<ae_name>/<int:year>/<int:month>")
@log_requests
@handle_request_errors
def forecast_history(ae_name: str, year: int, month: int):
    """View forecast history for an entity/period."""
    try:
        container = get_container()
        planning_service = safe_get_service(container, "planning_service")
        
        history = planning_service.get_forecast_history(ae_name, year, month)
        row = planning_service.get_planning_row(ae_name, year, month)
        
        return render_template(
            "planning_history.html",
            ae_name=ae_name,
            year=year,
            month=month,
            history=history,
            current_row=row
        )
    
    except Exception as e:
        logger.error(f"Error loading forecast history: {e}", exc_info=True)
        return create_error_response(str(e), 500)


@planning_bp.route("/budget")
@log_requests
@handle_request_errors
def budget_entry():
    """Budget entry page with sector expectations."""
    try:
        year = request.args.get("year", 2026, type=int)
        
        container = get_container()
        planning_service = safe_get_service(container, "planning_service")
        
        entities = planning_service.get_revenue_entities()
        budgets = _get_budget_data_for_year(year)
        sector_expectations = planning_service.get_all_sector_expectations(year)
        all_sectors = planning_service.get_all_sectors()
        
        return render_template(
            "budget_entry.html",
            entities=entities,
            budgets=budgets,
            sector_expectations=sector_expectations,
            all_sectors=all_sectors,
            selected_year=year
        )
    
    except Exception as e:
        logger.error(f"Error loading budget entry: {e}", exc_info=True)
        return render_template("error_500.html", message="Error loading budget entry"), 500


# ============================================================================
# Sector Detail API Routes
# ============================================================================

@planning_bp.route('/api/sector-detail/<ae_name>/<int:year>')
@handle_request_errors
def get_sector_detail(ae_name: str, year: int):
    """API endpoint for sector planning detail."""
    if ae_name == 'WorldLink':
        return jsonify({
            'success': False,
            'error': 'WorldLink does not have sector breakdown'
        }), 400
    
    container = get_container()
    db = container.get('database_connection')
    
    sector_planning_repo = SectorPlanningRepository(db)
    sector_expectation_repo = SectorExpectationRepository(db)
    service = SectorPlanningService(sector_planning_repo, sector_expectation_repo)
    
    detail = service.get_sector_detail(ae_name, year)
    
    return jsonify({
        'success': True,
        'data': detail.to_dict()
    })


@planning_bp.route('/api/sector-summary/<int:year>')
@handle_request_errors
def get_sector_summaries(year: int):
    """API endpoint to get sector gap summaries for all entities."""
    container = get_container()
    db = container.get('database_connection')
    
    planning_service = container.get('planning_service')
    entities = planning_service.get_revenue_entities()
    entity_names = [e.entity_name for e in entities if e.entity_name != 'WorldLink']
    
    sector_planning_repo = SectorPlanningRepository(db)
    sector_expectation_repo = SectorExpectationRepository(db)
    service = SectorPlanningService(sector_planning_repo, sector_expectation_repo)
    
    summaries = {}
    for ae in entity_names:
        summary = service.get_gap_summary(ae, year)
        if summary:
            summaries[ae] = summary
    
    return jsonify({
        'success': True,
        'summaries': summaries
    })


# ============================================================================
# Sector Expectations API
# ============================================================================

@planning_bp.route("/api/sectors")
@log_requests
@handle_request_errors
def api_get_sectors():
    """API: Get all available sectors."""
    try:
        container = get_container()
        planning_service = safe_get_service(container, "planning_service")
        sectors = planning_service.get_all_sectors()
        return create_success_response({"sectors": sectors})
    except Exception as e:
        logger.error(f"Error getting sectors: {e}", exc_info=True)
        return create_error_response(str(e), 500)


@planning_bp.route("/api/sectors/available/<ae_name>/<int:year>")
@log_requests
@handle_request_errors
def api_get_available_sectors(ae_name: str, year: int):
    """API: Get sectors available to add for an entity."""
    try:
        container = get_container()
        planning_service = safe_get_service(container, "planning_service")
        sectors = planning_service.get_available_sectors(ae_name, year)
        return create_success_response({"sectors": sectors})
    except Exception as e:
        logger.error(f"Error getting available sectors: {e}", exc_info=True)
        return create_error_response(str(e), 500)


@planning_bp.route("/api/sector-expectations/<ae_name>/<int:year>")
@log_requests
@handle_request_errors
def api_get_sector_expectations(ae_name: str, year: int):
    """API: Get sector expectations for an entity/year."""
    try:
        container = get_container()
        planning_service = safe_get_service(container, "planning_service")
        
        entity_exp = planning_service.get_sector_expectations_for_entity(ae_name, year)
        
        sectors_data = []
        grid = entity_exp.monthly_grid()
        
        for sector_id, sector_code, sector_name in entity_exp.sectors_used():
            sector_data = {
                "sector_id": sector_id,
                "sector_code": sector_code,
                "sector_name": sector_name,
                "amounts": {
                    str(m): float(grid.get(sector_id, {}).get(m, 0))
                    for m in range(1, 13)
                },
                "annual_total": float(entity_exp.total_for_sector(sector_id))
            }
            sectors_data.append(sector_data)
        
        return create_success_response({
            "entity_name": ae_name,
            "year": year,
            "sectors": sectors_data,
            "monthly_totals": {
                str(m): float(entity_exp.total_for_month(m))
                for m in range(1, 13)
            },
            "annual_total": float(entity_exp.annual_total())
        })
    
    except ValueError as e:
        return create_error_response(str(e), 404)
    except Exception as e:
        logger.error(f"Error getting sector expectations: {e}", exc_info=True)
        return create_error_response(str(e), 500)


@planning_bp.route("/api/sector-expectations/save", methods=["POST"])
@log_requests
@handle_request_errors
def api_save_sector_expectations():
    """API: Save sector expectations for an entity/year."""
    try:
        data = request.get_json()
        
        if not data:
            return create_error_response("No data provided", 400)
        
        required = ["ae_name", "year", "expectations"]
        for field in required:
            if field not in data:
                return create_error_response(f"Missing required field: {field}", 400)
        
        container = get_container()
        planning_service = safe_get_service(container, "planning_service")
        
        result = planning_service.save_sector_expectations(
            ae_name=data["ae_name"],
            year=data["year"],
            expectations=data["expectations"],
            updated_by=data.get("updated_by", "Web Interface")
        )
        
        logger.info(
            f"Saved sector expectations: {data['ae_name']} {data['year']} - "
            f"{result['saved_count']} rows"
        )
        
        return create_success_response(result)
    
    except Exception as e:
        logger.error(f"Error saving sector expectations: {e}", exc_info=True)
        return create_error_response(str(e), 500)


@planning_bp.route("/api/sector-expectations/add-sector", methods=["POST"])
@log_requests
@handle_request_errors
def api_add_sector_to_entity():
    """API: Add a new sector to an entity's expectations."""
    try:
        data = request.get_json()
        
        if not data:
            return create_error_response("No data provided", 400)
        
        required = ["ae_name", "sector_id", "year"]
        for field in required:
            if field not in data:
                return create_error_response(f"Missing required field: {field}", 400)
        
        container = get_container()
        planning_service = safe_get_service(container, "planning_service")
        
        result = planning_service.add_sector_to_entity(
            ae_name=data["ae_name"],
            sector_id=data["sector_id"],
            year=data["year"],
            updated_by=data.get("updated_by", "Web Interface")
        )
        
        logger.info(
            f"Added sector {result['sector_name']} to {data['ae_name']} for {data['year']}"
        )
        
        return create_success_response(result)
    
    except ValueError as e:
        return create_error_response(str(e), 400)
    except Exception as e:
        logger.error(f"Error adding sector: {e}", exc_info=True)
        return create_error_response(str(e), 500)


@planning_bp.route("/api/sector-expectations/remove-sector", methods=["POST"])
@log_requests
@handle_request_errors
def api_remove_sector_from_entity():
    """API: Remove a sector from an entity's expectations."""
    try:
        data = request.get_json()
        
        if not data:
            return create_error_response("No data provided", 400)
        
        required = ["ae_name", "sector_id", "year"]
        for field in required:
            if field not in data:
                return create_error_response(f"Missing required field: {field}", 400)
        
        container = get_container()
        planning_service = safe_get_service(container, "planning_service")
        
        deleted = planning_service.remove_sector_from_entity(
            ae_name=data["ae_name"],
            sector_id=data["sector_id"],
            year=data["year"]
        )
        
        if deleted:
            logger.info(
                f"Removed sector {data['sector_id']} from {data['ae_name']} for {data['year']}"
            )
            return create_success_response({"message": "Sector removed"})
        else:
            return create_error_response("Sector not found", 404)
    
    except Exception as e:
        logger.error(f"Error removing sector: {e}", exc_info=True)
        return create_error_response(str(e), 500)


@planning_bp.route("/api/sector-expectations/validate/<ae_name>/<int:year>")
@log_requests
@handle_request_errors
def api_validate_sector_expectations(ae_name: str, year: int):
    """API: Validate sector expectations against budget."""
    try:
        container = get_container()
        planning_service = safe_get_service(container, "planning_service")
        
        validation = planning_service.validate_sector_expectations(ae_name, year)
        
        return create_success_response({
            "entity_name": ae_name,
            "year": year,
            "is_valid": validation.is_valid,
            "errors": validation.errors,
            "warnings": validation.warnings,
            "month_details": {
                str(k): v for k, v in validation.month_details.items()
            }
        })
    
    except Exception as e:
        logger.error(f"Error validating sector expectations: {e}", exc_info=True)
        return create_error_response(str(e), 500)


# ============================================================================
# Planning API Routes
# ============================================================================

@planning_bp.route("/api/summary")
@log_requests
@handle_request_errors
def api_get_summary():
    """API: Get planning summary data."""
    try:
        container = get_container()
        planning_service = safe_get_service(container, "planning_service")
        
        planning_year = request.args.get("year", date.today().year, type=int)
        months_ahead = request.args.get("months_ahead", 2, type=int)
        
        summary = planning_service.get_planning_summary(
            months_ahead=months_ahead,
            planning_year=planning_year
        )
        company = planning_service.get_company_summary(
            months_ahead=months_ahead,
            planning_year=planning_year
        )
        
        data = {
            "planning_year": planning_year,
            "all_periods": [
                {"year": p.year, "month": p.month, "display": p.display, "key": p.key}
                for p in summary.all_periods
            ],
            "active_periods": [
                {"year": p.year, "month": p.month, "display": p.display, "key": p.key}
                for p in summary.active_periods
            ],
            "past_periods": [
                {"year": p.year, "month": p.month, "display": p.display, "key": p.key}
                for p in summary.past_periods
            ],
            "periods": [
                {"year": p.year, "month": p.month, "display": p.display}
                for p in summary.active_periods
            ],
            "entities": [
                {
                    "entity": {
                        "id": ed.entity.entity_id,
                        "name": ed.entity.entity_name,
                        "type": ed.entity.entity_type.value
                    },
                    "rows": [
                        {
                            "period": {"year": r.period.year, "month": r.period.month, "key": r.period.key},
                            "budget": float(r.budget.amount),
                            "forecast": float(r.forecast.amount),
                            "booked": float(r.booked.amount),
                            "pipeline": float(r.pipeline.amount),
                            "variance": float(r.variance_to_budget.amount),
                            "pct_booked": r.pct_booked,
                            "is_overridden": r.is_forecast_overridden
                        }
                        for r in ed.rows
                    ],
                    "rows_by_period": {
                        r.period.key: {
                            "budget": float(r.budget.amount),
                            "forecast": float(r.forecast.amount),
                            "booked": float(r.booked.amount),
                            "pipeline": float(r.pipeline.amount),
                            "variance": float(r.variance_to_budget.amount),
                            "is_overridden": r.is_forecast_overridden
                        }
                        for r in ed.rows
                    },
                    "totals": {
                        "budget": float(ed.total_budget.amount),
                        "forecast": float(ed.total_forecast.amount),
                        "booked": float(ed.total_booked.amount),
                        "pipeline": float(ed.total_pipeline.amount),
                        "variance": float(ed.total_variance.amount)
                    }
                }
                for ed in summary.entity_data
            ],
            "company": {
                "total_budget": float(company["total_budget"].amount),
                "total_forecast": float(company["total_forecast"].amount),
                "total_booked": float(company["total_booked"].amount),
                "total_pipeline": float(company["total_pipeline"].amount),
                "total_variance": float(company["total_variance"].amount),
                "periods": [
                    {
                        "period": {"year": p["period"].year, "month": p["period"].month, "key": p["period"].key},
                        "budget": float(p["budget"].amount),
                        "forecast": float(p["forecast"].amount),
                        "booked": float(p["booked"].amount),
                        "pipeline": float(p["pipeline"].amount),
                        "variance": float(p["variance"].amount),
                        "pct_booked": p["pct_booked"],
                        "is_active": p.get("is_active", False),
                        "is_past": p.get("is_past", False)
                    }
                    for p in company["periods"]
                ],
                "periods_by_key": {
                    p["period"].key: {
                        "budget": float(p["budget"].amount),
                        "forecast": float(p["forecast"].amount),
                        "booked": float(p["booked"].amount),
                        "pipeline": float(p["pipeline"].amount),
                        "variance": float(p["variance"].amount),
                        "is_active": p.get("is_active", False),
                        "is_past": p.get("is_past", False)
                    }
                    for p in company["periods"]
                }
            }
        }
        
        return create_success_response(data)
    
    except Exception as e:
        logger.error(f"Error getting planning summary: {e}", exc_info=True)
        return create_error_response(str(e), 500)


@planning_bp.route("/api/forecast", methods=["POST"])
@log_requests
@handle_request_errors
def api_update_forecast():
    """API: Update a single forecast value."""
    try:
        data = request.get_json()
        
        if not data:
            return create_error_response("No data provided", 400)
        
        required = ["ae_name", "year", "month", "amount"]
        for field in required:
            if field not in data:
                return create_error_response(f"Missing required field: {field}", 400)
        
        container = get_container()
        planning_service = safe_get_service(container, "planning_service")
        
        change = planning_service.update_forecast(
            ae_name=data["ae_name"],
            year=data["year"],
            month=data["month"],
            new_amount=Decimal(str(data["amount"])),
            updated_by=data.get("updated_by", "Web Interface"),
            notes=data.get("notes")
        )
        
        logger.info(
            f"Forecast updated: {data['ae_name']} {data['year']}-{data['month']:02d} "
            f"to ${data['amount']:,.0f}"
        )
        
        return create_success_response({
            "message": "Forecast updated successfully",
            "change": {
                "ae_name": change.ae_name,
                "period": change.period.display,
                "previous": float(change.previous_amount.amount) if change.previous_amount else None,
                "new": float(change.new_amount.amount)
            }
        })
    
    except ValueError as e:
        return create_error_response(str(e), 400)
    except Exception as e:
        logger.error(f"Error updating forecast: {e}", exc_info=True)
        return create_error_response(str(e), 500)


@planning_bp.route("/api/forecast/bulk", methods=["POST"])
@log_requests
@handle_request_errors
def api_bulk_update_forecasts():
    """API: Update multiple forecasts at once."""
    try:
        data = request.get_json()
        
        if not data or "updates" not in data:
            return create_error_response("No updates provided", 400)
        
        container = get_container()
        planning_service = safe_get_service(container, "planning_service")
        
        changes = planning_service.bulk_update_forecasts(
            updates=data["updates"],
            updated_by=data.get("updated_by", "Web Interface"),
            session_notes=data.get("session_notes")
        )
        
        logger.info(f"Bulk forecast update: {len(changes)} changes saved")
        
        return create_success_response({
            "success": True,
            "message": f"{len(changes)} forecasts updated successfully",
            "changes_count": len(changes)
        })
    
    except Exception as e:
        logger.error(f"Error in bulk forecast update: {e}", exc_info=True)
        return create_error_response(str(e), 500)


@planning_bp.route("/api/forecast/reset", methods=["POST"])
@log_requests
@handle_request_errors
def api_reset_forecast():
    """API: Reset forecast to budget."""
    try:
        data = request.get_json()
        
        required = ["ae_name", "year", "month"]
        for field in required:
            if field not in data:
                return create_error_response(f"Missing required field: {field}", 400)
        
        container = get_container()
        planning_service = safe_get_service(container, "planning_service")
        
        deleted = planning_service.reset_forecast_to_budget(
            ae_name=data["ae_name"],
            year=data["year"],
            month=data["month"]
        )
        
        if deleted:
            return create_success_response({
                "message": "Forecast reset to budget",
                "ae_name": data["ae_name"],
                "period": f"{data['year']}-{data['month']:02d}"
            })
        else:
            return create_success_response({
                "message": "No forecast override to reset",
                "ae_name": data["ae_name"]
            })
    
    except ValueError as e:
        return create_error_response(str(e), 400)
    except Exception as e:
        logger.error(f"Error resetting forecast: {e}", exc_info=True)
        return create_error_response(str(e), 500)


@planning_bp.route("/api/entities")
@log_requests
@handle_request_errors
def api_get_entities():
    """API: Get all revenue entities."""
    try:
        container = get_container()
        planning_service = safe_get_service(container, "planning_service")
        
        entities = planning_service.get_revenue_entities()
        
        return create_success_response({
            "entities": [
                {
                    "id": e.entity_id,
                    "name": e.entity_name,
                    "type": e.entity_type.value,
                    "is_active": e.is_active
                }
                for e in entities
            ]
        })
    
    except Exception as e:
        logger.error(f"Error getting entities: {e}", exc_info=True)
        return create_error_response(str(e), 500)


@planning_bp.route("/api/sync-entities", methods=["POST"])
@log_requests
@handle_request_errors
def api_sync_entities():
    """API: Sync revenue entities from budget/spots data."""
    try:
        data = request.get_json() or {}
        year = data.get("year", date.today().year)
        
        container = get_container()
        planning_service = safe_get_service(container, "planning_service")
        
        result = planning_service.sync_revenue_entities(year)
        
        return create_success_response(result)
    
    except Exception as e:
        logger.error(f"Error syncing entities: {e}", exc_info=True)
        return create_error_response(str(e), 500)


@planning_bp.route("/api/validate")
@log_requests
@handle_request_errors
def api_validate():
    """API: Validate planning data integrity."""
    try:
        year = request.args.get("year", date.today().year, type=int)
        
        container = get_container()
        planning_service = safe_get_service(container, "planning_service")
        
        result = planning_service.validate_planning_data(year)
        
        return create_success_response(result)
    
    except Exception as e:
        logger.error(f"Error validating planning data: {e}", exc_info=True)
        return create_error_response(str(e), 500)


@planning_bp.route("/api/history/<ae_name>/<int:year>/<int:month>")
@log_requests
@handle_request_errors
def api_get_history(ae_name: str, year: int, month: int):
    """API: Get forecast change history."""
    try:
        container = get_container()
        planning_service = safe_get_service(container, "planning_service")
        
        history = planning_service.get_forecast_history(ae_name, year, month)
        
        return create_success_response({
            "history": [
                {
                    "previous": float(h.previous_amount.amount) if h.previous_amount else None,
                    "new": float(h.new_amount.amount),
                    "changed_date": h.changed_date.isoformat(),
                    "changed_by": h.changed_by,
                    "notes": h.session_notes
                }
                for h in history
            ]
        })
    
    except Exception as e:
        logger.error(f"Error getting history: {e}", exc_info=True)
        return create_error_response(str(e), 500)


# ============================================================================
# Budget API Routes
# ============================================================================

@planning_bp.route("/api/budget/bulk", methods=["POST"])
@log_requests
@handle_request_errors
def api_bulk_save_budgets():
    """API: Save multiple budget entries."""
    try:
        data = request.get_json()
        
        if not data or "updates" not in data:
            return create_error_response("No updates provided", 400)
        
        container = get_container()
        db_connection = container.get("database_connection")
        
        saved_count = 0
        with db_connection.transaction() as conn:
            for update in data["updates"]:
                conn.execute("""
                    INSERT INTO budget (ae_name, year, month, budget_amount)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(ae_name, year, month) DO UPDATE SET
                        budget_amount = excluded.budget_amount,
                        updated_date = CURRENT_TIMESTAMP
                """, (
                    update["ae_name"],
                    update["year"],
                    update["month"],
                    update["amount"]
                ))
                saved_count += 1
        
        logger.info(f"Saved {saved_count} budget entries")
        
        return create_success_response({
            "message": f"Saved {saved_count} budget entries",
            "saved_count": saved_count
        })
    
    except Exception as e:
        logger.error(f"Error saving budgets: {e}", exc_info=True)
        return create_error_response(str(e), 500)


@planning_bp.route("/api/entities/add", methods=["POST"])
@log_requests
@handle_request_errors
def api_add_entity():
    """API: Add a new revenue entity."""
    try:
        data = request.get_json()
        
        if not data or "name" not in data:
            return create_error_response("Name required", 400)
        
        name = data["name"].strip()
        entity_type = data.get("type", "AE")
        
        if not name:
            return create_error_response("Name cannot be empty", 400)
        
        container = get_container()
        db_connection = container.get("database_connection")
        
        with db_connection.transaction() as conn:
            cursor = conn.execute(
                "SELECT entity_id FROM revenue_entities WHERE entity_name = ?",
                (name,)
            )
            if cursor.fetchone():
                return create_error_response(f"Entity '{name}' already exists", 400)
            
            conn.execute("""
                INSERT INTO revenue_entities (entity_name, entity_type, is_active)
                VALUES (?, ?, 1)
            """, (name, entity_type))
        
        logger.info(f"Added revenue entity: {name} ({entity_type})")
        
        return create_success_response({
            "message": f"Added {name}",
            "name": name,
            "type": entity_type
        })
    
    except Exception as e:
        logger.error(f"Error adding entity: {e}", exc_info=True)
        return create_error_response(str(e), 500)


# ============================================================================
# Helper Functions
# ============================================================================

def _get_budget_data_for_year(year: int) -> dict:
    """Get all budget data for a year, organized by AE and month."""
    try:
        container = get_container()
        db_connection = container.get("database_connection")
        
        with db_connection.connection() as conn:
            cursor = conn.execute("""
                SELECT ae_name, month, budget_amount
                FROM budget
                WHERE year = ?
            """, (year,))
            
            result = {}
            for row in cursor.fetchall():
                ae_name = row["ae_name"]
                if ae_name not in result:
                    result[ae_name] = {}
                result[ae_name][row["month"]] = float(row["budget_amount"])
            
            return result
    
    except Exception as e:
        logger.error(f"Error getting budget data: {e}")
        return {}


# ============================================================================
# Blueprint Registration
# ============================================================================

def register_planning_routes(app):
    """Register planning blueprint with the Flask app."""
    app.register_blueprint(planning_bp)
    logger.info("Planning routes registered at /planning")