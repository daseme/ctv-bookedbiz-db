"""
Planning session routes - Forecast management UI and API.

Provides:
- Planning session main page
- API endpoints for forecast updates
- Real-time data refresh
"""

import logging
from flask import Blueprint, render_template, request, jsonify
from datetime import date
from decimal import Decimal

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
# Main UI Routes
# ============================================================================

@planning_bp.route("/")
@log_requests
@handle_request_errors
def planning_session():
    """Planning session main page."""
    try:
        container = get_container()
        planning_service = safe_get_service(container, "planning_service")
        
        # Get planning summary for current window (current month + 2)
        summary = planning_service.get_planning_summary(months_ahead=2)
        company = planning_service.get_company_summary(months_ahead=2)
        
        # Format for template
        template_data = {
            "title": "Planning Session",
            "periods": summary.periods,
            "entity_data": summary.entity_data,
            "company": company,
            "current_date": date.today().isoformat(),
        }
        
        return render_template("planning_session.html", **template_data)
    
    except Exception as e:
        logger.error(f"Error loading planning session: {e}", exc_info=True)
        return render_template(
            "error_500.html", 
            message="Error loading planning session"
        ), 500


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


# ============================================================================
# API Routes
# ============================================================================

@planning_bp.route("/api/summary")
@log_requests
@handle_request_errors
def api_get_summary():
    """API: Get planning summary data."""
    try:
        container = get_container()
        planning_service = safe_get_service(container, "planning_service")
        
        months_ahead = request.args.get("months_ahead", 2, type=int)
        summary = planning_service.get_planning_summary(months_ahead=months_ahead)
        company = planning_service.get_company_summary(months_ahead=months_ahead)
        
        # Serialize for JSON
        data = {
            "periods": [
                {"year": p.year, "month": p.month, "display": p.display}
                for p in summary.periods
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
                            "period": {"year": r.period.year, "month": r.period.month},
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
                        "period": {"year": p["period"].year, "month": p["period"].month},
                        "budget": float(p["budget"].amount),
                        "forecast": float(p["forecast"].amount),
                        "booked": float(p["booked"].amount),
                        "pipeline": float(p["pipeline"].amount),
                        "variance": float(p["variance"].amount),
                        "pct_booked": p["pct_booked"]
                    }
                    for p in company["periods"]
                ]
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
# Blueprint Registration
# ============================================================================

def register_planning_routes(app):
    """Register planning blueprint with the Flask app."""
    app.register_blueprint(planning_bp)
    logger.info("Planning routes registered at /planning")


# ============================================================================
# Budget Entry Routes
# ============================================================================

@planning_bp.route("/budget")
@log_requests
@handle_request_errors
def budget_entry():
    """Budget entry page."""
    try:
        year = request.args.get("year", 2026, type=int)
        
        container = get_container()
        planning_service = safe_get_service(container, "planning_service")
        
        # Get all revenue entities
        entities = planning_service.get_revenue_entities()
        
        # Get existing budget data for the year
        budgets = _get_budget_data_for_year(year)
        
        return render_template(
            "budget_entry.html",
            entities=entities,
            budgets=budgets,
            selected_year=year
        )
    
    except Exception as e:
        logger.error(f"Error loading budget entry: {e}", exc_info=True)
        return render_template("error_500.html", message="Error loading budget entry"), 500


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
            # Check if already exists
            cursor = conn.execute(
                "SELECT entity_id FROM revenue_entities WHERE entity_name = ?",
                (name,)
            )
            if cursor.fetchone():
                return create_error_response(f"Entity '{name}' already exists", 400)
            
            # Insert new entity
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