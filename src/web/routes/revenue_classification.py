"""Revenue Classification Manager routes and API."""

import logging
from datetime import date
from flask import Blueprint, render_template, jsonify, request

from src.models.users import UserRole
from src.services.container import get_container
from src.web.utils.auth import role_required

logger = logging.getLogger(__name__)

revenue_class_bp = Blueprint("revenue_classification", __name__)


def _db():
    return get_container().get("database_connection")


def _svc():
    return get_container().get("revenue_classification_service")


def _parse_filters():
    """Extract filter params from query string."""
    filters = {}
    sector_id = request.args.get("sector_id", type=int)
    if sector_id:
        filters["sector_id"] = sector_id
    ae = request.args.get("ae", "").strip()
    if ae:
        filters["ae"] = ae
    classification = request.args.get("classification", "").strip()
    if classification in ("regular", "irregular"):
        filters["classification"] = classification
    return filters or None


@revenue_class_bp.route("/reports/revenue-classification-manager")
@role_required(UserRole.MANAGEMENT)
def revenue_classification_page():
    """Render the Revenue Classification Manager page."""
    return render_template(
        "revenue_classification_manager.html",
        title="Revenue Classification Manager",
    )


@revenue_class_bp.route("/api/revenue-classification/summary")
@role_required(UserRole.MANAGEMENT)
def api_summary():
    """Return summary stats and monthly chart data."""
    year = request.args.get("year", type=int) or date.today().year

    svc = _svc()
    filters = _parse_filters()
    with _db().connection_ro() as conn:
        return jsonify(svc.get_summary(conn, year, filters))


@revenue_class_bp.route("/api/revenue-classification/customers")
@role_required(UserRole.MANAGEMENT)
def api_customers():
    """Return customer list with revenue data."""
    year = request.args.get("year", type=int) or date.today().year

    svc = _svc()
    filters = _parse_filters()
    with _db().connection_ro() as conn:
        return jsonify(svc.get_customers(conn, year, filters))


@revenue_class_bp.route(
    "/api/revenue-classification/<int:customer_id>",
    methods=["PATCH"],
)
@role_required(UserRole.MANAGEMENT)
def api_update_classification(customer_id):
    """Update a customer's revenue classification and/or sector."""
    data = request.get_json(silent=True) or {}

    svc = _svc()
    with _db().connection() as conn:
        try:
            if "revenue_class" in data:
                revenue_class = data["revenue_class"].strip()
                if revenue_class not in ("regular", "irregular"):
                    return jsonify({"error": "revenue_class must be 'regular' or 'irregular'"}), 400
                svc.update_classification(conn, customer_id, revenue_class)

            if "sector_id" in data:
                sector_id = data["sector_id"]
                svc.update_sector(conn, customer_id, sector_id)

            conn.commit()
            return jsonify({"success": True})
        except ValueError as e:
            return jsonify({"error": str(e)}), 404
