# src/web/routes/customer_resolution.py
"""API routes for customer resolution."""

from flask import Blueprint, current_app, jsonify, request, render_template

customer_resolution_bp = Blueprint("customer_resolution", __name__)


def _get_service():
    from src.services.customer_resolution_service import CustomerResolutionService
    db_path = current_app.config.get("DB_PATH") or "./data/database/production.db"
    return CustomerResolutionService(db_path)


@customer_resolution_bp.route("/customer-resolution")
def page():
    return render_template("customer_resolution.html")


@customer_resolution_bp.route("/api/customer-resolution/stats")
def get_stats():
    return jsonify(_get_service().get_stats())


@customer_resolution_bp.route("/api/customer-resolution/unresolved")
def get_unresolved():
    min_revenue = float(request.args.get("min_revenue", 0))
    limit = int(request.args.get("limit", 100))
    items = _get_service().get_unresolved(min_revenue=min_revenue, limit=limit)
    return jsonify([
        {
            "bill_code": i.bill_code,
            "normalized_name": i.normalized_name,
            "agency": i.agency,
            "customer": i.customer,
            "revenue": i.revenue,
            "spot_count": i.spot_count,
            "first_seen": i.first_seen,
            "last_seen": i.last_seen,
        }
        for i in items
    ])


@customer_resolution_bp.route("/api/customer-resolution/create", methods=["POST"])
def create_customer():
    data = request.json
    bill_code = data.get("bill_code")
    normalized_name = data.get("normalized_name")
    
    if not bill_code or not normalized_name:
        return jsonify({"success": False, "error": "bill_code and normalized_name required"}), 400
    
    result = _get_service().create_customer_and_alias(
        bill_code=bill_code,
        normalized_name=normalized_name,
        created_by="web_user"
    )
    return jsonify(result)


@customer_resolution_bp.route("/api/customer-resolution/link", methods=["POST"])
def link_customer():
    data = request.json
    bill_code = data.get("bill_code")
    customer_id = data.get("customer_id")
    
    if not bill_code or not customer_id:
        return jsonify({"success": False, "error": "bill_code and customer_id required"}), 400
    
    result = _get_service().link_to_existing(
        bill_code=bill_code,
        customer_id=customer_id,
        created_by="web_user"
    )
    
    if not result["success"]:
        return jsonify(result), 400
    return jsonify(result)


@customer_resolution_bp.route("/api/customer-resolution/search")
def search_customers():
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify([])
    return jsonify(_get_service().search_customers(q))