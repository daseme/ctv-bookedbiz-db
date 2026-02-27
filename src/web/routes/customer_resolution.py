# src/web/routes/customer_resolution.py
"""API routes for customer resolution."""

from flask import Blueprint, current_app, jsonify, request, render_template

customer_resolution_bp = Blueprint("customer_resolution", __name__)


@customer_resolution_bp.before_request
def _require_admin_for_writes():
    if request.method in ('POST', 'PUT', 'DELETE'):
        from flask_login import current_user
        if not hasattr(current_user, 'role') or current_user.role.value != 'admin':
            return jsonify({"error": "Admin access required"}), 403


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


@customer_resolution_bp.route("/api/customer-resolution/check-similar", methods=["POST"])
def check_similar():
    """Check for existing customers similar to a proposed name."""
    data = request.json or {}
    normalized_name = (data.get("normalized_name") or "").strip()
    if not normalized_name:
        return jsonify({"similar": []}), 200
    similar = _get_service().find_similar_customers(normalized_name)
    return jsonify({"similar": similar})


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


@customer_resolution_bp.route("/api/customer-aliases/merge", methods=["POST"])
def merge_customers():
    """Merge source customer into target customer."""
    data = request.json
    source_id = data.get("source_id")
    target_id = data.get("target_id")
    
    if not source_id or not target_id:
        return jsonify({"success": False, "error": "source_id and target_id required"}), 400
    
    result = _get_service().merge_customers(
        source_id=int(source_id),
        target_id=int(target_id),
        merged_by="web_user"
    )
    
    if not result["success"]:
        return jsonify(result), 400
    return jsonify(result)

@customer_resolution_bp.route("/customer-aliases")
def aliases_page():
    """Customer aliases management page."""
    return render_template("customer_aliases.html")


@customer_resolution_bp.route("/api/customer-aliases")
def get_customers_with_aliases():
    """List customers with alias counts."""
    search = request.args.get("search", "").strip()
    min_aliases = int(request.args.get("min_aliases", 0))
    limit = int(request.args.get("limit", 200))
    
    items = _get_service().get_customers_with_aliases(
        search=search,
        min_aliases=min_aliases,
        limit=limit
    )
    return jsonify(items)


@customer_resolution_bp.route("/api/customer-aliases/<int:customer_id>")
def get_customer_detail(customer_id: int):
    """Get single customer with all aliases."""
    result = _get_service().get_customer_aliases(customer_id)
    if not result:
        return jsonify({"error": "Customer not found"}), 404
    return jsonify(result)


@customer_resolution_bp.route("/api/customer-aliases/<int:alias_id>", methods=["DELETE"])
def delete_alias(alias_id: int):
    """Soft-delete an alias."""
    result = _get_service().delete_alias(alias_id, deleted_by="web_user")
    if not result["success"]:
        return jsonify(result), 400
    return jsonify(result)


@customer_resolution_bp.route("/api/customer-aliases/<int:customer_id>/rename", methods=["POST"])
def rename_customer(customer_id: int):
    """Rename a customer's normalized_name."""
    data = request.json
    new_name = data.get("new_name", "").strip()

    if not new_name:
        return jsonify({"success": False, "error": "new_name required"}), 400

    result = _get_service().rename_customer(
        customer_id=customer_id,
        new_name=new_name,
        renamed_by="web_user"
    )

    if not result["success"]:
        return jsonify(result), 400
    return jsonify(result)


@customer_resolution_bp.route("/api/customer-aliases/<int:customer_id>/address", methods=["PUT"])
def update_customer_address(customer_id: int):
    """Update customer address."""
    data = request.json

    result = _get_service().update_customer_address(
        customer_id=customer_id,
        address=data.get("address"),
        city=data.get("city"),
        state=data.get("state"),
        zip_code=data.get("zip"),
        updated_by="web_user"
    )

    if not result["success"]:
        return jsonify(result), 400
    return jsonify(result)