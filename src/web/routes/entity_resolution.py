# src/web/routes/entity_resolution.py
"""Unified routes for advertiser + agency resolution and aliases."""

from flask import Blueprint, current_app, jsonify, request, render_template, redirect

entity_resolution_bp = Blueprint("entity_resolution", __name__)


@entity_resolution_bp.before_request
def _require_admin_for_writes():
    if request.method in ('POST', 'PUT', 'DELETE'):
        from flask_login import current_user
        if not hasattr(current_user, 'role') or current_user.role.value != 'admin':
            return jsonify({"error": "Admin access required"}), 403

# ── Config per entity type ──────────────────────────────────────────────

ENTITY_CONFIGS = {
    "advertiser": {
        "raw_field": "bill_code",
        "name_field": "normalized_name",
        "id_field": "customer_id",
        "label": "Direct Advertiser",
        "label_plural": "Direct Advertisers",
        "api_prefix_resolution": "/api/customer-resolution",
        "api_prefix_aliases": "/api/customer-aliases",
        "db_entity_type": "customer",
        "has_similarity_check": True,
        "search_placeholder_resolution": "Search bill codes...",
        "search_placeholder_aliases": "Search direct advertisers...",
        "empty_all_resolved": "All direct advertisers resolved!",
        "empty_none_found": "No unmatched bill codes found matching your filters.",
        "aliases_link": "/entity-aliases?tab=advertiser",
        "resolution_link": "/entity-resolution?tab=advertiser",
    },
    "agency": {
        "raw_field": "agency_raw",
        "name_field": "agency_name",
        "id_field": "agency_id",
        "label": "Agency",
        "label_plural": "Agencies",
        "api_prefix_resolution": "/api/agency-resolution",
        "api_prefix_aliases": "/api/agency-aliases",
        "db_entity_type": "agency",
        "has_similarity_check": False,
        "search_placeholder_resolution": "Search agency names...",
        "search_placeholder_aliases": "Search agencies...",
        "empty_all_resolved": "All agencies resolved!",
        "empty_none_found": "No unmatched agency names found matching your filters.",
        "aliases_link": "/entity-aliases?tab=agency",
        "resolution_link": "/entity-resolution?tab=agency",
    },
}


def _get_cfg(tab=None):
    if tab is None:
        tab = request.args.get("tab", "advertiser")
    return ENTITY_CONFIGS.get(tab, ENTITY_CONFIGS["advertiser"]), tab


def _get_customer_service():
    from src.services.customer_resolution_service import CustomerResolutionService
    db_path = current_app.config.get("DB_PATH") or "./data/database/production.db"
    return CustomerResolutionService(db_path)


def _get_agency_service():
    from src.services.agency_resolution_service import AgencyResolutionService
    db_path = current_app.config.get("DB_PATH") or "./data/database/production.db"
    return AgencyResolutionService(db_path)


# ── Page routes ─────────────────────────────────────────────────────────

@entity_resolution_bp.route("/entity-resolution")
def resolution_page():
    cfg, tab = _get_cfg()
    return render_template("entity_resolution.html", cfg=cfg, tab=tab)


@entity_resolution_bp.route("/entity-aliases")
def aliases_page():
    cfg, tab = _get_cfg()
    return render_template("entity_aliases.html", cfg=cfg, tab=tab)


# ── Redirects from old URLs ────────────────────────────────────────────

@entity_resolution_bp.route("/customer-resolution")
def redirect_customer_resolution():
    return redirect("/entity-resolution?tab=advertiser", code=301)


@entity_resolution_bp.route("/agency-resolution")
def redirect_agency_resolution():
    return redirect("/entity-resolution?tab=agency", code=301)


@entity_resolution_bp.route("/customer-aliases")
def redirect_customer_aliases():
    return redirect("/entity-aliases?tab=advertiser", code=301)


@entity_resolution_bp.route("/agency-aliases")
def redirect_agency_aliases():
    return redirect("/entity-aliases?tab=agency", code=301)


# ── Customer resolution API (unchanged paths) ──────────────────────────

@entity_resolution_bp.route("/api/customer-resolution/stats")
def customer_stats():
    return jsonify(_get_customer_service().get_stats())


@entity_resolution_bp.route("/api/customer-resolution/unresolved")
def customer_unresolved():
    min_revenue = float(request.args.get("min_revenue", 0))
    limit = int(request.args.get("limit", 100))
    items = _get_customer_service().get_unresolved(min_revenue=min_revenue, limit=limit)
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


@entity_resolution_bp.route("/api/customer-resolution/check-similar", methods=["POST"])
def customer_check_similar():
    data = request.json or {}
    normalized_name = (data.get("normalized_name") or "").strip()
    if not normalized_name:
        return jsonify({"similar": []}), 200
    similar = _get_customer_service().find_similar_customers(normalized_name)
    return jsonify({"similar": similar})


@entity_resolution_bp.route("/api/customer-resolution/create", methods=["POST"])
def customer_create():
    data = request.json
    bill_code = data.get("bill_code")
    normalized_name = data.get("normalized_name")
    if not bill_code or not normalized_name:
        return jsonify({"success": False, "error": "bill_code and normalized_name required"}), 400
    result = _get_customer_service().create_customer_and_alias(
        bill_code=bill_code, normalized_name=normalized_name, created_by="web_user"
    )
    return jsonify(result)


@entity_resolution_bp.route("/api/customer-resolution/link", methods=["POST"])
def customer_link():
    data = request.json
    bill_code = data.get("bill_code")
    customer_id = data.get("customer_id")
    if not bill_code or not customer_id:
        return jsonify({"success": False, "error": "bill_code and customer_id required"}), 400
    result = _get_customer_service().link_to_existing(
        bill_code=bill_code, customer_id=customer_id, created_by="web_user"
    )
    if not result["success"]:
        return jsonify(result), 400
    return jsonify(result)


@entity_resolution_bp.route("/api/customer-resolution/search")
def customer_search():
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify([])
    return jsonify(_get_customer_service().search_customers(q))


# ── Customer aliases API (unchanged paths) ──────────────────────────────

@entity_resolution_bp.route("/api/customer-aliases/merge", methods=["POST"])
def customer_merge():
    data = request.json
    source_id = data.get("source_id")
    target_id = data.get("target_id")
    if not source_id or not target_id:
        return jsonify({"success": False, "error": "source_id and target_id required"}), 400
    result = _get_customer_service().merge_customers(
        source_id=int(source_id), target_id=int(target_id), merged_by="web_user"
    )
    if not result["success"]:
        return jsonify(result), 400
    return jsonify(result)


@entity_resolution_bp.route("/api/customer-aliases")
def customer_aliases_list():
    search = request.args.get("search", "").strip()
    min_aliases = int(request.args.get("min_aliases", 0))
    limit = int(request.args.get("limit", 200))
    items = _get_customer_service().get_customers_with_aliases(
        search=search, min_aliases=min_aliases, limit=limit
    )
    return jsonify(items)


@entity_resolution_bp.route("/api/customer-aliases/<int:customer_id>")
def customer_detail(customer_id: int):
    result = _get_customer_service().get_customer_aliases(customer_id)
    if not result:
        return jsonify({"error": "Customer not found"}), 404
    return jsonify(result)


@entity_resolution_bp.route("/api/customer-aliases/<int:alias_id>", methods=["DELETE"])
def customer_delete_alias(alias_id: int):
    result = _get_customer_service().delete_alias(alias_id, deleted_by="web_user")
    if not result["success"]:
        return jsonify(result), 400
    return jsonify(result)


@entity_resolution_bp.route("/api/customer-aliases/<int:customer_id>/rename", methods=["POST"])
def customer_rename(customer_id: int):
    data = request.json
    new_name = data.get("new_name", "").strip()
    if not new_name:
        return jsonify({"success": False, "error": "new_name required"}), 400
    result = _get_customer_service().rename_customer(
        customer_id=customer_id, new_name=new_name, renamed_by="web_user"
    )
    if not result["success"]:
        return jsonify(result), 400
    return jsonify(result)


@entity_resolution_bp.route("/api/customer-aliases/<int:customer_id>/address", methods=["PUT"])
def customer_address(customer_id: int):
    data = request.json
    result = _get_customer_service().update_customer_address(
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


# ── Agency resolution API (unchanged paths) ─────────────────────────────

@entity_resolution_bp.route("/api/agency-resolution/stats")
def agency_stats():
    return jsonify(_get_agency_service().get_stats())


@entity_resolution_bp.route("/api/agency-resolution/unresolved")
def agency_unresolved():
    min_revenue = float(request.args.get("min_revenue", 0))
    limit = int(request.args.get("limit", 100))
    items = _get_agency_service().get_unresolved(min_revenue=min_revenue, limit=limit)
    return jsonify([
        {
            "agency_raw": i.agency_raw,
            "normalized_name": i.normalized_name,
            "revenue": i.revenue,
            "spot_count": i.spot_count,
            "first_seen": i.first_seen,
            "last_seen": i.last_seen,
        }
        for i in items
    ])


@entity_resolution_bp.route("/api/agency-resolution/create", methods=["POST"])
def agency_create():
    data = request.json
    agency_raw = data.get("agency_raw")
    agency_name = data.get("agency_name")
    if not agency_raw or not agency_name:
        return jsonify({"success": False, "error": "agency_raw and agency_name required"}), 400
    result = _get_agency_service().create_agency_and_alias(
        agency_raw=agency_raw, agency_name=agency_name, created_by="web_user"
    )
    return jsonify(result)


@entity_resolution_bp.route("/api/agency-resolution/link", methods=["POST"])
def agency_link():
    data = request.json
    agency_raw = data.get("agency_raw")
    agency_id = data.get("agency_id")
    if not agency_raw or not agency_id:
        return jsonify({"success": False, "error": "agency_raw and agency_id required"}), 400
    result = _get_agency_service().link_to_existing(
        agency_raw=agency_raw, agency_id=agency_id, created_by="web_user"
    )
    if not result["success"]:
        return jsonify(result), 400
    return jsonify(result)


@entity_resolution_bp.route("/api/agency-resolution/search")
def agency_search():
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify([])
    return jsonify(_get_agency_service().search_agencies(q))


# ── Agency aliases API (unchanged paths) ─────────────────────────────────

@entity_resolution_bp.route("/api/agency-aliases/merge", methods=["POST"])
def agency_merge():
    data = request.json
    source_id = data.get("source_id")
    target_id = data.get("target_id")
    if not source_id or not target_id:
        return jsonify({"success": False, "error": "source_id and target_id required"}), 400
    result = _get_agency_service().merge_agencies(
        source_id=int(source_id), target_id=int(target_id), merged_by="web_user"
    )
    if not result["success"]:
        return jsonify(result), 400
    return jsonify(result)


@entity_resolution_bp.route("/api/agency-aliases")
def agency_aliases_list():
    search = request.args.get("search", "").strip()
    min_aliases = int(request.args.get("min_aliases", 0))
    limit = int(request.args.get("limit", 200))
    items = _get_agency_service().get_agencies_with_aliases(
        search=search, min_aliases=min_aliases, limit=limit
    )
    return jsonify(items)


@entity_resolution_bp.route("/api/agency-aliases/<int:agency_id>")
def agency_detail(agency_id: int):
    result = _get_agency_service().get_agency_aliases(agency_id)
    if not result:
        return jsonify({"error": "Agency not found"}), 404
    return jsonify(result)


@entity_resolution_bp.route("/api/agency-aliases/<int:alias_id>", methods=["DELETE"])
def agency_delete_alias(alias_id: int):
    result = _get_agency_service().delete_alias(alias_id, deleted_by="web_user")
    if not result["success"]:
        return jsonify(result), 400
    return jsonify(result)


@entity_resolution_bp.route("/api/agency-aliases/<int:agency_id>/rename", methods=["POST"])
def agency_rename(agency_id: int):
    data = request.json
    new_name = data.get("new_name", "").strip()
    if not new_name:
        return jsonify({"success": False, "error": "new_name required"}), 400
    result = _get_agency_service().rename_agency(
        agency_id=agency_id, new_name=new_name, renamed_by="web_user"
    )
    if not result["success"]:
        return jsonify(result), 400
    return jsonify(result)


@entity_resolution_bp.route("/api/agency-aliases/<int:agency_id>/address", methods=["PUT"])
def agency_address(agency_id: int):
    data = request.json
    result = _get_agency_service().update_agency_address(
        agency_id=agency_id,
        address=data.get("address"),
        city=data.get("city"),
        state=data.get("state"),
        zip_code=data.get("zip"),
        updated_by="web_user"
    )
    if not result["success"]:
        return jsonify(result), 400
    return jsonify(result)
