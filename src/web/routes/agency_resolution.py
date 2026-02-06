# src/web/routes/agency_resolution.py
"""API routes for agency resolution."""

from flask import Blueprint, current_app, jsonify, request, render_template

agency_resolution_bp = Blueprint("agency_resolution", __name__)


def _get_service():
    from src.services.agency_resolution_service import AgencyResolutionService
    db_path = current_app.config.get("DB_PATH") or "./data/database/production.db"
    return AgencyResolutionService(db_path)


@agency_resolution_bp.route("/agency-resolution")
def page():
    return render_template("agency_resolution.html")


@agency_resolution_bp.route("/api/agency-resolution/stats")
def get_stats():
    return jsonify(_get_service().get_stats())


@agency_resolution_bp.route("/api/agency-resolution/unresolved")
def get_unresolved():
    min_revenue = float(request.args.get("min_revenue", 0))
    limit = int(request.args.get("limit", 100))
    items = _get_service().get_unresolved(min_revenue=min_revenue, limit=limit)
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


@agency_resolution_bp.route("/api/agency-resolution/create", methods=["POST"])
def create_agency():
    data = request.json
    agency_raw = data.get("agency_raw")
    agency_name = data.get("agency_name")

    if not agency_raw or not agency_name:
        return jsonify({"success": False, "error": "agency_raw and agency_name required"}), 400

    result = _get_service().create_agency_and_alias(
        agency_raw=agency_raw,
        agency_name=agency_name,
        created_by="web_user"
    )
    return jsonify(result)


@agency_resolution_bp.route("/api/agency-resolution/link", methods=["POST"])
def link_agency():
    data = request.json
    agency_raw = data.get("agency_raw")
    agency_id = data.get("agency_id")

    if not agency_raw or not agency_id:
        return jsonify({"success": False, "error": "agency_raw and agency_id required"}), 400

    result = _get_service().link_to_existing(
        agency_raw=agency_raw,
        agency_id=agency_id,
        created_by="web_user"
    )

    if not result["success"]:
        return jsonify(result), 400
    return jsonify(result)


@agency_resolution_bp.route("/api/agency-resolution/search")
def search_agencies():
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify([])
    return jsonify(_get_service().search_agencies(q))


@agency_resolution_bp.route("/api/agency-aliases/merge", methods=["POST"])
def merge_agencies():
    """Merge source agency into target agency."""
    data = request.json
    source_id = data.get("source_id")
    target_id = data.get("target_id")

    if not source_id or not target_id:
        return jsonify({"success": False, "error": "source_id and target_id required"}), 400

    result = _get_service().merge_agencies(
        source_id=int(source_id),
        target_id=int(target_id),
        merged_by="web_user"
    )

    if not result["success"]:
        return jsonify(result), 400
    return jsonify(result)


@agency_resolution_bp.route("/agency-aliases")
def aliases_page():
    """Agency aliases management page."""
    return render_template("agency_aliases.html")


@agency_resolution_bp.route("/api/agency-aliases")
def get_agencies_with_aliases():
    """List agencies with alias counts."""
    search = request.args.get("search", "").strip()
    min_aliases = int(request.args.get("min_aliases", 0))
    limit = int(request.args.get("limit", 200))

    items = _get_service().get_agencies_with_aliases(
        search=search,
        min_aliases=min_aliases,
        limit=limit
    )
    return jsonify(items)


@agency_resolution_bp.route("/api/agency-aliases/<int:agency_id>")
def get_agency_detail(agency_id: int):
    """Get single agency with all aliases."""
    result = _get_service().get_agency_aliases(agency_id)
    if not result:
        return jsonify({"error": "Agency not found"}), 404
    return jsonify(result)


@agency_resolution_bp.route("/api/agency-aliases/<int:alias_id>", methods=["DELETE"])
def delete_alias(alias_id: int):
    """Soft-delete an alias."""
    result = _get_service().delete_alias(alias_id, deleted_by="web_user")
    if not result["success"]:
        return jsonify(result), 400
    return jsonify(result)


@agency_resolution_bp.route("/api/agency-aliases/<int:agency_id>/rename", methods=["POST"])
def rename_agency(agency_id: int):
    """Rename an agency's name."""
    data = request.json
    new_name = data.get("new_name", "").strip()

    if not new_name:
        return jsonify({"success": False, "error": "new_name required"}), 400

    result = _get_service().rename_agency(
        agency_id=agency_id,
        new_name=new_name,
        renamed_by="web_user"
    )

    if not result["success"]:
        return jsonify(result), 400
    return jsonify(result)


@agency_resolution_bp.route("/api/agency-aliases/<int:agency_id>/address", methods=["PUT"])
def update_agency_address(agency_id: int):
    """Update agency address."""
    data = request.json

    result = _get_service().update_agency_address(
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
