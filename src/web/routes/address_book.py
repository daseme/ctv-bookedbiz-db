"""Unified Address Book routes -- thin HTTP layer.

All business logic lives in services. Route handlers parse HTTP
requests, call the appropriate service, and format responses.
"""

from flask import Blueprint, render_template, jsonify, request, Response
from flask_login import current_user

from src.services.container import get_container

address_book_bp = Blueprint("address_book", __name__)


@address_book_bp.before_request
def _require_admin_for_writes():
    if request.method in ("POST", "PUT", "DELETE"):
        if (not hasattr(current_user, "role")
                or current_user.role.value != "admin"):
            return jsonify({"error": "Admin access required"}), 403


def _db():
    return get_container().get("database_connection")


def _svc(name):
    return get_container().get(name)


# ------------------------------------------------------------------
# Page routes
# ------------------------------------------------------------------

@address_book_bp.route("/address-book")
def address_book_page():
    """Render the unified address book page."""
    return render_template("address_book.html")


@address_book_bp.route("/address-book/guide")
def address_book_guide():
    """Render the address book feature guide."""
    return render_template("address_book_guide.html")


# ------------------------------------------------------------------
# Reference data
# ------------------------------------------------------------------

@address_book_bp.route("/api/address-book/sectors")
def api_sectors():
    """Get list of all sectors for dropdown."""
    entity_svc = _svc("entity_service")
    with _db().connection_ro() as conn:
        return jsonify(entity_svc.get_sectors(conn))


@address_book_bp.route("/api/address-book/markets")
def api_markets():
    """Get distinct markets from spots data."""
    entity_svc = _svc("entity_service")
    with _db().connection_ro() as conn:
        return jsonify(entity_svc.get_markets(conn))


@address_book_bp.route("/api/address-book/ae-list")
def api_ae_list():
    """Get sorted list of AE names."""
    entity_svc = _svc("entity_service")
    with _db().connection_ro() as conn:
        return jsonify(entity_svc.get_ae_list(conn))


# ------------------------------------------------------------------
# Entity list and detail
# ------------------------------------------------------------------

@address_book_bp.route("/api/address-book")
def api_address_book():
    """Get all entities with contacts, sectors, markets, and metrics."""
    include_inactive = request.args.get("include_inactive", "0") == "1"
    entity_svc = _svc("entity_service")
    metrics_svc = _svc("entity_metrics_service")

    with _db().connection() as rw_conn:
        metrics_svc.auto_refresh_if_empty(rw_conn)
        rw_conn.commit()

    with _db().connection_ro() as conn:
        return jsonify(
            entity_svc.list_entities(conn, include_inactive)
        )


@address_book_bp.route(
    "/api/address-book/<entity_type>/<int:entity_id>"
)
def api_entity_detail(entity_type, entity_id):
    """Get full details for a single entity."""
    if entity_type not in ("agency", "customer"):
        return jsonify({"error": "Invalid entity type"}), 400

    entity_svc = _svc("entity_service")
    with _db().connection_ro() as conn:
        result = entity_svc.get_entity_detail(
            conn, entity_type, entity_id,
        )
        if "error" in result:
            status = result.pop("status", 404)
            return jsonify(result), status
        return jsonify(result)


# ------------------------------------------------------------------
# Entity mutations
# ------------------------------------------------------------------

@address_book_bp.route("/api/address-book/entities", methods=["POST"])
def api_create_entity():
    """Create a new agency or advertiser."""
    data = request.get_json() or {}
    entity_svc = _svc("entity_service")
    with _db().connection() as conn:
        try:
            result = entity_svc.create_entity(
                conn, data, current_user.full_name,
            )
            if "error" in result:
                status = result.pop("status", 409)
                conn.rollback()
                return jsonify(result), status
            if result.get("needs_confirmation"):
                return jsonify(result), 200
            conn.commit()
            return jsonify(result), 201
        except Exception as e:
            conn.rollback()
            return jsonify({"error": str(e)}), 500


@address_book_bp.route(
    "/api/address-book/<entity_type>/<int:entity_id>/deactivate",
    methods=["POST"],
)
def api_deactivate_entity(entity_type, entity_id):
    """Soft-deactivate an entity."""
    if entity_type not in ("agency", "customer"):
        return jsonify({"error": "Invalid entity type"}), 400

    entity_svc = _svc("entity_service")
    with _db().connection() as conn:
        try:
            result = entity_svc.deactivate_entity(
                conn, entity_type, entity_id, current_user.full_name,
            )
            if "error" in result:
                status = result.pop("status", 400)
                return jsonify(result), status
            conn.commit()
            return jsonify(result)
        except Exception as e:
            conn.rollback()
            return jsonify({"error": str(e)}), 500


@address_book_bp.route(
    "/api/address-book/<entity_type>/<int:entity_id>/reactivate",
    methods=["POST"],
)
def api_reactivate_entity(entity_type, entity_id):
    """Reactivate a deactivated entity."""
    if entity_type not in ("agency", "customer"):
        return jsonify({"error": "Invalid entity type"}), 400

    entity_svc = _svc("entity_service")
    with _db().connection() as conn:
        try:
            result = entity_svc.reactivate_entity(
                conn, entity_type, entity_id, current_user.full_name,
            )
            if "error" in result:
                status = result.pop("status", 400)
                return jsonify(result), status
            conn.commit()
            return jsonify(result)
        except Exception as e:
            conn.rollback()
            return jsonify({"error": str(e)}), 500


# ------------------------------------------------------------------
# Entity field updates
# ------------------------------------------------------------------

@address_book_bp.route(
    "/api/address-book/<entity_type>/<int:entity_id>/address",
    methods=["PUT"],
)
def api_update_address(entity_type, entity_id):
    """Update primary address for an entity."""
    if entity_type not in ("agency", "customer"):
        return jsonify({"error": "Invalid entity type"}), 400

    data = request.get_json() or {}
    entity_svc = _svc("entity_service")
    with _db().connection() as conn:
        try:
            result = entity_svc.update_address(
                conn, entity_type, entity_id,
                data.get("address"), data.get("city"),
                data.get("state"), data.get("zip"),
            )
            conn.commit()
            return jsonify(result)
        except Exception as e:
            conn.rollback()
            return jsonify({"success": False, "error": str(e)}), 500


@address_book_bp.route(
    "/api/address-book/<entity_type>/<int:entity_id>/notes",
    methods=["PUT"],
)
def api_update_notes(entity_type, entity_id):
    """Update notes for an entity."""
    if entity_type not in ("agency", "customer"):
        return jsonify({"error": "Invalid entity type"}), 400

    data = request.get_json() or {}
    entity_svc = _svc("entity_service")
    with _db().connection() as conn:
        try:
            result = entity_svc.update_notes(
                conn, entity_type, entity_id, data.get("notes"),
            )
            conn.commit()
            return jsonify(result)
        except Exception as e:
            conn.rollback()
            return jsonify({"success": False, "error": str(e)}), 500


@address_book_bp.route(
    "/api/address-book/<entity_type>/<int:entity_id>/billing-info",
    methods=["PUT"],
)
def api_update_billing_info(entity_type, entity_id):
    """Update billing info for an entity."""
    if entity_type not in ("agency", "customer"):
        return jsonify({"error": "Invalid entity type"}), 400

    data = request.get_json() or {}
    entity_svc = _svc("entity_service")
    with _db().connection() as conn:
        try:
            result = entity_svc.update_billing_info(
                conn, entity_type, entity_id, data,
            )
            if "error" in result:
                status = result.pop("status", 400)
                return jsonify(result), status
            conn.commit()
            return jsonify(result)
        except Exception as e:
            conn.rollback()
            return jsonify({"success": False, "error": str(e)}), 500


@address_book_bp.route(
    "/api/address-book/<entity_type>/<int:entity_id>/sector",
    methods=["PUT"],
)
def api_update_sector(entity_type, entity_id):
    """Update primary sector for a customer."""
    if entity_type != "customer":
        return jsonify({"error": "Only customers have sectors"}), 400

    data = request.get_json() or {}
    sector_id = data.get("sector_id")
    if sector_id is not None:
        try:
            sector_id = int(sector_id) if sector_id else None
        except (ValueError, TypeError):
            return jsonify({"error": "Invalid sector_id"}), 400

    entity_svc = _svc("entity_service")
    with _db().connection() as conn:
        try:
            result = entity_svc.update_sector(
                conn, entity_id, sector_id, current_user.full_name,
            )
            conn.commit()
            return jsonify(result)
        except Exception as e:
            conn.rollback()
            return jsonify({"success": False, "error": str(e)}), 500


@address_book_bp.route(
    "/api/address-book/customer/<int:entity_id>/sectors",
    methods=["PUT"],
)
def api_update_sectors(entity_id):
    """Replace all sector assignments for a customer."""
    data = request.get_json() or {}
    sectors = data.get("sectors", [])

    if not isinstance(sectors, list):
        return jsonify({"error": "sectors must be an array"}), 400

    primary_count = sum(1 for s in sectors if s.get("is_primary"))
    if len(sectors) > 0 and primary_count != 1:
        return jsonify({
            "error": "Exactly one sector must be marked as primary",
        }), 400

    entity_svc = _svc("entity_service")
    with _db().connection() as conn:
        try:
            result = entity_svc.update_sectors(
                conn, entity_id, sectors, current_user.full_name,
            )
            if "error" in result:
                status = result.pop("status", 400)
                return jsonify(result), status
            conn.commit()
            return jsonify(result)
        except Exception as e:
            conn.rollback()
            return jsonify({"success": False, "error": str(e)}), 500


@address_book_bp.route(
    "/api/address-book/customer/<int:entity_id>/agency",
    methods=["PUT"],
)
def api_update_agency(entity_id):
    """Update agency assignment for a customer."""
    data = request.get_json() or {}
    agency_id = data.get("agency_id")
    if agency_id is not None:
        try:
            agency_id = int(agency_id) if agency_id else None
        except (ValueError, TypeError):
            return jsonify({"error": "Invalid agency_id"}), 400

    entity_svc = _svc("entity_service")
    with _db().connection() as conn:
        try:
            result = entity_svc.update_agency(
                conn, entity_id, agency_id,
            )
            if "error" in result:
                status = result.pop("status", 400)
                return jsonify(result), status
            conn.commit()
            return jsonify(result)
        except Exception as e:
            conn.rollback()
            return jsonify({"success": False, "error": str(e)}), 500


# ------------------------------------------------------------------
# Agency sub-resources
# ------------------------------------------------------------------

@address_book_bp.route(
    "/api/address-book/agency/<int:agency_id>/customers"
)
def api_agency_customers(agency_id):
    """Get customers associated with an agency."""
    entity_svc = _svc("entity_service")
    with _db().connection_ro() as conn:
        result = entity_svc.get_agency_customers(conn, agency_id)
        if "error" in result:
            status = result.pop("status", 404)
            return jsonify(result), status
        return jsonify(result)


@address_book_bp.route(
    "/api/address-book/agency/<int:agency_id>/duplicates"
)
def api_agency_duplicates(agency_id):
    """Find potential duplicate clients within an agency."""
    entity_svc = _svc("entity_service")
    with _db().connection_ro() as conn:
        result = entity_svc.get_agency_duplicates(conn, agency_id)
        if "error" in result:
            status = result.pop("status", 404)
            return jsonify(result), status
        return jsonify(result)


# ------------------------------------------------------------------
# Spots link
# ------------------------------------------------------------------

@address_book_bp.route(
    "/api/address-book/<entity_type>/<int:entity_id>/spots-link"
)
def api_spots_link(entity_type, entity_id):
    """Return URL to filtered spots view."""
    if entity_type not in ("agency", "customer"):
        return jsonify({"error": "Invalid entity type"}), 400
    entity_svc = _svc("entity_service")
    return jsonify(entity_svc.get_spots_link(entity_type, entity_id))


# ------------------------------------------------------------------
# Metrics refresh
# ------------------------------------------------------------------

@address_book_bp.route(
    "/api/address-book/refresh-metrics", methods=["POST"],
)
def api_refresh_metrics():
    """Refresh entity_metrics for specific entity IDs."""
    data = request.json or {}
    customer_ids = data.get("customer_ids", [])
    agency_ids = data.get("agency_ids", [])

    if not customer_ids and not agency_ids:
        return jsonify({"error": "No IDs provided"}), 400

    metrics_svc = _svc("entity_metrics_service")
    with _db().connection() as conn:
        try:
            metrics_svc.refresh_metrics_for_ids(
                conn,
                customer_ids=[int(c) for c in customer_ids],
                agency_ids=[int(a) for a in agency_ids],
            )
            conn.commit()
            return jsonify({
                "success": True,
                "refreshed": len(customer_ids) + len(agency_ids),
            })
        except Exception as e:
            conn.rollback()
            return jsonify({"error": str(e)}), 500


# ------------------------------------------------------------------
# AE assignment
# ------------------------------------------------------------------

@address_book_bp.route(
    "/api/address-book/<entity_type>/<int:entity_id>/ae",
    methods=["PUT"],
)
def api_update_ae(entity_type, entity_id):
    """Update assigned AE for an entity."""
    if entity_type not in ("agency", "customer"):
        return jsonify({"error": "Invalid entity type"}), 400

    data = request.get_json() or {}
    assigned_ae = (data.get("assigned_ae") or "").strip() or None

    entity_svc = _svc("entity_service")
    with _db().connection() as conn:
        try:
            result = entity_svc.update_ae(
                conn, entity_type, entity_id,
                assigned_ae, current_user.full_name,
            )
            if "error" in result:
                status = result.pop("status", 404)
                return jsonify(result), status
            conn.commit()
            return jsonify(result)
        except Exception as e:
            conn.rollback()
            return jsonify({"success": False, "error": str(e)}), 500


@address_book_bp.route(
    "/api/address-book/<entity_type>/<int:entity_id>/ae-history"
)
def api_ae_history(entity_type, entity_id):
    """Get AE assignment history."""
    if entity_type not in ("agency", "customer"):
        return jsonify({"error": "Invalid entity type"}), 400

    entity_svc = _svc("entity_service")
    with _db().connection_ro() as conn:
        return jsonify(
            entity_svc.get_ae_history(conn, entity_type, entity_id)
        )


# ------------------------------------------------------------------
# Additional addresses
# ------------------------------------------------------------------

@address_book_bp.route(
    "/api/address-book/<entity_type>/<int:entity_id>/addresses"
)
def api_get_addresses(entity_type, entity_id):
    """Get active additional addresses for an entity."""
    if entity_type not in ("agency", "customer"):
        return jsonify({"error": "Invalid entity type"}), 400

    addr_svc = _svc("address_service")
    with _db().connection_ro() as conn:
        return jsonify(
            addr_svc.get_addresses(conn, entity_type, entity_id)
        )


@address_book_bp.route(
    "/api/address-book/<entity_type>/<int:entity_id>/addresses",
    methods=["POST"],
)
def api_create_address(entity_type, entity_id):
    """Create an additional address."""
    if entity_type not in ("agency", "customer"):
        return jsonify({"error": "Invalid entity type"}), 400

    data = request.get_json() or {}
    addr_svc = _svc("address_service")
    with _db().connection() as conn:
        try:
            result = addr_svc.create_address(
                conn, entity_type, entity_id,
                data, current_user.full_name,
            )
            if "error" in result:
                return jsonify(result), 400
            conn.commit()
            return jsonify(result), 201
        except Exception as e:
            conn.rollback()
            return jsonify({"success": False, "error": str(e)}), 500


@address_book_bp.route(
    "/api/address-book/addresses/<int:address_id>", methods=["PUT"],
)
def api_update_address_entry(address_id):
    """Update an additional address."""
    data = request.get_json() or {}
    addr_svc = _svc("address_service")
    with _db().connection() as conn:
        try:
            result = addr_svc.update_address(conn, address_id, data)
            if "error" in result:
                status = result.pop("status", 400)
                return jsonify(result), status
            conn.commit()
            return jsonify(result)
        except Exception as e:
            conn.rollback()
            return jsonify({"success": False, "error": str(e)}), 500


@address_book_bp.route(
    "/api/address-book/addresses/<int:address_id>",
    methods=["DELETE"],
)
def api_delete_address(address_id):
    """Soft-delete an additional address."""
    addr_svc = _svc("address_service")
    with _db().connection() as conn:
        try:
            result = addr_svc.delete_address(conn, address_id)
            conn.commit()
            return jsonify(result)
        except Exception as e:
            conn.rollback()
            return jsonify({"success": False, "error": str(e)}), 500


# ------------------------------------------------------------------
# Saved filters
# ------------------------------------------------------------------

@address_book_bp.route("/api/address-book/filters")
def api_get_filters():
    """Get saved filter presets."""
    filter_svc = _svc("saved_filter_service")
    with _db().connection_ro() as conn:
        return jsonify(filter_svc.get_filters(conn))


@address_book_bp.route(
    "/api/address-book/filters", methods=["POST"],
)
def api_save_filter():
    """Save a filter preset."""
    data = request.get_json() or {}
    filter_svc = _svc("saved_filter_service")
    with _db().connection() as conn:
        try:
            result = filter_svc.save_filter(
                conn,
                (data.get("filter_name") or "").strip(),
                data.get("filter_config", {}),
                current_user.full_name,
                is_shared=data.get("is_shared", False),
            )
            if "error" in result:
                return jsonify(result), 400
            conn.commit()
            return jsonify(result)
        except Exception as e:
            conn.rollback()
            return jsonify({"success": False, "error": str(e)}), 500


@address_book_bp.route(
    "/api/address-book/filters/<int:filter_id>", methods=["DELETE"],
)
def api_delete_filter(filter_id):
    """Delete a saved filter."""
    filter_svc = _svc("saved_filter_service")
    with _db().connection() as conn:
        try:
            result = filter_svc.delete_filter(conn, filter_id)
            conn.commit()
            return jsonify(result)
        except Exception as e:
            conn.rollback()
            return jsonify({"success": False, "error": str(e)}), 500


# ------------------------------------------------------------------
# CSV export / import
# ------------------------------------------------------------------

@address_book_bp.route("/api/address-book/export")
def api_export_csv():
    """Export filtered address book as CSV."""
    filters = {
        "search": request.args.get("search", ""),
        "type": request.args.get("type", "all"),
        "has_contacts": request.args.get("has_contacts", "all"),
        "has_address": request.args.get("has_address", "all"),
        "sector_id": request.args.get("sector_id", ""),
        "market": request.args.get("market", ""),
        "ae": request.args.get("ae", ""),
    }

    metrics_svc = _svc("entity_metrics_service")
    export_svc = _svc("export_service")

    with _db().connection() as rw_conn:
        metrics_svc.auto_refresh_if_empty(rw_conn)
        rw_conn.commit()

    with _db().connection_ro() as conn:
        metrics_map = metrics_svc.get_metrics_map(conn)
        csv_content = export_svc.export_entities_csv(
            conn, filters, metrics_map,
        )

    return Response(
        csv_content,
        mimetype="text/csv",
        headers={
            "Content-Disposition":
                "attachment; filename=address_book_export.csv",
        },
    )


@address_book_bp.route(
    "/api/address-book/import-contacts", methods=["POST"],
)
def api_import_contacts():
    """Import contacts from a CSV file."""
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    file = request.files["file"]
    if not file.filename or not file.filename.endswith(".csv"):
        return jsonify({"error": "File must be a .csv"}), 400

    try:
        content = file.read().decode("utf-8-sig")
    except UnicodeDecodeError:
        return jsonify({"error": "File must be UTF-8 encoded"}), 400

    export_svc = _svc("export_service")
    with _db().connection() as conn:
        result = export_svc.import_contacts_csv(
            conn, content, current_user.full_name,
        )
        if "error" in result:
            return jsonify(result), 400
        conn.commit()
        return jsonify(result)


# ------------------------------------------------------------------
# Activity log
# ------------------------------------------------------------------

@address_book_bp.route(
    "/api/address-book/<entity_type>/<int:entity_id>/activities"
)
def api_get_activities(entity_type, entity_id):
    """Get activity log for an entity."""
    if entity_type not in ("agency", "customer"):
        return jsonify({"error": "Invalid entity type"}), 400

    limit = request.args.get("limit", 50, type=int)
    activity_svc = _svc("activity_service")
    with _db().connection_ro() as conn:
        return jsonify(
            activity_svc.get_activities(
                conn, entity_type, entity_id, limit,
            )
        )


@address_book_bp.route(
    "/api/address-book/<entity_type>/<int:entity_id>/activities",
    methods=["POST"],
)
def api_create_activity(entity_type, entity_id):
    """Create a new activity log entry."""
    if entity_type not in ("agency", "customer"):
        return jsonify({"error": "Invalid entity type"}), 400

    data = request.get_json() or {}
    activity_svc = _svc("activity_service")
    with _db().connection() as conn:
        try:
            result = activity_svc.create_activity(
                conn, entity_type, entity_id,
                data.get("activity_type", "").strip(),
                data.get("description", "").strip(),
                current_user.full_name,
                contact_id=data.get("contact_id"),
                due_date=data.get("due_date", "").strip() or None,
            )
            if "error" in result:
                status = result.pop("status", 400)
                return jsonify(result), status
            conn.commit()
            return jsonify(result), 201
        except Exception as e:
            conn.rollback()
            return jsonify({"success": False, "error": str(e)}), 500


@address_book_bp.route(
    "/api/address-book/activities/<int:activity_id>/complete",
    methods=["POST"],
)
def api_complete_activity(activity_id):
    """Toggle completion of a follow-up activity."""
    activity_svc = _svc("activity_service")
    with _db().connection() as conn:
        try:
            result = activity_svc.toggle_completion(
                conn, activity_id,
            )
            if "error" in result:
                status = result.pop("status", 400)
                return jsonify(result), status
            conn.commit()
            return jsonify(result)
        except Exception as e:
            conn.rollback()
            return jsonify({"error": str(e)}), 500


@address_book_bp.route("/api/address-book/follow-ups")
def api_get_follow_ups():
    """Get incomplete and recently completed follow-ups."""
    ae_name = request.args.get("ae")
    activity_svc = _svc("activity_service")
    with _db().connection_ro() as conn:
        return jsonify(activity_svc.get_follow_ups(conn, ae_name=ae_name))
