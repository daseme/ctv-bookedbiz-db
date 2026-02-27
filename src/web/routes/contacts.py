# src/web/routes/contacts.py
"""API routes for contact management."""

from flask import Blueprint, current_app, jsonify, request

contacts_bp = Blueprint("contacts", __name__)


@contacts_bp.before_request
def _require_admin_for_writes():
    if request.method in ('POST', 'PUT', 'DELETE'):
        from flask_login import current_user
        if not hasattr(current_user, 'role') or current_user.role.value != 'admin':
            return jsonify({"error": "Admin access required"}), 403


def _get_service():
    from src.services.contact_service import ContactService
    db_path = current_app.config.get("DB_PATH") or "./.data/dev.db"
    return ContactService(db_path)


@contacts_bp.route("/api/contacts/<entity_type>/<int:entity_id>", methods=["GET"])
def get_contacts(entity_type: str, entity_id: int):
    """Get all contacts for an entity."""
    if entity_type not in ('customer', 'agency'):
        return jsonify({"success": False, "error": "Invalid entity_type"}), 400

    include_inactive = request.args.get("include_inactive", "").lower() == "true"
    contacts = _get_service().get_contacts(
        entity_type=entity_type,
        entity_id=entity_id,
        include_inactive=include_inactive
    )
    return jsonify(contacts)


@contacts_bp.route("/api/contacts/<entity_type>/<int:entity_id>", methods=["POST"])
def create_contact(entity_type: str, entity_id: int):
    """Create a new contact for an entity."""
    if entity_type not in ('customer', 'agency'):
        return jsonify({"success": False, "error": "Invalid entity_type"}), 400

    data = request.json or {}
    contact_name = data.get("contact_name", "").strip()

    if not contact_name:
        return jsonify({"success": False, "error": "contact_name is required"}), 400

    result = _get_service().create_contact(
        entity_type=entity_type,
        entity_id=entity_id,
        contact_name=contact_name,
        contact_title=data.get("contact_title"),
        email=data.get("email"),
        phone=data.get("phone"),
        contact_role=data.get("contact_role"),
        is_primary=data.get("is_primary", False),
        notes=data.get("notes"),
        created_by="web_user"
    )

    if not result["success"]:
        return jsonify(result), 400
    return jsonify(result), 201


@contacts_bp.route("/api/contacts/<int:contact_id>", methods=["GET"])
def get_contact(contact_id: int):
    """Get a single contact by ID."""
    contact = _get_service().get_contact(contact_id)
    if not contact:
        return jsonify({"error": "Contact not found"}), 404
    return jsonify(contact)


@contacts_bp.route("/api/contacts/<int:contact_id>", methods=["PUT"])
def update_contact(contact_id: int):
    """Update an existing contact."""
    data = request.json or {}

    result = _get_service().update_contact(
        contact_id=contact_id,
        contact_name=data.get("contact_name"),
        contact_title=data.get("contact_title"),
        email=data.get("email"),
        phone=data.get("phone"),
        contact_role=data.get("contact_role"),
        notes=data.get("notes"),
        updated_by="web_user"
    )

    if not result["success"]:
        return jsonify(result), 400
    return jsonify(result)


@contacts_bp.route("/api/contacts/<int:contact_id>", methods=["DELETE"])
def delete_contact(contact_id: int):
    """Soft-delete a contact."""
    result = _get_service().delete_contact(
        contact_id=contact_id,
        deleted_by="web_user"
    )

    if not result["success"]:
        return jsonify(result), 400
    return jsonify(result)


@contacts_bp.route("/api/contacts/<int:contact_id>/primary", methods=["POST"])
def set_primary_contact(contact_id: int):
    """Set a contact as the primary contact for its entity."""
    result = _get_service().set_primary(
        contact_id=contact_id,
        updated_by="web_user"
    )

    if not result["success"]:
        return jsonify(result), 400
    return jsonify(result)


@contacts_bp.route("/api/contacts/<entity_type>/<int:entity_id>/primary", methods=["GET"])
def get_primary_contact(entity_type: str, entity_id: int):
    """Get the primary contact for an entity."""
    if entity_type not in ('customer', 'agency'):
        return jsonify({"success": False, "error": "Invalid entity_type"}), 400

    contact = _get_service().get_primary_contact(
        entity_type=entity_type,
        entity_id=entity_id
    )

    if not contact:
        return jsonify({"error": "No primary contact found"}), 404
    return jsonify(contact)


@contacts_bp.route("/api/contacts/<int:contact_id>/touched", methods=["POST"])
def mark_contacted(contact_id: int):
    """Mark a contact as recently contacted (updates last_contacted timestamp)."""
    data = request.json or {}
    contacted_date = data.get("contacted_date")  # Optional: ISO format date

    result = _get_service().update_last_contacted(
        contact_id=contact_id,
        contacted_date=contacted_date
    )

    if not result["success"]:
        return jsonify(result), 400
    return jsonify(result)
