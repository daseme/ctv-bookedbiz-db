"""AE My Accounts — CRM-style page for AE book of business."""

import logging
from flask import Blueprint, render_template, jsonify, request
from flask_login import current_user

from src.models.users import UserRole
from src.services.container import get_container
from src.web.utils.auth import role_required

logger = logging.getLogger(__name__)

ae_crm_bp = Blueprint("ae_crm", __name__)


def _db():
    return get_container().get("database_connection")


def _svc(name):
    return get_container().get(name)


def _resolve_ae_name():
    """Determine which AE's accounts to show.

    Admin/Management can select an AE via ?ae= param.
    AE users see their own accounts based on full_name.
    Returns (ae_name, is_admin_view, ae_list).
    """
    is_admin = (
        hasattr(current_user, "role")
        and current_user.role.value in ("admin", "management")
    )
    ae_list = []
    selected_ae = request.args.get("ae", "")

    if is_admin:
        with _db().connection_ro() as conn:
            rows = conn.execute("""
                SELECT DISTINCT assigned_ae
                FROM (
                    SELECT assigned_ae FROM agencies
                    WHERE assigned_ae IS NOT NULL AND is_active = 1
                    UNION
                    SELECT assigned_ae FROM customers
                    WHERE assigned_ae IS NOT NULL AND is_active = 1
                )
                ORDER BY assigned_ae
            """).fetchall()
            ae_list = [r["assigned_ae"] for r in rows]

        ae_name = selected_ae if selected_ae else None
    else:
        ae_name = current_user.full_name

    return ae_name, is_admin, ae_list, selected_ae


@ae_crm_bp.route("/ae/my-accounts")
@role_required(UserRole.AE)
def ae_my_accounts():
    """Render the AE My Accounts page."""
    ae_name, is_admin, ae_list, selected_ae = _resolve_ae_name()
    return render_template(
        "ae_my_accounts.html",
        title="My Accounts",
        ae_name=ae_name or "All AEs",
        is_admin=is_admin,
        ae_list=ae_list,
        selected_ae=selected_ae,
    )


@ae_crm_bp.route("/api/ae/my-accounts")
@role_required(UserRole.AE)
def api_accounts():
    """Return account list JSON for the current AE."""
    ae_name, _, _, _ = _resolve_ae_name()
    crm_svc = _svc("ae_crm_service")
    with _db().connection_ro() as conn:
        return jsonify(crm_svc.get_accounts(conn, ae_name=ae_name))


@ae_crm_bp.route("/api/ae/my-accounts/stats")
@role_required(UserRole.AE)
def api_stats():
    """Return summary stats JSON for the current AE."""
    ae_name, _, _, _ = _resolve_ae_name()
    crm_svc = _svc("ae_crm_service")
    with _db().connection_ro() as conn:
        stats = crm_svc.get_stats(conn, ae_name=ae_name)
        return jsonify(stats)


@ae_crm_bp.route("/api/ae/my-accounts/recent-activity")
@role_required(UserRole.AE)
def api_recent_activity():
    """Return recent activities across all AE's accounts."""
    ae_name, _, _, _ = _resolve_ae_name()
    if not ae_name:
        return jsonify([])
    activity_svc = _svc("activity_service")
    with _db().connection_ro() as conn:
        return jsonify(
            activity_svc.get_recent_activity_for_ae(
                conn, ae_name=ae_name, limit=15
            )
        )


@ae_crm_bp.route("/api/ae/my-accounts/signal-queue")
@role_required(UserRole.AE)
def api_signal_queue():
    """Return signal action queue for the current AE."""
    ae_name, _, _, _ = _resolve_ae_name()
    if not ae_name:
        return jsonify([])
    signal_svc = _svc("signal_action_service")
    with _db().connection() as conn:
        return jsonify(signal_svc.get_queue(conn, ae_name=ae_name))


@ae_crm_bp.route(
    "/api/ae/my-accounts/signal-queue/<int:action_id>/snooze",
    methods=["POST"],
)
@role_required(UserRole.AE)
def api_snooze_signal(action_id):
    """Snooze a signal action."""
    data = request.get_json(silent=True) or {}
    reason = data.get("reason", "")
    snooze_until = data.get("snooze_until", "")
    if not snooze_until:
        return jsonify({"error": "snooze_until date is required"}), 400
    signal_svc = _svc("signal_action_service")
    with _db().connection() as conn:
        result = signal_svc.snooze_action(
            conn, action_id, reason, snooze_until,
            updated_by=current_user.full_name,
        )
        if "error" in result:
            return jsonify(result), result.get("status", 400)
        return jsonify(result)


@ae_crm_bp.route(
    "/api/ae/my-accounts/signal-queue/<int:action_id>/dismiss",
    methods=["POST"],
)
@role_required(UserRole.AE)
def api_dismiss_signal(action_id):
    """Dismiss a signal action."""
    data = request.get_json(silent=True) or {}
    reason = data.get("reason", "")
    signal_svc = _svc("signal_action_service")
    with _db().connection() as conn:
        result = signal_svc.dismiss_action(
            conn, action_id, reason,
            updated_by=current_user.full_name,
        )
        if "error" in result:
            return jsonify(result), result.get("status", 400)
        return jsonify(result)


@ae_crm_bp.route(
    "/api/ae/my-accounts/<entity_type>/<int:entity_id>/revenue-trend"
)
@role_required(UserRole.AE)
def api_revenue_trend(entity_type, entity_id):
    """Return monthly revenue trend for a single entity."""
    if entity_type not in ("agency", "customer"):
        return jsonify({"error": "Invalid entity type"}), 400
    crm_svc = _svc("ae_crm_service")
    with _db().connection_ro() as conn:
        return jsonify(crm_svc.get_revenue_trend(
            conn, entity_type, entity_id
        ))
