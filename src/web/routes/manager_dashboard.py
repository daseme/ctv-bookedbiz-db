"""Manager Dashboard -- cross-AE performance overview."""

import logging
from flask import Blueprint, render_template, jsonify

from src.models.users import UserRole
from src.services.container import get_container
from src.web.utils.auth import role_required

logger = logging.getLogger(__name__)

manager_bp = Blueprint("manager", __name__)


def _db():
    return get_container().get("database_connection")


def _svc(name):
    return get_container().get(name)


def _ae_names(conn):
    """Get list of active AE names from the database."""
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
    return [r["assigned_ae"] for r in rows]


@manager_bp.route("/manager/dashboard")
@role_required(UserRole.MANAGEMENT)
def manager_dashboard():
    """Render the Manager Dashboard page."""
    with _db().connection_ro() as conn:
        ae_list = _ae_names(conn)
    return render_template(
        "manager_dashboard.html",
        title="Manager Dashboard",
        ae_list=ae_list,
    )


@manager_bp.route("/api/manager/scoreboard")
@role_required(UserRole.MANAGEMENT)
def api_scoreboard():
    """Return per-AE scoreboard stats."""
    svc = _svc("manager_dashboard_service")
    with _db().connection_ro() as conn:
        ae_list = _ae_names(conn)
        return jsonify(svc.get_scoreboard(conn, ae_list))


@manager_bp.route("/api/manager/attention")
@role_required(UserRole.MANAGEMENT)
def api_attention():
    """Return attention-required items."""
    svc = _svc("manager_dashboard_service")
    with _db().connection_ro() as conn:
        return jsonify(svc.get_attention_items(conn))


@manager_bp.route("/api/manager/weekly-activity")
@role_required(UserRole.MANAGEMENT)
def api_weekly_activity():
    """Return weekly activity counts per AE."""
    svc = _svc("manager_dashboard_service")
    with _db().connection_ro() as conn:
        ae_list = _ae_names(conn)
        return jsonify(svc.get_weekly_activity(conn, ae_list))


@manager_bp.route("/api/manager/health-summary")
@role_required(UserRole.MANAGEMENT)
def api_health_summary():
    """Return async health/touch summary per AE.

    Slow endpoint -- calls health_score_service which queries spots table.
    Load this async on the client side.
    """
    health_svc = _svc("health_score_service")
    with _db().connection_ro() as conn:
        ae_list = _ae_names(conn)
        result = {}
        a_tier_overdue = []

        for ae in ae_list:
            scores = health_svc.get_health_with_tiers(conn, ae_name=ae)
            if not scores:
                result[ae] = {
                    "avg_health": 0,
                    "touch_compliance": 100,
                }
                continue

            avg_health = round(
                sum(s["health_score"] for s in scores) / len(scores)
            )
            compliance = health_svc.touch_compliance(scores)
            result[ae] = {
                "avg_health": avg_health,
                "touch_compliance": compliance,
            }

            for s in scores:
                if (s.get("tier") == "A"
                        and s.get("touch_status") == "red"):
                    entity_name = _resolve_name(
                        conn, s["entity_type"], s["entity_id"]
                    )
                    a_tier_overdue.append({
                        "item_type": "a_tier_overdue",
                        "entity_type": s["entity_type"],
                        "entity_id": s["entity_id"],
                        "entity_name": entity_name,
                        "days_since_touch": s.get("days_since_touch"),
                        "tier": "A",
                        "assigned_ae": ae,
                        "health_score": s["health_score"],
                    })

        return jsonify({
            "ae_health": result,
            "a_tier_overdue": a_tier_overdue,
        })


def _resolve_name(conn, entity_type, entity_id):
    """Look up entity name by type and ID."""
    if entity_type == "agency":
        row = conn.execute(
            "SELECT agency_name FROM agencies WHERE agency_id = ?",
            [entity_id],
        ).fetchone()
        return row["agency_name"] if row else "Unknown"
    row = conn.execute(
        "SELECT normalized_name FROM customers WHERE customer_id = ?",
        [entity_id],
    ).fetchone()
    return row["normalized_name"] if row else "Unknown"
