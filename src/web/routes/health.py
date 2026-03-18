"""Health monitoring API endpoints."""

import logging
import os
from datetime import datetime, timezone

from flask import Blueprint, jsonify

from src.services.container import get_container
from src.services.factory import (
    get_service_health_report,
    emergency_service_recovery,
)

logger = logging.getLogger(__name__)

health_bp = Blueprint("health", __name__, url_prefix="/health")


@health_bp.route("/")
def system_health():
    """Return health status of all registered services."""
    report = get_service_health_report()
    report["timestamp"] = datetime.now(timezone.utc).isoformat()

    status_code = 200
    if report["overall_status"] == "degraded":
        status_code = 206
    elif report["overall_status"] in ("unhealthy", "error"):
        status_code = 503

    return jsonify(report), status_code


@health_bp.route("/database")
def database_health():
    """Test database connectivity."""
    container = get_container()
    info = {"timestamp": datetime.now(timezone.utc).isoformat()}

    if not container.has_service("database_connection"):
        info.update({"status": "unavailable"})
        return jsonify(info), 503

    try:
        db = container.get("database_connection")
        with db.connection_ro() as conn:
            conn.execute("SELECT 1").fetchone()
        info.update({"status": "healthy"})
        return jsonify(info), 200
    except Exception as e:
        info.update({"status": "critical", "error": str(e)})
        return jsonify(info), 503


@health_bp.route("/metrics")
def system_metrics():
    """Return system resource metrics."""
    try:
        import psutil
    except ImportError:
        return jsonify({"error": "psutil not installed"}), 501

    container = get_container()
    return jsonify({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "system": {
            "cpu_percent": psutil.cpu_percent(interval=1),
            "memory_percent": psutil.virtual_memory().percent,
            "disk_usage_percent": psutil.disk_usage("/").percent,
            "process_id": os.getpid(),
        },
        "services": {
            "count": len(container.list_services()),
            "names": sorted(container.list_services().keys()),
        },
    })


@health_bp.route("/emergency/repair", methods=["POST"])
def emergency_repair():
    """Clear singletons and re-initialize all services."""
    from flask_login import current_user

    if not hasattr(current_user, "role") or current_user.role.value != "admin":
        return jsonify({"error": "Admin access required"}), 403

    result = emergency_service_recovery()
    status_code = 200 if result["success"] else 206
    return jsonify(result), status_code
