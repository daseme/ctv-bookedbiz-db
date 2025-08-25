# src/web/blueprints.py
"""
Blueprint registration & service validation hardened for Railway:
- No import-time crash if critical services fail to instantiate.
- Lazy validation by default on Railway to avoid constructing heavy services.
- Optional SKIP_SERVICE_VALIDATION=1 to bypass hard-fail in production.
- Optional EAGER_SERVICE_VALIDATION=1 to force deep checks anywhere.

Environment knobs:
  SKIP_SERVICE_VALIDATION: "1"/"true" -> never raise on validation failures
  EAGER_SERVICE_VALIDATION: "1"/"true" -> instantiate and check services now
"""
import os
import logging
from typing import Dict, Any

from flask import Flask, request

from web.routes.reports import reports_bp
from web.routes.api import api_bp
from web.routes.budget import budget_bp
from web.routes.health import health_bp
from web.routes.language_blocks import language_blocks_bp
from web.routes.pipeline_decay_api import decay_api_bp
from services.container import get_container
from utils.template_formatters import register_template_filters

logger = logging.getLogger(__name__)


# ---------- helpers ----------

def _truthy_env(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().lower() in {"1", "true", "yes", "y", "on"}

def _is_railway() -> bool:
    # Robust Railway detection: any RAILWAY_* var counts
    if any(k.startswith("RAILWAY_") for k in os.environ.keys()):
        return True
    # Back-compat with ad-hoc flags people sometimes set
    return _truthy_env("RAILWAY", False) or _truthy_env("RAILWAY_ENVIRONMENT", False)

def _resolve_environment() -> str:
    # Prefer container config when available; fall back to envs
    try:
        c = get_container()
        env = c.get_config("ENVIRONMENT", None)
        if env:
            return str(env)
    except Exception:
        pass
    return (
        os.getenv("ENVIRONMENT")
        or os.getenv("APP_ENV")
        or os.getenv("FLASK_ENV")
        or "production"
    ).lower()

def _should_skip_hard_fail() -> bool:
    # Skip hard-fail if explicitly asked, or if we are on Railway.
    if _truthy_env("SKIP_SERVICE_VALIDATION", False):
        return True
    if _is_railway():
        return True
    return False

def _eager_validation_default() -> bool:
    # On Railway default to lazy; elsewhere default to eager.
    if _truthy_env("EAGER_SERVICE_VALIDATION", None) is not None:
        return _truthy_env("EAGER_SERVICE_VALIDATION", False)
    return not _is_railway()


# ---------- blueprints ----------

def register_blueprints(app: Flask) -> None:
    try:
        app.register_blueprint(language_blocks_bp)
        logger.info("Registered language blocks blueprint")

        app.register_blueprint(reports_bp)
        logger.info("Registered reports blueprint")

        app.register_blueprint(api_bp)
        logger.info("Registered API blueprint")

        app.register_blueprint(budget_bp)
        logger.info("Registered budget blueprint")

        app.register_blueprint(health_bp)
        logger.info("Registered health monitoring blueprint")

        app.register_blueprint(decay_api_bp)
        logger.info("Registered pipeline decay API blueprint")

        register_template_filters(app)
        logger.info("Registered template filters")

        logger.info("All blueprints registered successfully")
    except Exception as e:
        logger.error(f"Error registering blueprints: {e}")
        raise


def configure_blueprint_services(app: Flask) -> None:
    """
    Configure & validate services. Never crash app on Railway unless explicitly forced.
    """
    try:
        container = get_container()
        app.config["SERVICE_CONTAINER"] = container

        # Mode flags
        environment = _resolve_environment()
        eager_validation = _eager_validation_default()
        skip_hard_fail = _should_skip_hard_fail()

        app.config["ENVIRONMENT_NAME"] = environment
        app.config["EAGER_SERVICE_VALIDATION"] = eager_validation
        app.config["SKIP_SERVICE_VALIDATION"] = skip_hard_fail

        required_services = [
            "database_connection",
            "report_data_service",
            "pipeline_service",
            "budget_service",
        ]

        service_status: Dict[str, str] = {}

        for name in required_services:
            try:
                if not container.has_service(name):
                    service_status[name] = "not_registered"
                    logger.warning(f"Required service '{name}' not registered")
                    continue

                if eager_validation:
                    # Instantiate & lightly probe
                    svc = container.get(name)
                    if svc is None:
                        service_status[name] = "available_but_null"
                        logger.warning(f"Service '{name}' returned None")
                    else:
                        service_status[name] = "healthy"
                        logger.debug(f"Verified service '{name}' is healthy")

                        # Extra note for pipeline decay feature visibility
                        if name == "pipeline_service":
                            decay_enabled = bool(
                                hasattr(svc, "decay_engine") and getattr(svc, "decay_engine") is not None
                            )
                            service_status["pipeline_service_decay"] = "enabled" if decay_enabled else "disabled"
                            if decay_enabled:
                                logger.info("Pipeline decay system detected and enabled")
                            else:
                                logger.warning("Pipeline decay system not available")
                else:
                    # Lazy: don't instantiate; mark as registered
                    service_status[name] = "registered"
                    if name == "pipeline_service":
                        service_status["pipeline_service_decay"] = "unknown"

            except Exception as e:
                service_status[name] = f"error: {e}"
                logger.error(f"Service '{name}' failed validation: {e}")

        app.config["SERVICE_STATUS"] = service_status

        # Identify hard failures
        critical_failures = [
            n for n, s in service_status.items()
            if s.startswith("error:") or s == "not_registered"
        ]

        if critical_failures:
            msg = f"Critical service failures in {environment}: {critical_failures}"
            if environment == "production" and not skip_hard_fail:
                logger.error(msg)
                # In strict prod, fail fast (only when not skipping)
                raise RuntimeError(f"Critical services failed in production: {critical_failures}")
            else:
                # Degraded but continue
                logger.warning(f"[Degraded Start] {msg} (skip_hard_fail={skip_hard_fail}, eager={eager_validation})")

        logger.info(
            "Blueprint services configured "
            f"(env={environment}, eager={eager_validation}, skip_hard_fail={skip_hard_fail})"
        )

    except Exception as e:
        logger.error(f"Error configuring blueprint services: {e}")
        raise


def register_common_error_handlers(app: Flask) -> None:
    from flask import render_template, jsonify

    @app.errorhandler(404)
    def not_found_error(error):
        logger.warning(f"404: {request.method} {request.path} from {request.remote_addr}")
        if (request.path.startswith("/api/") or
            request.path.startswith("/health/") or
            request.path.startswith("/api/pipeline/decay/")):
            return jsonify({
                "success": False,
                "error": "Not found",
                "status": 404,
                "error_code": "NOT_FOUND",
                "path": request.path,
            }), 404
        return render_template("error_404.html"), 404

    @app.errorhandler(500)
    def internal_error(error):
        logger.error(f"500: {error}", exc_info=True)
        is_decay = request.path.startswith("/api/pipeline/decay/")
        try:
            service_status = app.config.get("SERVICE_STATUS", {})
            unhealthy = [n for n, s in service_status.items() if s not in {"healthy", "registered"}]
            if unhealthy:
                logger.error(f"Error occurred with unhealthy services: {unhealthy}")
                if is_decay and "pipeline_service_decay" in service_status:
                    logger.error(f"Decay system status: {service_status.get('pipeline_service_decay')}")
        except Exception as e:
            logger.error(f"Health introspection failed during 500 handler: {e}")

        if (request.path.startswith("/api/") or
            request.path.startswith("/health/") or
            request.path.startswith("/api/pipeline/decay/")):
            payload = {
                "success": False,
                "error": "Internal server error",
                "status": 500,
                "error_code": "INTERNAL_ERROR",
                "path": request.path,
            }
            if is_decay:
                payload["decay_system"] = "Check /health/pipeline"
            return jsonify(payload), 500

        return render_template("error_500.html"), 500

    @app.errorhandler(400)
    def bad_request_error(error):
        logger.warning(f"400: {request.method} {request.path} - {error}")
        if (request.path.startswith("/api/") or
            request.path.startswith("/health/") or
            request.path.startswith("/api/pipeline/decay/")):
            return jsonify({
                "success": False,
                "error": "Bad request",
                "status": 400,
                "error_code": "BAD_REQUEST",
            }), 400
        return render_template("error_400.html"), 400

    @app.errorhandler(403)
    def forbidden_error(error):
        logger.warning(f"403: {request.method} {request.path} from {request.remote_addr}")
        if (request.path.startswith("/api/") or
            request.path.startswith("/health/") or
            request.path.startswith("/api/pipeline/decay/")):
            return jsonify({
                "success": False,
                "error": "Forbidden",
                "status": 403,
                "error_code": "FORBIDDEN",
            }), 403
        return render_template("error_403.html"), 403

    @app.errorhandler(503)
    def service_unavailable_error(error):
        logger.error(f"503: {request.method} {request.path}")
        if (request.path.startswith("/api/") or
            request.path.startswith("/health/") or
            request.path.startswith("/api/pipeline/decay/")):
            return jsonify({
                "success": False,
                "error": "Service temporarily unavailable",
                "status": 503,
                "error_code": "SERVICE_UNAVAILABLE",
                "message": "System is experiencing issues. Please try again later.",
            }), 503
        return render_template("error_503.html"), 503

    logger.info("Registered common error handlers")


def create_blueprint_context_processors(app: Flask) -> None:
    @app.context_processor
    def inject_common_variables():
        from datetime import date
        return {
            "current_year": date.today().year,
            "app_name": "CTV Reports",
            "version": "2.2.1",  # bumped
        }

    @app.context_processor
    def inject_service_status():
        try:
            services = []
            try:
                services = get_container().list_services()
            except Exception:
                pass

            service_status = app.config.get("SERVICE_STATUS", {})
            # Treat both "healthy" and "registered" as operational for UI purposes
            ok = {"healthy", "registered"}
            healthy_services = sum(1 for s in service_status.values() if s in ok)
            total_services = len({k for k in service_status.keys() if not k.endswith("_decay")})

            # Overall system health
            if total_services == 0:
                system_status = "unknown"
            elif healthy_services == total_services:
                system_status = "healthy"
            elif healthy_services > total_services * 0.5:
                system_status = "degraded"
            else:
                system_status = "critical"

            decay_flag = service_status.get("pipeline_service_decay", "unknown") == "enabled"

            return {
                "services_available": len(services),
                "healthy_services": healthy_services,
                "total_services": total_services,
                "service_status": system_status,
                "decay_system_enabled": decay_flag,
                "system_health": {
                    "status": system_status,
                    "health_check_url": "/health/",
                    "decay_system": decay_flag,
                    "last_check": "live",
                },
            }
        except Exception as e:
            logger.warning(f"Template service status error: {e}")
            return {
                "services_available": 0,
                "healthy_services": 0,
                "total_services": 0,
                "service_status": "error",
                "decay_system_enabled": False,
                "system_health": {
                    "status": "error",
                    "health_check_url": "/health/",
                    "decay_system": False,
                    "error": str(e),
                },
            }

    @app.context_processor
    def inject_decay_system_info():
        """
        Avoid instantiating pipeline_service when validation is lazy.
        """
        try:
            if not app.config.get("EAGER_SERVICE_VALIDATION", True):
                return {
                    "decay_system": {
                        "available": False,
                        "status": "unknown",
                        "api_base_url": "/api/pipeline/decay",
                    }
                }

            svc = get_container().get("pipeline_service")
            info = {
                "available": False,
                "status": "unknown",
                "api_base_url": "/api/pipeline/decay",
            }
            if hasattr(svc, "decay_engine") and getattr(svc, "decay_engine"):
                info.update({
                    "available": True,
                    "status": "enabled",
                    "features": [
                        "real_time_adjustments",
                        "calibration_baselines",
                        "decay_analytics",
                        "timeline_tracking",
                    ],
                })
            return {"decay_system": info}
        except Exception as e:
            return {"decay_system": {"available": False, "status": "error", "error": str(e)}}

    logger.info("Created context processors")


def setup_request_logging(app: Flask) -> None:
    @app.before_request
    def log_request_info():
        logger.debug(f"Request: {request.method} {request.path}")
        if request.args:
            logger.debug(f"Query parameters: {dict(request.args)}")

        if request.path.startswith("/health/"):
            logger.info(f"Health check requested: {request.path}")

        if request.path.startswith("/api/pipeline/decay/"):
            logger.info(f"Decay API request: {request.method} {request.path}")
            if request.is_json:
                payload = request.get_json(silent=True) or {}
                safe = {k: v for k, v in payload.items() if k not in {"webhook_signature", "auth_token"}}
                logger.debug(f"Decay API data: {safe}")

    @app.after_request
    def log_response_info(response):
        logger.debug(f"Response: {response.status_code} for {request.path}")

        if response.status_code >= 400:
            logger.warning(f"Error response: {response.status_code} for {request.method} {request.path}")

            if request.path.startswith("/api/pipeline/decay/"):
                logger.error(f"Decay API error: {response.status_code} for {request.path}")

            if response.status_code >= 500:
                try:
                    status = app.config.get("SERVICE_STATUS", {})
                    unhealthy = [n for n, s in status.items() if s not in {"healthy", "registered"}]
                    if unhealthy:
                        logger.error(f"500 with unhealthy services: {unhealthy}")
                except Exception:
                    pass
        return response

    logger.info("Setup request logging")


def configure_security_headers(app: Flask) -> None:
    @app.after_request
    def add_security_headers(response):
        if response.content_type and "text/html" in response.content_type:
            response.headers["X-Content-Type-Options"] = "nosniff"
            response.headers["X-Frame-Options"] = "DENY"
            response.headers["X-XSS-Protection"] = "1; mode=block"
            response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
            csp = "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'"
            response.headers["Content-Security-Policy"] = csp

        if (request.path.startswith("/api/") or
            request.path.startswith("/health/") or
            request.path.startswith("/api/pipeline/decay/")):
            response.headers["Access-Control-Allow-Origin"] = "*"
            response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
            response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization, X-Emergency-Token, X-Webhook-Signature"

        if request.path.startswith("/health/"):
            response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"

        if request.path.startswith("/api/pipeline/decay/"):
            response.headers["X-Decay-System"] = "enabled"
            response.headers["Cache-Control"] = "no-cache"

        return response

    logger.info("Configured security headers")


def initialize_blueprints(app: Flask) -> None:
    """
    Initialize all blueprint-related configuration. Never abort the entire
    startup just because of service validation if skipping is enabled or
    if we're on Railway.
    """
    try:
        try:
            configure_blueprint_services(app)
        except Exception as e:
            # Final safeguard: if configured to skip hard-fail, continue.
            if _should_skip_hard_fail():
                logger.warning(f"[Degraded Start] Service configuration failed: {e}")
                app.config.setdefault("SERVICE_STATUS", {})
            else:
                raise

        register_blueprints(app)
        register_common_error_handlers(app)
        create_blueprint_context_processors(app)

        if app.config.get("DEBUG", False):
            setup_request_logging(app)

        configure_security_headers(app)

        logger.info("Blueprint initialization complete")
    except Exception as e:
        logger.error(f"Error initializing blueprints: {e}")
        raise


def get_blueprint_info() -> Dict[str, Any]:
    return {
        "blueprints": [
            {
                "name": "reports",
                "url_prefix": "/reports",
                "description": "Report generation and display",
                "status": "active",
            },
            {
                "name": "api",
                "url_prefix": "/api",
                "description": "REST API endpoints",
                "status": "active",
            },
            {
                "name": "budget",
                "url_prefix": "/budget",
                "description": "Budget management interface",
                "status": "active",
            },
            {
                "name": "health",
                "url_prefix": "/health",
                "description": "System health monitoring and emergency repair",
                "status": "active",
                "endpoints": [
                    "/health/ - System health overview",
                    "/health/pipeline - Pipeline service health",
                    "/health/budget - Budget service health",
                    "/health/database - Database connection health",
                    "/health/consistency/validate - Data consistency check",
                    "/health/consistency/repair - Data consistency repair",
                    "/health/emergency/repair - Emergency system repair",
                    "/health/metrics - System performance metrics",
                ],
            },
            {
                "name": "decay_api",
                "url_prefix": "/api/pipeline/decay",
                "description": "Pipeline decay system API for real-time adjustments",
                "status": "active",
                "endpoints": [
                    "/api/pipeline/decay/revenue/booked",
                    "/api/pipeline/decay/revenue/removed",
                    "/api/pipeline/decay/calibration",
                    "/api/pipeline/decay/summary/<ae_id>/<month>",
                    "/api/pipeline/decay/timeline/<ae_id>/<month>",
                    "/api/pipeline/decay/analytics/<ae_id>",
                    "/api/pipeline/decay/ae/<ae_id>/summary",
                    "/api/pipeline/decay/webhook/revenue-change",
                    "/api/pipeline/decay/bulk/calibration",
                    "/api/pipeline/decay/system/status",
                    "/api/pipeline/decay/system/cleanup",
                    "/api/pipeline/decay/export/<ae_id>",
                ],
            },
        ],
        "total_blueprints": 5,
        "features": {
            "health_monitoring": True,
            "emergency_repair": True,
            "data_consistency_checks": True,
            "enhanced_error_handling": True,
            "security_headers": True,
            "request_logging": True,
            "pipeline_decay_system": True,
            "real_time_adjustments": True,
            "decay_analytics": True,
            "webhook_integration": True,
        },
    }
