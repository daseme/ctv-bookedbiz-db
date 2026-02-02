# src/web/blueprints.py
"""
Blueprint registration & service validation hardened for Railway.

Key behavior
------------
- Never hard-crash on Railway unless explicitly forced.
- Eager vs. lazy service validation controlled via env flags.
- Safe optional import of customer-sector routes (no early logger use).
- Consistent JSON error payloads for API/health/decay routes.
- Security headers + simple CORS for API/health paths.
- Optional verbose request/response logging in DEBUG.

Env Flags
---------
SKIP_SERVICE_VALIDATION : "1"/"true" -> bypass hard-fail even in production (default on Railway)
EAGER_SERVICE_VALIDATION: "1"/"true" -> instantiate/check services on startup (default off on Railway)
"""

from __future__ import annotations

import os
import logging
from typing import Dict, Any, Optional

from flask import Flask, request, render_template, jsonify

# --- module logger first (fixes early-use bug in optional imports) ---
logger = logging.getLogger(__name__)

# --- app blueprints & utilities ---
from src.web.routes.reports import reports_bp
from src.web.routes.api import api_bp
from src.web.routes.health import health_bp
from src.web.routes.language_blocks import language_blocks_bp
from src.web.routes.customer_detail_routes import customer_detail_bp
from src.services.container import get_container
from src.utils.template_formatters import register_template_filters

# Optional feature: customer sector API (do not break import if missing)
try:
    from src.web.routes.customer_sector_api import customer_sector_bp  # type: ignore

    CUSTOMER_SECTOR_AVAILABLE = True
except Exception as e:  # broad: any import-time failure should not sink app
    logger.warning("Customer-sector routes not available: %s", e)
    CUSTOMER_SECTOR_AVAILABLE = False
    customer_sector_bp = None  # type: ignore


# =============================================================================
# High-level entrypoints (call-graph roots)
# =============================================================================


def initialize_blueprints(app: Flask) -> None:
    """
    Initialize all blueprint-related configuration. Never abort full startup
    just because of service validation if skipping is enabled or if we're on Railway.
    """
    try:
        try:
            configure_blueprint_services(app)
        except Exception as e:
            if _should_skip_hard_fail():
                logger.warning("[Degraded Start] Service configuration failed: %s", e)
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
        logger.error("Error initializing blueprints: %s", e, exc_info=True)
        raise


def register_blueprints(app: Flask) -> None:
    """
    Register all Flask blueprints. Keep order stable for predictable URL rules.
    """
    try:
        app.register_blueprint(language_blocks_bp)
        logger.info("Registered language blocks blueprint")

        if CUSTOMER_SECTOR_AVAILABLE and customer_sector_bp:
            app.register_blueprint(customer_sector_bp)  # type: ignore[arg-type]
            logger.info("Registered customer-sector API blueprint")
        else:
            logger.info("Skipped customer-sector API blueprint (not available)")

        app.register_blueprint(reports_bp)
        logger.info("Registered reports blueprint")

        app.register_blueprint(customer_detail_bp, url_prefix='/reports')  # Add this
        logger.info("Registered customer detail blueprint")

        app.register_blueprint(api_bp)
        logger.info("Registered API blueprint")

        app.register_blueprint(health_bp)
        logger.info("Registered health monitoring blueprint")

        register_template_filters(app)
        logger.info("Registered template filters")

        logger.info("All blueprints registered successfully")
    except Exception as e:
        logger.error("Error registering blueprints: %s", e, exc_info=True)
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

        ]

        service_status: Dict[str, str] = {}

        for name in required_services:
            try:
                if not container.has_service(name):
                    service_status[name] = "not_registered"
                    logger.warning("Required service '%s' not registered", name)
                    continue

                if eager_validation:
                    # Instantiate & lightly probe
                    svc = container.get(name)
                    if svc is None:
                        service_status[name] = "available_but_null"
                        logger.warning("Service '%s' returned None", name)
                    else:
                        service_status[name] = "healthy"
                        logger.debug("Verified service '%s' is healthy", name)

            except Exception as e:
                service_status[name] = f"error: {e}"
                logger.error(
                    "Service '%s' failed validation: %s", name, e, exc_info=True
                )

        app.config["SERVICE_STATUS"] = service_status

        # Identify hard failures
        critical_failures = [
            n
            for n, s in service_status.items()
            if s.startswith("error:") or s == "not_registered"
        ]

        if critical_failures:
            msg = f"Critical service failures in {environment}: {critical_failures}"
            if environment == "production" and not skip_hard_fail:
                logger.error(msg)
                # In strict prod, fail fast (only when not skipping)
                raise RuntimeError(
                    f"Critical services failed in production: {critical_failures}"
                )
            else:
                # Degraded but continue
                logger.warning(
                    "[Degraded Start] %s (skip_hard_fail=%s, eager=%s)",
                    msg,
                    skip_hard_fail,
                    eager_validation,
                )

        logger.info(
            "Blueprint services configured (env=%s, eager=%s, skip_hard_fail=%s)",
            environment,
            eager_validation,
            skip_hard_fail,
        )

    except Exception as e:
        logger.error("Error configuring blueprint services: %s", e, exc_info=True)
        raise


# =============================================================================
# Error handlers, context processors, logging, security
# =============================================================================


def register_common_error_handlers(app: Flask) -> None:
    def _is_api_path(p: str) -> bool:
        return p.startswith("/api/") or p.startswith("/health/")

    @app.errorhandler(404)
    def not_found_error(error):
        logger.warning(
            "404: %s %s from %s", request.method, request.path, request.remote_addr
        )
        if _is_api_path(request.path):
            return jsonify(
                {
                    "success": False,
                    "error": "Not found",
                    "status": 404,
                    "error_code": "NOT_FOUND",
                    "path": request.path,
                }
            ), 404
        return render_template("error_404.html"), 404

    @app.errorhandler(500)
    def internal_error(error):
        logger.error("500: %s", error, exc_info=True)
        try:
            service_status = app.config.get("SERVICE_STATUS", {})
            unhealthy = [
                n
                for n, s in service_status.items()
                if s not in {"healthy", "registered"}
            ]
            if unhealthy:
                logger.error("Error occurred with unhealthy services: %s", unhealthy)
        except Exception as e:
            logger.error("Health introspection failed during 500 handler: %s", e)

        if _is_api_path(request.path):
            payload = {
                "success": False,
                "error": "Internal server error",
                "status": 500,
                "error_code": "INTERNAL_ERROR",
                "path": request.path,
            }

        return render_template("error_500.html"), 500

    @app.errorhandler(400)
    def bad_request_error(error):
        logger.warning("400: %s %s - %s", request.method, request.path, error)
        if request.path.startswith("/api/") or request.path.startswith("/health/"):
            return jsonify(
                {
                    "success": False,
                    "error": "Bad request",
                    "status": 400,
                    "error_code": "BAD_REQUEST",
                }
            ), 400
        return render_template("error_400.html"), 400

    @app.errorhandler(403)
    def forbidden_error(error):
        logger.warning(
            "403: %s %s from %s", request.method, request.path, request.remote_addr
        )
        if request.path.startswith("/api/") or request.path.startswith("/health/"):
            return jsonify(
                {
                    "success": False,
                    "error": "Forbidden",
                    "status": 403,
                    "error_code": "FORBIDDEN",
                }
            ), 403
        return render_template("error_403.html"), 403

    @app.errorhandler(503)
    def service_unavailable_error(error):
        logger.error("503: %s %s", request.method, request.path)
        if request.path.startswith("/api/") or request.path.startswith("/health/"):
            return jsonify(
                {
                    "success": False,
                    "error": "Service temporarily unavailable",
                    "status": 503,
                    "error_code": "SERVICE_UNAVAILABLE",
                    "message": "System is experiencing issues. Please try again later.",
                }
            ), 503
        return render_template("error_503.html"), 503

    logger.info("Registered common error handlers")


def create_blueprint_context_processors(app: Flask) -> None:
    @app.context_processor
    def inject_common_variables():
        from datetime import date

        return {
            "current_year": date.today().year,
            "app_name": "CTV Reports",
            "version": "2.2.1",  # bump as needed
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
            ok = {"healthy", "registered"}
            healthy_services = sum(1 for s in service_status.values() if s in ok)
            total_services = len(
                {k for k in service_status.keys() if not k.endswith("_decay")}
            )

            if total_services == 0:
                system_status = "unknown"
            elif healthy_services == total_services:
                system_status = "healthy"
            elif healthy_services > total_services * 0.5:
                system_status = "degraded"
            else:
                system_status = "critical"

            return {
                "services_available": len(services),
                "healthy_services": healthy_services,
                "total_services": total_services,
                "service_status": system_status,
                "system_health": {
                    "status": system_status,
                    "health_check_url": "/health/",
                    "last_check": "live",
                },
            }
        except Exception as e:
            logger.warning("Template service status error: %s", e)
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


def setup_request_logging(app: Flask) -> None:
    @app.before_request
    def log_request_info():
        logger.debug("Request: %s %s", request.method, request.path)
        if request.args:
            logger.debug("Query parameters: %s", dict(request.args))

        if request.path.startswith("/health/"):
            logger.info("Health check requested: %s", request.path)

    @app.after_request
    def log_response_info(response):
        logger.debug("Response: %s for %s", response.status_code, request.path)

        if response.status_code >= 400:
            logger.warning(
                "Error response: %s for %s %s",
                response.status_code,
                request.method,
                request.path,
            )

            if response.status_code >= 500:
                try:
                    status = app.config.get("SERVICE_STATUS", {})
                    unhealthy = [
                        n
                        for n, s in status.items()
                        if s not in {"healthy", "registered"}
                    ]
                    if unhealthy:
                        logger.error("500 with unhealthy services: %s", unhealthy)
                except Exception:
                    pass
        return response

    logger.info("Setup request logging")


def configure_security_headers(app: Flask) -> None:
    @app.after_request
    def add_security_headers(response):
        ct = response.content_type or ""
        path = request.path or ""

        if "text/html" in ct:
            response.headers["X-Content-Type-Options"] = "nosniff"
            response.headers["X-Frame-Options"] = "DENY"
            response.headers["X-XSS-Protection"] = "1; mode=block"
            response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
            # Allow inline for Jinja output; tighten if you move to hashed CSP.
            response.headers["Content-Security-Policy"] = (
                "default-src 'self'; "
                "script-src 'self' 'unsafe-inline' https://cdnjs.cloudflare.com https://cdn.jsdelivr.net; "
                "style-src 'self' 'unsafe-inline'"
            )

        if path.startswith("/api/") or path.startswith("/health/"):
            response.headers["Access-Control-Allow-Origin"] = "*"
            response.headers["Access-Control-Allow-Methods"] = (
                "GET, POST, PUT, DELETE, OPTIONS"
            )
            response.headers["Access-Control-Allow-Headers"] = (
                "Content-Type, Authorization, X-Emergency-Token, X-Webhook-Signature"
            )

        if path.startswith("/health/"):
            response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"

        return response

    logger.info("Configured security headers")


# =============================================================================
# Introspection
# =============================================================================


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
                "name": "health",
                "url_prefix": "/health",
                "description": "System health monitoring and emergency repair",
                "status": "active",
                "endpoints": [
                    "/health/ - System health overview",
                    "/health/database - Database connection health",
                    "/health/consistency/validate - Data consistency check",
                    "/health/consistency/repair - Data consistency repair",
                    "/health/emergency/repair - Emergency system repair",
                    "/health/metrics - System performance metrics",
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
            "real_time_adjustments": True,
            "decay_analytics": True,
            "webhook_integration": True,
        },
    }


# =============================================================================
# Internals (env & platform helpers)
# =============================================================================


def _env_bool(name: str, default: Optional[bool] = None) -> Optional[bool]:
    """
    Ternary env bool: returns True/False if present, else `default`.
    Recognizes: 1, true, yes, y, on / 0, false, no, n, off
    """
    val = os.getenv(name)
    if val is None:
        return default
    s = val.strip().lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _truthy_env(name: str, default: bool = False) -> bool:
    return bool(_env_bool(name, default))


def _is_railway() -> bool:
    # Robust Railway detection: any RAILWAY_* var counts or explicit flags
    if any(k.startswith("RAILWAY_") for k in os.environ.keys()):
        return True
    return _truthy_env("RAILWAY", False) or _truthy_env("RAILWAY_ENVIRONMENT", False)


def _resolve_environment() -> str:
    # Prefer container config when available; fall back to envs
    try:
        c = get_container()
        env = c.get_config("ENVIRONMENT", None)
        if env:
            return str(env).lower()
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
    # If the flag is set, respect it; otherwise default lazy on Railway, eager elsewhere.
    v = _env_bool("EAGER_SERVICE_VALIDATION", None)
    if v is not None:
        return v
    return not _is_railway()


__all__ = [
    "initialize_blueprints",
    "register_blueprints",
    "configure_blueprint_services",
    "register_common_error_handlers",
    "create_blueprint_context_processors",
    "setup_request_logging",
    "configure_security_headers",
    "get_blueprint_info",
]
