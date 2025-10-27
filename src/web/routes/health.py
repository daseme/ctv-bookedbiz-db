# src/web/routes/health.py
"""
Health monitoring and emergency repair API endpoints.
Provides system health checks and emergency recovery capabilities.
"""

import logging
from flask import Blueprint, request, jsonify
from datetime import datetime

from src.services.container import get_container
from src.services.factory import get_service_health_report, emergency_service_recovery
from src.web.utils.request_helpers import (
    create_success_response,
    create_error_response,
    handle_request_errors,
    log_requests,
    safe_get_service,
)

logger = logging.getLogger(__name__)

# Create health blueprint
health_bp = Blueprint("health", __name__, url_prefix="/health")


@health_bp.route("/")
@log_requests
@handle_request_errors
def system_health():
    """
    Comprehensive system health check.
    Returns detailed status of all services and data consistency.
    """
    try:
        health_report = get_service_health_report()

        # Add timestamp
        health_report["timestamp"] = datetime.utcnow().isoformat() + "Z"

        # Determine HTTP status code based on health
        status_code = 200
        if health_report["overall_status"] == "degraded":
            status_code = 206  # Partial Content
        elif health_report["overall_status"] in ["unhealthy", "error"]:
            status_code = 503  # Service Unavailable

        return create_success_response(health_report), status_code

    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return create_error_response(
            f"Health check system failure: {str(e)}",
            status_code=500,
            error_code="HEALTH_CHECK_FAILURE",
        )


@health_bp.route("/pipeline")
@log_requests
@handle_request_errors
def pipeline_health():
    """
    Detailed pipeline service health check including data consistency.
    """
    try:
        container = get_container()
        pipeline_service = safe_get_service(container, "pipeline_service")

        # Get comprehensive pipeline health info
        health_info = {
            "service_available": True,
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }

        # Get data source information
        if hasattr(pipeline_service, "get_data_source_info"):
            data_source_info = pipeline_service.get_data_source_info()
            health_info.update(data_source_info)

        # Run consistency validation
        if hasattr(pipeline_service, "validate_data_consistency"):
            consistency_result = pipeline_service.validate_data_consistency()
            health_info["consistency_check"] = {
                "is_consistent": consistency_result.is_consistent,
                "json_records": consistency_result.json_records,
                "db_records": consistency_result.db_records,
                "conflicts": len(consistency_result.conflicts),
                "missing_in_json": len(consistency_result.missing_in_json),
                "missing_in_db": len(consistency_result.missing_in_db),
                "recommendations": consistency_result.recommendations,
                "conflict_details": consistency_result.conflicts[
                    :5
                ],  # First 5 conflicts for brevity
            }

        # Determine status
        status = "healthy"
        if not health_info.get("consistency_status", {}).get("is_consistent", True):
            conflicts = health_info.get("consistency_check", {}).get("conflicts", 0)
            if conflicts > 10:
                status = "critical"
            elif conflicts > 0:
                status = "warning"

        health_info["status"] = status

        status_code = 200
        if status == "warning":
            status_code = 206
        elif status == "critical":
            status_code = 503

        return create_success_response(health_info), status_code

    except Exception as e:
        logger.error(f"Pipeline health check failed: {e}")
        return create_error_response(
            f"Pipeline health check failed: {str(e)}",
            status_code=500,
            error_code="PIPELINE_HEALTH_FAILURE",
        )


@health_bp.route("/budget")
@log_requests
@handle_request_errors
def budget_health():
    """
    Budget service health check with data validation.
    """
    try:
        container = get_container()
        budget_service = safe_get_service(container, "budget_service")

        health_info = {
            "service_available": True,
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }

        # Validate budget data
        if hasattr(budget_service, "validate_budget_data"):
            validation_result = budget_service.validate_budget_data(2025)
            health_info["validation"] = {
                "is_valid": validation_result.get("is_valid", False),
                "warnings": validation_result.get("warnings", []),
                "errors": validation_result.get("errors", []),
                "statistics": validation_result.get("statistics", {}),
            }

        # Test basic operations
        try:
            test_budget = budget_service.get_monthly_budget("TEST_AE", "2025-01")
            health_info["basic_operations"] = {
                "get_monthly_budget": "working",
                "test_result": test_budget,
            }
        except Exception as e:
            health_info["basic_operations"] = {"get_monthly_budget": f"error: {str(e)}"}

        # Determine status
        status = "healthy"
        if health_info.get("validation", {}).get("errors"):
            status = "warning"
        if not health_info.get("validation", {}).get("is_valid", True):
            status = "critical"

        health_info["status"] = status

        status_code = (
            200 if status == "healthy" else (206 if status == "warning" else 503)
        )

        return create_success_response(health_info), status_code

    except Exception as e:
        logger.error(f"Budget health check failed: {e}")
        return create_error_response(
            f"Budget health check failed: {str(e)}",
            status_code=500,
            error_code="BUDGET_HEALTH_FAILURE",
        )


@health_bp.route("/database")
@log_requests
@handle_request_errors
def database_health():
    """
    Database connection health check.
    """
    try:
        container = get_container()

        health_info = {"timestamp": datetime.utcnow().isoformat() + "Z"}

        # Check if database connection is available
        if container.has_service("database_connection"):
            try:
                db_connection = container.get("database_connection")

                # Test connection
                conn = db_connection.connect()

                # Test basic query
                cursor = conn.execute("SELECT 1 as test")
                result = cursor.fetchone()
                conn.close()

                health_info.update(
                    {
                        "connection_available": True,
                        "connection_test": "success",
                        "query_test": "success",
                        "status": "healthy",
                    }
                )

            except Exception as e:
                health_info.update(
                    {
                        "connection_available": True,
                        "connection_test": f"failed: {str(e)}",
                        "status": "critical",
                    }
                )
        else:
            health_info.update({"connection_available": False, "status": "unavailable"})

        status_code = 200 if health_info["status"] == "healthy" else 503

        return create_success_response(health_info), status_code

    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        return create_error_response(
            f"Database health check failed: {str(e)}",
            status_code=500,
            error_code="DATABASE_HEALTH_FAILURE",
        )


@health_bp.route("/consistency/validate", methods=["POST"])
@log_requests
@handle_request_errors
def validate_data_consistency():
    """
    Manually trigger data consistency validation across all services.
    """
    try:
        container = get_container()

        validation_results = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "services": {},
        }

        # Validate pipeline service consistency
        try:
            pipeline_service = safe_get_service(container, "pipeline_service")
            if hasattr(pipeline_service, "validate_data_consistency"):
                result = pipeline_service.validate_data_consistency()
                validation_results["services"]["pipeline"] = {
                    "is_consistent": result.is_consistent,
                    "conflicts": len(result.conflicts),
                    "recommendations": result.recommendations,
                    "details": {
                        "json_records": result.json_records,
                        "db_records": result.db_records,
                        "missing_in_json": len(result.missing_in_json),
                        "missing_in_db": len(result.missing_in_db),
                    },
                }
        except Exception as e:
            validation_results["services"]["pipeline"] = {"error": str(e)}

        # Add other service validations as needed

        # Determine overall consistency
        all_consistent = all(
            service_result.get("is_consistent", False)
            for service_result in validation_results["services"].values()
            if "is_consistent" in service_result
        )

        validation_results["overall_consistent"] = all_consistent
        validation_results["status"] = (
            "consistent" if all_consistent else "inconsistent"
        )

        status_code = 200 if all_consistent else 409  # Conflict

        return create_success_response(validation_results), status_code

    except Exception as e:
        logger.error(f"Consistency validation failed: {e}")
        return create_error_response(
            f"Consistency validation failed: {str(e)}",
            status_code=500,
            error_code="CONSISTENCY_VALIDATION_FAILURE",
        )


@health_bp.route("/consistency/repair", methods=["POST"])
@log_requests
@handle_request_errors
def repair_data_consistency():
    """
    Manually trigger data consistency repair.
    WARNING: This operation may modify data based on configured priorities.
    """
    try:
        # Check if repair is allowed
        force_repair = request.json.get("force", False) if request.json else False

        if not force_repair:
            return create_error_response(
                "Consistency repair requires 'force': true in request body",
                status_code=400,
                error_code="REPAIR_CONFIRMATION_REQUIRED",
            )

        container = get_container()

        repair_results = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "services": {},
            "warnings": [],
        }

        # Repair pipeline service consistency
        try:
            pipeline_service = safe_get_service(container, "pipeline_service")
            if hasattr(pipeline_service, "force_consistency_repair"):
                result = pipeline_service.force_consistency_repair()
                repair_results["services"]["pipeline"] = {
                    "repaired": result.is_consistent,
                    "conflicts_resolved": len(result.conflicts)
                    if not result.is_consistent
                    else 0,
                    "recommendations": result.recommendations,
                }

                if not result.is_consistent:
                    repair_results["warnings"].append(
                        "Pipeline consistency repair completed but issues remain"
                    )
        except Exception as e:
            repair_results["services"]["pipeline"] = {
                "error": str(e),
                "repaired": False,
            }
            repair_results["warnings"].append(f"Pipeline repair failed: {str(e)}")

        # Determine overall success
        all_repaired = all(
            service_result.get("repaired", False)
            for service_result in repair_results["services"].values()
        )

        repair_results["overall_success"] = all_repaired
        repair_results["status"] = "repaired" if all_repaired else "partial"

        status_code = 200 if all_repaired else 206  # Partial success

        return create_success_response(repair_results), status_code

    except Exception as e:
        logger.error(f"Consistency repair failed: {e}")
        return create_error_response(
            f"Consistency repair failed: {str(e)}",
            status_code=500,
            error_code="CONSISTENCY_REPAIR_FAILURE",
        )


@health_bp.route("/emergency/repair", methods=["POST"])
@log_requests
@handle_request_errors
def emergency_repair():
    """
    Emergency system repair - recreates all services and attempts data recovery.
    WARNING: This is a destructive operation that should only be used in emergencies.
    """
    try:
        # Check if emergency repair is authorized
        auth_token = request.headers.get("X-Emergency-Token")
        emergency_code = request.json.get("emergency_code") if request.json else None

        # Simple authorization check (in production, this should be more secure)
        if not emergency_code or emergency_code != "EMERGENCY_REPAIR_AUTHORIZED":
            return create_error_response(
                "Emergency repair requires proper authorization",
                status_code=401,
                error_code="EMERGENCY_REPAIR_UNAUTHORIZED",
            )

        logger.warning("Emergency repair initiated")

        # Perform emergency service recovery
        recovery_result = emergency_service_recovery()

        # Add individual service emergency repairs
        container = get_container()

        # Emergency repair for pipeline service
        try:
            pipeline_service = safe_get_service(container, "pipeline_service")
            if hasattr(pipeline_service, "emergency_repair"):
                pipeline_repair = pipeline_service.emergency_repair()
                recovery_result["pipeline_repair"] = pipeline_repair
        except Exception as e:
            recovery_result["pipeline_repair"] = {"success": False, "error": str(e)}

        recovery_result["emergency_repair_completed"] = True
        recovery_result["status"] = "emergency_repair_complete"

        status_code = 200 if recovery_result["success"] else 206

        logger.warning(
            f"Emergency repair completed with status: {recovery_result['success']}"
        )

        return create_success_response(recovery_result), status_code

    except Exception as e:
        logger.error(f"Emergency repair failed: {e}")
        return create_error_response(
            f"Emergency repair failed: {str(e)}",
            status_code=500,
            error_code="EMERGENCY_REPAIR_FAILURE",
        )


@health_bp.route("/metrics")
@log_requests
@handle_request_errors
def system_metrics():
    """
    System performance and health metrics.
    """
    try:
        import psutil
        import os

        container = get_container()

        metrics = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "system": {
                "cpu_percent": psutil.cpu_percent(interval=1),
                "memory_percent": psutil.virtual_memory().percent,
                "disk_usage_percent": psutil.disk_usage("/").percent,
                "process_id": os.getpid(),
            },
            "services": {
                "registered_services": len(container.list_services()),
                "service_list": list(container.list_services().keys()),
            },
            "application": {
                "environment": container.get_config("ENVIRONMENT", "unknown"),
                "debug_mode": container.get_config("DEBUG", False),
                "cache_enabled": container.get_config("CACHE_ENABLED", True),
            },
        }

        # Add service-specific metrics
        try:
            pipeline_service = safe_get_service(container, "pipeline_service")
            if hasattr(pipeline_service, "get_data_source_info"):
                info = pipeline_service.get_data_source_info()
                metrics["pipeline"] = {
                    "data_source": info.get("data_source"),
                    "json_available": info.get("json_file_exists", False),
                    "db_available": info.get("db_connection_available", False),
                    "consistency_status": info.get("consistency_status", {}).get(
                        "is_consistent", "unknown"
                    ),
                }
        except Exception as e:
            metrics["pipeline"] = {"error": str(e)}

        return create_success_response(metrics)

    except ImportError:
        # psutil not available
        return create_success_response(
            {
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "error": "System metrics not available (psutil not installed)",
                "basic_info": {"services": len(get_container().list_services())},
            }
        )
    except Exception as e:
        logger.error(f"Metrics collection failed: {e}")
        return create_error_response(
            f"Metrics collection failed: {str(e)}",
            status_code=500,
            error_code="METRICS_COLLECTION_FAILURE",
        )


# Error handlers for health blueprint
@health_bp.errorhandler(404)
def health_not_found_error(error):
    """Handle 404 errors in health blueprint."""
    return create_error_response(
        "Health endpoint not found",
        status_code=404,
        error_code="HEALTH_ENDPOINT_NOT_FOUND",
    )


@health_bp.errorhandler(500)
def health_internal_error(error):
    """Handle 500 errors in health blueprint."""
    logger.error(f"Internal error in health blueprint: {error}")
    return create_error_response(
        "Health system internal error",
        status_code=500,
        error_code="HEALTH_INTERNAL_ERROR",
    )
