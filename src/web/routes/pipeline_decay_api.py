# src/web/routes/pipeline_decay_api.py
"""
Pipeline Decay API endpoints for real-time pipeline management.
Provides endpoints for decay tracking, calibration, and analytics.
"""

import logging
from flask import Blueprint, request, jsonify
from datetime import datetime

from src.services.container import get_container
from src.web.utils.request_helpers import (
    create_success_response,
    create_error_response,
    handle_request_errors,
    log_requests,
    safe_get_service,
)

logger = logging.getLogger(__name__)

# Create pipeline decay API blueprint
decay_api_bp = Blueprint(
    "pipeline_decay_api", __name__, url_prefix="/api/pipeline/decay"
)


@decay_api_bp.route("/revenue/booked", methods=["POST"])
@log_requests
@handle_request_errors
def apply_revenue_booking():
    """
    Apply revenue booking with automatic pipeline decay.

    Request body:
    {
        "ae_id": "AE_001",
        "month": "2025-01",
        "amount": 5000,
        "customer": "BigCorp Media",
        "description": "Q1 campaign booked early"
    }
    """
    try:
        data = request.get_json()

        # Validate required fields
        required_fields = ["ae_id", "month", "amount"]
        for field in required_fields:
            if field not in data:
                return create_error_response(f"Missing required field: {field}", 400)

        container = get_container()
        pipeline_service = safe_get_service(container, "pipeline_service")

        # Apply revenue booking
        success = pipeline_service.apply_revenue_booking(
            ae_id=data["ae_id"],
            month=data["month"],
            amount=float(data["amount"]),
            customer=data.get("customer"),
            description=data.get("description"),
        )

        if success:
            # Get updated decay summary
            decay_summary = pipeline_service.get_pipeline_decay_summary(
                data["ae_id"], data["month"]
            )

            return create_success_response(
                {
                    "message": "Revenue booking applied successfully",
                    "decay_applied": True,
                    "decay_summary": decay_summary,
                }
            )
        else:
            return create_error_response("Failed to apply revenue booking", 500)

    except Exception as e:
        logger.error(f"Error applying revenue booking: {e}")
        return create_error_response(f"Revenue booking failed: {str(e)}", 500)


@decay_api_bp.route("/revenue/removed", methods=["POST"])
@log_requests
@handle_request_errors
def apply_revenue_removal():
    """
    Apply revenue removal with automatic pipeline decay.

    Request body:
    {
        "ae_id": "AE_001",
        "month": "2025-01",
        "amount": 3000,
        "customer": "TechStart Inc",
        "reason": "Campaign cancelled"
    }
    """
    try:
        data = request.get_json()

        # Validate required fields
        required_fields = ["ae_id", "month", "amount"]
        for field in required_fields:
            if field not in data:
                return create_error_response(f"Missing required field: {field}", 400)

        container = get_container()
        pipeline_service = safe_get_service(container, "pipeline_service")

        # Apply revenue removal
        success = pipeline_service.apply_revenue_removal(
            ae_id=data["ae_id"],
            month=data["month"],
            amount=float(data["amount"]),
            customer=data.get("customer"),
            reason=data.get("reason"),
        )

        if success:
            # Get updated decay summary
            decay_summary = pipeline_service.get_pipeline_decay_summary(
                data["ae_id"], data["month"]
            )

            return create_success_response(
                {
                    "message": "Revenue removal applied successfully",
                    "decay_applied": True,
                    "decay_summary": decay_summary,
                }
            )
        else:
            return create_error_response("Failed to apply revenue removal", 500)

    except Exception as e:
        logger.error(f"Error applying revenue removal: {e}")
        return create_error_response(f"Revenue removal failed: {str(e)}", 500)


@decay_api_bp.route("/calibration", methods=["POST"])
@log_requests
@handle_request_errors
def set_calibration_baseline():
    """
    Set new pipeline calibration baseline.

    Request body:
    {
        "ae_id": "AE_001",
        "month": "2025-01",
        "pipeline_value": 50000,
        "calibrated_by": "john_doe",
        "session_id": "RS_2025_01_15"
    }
    """
    try:
        data = request.get_json()

        # Validate required fields
        required_fields = ["ae_id", "month", "pipeline_value", "calibrated_by"]
        for field in required_fields:
            if field not in data:
                return create_error_response(f"Missing required field: {field}", 400)

        container = get_container()
        pipeline_service = safe_get_service(container, "pipeline_service")

        # Set calibration baseline
        success = pipeline_service.set_pipeline_calibration(
            ae_id=data["ae_id"],
            month=data["month"],
            pipeline_value=float(data["pipeline_value"]),
            calibrated_by=data["calibrated_by"],
            session_id=data.get("session_id"),
        )

        if success:
            # Get updated decay summary
            decay_summary = pipeline_service.get_pipeline_decay_summary(
                data["ae_id"], data["month"]
            )

            return create_success_response(
                {
                    "message": "Calibration baseline set successfully",
                    "calibration_applied": True,
                    "decay_summary": decay_summary,
                }
            )
        else:
            return create_error_response("Failed to set calibration baseline", 500)

    except Exception as e:
        logger.error(f"Error setting calibration baseline: {e}")
        return create_error_response(f"Calibration failed: {str(e)}", 500)


@decay_api_bp.route("/summary/<ae_id>/<month>")
@log_requests
@handle_request_errors
def get_decay_summary(ae_id: str, month: str):
    """
    Get comprehensive decay summary for AE and month.
    """
    try:
        container = get_container()
        pipeline_service = safe_get_service(container, "pipeline_service")

        # Get decay summary
        decay_summary = pipeline_service.get_pipeline_decay_summary(ae_id, month)

        if decay_summary:
            return create_success_response(decay_summary)
        else:
            return create_error_response(
                f"No decay data found for {ae_id} {month}", 404
            )

    except Exception as e:
        logger.error(f"Error getting decay summary: {e}")
        return create_error_response(f"Failed to get decay summary: {str(e)}", 500)


@decay_api_bp.route("/timeline/<ae_id>/<month>")
@log_requests
@handle_request_errors
def get_decay_timeline(ae_id: str, month: str):
    """
    Get decay timeline showing all events chronologically.
    """
    try:
        container = get_container()
        pipeline_service = safe_get_service(container, "pipeline_service")

        # Get decay timeline
        timeline = pipeline_service.get_decay_timeline(ae_id, month)

        return create_success_response(
            {
                "ae_id": ae_id,
                "month": month,
                "timeline": timeline,
                "event_count": len(timeline),
            }
        )

    except Exception as e:
        logger.error(f"Error getting decay timeline: {e}")
        return create_error_response(f"Failed to get decay timeline: {str(e)}", 500)


@decay_api_bp.route("/analytics/<ae_id>")
@log_requests
@handle_request_errors
def get_decay_analytics(ae_id: str):
    """
    Get comprehensive decay analytics for AE.

    Query parameters:
    - months: Comma-separated list of months (optional)
    """
    try:
        container = get_container()
        pipeline_service = safe_get_service(container, "pipeline_service")

        # Parse months parameter
        months_param = request.args.get("months")
        months = months_param.split(",") if months_param else None

        # Get decay analytics
        analytics = pipeline_service.get_decay_analytics(ae_id, months)

        return create_success_response(analytics)

    except Exception as e:
        logger.error(f"Error getting decay analytics: {e}")
        return create_error_response(f"Failed to get decay analytics: {str(e)}", 500)


@decay_api_bp.route("/ae/<ae_id>/summary")
@log_requests
@handle_request_errors
def get_ae_summary_with_decay(ae_id: str):
    """
    Get AE summary enhanced with decay information.
    """
    try:
        container = get_container()
        pipeline_service = safe_get_service(container, "pipeline_service")

        # Get monthly summaries with decay info
        monthly_summaries = pipeline_service.get_monthly_summary_with_decay(ae_id)

        # Get AE basic info
        ae_data = pipeline_service.get_pipeline_data(ae_id)
        ae_info = {
            "ae_id": ae_id,
            "ae_name": ae_data.get("ae_name", ""),
            "territory": ae_data.get("territory", ""),
        }

        # Calculate aggregate decay metrics
        total_decay = sum(s.get("total_decay", 0) for s in monthly_summaries)
        active_months = [
            s for s in monthly_summaries if s.get("has_decay_activity", False)
        ]

        aggregate_metrics = {
            "total_decay": total_decay,
            "active_months_count": len(active_months),
            "total_months": len(monthly_summaries),
            "avg_decay_per_month": total_decay / len(monthly_summaries)
            if monthly_summaries
            else 0,
            "most_active_month": max(
                active_months, key=lambda x: len(x.get("decay_events", []))
            )["month"]
            if active_months
            else None,
        }

        return create_success_response(
            {
                "ae_info": ae_info,
                "monthly_summaries": monthly_summaries,
                "aggregate_metrics": aggregate_metrics,
                "decay_enabled": True,
            }
        )

    except Exception as e:
        logger.error(f"Error getting AE summary with decay: {e}")
        return create_error_response(f"Failed to get AE summary: {str(e)}", 500)


@decay_api_bp.route("/webhook/revenue-change", methods=["POST"])
@log_requests
@handle_request_errors
def webhook_revenue_change():
    """
    Webhook endpoint for external revenue systems to trigger pipeline decay.

    Request body:
    {
        "ae_name": "John Doe",
        "month": "2025-01",
        "amount_change": -5000,
        "change_type": "revenue_booked",
        "customer": "BigCorp Media",
        "description": "Q1 campaign signed",
        "webhook_signature": "optional_security_signature"
    }
    """
    try:
        data = request.get_json()

        # Validate required fields
        required_fields = ["ae_name", "month", "amount_change", "change_type"]
        for field in required_fields:
            if field not in data:
                return create_error_response(f"Missing required field: {field}", 400)

        # Validate change type
        valid_change_types = ["revenue_booked", "revenue_removed"]
        if data["change_type"] not in valid_change_types:
            return create_error_response(
                f"Invalid change_type. Must be one of: {valid_change_types}", 400
            )

        container = get_container()
        pipeline_service = safe_get_service(container, "pipeline_service")

        # TODO: Add webhook signature validation for security
        # webhook_signature = data.get('webhook_signature')
        # if not validate_webhook_signature(data, webhook_signature):
        #     return create_error_response("Invalid webhook signature", 401)

        # Convert AE name to AE ID (you'll need to implement this mapping)
        ae_name = data["ae_name"]
        ae_id = f"AE_{ae_name.replace(' ', '_').upper()}"  # Simple mapping for now

        # Apply the appropriate revenue change
        if data["change_type"] == "revenue_booked":
            success = pipeline_service.apply_revenue_booking(
                ae_id=ae_id,
                month=data["month"],
                amount=abs(float(data["amount_change"])),  # Ensure positive for booking
                customer=data.get("customer"),
                description=data.get("description"),
            )
        else:  # revenue_removed
            success = pipeline_service.apply_revenue_removal(
                ae_id=ae_id,
                month=data["month"],
                amount=abs(float(data["amount_change"])),  # Ensure positive for removal
                customer=data.get("customer"),
                reason=data.get("description"),
            )

        if success:
            # Get updated decay summary
            decay_summary = pipeline_service.get_pipeline_decay_summary(
                ae_id, data["month"]
            )

            return create_success_response(
                {
                    "message": f"Webhook processed successfully: {data['change_type']}",
                    "ae_id": ae_id,
                    "ae_name": ae_name,
                    "month": data["month"],
                    "amount_change": data["amount_change"],
                    "decay_applied": True,
                    "decay_summary": decay_summary,
                }
            )
        else:
            return create_error_response("Failed to process webhook", 500)

    except Exception as e:
        logger.error(f"Error processing revenue change webhook: {e}")
        return create_error_response(f"Webhook processing failed: {str(e)}", 500)


@decay_api_bp.route("/bulk/calibration", methods=["POST"])
@log_requests
@handle_request_errors
def bulk_calibration():
    """
    Bulk calibration endpoint for bi-weekly review sessions.

    Request body:
    {
        "session_id": "RS_2025_01_15",
        "calibrated_by": "review_facilitator",
        "calibrations": [
            {
                "ae_id": "AE_001",
                "month": "2025-01",
                "pipeline_value": 50000
            },
            {
                "ae_id": "AE_001",
                "month": "2025-02",
                "pipeline_value": 45000
            }
        ]
    }
    """
    try:
        data = request.get_json()

        # Validate required fields
        required_fields = ["calibrations", "calibrated_by"]
        for field in required_fields:
            if field not in data:
                return create_error_response(f"Missing required field: {field}", 400)

        calibrations = data["calibrations"]
        if not isinstance(calibrations, list) or not calibrations:
            return create_error_response("Calibrations must be a non-empty list", 400)

        container = get_container()
        pipeline_service = safe_get_service(container, "pipeline_service")

        results = []
        successful = 0
        failed = 0

        for calibration in calibrations:
            # Validate individual calibration
            cal_required = ["ae_id", "month", "pipeline_value"]
            if not all(field in calibration for field in cal_required):
                results.append(
                    {
                        "ae_id": calibration.get("ae_id", "unknown"),
                        "month": calibration.get("month", "unknown"),
                        "success": False,
                        "error": "Missing required fields",
                    }
                )
                failed += 1
                continue

            # Apply calibration
            success = pipeline_service.set_pipeline_calibration(
                ae_id=calibration["ae_id"],
                month=calibration["month"],
                pipeline_value=float(calibration["pipeline_value"]),
                calibrated_by=data["calibrated_by"],
                session_id=data.get("session_id"),
            )

            result = {
                "ae_id": calibration["ae_id"],
                "month": calibration["month"],
                "pipeline_value": calibration["pipeline_value"],
                "success": success,
            }

            if success:
                successful += 1
                # Get updated decay summary
                decay_summary = pipeline_service.get_pipeline_decay_summary(
                    calibration["ae_id"], calibration["month"]
                )
                result["decay_summary"] = decay_summary
            else:
                failed += 1
                result["error"] = "Calibration failed"

            results.append(result)

        return create_success_response(
            {
                "message": f"Bulk calibration completed: {successful} successful, {failed} failed",
                "session_id": data.get("session_id"),
                "total_calibrations": len(calibrations),
                "successful": successful,
                "failed": failed,
                "results": results,
            }
        )

    except Exception as e:
        logger.error(f"Error processing bulk calibration: {e}")
        return create_error_response(f"Bulk calibration failed: {str(e)}", 500)


@decay_api_bp.route("/system/cleanup", methods=["POST"])
@log_requests
@handle_request_errors
def cleanup_old_events():
    """
    Clean up old decay events to prevent file bloat.

    Request body:
    {
        "days_to_keep": 90
    }
    """
    try:
        data = request.get_json()
        days_to_keep = data.get("days_to_keep", 90) if data else 90

        # Validate days_to_keep
        if not isinstance(days_to_keep, int) or days_to_keep < 1:
            return create_error_response("days_to_keep must be a positive integer", 400)

        container = get_container()
        pipeline_service = safe_get_service(container, "pipeline_service")

        # Perform cleanup
        if hasattr(pipeline_service, "decay_engine") and pipeline_service.decay_engine:
            events_removed = pipeline_service.decay_engine.cleanup_old_decay_events(
                days_to_keep
            )

            return create_success_response(
                {
                    "message": f"Cleanup completed: {events_removed} old events removed",
                    "events_removed": events_removed,
                    "days_to_keep": days_to_keep,
                    "cleanup_date": datetime.utcnow().isoformat() + "Z",
                }
            )
        else:
            return create_error_response("Decay system not available", 503)

    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        return create_error_response(f"Cleanup failed: {str(e)}", 500)


@decay_api_bp.route("/system/status")
@log_requests
@handle_request_errors
def get_system_status():
    """
    Get decay system status and health information.
    """
    try:
        container = get_container()
        pipeline_service = safe_get_service(container, "pipeline_service")

        status = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "decay_system_enabled": hasattr(pipeline_service, "decay_engine")
            and pipeline_service.decay_engine is not None,
            "system_health": "unknown",
        }

        if pipeline_service.decay_engine:
            try:
                # Get decay system information
                decay_data = pipeline_service.decay_engine._read_decay_data()

                status.update(
                    {
                        "system_health": "healthy",
                        "decay_file_exists": True,
                        "schema_version": decay_data.get("schema_version", "unknown"),
                        "last_updated": decay_data.get("last_updated"),
                        "tracked_aes": len(decay_data.get("decay_tracking", {})),
                        "total_months_tracked": sum(
                            len(ae_data)
                            for ae_data in decay_data.get("decay_tracking", {}).values()
                        ),
                        "total_decay_events": sum(
                            len(month_data.get("decay_events", []))
                            for ae_data in decay_data.get("decay_tracking", {}).values()
                            for month_data in ae_data.values()
                        ),
                        "data_source": pipeline_service.data_source.value
                        if hasattr(pipeline_service, "data_source")
                        else "unknown",
                    }
                )

                # Calculate recent activity
                recent_events = 0
                cutoff_date = (
                    datetime.utcnow().replace(day=1)
                    - datetime.utcnow().replace(
                        day=1,
                        month=datetime.utcnow().month - 1
                        if datetime.utcnow().month > 1
                        else 12,
                    )
                ).isoformat() + "Z"

                for ae_data in decay_data.get("decay_tracking", {}).values():
                    for month_data in ae_data.values():
                        for event in month_data.get("decay_events", []):
                            if event.get("timestamp", "") > cutoff_date:
                                recent_events += 1

                status["recent_activity"] = {
                    "events_last_30_days": recent_events,
                    "activity_level": "high"
                    if recent_events > 50
                    else "medium"
                    if recent_events > 10
                    else "low",
                }

            except Exception as e:
                status.update(
                    {
                        "system_health": "degraded",
                        "error": str(e),
                        "decay_file_exists": False,
                    }
                )
        else:
            status.update(
                {"system_health": "disabled", "message": "Decay system not initialized"}
            )

        return create_success_response(status)

    except Exception as e:
        logger.error(f"Error getting system status: {e}")
        return create_error_response(f"Failed to get system status: {str(e)}", 500)


@decay_api_bp.route("/export/<ae_id>")
@log_requests
@handle_request_errors
def export_decay_data(ae_id: str):
    """
    Export decay data for an AE in various formats.

    Query parameters:
    - format: csv, json (default: json)
    - months: comma-separated list of months (optional)
    """
    try:
        export_format = request.args.get("format", "json").lower()
        months_param = request.args.get("months")
        months = months_param.split(",") if months_param else None

        container = get_container()
        pipeline_service = safe_get_service(container, "pipeline_service")

        # Get decay analytics (includes all the data we need)
        analytics = pipeline_service.get_decay_analytics(ae_id, months)

        if export_format == "csv":
            return _export_decay_as_csv(analytics, ae_id)
        elif export_format == "json":
            return create_success_response(analytics)
        else:
            return create_error_response(
                f"Unsupported export format: {export_format}", 400
            )

    except Exception as e:
        logger.error(f"Error exporting decay data: {e}")
        return create_error_response(f"Export failed: {str(e)}", 500)


def _export_decay_as_csv(analytics: dict, ae_id: str):
    """Export decay analytics as CSV."""
    import csv
    import io
    from flask import Response

    output = io.StringIO()
    writer = csv.writer(output)

    # Write headers
    writer.writerow(
        [
            "AE_ID",
            "Month",
            "Calibrated_Pipeline",
            "Current_Pipeline",
            "Total_Decay",
            "Decay_Rate_Per_Day",
            "Decay_Percentage",
            "Days_Since_Calibration",
            "Event_Count",
        ]
    )

    # Write data
    monthly_summaries = analytics.get("monthly_summaries", {})
    for month, summary in monthly_summaries.items():
        writer.writerow(
            [
                ae_id,
                month,
                summary.get("calibrated_pipeline", 0),
                summary.get("current_pipeline", 0),
                summary.get("total_decay", 0),
                summary.get("decay_rate_per_day", 0),
                summary.get("decay_percentage", 0),
                summary.get("days_since_calibration", 0),
                len(summary.get("decay_events", [])),
            ]
        )

    # Create response
    csv_data = output.getvalue()

    return Response(
        csv_data,
        mimetype="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename=decay_export_{ae_id}.csv"
        },
    )


# Error handlers for decay API blueprint
@decay_api_bp.errorhandler(404)
def decay_api_not_found_error(error):
    """Handle 404 errors in decay API blueprint."""
    return create_error_response(
        "Decay API endpoint not found",
        status_code=404,
        error_code="DECAY_ENDPOINT_NOT_FOUND",
    )


@decay_api_bp.errorhandler(500)
def decay_api_internal_error(error):
    """Handle 500 errors in decay API blueprint."""
    logger.error(f"Internal error in decay API blueprint: {error}")
    return create_error_response(
        "Decay API internal error", status_code=500, error_code="DECAY_INTERNAL_ERROR"
    )
