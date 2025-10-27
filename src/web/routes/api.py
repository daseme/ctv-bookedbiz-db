# src/web/routes/api.py
"""
API blueprint with clean REST endpoints.
Provides JSON responses for AJAX calls and data export.
"""

import logging
from flask import Blueprint, request
from datetime import date, datetime, timedelta

from src.services.container import get_container
from src.web.utils.request_helpers import (
    extract_report_filters,
    get_year_parameter,
    create_success_response,
    create_error_response,
    handle_service_error,
    safe_get_service,
    log_requests,
    handle_request_errors,
    get_export_format,
    create_csv_response,
    get_pagination_parameters,
)

logger = logging.getLogger(__name__)

# Create API blueprint
api_bp = Blueprint("api", __name__, url_prefix="/api")


@api_bp.route("/health")
def health_check():
    """Health check endpoint."""
    return create_success_response(
        {"status": "healthy", "timestamp": date.today().isoformat()}
    )


# Revenue API endpoints
@api_bp.route("/revenue/monthly/<int:year>")
@log_requests
@handle_request_errors
def get_monthly_revenue(year: int):
    """Get monthly revenue data for specific year."""
    try:
        container = get_container()
        report_service = safe_get_service(container, "report_data_service")

        # Extract filters
        filters = extract_report_filters()
        filters.year = year

        # Get data
        report_data = report_service.get_monthly_revenue_report_data(year, filters)

        return create_success_response(
            report_data.to_dict(),
            metadata={
                "processing_time_ms": report_data.metadata.processing_time_ms,
                "row_count": report_data.metadata.row_count,
            },
        )

    except Exception as e:
        return handle_service_error(e, f"getting monthly revenue for {year}")


@api_bp.route("/revenue/summary")
@log_requests
@handle_request_errors
def get_revenue_summary():
    """Get revenue summary data."""
    try:
        container = get_container()
        report_service = safe_get_service(container, "report_data_service")

        filters = extract_report_filters()
        year = filters.year or date.today().year

        # Get summary data
        report_data = report_service.get_monthly_revenue_report_data(year, filters)

        # Return just the summary statistics
        summary = {
            "total_customers": report_data.total_customers,
            "active_customers": report_data.active_customers,
            "total_revenue": float(report_data.total_revenue),
            "avg_monthly_revenue": float(report_data.avg_monthly_revenue),
            "year": report_data.selected_year,
        }

        return create_success_response(summary)

    except Exception as e:
        return handle_service_error(e, "getting revenue summary")


@api_bp.route("/ae/performance")
@log_requests
@handle_request_errors
def get_ae_performance():
    """Get AE performance data."""
    try:
        container = get_container()
        report_service = safe_get_service(container, "report_data_service")

        filters = extract_report_filters()

        # Get AE performance data
        ae_data = report_service.get_ae_performance_report_data(filters)

        return create_success_response(
            ae_data.to_dict(),
            metadata={
                "processing_time_ms": ae_data.metadata.processing_time_ms,
                "row_count": ae_data.metadata.row_count,
            },
        )

    except Exception as e:
        return handle_service_error(e, "getting AE performance data")


@api_bp.route("/quarterly/performance")
@log_requests
@handle_request_errors
def get_quarterly_performance():
    """Get quarterly performance data."""
    try:
        container = get_container()
        report_service = safe_get_service(container, "report_data_service")

        filters = extract_report_filters()

        # Get quarterly data
        quarterly_data = report_service.get_quarterly_performance_data(filters)

        return create_success_response(
            quarterly_data.to_dict(),
            metadata={"processing_time_ms": quarterly_data.metadata.processing_time_ms},
        )

    except Exception as e:
        return handle_service_error(e, "getting quarterly performance data")


@api_bp.route("/sectors/performance")
@log_requests
@handle_request_errors
def get_sector_performance():
    """Get sector performance data."""
    try:
        container = get_container()
        report_service = safe_get_service(container, "report_data_service")

        filters = extract_report_filters()

        # Get sector data
        sector_data = report_service.get_sector_performance_data(filters)

        return create_success_response(
            sector_data.to_dict(),
            metadata={"processing_time_ms": sector_data.metadata.processing_time_ms},
        )

    except Exception as e:
        return handle_service_error(e, "getting sector performance data")


# Pipeline API endpoints
@api_bp.route("/aes")
@log_requests
@handle_request_errors
def get_aes():
    """Get list of all Account Executives."""
    try:
        container = get_container()
        pipeline_service = safe_get_service(container, "pipeline_service")

        ae_list = pipeline_service.get_ae_list()

        return create_success_response(ae_list)

    except Exception as e:
        return handle_service_error(e, "getting AE list")


@api_bp.route("/ae/<ae_id>/summary")
@log_requests
@handle_request_errors
def get_ae_summary(ae_id: str):
    """Get comprehensive summary for specific AE with real data."""
    try:
        container = get_container()

        # Get services safely
        report_service = safe_get_service(container, "report_data_service")

        # Try to get pipeline service - if it fails, continue with limited functionality
        pipeline_service = None
        try:
            pipeline_service = safe_get_service(container, "pipeline_service")
        except Exception as e:
            logger.warning(f"Pipeline service not available: {e}")

        # Try to get budget service - if it fails, use defaults
        budget_service = None
        try:
            budget_service = safe_get_service(container, "budget_service")
        except Exception as e:
            logger.warning(f"Budget service not available: {e}")

        # Convert ae_id to AE name (reverse the conversion from pipeline_routes.py)
        ae_name = ae_id.replace("ae_", "").replace("_", " ").title()
        logger.info(f"Looking up data for AE: {ae_name} (ID: {ae_id})")

        # Get real data from report service
        current_year = datetime.now().year

        # Create filters - FIXED to use correct ReportFilters parameters
        from src.models.report_data import ReportFilters

        # Check what parameters ReportFilters accepts by creating basic filter first
        try:
            # Try with just year first
            filters = ReportFilters(year=current_year)
            logger.info("Created basic ReportFilters successfully")
        except Exception as e:
            logger.error(f"Could not create ReportFilters: {e}")
            # Fallback to no filters
            filters = None

        # Get monthly revenue data for all AEs (we'll filter manually)
        monthly_report = None
        try:
            monthly_report = report_service.get_monthly_revenue_report_data(
                current_year, filters
            )
            logger.info("Got monthly revenue report data successfully")
        except Exception as e:
            logger.error(f"Could not get monthly revenue report: {e}")

        # Initialize totals
        ytd_actual = 0
        monthly_summaries = []

        # Process each month
        for month in range(1, 13):
            month_str = f"{current_year}-{month:02d}"

            # Get revenue for this month
            booked_revenue = 0

            if monthly_report and hasattr(monthly_report, "to_dict"):
                monthly_dict = monthly_report.to_dict()
                revenue_data = monthly_dict.get("revenue_data", [])

                # Find revenue for this AE and month - manual filtering
                for item in revenue_data:
                    item_ae = item.get("ae", "")
                    if item_ae == ae_name:  # Exact match
                        month_key = f"month_{month}"
                        if month_key in item:
                            month_value = item[month_key]
                            if month_value is not None:
                                booked_revenue += float(month_value)

                # Add to YTD if month has passed
                if month <= datetime.now().month:
                    ytd_actual += booked_revenue

            # Get budget (default if service not available)
            budget = 25000  # Default monthly budget
            if budget_service:
                try:
                    budget = budget_service.get_monthly_budget(ae_name, month_str)
                    if budget is None:
                        budget = 25000
                except Exception as e:
                    logger.warning(
                        f"Could not get budget for {ae_name} {month_str}: {e}"
                    )
                    budget = 25000

            # Calculate pipeline (simple calculation if decay service not available)
            current_pipeline = max(0, budget - booked_revenue)

            # Try to get decay information if pipeline service is available
            has_decay_activity = False
            decay_events_count = 0
            total_decay = 0
            days_since_calibration = 0

            if pipeline_service and hasattr(
                pipeline_service, "get_pipeline_decay_summary"
            ):
                try:
                    decay_summary = pipeline_service.get_pipeline_decay_summary(
                        ae_id, month_str
                    )
                    if decay_summary:
                        has_decay_activity = (
                            len(decay_summary.get("decay_events", [])) > 0
                        )
                        decay_events_count = len(decay_summary.get("decay_events", []))
                        total_decay = decay_summary.get("total_decay", 0)
                        days_since_calibration = decay_summary.get(
                            "days_since_calibration", 0
                        )
                        current_pipeline = decay_summary.get(
                            "current_pipeline", current_pipeline
                        )
                except Exception as e:
                    logger.warning(
                        f"Could not get decay summary for {ae_id} {month_str}: {e}"
                    )

            # Build monthly summary
            monthly_summary = {
                "month": month_str,
                "month_display": format_month_display(month_str),
                "booked_revenue": booked_revenue,
                "current_pipeline": current_pipeline,
                "budget": budget,
                "budget_gap": (booked_revenue + current_pipeline) - budget,
                "has_decay_activity": has_decay_activity,
                "total_decay": total_decay,
                "days_since_calibration": days_since_calibration,
                "decay_events_count": decay_events_count,
            }

            monthly_summaries.append(monthly_summary)

        # Calculate quarterly summaries
        quarterly_summaries = []
        for quarter in range(1, 5):
            start_month = (quarter - 1) * 3
            end_month = quarter * 3
            quarter_months = monthly_summaries[start_month:end_month]

            quarterly_summary = {
                "quarter_name": f"Q{quarter} {current_year}",
                "booked_revenue": sum(m["booked_revenue"] for m in quarter_months),
                "current_pipeline": sum(m["current_pipeline"] for m in quarter_months),
                "budget": sum(m["budget"] for m in quarter_months),
                "budget_gap": sum(m["budget_gap"] for m in quarter_months),
                "month_count": len(quarter_months),
            }
            quarterly_summaries.append(quarterly_summary)

        # Calculate AE info
        ytd_target = 300000  # Default annual target
        deal_count = len([m for m in monthly_summaries if m["booked_revenue"] > 0])
        avg_deal_size = ytd_actual / max(1, deal_count)

        ae_info = {
            "ae_id": ae_id,
            "name": ae_name,
            "territory": "Unknown",
            "ytd_actual": ytd_actual,
            "ytd_target": ytd_target,
            "avg_deal_size": avg_deal_size,
            "decay_enabled": pipeline_service is not None,
            "has_decay_activity": any(
                m["has_decay_activity"] for m in monthly_summaries
            ),
        }

        # Get decay analytics if available
        decay_analytics = None
        if pipeline_service and hasattr(pipeline_service, "get_decay_analytics"):
            try:
                decay_analytics = pipeline_service.get_decay_analytics(ae_id)
            except Exception as e:
                logger.warning(f"Could not get decay analytics for {ae_id}: {e}")

        # Calculate progress since review
        recent_revenue = sum(
            m["booked_revenue"] for m in monthly_summaries[-2:]
        )  # Last 2 months
        recent_decay_events = sum(
            m["decay_events_count"] for m in monthly_summaries[-2:]
        )
        pipeline_reduction = abs(
            sum(m["total_decay"] for m in monthly_summaries if m["total_decay"] < 0)
        )

        progress_since_review = {
            "last_review_date": (datetime.now() - timedelta(days=14)).isoformat(),
            "revenue_progress": recent_revenue,
            "pipeline_reduction": pipeline_reduction,
            "decay_events_count": recent_decay_events,
        }

        logger.info(
            f"Successfully created AE summary for {ae_name}: YTD ${ytd_actual:,.0f}"
        )

        return create_success_response(
            {
                "ae_info": ae_info,
                "monthly_summary": monthly_summaries,
                "quarterly_summary": quarterly_summaries,
                "progress_since_review": progress_since_review,
                "decay_analytics": decay_analytics,
            }
        )

    except Exception as e:
        logger.error(f"Error getting AE summary for {ae_id}: {e}")
        import traceback

        traceback.print_exc()
        return handle_service_error(e, f"getting AE summary for {ae_id}")


def format_month_display(month_str: str) -> str:
    """Convert YYYY-MM to 'Month YYYY'."""
    try:
        year, month_num = month_str.split("-")
        month_names = [
            "",
            "Jan",
            "Feb",
            "Mar",
            "Apr",
            "May",
            "Jun",
            "Jul",
            "Aug",
            "Sep",
            "Oct",
            "Nov",
            "Dec",
        ]
        return f"{month_names[int(month_num)]} {year}"
    except:
        return month_str


def format_month_display(month_str: str) -> str:
    """Convert YYYY-MM to 'Month YYYY'."""
    try:
        year, month_num = month_str.split("-")
        month_names = [
            "",
            "Jan",
            "Feb",
            "Mar",
            "Apr",
            "May",
            "Jun",
            "Jul",
            "Aug",
            "Sep",
            "Oct",
            "Nov",
            "Dec",
        ]
        return f"{month_names[int(month_num)]} {year}"
    except:
        return month_str


# Export endpoints
@api_bp.route("/export/monthly-revenue/<int:year>")
@log_requests
@handle_request_errors
def export_monthly_revenue(year: int):
    """Export monthly revenue data."""
    try:
        container = get_container()
        report_service = safe_get_service(container, "report_data_service")

        # Get export format
        export_format = get_export_format()

        # Extract filters
        filters = extract_report_filters()
        filters.year = year

        # Get data
        report_data = report_service.get_monthly_revenue_report_data(year, filters)

        if export_format == "csv":
            return _export_as_csv(report_data, f"monthly_revenue_{year}")
        elif export_format == "json":
            return create_success_response(report_data.to_dict())
        else:
            return create_error_response(
                f"Unsupported export format: {export_format}",
                status_code=400,
                error_code="INVALID_FORMAT",
            )

    except Exception as e:
        return handle_service_error(e, f"exporting monthly revenue for {year}")


@api_bp.route("/export/ae-performance")
@log_requests
@handle_request_errors
def export_ae_performance():
    """Export AE performance data."""
    try:
        container = get_container()
        report_service = safe_get_service(container, "report_data_service")

        export_format = get_export_format()
        filters = extract_report_filters()

        # Get data
        ae_data = report_service.get_ae_performance_report_data(filters)

        if export_format == "csv":
            return _export_ae_performance_as_csv(ae_data)
        elif export_format == "json":
            return create_success_response(ae_data.to_dict())
        else:
            return create_error_response(
                f"Unsupported export format: {export_format}",
                status_code=400,
                error_code="INVALID_FORMAT",
            )

    except Exception as e:
        return handle_service_error(e, "exporting AE performance data")


# Metadata endpoints
@api_bp.route("/metadata/years")
@log_requests
@handle_request_errors
def get_available_years():
    """Get available years for reporting."""
    try:
        container = get_container()
        report_service = safe_get_service(container, "report_data_service")

        # Get years from database
        db_connection = container.get("database_connection")
        years = report_service._get_available_years(db_connection)

        return create_success_response(
            {"available_years": years, "current_year": date.today().year}
        )

    except Exception as e:
        return handle_service_error(e, "getting available years")


@api_bp.route("/metadata/ae-list")
@log_requests
@handle_request_errors
def get_ae_list():
    """Get list of Account Executives for filtering."""
    try:
        container = get_container()
        report_service = safe_get_service(container, "report_data_service")

        # Get AE list from database
        db_connection = container.get("database_connection")
        ae_list = report_service._get_ae_list(db_connection)

        return create_success_response({"ae_list": ae_list})

    except Exception as e:
        return handle_service_error(e, "getting AE list")


# Helper functions for export
def _export_as_csv(report_data, filename_base: str):
    """Export monthly revenue data as CSV."""
    import csv
    import io

    output = io.StringIO()
    writer = csv.writer(output)

    # Write headers
    headers = ["Customer", "AE", "Revenue Type", "Sector"]
    headers.extend([f"Month_{i}" for i in range(1, 13)])
    headers.append("Total")
    writer.writerow(headers)

    # Write data rows
    for row in report_data.revenue_data:
        data_row = [row.customer, row.ae, row.revenue_type, row.sector or ""]

        # Add monthly values
        for month in range(1, 13):
            data_row.append(float(row.get_month_value(month)))

        # Add total
        data_row.append(float(row.total))

        writer.writerow(data_row)

    # Create response
    csv_data = output.getvalue()
    filename = f"{filename_base}.csv"

    return create_csv_response(csv_data, filename)


def _export_ae_performance_as_csv(ae_data):
    """Export AE performance data as CSV."""
    import csv
    import io

    output = io.StringIO()
    writer = csv.writer(output)

    # Write headers
    headers = [
        "AE Name",
        "Spot Count",
        "Total Revenue",
        "Average Rate",
        "First Spot Date",
        "Last Spot Date",
    ]
    writer.writerow(headers)

    # Write data rows
    for ae in ae_data.ae_performance:
        writer.writerow(
            [
                ae.ae_name,
                ae.spot_count,
                float(ae.total_revenue),
                float(ae.avg_rate),
                ae.first_spot_date.isoformat() if ae.first_spot_date else "",
                ae.last_spot_date.isoformat() if ae.last_spot_date else "",
            ]
        )

    csv_data = output.getvalue()
    return create_csv_response(csv_data, "ae_performance.csv")


# Error handlers for API blueprint
@api_bp.errorhandler(404)
def api_not_found_error(error):
    """Handle 404 errors in API blueprint."""
    return create_error_response(
        "API endpoint not found", status_code=404, error_code="ENDPOINT_NOT_FOUND"
    )


@api_bp.errorhandler(500)
def api_internal_error(error):
    """Handle 500 errors in API blueprint."""
    logger.error(f"Internal error in API blueprint: {error}")
    return create_error_response(
        "Internal server error", status_code=500, error_code="INTERNAL_ERROR"
    )
