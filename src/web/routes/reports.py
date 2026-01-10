# src/web/routes/reports.py
"""
Reports blueprint with clean, focused route handlers.
Uses dependency injection and delegates to service layer.
"""

import logging
from flask import Blueprint, render_template, request, jsonify, Response
import io
import csv
from datetime import date
from src.services.container import get_container
from src.models.report_data import ReportFilters
from src.services.report_data_service import YearRange
from src.services.management_performance_service import ManagementPerformanceService
from src.services.ae_dashboard_service import AEDashboardService
from src.web.utils.request_helpers import (
    extract_report_filters,
    get_year_parameter,
    create_json_response,
    create_success_response,
    create_error_response,
    handle_service_error,
    safe_get_service,
    log_requests,
    handle_request_errors,
)
from src.utils.template_formatters import prepare_template_context

logger = logging.getLogger(__name__)

# Create blueprint
reports_bp = Blueprint("reports", __name__, url_prefix="/reports")


@reports_bp.route("/")
@log_requests
def reports_index():
    """Reports index page with links to all reports."""
    return render_template("index.html")


@reports_bp.route("/revenue-dashboard-customer")
@log_requests
@handle_request_errors
def revenue_dashboard_customer():
    """Customer Revenue Dashboard - Interactive monthly revenue analysis."""
    try:
        # Get services
        container = get_container()
        report_service = safe_get_service(container, "report_data_service")

        # Extract parameters with proper defaults
        year = get_year_parameter(default_year=date.today().year)

        # Build filters from request parameters
        customer_search = request.args.get("customer_search", "").strip()
        ae_filter = request.args.get("ae_filter", "").strip()
        revenue_type = request.args.get("revenue_type", "").strip()
        revenue_field = request.args.get("revenue_field", "gross").strip()
        sector = request.args.get("sector", "").strip()
        market = request.args.get("market", "").strip()

        # Convert empty strings and 'all' to None
        filters = ReportFilters(
            year=year,
            customer_search=customer_search if customer_search else None,
            ae_filter=ae_filter if ae_filter and ae_filter != "all" else None,
            revenue_type=revenue_type
            if revenue_type and revenue_type != "all"
            else None,
            revenue_field=revenue_field
            if revenue_field in ["gross", "net"]
            else "gross",
            sector=sector if sector and sector != "all" else None,
            market=market if market and market != "all" else None,
        )

        # Get report data
        logger.info(
            f"Generating customer revenue dashboard for year {year} with filters: {filters.to_dict()}"
        )
        report_data = report_service.get_monthly_revenue_report_data(year, filters)

        # DEBUG SECTION - Using WARNING level so it shows in logs
        logger.warning("=" * 60)
        logger.warning("BVK DATA PIPELINE DEBUG START")
        logger.warning("=" * 60)
        logger.warning(f"Total customers in report: {len(report_data.revenue_data)}")

        # Create year range for debug queries
        year_range = YearRange.from_year(year)

        # Debug SQL query generation
        if hasattr(report_service.repository, "build_debug_query"):
            try:
                query, params = report_service.repository.build_debug_query(
                    year_range, filters
                )
                logger.warning("PYTHON-GENERATED SQL:")
                logger.warning(query)
                logger.warning(f"PARAMETERS: {params}")
            except Exception as e:
                logger.error(f"Error building debug query: {e}")
        else:
            logger.warning("No build_debug_query method found")

        logger.warning("=" * 60)
        logger.warning("BVK DEBUG END")
        logger.warning("=" * 60)

        # Create year range for debug queries
        year_range = YearRange.from_year(year)

        # Debug SQL query if method exists
        if hasattr(report_service.repository, "build_debug_query"):
            try:
                query, params = report_service.repository.build_debug_query(
                    year_range, filters
                )
                logger.info(f"PYTHON-GENERATED SQL: {query}")
                logger.info(f"PARAMETERS: {params}")
            except Exception as e:
                logger.error(f"Error building debug query: {e}")
        else:
            logger.info("No build_debug_query method found")

        # Get raw data for debugging
        try:
            raw_data = report_service.repository.get_customer_monthly_data(
                year_range, filters
            )

            # Filter for BVK September data
            bvk_september = [
                row
                for row in raw_data
                if row.get("month") == "09" and "bvk" in row.get("customer", "").lower()
            ]

            logger.info(f"RAW DATABASE QUERY RESULTS")
            logger.info(f"September BVK rows: {len(bvk_september)}")

            total_raw = 0
            for i, row in enumerate(bvk_september, 1):
                gross = row.get("gross_revenue", 0)
                total_raw += gross
                logger.info(
                    f"Row {i}: {row.get('customer')} | {row.get('revenue_type')} | ${gross}"
                )

            logger.info(f"Raw total: ${total_raw}")
            logger.info(f"Expected: $4,107.35")
            logger.info(f"Difference: ${4107.35 - total_raw}")

        except Exception as e:
            logger.error(f"Error in raw data debug: {e}")

        # Check processed data for BVK customers
        bvk_count = 0
        for customer in report_data.revenue_data:
            if "BVK" in customer.customer.upper():
                bvk_count += 1
                logger.info(f"BVK Customer #{bvk_count}:")
                logger.info(f"   Name: '{customer.customer}'")
                logger.info(f"   AE: '{customer.ae}'")
                logger.info(f"   Customer ID: '{customer.customer_id}'")

                if hasattr(customer, "to_dict"):
                    data = customer.to_dict()
                    sep_keys = [k for k in data.keys() if "sep" in k.lower()]
                    logger.info(f"   September keys: {sep_keys}")
                    for key in sep_keys:
                        logger.info(f"   {key}: {data[key]}")

        logger.info(f"Total BVK customers found: {bvk_count}")
        logger.info("BVK DEBUG END")
        logger.info("=" * 50)

        # Prepare template context
        template_data = {
            "title": "Customer Revenue Dashboard",
            "data": report_data.to_dict(),
        }

        logger.info(
            f"Dashboard generated successfully: {report_data.total_customers} customers, "
            f"{report_data.active_customers} active, ${report_data.total_revenue:,.0f} total revenue"
        )

        return render_template("revenue-dashboard-customer.html", **template_data)

    except Exception as e:
        logger.error(f"Error generating customer revenue dashboard: {e}", exc_info=True)
        return render_template(
            "error_500.html",
            message=f"Error generating customer revenue dashboard: {str(e)}",
        ), 500


@reports_bp.route("/customer-sector-manager")
def customer_sector_manager():
    """Customer and Sector Management Tool"""
    try:
        return render_template("customer_sector_manager.html")
    except Exception as e:
        logger.error(f"Error rendering customer sector manager: {e}")
        return render_template("error_500.html"), 500

# Add this to src/web/routes/reports.py

@reports_bp.route("/ae-dashboard")
@log_requests
@handle_request_errors
def ae_dashboard():
    """AE Account Management Dashboard - YoY Performance Analysis."""
    try:
        # Get services
        container = get_container()
        ae_service = safe_get_service(container, "ae_dashboard_service")
        
        # Extract parameters
        year = get_year_parameter(default_year=date.today().year)
        ae_filter = request.args.get("ae_filter", "").strip()
        
        # Convert "everyone" to None for service
        if ae_filter == "everyone" or not ae_filter:
            ae_filter = None
        
        # Get dashboard data
        logger.info(f"Generating AE dashboard for year {year}, AE filter: {ae_filter or 'Everyone'}")
        dashboard_data = ae_service.get_dashboard_data(year, ae_filter)
        
        # Prepare template context
        template_data = {
            "title": "Account Management Dashboard",
            "data": dashboard_data.to_dict(),
        }
        
        logger.info(
            f"AE Dashboard generated: {len(dashboard_data.customers)} customers, "
            f"${dashboard_data.total_ytd_2024:,.0f} total revenue"
        )
        
        return render_template("ae-dashboard.html", **template_data)
        
    except Exception as e:
        logger.error(f"Error generating AE dashboard: {e}", exc_info=True)
        return render_template(
            "error_500.html",
            message=f"Error generating AE dashboard: {str(e)}",
        ), 500

# ============================================================================
# Management Performance Report
# ============================================================================

@reports_bp.route('/management-performance')
@reports_bp.route('/management-performance/<int:year>')
def management_performance(year: int = None):
    """
    Management Performance Report - Company and entity quarterly performance.
    """
    from datetime import date
    from src.services.container import get_container
    
    container = get_container()
    service = container.get('management_performance_service')
    
    if year is None:
        year = date.today().year
    
    # Get pacing mode from query param (default to budget)
    pacing_mode = request.args.get('pacing', 'budget')
    if pacing_mode not in ('budget', 'forecast'):
        pacing_mode = 'budget'
    
    report_data = service.get_management_report(year, pacing_mode)
    
    return render_template(
        'management-performance.html',
        report=report_data
    )


@reports_bp.route('/management-performance/csv/<int:year>')
def management_performance_csv(year: int):
    """Export management performance data as CSV."""
    import csv
    import io
    from src.services.container import get_container
    
    container = get_container()
    service = container.get('management_performance_service')
    
    report_data = service.get_management_report(year)
    
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Header
    writer.writerow(['Entity', 'Quarter', 'Booked', 'Budget', 'Pacing', 'Budget %', 'YoY %'])
    
    # Company totals
    for q in report_data.company.quarterly:
        yoy = f"{q.yoy_change_pct:.1f}%" if q.yoy_change_pct is not None else "New"
        writer.writerow([
            'COMPANY TOTAL', q.quarter_label, 
            float(q.booked), float(q.budget), float(q.pacing),
            f"{q.budget_pacing_pct:.1f}%", yoy
        ])
    
    # Entity data
    for entity in report_data.entities:
        for q in entity.quarterly:
            yoy = f"{q.yoy_change_pct:.1f}%" if q.yoy_change_pct is not None else "New"
            writer.writerow([
                entity.entity_name, q.quarter_label,
                float(q.booked), float(q.budget), float(q.pacing),
                f"{q.budget_pacing_pct:.1f}%", yoy
            ])
    
    output.seek(0)
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename=management_performance_{year}.csv'}
    )

@reports_bp.route("/report1")
@log_requests
@handle_request_errors
def monthly_revenue_summary():
    """Monthly Revenue Summary Report (report1.html)."""
    try:
        container = get_container()
        report_service = safe_get_service(container, "report_data_service")

        # Get basic monthly summary (could be enhanced to use report service)
        year = get_year_parameter(default_year=date.today().year)
        filters = ReportFilters(year=year)

        report_data = report_service.get_monthly_revenue_report_data(year, filters)

        # For now, use existing template structure
        context = prepare_template_context(
            {"title": "Monthly Revenue Summary", "data": report_data.to_dict()}
        )

        return render_template("report1.html", **context)

    except Exception as e:
        logger.error(f"Error generating report1: {e}")
        return render_template("error_500.html", message="Error generating report"), 500


@reports_bp.route("/report2")
@log_requests
@handle_request_errors
def expectation_tracking_report():
    """Management Expectation Tracking Report (report2.html)."""
    try:
        container = get_container()
        report_service = safe_get_service(container, "report_data_service")

        year = get_year_parameter(default_year=date.today().year)
        filters = ReportFilters(year=year)

        # Get quarterly and AE performance data
        quarterly_data = report_service.get_quarterly_performance_data(filters)
        ae_data = report_service.get_ae_performance_report_data(filters)

        expectation_data = {
            "current_year": quarterly_data.current_year,
            "quarterly_data": [q.to_dict() for q in quarterly_data.quarterly_data],
            "ae_performance": [ae.to_dict() for ae in ae_data.ae_performance],
        }

        context = prepare_template_context(
            expectation_data, {"title": "Management Expectation Tracking"}
        )

        return render_template("report2.html", **context)

    except Exception as e:
        logger.error(f"Error generating report2: {e}")
        return render_template("error_500.html", message="Error generating report"), 500


@reports_bp.route("/report3")
@log_requests
@handle_request_errors
def performance_story_report():
    """Quarterly Performance Story Report (report3.html)."""
    try:
        container = get_container()
        report_service = safe_get_service(container, "report_data_service")

        year = get_year_parameter(default_year=date.today().year)
        filters = ReportFilters(year=year)

        # Get performance data
        quarterly_data = report_service.get_quarterly_performance_data(filters)
        ae_data = report_service.get_ae_performance_report_data(filters)

        performance_data = {
            **quarterly_data.to_dict(),
            "ae_performance": [ae.to_dict() for ae in ae_data.ae_performance],
        }

        context = prepare_template_context(
            performance_data, {"title": "Quarterly Performance Story"}
        )

        return render_template("report3.html", **context)

    except Exception as e:
        logger.error(f"Error generating report3: {e}")
        return render_template("error_500.html", message="Error generating report"), 500



@reports_bp.route("/report4")
@log_requests
@handle_request_errors
def quarterly_sectors_report():
    """Enhanced Quarterly Performance with Sector Analysis (sector-analysis.html)."""
    try:
        container = get_container()
        report_service = safe_get_service(container, "report_data_service")
        year = get_year_parameter(default_year=date.today().year)
        filters = ReportFilters(year=year)

        # Get sector data
        sector_data = report_service.get_sector_performance_data(filters)
        
        # Get available years for dropdown
        available_years = report_service.repository.get_available_years()

        return render_template(
            "sector-analysis.html",
            title="Quarterly Performance with Sector Analysis",
            data=sector_data,
            selected_year=year,
            available_years=available_years
        )

    except Exception as e:
        logger.error(f"Error generating report4: {e}", exc_info=True)
        return render_template("error_500.html", message=f"Error generating report: {str(e)}"), 500

@reports_bp.route("/market-analysis")
@log_requests
@handle_request_errors
def market_analysis_report():
    """Market Analysis Report - Language performance by market."""
    try:
        container = get_container()
        service = safe_get_service(container, "market_analysis_service")

        year = request.args.get("year", str(date.today().year))
        
        logger.info(f"Generating market analysis report for year {year}")
        data = service.get_market_analysis_data(year)

        return render_template(
            "market-analysis.html",
            title="Market Analysis",
            data=data.to_dict(),
        )

    except Exception as e:
        logger.error(f"Error generating market analysis: {e}", exc_info=True)
        return render_template(
            "error_500.html",
            message=f"Error generating market analysis: {str(e)}",
        ), 500


@reports_bp.route("/market-analysis/export/<report_type>")
@log_requests
@handle_request_errors
def market_analysis_export(report_type: str):
    """Export market analysis data to CSV."""
    try:
        container = get_container()
        service = safe_get_service(container, "market_analysis_service")

        year = request.args.get("year", str(date.today().year))
        
        logger.info(f"Exporting market analysis {report_type} for year {year}")
        
        data = service.get_csv_data(year, report_type)
        
        if not data:
            return "No data available", 404

        # Create CSV
        output = io.StringIO()
        if data:
            writer = csv.DictWriter(output, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)

        response = Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename=market_analysis_{report_type}_{year}.csv"
            }
        )
        return response

    except Exception as e:
        logger.error(f"Error exporting market analysis: {e}", exc_info=True)
        return f"Export error: {str(e)}", 500


@reports_bp.route("/language-blocks")
@log_requests
@handle_request_errors
def language_blocks_report():
    """Language Block Performance Report with Nordic design."""
    try:
        template_data = {
            "title": "Language Block Performance Report",
            "description": "Comprehensive analysis of language-specific advertising blocks",
        }

        logger.info("Rendering language blocks report template")
        return render_template("language_blocks_report.html", **template_data)

    except Exception as e:
        logger.error(f"Error rendering language blocks report: {e}")
        return render_template(
            "error_500.html", message="Error loading language blocks report"
        ), 500


# Error handlers for this blueprint
@reports_bp.errorhandler(404)
def not_found_error(error):
    """Handle 404 errors in reports blueprint."""
    return render_template("error_404.html"), 404


@reports_bp.errorhandler(500)
def internal_error(error):
    """Handle 500 errors in reports blueprint."""
    logger.error(f"Internal error in reports blueprint: {error}")
    return render_template("error_500.html"), 500
