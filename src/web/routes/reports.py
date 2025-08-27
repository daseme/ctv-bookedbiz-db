# src/web/routes/reports.py
"""
Reports blueprint with clean, focused route handlers.
Uses dependency injection and delegates to service layer.
"""
import logging
from flask import Blueprint, render_template, request
from datetime import date

from src.services.container import get_container
from src.models.report_data import ReportFilters
from src.web.utils.request_helpers import (
    extract_report_filters, get_year_parameter, create_json_response,
    create_success_response, create_error_response, handle_service_error,
    safe_get_service, log_requests, handle_request_errors
)
from src.utils.template_formatters import prepare_template_context

logger = logging.getLogger(__name__)

# Create blueprint
reports_bp = Blueprint('reports', __name__, url_prefix='/reports')


@reports_bp.route('/')
@log_requests
def reports_index():
    """Reports index page with links to all reports."""
    return render_template('index.html')


@reports_bp.route('/revenue-dashboard-customer')
@log_requests
@handle_request_errors
def revenue_dashboard_customer():
    """Customer Revenue Dashboard - Interactive monthly revenue analysis."""
    try:
        # Get services
        container = get_container()
        report_service = safe_get_service(container, 'report_data_service')
        
        # Extract parameters with proper defaults
        year = get_year_parameter(default_year=date.today().year)
        
        # Build filters from request parameters
        customer_search = request.args.get('customer_search', '').strip()
        ae_filter = request.args.get('ae_filter', '').strip()
        revenue_type = request.args.get('revenue_type', '').strip()
        revenue_field = request.args.get('revenue_field', 'gross').strip()
        sector = request.args.get('sector', '').strip()
        market = request.args.get('market', '').strip()

        # Convert empty strings and 'all' to None
        filters = ReportFilters(
            year=year,
            customer_search=customer_search if customer_search else None,
            ae_filter=ae_filter if ae_filter and ae_filter != 'all' else None,
            revenue_type=revenue_type if revenue_type and revenue_type != 'all' else None,
            revenue_field=revenue_field if revenue_field in ['gross', 'net'] else 'gross',
            sector=sector if sector and sector != 'all' else None,
            market=market if market and market != 'all' else None
        )
        
        # Get report data
        logger.info(f"Generating customer revenue dashboard for year {year} with filters: {filters.to_dict()}")
        report_data = report_service.get_monthly_revenue_report_data(year, filters)
        
        # Prepare template context
        template_data = {
            'title': "Customer Revenue Dashboard",
            'data': report_data.to_dict()
        }
        
        logger.info(f"Dashboard generated successfully: {report_data.total_customers} customers, "
                   f"{report_data.active_customers} active, ${report_data.total_revenue:,.0f} total revenue")
        
        return render_template('revenue-dashboard-customer.html', **template_data)
        
    except Exception as e:
        logger.error(f"Error generating customer revenue dashboard: {e}", exc_info=True)
        return render_template('error_500.html', 
                             message=f"Error generating customer revenue dashboard: {str(e)}"), 500


@reports_bp.route('/report1')
@log_requests  
@handle_request_errors
def monthly_revenue_summary():
    """Monthly Revenue Summary Report (report1.html)."""
    try:
        container = get_container()
        report_service = safe_get_service(container, 'report_data_service')
        
        # Get basic monthly summary (could be enhanced to use report service)
        year = get_year_parameter(default_year=date.today().year)
        filters = ReportFilters(year=year)
        
        report_data = report_service.get_monthly_revenue_report_data(year, filters)
        
        # For now, use existing template structure
        context = prepare_template_context({
            'title': "Monthly Revenue Summary",
            'data': report_data.to_dict()
        })
        
        return render_template('report1.html', **context)
        
    except Exception as e:
        logger.error(f"Error generating report1: {e}")
        return render_template('error_500.html', 
                             message="Error generating report"), 500


@reports_bp.route('/report2')
@log_requests
@handle_request_errors
def expectation_tracking_report():
    """Management Expectation Tracking Report (report2.html)."""
    try:
        container = get_container()
        report_service = safe_get_service(container, 'report_data_service')
        
        year = get_year_parameter(default_year=date.today().year)
        filters = ReportFilters(year=year)
        
        # Get quarterly and AE performance data
        quarterly_data = report_service.get_quarterly_performance_data(filters)
        ae_data = report_service.get_ae_performance_report_data(filters)
        
        expectation_data = {
            'current_year': quarterly_data.current_year,
            'quarterly_data': [q.to_dict() for q in quarterly_data.quarterly_data],
            'ae_performance': [ae.to_dict() for ae in ae_data.ae_performance]
        }
        
        context = prepare_template_context(expectation_data, {
            'title': "Management Expectation Tracking"
        })
        
        return render_template('report2.html', **context)
        
    except Exception as e:
        logger.error(f"Error generating report2: {e}")
        return render_template('error_500.html', 
                             message="Error generating report"), 500


@reports_bp.route('/report3')
@log_requests
@handle_request_errors
def performance_story_report():
    """Quarterly Performance Story Report (report3.html)."""
    try:
        container = get_container()
        report_service = safe_get_service(container, 'report_data_service')
        
        year = get_year_parameter(default_year=date.today().year)
        filters = ReportFilters(year=year)
        
        # Get performance data
        quarterly_data = report_service.get_quarterly_performance_data(filters)
        ae_data = report_service.get_ae_performance_report_data(filters)
        
        performance_data = {
            **quarterly_data.to_dict(),
            'ae_performance': [ae.to_dict() for ae in ae_data.ae_performance]
        }
        
        context = prepare_template_context(performance_data, {
            'title': "Quarterly Performance Story"
        })
        
        return render_template('report3.html', **context)
        
    except Exception as e:
        logger.error(f"Error generating report3: {e}")
        return render_template('error_500.html', 
                             message="Error generating report"), 500


@reports_bp.route('/report4')
@log_requests
@handle_request_errors
def quarterly_sectors_report():
    """Enhanced Quarterly Performance with Sector Analysis (report4.html)."""
    try:
        container = get_container()
        report_service = safe_get_service(container, 'report_data_service')
        
        year = get_year_parameter(default_year=date.today().year)
        filters = ReportFilters(year=year)
        
        # Get all required data
        quarterly_data = report_service.get_quarterly_performance_data(filters)
        sector_data = report_service.get_sector_performance_data(filters)
        ae_data = report_service.get_ae_performance_report_data(filters)
        
        combined_data = {
            **quarterly_data.to_dict(),
            **sector_data.to_dict(),
            'ae_performance': [ae.to_dict() for ae in ae_data.ae_performance]
        }
        
        context = prepare_template_context(combined_data, {
            'title': "Quarterly Performance with Sector Analysis"
        })
        
        return render_template('report4.html', **context)
        
    except Exception as e:
        logger.error(f"Error generating report4: {e}")
        return render_template('error_500.html', 
                             message="Error generating report"), 500


@reports_bp.route('/pipeline-revenue')
@log_requests
@handle_request_errors
def pipeline_revenue_management():
    """Pipeline Revenue Management main page."""
    try:
        container = get_container()
        pipeline_service = safe_get_service(container, 'pipeline_service')
        
        # Get pipeline data
        session_date = date.today().strftime('%Y-%m-%d')
        session = pipeline_service.get_review_session(session_date)
        ae_list = pipeline_service.get_ae_list()
        
        data = {
            'session': session,
            'ae_list': ae_list, 
            'session_date': session_date
        }
        
        context = prepare_template_context(data, {
            'title': "Pipeline Revenue Management"
        })
        
        return render_template('pipeline_revenue.html', **context)
        
    except Exception as e:
        logger.error(f"Error loading pipeline revenue management: {e}")
        return render_template('error_500.html', 
                             message="Error loading pipeline management"), 500

@reports_bp.route('/language-blocks')
@log_requests
@handle_request_errors
def language_blocks_report():
    """Language Block Performance Report with Nordic design."""
    try:
        template_data = {
            'title': "Language Block Performance Report",
            'description': "Comprehensive analysis of language-specific advertising blocks"
        }
        
        logger.info("Rendering language blocks report template")
        return render_template('language_blocks_report.html', **template_data)
        
    except Exception as e:
        logger.error(f"Error rendering language blocks report: {e}")
        return render_template('error_500.html', 
                             message="Error loading language blocks report"), 500

# Error handlers for this blueprint
@reports_bp.errorhandler(404)
def not_found_error(error):
    """Handle 404 errors in reports blueprint."""
    return render_template('error_404.html'), 404


@reports_bp.errorhandler(500)
def internal_error(error):
    """Handle 500 errors in reports blueprint."""
    logger.error(f"Internal error in reports blueprint: {error}")
    return render_template('error_500.html'), 500