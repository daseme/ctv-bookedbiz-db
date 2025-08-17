# src/web/utils/request_helpers.py
"""
Request and response helper utilities for Flask routes.
"""
import logging
from typing import Dict, Any, Optional
from flask import request, Response
from datetime import date
import json
from functools import wraps

from models.report_data import ReportFilters
from utils.template_formatters import serialize_for_javascript

logger = logging.getLogger(__name__)


class RequestValidationError(Exception):
    """Raised when request parameters are invalid."""
    pass


def extract_report_filters() -> ReportFilters:
    """Extract and validate report filters from request parameters."""
    try:
        year = request.args.get('year', type=int)
        customer_search = request.args.get('customer_search', '').strip() or None
        ae_filter = request.args.get('ae_filter', '').strip()
        ae_filter = ae_filter if ae_filter and ae_filter != 'all' else None
        revenue_type = request.args.get('revenue_type', '').strip()
        revenue_type = revenue_type if revenue_type and revenue_type != 'all' else None
        revenue_field = request.args.get('revenue_field', 'gross').strip()
        revenue_field = revenue_field if revenue_field in ['gross', 'net'] else 'gross'
        
        return ReportFilters(
            year=year,
            customer_search=customer_search,
            ae_filter=ae_filter,
            revenue_type=revenue_type,
            revenue_field=revenue_field
        )
    except Exception as e:
        logger.error(f"Error extracting filters: {e}")
        raise RequestValidationError(f"Parameter extraction failed: {e}") from e


def get_year_parameter(default_year: Optional[int] = None) -> int:
    """Get and validate year parameter from request."""
    if default_year is None:
        default_year = date.today().year
    
    year = request.args.get('year', default_year, type=int)
    if not isinstance(year, int) or year < 2000 or year > 2100:
        raise RequestValidationError(f"Invalid year: {year}")
    return year


def create_json_response(data: Any, status_code: int = 200) -> Response:
    """Create standardized JSON response."""
    try:
        json_data = serialize_for_javascript(data)
        return Response(json_data, status=status_code, mimetype='application/json')
    except Exception as e:
        logger.error(f"Error creating JSON response: {e}")
        error_data = json.dumps({'error': 'Serialization failed', 'status': 500})
        return Response(error_data, status=500, mimetype='application/json')


def create_success_response(data: Any, message: Optional[str] = None) -> Response:
    """Create standardized success response."""
    response_data = {'success': True, 'data': data}
    if message:
        response_data['message'] = message
    return create_json_response(response_data)


def create_error_response(error_message: str, status_code: int = 400, error_code: Optional[str] = None) -> Response:
    """Create standardized error response."""
    response_data = {'success': False, 'error': error_message, 'status': status_code}
    if error_code:
        response_data['error_code'] = error_code
    return create_json_response(response_data, status_code)


def handle_service_error(error: Exception, operation: str) -> Response:
    """Handle service layer errors."""
    logger.error(f"Service error during {operation}: {error}")
    return create_error_response(f"Service error: {operation}", 500, "SERVICE_ERROR")


def safe_get_service(container, service_name: str):
    """Safely get service from container."""
    try:
        service = container.get(service_name)
        if service is None:
            raise RequestValidationError(f"Service '{service_name}' is not available")
        return service
    except Exception as e:
        logger.error(f"Failed to get service '{service_name}': {e}")
        raise RequestValidationError(f"Service '{service_name}' is not available") from e


def get_export_format() -> str:
    """Get export format from request parameters."""
    format_param = request.args.get('format', 'csv').lower()
    if format_param not in ['csv', 'excel', 'json']:
        format_param = 'csv'
    return format_param


def get_pagination_parameters() -> Dict[str, int]:
    """Get pagination parameters from request."""
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    page = max(1, page)
    per_page = min(max(1, per_page), 1000)
    return {'page': page, 'per_page': per_page}


def create_csv_response(data: str, filename: str) -> Response:
    """Create CSV download response."""
    headers = {
        'Content-Disposition': f'attachment; filename="{filename}"',
        'Content-Type': 'text/csv; charset=utf-8'
    }
    return Response(data, headers=headers)


# Simple decorators without conflicts
def log_requests(func):
    """Decorator to log request information."""
    # Don't use @wraps to avoid conflicts
    def log_wrapper(*args, **kwargs):
        logger.debug(f"Request: {request.method} {request.path}")
        return func(*args, **kwargs)
    # Set unique name
    log_wrapper.__name__ = f"{func.__name__}_logged"
    return log_wrapper


def handle_request_errors(func):
    """Decorator to handle common request errors."""
    # Don't use @wraps to avoid conflicts
    def error_wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except RequestValidationError as e:
            return create_error_response(str(e), 400, "VALIDATION_ERROR")
        except Exception as e:
            logger.error(f"Unexpected error in {func.__name__}: {e}")
            return create_error_response("An unexpected error occurred", 500, "INTERNAL_ERROR")
    # Set unique name
    error_wrapper.__name__ = f"{func.__name__}_error_handled"
    return error_wrapper
