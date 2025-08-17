# src/web/utils/request_helpers.py
"""
Request and response helper utilities for Flask routes.
Provides parameter extraction, validation, and response formatting.
"""
import logging
from typing import Dict, Any, Optional, Union, List
from flask import request, jsonify, Response
from datetime import date, datetime
from decimal import Decimal
import json

from models.report_data import ReportFilters
from utils.template_formatters import serialize_for_javascript

logger = logging.getLogger(__name__)


class RequestValidationError(Exception):
    """Raised when request parameters are invalid."""
    pass


def extract_report_filters() -> ReportFilters:
    """
    Extract and validate report filters from request parameters.
    
    Returns:
        ReportFilters object with validated parameters
        
    Raises:
        RequestValidationError: If parameters are invalid
    """
    try:
        # Extract parameters
        year = request.args.get('year', type=int)
        customer_search = request.args.get('customer_search', '').strip()
        ae_filter = request.args.get('ae_filter', '').strip()
        revenue_type = request.args.get('revenue_type', '').strip()
        sector = request.args.get('sector', '').strip()
        market = request.args.get('market', '').strip()
        
        # Handle empty strings as None
        customer_search = customer_search if customer_search else None
        ae_filter = ae_filter if ae_filter and ae_filter != 'all' else None
        revenue_type = revenue_type if revenue_type and revenue_type != 'all' else None
        sector = sector if sector and sector != 'all' else None
        market = market if market and market != 'all' else None
        
        # Create filters object (will validate in constructor)
        filters = ReportFilters(
            year=year,
            customer_search=customer_search,
            ae_filter=ae_filter,
            revenue_type=revenue_type,
            sector=sector,
            market=market
        )
        
        logger.debug(f"Extracted filters: {filters.to_dict()}")
        return filters
        
    except ValueError as e:
        logger.warning(f"Invalid request parameters: {e}")
        raise RequestValidationError(f"Invalid parameters: {e}") from e
    except Exception as e:
        logger.error(f"Error extracting filters: {e}")
        raise RequestValidationError(f"Parameter extraction failed: {e}") from e


def get_year_parameter(default_year: Optional[int] = None) -> int:
    """
    Get and validate year parameter from request.
    
    Args:
        default_year: Default year if not provided
        
    Returns:
        Validated year
        
    Raises:
        RequestValidationError: If year is invalid
    """
    if default_year is None:
        default_year = date.today().year
    
    year = request.args.get('year', default_year, type=int)
    
    if not isinstance(year, int) or year < 2000 or year > 2100:
        raise RequestValidationError(f"Invalid year: {year}")
    
    return year


def get_pagination_parameters() -> Dict[str, int]:
    """
    Get pagination parameters from request.
    
    Returns:
        Dictionary with 'page' and 'per_page' keys
    """
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    
    # Validate pagination parameters
    page = max(1, page)
    per_page = min(max(1, per_page), 1000)  # Limit to 1000 records per page
    
    return {'page': page, 'per_page': per_page}


def create_json_response(
    data: Any, 
    status_code: int = 200,
    headers: Optional[Dict[str, str]] = None
) -> Response:
    """
    Create standardized JSON response.
    
    Args:
        data: Data to serialize
        status_code: HTTP status code
        headers: Optional additional headers
        
    Returns:
        Flask Response object
    """
    try:
        # Use custom serializer for Decimal and datetime objects
        json_data = serialize_for_javascript(data)
        
        response = Response(
            json_data,
            status=status_code,
            mimetype='application/json',
            headers=headers
        )
        
        return response
        
    except Exception as e:
        logger.error(f"Error creating JSON response: {e}")
        # Return error response
        error_data = json.dumps({'error': 'Serialization failed', 'status': 500})
        return Response(error_data, status=500, mimetype='application/json')


def create_success_response(
    data: Any, 
    message: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None
) -> Response:
    """
    Create standardized success response.
    
    Args:
        data: Response data
        message: Optional success message
        metadata: Optional metadata (pagination, etc.)
        
    Returns:
        JSON response with success structure
    """
    response_data = {
        'success': True,
        'data': data
    }
    
    if message:
        response_data['message'] = message
    
    if metadata:
        response_data['metadata'] = metadata
    
    return create_json_response(response_data)


def create_error_response(
    error_message: str,
    status_code: int = 400,
    error_code: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None
) -> Response:
    """
    Create standardized error response.
    
    Args:
        error_message: Human-readable error message
        status_code: HTTP status code
        error_code: Optional error code for client handling
        details: Optional additional error details
        
    Returns:
        JSON response with error structure
    """
    response_data = {
        'success': False,
        'error': error_message,
        'status': status_code
    }
    
    if error_code:
        response_data['error_code'] = error_code
    
    if details:
        response_data['details'] = details
    
    return create_json_response(response_data, status_code)


def log_request_info():
    """Log request information for debugging."""
    logger.debug(f"Request: {request.method} {request.path}")
    if request.args:
        logger.debug(f"Query params: {dict(request.args)}")
    if request.is_json and request.get_json():
        logger.debug(f"JSON body: {request.get_json()}")


def validate_json_request(required_fields: List[str] = None) -> Dict[str, Any]:
    """
    Validate JSON request body.
    
    Args:
        required_fields: List of required field names
        
    Returns:
        Parsed JSON data
        
    Raises:
        RequestValidationError: If request is invalid
    """
    if not request.is_json:
        raise RequestValidationError("Request must be JSON")
    
    try:
        data = request.get_json()
        if data is None:
            raise RequestValidationError("Request body is empty")
        
        if required_fields:
            missing_fields = [field for field in required_fields if field not in data]
            if missing_fields:
                raise RequestValidationError(f"Missing required fields: {missing_fields}")
        
        return data
        
    except json.JSONDecodeError as e:
        raise RequestValidationError(f"Invalid JSON: {e}") from e


def handle_service_error(error: Exception, operation: str) -> Response:
    """
    Handle service layer errors and convert to appropriate HTTP responses.
    
    Args:
        error: Service error
        operation: Description of operation that failed
        
    Returns:
        Error response
    """
    logger.error(f"Service error during {operation}: {error}")
    
    # Map service errors to HTTP status codes
    if "not found" in str(error).lower():
        return create_error_response(
            f"Resource not found: {operation}",
            status_code=404,
            error_code="NOT_FOUND"
        )
    elif "validation" in str(error).lower():
        return create_error_response(
            f"Validation error: {error}",
            status_code=400,
            error_code="VALIDATION_ERROR"
        )
    elif "permission" in str(error).lower() or "unauthorized" in str(error).lower():
        return create_error_response(
            f"Access denied: {operation}",
            status_code=403,
            error_code="ACCESS_DENIED"
        )
    else:
        # Generic server error
        return create_error_response(
            f"Service error: {operation}",
            status_code=500,
            error_code="SERVICE_ERROR"
        )


def get_export_format() -> str:
    """
    Get export format from request parameters.
    
    Returns:
        Export format ('csv', 'excel', 'json')
    """
    format_param = request.args.get('format', 'csv').lower()
    
    if format_param not in ['csv', 'excel', 'json']:
        format_param = 'csv'
    
    return format_param


def create_csv_response(data: str, filename: str) -> Response:
    """
    Create CSV download response.
    
    Args:
        data: CSV data string
        filename: Download filename
        
    Returns:
        CSV download response
    """
    headers = {
        'Content-Disposition': f'attachment; filename="{filename}"',
        'Content-Type': 'text/csv; charset=utf-8'
    }
    
    return Response(data, headers=headers)


def create_excel_response(data: bytes, filename: str) -> Response:
    """
    Create Excel download response.
    
    Args:
        data: Excel file bytes
        filename: Download filename
        
    Returns:
        Excel download response
    """
    headers = {
        'Content-Disposition': f'attachment; filename="{filename}"',
        'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    }
    
    return Response(data, headers=headers)


def safe_get_service(container, service_name: str):
    """
    Safely get service from container with error handling.
    
    Args:
        container: Service container
        service_name: Name of service to retrieve
        
    Returns:
        Service instance
        
    Raises:
        RequestValidationError: If service is not available
    """
    try:
        service = container.get(service_name)
        if service is None:
            raise RequestValidationError(f"Service '{service_name}' is not available")
        return service
        
    except Exception as e:
        logger.error(f"Failed to get service '{service_name}': {e}")
        raise RequestValidationError(f"Service '{service_name}' is not available") from e


# Decorator for request logging
def log_requests(func):
    """Decorator to log request information."""
    def wrapper(*args, **kwargs):
        log_request_info()
        return func(*args, **kwargs)
    return wrapper


# Decorator for error handling
def handle_request_errors(func):
    """Decorator to handle common request errors."""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except RequestValidationError as e:
            return create_error_response(str(e), 400, "VALIDATION_ERROR")
        except Exception as e:
            logger.error(f"Unexpected error in {func.__name__}: {e}")
            return create_error_response(
                "An unexpected error occurred",
                500,
                "INTERNAL_ERROR"
            )
    return wrapper