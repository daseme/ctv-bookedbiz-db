# src/web/utils/__init__.py
"""
Web utilities for Flask routes and request handling.
"""

from .request_helpers import (
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

__all__ = [
    "extract_report_filters",
    "get_year_parameter",
    "create_json_response",
    "create_success_response",
    "create_error_response",
    "handle_service_error",
    "safe_get_service",
    "log_requests",
    "handle_request_errors",
]
