# Create this file: src/web/routes/budget.py
"""
Budget management routes - handles budget CRUD operations and UI
"""
import logging
from flask import Blueprint, render_template, request, jsonify
from datetime import date, datetime
import json

from services.container import get_container
from web.utils.request_helpers import (
    handle_request_errors, log_requests, safe_get_service,
    create_success_response, create_error_response
)

logger = logging.getLogger(__name__)

# Create blueprint
budget_bp = Blueprint('budget', __name__, url_prefix='/budget')


@budget_bp.route('/')
@log_requests
@handle_request_errors
def budget_management():
    """Budget management main page."""
    try:
        container = get_container()
        budget_service = safe_get_service(container, 'budget_service')
        
        current_year = date.today().year
        
        # Get current year budget data
        try:
            company_budgets = budget_service.get_company_budget_totals(current_year)
            ae_budgets = budget_service.get_quarterly_budget_summary(current_year)
            has_current_data = bool(company_budgets)
        except Exception as e:
            logger.warning(f"No budget data found for {current_year}: {e}")
            company_budgets = {}
            ae_budgets = {}
            has_current_data = False
        
        template_data = {
            'current_year': current_year,
            'company_budgets': company_budgets,
            'ae_budgets': ae_budgets,
            'has_current_data': has_current_data,
            'title': 'Budget Management'
        }
        
        return render_template('budget-management-main.html', **template_data)
        
    except Exception as e:
        logger.error(f"Error loading budget management: {e}", exc_info=True)
        return render_template('error_500.html', 
                             message="Error loading budget management"), 500


@budget_bp.route('/api/upload', methods=['POST'])
@log_requests
@handle_request_errors
def upload_budget():
    """API endpoint to upload budget data."""
    try:
        data = request.get_json()
        
        if not data:
            return create_error_response("No data provided", 400)
        
        # Validate required fields
        required_fields = ['year', 'version_name', 'budget_data']
        for field in required_fields:
            if field not in data:
                return create_error_response(f"Missing required field: {field}", 400)
        
        container = get_container()
        budget_service = safe_get_service(container, 'budget_service')
        
        # Upload budget data
        version_id = budget_service.upload_budget_data(
            year=data['year'],
            version_name=data['version_name'],
            description=data.get('description', ''),
            budget_data=data['budget_data'],
            created_by=data.get('created_by', 'Web Interface')
        )
        
        logger.info(f"Budget uploaded successfully: version_id={version_id}, year={data['year']}")
        
        return create_success_response({
            'version_id': version_id,
            'message': 'Budget uploaded successfully'
        })
        
    except Exception as e:
        logger.error(f"Error uploading budget: {e}", exc_info=True)
        return create_error_response(f"Error uploading budget: {str(e)}", 500)


@budget_bp.route('/api/initialize', methods=['POST'])
@log_requests
@handle_request_errors
def initialize_sample_data():
    """Initialize sample budget data for demonstration."""
    try:
        container = get_container()
        budget_service = safe_get_service(container, 'budget_service')
        
        # Initialize sample data
        initialized = budget_service.initialize_sample_data_if_needed()
        
        if initialized:
            return create_success_response({
                'message': 'Sample budget data initialized successfully',
                'initialized': True
            })
        else:
            return create_success_response({
                'message': 'Budget data already exists',
                'initialized': False
            })
        
    except Exception as e:
        logger.error(f"Error initializing sample data: {e}")
        return create_error_response(f"Error initializing sample data: {str(e)}", 500)


# Error handlers for this blueprint
@budget_bp.errorhandler(404)
def not_found_error(error):
    """Handle 404 errors in budget blueprint."""
    return render_template('error_404.html'), 404


@budget_bp.errorhandler(500)
def internal_error(error):
    """Handle 500 errors in budget blueprint."""
    logger.error(f"Internal error in budget blueprint: {error}")
    return render_template('error_500.html'), 500