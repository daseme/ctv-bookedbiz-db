# src/web/blueprints.py
"""
Enhanced blueprint registration with Pipeline Decay API integration.
"""
import os
import logging
from flask import Flask, request
from typing import Dict, Any

from web.routes.reports import reports_bp
from web.routes.api import api_bp
from web.routes.budget import budget_bp
from web.routes.health import health_bp
from web.routes.language_blocks import language_blocks_bp
from web.routes.pipeline_decay_api import decay_api_bp  # New decay API
from services.container import get_container
from utils.template_formatters import register_template_filters

logger = logging.getLogger(__name__)


def register_blueprints(app: Flask) -> None:
    """
    Register all blueprints including the new Pipeline Decay API.
    
    Args:
        app: Flask application instance
    """
    try:
        # Register core application blueprints

        app.register_blueprint(language_blocks_bp)
        logger.info("Registered language blocks blueprint")

        app.register_blueprint(reports_bp)
        logger.info("Registered reports blueprint")
        
        app.register_blueprint(api_bp)
        logger.info("Registered API blueprint")
        
        app.register_blueprint(budget_bp)
        logger.info("Registered budget blueprint")
        
        # Register health monitoring blueprint
        app.register_blueprint(health_bp)
        logger.info("Registered health monitoring blueprint")
        
        # Register pipeline decay API blueprint
        app.register_blueprint(decay_api_bp)
        logger.info("Registered pipeline decay API blueprint")
        
        # Register template filters
        register_template_filters(app)
        logger.info("Registered template filters")
        
        logger.info("All blueprints registered successfully")
        
    except Exception as e:
        logger.error(f"Error registering blueprints: {e}")
        raise


def configure_blueprint_services(app: Flask) -> None:
    """
    Configure services for blueprints with decay system validation.
    
    Args:
        app: Flask application instance
    """
    try:
        # Get service container
        container = get_container()
        
        # Store container in app config for easy access
        app.config['SERVICE_CONTAINER'] = container
        
        # Verify core services are available with decay system checks
        required_services = [
            'database_connection',
            'report_data_service', 
            'pipeline_service',
            'budget_service'
        ]
        
        service_status = {}
        
        for service_name in required_services:
            try:
                if container.has_service(service_name):
                    service = container.get(service_name)
                    if service is not None:
                        service_status[service_name] = 'healthy'
                        
                        # Special check for pipeline service decay system
                        if service_name == 'pipeline_service':
                            decay_enabled = (hasattr(service, 'decay_engine') and 
                                           service.decay_engine is not None)
                            service_status[f'{service_name}_decay'] = 'enabled' if decay_enabled else 'disabled'
                            
                            if decay_enabled:
                                logger.info("Pipeline decay system detected and enabled")
                            else:
                                logger.warning("Pipeline decay system not available")
                        
                        logger.debug(f"Verified service '{service_name}' is healthy")
                    else:
                        service_status[service_name] = 'available_but_null'
                        logger.warning(f"Service '{service_name}' returned None")
                else:
                    service_status[service_name] = 'not_registered'
                    logger.warning(f"Required service '{service_name}' not registered")
            except Exception as e:
                service_status[service_name] = f'error: {str(e)}'
                logger.error(f"Service '{service_name}' failed validation: {e}")
        
        # Store service status in app config for health checks
        app.config['SERVICE_STATUS'] = service_status
        
        # Check if we have critical failures
        critical_failures = [
            name for name, status in service_status.items() 
            if status.startswith('error:') or status == 'not_registered'
        ]
        
        if critical_failures:
            environment = container.get_config('ENVIRONMENT', 'development')
            if environment.lower() == 'production':
                logger.error(f"Critical service failures in production: {critical_failures}")
                if os.getenv('RAILWAY_ENVIRONMENT') == 'true':
                    print(f"⚠️ Railway: Skipping service validation for: {critical_failures}")
                else:
                    raise RuntimeError(f"Critical services failed in production: {critical_failures}")
            else:
                logger.warning(f"Service failures in {environment}: {critical_failures}")
        
        logger.info("Blueprint services configured with decay system validation")
        
    except Exception as e:
        logger.error(f"Error configuring blueprint services: {e}")
        raise


def register_common_error_handlers(app: Flask) -> None:
    """
    Register common error handlers with enhanced logging for decay API.
    
    Args:
        app: Flask application instance
    """
    from flask import render_template, request, jsonify
    
    @app.errorhandler(404)
    def not_found_error(error):
        """Handle 404 errors globally with enhanced logging."""
        logger.warning(f"404 error: {request.method} {request.path} from {request.remote_addr}")
        
        if (request.path.startswith('/api/') or 
            request.path.startswith('/health/') or
            request.path.startswith('/api/pipeline/decay/')):
            return jsonify({
                'success': False,
                'error': 'Not found',
                'status': 404,
                'error_code': 'NOT_FOUND',
                'path': request.path
            }), 404
        return render_template('error_404.html'), 404
    
    @app.errorhandler(500)
    def internal_error(error):
        """Handle 500 errors globally with decay system context."""
        logger.error(f"Internal server error: {error}", exc_info=True)
        
        # Check if this is decay-related
        is_decay_request = request.path.startswith('/api/pipeline/decay/')
        
        # Try to get service health information
        try:
            container = get_container()
            service_status = app.config.get('SERVICE_STATUS', {})
            unhealthy_services = [
                name for name, status in service_status.items() 
                if not status == 'healthy'
            ]
            
            if unhealthy_services:
                logger.error(f"Error occurred with unhealthy services: {unhealthy_services}")
                
                # Special handling for decay system errors
                if is_decay_request and 'pipeline_service_decay' in service_status:
                    decay_status = service_status['pipeline_service_decay']
                    if decay_status != 'enabled':
                        logger.error(f"Decay API error with decay system {decay_status}")
                        
        except Exception as health_check_error:
            logger.error(f"Could not check service health during error: {health_check_error}")
        
        if (request.path.startswith('/api/') or 
            request.path.startswith('/health/') or
            request.path.startswith('/api/pipeline/decay/')):
            
            error_response = {
                'success': False,
                'error': 'Internal server error',
                'status': 500,
                'error_code': 'INTERNAL_ERROR',
                'path': request.path
            }
            
            # Add decay system context for decay API errors
            if is_decay_request:
                error_response['decay_system'] = 'Check decay system health at /health/pipeline'
            
            return jsonify(error_response), 500
            
        return render_template('error_500.html'), 500
    
    @app.errorhandler(400)
    def bad_request_error(error):
        """Handle 400 errors globally."""
        logger.warning(f"Bad request error: {request.method} {request.path} - {error}")
        
        if (request.path.startswith('/api/') or 
            request.path.startswith('/health/') or
            request.path.startswith('/api/pipeline/decay/')):
            return jsonify({
                'success': False,
                'error': 'Bad request',
                'status': 400,
                'error_code': 'BAD_REQUEST'
            }), 400
        return render_template('error_400.html'), 400
    
    @app.errorhandler(403)
    def forbidden_error(error):
        """Handle 403 errors globally."""
        logger.warning(f"Forbidden error: {request.method} {request.path} from {request.remote_addr}")
        
        if (request.path.startswith('/api/') or 
            request.path.startswith('/health/') or
            request.path.startswith('/api/pipeline/decay/')):
            return jsonify({
                'success': False,
                'error': 'Forbidden',
                'status': 403,
                'error_code': 'FORBIDDEN'
            }), 403
        return render_template('error_403.html'), 403
    
    @app.errorhandler(503)
    def service_unavailable_error(error):
        """Handle 503 errors - service unavailable."""
        logger.error(f"Service unavailable error: {request.method} {request.path}")
        
        if (request.path.startswith('/api/') or 
            request.path.startswith('/health/') or
            request.path.startswith('/api/pipeline/decay/')):
            return jsonify({
                'success': False,
                'error': 'Service temporarily unavailable',
                'status': 503,
                'error_code': 'SERVICE_UNAVAILABLE',
                'message': 'System is experiencing issues. Please try again later.'
            }), 503
        return render_template('error_503.html'), 503
    
    logger.info("Registered enhanced error handlers with decay API support")


def create_blueprint_context_processors(app: Flask) -> None:
    """
    Create context processors for templates with decay system information.
    
    Args:
        app: Flask application instance
    """
    @app.context_processor
    def inject_common_variables():
        """Inject common variables into all templates."""
        from datetime import date
        
        return {
            'current_year': date.today().year,
            'app_name': 'CTV Reports',
            'version': '2.2.0'  # Updated version for decay system
        }
    
    @app.context_processor
    def inject_service_status():
        """Inject enhanced service status information."""
        try:
            container = get_container()
            services = container.list_services()
            service_status = app.config.get('SERVICE_STATUS', {})
            
            healthy_services = sum(1 for status in service_status.values() if status == 'healthy')
            total_services = len(service_status)
            
            # Check decay system status
            decay_enabled = service_status.get('pipeline_service_decay', 'unknown') == 'enabled'
            
            # Determine overall system health
            if healthy_services == total_services:
                system_status = 'healthy'
            elif healthy_services > total_services * 0.5:
                system_status = 'degraded'
            else:
                system_status = 'critical'
            
            return {
                'services_available': len(services),
                'healthy_services': healthy_services,
                'total_services': total_services,
                'service_status': system_status,
                'decay_system_enabled': decay_enabled,
                'system_health': {
                    'status': system_status,
                    'health_check_url': '/health/',
                    'decay_system': decay_enabled,
                    'last_check': 'live'
                }
            }
        except Exception as e:
            logger.warning(f"Error getting service status for templates: {e}")
            return {
                'services_available': 0,
                'healthy_services': 0,
                'total_services': 0,
                'service_status': 'error',
                'decay_system_enabled': False,
                'system_health': {
                    'status': 'error',
                    'health_check_url': '/health/',
                    'decay_system': False,
                    'error': str(e)
                }
            }
    
    @app.context_processor
    def inject_decay_system_info():
        """Inject decay system information for templates."""
        try:
            container = get_container()
            pipeline_service = container.get('pipeline_service')
            
            decay_info = {
                'available': False,
                'status': 'unknown',
                'api_base_url': '/api/pipeline/decay'
            }
            
            if hasattr(pipeline_service, 'decay_engine') and pipeline_service.decay_engine:
                decay_info.update({
                    'available': True,
                    'status': 'enabled',
                    'features': [
                        'real_time_adjustments',
                        'calibration_baselines', 
                        'decay_analytics',
                        'timeline_tracking'
                    ]
                })
            
            return {'decay_system': decay_info}
            
        except Exception as e:
            return {
                'decay_system': {
                    'available': False,
                    'status': 'error',
                    'error': str(e)
                }
            }
    
    logger.info("Created enhanced blueprint context processors with decay system")


def setup_request_logging(app: Flask) -> None:
    """
    Setup enhanced request logging with decay API monitoring.
    
    Args:
        app: Flask application instance
    """
    @app.before_request
    def log_request_info():
        """Log request information with decay system context."""
        # Log basic request info
        logger.debug(f"Request: {request.method} {request.path}")
        if request.args:
            logger.debug(f"Query parameters: {dict(request.args)}")
        
        # For health endpoints, log additional context
        if request.path.startswith('/health/'):
            logger.info(f"Health check requested: {request.path}")
        
        # For decay API endpoints, log decay system requests
        if request.path.startswith('/api/pipeline/decay/'):
            logger.info(f"Decay API request: {request.method} {request.path}")
            if request.json:
                # Log important request data (but not sensitive info)
                safe_data = {
                    k: v for k, v in request.json.items() 
                    if k not in ['webhook_signature', 'auth_token']
                }
                logger.debug(f"Decay API data: {safe_data}")
    
    @app.after_request
    def log_response_info(response):
        """Log response information with enhanced error tracking."""
        logger.debug(f"Response: {response.status_code} for {request.path}")
        
        # Log errors with additional context
        if response.status_code >= 400:
            logger.warning(f"Error response: {response.status_code} for {request.method} {request.path}")
            
            # For decay API errors, add specific context
            if request.path.startswith('/api/pipeline/decay/'):
                logger.error(f"Decay API error: {response.status_code} for {request.path}")
            
            # For 5xx errors, check if services are healthy
            if response.status_code >= 500:
                try:
                    service_status = app.config.get('SERVICE_STATUS', {})
                    unhealthy = [name for name, status in service_status.items() if status != 'healthy']
                    if unhealthy:
                        logger.error(f"Error occurred with unhealthy services: {unhealthy}")
                except Exception:
                    pass  # Don't let health checking interfere with response
        
        return response
    
    logger.info("Setup enhanced request logging with decay API monitoring")


def configure_security_headers(app: Flask) -> None:
    """
    Configure security headers with enhanced protection for decay API.
    
    Args:
        app: Flask application instance
    """
    @app.after_request
    def add_security_headers(response):
        """Add enhanced security headers to responses."""
        # Only add security headers to HTML responses
        if response.content_type and 'text/html' in response.content_type:
            response.headers['X-Content-Type-Options'] = 'nosniff'
            response.headers['X-Frame-Options'] = 'DENY'
            response.headers['X-XSS-Protection'] = '1; mode=block'
            response.headers['Referrer-Policy'] = 'strict-origin-when-cross-origin'
            
            # Add CSP for enhanced security
            csp = "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'"
            response.headers['Content-Security-Policy'] = csp
            
        # Add CORS headers for API and health endpoints
        if (request.path.startswith('/api/') or 
            request.path.startswith('/health/') or
            request.path.startswith('/api/pipeline/decay/')):
            response.headers['Access-Control-Allow-Origin'] = '*'
            response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
            response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization, X-Emergency-Token, X-Webhook-Signature'
            
        # Add health check headers for monitoring
        if request.path.startswith('/health/'):
            response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
            response.headers['Pragma'] = 'no-cache'
            response.headers['Expires'] = '0'
        
        # Add decay API specific headers
        if request.path.startswith('/api/pipeline/decay/'):
            response.headers['X-Decay-System'] = 'enabled'
            response.headers['Cache-Control'] = 'no-cache'  # Decay data should always be fresh
            
        return response
    
    logger.info("Configured enhanced security headers with decay API support")


def initialize_blueprints(app: Flask) -> None:
    """
    Initialize all blueprint-related configurations with decay system support.
    
    Args:
        app: Flask application instance
    """
    try:
        # Configure services first with enhanced validation
        configure_blueprint_services(app)
        
        # Register blueprints including decay API
        register_blueprints(app)
        
        # Register enhanced error handlers
        register_common_error_handlers(app)
        
        # Create enhanced context processors
        create_blueprint_context_processors(app)
        
        # Setup enhanced request logging
        if app.config.get('DEBUG', False):
            setup_request_logging(app)
        
        # Configure enhanced security headers
        configure_security_headers(app)
        
        logger.info("Blueprint initialization completed successfully with decay system support")
        
    except Exception as e:
        logger.error(f"Error initializing blueprints: {e}")
        raise


def get_blueprint_info() -> Dict[str, Any]:
    """
    Get enhanced information about registered blueprints including decay API.
    
    Returns:
        Dictionary with blueprint information including decay system
    """
    return {
        'blueprints': [
            {
                'name': 'reports',
                'url_prefix': '/reports',
                'description': 'Report generation and display',
                'status': 'active'
            },
            {
                'name': 'api',
                'url_prefix': '/api',
                'description': 'REST API endpoints',
                'status': 'active'
            },
            {
                'name': 'budget',
                'url_prefix': '/budget',
                'description': 'Budget management interface',
                'status': 'active'
            },
            {
                'name': 'health',
                'url_prefix': '/health',
                'description': 'System health monitoring and emergency repair',
                'status': 'active',
                'endpoints': [
                    '/health/ - System health overview',
                    '/health/pipeline - Pipeline service health',
                    '/health/budget - Budget service health',
                    '/health/database - Database connection health',
                    '/health/consistency/validate - Data consistency check',
                    '/health/consistency/repair - Data consistency repair',
                    '/health/emergency/repair - Emergency system repair',
                    '/health/metrics - System performance metrics'
                ]
            },
            {
                'name': 'decay_api',
                'url_prefix': '/api/pipeline/decay',
                'description': 'Pipeline decay system API for real-time adjustments',
                'status': 'active',
                'endpoints': [
                    '/api/pipeline/decay/revenue/booked - Apply revenue booking',
                    '/api/pipeline/decay/revenue/removed - Apply revenue removal',
                    '/api/pipeline/decay/calibration - Set calibration baseline',
                    '/api/pipeline/decay/summary/<ae_id>/<month> - Get decay summary',
                    '/api/pipeline/decay/timeline/<ae_id>/<month> - Get decay timeline',
                    '/api/pipeline/decay/analytics/<ae_id> - Get decay analytics',
                    '/api/pipeline/decay/ae/<ae_id>/summary - Get AE summary with decay',
                    '/api/pipeline/decay/webhook/revenue-change - Webhook for external systems',
                    '/api/pipeline/decay/bulk/calibration - Bulk calibration endpoint',
                    '/api/pipeline/decay/system/status - Decay system status',
                    '/api/pipeline/decay/system/cleanup - Clean old decay events',
                    '/api/pipeline/decay/export/<ae_id> - Export decay data'
                ]
            }
        ],
        'total_blueprints': 5,
        'features': {
            'health_monitoring': True,
            'emergency_repair': True,
            'data_consistency_checks': True,
            'enhanced_error_handling': True,
            'security_headers': True,
            'request_logging': True,
            'pipeline_decay_system': True,  # New feature
            'real_time_adjustments': True,  # New feature
            'decay_analytics': True,        # New feature
            'webhook_integration': True     # New feature
        }
    }