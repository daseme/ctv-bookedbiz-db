# src/web/app.py
"""
Clean Flask application factory with dependency injection.
All route handlers moved to blueprints, focuses on app configuration.
"""
import logging
from flask import Flask
from typing import Optional

from services.container import ServiceCreationError
from services.factory import initialize_services
from config.settings import get_settings
from web.blueprints import initialize_blueprints

logger = logging.getLogger(__name__)


# src/web/app.py - Replace the blueprint registration section

# src/web/app.py - Replace the before_first_request section

def create_app(environment: Optional[str] = None) -> Flask:
    """
    Flask application factory with clean architecture.
    """
    # Load configuration
    settings = get_settings(environment)
    
    # Initialize service container
    try:
        initialize_services()
        logger.info("Service container initialized successfully")
    except ServiceCreationError as e:
        logger.error(f"Failed to initialize services: {e}")
        raise
    
    # Create Flask app
    app = Flask(__name__)
    
    # Configure Flask app
    app.config.update({
        'SECRET_KEY': settings.web.secret_key,
        'DEBUG': settings.web.debug,
        'MAX_CONTENT_LENGTH': settings.web.max_content_length,
        'ENVIRONMENT': settings.environment,
        'PROJECT_ROOT': str(settings.project_root),
        'DB_PATH': settings.database.db_path,
        'DATA_PATH': settings.services.data_path,
    })
    
    # Initialize blueprints and all related configurations
    try:
        initialize_blueprints(app)
        logger.info("Blueprints initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize blueprints: {e}")
        raise
    
    # Register pipeline blueprint
    try:
        from web.routes.pipeline_routes import pipeline_bp
        app.register_blueprint(pipeline_bp)
        logger.info("Pipeline blueprint registered successfully")
    except ImportError as e:
        logger.warning(f"Pipeline blueprint not available: {e}")
    except Exception as e:
        logger.error(f"Failed to register pipeline blueprint: {e}")
    
    """
    # Register decay API blueprint (if available)
    try:
        from web.routes.pipeline_decay_api import decay_api_bp
        app.register_blueprint(decay_api_bp)
        logger.info("Pipeline decay API blueprint registered successfully")
    except ImportError as e:
        logger.info(f"Pipeline decay API blueprint not available (optional): {e}")
    except Exception as e:
        logger.warning(f"Failed to register pipeline decay API blueprint: {e}")
    """
    
    # Initialize decay system check (non-blocking) - FIXED for newer Flask
    def check_decay_system():
        """Check decay system availability."""
        try:
            from services.container import get_container
            container = get_container()
            pipeline_service = container.get('pipeline_service')
            
            if hasattr(pipeline_service, 'decay_engine') and pipeline_service.decay_engine:
                logger.info("✅ Pipeline decay system is active")
            else:
                logger.info("ℹ️ Pipeline decay system not active (using basic pipeline management)")
                
        except Exception as e:
            logger.info(f"ℹ️ Decay system check failed (continuing with basic features): {e}")
    
    # REPLACE before_first_request with context processor (works in all Flask versions)
    @app.context_processor
    def inject_decay_status():
        """Check decay system on first template render."""
        if not hasattr(app, '_decay_system_checked'):
            check_decay_system()
            app._decay_system_checked = True
        return {}
    
    # Register root route (redirect to reports)
    @app.route('/')
    def index():
        """Root route redirects to reports index."""
        from flask import redirect, url_for
        try:
            return redirect(url_for('reports.reports_index'))
        except:
            try:
                return redirect(url_for('reports.reports_index_logged'))
            except:
                return redirect('/reports/')
    
    # Health check endpoint
    @app.route('/health')
    def health_check():
        """Application health check."""
        from flask import jsonify
        from services.container import get_container
        
        try:
            container = get_container()
            services = container.list_services()
            
            return jsonify({
                'status': 'healthy',
                'environment': settings.environment,
                'services_count': len(services),
                'database_connected': container.has_service('database_connection'),
                'report_service_available': container.has_service('report_data_service'),
                'pipeline_service_available': container.has_service('pipeline_service')
            })
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return jsonify({
                'status': 'unhealthy',
                'error': str(e)
            }), 500
    
    # Application info endpoint
    @app.route('/info')
    def app_info():
        """Application information endpoint."""
        from flask import jsonify
        from web.blueprints import get_blueprint_info
        
        return jsonify({
            'app_name': 'CTV Reporting System',
            'version': '2.0.0',
            'environment': settings.environment,
            'debug': settings.web.debug,
            'blueprints': get_blueprint_info(),
            'features': {
                'monthly_revenue_dashboard': True,
                'ae_performance_tracking': True,
                'quarterly_reporting': True,
                'sector_analysis': True,
                'pipeline_management': True,
                'data_export': True,
                'rest_api': True,
                'pipeline_decay_system': True
            }
        })
    
    logger.info(f"Flask app created for environment: {settings.environment}")
    return app


def create_wsgi_app() -> Flask:
    """
    Create WSGI application for production deployment.
    
    Returns:
        Configured Flask application for WSGI
    """
    return create_app('production')


def create_development_app() -> Flask:
    """
    Create development application with debug features.
    
    Returns:
        Configured Flask application for development
    """
    return create_app('development')


def create_testing_app() -> Flask:
    """
    Create testing application with test configuration.
    
    Returns:
        Configured Flask application for testing
    """
    return create_app('testing')


if __name__ == '__main__':
    # Development server
    app = create_development_app()
    settings = get_settings('development')
    
    logger.info(f"Starting development server on {settings.web.host}:{settings.web.port}")
    app.run(
        debug=settings.web.debug,
        host=settings.web.host,
        port=settings.web.port
    )