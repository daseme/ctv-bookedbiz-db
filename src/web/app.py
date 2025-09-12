# src/web/app.py
"""
Clean Flask application factory with dependency injection.
All route handlers moved to blueprints, focuses on app configuration.
"""
# src/web/app.py  (only the changed/important parts)

import psutil
import time
from datetime import datetime, timedelta
import logging
from flask import Flask, jsonify
from typing import Optional

from src.services.container import ServiceCreationError
from src.services.factory import initialize_services
from src.config.settings import get_settings
from src.web.blueprints import initialize_blueprints

logger = logging.getLogger(__name__)

def create_app(environment: Optional[str] = None) -> Flask:
    print("🚨 DEBUG: create_app function called!", environment)
    settings = get_settings(environment)

    try:
        initialize_services()
        logger.info("Service container initialized successfully")
    except ServiceCreationError as e:
        logger.error(f"Failed to initialize services: {e}")
        raise

    app = Flask(__name__)

    # Add DATABASE_PATH alias so blueprints can read it
    app.config.update({
        'SECRET_KEY': settings.web.secret_key,
        'DEBUG': settings.web.debug,
        'MAX_CONTENT_LENGTH': settings.web.max_content_length,
        'ENVIRONMENT': settings.environment,
        'PROJECT_ROOT': str(settings.project_root),
        'DB_PATH': settings.database.db_path,
        'DATABASE_PATH': settings.database.db_path,  # <-- compat for customer_normalization
        'DATA_PATH': settings.services.data_path,
    })

    try:
        initialize_blueprints(app)
        logger.info("Blueprints initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize blueprints: {e}")
        raise

    # Register pipeline blueprint (as before)
    try:
        from src.web.routes.pipeline_routes import pipeline_bp
        app.register_blueprint(pipeline_bp)
        logger.info("Pipeline blueprint registered successfully")
    except ImportError as e:
        logger.warning(f"Pipeline blueprint not available: {e}")
    except Exception as e:
        logger.error(f"Failed to register pipeline blueprint: {e}")

    # ✅ Register the customer normalization blueprint *after* app exists
    try:
        from src.web.routes.customer_normalization import customer_norm_bp
        app.register_blueprint(customer_norm_bp)
        logger.info("Customer normalization blueprint registered successfully")
    except Exception as e:
        logger.error(f"Failed to register customer normalization blueprint: {e}")


    # ✅ Register the customer CANON tool *after* app exists
    try:
        from src.web.routes.canon_tools import canon_bp
        app.register_blueprint(canon_bp)
        logger.info("Customer canon blueprint registered successfully")
    except Exception as e:
        logger.error(f"Failed to register customer canon blueprint: {e}")




    logger.info(f"Flask app created for environment: {settings.environment}")
    return app

    
    # Initialize decay system check (non-blocking) - FIXED for newer Flask
    def check_decay_system():
        """Check decay system availability."""
        try:
            from src.services.container import get_container
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
        from src.services.container import get_container
        
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
        from src.web.blueprints import get_blueprint_info
        
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
    
    # System monitoring endpoint for Pi2 control panel
    @app.route('/api/system-stats')
    def system_stats():
        """System monitoring endpoint for control panel integration."""
        try:
            # Get CPU usage (1-second sample)
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # Get memory info
            memory = psutil.virtual_memory()
            
            # Get disk usage for root partition
            disk = psutil.disk_usage('/')
            
            # Get system temperature (Pi-specific)
            try:
                with open('/sys/class/thermal/thermal_zone0/temp', 'r') as f:
                    temp = int(f.read()) / 1000.0  # Convert to Celsius
            except:
                temp = 0
            
            # Get load average
            load_avg = psutil.getloadavg()
            
            # Get uptime
            boot_time = psutil.boot_time()
            uptime_seconds = int(time.time() - boot_time)
            
            # Get network stats
            network = psutil.net_io_counters()
            
            return jsonify({
                "status": "success",
                "timestamp": datetime.now().isoformat(),
                "system": {
                    "cpu_percent": round(cpu_percent, 1),
                    "memory_percent": round(memory.percent, 1),
                    "memory_used_mb": round(memory.used / 1024 / 1024, 1),
                    "memory_total_mb": round(memory.total / 1024 / 1024, 1),
                    "disk_percent": round((disk.used / disk.total) * 100, 1),
                    "disk_used_gb": round(disk.used / 1024 / 1024 / 1024, 1),
                    "disk_total_gb": round(disk.total / 1024 / 1024 / 1024, 1),
                    "temperature": round(temp, 1),
                    "load_average": [round(x, 2) for x in load_avg],
                    "uptime_seconds": uptime_seconds,
                    "uptime_formatted": str(timedelta(seconds=uptime_seconds))
                },
                "network": {
                    "bytes_sent": network.bytes_sent,
                    "bytes_recv": network.bytes_recv,
                    "packets_sent": network.packets_sent,
                    "packets_recv": network.packets_recv
                }
            })
        except Exception as e:
            logger.error(f"System stats error: {e}")
            return jsonify({
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }), 500
    
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