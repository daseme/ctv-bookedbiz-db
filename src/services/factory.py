# src/services/factory.py
"""
Enhanced Service factory functions with critical fixes applied.
Handles dependency injection with data consistency and concurrency controls.
"""

import os
import logging
from typing import Optional
from enum import Enum

from .container import get_container, ServiceCreationError
from .pipeline_service import DataSourceType  # Import from pipeline service

logger = logging.getLogger(__name__)


########## debug code ############
# Add this to the TOP of your src/services/factory.py file

import traceback
import os


def debug_container_registration(container):
    """Debug what services are actually registered"""
    print("\n" + "=" * 60)
    print("🔍 CONTAINER REGISTRATION DEBUG")
    print("=" * 60)

    # Check container type and attributes
    print(f"📦 Container type: {type(container).__name__}")
    print(f"📦 Container module: {type(container).__module__}")

    # Check what services are registered
    if hasattr(container, "_services"):
        services = container._services
        print(f"📋 Total services registered: {len(services)}")
        if services:
            print("📋 Registered services:")
            for name in sorted(services.keys()):
                service_info = services[name]
                print(f"   ✅ {name} -> {type(service_info)}")
        else:
            print("❌ NO SERVICES REGISTERED!")
    else:
        print("❌ Container has no '_services' attribute")
        print(
            f"📋 Container attributes: {[attr for attr in dir(container) if not attr.startswith('_')]}"
        )

    # Check for report_data_service specifically
    target_service = "report_data_service"
    try:
        service = container.get(target_service)
        print(f"✅ {target_service} retrieval test: SUCCESS -> {type(service)}")
    except Exception as e:
        print(f"❌ {target_service} retrieval test: FAILED -> {e}")

    print("=" * 60 + "\n")

# Add these functions to src/services/factory.py

def create_ae_dashboard_service():
    """Create AEDashboardService with enhanced error handling."""
    try:
        # Try to import AEDashboardService
        try:
            from .ae_dashboard_service import AEDashboardService
        except ImportError:
            logger.warning("AEDashboardService not available - using mock")

            # Return a mock for testing
            class MockAEDashboardService:
                def __init__(self, db_connection=None):
                    self.db_connection = db_connection

                def get_dashboard_data(self, year, ae_filter=None):
                    return {
                        "mock": True,
                        "selected_year": year,
                        "selected_ae": ae_filter,
                        "customers": [],
                        "sectors": [],
                        "total_ytd_2024": 0,
                        "total_ytd_2023": 0,
                        "ae_list": [],
                        "sector_list": []
                    }

            return MockAEDashboardService()

        container = get_container()
        
        # Get database connection
        try:
            db_connection = container.get("database_connection")
            logger.debug("Creating AEDashboardService with database connection")
        except Exception as e:
            logger.warning(f"Database connection issue for AE dashboard service: {e}")
            db_connection = None

        # Create service
        service = AEDashboardService(db_connection)

        # Validate AE dashboard service health
        _validate_ae_dashboard_service_health(service)

        return service

    except ImportError as e:
        logger.error(f"Failed to import AEDashboardService: {e}")
        raise ServiceCreationError(f"Could not import AEDashboardService: {e}") from e
    except Exception as e:
        logger.error(f"Failed to create AE dashboard service: {e}")
        raise ServiceCreationError(f"AE dashboard service creation failed: {e}") from e


def _validate_ae_dashboard_service_health(service) -> None:
    """
    Validate that the AE dashboard service is healthy and properly configured.

    Args:
        service: AEDashboardService instance to validate

    Raises:
        ServiceCreationError: If service health validation fails
    """
    try:
        # Test database connection
        container = get_container()

        # Try to get database connection
        try:
            db_connection = container.get("database_connection")
            if not db_connection:
                logger.warning("AE dashboard service has no database connection")
        except Exception as e:
            logger.warning(f"Database connection issue for AE dashboard service: {e}")
            # In development, this might be OK with mock data
            environment = container.get_config("ENVIRONMENT", "development")
            if environment.lower() == "production":
                raise ServiceCreationError(
                    "AE dashboard service requires database connection in production"
                )

        logger.info("AE dashboard service health validation passed")

    except Exception as e:
        logger.error(f"AE dashboard service health validation failed: {e}")
        raise ServiceCreationError(f"AE dashboard service health check failed: {e}") from e


def configure_container_from_environment():
    """Configure container with environment-specific settings and enhanced validation."""
    container = get_container()

    # Determine project root
    project_root = os.environ.get("PROJECT_ROOT")
    if project_root is None:
        # Default to two levels up from this file
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))

    # Validate project root exists
    if not os.path.exists(project_root):
        logger.warning(f"Project root does not exist: {project_root}")
        # Try to create it or find alternative
        try:
            os.makedirs(project_root, exist_ok=True)
        except Exception as e:
            logger.error(f"Cannot create project root: {e}")

    # Build configuration with validation
    config = {
        "PROJECT_ROOT": project_root,
        "DB_PATH": os.environ.get(
            "DB_PATH", os.path.join(project_root, "data/database/production.db")
        ),
        "DATA_PATH": os.environ.get(
            "DATA_PATH", os.path.join(project_root, "data/processed")
        ),
        "ENVIRONMENT": os.environ.get("FLASK_ENV", "development"),
        "DEBUG": os.environ.get("DEBUG", "false").lower() == "true",
        "LOG_LEVEL": os.environ.get("LOG_LEVEL", "INFO"),
        "CACHE_ENABLED": os.environ.get("CACHE_ENABLED", "true").lower() == "true",
        "CACHE_TTL": int(os.environ.get("CACHE_TTL", "300")),
        # Enhanced configuration for critical fixes
        "CONCURRENCY_ENABLED": os.environ.get("CONCURRENCY_ENABLED", "true").lower()
        == "true",
        "CONSISTENCY_CHECK_ENABLED": os.environ.get(
            "CONSISTENCY_CHECK_ENABLED", "true"
        ).lower()
        == "true",
        "AUTO_REPAIR_ENABLED": os.environ.get("AUTO_REPAIR_ENABLED", "true").lower()
        == "true",
        "EMERGENCY_MODE": os.environ.get("EMERGENCY_MODE", "false").lower() == "true",
    }

    # Validate critical paths
    data_path = config["DATA_PATH"]
    if not os.path.exists(data_path):
        logger.info(f"Creating data directory: {data_path}")
        try:
            os.makedirs(data_path, exist_ok=True)
        except Exception as e:
            logger.error(f"Cannot create data directory: {e}")
            # Use temp directory as fallback
            import tempfile

            config["DATA_PATH"] = tempfile.mkdtemp()
            logger.warning(f"Using temporary data directory: {config['DATA_PATH']}")

    # Validate database path directory
    db_path = config["DB_PATH"]
    db_dir = os.path.dirname(db_path)
    if not os.path.exists(db_dir):
        logger.info(f"Creating database directory: {db_dir}")
        try:
            os.makedirs(db_dir, exist_ok=True)
        except Exception as e:
            logger.error(f"Cannot create database directory: {e}")

    container.set_config(config)
    logger.info(f"Configured container for environment: {config['ENVIRONMENT']}")
    logger.debug(f"Container configuration: {config}")


def initialize_services():
    """Initialize services using real service factory functions with debugging"""
    print("🏭 FACTORY: Starting service initialization...")

    try:
        # Configure container from environment first
        print("📋 Configuring container from environment...")
        configure_container_from_environment()

        # Get container
        container = get_container()
        print(f"📦 Using container: {type(container).__name__}")

        # Register all required services
        print("🔧 Registering database_connection...")
        container.register_singleton("database_connection", create_database_connection)
        print("✅ database_connection registered")

        print("🔧 Registering report_data_service...")
        container.register_singleton("report_data_service", create_report_data_service)
        print("✅ report_data_service registered")

        print("🔧 Registering budget_service...")
        container.register_singleton("budget_service", create_budget_service)
        print("✅ budget_service registered")

        print("🔧 Registering ae_dashboard_service...")
        container.register_singleton("ae_dashboard_service", create_ae_dashboard_service)
        print("✅ ae_dashboard_service registered")

        print("🔧 Registering planning_service...")
        container.register_singleton("planning_service", create_planning_service)
        print("✅ planning_service registered")

        # List what got registered
        services = container.list_services()
        print(f"📋 Final registered services: {services}")

        return container

    except Exception as e:
        print(f"💥 Service initialization failed: {e}")
        import traceback
        traceback.print_exc()
        raise

def register_default_services():
    """Register all default services with the container with enhanced error handling."""
    container = get_container()

    try:
        # Core infrastructure services
        container.register_singleton("database_connection", create_database_connection)
        logger.debug("Registered database_connection service")

        # Enhanced business services with critical fixes
        container.register_singleton("pipeline_service", create_pipeline_service)
        logger.debug("Registered enhanced pipeline_service")

        container.register_singleton("budget_service", create_budget_service)
        logger.debug("Registered budget_service")

        # Report services
        container.register_singleton("report_data_service", create_report_data_service)
        logger.debug("Registered report_data_service")

        # ADD THIS LINE:
        container.register_singleton("ae_dashboard_service", create_ae_dashboard_service)
        logger.debug("Registered ae_dashboard_service")

        container.register_singleton("planning_service", create_planning_service)
        logger.debug("Registered planning_service")

        logger.info("Registered all default services with container")

    except Exception as e:
        logger.error(f"Failed to register services: {e}")
        raise ServiceCreationError(f"Service registration failed: {e}") from e

def emergency_register_report_service(container):
    """Emergency registration of report_data_service for Railway"""
    print("🚨 EMERGENCY: Registering report_data_service manually...")

    try:
        # Try to import the service
        from src.services.report_data_service import ReportDataService

        print("✅ Successfully imported ReportDataService")

        # Try to get database connection
        try:
            from src.database.connection import DatabaseConnection
            
            container = get_container()
            db_path = container.get_config("DB_PATH", "data/database/production.db")
            db_connection = DatabaseConnection(db_path)
            print("✅ Database connection successful")
        except Exception as e:
            print(f"⚠️ Database connection failed: {e}")
            db_connection = None

        # Create service factory function
        def create_report_service():
            return ReportDataService(db_connection)

        # Register the service
        container.register_factory("report_data_service", create_report_service)
        print("✅ report_data_service manually registered")

        # Test the registration
        test_service = container.get("report_data_service")
        print(f"✅ Manual registration test successful: {type(test_service)}")
        return True

    except ImportError as e:
        print(f"❌ Could not import ReportDataService: {e}")

        # Register mock service for Railway
        class MockReportService:
            def __init__(self):
                print("🔄 MockReportService created for Railway")

            def get_customer_revenue_data(self, *args, **kwargs):
                return {
                    "message": "Service temporarily unavailable in Railway environment",
                    "status": "mock_service",
                }

        container.register_factory("report_data_service", lambda: MockReportService())
        print("✅ Mock report_data_service registered for Railway")
        return True

    except Exception as e:
        print(f"❌ Emergency registration failed: {e}")
        print(f"❌ Traceback: {traceback.format_exc()}")
        return False


def create_emergency_container():
    """Create minimal working container for Railway"""
    print("🚨 Creating emergency container...")

    try:
        from src.services.container import ServiceContainer

        container = ServiceContainer()
    except:
        # Create ultra-minimal container
        class EmergencyContainer:
            def __init__(self):
                self._services = {}
                print("🆘 Ultra-minimal emergency container created")

            def register(self, name, factory):
                self._services[name] = factory
                print(f"📝 Emergency registered: {name}")

            def get(self, name):
                if name not in self._services:
                    raise Exception(
                        f"Service '{name}' not found in emergency container"
                    )
                factory = self._services[name]
                if callable(factory):
                    return factory()
                return factory

        container = EmergencyContainer()

    # Always register report_data_service in emergency mode
    emergency_register_report_service(container)

    return container


#################################


class DeploymentMode(Enum):
    """Deployment modes for service configuration."""

    DEVELOPMENT = "development"
    TESTING = "testing"
    PRODUCTION = "production"
    EMERGENCY = "emergency"  # Emergency mode with maximum safety


def create_database_connection(db_path: Optional[str] = None):
    """
    Create database connection with configuration.

    Args:
        db_path: Optional database path override

    Returns:
        Configured DatabaseConnection instance
    """
    try:
        # Try to import DatabaseConnection
        try:
            from src.database.connection import DatabaseConnection
        except ImportError:
            logger.warning("DatabaseConnection not available - using mock")

            # Return a mock for testing
            class MockDatabaseConnection:
                def __init__(self, db_path):
                    self.db_path = db_path

                def connect(self):
                    import sqlite3

                    conn = sqlite3.connect(self.db_path)
                    conn.row_factory = sqlite3.Row
                    return conn

            return MockDatabaseConnection(db_path or ":memory:")

        container = get_container()

        if db_path is None:
            # Get from container config or use default
            project_root = container.get_config(
                "PROJECT_ROOT",
                os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")),
            )
            db_path = container.get_config(
                "DB_PATH", os.path.join(project_root, "data/database/production.db")
            )

        logger.info(f"Creating database connection to: {db_path}")
        return DatabaseConnection(db_path)

    except Exception as e:
        logger.error(f"Failed to create database connection: {e}")
        raise ServiceCreationError(f"Database connection creation failed: {e}") from e


def create_pipeline_service():
    """
    Create enhanced PipelineService with critical fixes applied.

    Returns:
        Enhanced PipelineService with concurrency control and data consistency
    """
    try:
        # Import our enhanced service
        from .pipeline_service import PipelineService

        container = get_container()

        # Get configuration
        project_root = container.get_config(
            "PROJECT_ROOT",
            os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")),
        )
        data_path = container.get_config(
            "DATA_PATH", os.path.join(project_root, "data/processed")
        )

        # Determine deployment mode and data source strategy
        environment = container.get_config("ENVIRONMENT", "development")
        deployment_mode = (
            DeploymentMode(environment.lower())
            if environment.lower() in [e.value for e in DeploymentMode]
            else DeploymentMode.DEVELOPMENT
        )

        # Choose data source based on environment
        data_source = _get_data_source_for_environment(deployment_mode)

        # Get database connection if needed
        db_connection = None
        if data_source in [
            DataSourceType.DB_ONLY,
            DataSourceType.DB_PRIMARY,
            DataSourceType.JSON_PRIMARY,
        ]:
            try:
                db_connection = container.get("database_connection")
                logger.info("Database connection available for pipeline service")
            except:
                logger.warning(
                    "Database connection not available, falling back to JSON_ONLY"
                )
                data_source = DataSourceType.JSON_ONLY

        logger.info(
            f"Creating PipelineService with data_source: {data_source.value}, data_path: {data_path}"
        )

        # Create enhanced service
        service = PipelineService(
            db_connection=db_connection, data_path=data_path, data_source=data_source
        )

        # Validate service health on creation
        _validate_pipeline_service_health(service)

        return service

    except ImportError as e:
        logger.error(f"Failed to import enhanced PipelineService: {e}")
        raise ServiceCreationError(
            f"Could not import enhanced PipelineService: {e}"
        ) from e
    except Exception as e:
        logger.error(f"Failed to create pipeline service: {e}")
        raise ServiceCreationError(f"Pipeline service creation failed: {e}") from e


def _get_data_source_for_environment(deployment_mode: DeploymentMode) -> DataSourceType:
    """
    Determine the appropriate data source strategy based on deployment mode.

    Args:
        deployment_mode: Current deployment mode

    Returns:
        DataSourceType appropriate for the environment
    """
    data_source_mapping = {
        DeploymentMode.DEVELOPMENT: DataSourceType.JSON_PRIMARY,  # Fast development with DB backup
        DeploymentMode.TESTING: DataSourceType.JSON_ONLY,  # Simple testing without DB complexity
        DeploymentMode.PRODUCTION: DataSourceType.DB_PRIMARY,  # Robust production with JSON cache
        DeploymentMode.EMERGENCY: DataSourceType.JSON_ONLY,  # Emergency fallback, minimal dependencies
    }

    data_source = data_source_mapping.get(deployment_mode, DataSourceType.JSON_PRIMARY)
    logger.info(
        f"Selected data source {data_source.value} for deployment mode {deployment_mode.value}"
    )

    return data_source


def _validate_pipeline_service_health(service) -> None:
    """
    Validate that the pipeline service is healthy and properly configured.

    Args:
        service: PipelineService instance to validate

    Raises:
        ServiceCreationError: If service health validation fails
    """
    try:
        # Get data source info
        info = service.get_data_source_info()

        # Validate basic functionality
        if not info:
            raise ServiceCreationError(
                "Service failed to provide data source information"
            )

        # Check consistency if dual source
        consistency_status = info.get("consistency_status", {})
        if not consistency_status.get("is_consistent", True):
            conflicts = consistency_status.get("conflicts", 0)
            if conflicts > 0:
                logger.warning(
                    f"Pipeline service has {conflicts} data consistency conflicts"
                )

                # In production, this might be critical
                container = get_container()
                environment = container.get_config("ENVIRONMENT", "development")
                if environment.lower() == "production" and conflicts > 10:
                    raise ServiceCreationError(
                        f"Too many consistency conflicts ({conflicts}) for production"
                    )

        # Test basic operations
        test_data = service.get_pipeline_data("HEALTH_CHECK_AE", "2025-01")
        if test_data is None:
            raise ServiceCreationError("Service failed basic data retrieval test")

        logger.info("Pipeline service health validation passed")

    except Exception as e:
        logger.error(f"Pipeline service health validation failed: {e}")

        # Try emergency repair
        try:
            repair_result = service.emergency_repair()
            if repair_result.get("success"):
                logger.info(
                    "Emergency repair successful, service should be healthy now"
                )
            else:
                raise ServiceCreationError(
                    f"Service unhealthy and emergency repair failed: {repair_result.get('error')}"
                )
        except Exception as repair_error:
            raise ServiceCreationError(
                f"Service health check failed and emergency repair failed: {repair_error}"
            ) from e

def create_planning_service():
    """Create PlanningService with database connection."""
    try:
        from src.services.planning_service import PlanningService
        
        container = get_container()
        
        # Get database connection
        try:
            db_connection = container.get("database_connection")
            logger.debug("Creating PlanningService with database connection")
        except Exception as e:
            logger.error(f"Database connection required for planning service: {e}")
            raise ServiceCreationError(
                "PlanningService requires database connection"
            ) from e
        
        # Create service
        service = PlanningService(db_connection)
        
        logger.info("PlanningService created successfully")
        return service
        
    except ImportError as e:
        logger.error(f"Failed to import PlanningService: {e}")
        raise ServiceCreationError(f"Could not import PlanningService: {e}") from e
    except Exception as e:
        logger.error(f"Failed to create planning service: {e}")
        raise ServiceCreationError(f"Planning service creation failed: {e}") from e

def create_budget_service():
    """Create BudgetService with enhanced error handling."""
    try:
        # Try to import BudgetService
        try:
            from .budget_service import BudgetService
        except ImportError:
            logger.warning("BudgetService not available - using mock")

            # Return a mock for testing
            class MockBudgetService:
                def __init__(self, data_path=None, db_path=None):
                    self.data_path = data_path
                    self.db_path = db_path

                def get_monthly_budget(self, ae_name, month):
                    return 0

                def get_annual_target(self, ae_name):
                    return 1000000

                def get_company_budget_totals(self, year):
                    return {}

                def get_quarterly_budget_summary(self, year):
                    return {}

                def validate_budget_data(self, year=2025):
                    return {"is_valid": True, "warnings": [], "errors": []}

            return MockBudgetService()

        container = get_container()

        # Get configuration paths
        project_root = container.get_config(
            "PROJECT_ROOT",
            os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")),
        )
        data_path = container.get_config(
            "DATA_PATH", os.path.join(project_root, "data")
        )
        db_path = container.get_config(
            "DB_PATH", os.path.join(project_root, "data/database/production.db")
        )

        logger.debug(
            f"Creating BudgetService with data_path: {data_path}, db_path: {db_path}"
        )

        # Create service with enhanced error handling
        service = BudgetService(data_path=data_path, db_path=db_path)

        # Validate budget service health
        _validate_budget_service_health(service)

        return service

    except ImportError as e:
        logger.error(f"Failed to import BudgetService: {e}")
        raise ServiceCreationError(f"Could not import BudgetService: {e}") from e
    except Exception as e:
        logger.error(f"Failed to create budget service: {e}")
        raise ServiceCreationError(f"Budget service creation failed: {e}") from e


def _validate_budget_service_health(service) -> None:
    """
    Validate that the budget service is healthy and properly configured.

    Args:
        service: BudgetService instance to validate

    Raises:
        ServiceCreationError: If service health validation fails
    """
    try:
        # Test basic budget operations
        current_year = 2025

        # Test validation function
        validation_result = service.validate_budget_data(current_year)
        if not validation_result.get("is_valid", False):
            errors = validation_result.get("errors", [])
            if errors:
                logger.warning(f"Budget service validation issues: {errors}")

                # Check if this is critical for the environment
                container = get_container()
                environment = container.get_config("ENVIRONMENT", "development")
                if environment.lower() == "production" and len(errors) > 0:
                    # In production, budget errors might be critical
                    critical_errors = [
                        e for e in errors if "No budget data found" in str(e)
                    ]
                    if critical_errors:
                        raise ServiceCreationError(
                            f"Critical budget validation errors in production: {critical_errors}"
                        )

        # Test basic retrieval
        test_budget = service.get_monthly_budget("TEST_AE", "2025-01")
        if test_budget is None:
            raise ServiceCreationError("Budget service failed basic retrieval test")

        logger.info("Budget service health validation passed")

    except Exception as e:
        logger.error(f"Budget service health validation failed: {e}")
        raise ServiceCreationError(f"Budget service health check failed: {e}") from e


def create_report_data_service():
    """Create ReportDataService with enhanced error handling."""
    try:
        # Try to import ReportDataService
        try:
            from .report_data_service import ReportDataService
        except ImportError:
            logger.warning("ReportDataService not available - using mock")

            # Return a mock for testing
            class MockReportDataService:
                def __init__(self, container=None):
                    self.container = container or get_container()

                def get_monthly_revenue_report_data(self, year, filters=None):
                    return {"mock": True}

                def get_ae_performance_report_data(self, filters=None):
                    return {"mock": True}

                def get_quarterly_performance_data(self, filters=None):
                    return {"mock": True}

                def get_sector_performance_data(self, filters=None):
                    return {"mock": True}

            return MockReportDataService()

        container = get_container()
        logger.debug("Creating ReportDataService")

        # Create service
        service = ReportDataService(container)

        # Validate report service health
        _validate_report_service_health(service)

        return service

    except ImportError as e:
        logger.error(f"Failed to import ReportDataService: {e}")
        raise ServiceCreationError(f"Could not import ReportDataService: {e}") from e
    except Exception as e:
        logger.error(f"Failed to create report data service: {e}")
        raise ServiceCreationError(f"Report data service creation failed: {e}") from e


def _validate_report_service_health(service) -> None:
    """
    Validate that the report service is healthy and properly configured.

    Args:
        service: ReportDataService instance to validate

    Raises:
        ServiceCreationError: If service health validation fails
    """
    try:
        # Test database connection through container
        container = get_container()

        # Try to get database connection
        try:
            db_connection = container.get("database_connection")
            if not db_connection:
                raise ServiceCreationError(
                    "Report service requires database connection"
                )
        except Exception as e:
            logger.warning(f"Database connection issue for report service: {e}")
            # In development, this might be OK with mock data
            environment = container.get_config("ENVIRONMENT", "development")
            if environment.lower() == "production":
                raise ServiceCreationError(
                    "Report service requires database connection in production"
                )

        logger.info("Report service health validation passed")

    except Exception as e:
        logger.error(f"Report service health validation failed: {e}")
        raise ServiceCreationError(f"Report service health check failed: {e}") from e

def register_critical_services(container):
    """Register only the most critical services for Railway"""

    print("🔧 Registering critical services...")

    # Try to register report_data_service
    try:

        def create_report_service():
            # Import here to avoid circular imports
            from src.services.report_data_service import ReportDataService

            # Use a simple connection or mock
            return ReportDataService(None)  # Pass None if DB is optional

        container.register_factory("report_data_service", create_report_service)
        print("✅ report_data_service registered")

    except Exception as e:
        print(f"⚠️ Could not register report_data_service: {e}")

        # Register a mock service
        class MockReportService:
            def get_customer_revenue_data(self, *args, **kwargs):
                return {"message": "Database not available in Railway environment"}

        container.register_factory("report_data_service", lambda: MockReportService())
        print("✅ Mock report_data_service registered")


def register_all_services(container):
    """Register all services - your existing registration code goes here"""

    print("🔧 Registering all services...")

    # Your existing service registration code
    # Example:
    # container.register('report_data_service', lambda: ReportDataService(db))
    # container.register('other_service', lambda: OtherService())

    # For now, just register the critical one
    register_critical_services(container)


def _validate_service_container_health() -> None:
    """
    Validate that the service container is healthy and all services are properly configured.

    Raises:
        ServiceCreationError: If container health validation fails
    """
    try:
        container = get_container()

        # Test critical services
        critical_services = [
            "pipeline_service",
            "budget_service",
            "report_data_service",
            "planning_service",
        ]

        for service_name in critical_services:
            try:
                if container.has_service(service_name):
                    # Try to get the service (this will create singletons)
                    service = container.get(service_name)
                    if service is None:
                        logger.warning(f"Service {service_name} returned None")
                    else:
                        logger.debug(f"Service {service_name} is healthy")
                else:
                    logger.warning(f"Required service '{service_name}' not registered")
            except Exception as e:
                logger.error(f"Service {service_name} failed health check: {e}")

                # In production, this might be critical
                environment = container.get_config("ENVIRONMENT", "development")
                if environment.lower() == "production":
                    raise ServiceCreationError(
                        f"Critical service {service_name} failed in production: {e}"
                    )

        # Test database connection if available
        if container.has_service("database_connection"):
            try:
                db_conn = container.get("database_connection")
                if db_conn:
                    # Test basic connection
                    conn = db_conn.connect()
                    conn.close()
                    logger.debug("Database connection is healthy")
            except Exception as e:
                logger.warning(f"Database connection health check failed: {e}")

        logger.info("Service container health validation passed")

    except Exception as e:
        logger.error(f"Service container health validation failed: {e}")
        raise ServiceCreationError(f"Container health check failed: {e}") from e


def get_service_health_report() -> dict:
    """
    Get a comprehensive health report of all services.

    Returns:
        Dictionary containing health status of all services
    """
    try:
        container = get_container()

        health_report = {
            "timestamp": os.environ.get("HEALTH_CHECK_TIME", "unknown"),
            "environment": container.get_config("ENVIRONMENT", "unknown"),
            "services": {},
            "overall_status": "healthy",
            "issues": [],
        }

        # Check each service
        service_names = [
            "database_connection",
            "pipeline_service",
            "budget_service",
            "report_data_service",
        ]

        for service_name in service_names:
            service_health = {
                "registered": container.has_service(service_name),
                "healthy": False,
                "error": None,
                "details": {},
            }

            if service_health["registered"]:
                try:
                    service = container.get(service_name)
                    if service:
                        service_health["healthy"] = True

                        # Get service-specific details
                        if service_name == "pipeline_service" and hasattr(
                            service, "get_data_source_info"
                        ):
                            service_health["details"] = service.get_data_source_info()
                        elif service_name == "budget_service" and hasattr(
                            service, "validate_budget_data"
                        ):
                            service_health["details"] = service.validate_budget_data()

                except Exception as e:
                    service_health["error"] = str(e)
                    health_report["issues"].append(f"Service {service_name}: {e}")

            health_report["services"][service_name] = service_health

        # Determine overall status
        unhealthy_services = [
            name
            for name, health in health_report["services"].items()
            if not health["healthy"]
        ]

        if unhealthy_services:
            health_report["overall_status"] = (
                "degraded"
                if len(unhealthy_services) < len(service_names)
                else "unhealthy"
            )

        return health_report

    except Exception as e:
        return {
            "timestamp": "error",
            "environment": "unknown",
            "services": {},
            "overall_status": "error",
            "issues": [f"Health report generation failed: {e}"],
        }


def emergency_service_recovery():
    """
    Emergency service recovery function to restore services after critical failures.

    Returns:
        Dictionary with recovery status and actions taken
    """
    recovery_log = {
        "timestamp": os.environ.get("RECOVERY_TIME", "unknown"),
        "actions_taken": [],
        "services_recovered": [],
        "services_failed": [],
        "success": False,
    }

    try:
        container = get_container()

        # Clear all singleton instances to force recreation
        container.clear_singletons()
        recovery_log["actions_taken"].append("Cleared singleton instances")

        # Re-initialize services
        try:
            initialize_services()
            recovery_log["actions_taken"].append("Re-initialized services")

            # Test each service
            for service_name in [
                "pipeline_service",
                "budget_service",
                "report_data_service",
            ]:
                try:
                    service = container.get(service_name)
                    if service:
                        recovery_log["services_recovered"].append(service_name)
                    else:
                        recovery_log["services_failed"].append(
                            f"{service_name}: returned None"
                        )
                except Exception as e:
                    recovery_log["services_failed"].append(f"{service_name}: {e}")

            recovery_log["success"] = len(recovery_log["services_failed"]) == 0

        except Exception as e:
            recovery_log["actions_taken"].append(f"Service initialization failed: {e}")
            recovery_log["success"] = False

        return recovery_log

    except Exception as e:
        recovery_log["actions_taken"].append(f"Emergency recovery failed: {e}")
        recovery_log["success"] = False
        return recovery_log