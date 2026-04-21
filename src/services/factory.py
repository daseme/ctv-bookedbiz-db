"""Service factory functions and dependency injection setup."""

import os
import logging
from typing import Optional

from .container import get_container, ServiceCreationError

logger = logging.getLogger(__name__)


def _resolve_db_path() -> str:
    """Resolve DB_PATH from environment. Fail if not set."""
    path = os.environ.get("DB_PATH") or os.environ.get("DATABASE_PATH")
    if not path:
        raise ServiceCreationError(
            "DB_PATH not set. Set DB_PATH or DATABASE_PATH env var."
        )
    return path


def configure_container_from_environment():
    """Configure container with environment-specific settings."""
    container = get_container()

    project_root = os.environ.get("PROJECT_ROOT")
    if project_root is None:
        project_root = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../..")
        )

    if not os.path.exists(project_root):
        try:
            os.makedirs(project_root, exist_ok=True)
        except Exception as e:
            logger.error(f"Cannot create project root: {e}")

    config = {
        "PROJECT_ROOT": project_root,
        "DB_PATH": _resolve_db_path(),
        "DATA_PATH": os.environ.get(
            "DATA_PATH", os.path.join(project_root, "data/processed")
        ),
        "ENVIRONMENT": os.environ.get("FLASK_ENV", "development"),
        "DEBUG": os.environ.get("DEBUG", "false").lower() == "true",
        "LOG_LEVEL": os.environ.get("LOG_LEVEL", "INFO"),
        "CACHE_ENABLED": os.environ.get(
            "CACHE_ENABLED", "true"
        ).lower() == "true",
        "CACHE_TTL": int(os.environ.get("CACHE_TTL", "300")),
    }

    data_path = config["DATA_PATH"]
    if not os.path.exists(data_path):
        try:
            os.makedirs(data_path, exist_ok=True)
        except Exception as e:
            logger.error(f"Cannot create data directory: {e}")

    db_dir = os.path.dirname(config["DB_PATH"])
    if not os.path.exists(db_dir):
        try:
            os.makedirs(db_dir, exist_ok=True)
        except Exception as e:
            logger.error(f"Cannot create database directory: {e}")

    container.set_config(config)
    logger.info(
        f"Configured container for environment: {config['ENVIRONMENT']}"
    )


def initialize_services():
    """Initialize and register all services with the container."""
    configure_container_from_environment()
    container = get_container()

    container.register_singleton(
        "database_connection", create_database_connection
    )
    container.register_singleton(
        "report_data_service", create_report_data_service
    )
    container.register_singleton(
        "budget_service", create_budget_service
    )
    container.register_singleton(
        "ae_dashboard_service", create_ae_dashboard_service
    )
    container.register_singleton(
        "planning_service", create_planning_service
    )
    container.register_singleton(
        "market_analysis_service", create_market_analysis_service
    )
    container.register_singleton(
        "management_performance_service",
        create_management_performance_service,
    )
    container.register_singleton(
        "user_service", create_user_service
    )
    container.register_singleton(
        "pricing_analysis_service", create_pricing_analysis_service
    )
    container.register_singleton(
        "pricing_trends_service", create_pricing_trends_service
    )
    container.register_singleton(
        "pending_order_service", create_pending_order_service
    )
    container.register_singleton(
        "entity_metrics_service", create_entity_metrics_service
    )
    container.register_singleton(
        "entity_service", create_entity_service
    )
    container.register_singleton(
        "activity_service", create_activity_service
    )
    container.register_singleton(
        "address_service", create_address_service
    )
    container.register_singleton(
        "saved_filter_service", create_saved_filter_service
    )
    container.register_singleton(
        "export_service", create_export_service
    )
    container.register_singleton(
        "ae_crm_service", create_ae_crm_service
    )
    container.register_singleton(
        "signal_action_service", create_signal_action_service
    )
    container.register_singleton(
        "health_score_service", create_health_score_service
    )
    container.register_singleton(
        "manager_dashboard_service", create_manager_dashboard_service
    )
    container.register_singleton(
        "revenue_classification_service",
        create_revenue_classification_service,
    )
    container.register_singleton(
        "sheet_export_service", create_sheet_export_service
    )
    container.register_singleton(
        "planning_export_service", create_planning_export_service
    )

    logger.info(
        f"Registered {len(container.list_services())} services"
    )
    return container


# ---------------------------------------------------------------------------
# Factory functions
# ---------------------------------------------------------------------------

def create_database_connection(db_path: Optional[str] = None):
    """Create DatabaseConnection from container config or override."""
    from src.database.connection import DatabaseConnection

    if db_path is None:
        container = get_container()
        db_path = container.get_config("DB_PATH")
        if not db_path:
            raise ServiceCreationError(
                "DB_PATH not configured in container"
            )

    logger.info(f"Creating database connection to: {db_path}")
    return DatabaseConnection(db_path)


def create_report_data_service():
    """Factory function for ReportDataService."""
    from .report_data_service import ReportDataService

    container = get_container()
    return ReportDataService(container)


def create_budget_service():
    """Factory function for BudgetService."""
    from .budget_service import BudgetService

    container = get_container()
    project_root = container.get_config(
        "PROJECT_ROOT",
        os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")),
    )
    data_path = container.get_config(
        "DATA_PATH", os.path.join(project_root, "data")
    )
    db_connection = container.get("database_connection")
    return BudgetService(data_path=data_path, db=db_connection)


def create_ae_dashboard_service():
    """Factory function for AEDashboardService."""
    from .ae_dashboard_service import AEDashboardService

    container = get_container()
    db_connection = container.get("database_connection")
    return AEDashboardService(db_connection)


def create_planning_service():
    """Factory function for PlanningService."""
    from src.services.planning_service import PlanningService

    container = get_container()
    db_connection = container.get("database_connection")
    return PlanningService(db_connection)


def create_market_analysis_service():
    """Factory function for MarketAnalysisService."""
    from src.services.market_analysis_service import MarketAnalysisService

    container = get_container()
    db_connection = container.get("database_connection")
    return MarketAnalysisService(db_connection)


def create_management_performance_service():
    """Factory function for ManagementPerformanceService."""
    from src.services.management_performance_service import (
        ManagementPerformanceService,
    )

    container = get_container()
    db_connection = container.get("database_connection")
    return ManagementPerformanceService(db_connection)


def create_user_service():
    """Factory function for UserService."""
    from src.services.user_service import UserService

    container = get_container()
    db_connection = container.get("database_connection")
    return UserService(db_connection)


def create_pricing_analysis_service():
    """Factory function for PricingAnalysisService."""
    from src.services.pricing_analysis_service import PricingAnalysisService

    container = get_container()
    db_connection = container.get("database_connection")
    return PricingAnalysisService(db_connection)


def create_pricing_trends_service():
    """Factory function for PricingTrendsService."""
    from src.services.pricing_trends_service import PricingTrendsService

    container = get_container()
    db_connection = container.get("database_connection")
    return PricingTrendsService(db_connection)


def create_entity_service():
    """Factory function for EntityService."""
    from src.services.entity_service import EntityService

    container = get_container()
    db_connection = container.get("database_connection")
    return EntityService(db_connection)


def create_activity_service():
    """Factory function for ActivityService."""
    from src.services.activity_service import ActivityService

    container = get_container()
    db_connection = container.get("database_connection")
    return ActivityService(db_connection)


def create_address_service():
    """Factory function for AddressService."""
    from src.services.address_service import AddressService

    container = get_container()
    db_connection = container.get("database_connection")
    return AddressService(db_connection)


def create_saved_filter_service():
    """Factory function for SavedFilterService."""
    from src.services.saved_filter_service import SavedFilterService

    container = get_container()
    db_connection = container.get("database_connection")
    return SavedFilterService(db_connection)


def create_export_service():
    """Factory function for ExportService."""
    from src.services.export_service import ExportService

    container = get_container()
    db_connection = container.get("database_connection")
    return ExportService(db_connection)


def create_ae_crm_service():
    """Factory function for AeCrmService."""
    from src.services.ae_crm_service import AeCrmService

    container = get_container()
    db_connection = container.get("database_connection")
    return AeCrmService(db_connection)


def create_entity_metrics_service():
    """Factory function for EntityMetricsService."""
    from src.services.entity_metrics_service import EntityMetricsService

    container = get_container()
    db_connection = container.get("database_connection")
    return EntityMetricsService(db_connection)


def create_pending_order_service():
    """Factory function for PendingOrderService."""
    from src.services.pending_order_service import PendingOrderService

    return PendingOrderService()


def create_signal_action_service():
    """Factory function for SignalActionService."""
    from src.services.signal_action_service import SignalActionService

    container = get_container()
    db_connection = container.get("database_connection")
    return SignalActionService(db_connection)


def create_health_score_service():
    """Factory function for HealthScoreService."""
    from src.services.health_score_service import HealthScoreService

    container = get_container()
    db_connection = container.get("database_connection")
    return HealthScoreService(db_connection)


def create_manager_dashboard_service():
    """Factory function for ManagerDashboardService."""
    from src.services.manager_dashboard_service import (
        ManagerDashboardService,
    )

    container = get_container()
    db_connection = container.get("database_connection")
    return ManagerDashboardService(db_connection)


def create_revenue_classification_service():
    """Factory function for RevenueClassificationService."""
    from src.services.revenue_classification_service import (
        RevenueClassificationService,
    )

    container = get_container()
    db_connection = container.get("database_connection")
    return RevenueClassificationService(db_connection)


def create_sheet_export_service():
    """Factory: SheetExportService wired to the singleton DB connection."""
    from .sheet_export_service import SheetExportService

    container = get_container()
    db_connection = container.get("database_connection")
    return SheetExportService(db_connection)


def create_planning_export_service():
    """Factory: PlanningExportService wired to the singleton DB connection."""
    from .planning_export_service import PlanningExportService

    container = get_container()
    db_connection = container.get("database_connection")
    return PlanningExportService(db_connection)


# ---------------------------------------------------------------------------
# Health reporting (used by health.py routes)
# ---------------------------------------------------------------------------

def get_service_health_report() -> dict:
    """Return health status of registered services."""
    container = get_container()
    report = {
        "environment": container.get_config("ENVIRONMENT", "unknown"),
        "services": {},
        "overall_status": "healthy",
        "issues": [],
    }

    for name in container.list_services():
        entry = {"registered": True, "healthy": False, "error": None}
        try:
            svc = container.get(name)
            entry["healthy"] = svc is not None
        except Exception as e:
            entry["error"] = str(e)
            report["issues"].append(f"{name}: {e}")
        report["services"][name] = entry

    if report["issues"]:
        report["overall_status"] = "degraded"
    return report


def emergency_service_recovery() -> dict:
    """Clear singletons and re-initialize all services."""
    log = {
        "actions_taken": [],
        "services_recovered": [],
        "services_failed": [],
        "success": False,
    }

    try:
        container = get_container()
        container.clear_singletons()
        log["actions_taken"].append("Cleared singleton instances")

        initialize_services()
        log["actions_taken"].append("Re-initialized services")

        for name in container.list_services():
            try:
                svc = container.get(name)
                if svc:
                    log["services_recovered"].append(name)
                else:
                    log["services_failed"].append(f"{name}: returned None")
            except Exception as e:
                log["services_failed"].append(f"{name}: {e}")

        log["success"] = len(log["services_failed"]) == 0

    except Exception as e:
        log["actions_taken"].append(f"Recovery failed: {e}")

    return log
