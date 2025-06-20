"""
Service Container for dependency injection and service management.
Provides centralized service registration and resolution with support
for singleton and factory patterns.
"""
from typing import Dict, Any, Callable, TypeVar, Type, Optional
import logging
from functools import wraps

logger = logging.getLogger(__name__)

T = TypeVar('T')


class ServiceContainer:
    """
    Dependency injection container for managing service instances and dependencies.
    
    Supports:
    - Singleton services (created once, reused)
    - Factory services (created fresh each time)
    - Dependency injection between services
    - Configuration injection
    """
    
    def __init__(self):
        self._services: Dict[str, Any] = {}
        self._factories: Dict[str, Callable] = {}
        self._singletons: Dict[str, Any] = {}
        self._config: Dict[str, Any] = {}
        
    def register_singleton(self, name: str, factory: Callable[[], T]) -> None:
        """Register a service as singleton (created once, reused)."""
        self._factories[name] = factory
        logger.debug(f"Registered singleton service: {name}")
        
    def register_factory(self, name: str, factory: Callable[[], T]) -> None:
        """Register a service as factory (created fresh each time)."""
        self._services[name] = factory
        logger.debug(f"Registered factory service: {name}")
        
    def register_instance(self, name: str, instance: T) -> None:
        """Register a pre-created service instance."""
        self._singletons[name] = instance
        logger.debug(f"Registered service instance: {name}")
        
    def set_config(self, config: Dict[str, Any]) -> None:
        """Set configuration dictionary for injection into services."""
        self._config = config
        logger.debug(f"Updated container configuration with {len(config)} items")
        
    def get(self, name: str) -> Any:
        """
        Resolve and return a service instance.
        
        Args:
            name: Service name to resolve
            
        Returns:
            Service instance
            
        Raises:
            ServiceNotFoundError: If service is not registered
            ServiceCreationError: If service creation fails
        """
        # Check if already created singleton
        if name in self._singletons:
            return self._singletons[name]
            
        # Check if singleton factory exists
        if name in self._factories:
            try:
                logger.debug(f"Creating singleton service: {name}")
                instance = self._factories[name]()
                self._singletons[name] = instance
                return instance
            except Exception as e:
                logger.error(f"Failed to create singleton service '{name}': {e}")
                raise ServiceCreationError(f"Failed to create singleton service '{name}': {e}") from e
            
        # Check if factory exists
        if name in self._services:
            try:
                logger.debug(f"Creating factory service: {name}")
                return self._services[name]()
            except Exception as e:
                logger.error(f"Failed to create factory service '{name}': {e}")
                raise ServiceCreationError(f"Failed to create factory service '{name}': {e}") from e
            
        # Service not found - raise ServiceNotFoundError directly (no try/catch wrapper!)
        raise ServiceNotFoundError(f"Service '{name}' not found in container")
    
    def get_config(self, key: str, default: Any = None) -> Any:
        """Get configuration value by key."""
        return self._config.get(key, default)
        
    def has_service(self, name: str) -> bool:
        """Check if service is registered."""
        return (name in self._services or 
                name in self._factories or 
                name in self._singletons)
    
    def clear_singletons(self) -> None:
        """Clear all singleton instances (useful for testing)."""
        self._singletons.clear()
        logger.debug("Cleared all singleton instances")
        
    def list_services(self) -> Dict[str, str]:
        """List all registered services and their types."""
        services = {}
        for name in self._singletons:
            services[name] = "singleton_instance"
        for name in self._factories:
            services[name] = "singleton_factory"
        for name in self._services:
            services[name] = "factory"
        return services


class ServiceNotFoundError(Exception):
    """Raised when a requested service is not found in the container."""
    pass


class ServiceCreationError(Exception):
    """Raised when service creation fails."""
    pass


# Global container instance
_container: Optional[ServiceContainer] = None


def get_container() -> ServiceContainer:
    """Get the global service container instance."""
    global _container
    if _container is None:
        _container = ServiceContainer()
    return _container


def inject(*service_names: str):
    """
    Decorator to inject services into function parameters.
    
    Usage:
        @inject('database_connection', 'config')
        def my_function(db, config):
            # db and config are automatically injected
            pass
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            container = get_container()
            injected_args = []
            
            for service_name in service_names:
                try:
                    service = container.get(service_name)
                    injected_args.append(service)
                except (ServiceNotFoundError, ServiceCreationError) as e:
                    logger.error(f"Failed to inject service '{service_name}': {e}")
                    raise
                    
            return func(*injected_args, *args, **kwargs)
        return wrapper
    return decorator


def reset_container() -> None:
    """Reset the global container (useful for testing)."""
    global _container
    _container = None