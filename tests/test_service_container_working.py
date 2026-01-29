import pytest
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from services.container import (
    ServiceContainer,
    get_container,
    reset_container,
    ServiceNotFoundError,
)


class TestServiceContainer:
    """Test the ServiceContainer class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.container = ServiceContainer()

    def test_register_and_get_singleton(self):
        """Test singleton service registration and retrieval."""

        def mock_factory():
            return "mock_service_instance"

        self.container.register_singleton("test_service", mock_factory)
        result1 = self.container.get("test_service")
        result2 = self.container.get("test_service")

        assert result1 == "mock_service_instance"
        assert result2 == "mock_service_instance"
        assert result1 is result2

    def test_register_and_get_factory(self):
        """Test factory service registration and retrieval."""
        call_count = 0

        def mock_factory():
            nonlocal call_count
            call_count += 1
            return f"mock_service_{call_count}"

        self.container.register_factory("test_service", mock_factory)
        result1 = self.container.get("test_service")
        result2 = self.container.get("test_service")

        assert result1 == "mock_service_1"
        assert result2 == "mock_service_2"
        assert result1 != result2

    def test_service_not_found(self):
        """Test ServiceNotFoundError for unregistered services."""
        with pytest.raises(ServiceNotFoundError):
            self.container.get("nonexistent_service")

    def test_config_management(self):
        """Test configuration setting and retrieval."""
        test_config = {"key1": "value1", "key2": 42}

        self.container.set_config(test_config)

        assert self.container.get_config("key1") == "value1"
        assert self.container.get_config("key2") == 42
        assert self.container.get_config("missing_key") is None
        assert self.container.get_config("missing_key", "default") == "default"

    def test_has_service(self):
        """Test service existence checking."""

        def mock_factory():
            return "mock"

        assert not self.container.has_service("test_service")

        self.container.register_singleton("test_service", mock_factory)
        assert self.container.has_service("test_service")


class TestGlobalContainer:
    """Test global container functions."""

    def setup_method(self):
        """Reset global state."""
        reset_container()

    def test_get_container_singleton(self):
        """Test that get_container returns same instance."""
        container1 = get_container()
        container2 = get_container()

        assert container1 is container2

    def test_reset_container(self):
        """Test container reset functionality."""
        container1 = get_container()
        reset_container()
        container2 = get_container()

        assert container1 is not container2


def test_basic_functionality():
    """Test that basic functionality works."""
    container = ServiceContainer()

    container.set_config({"test": "value"})
    assert container.get_config("test") == "value"

    def mock_service():
        return "test_service"

    container.register_singleton("test", mock_service)
    result = container.get("test")
    assert result == "test_service"


def test_imports_work():
    """Test that all imports work correctly."""
    assert ServiceContainer is not None
    assert get_container is not None
    print("✅ All imports successful!")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
