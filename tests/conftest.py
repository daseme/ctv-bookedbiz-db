"""Shared pytest fixtures for the test suite."""

import os
import pytest

# Point services at the real dev database BEFORE any app code imports.
os.environ["DB_PATH"] = ".data/dev.db"
os.environ["ENVIRONMENT"] = "development"


@pytest.fixture(scope="session")
def app():
    """Create Flask app configured for testing with dev.db."""
    from src.web.app import create_app

    app = create_app()
    app.config["TESTING"] = True
    # Disable login redirects for smoke tests
    app.config["LOGIN_DISABLED"] = True
    return app


@pytest.fixture(scope="session")
def client(app):
    """Test client that persists for the session (no per-test overhead)."""
    return app.test_client()
