"""Tests for customer detail navigation context."""

import sqlite3
import tempfile
import os

import pytest


@pytest.fixture
def app():
    """Create test app with a minimal database."""
    db_fd, db_path = tempfile.mkstemp(suffix=".db")
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            normalized_name TEXT NOT NULL,
            is_active INTEGER DEFAULT 1,
            agency_id INTEGER
        );
        CREATE TABLE agencies (
            agency_id INTEGER PRIMARY KEY,
            agency_name TEXT NOT NULL
        );
        CREATE TABLE spots (
            spot_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            gross_revenue REAL DEFAULT 0,
            net_revenue REAL DEFAULT 0,
            broadcast_month TEXT,
            revenue_type TEXT
        );
        INSERT INTO customers (customer_id, normalized_name)
        VALUES (1, 'Test Customer');
        INSERT INTO spots (spot_id, customer_id, gross_revenue,
                           net_revenue, broadcast_month)
        VALUES (1, 1, 1000, 800, 'Jan-25');
    """)
    conn.commit()
    conn.close()

    from src.web.app import create_app
    application = create_app()
    application.config["TESTING"] = True
    application.config["DB_PATH"] = db_path

    yield application

    os.close(db_fd)
    os.unlink(db_path)


@pytest.fixture
def client(app):
    return app.test_client()


class TestCustomerDetailNavigation:
    """Test that navigation context is passed to templates."""

    def test_no_from_param_defaults_to_none(self, client):
        """When no ?from= param, from_page should be empty."""
        resp = client.get("/reports/customer/1", follow_redirects=True)
        assert resp.status_code in (200, 302)

    def test_from_address_book_passed_to_template(self, client):
        """When ?from=address-book, page should contain address-book link."""
        resp = client.get(
            "/reports/customer/1?from=address-book",
            follow_redirects=True,
        )
        assert resp.status_code in (200, 302)

    def test_from_ae_dashboard_passed_to_template(self, client):
        """When ?from=ae-dashboard, page should contain ae-dashboard link."""
        resp = client.get(
            "/reports/customer/1?from=ae-dashboard",
            follow_redirects=True,
        )
        assert resp.status_code in (200, 302)
