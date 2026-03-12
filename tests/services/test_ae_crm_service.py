"""Tests for AeCrmService -- AE-scoped account queries."""

import sqlite3
import pytest

from src.services.ae_crm_service import AeCrmService
from src.database.connection import DatabaseConnection


@pytest.fixture()
def crm_db(tmp_path):
    """Temp DB with full schema for CRM service tests."""
    db_path = str(tmp_path / "test_crm.db")
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")

    conn.executescript("""
        CREATE TABLE agencies (
            agency_id INTEGER PRIMARY KEY,
            agency_name TEXT NOT NULL,
            assigned_ae TEXT,
            is_active INTEGER DEFAULT 1
        );
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            normalized_name TEXT NOT NULL,
            agency_id INTEGER,
            assigned_ae TEXT,
            is_active INTEGER DEFAULT 1
        );
        CREATE TABLE entity_signals (
            entity_type TEXT NOT NULL,
            entity_id INTEGER NOT NULL,
            signal_type TEXT NOT NULL,
            signal_label TEXT NOT NULL,
            signal_priority INTEGER NOT NULL,
            trailing_revenue REAL,
            prior_revenue REAL,
            computed_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (entity_type, entity_id, signal_type)
        );
        CREATE TABLE entity_activity (
            activity_id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_type TEXT NOT NULL,
            entity_id INTEGER NOT NULL,
            activity_type TEXT NOT NULL,
            activity_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            description TEXT,
            created_by TEXT,
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            contact_id INTEGER,
            due_date TEXT,
            is_completed INTEGER DEFAULT 0,
            completed_date TIMESTAMP
        );
        CREATE TABLE entity_contacts (
            contact_id INTEGER PRIMARY KEY,
            entity_type TEXT,
            entity_id INTEGER,
            contact_name TEXT,
            phone TEXT,
            email TEXT,
            is_primary INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1
        );
        CREATE TABLE spots (
            spot_id INTEGER PRIMARY KEY AUTOINCREMENT,
            sales_person TEXT,
            bill_code TEXT,
            broadcast_month TEXT,
            gross_rate REAL,
            revenue_type TEXT,
            customer_id INTEGER,
            is_historical INTEGER DEFAULT 0
        );

        -- Alice's accounts
        INSERT INTO agencies VALUES (1, 'Agency Alpha', 'Alice', 1);
        INSERT INTO customers VALUES
            (10, 'Customer One', NULL, 'Alice', 1);
        INSERT INTO customers VALUES
            (11, 'Customer Two', 1, 'Alice', 1);

        -- Bob's account
        INSERT INTO customers VALUES
            (20, 'Customer Three', NULL, 'Bob', 1);

        -- Inactive account assigned to Alice
        INSERT INTO customers VALUES
            (30, 'Dead Account', NULL, 'Alice', 0);

        -- Signal for Customer One
        INSERT INTO entity_signals VALUES
            ('customer', 10, 'declining', 'Declining', 2, 5000, 8000,
             '2026-03-01');

        -- Spots for trailing revenue (use recent months)
        INSERT INTO spots (sales_person, broadcast_month, gross_rate,
                           customer_id, revenue_type)
        VALUES
            ('Alice', 'Jan-26', 3000, 10, 'Cash'),
            ('Alice', 'Feb-26', 2000, 10, 'Cash'),
            ('Alice', 'Jan-26', 1500, 11, 'Cash');
    """)
    conn.commit()
    conn.close()
    return DatabaseConnection(db_path)


@pytest.fixture()
def crm_service(crm_db):
    return AeCrmService(crm_db)


class TestGetAccounts:
    """Test AE-scoped account listing."""

    def test_returns_only_assigned_accounts(self, crm_service, crm_db):
        with crm_db.connection_ro() as conn:
            accounts = crm_service.get_accounts(conn, ae_name="Alice")
        names = {a["entity_name"] for a in accounts}
        assert names == {"Agency Alpha", "Customer One", "Customer Two"}

    def test_excludes_other_ae_accounts(self, crm_service, crm_db):
        with crm_db.connection_ro() as conn:
            accounts = crm_service.get_accounts(conn, ae_name="Alice")
        names = {a["entity_name"] for a in accounts}
        assert "Customer Three" not in names

    def test_excludes_inactive_accounts(self, crm_service, crm_db):
        with crm_db.connection_ro() as conn:
            accounts = crm_service.get_accounts(conn, ae_name="Alice")
        names = {a["entity_name"] for a in accounts}
        assert "Dead Account" not in names

    def test_includes_signal_data(self, crm_service, crm_db):
        with crm_db.connection_ro() as conn:
            accounts = crm_service.get_accounts(conn, ae_name="Alice")
        cust_one = next(
            a for a in accounts if a["entity_name"] == "Customer One"
        )
        assert cust_one["signal_type"] == "declining"
        assert cust_one["signal_priority"] == 2

    def test_includes_trailing_revenue(self, crm_service, crm_db):
        with crm_db.connection_ro() as conn:
            accounts = crm_service.get_accounts(conn, ae_name="Alice")
        cust_one = next(
            a for a in accounts if a["entity_name"] == "Customer One"
        )
        assert cust_one["trailing_revenue"] == 5000
        cust_two = next(
            a for a in accounts if a["entity_name"] == "Customer Two"
        )
        assert cust_two["trailing_revenue"] == 0

    def test_no_ae_returns_all_active(self, crm_service, crm_db):
        with crm_db.connection_ro() as conn:
            accounts = crm_service.get_accounts(conn, ae_name=None)
        assert len(accounts) == 4  # 1 agency + 3 active customers

    def test_sorted_by_signal_priority(self, crm_service, crm_db):
        with crm_db.connection_ro() as conn:
            accounts = crm_service.get_accounts(conn, ae_name="Alice")
        assert accounts[0]["entity_name"] == "Customer One"


class TestGetStats:
    """Test summary stats for AE."""

    def test_account_count(self, crm_service, crm_db):
        with crm_db.connection_ro() as conn:
            stats = crm_service.get_stats(conn, ae_name="Alice")
        assert stats["account_count"] == 3

    def test_trailing_revenue(self, crm_service, crm_db):
        with crm_db.connection_ro() as conn:
            stats = crm_service.get_stats(conn, ae_name="Alice")
        assert stats["trailing_revenue"] == 5000

    def test_signal_count(self, crm_service, crm_db):
        with crm_db.connection_ro() as conn:
            stats = crm_service.get_stats(conn, ae_name="Alice")
        assert stats["signal_count"] == 1

    def test_follow_up_and_overdue_counts(self, crm_service, crm_db):
        with crm_db.connection() as conn:
            conn.execute("""
                INSERT INTO entity_activity
                    (entity_type, entity_id, activity_type, description,
                     due_date, is_completed)
                VALUES ('customer', 10, 'follow_up', 'Overdue task',
                        '2025-01-01', 0)
            """)
            conn.commit()
        with crm_db.connection_ro() as conn:
            stats = crm_service.get_stats(conn, ae_name="Alice")
        assert stats["follow_up_count"] >= 1
        assert stats["overdue_count"] >= 1


class TestGetRevenueTrend:
    """Test revenue trend for an entity."""

    def test_returns_monthly_data(self, crm_service, crm_db):
        with crm_db.connection_ro() as conn:
            trend = crm_service.get_revenue_trend(conn, "customer", 10)
        assert len(trend) > 0
        assert "broadcast_month" in trend[0]
        assert "revenue" in trend[0]

    def test_excludes_trade_revenue(self, crm_service, crm_db):
        with crm_db.connection() as conn:
            conn.execute("""
                INSERT INTO spots (broadcast_month, gross_rate,
                                   customer_id, revenue_type)
                VALUES ('Jan-26', 9999, 10, 'Trade')
            """)
            conn.commit()
        with crm_db.connection_ro() as conn:
            trend = crm_service.get_revenue_trend(conn, "customer", 10)
        total = sum(t["revenue"] for t in trend)
        assert 9999 not in [t["revenue"] for t in trend]

    def test_agency_aggregates_linked_customers(self, crm_service, crm_db):
        with crm_db.connection_ro() as conn:
            trend = crm_service.get_revenue_trend(conn, "agency", 1)
        revenues = [t["revenue"] for t in trend]
        assert sum(revenues) > 0

    def test_empty_for_no_spots(self, crm_service, crm_db):
        with crm_db.connection_ro() as conn:
            trend = crm_service.get_revenue_trend(conn, "customer", 20)
        assert len(trend) == 0
