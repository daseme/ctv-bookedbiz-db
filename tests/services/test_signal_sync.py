"""Tests for signal sync integration in refresh_signals.

Verifies that refresh_signals() captures a before-snapshot,
recomputes signals, then calls sync_from_signals() to create
and acknowledge signal_actions.
"""

import os
import sqlite3
import tempfile
from datetime import date, timedelta

import pytest
from dateutil.relativedelta import relativedelta

from src.database.connection import DatabaseConnection
from src.services.entity_metrics_service import EntityMetricsService


SCHEMA = """
CREATE TABLE agencies (
    agency_id INTEGER PRIMARY KEY,
    agency_name TEXT UNIQUE,
    is_active INTEGER DEFAULT 1,
    assigned_ae TEXT
);
CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY,
    normalized_name TEXT UNIQUE,
    agency_id INTEGER,
    is_active INTEGER DEFAULT 1,
    assigned_ae TEXT
);
CREATE TABLE spots (
    spot_id INTEGER PRIMARY KEY,
    agency_id INTEGER,
    customer_id INTEGER,
    market_name TEXT,
    air_date TEXT,
    gross_rate REAL DEFAULT 0,
    revenue_type TEXT,
    broadcast_month TEXT,
    is_historical INTEGER DEFAULT 0
);
CREATE TABLE entity_metrics (
    entity_type TEXT,
    entity_id INTEGER,
    markets TEXT,
    last_active TEXT,
    total_revenue REAL DEFAULT 0,
    spot_count INTEGER DEFAULT 0,
    agency_spot_count INTEGER DEFAULT 0,
    updated_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (entity_type, entity_id)
);
CREATE TABLE entity_signals (
    entity_type TEXT,
    entity_id INTEGER,
    signal_type TEXT,
    signal_label TEXT,
    signal_priority INTEGER,
    trailing_revenue REAL,
    prior_revenue REAL,
    computed_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (entity_type, entity_id, signal_type)
);
CREATE TABLE signal_actions (
    action_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL CHECK (entity_type IN ('customer', 'agency')),
    entity_id INTEGER NOT NULL,
    signal_type TEXT NOT NULL,
    assigned_ae TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'new'
        CHECK (status IN ('new', 'acknowledged', 'snoozed', 'dismissed')),
    reason TEXT,
    snooze_until DATE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_by TEXT
);
"""


@pytest.fixture()
def db_path():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    c = sqlite3.connect(path)
    c.executescript(SCHEMA)
    c.commit()
    c.close()
    yield path
    os.unlink(path)


@pytest.fixture()
def service(db_path):
    db = DatabaseConnection(db_path)
    return EntityMetricsService(db)


@pytest.fixture()
def conn(db_path):
    c = sqlite3.connect(db_path)
    c.row_factory = sqlite3.Row
    yield c
    c.close()


class TestSyncCreatesActionsAfterRefresh:
    """refresh_signals creates signal_actions for new signals."""

    def test_sync_creates_actions_after_refresh(self, service, conn):
        """Churned signal triggers a new signal_action with status='new'."""
        # Seed customer with assigned_ae
        conn.execute(
            "INSERT INTO customers (customer_id, normalized_name, assigned_ae)"
            " VALUES (1, 'ChurnedCo', 'Alice')"
        )
        # Spot 18 months ago: prior_12m > $10K, trailing = $0 -> churned
        prior_date = (date.today() - timedelta(days=540)).isoformat()
        conn.execute(
            "INSERT INTO spots (spot_id, customer_id, market_name,"
            " air_date, gross_rate, revenue_type)"
            " VALUES (1, 1, 'Denver', ?, 15000, 'Cash')",
            [prior_date],
        )
        conn.commit()

        service.refresh_signals(conn)

        # Signal should exist
        sig = conn.execute(
            "SELECT * FROM entity_signals"
            " WHERE entity_type='customer' AND entity_id=1"
            " AND signal_type='churned'"
        ).fetchone()
        assert sig is not None

        # Signal action should have been created
        action = conn.execute(
            "SELECT * FROM signal_actions"
            " WHERE entity_type='customer' AND entity_id=1"
            " AND signal_type='churned'"
        ).fetchone()
        assert action is not None
        assert action["status"] == "new"
        assert action["assigned_ae"] == "Alice"


class TestSyncAcknowledgesRecoveredEntities:
    """Signal removed after refresh -> action acknowledged."""

    def test_sync_acknowledges_recovered_entities(self, service, conn):
        """Pre-existing signal action gets acknowledged when signal disappears."""
        # Seed customer with assigned_ae
        conn.execute(
            "INSERT INTO customers (customer_id, normalized_name, assigned_ae)"
            " VALUES (2, 'RecoveredCo', 'Bob')"
        )
        # Pre-seed a signal that will NOT be recomputed (no matching spots)
        conn.execute(
            "INSERT INTO entity_signals"
            " (entity_type, entity_id, signal_type, signal_label,"
            "  signal_priority, trailing_revenue, prior_revenue)"
            " VALUES ('customer', 2, 'churned', 'old signal', 1, 0, 20000)"
        )
        # Pre-seed a signal_action for it
        conn.execute(
            "INSERT INTO signal_actions"
            " (entity_type, entity_id, signal_type, assigned_ae, status)"
            " VALUES ('customer', 2, 'churned', 'Bob', 'new')"
        )
        conn.commit()

        # refresh_signals will DELETE all signals, recompute (nothing for
        # customer 2 since no spots), then sync sees the signal was removed
        service.refresh_signals(conn)

        action = conn.execute(
            "SELECT status, updated_by FROM signal_actions"
            " WHERE entity_type='customer' AND entity_id=2"
            " AND signal_type='churned'"
        ).fetchone()
        assert action is not None
        assert action["status"] == "acknowledged"
        assert action["updated_by"] == "system:signal_recovered"


class TestSyncUsesAeFromEntity:
    """Signal actions get assigned_ae from the entity."""

    def test_sync_uses_ae_from_entity(self, service, conn):
        """Signal action assigned_ae comes from customer.assigned_ae."""
        conn.execute(
            "INSERT INTO customers (customer_id, normalized_name, assigned_ae)"
            " VALUES (3, 'AeCo', 'Alice')"
        )
        prior_date = (date.today() - timedelta(days=540)).isoformat()
        conn.execute(
            "INSERT INTO spots (spot_id, customer_id, market_name,"
            " air_date, gross_rate, revenue_type)"
            " VALUES (10, 3, 'Denver', ?, 20000, 'Cash')",
            [prior_date],
        )
        conn.commit()

        service.refresh_signals(conn)

        action = conn.execute(
            "SELECT assigned_ae FROM signal_actions"
            " WHERE entity_type='customer' AND entity_id=3"
        ).fetchone()
        assert action is not None
        assert action["assigned_ae"] == "Alice"


class TestSyncSkipsEntitiesWithoutAe:
    """Entities without assigned_ae get no signal_actions."""

    def test_sync_skips_entities_without_ae(self, service, conn):
        """Customer with assigned_ae=NULL produces no signal_action."""
        conn.execute(
            "INSERT INTO customers (customer_id, normalized_name)"
            " VALUES (4, 'NoAeCo')"
        )
        prior_date = (date.today() - timedelta(days=540)).isoformat()
        conn.execute(
            "INSERT INTO spots (spot_id, customer_id, market_name,"
            " air_date, gross_rate, revenue_type)"
            " VALUES (20, 4, 'Denver', ?, 25000, 'Cash')",
            [prior_date],
        )
        conn.commit()

        service.refresh_signals(conn)

        # Signal should exist (churned)
        sig = conn.execute(
            "SELECT * FROM entity_signals"
            " WHERE entity_type='customer' AND entity_id=4"
        ).fetchone()
        assert sig is not None

        # But no signal_action (no assigned_ae)
        action = conn.execute(
            "SELECT * FROM signal_actions"
            " WHERE entity_type='customer' AND entity_id=4"
        ).fetchone()
        assert action is None
