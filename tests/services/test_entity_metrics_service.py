"""Tests for EntityMetricsService."""

import os
import tempfile
import sqlite3
from datetime import date, timedelta

import pytest

from src.database.connection import DatabaseConnection
from src.services.entity_metrics_service import EntityMetricsService

SCHEMA = """
CREATE TABLE agencies (
    agency_id INTEGER PRIMARY KEY,
    agency_name TEXT UNIQUE,
    is_active INTEGER DEFAULT 1
);
CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY,
    normalized_name TEXT UNIQUE,
    agency_id INTEGER,
    is_active INTEGER DEFAULT 1
);
CREATE TABLE spots (
    spot_id INTEGER PRIMARY KEY,
    agency_id INTEGER,
    customer_id INTEGER,
    market_name TEXT,
    air_date TEXT,
    gross_rate REAL DEFAULT 0,
    revenue_type TEXT,
    broadcast_month TEXT
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
"""


@pytest.fixture()
def db_path():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    conn.commit()
    conn.close()
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


def _insert_spot(conn, spot_id, customer_id=None, agency_id=None,
                 market_name="Denver", air_date="2025-06-15",
                 gross_rate=1000.0, revenue_type="Cash"):
    conn.execute(
        "INSERT INTO spots "
        "(spot_id, customer_id, agency_id, market_name, air_date, "
        "gross_rate, revenue_type) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (spot_id, customer_id, agency_id, market_name, air_date,
         gross_rate, revenue_type),
    )


class TestRefreshMetrics:
    def test_refresh_metrics_computes_from_spots(self, service, conn):
        """Total revenue excludes Trade; spot_count includes ALL spots."""
        _insert_spot(conn, 1, customer_id=10, gross_rate=5000,
                     revenue_type="Cash")
        _insert_spot(conn, 2, customer_id=10, gross_rate=3000,
                     revenue_type="Trade")
        _insert_spot(conn, 3, customer_id=10, gross_rate=2000,
                     revenue_type=None)
        conn.commit()

        service.refresh_metrics(conn)

        row = conn.execute(
            "SELECT * FROM entity_metrics "
            "WHERE entity_type='customer' AND entity_id=10"
        ).fetchone()

        assert row is not None
        # Cash(5000) + NULL-type(2000) = 7000; Trade excluded from revenue
        assert row["total_revenue"] == 7000.0
        # All 3 spots counted regardless of revenue_type
        assert row["spot_count"] == 3

    def test_agency_metrics_computed(self, service, conn):
        _insert_spot(conn, 1, agency_id=5, gross_rate=10000,
                     revenue_type="Cash")
        conn.commit()

        service.refresh_metrics(conn)

        row = conn.execute(
            "SELECT * FROM entity_metrics "
            "WHERE entity_type='agency' AND entity_id=5"
        ).fetchone()
        assert row is not None
        assert row["total_revenue"] == 10000.0
        assert row["spot_count"] == 1


class TestRefreshSignals:
    def test_refresh_signals_detects_churned(self, service, conn):
        """Prior-year revenue > $10K, zero trailing + future -> churned."""
        # 18 months ago = solidly in prior period
        prior_date = (date.today() - timedelta(days=540)).isoformat()
        _insert_spot(conn, 1, customer_id=20, gross_rate=15000,
                     air_date=prior_date)
        conn.commit()

        service.refresh_signals(conn)

        row = conn.execute(
            "SELECT * FROM entity_signals "
            "WHERE entity_type='customer' AND entity_id=20 "
            "AND signal_type='churned'"
        ).fetchone()
        assert row is not None
        assert row["signal_priority"] == 1
        assert "$15K" in row["signal_label"]

    def test_gone_quiet_suppressed_when_churned(self, service, conn):
        """Gone-quiet signal should not appear if entity is already churned."""
        # Place spots in the prior-12m window (13-23 months ago) so
        # prior_12m > $10K, trailing_12m = 0, future = 0 -> churned.
        # Also add older spots spanning many months so active_months >= 20
        # which would otherwise trigger gone_quiet.
        spot_id = 1
        for months_ago in range(13, 24):
            d = (date.today() - timedelta(days=months_ago * 30)).isoformat()
            _insert_spot(conn, spot_id, customer_id=30, gross_rate=2000,
                         air_date=d)
            spot_id += 1
        # Add older spots to push active_months_24m >= 20
        for months_ago in range(14, 25):
            d = (date.today() - timedelta(days=months_ago * 30)).isoformat()
            _insert_spot(conn, spot_id, customer_id=30, gross_rate=100,
                         air_date=d)
            spot_id += 1
        conn.commit()

        service.refresh_signals(conn)

        signals = conn.execute(
            "SELECT signal_type FROM entity_signals "
            "WHERE entity_type='customer' AND entity_id=30"
        ).fetchall()
        types = [r["signal_type"] for r in signals]
        # Should have churned but NOT gone_quiet
        assert "churned" in types
        assert "gone_quiet" not in types

    def test_growing_signal(self, service, conn):
        """Trailing > 130% of prior triggers growing signal."""
        prior_date = (date.today() - timedelta(days=540)).isoformat()
        recent_date = (date.today() - timedelta(days=30)).isoformat()
        _insert_spot(conn, 1, customer_id=40, gross_rate=10000,
                     air_date=prior_date)
        _insert_spot(conn, 2, customer_id=40, gross_rate=20000,
                     air_date=recent_date)
        conn.commit()

        service.refresh_signals(conn)

        row = conn.execute(
            "SELECT * FROM entity_signals "
            "WHERE entity_type='customer' AND entity_id=40 "
            "AND signal_type='growing'"
        ).fetchone()
        assert row is not None
        assert "+100%" in row["signal_label"]


class TestAutoRefreshIfEmpty:
    def test_auto_refresh_if_empty_populates_cache(self, service, conn):
        _insert_spot(conn, 1, customer_id=50, gross_rate=5000)
        conn.commit()

        # Table starts empty
        count = conn.execute(
            "SELECT COUNT(*) FROM entity_metrics"
        ).fetchone()[0]
        assert count == 0

        service.auto_refresh_if_empty(conn)

        count = conn.execute(
            "SELECT COUNT(*) FROM entity_metrics"
        ).fetchone()[0]
        assert count > 0


class TestRefreshMetricsForIds:
    def test_refresh_metrics_for_specific_ids(self, service, conn):
        """Targeted refresh only updates specified entities."""
        _insert_spot(conn, 1, customer_id=60, gross_rate=1000)
        _insert_spot(conn, 2, customer_id=70, gross_rate=2000)
        conn.commit()

        # Full refresh first
        service.refresh_metrics(conn)
        # Verify both exist
        assert conn.execute(
            "SELECT COUNT(*) FROM entity_metrics"
        ).fetchone()[0] == 2

        # Now update only customer 60's spots
        conn.execute(
            "UPDATE spots SET gross_rate=9999 WHERE customer_id=60"
        )
        conn.commit()

        service.refresh_metrics_for_ids(conn, customer_ids=[60])

        row60 = conn.execute(
            "SELECT total_revenue FROM entity_metrics "
            "WHERE entity_type='customer' AND entity_id=60"
        ).fetchone()
        row70 = conn.execute(
            "SELECT total_revenue FROM entity_metrics "
            "WHERE entity_type='customer' AND entity_id=70"
        ).fetchone()
        assert row60["total_revenue"] == 9999.0
        # Customer 70 unchanged
        assert row70["total_revenue"] == 2000.0


class TestGetMaps:
    def test_get_metrics_map(self, service, conn):
        _insert_spot(conn, 1, customer_id=80, gross_rate=5000)
        _insert_spot(conn, 2, agency_id=3, gross_rate=8000)
        conn.commit()
        service.refresh_metrics(conn)

        m = service.get_metrics_map(conn)
        assert ("customer", 80) in m
        assert ("agency", 3) in m
        assert m[("customer", 80)]["total_revenue"] == 5000.0

    def test_get_entity_signals(self, service, conn):
        prior_date = (date.today() - timedelta(days=540)).isoformat()
        _insert_spot(conn, 1, customer_id=90, gross_rate=15000,
                     air_date=prior_date)
        conn.commit()
        service.refresh_signals(conn)

        signals = service.get_entity_signals(conn, "customer", 90)
        assert isinstance(signals, list)
        assert len(signals) >= 1
        assert signals[0]["signal_type"] == "churned"
