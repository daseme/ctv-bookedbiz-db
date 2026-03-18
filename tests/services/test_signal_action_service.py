"""Tests for SignalActionService queue management."""

import sqlite3
from datetime import date, timedelta

import pytest

from src.services.signal_action_service import SignalActionService


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
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

CREATE TABLE entity_signals (
    entity_type TEXT NOT NULL,
    entity_id INTEGER NOT NULL,
    signal_type TEXT NOT NULL,
    signal_label TEXT,
    signal_priority INTEGER,
    trailing_revenue REAL,
    prior_revenue REAL,
    PRIMARY KEY (entity_type, entity_id, signal_type)
);

CREATE TABLE agencies (
    agency_id INTEGER PRIMARY KEY,
    agency_name TEXT,
    is_active INTEGER DEFAULT 1,
    assigned_ae TEXT
);

CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY,
    normalized_name TEXT,
    is_active INTEGER DEFAULT 1,
    assigned_ae TEXT
);
"""


@pytest.fixture
def conn():
    """In-memory SQLite connection with schema and row_factory set."""
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.executescript(SCHEMA_SQL)
    return c


@pytest.fixture
def service():
    """SignalActionService with a dummy db_connection (not used in these tests)."""

    class _FakeDB:
        pass

    return SignalActionService(_FakeDB())


def _insert_action(conn, *, entity_type="customer", entity_id=1,
                   signal_type="no_activity", assigned_ae="Alice",
                   status="new", snooze_until=None, reason=None,
                   created_date=None):
    """Helper to insert a signal_action row and return its action_id."""
    created_date = created_date or date.today().isoformat()
    conn.execute(
        """
        INSERT INTO signal_actions
            (entity_type, entity_id, signal_type, assigned_ae, status,
             snooze_until, reason, created_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [entity_type, entity_id, signal_type, assigned_ae, status,
         snooze_until, reason, created_date],
    )
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def _insert_signal(conn, *, entity_type="customer", entity_id=1,
                   signal_type="no_activity", signal_label="No Activity",
                   signal_priority=2, trailing_revenue=5000.0):
    conn.execute(
        """
        INSERT INTO entity_signals
            (entity_type, entity_id, signal_type, signal_label,
             signal_priority, trailing_revenue)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [entity_type, entity_id, signal_type, signal_label,
         signal_priority, trailing_revenue],
    )


# ---------------------------------------------------------------------------
# TestGetQueue
# ---------------------------------------------------------------------------

class TestGetQueue:
    def test_returns_new_actions_for_ae(self, conn, service):
        _insert_action(conn, assigned_ae="Alice", entity_id=1)
        _insert_action(conn, assigned_ae="Alice", entity_id=2)
        _insert_action(conn, assigned_ae="Bob", entity_id=3)

        results = service.get_queue(conn, "Alice")

        assert len(results) == 2
        assert all(r["assigned_ae"] == "Alice" for r in results)

    def test_excludes_acknowledged_and_dismissed(self, conn, service):
        _insert_action(conn, status="new", entity_id=1)
        _insert_action(conn, status="acknowledged", entity_id=2)
        _insert_action(conn, status="dismissed", entity_id=3)
        # Snoozed but not expired — should be excluded
        _insert_action(conn, status="snoozed", entity_id=4,
                       snooze_until=(date.today() + timedelta(days=1)).isoformat())

        results = service.get_queue(conn, "Alice")

        assert len(results) == 1
        assert results[0]["entity_id"] == 1

    def test_includes_expired_snooze_as_new(self, conn, service):
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        _insert_action(conn, status="snoozed", snooze_until=yesterday)

        results = service.get_queue(conn, "Alice")

        assert len(results) == 1
        assert results[0]["status"] == "new"

    def test_excludes_active_snooze(self, conn, service):
        tomorrow = (date.today() + timedelta(days=1)).isoformat()
        _insert_action(conn, status="snoozed", snooze_until=tomorrow)

        results = service.get_queue(conn, "Alice")

        assert len(results) == 0

    def test_sorted_by_priority_then_age(self, conn, service):
        # Insert entity_signals with different priorities
        _insert_signal(conn, entity_id=1, signal_type="at_risk",
                       signal_priority=1, trailing_revenue=1000.0)
        _insert_signal(conn, entity_id=2, signal_type="no_activity",
                       signal_priority=2, trailing_revenue=2000.0)
        _insert_signal(conn, entity_id=3, signal_type="no_activity",
                       signal_priority=2, trailing_revenue=3000.0)

        older = "2025-01-01"
        newer = "2025-06-01"

        _insert_action(conn, entity_id=2, signal_type="no_activity",
                       created_date=newer)
        _insert_action(conn, entity_id=1, signal_type="at_risk",
                       created_date=newer)
        _insert_action(conn, entity_id=3, signal_type="no_activity",
                       created_date=older)

        results = service.get_queue(conn, "Alice")

        # Priority 1 first, then priority 2 oldest-first
        assert results[0]["entity_id"] == 1       # priority 1
        assert results[1]["entity_id"] == 3       # priority 2, older
        assert results[2]["entity_id"] == 2       # priority 2, newer

    def test_includes_signal_label_and_revenue(self, conn, service):
        _insert_signal(conn, entity_id=1, signal_type="no_activity",
                       signal_label="No Activity", trailing_revenue=9999.0)
        _insert_action(conn, entity_id=1, signal_type="no_activity")

        results = service.get_queue(conn, "Alice")

        assert len(results) == 1
        assert results[0]["signal_label"] == "No Activity"
        assert results[0]["trailing_revenue"] == 9999.0


# ---------------------------------------------------------------------------
# TestSnooze
# ---------------------------------------------------------------------------

class TestSnooze:
    def test_snooze_sets_status_and_date(self, conn, service):
        action_id = _insert_action(conn)
        tomorrow = (date.today() + timedelta(days=7)).isoformat()

        result = service.snooze_action(conn, action_id, "Following up next week",
                                       tomorrow, "Alice")

        assert result["success"] is True
        row = conn.execute(
            "SELECT status, snooze_until, reason FROM signal_actions WHERE action_id = ?",
            [action_id],
        ).fetchone()
        assert row["status"] == "snoozed"
        assert row["snooze_until"] == tomorrow
        assert row["reason"] == "Following up next week"

    def test_snooze_requires_reason(self, conn, service):
        action_id = _insert_action(conn)
        tomorrow = (date.today() + timedelta(days=1)).isoformat()

        result = service.snooze_action(conn, action_id, "  ", tomorrow, "Alice")

        assert "error" in result

    def test_snooze_nonexistent_returns_error(self, conn, service):
        tomorrow = (date.today() + timedelta(days=1)).isoformat()

        result = service.snooze_action(conn, 9999, "reason", tomorrow, "Alice")

        assert "error" in result


# ---------------------------------------------------------------------------
# TestDismiss
# ---------------------------------------------------------------------------

class TestDismiss:
    def test_dismiss_sets_status(self, conn, service):
        action_id = _insert_action(conn)

        result = service.dismiss_action(conn, action_id, "Not our target demo",
                                        "Alice")

        assert result["success"] is True
        row = conn.execute(
            "SELECT status, reason FROM signal_actions WHERE action_id = ?",
            [action_id],
        ).fetchone()
        assert row["status"] == "dismissed"
        assert row["reason"] == "Not our target demo"

    def test_dismiss_requires_reason(self, conn, service):
        action_id = _insert_action(conn)

        result = service.dismiss_action(conn, action_id, "", "Alice")

        assert "error" in result


# ---------------------------------------------------------------------------
# TestAcknowledgeForEntity
# ---------------------------------------------------------------------------

class TestAcknowledgeForEntity:
    def test_acknowledges_all_new_actions_for_entity(self, conn, service):
        _insert_action(conn, entity_id=10, signal_type="no_activity")
        _insert_action(conn, entity_id=10, signal_type="at_risk")

        service.acknowledge_for_entity(conn, "customer", 10, "Alice")

        rows = conn.execute(
            "SELECT status FROM signal_actions WHERE entity_id = 10"
        ).fetchall()
        assert all(r["status"] == "acknowledged" for r in rows)

    def test_does_not_touch_dismissed_actions(self, conn, service):
        _insert_action(conn, entity_id=10, status="dismissed",
                       reason="already dismissed")

        service.acknowledge_for_entity(conn, "customer", 10, "Alice")

        row = conn.execute(
            "SELECT status FROM signal_actions WHERE entity_id = 10"
        ).fetchone()
        assert row["status"] == "dismissed"

    def test_acknowledges_expired_snoozed_actions(self, conn, service):
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        _insert_action(conn, entity_id=10, status="snoozed",
                       snooze_until=yesterday)

        service.acknowledge_for_entity(conn, "customer", 10, "Alice")

        row = conn.execute(
            "SELECT status FROM signal_actions WHERE entity_id = 10"
        ).fetchone()
        assert row["status"] == "acknowledged"


# ---------------------------------------------------------------------------
# TestSyncFromSignals
# ---------------------------------------------------------------------------

class TestSyncFromSignals:
    def test_creates_actions_for_new_signals(self, conn, service):
        _insert_signal(conn, entity_id=1, signal_type="no_activity")
        _insert_signal(conn, entity_id=2, signal_type="at_risk")

        ae_lookup = {("customer", 1): "Alice", ("customer", 2): "Alice"}

        service.sync_from_signals(conn, set(), ae_lookup)

        count = conn.execute(
            "SELECT COUNT(*) FROM signal_actions WHERE status = 'new'"
        ).fetchone()[0]
        assert count == 2

    def test_acknowledges_actions_for_removed_signals(self, conn, service):
        # Signal was in the before_snapshot but is NOT in current entity_signals
        before = {("customer", 5, "no_activity")}
        _insert_action(conn, entity_id=5, signal_type="no_activity",
                       assigned_ae="Alice")

        service.sync_from_signals(conn, before, {})

        row = conn.execute(
            "SELECT status FROM signal_actions WHERE entity_id = 5"
        ).fetchone()
        assert row["status"] == "acknowledged"

    def test_does_not_duplicate_existing_open_actions(self, conn, service):
        _insert_signal(conn, entity_id=1, signal_type="no_activity")
        _insert_action(conn, entity_id=1, signal_type="no_activity",
                       status="new")

        ae_lookup = {("customer", 1): "Alice"}
        service.sync_from_signals(conn, set(), ae_lookup)

        count = conn.execute(
            "SELECT COUNT(*) FROM signal_actions WHERE entity_id = 1"
        ).fetchone()[0]
        assert count == 1

    def test_reverts_expired_snoozes_before_sync(self, conn, service):
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        _insert_action(conn, entity_id=1, signal_type="no_activity",
                       status="snoozed", snooze_until=yesterday)
        # Signal still exists — appears in both snapshots, so no net change
        _insert_signal(conn, entity_id=1, signal_type="no_activity")
        before = {("customer", 1, "no_activity")}
        ae_lookup = {("customer", 1): "Alice"}

        service.sync_from_signals(conn, before, ae_lookup)

        row = conn.execute(
            "SELECT status FROM signal_actions WHERE entity_id = 1"
        ).fetchone()
        assert row["status"] == "new"
