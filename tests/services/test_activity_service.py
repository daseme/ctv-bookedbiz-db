"""Tests for ActivityService."""

import sqlite3
import pytest
from src.database.connection import DatabaseConnection
from src.services.activity_service import ActivityService, VALID_ACTIVITY_TYPES


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE agencies (
            agency_id INTEGER PRIMARY KEY,
            agency_name TEXT, is_active INTEGER DEFAULT 1
        )
    """)
    conn.execute("""
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            normalized_name TEXT, is_active INTEGER DEFAULT 1
        )
    """)
    conn.execute("""
        CREATE TABLE entity_activity (
            activity_id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_type TEXT, entity_id INTEGER,
            activity_type TEXT, description TEXT,
            activity_date TEXT DEFAULT (datetime('now')),
            created_by TEXT,
            created_date TEXT DEFAULT (datetime('now')),
            contact_id INTEGER, due_date TEXT,
            is_completed INTEGER DEFAULT 0,
            completed_date TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE entity_contacts (
            contact_id INTEGER PRIMARY KEY,
            contact_name TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE signal_actions (
            action_id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_type TEXT NOT NULL,
            entity_id INTEGER NOT NULL,
            signal_type TEXT NOT NULL,
            assigned_ae TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'new',
            reason TEXT,
            snooze_until DATE,
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_by TEXT
        )
    """)
    conn.execute(
        "INSERT INTO agencies (agency_id, agency_name) VALUES (1, 'Test Agency')"
    )
    conn.execute(
        "INSERT INTO customers (customer_id, normalized_name) "
        "VALUES (1, 'Test Customer')"
    )
    conn.commit()

    db_conn = DatabaseConnection.__new__(DatabaseConnection)
    db_conn.db_path = ":memory:"
    svc = ActivityService(db_conn)
    yield svc, conn
    conn.close()


def test_create_and_get_activities(db):
    svc, conn = db
    result = svc.create_activity(
        conn, "agency", 1, "note", "Called about renewal",
        "admin", contact_id=None, due_date=None,
    )
    conn.commit()
    assert result["success"] is True
    assert "activity_id" in result

    activities = svc.get_activities(conn, "agency", 1)
    assert len(activities) == 1
    assert activities[0]["activity_type"] == "note"
    assert activities[0]["description"] == "Called about renewal"


def test_create_activity_invalid_type(db):
    svc, conn = db
    result = svc.create_activity(
        conn, "agency", 1, "invalid_type", "desc", "admin",
    )
    assert "error" in result
    assert "Invalid activity_type" in result["error"]


def test_create_follow_up_requires_due_date(db):
    svc, conn = db
    result = svc.create_activity(
        conn, "agency", 1, "follow_up", "Follow up", "admin",
    )
    assert "error" in result
    assert "due_date" in result["error"]


def test_toggle_completion(db):
    svc, conn = db
    create_result = svc.create_activity(
        conn, "agency", 1, "follow_up", "Follow up call",
        "admin", due_date="2026-04-01",
    )
    conn.commit()
    activity_id = create_result["activity_id"]

    result = svc.toggle_completion(conn, activity_id)
    conn.commit()
    assert result["success"] is True
    assert result["is_completed"] == 1

    result2 = svc.toggle_completion(conn, activity_id)
    conn.commit()
    assert result2["is_completed"] == 0


def test_toggle_non_followup_fails(db):
    svc, conn = db
    svc.create_activity(
        conn, "agency", 1, "note", "A note", "admin",
    )
    conn.commit()
    activity_id = conn.execute(
        "SELECT activity_id FROM entity_activity"
    ).fetchone()[0]

    result = svc.toggle_completion(conn, activity_id)
    assert "error" in result


def test_get_follow_ups(db):
    svc, conn = db
    svc.create_activity(
        conn, "agency", 1, "follow_up", "Overdue task",
        "admin", due_date="2025-01-01",
    )
    svc.create_activity(
        conn, "customer", 1, "follow_up", "Future task",
        "admin", due_date="2099-12-31",
    )
    conn.commit()

    follow_ups = svc.get_follow_ups(conn)
    assert len(follow_ups) >= 2
    urgencies = {f["description"]: f["urgency"] for f in follow_ups}
    assert urgencies["Overdue task"] == "overdue"
    assert urgencies["Future task"] == "upcoming"


def test_create_activity_entity_not_found(db):
    svc, conn = db
    result = svc.create_activity(
        conn, "agency", 999, "note", "desc", "admin",
    )
    assert "error" in result
    assert result.get("status") == 404


@pytest.fixture
def ae_db():
    """DB with assigned_ae columns and seed data for AE filtering."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE agencies (
            agency_id INTEGER PRIMARY KEY,
            agency_name TEXT, assigned_ae TEXT,
            is_active INTEGER DEFAULT 1
        );
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            normalized_name TEXT, agency_id INTEGER,
            assigned_ae TEXT, is_active INTEGER DEFAULT 1
        );
        CREATE TABLE entity_contacts (
            contact_id INTEGER PRIMARY KEY,
            contact_name TEXT
        );
        CREATE TABLE entity_activity (
            activity_id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_type TEXT, entity_id INTEGER,
            activity_type TEXT, description TEXT,
            activity_date TEXT DEFAULT (datetime('now')),
            created_by TEXT,
            created_date TEXT DEFAULT (datetime('now')),
            contact_id INTEGER, due_date TEXT,
            is_completed INTEGER DEFAULT 0,
            completed_date TEXT
        );

        INSERT INTO agencies (agency_id, agency_name, assigned_ae)
            VALUES (1, 'Agency Alpha', 'Alice');
        INSERT INTO customers (customer_id, normalized_name, assigned_ae)
            VALUES (10, 'Customer One', 'Alice'),
                   (20, 'Customer Two', 'Bob');

        -- Alice's follow-ups
        INSERT INTO entity_activity
            (entity_type, entity_id, activity_type, description,
             created_by, due_date, is_completed)
        VALUES
            ('customer', 10, 'follow_up', 'Call about renewal',
             'Alice', '2026-03-15', 0),
            ('agency', 1, 'follow_up', 'Review Q2 plan',
             'Alice', '2026-03-10', 0);

        -- Bob's follow-up
        INSERT INTO entity_activity
            (entity_type, entity_id, activity_type, description,
             created_by, due_date, is_completed)
        VALUES
            ('customer', 20, 'follow_up', 'Send proposal',
             'Bob', '2026-03-12', 0);
    """)
    conn.commit()

    db_conn = DatabaseConnection.__new__(DatabaseConnection)
    db_conn.db_path = ":memory:"
    svc = ActivityService(db_conn)
    yield svc, conn
    conn.close()


class TestGetFollowUpsAeFilter:
    """Test get_follow_ups with optional ae_name parameter."""

    def test_no_filter_returns_all(self, ae_db):
        svc, conn = ae_db
        results = svc.get_follow_ups(conn)
        assert len(results) == 3

    def test_filter_by_ae_returns_only_assigned(self, ae_db):
        svc, conn = ae_db
        results = svc.get_follow_ups(conn, ae_name="Alice")
        assert len(results) == 2
        names = {r["entity_name"] for r in results}
        assert names == {"Customer One", "Agency Alpha"}

    def test_filter_by_ae_excludes_others(self, ae_db):
        svc, conn = ae_db
        results = svc.get_follow_ups(conn, ae_name="Bob")
        assert len(results) == 1
        assert results[0]["entity_name"] == "Customer Two"

    def test_filter_nonexistent_ae_returns_empty(self, ae_db):
        svc, conn = ae_db
        results = svc.get_follow_ups(conn, ae_name="Nobody")
        assert len(results) == 0


class _FakeDbConnection:
    """Wraps a raw sqlite3 connection to provide connection()/connection_ro()."""

    def __init__(self, conn):
        self._conn = conn

    class _ContextWrapper:
        def __init__(self, conn):
            self._conn = conn
        def __enter__(self):
            return self._conn
        def __exit__(self, *args):
            pass

    def connection(self):
        return self._ContextWrapper(self._conn)

    def connection_ro(self):
        return self._ContextWrapper(self._conn)


@pytest.fixture
def activity_db():
    """DB with assigned_ae columns for cross-account activity tests."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE agencies (
            agency_id INTEGER PRIMARY KEY,
            agency_name TEXT, assigned_ae TEXT,
            is_active INTEGER DEFAULT 1
        );
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            normalized_name TEXT, agency_id INTEGER,
            assigned_ae TEXT, is_active INTEGER DEFAULT 1
        );
        CREATE TABLE entity_contacts (
            contact_id INTEGER PRIMARY KEY,
            contact_name TEXT
        );
        CREATE TABLE entity_activity (
            activity_id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_type TEXT, entity_id INTEGER,
            activity_type TEXT, description TEXT,
            activity_date TEXT DEFAULT (datetime('now')),
            created_by TEXT,
            created_date TEXT DEFAULT (datetime('now')),
            contact_id INTEGER, due_date TEXT,
            is_completed INTEGER DEFAULT 0,
            completed_date TEXT
        );

        INSERT INTO agencies (agency_id, agency_name, assigned_ae)
            VALUES (1, 'Agency Alpha', 'Alice');
        INSERT INTO customers (customer_id, normalized_name, assigned_ae)
            VALUES (10, 'Customer One', 'Alice'),
                   (20, 'Customer Two', 'Bob');
    """)
    conn.commit()
    fake_db = _FakeDbConnection(conn)
    yield fake_db
    conn.close()


@pytest.fixture
def activity_service(activity_db):
    db_conn = DatabaseConnection.__new__(DatabaseConnection)
    db_conn.db_path = ":memory:"
    return ActivityService(db_conn)


class TestGetRecentActivityForAe:
    """Test cross-account recent activity for an AE."""

    def _seed_activities(self, activity_db):
        with activity_db.connection() as conn:
            conn.execute("""
                INSERT INTO entity_activity
                    (entity_type, entity_id, activity_type,
                     description, created_by, activity_date)
                VALUES
                    ('customer', 10, 'note', 'Called client',
                     'Alice', '2026-03-12 10:00:00'),
                    ('customer', 10, 'email', 'Sent proposal',
                     'Alice', '2026-03-11 09:00:00'),
                    ('agency', 1, 'meeting', 'Quarterly review',
                     'Alice', '2026-03-10 14:00:00'),
                    ('customer', 20, 'call', 'Bob activity',
                     'Bob', '2026-03-12 11:00:00')
            """)
            conn.commit()

    def test_returns_only_ae_activities(
        self, activity_service, activity_db
    ):
        self._seed_activities(activity_db)
        with activity_db.connection_ro() as conn:
            results = activity_service.get_recent_activity_for_ae(
                conn, ae_name="Alice"
            )
        entity_names = {r["entity_name"] for r in results}
        assert "Customer Two" not in entity_names
        assert "Customer One" in entity_names

    def test_respects_limit(self, activity_service, activity_db):
        self._seed_activities(activity_db)
        with activity_db.connection_ro() as conn:
            results = activity_service.get_recent_activity_for_ae(
                conn, ae_name="Alice", limit=2
            )
        assert len(results) <= 2

    def test_ordered_by_date_desc(self, activity_service, activity_db):
        self._seed_activities(activity_db)
        with activity_db.connection_ro() as conn:
            results = activity_service.get_recent_activity_for_ae(
                conn, ae_name="Alice"
            )
        dates = [r["activity_date"] for r in results]
        assert dates == sorted(dates, reverse=True)

    def test_includes_entity_name(self, activity_service, activity_db):
        self._seed_activities(activity_db)
        with activity_db.connection_ro() as conn:
            results = activity_service.get_recent_activity_for_ae(
                conn, ae_name="Alice"
            )
        for r in results:
            assert r["entity_name"] is not None


class TestAutoAcknowledgeOnActivity:
    """Test that logging qualifying activities auto-acknowledges signals."""

    @pytest.fixture
    def ack_db(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript("""
            CREATE TABLE agencies (
                agency_id INTEGER PRIMARY KEY,
                agency_name TEXT, is_active INTEGER DEFAULT 1
            );
            CREATE TABLE customers (
                customer_id INTEGER PRIMARY KEY,
                normalized_name TEXT, is_active INTEGER DEFAULT 1
            );
            CREATE TABLE entity_contacts (
                contact_id INTEGER PRIMARY KEY, contact_name TEXT
            );
            CREATE TABLE entity_activity (
                activity_id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_type TEXT, entity_id INTEGER,
                activity_type TEXT, description TEXT,
                activity_date TEXT DEFAULT (datetime('now')),
                created_by TEXT,
                created_date TEXT DEFAULT (datetime('now')),
                contact_id INTEGER, due_date TEXT,
                is_completed INTEGER DEFAULT 0,
                completed_date TEXT
            );
            CREATE TABLE signal_actions (
                action_id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_type TEXT NOT NULL,
                entity_id INTEGER NOT NULL,
                signal_type TEXT NOT NULL,
                assigned_ae TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'new',
                reason TEXT, snooze_until DATE,
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_by TEXT
            );
            INSERT INTO customers VALUES (1, 'Test Customer', 1);
            INSERT INTO agencies VALUES (1, 'Test Agency', 1);
        """)
        conn.commit()
        db_conn = DatabaseConnection.__new__(DatabaseConnection)
        db_conn.db_path = ":memory:"
        svc = ActivityService(db_conn)
        yield svc, conn
        conn.close()

    def _seed_signal(self, conn, entity_type="customer", entity_id=1,
                     signal_type="churned", status="new"):
        conn.execute(
            "INSERT INTO signal_actions "
            "(entity_type, entity_id, signal_type, assigned_ae, status) "
            "VALUES (?, ?, ?, 'Alice', ?)",
            [entity_type, entity_id, signal_type, status],
        )
        conn.commit()

    def test_logging_call_acknowledges_signal(self, ack_db):
        svc, conn = ack_db
        self._seed_signal(conn)
        svc.create_activity(
            conn, "customer", 1, "call", "Called client", "Alice",
        )
        row = conn.execute(
            "SELECT status FROM signal_actions WHERE action_id = 1"
        ).fetchone()
        assert row["status"] == "acknowledged"

    def test_logging_note_acknowledges_signal(self, ack_db):
        svc, conn = ack_db
        self._seed_signal(conn)
        svc.create_activity(
            conn, "customer", 1, "note", "Left a note", "Alice",
        )
        row = conn.execute(
            "SELECT status FROM signal_actions WHERE action_id = 1"
        ).fetchone()
        assert row["status"] == "acknowledged"

    def test_status_change_does_not_acknowledge(self, ack_db):
        svc, conn = ack_db
        self._seed_signal(conn)
        svc.create_activity(
            conn, "customer", 1, "status_change", "Changed status", "system",
        )
        row = conn.execute(
            "SELECT status FROM signal_actions WHERE action_id = 1"
        ).fetchone()
        assert row["status"] == "new"

    def test_follow_up_does_not_acknowledge(self, ack_db):
        svc, conn = ack_db
        self._seed_signal(conn)
        svc.create_activity(
            conn, "customer", 1, "follow_up", "Set reminder", "Alice",
            due_date="2026-04-01",
        )
        row = conn.execute(
            "SELECT status FROM signal_actions WHERE action_id = 1"
        ).fetchone()
        assert row["status"] == "new"

    def test_acknowledge_does_not_touch_dismissed(self, ack_db):
        svc, conn = ack_db
        self._seed_signal(conn, status="dismissed")
        svc.create_activity(
            conn, "customer", 1, "call", "Called again", "Alice",
        )
        row = conn.execute(
            "SELECT status FROM signal_actions WHERE action_id = 1"
        ).fetchone()
        assert row["status"] == "dismissed"

    def test_no_signal_actions_no_error(self, ack_db):
        svc, conn = ack_db
        result = svc.create_activity(
            conn, "customer", 1, "call", "Just a call", "Alice",
        )
        assert result["success"] is True
