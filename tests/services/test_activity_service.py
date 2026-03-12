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
