"""Tests for SavedFilterService."""

import sqlite3
import pytest
from src.database.connection import DatabaseConnection
from src.services.saved_filter_service import SavedFilterService


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE saved_filters (
            filter_id INTEGER PRIMARY KEY AUTOINCREMENT,
            filter_name TEXT, filter_type TEXT,
            filter_config TEXT,
            created_by TEXT,
            created_date TEXT DEFAULT (datetime('now')),
            is_shared INTEGER DEFAULT 0
        )
    """)
    conn.commit()

    db_conn = DatabaseConnection.__new__(DatabaseConnection)
    db_conn.db_path = ":memory:"
    svc = SavedFilterService(db_conn)
    yield svc, conn
    conn.close()


def test_save_and_get_filters(db):
    svc, conn = db
    result = svc.save_filter(
        conn, "My Filter", {"search": "test"}, "admin",
    )
    conn.commit()
    assert result["success"] is True
    assert "filter_id" in result

    filters = svc.get_filters(conn)
    assert len(filters) == 1
    assert filters[0]["filter_name"] == "My Filter"
    assert filters[0]["filter_config"] == {"search": "test"}


def test_save_filter_empty_name(db):
    svc, conn = db
    result = svc.save_filter(conn, "", {}, "admin")
    assert "error" in result


def test_delete_filter(db):
    svc, conn = db
    create = svc.save_filter(
        conn, "Temp Filter", {}, "admin",
    )
    conn.commit()
    fid = create["filter_id"]

    result = svc.delete_filter(conn, fid)
    conn.commit()
    assert result["success"] is True

    filters = svc.get_filters(conn)
    assert len(filters) == 0


def test_shared_filter(db):
    svc, conn = db
    svc.save_filter(
        conn, "Shared", {"type": "agency"}, "admin", is_shared=True,
    )
    conn.commit()

    filters = svc.get_filters(conn)
    assert filters[0]["is_shared"] == 1
