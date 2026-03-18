"""Tests for SQLite triggers that auto-backfill spots on alias changes and spot ingestion."""

import sqlite3
import tempfile
import os

import pytest


MIGRATION_PATHS = [
    "sql/migrations/022_alias_spot_backfill_triggers.sql",
    "sql/migrations/023_spot_insert_alias_lookup_trigger.sql",
]

SCHEMA = """
CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY,
    normalized_name TEXT NOT NULL,
    is_active BOOLEAN DEFAULT 1
);

CREATE TABLE spots (
    spot_id INTEGER PRIMARY KEY AUTOINCREMENT,
    bill_code TEXT NOT NULL,
    customer_id INTEGER,
    air_date DATE NOT NULL
);

CREATE TABLE entity_aliases (
    alias_id INTEGER PRIMARY KEY AUTOINCREMENT,
    alias_name TEXT NOT NULL,
    entity_type TEXT NOT NULL CHECK (entity_type IN ('customer', 'agency')),
    target_entity_id INTEGER NOT NULL,
    confidence_score INTEGER DEFAULT 100,
    created_by TEXT NOT NULL,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT 1,
    notes TEXT,
    UNIQUE(alias_name, entity_type)
);
"""


@pytest.fixture()
def db():
    """Create a temp DB with schema and triggers applied."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    for migration in MIGRATION_PATHS:
        with open(migration) as f:
            conn.executescript(f.read())
    conn.execute(
        "INSERT INTO customers (customer_id, normalized_name) "
        "VALUES (1, 'acme corp')"
    )
    conn.execute(
        "INSERT INTO customers (customer_id, normalized_name) "
        "VALUES (2, 'globex inc')"
    )
    conn.commit()
    yield conn
    conn.close()
    os.unlink(path)


def _insert_spot(conn, bill_code, customer_id=None):
    conn.execute(
        "INSERT INTO spots (bill_code, air_date, customer_id) "
        "VALUES (?, '2025-01-15', ?)",
        (bill_code, customer_id),
    )
    conn.commit()


def _get_spot_customer_id(conn, bill_code):
    row = conn.execute(
        "SELECT customer_id FROM spots WHERE bill_code = ?",
        (bill_code,),
    ).fetchone()
    return row[0] if row else None


class TestInsertTrigger:
    """AFTER INSERT trigger on entity_aliases."""

    def test_backfills_null_customer_id(self, db):
        _insert_spot(db, "ACME-001")
        db.execute(
            "INSERT INTO entity_aliases "
            "(alias_name, entity_type, target_entity_id, created_by) "
            "VALUES ('ACME-001', 'customer', 1, 'test')"
        )
        db.commit()
        assert _get_spot_customer_id(db, "ACME-001") == 1

    def test_does_not_overwrite_existing_customer_id(self, db):
        _insert_spot(db, "ACME-001", customer_id=2)
        db.execute(
            "INSERT INTO entity_aliases "
            "(alias_name, entity_type, target_entity_id, created_by) "
            "VALUES ('ACME-001', 'customer', 1, 'test')"
        )
        db.commit()
        assert _get_spot_customer_id(db, "ACME-001") == 2

    def test_inactive_alias_does_not_backfill(self, db):
        _insert_spot(db, "ACME-001")
        db.execute(
            "INSERT INTO entity_aliases "
            "(alias_name, entity_type, target_entity_id, created_by, "
            "is_active) "
            "VALUES ('ACME-001', 'customer', 1, 'test', 0)"
        )
        db.commit()
        assert _get_spot_customer_id(db, "ACME-001") is None

    def test_agency_alias_does_not_backfill_spots(self, db):
        _insert_spot(db, "AGENCY-X")
        db.execute(
            "INSERT INTO entity_aliases "
            "(alias_name, entity_type, target_entity_id, created_by) "
            "VALUES ('AGENCY-X', 'agency', 1, 'test')"
        )
        db.commit()
        assert _get_spot_customer_id(db, "AGENCY-X") is None

    def test_backfills_multiple_spots_same_bill_code(self, db):
        for _ in range(3):
            _insert_spot(db, "ACME-001")
        db.execute(
            "INSERT INTO entity_aliases "
            "(alias_name, entity_type, target_entity_id, created_by) "
            "VALUES ('ACME-001', 'customer', 1, 'test')"
        )
        db.commit()
        rows = db.execute(
            "SELECT customer_id FROM spots WHERE bill_code = 'ACME-001'"
        ).fetchall()
        assert all(row[0] == 1 for row in rows)
        assert len(rows) == 3


class TestUpdateTrigger:
    """AFTER UPDATE trigger on entity_aliases."""

    def test_reactivation_backfills(self, db):
        _insert_spot(db, "ACME-001")
        db.execute(
            "INSERT INTO entity_aliases "
            "(alias_name, entity_type, target_entity_id, created_by, "
            "is_active) "
            "VALUES ('ACME-001', 'customer', 1, 'test', 0)"
        )
        db.commit()
        assert _get_spot_customer_id(db, "ACME-001") is None

        db.execute(
            "UPDATE entity_aliases SET is_active = 1 "
            "WHERE alias_name = 'ACME-001' AND entity_type = 'customer'"
        )
        db.commit()
        assert _get_spot_customer_id(db, "ACME-001") == 1

    def test_target_change_backfills(self, db):
        _insert_spot(db, "ACME-001")
        db.execute(
            "INSERT INTO entity_aliases "
            "(alias_name, entity_type, target_entity_id, created_by) "
            "VALUES ('ACME-001', 'customer', 1, 'test')"
        )
        db.commit()
        # Second spot gets customer_id=1 immediately via spot insert trigger
        _insert_spot(db, "ACME-001")

        # Change target — only affects spots with NULL customer_id
        db.execute(
            "UPDATE entity_aliases SET target_entity_id = 2 "
            "WHERE alias_name = 'ACME-001' AND entity_type = 'customer'"
        )
        db.commit()
        rows = db.execute(
            "SELECT customer_id FROM spots "
            "WHERE bill_code = 'ACME-001' ORDER BY spot_id"
        ).fetchall()
        # Both spots already have customer_id=1 (no NULL spots to update)
        assert rows[0][0] == 1
        assert rows[1][0] == 1

    def test_no_op_update_does_not_trigger(self, db):
        """Updating unrelated fields should not trigger backfill."""
        _insert_spot(db, "ACME-001")
        db.execute(
            "INSERT INTO entity_aliases "
            "(alias_name, entity_type, target_entity_id, created_by, "
            "is_active) "
            "VALUES ('ACME-001', 'customer', 1, 'test', 0)"
        )
        db.commit()

        # Update notes only — is_active stays 0
        db.execute(
            "UPDATE entity_aliases SET notes = 'updated' "
            "WHERE alias_name = 'ACME-001' AND entity_type = 'customer'"
        )
        db.commit()
        assert _get_spot_customer_id(db, "ACME-001") is None


class TestSpotInsertTrigger:
    """AFTER INSERT trigger on spots — looks up existing aliases."""

    def _insert_alias(self, conn, alias_name, target_id, is_active=1):
        conn.execute(
            "INSERT INTO entity_aliases "
            "(alias_name, entity_type, target_entity_id, created_by, "
            "is_active) "
            "VALUES (?, 'customer', ?, 'test', ?)",
            (alias_name, target_id, is_active),
        )
        conn.commit()

    def test_spot_gets_customer_from_existing_alias(self, db):
        self._insert_alias(db, "ACME-001", 1)
        _insert_spot(db, "ACME-001")
        assert _get_spot_customer_id(db, "ACME-001") == 1

    def test_spot_without_alias_stays_null(self, db):
        _insert_spot(db, "UNKNOWN-001")
        assert _get_spot_customer_id(db, "UNKNOWN-001") is None

    def test_spot_with_explicit_customer_id_not_overwritten(self, db):
        self._insert_alias(db, "ACME-001", 1)
        _insert_spot(db, "ACME-001", customer_id=2)
        assert _get_spot_customer_id(db, "ACME-001") == 2

    def test_inactive_alias_not_used(self, db):
        self._insert_alias(db, "ACME-001", 1, is_active=0)
        _insert_spot(db, "ACME-001")
        assert _get_spot_customer_id(db, "ACME-001") is None

    def test_multiple_spots_each_resolved(self, db):
        self._insert_alias(db, "ACME-001", 1)
        for _ in range(3):
            _insert_spot(db, "ACME-001")
        rows = db.execute(
            "SELECT customer_id FROM spots WHERE bill_code = 'ACME-001'"
        ).fetchall()
        assert all(row[0] == 1 for row in rows)
        assert len(rows) == 3
