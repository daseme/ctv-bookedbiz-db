"""Tests for AddressService."""

import sqlite3
import pytest
from src.database.connection import DatabaseConnection
from src.services.address_service import AddressService, VALID_ADDRESS_LABELS


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE entity_addresses (
            address_id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_type TEXT, entity_id INTEGER,
            address_label TEXT, address TEXT,
            city TEXT, state TEXT, zip TEXT,
            is_primary INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1,
            created_by TEXT, notes TEXT,
            updated_date TEXT
        )
    """)
    conn.commit()

    db_conn = DatabaseConnection.__new__(DatabaseConnection)
    db_conn.db_path = ":memory:"
    svc = AddressService(db_conn)
    yield svc, conn
    conn.close()


def test_create_and_get_addresses(db):
    svc, conn = db
    result = svc.create_address(
        conn, "agency", 1,
        {"address_label": "Billing", "address": "123 Main St",
         "city": "Springfield", "state": "IL", "zip": "62701"},
        "admin",
    )
    conn.commit()
    assert result["success"] is True
    assert "address_id" in result

    addresses = svc.get_addresses(conn, "agency", 1)
    assert len(addresses) == 1
    assert addresses[0]["address"] == "123 Main St"
    assert addresses[0]["address_label"] == "Billing"


def test_create_address_invalid_label(db):
    svc, conn = db
    result = svc.create_address(
        conn, "agency", 1,
        {"address_label": "InvalidLabel"},
        "admin",
    )
    assert "error" in result


def test_update_address(db):
    svc, conn = db
    create = svc.create_address(
        conn, "customer", 1,
        {"address_label": "Office", "city": "Chicago"},
        "admin",
    )
    conn.commit()
    addr_id = create["address_id"]

    result = svc.update_address(conn, addr_id, {"city": "Detroit"})
    conn.commit()
    assert result["success"] is True

    addresses = svc.get_addresses(conn, "customer", 1)
    assert addresses[0]["city"] == "Detroit"


def test_update_nonexistent_address(db):
    svc, conn = db
    result = svc.update_address(conn, 999, {"city": "Nowhere"})
    assert "error" in result
    assert result.get("status") == 404


def test_delete_address(db):
    svc, conn = db
    create = svc.create_address(
        conn, "agency", 1,
        {"address_label": "Shipping", "city": "Austin"},
        "admin",
    )
    conn.commit()
    addr_id = create["address_id"]

    result = svc.delete_address(conn, addr_id)
    conn.commit()
    assert result["success"] is True

    addresses = svc.get_addresses(conn, "agency", 1)
    assert len(addresses) == 0


def test_valid_labels_constant():
    assert "Billing" in VALID_ADDRESS_LABELS
    assert "Other" in VALID_ADDRESS_LABELS
    assert len(VALID_ADDRESS_LABELS) == 5
