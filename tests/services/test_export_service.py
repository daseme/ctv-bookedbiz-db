"""Tests for ExportService."""

import sqlite3
import pytest
from src.database.connection import DatabaseConnection
from src.services.export_service import ExportService, VALID_IMPORT_ROLES


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE agencies (
            agency_id INTEGER PRIMARY KEY,
            agency_name TEXT, address TEXT, city TEXT,
            state TEXT, zip TEXT, notes TEXT,
            assigned_ae TEXT, po_number TEXT,
            edi_billing INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1
        )
    """)
    conn.execute("""
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            normalized_name TEXT, address TEXT, city TEXT,
            state TEXT, zip TEXT, notes TEXT,
            assigned_ae TEXT, po_number TEXT,
            edi_billing INTEGER DEFAULT 0,
            sector_id INTEGER, agency_id INTEGER,
            is_active INTEGER DEFAULT 1
        )
    """)
    conn.execute("""
        CREATE TABLE sectors (
            sector_id INTEGER PRIMARY KEY,
            sector_name TEXT, sector_code TEXT,
            is_active INTEGER DEFAULT 1
        )
    """)
    conn.execute("""
        CREATE TABLE entity_contacts (
            contact_id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_type TEXT, entity_id INTEGER,
            contact_name TEXT, contact_title TEXT,
            email TEXT, phone TEXT,
            contact_role TEXT, is_primary INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1,
            created_by TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE customer_sectors (
            customer_id INTEGER, sector_id INTEGER,
            PRIMARY KEY (customer_id, sector_id)
        )
    """)
    conn.execute(
        "INSERT INTO agencies VALUES (1, 'Acme Agency', '123 Main', "
        "'NYC', 'NY', '10001', NULL, 'John', NULL, 0, 1)"
    )
    conn.execute(
        "INSERT INTO customers VALUES (1, 'Direct Corp', '456 Oak', "
        "'LA', 'CA', '90001', NULL, 'Jane', NULL, 0, NULL, NULL, 1)"
    )
    conn.execute(
        "INSERT INTO customers VALUES (2, 'Acme:Sub Client', NULL, "
        "NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL, 1, 1)"
    )
    conn.commit()

    db_conn = DatabaseConnection.__new__(DatabaseConnection)
    db_conn.db_path = ":memory:"
    svc = ExportService(db_conn)
    yield svc, conn
    conn.close()


def test_export_csv_basic(db):
    svc, conn = db
    metrics_map = {}
    csv_content = svc.export_entities_csv(
        conn, {"type": "all"}, metrics_map,
    )
    assert "Entity Name" in csv_content
    assert "Acme Agency" in csv_content
    assert "Direct Corp" in csv_content
    # Agency sub-client excluded (has ':' in name)
    assert "Acme:Sub Client" not in csv_content


def test_export_csv_filter_by_type(db):
    svc, conn = db
    csv_agency = svc.export_entities_csv(
        conn, {"type": "agency"}, {},
    )
    assert "Acme Agency" in csv_agency
    assert "Direct Corp" not in csv_agency

    csv_customer = svc.export_entities_csv(
        conn, {"type": "customer"}, {},
    )
    assert "Acme Agency" not in csv_customer
    assert "Direct Corp" in csv_customer


def test_import_contacts_csv(db):
    svc, conn = db
    csv_content = (
        "Entity Name,Type,Contact Name,Title,Email,Phone,Role\n"
        "Acme Agency,agency,Bob Smith,VP,bob@acme.com,555-1234,decision_maker\n"
        "Direct Corp,customer,Jane Doe,,jane@direct.com,,\n"
        "Missing Entity,agency,Nobody,,,, \n"
    )
    result = svc.import_contacts_csv(conn, csv_content, "test_import")
    conn.commit()

    assert result["imported"] == 2
    assert result["skipped"] == 1
    assert any("Missing Entity" in e for e in result["errors"])

    contacts = conn.execute(
        "SELECT * FROM entity_contacts"
    ).fetchall()
    assert len(contacts) == 2


def test_import_missing_columns(db):
    svc, conn = db
    csv_content = "Name,Phone\nTest,555\n"
    result = svc.import_contacts_csv(conn, csv_content, "admin")
    assert "error" in result


def test_valid_import_roles():
    assert "decision_maker" in VALID_IMPORT_ROLES
    assert "billing" in VALID_IMPORT_ROLES
