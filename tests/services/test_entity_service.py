"""Tests for EntityService."""

import os
import tempfile
import sqlite3

import pytest

from src.database.connection import DatabaseConnection
from src.services.entity_service import EntityService

SCHEMA = """
CREATE TABLE agencies (
    agency_id INTEGER PRIMARY KEY,
    agency_name TEXT UNIQUE,
    address TEXT, city TEXT, state TEXT, zip TEXT,
    notes TEXT,
    po_number TEXT,
    edi_billing INTEGER DEFAULT 0,
    commission_rate REAL,
    order_rate_basis TEXT,
    assigned_ae TEXT,
    is_active INTEGER DEFAULT 1
);
CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY,
    normalized_name TEXT UNIQUE,
    sector_id INTEGER,
    agency_id INTEGER,
    address TEXT, city TEXT, state TEXT, zip TEXT,
    notes TEXT,
    po_number TEXT,
    edi_billing INTEGER DEFAULT 0,
    affidavit_required INTEGER DEFAULT 0,
    commission_rate REAL,
    order_rate_basis TEXT,
    assigned_ae TEXT,
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
    broadcast_month TEXT,
    sales_person TEXT
);
CREATE TABLE sectors (
    sector_id INTEGER PRIMARY KEY,
    sector_code TEXT,
    sector_name TEXT,
    sector_group TEXT DEFAULT 'Other',
    is_active INTEGER DEFAULT 1
);
CREATE TABLE customer_sectors (
    customer_id INTEGER,
    sector_id INTEGER,
    is_primary INTEGER DEFAULT 0,
    assigned_by TEXT,
    PRIMARY KEY (customer_id, sector_id)
);
CREATE TABLE entity_contacts (
    contact_id INTEGER PRIMARY KEY,
    entity_type TEXT,
    entity_id INTEGER,
    contact_name TEXT,
    contact_title TEXT,
    email TEXT,
    phone TEXT,
    contact_role TEXT,
    is_primary INTEGER DEFAULT 0,
    last_contacted TEXT,
    created_by TEXT,
    is_active INTEGER DEFAULT 1
);
CREATE TABLE entity_addresses (
    address_id INTEGER PRIMARY KEY,
    entity_type TEXT,
    entity_id INTEGER,
    address_label TEXT,
    address TEXT,
    city TEXT, state TEXT, zip TEXT,
    is_primary INTEGER DEFAULT 0,
    notes TEXT,
    created_by TEXT,
    is_active INTEGER DEFAULT 1,
    updated_date TEXT
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
CREATE TABLE entity_aliases (
    alias_id INTEGER PRIMARY KEY,
    entity_type TEXT,
    alias_name TEXT,
    target_entity_id INTEGER,
    is_active INTEGER DEFAULT 1
);
CREATE TABLE ae_assignments (
    assignment_id INTEGER PRIMARY KEY,
    entity_type TEXT,
    entity_id INTEGER,
    ae_name TEXT,
    assigned_date TEXT DEFAULT (datetime('now')),
    ended_date TEXT,
    created_by TEXT,
    notes TEXT
);
CREATE TABLE canon_audit (
    audit_id INTEGER PRIMARY KEY,
    actor TEXT,
    action TEXT,
    key TEXT,
    value TEXT,
    extra TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);
CREATE TABLE saved_filters (
    filter_id INTEGER PRIMARY KEY,
    filter_name TEXT,
    filter_type TEXT,
    filter_config TEXT,
    created_by TEXT,
    created_date TEXT DEFAULT (datetime('now')),
    is_shared INTEGER DEFAULT 0
);
"""


@pytest.fixture()
def db_path():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    raw = sqlite3.connect(path)
    raw.executescript(SCHEMA)
    raw.commit()
    raw.close()
    yield path
    os.unlink(path)


@pytest.fixture()
def service(db_path):
    db = DatabaseConnection(db_path)
    return EntityService(db)


@pytest.fixture()
def conn(db_path):
    c = sqlite3.connect(db_path)
    c.row_factory = sqlite3.Row
    yield c
    c.close()


def _seed_agency(conn, name="Test Agency", is_active=1):
    conn.execute(
        "INSERT INTO agencies (agency_name, is_active) "
        "VALUES (?, ?)",
        [name, is_active]
    )
    conn.commit()
    return conn.execute(
        "SELECT last_insert_rowid()"
    ).fetchone()[0]


def _seed_customer(conn, name="Test Customer", is_active=1,
                   agency_id=None):
    conn.execute(
        "INSERT INTO customers "
        "(normalized_name, is_active, agency_id) "
        "VALUES (?, ?, ?)",
        [name, is_active, agency_id]
    )
    conn.commit()
    return conn.execute(
        "SELECT last_insert_rowid()"
    ).fetchone()[0]


def _seed_sector(conn, name="Auto", code="AUT",
                 group="Commercial"):
    conn.execute(
        "INSERT INTO sectors "
        "(sector_code, sector_name, sector_group) "
        "VALUES (?, ?, ?)",
        [code, name, group]
    )
    conn.commit()
    return conn.execute(
        "SELECT last_insert_rowid()"
    ).fetchone()[0]


class TestListEntities:
    def test_returns_agencies_and_customers(self, service,
                                            conn):
        _seed_agency(conn, "Alpha Agency")
        _seed_customer(conn, "Beta Customer")
        # Seed empty metrics to avoid missing table errors
        conn.execute(
            "INSERT INTO entity_metrics "
            "(entity_type, entity_id, total_revenue, "
            "spot_count, agency_spot_count) "
            "VALUES ('agency', 1, 0, 0, 0)"
        )
        conn.execute(
            "INSERT INTO entity_metrics "
            "(entity_type, entity_id, total_revenue, "
            "spot_count, agency_spot_count) "
            "VALUES ('customer', 1, 100, 5, 0)"
        )
        conn.commit()

        results = service.list_entities(conn)
        names = [r["entity_name"] for r in results]
        assert "Alpha Agency" in names
        assert "Beta Customer" in names

    def test_excludes_inactive_by_default(self, service,
                                          conn):
        _seed_agency(conn, "Active Agency")
        _seed_agency(conn, "Dead Agency", is_active=0)

        results = service.list_entities(conn)
        names = [r["entity_name"] for r in results]
        assert "Active Agency" in names
        assert "Dead Agency" not in names

    def test_includes_inactive_when_requested(self, service,
                                              conn):
        _seed_agency(conn, "Active Agency")
        _seed_agency(conn, "Dead Agency", is_active=0)

        results = service.list_entities(
            conn, include_inactive=True
        )
        names = [r["entity_name"] for r in results]
        assert "Dead Agency" in names


class TestCreateEntity:
    def test_create_customer(self, service, conn):
        result = service.create_entity(conn, {
            "entity_type": "customer",
            "name": "New Advertiser",
            "force": True
        }, "test_user")
        conn.commit()

        assert result["entity_type"] == "customer"
        assert result["entity_id"] > 0
        assert result["name"] == "New Advertiser"

        row = conn.execute(
            "SELECT normalized_name FROM customers "
            "WHERE customer_id = ?",
            [result["entity_id"]]
        ).fetchone()
        assert row["normalized_name"] == "New Advertiser"

    def test_create_agency(self, service, conn):
        result = service.create_entity(conn, {
            "entity_type": "agency",
            "name": "New Agency",
            "force": True
        }, "test_user")
        conn.commit()

        assert result["entity_type"] == "agency"
        assert result["entity_id"] > 0

    def test_duplicate_detection(self, service, conn):
        """Create entity, try fuzzy duplicate without force."""
        _seed_customer(conn, "Denver Auto Dealers")

        result = service.create_entity(conn, {
            "entity_type": "customer",
            "name": "Denver Auto Dealers Association",
        }, "test_user")

        assert result.get("needs_confirmation") is True
        assert len(result["similar_entities"]) > 0

    def test_exact_duplicate_blocked(self, service, conn):
        _seed_agency(conn, "Exact Match")

        result = service.create_entity(conn, {
            "entity_type": "agency",
            "name": "Exact Match",
            "force": True
        }, "test_user")

        assert "already exists" in result["error"]
        assert result["status"] == 409

    def test_missing_name_returns_error(self, service, conn):
        result = service.create_entity(conn, {
            "entity_type": "customer",
            "name": "",
        }, "test_user")

        assert result["error"] == "Name is required"


class TestDeactivateEntity:
    def test_deactivate_entity(self, service, conn):
        aid = _seed_agency(conn, "To Deactivate")

        result = service.deactivate_entity(
            conn, "agency", aid, "test_user"
        )
        conn.commit()

        assert result["success"] is True

        row = conn.execute(
            "SELECT is_active FROM agencies "
            "WHERE agency_id = ?", [aid]
        ).fetchone()
        assert row["is_active"] == 0

    def test_deactivate_already_inactive(self, service, conn):
        aid = _seed_agency(conn, "Already Dead", is_active=0)

        result = service.deactivate_entity(
            conn, "agency", aid, "test_user"
        )
        assert "already inactive" in result["error"]

    def test_deactivate_not_found(self, service, conn):
        result = service.deactivate_entity(
            conn, "agency", 9999, "test_user"
        )
        assert result["error"] == "Entity not found"


class TestReactivateEntity:
    def test_reactivate_entity(self, service, conn):
        aid = _seed_agency(conn, "To Reactivate",
                           is_active=0)

        result = service.reactivate_entity(
            conn, "agency", aid, "test_user"
        )
        conn.commit()

        assert result["success"] is True

        row = conn.execute(
            "SELECT is_active FROM agencies "
            "WHERE agency_id = ?", [aid]
        ).fetchone()
        assert row["is_active"] == 1


class TestUpdateBillingInfo:
    def test_validates_commission_rate(self, service, conn):
        aid = _seed_agency(conn, "Billing Test")

        result = service.update_billing_info(
            conn, "agency", aid,
            {"commission_rate": 150}
        )
        assert "0-100" in result["error"]

    def test_valid_commission_rate(self, service, conn):
        aid = _seed_agency(conn, "Billing OK")

        result = service.update_billing_info(
            conn, "agency", aid,
            {"commission_rate": 15, "edi_billing": True}
        )
        conn.commit()

        assert result["success"] is True

        row = conn.execute(
            "SELECT commission_rate, edi_billing "
            "FROM agencies WHERE agency_id = ?", [aid]
        ).fetchone()
        assert row["commission_rate"] == 15.0
        assert row["edi_billing"] == 1

    def test_invalid_order_rate_basis(self, service, conn):
        aid = _seed_agency(conn, "ORB Test")

        result = service.update_billing_info(
            conn, "agency", aid,
            {"order_rate_basis": "invalid"}
        )
        assert "gross" in result["error"]


class TestGetAgencyCustomers:
    def test_returns_linked_customers(self, service, conn):
        aid = _seed_agency(conn, "Link Agency")
        cid = _seed_customer(
            conn, "Link Customer", agency_id=aid
        )

        result = service.get_agency_customers(conn, aid)
        assert result["agency_name"] == "Link Agency"
        assert len(result["customers"]) == 1
        assert (result["customers"][0]["customer_id"]
                == cid)

    def test_agency_not_found(self, service, conn):
        result = service.get_agency_customers(conn, 9999)
        assert result["error"] == "Agency not found"


class TestUpdateAE:
    def test_creates_history(self, service, conn):
        aid = _seed_agency(conn, "AE Test Agency")

        result = service.update_ae(
            conn, "agency", aid, "Jane Doe", "admin"
        )
        conn.commit()

        assert result["success"] is True
        assert result["assigned_ae"] == "Jane Doe"

        # Check ae_assignments table
        rows = conn.execute(
            "SELECT ae_name, created_by "
            "FROM ae_assignments "
            "WHERE entity_type = 'agency' "
            "AND entity_id = ?",
            [aid]
        ).fetchall()
        assert len(rows) == 1
        assert rows[0]["ae_name"] == "Jane Doe"
        assert rows[0]["created_by"] == "admin"

    def test_ends_previous_assignment(self, service, conn):
        aid = _seed_agency(conn, "AE Switch Agency")

        service.update_ae(
            conn, "agency", aid, "First AE", "admin"
        )
        conn.commit()

        service.update_ae(
            conn, "agency", aid, "Second AE", "admin"
        )
        conn.commit()

        rows = conn.execute(
            "SELECT ae_name, ended_date "
            "FROM ae_assignments "
            "WHERE entity_type = 'agency' "
            "AND entity_id = ? "
            "ORDER BY assignment_id",
            [aid]
        ).fetchall()
        assert len(rows) == 2
        assert rows[0]["ended_date"] is not None
        assert rows[1]["ended_date"] is None

    def test_entity_not_found(self, service, conn):
        result = service.update_ae(
            conn, "agency", 9999, "AE", "admin"
        )
        assert result["error"] == "Entity not found"


class TestGetAEHistory:
    def test_returns_history(self, service, conn):
        aid = _seed_agency(conn, "History Agency")
        service.update_ae(
            conn, "agency", aid, "AE One", "admin"
        )
        conn.commit()

        history = service.get_ae_history(
            conn, "agency", aid
        )
        assert len(history) == 1
        assert history[0]["ae_name"] == "AE One"


class TestGetSectors:
    def test_returns_active_sectors(self, service, conn):
        _seed_sector(conn, "Auto", "AUT")
        _seed_sector(conn, "Health", "HLT", "Healthcare")

        sectors = service.get_sectors(conn)
        assert len(sectors) == 2
        names = [s["sector_name"] for s in sectors]
        assert "Auto" in names
        assert "Health" in names


class TestGetMarkets:
    def test_returns_distinct_markets(self, service, conn):
        cid = _seed_customer(conn, "Market Customer")
        conn.execute(
            "INSERT INTO spots "
            "(customer_id, market_name, gross_rate) "
            "VALUES (?, 'Denver', 100)",
            [cid]
        )
        conn.execute(
            "INSERT INTO spots "
            "(customer_id, market_name, gross_rate) "
            "VALUES (?, 'Denver', 200)",
            [cid]
        )
        conn.commit()

        markets = service.get_markets(conn)
        assert markets == ["Denver"]


class TestGetSpotsLink:
    def test_customer_link(self, service):
        result = service.get_spots_link("customer", 42)
        assert result["url"] == (
            "/datasette/dev/spots?customer_id=42"
        )

    def test_agency_link(self, service):
        result = service.get_spots_link("agency", 7)
        assert result["url"] == (
            "/datasette/dev/spots?agency_id=7"
        )

    def test_invalid_type(self, service):
        result = service.get_spots_link("invalid", 1)
        assert "error" in result


class TestUpdateAddress:
    def test_updates_address_fields(self, service, conn):
        aid = _seed_agency(conn, "Address Agency")

        result = service.update_address(
            conn, "agency", aid, {
                "address": "123 Main St",
                "city": "Denver",
                "state": "CO",
                "zip": "80202"
            }
        )
        conn.commit()

        assert result["success"] is True

        row = conn.execute(
            "SELECT address, city, state, zip "
            "FROM agencies WHERE agency_id = ?", [aid]
        ).fetchone()
        assert row["address"] == "123 Main St"
        assert row["city"] == "Denver"


class TestUpdateNotes:
    def test_updates_notes(self, service, conn):
        cid = _seed_customer(conn, "Notes Customer")

        result = service.update_notes(
            conn, "customer", cid, "Important note"
        )
        conn.commit()

        assert result["success"] is True

        row = conn.execute(
            "SELECT notes FROM customers "
            "WHERE customer_id = ?", [cid]
        ).fetchone()
        assert row["notes"] == "Important note"


class TestUpdateSectors:
    def test_bulk_replace_sectors(self, service, conn):
        cid = _seed_customer(conn, "Sector Customer")
        sid1 = _seed_sector(conn, "Auto", "AUT")
        sid2 = _seed_sector(conn, "Legal", "LGL")

        result = service.update_sectors(
            conn, cid,
            [
                {"sector_id": sid1, "is_primary": True},
                {"sector_id": sid2}
            ],
            "test_user"
        )
        conn.commit()

        assert result["success"] is True
        assert len(result["sectors"]) == 2

    def test_requires_exactly_one_primary(self, service,
                                          conn):
        cid = _seed_customer(conn, "Primary Test")
        sid = _seed_sector(conn, "Auto", "AUT2")

        result = service.update_sectors(
            conn, cid,
            [{"sector_id": sid}],
            "test_user"
        )
        assert "primary" in result["error"].lower()


class TestUpdateAgency:
    def test_assigns_agency(self, service, conn):
        aid = _seed_agency(conn, "Parent Agency")
        cid = _seed_customer(conn, "Child Customer")

        result = service.update_agency(conn, cid, aid)
        conn.commit()

        assert result["success"] is True
        assert result["agency_name"] == "Parent Agency"

    def test_invalid_agency(self, service, conn):
        cid = _seed_customer(conn, "Orphan Customer")

        result = service.update_agency(conn, cid, 9999)
        assert "not found" in result["error"]


class TestListEntitiesAgencyFields:
    """Test agency-related fields in list_entities response."""

    def test_customer_includes_agency_id(self, service, conn):
        """Customers linked to an agency include agency_id."""
        conn.executescript("""
            INSERT OR IGNORE INTO agencies (agency_id, agency_name, is_active)
            VALUES (100, 'Test Agency', 1);
            INSERT OR IGNORE INTO customers
                (customer_id, normalized_name, is_active, agency_id)
            VALUES (200, 'Agency Client', 1, 100);
        """)
        results = service.list_entities(conn)
        client = next(
            (r for r in results
             if r["entity_type"] == "customer"
             and r["entity_id"] == 200),
            None,
        )
        assert client is not None
        assert client["agency_id"] == 100
        assert client["agency_name"] == "Test Agency"

    def test_direct_advertiser_has_null_agency(self, service, conn):
        """Direct advertisers have agency_id=None."""
        conn.executescript("""
            INSERT OR IGNORE INTO customers
                (customer_id, normalized_name, is_active, agency_id)
            VALUES (201, 'Direct Advertiser', 1, NULL);
        """)
        results = service.list_entities(conn)
        direct = next(
            (r for r in results
             if r["entity_type"] == "customer"
             and r["entity_id"] == 201),
            None,
        )
        assert direct is not None
        assert direct["agency_id"] is None
        assert direct["agency_name"] is None

    def test_agency_includes_client_count(self, service, conn):
        """Agencies include a count of active linked customers."""
        conn.executescript("""
            INSERT OR IGNORE INTO agencies (agency_id, agency_name, is_active)
            VALUES (100, 'Test Agency', 1);
            INSERT OR IGNORE INTO customers
                (customer_id, normalized_name, is_active, agency_id)
            VALUES (200, 'Client A', 1, 100);
            INSERT OR IGNORE INTO customers
                (customer_id, normalized_name, is_active, agency_id)
            VALUES (201, 'Client B', 1, 100);
            INSERT OR IGNORE INTO customers
                (customer_id, normalized_name, is_active, agency_id)
            VALUES (202, 'Inactive Client', 0, 100);
        """)
        results = service.list_entities(conn)
        agency = next(
            (r for r in results
             if r["entity_type"] == "agency"
             and r["entity_id"] == 100),
            None,
        )
        assert agency is not None
        assert agency["client_count"] == 2
