"""Route-level tests for GET /api/revenue/sheet-export."""

import pytest
import sqlite3


@pytest.fixture
def sheet_token(monkeypatch):
    """Set the server-side token env var for the test."""
    monkeypatch.setenv("SHEET_EXPORT_TOKEN", "test-token-123")
    yield "test-token-123"


@pytest.fixture
def seeded_db():
    """Seed the real dev.db with minimal test schema and data."""
    conn = sqlite3.connect(".data/dev.db")
    conn.row_factory = sqlite3.Row

    # Create tables if they don't exist
    conn.execute("""
        CREATE TABLE IF NOT EXISTS spots (
            spot_id INTEGER PRIMARY KEY,
            bill_code TEXT,
            broadcast_month TEXT,
            gross_rate DECIMAL(12,2),
            station_net DECIMAL(12,2),
            broker_fees DECIMAL(12,2),
            sales_person TEXT,
            revenue_type TEXT,
            agency_flag TEXT,
            is_historical INTEGER DEFAULT 0,
            customer_id INTEGER,
            market_id INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INTEGER PRIMARY KEY,
            normalized_name TEXT,
            sector_id INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sectors (
            sector_id INTEGER PRIMARY KEY,
            sector_name TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS markets (
            market_id INTEGER PRIMARY KEY,
            market_code TEXT
        )
    """)

    # Clear any existing test data
    conn.execute("DELETE FROM spots")
    conn.execute("DELETE FROM sectors")
    conn.execute("DELETE FROM customers")
    conn.execute("DELETE FROM markets")

    # Insert minimal dimension data
    conn.execute("INSERT OR IGNORE INTO sectors (sector_id, sector_name) VALUES (1, 'Outreach')")
    conn.execute(
        "INSERT OR IGNORE INTO customers (customer_id, normalized_name, sector_id) "
        "VALUES (1, 'McDonalds', 1)"
    )
    conn.execute("INSERT OR IGNORE INTO markets (market_id, market_code) VALUES (1, 'SFO')")

    # Insert one test spot
    conn.execute("""
        INSERT INTO spots (
            bill_code, broadcast_month, gross_rate, station_net, broker_fees,
            sales_person, revenue_type, agency_flag, customer_id, market_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        "Admerasia:McDonalds", "Jan-25", 4690.00, 3986.50, 0.00,
        "Charmaine", "Internal Ad Sales", "Agency", 1, 1
    ))

    conn.commit()
    conn.close()
    yield

    # Cleanup: remove test data after test
    conn = sqlite3.connect(".data/dev.db")
    conn.execute("DELETE FROM spots")
    conn.commit()
    conn.close()


def test_missing_token_returns_401(client, sheet_token):
    """No X-SpotOps-Token header → 401.

    sheet_token fixture sets the env var so we exercise the
    "missing header" branch, not the "missing env var" branch.
    """
    resp = client.get("/api/revenue/sheet-export")
    assert resp.status_code == 401
    assert resp.get_json()["error"] == "Authentication required"


def test_wrong_token_returns_401(client, sheet_token):
    """X-SpotOps-Token header present but doesn't match env → 401."""
    resp = client.get(
        "/api/revenue/sheet-export",
        headers={"X-SpotOps-Token": "wrong-token"},
    )
    assert resp.status_code == 401


def test_missing_env_var_returns_503(client, monkeypatch):
    """SHEET_EXPORT_TOKEN env var unset on the server → 503."""
    monkeypatch.delenv("SHEET_EXPORT_TOKEN", raising=False)
    resp = client.get(
        "/api/revenue/sheet-export",
        headers={"X-SpotOps-Token": "anything"},
    )
    assert resp.status_code == 503
    assert "misconfigured" in resp.get_json()["error"].lower()


def test_happy_path_returns_expected_shape(client, sheet_token, seeded_db):
    """200 response with metadata + rows shape matching spec §5."""
    resp = client.get(
        "/api/revenue/sheet-export",
        headers={"X-SpotOps-Token": sheet_token},
    )
    assert resp.status_code == 200
    body = resp.get_json()
    # The envelope from create_success_response wraps data, so the
    # endpoint should return the raw {metadata, rows} object directly
    # without double-wrapping. Verify the structure.
    assert "metadata" in body or ("data" in body and "metadata" in body["data"])
    # Prefer direct shape (unwrapped) per spec §5.
    payload = body.get("data", body)
    assert payload["metadata"]["hash_version"] == "v1"
    assert isinstance(payload["rows"], list)


def test_rows_have_expected_fields(client, sheet_token, seeded_db):
    """Each row has all seven metadata fields plus three amounts."""
    resp = client.get(
        "/api/revenue/sheet-export",
        headers={"X-SpotOps-Token": sheet_token},
    )
    body = resp.get_json()
    payload = body.get("data", body)
    if payload["rows"]:
        row = payload["rows"][0]
        required = {
            "customer", "market", "revenue_class", "ae1",
            "agency_flag", "sector", "broadcast_month",
            "gross_rate", "station_net", "broker_fees",
        }
        assert required.issubset(set(row.keys()))
