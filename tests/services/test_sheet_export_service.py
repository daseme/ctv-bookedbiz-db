"""Unit tests for SheetExportService."""

import sqlite3
from contextlib import contextmanager

import pytest
from src.services.sheet_export_service import SheetExportService


class _FakeDB:
    """Test helper: wraps a single sqlite3.Connection so repeated
    `.connection()` calls yield the same DB. `DatabaseConnection` opens
    a fresh connection on every call, which doesn't work for in-memory
    SQLite (each connection = a separate empty DB).
    """

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    @contextmanager
    def connection(self):
        yield self._conn


@pytest.fixture
def db():
    """In-memory SQLite with the minimum schema the service needs."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE spots (
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
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            normalized_name TEXT,
            sector_id INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE sectors (
            sector_id INTEGER PRIMARY KEY,
            sector_name TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE markets (
            market_id INTEGER PRIMARY KEY,
            market_code TEXT
        )
    """)
    conn.commit()
    return _FakeDB(conn)


def _insert_spot(conn, **overrides):
    """Insert a spot with sensible defaults. Returns the inserted row id."""
    defaults = dict(
        bill_code="Admerasia:McDonalds",
        broadcast_month="Jan-25",
        gross_rate=4690.00,
        station_net=3986.50,
        broker_fees=0.00,
        sales_person="Charmaine",
        revenue_type="Internal Ad Sales",
        agency_flag="Agency",
        is_historical=0,
        customer_id=1,
        market_id=1,
    )
    defaults.update(overrides)
    cols = ",".join(defaults.keys())
    placeholders = ",".join("?" * len(defaults))
    conn.execute(
        f"INSERT INTO spots ({cols}) VALUES ({placeholders})",
        list(defaults.values()),
    )


def _seed_dims(conn):
    """Seed minimal dimension rows matching the spot defaults."""
    conn.execute("INSERT INTO sectors (sector_id, sector_name) VALUES (1, 'Outreach')")
    conn.execute(
        "INSERT INTO customers (customer_id, normalized_name, sector_id) "
        "VALUES (1, 'McDonalds', 1)"
    )
    conn.execute("INSERT INTO markets (market_id, market_code) VALUES (1, 'SFO')")


def test_single_spot_produces_one_row(db):
    """A single spot produces a single row with the expected shape."""
    with db.connection() as conn:
        _seed_dims(conn)
        _insert_spot(conn)
        conn.commit()

    service = SheetExportService(db)
    result = service.get_rows()

    assert result["metadata"]["hash_version"] == "v1"
    assert result["metadata"]["row_count"] == 1
    row = result["rows"][0]
    assert row["customer"] == "Admerasia:McDonalds"
    assert row["market"] == "SFO"
    assert row["revenue_class"] == "Internal Ad Sales"
    assert row["ae1"] == "Charmaine"
    assert row["agency_flag"] == "Y"
    assert row["sector"] == "Outreach"
    assert row["broadcast_month"] == "2025-01-01"  # ISO, not Mmm-YY
    assert row["gross_rate"] == 4690.00
    assert row["station_net"] == 3986.50
    assert row["broker_fees"] == 0.00


def test_trade_rows_excluded(db):
    """revenue_type = 'Trade' rows are filtered out."""
    with db.connection() as conn:
        _seed_dims(conn)
        _insert_spot(conn, revenue_type="Internal Ad Sales", gross_rate=100.0)
        _insert_spot(conn, revenue_type="Trade", gross_rate=500.0)
        conn.commit()

    service = SheetExportService(db)
    result = service.get_rows()

    # Only the non-Trade row should be present.
    assert result["metadata"]["row_count"] == 1
    assert result["rows"][0]["revenue_class"] == "Internal Ad Sales"
    assert result["rows"][0]["gross_rate"] == 100.0
