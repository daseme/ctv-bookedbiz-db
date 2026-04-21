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
            contract TEXT,
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


def test_historical_forward_bookings_included(db):
    """is_historical=1 spots are INCLUDED (forward bookings — design §3.8)."""
    with db.connection() as conn:
        _seed_dims(conn)
        _insert_spot(conn, is_historical=0, broadcast_month="Jan-25", gross_rate=100.0)
        _insert_spot(conn, is_historical=1, broadcast_month="Jul-26", gross_rate=250.0)
        conn.commit()

    service = SheetExportService(db)
    result = service.get_rows()

    # Two rows (same tuple, different months).
    assert result["metadata"]["row_count"] == 2
    months = {r["broadcast_month"] for r in result["rows"]}
    assert months == {"2025-01-01", "2026-07-01"}


def test_zero_sum_groupings_are_suppressed(db):
    """If all three amounts sum to zero for a (tuple, month), skip the row."""
    with db.connection() as conn:
        _seed_dims(conn)
        # Two spots that exactly cancel each other (refund scenario).
        _insert_spot(conn, gross_rate=500.0, station_net=425.0, broker_fees=0.0)
        _insert_spot(conn, gross_rate=-500.0, station_net=-425.0, broker_fees=0.0)
        # And one real non-zero row.
        _insert_spot(
            conn,
            broadcast_month="Feb-25",
            gross_rate=100.0,
            station_net=85.0,
            broker_fees=0.0,
        )
        conn.commit()

    service = SheetExportService(db)
    result = service.get_rows()

    # The Jan-25 pair cancels out and is suppressed. Only Feb-25 survives.
    assert result["metadata"]["row_count"] == 1
    assert result["rows"][0]["broadcast_month"] == "2025-02-01"
    assert result["rows"][0]["gross_rate"] == 100.0


def test_agency_flag_converted_from_text_to_y_n(db):
    """spots.agency_flag is TEXT ('Agency' / 'Non-agency'); emit 'Y' / 'N'."""
    with db.connection() as conn:
        _seed_dims(conn)
        _insert_spot(conn, agency_flag="Agency", broadcast_month="Jan-25")
        _insert_spot(
            conn,
            bill_code="Direct Customer",  # no agency prefix
            agency_flag="Non-agency",
            broadcast_month="Jan-25",
        )
        conn.commit()

    service = SheetExportService(db)
    result = service.get_rows()

    flags = {r["customer"]: r["agency_flag"] for r in result["rows"]}
    assert flags["Admerasia:McDonalds"] == "Y"
    assert flags["Direct Customer"] == "N"


def test_metadata_contains_hash_version_v1(db):
    """Response metadata.hash_version is 'v1' — PQ asserts match on refresh."""
    with db.connection() as conn:
        _seed_dims(conn)
        _insert_spot(conn)
        conn.commit()

    service = SheetExportService(db)
    result = service.get_rows()

    assert result["metadata"]["hash_version"] == "v1"
    assert "generated_at" in result["metadata"]
    # ISO-8601 UTC with trailing Z (per spec §5).
    assert result["metadata"]["generated_at"].endswith("Z")


def test_malformed_broadcast_month_raises(db):
    """A spot with an invalid broadcast_month format surfaces a clear error.

    The importer's triggers should prevent this in production (see
    data-reference.md §2), but the service shouldn't silently emit bad ISO.
    """
    with db.connection() as conn:
        _seed_dims(conn)
        # Bypass triggers by using a fresh table insertion path: the in-memory
        # fixture has no triggers, so we can insert the malformed value directly.
        _insert_spot(conn, broadcast_month="BADMON")
        conn.commit()

    service = SheetExportService(db)
    with pytest.raises(ValueError, match="Malformed broadcast_month"):
        service.get_rows()


# ---------------------------------------------------------------------------
# v1.1: row_hash helper — pinned test vectors (see client contract §4)
# ---------------------------------------------------------------------------

def test_row_hash_pinned_vector_1():
    """Vector 1: typical row. See client contract §4."""
    from src.services.sheet_export_service import row_hash
    h = row_hash("Admerasia", "SFO", "Internal Ad Sales", "Charmaine", "N", "Tech")
    assert h == "e1f072e9ab7cd84e2fa029e560d8391f279de26c"


def test_row_hash_pinned_vector_2():
    """Vector 2: whitespace + case + null. See client contract §4."""
    from src.services.sheet_export_service import row_hash
    h = row_hash("  TEST Customer  ", "NYC", "", None, "Y", "  Retail  ")
    assert h == "6ae1bc9292210c26787a3e7735f96cb5092a000f"


# ---------------------------------------------------------------------------
# v1.1: metadata envelope additions (schema_version, row_hash_source)
# ---------------------------------------------------------------------------

def test_metadata_has_schema_version_and_row_hash_source(db):
    """Envelope gains `schema_version` and `row_hash_source` per handoff §6."""
    with db.connection() as conn:
        _seed_dims(conn)
        _insert_spot(conn, contract="1234")
        conn.commit()

    service = SheetExportService(db)
    result = service.get_rows()

    assert result["metadata"]["schema_version"] == "1.1"
    assert result["metadata"]["row_hash_source"] == "server"
    # hash_version is unchanged — algorithm didn't change, only compute location.
    assert result["metadata"]["hash_version"] == "v1"


# ---------------------------------------------------------------------------
# v1.1: per-row row_hash field
# ---------------------------------------------------------------------------

def test_every_row_carries_row_hash(db):
    """Every row has a 40-char lowercase hex row_hash."""
    with db.connection() as conn:
        _seed_dims(conn)
        _insert_spot(conn, contract="1234", broadcast_month="Jan-25")
        _insert_spot(conn, contract="1234", broadcast_month="Feb-25")
        conn.commit()

    service = SheetExportService(db)
    result = service.get_rows()

    assert len(result["rows"]) == 2
    for row in result["rows"]:
        assert "row_hash" in row
        assert isinstance(row["row_hash"], str)
        assert len(row["row_hash"]) == 40
        assert row["row_hash"] == row["row_hash"].lower()
        # hex only
        int(row["row_hash"], 16)


def test_row_hash_identical_for_same_tuple_different_months(db):
    """Same 6-field tuple across two months → identical row_hash."""
    with db.connection() as conn:
        _seed_dims(conn)
        _insert_spot(conn, contract="1234", broadcast_month="Jan-25")
        _insert_spot(conn, contract="1234", broadcast_month="Feb-25")
        conn.commit()

    service = SheetExportService(db)
    result = service.get_rows()

    hashes = {r["row_hash"] for r in result["rows"]}
    assert len(hashes) == 1


# ---------------------------------------------------------------------------
# v1.1: broker_yn / broker_pct with contract selection
# ---------------------------------------------------------------------------

def test_broker_fields_from_non_sentinel_contract_with_fees(db):
    """Non-sentinel contract, broker_fees > 0 → broker_yn='Y', broker_pct=fees/gross."""
    with db.connection() as conn:
        _seed_dims(conn)
        _insert_spot(
            conn, contract="1234",
            gross_rate=500.0, station_net=400.0, broker_fees=100.0,
        )
        conn.commit()

    service = SheetExportService(db)
    result = service.get_rows()
    row = result["rows"][0]
    assert row["broker_yn"] == "Y"
    assert row["broker_pct"] == pytest.approx(0.2)


def test_broker_fields_from_non_sentinel_contract_no_fees(db):
    """Non-sentinel contract, broker_fees = 0 → broker_yn='N', broker_pct=0.0."""
    with db.connection() as conn:
        _seed_dims(conn)
        _insert_spot(
            conn, contract="1234",
            gross_rate=500.0, station_net=450.0, broker_fees=0.0,
        )
        conn.commit()

    service = SheetExportService(db)
    result = service.get_rows()
    row = result["rows"][0]
    assert row["broker_yn"] == "N"
    assert row["broker_pct"] == pytest.approx(0.0)


@pytest.mark.parametrize("sentinel", [None, "", "N"])
def test_broker_fields_null_when_sentinel_only(db, sentinel):
    """All spots in tuple have sentinel contract → broker_yn=None, broker_pct=None.

    Per handoff §5.3: even if broker_fees > 0 on sentinel spots, we do not
    attribute broker presence to a contract that doesn't exist.
    """
    with db.connection() as conn:
        _seed_dims(conn)
        _insert_spot(
            conn, contract=sentinel,
            gross_rate=500.0, station_net=400.0, broker_fees=100.0,
        )
        conn.commit()

    service = SheetExportService(db)
    result = service.get_rows()
    row = result["rows"][0]
    assert row["broker_yn"] is None, f"sentinel={sentinel!r}"
    assert row["broker_pct"] is None, f"sentinel={sentinel!r}"


def test_multi_contract_selection_picks_latest_broadcast_month(db):
    """Tuple with two contracts; the one with latest broadcast_month wins.

    Selected contract's broker_fees / gross_rate drive broker_yn / broker_pct
    for ALL rows of the tuple (not just the selected month's row).
    """
    with db.connection() as conn:
        _seed_dims(conn)
        # Older contract — should be ignored for broker selection.
        _insert_spot(
            conn, contract="1000", broadcast_month="Jan-25",
            gross_rate=200.0, station_net=180.0, broker_fees=0.0,
        )
        # Newer contract — should be selected.
        _insert_spot(
            conn, contract="2000", broadcast_month="Jul-25",
            gross_rate=500.0, station_net=400.0, broker_fees=100.0,
        )
        conn.commit()

    service = SheetExportService(db)
    result = service.get_rows()

    # Two monthly rows for the same tuple; both carry the newer contract's values.
    # (If we didn't select per-contract, Jul-25's aggregated broker_pct would be
    # 100/500 = 0.2 anyway, but Jan-25's would be 0/200 = 0.0 — divergent hashes.
    # Per-contract selection makes them identical.)
    assert len(result["rows"]) == 2
    for row in result["rows"]:
        assert row["broker_yn"] == "Y", f"month={row['broadcast_month']}"
        assert row["broker_pct"] == pytest.approx(0.2), f"month={row['broadcast_month']}"


def test_multi_contract_tiebreak_on_spot_id_desc(db):
    """Same tuple, same broadcast_month, different contracts → higher spot_id wins."""
    with db.connection() as conn:
        _seed_dims(conn)
        # Older spot_id — should lose tiebreak.
        _insert_spot(
            conn, contract="1000", broadcast_month="Jul-25",
            gross_rate=200.0, station_net=180.0, broker_fees=0.0,
        )
        # Newer spot_id — should win tiebreak.
        _insert_spot(
            conn, contract="2000", broadcast_month="Jul-25",
            gross_rate=500.0, station_net=400.0, broker_fees=100.0,
        )
        conn.commit()

    service = SheetExportService(db)
    result = service.get_rows()

    # Aggregated into one monthly row; broker fields come from contract 2000.
    assert len(result["rows"]) == 1
    row = result["rows"][0]
    assert row["broker_yn"] == "Y"
    assert row["broker_pct"] == pytest.approx(0.2)


def test_sentinel_contract_coexists_with_real_contract(db):
    """When a tuple has both sentinel and real contracts, the real one wins.

    The sentinel spots are ignored for broker selection, so broker_yn/pct come
    from the non-sentinel contract.
    """
    with db.connection() as conn:
        _seed_dims(conn)
        _insert_spot(
            conn, contract="N", broadcast_month="Jul-25",
            gross_rate=200.0, station_net=180.0, broker_fees=0.0,
        )
        _insert_spot(
            conn, contract="1234", broadcast_month="Jan-25",
            gross_rate=500.0, station_net=400.0, broker_fees=100.0,
        )
        conn.commit()

    service = SheetExportService(db)
    result = service.get_rows()

    # Real contract wins even though its broadcast_month is earlier — only
    # non-sentinel contracts are candidates.
    for row in result["rows"]:
        assert row["broker_yn"] == "Y"
        assert row["broker_pct"] == pytest.approx(0.2)
