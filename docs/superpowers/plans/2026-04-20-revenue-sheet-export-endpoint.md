# Revenue Sheet Export Endpoint — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the Flask endpoint `GET /api/revenue/sheet-export` that returns the long-format JSON the Excel-side Power Query consumes. No schema changes. No new downstream consumers. Purely additive to `src/web/routes/api.py`, `src/services/`, and tests.

**Architecture:** Clean-architecture slice: route handler (thin) → new `SheetExportService` (owns SQL + shape conversion) → `database_connection` container-provided singleton. Auth via a small `X-SpotOps-Token` decorator new to `src/web/utils/auth.py`. No repository layer — the query is a single GROUP BY aggregation tight to this one endpoint; adding a repository would be over-abstraction (see design doc §5: "unusual shape, no other consumer on the horizon").

**Tech Stack:** Flask 3, SQLite (via existing `DatabaseConnection` wrapper), pytest, existing `src/services/container.py` DI.

**Spec source:** `docs/superpowers/specs/2026-04-20-revenue-sheet-export-design.md` (§3, §5, §6.6, §10, §11 are load-bearing).
**Audit source:** `docs/superpowers/specs/2026-04-20-db-schema-audit.md` (explains why the query bypasses `spots_reporting`).

---

## File structure

| File | Status | Responsibility |
|---|---|---|
| `src/services/sheet_export_service.py` | **Create** | Owns the SQL query, `Mmm-YY`→ISO conversion, response shape. Single public method `get_rows(start_month=None, end_month=None) -> dict`. |
| `src/services/factory.py` | Modify | Register `sheet_export_service` in `initialize_services()`. |
| `src/web/utils/auth.py` | Modify | Add `require_sheet_export_token` decorator. Token comes from env `SHEET_EXPORT_TOKEN`. |
| `src/web/routes/api.py` | Modify | Add `@api_bp.route("/revenue/sheet-export")` handler. Thin — delegates to service. |
| `tests/services/test_sheet_export_service.py` | **Create** | Service-level unit tests: grouping, Trade exclusion, `is_historical` inclusion, ISO conversion, `HAVING` suppression, `agency_flag` conversion, `hash_version` present. Uses in-memory SQLite fixture. |
| `tests/test_sheet_export_endpoint.py` | **Create** | Route-level tests: 401 missing/wrong token, 503 missing env var, 200 happy path, response shape. Uses Flask test client + mocked service. |

Each file has one responsibility. The service is the brain; the route is plumbing; the auth decorator is reusable but scoped to this endpoint for now.

---

## Task 1: Env-var-driven auth decorator

**Files:**
- Modify: `src/web/utils/auth.py` (append to end of file)
- Test: `tests/test_sheet_export_endpoint.py` (new — initial partial version)

- [ ] **Step 1: Write the failing test for missing token → 401**

Create `tests/test_sheet_export_endpoint.py` with:

```python
"""Route-level tests for GET /api/revenue/sheet-export."""

import os
import pytest


@pytest.fixture
def sheet_token(monkeypatch):
    """Set the server-side token env var for the test."""
    monkeypatch.setenv("SHEET_EXPORT_TOKEN", "test-token-123")
    yield "test-token-123"


def test_missing_token_returns_401(client):
    """No X-SpotOps-Token header → 401."""
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
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest tests/test_sheet_export_endpoint.py -v
```

Expected: all three tests fail with `404 Not Found` (route doesn't exist yet) or import errors. Either is fine — we just need red before green.

- [ ] **Step 3: Implement the decorator**

Append to `src/web/utils/auth.py`:

```python
import os
from flask import jsonify


def require_sheet_export_token(f: Callable) -> Callable:
    """
    Require a matching X-SpotOps-Token header.

    - Missing env var SHEET_EXPORT_TOKEN → 503 (misconfigured server).
    - Missing or wrong header → 401.

    Used only on /api/revenue/sheet-export for v1.
    """

    @wraps(f)
    def decorated_function(*args, **kwargs):
        expected = os.environ.get("SHEET_EXPORT_TOKEN")
        if not expected:
            return jsonify(
                {"error": "Sheet export endpoint is misconfigured (no token set)"}
            ), 503

        provided = request.headers.get("X-SpotOps-Token")
        if provided != expected:
            return jsonify({"error": "Authentication required"}), 401

        return f(*args, **kwargs)

    return decorated_function
```

- [ ] **Step 4: Wire a stub route so the 401/503 tests can exercise the decorator**

Add to `src/web/routes/api.py` (temporarily — will flesh out in Task 3):

```python
from src.web.utils.auth import require_sheet_export_token


@api_bp.route("/revenue/sheet-export")
@require_sheet_export_token
def get_sheet_export():
    """Revenue export for the Excel Power Query workbook. See
    docs/superpowers/specs/2026-04-20-revenue-sheet-export-design.md §5.
    """
    return create_success_response({"rows": [], "metadata": {}})
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
pytest tests/test_sheet_export_endpoint.py -v
```

Expected: all three auth tests PASS.

- [ ] **Step 6: Commit**

```bash
git add src/web/utils/auth.py src/web/routes/api.py tests/test_sheet_export_endpoint.py
git commit -m "feat: add X-SpotOps-Token auth for sheet export endpoint"
```

---

## Task 2: SheetExportService — happy-path query shape

**Files:**
- Create: `src/services/sheet_export_service.py`
- Create: `tests/services/test_sheet_export_service.py`

- [ ] **Step 1: Write the failing test for a single-spot happy path**

Create `tests/services/test_sheet_export_service.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest tests/services/test_sheet_export_service.py::test_single_spot_produces_one_row -v
```

Expected: FAIL with `ModuleNotFoundError: src.services.sheet_export_service`.

- [ ] **Step 3: Write the service — happy path only**

Create `src/services/sheet_export_service.py`:

```python
"""
Service for the /api/revenue/sheet-export endpoint.

Emits long-format revenue rows grouped by the display tuple
(customer, market, revenue_class, ae1, agency_flag, sector) × broadcast_month.

See docs/superpowers/specs/2026-04-20-revenue-sheet-export-design.md §5
for the spec and §6.6 for the hash version compatibility contract.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

HASH_VERSION = "v1"

# Mmm -> month number, for Mmm-YY -> ISO conversion.
_MONTH_MAP = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}


def _broadcast_month_to_iso(bm: str) -> str:
    """Convert 'Jan-25' to '2025-01-01'. Raises on malformed input."""
    if not bm or len(bm) != 6 or bm[3] != "-":
        raise ValueError(f"Malformed broadcast_month: {bm!r}")
    month_name, year_suffix = bm[:3], bm[4:]
    month_num = _MONTH_MAP[month_name]
    # 2-digit years: 00..49 → 2000s, 50..99 → 1900s? For this domain,
    # assume 2000s (earliest data is 2022 per design doc §5).
    year = 2000 + int(year_suffix)
    return f"{year:04d}-{month_num:02d}-01"


class SheetExportService:
    """Pulls the sheet-export dataset from the DB and shapes it for JSON."""

    def __init__(self, database_connection):
        self._db = database_connection

    def get_rows(
        self,
        start_month: Optional[str] = None,
        end_month: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Return {"metadata": {...}, "rows": [...]}. See spec §5."""
        rows = self._query(start_month, end_month)
        return {
            "metadata": {
                "generated_at": datetime.now(timezone.utc).isoformat(
                    timespec="seconds"
                ).replace("+00:00", "Z"),
                "start_month": start_month,
                "end_month": end_month,
                "hash_version": HASH_VERSION,
                "row_count": len(rows),
            },
            "rows": rows,
        }

    def _query(
        self,
        start_month: Optional[str],
        end_month: Optional[str],
    ) -> List[Dict[str, Any]]:
        """Run the GROUP BY aggregation and shape each row.

        Bypasses spots_reporting view because it doesn't expose agency_flag
        (see 2026-04-20-db-schema-audit.md, Schema Surprise #1).
        """
        sql = """
            SELECT
              s.bill_code                                                AS customer,
              m.market_code                                              AS market,
              s.revenue_type                                             AS revenue_class,
              s.sales_person                                             AS ae1,
              CASE WHEN s.agency_flag = 'Agency' THEN 'Y' ELSE 'N' END   AS agency_flag,
              sect.sector_name                                           AS sector,
              s.broadcast_month                                          AS broadcast_month_raw,
              SUM(s.gross_rate)                                          AS gross_rate,
              SUM(s.station_net)                                         AS station_net,
              SUM(s.broker_fees)                                         AS broker_fees
            FROM spots s
            LEFT JOIN customers c   ON s.customer_id = c.customer_id
            LEFT JOIN sectors   sect ON c.sector_id = sect.sector_id
            LEFT JOIN markets   m   ON s.market_id = m.market_id
            WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
            GROUP BY 1, 2, 3, 4, 5, 6, 7
            HAVING COALESCE(SUM(s.gross_rate),0) <> 0
                OR COALESCE(SUM(s.station_net),0) <> 0
                OR COALESCE(SUM(s.broker_fees),0) <> 0
            ORDER BY 1, 2, 3, 4, 5, 6,
              CASE SUBSTR(s.broadcast_month, 1, 3)
                WHEN 'Jan' THEN 1  WHEN 'Feb' THEN 2  WHEN 'Mar' THEN 3
                WHEN 'Apr' THEN 4  WHEN 'May' THEN 5  WHEN 'Jun' THEN 6
                WHEN 'Jul' THEN 7  WHEN 'Aug' THEN 8  WHEN 'Sep' THEN 9
                WHEN 'Oct' THEN 10 WHEN 'Nov' THEN 11 WHEN 'Dec' THEN 12
              END,
              SUBSTR(s.broadcast_month, 5, 2)
        """
        with self._db.connection() as conn:
            cursor = conn.execute(sql)
            out: List[Dict[str, Any]] = []
            for r in cursor.fetchall():
                out.append({
                    "customer":        r["customer"],
                    "market":          r["market"],
                    "revenue_class":   r["revenue_class"],
                    "ae1":             r["ae1"],
                    "agency_flag":     r["agency_flag"],
                    "sector":          r["sector"],
                    "broadcast_month": _broadcast_month_to_iso(
                        r["broadcast_month_raw"]
                    ),
                    "gross_rate":      float(r["gross_rate"] or 0),
                    "station_net":     float(r["station_net"] or 0),
                    "broker_fees":     float(r["broker_fees"] or 0),
                })
            return out
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
pytest tests/services/test_sheet_export_service.py::test_single_spot_produces_one_row -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/services/sheet_export_service.py tests/services/test_sheet_export_service.py
git commit -m "feat: SheetExportService happy-path query and ISO month conversion"
```

---

## Task 3: Service — Trade exclusion

**Files:**
- Modify: `tests/services/test_sheet_export_service.py` (append)

- [ ] **Step 1: Write the failing test**

Append to `tests/services/test_sheet_export_service.py`:

```python
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
```

- [ ] **Step 2: Run to verify — should already PASS**

```bash
pytest tests/services/test_sheet_export_service.py::test_trade_rows_excluded -v
```

Expected: PASS (the `WHERE` clause already excludes Trade). This is a regression guard, not a red-then-green — Trade exclusion is the kind of invariant that breaks silently during refactors.

- [ ] **Step 3: Commit**

```bash
git add tests/services/test_sheet_export_service.py
git commit -m "test: verify Trade revenue is excluded from sheet export"
```

---

## Task 4: Service — is_historical forward bookings included

**Files:**
- Modify: `tests/services/test_sheet_export_service.py` (append)

- [ ] **Step 1: Write the failing test**

Append:

```python
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
```

- [ ] **Step 2: Run to verify — should PASS**

```bash
pytest tests/services/test_sheet_export_service.py::test_historical_forward_bookings_included -v
```

Expected: PASS (there is no `is_historical` filter in the query). Regression guard.

- [ ] **Step 3: Commit**

```bash
git add tests/services/test_sheet_export_service.py
git commit -m "test: verify forward bookings (is_historical=1) are included"
```

---

## Task 5: Service — HAVING zero-suppression

**Files:**
- Modify: `tests/services/test_sheet_export_service.py` (append)

- [ ] **Step 1: Write the failing test**

Append:

```python
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
```

- [ ] **Step 2: Run to verify — should PASS**

```bash
pytest tests/services/test_sheet_export_service.py::test_zero_sum_groupings_are_suppressed -v
```

Expected: PASS (HAVING clause already suppresses these).

- [ ] **Step 3: Commit**

```bash
git add tests/services/test_sheet_export_service.py
git commit -m "test: HAVING suppresses zero-sum groupings"
```

---

## Task 6: Service — agency_flag conversion

**Files:**
- Modify: `tests/services/test_sheet_export_service.py` (append)

- [ ] **Step 1: Write the failing test**

Append:

```python
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
```

- [ ] **Step 2: Run to verify — should PASS**

```bash
pytest tests/services/test_sheet_export_service.py::test_agency_flag_converted_from_text_to_y_n -v
```

Expected: PASS (CASE expression already handles this).

- [ ] **Step 3: Commit**

```bash
git add tests/services/test_sheet_export_service.py
git commit -m "test: agency_flag TEXT → Y/N conversion"
```

---

## Task 7: Service — metadata and hash_version

**Files:**
- Modify: `tests/services/test_sheet_export_service.py` (append)

- [ ] **Step 1: Write the failing test**

Append:

```python
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
```

- [ ] **Step 2: Run to verify — should PASS**

```bash
pytest tests/services/test_sheet_export_service.py::test_metadata_contains_hash_version_v1 -v
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/services/test_sheet_export_service.py
git commit -m "test: metadata carries hash_version v1 and generated_at"
```

---

## Task 8: Malformed broadcast_month error

**Files:**
- Modify: `tests/services/test_sheet_export_service.py` (append)

- [ ] **Step 1: Write the failing test**

Append:

```python
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
```

- [ ] **Step 2: Run to verify — should PASS**

```bash
pytest tests/services/test_sheet_export_service.py::test_malformed_broadcast_month_raises -v
```

Expected: PASS (the `_broadcast_month_to_iso` helper already raises).

- [ ] **Step 3: Commit**

```bash
git add tests/services/test_sheet_export_service.py
git commit -m "test: malformed broadcast_month raises ValueError"
```

---

## Task 9: Wire service into container factory

**Files:**
- Modify: `src/services/factory.py`

- [ ] **Step 1: Add the factory function**

Append to `src/services/factory.py` (near the other `create_*_service` functions):

```python
def create_sheet_export_service():
    """Factory: SheetExportService wired to the singleton DB connection."""
    from .sheet_export_service import SheetExportService

    container = get_container()
    db_connection = container.get("database_connection")
    return SheetExportService(db_connection)
```

- [ ] **Step 2: Register it in `initialize_services()`**

In the same file, find the block that contains `container.register_singleton("report_data_service", create_report_data_service)` and add:

```python
    container.register_singleton(
        "sheet_export_service", create_sheet_export_service
    )
```

- [ ] **Step 3: Run the full test suite to make sure nothing broke**

```bash
pytest tests/services/test_sheet_export_service.py tests/test_sheet_export_endpoint.py -v
```

Expected: all tests still pass.

- [ ] **Step 4: Commit**

```bash
git add src/services/factory.py
git commit -m "feat: register sheet_export_service in container"
```

---

## Task 10: Route handler — replace stub with real implementation

**Files:**
- Modify: `src/web/routes/api.py`
- Modify: `tests/test_sheet_export_endpoint.py` (append happy-path test)

- [ ] **Step 1: Write the failing test for the happy path**

Append to `tests/test_sheet_export_endpoint.py`:

```python
def test_happy_path_returns_expected_shape(client, sheet_token):
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


def test_rows_have_expected_fields(client, sheet_token):
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
```

- [ ] **Step 2: Replace the stub in `src/web/routes/api.py`**

Replace the stub route added in Task 1 with:

```python
@api_bp.route("/revenue/sheet-export")
@require_sheet_export_token
@log_requests
@handle_request_errors
def get_sheet_export():
    """
    Revenue export for the Excel Power Query workbook.

    Long-format: one row per (customer, market, revenue_class, ae1,
    agency_flag, sector, broadcast_month). Amounts summed across spots
    for that tuple + month. See:
      docs/superpowers/specs/2026-04-20-revenue-sheet-export-design.md §5
    """
    container = get_container()
    service = safe_get_service(container, "sheet_export_service")

    start_month = request.args.get("start_month") or None
    end_month = request.args.get("end_month") or None

    try:
        payload = service.get_rows(start_month=start_month, end_month=end_month)
    except Exception as e:
        return handle_service_error(e, "generating sheet export")

    # Return the raw payload (NOT wrapped by create_success_response) —
    # the Excel-side agent expects the shape from spec §5 directly.
    return create_json_response(payload)
```

Ensure imports at top of file include `request` from flask and `create_json_response` from `request_helpers`:

```python
from flask import Blueprint, request  # add `request` to existing import
from src.web.utils.request_helpers import (
    ...
    create_json_response,  # add this
)
```

- [ ] **Step 3: Run the tests**

```bash
pytest tests/test_sheet_export_endpoint.py -v
```

Expected: all four tests PASS (three auth + one happy path). The `test_rows_have_expected_fields` test passes whether rows is empty or populated — it guards structure, not content.

- [ ] **Step 4: Run the service tests to confirm nothing regressed**

```bash
pytest tests/services/test_sheet_export_service.py -v
```

Expected: all seven service tests still PASS.

- [ ] **Step 5: Commit**

```bash
git add src/web/routes/api.py tests/test_sheet_export_endpoint.py
git commit -m "feat: wire /api/revenue/sheet-export route to service"
```

---

## Task 11: Integration verification against dev.db

This task is **manual** — it can't run in CI without Kurt's dev database. Doesn't block a PR merge; it's the "did I ship the right thing" check.

- [ ] **Step 1: Start the dev server**

In the container / dev stack:

```bash
SHEET_EXPORT_TOKEN=dev-secret-abc123 python -m src.web.app
```

- [ ] **Step 2: Curl the endpoint**

```bash
curl -sS \
  -H "X-SpotOps-Token: dev-secret-abc123" \
  http://localhost:5000/api/revenue/sheet-export | jq '.metadata, (.rows | length), (.rows[0])'
```

Expected: `metadata.hash_version == "v1"`, `row_count` large (thousands), first row has all ten fields.

- [ ] **Step 3: Spot-check one tuple against a hand-pivot**

Pick `Admerasia:McDonalds` / `SFO` / `Internal Ad Sales` / `Charmaine`. Filter the curl output to that tuple and compare the per-month gross_rate sums against a manual SQL query:

```sql
SELECT broadcast_month, SUM(gross_rate), SUM(station_net), SUM(broker_fees)
FROM spots
WHERE bill_code = 'Admerasia:McDonalds'
  AND market_id = (SELECT market_id FROM markets WHERE market_code = 'SFO')
  AND revenue_type = 'Internal Ad Sales'
  AND sales_person = 'Charmaine'
  AND (revenue_type != 'Trade' OR revenue_type IS NULL)
GROUP BY broadcast_month
ORDER BY broadcast_month;
```

Expected: every month value matches.

- [ ] **Step 4: Test the auth failure modes end-to-end**

```bash
# Missing header → 401
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:5000/api/revenue/sheet-export

# Wrong token → 401
curl -s -o /dev/null -w "%{http_code}\n" \
  -H "X-SpotOps-Token: wrong" \
  http://localhost:5000/api/revenue/sheet-export

# Missing env var → 503 (restart server without SHEET_EXPORT_TOKEN first)
```

Expected: 401, 401, 503.

- [ ] **Step 5: Confirm Tailscale reachability from Kurt's Excel machine**

Hand Kurt this curl command (substituting the Tailscale IP/hostname and production token) for him to run from the machine that will host the workbook:

```bash
curl -sS -H "X-SpotOps-Token: $SHEET_EXPORT_TOKEN" \
  http://<tailscale-host>/api/revenue/sheet-export | jq '.metadata'
```

Expected: response returns in < 10s with metadata block.

---

## Task 12: Runbook snippet + env-var ops doc

**Files:**
- Create: `docs/sheet-export-runbook.md`

- [ ] **Step 1: Write the runbook**

Create `docs/sheet-export-runbook.md`:

```markdown
# Sheet Export Endpoint — Runbook

**Endpoint:** `GET /api/revenue/sheet-export`
**Purpose:** Feeds Kurt's `Revenue Master.xlsx` Power Query refresh. See
`docs/superpowers/specs/2026-04-20-revenue-sheet-export-design.md` for design.

## Environment setup

The endpoint requires the env var `SHEET_EXPORT_TOKEN` to be set on the
server. Without it, the endpoint returns `503`.

- **Dev:** add to the dev stack's `.env` or export before running Flask:
  ```bash
  export SHEET_EXPORT_TOKEN=dev-secret-abc123
  ```
- **Prod:** set in systemd env file for `spotops-dev.service` (or
  equivalent): `/etc/systemd/system/spotops.service.d/override.conf`:
  ```
  [Service]
  Environment="SHEET_EXPORT_TOKEN=<production-value>"
  ```
  Then `systemctl daemon-reload && systemctl restart spotops`.

## Token rotation

1. Generate new token: `openssl rand -hex 32`.
2. Update env on server and restart Flask.
3. Kurt opens the workbook → unhides `Config` tab → updates `ApiToken`
   cell → saves.
4. Refresh from Excel.

## Smoke test (post-deploy)

```bash
curl -sS -H "X-SpotOps-Token: $SHEET_EXPORT_TOKEN" \
  http://localhost:5000/api/revenue/sheet-export \
  | jq '.metadata'
```

Expected: `{"generated_at": "...", "hash_version": "v1", "row_count": N, ...}`
with N > 0 on a populated DB.

## Failure modes

| Symptom | Likely cause | Fix |
|---|---|---|
| `503 {"error": "...misconfigured..."}` | `SHEET_EXPORT_TOKEN` env var not set | Set env var, restart Flask |
| `401 {"error": "Authentication required"}` | Client token doesn't match env var | Check Config tab token matches server env |
| Empty `rows` array on populated DB | `start_month`/`end_month` query params frame nothing | Remove params, refresh |
| Power Query refresh errors on `hash_version` mismatch | Server emits different `hash_version` than Config tab expects | Bump PQ Config → same version, or roll server back |
```

- [ ] **Step 2: Commit**

```bash
git add docs/sheet-export-runbook.md
git commit -m "docs: add runbook for sheet export endpoint"
```

---

## Task 13: Final smoke + open PR

- [ ] **Step 1: Run the full new test set**

```bash
pytest tests/services/test_sheet_export_service.py tests/test_sheet_export_endpoint.py -v
```

Expected: all tests PASS (7 service tests + 4 endpoint tests = 11 total).

- [ ] **Step 2: Run the broader test suite to catch regressions**

```bash
pytest -q
```

Expected: no new failures vs. baseline. The auth decorator is new and scoped — it shouldn't touch anything else.

- [ ] **Step 3: Push branch and open PR**

```bash
git push -u origin feat/revenue-sheet-export-endpoint
```

Then visit the GitHub compare URL (GH prints it after push) and open the PR against `main`. Title: `Add /api/revenue/sheet-export endpoint`. Body references both the design doc and audit doc, lists the test count, and notes that the manual Tailscale reachability check (Task 11 step 5) is required before the Excel-side agent starts PQ v0 work.

---

## Self-review notes (from plan author)

**Spec coverage** — every §5 requirement maps to a task:

- Route + auth (§5 headers) → Tasks 1, 10.
- Query logic (§5 SQL) → Task 2.
- Trade exclusion (§3, §5) → Task 3.
- `is_historical = 1` inclusion (§3.8, §5) → Task 4.
- HAVING suppression (§5) → Task 5.
- `agency_flag` TEXT → Y/N (§5) → Task 6.
- `hash_version` in metadata (§5, §10) → Task 7.
- Malformed month error (§10) → Task 8.
- Container wiring → Task 9.
- Integration verification (§11) → Task 11.
- Runbook (§14 Step 7) → Task 12.

**Not covered in this plan (intentional, out of scope per handoff doc §2):**

- Power Query workbook, sheet layout, banners, `tblCommissionByAE`, hash implementation in M — Excel-side agent.
- Reconciliation / forecast-migration script — runs on Kurt's machine, not the server (handoff doc §5).
- Scheduled refresh / write-back from Excel / downstream workbook automation — v2+.

**Known deferred concerns:**

- Per-contract rate semantics deferred (design doc §13). Revisit only if real data shows rate drift within a single `(customer, revenue_class)` tuple over time.
- Hash test-vector SHA1 hex is pinned by the Excel-side agent once PQ implements §6.6; server-side can cross-check later if a future feature needs to produce hashes server-side.
