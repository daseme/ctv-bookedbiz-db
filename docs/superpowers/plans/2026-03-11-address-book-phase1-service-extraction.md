# Address Book Phase 1: Service Layer Extraction

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development
> (if subagents available) or superpowers:executing-plans to implement this plan.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract all business logic and SQL from `src/web/routes/address_book.py`
(2,226 lines, ~130 SQL statements) into focused services, leaving the route file
as a thin HTTP layer under 500 lines.

**Architecture:** New services inherit from `BaseService(db_connection)` following
the existing pattern in `src/services/base_service.py`. Services are registered as
singletons in `src/services/factory.py`. Route handlers call service methods and
return JSON/HTML — no SQL, no business logic in routes.

**Tech Stack:** Python, Flask, SQLite, pytest

**Spec:** `docs/superpowers/specs/2026-03-11-address-book-v2-design.md`

---

## File Structure

### New Files

| File | Responsibility |
|------|---------------|
| `src/services/entity_metrics_service.py` | Compute/cache entity_metrics and entity_signals tables. Revenue aggregation, signal thresholds. Houses `refresh_entity_metrics()` and `refresh_entity_signals()`. |
| `src/services/entity_service.py` | CRUD for agencies and customers. Create, read, update fields (address, notes, billing, sector, agency assignment), deactivate, reactivate. Agency client listing and duplicate detection. |
| `src/services/activity_service.py` | Activity CRUD, follow-up management, completion toggling. |
| `src/services/address_service.py` | Additional address CRUD for entity_addresses table. |
| `src/services/saved_filter_service.py` | Saved filter preset CRUD. |
| `src/services/export_service.py` | CSV export and contact import logic. |
| `src/utils/formatting.py` | `fmt_revenue()` and `client_portion()` helpers. (Alternatively, add to existing `src/utils/template_formatters.py` if scope fits.) |
| `tests/services/test_entity_metrics_service.py` | Tests for metrics and signal computation. |
| `tests/services/test_entity_service.py` | Tests for entity CRUD operations. |
| `tests/services/test_activity_service.py` | Tests for activity and follow-up operations. |

### Modified Files

| File | Changes |
|------|---------|
| `src/web/routes/address_book.py` | Remove all SQL and business logic. Route handlers call services. Target: ~400 lines. |
| `src/services/broadcast_month_import_service.py` | Change import from `src.web.routes.address_book` to `src.services.entity_metrics_service`. |
| `src/services/factory.py` | Register new services in `initialize_services()`. |
| `src/services/customer_detail_service.py` | Refactor to use `entity_metrics_service` for shared revenue queries (Phase 1b — after core extraction). |
| `src/web/routes/address_book.py` | Remove all inline SQL and business logic. Route handlers retrieve services from the container via `get_container().get("service_name")` and delegate. Target: ~400 lines. |

---

## Chunk 1: Entity Metrics Service (Critical Path)

This must be done first — it untangles the cross-layer import where
`broadcast_month_import_service.py` imports from the route file.

### Task 1: Create entity_metrics_service with refresh functions

**Files:**
- Create: `src/services/entity_metrics_service.py`
- Create: `tests/services/test_entity_metrics_service.py`
- Create: `src/utils/formatting.py`

- [ ] **Step 1: Create formatting helpers**

Create `src/utils/formatting.py` with the two helpers currently in the route file:

```python
def fmt_revenue(val):
    """Format revenue for display: $1.2M / $145K / $800."""
    if val is None:
        return "$0"
    val = abs(val)
    if val >= 1_000_000:
        return f"${val / 1_000_000:.1f}M"
    if val >= 1_000:
        return f"${val / 1_000:.0f}K"
    return f"${val:,.0f}"


def client_portion(name):
    """Strip agency prefix: 'Misfit:CA Colleges' -> 'CA Colleges'."""
    if not name:
        return name
    idx = name.find(":")
    return name[idx + 1:] if idx >= 0 else name
```

- [ ] **Step 2: Write failing test for refresh_entity_metrics**

Create `tests/services/test_entity_metrics_service.py`. Use a temp database
fixture with minimal schema (spots, agencies, customers, entity_metrics,
entity_signals tables).

```python
import os
import tempfile
import sqlite3
import pytest

SCHEMA = """
CREATE TABLE agencies (
    agency_id INTEGER PRIMARY KEY,
    agency_name TEXT UNIQUE,
    is_active INTEGER DEFAULT 1
);
CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY,
    normalized_name TEXT UNIQUE,
    agency_id INTEGER,
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
    broadcast_month TEXT
);
CREATE TABLE entity_metrics (
    entity_type TEXT,
    entity_id INTEGER,
    markets TEXT,
    last_active TEXT,
    total_revenue REAL,
    spot_count INTEGER,
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
"""


@pytest.fixture()
def db_path():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    conn.commit()
    conn.close()
    yield path
    os.unlink(path)


def test_refresh_entity_metrics_computes_from_spots(db_path):
    """Metrics are computed from spots, excluding Trade revenue."""
    from src.database.connection import DatabaseConnection
    from src.services.entity_metrics_service import EntityMetricsService

    db = DatabaseConnection(db_path)
    service = EntityMetricsService(db)

    with db.connection() as conn:
        conn.execute(
            "INSERT INTO agencies (agency_id, agency_name) VALUES (1, 'TestAgency')"
        )
        conn.execute(
            "INSERT INTO customers (customer_id, normalized_name) VALUES (1, 'TestCust')"
        )
        conn.execute(
            "INSERT INTO spots (spot_id, agency_id, customer_id, market_name, "
            "air_date, gross_rate, revenue_type, broadcast_month) "
            "VALUES (1, 1, 1, 'Denver', '2025-06-15', 500.00, 'Cash', 'Jun-25')"
        )
        conn.execute(
            "INSERT INTO spots (spot_id, agency_id, customer_id, market_name, "
            "air_date, gross_rate, revenue_type, broadcast_month) "
            "VALUES (2, 1, 1, 'Denver', '2025-06-16', 200.00, 'Trade', 'Jun-25')"
        )

    with db.connection() as conn:
        service.refresh_metrics(conn)

    with db.connection_ro() as conn:
        row = conn.execute(
            "SELECT * FROM entity_metrics WHERE entity_type='customer' AND entity_id=1"
        ).fetchone()
        assert row is not None
        assert row["total_revenue"] == 500.00  # Trade excluded from revenue
        assert row["spot_count"] == 2  # COUNT(*) includes all spots
        assert row["markets"] == "Denver"
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd /opt/apps/ctv-bookedbiz-db && python -m pytest tests/services/test_entity_metrics_service.py::test_refresh_entity_metrics_computes_from_spots -v`
Expected: FAIL — `ImportError: cannot import name 'EntityMetricsService'`

- [ ] **Step 4: Implement EntityMetricsService**

Create `src/services/entity_metrics_service.py`. Extract `refresh_entity_metrics`,
`refresh_entity_signals`, and `_ensure_cache_tables` from
`src/web/routes/address_book.py` lines 34-224.

```python
import logging
from datetime import date, datetime

from src.services.base_service import BaseService
from src.utils.formatting import fmt_revenue

logger = logging.getLogger(__name__)


class EntityMetricsService(BaseService):
    """Compute and cache entity_metrics and entity_signals tables."""

    _CREATE_ENTITY_METRICS = """
    CREATE TABLE IF NOT EXISTS entity_metrics (
        entity_type TEXT NOT NULL,
        entity_id INTEGER NOT NULL,
        markets TEXT,
        last_active TEXT,
        total_revenue REAL DEFAULT 0,
        spot_count INTEGER DEFAULT 0,
        agency_spot_count INTEGER DEFAULT 0,
        updated_at TEXT DEFAULT (datetime('now')),
        PRIMARY KEY (entity_type, entity_id)
    )"""

    _CREATE_ENTITY_SIGNALS = """
    CREATE TABLE IF NOT EXISTS entity_signals (
        entity_type TEXT NOT NULL,
        entity_id INTEGER NOT NULL,
        signal_type TEXT NOT NULL,
        signal_label TEXT,
        signal_priority INTEGER,
        trailing_revenue REAL,
        prior_revenue REAL,
        computed_at TEXT DEFAULT (datetime('now')),
        PRIMARY KEY (entity_type, entity_id, signal_type)
    )"""

    def ensure_cache_tables(self, conn):
        """Create cache tables if they don't exist."""
        conn.execute(self._CREATE_ENTITY_METRICS)
        conn.execute(self._CREATE_ENTITY_SIGNALS)

    def refresh_metrics(self, conn):
        """Rebuild entity_metrics from spots data."""
        # Copy the exact logic from address_book.py lines 69-95
        # Key: DELETE all then INSERT from spots, excluding Trade
        self.ensure_cache_tables(conn)
        conn.execute("DELETE FROM entity_metrics")
        # Agency metrics
        conn.execute("""
            INSERT INTO entity_metrics
                (entity_type, entity_id, markets, last_active,
                 total_revenue, spot_count, agency_spot_count)
            SELECT 'agency', s.agency_id,
                GROUP_CONCAT(DISTINCT s.market_name),
                MAX(s.air_date),
                SUM(CASE WHEN (s.revenue_type != 'Trade'
                    OR s.revenue_type IS NULL) THEN s.gross_rate ELSE 0 END),
                COUNT(*),
                0
            FROM spots s
            WHERE s.agency_id IS NOT NULL
            GROUP BY s.agency_id
        """)
        # Customer metrics
        conn.execute("""
            INSERT INTO entity_metrics
                (entity_type, entity_id, markets, last_active,
                 total_revenue, spot_count, agency_spot_count)
            SELECT 'customer', s.customer_id,
                GROUP_CONCAT(DISTINCT s.market_name),
                MAX(s.air_date),
                SUM(CASE WHEN (s.revenue_type != 'Trade'
                    OR s.revenue_type IS NULL) THEN s.gross_rate ELSE 0 END),
                COUNT(*),
                COUNT(s.agency_id)
            FROM spots s
            WHERE s.customer_id IS NOT NULL
            GROUP BY s.customer_id
        """)

    def refresh_signals(self, conn):
        """Rebuild entity_signals from spots data."""
        # Copy the exact logic from address_book.py lines 111-224
        # This is the signal computation with 5 signal types
        # IMPORTANT: Copy verbatim — do not refactor the signal logic
        self.ensure_cache_tables(conn)
        conn.execute("DELETE FROM entity_signals")
        # ... (copy lines 118-224 from address_book.py exactly)

    def refresh_metrics_for_ids(self, conn, customer_ids=None,
                                 agency_ids=None):
        """Targeted refresh for specific entities (after merge)."""
        # Copy logic from api_refresh_metrics route handler
        # lines 1402-1462 of address_book.py
        self.ensure_cache_tables(conn)
        if customer_ids:
            placeholders = ",".join("?" * len(customer_ids))
            conn.execute(
                f"DELETE FROM entity_metrics WHERE entity_type='customer' "
                f"AND entity_id IN ({placeholders})",
                customer_ids,
            )
            conn.execute(f"""
                INSERT INTO entity_metrics
                    (entity_type, entity_id, markets, last_active,
                     total_revenue, spot_count, agency_spot_count)
                SELECT 'customer', s.customer_id,
                    GROUP_CONCAT(DISTINCT s.market_name),
                    MAX(s.air_date),
                    SUM(CASE WHEN (s.revenue_type != 'Trade'
                        OR s.revenue_type IS NULL)
                        THEN s.gross_rate ELSE 0 END),
                    COUNT(CASE WHEN (s.revenue_type != 'Trade'
                        OR s.revenue_type IS NULL) THEN 1 END),
                    COUNT(s.agency_id)
                FROM spots s
                WHERE s.customer_id IN ({placeholders})
                GROUP BY s.customer_id
            """, customer_ids)
        if agency_ids:
            placeholders = ",".join("?" * len(agency_ids))
            conn.execute(
                f"DELETE FROM entity_metrics WHERE entity_type='agency' "
                f"AND entity_id IN ({placeholders})",
                agency_ids,
            )
            conn.execute(f"""
                INSERT INTO entity_metrics
                    (entity_type, entity_id, markets, last_active,
                     total_revenue, spot_count, agency_spot_count)
                SELECT 'agency', s.agency_id,
                    GROUP_CONCAT(DISTINCT s.market_name),
                    MAX(s.air_date),
                    SUM(CASE WHEN (s.revenue_type != 'Trade'
                        OR s.revenue_type IS NULL)
                        THEN s.gross_rate ELSE 0 END),
                    COUNT(CASE WHEN (s.revenue_type != 'Trade'
                        OR s.revenue_type IS NULL) THEN 1 END),
                    0
                FROM spots s
                WHERE s.agency_id IN ({placeholders})
                GROUP BY s.agency_id
            """, agency_ids)

    def get_metrics_map(self, conn):
        """Load all metrics into a dict keyed by (entity_type, entity_id)."""
        rows = conn.execute("SELECT * FROM entity_metrics").fetchall()
        return {
            (r["entity_type"], r["entity_id"]): dict(r) for r in rows
        }

    def get_signals_map(self, conn):
        """Load all signals into a dict keyed by (entity_type, entity_id)."""
        rows = conn.execute(
            "SELECT entity_type, entity_id, signal_type, signal_label, "
            "signal_priority FROM entity_signals ORDER BY signal_priority"
        ).fetchall()
        result = {}
        for r in rows:
            key = (r["entity_type"], r["entity_id"])
            if key not in result:
                result[key] = dict(r)
        return result

    def get_entity_signals(self, conn, entity_type, entity_id):
        """Get signals for a single entity."""
        rows = conn.execute(
            "SELECT signal_type, signal_label, signal_priority "
            "FROM entity_signals WHERE entity_type = ? AND entity_id = ?",
            (entity_type, entity_id),
        ).fetchall()
        return [dict(r) for r in rows]

    def auto_refresh_if_empty(self, conn):
        """Refresh caches if they're empty (lazy initialization)."""
        self.ensure_cache_tables(conn)
        count = conn.execute(
            "SELECT COUNT(*) as c FROM entity_metrics"
        ).fetchone()["c"]
        if count == 0:
            self.refresh_metrics(conn)
            self.refresh_signals(conn)
```

Note: The `refresh_signals` method body should be copied **verbatim** from
`address_book.py` lines 111-224. The signal computation logic is complex
(5 signal types with revenue gating, tier-based thresholds, deduplication)
and must not be modified during extraction.

- [ ] **Step 5: Run test to verify it passes**

Run: `cd /opt/apps/ctv-bookedbiz-db && python -m pytest tests/services/test_entity_metrics_service.py::test_refresh_entity_metrics_computes_from_spots -v`
Expected: PASS

- [ ] **Step 6: Write test for signal computation**

Add to `tests/services/test_entity_metrics_service.py`:

```python
def test_refresh_signals_detects_churned(db_path):
    """Churned signal: prior year >= $10K, trailing + future == $0."""
    from src.database.connection import DatabaseConnection
    from src.services.entity_metrics_service import EntityMetricsService

    db = DatabaseConnection(db_path)
    service = EntityMetricsService(db)

    with db.connection() as conn:
        conn.execute(
            "INSERT INTO customers (customer_id, normalized_name) "
            "VALUES (1, 'ChurnedCust')"
        )
        # Spots only in prior year (>12 months ago), totaling > $10K
        conn.execute(
            "INSERT INTO spots (spot_id, customer_id, market_name, "
            "air_date, gross_rate, revenue_type, broadcast_month) "
            "VALUES (1, 1, 'Denver', '2024-06-15', 15000.00, 'Cash', 'Jun-24')"
        )

    with db.connection() as conn:
        service.refresh_signals(conn)

    with db.connection_ro() as conn:
        row = conn.execute(
            "SELECT * FROM entity_signals "
            "WHERE entity_type='customer' AND entity_id=1"
        ).fetchone()
        assert row is not None
        assert row["signal_type"] == "churned"
        assert row["signal_priority"] == 1
```

- [ ] **Step 7: Run test to verify it passes**

Run: `cd /opt/apps/ctv-bookedbiz-db && python -m pytest tests/services/test_entity_metrics_service.py::test_refresh_signals_detects_churned -v`
Expected: PASS (if signals logic was copied correctly)

- [ ] **Step 8: Write test for signal deduplication and auto_refresh_if_empty**

```python
def test_gone_quiet_suppressed_when_churned(db_path):
    """Gone_quiet signal is suppressed if churned already applies."""
    from src.database.connection import DatabaseConnection
    from src.services.entity_metrics_service import EntityMetricsService

    db = DatabaseConnection(db_path)
    service = EntityMetricsService(db)

    with db.connection() as conn:
        conn.execute(
            "INSERT INTO customers (customer_id, normalized_name) "
            "VALUES (1, 'ChurnedAndQuiet')"
        )
        # High revenue only in prior period, nothing recent — triggers both
        conn.execute(
            "INSERT INTO spots (spot_id, customer_id, market_name, "
            "air_date, gross_rate, revenue_type, broadcast_month) "
            "VALUES (1, 1, 'Denver', '2024-01-15', 20000.00, 'Cash', 'Jan-24')"
        )
        service.refresh_signals(conn)

    with db.connection_ro() as conn:
        rows = conn.execute(
            "SELECT signal_type FROM entity_signals "
            "WHERE entity_type='customer' AND entity_id=1"
        ).fetchall()
        types = [r["signal_type"] for r in rows]
        assert "churned" in types
        # gone_quiet should be suppressed when churned is present
        assert "gone_quiet" not in types


def test_auto_refresh_if_empty_populates_cache(db_path):
    """auto_refresh_if_empty triggers refresh when tables are empty."""
    from src.database.connection import DatabaseConnection
    from src.services.entity_metrics_service import EntityMetricsService

    db = DatabaseConnection(db_path)
    service = EntityMetricsService(db)

    with db.connection() as conn:
        conn.execute(
            "INSERT INTO customers (customer_id, normalized_name) "
            "VALUES (1, 'TestCust')"
        )
        conn.execute(
            "INSERT INTO spots (spot_id, customer_id, gross_rate, "
            "revenue_type, broadcast_month, air_date, market_name) "
            "VALUES (1, 1, 500, 'Cash', 'Jan-25', '2025-01-15', 'Denver')"
        )
        service.auto_refresh_if_empty(conn)

    with db.connection_ro() as conn:
        count = conn.execute(
            "SELECT COUNT(*) as c FROM entity_metrics"
        ).fetchone()["c"]
        assert count > 0
```

- [ ] **Step 9: Run signal and auto-refresh tests**

Run: `cd /opt/apps/ctv-bookedbiz-db && python -m pytest tests/services/test_entity_metrics_service.py -v`
Expected: All PASS

- [ ] **Step 10: Write test for targeted refresh**

```python
def test_refresh_metrics_for_specific_ids(db_path):
    """Targeted refresh only updates specified entities."""
    from src.database.connection import DatabaseConnection
    from src.services.entity_metrics_service import EntityMetricsService

    db = DatabaseConnection(db_path)
    service = EntityMetricsService(db)

    with db.connection() as conn:
        conn.execute(
            "INSERT INTO customers (customer_id, normalized_name) "
            "VALUES (1, 'Cust1'), (2, 'Cust2')"
        )
        conn.execute(
            "INSERT INTO spots (spot_id, customer_id, gross_rate, "
            "revenue_type, broadcast_month, air_date, market_name) "
            "VALUES (1, 1, 100, 'Cash', 'Jan-25', '2025-01-15', 'Denver')"
        )
        conn.execute(
            "INSERT INTO spots (spot_id, customer_id, gross_rate, "
            "revenue_type, broadcast_month, air_date, market_name) "
            "VALUES (2, 2, 200, 'Cash', 'Jan-25', '2025-01-15', 'Denver')"
        )
        service.refresh_metrics(conn)
        # Now add a spot to cust1 and refresh only cust1
        conn.execute(
            "INSERT INTO spots (spot_id, customer_id, gross_rate, "
            "revenue_type, broadcast_month, air_date, market_name) "
            "VALUES (3, 1, 300, 'Cash', 'Feb-25', '2025-02-15', 'Denver')"
        )
        service.refresh_metrics_for_ids(conn, customer_ids=[1])

    with db.connection_ro() as conn:
        cust1 = conn.execute(
            "SELECT total_revenue FROM entity_metrics "
            "WHERE entity_type='customer' AND entity_id=1"
        ).fetchone()
        cust2 = conn.execute(
            "SELECT total_revenue FROM entity_metrics "
            "WHERE entity_type='customer' AND entity_id=2"
        ).fetchone()
        assert cust1["total_revenue"] == 400.00  # 100 + 300
        assert cust2["total_revenue"] == 200.00  # unchanged
```

- [ ] **Step 11: Run all metrics tests**

Run: `cd /opt/apps/ctv-bookedbiz-db && python -m pytest tests/services/test_entity_metrics_service.py -v`
Expected: All PASS

- [ ] **Step 12: Commit**

```bash
git add src/services/entity_metrics_service.py src/utils/formatting.py tests/services/test_entity_metrics_service.py
git commit -m "feat: extract EntityMetricsService from address-book routes"
```

### Task 2: Fix cross-layer import in broadcast_month_import_service

**Files:**
- Modify: `src/services/broadcast_month_import_service.py:720-728`
- Modify: `src/services/factory.py`

- [ ] **Step 1: Register EntityMetricsService in factory**

Add to `src/services/factory.py` in the `initialize_services()` function:

```python
from src.services.entity_metrics_service import EntityMetricsService

container.register_singleton(
    "entity_metrics_service",
    lambda: EntityMetricsService(container.get("database_connection")),
)
```

- [ ] **Step 2: Update broadcast_month_import_service import**

In `src/services/broadcast_month_import_service.py`, change lines 720-728 from:

```python
from src.web.routes.address_book import (
    refresh_entity_metrics,
    refresh_entity_signals,
)
```

To:

```python
from src.services.entity_metrics_service import EntityMetricsService
```

And update the call site to instantiate the service (note: `BaseService` stores
the connection as `self.db_connection`):

```python
metrics_service = EntityMetricsService(self.db_connection)
with self.safe_transaction() as conn:
    metrics_service.refresh_metrics(conn)
    tqdm.write("✅ Entity metrics cache refreshed")
    metrics_service.refresh_signals(conn)
    tqdm.write("✅ Entity signals cache refreshed")
```

- [ ] **Step 3: Run existing tests to verify no regressions**

Run: `cd /opt/apps/ctv-bookedbiz-db && python -m pytest tests/ -v --timeout=60 -x`
Expected: All existing tests PASS

- [ ] **Step 4: Commit**

```bash
git add src/services/broadcast_month_import_service.py src/services/factory.py
git commit -m "fix: untangle cross-layer import, use EntityMetricsService"
```

### Task 3: Update address-book routes to use EntityMetricsService

**Files:**
- Modify: `src/web/routes/address_book.py`

- [ ] **Step 1: Replace inline metrics/signals code in route file**

Remove from `address_book.py`:
- Lines 34-62: `_CREATE_ENTITY_METRICS`, `_CREATE_ENTITY_SIGNALS` constants
- Lines 63-66: `_ensure_cache_tables()` function
- Lines 69-95: `refresh_entity_metrics()` function
- Lines 98-108: `_fmt_revenue()` function
- Lines 111-224: `refresh_entity_signals()` function

Add imports at top of route file:

```python
from src.services.container import get_container
from src.utils.formatting import fmt_revenue
```

Route handlers retrieve services from the container singleton (not new instances):

```python
container = get_container()
metrics_svc = container.get("entity_metrics_service")
```

- [ ] **Step 2: Update api_address_book handler (main list)**

Replace the inline cache logic (ensure tables, auto-refresh, load metrics/signals)
with service calls:

```python
container = get_container()
metrics_svc = container.get("entity_metrics_service")
db = container.get("database_connection")
with db.connection() as conn:
    metrics_svc.auto_refresh_if_empty(conn)
with db.connection_ro() as conn:
    metrics_map = metrics_svc.get_metrics_map(conn)
    signals_map = metrics_svc.get_signals_map(conn)
```

- [ ] **Step 3: Update api_entity_detail handler**

Replace inline signal query with:

```python
metrics_svc = get_container().get("entity_metrics_service")
with get_container().get("database_connection").connection_ro() as conn:
    signals = metrics_svc.get_entity_signals(conn, entity_type, entity_id)
```

- [ ] **Step 4: Update api_refresh_metrics handler**

Replace inline refresh logic with:

```python
metrics_svc = get_container().get("entity_metrics_service")
with get_container().get("database_connection").connection() as conn:
    metrics_svc.refresh_metrics_for_ids(
        conn,
        customer_ids=data.get("customer_ids"),
        agency_ids=data.get("agency_ids"),
    )
```

- [ ] **Step 5: Update api_export_csv handler**

Replace inline cache check and metrics loading with service calls,
same pattern as api_address_book.

- [ ] **Step 6: Run full test suite**

Run: `cd /opt/apps/ctv-bookedbiz-db && python -m pytest tests/ -v --timeout=60 -x`
Expected: All PASS

- [ ] **Step 7: Verify address-book page loads correctly**

Run: `curl -s -o /dev/null -w "%{http_code}" http://localhost/api/address-book`
Expected: 200 (or 302 if auth redirect — check from the running service)

- [ ] **Step 8: Commit**

```bash
git add src/web/routes/address_book.py
git commit -m "refactor: address-book routes use EntityMetricsService"
```

---

## Chunk 2: Entity Service

### Task 4: Create EntityService with entity CRUD

**Files:**
- Create: `src/services/entity_service.py`
- Create: `tests/services/test_entity_service.py`

- [ ] **Step 1: Write failing test for entity listing**

Create `tests/services/test_entity_service.py` with temp DB fixture.
Schema needs: agencies, customers, sectors, customer_sectors, entity_contacts,
entity_metrics, entity_signals, entity_aliases tables.

```python
def test_list_entities_returns_agencies_and_customers(db_path):
    """List returns both entity types with metrics joined."""
    from src.database.connection import DatabaseConnection
    from src.services.entity_service import EntityService

    db = DatabaseConnection(db_path)
    service = EntityService(db)

    with db.connection() as conn:
        conn.execute(
            "INSERT INTO agencies (agency_id, agency_name) "
            "VALUES (1, 'TestAgency')"
        )
        conn.execute(
            "INSERT INTO customers (customer_id, normalized_name) "
            "VALUES (1, 'TestCust')"
        )

    with db.connection_ro() as conn:
        result = service.list_entities(conn, include_inactive=False)

    assert len(result["agencies"]) == 1
    assert len(result["customers"]) == 1
    assert result["agencies"][0]["agency_name"] == "TestAgency"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /opt/apps/ctv-bookedbiz-db && python -m pytest tests/services/test_entity_service.py::test_list_entities_returns_agencies_and_customers -v`
Expected: FAIL — `ImportError`

- [ ] **Step 3: Implement EntityService**

Create `src/services/entity_service.py`. Extract from `address_book.py`:

Methods to implement (extract from corresponding route handlers):

| Method | Extracts from | Lines |
|--------|--------------|-------|
| `list_entities(conn, include_inactive)` | `api_address_book` | 273-443 |
| `get_entity_detail(conn, entity_type, entity_id)` | `api_entity_detail` | 447-549 |
| `create_entity(conn, data)` | `api_create_entity` | 1117-1308 |
| `deactivate_entity(conn, entity_type, entity_id, actor)` | `api_deactivate_entity` | 1312-1353 |
| `reactivate_entity(conn, entity_type, entity_id, actor)` | `api_reactivate_entity` | 1357-1398 |
| `update_address(conn, entity_type, entity_id, data)` | `api_update_address` | 553-581 |
| `update_notes(conn, entity_type, entity_id, notes)` | `api_update_notes` | 585-608 |
| `update_billing_info(conn, entity_type, entity_id, data)` | `api_update_billing_info` | 612-688 |
| `update_sector(conn, entity_id, sector_id, actor)` | `api_update_sector` | 692-743 |
| `update_sectors(conn, entity_id, sectors, actor)` | `api_update_sectors` | 747-822 |
| `update_agency(conn, entity_id, agency_id)` | `api_update_agency` | 826-859 |
| `get_agency_customers(conn, agency_id)` | `api_agency_customers` | 863-973 |
| `get_agency_duplicates(conn, agency_id)` | `api_agency_duplicates` | 984-1094 |
| `update_ae(conn, entity_type, entity_id, ae_name, actor)` | `api_update_ae` | 2119-2178 |
| `get_ae_history(conn, entity_type, entity_id)` | `api_ae_history` | 2182-2195 |
| `get_ae_list(conn)` | `api_ae_list` | 2199-2226 |
| `get_spots_link(entity_type, entity_id)` | `api_spots_link` | 1098-1109 |

```python
from src.services.base_service import BaseService
from src.services.customer_resolution_service import _score_name
from src.utils.formatting import client_portion


class EntityService(BaseService):
    """CRUD operations for agencies and customers."""

    def list_entities(self, conn, include_inactive=False):
        """Return all agencies and customers with contact stats and metrics."""
        # Extract logic from api_address_book (lines 273-443)
        # Batch contact stats, sector counts, load metrics/signals
        # Return dict with 'agencies' and 'customers' lists
        ...

    def get_entity_detail(self, conn, entity_type, entity_id):
        """Return full detail for a single entity."""
        # Extract logic from api_entity_detail (lines 447-549)
        ...

    def create_entity(self, conn, data):
        """Create a new agency or customer with optional contact."""
        # Extract logic from api_create_entity (lines 1117-1308)
        # Returns dict with entity_id or fuzzy matches for confirmation
        ...

    # ... remaining methods follow same pattern
```

Each method should take a `conn` parameter (database connection) and return
plain dicts — no Flask-specific objects (no `jsonify`, no `request`).

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /opt/apps/ctv-bookedbiz-db && python -m pytest tests/services/test_entity_service.py -v`
Expected: PASS

- [ ] **Step 5: Write additional tests**

Add tests for:
- `create_entity` with duplicate detection (force=False returns fuzzy matches)
- `deactivate_entity` sets is_active=0
- `update_billing_info` validates commission_rate range
- `get_agency_customers` returns clients from all 3 sources
- `update_ae` creates assignment history record

Each test: create fixture data, call service method, assert result.

- [ ] **Step 6: Run all entity service tests**

Run: `cd /opt/apps/ctv-bookedbiz-db && python -m pytest tests/services/test_entity_service.py -v`
Expected: All PASS

- [ ] **Step 7: Register in factory**

Add to `src/services/factory.py`:

```python
from src.services.entity_service import EntityService

container.register_singleton(
    "entity_service",
    lambda: EntityService(container.get("database_connection")),
)
```

- [ ] **Step 8: Commit**

```bash
git add src/services/entity_service.py tests/services/test_entity_service.py src/services/factory.py
git commit -m "feat: extract EntityService from address-book routes"
```

### Task 5: Wire address-book routes to EntityService

**Files:**
- Modify: `src/web/routes/address_book.py`

- [ ] **Step 1: Replace entity CRUD handlers with service calls**

Route handlers get services from the container. Example for `api_address_book`:

```python
@address_book_bp.route("/api/address-book")
def api_address_book():
    include_inactive = request.args.get("include_inactive", "0") == "1"
    container = get_container()
    entity_svc = container.get("entity_service")
    db = container.get("database_connection")
    with db.connection_ro() as conn:
        result = entity_svc.list_entities(conn, include_inactive)
    return jsonify(result)
```

Repeat for all 16 entity-related route handlers. Each becomes 3-10 lines:
parse request → call service → return jsonify.

- [ ] **Step 3: Run full test suite**

Run: `cd /opt/apps/ctv-bookedbiz-db && python -m pytest tests/ -v --timeout=60 -x`
Expected: All PASS

- [ ] **Step 4: Commit**

```bash
git add src/web/routes/address_book.py
git commit -m "refactor: address-book entity routes use EntityService"
```

---

## Chunk 3: Activity, Address, and Filter Services

### Task 6: Create ActivityService

**Files:**
- Create: `src/services/activity_service.py`
- Create: `tests/services/test_activity_service.py`

- [ ] **Step 1: Write failing test for activity creation**

```python
def test_create_activity(db_path):
    """Creating an activity stores it and returns the ID."""
    from src.database.connection import DatabaseConnection
    from src.services.activity_service import ActivityService

    db = DatabaseConnection(db_path)
    service = ActivityService(db)

    with db.connection() as conn:
        conn.execute(
            "INSERT INTO customers (customer_id, normalized_name) "
            "VALUES (1, 'TestCust')"
        )
        result = service.create_activity(
            conn,
            entity_type="customer",
            entity_id=1,
            activity_type="note",
            description="Called about renewal",
            created_by="admin",
        )

    assert "activity_id" in result
    with db.connection_ro() as conn:
        row = conn.execute(
            "SELECT * FROM entity_activity WHERE activity_id = ?",
            (result["activity_id"],),
        ).fetchone()
        assert row["description"] == "Called about renewal"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /opt/apps/ctv-bookedbiz-db && python -m pytest tests/services/test_activity_service.py::test_create_activity -v`
Expected: FAIL — `ImportError`

- [ ] **Step 3: Implement ActivityService**

Create `src/services/activity_service.py`. Extract from `address_book.py`:

| Method | Extracts from | Lines |
|--------|--------------|-------|
| `get_activities(conn, entity_type, entity_id, limit)` | `api_get_activities` | 1943-1973 |
| `create_activity(conn, entity_type, entity_id, ...)` | `api_create_activity` | 1977-2032 |
| `toggle_completion(conn, activity_id)` | `api_complete_activity` | 2036-2063 |
| `get_follow_ups(conn)` | `api_get_follow_ups` | 2067-2111 |

```python
from datetime import date, datetime

from src.services.base_service import BaseService

VALID_ACTIVITY_TYPES = [
    "note", "call", "email", "meeting", "status_change", "follow_up",
]


class ActivityService(BaseService):
    """Activity logging and follow-up management."""

    def get_activities(self, conn, entity_type, entity_id, limit=50):
        # Extract from lines 1943-1973
        ...

    def create_activity(self, conn, entity_type, entity_id,
                        activity_type, description, created_by,
                        contact_id=None, due_date=None):
        # Extract from lines 1977-2032
        # Validate activity_type, require due_date for follow_up
        ...

    def toggle_completion(self, conn, activity_id):
        # Extract from lines 2036-2063
        ...

    def get_follow_ups(self, conn):
        # Extract from lines 2067-2111
        # Returns incomplete + recently completed (7 days)
        # Assigns urgency classification
        ...
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /opt/apps/ctv-bookedbiz-db && python -m pytest tests/services/test_activity_service.py -v`

- [ ] **Step 5: Write additional tests**

- `test_follow_up_requires_due_date` — creating follow_up without due_date raises error
- `test_toggle_completion_marks_done` — toggling sets is_completed and completed_date
- `test_get_follow_ups_includes_recent_completed` — completed within 7 days appear
- `test_get_follow_ups_urgency_classification` — overdue/due-today/due-soon/upcoming

- [ ] **Step 6: Run all activity tests, commit**

Run: `cd /opt/apps/ctv-bookedbiz-db && python -m pytest tests/services/test_activity_service.py -v`

```bash
git add src/services/activity_service.py tests/services/test_activity_service.py
git commit -m "feat: extract ActivityService from address-book routes"
```

### Task 7: Create AddressService, SavedFilterService, and ExportService

**Files:**
- Create: `src/services/address_service.py`
- Create: `src/services/saved_filter_service.py`
- Create: `src/services/export_service.py`

These are smaller services. Addresses: 4 methods. Filters: 3 methods.
Export: 2 methods (~190 lines for CSV export + ~97 lines for contact import).
Without extracting export/import, the route file cannot hit the 500-line target.

- [ ] **Step 1: Implement AddressService**

Extract from address_book.py lines 1473-1586:

```python
from src.services.base_service import BaseService

VALID_ADDRESS_LABELS = ["Billing", "Shipping", "PO Box", "Office", "Other"]


class AddressService(BaseService):

    def get_addresses(self, conn, entity_type, entity_id):
        ...

    def create_address(self, conn, entity_type, entity_id, data, created_by):
        ...

    def update_address(self, conn, address_id, data):
        ...

    def delete_address(self, conn, address_id):
        ...
```

- [ ] **Step 2: Implement SavedFilterService**

Extract from address_book.py lines 1594-1645:

```python
import json
from src.services.base_service import BaseService


class SavedFilterService(BaseService):

    def get_filters(self, conn, filter_type="address_book"):
        ...

    def save_filter(self, conn, name, config, created_by, is_shared=False):
        ...

    def delete_filter(self, conn, filter_id):
        ...
```

- [ ] **Step 3: Implement ExportService**

Extract from address_book.py lines 1653-1932:

```python
import csv
import io
from src.services.base_service import BaseService

VALID_IMPORT_ROLES = [
    "decision_maker", "account_manager", "billing", "technical", "other",
]


class ExportService(BaseService):

    def export_entities_csv(self, conn, filters):
        """Generate CSV content for filtered entities."""
        # Extract logic from api_export_csv (lines 1653-1835)
        # Takes filter dict, returns CSV string
        ...

    def import_contacts_csv(self, conn, csv_content, created_by):
        """Parse CSV and create contacts for matching entities."""
        # Extract logic from api_import_contacts (lines 1846-1932)
        # Returns dict with imported/skipped/errors counts
        ...
```

- [ ] **Step 4: Write tests for all three services**

Minimal tests: one test per method, temp DB fixture.

- [ ] **Step 5: Register all in factory**

```python
from src.services.address_service import AddressService
from src.services.saved_filter_service import SavedFilterService
from src.services.export_service import ExportService

container.register_singleton(
    "address_service",
    lambda: AddressService(container.get("database_connection")),
)
container.register_singleton(
    "saved_filter_service",
    lambda: SavedFilterService(container.get("database_connection")),
)
container.register_singleton(
    "export_service",
    lambda: ExportService(container.get("database_connection")),
)
```

- [ ] **Step 6: Run all tests, commit**

```bash
git add src/services/address_service.py src/services/saved_filter_service.py src/services/export_service.py tests/services/
git commit -m "feat: extract AddressService, SavedFilterService, ExportService"
```

### Task 8: Wire remaining routes to services

**Files:**
- Modify: `src/web/routes/address_book.py`

- [ ] **Step 1: Replace activity route handlers with service calls**

Wire `api_get_activities`, `api_create_activity`, `api_complete_activity`,
`api_get_follow_ups` to use `ActivityService`.

- [ ] **Step 2: Replace address route handlers with service calls**

Wire `api_get_addresses`, `api_create_address`, `api_update_address_entry`,
`api_delete_address` to use `AddressService`.

- [ ] **Step 3: Replace filter route handlers with service calls**

Wire `api_get_filters`, `api_save_filter`, `api_delete_filter` to use
`SavedFilterService`.

- [ ] **Step 4: Replace export/import handlers with ExportService calls**

Wire `api_export_csv` and `api_import_contacts` to use `ExportService`.
The route handlers handle the HTTP file upload/download; the service does
the CSV generation/parsing and SQL.

- [ ] **Step 5: Replace contact inline SQL with ContactService calls**

The address-book detail handler (`api_entity_detail`) currently queries
`entity_contacts` with inline SQL. Replace with:

```python
contact_svc = get_container().get("contact_service")
contacts = contact_svc.get_contacts(entity_type, entity_id)
```

Note: `ContactService` must be registered in the container if not already.
Check `factory.py` — if missing, add registration.

For the batch contact stats query in `api_address_book` (which counts contacts
per entity in a single GROUP BY query for N+1 avoidance): add a
`get_contact_stats_batch(conn)` method to `ContactService` that returns
`{(entity_type, entity_id): {"count": N, "primary_name": "..."}}`. This
preserves the batch optimization while moving SQL to the service.

- [ ] **Step 6: Move sectors and markets lookups to EntityService**

`api_sectors` (line 242) and `api_markets` (line 259) have inline SQL for
dropdown data. Add to EntityService:

```python
def get_sectors(self, conn):
    """Active sectors for dropdown."""
    ...

def get_markets(self, conn):
    """Distinct markets from spots for dropdown."""
    ...
```

- [ ] **Step 7: Remove all dead code from route file**

After all handlers are wired to services, remove:
- All helper functions that were extracted (`_fmt_revenue`, `_client_portion`,
  `refresh_entity_metrics`, `refresh_entity_signals`, `_ensure_cache_tables`)
- All constants (`_CREATE_ENTITY_METRICS`, `_CREATE_ENTITY_SIGNALS`,
  `VALID_ADDRESS_LABELS`, `VALID_ACTIVITY_TYPES`, `VALID_IMPORT_ROLES`)
- Unused imports (`sqlite3`, `csv`, `io`, `math`, `json` if no longer needed)

- [ ] **Step 8: Verify route file size**

Run: `wc -l src/web/routes/address_book.py`
Expected: Under 500 lines. If over 500, identify remaining inline logic and
extract.

- [ ] **Step 9: Run full test suite**

Run: `cd /opt/apps/ctv-bookedbiz-db && python -m pytest tests/ -v --timeout=60 -x`
Expected: All PASS

- [ ] **Step 10: Manual smoke test**

Access the address-book page in a browser or via curl. Verify:
- Entity list loads with correct counts
- Detail modal opens and shows contacts, addresses, signals
- Creating/editing an entity works
- Activity log displays and accepts new entries
- CSV export downloads
- Saved filters load and save

- [ ] **Step 11: Commit**

```bash
git add src/web/routes/address_book.py
git commit -m "refactor: address-book routes fully wired to services"
```

---

## Chunk 4: Customer Detail Service Alignment

### Task 9: Refactor CustomerDetailService to use shared services

**Files:**
- Modify: `src/services/customer_detail_service.py`
- Modify: `src/web/routes/customer_detail_routes.py`

- [ ] **Step 1: Identify overlap**

`CustomerDetailService` has its own revenue queries that duplicate what
`EntityMetricsService` computes. The overlap is in:
- Revenue totals (lifetime gross/net) — `_get_summary()` line ~80
- Market data — `_get_market_breakdown()` line ~200

However, `CustomerDetailService` also provides detailed breakdowns (by AE,
by language, by market with percentages, monthly trends) that
`EntityMetricsService` does not. These are customer-detail-specific and should
stay in `CustomerDetailService`.

The key alignment is ensuring both services use the same Trade exclusion filter
and the same revenue computation. Verify by comparing SQL.

- [ ] **Step 2: Compare revenue SQL between services**

Read both services and verify that the Trade exclusion clause is identical:
- `EntityMetricsService`: `(s.revenue_type != 'Trade' OR s.revenue_type IS NULL)`
- `CustomerDetailService._get_summary()`: same clause

If they differ, align them. If they match, document the verification and
move on — no code change needed.

- [ ] **Step 3: Fix broken monthly-trend API endpoint**

`customer_detail_routes.py` has a broken `/api/customer/<id>/monthly-trend`
endpoint that references `current_app.extensions.get('db')` instead of using
the container. Fix:

```python
from src.services.container import get_container

@customer_detail_bp.route("/api/customer/<int:customer_id>/monthly-trend")
def customer_monthly_trend_api(customer_id):
    container = get_container()
    db = container.get("database_connection")
    service = CustomerDetailService(db)
    # ... rest of handler
```

- [ ] **Step 4: Run tests, commit**

```bash
git add src/services/customer_detail_service.py src/web/routes/customer_detail_routes.py
git commit -m "fix: align customer-detail with shared services, fix broken API"
```

---

## Chunk 5: Final Verification

### Task 10: Full regression testing and cleanup

- [ ] **Step 1: Run complete test suite**

Run: `cd /opt/apps/ctv-bookedbiz-db && python -m pytest tests/ -v --timeout=120`
Expected: All PASS (except pre-existing broken `test_language_block_service_basic.py`)

- [ ] **Step 2: Verify line counts**

```bash
wc -l src/web/routes/address_book.py
wc -l src/services/entity_metrics_service.py
wc -l src/services/entity_service.py
wc -l src/services/activity_service.py
wc -l src/services/address_service.py
wc -l src/services/saved_filter_service.py
```

Expected:
- `address_book.py`: < 500 lines
- Each service: < 300 lines
- No single function > 100 lines

- [ ] **Step 3: Verify no remaining inline SQL in routes**

Run: `cd /opt/apps/ctv-bookedbiz-db && grep -c "conn.execute\|\.execute(" src/web/routes/address_book.py`
Expected: 0 (or near-zero — only if route-specific query that doesn't belong in a service)

- [ ] **Step 4: Verify import service still works**

Run a manual test of the broadcast month import flow to confirm
`refresh_entity_metrics` and `refresh_entity_signals` are called correctly
from the new service location.

- [ ] **Step 5: Final commit with verification note**

```bash
git add -A
git commit -m "refactor: Phase 1 complete — address-book service extraction

Extracted 5 services from 2,226-line route file:
- EntityMetricsService: metrics/signals computation
- EntityService: entity CRUD and management
- ActivityService: activity log and follow-ups
- AddressService: additional address CRUD
- SavedFilterService: filter preset management

Route file reduced to ~400 lines (thin HTTP layer).
Cross-layer import from broadcast_month_import_service fixed.
All existing tests pass."
```
