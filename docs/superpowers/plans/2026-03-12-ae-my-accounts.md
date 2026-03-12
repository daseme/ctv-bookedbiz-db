# AE My Accounts — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a standalone CRM-style page at `/ae/my-accounts` where AEs see their assigned accounts with activity logging, follow-ups, signals, and revenue snapshots.

**Architecture:** New `ae_crm` blueprint with its own service (`ae_crm_service.py`) that runs targeted queries for one AE's accounts rather than loading all entities. Modifies `activity_service.py` to support AE-scoped follow-ups and cross-account activity feeds. Reuses existing activity CRUD endpoints from address book.

**Tech Stack:** Flask blueprint, Jinja2 template extending base.html, vanilla JS in separate file, Chart.js for revenue sparkline, Nord-themed CSS.

**Spec:** `docs/superpowers/specs/2026-03-12-ae-my-accounts-design.md`

---

## File Map

### New Files

| File | Responsibility |
|------|---------------|
| `src/services/ae_crm_service.py` | AE-scoped account queries, stats, revenue trends |
| `src/web/routes/ae_crm.py` | Blueprint with page + API routes |
| `src/web/templates/ae_my_accounts.html` | Page template |
| `src/web/static/js/ae_my_accounts.js` | All client-side logic |
| `tests/services/test_ae_crm_service.py` | Service unit tests |

### Modified Files

| File | Change |
|------|--------|
| `src/services/activity_service.py` | Add `ae_name` param to `get_follow_ups()`, add `get_recent_activity_for_ae()` |
| `src/services/factory.py` | Register `ae_crm_service` |
| `src/web/blueprints.py` | Import and register `ae_crm_bp` |
| `src/web/routes/address_book.py` | Pass `ae` query param through to `get_follow_ups()` |
| `tests/services/test_activity_service.py` | Tests for new/modified methods |

---

## Chunk 1: Service Layer

### Task 1: Add AE-scoped follow-ups to ActivityService

**Files:**
- Modify: `src/services/activity_service.py:138-174`
- Create: `tests/services/test_activity_service.py`

- [ ] **Step 1: Write test for get_follow_ups with ae_name filter**

Create `tests/services/test_activity_service.py`:

```python
"""Tests for ActivityService AE-scoped methods."""

import sqlite3
import pytest
from datetime import date, timedelta

from src.services.activity_service import ActivityService
from src.database.connection import DatabaseConnection


@pytest.fixture()
def activity_db(tmp_path):
    """Temp DB with activity schema and seed data."""
    db_path = str(tmp_path / "test_activity.db")
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")

    conn.executescript("""
        CREATE TABLE agencies (
            agency_id INTEGER PRIMARY KEY,
            agency_name TEXT NOT NULL,
            assigned_ae TEXT,
            is_active INTEGER DEFAULT 1
        );
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            normalized_name TEXT NOT NULL,
            agency_id INTEGER,
            assigned_ae TEXT,
            is_active INTEGER DEFAULT 1
        );
        CREATE TABLE entity_contacts (
            contact_id INTEGER PRIMARY KEY,
            entity_type TEXT,
            entity_id INTEGER,
            contact_name TEXT,
            is_primary INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1
        );
        CREATE TABLE entity_activity (
            activity_id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_type TEXT NOT NULL,
            entity_id INTEGER NOT NULL,
            activity_type TEXT NOT NULL,
            activity_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            description TEXT,
            created_by TEXT,
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            contact_id INTEGER,
            due_date TEXT,
            is_completed INTEGER DEFAULT 0,
            completed_date TIMESTAMP
        );

        INSERT INTO agencies (agency_id, agency_name, assigned_ae)
            VALUES (1, 'Agency Alpha', 'Alice');
        INSERT INTO customers (customer_id, normalized_name, assigned_ae)
            VALUES (10, 'Customer One', 'Alice'),
                   (20, 'Customer Two', 'Bob');

        -- Alice's follow-ups
        INSERT INTO entity_activity
            (entity_type, entity_id, activity_type, description,
             created_by, due_date, is_completed)
        VALUES
            ('customer', 10, 'follow_up', 'Call about renewal',
             'Alice', '2026-03-15', 0),
            ('agency', 1, 'follow_up', 'Review Q2 plan',
             'Alice', '2026-03-10', 0);

        -- Bob's follow-up
        INSERT INTO entity_activity
            (entity_type, entity_id, activity_type, description,
             created_by, due_date, is_completed)
        VALUES
            ('customer', 20, 'follow_up', 'Send proposal',
             'Bob', '2026-03-12', 0);
    """)
    conn.commit()
    conn.close()
    return DatabaseConnection(db_path)


@pytest.fixture()
def activity_service(activity_db):
    return ActivityService(activity_db)


class TestGetFollowUpsAeFilter:
    """Test get_follow_ups with optional ae_name parameter."""

    def test_no_filter_returns_all(self, activity_service, activity_db):
        with activity_db.connection_ro() as conn:
            results = activity_service.get_follow_ups(conn)
        assert len(results) == 3

    def test_filter_by_ae_returns_only_assigned(
        self, activity_service, activity_db
    ):
        with activity_db.connection_ro() as conn:
            results = activity_service.get_follow_ups(conn, ae_name="Alice")
        assert len(results) == 2
        names = {r["entity_name"] for r in results}
        assert names == {"Customer One", "Agency Alpha"}

    def test_filter_by_ae_excludes_others(
        self, activity_service, activity_db
    ):
        with activity_db.connection_ro() as conn:
            results = activity_service.get_follow_ups(conn, ae_name="Bob")
        assert len(results) == 1
        assert results[0]["entity_name"] == "Customer Two"

    def test_filter_nonexistent_ae_returns_empty(
        self, activity_service, activity_db
    ):
        with activity_db.connection_ro() as conn:
            results = activity_service.get_follow_ups(
                conn, ae_name="Nobody"
            )
        assert len(results) == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /opt/apps/ctv-bookedbiz-db && python -m pytest tests/services/test_activity_service.py -v`
Expected: FAIL — `get_follow_ups()` doesn't accept `ae_name` parameter

- [ ] **Step 3: Implement ae_name filter in get_follow_ups**

In `src/services/activity_service.py`, replace `get_follow_ups` method (lines 138-174):

```python
    def get_follow_ups(self, conn, ae_name=None):
        """Get incomplete follow-ups plus recently completed ones.

        Args:
            ae_name: If provided, only return follow-ups for entities
                assigned to this AE. None returns all.

        Returns list of follow-up dicts with urgency classification.
        """
        ae_filter = ""
        params = []
        if ae_name:
            ae_filter = """
              AND (
                  (ea.entity_type = 'customer' AND ea.entity_id IN (
                      SELECT customer_id FROM customers
                      WHERE assigned_ae = ?))
                  OR
                  (ea.entity_type = 'agency' AND ea.entity_id IN (
                      SELECT agency_id FROM agencies
                      WHERE assigned_ae = ?))
              )
            """
            params = [ae_name, ae_name]

        rows = conn.execute(f"""
            SELECT
                ea.activity_id,
                ea.entity_type,
                ea.entity_id,
                ea.description,
                ea.due_date,
                ea.is_completed,
                ea.completed_date,
                ea.activity_date,
                CASE ea.entity_type
                    WHEN 'agency' THEN (
                        SELECT agency_name FROM agencies
                        WHERE agency_id = ea.entity_id)
                    WHEN 'customer' THEN (
                        SELECT normalized_name FROM customers
                        WHERE customer_id = ea.entity_id)
                END AS entity_name
            FROM entity_activity ea
            WHERE ea.activity_type = 'follow_up'
              AND (ea.is_completed = 0
                   OR ea.completed_date >= datetime('now', '-7 days'))
              {ae_filter}
            ORDER BY ea.is_completed ASC, ea.due_date ASC
        """, params).fetchall()

        today = date.today().isoformat()
        results = []
        for row in rows:
            d = dict(row)
            d["urgency"] = self._classify_urgency(d, today)
            results.append(d)
        return results
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /opt/apps/ctv-bookedbiz-db && python -m pytest tests/services/test_activity_service.py -v`
Expected: All PASS

- [ ] **Step 5: Update address_book route to pass ae param through**

In `src/web/routes/address_book.py`, find the `api_get_follow_ups` function and add the `ae` query parameter passthrough. The route currently calls `activity_svc.get_follow_ups(conn)`. Change to:

```python
@address_book_bp.route("/api/address-book/follow-ups")
def api_get_follow_ups():
    ae_name = request.args.get("ae")
    activity_svc = _svc("activity_service")
    with _db().connection_ro() as conn:
        return jsonify(activity_svc.get_follow_ups(conn, ae_name=ae_name))
```

- [ ] **Step 6: Commit**

```bash
git add src/services/activity_service.py \
        src/web/routes/address_book.py \
        tests/services/test_activity_service.py
git commit -m "feat: add AE filter to follow-ups query

get_follow_ups() now accepts optional ae_name parameter to scope
follow-ups to entities assigned to a specific AE. The /api/address-book/
follow-ups endpoint passes through the ?ae= query param."
```

### Task 2: Add cross-account recent activity method

**Files:**
- Modify: `src/services/activity_service.py`
- Modify: `tests/services/test_activity_service.py`

- [ ] **Step 1: Write test for get_recent_activity_for_ae**

Append to `tests/services/test_activity_service.py`:

```python
class TestGetRecentActivityForAe:
    """Test cross-account recent activity for an AE."""

    def _seed_activities(self, activity_db):
        with activity_db.connection() as conn:
            conn.execute("""
                INSERT INTO entity_activity
                    (entity_type, entity_id, activity_type,
                     description, created_by, activity_date)
                VALUES
                    ('customer', 10, 'note', 'Called client',
                     'Alice', '2026-03-12 10:00:00'),
                    ('customer', 10, 'email', 'Sent proposal',
                     'Alice', '2026-03-11 09:00:00'),
                    ('agency', 1, 'meeting', 'Quarterly review',
                     'Alice', '2026-03-10 14:00:00'),
                    ('customer', 20, 'call', 'Bob activity',
                     'Bob', '2026-03-12 11:00:00')
            """)
            conn.commit()

    def test_returns_only_ae_activities(
        self, activity_service, activity_db
    ):
        self._seed_activities(activity_db)
        with activity_db.connection_ro() as conn:
            results = activity_service.get_recent_activity_for_ae(
                conn, ae_name="Alice"
            )
        entity_names = {r["entity_name"] for r in results}
        assert "Customer Two" not in entity_names
        assert "Customer One" in entity_names

    def test_respects_limit(self, activity_service, activity_db):
        self._seed_activities(activity_db)
        with activity_db.connection_ro() as conn:
            results = activity_service.get_recent_activity_for_ae(
                conn, ae_name="Alice", limit=2
            )
        assert len(results) <= 2

    def test_ordered_by_date_desc(self, activity_service, activity_db):
        self._seed_activities(activity_db)
        with activity_db.connection_ro() as conn:
            results = activity_service.get_recent_activity_for_ae(
                conn, ae_name="Alice"
            )
        dates = [r["activity_date"] for r in results]
        assert dates == sorted(dates, reverse=True)

    def test_includes_entity_name(self, activity_service, activity_db):
        self._seed_activities(activity_db)
        with activity_db.connection_ro() as conn:
            results = activity_service.get_recent_activity_for_ae(
                conn, ae_name="Alice"
            )
        for r in results:
            assert r["entity_name"] is not None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /opt/apps/ctv-bookedbiz-db && python -m pytest tests/services/test_activity_service.py::TestGetRecentActivityForAe -v`
Expected: FAIL — method doesn't exist

- [ ] **Step 3: Implement get_recent_activity_for_ae**

Add to `src/services/activity_service.py` after `get_follow_ups`:

```python
    def get_recent_activity_for_ae(self, conn, ae_name, limit=15):
        """Get recent activities across all entities assigned to an AE.

        Returns list of activity dicts with entity_name, ordered by
        activity_date descending.
        """
        rows = conn.execute("""
            SELECT
                ea.activity_id,
                ea.entity_type,
                ea.entity_id,
                ea.activity_type,
                ea.activity_date,
                ea.description,
                ea.created_by,
                ea.due_date,
                ea.is_completed,
                CASE ea.entity_type
                    WHEN 'agency' THEN (
                        SELECT agency_name FROM agencies
                        WHERE agency_id = ea.entity_id)
                    WHEN 'customer' THEN (
                        SELECT normalized_name FROM customers
                        WHERE customer_id = ea.entity_id)
                END AS entity_name
            FROM entity_activity ea
            WHERE (
                (ea.entity_type = 'customer' AND ea.entity_id IN (
                    SELECT customer_id FROM customers
                    WHERE assigned_ae = ?))
                OR
                (ea.entity_type = 'agency' AND ea.entity_id IN (
                    SELECT agency_id FROM agencies
                    WHERE assigned_ae = ?))
            )
            ORDER BY ea.activity_date DESC
            LIMIT ?
        """, [ae_name, ae_name, limit]).fetchall()
        return [dict(r) for r in rows]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /opt/apps/ctv-bookedbiz-db && python -m pytest tests/services/test_activity_service.py -v`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add src/services/activity_service.py \
        tests/services/test_activity_service.py
git commit -m "feat: add cross-account recent activity query for AE

New get_recent_activity_for_ae() method returns last N activities
across all entities assigned to a specific AE."
```

### Task 3: Create AeCrmService

**Files:**
- Create: `src/services/ae_crm_service.py`
- Create: `tests/services/test_ae_crm_service.py`
- Modify: `src/services/factory.py`

- [ ] **Step 1: Write test for get_accounts**

Create `tests/services/test_ae_crm_service.py`:

```python
"""Tests for AeCrmService — AE-scoped account queries."""

import sqlite3
import pytest

from src.services.ae_crm_service import AeCrmService
from src.database.connection import DatabaseConnection


@pytest.fixture()
def crm_db(tmp_path):
    """Temp DB with full schema for CRM service tests."""
    db_path = str(tmp_path / "test_crm.db")
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")

    conn.executescript("""
        CREATE TABLE agencies (
            agency_id INTEGER PRIMARY KEY,
            agency_name TEXT NOT NULL,
            assigned_ae TEXT,
            is_active INTEGER DEFAULT 1
        );
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            normalized_name TEXT NOT NULL,
            agency_id INTEGER,
            assigned_ae TEXT,
            is_active INTEGER DEFAULT 1
        );
        CREATE TABLE entity_signals (
            entity_type TEXT NOT NULL,
            entity_id INTEGER NOT NULL,
            signal_type TEXT NOT NULL,
            signal_label TEXT NOT NULL,
            signal_priority INTEGER NOT NULL,
            trailing_revenue REAL,
            prior_revenue REAL,
            computed_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (entity_type, entity_id, signal_type)
        );
        CREATE TABLE entity_activity (
            activity_id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_type TEXT NOT NULL,
            entity_id INTEGER NOT NULL,
            activity_type TEXT NOT NULL,
            activity_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            description TEXT,
            created_by TEXT,
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            contact_id INTEGER,
            due_date TEXT,
            is_completed INTEGER DEFAULT 0,
            completed_date TIMESTAMP
        );
        CREATE TABLE entity_contacts (
            contact_id INTEGER PRIMARY KEY,
            entity_type TEXT,
            entity_id INTEGER,
            contact_name TEXT,
            phone TEXT,
            email TEXT,
            is_primary INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1
        );
        CREATE TABLE spots (
            spot_id INTEGER PRIMARY KEY AUTOINCREMENT,
            sales_person TEXT,
            bill_code TEXT,
            broadcast_month TEXT,
            gross_rate REAL,
            revenue_type TEXT,
            customer_id INTEGER,
            is_historical INTEGER DEFAULT 0
        );

        -- Alice's accounts
        INSERT INTO agencies VALUES (1, 'Agency Alpha', 'Alice', 1);
        INSERT INTO customers VALUES (10, 'Customer One', NULL, 'Alice', 1);
        INSERT INTO customers VALUES (11, 'Customer Two', 1, 'Alice', 1);

        -- Bob's account
        INSERT INTO customers VALUES (20, 'Customer Three', NULL, 'Bob', 1);

        -- Inactive account assigned to Alice
        INSERT INTO customers VALUES (30, 'Dead Account', NULL, 'Alice', 0);

        -- Signal for Customer One
        INSERT INTO entity_signals VALUES
            ('customer', 10, 'declining', 'Declining', 2, 5000, 8000,
             '2026-03-01');

        -- Spots for trailing revenue
        INSERT INTO spots (sales_person, broadcast_month, gross_rate,
                           customer_id, revenue_type)
        VALUES
            ('Alice', 'Jan-26', 3000, 10, 'Cash'),
            ('Alice', 'Feb-26', 2000, 10, 'Cash'),
            ('Alice', 'Jan-26', 1500, 11, 'Cash');
    """)
    conn.commit()
    conn.close()
    return DatabaseConnection(db_path)


@pytest.fixture()
def crm_service(crm_db):
    return AeCrmService(crm_db)


class TestGetAccounts:
    """Test AE-scoped account listing."""

    def test_returns_only_assigned_accounts(self, crm_service, crm_db):
        with crm_db.connection_ro() as conn:
            accounts = crm_service.get_accounts(conn, ae_name="Alice")
        names = {a["entity_name"] for a in accounts}
        assert names == {"Agency Alpha", "Customer One", "Customer Two"}

    def test_excludes_other_ae_accounts(self, crm_service, crm_db):
        with crm_db.connection_ro() as conn:
            accounts = crm_service.get_accounts(conn, ae_name="Alice")
        names = {a["entity_name"] for a in accounts}
        assert "Customer Three" not in names

    def test_excludes_inactive_accounts(self, crm_service, crm_db):
        with crm_db.connection_ro() as conn:
            accounts = crm_service.get_accounts(conn, ae_name="Alice")
        names = {a["entity_name"] for a in accounts}
        assert "Dead Account" not in names

    def test_includes_signal_data(self, crm_service, crm_db):
        with crm_db.connection_ro() as conn:
            accounts = crm_service.get_accounts(conn, ae_name="Alice")
        cust_one = next(
            a for a in accounts if a["entity_name"] == "Customer One"
        )
        assert cust_one["signal_type"] == "declining"
        assert cust_one["signal_priority"] == 2

    def test_includes_trailing_revenue(self, crm_service, crm_db):
        with crm_db.connection_ro() as conn:
            accounts = crm_service.get_accounts(conn, ae_name="Alice")
        cust_one = next(
            a for a in accounts if a["entity_name"] == "Customer One"
        )
        assert cust_one["trailing_revenue"] == 5000
        # No signal for Customer Two, so trailing_revenue = 0
        cust_two = next(
            a for a in accounts if a["entity_name"] == "Customer Two"
        )
        assert cust_two["trailing_revenue"] == 0

    def test_no_ae_returns_all_active(self, crm_service, crm_db):
        with crm_db.connection_ro() as conn:
            accounts = crm_service.get_accounts(conn, ae_name=None)
        assert len(accounts) == 4  # 1 agency + 3 active customers

    def test_sorted_by_signal_priority(self, crm_service, crm_db):
        with crm_db.connection_ro() as conn:
            accounts = crm_service.get_accounts(conn, ae_name="Alice")
        # Customer One has signal priority 2, others have None
        assert accounts[0]["entity_name"] == "Customer One"


class TestGetStats:
    """Test summary stats for AE."""

    def test_account_count(self, crm_service, crm_db):
        with crm_db.connection_ro() as conn:
            stats = crm_service.get_stats(conn, ae_name="Alice")
        assert stats["account_count"] == 3

    def test_trailing_revenue(self, crm_service, crm_db):
        with crm_db.connection_ro() as conn:
            stats = crm_service.get_stats(conn, ae_name="Alice")
        # entity_signals has trailing_revenue=5000 for customer 10 only
        assert stats["trailing_revenue"] == 5000

    def test_signal_count(self, crm_service, crm_db):
        with crm_db.connection_ro() as conn:
            stats = crm_service.get_stats(conn, ae_name="Alice")
        assert stats["signal_count"] == 1

    def test_follow_up_and_overdue_counts(self, crm_service, crm_db):
        with crm_db.connection() as conn:
            # Add overdue follow-up for Alice's customer
            conn.execute("""
                INSERT INTO entity_activity
                    (entity_type, entity_id, activity_type, description,
                     due_date, is_completed)
                VALUES ('customer', 10, 'follow_up', 'Overdue task',
                        '2025-01-01', 0)
            """)
            conn.commit()
        with crm_db.connection_ro() as conn:
            stats = crm_service.get_stats(conn, ae_name="Alice")
        assert stats["follow_up_count"] >= 1
        assert stats["overdue_count"] >= 1


class TestGetRevenueTrend:
    """Test revenue trend for an entity."""

    def test_returns_monthly_data(self, crm_service, crm_db):
        with crm_db.connection_ro() as conn:
            trend = crm_service.get_revenue_trend(
                conn, "customer", 10
            )
        assert len(trend) > 0
        assert "broadcast_month" in trend[0]
        assert "revenue" in trend[0]

    def test_excludes_trade_revenue(self, crm_service, crm_db):
        with crm_db.connection() as conn:
            conn.execute("""
                INSERT INTO spots (broadcast_month, gross_rate,
                                   customer_id, revenue_type)
                VALUES ('Jan-26', 9999, 10, 'Trade')
            """)
            conn.commit()
        with crm_db.connection_ro() as conn:
            trend = crm_service.get_revenue_trend(
                conn, "customer", 10
            )
        total = sum(t["revenue"] for t in trend)
        assert 9999 not in [t["revenue"] for t in trend]

    def test_agency_aggregates_linked_customers(self, crm_service, crm_db):
        with crm_db.connection_ro() as conn:
            trend = crm_service.get_revenue_trend(
                conn, "agency", 1
            )
        # Agency 1 has customer 11 with spots
        revenues = [t["revenue"] for t in trend]
        assert sum(revenues) > 0

    def test_empty_for_no_spots(self, crm_service, crm_db):
        with crm_db.connection_ro() as conn:
            trend = crm_service.get_revenue_trend(
                conn, "customer", 20  # Bob's customer, no spots
            )
        assert len(trend) == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /opt/apps/ctv-bookedbiz-db && python -m pytest tests/services/test_ae_crm_service.py -v`
Expected: FAIL — module doesn't exist

- [ ] **Step 3: Implement AeCrmService**

Create `src/services/ae_crm_service.py`:

```python
"""Service for AE CRM page — scoped account queries and stats."""

import logging
from src.services.base_service import BaseService

logger = logging.getLogger(__name__)


class AeCrmService(BaseService):
    """AE-scoped account queries for the My Accounts page."""

    def __init__(self, db_connection):
        super().__init__(db_connection)

    def get_accounts(self, conn, ae_name=None):
        """Get accounts assigned to an AE with signals and last activity.

        Args:
            ae_name: Filter to this AE's accounts. None returns all active.

        Returns list of account dicts sorted by signal priority then name.
        """
        ae_filter = ""
        params = []
        if ae_name:
            ae_filter = "AND e.assigned_ae = ?"
            params = [ae_name]

        rows = conn.execute(f"""
            SELECT
                e.entity_type,
                e.entity_id,
                e.entity_name,
                e.assigned_ae,
                es.signal_type,
                es.signal_label,
                es.signal_priority,
                COALESCE(es.trailing_revenue, 0) AS trailing_revenue,
                (SELECT MAX(ea.activity_date)
                 FROM entity_activity ea
                 WHERE ea.entity_type = e.entity_type
                   AND ea.entity_id = e.entity_id
                ) AS last_activity_date,
                (SELECT ea.due_date
                 FROM entity_activity ea
                 WHERE ea.entity_type = e.entity_type
                   AND ea.entity_id = e.entity_id
                   AND ea.activity_type = 'follow_up'
                   AND ea.is_completed = 0
                 ORDER BY ea.due_date ASC
                 LIMIT 1
                ) AS next_follow_up_date,
                (SELECT ea.description
                 FROM entity_activity ea
                 WHERE ea.entity_type = e.entity_type
                   AND ea.entity_id = e.entity_id
                   AND ea.activity_type = 'follow_up'
                   AND ea.is_completed = 0
                 ORDER BY ea.due_date ASC
                 LIMIT 1
                ) AS next_follow_up_desc
            FROM (
                SELECT 'agency' AS entity_type,
                       agency_id AS entity_id,
                       agency_name AS entity_name,
                       assigned_ae
                FROM agencies WHERE is_active = 1
                UNION ALL
                SELECT 'customer' AS entity_type,
                       customer_id AS entity_id,
                       normalized_name AS entity_name,
                       assigned_ae
                FROM customers WHERE is_active = 1
            ) e
            LEFT JOIN entity_signals es
                ON es.entity_type = e.entity_type
               AND es.entity_id = e.entity_id
            WHERE 1=1 {ae_filter}
            ORDER BY
                CASE WHEN es.signal_priority IS NOT NULL
                     THEN 0 ELSE 1 END,
                es.signal_priority ASC,
                e.entity_name ASC
        """, params).fetchall()

        return [dict(r) for r in rows]

    def get_stats(self, conn, ae_name=None):
        """Get summary stats for the AE's book of business.

        Returns dict with account_count, trailing_revenue,
        signal_count, follow_up_count, overdue_count.
        """
        accounts = self.get_accounts(conn, ae_name)
        signal_count = sum(
            1 for a in accounts if a.get("signal_type")
        )
        trailing_revenue = sum(
            a.get("trailing_revenue", 0) for a in accounts
        )

        ae_filter = ""
        params = []
        if ae_name:
            ae_filter = """
              AND (
                  (ea.entity_type = 'customer' AND ea.entity_id IN (
                      SELECT customer_id FROM customers
                      WHERE assigned_ae = ?))
                  OR
                  (ea.entity_type = 'agency' AND ea.entity_id IN (
                      SELECT agency_id FROM agencies
                      WHERE assigned_ae = ?))
              )
            """
            params = [ae_name, ae_name]

        follow_up_row = conn.execute(f"""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN ea.due_date < date('now')
                    THEN 1 ELSE 0 END) AS overdue
            FROM entity_activity ea
            WHERE ea.activity_type = 'follow_up'
              AND ea.is_completed = 0
              {ae_filter}
        """, params).fetchone()

        return {
            "account_count": len(accounts),
            "trailing_revenue": trailing_revenue,
            "signal_count": signal_count,
            "follow_up_count": follow_up_row["total"] or 0,
            "overdue_count": follow_up_row["overdue"] or 0,
        }

    def get_revenue_trend(self, conn, entity_type, entity_id):
        """Get monthly revenue for an entity over the last 12 months.

        Returns list of {broadcast_month, revenue} dicts ordered
        chronologically. Uses a derived ISO key for correct sort order
        since broadcast_month format ('Jan-25') doesn't sort lexically.
        """
        if entity_type == "agency":
            entity_clause = """
                s.customer_id IN (
                    SELECT customer_id FROM customers
                    WHERE agency_id = ?)
            """
        else:
            entity_clause = "s.customer_id = ?"

        rows = conn.execute(f"""
            SELECT
                s.broadcast_month,
                COALESCE(SUM(s.gross_rate), 0) AS revenue
            FROM spots s
            WHERE {entity_clause}
              AND s.is_historical = 0
              AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
              AND ('20' || SUBSTR(s.broadcast_month, 5, 2)
                   || '-' ||
                   CASE SUBSTR(s.broadcast_month, 1, 3)
                       WHEN 'Jan' THEN '01' WHEN 'Feb' THEN '02'
                       WHEN 'Mar' THEN '03' WHEN 'Apr' THEN '04'
                       WHEN 'May' THEN '05' WHEN 'Jun' THEN '06'
                       WHEN 'Jul' THEN '07' WHEN 'Aug' THEN '08'
                       WHEN 'Sep' THEN '09' WHEN 'Oct' THEN '10'
                       WHEN 'Nov' THEN '11' WHEN 'Dec' THEN '12'
                   END) >= strftime('%%Y-%%m', 'now', '-12 months')
            GROUP BY s.broadcast_month
            ORDER BY
                '20' || SUBSTR(s.broadcast_month, 5, 2)
                || '-' ||
                CASE SUBSTR(s.broadcast_month, 1, 3)
                    WHEN 'Jan' THEN '01' WHEN 'Feb' THEN '02'
                    WHEN 'Mar' THEN '03' WHEN 'Apr' THEN '04'
                    WHEN 'May' THEN '05' WHEN 'Jun' THEN '06'
                    WHEN 'Jul' THEN '07' WHEN 'Aug' THEN '08'
                    WHEN 'Sep' THEN '09' WHEN 'Oct' THEN '10'
                    WHEN 'Nov' THEN '11' WHEN 'Dec' THEN '12'
                END ASC
        """, [entity_id]).fetchall()

        return [dict(r) for r in rows]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /opt/apps/ctv-bookedbiz-db && python -m pytest tests/services/test_ae_crm_service.py -v`
Expected: All PASS

- [ ] **Step 5: Register service in factory**

In `src/services/factory.py`, add the factory function after the last `create_*` function (before `initialize_services`):

```python
def create_ae_crm_service():
    """Factory function for AeCrmService."""
    from src.services.ae_crm_service import AeCrmService

    container = get_container()
    db_connection = container.get("database_connection")
    return AeCrmService(db_connection)
```

And in `initialize_services()` (line ~327, after the `export_service` registration), add:

```python
        print("🔧 Registering ae_crm_service...")
        container.register_singleton("ae_crm_service", create_ae_crm_service)
        print("✅ ae_crm_service registered")
```

**Important:** Register in `initialize_services()`, NOT `register_all_services()`. The app calls `initialize_services()` at startup.

- [ ] **Step 6: Commit**

```bash
git add src/services/ae_crm_service.py \
        tests/services/test_ae_crm_service.py \
        src/services/factory.py
git commit -m "feat: add AeCrmService for scoped account queries

New service with get_accounts(), get_stats(), and get_revenue_trend()
methods. Queries only entities assigned to a specific AE rather than
loading all entities. Registered in service factory."
```

---

## Chunk 2: Blueprint, Routes & Template

### Task 4: Create blueprint with page route

**Files:**
- Create: `src/web/routes/ae_crm.py`
- Modify: `src/web/blueprints.py`
- Create: `src/web/templates/ae_my_accounts.html` (scaffold)

- [ ] **Step 1: Create route file with page endpoint**

Create `src/web/routes/ae_crm.py`:

```python
"""AE My Accounts — CRM-style page for AE book of business."""

import logging
from flask import Blueprint, render_template, jsonify, request
from flask_login import current_user

from src.models.users import UserRole
from src.services.container import get_container
from src.web.utils.auth import role_required

logger = logging.getLogger(__name__)

ae_crm_bp = Blueprint("ae_crm", __name__)


def _db():
    return get_container().get("database_connection")


def _svc(name):
    return get_container().get(name)


def _resolve_ae_name():
    """Determine which AE's accounts to show.

    Admin/Management can select an AE via ?ae= param.
    AE users see their own accounts based on full_name.
    Returns (ae_name, is_admin_view, ae_list).
    """
    is_admin = (
        hasattr(current_user, "role")
        and current_user.role.value in ("admin", "management")
    )
    ae_list = []
    selected_ae = request.args.get("ae", "")

    if is_admin:
        with _db().connection_ro() as conn:
            rows = conn.execute("""
                SELECT DISTINCT assigned_ae
                FROM (
                    SELECT assigned_ae FROM agencies
                    WHERE assigned_ae IS NOT NULL AND is_active = 1
                    UNION
                    SELECT assigned_ae FROM customers
                    WHERE assigned_ae IS NOT NULL AND is_active = 1
                )
                ORDER BY assigned_ae
            """).fetchall()
            ae_list = [r["assigned_ae"] for r in rows]

        ae_name = selected_ae if selected_ae else None
    else:
        ae_name = current_user.full_name

    return ae_name, is_admin, ae_list, selected_ae


@ae_crm_bp.route("/ae/my-accounts")
@role_required(UserRole.AE)
def ae_my_accounts():
    """Render the AE My Accounts page."""
    ae_name, is_admin, ae_list, selected_ae = _resolve_ae_name()
    return render_template(
        "ae_my_accounts.html",
        title="My Accounts",
        ae_name=ae_name or "All AEs",
        is_admin=is_admin,
        ae_list=ae_list,
        selected_ae=selected_ae,
    )


@ae_crm_bp.route("/api/ae/my-accounts")
@role_required(UserRole.AE)
def api_accounts():
    """Return account list JSON for the current AE."""
    ae_name, _, _, _ = _resolve_ae_name()
    crm_svc = _svc("ae_crm_service")
    with _db().connection_ro() as conn:
        return jsonify(crm_svc.get_accounts(conn, ae_name=ae_name))


@ae_crm_bp.route("/api/ae/my-accounts/stats")
@role_required(UserRole.AE)
def api_stats():
    """Return summary stats JSON for the current AE."""
    ae_name, _, _, _ = _resolve_ae_name()
    crm_svc = _svc("ae_crm_service")
    activity_svc = _svc("activity_service")
    with _db().connection_ro() as conn:
        stats = crm_svc.get_stats(conn, ae_name=ae_name)
        return jsonify(stats)


@ae_crm_bp.route(
    "/api/ae/my-accounts/<entity_type>/<int:entity_id>/revenue-trend"
)
@role_required(UserRole.AE)
def api_revenue_trend(entity_type, entity_id):
    """Return monthly revenue trend for a single entity."""
    if entity_type not in ("agency", "customer"):
        return jsonify({"error": "Invalid entity type"}), 400
    crm_svc = _svc("ae_crm_service")
    with _db().connection_ro() as conn:
        return jsonify(crm_svc.get_revenue_trend(
            conn, entity_type, entity_id
        ))
```

- [ ] **Step 2: Register blueprint**

In `src/web/blueprints.py`, add import at line 44 (after the address_book import):

```python
from src.web.routes.ae_crm import ae_crm_bp
```

And in `register_blueprints()`, add before the template filters registration (before line 135):

```python
        app.register_blueprint(ae_crm_bp)
        logger.info("Registered AE CRM blueprint")
```

- [ ] **Step 3: Create scaffold template**

Create `src/web/templates/ae_my_accounts.html`:

```html
{% extends "base.html" %}
{% from "macros/breadcrumbs.html" import breadcrumb_trail %}

{% block title %}{{ title }} - CTV Reports{% endblock %}
{% block header_title %}{{ title }}{% endblock %}
{% block breadcrumb %}
{{ breadcrumb_trail([], "My Accounts") }}
{% endblock %}

{% block extra_styles %}
<style>
.crm-container { max-width: 1400px; margin: 0 auto; padding: 0 20px; }

/* AE Selector */
.ae-selector {
    background: white; border: 1px solid #e2e8f0; border-radius: 8px;
    padding: 16px 20px; margin-bottom: 20px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    display: flex; align-items: center; gap: 12px;
}
.ae-selector select {
    padding: 8px 12px; border: 1px solid #e2e8f0; border-radius: 4px;
    font-size: 14px; min-width: 200px;
}

/* Summary Bar */
.summary-bar {
    display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px;
    margin-bottom: 20px;
}
.stat-card {
    background: white; border: 1px solid #e2e8f0; border-radius: 8px;
    padding: 16px 20px; text-align: center;
    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
}
.stat-card .stat-value {
    font-size: 28px; font-weight: 700; color: #2d3748;
}
.stat-card .stat-label {
    font-size: 13px; color: #718096; margin-top: 4px;
}
.stat-card.has-warning .stat-value { color: #c05621; }

/* Action Items */
.action-items {
    background: #fffbeb; border: 1px solid #fde68a; border-radius: 8px;
    padding: 16px 20px; margin-bottom: 20px;
}
.action-items h3 { margin: 0 0 12px; font-size: 15px; color: #92400e; }
.action-item-row {
    display: flex; align-items: center; gap: 12px; padding: 8px 0;
    border-bottom: 1px solid #fef3c7;
}
.action-item-row:last-child { border-bottom: none; }
.urgency-overdue { color: #e53e3e; font-weight: 600; }
.urgency-due-today { color: #dd6b20; font-weight: 600; }

/* Accounts Table */
.accounts-section {
    background: white; border: 1px solid #e2e8f0; border-radius: 8px;
    padding: 20px; margin-bottom: 20px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
}
.accounts-section h3 {
    margin: 0 0 12px; font-size: 15px; font-weight: 600; color: #2d3748;
}
.search-bar {
    width: 100%; padding: 10px 14px; border: 1px solid #e2e8f0;
    border-radius: 6px; font-size: 14px; margin-bottom: 12px;
}
.accounts-table {
    width: 100%; border-collapse: collapse; font-size: 14px;
}
.accounts-table th {
    text-align: left; padding: 10px 12px; border-bottom: 2px solid #e2e8f0;
    color: #718096; font-weight: 600; font-size: 12px;
    text-transform: uppercase; letter-spacing: 0.5px; cursor: pointer;
}
.accounts-table td {
    padding: 10px 12px; border-bottom: 1px solid #f0f0f0;
}
.accounts-table tr:hover { background: #f7fafc; }
.accounts-table .account-name {
    color: #2b6cb0; cursor: pointer; font-weight: 500;
}
.accounts-table .account-name:hover { text-decoration: underline; }

/* Type & Signal Badges */
.badge {
    display: inline-block; padding: 2px 8px; border-radius: 12px;
    font-size: 11px; font-weight: 600; text-transform: uppercase;
}
.badge-customer { background: #ebf4ff; color: #2b6cb0; }
.badge-agency { background: #f0fff4; color: #276749; }
.badge-declining { background: #fed7d7; color: #c53030; }
.badge-churned { background: #fff5f5; color: #e53e3e; }
.badge-gone_quiet { background: #fefcbf; color: #975a16; }
.badge-growing { background: #c6f6d5; color: #276749; }
.badge-new_account { background: #bee3f8; color: #2a4365; }

/* Slide-out Detail Panel */
.detail-overlay {
    display: none; position: fixed; top: 0; right: 0;
    width: 480px; height: 100vh; background: white;
    border-left: 1px solid #e2e8f0;
    box-shadow: -4px 0 20px rgba(0,0,0,0.1);
    z-index: 1000; overflow-y: auto;
    transition: transform 0.2s ease;
}
.detail-overlay.open { display: block; }
.detail-header {
    padding: 20px; border-bottom: 1px solid #e2e8f0;
    position: sticky; top: 0; background: white; z-index: 1;
}
.detail-header .close-btn {
    float: right; background: none; border: none;
    font-size: 20px; cursor: pointer; color: #718096;
}
.detail-header h2 {
    margin: 0 0 8px; font-size: 18px; color: #2d3748;
}
.detail-tabs {
    display: flex; border-bottom: 1px solid #e2e8f0;
    padding: 0 20px;
}
.detail-tab {
    padding: 12px 16px; cursor: pointer; font-size: 13px;
    color: #718096; border-bottom: 2px solid transparent;
    font-weight: 500;
}
.detail-tab.active {
    color: #2b6cb0; border-bottom-color: #2b6cb0;
}
.detail-body { padding: 20px; }

/* Activity Timeline */
.activity-form {
    display: flex; gap: 8px; margin-bottom: 16px; flex-wrap: wrap;
}
.activity-form select {
    padding: 8px 10px; border: 1px solid #e2e8f0; border-radius: 4px;
    font-size: 13px;
}
.activity-form input[type="text"] {
    flex: 1; min-width: 200px; padding: 8px 10px;
    border: 1px solid #e2e8f0; border-radius: 4px; font-size: 13px;
}
.activity-form button {
    padding: 8px 16px; background: #2b6cb0; color: white;
    border: none; border-radius: 4px; cursor: pointer; font-size: 13px;
}
.activity-form button:hover { background: #2c5282; }
.follow-up-toggle {
    font-size: 12px; color: #718096; cursor: pointer; width: 100%;
}
.timeline-entry {
    padding: 10px 0; border-bottom: 1px solid #f0f0f0;
}
.timeline-entry:last-child { border-bottom: none; }
.timeline-meta {
    font-size: 12px; color: #718096; margin-bottom: 4px;
}
.timeline-desc { font-size: 14px; color: #2d3748; }
.badge-note { background: #e2e8f0; color: #4a5568; }
.badge-call { background: #bee3f8; color: #2a4365; }
.badge-email { background: #c6f6d5; color: #276749; }
.badge-meeting { background: #e9d8fd; color: #553c9a; }
.badge-follow_up { background: #fefcbf; color: #975a16; }

/* Recent Activity Feed */
.recent-activity {
    background: white; border: 1px solid #e2e8f0; border-radius: 8px;
    padding: 20px; margin-bottom: 20px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
}
.recent-activity h3 {
    margin: 0 0 12px; font-size: 15px; font-weight: 600; color: #2d3748;
}

/* Revenue Chart */
.revenue-chart-container {
    height: 200px; margin-top: 12px;
}

/* Empty States */
.empty-state {
    text-align: center; padding: 40px 20px; color: #a0aec0;
}
.empty-state p { margin: 8px 0; font-size: 14px; }

/* Responsive */
@media (max-width: 768px) {
    .summary-bar { grid-template-columns: repeat(2, 1fr); }
    .detail-overlay { width: 100%; }
}
</style>
{% endblock %}

{% block content %}
<div class="crm-container">

    {% if is_admin %}
    <div class="ae-selector">
        <label style="font-weight:600; color:#4a5568; font-size:14px;">
            AE:
        </label>
        <select id="ae-select" onchange="switchAe(this.value)">
            <option value="">All AEs</option>
            {% for ae in ae_list %}
            <option value="{{ ae }}"
                {% if selected_ae == ae %}selected{% endif %}>
                {{ ae }}
            </option>
            {% endfor %}
        </select>
    </div>
    {% endif %}

    <!-- Summary Bar -->
    <div class="summary-bar" id="summary-bar">
        <div class="stat-card">
            <div class="stat-value" id="stat-accounts">-</div>
            <div class="stat-label">Active Accounts</div>
        </div>
        <div class="stat-card">
            <div class="stat-value" id="stat-revenue">-</div>
            <div class="stat-label">Trailing 12-Mo Revenue</div>
        </div>
        <div class="stat-card">
            <div class="stat-value" id="stat-signals">-</div>
            <div class="stat-label">Need Attention</div>
        </div>
        <div class="stat-card" id="card-followups">
            <div class="stat-value" id="stat-followups">-</div>
            <div class="stat-label">Open Follow-ups
                <span id="stat-overdue-label" style="display:none;">
                    (<span id="stat-overdue" style="color:#e53e3e;"></span> overdue)
                </span>
            </div>
        </div>
    </div>

    <!-- Action Items (hidden when empty) -->
    <div class="action-items" id="action-items" style="display:none;">
        <h3>Action Items</h3>
        <div id="action-items-list"></div>
    </div>

    <!-- Accounts Table -->
    <div class="accounts-section">
        <h3>Accounts
            <span id="accounts-count"
                  style="font-weight:400; color:#718096;"></span>
        </h3>
        <input type="text" class="search-bar" id="account-search"
               placeholder="Search accounts...">
        <table class="accounts-table">
            <thead>
                <tr>
                    <th data-sort="entity_name">Name</th>
                    <th data-sort="entity_type">Type</th>
                    <th data-sort="signal_priority">Signal</th>
                    <th data-sort="trailing_revenue">Trailing Revenue</th>
                    <th data-sort="last_activity_date">Last Activity</th>
                    <th data-sort="next_follow_up_date">Next Follow-up</th>
                    <th>Actions</th>
                </tr>
            </thead>
            <tbody id="accounts-tbody"></tbody>
        </table>
        <div class="empty-state" id="accounts-empty" style="display:none;">
            <p>No accounts assigned. Contact your manager.</p>
        </div>
    </div>

    <!-- Recent Activity -->
    <div class="recent-activity">
        <h3>Recent Activity</h3>
        <div id="recent-activity-list">
            <div class="empty-state">
                <p>No activity yet. Log your first note or call.</p>
            </div>
        </div>
    </div>
</div>

<!-- Detail Panel -->
<div class="detail-overlay" id="detail-panel">
    <div class="detail-header">
        <button class="close-btn" onclick="closeDetail()">&times;</button>
        <h2 id="detail-name"></h2>
        <div id="detail-badges"></div>
        <a id="detail-link" href="#"
           style="font-size:13px; color:#2b6cb0;">
            View Full Detail &rarr;
        </a>
    </div>
    <div class="detail-tabs">
        <div class="detail-tab active" data-tab="activity"
             onclick="switchTab('activity')">Activity</div>
        <div class="detail-tab" data-tab="info"
             onclick="switchTab('info')">Info</div>
        <div class="detail-tab" data-tab="revenue"
             onclick="switchTab('revenue')">Revenue</div>
    </div>
    <div class="detail-body" id="detail-body">
        <!-- Tab content loaded dynamically -->
    </div>
</div>

{% endblock %}

{% block extra_scripts %}
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<script>
    const CRM_AE_NAME = {{ ae_name | tojson }};
</script>
<script src="/static/js/ae_my_accounts.js"></script>
{% endblock %}
```

- [ ] **Step 4: Create JS scaffold**

Create `src/web/static/js/ae_my_accounts.js`:

```javascript
/* AE My Accounts — CRM page client logic */

let allAccounts = [];
let currentSort = { key: 'signal_priority', dir: 'asc' };
let currentEntity = null;
let revenueChart = null;

// ── Init ──────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
    loadStats();
    loadAccounts();
    loadActionItems();
    loadRecentActivity();

    document.getElementById('account-search')
        .addEventListener('input', filterAccounts);

    document.querySelectorAll('.accounts-table th[data-sort]')
        .forEach(th => th.addEventListener('click', () => {
            sortAccounts(th.dataset.sort);
        }));

    document.addEventListener('keydown', e => {
        if (e.key === 'Escape') closeDetail();
    });

    // Restore last open panel
    const saved = sessionStorage.getItem('crm_open_entity');
    if (saved) {
        try {
            const { type, id } = JSON.parse(saved);
            openDetail(type, id);
        } catch { /* ignore */ }
    }
});

// ── AE Selector ───────────────────────────────────────────────────
function switchAe(ae) {
    const url = new URL(window.location);
    if (ae) {
        url.searchParams.set('ae', ae);
    } else {
        url.searchParams.delete('ae');
    }
    window.location = url;
}

// ── Stats ─────────────────────────────────────────────────────────
async function loadStats() {
    try {
        const resp = await fetch('/api/ae/my-accounts/stats' +
            window.location.search);
        const stats = await resp.json();
        document.getElementById('stat-accounts')
            .textContent = stats.account_count;
        document.getElementById('stat-revenue')
            .textContent = '$' + Math.round(
                stats.trailing_revenue).toLocaleString();
        document.getElementById('stat-signals')
            .textContent = stats.signal_count;
        document.getElementById('stat-followups')
            .textContent = stats.follow_up_count;
        if (stats.overdue_count > 0) {
            document.getElementById('stat-overdue')
                .textContent = stats.overdue_count;
            document.getElementById('stat-overdue-label')
                .style.display = 'inline';
            document.getElementById('card-followups')
                .classList.add('has-warning');
        }
    } catch (err) {
        console.error('Failed to load stats:', err);
    }
}

// ── Accounts ──────────────────────────────────────────────────────
async function loadAccounts() {
    try {
        const resp = await fetch('/api/ae/my-accounts' +
            window.location.search);
        allAccounts = await resp.json();
        renderAccounts(allAccounts);
    } catch (err) {
        console.error('Failed to load accounts:', err);
    }
}

function renderAccounts(accounts) {
    const tbody = document.getElementById('accounts-tbody');
    const empty = document.getElementById('accounts-empty');
    const count = document.getElementById('accounts-count');
    count.textContent = `(${accounts.length})`;

    if (accounts.length === 0) {
        tbody.innerHTML = '';
        empty.style.display = 'block';
        return;
    }
    empty.style.display = 'none';

    tbody.innerHTML = accounts.map(a => `
        <tr>
            <td>
                <span class="account-name"
                      onclick="openDetail('${a.entity_type}', ${a.entity_id})">
                    ${esc(a.entity_name)}
                </span>
            </td>
            <td><span class="badge badge-${a.entity_type}">
                ${a.entity_type}</span></td>
            <td>${a.signal_label
                ? `<span class="badge badge-${a.signal_type}">${esc(a.signal_label)}</span>`
                : '<span style="color:#cbd5e0;">&mdash;</span>'}</td>
            <td style="text-align:right;">${a.trailing_revenue
                ? '$' + Math.round(a.trailing_revenue).toLocaleString()
                : '&mdash;'}</td>
            <td>${a.last_activity_date
                ? formatDate(a.last_activity_date) : '&mdash;'}</td>
            <td>${a.next_follow_up_date
                ? `${formatDate(a.next_follow_up_date)}`
                : '&mdash;'}</td>
            <td>
                <button class="badge badge-call"
                        style="cursor:pointer; border:none;"
                        onclick="openDetail('${a.entity_type}', ${a.entity_id})">
                    + Log
                </button>
            </td>
        </tr>
    `).join('');
}

function filterAccounts() {
    const q = document.getElementById('account-search')
        .value.toLowerCase();
    const filtered = allAccounts.filter(
        a => a.entity_name.toLowerCase().includes(q)
    );
    renderAccounts(filtered);
}

function sortAccounts(key) {
    if (currentSort.key === key) {
        currentSort.dir = currentSort.dir === 'asc' ? 'desc' : 'asc';
    } else {
        currentSort = { key, dir: 'asc' };
    }
    allAccounts.sort((a, b) => {
        let va = a[key], vb = b[key];
        if (va == null) va = key === 'signal_priority' ? 999 : '';
        if (vb == null) vb = key === 'signal_priority' ? 999 : '';
        if (va < vb) return currentSort.dir === 'asc' ? -1 : 1;
        if (va > vb) return currentSort.dir === 'asc' ? 1 : -1;
        return 0;
    });
    renderAccounts(allAccounts);
}

// ── Action Items ──────────────────────────────────────────────────
async function loadActionItems() {
    try {
        let url = '/api/address-book/follow-ups';
        if (CRM_AE_NAME && CRM_AE_NAME !== 'All AEs') {
            url += `?ae=${encodeURIComponent(CRM_AE_NAME)}`;
        }

        const resp = await fetch(url);
        const items = await resp.json();
        const today = todayISO();
        const actionable = items.filter(
            i => !i.is_completed &&
                 i.due_date && i.due_date <= today
        ).map(i => ({
            ...i,
            urgency: i.due_date < today ? 'overdue' : 'due-today'
        }));

        const container = document.getElementById('action-items');
        const list = document.getElementById('action-items-list');

        if (actionable.length === 0) {
            container.style.display = 'none';
            return;
        }
        container.style.display = 'block';

        list.innerHTML = actionable.map(item => `
            <div class="action-item-row">
                <span class="urgency-${item.urgency}">
                    ${item.urgency === 'overdue' ? 'OVERDUE' : 'TODAY'}
                </span>
                <span class="account-name"
                      onclick="openDetail('${item.entity_type}', ${item.entity_id})">
                    ${esc(item.entity_name || 'Unknown')}
                </span>
                <span style="flex:1; color:#4a5568;">
                    ${esc(item.description || '')}
                </span>
                <span style="color:#718096; font-size:13px;">
                    ${item.due_date}
                </span>
                <button class="badge badge-email"
                        style="cursor:pointer; border:none;"
                        onclick="completeFollowUp(${item.activity_id}, this)">
                    Complete
                </button>
            </div>
        `).join('');
    } catch (err) {
        console.error('Failed to load action items:', err);
    }
}

async function completeFollowUp(activityId, btn) {
    try {
        const resp = await fetch(
            `/api/address-book/activities/${activityId}/complete`,
            { method: 'POST' }
        );
        if (resp.ok) {
            btn.closest('.action-item-row').remove();
            loadStats();
            if (currentEntity) loadActivityTab();
        }
    } catch (err) {
        console.error('Failed to complete follow-up:', err);
    }
}

// ── Detail Panel ──────────────────────────────────────────────────
function openDetail(entityType, entityId) {
    currentEntity = { type: entityType, id: entityId };
    sessionStorage.setItem('crm_open_entity',
        JSON.stringify(currentEntity));

    const account = allAccounts.find(
        a => a.entity_type === entityType && a.entity_id === entityId
    );

    document.getElementById('detail-name')
        .textContent = account ? account.entity_name : '';

    const badges = document.getElementById('detail-badges');
    let badgeHtml = `<span class="badge badge-${entityType}">
        ${entityType}</span> `;
    if (account && account.signal_label) {
        badgeHtml += `<span class="badge badge-${account.signal_type}">
            ${esc(account.signal_label)}</span>`;
    }
    badges.innerHTML = badgeHtml;

    const link = document.getElementById('detail-link');
    if (entityType === 'customer') {
        link.href = `/reports/customer-detail/${entityId}`;
        link.style.display = 'inline';
    } else {
        link.style.display = 'none';
    }

    document.getElementById('detail-panel').classList.add('open');
    switchTab('activity');
}

function closeDetail() {
    document.getElementById('detail-panel').classList.remove('open');
    currentEntity = null;
    sessionStorage.removeItem('crm_open_entity');
}

function switchTab(tab) {
    document.querySelectorAll('.detail-tab').forEach(t => {
        t.classList.toggle('active', t.dataset.tab === tab);
    });
    if (tab === 'activity') loadActivityTab();
    else if (tab === 'info') loadInfoTab();
    else if (tab === 'revenue') loadRevenueTab();
}

// ── Activity Tab ──────────────────────────────────────────────────
async function loadActivityTab() {
    if (!currentEntity) return;
    const body = document.getElementById('detail-body');
    const { type, id } = currentEntity;

    body.innerHTML = `
        <div class="activity-form" id="activity-form">
            <select id="act-type">
                <option value="note">Note</option>
                <option value="call">Call</option>
                <option value="email">Email</option>
                <option value="meeting">Meeting</option>
            </select>
            <input type="text" id="act-desc"
                   placeholder="What happened?">
            <button onclick="submitActivity()">Log</button>
            <div class="follow-up-toggle"
                 onclick="toggleFollowUpField()">
                + Add follow-up date
            </div>
            <input type="date" id="act-due" style="display:none;">
        </div>
        <div id="activity-timeline">Loading...</div>
    `;

    try {
        const resp = await fetch(
            `/api/address-book/${type}/${id}/activities?limit=50`
        );
        const activities = await resp.json();
        const timeline = document.getElementById('activity-timeline');

        if (activities.length === 0) {
            timeline.innerHTML = `<div class="empty-state">
                <p>No activity yet. Log your first note or call.</p>
            </div>`;
            return;
        }

        timeline.innerHTML = activities.map(a => `
            <div class="timeline-entry">
                <div class="timeline-meta">
                    <span class="badge badge-${a.activity_type}">
                        ${a.activity_type}</span>
                    ${formatDate(a.activity_date)}
                    ${a.created_by
                        ? `&middot; ${esc(a.created_by)}` : ''}
                    ${a.activity_type === 'follow_up' && a.due_date
                        ? `&middot; Due: ${a.due_date}
                           ${a.is_completed
                               ? '<span style="color:#38a169;">&#10003;</span>'
                               : ''}` : ''}
                </div>
                <div class="timeline-desc">
                    ${esc(a.description || '')}
                </div>
            </div>
        `).join('');
    } catch (err) {
        document.getElementById('activity-timeline')
            .innerHTML = '<p style="color:#e53e3e;">Failed to load.</p>';
    }
}

function toggleFollowUpField() {
    const input = document.getElementById('act-due');
    input.style.display = input.style.display === 'none'
        ? 'inline-block' : 'none';
}

async function submitActivity() {
    if (!currentEntity) return;
    const { type, id } = currentEntity;
    const actType = document.getElementById('act-type').value;
    const desc = document.getElementById('act-desc').value.trim();
    if (!desc) return;

    const dueDate = document.getElementById('act-due').value || null;
    const finalType = dueDate ? 'follow_up' : actType;

    try {
        const resp = await fetch(
            `/api/address-book/${type}/${id}/activities`,
            {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    activity_type: finalType,
                    description: desc,
                    due_date: dueDate,
                }),
            }
        );
        if (resp.ok) {
            document.getElementById('act-desc').value = '';
            document.getElementById('act-due').value = '';
            document.getElementById('act-due').style.display = 'none';
            loadActivityTab();
            loadRecentActivity();
            loadStats();
            loadAccounts();
        }
    } catch (err) {
        console.error('Failed to submit activity:', err);
    }
}

// ── Info Tab ──────────────────────────────────────────────────────
async function loadInfoTab() {
    if (!currentEntity) return;
    const body = document.getElementById('detail-body');
    const { type, id } = currentEntity;

    body.innerHTML = 'Loading...';
    try {
        const resp = await fetch(`/api/address-book/${type}/${id}`);
        const data = await resp.json();

        let html = '<div style="font-size:14px;">';

        // Primary contact
        const pc = (data.contacts || []).find(c => c.is_primary);
        if (pc) {
            html += `<div style="margin-bottom:16px;">
                <strong style="color:#718096;">Primary Contact</strong><br>
                ${esc(pc.contact_name || '')}
                ${pc.phone ? `<br><a href="tel:${pc.phone}">${esc(pc.phone)}</a>` : ''}
                ${pc.email ? `<br><a href="mailto:${pc.email}">${esc(pc.email)}</a>` : ''}
            </div>`;
        }

        // Sectors
        if (data.sectors && data.sectors.length) {
            html += `<div style="margin-bottom:16px;">
                <strong style="color:#718096;">Sectors</strong><br>
                ${data.sectors.map(s => esc(s.sector_name || s)).join(', ')}
            </div>`;
        }

        // Agency
        if (data.agency_name) {
            html += `<div style="margin-bottom:16px;">
                <strong style="color:#718096;">Agency</strong><br>
                ${esc(data.agency_name)}
            </div>`;
        }

        // Markets
        if (data.markets) {
            html += `<div style="margin-bottom:16px;">
                <strong style="color:#718096;">Markets</strong><br>
                ${esc(data.markets)}
            </div>`;
        }

        // AE
        if (data.assigned_ae) {
            html += `<div>
                <strong style="color:#718096;">Assigned AE</strong><br>
                ${esc(data.assigned_ae)}
            </div>`;
        }

        html += '</div>';
        body.innerHTML = html;
    } catch (err) {
        body.innerHTML = '<p style="color:#e53e3e;">Failed to load.</p>';
    }
}

// ── Revenue Tab ───────────────────────────────────────────────────
async function loadRevenueTab() {
    if (!currentEntity) return;
    const body = document.getElementById('detail-body');
    const { type, id } = currentEntity;

    body.innerHTML = `<div class="revenue-chart-container">
        <canvas id="revenue-canvas"></canvas>
    </div>`;

    try {
        const resp = await fetch(
            `/api/ae/my-accounts/${type}/${id}/revenue-trend`
        );
        const data = await resp.json();

        if (data.length === 0) {
            body.innerHTML = `<div class="empty-state">
                <p>No revenue data yet.</p></div>`;
            return;
        }

        const ctx = document.getElementById('revenue-canvas')
            .getContext('2d');
        if (revenueChart) revenueChart.destroy();

        revenueChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: data.map(d => d.broadcast_month),
                datasets: [{
                    label: 'Revenue',
                    data: data.map(d => d.revenue),
                    backgroundColor: '#4299e1',
                    borderRadius: 3,
                }],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            label: ctx =>
                                '$' + ctx.parsed.y.toLocaleString(),
                        },
                    },
                },
                scales: {
                    y: {
                        ticks: {
                            callback: v =>
                                '$' + (v / 1000).toFixed(0) + 'k',
                        },
                    },
                },
            },
        });
    } catch (err) {
        body.innerHTML = '<p style="color:#e53e3e;">Failed to load.</p>';
    }
}

// ── Recent Activity Feed ──────────────────────────────────────────
async function loadRecentActivity() {
    const qs = new URLSearchParams(window.location.search);
    const ae = qs.get('ae') || '';
    let url = '/api/ae/my-accounts/recent-activity' +
        window.location.search;

    try {
        const resp = await fetch(url);
        const activities = await resp.json();
        const container = document.getElementById('recent-activity-list');

        if (activities.length === 0) {
            container.innerHTML = `<div class="empty-state">
                <p>No activity yet. Log your first note or call.</p>
            </div>`;
            return;
        }

        container.innerHTML = activities.map(a => `
            <div class="timeline-entry">
                <div class="timeline-meta">
                    <span class="badge badge-${a.activity_type}">
                        ${a.activity_type}</span>
                    ${formatDate(a.activity_date)}
                    &middot;
                    <span class="account-name"
                          onclick="openDetail('${a.entity_type}', ${a.entity_id})">
                        ${esc(a.entity_name || 'Unknown')}
                    </span>
                </div>
                <div class="timeline-desc">
                    ${esc(a.description || '')}
                </div>
            </div>
        `).join('');
    } catch (err) {
        console.error('Failed to load recent activity:', err);
    }
}

// ── Utilities ─────────────────────────────────────────────────────
function esc(str) {
    const div = document.createElement('div');
    div.textContent = str || '';
    return div.innerHTML;
}

function formatDate(dt) {
    if (!dt) return '';
    const d = new Date(dt);
    if (isNaN(d)) return dt;
    return d.toLocaleDateString('en-US', {
        month: 'short', day: 'numeric',
    });
}

function todayISO() {
    return new Date().toISOString().split('T')[0];
}
```

- [ ] **Step 5: Add recent-activity API endpoint to route**

In `src/web/routes/ae_crm.py`, add after `api_revenue_trend`:

```python
@ae_crm_bp.route("/api/ae/my-accounts/recent-activity")
@role_required(UserRole.AE)
def api_recent_activity():
    """Return recent activities across all AE's accounts."""
    ae_name, _, _, _ = _resolve_ae_name()
    activity_svc = _svc("activity_service")
    with _db().connection_ro() as conn:
        return jsonify(
            activity_svc.get_recent_activity_for_ae(
                conn, ae_name=ae_name or "", limit=15
            )
        )
```

- [ ] **Step 6: Restart service and verify page loads**

Run: `sudo systemctl restart ctv-bookedbiz-db.service`
Verify: `curl -s -o /dev/null -w "%{http_code}" http://localhost/ae/my-accounts`
Expected: 302 (redirect to login, confirming route is registered)

- [ ] **Step 7: Commit**

```bash
git add src/web/routes/ae_crm.py \
        src/web/blueprints.py \
        src/web/templates/ae_my_accounts.html \
        src/web/static/js/ae_my_accounts.js
git commit -m "feat: add AE My Accounts page with CRM layout

New /ae/my-accounts page with summary bar, accounts table,
detail slide-out panel with activity timeline and revenue chart,
action items section, and recent activity feed.

Blueprint registered, JS extracted to separate file, reuses
existing address-book activity endpoints."
```

---

## Chunk 3: Polish & Testing

### Task 5: Verify end-to-end and fix issues

This task is manual verification after deploying. No pre-written code —
work through each check and fix what breaks.

- [ ] **Step 1: Run linter and all tests**

Run: `cd /opt/apps/ctv-bookedbiz-db && ruff check src/services/ae_crm_service.py src/web/routes/ae_crm.py src/services/activity_service.py && python -m pytest tests/ -v --tb=short -x`
Expected: No lint errors, all tests PASS

- [ ] **Step 2: Restart service and test as admin**

```bash
sudo systemctl restart ctv-bookedbiz-db.service
```

Open `http://spotops/ae/my-accounts` in browser. Verify:
- Page loads with AE selector dropdown
- Selecting an AE loads their accounts
- Summary bar shows correct stats
- Accounts table populates with sortable columns
- Search filters accounts by name

- [ ] **Step 3: Test detail panel**

Click an account name. Verify:
- Slide-out panel opens
- Activity tab loads (may be empty — that's OK)
- Log a test note via the quick-add form
- Verify it appears in the timeline
- Verify it appears in the Recent Activity feed
- Switch to Info tab — verify account details load
- Switch to Revenue tab — verify chart renders

- [ ] **Step 4: Test follow-ups**

If follow-ups exist:
- Verify Action Items section appears when overdue/today items exist
- Click "Complete" on a follow-up
- Verify open follow-ups count decrements by 1 in summary bar
- If last actionable item, verify Action Items section disappears

If no follow-ups exist, create one via the activity form with a due date
set to today or earlier, then verify it shows in the action items.

- [ ] **Step 5: Test empty states**

- Select an AE with no accounts (or use a non-matching name via `?ae=Nobody`)
- Verify: summary shows zeros, table shows "No accounts assigned" message
- Open an account with no revenue data, switch to Revenue tab
- Verify: "No revenue data yet" message displays

- [ ] **Step 6: Commit any fixes**

```bash
git add src/services/ae_crm_service.py src/web/routes/ae_crm.py \
        src/web/templates/ae_my_accounts.html src/web/static/js/ae_my_accounts.js
git commit -m "fix: address issues found during manual testing"
```

(Only if fixes were needed.)

### Task 6: Final commit and summary

- [ ] **Step 1: Run full test suite one more time**

Run: `cd /opt/apps/ctv-bookedbiz-db && python -m pytest tests/ -v --tb=short`
Expected: All PASS

- [ ] **Step 2: Verify git log looks clean**

Run: `git log --oneline -10`
Verify: Atomic commits with descriptive messages.
