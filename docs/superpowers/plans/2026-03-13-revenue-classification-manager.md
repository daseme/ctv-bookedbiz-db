# Revenue Classification Manager — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `revenue_class` column to customers and build a management page that shows revenue broken out by regular/irregular classification with inline reclassification.

**Architecture:** New column on `customers` table, new `RevenueClassificationService` extending `BaseService`, new blueprint with page route + 3 API endpoints, Chart.js grouped bar chart, client-side filtering/sorting.

**Tech Stack:** Python/Flask, SQLite, Jinja2, vanilla JS, Chart.js

**Spec:** `docs/superpowers/specs/2026-03-13-revenue-classification-manager-design.md`

---

## File Structure

| Action | File | Responsibility |
|--------|------|---------------|
| Create | `sql/migrations/026_revenue_classification.sql` | Add column + seed political customers |
| Create | `src/services/revenue_classification_service.py` | Service with get_summary, get_customers, update_classification |
| Create | `tests/services/test_revenue_classification_service.py` | Unit tests for all service methods |
| Create | `src/web/routes/revenue_classification.py` | Blueprint: page route + 3 API endpoints |
| Create | `src/web/templates/revenue_classification_manager.html` | Light-theme page template with guide modal |
| Create | `src/web/static/js/revenue_classification_manager.js` | Client-side logic: chart, filters, sorting, toggle |
| Modify | `src/services/factory.py` | Register revenue_classification_service |
| Modify | `src/web/blueprints.py` | Register revenue_classification blueprint |
| Modify | `src/web/templates/base.html` | Add nav link in Data Management section |

---

## Task 1: Migration

**Files:**
- Create: `sql/migrations/026_revenue_classification.sql`

- [ ] **Step 1: Write the migration SQL**

```sql
-- 026_revenue_classification.sql
-- Add revenue classification (regular/irregular) to customers

ALTER TABLE customers ADD COLUMN revenue_class TEXT DEFAULT 'regular'
    CHECK (revenue_class IN ('regular', 'irregular'));

-- Seed political customers as irregular
UPDATE customers SET revenue_class = 'irregular'
WHERE sector_id IN (
    SELECT sector_id FROM sectors
    WHERE sector_code IN ('POLITICAL', 'POLITICAL-OUTREACH', 'POLITICALOUTREACH')
);
```

- [ ] **Step 2: Verify migration runs against dev DB**

```bash
sqlite3 /var/lib/ctv-bookedbiz-db/production.db < sql/migrations/026_revenue_classification.sql
```

Expected: No errors. Verify with:

```bash
sqlite3 /var/lib/ctv-bookedbiz-db/production.db "SELECT revenue_class, COUNT(*) FROM customers GROUP BY revenue_class;"
```

Expected: Two rows — `regular` with majority, `irregular` with political customers.

- [ ] **Step 3: Commit**

```bash
git add sql/migrations/026_revenue_classification.sql
git commit -m "feat: add revenue_class column to customers table"
```

---

## Task 2: Service — RevenueClassificationService

**Files:**
- Create: `src/services/revenue_classification_service.py`
- Create: `tests/services/test_revenue_classification_service.py`

**Context:**
- Services extend `BaseService` from `src.services.base_service` which takes `db_connection` in `__init__`
- All service methods accept `conn` (sqlite3.Connection with Row factory) as first arg after `self`
- Broadcast month format: `'Jan-25'` (mmm-yy). Year extraction: `CAST('20' || SUBSTR(broadcast_month, 5, 2) AS INTEGER)`
- Revenue excludes Trade: `WHERE (revenue_type != 'Trade' OR revenue_type IS NULL)`
- Only include spots with non-NULL `customer_id`

- [ ] **Step 1: Write failing tests for get_summary**

Create `tests/services/test_revenue_classification_service.py`:

```python
"""Tests for RevenueClassificationService."""

import sqlite3

import pytest


@pytest.fixture
def rc_db():
    """In-memory DB with tables and seed data for classification tests."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    conn.executescript("""
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            normalized_name TEXT,
            assigned_ae TEXT,
            sector_id INTEGER,
            revenue_class TEXT DEFAULT 'regular'
                CHECK (revenue_class IN ('regular', 'irregular')),
            is_active INTEGER DEFAULT 1
        );
        CREATE TABLE sectors (
            sector_id INTEGER PRIMARY KEY,
            sector_name TEXT,
            sector_code TEXT
        );
        CREATE TABLE spots (
            spot_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            broadcast_month TEXT,
            gross_rate REAL,
            revenue_type TEXT,
            is_historical INTEGER DEFAULT 0
        );

        -- Sectors
        INSERT INTO sectors VALUES (1, 'Automotive', 'AUTO');
        INSERT INTO sectors VALUES (2, 'Political', 'POLITICAL');

        -- Customers
        INSERT INTO customers VALUES
            (10, 'Acme Auto', 'Alice', 1, 'regular', 1);
        INSERT INTO customers VALUES
            (20, 'Campaign Co', 'Bob', 2, 'irregular', 1);
        INSERT INTO customers VALUES
            (30, 'Beta Motors', 'Alice', 1, 'regular', 1);
        INSERT INTO customers VALUES
            (40, 'Inactive Corp', 'Alice', 1, 'regular', 0);

        -- 2025 spots for Acme Auto (regular)
        INSERT INTO spots VALUES (1, 10, 'Jan-25', 5000.0, 'Cash', 1);
        INSERT INTO spots VALUES (2, 10, 'Feb-25', 3000.0, 'Cash', 1);
        INSERT INTO spots VALUES (3, 10, 'Mar-25', 4000.0, NULL, 0);

        -- 2025 spots for Campaign Co (irregular)
        INSERT INTO spots VALUES (4, 20, 'Feb-25', 8000.0, 'Cash', 1);
        INSERT INTO spots VALUES (5, 20, 'Mar-25', 6000.0, 'Cash', 0);

        -- Trade spot (should be excluded)
        INSERT INTO spots VALUES (6, 10, 'Jan-25', 2000.0, 'Trade', 1);

        -- Unresolved spot (NULL customer_id, should be excluded)
        INSERT INTO spots VALUES (7, NULL, 'Jan-25', 9000.0, 'Cash', 1);

        -- 2024 spots for YoY comparison
        INSERT INTO spots VALUES (8, 10, 'Jan-24', 4000.0, 'Cash', 1);
        INSERT INTO spots VALUES (9, 10, 'Feb-24', 3500.0, 'Cash', 1);
        INSERT INTO spots VALUES (10, 20, 'Mar-24', 7000.0, 'Cash', 1);

        -- Beta Motors: no revenue (tests zero-revenue customer display)
    """)

    yield conn
    conn.close()


class TestGetSummary:
    def test_returns_regular_and_irregular_totals(self, rc_db):
        from src.services.revenue_classification_service import (
            RevenueClassificationService,
        )

        svc = RevenueClassificationService.__new__(
            RevenueClassificationService
        )
        result = svc.get_summary(rc_db, 2025)

        assert result["regular_total"] == 12000.0  # 5k + 3k + 4k
        assert result["irregular_total"] == 14000.0  # 8k + 6k

    def test_excludes_trade_revenue(self, rc_db):
        from src.services.revenue_classification_service import (
            RevenueClassificationService,
        )

        svc = RevenueClassificationService.__new__(
            RevenueClassificationService
        )
        result = svc.get_summary(rc_db, 2025)

        # Trade spot of 2000 on Acme excluded
        assert result["regular_total"] == 12000.0

    def test_excludes_null_customer_id(self, rc_db):
        from src.services.revenue_classification_service import (
            RevenueClassificationService,
        )

        svc = RevenueClassificationService.__new__(
            RevenueClassificationService
        )
        result = svc.get_summary(rc_db, 2025)

        # Unresolved 9000 spot excluded
        total = result["regular_total"] + result["irregular_total"]
        assert total == 26000.0

    def test_regular_percentage(self, rc_db):
        from src.services.revenue_classification_service import (
            RevenueClassificationService,
        )

        svc = RevenueClassificationService.__new__(
            RevenueClassificationService
        )
        result = svc.get_summary(rc_db, 2025)

        expected_pct = 12000.0 / 26000.0 * 100
        assert abs(result["regular_pct"] - expected_pct) < 0.1

    def test_monthly_breakdown(self, rc_db):
        from src.services.revenue_classification_service import (
            RevenueClassificationService,
        )

        svc = RevenueClassificationService.__new__(
            RevenueClassificationService
        )
        result = svc.get_summary(rc_db, 2025)

        monthly = {m["month"]: m for m in result["monthly"]}
        assert monthly["Jan"]["regular"] == 5000.0
        assert monthly["Jan"].get("irregular", 0) == 0
        assert monthly["Feb"]["regular"] == 3000.0
        assert monthly["Feb"]["irregular"] == 8000.0

    def test_available_years(self, rc_db):
        from src.services.revenue_classification_service import (
            RevenueClassificationService,
        )

        svc = RevenueClassificationService.__new__(
            RevenueClassificationService
        )
        result = svc.get_summary(rc_db, 2025)

        assert 2024 in result["available_years"]
        assert 2025 in result["available_years"]

    def test_unclassified_count(self, rc_db):
        from src.services.revenue_classification_service import (
            RevenueClassificationService,
        )

        svc = RevenueClassificationService.__new__(
            RevenueClassificationService
        )
        result = svc.get_summary(rc_db, 2025)
        assert result["unclassified_count"] == 0

        # Add an unclassified customer
        rc_db.execute(
            "INSERT INTO customers VALUES (50, 'New Co', 'Alice', 1, NULL, 1)"
        )
        result = svc.get_summary(rc_db, 2025)
        assert result["unclassified_count"] == 1

    def test_filter_by_sector(self, rc_db):
        from src.services.revenue_classification_service import (
            RevenueClassificationService,
        )

        svc = RevenueClassificationService.__new__(
            RevenueClassificationService
        )
        result = svc.get_summary(
            rc_db, 2025, filters={"sector_id": 1}
        )

        # Only Automotive sector (Acme Auto)
        assert result["regular_total"] == 12000.0
        assert result["irregular_total"] == 0

    def test_filter_by_ae(self, rc_db):
        from src.services.revenue_classification_service import (
            RevenueClassificationService,
        )

        svc = RevenueClassificationService.__new__(
            RevenueClassificationService
        )
        result = svc.get_summary(rc_db, 2025, filters={"ae": "Bob"})

        # Only Bob's customer (Campaign Co)
        assert result["regular_total"] == 0
        assert result["irregular_total"] == 14000.0


class TestGetCustomers:
    def test_returns_all_active_customers(self, rc_db):
        from src.services.revenue_classification_service import (
            RevenueClassificationService,
        )

        svc = RevenueClassificationService.__new__(
            RevenueClassificationService
        )
        result = svc.get_customers(rc_db, 2025)

        names = [c["name"] for c in result]
        assert "Acme Auto" in names
        assert "Campaign Co" in names
        assert "Beta Motors" in names  # zero revenue, still active
        assert "Inactive Corp" not in names

    def test_revenue_totals(self, rc_db):
        from src.services.revenue_classification_service import (
            RevenueClassificationService,
        )

        svc = RevenueClassificationService.__new__(
            RevenueClassificationService
        )
        result = svc.get_customers(rc_db, 2025)

        acme = next(c for c in result if c["name"] == "Acme Auto")
        assert acme["current_year_revenue"] == 12000.0
        assert acme["prior_year_revenue"] == 7500.0  # 4000 + 3500

    def test_yoy_change(self, rc_db):
        from src.services.revenue_classification_service import (
            RevenueClassificationService,
        )

        svc = RevenueClassificationService.__new__(
            RevenueClassificationService
        )
        result = svc.get_customers(rc_db, 2025)

        acme = next(c for c in result if c["name"] == "Acme Auto")
        assert acme["yoy_dollar"] == 4500.0  # 12000 - 7500
        assert abs(acme["yoy_pct"] - 60.0) < 0.1  # 4500/7500*100

    def test_yoy_new_customer(self, rc_db):
        """Zero prior-year revenue should show yoy_pct as None."""
        from src.services.revenue_classification_service import (
            RevenueClassificationService,
        )

        svc = RevenueClassificationService.__new__(
            RevenueClassificationService
        )
        # Beta Motors has no revenue at all
        result = svc.get_customers(rc_db, 2025)
        beta = next(c for c in result if c["name"] == "Beta Motors")
        assert beta["prior_year_revenue"] == 0
        assert beta["yoy_pct"] is None

    def test_includes_classification_and_sector(self, rc_db):
        from src.services.revenue_classification_service import (
            RevenueClassificationService,
        )

        svc = RevenueClassificationService.__new__(
            RevenueClassificationService
        )
        result = svc.get_customers(rc_db, 2025)

        acme = next(c for c in result if c["name"] == "Acme Auto")
        assert acme["revenue_class"] == "regular"
        assert acme["sector_name"] == "Automotive"
        assert acme["assigned_ae"] == "Alice"

    def test_filter_by_classification(self, rc_db):
        from src.services.revenue_classification_service import (
            RevenueClassificationService,
        )

        svc = RevenueClassificationService.__new__(
            RevenueClassificationService
        )
        result = svc.get_customers(
            rc_db, 2025, filters={"classification": "irregular"}
        )

        names = [c["name"] for c in result]
        assert "Campaign Co" in names
        assert "Acme Auto" not in names


class TestUpdateClassification:
    def test_update_to_irregular(self, rc_db):
        from src.services.revenue_classification_service import (
            RevenueClassificationService,
        )

        svc = RevenueClassificationService.__new__(
            RevenueClassificationService
        )
        svc.update_classification(rc_db, 10, "irregular")

        row = rc_db.execute(
            "SELECT revenue_class FROM customers WHERE customer_id = 10"
        ).fetchone()
        assert row["revenue_class"] == "irregular"

    def test_update_to_regular(self, rc_db):
        from src.services.revenue_classification_service import (
            RevenueClassificationService,
        )

        svc = RevenueClassificationService.__new__(
            RevenueClassificationService
        )
        svc.update_classification(rc_db, 20, "regular")

        row = rc_db.execute(
            "SELECT revenue_class FROM customers WHERE customer_id = 20"
        ).fetchone()
        assert row["revenue_class"] == "regular"

    def test_rejects_invalid_value(self, rc_db):
        from src.services.revenue_classification_service import (
            RevenueClassificationService,
        )

        svc = RevenueClassificationService.__new__(
            RevenueClassificationService
        )
        with pytest.raises(ValueError):
            svc.update_classification(rc_db, 10, "unknown")

    def test_rejects_nonexistent_customer(self, rc_db):
        from src.services.revenue_classification_service import (
            RevenueClassificationService,
        )

        svc = RevenueClassificationService.__new__(
            RevenueClassificationService
        )
        with pytest.raises(ValueError):
            svc.update_classification(rc_db, 9999, "regular")
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/services/test_revenue_classification_service.py -v
```

Expected: ImportError — `revenue_classification_service` module doesn't exist yet.

- [ ] **Step 3: Implement the service**

Create `src/services/revenue_classification_service.py`:

```python
"""Service for revenue classification (regular/irregular) analysis."""

from src.services.base_service import BaseService

MONTH_ABBREVS = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]

YEAR_EXTRACT = "CAST('20' || SUBSTR(s.broadcast_month, 5, 2) AS INTEGER)"

TRADE_FILTER = "(s.revenue_type != 'Trade' OR s.revenue_type IS NULL)"

VALID_CLASSES = ("regular", "irregular")


class RevenueClassificationService(BaseService):
    """Analyze and manage customer revenue classification."""

    def __init__(self, db_connection):
        super().__init__(db_connection)

    def get_summary(self, conn, year, filters=None):
        """Return summary stats, monthly breakdown, and available years.

        Args:
            conn: sqlite3.Connection with Row factory.
            year: Integer year (e.g. 2025).
            filters: Optional dict with sector_id, ae, classification keys.

        Returns:
            Dict with regular_total, irregular_total, regular_pct,
            unclassified_count, monthly list, available_years list.
        """
        filters = filters or {}
        where_clauses = [
            "s.customer_id IS NOT NULL",
            TRADE_FILTER,
            f"{YEAR_EXTRACT} = :year",
        ]
        params = {"year": year}

        if filters.get("sector_id"):
            where_clauses.append("c.sector_id = :sector_id")
            params["sector_id"] = filters["sector_id"]
        if filters.get("ae"):
            where_clauses.append("c.assigned_ae = :ae")
            params["ae"] = filters["ae"]
        if filters.get("classification"):
            where_clauses.append("c.revenue_class = :classification")
            params["classification"] = filters["classification"]

        where = " AND ".join(where_clauses)

        rows = conn.execute(f"""
            SELECT
                SUBSTR(s.broadcast_month, 1, 3) AS month_abbr,
                c.revenue_class,
                SUM(s.gross_rate) AS total
            FROM spots s
            JOIN customers c ON s.customer_id = c.customer_id
            WHERE {where}
            GROUP BY month_abbr, c.revenue_class
        """, params).fetchall()

        regular_total = 0.0
        irregular_total = 0.0
        monthly_data = {}

        for row in rows:
            abbr = row["month_abbr"]
            cls = row["revenue_class"] or "regular"
            amount = row["total"] or 0.0

            if cls == "regular":
                regular_total += amount
            else:
                irregular_total += amount

            if abbr not in monthly_data:
                monthly_data[abbr] = {"month": abbr, "regular": 0, "irregular": 0}
            monthly_data[abbr][cls] += amount

        grand_total = regular_total + irregular_total
        regular_pct = (
            (regular_total / grand_total * 100) if grand_total > 0 else 0
        )

        monthly = []
        for abbr in MONTH_ABBREVS:
            monthly.append(
                monthly_data.get(abbr, {"month": abbr, "regular": 0, "irregular": 0})
            )

        unclassified = conn.execute(
            "SELECT COUNT(*) AS cnt FROM customers "
            "WHERE is_active = 1 AND revenue_class IS NULL"
        ).fetchone()["cnt"]

        year_rows = conn.execute(f"""
            SELECT DISTINCT {YEAR_EXTRACT} AS yr
            FROM spots s
            WHERE s.customer_id IS NOT NULL AND {TRADE_FILTER}
            ORDER BY yr
        """).fetchall()
        available_years = [r["yr"] for r in year_rows]

        return {
            "regular_total": regular_total,
            "irregular_total": irregular_total,
            "regular_pct": round(regular_pct, 1),
            "unclassified_count": unclassified,
            "monthly": monthly,
            "available_years": available_years,
        }

    def get_customers(self, conn, year, filters=None):
        """Return customer list with current and prior year revenue.

        Args:
            conn: sqlite3.Connection with Row factory.
            year: Integer year (e.g. 2025).
            filters: Optional dict with sector_id, ae, classification keys.

        Returns:
            List of dicts with customer_id, name, sector_name,
            revenue_class, assigned_ae, current_year_revenue,
            prior_year_revenue, yoy_dollar, yoy_pct.
        """
        filters = filters or {}
        cust_where = ["c.is_active = 1"]
        params = {"year": year, "prior_year": year - 1}

        if filters.get("sector_id"):
            cust_where.append("c.sector_id = :sector_id")
            params["sector_id"] = filters["sector_id"]
        if filters.get("ae"):
            cust_where.append("c.assigned_ae = :ae")
            params["ae"] = filters["ae"]
        if filters.get("classification"):
            cust_where.append("c.revenue_class = :classification")
            params["classification"] = filters["classification"]

        cust_filter = " AND ".join(cust_where)

        spot_filter = (
            "s.customer_id IS NOT NULL AND "
            f"{TRADE_FILTER}"
        )

        rows = conn.execute(f"""
            SELECT
                c.customer_id,
                c.normalized_name AS name,
                c.sector_id,
                COALESCE(sec.sector_name, '') AS sector_name,
                COALESCE(c.revenue_class, 'regular') AS revenue_class,
                COALESCE(c.assigned_ae, '') AS assigned_ae,
                COALESCE(cy.rev, 0) AS current_year_revenue,
                COALESCE(py.rev, 0) AS prior_year_revenue
            FROM customers c
            LEFT JOIN sectors sec ON c.sector_id = sec.sector_id
            LEFT JOIN (
                SELECT s.customer_id, SUM(s.gross_rate) AS rev
                FROM spots s
                WHERE {spot_filter}
                  AND {YEAR_EXTRACT} = :year
                GROUP BY s.customer_id
            ) cy ON c.customer_id = cy.customer_id
            LEFT JOIN (
                SELECT s.customer_id, SUM(s.gross_rate) AS rev
                FROM spots s
                WHERE {spot_filter}
                  AND {YEAR_EXTRACT} = :prior_year
                GROUP BY s.customer_id
            ) py ON c.customer_id = py.customer_id
            WHERE {cust_filter}
            ORDER BY current_year_revenue DESC
        """, params).fetchall()

        result = []
        for r in rows:
            cur = r["current_year_revenue"]
            prior = r["prior_year_revenue"]
            yoy_dollar = cur - prior
            yoy_pct = (
                round(yoy_dollar / prior * 100, 1)
                if prior > 0 else None
            )
            result.append({
                "customer_id": r["customer_id"],
                "name": r["name"],
                "sector_id": r["sector_id"],
                "sector_name": r["sector_name"],
                "revenue_class": r["revenue_class"],
                "assigned_ae": r["assigned_ae"],
                "current_year_revenue": cur,
                "prior_year_revenue": prior,
                "yoy_dollar": yoy_dollar,
                "yoy_pct": yoy_pct,
            })

        return result

    def update_classification(self, conn, customer_id, revenue_class):
        """Update a customer's revenue classification.

        Args:
            conn: sqlite3.Connection.
            customer_id: Integer customer ID.
            revenue_class: 'regular' or 'irregular'.

        Raises:
            ValueError: If revenue_class is invalid or customer not found.
        """
        if revenue_class not in VALID_CLASSES:
            raise ValueError(
                f"Invalid revenue_class '{revenue_class}'. "
                f"Must be one of: {VALID_CLASSES}"
            )

        row = conn.execute(
            "SELECT customer_id FROM customers WHERE customer_id = ?",
            (customer_id,),
        ).fetchone()
        if not row:
            raise ValueError(f"Customer {customer_id} not found")

        conn.execute(
            "UPDATE customers SET revenue_class = ? WHERE customer_id = ?",
            (revenue_class, customer_id),
        )
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/services/test_revenue_classification_service.py -v
```

Expected: All 20 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/services/revenue_classification_service.py tests/services/test_revenue_classification_service.py
git commit -m "feat: add RevenueClassificationService with tests"
```

---

## Task 3: Routes and Blueprint

**Files:**
- Create: `src/web/routes/revenue_classification.py`
- Modify: `src/services/factory.py`
- Modify: `src/web/blueprints.py`

**Context:**
- Follow the pattern from `src/web/routes/manager_dashboard.py`: helper functions `_db()` and `_svc()`, `@role_required(UserRole.MANAGEMENT)` on all routes
- Use `connection_ro()` for GET endpoints, `connection()` for PATCH
- Register in factory using the same pattern as `create_health_score_service`
- Register blueprint in `src/web/blueprints.py` after manager_bp

- [ ] **Step 1: Create the routes file**

Create `src/web/routes/revenue_classification.py`:

```python
"""Revenue Classification Manager routes and API."""

import logging
from flask import Blueprint, render_template, jsonify, request

from src.models.users import UserRole
from src.services.container import get_container
from src.web.utils.auth import role_required

logger = logging.getLogger(__name__)

revenue_class_bp = Blueprint("revenue_classification", __name__)


def _db():
    return get_container().get("database_connection")


def _svc():
    return get_container().get("revenue_classification_service")


def _parse_filters():
    """Extract filter params from query string."""
    filters = {}
    sector_id = request.args.get("sector_id", type=int)
    if sector_id:
        filters["sector_id"] = sector_id
    ae = request.args.get("ae", "").strip()
    if ae:
        filters["ae"] = ae
    classification = request.args.get("classification", "").strip()
    if classification in ("regular", "irregular"):
        filters["classification"] = classification
    return filters or None


@revenue_class_bp.route("/reports/revenue-classification-manager")
@role_required(UserRole.MANAGEMENT)
def revenue_classification_page():
    """Render the Revenue Classification Manager page."""
    return render_template(
        "revenue_classification_manager.html",
        title="Revenue Classification Manager",
    )


@revenue_class_bp.route("/api/revenue-classification/summary")
@role_required(UserRole.MANAGEMENT)
def api_summary():
    """Return summary stats and monthly chart data."""
    year = request.args.get("year", type=int)
    if not year:
        return jsonify({"error": "year parameter required"}), 400

    svc = _svc()
    filters = _parse_filters()
    with _db().connection_ro() as conn:
        return jsonify(svc.get_summary(conn, year, filters))


@revenue_class_bp.route("/api/revenue-classification/customers")
@role_required(UserRole.MANAGEMENT)
def api_customers():
    """Return customer list with revenue data."""
    year = request.args.get("year", type=int)
    if not year:
        return jsonify({"error": "year parameter required"}), 400

    svc = _svc()
    filters = _parse_filters()
    with _db().connection_ro() as conn:
        return jsonify(svc.get_customers(conn, year, filters))


@revenue_class_bp.route(
    "/api/revenue-classification/<int:customer_id>",
    methods=["PATCH"],
)
@role_required(UserRole.MANAGEMENT)
def api_update_classification(customer_id):
    """Update a customer's revenue classification."""
    data = request.get_json(silent=True) or {}
    revenue_class = data.get("revenue_class", "").strip()

    if revenue_class not in ("regular", "irregular"):
        return jsonify({"error": "revenue_class must be 'regular' or 'irregular'"}), 400

    svc = _svc()
    with _db().connection() as conn:
        try:
            svc.update_classification(conn, customer_id, revenue_class)
            conn.commit()
            return jsonify({"success": True, "revenue_class": revenue_class})
        except ValueError as e:
            return jsonify({"error": str(e)}), 404
```

- [ ] **Step 2: Register factory function**

Add a new factory function to `src/services/factory.py` — after the `create_manager_dashboard_service` function definition (around line 955):

```python
def create_revenue_classification_service():
    """Factory function for RevenueClassificationService."""
    from src.services.revenue_classification_service import RevenueClassificationService

    container = get_container()
    db_connection = container.get("database_connection")
    return RevenueClassificationService(db_connection)
```

And in the `initialize_services()` function (around line 242), after the `manager_dashboard_service` registration (around line 344), add:

```python
        container.register_singleton("revenue_classification_service", create_revenue_classification_service)
```

**Important:** Add to `initialize_services()`, NOT to `register_default_services()`. The app uses `initialize_services()` at startup.

- [ ] **Step 3: Register blueprint**

In `src/web/blueprints.py`:

Add import at top with other blueprint imports:
```python
from src.web.routes.revenue_classification import revenue_class_bp
```

Add registration in `register_blueprints()` after the manager_bp registration:
```python
        app.register_blueprint(revenue_class_bp)
        logger.info("Registered revenue classification blueprint")
```

- [ ] **Step 4: Verify the app starts without errors**

```bash
cd /opt/apps/ctv-bookedbiz-db && python -c "from src.web.routes.revenue_classification import revenue_class_bp; print('Import OK')"
```

- [ ] **Step 5: Commit**

```bash
git add src/web/routes/revenue_classification.py src/services/factory.py src/web/blueprints.py
git commit -m "feat: add revenue classification routes, factory, and blueprint"
```

---

## Task 4: Template

**Files:**
- Create: `src/web/templates/revenue_classification_manager.html`

**Context:**
- Extends `base.html` using blocks: `title`, `breadcrumb`, `extra_styles`, `header_title`, `header_subtitle`, `content`, `extra_js`
- Light theme (white cards, blue accents) — same as customer_sector_manager.html
- Guide modal uses same CSS pattern as customer_sector_manager.html (`.info-btn`, `.info-modal`, `#info-modal`)
- Chart.js loaded from CDN (already available in base.html or load from cdnjs)

- [ ] **Step 1: Create the template**

Create `src/web/templates/revenue_classification_manager.html`:

```html
{% extends "base.html" %}

{% block title %}Revenue Classification Manager - CTV Booked Biz{% endblock %}

{% block breadcrumb %}
<span class="breadcrumb-separator">></span>
<span>Data Management</span>
<span class="breadcrumb-separator">></span>
<span class="breadcrumb-current">Revenue Classification Manager</span>
{% endblock %}

{% block extra_styles %}
<style>
  .rcm-wrap{max-width:1400px;margin:0 auto;padding:20px}
  .rcm-summary{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:24px}
  .rcm-card{background:#fff;border-radius:10px;padding:20px;box-shadow:0 1px 3px rgba(0,0,0,.08);text-align:center}
  .rcm-card .value{font-size:28px;font-weight:700;color:#0f172a}
  .rcm-card .label{font-size:13px;color:#64748b;margin-top:4px}
  .rcm-card.regular .value{color:#2563eb}
  .rcm-card.irregular .value{color:#d97706}
  .rcm-controls{display:flex;gap:12px;flex-wrap:wrap;align-items:center;margin-bottom:24px;background:#fff;padding:16px;border-radius:10px;box-shadow:0 1px 3px rgba(0,0,0,.08)}
  .rcm-controls select,.rcm-controls input{padding:8px 12px;border:1px solid #d1d5db;border-radius:6px;font-size:13px;background:#fff}
  .rcm-controls select:focus,.rcm-controls input:focus{outline:none;border-color:#3b82f6;box-shadow:0 0 0 2px rgba(59,130,246,.15)}
  .rcm-chart-wrap{background:#fff;border-radius:10px;padding:20px;margin-bottom:24px;box-shadow:0 1px 3px rgba(0,0,0,.08)}
  .rcm-chart-wrap canvas{max-height:350px}
  .rcm-table-wrap{background:#fff;border-radius:10px;padding:20px;box-shadow:0 1px 3px rgba(0,0,0,.08)}
  .rcm-table{width:100%;border-collapse:collapse;font-size:13px}
  .rcm-table th{padding:10px 12px;text-align:left;border-bottom:2px solid #e2e8f0;color:#64748b;font-weight:600;cursor:pointer;white-space:nowrap;user-select:none}
  .rcm-table th:hover{color:#0f172a}
  .rcm-table th .sort-arrow{margin-left:4px;opacity:.4}
  .rcm-table th.sorted .sort-arrow{opacity:1}
  .rcm-table td{padding:10px 12px;border-bottom:1px solid #f1f5f9}
  .rcm-table tr:hover{background:#f8fafc}
  .rcm-table .customer-link{color:#2563eb;text-decoration:none}
  .rcm-table .customer-link:hover{text-decoration:underline}
  .cls-toggle{padding:4px 14px;border-radius:20px;border:1px solid;font-size:12px;font-weight:600;cursor:pointer;transition:all .15s}
  .cls-toggle.regular{background:#eff6ff;border-color:#93c5fd;color:#2563eb}
  .cls-toggle.irregular{background:#fffbeb;border-color:#fcd34d;color:#d97706}
  .cls-toggle:hover{filter:brightness(.95)}
  .yoy-positive{color:#16a34a;font-weight:600}
  .yoy-negative{color:#dc2626;font-weight:600}
  .yoy-new{color:#64748b;font-style:italic}
  .info-btn{width:28px;height:28px;border-radius:50%;border:1px solid #94a3b8;background:#fff;color:#64748b;font-size:14px;font-weight:600;cursor:pointer;display:flex;align-items:center;justify-content:center;line-height:1}
  .info-btn:hover{background:#f0f9ff;border-color:#0ea5e9;color:#0ea5e9}
  .info-modal{background:#fff;border-radius:12px;width:100%;max-width:560px;box-shadow:0 20px 40px rgba(0,0,0,0.25)}
  .info-modal .modal-header{padding:20px 24px;border-bottom:1px solid #e2e8f0;display:flex;justify-content:space-between;align-items:center;background:#f0f9ff}
  .info-modal .modal-header h3{margin:0;font-size:18px;color:#0369a1}
  .info-modal .modal-body{padding:24px;max-height:70vh;overflow-y:auto}
  .info-modal .info-section{margin-bottom:20px}
  .info-modal .info-section:last-child{margin-bottom:0}
  .info-modal .info-section h4{margin:0 0 6px;font-size:14px;color:#0f172a;display:flex;align-items:center;gap:8px}
  .info-modal .info-section p{margin:0;font-size:13px;color:#475569;line-height:1.5}
  .info-modal .info-section ul{margin:4px 0 0;padding-left:18px;font-size:13px;color:#475569;line-height:1.6}
  #info-modal{position:fixed;inset:0;background:rgba(0,0,0,.35);display:none;align-items:center;justify-content:center;z-index:999}
  #info-modal.active{display:flex}
  .rcm-header-row{display:flex;justify-content:space-between;align-items:center;margin-bottom:16px}
  .loading-msg{text-align:center;padding:40px;color:#94a3b8;font-size:14px}
</style>
{% endblock %}

{% block header_title %}Revenue Classification Manager{% endblock %}
{% block header_subtitle %}Analyze and manage regular vs irregular revenue for board reporting{% endblock %}

{% block content %}
<div class="rcm-wrap">
  <!-- Header with guide button -->
  <div class="rcm-header-row">
    <div></div>
    <button class="info-btn" onclick="document.getElementById('info-modal').classList.add('active')" title="How to use this tool">?</button>
  </div>

  <!-- Summary Cards -->
  <div class="rcm-summary">
    <div class="rcm-card regular">
      <div class="value" id="regular-total">-</div>
      <div class="label">Regular Revenue</div>
    </div>
    <div class="rcm-card irregular">
      <div class="value" id="irregular-total">-</div>
      <div class="label">Irregular Revenue</div>
    </div>
    <div class="rcm-card">
      <div class="value" id="regular-pct">-</div>
      <div class="label">Regular % of Total</div>
    </div>
    <div class="rcm-card">
      <div class="value" id="unclassified-count">-</div>
      <div class="label">Unclassified Customers</div>
    </div>
  </div>

  <!-- Controls -->
  <div class="rcm-controls">
    <select id="year-select"><option>Loading...</option></select>
    <select id="sector-filter"><option value="">All Sectors</option></select>
    <select id="ae-filter"><option value="">All AEs</option></select>
    <select id="class-filter">
      <option value="">All Classifications</option>
      <option value="regular">Regular</option>
      <option value="irregular">Irregular</option>
    </select>
    <input type="text" id="search-input" placeholder="Search customers...">
  </div>

  <!-- Chart -->
  <div class="rcm-chart-wrap">
    <canvas id="revenue-chart"></canvas>
  </div>

  <!-- Customer Table -->
  <div class="rcm-table-wrap">
    <table class="rcm-table">
      <thead>
        <tr>
          <th data-col="name">Customer <span class="sort-arrow">&#9650;</span></th>
          <th data-col="sector_name">Sector <span class="sort-arrow">&#9650;</span></th>
          <th data-col="revenue_class">Classification <span class="sort-arrow">&#9650;</span></th>
          <th data-col="assigned_ae">AE <span class="sort-arrow">&#9650;</span></th>
          <th data-col="current_year_revenue" id="th-current-year">2026 Revenue <span class="sort-arrow">&#9650;</span></th>
          <th data-col="prior_year_revenue" id="th-prior-year">2025 Revenue <span class="sort-arrow">&#9650;</span></th>
          <th data-col="yoy_dollar">YoY Change <span class="sort-arrow">&#9650;</span></th>
        </tr>
      </thead>
      <tbody id="customer-tbody">
        <tr><td colspan="7" class="loading-msg">Loading...</td></tr>
      </tbody>
    </table>
  </div>
</div>

<!-- Guide Modal -->
<div id="info-modal">
  <div class="info-modal">
    <div class="modal-header">
      <h3>Revenue Classification Manager</h3>
      <button onclick="document.getElementById('info-modal').classList.remove('active')" style="background:none;border:none;font-size:20px;cursor:pointer;color:#64748b">&times;</button>
    </div>
    <div class="modal-body">
      <div class="info-section">
        <h4>What is this?</h4>
        <p>This tool helps you understand revenue in two buckets: <strong>Regular</strong> (recurring, predictable revenue like casinos, auto dealers, healthcare) and <strong>Irregular</strong> (cyclical or one-time like political campaigns or COVID-era government spending).</p>
      </div>
      <div class="info-section">
        <h4>Why it matters</h4>
        <p>The board needs to see the predictability story. Regular revenue is what we can count on year-over-year. Irregular revenue inflates totals during peak cycles but disappears in off-years. Separating them makes forecasting reliable.</p>
      </div>
      <div class="info-section">
        <h4>Summary Cards</h4>
        <ul>
          <li><strong>Regular Revenue</strong> — Total booked revenue from regular customers for the selected year</li>
          <li><strong>Irregular Revenue</strong> — Total booked revenue from irregular customers</li>
          <li><strong>Regular %</strong> — What portion of total revenue is predictable</li>
          <li><strong>Unclassified</strong> — Customers without a classification (should be zero)</li>
        </ul>
      </div>
      <div class="info-section">
        <h4>Monthly Chart</h4>
        <p>Shows regular (blue) and irregular (amber) revenue side by side for each month. Includes forward-booked months, not just closed months.</p>
      </div>
      <div class="info-section">
        <h4>Reclassifying Customers</h4>
        <p>Click the <strong>Regular</strong> or <strong>Irregular</strong> button in the table to toggle a customer's classification. Changes save immediately and update all totals.</p>
      </div>
      <div class="info-section">
        <h4>Filters</h4>
        <p>Use the year selector, sector, AE, and classification dropdowns to focus on specific subsets. The search box filters by customer name.</p>
      </div>
    </div>
  </div>
</div>
{% endblock %}

{% block extra_js %}
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<script src="{{ url_for('static', filename='js/revenue_classification_manager.js') }}"></script>
{% endblock %}
```

- [ ] **Step 2: Commit**

```bash
git add src/web/templates/revenue_classification_manager.html
git commit -m "feat: add revenue classification manager template"
```

---

## Task 5: JavaScript

**Files:**
- Create: `src/web/static/js/revenue_classification_manager.js`

**Context:**
- API endpoints: `/api/revenue-classification/summary?year=YYYY`, `/api/revenue-classification/customers?year=YYYY`, `PATCH /api/revenue-classification/<id>`
- All filters (sector, AE, classification) sent as query params to API
- Chart.js 4.x API (use `new Chart(ctx, config)`)
- Format dollar amounts with `$XX,XXX` pattern
- Sorting is client-side on the `allCustomers` array
- Year selector populated from `summary.available_years`
- Sector and AE dropdowns populated from distinct values in customer data

- [ ] **Step 1: Create the JavaScript file**

Create `src/web/static/js/revenue_classification_manager.js`:

```javascript
(function () {
  'use strict';

  const $ = (sel) => document.querySelector(sel);
  const fmt = (n) => '$' + Math.round(n).toLocaleString();

  let chart = null;
  let allCustomers = [];
  let sortCol = 'current_year_revenue';
  let sortAsc = false;

  const yearSel = $('#year-select');
  const sectorSel = $('#sector-filter');
  const aeSel = $('#ae-filter');
  const classSel = $('#class-filter');
  const searchInput = $('#search-input');

  function currentYear() {
    return parseInt(yearSel.value, 10);
  }

  function filterParams() {
    const p = new URLSearchParams({ year: currentYear() });
    if (sectorSel.value) p.set('sector_id', sectorSel.value);
    if (aeSel.value) p.set('ae', aeSel.value);
    if (classSel.value) p.set('classification', classSel.value);
    return p.toString();
  }

  async function loadData() {
    const [summaryRes, customersRes] = await Promise.all([
      fetch('/api/revenue-classification/summary?' + filterParams()),
      fetch('/api/revenue-classification/customers?' + filterParams()),
    ]);

    const summary = await summaryRes.json();
    const customers = await customersRes.json();

    renderSummary(summary);
    renderChart(summary.monthly);
    populateYears(summary.available_years);
    allCustomers = customers;
    populateFilterDropdowns(customers);
    renderTable();
  }

  function renderSummary(s) {
    $('#regular-total').textContent = fmt(s.regular_total);
    $('#irregular-total').textContent = fmt(s.irregular_total);
    $('#regular-pct').textContent = s.regular_pct.toFixed(1) + '%';
    $('#unclassified-count').textContent = s.unclassified_count;
  }

  function populateYears(years) {
    const cur = yearSel.value;
    if (yearSel.options.length <= 1 || yearSel.options[0].text === 'Loading...') {
      yearSel.innerHTML = '';
      const now = new Date().getFullYear();
      const defaultYear = years.includes(now) ? now : years[years.length - 1];
      years.forEach((y) => {
        const opt = new Option(y, y);
        if (y === defaultYear && !cur) opt.selected = true;
        yearSel.appendChild(opt);
      });
      if (cur && years.includes(parseInt(cur, 10))) {
        yearSel.value = cur;
      }
    }
    const yr = currentYear();
    const thCur = $('#th-current-year');
    const thPrior = $('#th-prior-year');
    if (thCur) thCur.firstChild.textContent = yr + ' Revenue ';
    if (thPrior) thPrior.firstChild.textContent = (yr - 1) + ' Revenue ';
  }

  function populateFilterDropdowns(customers) {
    const curSector = sectorSel.value;
    const curAe = aeSel.value;

    // Build unique sector (id, name) pairs and AE names
    const sectorMap = new Map();
    customers.forEach((c) => {
      if (c.sector_id && c.sector_name) sectorMap.set(c.sector_id, c.sector_name);
    });
    const aes = [...new Set(customers.map((c) => c.assigned_ae).filter(Boolean))].sort();

    if (sectorSel.options.length <= 1) {
      [...sectorMap.entries()]
        .sort((a, b) => a[1].localeCompare(b[1]))
        .forEach(([id, name]) => sectorSel.appendChild(new Option(name, id)));
    }
    if (aeSel.options.length <= 1) {
      aes.forEach((a) => aeSel.appendChild(new Option(a, a)));
    }

    if (curSector) sectorSel.value = curSector;
    if (curAe) aeSel.value = curAe;
  }

  function renderChart(monthly) {
    const ctx = $('#revenue-chart');
    if (chart) chart.destroy();

    chart = new Chart(ctx, {
      type: 'bar',
      data: {
        labels: monthly.map((m) => m.month),
        datasets: [
          {
            label: 'Regular',
            data: monthly.map((m) => m.regular),
            backgroundColor: '#3b82f6',
            borderRadius: 4,
          },
          {
            label: 'Irregular',
            data: monthly.map((m) => m.irregular),
            backgroundColor: '#f59e0b',
            borderRadius: 4,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { position: 'top' },
          tooltip: {
            callbacks: {
              label: (ctx) => ctx.dataset.label + ': ' + fmt(ctx.raw),
            },
          },
        },
        scales: {
          y: {
            ticks: {
              callback: (v) => '$' + (v / 1000).toFixed(0) + 'k',
            },
          },
        },
      },
    });
  }

  function renderTable() {
    const search = searchInput.value.toLowerCase();
    let filtered = allCustomers;

    if (search) {
      filtered = filtered.filter((c) => c.name.toLowerCase().includes(search));
    }

    filtered.sort((a, b) => {
      let va = a[sortCol];
      let vb = b[sortCol];
      if (typeof va === 'string') {
        va = (va || '').toLowerCase();
        vb = (vb || '').toLowerCase();
      }
      if (va == null) va = -Infinity;
      if (vb == null) vb = -Infinity;
      if (va < vb) return sortAsc ? -1 : 1;
      if (va > vb) return sortAsc ? 1 : -1;
      return 0;
    });

    const tbody = $('#customer-tbody');
    if (filtered.length === 0) {
      tbody.innerHTML = '<tr><td colspan="7" class="loading-msg">No customers found</td></tr>';
      return;
    }

    tbody.innerHTML = filtered
      .map(
        (c) => `
      <tr>
        <td><a href="/address-book/customer/${c.customer_id}" class="customer-link">${esc(c.name)}</a></td>
        <td>${esc(c.sector_name)}</td>
        <td>
          <button class="cls-toggle ${c.revenue_class}"
            data-id="${c.customer_id}"
            data-cls="${c.revenue_class}"
            onclick="window._rcmToggle(this)">
            ${c.revenue_class === 'regular' ? 'Regular' : 'Irregular'}
          </button>
        </td>
        <td>${esc(c.assigned_ae)}</td>
        <td>${fmt(c.current_year_revenue)}</td>
        <td>${fmt(c.prior_year_revenue)}</td>
        <td>${yoyCell(c)}</td>
      </tr>`
      )
      .join('');
  }

  function yoyCell(c) {
    if (c.yoy_pct === null || c.yoy_pct === undefined) {
      if (c.current_year_revenue > 0) {
        return '<span class="yoy-new">New</span>';
      }
      return '<span class="yoy-new">-</span>';
    }
    const cls = c.yoy_dollar >= 0 ? 'yoy-positive' : 'yoy-negative';
    const sign = c.yoy_dollar >= 0 ? '+' : '';
    return `<span class="${cls}">${sign}${fmt(c.yoy_dollar)} (${sign}${c.yoy_pct.toFixed(1)}%)</span>`;
  }

  function esc(str) {
    if (!str) return '';
    const d = document.createElement('div');
    d.textContent = str;
    return d.innerHTML;
  }

  window._rcmToggle = async function (btn) {
    const id = btn.dataset.id;
    const oldCls = btn.dataset.cls;
    const newCls = oldCls === 'regular' ? 'irregular' : 'regular';

    btn.textContent = newCls === 'regular' ? 'Regular' : 'Irregular';
    btn.className = 'cls-toggle ' + newCls;
    btn.dataset.cls = newCls;

    const cust = allCustomers.find((c) => c.customer_id === parseInt(id, 10));
    if (cust) cust.revenue_class = newCls;

    try {
      const res = await fetch('/api/revenue-classification/' + id, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ revenue_class: newCls }),
      });
      if (!res.ok) throw new Error('Save failed');

      const [summaryRes] = await Promise.all([
        fetch('/api/revenue-classification/summary?' + filterParams()),
      ]);
      const summary = await summaryRes.json();
      renderSummary(summary);
      renderChart(summary.monthly);
    } catch (e) {
      btn.textContent = oldCls === 'regular' ? 'Regular' : 'Irregular';
      btn.className = 'cls-toggle ' + oldCls;
      btn.dataset.cls = oldCls;
      if (cust) cust.revenue_class = oldCls;
      console.error('Classification update failed:', e);
    }
  };

  // Sorting
  document.querySelectorAll('.rcm-table th[data-col]').forEach((th) => {
    th.addEventListener('click', () => {
      const col = th.dataset.col;
      if (sortCol === col) {
        sortAsc = !sortAsc;
      } else {
        sortCol = col;
        sortAsc = col === 'name' || col === 'sector_name' || col === 'assigned_ae';
      }

      document.querySelectorAll('.rcm-table th').forEach((h) => h.classList.remove('sorted'));
      th.classList.add('sorted');
      th.querySelector('.sort-arrow').textContent = sortAsc ? '\u25B2' : '\u25BC';

      renderTable();
    });
  });

  // Filter events
  yearSel.addEventListener('change', loadData);
  sectorSel.addEventListener('change', loadData);
  aeSel.addEventListener('change', loadData);
  classSel.addEventListener('change', loadData);
  searchInput.addEventListener('input', renderTable);

  // Initial load
  loadData();
})();
```

- [ ] **Step 2: Commit**

```bash
git add src/web/static/js/revenue_classification_manager.js
git commit -m "feat: add revenue classification manager JavaScript"
```

---

## Task 6: Navigation Link

**Files:**
- Modify: `src/web/templates/base.html`

**Context:**
- Add link in the Data Management section, after the Customer Sector Manager link (around line 646)
- Same HTML pattern as other nav links: `nav-dropdown-item` with `nav-dropdown-item-title` and `nav-dropdown-item-desc`
- Role-gated to management/admin in the `activeLinks` JS section (around line 775)

- [ ] **Step 1: Add nav link**

In `src/web/templates/base.html`, after the Customer Sector Manager link (after the `</a>` around line 646), add:

```html
                        <a href="/reports/revenue-classification-manager" class="nav-dropdown-item">
                            <div class="nav-dropdown-item-title">Revenue Classification</div>
                            <div class="nav-dropdown-item-desc">Manage regular vs irregular revenue</div>
                        </a>
```

- [ ] **Step 2: Add to activeLinks mapping**

In the JavaScript section of base.html (around line 775), add to the `activeLinks` object:

```javascript
                '/reports/revenue-classification-manager': 'datamanagement',
```

- [ ] **Step 3: Commit**

```bash
git add src/web/templates/base.html
git commit -m "feat: add revenue classification nav link in base template"
```

---

## Task 7: Integration Test and Verify

- [ ] **Step 1: Run all service tests**

```bash
pytest tests/services/test_revenue_classification_service.py -v
```

Expected: All 20 tests pass.

- [ ] **Step 2: Run full test suite to check for regressions**

```bash
pytest tests/ -v --tb=short
```

Expected: No new failures.

- [ ] **Step 3: Restart service and verify page loads**

```bash
sudo systemctl restart ctv-bookedbiz-db.service
```

Then verify:
- Page loads at `http://spotops/reports/revenue-classification-manager`
- Summary cards show data
- Chart renders
- Customer table populates
- Classification toggle works
- Filters work

- [ ] **Step 4: Commit any fixes needed**

If integration issues arise, fix and commit with descriptive message.
