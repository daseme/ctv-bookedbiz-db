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
            agency_id INTEGER,
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
            (10, 'Acme Auto', 'Alice', 1, NULL, 'regular', 1);
        INSERT INTO customers VALUES
            (20, 'Campaign Co', 'Bob', 2, NULL, 'irregular', 1);
        INSERT INTO customers VALUES
            (30, 'Beta Motors', 'Alice', 1, NULL, 'regular', 1);
        INSERT INTO customers VALUES
            (40, 'Inactive Corp', 'Alice', 1, NULL, 'regular', 0);

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

        assert result["regular_total"] == 12000.0
        assert result["irregular_total"] == 14000.0

    def test_excludes_trade_revenue(self, rc_db):
        from src.services.revenue_classification_service import (
            RevenueClassificationService,
        )

        svc = RevenueClassificationService.__new__(
            RevenueClassificationService
        )
        result = svc.get_summary(rc_db, 2025)

        assert result["regular_total"] == 12000.0

    def test_excludes_null_customer_id(self, rc_db):
        from src.services.revenue_classification_service import (
            RevenueClassificationService,
        )

        svc = RevenueClassificationService.__new__(
            RevenueClassificationService
        )
        result = svc.get_summary(rc_db, 2025)

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

        rc_db.execute(
            "INSERT INTO customers VALUES (50, 'New Co', 'Alice', 1, NULL, NULL, 1)"
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
        assert "Beta Motors" in names
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
        assert acme["prior_year_revenue"] == 7500.0

    def test_yoy_change(self, rc_db):
        from src.services.revenue_classification_service import (
            RevenueClassificationService,
        )

        svc = RevenueClassificationService.__new__(
            RevenueClassificationService
        )
        result = svc.get_customers(rc_db, 2025)

        acme = next(c for c in result if c["name"] == "Acme Auto")
        assert acme["yoy_dollar"] == 4500.0
        assert abs(acme["yoy_pct"] - 60.0) < 0.1

    def test_yoy_new_customer(self, rc_db):
        from src.services.revenue_classification_service import (
            RevenueClassificationService,
        )

        svc = RevenueClassificationService.__new__(
            RevenueClassificationService
        )
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
