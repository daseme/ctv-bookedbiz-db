"""Unit tests for PlanningExportService.

See docs/planning-export-client-contract.md §2 (response shape) and §3
(null rules). Helpers are tested in isolation for the null-handling
semantics Kurt specified:

  - expected  = max(booked, COALESCE(forecast, budget))
  - pipeline  = max(0, COALESCE(forecast, budget) − booked)   FLOORED
  - vs_budget = booked − budget                                SIGNED
"""

import sqlite3
from contextlib import contextmanager

import pytest

from src.services.planning_export_service import (
    PlanningExportService,
    compute_expected,
    compute_pipeline,
    compute_vs_budget,
)


# ---------------------------------------------------------------------------
# Helper-level tests (no DB). Null rules must match client contract §3.
# ---------------------------------------------------------------------------


class TestComputeExpected:
    """max(booked, COALESCE(forecast, budget)). booked is never None."""

    def test_forecast_wins_when_present(self):
        # forecast 100, budget 80, booked 50 → max(50, 100) = 100
        assert compute_expected(booked=50.0, forecast=100.0, budget=80.0) == 100.0

    def test_budget_is_fallback_when_forecast_null(self):
        # forecast null, budget 80, booked 50 → max(50, 80) = 80
        assert compute_expected(booked=50.0, forecast=None, budget=80.0) == 80.0

    def test_booked_wins_when_overbooked(self):
        # forecast 100, booked 150 → max(150, 100) = 150 (already committed)
        assert compute_expected(booked=150.0, forecast=100.0, budget=None) == 150.0

    def test_no_plan_falls_back_to_booked(self):
        # No forecast, no budget, booked 42 → 42 (just the booked value)
        assert compute_expected(booked=42.0, forecast=None, budget=None) == 42.0

    def test_zero_booked_with_plan(self):
        # booked 0, forecast 100 → 100 (the plan)
        assert compute_expected(booked=0.0, forecast=100.0, budget=None) == 100.0

    def test_zero_booked_no_plan(self):
        # booked 0, no plan → 0
        assert compute_expected(booked=0.0, forecast=None, budget=None) == 0.0


class TestComputePipeline:
    """max(0, COALESCE(forecast, budget) − booked). Null when no plan."""

    def test_forecast_gap(self):
        # forecast 100, booked 40 → 60
        assert compute_pipeline(booked=40.0, forecast=100.0, budget=None) == 60.0

    def test_budget_gap_when_forecast_null(self):
        # forecast null, budget 80, booked 30 → 50
        assert compute_pipeline(booked=30.0, forecast=None, budget=80.0) == 50.0

    def test_forecast_preferred_over_budget(self):
        # forecast 100 wins over budget 80
        assert compute_pipeline(booked=0.0, forecast=100.0, budget=80.0) == 100.0

    def test_floored_at_zero_when_overbooked(self):
        # booked 150, forecast 100 → max(0, -50) = 0, NEVER negative
        assert compute_pipeline(booked=150.0, forecast=100.0, budget=None) == 0.0

    def test_null_when_no_plan(self):
        # no forecast, no budget → null (can't compute gap without a target)
        assert compute_pipeline(booked=50.0, forecast=None, budget=None) is None

    def test_zero_when_exactly_booked(self):
        # forecast 100, booked 100 → 0
        assert compute_pipeline(booked=100.0, forecast=100.0, budget=None) == 0.0


class TestComputeVsBudget:
    """booked − budget, signed. Null when budget is null."""

    def test_under_budget_signed_negative(self):
        # booked 30, budget 80 → -50 (behind)
        assert compute_vs_budget(booked=30.0, budget=80.0) == -50.0

    def test_over_budget_signed_positive(self):
        # booked 100, budget 80 → 20 (ahead)
        assert compute_vs_budget(booked=100.0, budget=80.0) == 20.0

    def test_exactly_on_budget(self):
        assert compute_vs_budget(booked=80.0, budget=80.0) == 0.0

    def test_null_when_budget_null(self):
        # Can't compute gap against a target that doesn't exist
        assert compute_vs_budget(booked=100.0, budget=None) is None

    def test_zero_booked_with_budget(self):
        # booked 0, budget 80 → -80 (full behind)
        assert compute_vs_budget(booked=0.0, budget=80.0) == -80.0


# ---------------------------------------------------------------------------
# Service-level tests. Fake DB fixture seeds minimal schema.
# ---------------------------------------------------------------------------


class _FakeDB:
    """Single-connection wrapper so in-memory SQLite persists across calls."""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    @contextmanager
    def connection(self):
        yield self._conn


@pytest.fixture
def db():
    """In-memory SQLite with the schema the planning-export service reads."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE spots (
            spot_id INTEGER PRIMARY KEY,
            bill_code TEXT,
            broadcast_month TEXT,
            gross_rate DECIMAL(12,2),
            sales_person TEXT,
            revenue_type TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE budget (
            budget_id INTEGER PRIMARY KEY,
            ae_name TEXT NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            budget_amount DECIMAL(12,2) NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE forecast (
            forecast_id INTEGER PRIMARY KEY,
            ae_name TEXT NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            forecast_amount DECIMAL(12,2) NOT NULL,
            new_accounts_forecast INTEGER DEFAULT 0,
            new_dollars_forecast DECIMAL(12,2) DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE revenue_entities (
            entity_id INTEGER PRIMARY KEY,
            entity_name TEXT NOT NULL,
            entity_type TEXT,
            is_active BOOLEAN DEFAULT 1
        )
    """)
    conn.commit()
    return _FakeDB(conn)


def _insert_spot(conn, **overrides):
    defaults = dict(
        bill_code="Acme Inc",
        broadcast_month="Jan-26",
        gross_rate=1000.0,
        sales_person="Charmaine Lane",
        revenue_type="Internal Ad Sales",
    )
    defaults.update(overrides)
    cols = ",".join(defaults.keys())
    placeholders = ",".join("?" * len(defaults))
    conn.execute(
        f"INSERT INTO spots ({cols}) VALUES ({placeholders})",
        list(defaults.values()),
    )


def _insert_budget(conn, ae_name, year, month, amount):
    conn.execute(
        "INSERT INTO budget (ae_name, year, month, budget_amount) VALUES (?, ?, ?, ?)",
        (ae_name, year, month, amount),
    )


def _insert_forecast(conn, ae_name, year, month, amount, new_accts=0, new_dollars=0):
    conn.execute(
        """INSERT INTO forecast
               (ae_name, year, month, forecast_amount,
                new_accounts_forecast, new_dollars_forecast)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (ae_name, year, month, amount, new_accts, new_dollars),
    )


def _insert_entity(conn, entity_name, entity_type="AE", is_active=1):
    conn.execute(
        "INSERT INTO revenue_entities (entity_name, entity_type, is_active) VALUES (?, ?, ?)",
        (entity_name, entity_type, is_active),
    )


def test_metadata_envelope_has_schema_version_and_year(db):
    """Envelope has schema_version='1.0', the requested year, row_count."""
    with db.connection() as conn:
        _insert_entity(conn, "Charmaine Lane")
        _insert_budget(conn, "Charmaine Lane", 2026, 1, 100000.0)
        conn.commit()

    svc = PlanningExportService(db)
    result = svc.get_rows(year=2026)

    assert result["metadata"]["schema_version"] == "1.0"
    assert result["metadata"]["year"] == 2026
    assert result["metadata"]["row_count"] == len(result["rows"])
    assert result["metadata"]["generated_at"].endswith("Z")


def test_emits_row_for_budget_only(db):
    """Row emitted when only a budget row exists for (ae, month)."""
    with db.connection() as conn:
        _insert_entity(conn, "Charmaine Lane")
        _insert_budget(conn, "Charmaine Lane", 2026, 3, 80000.0)
        conn.commit()

    svc = PlanningExportService(db)
    result = svc.get_rows(year=2026)

    rows = [r for r in result["rows"] if r["broadcast_month"] == "2026-03-01"]
    assert len(rows) == 1
    row = rows[0]
    assert row["ae1"] == "Charmaine Lane"
    assert row["budget"] == 80000.0
    assert row["forecast"] is None
    assert row["booked"] == 0.0
    assert row["new_accts"] is None
    assert row["new_dollars"] is None
    # Derived: no forecast → budget is plan
    assert row["expected"] == 80000.0
    assert row["pipeline"] == 80000.0  # max(0, 80000 - 0)
    assert row["vs_budget"] == -80000.0  # 0 - 80000


def test_emits_row_for_forecast_only(db):
    """Row emitted when only a forecast row exists, no budget, no booked."""
    with db.connection() as conn:
        _insert_entity(conn, "Charmaine Lane")
        _insert_forecast(
            conn, "Charmaine Lane", 2026, 5, 150000.0, new_accts=3, new_dollars=25000.0
        )
        conn.commit()

    svc = PlanningExportService(db)
    result = svc.get_rows(year=2026)

    rows = [r for r in result["rows"] if r["broadcast_month"] == "2026-05-01"]
    assert len(rows) == 1
    row = rows[0]
    assert row["budget"] is None
    assert row["forecast"] == 150000.0
    assert row["booked"] == 0.0
    assert row["new_accts"] == 3
    assert row["new_dollars"] == 25000.0
    assert row["expected"] == 150000.0
    assert row["pipeline"] == 150000.0
    assert row["vs_budget"] is None  # no budget → null


def test_emits_row_for_booked_only(db):
    """Row emitted when only booked revenue exists, no budget, no forecast."""
    with db.connection() as conn:
        _insert_entity(conn, "Charmaine Lane")
        _insert_spot(
            conn,
            sales_person="Charmaine Lane",
            broadcast_month="Jun-26",
            gross_rate=5000.0,
        )
        conn.commit()

    svc = PlanningExportService(db)
    result = svc.get_rows(year=2026)

    rows = [r for r in result["rows"] if r["broadcast_month"] == "2026-06-01"]
    assert len(rows) == 1
    row = rows[0]
    assert row["budget"] is None
    assert row["forecast"] is None
    assert row["booked"] == 5000.0
    # No plan → pipeline null, vs_budget null, expected = booked
    assert row["expected"] == 5000.0
    assert row["pipeline"] is None
    assert row["vs_budget"] is None


def test_suppresses_rows_with_no_activity(db):
    """Months with no budget, no forecast, and no spots don't get a row."""
    with db.connection() as conn:
        _insert_entity(conn, "Charmaine Lane")
        _insert_budget(conn, "Charmaine Lane", 2026, 1, 100000.0)
        # No activity for Charmaine in Feb-26 through Dec-26.
        conn.commit()

    svc = PlanningExportService(db)
    result = svc.get_rows(year=2026)

    # Only one row (Jan-26), not 12.
    assert len(result["rows"]) == 1
    assert result["rows"][0]["broadcast_month"] == "2026-01-01"


def test_year_filter_excludes_other_years(db):
    """Requesting year=2026 returns no rows from 2025 data."""
    with db.connection() as conn:
        _insert_entity(conn, "Charmaine Lane")
        _insert_budget(conn, "Charmaine Lane", 2025, 1, 50000.0)
        _insert_budget(conn, "Charmaine Lane", 2026, 1, 100000.0)
        _insert_spot(
            conn, sales_person="Charmaine Lane", broadcast_month="Jan-25", gross_rate=1.0
        )
        _insert_spot(
            conn, sales_person="Charmaine Lane", broadcast_month="Jan-26", gross_rate=2.0
        )
        conn.commit()

    svc = PlanningExportService(db)
    result = svc.get_rows(year=2026)

    # Only Jan-26 row present.
    assert len(result["rows"]) == 1
    row = result["rows"][0]
    assert row["broadcast_month"] == "2026-01-01"
    assert row["budget"] == 100000.0
    assert row["booked"] == 2.0


def test_worldlink_booked_matched_by_bill_code_prefix(db):
    """WorldLink booked uses bill_code LIKE 'WorldLink%', not sales_person."""
    with db.connection() as conn:
        _insert_entity(conn, "WorldLink")
        # A spot with WorldLink-prefixed bill_code but a different sales_person —
        # belongs to WorldLink per planning_repository.get_booked_revenue.
        _insert_spot(
            conn,
            bill_code="WorldLink:Acme",
            sales_person="Some Other AE",
            broadcast_month="Apr-26",
            gross_rate=7500.0,
        )
        conn.commit()

    svc = PlanningExportService(db)
    result = svc.get_rows(year=2026)

    worldlink_rows = [r for r in result["rows"] if r["ae1"] == "WorldLink"]
    assert len(worldlink_rows) == 1
    assert worldlink_rows[0]["booked"] == 7500.0


def test_house_booked_excludes_worldlink_prefixed_bills(db):
    """House booked = sales_person='House' AND bill_code NOT LIKE 'WorldLink%'."""
    with db.connection() as conn:
        _insert_entity(conn, "House")
        # House spot that counts toward House booked.
        _insert_spot(
            conn,
            bill_code="Direct Client",
            sales_person="House",
            broadcast_month="Apr-26",
            gross_rate=3000.0,
        )
        # WorldLink-prefixed bill with sales_person='House' — excluded from House booked.
        _insert_spot(
            conn,
            bill_code="WorldLink:Retail",
            sales_person="House",
            broadcast_month="Apr-26",
            gross_rate=999.0,
        )
        conn.commit()

    svc = PlanningExportService(db)
    result = svc.get_rows(year=2026)

    house_rows = [r for r in result["rows"] if r["ae1"] == "House"]
    assert len(house_rows) == 1
    # Only the 3000 spot; the 999 WorldLink-prefixed spot is excluded.
    assert house_rows[0]["booked"] == 3000.0


def test_regular_ae_booked_matches_sales_person(db):
    """Regular AEs match on sales_person; no bill_code filter."""
    with db.connection() as conn:
        _insert_entity(conn, "Charmaine Lane")
        _insert_spot(
            conn,
            bill_code="WorldLink:Retail",  # even WorldLink-prefixed
            sales_person="Charmaine Lane",
            broadcast_month="Jul-26",
            gross_rate=4000.0,
        )
        _insert_spot(
            conn,
            bill_code="Direct Client",
            sales_person="Charmaine Lane",
            broadcast_month="Jul-26",
            gross_rate=1000.0,
        )
        conn.commit()

    svc = PlanningExportService(db)
    result = svc.get_rows(year=2026)

    rows = [
        r for r in result["rows"]
        if r["ae1"] == "Charmaine Lane" and r["broadcast_month"] == "2026-07-01"
    ]
    assert len(rows) == 1
    # Regular AE sums across ALL bill_codes including WorldLink-prefixed.
    # This matches planning_repository.get_booked_revenue behavior.
    assert rows[0]["booked"] == 5000.0


def test_trade_revenue_excluded_from_booked(db):
    """Trade spots do not count toward booked (matches Trade filter everywhere)."""
    with db.connection() as conn:
        _insert_entity(conn, "Charmaine Lane")
        _insert_spot(
            conn,
            sales_person="Charmaine Lane",
            broadcast_month="Aug-26",
            gross_rate=1000.0,
            revenue_type="Internal Ad Sales",
        )
        _insert_spot(
            conn,
            sales_person="Charmaine Lane",
            broadcast_month="Aug-26",
            gross_rate=999.0,
            revenue_type="Trade",
        )
        conn.commit()

    svc = PlanningExportService(db)
    result = svc.get_rows(year=2026)

    rows = [
        r for r in result["rows"]
        if r["ae1"] == "Charmaine Lane" and r["broadcast_month"] == "2026-08-01"
    ]
    assert rows[0]["booked"] == 1000.0  # Trade excluded


def test_inactive_entities_excluded(db):
    """AEs with is_active=0 don't appear in rows."""
    with db.connection() as conn:
        _insert_entity(conn, "Active AE", is_active=1)
        _insert_entity(conn, "Retired AE", is_active=0)
        _insert_budget(conn, "Active AE", 2026, 1, 1000.0)
        _insert_budget(conn, "Retired AE", 2026, 1, 1000.0)
        conn.commit()

    svc = PlanningExportService(db)
    result = svc.get_rows(year=2026)

    names = {r["ae1"] for r in result["rows"]}
    assert "Active AE" in names
    assert "Retired AE" not in names


def test_forecast_without_new_business_emits_zero_not_null(db):
    """new_accts/new_dollars default to 0 in the forecast table; emit as 0, not null."""
    with db.connection() as conn:
        _insert_entity(conn, "Charmaine Lane")
        _insert_forecast(
            conn, "Charmaine Lane", 2026, 9, 100000.0, new_accts=0, new_dollars=0
        )
        conn.commit()

    svc = PlanningExportService(db)
    result = svc.get_rows(year=2026)

    row = next(
        r for r in result["rows"]
        if r["ae1"] == "Charmaine Lane" and r["broadcast_month"] == "2026-09-01"
    )
    # Forecast row exists → new_accts/new_dollars are 0, not null.
    assert row["new_accts"] == 0
    assert row["new_dollars"] == 0.0


def test_full_row_shape_when_all_inputs_present(db):
    """When budget + forecast + booked all present, all fields populated."""
    with db.connection() as conn:
        _insert_entity(conn, "Charmaine Lane")
        _insert_budget(conn, "Charmaine Lane", 2026, 4, 100000.0)
        _insert_forecast(
            conn, "Charmaine Lane", 2026, 4, 120000.0, new_accts=2, new_dollars=15000.0
        )
        _insert_spot(
            conn,
            sales_person="Charmaine Lane",
            broadcast_month="Apr-26",
            gross_rate=40000.0,
        )
        conn.commit()

    svc = PlanningExportService(db)
    result = svc.get_rows(year=2026)

    row = next(
        r for r in result["rows"]
        if r["ae1"] == "Charmaine Lane" and r["broadcast_month"] == "2026-04-01"
    )
    assert row == {
        "ae1": "Charmaine Lane",
        "broadcast_month": "2026-04-01",
        "budget": 100000.0,
        "forecast": 120000.0,
        "booked": 40000.0,
        "new_accts": 2,
        "new_dollars": 15000.0,
        # expected = max(40000, max(120000, 100000)) = 120000
        "expected": 120000.0,
        # pipeline = max(0, 120000 - 40000) = 80000
        "pipeline": 80000.0,
        # vs_budget = 40000 - 100000 = -60000
        "vs_budget": -60000.0,
    }


def test_defaults_year_to_current_when_none(db):
    """year=None defaults to current year (date.today().year)."""
    from datetime import date
    with db.connection() as conn:
        _insert_entity(conn, "Charmaine Lane")
        _insert_budget(conn, "Charmaine Lane", date.today().year, 1, 1.0)
        conn.commit()

    svc = PlanningExportService(db)
    result = svc.get_rows()  # No year arg.

    assert result["metadata"]["year"] == date.today().year
    assert len(result["rows"]) == 1
