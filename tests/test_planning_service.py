"""
Service-level tests for PlanningService using a temp SQLite database.

Tests cover:
- Forecast update rules (past/current/future periods, unknown entity)
- Forecast history recording
- Forecast reset
- Bulk update with mixed valid/invalid periods
- Planning data retrieval (summary, entities, validation)
"""

import sqlite3
from datetime import date
from decimal import Decimal
from unittest.mock import patch

import pytest

from src.database.connection import DatabaseConnection
from src.services.planning_service import PlanningService


# =============================================================================
# Schema DDL
# =============================================================================

_SCHEMA = """
CREATE TABLE revenue_entities (
    entity_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_name TEXT    NOT NULL UNIQUE,
    entity_type TEXT    NOT NULL DEFAULT 'AE',
    is_active   INTEGER NOT NULL DEFAULT 1,
    created_date TEXT,
    notes       TEXT
);

CREATE TABLE budget (
    ae_name       TEXT    NOT NULL,
    year          INTEGER NOT NULL,
    month         INTEGER NOT NULL,
    budget_amount REAL    NOT NULL,
    updated_date  TEXT,
    PRIMARY KEY (ae_name, year, month)
);

CREATE TABLE forecast (
    forecast_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    ae_name        TEXT    NOT NULL,
    year           INTEGER NOT NULL,
    month          INTEGER NOT NULL,
    forecast_amount REAL   NOT NULL,
    updated_date   TEXT    DEFAULT CURRENT_TIMESTAMP,
    updated_by     TEXT,
    notes          TEXT,
    UNIQUE (ae_name, year, month)
);

CREATE TABLE forecast_history (
    history_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    ae_name         TEXT    NOT NULL,
    year            INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    previous_amount REAL,
    new_amount      REAL    NOT NULL,
    changed_date    TEXT    DEFAULT CURRENT_TIMESTAMP,
    changed_by      TEXT,
    session_notes   TEXT
);

CREATE TABLE spots (
    spot_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    sales_person    TEXT,
    bill_code       TEXT,
    broadcast_month TEXT,
    gross_rate      REAL,
    revenue_type    TEXT
);

CREATE TABLE sectors (
    sector_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    sector_code TEXT NOT NULL UNIQUE,
    sector_name TEXT NOT NULL,
    is_active   INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE sector_expectations (
    expectation_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    ae_name               TEXT    NOT NULL,
    sector_id             INTEGER NOT NULL REFERENCES sectors(sector_id),
    year                  INTEGER NOT NULL,
    month                 INTEGER NOT NULL,
    expected_amount       REAL    NOT NULL DEFAULT 0,
    notes                 TEXT,
    created_date          TEXT    DEFAULT CURRENT_TIMESTAMP,
    updated_date          TEXT    DEFAULT CURRENT_TIMESTAMP,
    updated_by            TEXT,
    new_accounts_forecast INTEGER,
    new_dollars_forecast  REAL,
    UNIQUE (ae_name, sector_id, year, month)
);
"""


def _seed(conn: sqlite3.Connection) -> None:
    """Insert baseline test data."""
    # Revenue entities
    conn.executemany(
        "INSERT INTO revenue_entities (entity_name, entity_type) VALUES (?, ?)",
        [
            ("Alice", "AE"),
            ("Bob", "AE"),
            ("House", "House"),
        ],
    )

    # Budget: all 12 months of 2026
    budget_rows = []
    for month in range(1, 13):
        budget_rows.append(("Alice", 2026, month, 10000.0))
        budget_rows.append(("Bob", 2026, month, 8000.0))
        budget_rows.append(("House", 2026, month, 5000.0))
    conn.executemany(
        "INSERT INTO budget (ae_name, year, month, budget_amount) VALUES (?, ?, ?, ?)",
        budget_rows,
    )

    # Spots — broadcast_month uses 'Jan-26' format
    conn.executemany(
        "INSERT INTO spots (sales_person, bill_code, broadcast_month, gross_rate, revenue_type)"
        " VALUES (?, ?, ?, ?, ?)",
        [
            # Alice Jan-26: $3000 + $2000 = $5000
            ("Alice", "ACME", "Jan-26", 3000.0, None),
            ("Alice", "ACME", "Jan-26", 2000.0, None),
            # Alice Feb-26: $4000
            ("Alice", "ACME", "Feb-26", 4000.0, None),
            # Bob Jan-26: $1500
            ("Bob", "BETA", "Jan-26", 1500.0, None),
            # House Jan-26: $1000
            ("House", "HOUSE_AD", "Jan-26", 1000.0, None),
            # Trade spots — must be excluded from all revenue queries
            ("Alice", "TRADE", "Jan-26", 9999.0, "Trade"),
        ],
    )

    # Forecast override: Alice March 2026 → $15,000
    conn.execute(
        "INSERT INTO forecast (ae_name, year, month, forecast_amount, updated_by)"
        " VALUES (?, ?, ?, ?, ?)",
        ("Alice", 2026, 3, 15000.0, "test_setup"),
    )


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture()
def planning_db(tmp_path):
    """Return a DatabaseConnection backed by a fresh temp SQLite file."""
    db_file = tmp_path / "test_planning.db"
    db = DatabaseConnection(str(db_file))

    with db.transaction() as conn:
        conn.executescript(_SCHEMA)
        _seed(conn)

    return db


@pytest.fixture()
def planning_service(planning_db):
    """Return a PlanningService wired to the temp database."""
    return PlanningService(planning_db)


# =============================================================================
# TestForecastUpdateRules
# =============================================================================


class TestForecastUpdateRules:
    """Tests covering the business rules around when forecasts may be changed."""

    def test_cannot_update_past_month(self, planning_service):
        """Updating a closed period (before current month) must raise ValueError."""
        mock_today = date(2026, 3, 15)
        with patch("src.models.planning.date") as mock_date:
            mock_date.today.return_value = mock_today
            mock_date.side_effect = lambda *args, **kw: date(*args, **kw)

            with pytest.raises(ValueError, match="closed period"):
                planning_service.update_forecast(
                    ae_name="Alice",
                    year=2026,
                    month=1,  # January is past when today is March 15
                    new_amount=Decimal("12000"),
                    updated_by="tester",
                )

    def test_can_update_current_month(self, planning_service):
        """Current month must be updatable."""
        mock_today = date(2026, 3, 15)
        with patch("src.models.planning.date") as mock_date:
            mock_date.today.return_value = mock_today
            mock_date.side_effect = lambda *args, **kw: date(*args, **kw)

            change = planning_service.update_forecast(
                ae_name="Alice",
                year=2026,
                month=3,
                new_amount=Decimal("16000"),
                updated_by="tester",
            )

        assert change.ae_name == "Alice"
        assert change.new_amount.amount == Decimal("16000")

    def test_can_update_future_month(self, planning_service):
        """Future months must be updatable."""
        mock_today = date(2026, 3, 15)
        with patch("src.models.planning.date") as mock_date:
            mock_date.today.return_value = mock_today
            mock_date.side_effect = lambda *args, **kw: date(*args, **kw)

            change = planning_service.update_forecast(
                ae_name="Bob",
                year=2026,
                month=6,
                new_amount=Decimal("9500"),
                updated_by="tester",
            )

        assert change.new_amount.amount == Decimal("9500")

    def test_unknown_entity_raises_value_error(self, planning_service):
        """Updating a forecast for an entity not in revenue_entities must fail."""
        mock_today = date(2026, 3, 15)
        with patch("src.models.planning.date") as mock_date:
            mock_date.today.return_value = mock_today
            mock_date.side_effect = lambda *args, **kw: date(*args, **kw)

            with pytest.raises(ValueError, match="Unknown revenue entity"):
                planning_service.update_forecast(
                    ae_name="NoSuchPerson",
                    year=2026,
                    month=4,
                    new_amount=Decimal("5000"),
                    updated_by="tester",
                )

    def test_update_records_history(self, planning_service):
        """After a forecast update the history table must contain the change."""
        mock_today = date(2026, 3, 15)
        with patch("src.models.planning.date") as mock_date:
            mock_date.today.return_value = mock_today
            mock_date.side_effect = lambda *args, **kw: date(*args, **kw)

            planning_service.update_forecast(
                ae_name="Bob",
                year=2026,
                month=4,
                new_amount=Decimal("9000"),
                updated_by="tester",
            )

        history = planning_service.get_forecast_history("Bob", 2026, 4)
        assert len(history) == 1
        assert history[0].new_amount.amount == Decimal("9000")

    def test_update_captures_previous_amount(self, planning_service):
        """When overwriting an existing forecast the previous_amount must be recorded."""
        # Alice month 3 already has a $15,000 forecast from seed data.
        mock_today = date(2026, 3, 15)
        with patch("src.models.planning.date") as mock_date:
            mock_date.today.return_value = mock_today
            mock_date.side_effect = lambda *args, **kw: date(*args, **kw)

            change = planning_service.update_forecast(
                ae_name="Alice",
                year=2026,
                month=3,
                new_amount=Decimal("18000"),
                updated_by="tester",
            )

        assert change.previous_amount is not None
        assert change.previous_amount.amount == Decimal("15000")


# =============================================================================
# TestForecastReset
# =============================================================================


class TestForecastReset:
    """Tests for resetting a forecast override back to budget."""

    def test_reset_removes_override(self, planning_service):
        """Reset on an existing override must return True."""
        mock_today = date(2026, 3, 15)
        with patch("src.models.planning.date") as mock_date:
            mock_date.today.return_value = mock_today
            mock_date.side_effect = lambda *args, **kw: date(*args, **kw)

            # Alice month 3 has a $15,000 override from seed data
            result = planning_service.reset_forecast_to_budget("Alice", 2026, 3)

        assert result is True

    def test_reset_returns_false_when_no_override(self, planning_service):
        """Reset when no override exists must return False."""
        mock_today = date(2026, 3, 15)
        with patch("src.models.planning.date") as mock_date:
            mock_date.today.return_value = mock_today
            mock_date.side_effect = lambda *args, **kw: date(*args, **kw)

            # Alice month 6 has no override
            result = planning_service.reset_forecast_to_budget("Alice", 2026, 6)

        assert result is False

    def test_reset_past_month_raises_value_error(self, planning_service):
        """Reset of a closed period must raise ValueError."""
        mock_today = date(2026, 3, 15)
        with patch("src.models.planning.date") as mock_date:
            mock_date.today.return_value = mock_today
            mock_date.side_effect = lambda *args, **kw: date(*args, **kw)

            with pytest.raises(ValueError, match="closed period"):
                planning_service.reset_forecast_to_budget("Alice", 2026, 1)


# =============================================================================
# TestBulkUpdate
# =============================================================================


class TestBulkUpdate:
    """Tests for bulk_update_forecasts."""

    def test_bulk_update_multiple_valid(self, planning_service):
        """All valid future updates should be applied."""
        mock_today = date(2026, 3, 15)
        with patch("src.models.planning.date") as mock_date:
            mock_date.today.return_value = mock_today
            mock_date.side_effect = lambda *args, **kw: date(*args, **kw)

            changes = planning_service.bulk_update_forecasts(
                updates=[
                    {"ae_name": "Alice", "year": 2026, "month": 4, "amount": 11000},
                    {"ae_name": "Bob", "year": 2026, "month": 5, "amount": 8500},
                ],
                updated_by="tester",
            )

        assert len(changes) == 2

    def test_bulk_update_skips_past_months(self, planning_service):
        """Past-month updates are silently skipped; future-month updates proceed."""
        mock_today = date(2026, 3, 15)
        with patch("src.models.planning.date") as mock_date:
            mock_date.today.return_value = mock_today
            mock_date.side_effect = lambda *args, **kw: date(*args, **kw)

            changes = planning_service.bulk_update_forecasts(
                updates=[
                    # Past — should be skipped
                    {"ae_name": "Alice", "year": 2026, "month": 1, "amount": 99999},
                    # Future — should be applied
                    {"ae_name": "Alice", "year": 2026, "month": 6, "amount": 12000},
                ],
                updated_by="tester",
            )

        assert len(changes) == 1
        assert changes[0].period.month == 6


# =============================================================================
# TestPlanningDataRetrieval
# =============================================================================


class TestPlanningDataRetrieval:
    """Tests for get_planning_summary, get_revenue_entities, and validate_planning_data."""

    def test_get_planning_summary_loads_all_entities(self, planning_service):
        """Summary must contain data for all 3 seed entities."""
        mock_today = date(2026, 3, 15)
        with patch("src.models.planning.date") as mock_date:
            mock_date.today.return_value = mock_today
            mock_date.side_effect = lambda *args, **kw: date(*args, **kw)

            summary = planning_service.get_planning_summary(planning_year=2026)

        entity_names = {ed.entity.entity_name for ed in summary.entity_data}
        assert entity_names == {"Alice", "Bob", "House"}

    def test_summary_has_12_periods(self, planning_service):
        """Full-year summary must have exactly 12 periods."""
        mock_today = date(2026, 3, 15)
        with patch("src.models.planning.date") as mock_date:
            mock_date.today.return_value = mock_today
            mock_date.side_effect = lambda *args, **kw: date(*args, **kw)

            summary = planning_service.get_planning_summary(planning_year=2026)

        assert len(summary.all_periods) == 12

    def test_each_entity_has_12_rows(self, planning_service):
        """Every entity in the summary must have 12 planning rows."""
        mock_today = date(2026, 3, 15)
        with patch("src.models.planning.date") as mock_date:
            mock_date.today.return_value = mock_today
            mock_date.side_effect = lambda *args, **kw: date(*args, **kw)

            summary = planning_service.get_planning_summary(planning_year=2026)

        for entity_data in summary.entity_data:
            assert len(entity_data.rows) == 12, (
                f"{entity_data.entity.entity_name} has {len(entity_data.rows)} rows"
            )

    def test_forecast_defaults_to_budget_when_no_override(self, planning_service):
        """Bob month 4 has no forecast override — forecast_entered must equal budget."""
        mock_today = date(2026, 3, 15)
        with patch("src.models.planning.date") as mock_date:
            mock_date.today.return_value = mock_today
            mock_date.side_effect = lambda *args, **kw: date(*args, **kw)

            summary = planning_service.get_planning_summary(planning_year=2026)

        bob_data = next(
            ed for ed in summary.entity_data if ed.entity.entity_name == "Bob"
        )
        from src.models.planning import PlanningPeriod

        row = bob_data.row_for_period(PlanningPeriod(year=2026, month=4))
        assert row is not None
        assert row.forecast_entered.amount == Decimal("8000")

    def test_forecast_override_used_when_present(self, planning_service):
        """Alice month 3 has a $15,000 override — forecast_entered must reflect it."""
        mock_today = date(2026, 3, 15)
        with patch("src.models.planning.date") as mock_date:
            mock_date.today.return_value = mock_today
            mock_date.side_effect = lambda *args, **kw: date(*args, **kw)

            summary = planning_service.get_planning_summary(planning_year=2026)

        alice_data = next(
            ed for ed in summary.entity_data if ed.entity.entity_name == "Alice"
        )
        from src.models.planning import PlanningPeriod

        row = alice_data.row_for_period(PlanningPeriod(year=2026, month=3))
        assert row is not None
        assert row.forecast_entered.amount == Decimal("15000")

    def test_booked_revenue_from_spots(self, planning_service):
        """Alice Jan-26 has $3000 + $2000 spots = $5000 booked (Trade excluded)."""
        mock_today = date(2026, 3, 15)
        with patch("src.models.planning.date") as mock_date:
            mock_date.today.return_value = mock_today
            mock_date.side_effect = lambda *args, **kw: date(*args, **kw)

            summary = planning_service.get_planning_summary(planning_year=2026)

        alice_data = next(
            ed for ed in summary.entity_data if ed.entity.entity_name == "Alice"
        )
        from src.models.planning import PlanningPeriod

        row = alice_data.row_for_period(PlanningPeriod(year=2026, month=1))
        assert row is not None
        assert row.booked.amount == Decimal("5000")

    def test_company_summary_budget_total(self, planning_service):
        """Total budget across all entities for 2026 = (10000+8000+5000)*12 = 276000."""
        mock_today = date(2026, 3, 15)
        with patch("src.models.planning.date") as mock_date:
            mock_date.today.return_value = mock_today
            mock_date.side_effect = lambda *args, **kw: date(*args, **kw)

            summary = planning_service.get_company_summary(planning_year=2026)

        assert summary["total_budget"].amount == Decimal("276000")

    def test_get_revenue_entities_returns_three(self, planning_service):
        """get_revenue_entities must return all 3 seeded entities."""
        entities = planning_service.get_revenue_entities()
        assert len(entities) == 3

    def test_validate_planning_data_is_valid(self, planning_service):
        """Seed data is complete — validate_planning_data must report is_valid=True."""
        result = planning_service.validate_planning_data(2026)
        assert result["is_valid"] is True
        assert result["issues"] == []
