"""
Tests for PlanningPeriod and Money domain models.
"""

from datetime import date
from decimal import Decimal
from unittest.mock import patch

import pytest

from src.models.planning import (
    EntityPlanningData,
    EntitySectorExpectations,
    EntityType,
    ForecastUpdate,
    Money,
    PlanningPeriod,
    PlanningRow,
    RevenueEntity,
    SectorExpectation,
    SectorExpectationValidation,
)


# =============================================================================
# PlanningPeriod — Construction
# =============================================================================


def test_planning_period_valid_construction():
    p = PlanningPeriod(year=2026, month=3)
    assert p.year == 2026
    assert p.month == 3


def test_planning_period_invalid_month_zero():
    with pytest.raises(ValueError, match="Month must be between 1 and 12"):
        PlanningPeriod(year=2026, month=0)


def test_planning_period_invalid_month_thirteen():
    with pytest.raises(ValueError, match="Month must be between 1 and 12"):
        PlanningPeriod(year=2026, month=13)


def test_planning_period_invalid_year_too_low():
    with pytest.raises(ValueError, match="Year must be between 2000 and 2100"):
        PlanningPeriod(year=1999, month=1)


def test_planning_period_invalid_year_too_high():
    with pytest.raises(ValueError, match="Year must be between 2000 and 2100"):
        PlanningPeriod(year=2101, month=1)


def test_planning_period_boundary_year_2000():
    p = PlanningPeriod(year=2000, month=1)
    assert p.year == 2000


def test_planning_period_boundary_year_2100():
    p = PlanningPeriod(year=2100, month=12)
    assert p.year == 2100


# =============================================================================
# PlanningPeriod — Properties
# =============================================================================


def test_planning_period_key():
    p = PlanningPeriod(year=2025, month=1)
    assert p.key == "2025-1"


def test_planning_period_key_double_digit_month():
    p = PlanningPeriod(year=2025, month=12)
    assert p.key == "2025-12"


def test_planning_period_broadcast_month():
    p = PlanningPeriod(year=2025, month=1)
    assert p.broadcast_month == "Jan-25"


def test_planning_period_broadcast_month_december():
    p = PlanningPeriod(year=2026, month=12)
    assert p.broadcast_month == "Dec-26"


def test_planning_period_display():
    p = PlanningPeriod(year=2025, month=3)
    assert p.display == "Mar 2025"


def test_planning_period_sort_key():
    p = PlanningPeriod(year=2025, month=3)
    assert p.sort_key == 202503


def test_planning_period_sort_key_ordering():
    p1 = PlanningPeriod(year=2025, month=3)
    p2 = PlanningPeriod(year=2025, month=4)
    p3 = PlanningPeriod(year=2026, month=1)
    assert p1.sort_key < p2.sort_key < p3.sort_key


# =============================================================================
# PlanningPeriod — is_past / is_current / is_future (mocked to 2026-03-15)
# =============================================================================

MOCK_TODAY = date(2026, 3, 15)


@pytest.fixture
def mock_today():
    with patch("src.models.planning.date") as mock_date:
        mock_date.today.return_value = MOCK_TODAY
        # Allow date() constructor to still work
        mock_date.side_effect = lambda *args, **kwargs: date(*args, **kwargs)
        yield mock_date


def test_is_past_previous_year(mock_today):
    p = PlanningPeriod(year=2025, month=12)
    assert p.is_past is True


def test_is_past_same_year_earlier_month(mock_today):
    p = PlanningPeriod(year=2026, month=2)
    assert p.is_past is True


def test_is_past_current_month_is_not_past(mock_today):
    p = PlanningPeriod(year=2026, month=3)
    assert p.is_past is False


def test_is_current_current_month(mock_today):
    p = PlanningPeriod(year=2026, month=3)
    assert p.is_current is True


def test_is_current_past_month_is_not_current(mock_today):
    p = PlanningPeriod(year=2026, month=2)
    assert p.is_current is False


def test_is_current_future_month_is_not_current(mock_today):
    p = PlanningPeriod(year=2026, month=4)
    assert p.is_current is False


def test_is_future_next_month(mock_today):
    p = PlanningPeriod(year=2026, month=4)
    assert p.is_future is True


def test_is_future_next_year(mock_today):
    p = PlanningPeriod(year=2027, month=1)
    assert p.is_future is True


def test_is_future_current_month_is_not_future(mock_today):
    p = PlanningPeriod(year=2026, month=3)
    assert p.is_future is False


def test_is_future_past_month_is_not_future(mock_today):
    p = PlanningPeriod(year=2026, month=1)
    assert p.is_future is False


# =============================================================================
# PlanningPeriod — quarter and is_quarter_end
# =============================================================================


def test_quarter_q1():
    assert PlanningPeriod(year=2026, month=1).quarter == 1
    assert PlanningPeriod(year=2026, month=2).quarter == 1
    assert PlanningPeriod(year=2026, month=3).quarter == 1


def test_quarter_q2():
    assert PlanningPeriod(year=2026, month=4).quarter == 2
    assert PlanningPeriod(year=2026, month=6).quarter == 2


def test_quarter_q3():
    assert PlanningPeriod(year=2026, month=7).quarter == 3
    assert PlanningPeriod(year=2026, month=9).quarter == 3


def test_quarter_q4():
    assert PlanningPeriod(year=2026, month=10).quarter == 4
    assert PlanningPeriod(year=2026, month=12).quarter == 4


def test_is_quarter_end_march():
    assert PlanningPeriod(year=2026, month=3).is_quarter_end is True


def test_is_quarter_end_june():
    assert PlanningPeriod(year=2026, month=6).is_quarter_end is True


def test_is_quarter_end_september():
    assert PlanningPeriod(year=2026, month=9).is_quarter_end is True


def test_is_quarter_end_december():
    assert PlanningPeriod(year=2026, month=12).is_quarter_end is True


def test_is_quarter_end_non_quarter_month():
    assert PlanningPeriod(year=2026, month=1).is_quarter_end is False
    assert PlanningPeriod(year=2026, month=4).is_quarter_end is False
    assert PlanningPeriod(year=2026, month=11).is_quarter_end is False


# =============================================================================
# PlanningPeriod — Comparison operators
# =============================================================================


def test_less_than_different_years():
    assert PlanningPeriod(year=2025, month=12) < PlanningPeriod(year=2026, month=1)


def test_less_than_same_year_different_months():
    assert PlanningPeriod(year=2026, month=1) < PlanningPeriod(year=2026, month=2)


def test_less_than_equal_is_false():
    p = PlanningPeriod(year=2026, month=3)
    assert not (p < p)


def test_less_than_or_equal_same():
    p = PlanningPeriod(year=2026, month=3)
    assert p <= p


def test_less_than_or_equal_earlier():
    assert PlanningPeriod(year=2026, month=2) <= PlanningPeriod(year=2026, month=3)


def test_greater_than():
    assert PlanningPeriod(year=2026, month=4) > PlanningPeriod(year=2026, month=3)


def test_greater_than_or_equal_same():
    p = PlanningPeriod(year=2026, month=3)
    assert p >= p


def test_greater_than_or_equal_later():
    assert PlanningPeriod(year=2026, month=4) >= PlanningPeriod(year=2026, month=3)


# =============================================================================
# PlanningPeriod — Equality and hash
# =============================================================================


def test_equality_same_values():
    p1 = PlanningPeriod(year=2026, month=3)
    p2 = PlanningPeriod(year=2026, month=3)
    assert p1 == p2


def test_equality_different_month():
    assert PlanningPeriod(year=2026, month=3) != PlanningPeriod(year=2026, month=4)


def test_equality_different_year():
    assert PlanningPeriod(year=2025, month=3) != PlanningPeriod(year=2026, month=3)


def test_hash_same_values_equal():
    p1 = PlanningPeriod(year=2026, month=3)
    p2 = PlanningPeriod(year=2026, month=3)
    assert hash(p1) == hash(p2)


def test_usable_as_dict_key():
    p = PlanningPeriod(year=2026, month=3)
    d = {p: "value"}
    assert d[PlanningPeriod(year=2026, month=3)] == "value"


def test_usable_in_set():
    p1 = PlanningPeriod(year=2026, month=3)
    p2 = PlanningPeriod(year=2026, month=3)
    p3 = PlanningPeriod(year=2026, month=4)
    s = {p1, p2, p3}
    assert len(s) == 2


# =============================================================================
# PlanningPeriod — from_broadcast_month
# =============================================================================


def test_from_broadcast_month_valid():
    p = PlanningPeriod.from_broadcast_month("Jan-25")
    assert p.year == 2025
    assert p.month == 1


def test_from_broadcast_month_december():
    p = PlanningPeriod.from_broadcast_month("Dec-26")
    assert p.year == 2026
    assert p.month == 12


def test_from_broadcast_month_case_insensitive():
    p = PlanningPeriod.from_broadcast_month("jan-25")
    assert p.year == 2025
    assert p.month == 1


def test_from_broadcast_month_invalid_format_no_dash():
    with pytest.raises(ValueError):
        PlanningPeriod.from_broadcast_month("Jan25")


def test_from_broadcast_month_invalid_format_too_many_parts():
    with pytest.raises(ValueError):
        PlanningPeriod.from_broadcast_month("Jan-25-extra")


def test_from_broadcast_month_invalid_month_abbrev():
    with pytest.raises(ValueError):
        PlanningPeriod.from_broadcast_month("Xyz-25")


# =============================================================================
# PlanningPeriod — next()
# =============================================================================


def test_next_same_year():
    p = PlanningPeriod(year=2026, month=3)
    assert p.next() == PlanningPeriod(year=2026, month=4)


def test_next_year_wrap():
    p = PlanningPeriod(year=2026, month=12)
    assert p.next() == PlanningPeriod(year=2027, month=1)


def test_next_multiple_months():
    p = PlanningPeriod(year=2026, month=11)
    assert p.next(3) == PlanningPeriod(year=2027, month=2)


def test_next_zero_months():
    p = PlanningPeriod(year=2026, month=3)
    assert p.next(0) == p


# =============================================================================
# PlanningPeriod — full_year()
# =============================================================================


def test_full_year_returns_twelve_periods():
    periods = PlanningPeriod.full_year(2026)
    assert len(periods) == 12


def test_full_year_correct_months():
    periods = PlanningPeriod.full_year(2026)
    assert periods[0] == PlanningPeriod(year=2026, month=1)
    assert periods[11] == PlanningPeriod(year=2026, month=12)


def test_full_year_all_same_year():
    periods = PlanningPeriod.full_year(2026)
    assert all(p.year == 2026 for p in periods)


# =============================================================================
# PlanningPeriod — planning_window()
# =============================================================================


def test_planning_window_default_length(mock_today):
    # default months_ahead=2 → current + 2 = 3 periods
    window = PlanningPeriod.planning_window()
    assert len(window) == 3


def test_planning_window_starts_at_current(mock_today):
    window = PlanningPeriod.planning_window()
    assert window[0] == PlanningPeriod(year=2026, month=3)


def test_planning_window_ends_at_current_plus_n(mock_today):
    window = PlanningPeriod.planning_window(2)
    assert window[-1] == PlanningPeriod(year=2026, month=5)


def test_planning_window_custom_months_ahead(mock_today):
    window = PlanningPeriod.planning_window(months_ahead=5)
    assert len(window) == 6
    assert window[-1] == PlanningPeriod(year=2026, month=8)


# =============================================================================
# PlanningPeriod — past_periods()
# =============================================================================


def test_past_periods_returns_only_past(mock_today):
    # Mock today = 2026-03-15, so past months in 2026 = Jan, Feb
    periods = PlanningPeriod.past_periods(2026)
    assert all(p.is_past for p in periods)


def test_past_periods_correct_count_current_year(mock_today):
    # Jan and Feb 2026 are past when today = 2026-03-15
    periods = PlanningPeriod.past_periods(2026)
    assert len(periods) == 2
    assert periods[0] == PlanningPeriod(year=2026, month=1)
    assert periods[1] == PlanningPeriod(year=2026, month=2)


def test_past_periods_previous_year_all_past(mock_today):
    # All 12 months of 2025 are past
    periods = PlanningPeriod.past_periods(2025)
    assert len(periods) == 12


def test_past_periods_future_year_empty(mock_today):
    periods = PlanningPeriod.past_periods(2027)
    assert periods == []


# =============================================================================
# Money — Construction
# =============================================================================


def test_money_from_decimal():
    m = Money(Decimal("1000.50"))
    assert m.amount == Decimal("1000.50")


def test_money_from_int():
    m = Money(1000)
    assert m.amount == Decimal("1000")


def test_money_from_string_decimal():
    m = Money(Decimal("0"))
    assert m.amount == Decimal("0")


# =============================================================================
# Money — Formatting
# =============================================================================


def test_money_formatted_whole():
    m = Money(Decimal("1000"))
    assert m.formatted == "$1,000"


def test_money_formatted_large():
    m = Money(Decimal("1234567"))
    assert m.formatted == "$1,234,567"


def test_money_formatted_zero():
    m = Money(Decimal("0"))
    assert m.formatted == "$0"


def test_money_formatted_with_sign_positive():
    m = Money(Decimal("5000"))
    assert m.formatted_with_sign == "+$5,000"


def test_money_formatted_with_sign_negative():
    m = Money(Decimal("-3000"))
    assert m.formatted_with_sign == "-$3,000"


def test_money_formatted_with_sign_zero():
    m = Money(Decimal("0"))
    assert m.formatted_with_sign == "+$0"


# =============================================================================
# Money — Arithmetic
# =============================================================================


def test_money_addition():
    a = Money(Decimal("1000"))
    b = Money(Decimal("500"))
    result = a + b
    assert result.amount == Decimal("1500")


def test_money_subtraction():
    a = Money(Decimal("1000"))
    b = Money(Decimal("300"))
    result = a - b
    assert result.amount == Decimal("700")


def test_money_subtraction_negative_result():
    a = Money(Decimal("100"))
    b = Money(Decimal("500"))
    result = a - b
    assert result.amount == Decimal("-400")


def test_money_addition_returns_new_instance():
    a = Money(Decimal("1000"))
    b = Money(Decimal("500"))
    result = a + b
    assert result is not a
    assert result is not b


# =============================================================================
# Money — zero() factory
# =============================================================================


def test_money_zero():
    m = Money.zero()
    assert m.amount == Decimal("0")


def test_money_zero_formatted():
    assert Money.zero().formatted == "$0"


# =============================================================================
# Money — Frozen (immutable)
# =============================================================================


def test_money_is_frozen():
    m = Money(Decimal("1000"))
    with pytest.raises((AttributeError, TypeError)):
        m.amount = Decimal("9999")  # type: ignore[misc]


# =============================================================================
# Helpers
# =============================================================================


def _make_entity(name="TestAE"):
    return RevenueEntity(entity_id=1, entity_name=name, entity_type=EntityType.AE)


def _make_row(
    budget=10_000,
    forecast_entered=10_000,
    booked=5_000,
    year=2099,
    month=6,
):
    """Create a PlanningRow. Defaults to a future period (year=2099)."""
    entity = _make_entity()
    period = PlanningPeriod(year=year, month=month)
    return PlanningRow(
        entity=entity,
        period=period,
        budget=Money(Decimal(str(budget))),
        forecast_entered=Money(Decimal(str(forecast_entered))),
        booked=Money(Decimal(str(booked))),
    )


# =============================================================================
# PlanningRow — forecast property
# =============================================================================


def test_planning_row_forecast_past_returns_booked():
    # Past period: year=2020 is always in the past
    row = _make_row(budget=10_000, forecast_entered=10_000, booked=7_500, year=2020)
    assert row.forecast.amount == Decimal("7500")


def test_planning_row_forecast_past_ignores_forecast_entered():
    # Even if forecast_entered > booked, past period returns booked
    row = _make_row(budget=10_000, forecast_entered=12_000, booked=8_000, year=2020)
    assert row.forecast.amount == Decimal("8000")


def test_planning_row_forecast_future_returns_forecast_entered_when_higher():
    row = _make_row(forecast_entered=10_000, booked=5_000, year=2099)
    assert row.forecast.amount == Decimal("10000")


def test_planning_row_forecast_future_auto_raises_when_booked_exceeds():
    # booked > forecast_entered → forecast auto-raises to booked
    row = _make_row(forecast_entered=10_000, booked=12_000, year=2099)
    assert row.forecast.amount == Decimal("12000")


def test_planning_row_forecast_future_equal_booked_and_forecast():
    row = _make_row(forecast_entered=10_000, booked=10_000, year=2099)
    assert row.forecast.amount == Decimal("10000")


def test_planning_row_forecast_zero_booked_future():
    row = _make_row(forecast_entered=10_000, booked=0, year=2099)
    assert row.forecast.amount == Decimal("10000")


# =============================================================================
# PlanningRow — derived properties
# =============================================================================


def test_planning_row_pipeline_is_forecast_minus_booked():
    row = _make_row(forecast_entered=10_000, booked=3_000, year=2099)
    assert row.pipeline.amount == Decimal("7000")


def test_planning_row_pipeline_zero_when_fully_booked():
    row = _make_row(forecast_entered=10_000, booked=10_000, year=2099)
    assert row.pipeline.amount == Decimal("0")


def test_planning_row_pipeline_zero_for_past_period():
    # Past period: forecast == booked, so pipeline == 0
    row = _make_row(forecast_entered=10_000, booked=7_000, year=2020)
    assert row.pipeline.amount == Decimal("0")


def test_planning_row_variance_to_budget_positive():
    row = _make_row(budget=10_000, forecast_entered=12_000, booked=0, year=2099)
    assert row.variance_to_budget.amount == Decimal("2000")


def test_planning_row_variance_to_budget_negative():
    row = _make_row(budget=10_000, forecast_entered=8_000, booked=0, year=2099)
    assert row.variance_to_budget.amount == Decimal("-2000")


def test_planning_row_pct_booked_normal():
    row = _make_row(forecast_entered=10_000, booked=2_500, year=2099)
    assert row.pct_booked == pytest.approx(25.0)


def test_planning_row_pct_booked_fully_booked():
    row = _make_row(forecast_entered=10_000, booked=10_000, year=2099)
    assert row.pct_booked == pytest.approx(100.0)


def test_planning_row_pct_booked_zero_forecast_returns_zero():
    row = _make_row(forecast_entered=0, booked=0, year=2099)
    assert row.pct_booked == 0.0


def test_planning_row_is_forecast_overridden_true():
    row = _make_row(budget=10_000, forecast_entered=12_000, year=2099)
    assert row.is_forecast_overridden is True


def test_planning_row_is_forecast_overridden_false_when_equal():
    row = _make_row(budget=10_000, forecast_entered=10_000, year=2099)
    assert row.is_forecast_overridden is False


def test_planning_row_is_booked_exceeds_forecast_entered_true():
    row = _make_row(forecast_entered=10_000, booked=11_000, year=2099)
    assert row.is_booked_exceeds_forecast_entered is True


def test_planning_row_is_booked_exceeds_forecast_entered_false():
    row = _make_row(forecast_entered=10_000, booked=9_000, year=2099)
    assert row.is_booked_exceeds_forecast_entered is False


# =============================================================================
# EntityPlanningData — totals
# =============================================================================


def _make_entity_planning_data():
    entity = _make_entity()
    rows = [
        _make_row(budget=10_000, forecast_entered=10_000, booked=5_000, year=2099, month=1),
        _make_row(budget=8_000, forecast_entered=9_000, booked=9_000, year=2099, month=2),
        _make_row(budget=12_000, forecast_entered=12_000, booked=6_000, year=2099, month=3),
    ]
    return EntityPlanningData(entity=entity, rows=rows)


def test_entity_planning_data_total_budget():
    epd = _make_entity_planning_data()
    assert epd.total_budget.amount == Decimal("30000")


def test_entity_planning_data_total_forecast():
    # month1: max(10000, 5000)=10000, month2: max(9000, 9000)=9000, month3: max(12000, 6000)=12000
    epd = _make_entity_planning_data()
    assert epd.total_forecast.amount == Decimal("31000")


def test_entity_planning_data_total_booked():
    epd = _make_entity_planning_data()
    assert epd.total_booked.amount == Decimal("20000")


def test_entity_planning_data_total_pipeline():
    epd = _make_entity_planning_data()
    # total_forecast - total_booked = 31000 - 20000
    assert epd.total_pipeline.amount == Decimal("11000")


def test_entity_planning_data_total_variance():
    epd = _make_entity_planning_data()
    # total_forecast - total_budget = 31000 - 30000
    assert epd.total_variance.amount == Decimal("1000")


def test_entity_planning_data_rows_by_period_keys():
    epd = _make_entity_planning_data()
    rbp = epd.rows_by_period
    assert "2099-1" in rbp
    assert "2099-2" in rbp
    assert "2099-3" in rbp


def test_entity_planning_data_rows_by_period_correct_row():
    epd = _make_entity_planning_data()
    row = epd.rows_by_period["2099-1"]
    assert row.period == PlanningPeriod(year=2099, month=1)
    assert row.budget.amount == Decimal("10000")


def test_entity_planning_data_row_for_period_found():
    epd = _make_entity_planning_data()
    period = PlanningPeriod(year=2099, month=2)
    row = epd.row_for_period(period)
    assert row is not None
    assert row.budget.amount == Decimal("8000")


def test_entity_planning_data_row_for_period_not_found():
    epd = _make_entity_planning_data()
    period = PlanningPeriod(year=2099, month=9)
    assert epd.row_for_period(period) is None


def test_entity_planning_data_empty_rows_totals_zero():
    entity = _make_entity()
    epd = EntityPlanningData(entity=entity, rows=[])
    assert epd.total_budget.amount == Decimal("0")
    assert epd.total_forecast.amount == Decimal("0")
    assert epd.total_booked.amount == Decimal("0")


# =============================================================================
# EntitySectorExpectations
# =============================================================================


def _make_sector_expectation(sector_id, sector_code, sector_name, month, amount):
    return SectorExpectation(
        ae_name="TestAE",
        sector_id=sector_id,
        sector_code=sector_code,
        sector_name=sector_name,
        year=2026,
        month=month,
        expected_amount=Decimal(str(amount)),
    )


def _make_entity_sector_expectations():
    expectations = [
        _make_sector_expectation(1, "AUTO", "Automotive", 1, 5_000),
        _make_sector_expectation(1, "AUTO", "Automotive", 2, 6_000),
        _make_sector_expectation(2, "RETAIL", "Retail", 1, 3_000),
        _make_sector_expectation(2, "RETAIL", "Retail", 2, 4_000),
    ]
    return EntitySectorExpectations(
        entity_name="TestAE",
        entity_type=EntityType.AE,
        year=2026,
        expectations=expectations,
    )


def test_entity_sector_expectations_for_month():
    ese = _make_entity_sector_expectations()
    month1 = ese.for_month(1)
    assert len(month1) == 2
    assert all(e.month == 1 for e in month1)


def test_entity_sector_expectations_total_for_month():
    ese = _make_entity_sector_expectations()
    assert ese.total_for_month(1) == Decimal("8000")
    assert ese.total_for_month(2) == Decimal("10000")


def test_entity_sector_expectations_total_for_month_missing_returns_zero():
    ese = _make_entity_sector_expectations()
    assert ese.total_for_month(6) == Decimal("0")


def test_entity_sector_expectations_total_for_sector():
    ese = _make_entity_sector_expectations()
    assert ese.total_for_sector(1) == Decimal("11000")
    assert ese.total_for_sector(2) == Decimal("7000")


def test_entity_sector_expectations_annual_total():
    ese = _make_entity_sector_expectations()
    assert ese.annual_total() == Decimal("18000")


def test_entity_sector_expectations_sectors_used_unique():
    ese = _make_entity_sector_expectations()
    sectors = ese.sectors_used()
    assert len(sectors) == 2
    sector_ids = [s[0] for s in sectors]
    assert 1 in sector_ids
    assert 2 in sector_ids


def test_entity_sector_expectations_monthly_grid_structure():
    ese = _make_entity_sector_expectations()
    grid = ese.monthly_grid()
    assert grid[1][1] == Decimal("5000")
    assert grid[1][2] == Decimal("6000")
    assert grid[2][1] == Decimal("3000")
    assert grid[2][2] == Decimal("4000")


def test_entity_sector_expectations_monthly_grid_missing_key():
    ese = _make_entity_sector_expectations()
    grid = ese.monthly_grid()
    assert 3 not in grid


# =============================================================================
# SectorExpectationValidation
# =============================================================================


def test_sector_expectation_validation_starts_valid():
    v = SectorExpectationValidation(entity_name="TestAE", year=2026, is_valid=True)
    assert v.is_valid is True
    assert v.errors == []


def test_sector_expectation_validation_add_month_mismatch_makes_invalid():
    v = SectorExpectationValidation(entity_name="TestAE", year=2026, is_valid=True)
    v.add_month_mismatch(month=3, budget=Decimal("10000"), sector_total=Decimal("8000"))
    assert v.is_valid is False
    assert len(v.errors) == 1
    assert v.month_details[3]["balanced"] is False
    assert v.month_details[3]["difference"] == Decimal("-2000")


def test_sector_expectation_validation_add_month_balanced_stays_valid():
    v = SectorExpectationValidation(entity_name="TestAE", year=2026, is_valid=True)
    v.add_month_balanced(month=4, amount=Decimal("12000"))
    assert v.is_valid is True
    assert v.month_details[4]["balanced"] is True
    assert v.month_details[4]["difference"] == Decimal("0")


# =============================================================================
# ForecastUpdate — period property
# =============================================================================


def test_forecast_update_period_returns_correct_planning_period():
    update = ForecastUpdate(
        ae_name="TestAE",
        year=2026,
        month=5,
        new_amount=Decimal("15000"),
        updated_by="user1",
    )
    assert update.period == PlanningPeriod(year=2026, month=5)
