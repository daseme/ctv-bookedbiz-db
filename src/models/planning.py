"""
Planning Tool Domain Models

Pure business entities with no dependencies on data access or presentation layers.
"""

from dataclasses import dataclass, field
from datetime import datetime, date
from decimal import Decimal
from enum import Enum
from typing import List, Optional, Dict, Any


# ============================================================================
# Enums
# ============================================================================


class EntityType(Enum):
    """Type of revenue-generating entity"""

    AE = "AE"
    HOUSE = "House"
    AGENCY = "Agency"

    @property
    def display_name(self) -> str:
        return self.value


class Month(Enum):
    """Month enum with display and parsing utilities"""

    JAN = (1, "Jan")
    FEB = (2, "Feb")
    MAR = (3, "Mar")
    APR = (4, "Apr")
    MAY = (5, "May")
    JUN = (6, "Jun")
    JUL = (7, "Jul")
    AUG = (8, "Aug")
    SEP = (9, "Sep")
    OCT = (10, "Oct")
    NOV = (11, "Nov")
    DEC = (12, "Dec")

    def __init__(self, number: int, abbrev: str):
        self._number = number
        self._abbrev = abbrev

    @property
    def number(self) -> int:
        return self._number

    @property
    def abbrev(self) -> str:
        return self._abbrev

    @classmethod
    def from_number(cls, n: int) -> "Month":
        for m in cls:
            if m.number == n:
                return m
        raise ValueError(f"Invalid month number: {n}")

    @classmethod
    def from_abbrev(cls, abbrev: str) -> "Month":
        for m in cls:
            if m.abbrev.upper() == abbrev.upper():
                return m
        raise ValueError(f"Invalid month abbreviation: {abbrev}")


# ============================================================================
# Value Objects
# ============================================================================


@dataclass(frozen=True)
class PlanningPeriod:
    """Represents a year/month combination"""

    year: int
    month: int

    def __post_init__(self):
        if not (2000 <= self.year <= 2100):
            raise ValueError(f"Year must be between 2000 and 2100: {self.year}")
        if not (1 <= self.month <= 12):
            raise ValueError(f"Month must be between 1 and 12: {self.month}")

    @property
    def key(self) -> str:
        """Unique key for dictionary lookups (e.g., '2025-1')"""
        return f"{self.year}-{self.month}"

    @property
    def broadcast_month(self) -> str:
        """Format as 'Jan-25' for spots table compatibility"""
        m = Month.from_number(self.month)
        return f"{m.abbrev}-{str(self.year)[2:]}"

    @property
    def display(self) -> str:
        """Human-readable format"""
        m = Month.from_number(self.month)
        return f"{m.abbrev} {self.year}"

    @property
    def sort_key(self) -> int:
        """For sorting periods chronologically"""
        return self.year * 100 + self.month

    @property
    def is_past(self) -> bool:
        """Check if this period is in the past (before current month)"""
        today = date.today()
        if self.year < today.year:
            return True
        if self.year == today.year and self.month < today.month:
            return True
        return False

    @property
    def is_current(self) -> bool:
        """Check if this is the current month"""
        today = date.today()
        return self.year == today.year and self.month == today.month

    @property
    def is_future(self) -> bool:
        """Check if this period is in the future"""
        return not self.is_past and not self.is_current

    @property
    def quarter(self) -> int:
        """Get the quarter (1-4) for this period"""
        return (self.month - 1) // 3 + 1

    @property
    def is_quarter_end(self) -> bool:
        """Check if this month is end of a quarter (Mar, Jun, Sep, Dec)"""
        return self.month in [3, 6, 9, 12]

    def __lt__(self, other: "PlanningPeriod") -> bool:
        if self.year != other.year:
            return self.year < other.year
        return self.month < other.month

    def __le__(self, other: "PlanningPeriod") -> bool:
        return self == other or self < other

    def __gt__(self, other: "PlanningPeriod") -> bool:
        return not self <= other

    def __ge__(self, other: "PlanningPeriod") -> bool:
        return not self < other

    def __hash__(self) -> int:
        return hash((self.year, self.month))

    @classmethod
    def from_broadcast_month(cls, bm: str) -> "PlanningPeriod":
        """Parse 'Jan-25' format"""
        parts = bm.split("-")
        if len(parts) != 2:
            raise ValueError(f"Invalid broadcast_month format: {bm}")
        month = Month.from_abbrev(parts[0])
        year = 2000 + int(parts[1])
        return cls(year=year, month=month.number)

    @classmethod
    def current(cls) -> "PlanningPeriod":
        """Get current month"""
        today = date.today()
        return cls(year=today.year, month=today.month)

    def next(self, months: int = 1) -> "PlanningPeriod":
        """Get period N months ahead"""
        total_months = self.year * 12 + self.month - 1 + months
        new_year = total_months // 12
        new_month = total_months % 12 + 1
        return PlanningPeriod(year=new_year, month=new_month)

    @classmethod
    def planning_window(cls, months_ahead: int = 2) -> List["PlanningPeriod"]:
        """Get current month plus N months ahead"""
        current = cls.current()
        return [current.next(i) for i in range(months_ahead + 1)]

    @classmethod
    def full_year(cls, year: int) -> List["PlanningPeriod"]:
        """Get all 12 periods for a year"""
        return [cls(year=year, month=m) for m in range(1, 13)]

    @classmethod
    def past_periods(cls, year: int) -> List["PlanningPeriod"]:
        """Get all past periods for a given year (before current month)"""
        periods = []
        for month in range(1, 13):
            period = cls(year=year, month=month)
            if period.is_past:
                periods.append(period)
        return periods

    @classmethod
    def future_periods(cls, year: int) -> List["PlanningPeriod"]:
        """Get all future periods for a given year (after current month)"""
        periods = []
        for month in range(1, 13):
            period = cls(year=year, month=month)
            if period.is_future:
                periods.append(period)
        return periods


@dataclass(frozen=True)
class Money:
    """Value object for monetary amounts"""

    amount: Decimal

    def __post_init__(self):
        object.__setattr__(self, "amount", Decimal(str(self.amount)))

    @property
    def formatted(self) -> str:
        """Format as currency string"""
        return f"${self.amount:,.0f}"

    @property
    def formatted_with_sign(self) -> str:
        """Format with explicit +/- sign"""
        if self.amount >= 0:
            return f"+${self.amount:,.0f}"
        return f"-${abs(self.amount):,.0f}"

    def __add__(self, other: "Money") -> "Money":
        return Money(self.amount + other.amount)

    def __sub__(self, other: "Money") -> "Money":
        return Money(self.amount - other.amount)

    @classmethod
    def zero(cls) -> "Money":
        return cls(Decimal("0"))


# ============================================================================
# Domain Entities
# ============================================================================


@dataclass
class RevenueEntity:
    """A person or entity that generates revenue (AE, House, Agency)"""

    entity_id: int
    entity_name: str
    entity_type: EntityType
    is_active: bool = True
    notes: Optional[str] = None

    @property
    def display_name(self) -> str:
        return self.entity_name

    @property
    def type_badge(self) -> str:
        """Short badge for UI display"""
        return self.entity_type.value


@dataclass
class PlanningRow:
    """Planning data for one entity and one period"""

    entity: RevenueEntity
    period: PlanningPeriod
    budget: Money
    forecast_entered: Money  # What user entered (or defaulted from budget)
    booked: Money
    forecast_updated: Optional[datetime] = None
    forecast_updated_by: Optional[str] = None

    @property
    def forecast(self) -> Money:
        """
        Effective forecast: max of entered forecast and booked.
        You can't expect less than what you already have.
        """
        return Money(max(self.forecast_entered.amount, self.booked.amount))

    @property
    def pipeline(self) -> Money:
        """Derived: what still needs to come in to hit forecast"""
        return self.forecast - self.booked

    @property
    def variance_to_budget(self) -> Money:
        """Derived: effective forecast vs original plan"""
        return self.forecast - self.budget

    @property
    def pct_booked(self) -> float:
        """Percentage of forecast already booked"""
        if self.forecast.amount == 0:
            return 0.0
        return float(self.booked.amount / self.forecast.amount * 100)

    @property
    def pct_booked_formatted(self) -> str:
        return f"{self.pct_booked:.1f}%"

    @property
    def is_forecast_overridden(self) -> bool:
        """True if user-entered forecast differs from budget"""
        return self.forecast_entered.amount != self.budget.amount

    @property
    def is_booked_exceeds_forecast_entered(self) -> bool:
        """True if booked has exceeded the user-entered forecast"""
        return self.booked.amount > self.forecast_entered.amount


@dataclass
class EntityPlanningData:
    """All planning data for one entity across multiple periods"""

    entity: RevenueEntity
    rows: List[PlanningRow] = field(default_factory=list)

    @property
    def rows_by_period(self) -> Dict[str, PlanningRow]:
        """Get rows indexed by period key for efficient template lookups"""
        return {row.period.key: row for row in self.rows}

    @property
    def total_budget(self) -> Money:
        return Money(sum(r.budget.amount for r in self.rows))

    @property
    def total_forecast(self) -> Money:
        return Money(sum(r.forecast.amount for r in self.rows))

    @property
    def total_booked(self) -> Money:
        return Money(sum(r.booked.amount for r in self.rows))

    @property
    def total_pipeline(self) -> Money:
        return self.total_forecast - self.total_booked

    @property
    def total_variance(self) -> Money:
        return self.total_forecast - self.total_budget

    def row_for_period(self, period: PlanningPeriod) -> Optional[PlanningRow]:
        """Get row for a specific period"""
        for row in self.rows:
            if row.period == period:
                return row
        return None


@dataclass
class PlanningSummary:
    """Aggregate summary across all entities for full year view"""

    entity_data: List[EntityPlanningData] = field(default_factory=list)
    planning_year: int = field(default_factory=lambda: date.today().year)
    all_periods: List[PlanningPeriod] = field(default_factory=list)
    active_periods: List[PlanningPeriod] = field(default_factory=list)
    past_periods: List[PlanningPeriod] = field(default_factory=list)

    # Legacy support - 'periods' maps to active_periods
    @property
    def periods(self) -> List[PlanningPeriod]:
        """Legacy: returns active_periods for backward compatibility"""
        return self.active_periods if self.active_periods else self.all_periods

    def __post_init__(self):
        """Initialize periods if not provided"""
        if not self.all_periods:
            self.all_periods = PlanningPeriod.full_year(self.planning_year)
        if not self.active_periods:
            self.active_periods = PlanningPeriod.planning_window(2)
        if not self.past_periods:
            self.past_periods = PlanningPeriod.past_periods(self.planning_year)

    @property
    def future_periods(self) -> List[PlanningPeriod]:
        """Periods that are not past and not in active window"""
        active_set = set(self.active_periods)
        past_set = set(self.past_periods)
        return [
            p for p in self.all_periods if p not in past_set and p not in active_set
        ]

    @property
    def total_budget(self) -> Money:
        return Money(sum(e.total_budget.amount for e in self.entity_data))

    @property
    def total_forecast(self) -> Money:
        return Money(sum(e.total_forecast.amount for e in self.entity_data))

    @property
    def total_booked(self) -> Money:
        return Money(sum(e.total_booked.amount for e in self.entity_data))

    @property
    def total_pipeline(self) -> Money:
        return self.total_forecast - self.total_booked

    @property
    def total_variance(self) -> Money:
        return self.total_forecast - self.total_budget

    def totals_by_period(self, period: PlanningPeriod) -> Dict[str, Money]:
        """Get column totals for a specific period"""
        budget = Money.zero()
        forecast = Money.zero()
        booked = Money.zero()

        for entity_data in self.entity_data:
            row = entity_data.row_for_period(period)
            if row:
                budget = budget + row.budget
                forecast = forecast + row.forecast
                booked = booked + row.booked

        return {
            "budget": budget,
            "forecast": forecast,
            "booked": booked,
            "pipeline": forecast - booked,
            "variance": forecast - budget,
        }


# ============================================================================
# Add to src/models/planning.py
# ============================================================================


class PaceStatus(Enum):
    """Burn-down pace status thresholds."""

    AHEAD = "ahead"  # pace_ratio >= 1.05
    ON_TRACK = "on_track"  # 0.85 <= pace_ratio < 1.05
    BEHIND = "behind"  # pace_ratio < 0.85
    COMPLETE = "complete"  # no days remaining or fully booked

    @property
    def label(self) -> str:
        """Human-readable label."""
        return {
            PaceStatus.AHEAD: "Ahead",
            PaceStatus.ON_TRACK: "On Track",
            PaceStatus.BEHIND: "Behind",
            PaceStatus.COMPLETE: "Complete",
        }[self]

    @property
    def css_class(self) -> str:
        """CSS class for styling."""
        return f"pace-{self.value.replace('_', '-')}"


@dataclass
class BurnDownMetrics:
    """
    Burn-down metrics for a single planning period (month).

    This is the "metrics contract" that both service layer and UI consume.
    Computed server-side, rendered in template.
    """

    period: PlanningPeriod

    # Sellable days
    sellable_days_total: int
    sellable_days_elapsed: int
    sellable_days_remaining: int
    adjustment: int = 0
    adjustment_reason: Optional[str] = None

    # Dollar amounts (company-wide for the month)
    forecast_total: Money = field(default_factory=Money.zero)
    booked_total: Money = field(default_factory=Money.zero)

    # Booked month-to-date (by effective_date, for pace calculation)
    booked_mtd: Money = field(default_factory=Money.zero)

    @property
    def remaining_to_forecast(self) -> Money:
        """Dollars remaining to hit forecast."""
        diff = self.forecast_total.amount - self.booked_total.amount
        return Money(max(diff, Decimal("0")))

    @property
    def required_daily_pace(self) -> Money:
        """Dollars per day required to hit forecast."""
        if self.sellable_days_remaining <= 0:
            return Money.zero()
        return Money(self.remaining_to_forecast.amount / self.sellable_days_remaining)

    @property
    def actual_daily_pace(self) -> Money:
        """Actual dollars per day booked so far (by effective_date)."""
        if self.sellable_days_elapsed <= 0:
            return Money.zero()
        return Money(self.booked_mtd.amount / self.sellable_days_elapsed)

    @property
    def pace_ratio(self) -> float:
        """Ratio of actual pace to required pace. >1 = ahead, <1 = behind."""
        if self.required_daily_pace.amount <= 0:
            # No pace required (fully booked or no forecast)
            return 1.0 if self.remaining_to_forecast.amount <= 0 else 0.0
        return float(self.actual_daily_pace.amount / self.required_daily_pace.amount)

    @property
    def pace_status(self) -> PaceStatus:
        """Categorized pace status for UI."""
        if self.sellable_days_remaining <= 0:
            return PaceStatus.COMPLETE
        if self.remaining_to_forecast.amount <= 0:
            return PaceStatus.COMPLETE
        if self.pace_ratio >= 1.05:
            return PaceStatus.AHEAD
        if self.pace_ratio >= 0.85:
            return PaceStatus.ON_TRACK
        return PaceStatus.BEHIND

    @property
    def pace_bar_percent(self) -> int:
        """Progress bar percentage (capped at 150 for display)."""
        return min(int(self.pace_ratio * 100), 150)

    # Formatting helpers for template
    @property
    def required_pace_formatted(self) -> str:
        """Format required pace as compact string (e.g., '$16.4k/day')."""
        return self._format_compact_currency(self.required_daily_pace.amount) + "/day"

    @property
    def actual_pace_formatted(self) -> str:
        """Format actual pace as compact string."""
        return self._format_compact_currency(self.actual_daily_pace.amount) + "/day"

    @property
    def days_left_text(self) -> str:
        """Text for days remaining."""
        if self.sellable_days_remaining == 0:
            return "No days left"
        elif self.sellable_days_remaining == 1:
            return "1 day left"
        return f"{self.sellable_days_remaining} days left"

    def _format_compact_currency(self, amount: Decimal) -> str:
        """Format as compact currency (e.g., $16.4k, $1.2M)."""
        abs_amount = abs(float(amount))
        sign = "-" if amount < 0 else ""

        if abs_amount >= 1_000_000:
            return f"{sign}${abs_amount / 1_000_000:.1f}M"
        elif abs_amount >= 1_000:
            return f"{sign}${abs_amount / 1_000:.1f}k"
        else:
            return f"{sign}${abs_amount:.0f}"


# ============================================================================
# Command Objects (for updates)
# ============================================================================


@dataclass
class ForecastUpdate:
    """Command to update a forecast value"""

    ae_name: str
    year: int
    month: int
    new_amount: Decimal
    updated_by: str
    notes: Optional[str] = None

    @property
    def period(self) -> PlanningPeriod:
        return PlanningPeriod(year=self.year, month=self.month)


@dataclass
class ForecastChange:
    """Record of a forecast change (for history)"""

    ae_name: str
    period: PlanningPeriod
    previous_amount: Optional[Money]
    new_amount: Money
    changed_date: datetime
    changed_by: str
    session_notes: Optional[str] = None


# ============================================================================
# Sector Expectations - Domain Models
# Add this section to src/models/planning.py
# ============================================================================


@dataclass
class SectorExpectation:
    """
    Sector-level budget expectation for an AE/month.

    These capture the "theory of the year" - the intended revenue mix
    that explains HOW an AE will hit their budget number.
    """

    ae_name: str
    sector_id: int
    sector_code: str  # Denormalized for display (e.g., "AUTO")
    sector_name: str  # Denormalized for display (e.g., "Automotive")
    year: int
    month: int
    expected_amount: Decimal
    notes: Optional[str] = None
    expectation_id: Optional[int] = None
    created_date: Optional[datetime] = None
    updated_date: Optional[datetime] = None
    updated_by: Optional[str] = None
    # Per-month forecasts for this sector
    new_accounts_forecast: Optional[int] = None
    new_dollars_forecast: Optional[Decimal] = None

    @property
    def period(self) -> PlanningPeriod:
        """Get the planning period for this expectation."""
        return PlanningPeriod(year=self.year, month=self.month)

    @property
    def expected_money(self) -> Money:
        """Get expected amount as Money object."""
        return Money(self.expected_amount)


@dataclass
class EntitySectorExpectations:
    """
    All sector expectations for one entity (AE/House) for a year.

    Used for validation: sector expectations must sum to budget for each month.
    """

    entity_name: str
    entity_type: EntityType
    year: int
    expectations: List[SectorExpectation] = field(default_factory=list)

    def for_month(self, month: int) -> List[SectorExpectation]:
        """Get all expectations for a specific month."""
        return [e for e in self.expectations if e.month == month]

    def for_sector(self, sector_id: int) -> List[SectorExpectation]:
        """Get all expectations for a specific sector (across all months)."""
        return [e for e in self.expectations if e.sector_id == sector_id]

    def total_for_month(self, month: int) -> Decimal:
        """Sum of all sector expectations for a month."""
        return sum((e.expected_amount for e in self.for_month(month)), Decimal("0"))

    def total_for_sector(self, sector_id: int) -> Decimal:
        """Sum of all months for a specific sector."""
        return sum(
            (e.expected_amount for e in self.for_sector(sector_id)), Decimal("0")
        )

    def annual_total(self) -> Decimal:
        """Total across all sectors and months."""
        return sum((e.expected_amount for e in self.expectations), Decimal("0"))

    def sectors_used(self) -> List[tuple]:
        """Get unique (sector_id, sector_code, sector_name) tuples used."""
        seen = {}
        for e in self.expectations:
            if e.sector_id not in seen:
                seen[e.sector_id] = (e.sector_id, e.sector_code, e.sector_name)
        return list(seen.values())

    def monthly_grid(self) -> Dict[int, Dict[int, Decimal]]:
        """
        Build a grid: sector_id -> month -> amount.
        Useful for template rendering.
        """
        grid = {}
        for e in self.expectations:
            if e.sector_id not in grid:
                grid[e.sector_id] = {}
            grid[e.sector_id][e.month] = e.expected_amount
        return grid

    def new_accounts_grid(self) -> Dict[int, Dict[int, Optional[int]]]:
        """Build a grid: sector_id -> month -> new_accounts_forecast."""
        grid: Dict[int, Dict[int, Optional[int]]] = {}
        for e in self.expectations:
            if e.sector_id not in grid:
                grid[e.sector_id] = {}
            grid[e.sector_id][e.month] = e.new_accounts_forecast
        return grid

    def new_dollars_grid(self) -> Dict[int, Dict[int, Optional[Decimal]]]:
        """Build a grid: sector_id -> month -> new_dollars_forecast."""
        grid: Dict[int, Dict[int, Optional[Decimal]]] = {}
        for e in self.expectations:
            if e.sector_id not in grid:
                grid[e.sector_id] = {}
            grid[e.sector_id][e.month] = e.new_dollars_forecast
        return grid


@dataclass
class SectorExpectationValidation:
    """Result of validating sector expectations against budget."""

    entity_name: str
    year: int
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    # Month-level detail
    month_details: Dict[int, Dict[str, Any]] = field(default_factory=dict)

    def add_month_mismatch(self, month: int, budget: Decimal, sector_total: Decimal):
        """Record a month where sectors don't sum to budget."""
        diff = sector_total - budget
        self.errors.append(
            f"Month {month}: sectors (${sector_total:,.0f}) ≠ budget (${budget:,.0f}), "
            f"difference: ${diff:+,.0f}"
        )
        self.month_details[month] = {
            "budget": budget,
            "sector_total": sector_total,
            "difference": diff,
            "balanced": False,
        }
        self.is_valid = False

    def add_month_balanced(self, month: int, amount: Decimal):
        """Record a month that balances correctly."""
        self.month_details[month] = {
            "budget": amount,
            "sector_total": amount,
            "difference": Decimal("0"),
            "balanced": True,
        }
