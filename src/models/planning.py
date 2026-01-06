"""
Planning Tool Domain Models

Pure business entities with no dependencies on data access or presentation layers.
"""

from dataclasses import dataclass, field
from datetime import datetime, date
from decimal import Decimal
from enum import Enum
from typing import List, Optional, Dict


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


@dataclass(frozen=True)
class Money:
    """Value object for monetary amounts"""
    amount: Decimal
    
    def __post_init__(self):
        object.__setattr__(self, 'amount', Decimal(str(self.amount)))
    
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
    forecast: Money
    booked: Money
    forecast_updated: Optional[datetime] = None
    forecast_updated_by: Optional[str] = None
    
    @property
    def pipeline(self) -> Money:
        """Derived: what still needs to come in"""
        return self.forecast - self.booked
    
    @property
    def variance_to_budget(self) -> Money:
        """Derived: forecast vs original plan"""
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
        """True if forecast differs from budget"""
        return self.forecast.amount != self.budget.amount


@dataclass
class EntityPlanningData:
    """All planning data for one entity across multiple periods"""
    entity: RevenueEntity
    rows: List[PlanningRow] = field(default_factory=list)
    
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
    """Aggregate summary across all entities"""
    periods: List[PlanningPeriod]
    entity_data: List[EntityPlanningData] = field(default_factory=list)
    
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
            "variance": forecast - budget
        }


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