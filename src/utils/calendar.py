"""
Sellable Days Calendar Utility

Calculates weekdays (sellable days) for planning purposes.
Supports per-month adjustments from database.
"""

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional, Callable
import calendar


@dataclass
class SellableDaysInfo:
    """Sellable days breakdown for a month."""
    year: int
    month: int
    
    # Base weekday counts
    weekdays_total: int
    weekdays_elapsed: int
    weekdays_remaining: int
    
    # Adjustment from DB
    adjustment: int
    adjustment_reason: Optional[str]
    
    @property
    def sellable_total(self) -> int:
        """Total sellable days after adjustment."""
        return max(self.weekdays_total + self.adjustment, 0)
    
    @property
    def sellable_elapsed(self) -> int:
        """Sellable days elapsed (capped at adjusted total)."""
        return min(self.weekdays_elapsed, self.sellable_total)
    
    @property
    def sellable_remaining(self) -> int:
        """Sellable days remaining."""
        return max(self.sellable_total - self.sellable_elapsed, 0)
    
    @property
    def is_month_complete(self) -> bool:
        """True if no sellable days remain."""
        return self.sellable_remaining == 0


class SellableDaysCalendar:
    """
    Calculator for sellable (weekday) days in a month.
    
    Usage:
        calendar = SellableDaysCalendar(adjustment_lookup_fn)
        info = calendar.get_sellable_days(2026, 1)
        print(f"{info.sellable_remaining} days left")
    """
    
    def __init__(self, adjustment_lookup: Optional[Callable[[int, int], tuple]] = None):
        """
        Args:
            adjustment_lookup: Function(year, month) -> (adjustment: int, reason: str|None)
                              If None, no adjustments are applied.
        """
        self._adjustment_lookup = adjustment_lookup
    
    def get_sellable_days(
        self, 
        year: int, 
        month: int, 
        as_of: Optional[date] = None
    ) -> SellableDaysInfo:
        """
        Calculate sellable days for a month.
        
        Args:
            year: The year
            month: The month (1-12)
            as_of: Calculate elapsed/remaining as of this date (default: today)
        
        Returns:
            SellableDaysInfo with all calculations
        """
        if as_of is None:
            as_of = date.today()
        
        # Get month boundaries
        first_day = date(year, month, 1)
        last_day = date(year, month, calendar.monthrange(year, month)[1])
        
        # Calculate weekdays
        weekdays_total = self._count_weekdays(first_day, last_day)
        
        # Calculate elapsed (up to as_of or end of month, whichever is earlier)
        if as_of < first_day:
            weekdays_elapsed = 0
        elif as_of >= last_day:
            weekdays_elapsed = weekdays_total
        else:
            weekdays_elapsed = self._count_weekdays(first_day, as_of)
        
        weekdays_remaining = weekdays_total - weekdays_elapsed
        
        # Get adjustment from DB
        adjustment = 0
        adjustment_reason = None
        if self._adjustment_lookup:
            adj_result = self._adjustment_lookup(year, month)
            if adj_result:
                adjustment, adjustment_reason = adj_result
        
        return SellableDaysInfo(
            year=year,
            month=month,
            weekdays_total=weekdays_total,
            weekdays_elapsed=weekdays_elapsed,
            weekdays_remaining=weekdays_remaining,
            adjustment=adjustment,
            adjustment_reason=adjustment_reason
        )
    
    def _count_weekdays(self, start: date, end: date) -> int:
        """Count weekdays (Mon-Fri) between start and end inclusive."""
        if start > end:
            return 0
        
        count = 0
        current = start
        while current <= end:
            if current.weekday() < 5:  # Mon=0, Fri=4
                count += 1
            current += timedelta(days=1)
        
        return count
    
    @staticmethod
    def weekdays_in_month(year: int, month: int) -> int:
        """Quick helper: total weekdays in a month (no adjustments)."""
        first_day = date(year, month, 1)
        last_day = date(year, month, calendar.monthrange(year, month)[1])
        
        count = 0
        current = first_day
        while current <= last_day:
            if current.weekday() < 5:
                count += 1
            current += timedelta(days=1)
        
        return count