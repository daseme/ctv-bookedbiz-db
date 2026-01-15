"""
Domain models for sector-level planning detail view.
Combines sector expectations with booked actuals for drill-down display.
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from decimal import Decimal


@dataclass
class SectorMonthDetail:
    """Single sector/month cell in the planning detail grid."""
    sector_id: int
    sector_code: str
    sector_name: str
    month: int
    expected: Decimal = Decimal('0')
    booked: Decimal = Decimal('0')
    
    @property
    def gap(self) -> Decimal:
        """Negative = behind expectation, Positive = ahead."""
        return self.booked - self.expected
    
    @property
    def gap_pct(self) -> float:
        """Gap as percentage of expected. None if no expectation."""
        if self.expected == 0:
            return 0.0 if self.booked == 0 else 100.0
        return float((self.gap / self.expected) * 100)
    
    @property
    def is_on_track(self) -> bool:
        """True if booked >= expected."""
        return self.booked >= self.expected
    
    @property
    def expected_formatted(self) -> str:
        return f"${self.expected:,.0f}"
    
    @property
    def booked_formatted(self) -> str:
        return f"${self.booked:,.0f}"
    
    @property
    def gap_formatted(self) -> str:
        sign = '+' if self.gap >= 0 else ''
        return f"{sign}${self.gap:,.0f}"


@dataclass
class SectorPlanningRow:
    """A single sector's data across all months."""
    sector_id: int
    sector_code: str
    sector_name: str
    months: Dict[int, SectorMonthDetail] = field(default_factory=dict)
    
    def for_month(self, month: int) -> SectorMonthDetail:
        """Get detail for a specific month, creating empty if missing."""
        if month not in self.months:
            self.months[month] = SectorMonthDetail(
                sector_id=self.sector_id,
                sector_code=self.sector_code,
                sector_name=self.sector_name,
                month=month
            )
        return self.months[month]
    
    @property
    def total_expected(self) -> Decimal:
        return sum(m.expected for m in self.months.values())
    
    @property
    def total_booked(self) -> Decimal:
        return sum(m.booked for m in self.months.values())
    
    @property
    def total_gap(self) -> Decimal:
        return self.total_booked - self.total_expected
    
    @property
    def total_expected_formatted(self) -> str:
        return f"${self.total_expected:,.0f}"
    
    @property
    def total_booked_formatted(self) -> str:
        return f"${self.total_booked:,.0f}"
    
    @property
    def total_gap_formatted(self) -> str:
        sign = '+' if self.total_gap >= 0 else ''
        return f"{sign}${self.total_gap:,.0f}"


@dataclass
class EntitySectorPlanningDetail:
    """Complete sector planning detail for an entity."""
    ae_name: str
    year: int
    sectors: List[SectorPlanningRow] = field(default_factory=list)
    has_sector_expectations: bool = False
    
    @property
    def total_expected(self) -> Decimal:
        return sum(s.total_expected for s in self.sectors)
    
    @property
    def total_booked(self) -> Decimal:
        return sum(s.total_booked for s in self.sectors)
    
    @property
    def total_gap(self) -> Decimal:
        return self.total_booked - self.total_expected
    
    def worst_gaps(self, n: int = 3) -> List[SectorPlanningRow]:
        """Returns sectors with largest negative gaps."""
        return sorted(
            [s for s in self.sectors if s.total_gap < 0],
            key=lambda s: s.total_gap
        )[:n]
    
    def best_performers(self, n: int = 3) -> List[SectorPlanningRow]:
        """Returns sectors most ahead of expectation."""
        return sorted(
            [s for s in self.sectors if s.total_gap > 0],
            key=lambda s: s.total_gap,
            reverse=True
        )[:n]
    
    def summary_message(self) -> Optional[str]:
        """Quick summary of sector status for the entity."""
        if not self.has_sector_expectations:
            return "No sector expectations defined"
        
        worst = self.worst_gaps(1)
        best = self.best_performers(1)
        
        parts = []
        if worst:
            s = worst[0]
            parts.append(f"{s.sector_name} {s.total_gap_formatted}")
        if best:
            s = best[0]
            parts.append(f"{s.sector_name} {s.total_gap_formatted}")
        
        return " | ".join(parts) if parts else "On track"
    
    def to_dict(self) -> dict:
        """Serialize for JSON API response."""
        return {
            'ae_name': self.ae_name,
            'year': self.year,
            'has_sector_expectations': self.has_sector_expectations,
            'total_expected': float(self.total_expected),
            'total_booked': float(self.total_booked),
            'total_gap': float(self.total_gap),
            'sectors': [
                {
                    'sector_id': s.sector_id,
                    'sector_code': s.sector_code,
                    'sector_name': s.sector_name,
                    'total_expected': float(s.total_expected),
                    'total_booked': float(s.total_booked),
                    'total_gap': float(s.total_gap),
                    'months': {
                        m: {
                            'expected': float(d.expected),
                            'booked': float(d.booked),
                            'gap': float(d.gap)
                        }
                        for m, d in s.months.items()
                    }
                }
                for s in self.sectors
            ]
        }