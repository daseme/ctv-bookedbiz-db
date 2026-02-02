"""
Domain models for pricing intelligence and trends analysis.
Immutable dataclasses for pricing trend metrics.
"""
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class TrendPoint:
    """Single point in a time-series trend"""
    period: str  # 'Jan-24', 'Feb-24', etc.
    dimension_value: str  # The specific sector/language/AE being tracked
    average_rate: float
    spot_count: int
    total_revenue: float
    
    @property
    def average_rate_formatted(self) -> str:
        return f"${self.average_rate:,.2f}"
    
    @property
    def total_revenue_formatted(self) -> str:
        return f"${self.total_revenue:,.2f}"


@dataclass(frozen=True)
class MarginTrendPoint:
    """Margin analysis over time"""
    period: str
    dimension_value: str
    gross_rate_avg: float
    station_net_avg: float
    margin_percentage: float
    spot_count: int
    
    @property
    def margin_formatted(self) -> str:
        return f"{self.margin_percentage:.1f}%"
    
    @property
    def gross_formatted(self) -> str:
        return f"${self.gross_rate_avg:,.2f}"
    
    @property
    def net_formatted(self) -> str:
        return f"${self.station_net_avg:,.2f}"


@dataclass(frozen=True)
class RateVolatility:
    """Measures pricing consistency for a dimension"""
    dimension_value: str
    average_rate: float
    std_deviation: float
    coefficient_variation: float
    min_rate: float
    max_rate: float
    spot_count: int
    
    @property
    def is_consistent(self) -> bool:
        """CV < 0.3 indicates consistent pricing"""
        return self.coefficient_variation < 0.3


@dataclass(frozen=True)
class ConcentrationMetrics:
    """Revenue concentration risk metrics"""
    period: str
    total_revenue: float
    total_customers: int
    herfindahl_index: float
    top_10_revenue: float
    top_10_percentage: float
    top_20_revenue: float
    top_20_percentage: float
    top_50_revenue: float
    top_50_percentage: float
    
    @property
    def concentration_risk(self) -> str:
        """Interpret HHI for business users"""
        if self.herfindahl_index > 0.25:
            return "High Risk"
        elif self.herfindahl_index > 0.15:
            return "Moderate Risk"
        else:
            return "Well Diversified"
    
    @property
    def hhi_formatted(self) -> str:
        return f"{self.herfindahl_index:.4f}"


@dataclass(frozen=True)
class TopCustomerContribution:
    """Individual customer's contribution to concentration"""
    customer_id: int
    customer_name: str
    revenue: float
    percentage_of_total: float
    rank: int
    
    @property
    def revenue_formatted(self) -> str:
        return f"${self.revenue:,.2f}"
    
    @property
    def percentage_formatted(self) -> str:
        return f"{self.percentage_of_total:.2f}%"