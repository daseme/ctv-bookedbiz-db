# src/models/report_data.py
"""
Data models for report generation.
Provides structured data transfer objects for all report types.
"""
from typing import List, Dict, Any, Optional, Union
from datetime import date, datetime
from decimal import Decimal
from dataclasses import dataclass, asdict, field
import calendar


@dataclass
class ReportFilters:
    """Filters for report data queries."""
    year: Optional[int] = None
    customer_search: Optional[str] = None
    ae_filter: Optional[str] = None
    revenue_type: Optional[str] = None
    sector: Optional[str] = None
    market: Optional[str] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    revenue_field: str = 'gross'  # 'gross' or 'net'
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {k: v for k, v in asdict(self).items() if v is not None}


@dataclass
class ReportMetadata:
    """Metadata for report generation."""
    report_type: str
    parameters: Dict[str, Any]
    row_count: int
    processing_time_ms: float
    generated_at: datetime = None
    data_last_updated: Optional[datetime] = None
    
    def __post_init__(self):
        if self.generated_at is None:
            self.generated_at = datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = asdict(self)
        result['generated_at'] = self.generated_at.isoformat()
        return result


@dataclass
class MonthStatus:
    """Month closure status information."""
    month_number: int
    month_name: str
    status: str  # 'OPEN', 'CLOSED', 'UNKNOWN'
    closed_date: Optional[date] = None
    closed_by: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        result = asdict(self)
        if self.closed_date:
            result['closed_date'] = self.closed_date.isoformat()
        return result


@dataclass
class CustomerMonthlyRow:
    """Represents a customer's monthly revenue data"""
    customer_id: str
    customer: str
    ae: str
    revenue_type: str
    sector: str
    is_new_customer: bool = False  # New field to track new customers
    
    # Monthly values (initialized to zero)
    month_1_gross: Decimal = field(default_factory=lambda: Decimal('0'))
    month_1_net: Decimal = field(default_factory=lambda: Decimal('0'))
    month_2_gross: Decimal = field(default_factory=lambda: Decimal('0'))
    month_2_net: Decimal = field(default_factory=lambda: Decimal('0'))
    month_3_gross: Decimal = field(default_factory=lambda: Decimal('0'))
    month_3_net: Decimal = field(default_factory=lambda: Decimal('0'))
    month_4_gross: Decimal = field(default_factory=lambda: Decimal('0'))
    month_4_net: Decimal = field(default_factory=lambda: Decimal('0'))
    month_5_gross: Decimal = field(default_factory=lambda: Decimal('0'))
    month_5_net: Decimal = field(default_factory=lambda: Decimal('0'))
    month_6_gross: Decimal = field(default_factory=lambda: Decimal('0'))
    month_6_net: Decimal = field(default_factory=lambda: Decimal('0'))
    month_7_gross: Decimal = field(default_factory=lambda: Decimal('0'))
    month_7_net: Decimal = field(default_factory=lambda: Decimal('0'))
    month_8_gross: Decimal = field(default_factory=lambda: Decimal('0'))
    month_8_net: Decimal = field(default_factory=lambda: Decimal('0'))
    month_9_gross: Decimal = field(default_factory=lambda: Decimal('0'))
    month_9_net: Decimal = field(default_factory=lambda: Decimal('0'))
    month_10_gross: Decimal = field(default_factory=lambda: Decimal('0'))
    month_10_net: Decimal = field(default_factory=lambda: Decimal('0'))
    month_11_gross: Decimal = field(default_factory=lambda: Decimal('0'))
    month_11_net: Decimal = field(default_factory=lambda: Decimal('0'))
    month_12_gross: Decimal = field(default_factory=lambda: Decimal('0'))
    month_12_net: Decimal = field(default_factory=lambda: Decimal('0'))
    
    def set_month_value(self, month: int, gross_value: Decimal, net_value: Decimal) -> None:
        """Set the gross and net values for a specific month (1-12)"""
        if 1 <= month <= 12:
            setattr(self, f'month_{month}_gross', gross_value)
            setattr(self, f'month_{month}_net', net_value)
    
    def get_month_gross(self, month: int) -> Decimal:
        """Get gross value for a specific month (1-12)"""
        if 1 <= month <= 12:
            return getattr(self, f'month_{month}_gross', Decimal('0'))
        return Decimal('0')
    
    def get_month_net(self, month: int) -> Decimal:
        """Get net value for a specific month (1-12)"""
        if 1 <= month <= 12:
            return getattr(self, f'month_{month}_net', Decimal('0'))
        return Decimal('0')
    
    @property
    def total_gross(self) -> Decimal:
        """Total gross revenue across all months"""
        return sum(getattr(self, f'month_{m}_gross', Decimal('0')) for m in range(1, 13))
    
    @property
    def total_net(self) -> Decimal:
        """Total net revenue across all months"""
        return sum(getattr(self, f'month_{m}_net', Decimal('0')) for m in range(1, 13))
    
    @property
    def total(self) -> Decimal:
        """Default total (gross revenue)"""
        return self.total_gross
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for template use"""
        result = {
            'customer_id': self.customer_id,
            'customer': self.customer,
            'ae': self.ae,
            'revenue_type': self.revenue_type,
            'sector': self.sector,
            'is_new_customer': self.is_new_customer,
            'total_gross': float(self.total_gross),
            'total_net': float(self.total_net),
            'total': float(self.total)
        }
        
        # Add monthly values
        for month in range(1, 13):
            result[f'month_{month}_gross'] = float(self.get_month_gross(month))
            result[f'month_{month}_net'] = float(self.get_month_net(month))
            result[f'month_{month}'] = float(self.get_month_gross(month))  # Default to gross
        
        return result

@dataclass
class AEPerformanceData:
    """AE performance data."""
    ae_name: str
    spot_count: int
    total_revenue: Decimal
    avg_rate: Decimal
    first_spot_date: Optional[date] = None
    last_spot_date: Optional[date] = None
    performance_pct: Optional[float] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        result = {
            'ae_name': self.ae_name,
            'spot_count': self.spot_count,
            'total_revenue': float(self.total_revenue),
            'avg_rate': float(self.avg_rate),
            'performance_pct': self.performance_pct
        }
        
        if self.first_spot_date:
            result['first_spot_date'] = self.first_spot_date.isoformat()
        if self.last_spot_date:
            result['last_spot_date'] = self.last_spot_date.isoformat()
            
        return result


@dataclass 
class QuarterlyData:
    """Quarterly performance data."""
    quarter: str
    year: int
    spot_count: int
    total_revenue: Decimal
    avg_rate: Decimal
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'quarter': self.quarter,
            'year': self.year,
            'spot_count': self.spot_count,
            'total_revenue': float(self.total_revenue),
            'avg_rate': float(self.avg_rate)
        }


@dataclass
class SectorData:
    """Sector performance data."""
    sector_name: str
    sector_code: str
    spot_count: int
    total_revenue: Decimal
    avg_rate: Decimal
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'sector_name': self.sector_name,
            'sector_code': self.sector_code,
            'spot_count': self.spot_count,
            'total_revenue': float(self.total_revenue),
            'avg_rate': float(self.avg_rate)
        }


@dataclass
class CustomerSectorData:
    """Customer data by sector."""
    sector_name: str
    customer_name: str 
    spot_count: int
    total_revenue: Decimal
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'sector_name': self.sector_name,
            'customer_name': self.customer_name,
            'spot_count': self.spot_count,
            'total_revenue': float(self.total_revenue)
        }


@dataclass
class MonthlyRevenueReportData:
    """Complete monthly revenue report data structure."""
    selected_year: int
    available_years: List[int]
    total_customers: int
    active_customers: int
    total_revenue: Decimal
    avg_monthly_revenue: Decimal
    revenue_data: List[CustomerMonthlyRow]
    ae_list: List[str]
    revenue_types: List[str]
    month_status: List[MonthStatus]
    filters: ReportFilters
    metadata: ReportMetadata
    new_customers: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for template consumption."""
        return {
            'selected_year': self.selected_year,
            'available_years': self.available_years,
            'total_customers': self.total_customers,
            'active_customers': self.active_customers,
            'new_customers': self.new_customers,
            'total_revenue': float(self.total_revenue),
            'avg_monthly_revenue': float(self.avg_monthly_revenue),
            'revenue_data': [row.to_dict() for row in self.revenue_data],
            'ae_list': self.ae_list,
            'revenue_types': self.revenue_types,
            'month_status': [status.to_dict() for status in self.month_status],
            'filters': self.filters.to_dict(),
            'metadata': self.metadata.to_dict()
        }


@dataclass
class AEPerformanceReportData:
    """AE performance report data structure."""
    ae_performance: List[AEPerformanceData]
    total_revenue: Decimal
    avg_performance_pct: Optional[float]
    top_performer: Optional[str]
    filters: ReportFilters
    metadata: ReportMetadata
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'ae_performance': [ae.to_dict() for ae in self.ae_performance],
            'total_revenue': float(self.total_revenue),
            'avg_performance_pct': self.avg_performance_pct,
            'top_performer': self.top_performer,
            'filters': self.filters.to_dict(),
            'metadata': self.metadata.to_dict()
        }


@dataclass
class QuarterlyPerformanceReportData:
    """Quarterly performance report data structure."""
    current_year: int
    quarterly_data: List[QuarterlyData]
    ae_performance: List[AEPerformanceData]
    total_revenue: Decimal
    filters: ReportFilters
    metadata: ReportMetadata
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'current_year': self.current_year,
            'quarterly_data': [q.to_dict() for q in self.quarterly_data],
            'ae_performance': [ae.to_dict() for ae in self.ae_performance],
            'total_revenue': float(self.total_revenue),
            'filters': self.filters.to_dict(),
            'metadata': self.metadata.to_dict()
        }


@dataclass
class SectorPerformanceReportData:
    """Sector performance report data structure."""
    sectors: List[SectorData]
    top_customers_by_sector: List[CustomerSectorData]
    total_revenue: Decimal
    sector_count: int
    filters: ReportFilters
    metadata: ReportMetadata
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'sectors': [s.to_dict() for s in self.sectors],
            'top_customers_by_sector': [c.to_dict() for c in self.top_customers_by_sector],
            'total_revenue': float(self.total_revenue),
            'sector_count': self.sector_count,
            'filters': self.filters.to_dict(),
            'metadata': self.metadata.to_dict()
        }


# Factory functions for creating models from database data
def create_customer_monthly_row_from_dict(data: Dict[str, Any]) -> CustomerMonthlyRow:
    """Create CustomerMonthlyRow from dictionary data."""
    row = CustomerMonthlyRow(
        customer_id=data['customer_id'],
        customer=data['customer'],
        ae=data['ae'], 
        revenue_type=data['revenue_type'],
        sector=data.get('sector')
    )
    
    # Set monthly values (assuming old format with only gross values)
    for month in range(1, 13):
        month_key = f'month_{month}'
        if month_key in data:
            gross_val = data[month_key]
            net_val = gross_val * Decimal('0.9')  # Approximate net as 90% of gross
            row.set_month_value(month, gross_val, net_val)
    
    return row


def create_month_status_from_closure_data(closures: List[Dict[str, Any]], year: int) -> List[MonthStatus]:
    """Create month status list from closure data."""
    closure_map = {c['broadcast_month']: c for c in closures}
    year_suffix = str(year)[-2:]  # Get last 2 digits
    
    month_status = []
    for month in range(1, 13):
        month_name = calendar.month_abbr[month]
        broadcast_month = f"{month_name}-{year_suffix}"
        
        if broadcast_month in closure_map:
            closure = closure_map[broadcast_month]
            status = MonthStatus(
                month_number=month,
                month_name=month_name,
                status='CLOSED',
                closed_date=datetime.strptime(closure['closed_date'], '%Y-%m-%d').date(),
                closed_by=closure['closed_by']
            )
        else:
            status = MonthStatus(
                month_number=month,
                month_name=month_name,
                status='OPEN'
            )
        
        month_status.append(status)
    
    return month_status