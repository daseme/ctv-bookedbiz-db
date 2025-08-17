# src/models/report_data.py
"""
Data models for report generation.
Provides structured data transfer objects for all report types.
"""
from typing import List, Dict, Any, Optional, Union
from datetime import date, datetime
from decimal import Decimal
from dataclasses import dataclass, asdict
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


class CustomerMonthlyRow:
    """Customer monthly revenue row with flexible month access."""
    
    def __init__(self, customer_id: int, customer: str, ae: str, 
                 revenue_type: str, sector: Optional[str] = None):
        self.customer_id = customer_id
        self.customer = customer
        self.ae = ae
        self.revenue_type = revenue_type
        self.sector = sector
        
        # Initialize all months to 0 for both gross and net
        self._months_gross = {f'month_{i}': Decimal('0') for i in range(1, 13)}
        self._months_net = {f'month_{i}': Decimal('0') for i in range(1, 13)}
    
    def set_month_value(self, month: int, gross_value: Union[Decimal, float, int], 
                       net_value: Union[Decimal, float, int]):
        """Set both gross and net values for specific month."""
        if 1 <= month <= 12:
            self._months_gross[f'month_{month}'] = Decimal(str(gross_value))
            self._months_net[f'month_{month}'] = Decimal(str(net_value))
    
    def get_month_value(self, month: int, revenue_field: str = 'gross') -> Decimal:
        """Get value for specific month (gross or net)."""
        if 1 <= month <= 12:
            if revenue_field == 'net':
                return self._months_net[f'month_{month}']
            return self._months_gross[f'month_{month}']
        return Decimal('0')
    
    def get_total(self, revenue_field: str = 'gross') -> Decimal:
        """Calculate total across all months for specified revenue type."""
        if revenue_field == 'net':
            return sum(self._months_net.values())
        return sum(self._months_gross.values())
    
    @property
    def total(self) -> Decimal:
        """Calculate total gross revenue (for backward compatibility)."""
        return self.get_total('gross')
    
    @property 
    def total_gross(self) -> Decimal:
        """Total gross revenue."""
        return self.get_total('gross')
    
    @property
    def total_net(self) -> Decimal:
        """Total net revenue."""
        return self.get_total('net')
    
    # Keep existing month properties for backward compatibility
    @property 
    def month_1(self) -> Decimal:
        return self._months_gross['month_1']
    # ... (keep all the existing month_2 through month_12 properties)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = {
            'customer_id': self.customer_id,
            'customer': self.customer,
            'ae': self.ae,
            'revenue_type': self.revenue_type,
            'sector': self.sector,
            'total': float(self.total),
            'total_gross': float(self.total_gross),
            'total_net': float(self.total_net)
        }
        
        # Add monthly values as floats for JSON compatibility
        for month in range(1, 13):
            result[f'month_{month}'] = float(self.get_month_value(month, 'gross'))
            result[f'month_{month}_gross'] = float(self.get_month_value(month, 'gross'))
            result[f'month_{month}_net'] = float(self.get_month_value(month, 'net'))
        
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
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for template consumption."""
        return {
            'selected_year': self.selected_year,
            'available_years': self.available_years,
            'total_customers': self.total_customers,
            'active_customers': self.active_customers,
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