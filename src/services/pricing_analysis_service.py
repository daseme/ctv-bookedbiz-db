# src/services/pricing_analysis_service.py
"""
Pricing analysis service for spot rate analysis across multiple dimensions.
Uses broadcast_month and month_closures for accurate period comparisons.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Tuple, Any
import logging

logger = logging.getLogger(__name__)


# Month abbreviation mappings
MONTH_ABBREV = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
MONTH_TO_NUM = {abbr: f"{i+1:02d}" for i, abbr in enumerate(MONTH_ABBREV)}
NUM_TO_MONTH = {f"{i+1:02d}": abbr for i, abbr in enumerate(MONTH_ABBREV)}


def broadcast_month_to_iso(bm: str) -> str:
    """Convert 'Jan-25' to '2025-01' for sorting."""
    if not bm or len(bm) != 6:
        return ''
    month_abbr = bm[:3]
    year_suffix = bm[4:6]
    month_num = MONTH_TO_NUM.get(month_abbr, '00')
    return f"20{year_suffix}-{month_num}"


def iso_to_broadcast_month(iso: str) -> str:
    """Convert '2025-01' to 'Jan-25'."""
    if not iso or len(iso) != 7:
        return ''
    year = iso[2:4]
    month_num = iso[5:7]
    month_abbr = NUM_TO_MONTH.get(month_num, 'Jan')
    return f"{month_abbr}-{year}"


def get_prior_year_month(bm: str) -> str:
    """Convert 'Jan-25' to 'Jan-24'."""
    if not bm or len(bm) != 6:
        return ''
    month_abbr = bm[:3]
    year_suffix = int(bm[4:6])
    prior_year = year_suffix - 1
    return f"{month_abbr}-{prior_year:02d}"


def get_months_in_range(start_bm: str, end_bm: str, closed_months: List[str]) -> List[str]:
    """Get all closed months between start and end (inclusive)."""
    start_iso = broadcast_month_to_iso(start_bm)
    end_iso = broadcast_month_to_iso(end_bm)
    
    result = []
    for m in closed_months:
        m_iso = broadcast_month_to_iso(m)
        if start_iso <= m_iso <= end_iso:
            result.append(m)
    
    # Sort chronologically
    result.sort(key=broadcast_month_to_iso)
    return result


@dataclass
class PricingSummary:
    """Aggregated pricing metrics for a dimension value."""
    dimension_value: str
    avg_rate: float
    spot_count: int
    total_revenue: float
    prior_avg_rate: Optional[float] = None
    prior_spot_count: Optional[int] = None
    prior_total_revenue: Optional[float] = None

    @property
    def yoy_rate_change(self) -> Optional[float]:
        if self.prior_avg_rate and self.prior_avg_rate > 0:
            return ((self.avg_rate - self.prior_avg_rate) / self.prior_avg_rate) * 100
        return None

    @property
    def yoy_volume_change(self) -> Optional[float]:
        if self.prior_spot_count and self.prior_spot_count > 0:
            return ((self.spot_count - self.prior_spot_count) / self.prior_spot_count) * 100
        return None


@dataclass
class MonthlyTrend:
    """Monthly pricing trend data point."""
    broadcast_month: str
    avg_rate: float
    spot_count: int
    total_revenue: float
    sort_key: str = ""
    
    def __post_init__(self):
        self.sort_key = broadcast_month_to_iso(self.broadcast_month)


@dataclass
class QuarterlySummary:
    """Quarterly pricing summary."""
    year_quarter: str
    avg_rate: float
    spot_count: int
    total_revenue: float
    prior_avg_rate: Optional[float] = None

    @property
    def yoy_rate_change(self) -> Optional[float]:
        if self.prior_avg_rate and self.prior_avg_rate > 0:
            return ((self.avg_rate - self.prior_avg_rate) / self.prior_avg_rate) * 100
        return None


@dataclass
class PeriodContext:
    """Context for the analysis period."""
    months: List[str]  # ['Jan-25', 'Feb-25', ...]
    prior_months: List[str]  # ['Jan-24', 'Feb-24', ...]
    display_range: str  # "Jan-25 to Dec-25"
    is_closed: bool = True  # All months are closed
    
    @property
    def months_sql_list(self) -> str:
        """Generate SQL IN clause values."""
        return ', '.join(f"'{m}'" for m in self.months)
    
    @property
    def prior_months_sql_list(self) -> str:
        """Generate SQL IN clause values for prior year."""
        return ', '.join(f"'{m}'" for m in self.prior_months)
    
    @property
    def start_month(self) -> Optional[str]:
        """First month in the period."""
        return self.months[0] if self.months else None
    
    @property
    def end_month(self) -> Optional[str]:
        """Last month in the period."""
        return self.months[-1] if self.months else None


@dataclass
class PeriodPreset:
    """A preset period selection."""
    from_month: str
    to_month: str


@dataclass
class AvailablePeriods:
    """Available periods and presets for UI."""
    closed_months: List[str]  # All closed months, chronological (oldest first)
    presets: Dict[str, Optional[PeriodPreset]]
    default: PeriodPreset
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for template/JSON."""
        return {
            "closed_months": self.closed_months,
            "presets": {
                k: {"from": v.from_month, "to": v.to_month} if v else None
                for k, v in self.presets.items()
            },
            "default": {
                "from": self.default.from_month,
                "to": self.default.to_month
            }
        }


@dataclass
class DrilldownContext:
    """Context for drill-down navigation."""
    entry_point: str
    current_level: int
    path: List[str]
    filters: Dict[str, str] = field(default_factory=dict)

    @property
    def breadcrumbs(self) -> List[Dict[str, str]]:
        crumbs = [{"label": "All", "dimension": self.entry_point, "value": None}]
        for dim, val in self.filters.items():
            crumbs.append({"label": val, "dimension": dim, "value": val})
        return crumbs

    @property
    def next_dimension(self) -> Optional[str]:
        if self.current_level < len(self.path) - 1:
            return self.path[self.current_level + 1]
        return None

    @property
    def is_terminal(self) -> bool:
        return self.current_level >= len(self.path) - 1


# Drill-down hierarchy definitions
DRILL_PATHS = {
    'ae': ['ae', 'market', 'sector', 'customer', 'monthly'],
    'market': ['market', 'ae', 'sector', 'customer', 'monthly'],
    'sector': ['sector', 'customer', 'ae', 'market', 'monthly'],
    'language': ['language', 'day_part', 'market', 'ae', 'monthly'],
    'day_part': ['day_part', 'language', 'market', 'ae', 'monthly'],
    'day_type': ['day_type', 'day_part', 'market', 'ae', 'monthly'],
    'tenure': ['tenure', 'sector', 'ae', 'customer', 'monthly'],
}

# Map dimension names to SQL columns
DIMENSION_COLUMNS = {
    'ae': 'sales_person',
    'market': 'market_name',
    'sector': 'sector_name',
    'customer': 'customer_name',
    'language': 'language_code',
    'day_part': 'day_part',
    'day_type': 'day_type',
    'tenure': 'tenure_cohort',
    'monthly': 'broadcast_month',
}

# Display names for dimensions
DIMENSION_LABELS = {
    'ae': 'Account Executive',
    'market': 'Market',
    'sector': 'Sector',
    'customer': 'Customer',
    'language': 'Language',
    'day_part': 'Day Part',
    'day_type': 'Day Type',
    'tenure': 'Customer Tenure',
    'monthly': 'Month',
}


class PricingAnalysisService:
    """Service for pricing analysis queries using broadcast_month periods."""

    def __init__(self, db_connection):
        self.db = db_connection
        logger.info("PricingAnalysisService initialized")

    def get_closed_months(self, limit: int = 100) -> List[str]:
        """Get list of closed months, sorted chronologically (oldest first)."""
        query = """
            SELECT broadcast_month
            FROM month_closures
            ORDER BY 
                '20' || SUBSTR(broadcast_month, 5, 2) || '-' ||
                CASE SUBSTR(broadcast_month, 1, 3)
                    WHEN 'Jan' THEN '01' WHEN 'Feb' THEN '02' WHEN 'Mar' THEN '03'
                    WHEN 'Apr' THEN '04' WHEN 'May' THEN '05' WHEN 'Jun' THEN '06'
                    WHEN 'Jul' THEN '07' WHEN 'Aug' THEN '08' WHEN 'Sep' THEN '09'
                    WHEN 'Oct' THEN '10' WHEN 'Nov' THEN '11' WHEN 'Dec' THEN '12'
                END ASC
            LIMIT ?
        """
        with self.db.connection() as conn:
            cursor = conn.execute(query, [limit])
            return [row[0] for row in cursor.fetchall()]

    def get_available_periods(self) -> AvailablePeriods:
        """
        Returns available months and computed presets.
        Closed months are sorted chronologically (oldest first).
        """
        closed_months = self.get_closed_months(100)
        
        if not closed_months:
            # No closed months - return empty
            return AvailablePeriods(
                closed_months=[],
                presets={
                    "trailing_12": None,
                    "ytd": None,
                    "prior_year": None,
                    "last_quarter": None
                },
                default=PeriodPreset("", "")
            )
        
        # Most recent is last (chronological order)
        most_recent = closed_months[-1]
        most_recent_iso = broadcast_month_to_iso(most_recent)
        current_year = most_recent_iso[:4]
        prior_year = str(int(current_year) - 1)
        
        presets: Dict[str, Optional[PeriodPreset]] = {}
        
        # Trailing 12: most recent 12 closed months
        trailing_12_months = closed_months[-12:] if len(closed_months) >= 12 else closed_months
        if trailing_12_months:
            presets["trailing_12"] = PeriodPreset(
                trailing_12_months[0],
                trailing_12_months[-1]
            )
        else:
            presets["trailing_12"] = None
        
        # YTD: Jan of current year to most recent closed month of current year
        current_year_months = [
            m for m in closed_months 
            if broadcast_month_to_iso(m).startswith(current_year)
        ]
        if current_year_months:
            presets["ytd"] = PeriodPreset(
                current_year_months[0],
                current_year_months[-1]
            )
        else:
            presets["ytd"] = None
        
        # Prior Year: All 12 months of prior year (if all closed)
        prior_year_months = [
            m for m in closed_months 
            if broadcast_month_to_iso(m).startswith(prior_year)
        ]
        if len(prior_year_months) == 12:
            presets["prior_year"] = PeriodPreset(
                prior_year_months[0],
                prior_year_months[-1]
            )
        else:
            presets["prior_year"] = None
        
        # Last Quarter: most recent fully-closed quarter
        presets["last_quarter"] = self._compute_last_quarter_preset(closed_months)
        
        # Default: trailing 12 (or all available)
        default = presets["trailing_12"] or PeriodPreset(
            closed_months[0], 
            closed_months[-1]
        )
        
        return AvailablePeriods(
            closed_months=closed_months,
            presets=presets,
            default=default
        )
    
    def _compute_last_quarter_preset(
        self, 
        closed_months: List[str]
    ) -> Optional[PeriodPreset]:
        """Find most recent fully-closed quarter."""
        if not closed_months:
            return None
        
        # Quarter definitions (month numbers)
        quarters = {
            'Q1': ['01', '02', '03'],
            'Q2': ['04', '05', '06'],
            'Q3': ['07', '08', '09'],
            'Q4': ['10', '11', '12']
        }
        
        # Convert to ISO for easier comparison
        closed_iso = set(broadcast_month_to_iso(m) for m in closed_months)
        
        # Get most recent month to determine search range
        most_recent_iso = broadcast_month_to_iso(closed_months[-1])
        most_recent_year = int(most_recent_iso[:4])
        most_recent_month = int(most_recent_iso[5:7])
        
        # Check quarters in reverse order (most recent first)
        for year in range(most_recent_year, most_recent_year - 3, -1):
            for q_name in ['Q4', 'Q3', 'Q2', 'Q1']:
                q_months = quarters[q_name]
                year_str = str(year)
                
                # Build ISO months for this quarter
                q_iso_months = [f"{year_str}-{m}" for m in q_months]
                
                # Skip if quarter is in the future
                if q_iso_months[-1] > most_recent_iso:
                    continue
                
                # Check if all 3 months are closed
                if all(m in closed_iso for m in q_iso_months):
                    return PeriodPreset(
                        iso_to_broadcast_month(q_iso_months[0]),
                        iso_to_broadcast_month(q_iso_months[-1])
                    )
        
        return None

    def get_period_from_params(
        self,
        from_month: Optional[str],
        to_month: Optional[str]
    ) -> PeriodContext:
        """
        Build PeriodContext from URL params with validation.
        Falls back to trailing 12 if params invalid.
        """
        available = self.get_available_periods()
        closed_set = set(available.closed_months)
        
        # Validate from_month
        valid_from = from_month if from_month in closed_set else None
        # Validate to_month  
        valid_to = to_month if to_month in closed_set else None
        
        # Check chronological order
        if valid_from and valid_to:
            from_iso = broadcast_month_to_iso(valid_from)
            to_iso = broadcast_month_to_iso(valid_to)
            if from_iso > to_iso:
                logger.warning(
                    f"Invalid period range: {from_month} > {to_month}, using default"
                )
                valid_from = None
                valid_to = None
        
        # Apply defaults for missing values
        if not valid_from and not valid_to:
            # Use default (trailing 12)
            valid_from = available.default.from_month
            valid_to = available.default.to_month
        elif not valid_from:
            # Only to provided - use default from
            valid_from = available.default.from_month
        elif not valid_to:
            # Only from provided - use most recent closed
            valid_to = available.closed_months[-1] if available.closed_months else valid_from
        
        # Build list of months in range
        months = get_months_in_range(valid_from, valid_to, available.closed_months)
        prior_months = [get_prior_year_month(m) for m in months]
        
        display_range = f"{valid_from} to {valid_to}" if months else "No closed months"
        
        return PeriodContext(
            months=months,
            prior_months=prior_months,
            display_range=display_range,
            is_closed=True
        )

    def get_trailing_12_closed_months(self) -> PeriodContext:
        """Get the trailing 12 closed months with prior year equivalents."""
        return self.get_period_from_params(None, None)

    def _build_month_filter(self, months: List[str]) -> Tuple[str, List[str]]:
        """Build WHERE clause for broadcast_month IN (...)."""
        if not months:
            return "1=0", []  # No months = no results
        
        placeholders = ', '.join('?' for _ in months)
        return f"broadcast_month IN ({placeholders})", list(months)

    def _build_filter_clause(
        self,
        filters: Optional[Dict[str, str]],
        exclude_dimension: Optional[str] = None
    ) -> Tuple[List[str], List[str]]:
        """Build WHERE clause components from filters."""
        clauses = []
        params = []

        if filters:
            for key, value in filters.items():
                if value and key in DIMENSION_COLUMNS and key != exclude_dimension:
                    clauses.append(f"{DIMENSION_COLUMNS[key]} = ?")
                    params.append(value)

        return clauses, params

    def get_monthly_trend(
        self,
        period: PeriodContext,
        filters: Optional[Dict[str, str]] = None
    ) -> List[MonthlyTrend]:
        """Get monthly pricing trend for the period."""
        month_clause, month_params = self._build_month_filter(period.months)
        
        where_clauses = [month_clause]
        params = month_params.copy()

        filter_clauses, filter_params = self._build_filter_clause(filters)
        where_clauses.extend(filter_clauses)
        params.extend(filter_params)

        query = f"""
            SELECT
                broadcast_month,
                ROUND(AVG(gross_rate), 2) as avg_rate,
                COUNT(*) as spot_count,
                ROUND(SUM(gross_rate), 2) as total_revenue
            FROM v_pricing_analysis
            WHERE {' AND '.join(where_clauses)}
            GROUP BY broadcast_month
        """

        with self.db.connection() as conn:
            cursor = conn.execute(query, params)
            results = [
                MonthlyTrend(
                    broadcast_month=row[0],
                    avg_rate=row[1] or 0,
                    spot_count=row[2] or 0,
                    total_revenue=row[3] or 0
                )
                for row in cursor.fetchall()
            ]
        
        # Sort by ISO date (chronological)
        results.sort(key=lambda x: x.sort_key)
        return results

    def get_quarterly_summary(
        self,
        period: PeriodContext,
        filters: Optional[Dict[str, str]] = None
    ) -> List[QuarterlySummary]:
        """Get quarterly summary with YoY comparison."""
        month_clause, month_params = self._build_month_filter(period.months)
        prior_month_clause, prior_month_params = self._build_month_filter(period.prior_months)

        filter_clauses, filter_params = self._build_filter_clause(filters)

        # Current period by quarter - filter out NULL year_quarter from bad air_date data
        where_clauses = [month_clause, "year_quarter IS NOT NULL"] + filter_clauses
        params = month_params + filter_params

        query = f"""
            SELECT
                year_quarter,
                ROUND(AVG(gross_rate), 2) as avg_rate,
                COUNT(*) as spot_count,
                ROUND(SUM(gross_rate), 2) as total_revenue
            FROM v_pricing_analysis
            WHERE {' AND '.join(where_clauses)}
            GROUP BY year_quarter
            ORDER BY year_quarter
        """

        with self.db.connection() as conn:
            cursor = conn.execute(query, params)
            current = {row[0]: row for row in cursor.fetchall()}

            # Prior period - also filter out NULL year_quarter
            prior_where = [prior_month_clause, "year_quarter IS NOT NULL"] + filter_clauses
            prior_params = prior_month_params + filter_params
            prior_query = f"""
                SELECT
                    year_quarter,
                    ROUND(AVG(gross_rate), 2) as avg_rate
                FROM v_pricing_analysis
                WHERE {' AND '.join(prior_where)}
                GROUP BY year_quarter
            """
            cursor = conn.execute(prior_query, prior_params)
            # Map prior year quarters to current year (2024-Q1 -> 2025-Q1)
            prior = {}
            for row in cursor.fetchall():
                yq = row[0]  # e.g., "2024-Q1"
                if yq and '-' in yq:
                    parts = yq.split('-')
                    current_yq = f"{int(parts[0]) + 1}-{parts[1]}"
                    prior[current_yq] = row[1]

        results = []
        for yq in sorted(current.keys()):
            row = current[yq]
            results.append(QuarterlySummary(
                year_quarter=yq,
                avg_rate=row[1] or 0,
                spot_count=row[2] or 0,
                total_revenue=row[3] or 0,
                prior_avg_rate=prior.get(yq)
            ))

        return results

    def get_dimension_summary(
        self,
        dimension: str,
        period: PeriodContext,
        filters: Optional[Dict[str, str]] = None,
        min_spots: int = 10
    ) -> List[PricingSummary]:
        """Get pricing summary grouped by a dimension."""
        if dimension not in DIMENSION_COLUMNS:
            raise ValueError(f"Invalid dimension: {dimension}")

        column = DIMENSION_COLUMNS[dimension]
        
        month_clause, month_params = self._build_month_filter(period.months)
        prior_month_clause, prior_month_params = self._build_month_filter(period.prior_months)

        filter_clauses, filter_params = self._build_filter_clause(filters, exclude_dimension=dimension)

        where_clauses = [month_clause, f"{column} IS NOT NULL"] + filter_clauses
        params = month_params + filter_params

        query = f"""
            SELECT
                {column} as dimension_value,
                ROUND(AVG(gross_rate), 2) as avg_rate,
                COUNT(*) as spot_count,
                ROUND(SUM(gross_rate), 2) as total_revenue
            FROM v_pricing_analysis
            WHERE {' AND '.join(where_clauses)}
            GROUP BY {column}
            HAVING COUNT(*) >= ?
            ORDER BY avg_rate DESC
        """

        with self.db.connection() as conn:
            cursor = conn.execute(query, params + [min_spots])
            current = {row[0]: row for row in cursor.fetchall()}

            # Prior period
            prior_where = [prior_month_clause, f"{column} IS NOT NULL"] + filter_clauses
            prior_params_full = prior_month_params + filter_params + [min_spots]
            prior_query = f"""
                SELECT
                    {column} as dimension_value,
                    ROUND(AVG(gross_rate), 2) as avg_rate,
                    COUNT(*) as spot_count,
                    ROUND(SUM(gross_rate), 2) as total_revenue
                FROM v_pricing_analysis
                WHERE {' AND '.join(prior_where)}
                GROUP BY {column}
                HAVING COUNT(*) >= ?
            """
            cursor = conn.execute(prior_query, prior_params_full)
            prior = {row[0]: row for row in cursor.fetchall()}

        results = []
        for dim_val, row in current.items():
            prior_row = prior.get(dim_val)
            results.append(PricingSummary(
                dimension_value=dim_val,
                avg_rate=row[1] or 0,
                spot_count=row[2] or 0,
                total_revenue=row[3] or 0,
                prior_avg_rate=prior_row[1] if prior_row else None,
                prior_spot_count=prior_row[2] if prior_row else None,
                prior_total_revenue=prior_row[3] if prior_row else None
            ))

        results.sort(key=lambda x: x.avg_rate, reverse=True)
        return results

    def get_overall_summary(
        self,
        period: PeriodContext,
        filters: Optional[Dict[str, str]] = None
    ) -> PricingSummary:
        """Get overall pricing summary for the period."""
        month_clause, month_params = self._build_month_filter(period.months)
        prior_month_clause, prior_month_params = self._build_month_filter(period.prior_months)

        filter_clauses, filter_params = self._build_filter_clause(filters)

        where_clauses = [month_clause] + filter_clauses
        params = month_params + filter_params

        query = f"""
            SELECT
                ROUND(AVG(gross_rate), 2) as avg_rate,
                COUNT(*) as spot_count,
                ROUND(SUM(gross_rate), 2) as total_revenue
            FROM v_pricing_analysis
            WHERE {' AND '.join(where_clauses)}
        """

        with self.db.connection() as conn:
            cursor = conn.execute(query, params)
            current = cursor.fetchone()

            prior_where = [prior_month_clause] + filter_clauses
            prior_params_full = prior_month_params + filter_params
            prior_query = f"""
                SELECT
                    ROUND(AVG(gross_rate), 2) as avg_rate,
                    COUNT(*) as spot_count,
                    ROUND(SUM(gross_rate), 2) as total_revenue
                FROM v_pricing_analysis
                WHERE {' AND '.join(prior_where)}
            """
            cursor = conn.execute(prior_query, prior_params_full)
            prior = cursor.fetchone()

        return PricingSummary(
            dimension_value='Overall',
            avg_rate=current[0] or 0 if current else 0,
            spot_count=current[1] or 0 if current else 0,
            total_revenue=current[2] or 0 if current else 0,
            prior_avg_rate=prior[0] if prior else None,
            prior_spot_count=prior[1] if prior else None,
            prior_total_revenue=prior[2] if prior else None
        )

    def get_filter_options(self) -> Dict[str, List[str]]:
        """Get available filter options."""
        options = {}

        # Static options for computed dimensions
        options['day_part'] = [
            'Early Morning', 'Daytime', 'Early Fringe', 
            'Prime Access', 'Prime', 'Late Fringe', 'Overnight'
        ]
        options['day_type'] = ['Weekday', 'Weekend']

        with self.db.connection() as conn:
            # Markets
            cursor = conn.execute("""
                SELECT DISTINCT market_name 
                FROM markets 
                WHERE is_active = 1 AND market_name IS NOT NULL 
                ORDER BY market_name
            """)
            options['market'] = [row[0] for row in cursor.fetchall()]
            
            # Languages - from the view for active languages
            cursor = conn.execute("""
                SELECT DISTINCT language_code 
                FROM v_pricing_analysis 
                WHERE language_code IS NOT NULL 
                ORDER BY language_code
                LIMIT 50
            """)
            options['language'] = [row[0] for row in cursor.fetchall()]
            
            # Sectors
            cursor = conn.execute("""
                SELECT DISTINCT sector_name 
                FROM sectors 
                WHERE is_active = 1 AND sector_name IS NOT NULL 
                ORDER BY sector_name
            """)
            options['sector'] = [row[0] for row in cursor.fetchall()]

        return options

    def create_drilldown_context(
        self,
        entry_point: str,
        filters: Optional[Dict[str, str]] = None
    ) -> DrilldownContext:
        """Create drill-down navigation context."""
        if entry_point not in DRILL_PATHS:
            raise ValueError(f"Invalid entry point: {entry_point}")

        path = DRILL_PATHS[entry_point]
        filters = filters or {}

        current_level = 0
        for i, dim in enumerate(path[:-1]):
            if dim in filters:
                current_level = i + 1

        return DrilldownContext(
            entry_point=entry_point,
            current_level=current_level,
            path=path,
            filters=filters
        )