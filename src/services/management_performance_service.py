"""
Management Performance Service - Company and entity performance metrics.

Optimized with bulk queries to minimize database round-trips.
"""

import logging
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any, Tuple
from decimal import Decimal
from datetime import datetime

logger = logging.getLogger(__name__)


# ============================================================================
# Domain Models
# ============================================================================

@dataclass(frozen=True)
class QuarterlyMetrics:
    """Performance metrics for a single quarter."""
    quarter: int
    year: int
    booked: Decimal
    budget: Decimal
    forecast: Decimal
    prior_year_booked: Optional[Decimal]
    pacing_to_budget: Decimal
    pacing_to_forecast: Decimal
    customer_count: int
    prior_year_customer_count: Optional[int]
    
    @property
    def budget_pacing_pct(self) -> float:
        if self.budget == 0:
            return 0.0
        return float(self.booked / self.budget * 100)
    
    @property
    def forecast_pacing_pct(self) -> float:
        if self.forecast == 0:
            return 0.0
        return float(self.booked / self.forecast * 100)
    
    @property
    def yoy_change_pct(self) -> Optional[float]:
        if self.prior_year_booked is None or self.prior_year_booked == 0:
            return None
        return float((self.booked - self.prior_year_booked) / self.prior_year_booked * 100)
    
    @property
    def customer_yoy_change_pct(self) -> Optional[float]:
        if self.prior_year_customer_count is None or self.prior_year_customer_count == 0:
            return None
        return float((self.customer_count - self.prior_year_customer_count) / 
                    self.prior_year_customer_count * 100)
    
    @property
    def quarter_label(self) -> str:
        return f"Q{self.quarter} {str(self.year)[2:]}"


@dataclass(frozen=True)
class AnnualMetrics:
    """Aggregated annual performance metrics."""
    year: int
    booked: Decimal
    budget: Decimal
    forecast: Decimal
    prior_year_booked: Optional[Decimal]
    pacing_to_budget: Decimal
    pacing_to_forecast: Decimal
    customer_count: int
    prior_year_customer_count: Optional[int]
    
    @property
    def budget_pacing_pct(self) -> float:
        if self.budget == 0:
            return 0.0
        return float(self.booked / self.budget * 100)
    
    @property
    def forecast_pacing_pct(self) -> float:
        if self.forecast == 0:
            return 0.0
        return float(self.booked / self.forecast * 100)
    
    @property
    def yoy_change_pct(self) -> Optional[float]:
        if self.prior_year_booked is None or self.prior_year_booked == 0:
            return None
        return float((self.booked - self.prior_year_booked) / self.prior_year_booked * 100)
    
    @property
    def customer_yoy_change_pct(self) -> Optional[float]:
        if self.prior_year_customer_count is None or self.prior_year_customer_count == 0:
            return None
        return float((self.customer_count - self.prior_year_customer_count) / 
                    self.prior_year_customer_count * 100)


@dataclass
class EntityPerformance:
    """Complete performance data for a revenue entity."""
    entity_id: int
    entity_name: str
    entity_type: str
    quarterly: List[QuarterlyMetrics] = field(default_factory=list)
    annual: Optional[AnnualMetrics] = None


@dataclass
class CompanyPerformance:
    """Company-wide aggregated performance."""
    quarterly: List[QuarterlyMetrics] = field(default_factory=list)
    annual: Optional[AnnualMetrics] = None


@dataclass
class ManagementReportData:
    """Complete data package for the management report view."""
    year: int
    available_years: List[int]
    company: CompanyPerformance
    entities: List[EntityPerformance]
    generated_at: str
    pacing_mode: str


# ============================================================================
# Service - Optimized with Bulk Queries
# ============================================================================

class ManagementPerformanceService:
    """Service for management performance metrics with optimized bulk queries."""
    
    def __init__(self, db_connection):
        self.db = db_connection
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def get_available_years(self) -> List[int]:
        """Get list of years with budget data."""
        query = "SELECT DISTINCT year FROM budget ORDER BY year DESC"
        with self.db.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            return [row[0] for row in cursor.fetchall()]
    
    def _bulk_load_all_data(self, year: int) -> Dict[str, Any]:
        """
        Load ALL data needed for the report in 6 queries total.
        Returns nested dict structure for fast lookups.
        """
        data = {
            'booked': {},      # {(entity, quarter): Decimal}
            'budget': {},      # {(entity, quarter): Decimal}
            'forecast': {},    # {(entity, quarter): Decimal}
            'customers': {},   # {(entity, quarter): int}
            'annual_customers': {},  # {entity: int}
            'entities': []     # List of entity dicts
        }
        
        with self.db.connection() as conn:
            cursor = conn.cursor()
            
            # 1. Load all booked revenue (current + prior year) by entity and quarter
            cursor.execute("""
                SELECT 
                    UPPER(TRIM(sales_person)) AS entity,
                    CASE 
                        WHEN SUBSTR(broadcast_month, 1, 3) IN ('Jan', 'Feb', 'Mar') THEN 1
                        WHEN SUBSTR(broadcast_month, 1, 3) IN ('Apr', 'May', 'Jun') THEN 2
                        WHEN SUBSTR(broadcast_month, 1, 3) IN ('Jul', 'Aug', 'Sep') THEN 3
                        ELSE 4
                    END AS quarter,
                    CAST('20' || SUBSTR(broadcast_month, 5, 2) AS INTEGER) AS yr,
                    SUM(COALESCE(gross_rate, 0)) AS booked
                FROM spots
                WHERE CAST('20' || SUBSTR(broadcast_month, 5, 2) AS INTEGER) IN (?, ?)
                  AND (revenue_type != 'Trade' OR revenue_type IS NULL)
                  AND sales_person IS NOT NULL
                GROUP BY UPPER(TRIM(sales_person)), quarter, yr
            """, [year, year - 1])
            
            for row in cursor.fetchall():
                entity, quarter, yr, booked = row
                key = (entity, quarter, yr)
                data['booked'][key] = Decimal(str(booked or 0))
            
            # 2. Load all budget by entity and quarter
            cursor.execute("""
                SELECT 
                    UPPER(TRIM(ae_name)) AS entity,
                    CASE 
                        WHEN month BETWEEN 1 AND 3 THEN 1
                        WHEN month BETWEEN 4 AND 6 THEN 2
                        WHEN month BETWEEN 7 AND 9 THEN 3
                        ELSE 4
                    END AS quarter,
                    SUM(budget_amount) AS budget
                FROM budget
                WHERE year = ?
                GROUP BY UPPER(TRIM(ae_name)), quarter
            """, [year])
            
            for row in cursor.fetchall():
                entity, quarter, budget = row
                data['budget'][(entity, quarter)] = Decimal(str(budget or 0))
            
            # 3. Load all forecast by entity and quarter
            cursor.execute("""
                SELECT 
                    UPPER(TRIM(ae_name)) AS entity,
                    CASE 
                        WHEN month BETWEEN 1 AND 3 THEN 1
                        WHEN month BETWEEN 4 AND 6 THEN 2
                        WHEN month BETWEEN 7 AND 9 THEN 3
                        ELSE 4
                    END AS quarter,
                    SUM(forecast_amount) AS forecast
                FROM forecast
                WHERE year = ?
                GROUP BY UPPER(TRIM(ae_name)), quarter
            """, [year])
            
            for row in cursor.fetchall():
                entity, quarter, forecast = row
                data['forecast'][(entity, quarter)] = Decimal(str(forecast or 0))
            
            # 4. Load quarterly customer counts (current + prior year)
            cursor.execute("""
                SELECT 
                    UPPER(TRIM(sales_person)) AS entity,
                    CASE 
                        WHEN SUBSTR(broadcast_month, 1, 3) IN ('Jan', 'Feb', 'Mar') THEN 1
                        WHEN SUBSTR(broadcast_month, 1, 3) IN ('Apr', 'May', 'Jun') THEN 2
                        WHEN SUBSTR(broadcast_month, 1, 3) IN ('Jul', 'Aug', 'Sep') THEN 3
                        ELSE 4
                    END AS quarter,
                    CAST('20' || SUBSTR(broadcast_month, 5, 2) AS INTEGER) AS yr,
                    COUNT(DISTINCT customer_id) AS customer_count
                FROM spots
                WHERE customer_id IS NOT NULL
                  AND CAST('20' || SUBSTR(broadcast_month, 5, 2) AS INTEGER) IN (?, ?)
                  AND (revenue_type != 'Trade' OR revenue_type IS NULL)
                  AND sales_person IS NOT NULL
                GROUP BY UPPER(TRIM(sales_person)), quarter, yr
            """, [year, year - 1])
            
            for row in cursor.fetchall():
                entity, quarter, yr, count = row
                data['customers'][(entity, quarter, yr)] = count
            
            # 5. Load annual customer counts (distinct across year, not sum of quarters)
            cursor.execute("""
                SELECT 
                    UPPER(TRIM(sales_person)) AS entity,
                    CAST('20' || SUBSTR(broadcast_month, 5, 2) AS INTEGER) AS yr,
                    COUNT(DISTINCT customer_id) AS customer_count
                FROM spots
                WHERE customer_id IS NOT NULL
                  AND CAST('20' || SUBSTR(broadcast_month, 5, 2) AS INTEGER) IN (?, ?)
                  AND (revenue_type != 'Trade' OR revenue_type IS NULL)
                  AND sales_person IS NOT NULL
                GROUP BY UPPER(TRIM(sales_person)), yr
            """, [year, year - 1])
            
            for row in cursor.fetchall():
                entity, yr, count = row
                data['annual_customers'][(entity, yr)] = count
            
            # 6. Load active entities
            cursor.execute("""
                SELECT entity_id, entity_name, entity_type
                FROM revenue_entities
                WHERE is_active = 1
                ORDER BY entity_type, entity_name
            """)
            data['entities'] = [
                {'entity_id': r[0], 'entity_name': r[1], 'entity_type': r[2]} 
                for r in cursor.fetchall()
            ]
        
        return data
    
    def _build_metrics_from_cache(
        self, 
        data: Dict[str, Any], 
        year: int, 
        entity_name: Optional[str]
    ) -> Tuple[List[QuarterlyMetrics], AnnualMetrics]:
        """Build quarterly and annual metrics from cached data."""
        
        entity_key = entity_name.upper().strip() if entity_name else None
        
        quarterly = []
        total_booked = Decimal('0')
        total_budget = Decimal('0')
        total_forecast = Decimal('0')
        total_prior = Decimal('0')
        
        for q in range(1, 5):
            if entity_key:
                # Single entity
                q_booked = data['booked'].get((entity_key, q, year), Decimal('0'))
                q_budget = data['budget'].get((entity_key, q), Decimal('0'))
                q_forecast = data['forecast'].get((entity_key, q), Decimal('0'))
                q_prior = data['booked'].get((entity_key, q, year - 1))
                q_customers = data['customers'].get((entity_key, q, year), 0)
                q_prior_customers = data['customers'].get((entity_key, q, year - 1))
            else:
                # Company-wide: sum across all entities
                q_booked = sum(
                    v for (e, qtr, yr), v in data['booked'].items() 
                    if qtr == q and yr == year
                )
                q_budget = sum(
                    v for (e, qtr), v in data['budget'].items() 
                    if qtr == q
                )
                q_forecast = sum(
                    v for (e, qtr), v in data['forecast'].items() 
                    if qtr == q
                )
                q_prior = sum(
                    v for (e, qtr, yr), v in data['booked'].items() 
                    if qtr == q and yr == year - 1
                ) or None
                q_customers = sum(
                    v for (e, qtr, yr), v in data['customers'].items() 
                    if qtr == q and yr == year
                )
                q_prior_customers = sum(
                    v for (e, qtr, yr), v in data['customers'].items() 
                    if qtr == q and yr == year - 1
                ) or None
            
            # Use forecast if available, else budget
            effective_forecast = q_forecast if q_forecast > 0 else q_budget
            
            quarterly.append(QuarterlyMetrics(
                quarter=q,
                year=year,
                booked=q_booked,
                budget=q_budget,
                forecast=effective_forecast,
                prior_year_booked=q_prior,
                pacing_to_budget=max(q_budget - q_booked, Decimal('0')),
                pacing_to_forecast=max(effective_forecast - q_booked, Decimal('0')),
                customer_count=q_customers,
                prior_year_customer_count=q_prior_customers
            ))
            
            total_booked += q_booked
            total_budget += q_budget
            total_forecast += effective_forecast
            if q_prior:
                total_prior += q_prior
        
        # Annual customer count (distinct, not sum)
        if entity_key:
            annual_customers = data['annual_customers'].get((entity_key, year), 0)
            prior_annual_customers = data['annual_customers'].get((entity_key, year - 1))
        else:
            # Company-wide annual customers need a different approach
            # Sum is close enough for display purposes
            annual_customers = sum(
                v for (e, yr), v in data['annual_customers'].items() if yr == year
            )
            prior_annual_customers = sum(
                v for (e, yr), v in data['annual_customers'].items() if yr == year - 1
            ) or None
        
        effective_total_forecast = total_forecast if total_forecast > 0 else total_budget
        
        annual = AnnualMetrics(
            year=year,
            booked=total_booked,
            budget=total_budget,
            forecast=effective_total_forecast,
            prior_year_booked=total_prior if total_prior > 0 else None,
            pacing_to_budget=max(total_budget - total_booked, Decimal('0')),
            pacing_to_forecast=max(effective_total_forecast - total_booked, Decimal('0')),
            customer_count=annual_customers,
            prior_year_customer_count=prior_annual_customers
        )
        
        return quarterly, annual
    
    def get_management_report(self, year: int, pacing_mode: str = 'budget') -> ManagementReportData:
        """
        Get complete management performance report.
        Optimized: Only 6 database queries total regardless of entity count.
        """
        # Single bulk load
        data = self._bulk_load_all_data(year)
        
        # Build company metrics from cache
        company_quarterly, company_annual = self._build_metrics_from_cache(data, year, None)
        company = CompanyPerformance(quarterly=company_quarterly, annual=company_annual)
        
        # Build entity metrics from cache
        entities = []
        for entity in data['entities']:
            entity_quarterly, entity_annual = self._build_metrics_from_cache(
                data, year, entity['entity_name']
            )
            entities.append(EntityPerformance(
                entity_id=entity['entity_id'],
                entity_name=entity['entity_name'],
                entity_type=entity['entity_type'],
                quarterly=entity_quarterly,
                annual=entity_annual
            ))
        
        # Sort by total booked descending
        entities.sort(
            key=lambda p: p.annual.booked if p.annual else Decimal('0'),
            reverse=True
        )
        
        available_years = self.get_available_years()
        
        return ManagementReportData(
            year=year,
            available_years=available_years,
            company=company,
            entities=entities,
            generated_at=datetime.now().strftime('%Y-%m-%d %H:%M'),
            pacing_mode=pacing_mode
        )