#!/usr/bin/env python3
"""
Updated ReportDataService with New Customer Detection

Added functionality to identify customers who are new this year (never appeared in previous years)
"""

import logging
import time
from typing import List, Dict, Any, Optional, Tuple, Protocol, Set
from datetime import datetime, date
from decimal import Decimal
from dataclasses import dataclass
from enum import Enum

# ============================================================================
# Domain Models (updated with new customer tracking)
# ============================================================================

@dataclass
class YearRange:
    """Value object representing a year range for filtering"""
    year: int
    suffix: str
    
    @classmethod
    def from_year(cls, year: int) -> 'YearRange':
        return cls(year=year, suffix=str(year)[-2:])
    
    @property
    def like_pattern(self) -> str:
        return f"%-{self.suffix}"

class OrderingStrategy(Enum):
    """Enumeration of available ordering strategies for reports"""
    AE_THEN_CUSTOMER = ("ae_customer", "AE then Customer (Alphabetical)")
    REVENUE_DESC = ("revenue_desc", "Revenue (Highest to Lowest)")
    CUSTOMER_ALPHA = ("customer_alpha", "Customer (Alphabetical)")
    
    def __init__(self, key: str, display_name: str):
        self.key = key
        self.display_name = display_name

@dataclass
class AEFilter:
    """Value object for AE filtering logic"""
    ae_value: Optional[str]
    
    @classmethod
    def from_input(cls, ae_filter: Optional[str]) -> 'AEFilter':
        if not ae_filter or ae_filter.strip().lower() == "all":
            return cls(ae_value=None)
        return cls(ae_value=ae_filter.strip())
    
    def is_unknown_filter(self) -> bool:
        return self.ae_value and self.ae_value.lower() == "unknown"
    
    def build_where_clause(self, column: str = "s.sales_person") -> Tuple[str, List[Any]]:
        """Build SQL predicate and parameters for AE filtering"""
        if not self.ae_value:
            return "", []
            
        if column == "ae_key":  # already normalized
            if self.is_unknown_filter():
                return "ae_key = 'UNKNOWN'", []
            return "ae_key = UPPER(TRIM(?))", [self.ae_value]
        
        # Raw column filtering
        if self.is_unknown_filter():
            return f"( {column} IS NULL OR TRIM({column}) = '' )", []
        return f"UPPER(TRIM({column})) = UPPER(TRIM(?))", [self.ae_value]

# ============================================================================
# Query Builder (enhanced with new customer queries)
# ============================================================================

class RevenueQueryBuilder:
    """Utility class for building reusable SQL query components"""
    
    @staticmethod
    def build_broadcast_month_case(expr: str = "s.broadcast_month") -> str:
        """Build CASE statement to extract month number from broadcast_month"""
        return f"""
            CASE 
                WHEN {expr} LIKE 'Jan-%' THEN '01'
                WHEN {expr} LIKE 'Feb-%' THEN '02'
                WHEN {expr} LIKE 'Mar-%' THEN '03'
                WHEN {expr} LIKE 'Apr-%' THEN '04'
                WHEN {expr} LIKE 'May-%' THEN '05'
                WHEN {expr} LIKE 'Jun-%' THEN '06'
                WHEN {expr} LIKE 'Jul-%' THEN '07'
                WHEN {expr} LIKE 'Aug-%' THEN '08'
                WHEN {expr} LIKE 'Sep-%' THEN '09'
                WHEN {expr} LIKE 'Oct-%' THEN '10'
                WHEN {expr} LIKE 'Nov-%' THEN '11'
                WHEN {expr} LIKE 'Dec-%' THEN '12'
            END
        """.strip()
    
    @staticmethod
    def build_year_case(expr: str = "broadcast_month") -> str:
        """Build CASE statement to extract year from broadcast_month"""
        return f"""
            CASE 
                WHEN {expr} LIKE '%-21' THEN 2021
                WHEN {expr} LIKE '%-22' THEN 2022
                WHEN {expr} LIKE '%-23' THEN 2023
                WHEN {expr} LIKE '%-24' THEN 2024
                WHEN {expr} LIKE '%-25' THEN 2025
                WHEN {expr} LIKE '%-26' THEN 2026
                WHEN {expr} LIKE '%-27' THEN 2027
                WHEN {expr} LIKE '%-28' THEN 2028
                WHEN {expr} LIKE '%-29' THEN 2029
                WHEN {expr} LIKE '%-30' THEN 2030
            END
        """.strip()
    
    @staticmethod
    def build_quarter_case(expr: str = "s.broadcast_month") -> str:
        """Build CASE statement to extract quarter from broadcast_month"""
        return f"""
            CASE 
              WHEN {expr} LIKE 'Jan-%' OR {expr} LIKE 'Feb-%' OR {expr} LIKE 'Mar-%' THEN 'Q1'
              WHEN {expr} LIKE 'Apr-%' OR {expr} LIKE 'May-%' OR {expr} LIKE 'Jun-%' THEN 'Q2'
              WHEN {expr} LIKE 'Jul-%' OR {expr} LIKE 'Aug-%' OR {expr} LIKE 'Sep-%' THEN 'Q3'
              WHEN {expr} LIKE 'Oct-%' OR {expr} LIKE 'Nov-%' OR {expr} LIKE 'Dec-%' THEN 'Q4'
            END
        """.strip()
    
    @staticmethod
    def build_ae_normalization() -> str:
        """Build AE normalization expression"""
        return """
            CASE
                WHEN s.sales_person IS NULL OR TRIM(s.sales_person) = '' THEN 'UNKNOWN'
                ELSE UPPER(TRIM(s.sales_person))
            END
        """.strip()
    
    @staticmethod
    def build_ae_display() -> str:
        """Build AE display name expression"""
        return """
            CASE 
              WHEN s.sales_person IS NULL OR TRIM(s.sales_person) = '' THEN 'Unknown'
              ELSE TRIM(s.sales_person)
            END
        """.strip()
    
    @staticmethod
    def build_base_filters() -> str:
        """Build common filters for revenue queries"""
        return """
            (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
            AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL)
        """

# ============================================================================
# Repository Interfaces (updated with new customer detection)
# ============================================================================

class DatabaseConnection(Protocol):
    """Protocol for database connections"""
    def connect(self): ...
    def close(self) -> None: ...

class RevenueRepository(Protocol):
    """Protocol defining revenue data access operations"""
    
    def get_customer_monthly_data(
        self, 
        year_range: YearRange, 
        filters: 'ReportFilters'
    ) -> List[Dict[str, Any]]: ...
    
    def get_new_customers_for_year(self, year: int) -> Set[str]: ...
    
    def get_ae_performance_data(
        self, 
        filters: 'ReportFilters'
    ) -> List[Dict[str, Any]]: ...
    
    def get_available_years(self) -> List[int]: ...
    
    def get_ae_list(self, year: Optional[int] = None) -> List[str]: ...

# ============================================================================
# Data Access Layer (enhanced with new customer detection)
# ============================================================================

class SQLiteRevenueRepository:
    """SQLite implementation of revenue data access"""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection
        self.query_builder = RevenueQueryBuilder()
    
    def get_new_customers_for_year(self, year: int) -> Set[str]:
        """Get set of customers who are new in the specified year (using audit view)"""
        current_year_range = YearRange.from_year(year)
        
        # Get all customers from current year using audit view
        current_year_query = f"""
            SELECT DISTINCT audit.normalized_name as customer
            FROM spots s
            LEFT JOIN v_customer_normalization_audit audit ON audit.raw_text = s.bill_code
            WHERE s.broadcast_month LIKE ?
            AND audit.normalized_name IS NOT NULL
            AND {self.query_builder.build_base_filters()}
        """
        
        # Get all customers from previous years using audit view
        previous_years_query = f"""
            SELECT DISTINCT audit.normalized_name as customer
            FROM spots s
            LEFT JOIN v_customer_normalization_audit audit ON audit.raw_text = s.bill_code
            WHERE s.broadcast_month NOT LIKE ?
            AND s.broadcast_month IS NOT NULL
            AND s.broadcast_month <> ''
            AND audit.normalized_name IS NOT NULL
            AND {self.query_builder.build_base_filters()}
        """
        
        conn = self.db.connect()
        try:
            cursor = conn.cursor()
            
            # Get current year customers
            cursor.execute(current_year_query, (current_year_range.like_pattern,))
            current_customers = {row[0] for row in cursor.fetchall()}
            
            # Get previous years customers
            cursor.execute(previous_years_query, (current_year_range.like_pattern,))
            previous_customers = {row[0] for row in cursor.fetchall()}
            
            # New customers = current year customers - previous years customers
            new_customers = current_customers - previous_customers
            
            return new_customers
            
        finally:
            conn.close()
    
    # Update the get_customer_monthly_data method in ReportDataService
    # File: src/services/report_data_service.py

    def get_customer_monthly_data(
        self, 
        year_range: YearRange, 
        filters: 'ReportFilters'
    ) -> List[Dict[str, Any]]:
        """Get customer monthly revenue data with properly normalized names from audit view"""
        month_expr = self.query_builder.build_broadcast_month_case()
        ae_display = self.query_builder.build_ae_display()
        
        query = f"""
            SELECT
                COALESCE(s.customer_id, s.bill_code) AS customer_id,
                COALESCE(audit.normalized_name, s.bill_code, 'Unknown') AS customer,
                s.bill_code AS original_customer_name,
                {ae_display} AS ae,
                -- Combine all revenue types into a summary
                GROUP_CONCAT(COALESCE(s.revenue_type, 'Regular'), ', ') AS revenue_type,
                COALESCE(sect.sector_name, 'Unknown') AS sector,
                {month_expr} AS month,
                ROUND(SUM(COALESCE(s.gross_rate, 0)), 2) AS gross_revenue,
                ROUND(SUM(COALESCE(s.station_net, 0)), 2) AS net_revenue,
                CASE WHEN audit.normalized_name IS NOT NULL THEN 'normalized' ELSE 'raw' END as name_source
            FROM spots s
            LEFT JOIN customers c ON s.customer_id = c.customer_id
            LEFT JOIN v_customer_normalization_audit audit ON audit.raw_text = s.bill_code
            LEFT JOIN sectors sect ON c.sector_id = sect.sector_id
            WHERE s.broadcast_month LIKE ?
            AND {self.query_builder.build_base_filters()}
        """
        
        params: List[Any] = [year_range.like_pattern]
        query, params = self._apply_filters(query, params, filters)
        
        # REMOVE revenue_type from GROUP BY to combine all revenue types per customer
        query += """
            GROUP BY 
                
                COALESCE(audit.normalized_name, s.bill_code, 'Unknown'),
                ae, 
                sect.sector_name, 
                month
        """
        
        conn = self.db.connect()
        try:
            cursor = conn.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            data = [dict(zip(columns, row)) for row in rows]
            
            # Get new customers for this year using audit view
            new_customers = self.get_new_customers_for_year(year_range.year)
            
            # Add new customer flag to each row
            for row in data:
                row['is_new_customer'] = row['customer'] in new_customers
            
            return data
            
        finally:
            conn.close()
    
    def get_ae_performance_data(self, filters: 'ReportFilters') -> List[Dict[str, Any]]:
        """Get AE performance data"""
        ae_key = self.query_builder.build_ae_normalization()
        ae_display = self.query_builder.build_ae_display()
        
        query = f"""
            WITH norm AS (
                SELECT
                    {ae_key} AS ae_key,
                    {ae_display} AS ae_display,
                    s.gross_rate,
                    s.air_date,
                    s.broadcast_month
                FROM spots s
                LEFT JOIN customers c ON s.customer_id = c.customer_id
                WHERE {self.query_builder.build_base_filters()}
            )
            SELECT
                MIN(ae_display) AS ae_name,
                COUNT(*) AS spot_count,
                ROUND(SUM(COALESCE(gross_rate, 0)), 2) AS total_revenue,
                ROUND(AVG(COALESCE(gross_rate, 0)), 2) AS avg_rate,
                MIN(air_date) AS first_spot_date,
                MAX(air_date) AS last_spot_date
            FROM norm
            WHERE 1=1
        """
        
        params: List[Any] = []
        query, params = self._apply_ae_and_year_filters(query, params, filters)
        query += " GROUP BY ae_key ORDER BY ae_name"  # Alpha ordering by AE
        
        conn = self.db.connect()
        try:
            cursor = conn.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
        finally:
            conn.close()
    
    def get_available_years(self) -> List[int]:
        """Get list of available years from data"""
        year_expr = self.query_builder.build_year_case('broadcast_month')
        query = f"""
            SELECT DISTINCT {year_expr} AS year
            FROM spots
            WHERE broadcast_month IS NOT NULL
              AND broadcast_month <> ''
              AND broadcast_month LIKE '%-__'
            ORDER BY year DESC
        """
        
        conn = self.db.connect()
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            years = [int(r[0]) for r in cursor.fetchall() if r[0] is not None]
            return sorted(list(set(years)), reverse=True)
        finally:
            conn.close()
    
    def get_ae_list(self, year: Optional[int] = None) -> List[str]:
        """Get list of available AEs, optionally filtered by year"""
        query = """
            SELECT MIN(TRIM(sales_person)) AS ae_display
            FROM spots
            WHERE sales_person IS NOT NULL AND TRIM(sales_person) <> ''
        """
        
        params: List[Any] = []
        if year:
            year_range = YearRange.from_year(year)
            query += " AND broadcast_month LIKE ?"
            params.append(year_range.like_pattern)
        
        query += " GROUP BY UPPER(TRIM(sales_person)) ORDER BY ae_display"
        
        conn = self.db.connect()
        try:
            cursor = conn.cursor()
            cursor.execute(query, params)
            return [r[0] for r in cursor.fetchall()]
        finally:
            conn.close()
    
    def _apply_filters(self, query: str, params: List[Any], filters: 'ReportFilters') -> Tuple[str, List[Any]]:
        """Apply common filters to query with normalized name search"""
        if filters.customer_search:
            # Search both normalized name and original bill_code
            query += """
                AND (
                    LOWER(COALESCE(c.normalized_name, '')) LIKE LOWER(?) OR
                    LOWER(COALESCE(s.bill_code, '')) LIKE LOWER(?)
                )
            """
            search_param = f"%{filters.customer_search}%"
            params.extend([search_param, search_param])
        
        # ADD MISSING AE FILTER LOGIC
        ae_filter = AEFilter.from_input(filters.ae_filter)
        ae_sql, ae_params = ae_filter.build_where_clause("s.sales_person")
        if ae_sql:
            query += f" AND {ae_sql}"
            params.extend(ae_params)
        
        if filters.revenue_type and filters.revenue_type != 'all':
            query += " AND s.revenue_type = ?"
            params.append(filters.revenue_type)
        
        return query, params
    
    def _apply_ae_and_year_filters(self, query: str, params: List[Any], filters: 'ReportFilters') -> Tuple[str, List[Any]]:
        """Apply AE and year filters to query"""
        conditions: List[str] = []
        
        if filters.year:
            year_range = YearRange.from_year(filters.year)
            conditions.append("broadcast_month LIKE ?")
            params.append(year_range.like_pattern)
        
        ae_filter = AEFilter.from_input(filters.ae_filter)
        ae_sql, ae_params = ae_filter.build_where_clause("ae_key")
        if ae_sql:
            conditions.append(ae_sql)
            params.extend(ae_params)
        
        if conditions:
            query += " AND " + " AND ".join(conditions)
        
        return query, params

# ============================================================================
# Business Logic Layer (updated to handle new customers)
# ============================================================================

class CustomerMonthlyDataProcessor:
    """Service for processing customer monthly revenue data"""
    
    def __init__(self, ordering_strategy: OrderingStrategy = OrderingStrategy.AE_THEN_CUSTOMER):
        self.ordering_strategy = ordering_strategy
    
    def process_monthly_data(self, raw_data: List[Dict[str, Any]]) -> List['CustomerMonthlyRow']:
        """Process raw monthly data into structured customer rows"""
        buckets = self._group_data_by_customer(raw_data)
        customer_rows = self._build_customer_rows(buckets)
        return self._apply_ordering(customer_rows)
    
    def _group_data_by_customer(self, raw_data: List[Dict[str, Any]]) -> Dict[Any, Dict[str, Any]]:
        """Group raw data by customer key using normalized names"""
        buckets: Dict[Any, Dict[str, Any]] = {}
        
        for row in raw_data:
            # Use customer_id as primary key, normalized name as display
            key = (row['customer'], row['ae'])
            if key not in buckets:
                buckets[key] = {
                    'customer_id': row['customer_id'],
                    'customer': row['customer'],  # This is now the normalized name
                    'original_customer_name': row.get('original_customer_name', row['customer']),
                    'ae': row['ae'],
                    'revenue_type': row['revenue_type'],
                    'sector': row['sector'],
                    'is_new_customer': row.get('is_new_customer', False),
                    'name_source': row.get('name_source', 'raw'),
                    'months_gross': {m: Decimal('0') for m in range(1, 13)},
                    'months_net': {m: Decimal('0') for m in range(1, 13)},
                }
            
            try:
                month_num = int(row['month'])
                if 1 <= month_num <= 12:
                    buckets[key]['months_gross'][month_num] = Decimal(str(row['gross_revenue']))
                    buckets[key]['months_net'][month_num] = Decimal(str(row['net_revenue']))
            except (TypeError, ValueError):
                continue
        
        return buckets
    
    def _build_customer_rows(self, buckets: Dict[Any, Dict[str, Any]]) -> List['CustomerMonthlyRow']:
        """Build CustomerMonthlyRow objects from grouped data"""
        from src.models.report_data import CustomerMonthlyRow
        
        result: List[CustomerMonthlyRow] = []
        for data in buckets.values():
            row = CustomerMonthlyRow(
                customer_id=data['customer_id'],
                customer=data['customer'],
                ae=data['ae'],
                revenue_type=data['revenue_type'],
                sector=data['sector'],
                is_new_customer=data['is_new_customer']
            )
            for month in range(1, 13):
                row.set_month_value(
                    month, 
                    data['months_gross'][month], 
                    data['months_net'][month]
                )
            result.append(row)
        
        return result
    
    def _apply_ordering(self, rows: List['CustomerMonthlyRow']) -> List['CustomerMonthlyRow']:
        """Apply the configured ordering strategy"""
        if self.ordering_strategy == OrderingStrategy.AE_THEN_CUSTOMER:
            return sorted(rows, key=lambda x: (x.ae.upper(), x.customer.upper()))
        elif self.ordering_strategy == OrderingStrategy.CUSTOMER_ALPHA:
            return sorted(rows, key=lambda x: x.customer.upper())
        elif self.ordering_strategy == OrderingStrategy.REVENUE_DESC:
            return sorted(rows, key=lambda x: x.total, reverse=True)
        else:
            return rows  # No ordering

# ============================================================================
# Service Layer (updated with new customer statistics)
# ============================================================================

class ReportDataService:
    """Main service for generating revenue reports with clean architecture"""
    
    def __init__(self, container=None):
        self.container = container or self._get_default_container()
        self.repository = self._create_repository()
        self.processor = CustomerMonthlyDataProcessor(OrderingStrategy.AE_THEN_CUSTOMER)
        self.query_builder = RevenueQueryBuilder()
        
        # Configuration
        self._cache_enabled = self.container.get_config('CACHE_ENABLED', True)
        self._cache_ttl = self.container.get_config('CACHE_TTL', 300)
    
    def get_monthly_revenue_report_data(
        self,
        year: int,
        filters: Optional['ReportFilters'] = None
    ) -> 'MonthlyRevenueReportData':
        """Generate monthly revenue report with AE-then-customer ordering"""
        start_time = time.time()
        
        # Validate inputs
        if year < 2020 or year > 2030:
            raise ValueError(f"Year {year} is outside valid range (2020-2030)")
        
        filters = filters or self._create_default_filters(year)
        year_range = YearRange.from_year(year)
        
        logger.info("Generating monthly revenue report for year=%s with filters=%s", year, filters.to_dict())
        
        # Get data through repository (now includes new customer flags)
        raw_data = self.repository.get_customer_monthly_data(year_range, filters)
        revenue_data = self.processor.process_monthly_data(raw_data)
        
        # Get supporting data
        available_years = self.repository.get_available_years()
        ae_list = self.repository.get_ae_list(year)
        revenue_types = self._get_revenue_types()
        month_status = self._get_month_status(year)
        
        # Calculate statistics with new customer info
        stats = self._calculate_revenue_statistics(revenue_data, filters.revenue_field or 'gross')
        
        # Build metadata
        processing_time = (time.time() - start_time) * 1000.0
        metadata = self._create_metadata(
            "monthly_revenue",
            {'year': year, 'filters': filters.to_dict()},
            len(revenue_data),
            processing_time
        )
        
        # Import here to avoid circular imports
        from src.models.report_data import MonthlyRevenueReportData
        
        return MonthlyRevenueReportData(
            selected_year=year,
            available_years=available_years,
            total_customers=stats['total_customers'],
            active_customers=stats['active_customers'],
            new_customers=stats.get('new_customers', 0),  # Add new customer count
            total_revenue=Decimal(str(round(stats['total_revenue'], 2))),
            avg_monthly_revenue=Decimal(str(round(stats['avg_monthly_revenue'], 2))),
            revenue_data=revenue_data,
            ae_list=ae_list,
            revenue_types=revenue_types,
            month_status=month_status,
            filters=filters,
            metadata=metadata
        )
    

    def get_customer_normalization_stats(self, year: int) -> Dict[str, Any]:
        """Get statistics about customer name normalization quality"""
        year_range = YearRange.from_year(year)
        
        query = f"""
            SELECT 
                COUNT(DISTINCT s.bill_code) as total_raw_names,
                COUNT(DISTINCT c.normalized_name) as total_normalized_names,
                COUNT(DISTINCT CASE WHEN c.customer_id IS NOT NULL THEN s.bill_code END) as mapped_names,
                COUNT(DISTINCT CASE WHEN c.customer_id IS NULL THEN s.bill_code END) as unmapped_names,
                ROUND(
                    (COUNT(DISTINCT CASE WHEN c.customer_id IS NOT NULL THEN s.bill_code END) * 100.0) / 
                    COUNT(DISTINCT s.bill_code), 2
                ) as normalization_coverage_pct
            FROM spots s
            LEFT JOIN customers c ON s.customer_id = c.customer_id
            WHERE s.broadcast_month LIKE ?
            AND {self.query_builder.build_base_filters()}
        """
        
        # FIX: Use repository connection, not self.db
        conn = self.repository.db.connect()
        try:
            cursor = conn.cursor()
            cursor.execute(query, [year_range.like_pattern])
            row = cursor.fetchone()
            
            return {
                'total_raw_names': row[0],
                'total_normalized_names': row[1], 
                'mapped_names': row[2],
                'unmapped_names': row[3],
                'normalization_coverage_pct': row[4]
            }
        finally:
            conn.close()

    def get_ae_performance_report_data(
        self,
        filters: Optional['ReportFilters'] = None
    ) -> 'AEPerformanceReportData':
        """Generate AE performance report with alphabetical ordering"""
        start_time = time.time()
        filters = filters or self._create_default_filters()
        
        logger.info("Generating AE performance report with filters=%s", filters.to_dict())
        
        # Get data through repository (already ordered by AE name)
        raw_data = self.repository.get_ae_performance_data(filters)
        ae_data = self._build_ae_performance_data(raw_data)
        
        # Calculate aggregates
        total_revenue = sum(ae.total_revenue for ae in ae_data)
        performance_percentages = [
            ae.performance_pct for ae in ae_data 
            if hasattr(ae, 'performance_pct') and ae.performance_pct is not None
        ]
        avg_performance_pct = sum(performance_percentages) / len(performance_percentages) if performance_percentages else None
        top_performer = max(ae_data, key=lambda x: x.total_revenue).ae_name if ae_data else None
        
        # Build metadata
        processing_time = (time.time() - start_time) * 1000.0
        metadata = self._create_metadata(
            "ae_performance",
            {'filters': filters.to_dict()},
            len(ae_data),
            processing_time
        )
        
        # Import here to avoid circular imports
        from src.models.report_data import AEPerformanceReportData
        
        return AEPerformanceReportData(
            ae_performance=ae_data,
            total_revenue=total_revenue,
            avg_performance_pct=avg_performance_pct,
            top_performer=top_performer,
            filters=filters,
            metadata=metadata
        )
    
    def _create_repository(self) -> RevenueRepository:
        """Factory method for creating repository instance"""
        db_connection = self.container.get('database_connection')
        return SQLiteRevenueRepository(db_connection)
    
    def _get_default_container(self):
        """Get default container if none provided"""
        from src.services.container import get_container
        return get_container()
    
    def _create_default_filters(self, year: Optional[int] = None) -> 'ReportFilters':
        """Create default filters"""
        from src.models.report_data import ReportFilters
        return ReportFilters(year=year)
    
    def _calculate_revenue_statistics(self, revenue_data: List['CustomerMonthlyRow'], revenue_field: str) -> Dict[str, Any]:
        """Calculate revenue statistics from processed data with new customer counts"""
        from src.utils.template_formatters import calculate_statistics
        
        rows_for_stats = []
        new_customer_count = 0
        
        for row in revenue_data:
            data = row.to_dict()
            data['total'] = float(row.total_net) if revenue_field.lower() == 'net' else float(row.total_gross)
            rows_for_stats.append(data)
            
            if getattr(row, 'is_new_customer', False):
                new_customer_count += 1
        
        stats = calculate_statistics(rows_for_stats)
        stats['new_customers'] = new_customer_count
        
        return stats
    
    def _build_ae_performance_data(self, raw_data: List[Dict[str, Any]]) -> List['AEPerformanceData']:
        """Build AE performance data objects from raw data"""
        from src.models.report_data import AEPerformanceData
        
        result = []
        for row in raw_data:
            result.append(AEPerformanceData(
                ae_name=row['ae_name'],
                spot_count=row['spot_count'],
                total_revenue=Decimal(str(row['total_revenue'])),
                avg_rate=Decimal(str(row['avg_rate'])),
                first_spot_date=datetime.strptime(row['first_spot_date'], '%Y-%m-%d').date() if row['first_spot_date'] else None,
                last_spot_date=datetime.strptime(row['last_spot_date'], '%Y-%m-%d').date() if row['last_spot_date'] else None
            ))
        return result
    
    def _create_metadata(self, report_type: str, parameters: Dict[str, Any], row_count: int, processing_time: float) -> 'ReportMetadata':
        """Create report metadata"""
        from src.models.report_data import ReportMetadata
        
        return ReportMetadata(
            report_type=report_type,
            parameters=parameters,
            row_count=row_count,
            processing_time_ms=processing_time,
            data_last_updated=self._get_data_last_updated()
        )
    
    def _get_revenue_types(self) -> List[str]:
        """Get available revenue types"""
        query = """
            SELECT DISTINCT COALESCE(revenue_type, 'Regular') AS revenue_type
            FROM spots
            WHERE (revenue_type != 'Trade' OR revenue_type IS NULL)
            ORDER BY revenue_type
        """
        conn = self.repository.db.connect()
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            return [row[0] for row in cursor.fetchall()]
        finally:
            conn.close()
    
    def _get_month_status(self, year: int) -> List['MonthStatus']:
        """Get month closure status"""
        year_range = YearRange.from_year(year)
        query = "SELECT broadcast_month, closed_date, closed_by FROM month_closures WHERE broadcast_month LIKE ?"
        
        conn = self.repository.db.connect()
        try:
            cursor = conn.cursor()
            cursor.execute(query, (year_range.like_pattern,))
            rows = cursor.fetchall()
            closures = [
                {'broadcast_month': row[0], 'closed_date': row[1], 'closed_by': row[2]} 
                for row in rows
            ]
            return create_month_status_from_closure_data(closures, year)
        finally:
            conn.close()
    
    def _get_data_last_updated(self) -> datetime:
        """Get last data update timestamp"""
        query = "SELECT MAX(load_date) AS last_update FROM spots WHERE load_date IS NOT NULL"
        conn = self.repository.db.connect()
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            row = cursor.fetchone()
            if row and row[0]:
                try:
                    return datetime.fromisoformat(row[0])
                except Exception:
                    pass
            return datetime.now()
        finally:
            conn.close()

# ============================================================================
# Factory Functions
# ============================================================================

def create_report_data_service():
    """Factory function to create configured ReportDataService"""
    return ReportDataService()

def create_revenue_repository(db_connection: DatabaseConnection) -> RevenueRepository:
    """Factory function to create configured RevenueRepository"""
    return SQLiteRevenueRepository(db_connection)

# ============================================================================
# Module Configuration
# ============================================================================

logger = logging.getLogger(__name__)

# Proper imports at top of file
from src.models.report_data import (
    ReportFilters, MonthlyRevenueReportData, AEPerformanceReportData,
    CustomerMonthlyRow, AEPerformanceData, ReportMetadata, MonthStatus,
    create_month_status_from_closure_data
)
from src.services.container import get_container
from src.utils.template_formatters import calculate_statistics