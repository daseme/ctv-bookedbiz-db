"""
Complete Query Builders for Revenue Analysis
===========================================

This file contains all the BaseQueryBuilder classes needed for the
revenue analysis system.

File: src/query_builders.py
"""

from typing import List, Optional
import sqlite3
from dataclasses import dataclass
from enum import Enum


class CustomerIntent(Enum):
    """Customer intent classification for spot placement"""
    LANGUAGE_SPECIFIC = "language_specific"
    TIME_SPECIFIC = "time_specific"
    INDIFFERENT = "indifferent"
    NO_GRID_COVERAGE = "no_grid_coverage"


class AssignmentMethod(Enum):
    """Method used for spot assignment"""
    AUTO_COMPUTED = "auto_computed"
    MANUAL_OVERRIDE = "manual_override"
    NO_GRID_AVAILABLE = "no_grid_available"


@dataclass
class QueryResult:
    """Result container for query execution"""
    revenue: float
    spot_count: int
    query_used: str
    execution_time: Optional[float] = None


class BaseQueryBuilder:
    """
    Centralized query building for revenue analysis
    
    Eliminates duplication of base filters and provides consistent
    query building patterns across all revenue categories.
    """
    
    def __init__(self, year: str = "2024"):
        self.year = year
        self.filters: List[str] = []
        self.joins: List[str] = []
        self.base_table = "spots s"
        
        # Track which common joins we've added to avoid duplicates
        self._added_joins = set()
    
    def apply_standard_filters(self) -> 'BaseQueryBuilder':
        """Apply the base filters that ALL revenue categories need"""
        # Year filter - works for any year format
        year_suffix = self.year[-2:]  # Get last 2 digits (e.g., "24" from "2024")
        self.add_filter(f"s.broadcast_month LIKE '%-{year_suffix}'")
        
        # Trade revenue exclusion - core business rule
        self.add_filter("(s.revenue_type != 'Trade' OR s.revenue_type IS NULL)")
        
        # BNS inclusion - handles bonus spots with NULL revenue
        self.add_filter("(s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')")
        
        return self
    
    def exclude_worldlink(self) -> 'BaseQueryBuilder':
        """Apply WorldLink exclusion with NULL-safe logic"""
        self.add_left_join("agencies a", "s.agency_id = a.agency_id")
        
        # NULL-safe WorldLink exclusion
        self.add_filter("COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'")
        self.add_filter("COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'")
        
        return self
    
    def add_language_block_join(self) -> 'BaseQueryBuilder':
        """Add the standard language block join used by most categories"""
        self.add_left_join("spot_language_blocks slb", "s.spot_id = slb.spot_id")
        return self
    
    def add_customer_join(self) -> 'BaseQueryBuilder':
        """Add customer information join"""
        self.add_left_join("customers c", "s.customer_id = c.customer_id")
        return self
    
    def add_language_joins(self) -> 'BaseQueryBuilder':
        """Add the full language block chain for individual language queries"""
        self.add_language_block_join()
        self.add_left_join("language_blocks lb", "slb.block_id = lb.block_id")
        self.add_left_join("languages l", "lb.language_id = l.language_id")
        return self
    
    def add_filter(self, condition: str) -> 'BaseQueryBuilder':
        """Add a WHERE condition"""
        self.filters.append(condition)
        return self
    
    def add_left_join(self, table: str, condition: str) -> 'BaseQueryBuilder':
        """Add a LEFT JOIN, avoiding duplicates"""
        join_key = table.split(' ')[0]  # Extract table name (before alias)
        
        if join_key not in self._added_joins:
            self.joins.append(f"LEFT JOIN {table} ON {condition}")
            self._added_joins.add(join_key)
        
        return self
    
    def add_inner_join(self, table: str, condition: str) -> 'BaseQueryBuilder':
        """Add an INNER JOIN, avoiding duplicates"""
        join_key = table.split(' ')[0]  # Extract table name (before alias)
        
        if join_key not in self._added_joins:
            self.joins.append(f"JOIN {table} ON {condition}")
            self._added_joins.add(join_key)
        
        return self
    
    def build_where_clause(self) -> str:
        """Build the complete WHERE clause"""
        if not self.filters:
            return ""
        return "WHERE " + " AND ".join(self.filters)
    
    def build_from_clause(self) -> str:
        """Build the complete FROM clause with all joins"""
        result = f"FROM {self.base_table}"
        if self.joins:
            result += "\n" + "\n".join(self.joins)
        return result
    
    def build_select_revenue_query(self, additional_select: str = "") -> str:
        """Build a complete revenue query with optional additional SELECT fields"""
        select_clause = "SELECT SUM(COALESCE(s.gross_rate, 0)) as revenue"
        
        if additional_select:
            select_clause += f", {additional_select}"
        
        return f"{select_clause}\n{self.build_from_clause()}\n{self.build_where_clause()}"
    
    def build_select_count_query(self) -> str:
        """Build a query to count spots matching the criteria"""
        return f"SELECT COUNT(*) as spot_count\n{self.build_from_clause()}\n{self.build_where_clause()}"
    
    def execute_revenue_query(self, db_connection) -> QueryResult:
        """Execute the revenue query and return structured result"""
        import time
        
        start_time = time.time()
        
        # Build and execute revenue query
        revenue_query = self.build_select_revenue_query()
        cursor = db_connection.cursor()
        cursor.execute(revenue_query)
        revenue_result = cursor.fetchone()[0] or 0.0
        
        # Build and execute count query
        count_query = self.build_select_count_query()
        cursor.execute(count_query)
        count_result = cursor.fetchone()[0] or 0
        
        execution_time = time.time() - start_time
        
        return QueryResult(
            revenue=revenue_result,
            spot_count=count_result,
            query_used=revenue_query,
            execution_time=execution_time
        )
    
    def clone(self) -> 'BaseQueryBuilder':
        """Create a copy of this builder for branching logic"""
        new_builder = BaseQueryBuilder(self.year)
        new_builder.filters = self.filters.copy()
        new_builder.joins = self.joins.copy()
        new_builder._added_joins = self._added_joins.copy()
        return new_builder
    
    def debug_print(self) -> None:
        """Print the current query for debugging"""
        print("=== DEBUG: Current Query ===")
        print(self.build_select_revenue_query())
        print("============================")


# Category-specific builders that extend the base
class IndividualLanguageQueryBuilder(BaseQueryBuilder):
    """Builder for individual language block revenue queries"""
    
    def __init__(self, year: str = "2024"):
        super().__init__(year)
        self.apply_standard_filters()
        self.exclude_worldlink()
        self.add_language_joins()
    
    def add_individual_language_conditions(self) -> 'IndividualLanguageQueryBuilder':
        """Add conditions specific to individual language blocks"""
        self.add_filter("((slb.spans_multiple_blocks = 0 AND slb.block_id IS NOT NULL) OR "
                       "(slb.spans_multiple_blocks IS NULL AND slb.block_id IS NOT NULL))")
        return self
    
    def build_language_summary_query(self) -> str:
        """Build query for language-by-language breakdown"""
        return f"""
        SELECT 
            CASE 
                WHEN l.language_name IN ('Mandarin', 'Cantonese') THEN 'Chinese'
                ELSE COALESCE(l.language_name, 'Unknown Language')
            END as language,
            COUNT(*) as spots,
            SUM(COALESCE(s.gross_rate, 0)) as revenue,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bonus_spots
        {self.build_from_clause()}
        {self.build_where_clause()}
        GROUP BY CASE 
            WHEN l.language_name IN ('Mandarin', 'Cantonese') THEN 'Chinese'
            ELSE COALESCE(l.language_name, 'Unknown Language')
        END
        ORDER BY SUM(COALESCE(s.gross_rate, 0)) DESC
        """


class ChinesePrimeTimeQueryBuilder(BaseQueryBuilder):
    """Builder for Chinese Prime Time revenue queries"""
    
    def __init__(self, year: str = "2024"):
        super().__init__(year)
        self.apply_standard_filters()
        self.exclude_worldlink()
        self.add_language_block_join()
    
    def add_chinese_prime_time_conditions(self) -> 'ChinesePrimeTimeQueryBuilder':
        """Add Chinese Prime Time scheduling conditions"""
        self.add_filter("""
            (
                -- Chinese Prime Time M-F 7pm-11:59pm
                (s.time_in >= '19:00:00' AND s.time_out <= '23:59:59' 
                 AND s.day_of_week IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'))
                OR
                -- Chinese Weekend 8pm-11:59pm  
                (s.time_in >= '20:00:00' AND s.time_out <= '23:59:59'
                 AND s.day_of_week IN ('Saturday', 'Sunday'))
            )
        """)
        return self
    
    def add_multi_language_conditions(self) -> 'ChinesePrimeTimeQueryBuilder':
        """Add conditions for multi-language spots"""
        self.add_filter("(slb.spans_multiple_blocks = 1 OR "
                       "(slb.spans_multiple_blocks = 0 AND slb.block_id IS NULL) OR "
                       "(slb.spans_multiple_blocks IS NULL AND slb.block_id IS NULL))")
        return self


class DirectResponseQueryBuilder(BaseQueryBuilder):
    """Builder for Direct Response (WorldLink) revenue queries"""
    
    def __init__(self, year: str = "2024"):
        super().__init__(year)
        self.apply_standard_filters()
        self.add_left_join("agencies a", "s.agency_id = a.agency_id")
    
    def add_worldlink_conditions(self) -> 'DirectResponseQueryBuilder':
        """Add WorldLink identification conditions"""
        self.add_filter("(COALESCE(a.agency_name, '') LIKE '%WorldLink%' OR "
                       "COALESCE(s.bill_code, '') LIKE '%WorldLink%')")
        return self


class MultiLanguageQueryBuilder(BaseQueryBuilder):
    """Builder for Multi-Language (Cross-Audience) revenue queries"""
    
    def __init__(self, year: str = "2024"):
        super().__init__(year)
        self.apply_standard_filters()
        self.exclude_worldlink()
        self.add_language_block_join()
    
    def add_multi_language_conditions(self) -> 'MultiLanguageQueryBuilder':
        """Add conditions for multi-language spots"""
        self.add_filter("(slb.spans_multiple_blocks = 1 OR "
                       "(slb.spans_multiple_blocks = 0 AND slb.block_id IS NULL) OR "
                       "(slb.spans_multiple_blocks IS NULL AND slb.block_id IS NULL))")
        return self
    
    def exclude_chinese_prime_time(self) -> 'MultiLanguageQueryBuilder':
        """Exclude Chinese Prime Time hours - this is the key complexity"""
        self.add_filter("""NOT (
            -- Exclude Chinese Prime Time M-F 7pm-11:59pm
            (s.time_in >= '19:00:00' AND s.time_out <= '23:59:59' 
             AND s.day_of_week IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'))
            OR
            -- Exclude Chinese Weekend 8pm-11:59pm  
            (s.time_in >= '20:00:00' AND s.time_out <= '23:59:59'
             AND s.day_of_week IN ('Saturday', 'Sunday'))
        )""")
        return self
    
    def exclude_nkb_overnight_shopping(self) -> 'MultiLanguageQueryBuilder':
        """Exclude NKB spots that belong to Overnight Shopping category"""
        self.add_customer_join()
        self.add_filter("COALESCE(c.normalized_name, '') NOT LIKE '%NKB%'")
        self.add_filter("COALESCE(s.bill_code, '') NOT LIKE '%NKB%'")
        # Agency join already added by exclude_worldlink()
        self.add_filter("COALESCE(a.agency_name, '') NOT LIKE '%NKB%'")
        return self


class OtherNonLanguageQueryBuilder(BaseQueryBuilder):
    """Builder for Other Non-Language revenue queries"""
    
    def __init__(self, year: str = "2024"):
        super().__init__(year)
        self.apply_standard_filters()
        self.exclude_worldlink()
        self.add_customer_join()
    
    def add_no_language_assignment_condition(self) -> 'OtherNonLanguageQueryBuilder':
        """Add condition for spots with no language assignment"""
        # LEFT JOIN so we can check for NULL
        self.add_left_join("spot_language_blocks slb", "s.spot_id = slb.spot_id")
        self.add_filter("slb.spot_id IS NULL")
        return self
    
    def exclude_prd_svc_spots(self) -> 'OtherNonLanguageQueryBuilder':
        """Exclude PRD and SVC spot types"""
        self.add_filter("(s.spot_type NOT IN ('PRD', 'SVC') OR s.spot_type IS NULL OR s.spot_type = '')")
        return self
    
    def exclude_nkb_spots(self) -> 'OtherNonLanguageQueryBuilder':
        """Exclude NKB spots (they go to overnight shopping)"""
        self.add_filter("COALESCE(c.normalized_name, '') NOT LIKE '%NKB%'")
        self.add_filter("COALESCE(s.bill_code, '') NOT LIKE '%NKB%'")
        # Agency join already added by exclude_worldlink()
        self.add_filter("COALESCE(a.agency_name, '') NOT LIKE '%NKB%'")
        return self


class OvernightShoppingQueryBuilder(BaseQueryBuilder):
    """Builder for Overnight Shopping revenue queries"""
    
    def __init__(self, year: str = "2024"):
        super().__init__(year)
        self.apply_standard_filters()
        self.exclude_worldlink()
        self.add_customer_join()
    
    def add_no_language_assignment_condition(self) -> 'OvernightShoppingQueryBuilder':
        """Add condition for spots with no language assignment"""
        # LEFT JOIN so we can check for NULL
        self.add_left_join("spot_language_blocks slb", "s.spot_id = slb.spot_id")
        self.add_filter("slb.spot_id IS NULL")
        return self
    
    def exclude_prd_svc_spots(self) -> 'OvernightShoppingQueryBuilder':
        """Exclude PRD and SVC spot types"""
        self.add_filter("(s.spot_type NOT IN ('PRD', 'SVC') OR s.spot_type IS NULL OR s.spot_type = '')")
        return self
    
    def include_only_nkb_spots(self) -> 'OvernightShoppingQueryBuilder':
        """Include ONLY NKB spots (overnight shopping programming)"""
        self.add_filter("""(
            COALESCE(c.normalized_name, '') LIKE '%NKB%' 
            OR COALESCE(s.bill_code, '') LIKE '%NKB%'
            OR COALESCE(a.agency_name, '') LIKE '%NKB%'
        )""")
        return self


class BrandedContentQueryBuilder(BaseQueryBuilder):
    """Builder for Branded Content (PRD) revenue queries"""
    
    def __init__(self, year: str = "2024"):
        super().__init__(year)
        self.apply_standard_filters()
        # Note: No WorldLink exclusion needed for PRD
    
    def add_no_language_assignment_condition(self) -> 'BrandedContentQueryBuilder':
        """Add condition for spots with no language assignment"""
        self.add_left_join("spot_language_blocks slb", "s.spot_id = slb.spot_id")
        self.add_filter("slb.spot_id IS NULL")
        return self
    
    def add_prd_spot_type_condition(self) -> 'BrandedContentQueryBuilder':
        """Add condition for PRD spot type"""
        self.add_filter("s.spot_type = 'PRD'")
        return self


class ServicesQueryBuilder(BaseQueryBuilder):
    """Builder for Services (SVC) revenue queries"""
    
    def __init__(self, year: str = "2024"):
        super().__init__(year)
        self.apply_standard_filters()
        # Note: No WorldLink exclusion needed for SVC
    
    def add_no_language_assignment_condition(self) -> 'ServicesQueryBuilder':
        """Add condition for spots with no language assignment"""
        self.add_left_join("spot_language_blocks slb", "s.spot_id = slb.spot_id")
        self.add_filter("slb.spot_id IS NULL")
        return self
    
    def add_svc_spot_type_condition(self) -> 'ServicesQueryBuilder':
        """Add condition for SVC spot type"""
        self.add_filter("s.spot_type = 'SVC'")
        return self


# Validation helper function
def validate_query_migration(old_query: str, new_builder: BaseQueryBuilder, 
                           db_connection, tolerance: float = 0.01) -> bool:
    """
    Validate that the new query builder produces the same results as the old query
    
    Args:
        old_query: The original SQL query
        new_builder: The new BaseQueryBuilder instance
        db_connection: Database connection
        tolerance: Allowable difference in revenue (for floating point precision)
    
    Returns:
        True if results match within tolerance
    """
    cursor = db_connection.cursor()
    
    # Execute old query
    cursor.execute(old_query)
    old_result = cursor.fetchone()[0] or 0.0
    
    # Execute new query
    new_result = new_builder.execute_revenue_query(db_connection)
    
    # Compare results
    difference = abs(old_result - new_result.revenue)
    matches = difference <= tolerance
    
    if not matches:
        print(f"❌ VALIDATION FAILED:")
        print(f"   Old query result: ${old_result:,.2f}")
        print(f"   New query result: ${new_result.revenue:,.2f}")
        print(f"   Difference: ${difference:,.2f}")
        print(f"   Tolerance: ${tolerance:,.2f}")
    else:
        print(f"✅ VALIDATION PASSED: Results match within ${tolerance:,.2f}")
    
    return matches