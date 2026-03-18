#!/usr/bin/env python3
"""
Query Builder Utilities for CTV Application

Consolidated query patterns used across multiple services and repositories.
Eliminates duplicate SQL fragments and provides consistent query building.
"""

from typing import List, Tuple


class CustomerNormalizationQueryBuilder:
    """
    Query builder for customer normalization patterns.
    
    Consolidates the repeated customer normalization JOIN patterns used across
    multiple services for linking spots to normalized customer data.
    """
    
    @staticmethod
    def build_customer_join(
        spot_alias: str = "s", 
        audit_alias: str = "audit",
        join_type: str = "LEFT JOIN"
    ) -> str:
        """
        Build standard customer normalization JOIN clause.
        
        Args:
            spot_alias: Alias for the spots table (default: "s")
            audit_alias: Alias for the v_customer_normalization_audit view (default: "audit")  
            join_type: Type of JOIN to use (default: "LEFT JOIN")
            
        Returns:
            SQL JOIN clause string
            
        Examples:
            >>> CustomerNormalizationQueryBuilder.build_customer_join()
            "LEFT JOIN v_customer_normalization_audit audit ON audit.raw_text = s.bill_code"
            >>> CustomerNormalizationQueryBuilder.build_customer_join("spots", "norm", "INNER JOIN")
            "INNER JOIN v_customer_normalization_audit norm ON norm.raw_text = spots.bill_code"
        """
        return f"{join_type} v_customer_normalization_audit {audit_alias} ON {audit_alias}.raw_text = {spot_alias}.bill_code"
    
class BroadcastMonthQueryBuilder:
    """
    Query builder for broadcast month filtering patterns.
    
    Consolidates the repeated broadcast month filtering patterns used across
    multiple services for year-based filtering.
    """
    
    @staticmethod
    def build_year_filter(
        year_suffixes: List[str],
        month_column: str = "broadcast_month",
        table_alias: str = "s"
    ) -> Tuple[str, List[str]]:
        """
        Build SQL filter for multiple year suffixes.
        
        Args:
            year_suffixes: List of 2-digit year suffixes like ["23", "24"]
            month_column: Column name for broadcast month (default: "broadcast_month")
            table_alias: Table alias (default: "s")
            
        Returns:
            Tuple of (sql_condition, parameters)
            - sql_condition: SQL WHERE condition string
            - parameters: List of parameter values for the condition
            
        Examples:
            >>> BroadcastMonthQueryBuilder.build_year_filter(["24"])
            ("s.broadcast_month LIKE ?", ["%-24"])
            >>> BroadcastMonthQueryBuilder.build_year_filter(["23", "24"])
            ("(s.broadcast_month LIKE ? OR s.broadcast_month LIKE ?)", ["%-23", "%-24"])
        """
        full_column = f"{table_alias}.{month_column}" if table_alias else month_column
        
        if len(year_suffixes) == 1:
            return f"{full_column} LIKE ?", [f"%-{year_suffixes[0]}"]
        else:
            conditions = " OR ".join(f"{full_column} LIKE ?" for _ in year_suffixes)
            parameters = [f"%-{suffix}" for suffix in year_suffixes]
            return f"({conditions})", parameters
    
    @staticmethod
    def build_month_filter(
        months: List[str],
        year_suffix: str,
        month_column: str = "broadcast_month", 
        table_alias: str = "s"
    ) -> Tuple[str, List[str]]:
        """
        Build SQL filter for specific months in a year.
        
        Args:
            months: List of month abbreviations like ["Jan", "Feb", "Mar"]
            year_suffix: 2-digit year suffix like "24"
            month_column: Column name for broadcast month (default: "broadcast_month") 
            table_alias: Table alias (default: "s")
            
        Returns:
            Tuple of (sql_condition, parameters)
            
        Examples:
            >>> BroadcastMonthQueryBuilder.build_month_filter(["Jan", "Feb"], "24")
            ("(s.broadcast_month IN (?, ?))", ["Jan-24", "Feb-24"])
        """
        full_column = f"{table_alias}.{month_column}" if table_alias else month_column
        month_values = [f"{month}-{year_suffix}" for month in months]
        placeholders = ", ".join("?" for _ in month_values)
        
        return f"({full_column} IN ({placeholders}))", month_values


class RevenueQueryBuilder:
    """Reusable SQL query components for revenue reporting."""

    @staticmethod
    def build_broadcast_month_case(
        expr: str = "s.broadcast_month",
    ) -> str:
        """CASE returning zero-padded month string ('01'..'12')."""
        return f"""CASE
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
            END""".strip()

    @staticmethod
    def build_month_number_case(
        expr: str = "broadcast_month",
    ) -> str:
        """CASE returning integer month number (1..12) from broadcast_month."""
        return f"""CASE SUBSTR({expr}, 1, 3)
                WHEN 'Jan' THEN 1 WHEN 'Feb' THEN 2 WHEN 'Mar' THEN 3
                WHEN 'Apr' THEN 4 WHEN 'May' THEN 5 WHEN 'Jun' THEN 6
                WHEN 'Jul' THEN 7 WHEN 'Aug' THEN 8 WHEN 'Sep' THEN 9
                WHEN 'Oct' THEN 10 WHEN 'Nov' THEN 11 WHEN 'Dec' THEN 12
            END""".strip()

    @staticmethod
    def build_year_case(expr: str = "broadcast_month") -> str:
        """CASE returning 4-digit year integer from broadcast_month."""
        return f"""CASE
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
            END""".strip()

    @staticmethod
    def build_quarter_case(expr: str = "s.broadcast_month") -> str:
        """CASE returning quarter string ('Q1'..'Q4')."""
        return f"""CASE
              WHEN {expr} LIKE 'Jan-%' OR {expr} LIKE 'Feb-%' OR {expr} LIKE 'Mar-%' THEN 'Q1'
              WHEN {expr} LIKE 'Apr-%' OR {expr} LIKE 'May-%' OR {expr} LIKE 'Jun-%' THEN 'Q2'
              WHEN {expr} LIKE 'Jul-%' OR {expr} LIKE 'Aug-%' OR {expr} LIKE 'Sep-%' THEN 'Q3'
              WHEN {expr} LIKE 'Oct-%' OR {expr} LIKE 'Nov-%' OR {expr} LIKE 'Dec-%' THEN 'Q4'
            END""".strip()

    @staticmethod
    def build_quarter_number_case(
        expr: str = "broadcast_month",
    ) -> str:
        """CASE returning integer quarter (1..4) from broadcast_month."""
        return f"""CASE
                WHEN SUBSTR({expr}, 1, 3) IN ('Jan', 'Feb', 'Mar') THEN 1
                WHEN SUBSTR({expr}, 1, 3) IN ('Apr', 'May', 'Jun') THEN 2
                WHEN SUBSTR({expr}, 1, 3) IN ('Jul', 'Aug', 'Sep') THEN 3
                ELSE 4
            END""".strip()

    @staticmethod
    def build_year_filter(
        year_suffixes: List[str],
        month_column: str = "s.broadcast_month",
    ) -> Tuple[str, List[str]]:
        """Build SQL filter for multiple year suffixes using shared utility."""
        return BroadcastMonthQueryBuilder.build_year_filter(
            year_suffixes, "broadcast_month", "s"
        )

    @staticmethod
    def build_ae_normalization() -> str:
        """AE normalization expression (NULL/blank → 'UNKNOWN')."""
        return """CASE
                WHEN s.sales_person IS NULL OR TRIM(s.sales_person) = '' THEN 'UNKNOWN'
                ELSE UPPER(TRIM(s.sales_person))
            END""".strip()

    @staticmethod
    def build_ae_display() -> str:
        """AE display name expression (NULL/blank → 'Unknown')."""
        return """CASE
              WHEN s.sales_person IS NULL OR TRIM(s.sales_person) = '' THEN 'Unknown'
              ELSE TRIM(s.sales_person)
            END""".strip()

    @staticmethod
    def build_base_filters() -> str:
        """Common WHERE filters for revenue queries."""
        return """
            (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
            AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL)
        """