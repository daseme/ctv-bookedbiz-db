#!/usr/bin/env python3
"""
Query Builder Utilities for CTV Application

Consolidated query patterns used across multiple services and repositories.
Eliminates duplicate SQL fragments and provides consistent query building.
"""

from typing import Dict, List, Tuple, Optional


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
    
    @staticmethod
    def build_customer_select(
        spot_alias: str = "s",
        audit_alias: str = "audit",
        include_original: bool = True
    ) -> str:
        """
        Build standard customer normalization SELECT clause.
        
        Args:
            spot_alias: Alias for the spots table (default: "s")
            audit_alias: Alias for the audit table (default: "audit")
            include_original: Whether to include original bill_code (default: True)
            
        Returns:
            SQL SELECT clause for normalized customer data
            
        Examples:
            >>> CustomerNormalizationQueryBuilder.build_customer_select()
            "COALESCE(audit.normalized_name, s.bill_code, 'Unknown') AS customer, s.bill_code AS original_customer"
        """
        customer_clause = f"COALESCE({audit_alias}.normalized_name, {spot_alias}.bill_code, 'Unknown') AS customer"
        
        if include_original:
            original_clause = f", {spot_alias}.bill_code AS original_customer"
            return customer_clause + original_clause
        else:
            return customer_clause
    
    @staticmethod 
    def build_customer_id_select(
        spot_alias: str = "s",
        audit_alias: str = "audit"
    ) -> str:
        """
        Build customer ID selection with fallback to bill_code.
        
        Args:
            spot_alias: Alias for the spots table (default: "s")
            audit_alias: Alias for the audit table (default: "audit")
            
        Returns:
            SQL SELECT clause for customer ID with fallback
            
        Examples:
            >>> CustomerNormalizationQueryBuilder.build_customer_id_select()
            "COALESCE(s.customer_id, s.bill_code) AS customer_id"
        """
        return f"COALESCE({spot_alias}.customer_id, {spot_alias}.bill_code) AS customer_id"


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