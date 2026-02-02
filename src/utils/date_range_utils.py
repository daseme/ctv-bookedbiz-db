#!/usr/bin/env python3
"""
Date Range Utilities for CTV Application

Consolidated date and year parsing functions used across multiple analysis modules.
Extracted from unified_analysis-old.py and market_analysis.py to eliminate duplication.
"""

from typing import List, Tuple


class DateRangeUtils:
    """Utility class for parsing and handling date ranges in the CTV application."""
    
    @staticmethod
    def parse_year_range(year_input: str) -> Tuple[List[str], List[str]]:
        """
        Parse year input to handle both single years and ranges.
        
        Args:
            year_input: String like "2024" or "2023-2024"
            
        Returns:
            Tuple of (full_years, year_suffixes)
            - full_years: List of full year strings ["2023", "2024"]  
            - year_suffixes: List of 2-digit suffixes ["23", "24"]
            
        Raises:
            ValueError: If start year is greater than end year
            
        Examples:
            >>> DateRangeUtils.parse_year_range("2024")
            (["2024"], ["24"])
            >>> DateRangeUtils.parse_year_range("2023-2024") 
            (["2023", "2024"], ["23", "24"])
        """
        if "-" in year_input and len(year_input) > 4:
            start_year, end_year = year_input.split("-")
            start_year = int(start_year)
            end_year = int(end_year)
            if start_year > end_year:
                raise ValueError(
                    f"Start year {start_year} cannot be greater than end year {end_year}"
                )
            full_years = [str(year) for year in range(start_year, end_year + 1)]
            year_suffixes = [year[-2:] for year in full_years]
        else:
            full_years = [year_input]
            year_suffixes = [year_input[-2:]]
        return full_years, year_suffixes
    
    @staticmethod
    def build_year_filter(year_suffixes: List[str]) -> Tuple[str, List[str]]:
        """
        Build SQL filter for multiple year suffixes.
        
        Args:
            year_suffixes: List of 2-digit year suffixes like ["23", "24"]
            
        Returns:
            Tuple of (sql_condition, parameters)
            - sql_condition: SQL WHERE condition string
            - parameters: List of parameter values for the condition
            
        Examples:
            >>> DateRangeUtils.build_year_filter(["24"])
            ("s.broadcast_month LIKE ?", ["%-24"])
            >>> DateRangeUtils.build_year_filter(["23", "24"])
            ("(s.broadcast_month LIKE ? OR s.broadcast_month LIKE ?)", ["%-23", "%-24"])
        """
        if len(year_suffixes) == 1:
            return "s.broadcast_month LIKE ?", [f"%-{year_suffixes[0]}"]
        else:
            conditions = " OR ".join("s.broadcast_month LIKE ?" for _ in year_suffixes)
            parameters = [f"%-{suffix}" for suffix in year_suffixes]
            return f"({conditions})", parameters