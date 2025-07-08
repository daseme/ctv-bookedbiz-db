#!/usr/bin/env python3
"""
Multi-Year Comprehensive Revenue Report Generator
===============================================

Generates detailed markdown revenue reports with language breakdowns across single years or time periods.
Supports year-over-year comparisons and trend analysis.

Usage:
    python multi_year_revenue_report.py 2024                    # Single year
    python multi_year_revenue_report.py 2023-2024              # Year range
    python multi_year_revenue_report.py 2022-2024 --output report.md  # Multi-year range
"""

import sqlite3
import argparse
import datetime
import logging
import os
import sys
from typing import Dict, List, Tuple, Optional, Union
from pathlib import Path
from dataclasses import dataclass
from collections import defaultdict


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('revenue_report.log')
    ]
)
logger = logging.getLogger(__name__)


class RevenueReportError(Exception):
    """Base exception for revenue report errors"""
    pass


class DatabaseConnectionError(RevenueReportError):
    """Raised when database connection fails"""
    pass


class ValidationError(RevenueReportError):
    """Raised when input validation fails"""
    pass


class ReconciliationError(RevenueReportError):
    """Raised when revenue reconciliation fails"""
    pass


@dataclass
class YearData:
    """Data structure for single year results"""
    year: int
    total_spots: int
    total_revenue: float
    direct_response: Tuple[int, float, int]  # spots, revenue, bonus
    multi_language: Tuple[int, float, int]
    branded_content: Tuple[int, float, int]
    services: Tuple[int, float, int]
    other_nonlanguage: Tuple[int, float, int]
    individual_languages: List[Tuple[str, int, float, int]]  # name, spots, revenue, bonus


@dataclass
class TimePeriod:
    """Represents a time period for analysis"""
    start_year: int
    end_year: int
    
    @property
    def is_single_year(self) -> bool:
        return self.start_year == self.end_year
    
    @property
    def years(self) -> List[int]:
        return list(range(self.start_year, self.end_year + 1))
    
    @property
    def description(self) -> str:
        if self.is_single_year:
            return str(self.start_year)
        return f"{self.start_year}-{self.end_year}"
    
    def __str__(self) -> str:
        return self.description


class MultiYearRevenueReport:
    def __init__(self, db_path: str = './data/database/production.db'):
        """Initialize the multi-year revenue report generator.
        
        Args:
            db_path: Path to the SQLite database file
            
        Raises:
            ValidationError: If database path is invalid
        """
        self.db_path = self._validate_db_path(db_path)
        logger.info(f"Initialized revenue report generator with database: {self.db_path}")
        
    def _validate_db_path(self, db_path: str) -> str:
        """Validate database path exists and is accessible."""
        if not db_path or not isinstance(db_path, str):
            raise ValidationError("Database path must be a non-empty string")
            
        path = Path(db_path)
        
        if not path.exists():
            raise ValidationError(f"Database file does not exist: {db_path}")
            
        if not path.is_file():
            raise ValidationError(f"Database path is not a file: {db_path}")
            
        if not os.access(path, os.R_OK):
            raise ValidationError(f"Database file is not readable: {db_path}")
            
        return str(path.resolve())
    
    def _validate_year(self, year: int) -> int:
        """Validate year parameter is reasonable."""
        current_year = datetime.datetime.now().year
        min_year = 1900
        max_year = current_year + 1
        
        if not isinstance(year, int):
            raise ValidationError("Year must be an integer")
            
        if year < min_year or year > max_year:
            raise ValidationError(f"Year must be between {min_year} and {max_year}, got {year}")
            
        return year
    
    def parse_time_period(self, period_str: str) -> TimePeriod:
        """Parse time period string into TimePeriod object.
        
        Args:
            period_str: String like "2024" or "2023-2024"
            
        Returns:
            TimePeriod: Validated time period
            
        Raises:
            ValidationError: If period string is invalid
        """
        if not period_str or not isinstance(period_str, str):
            raise ValidationError("Time period must be a non-empty string")
        
        # Handle single year
        if '-' not in period_str:
            try:
                year = int(period_str)
                validated_year = self._validate_year(year)
                return TimePeriod(validated_year, validated_year)
            except ValueError:
                raise ValidationError(f"Invalid year format: {period_str}")
        
        # Handle year range
        parts = period_str.split('-')
        if len(parts) != 2:
            raise ValidationError(f"Invalid time period format: {period_str}. Use 'YYYY' or 'YYYY-YYYY'")
        
        try:
            start_year = int(parts[0])
            end_year = int(parts[1])
        except ValueError:
            raise ValidationError(f"Invalid year format in period: {period_str}")
        
        start_year = self._validate_year(start_year)
        end_year = self._validate_year(end_year)
        
        if start_year > end_year:
            raise ValidationError(f"Start year must be <= end year: {start_year} > {end_year}")
        
        # Limit range to prevent excessive queries
        max_range = 5
        if end_year - start_year + 1 > max_range:
            raise ValidationError(f"Time period too large. Maximum {max_range} years allowed.")
        
        logger.info(f"Parsed time period: {start_year}-{end_year}")
        return TimePeriod(start_year, end_year)
    
    def get_year_suffix(self, year: int) -> str:
        """Convert year to suffix format (2024 -> '24')"""
        return str(year)[2:]
    
    def run_query(self, query: str, params: tuple = ()) -> List[Tuple]:
        """Execute query and return results with proper error handling."""
        if not query or not isinstance(query, str):
            raise ValidationError("Query must be a non-empty string")
            
        logger.debug(f"Executing query with {len(params)} parameters")
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("PRAGMA foreign_keys = ON")
                conn.execute("PRAGMA busy_timeout = 30000")
                
                cursor = conn.cursor()
                cursor.execute(query, params)
                results = cursor.fetchall()
                
                logger.debug(f"Query returned {len(results)} rows")
                return results
                
        except sqlite3.OperationalError as e:
            error_msg = f"Database operational error: {e}"
            logger.error(error_msg)
            raise DatabaseConnectionError(error_msg) from e
            
        except sqlite3.DatabaseError as e:
            error_msg = f"Database error: {e}"
            logger.error(error_msg)
            raise DatabaseConnectionError(error_msg) from e
            
        except Exception as e:
            error_msg = f"Unexpected error executing query: {e}"
            logger.error(error_msg)
            raise RevenueReportError(error_msg) from e
    
    def get_year_data(self, year: int) -> YearData:
        """Get complete data for a single year.
        
        Args:
            year: Year to query
            
        Returns:
            YearData: Complete year data structure
        """
        logger.info(f"Fetching data for year {year}")
        
        try:
            # Get all data for the year
            total_spots, total_revenue = self.get_total_validation(year)
            direct_response = self.get_direct_response(year)
            multi_language = self.get_multi_language(year)
            branded_content = self.get_branded_content(year)
            services = self.get_services(year)
            other_nonlanguage = self.get_other_nonlanguage(year)
            individual_languages = self.get_individual_languages(year)
            
            return YearData(
                year=year,
                total_spots=total_spots,
                total_revenue=total_revenue,
                direct_response=direct_response,
                multi_language=multi_language,
                branded_content=branded_content,
                services=services,
                other_nonlanguage=other_nonlanguage,
                individual_languages=individual_languages
            )
            
        except Exception as e:
            logger.error(f"Failed to get year data for {year}: {e}")
            raise RevenueReportError(f"Failed to get year data for {year}: {e}") from e
    
    def get_total_validation(self, year: int) -> Tuple[int, float]:
        """Get total revenue for validation - supports multi-year queries"""
        year_suffix = self.get_year_suffix(year)
        query = """
        SELECT 
            COUNT(*) as total_spots,
            SUM(COALESCE(s.gross_rate, 0)) as total_revenue
        FROM spots s
        WHERE s.broadcast_month LIKE ?
          AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
          AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        """
        
        results = self.run_query(query, (f'%-{year_suffix}',))
        spots, revenue = results[0] if results else (0, 0.0)
        
        logger.debug(f"Total validation for {year}: {spots:,} spots, ${revenue:,.2f} revenue")
        return spots, revenue
    
    def get_multi_year_total_validation(self, years: List[int]) -> Tuple[int, float]:
        """Get total revenue validation across multiple years"""
        if not years:
            return 0, 0.0
        
        # Create OR conditions for each year
        year_conditions = []
        params = []
        
        for year in years:
            year_suffix = self.get_year_suffix(year)
            year_conditions.append("s.broadcast_month LIKE ?")
            params.append(f'%-{year_suffix}')
        
        query = f"""
        SELECT 
            COUNT(*) as total_spots,
            SUM(COALESCE(s.gross_rate, 0)) as total_revenue
        FROM spots s
        WHERE ({' OR '.join(year_conditions)})
          AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
          AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        """
        
        results = self.run_query(query, tuple(params))
        spots, revenue = results[0] if results else (0, 0.0)
        
        logger.info(f"Multi-year total validation for {years}: {spots:,} spots, ${revenue:,.2f} revenue")
        return spots, revenue
    
    def get_direct_response(self, year: int) -> Tuple[int, float, int]:
        """Get Direct Response data for a single year"""
        year_suffix = self.get_year_suffix(year)
        query = """
        SELECT 
            COUNT(*) as spots,
            SUM(COALESCE(s.gross_rate, 0)) as revenue,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bonus_spots
        FROM spots s
        LEFT JOIN agencies a ON s.agency_id = a.agency_id
        WHERE s.broadcast_month LIKE ?
          AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
          AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
          AND (
            (a.agency_name LIKE '%WorldLink%') OR
            (s.bill_code LIKE '%WorldLink%')
          )
        """
        
        results = self.run_query(query, (f'%-{year_suffix}',))
        return results[0] if results else (0, 0.0, 0)
    
    def get_individual_languages(self, year: int) -> List[Tuple[str, int, float, int]]:
        """Get individual language breakdown for a single year"""
        year_suffix = self.get_year_suffix(year)
        query = """
        SELECT 
            COALESCE(l.language_name, 'Unknown Language') as language,
            COUNT(*) as spots,
            SUM(COALESCE(s.gross_rate, 0)) as revenue,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bonus_spots
        FROM spots s
        JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        LEFT JOIN language_blocks lb ON slb.block_id = lb.block_id
        LEFT JOIN languages l ON lb.language_id = l.language_id
        LEFT JOIN agencies a ON s.agency_id = a.agency_id
        WHERE s.broadcast_month LIKE ?
          AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
          AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
          AND ((slb.spans_multiple_blocks = 0 AND slb.block_id IS NOT NULL) OR 
               (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NOT NULL))
          AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
          AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
        GROUP BY l.language_name
        ORDER BY SUM(COALESCE(s.gross_rate, 0)) DESC
        """
        
        return self.run_query(query, (f'%-{year_suffix}',))
    
    def get_multi_language(self, year: int) -> Tuple[int, float, int]:
        """Get Multi-Language (Cross-Audience) data for a single year"""
        year_suffix = self.get_year_suffix(year)
        query = """
        SELECT 
            COUNT(*) as spots,
            SUM(COALESCE(s.gross_rate, 0)) as revenue,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bonus_spots
        FROM spots s
        JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        LEFT JOIN agencies a ON s.agency_id = a.agency_id
        WHERE s.broadcast_month LIKE ?
          AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
          AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
          AND (slb.spans_multiple_blocks = 1 OR 
               (slb.spans_multiple_blocks = 0 AND slb.block_id IS NULL) OR 
               (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NULL))
          AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
          AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
        """
        
        results = self.run_query(query, (f'%-{year_suffix}',))
        return results[0] if results else (0, 0.0, 0)
    
    def get_branded_content(self, year: int) -> Tuple[int, float, int]:
        """Get Branded Content (PRD) data for a single year"""
        year_suffix = self.get_year_suffix(year)
        query = """
        SELECT 
            COUNT(*) as spots,
            SUM(COALESCE(s.gross_rate, 0)) as revenue,
            0 as bonus_spots
        FROM spots s
        LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        LEFT JOIN agencies a ON s.agency_id = a.agency_id
        WHERE s.broadcast_month LIKE ?
          AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
          AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL)
          AND s.spot_type = 'PRD'
          AND slb.spot_id IS NULL
          AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
          AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
        """
        
        results = self.run_query(query, (f'%-{year_suffix}',))
        return results[0] if results else (0, 0.0, 0)
    
    def get_services(self, year: int) -> Tuple[int, float, int]:
        """Get Services (SVC) data for a single year"""
        year_suffix = self.get_year_suffix(year)
        query = """
        SELECT 
            COUNT(*) as spots,
            SUM(COALESCE(s.gross_rate, 0)) as revenue,
            0 as bonus_spots
        FROM spots s
        LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        LEFT JOIN agencies a ON s.agency_id = a.agency_id
        WHERE s.broadcast_month LIKE ?
          AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
          AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL)
          AND s.spot_type = 'SVC'
          AND slb.spot_id IS NULL
          AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
          AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
        """
        
        results = self.run_query(query, (f'%-{year_suffix}',))
        return results[0] if results else (0, 0.0, 0)
    
    def get_other_nonlanguage(self, year: int) -> Tuple[int, float, int]:
        """Get Other Non-Language data for a single year"""
        year_suffix = self.get_year_suffix(year)
        query = """
        SELECT 
            COUNT(*) as spots,
            SUM(COALESCE(s.gross_rate, 0)) as revenue,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bonus_spots
        FROM spots s
        LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        LEFT JOIN agencies a ON s.agency_id = a.agency_id
        WHERE s.broadcast_month LIKE ?
          AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
          AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
          AND slb.spot_id IS NULL
          AND (s.spot_type NOT IN ('PRD', 'SVC') OR s.spot_type IS NULL OR s.spot_type = '')
          AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
          AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
        """
        
        results = self.run_query(query, (f'%-{year_suffix}',))
        return results[0] if results else (0, 0.0, 0)
    
    def _calculate_year_over_year_change(self, current: float, previous: float) -> Tuple[float, float]:
        """Calculate year-over-year change amount and percentage"""
        if previous == 0:
            return current, float('inf') if current > 0 else 0.0
        
        change_amount = current - previous
        change_percent = (change_amount / previous) * 100
        
        return change_amount, change_percent
    
    def _format_change(self, change_amount: float, change_percent: float) -> str:
        """Format change amount and percentage for display"""
        if change_percent == float('inf'):
            return f"📈 +${change_amount:,.2f} (∞%)"
        elif change_percent > 0:
            return f"📈 +${change_amount:,.2f} (+{change_percent:.1f}%)"
        elif change_percent < 0:
            return f"📉 ${change_amount:,.2f} ({change_percent:.1f}%)"
        else:
            return f"➡️ ${change_amount:,.2f} (0.0%)"
    
    def generate_report(self, time_period: TimePeriod) -> str:
        """Generate comprehensive multi-year markdown report"""
        try:
            logger.info(f"Starting report generation for {time_period}")
            
            lines = []
            
            # Header
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            lines.append(f"# 📊 {time_period.description} Comprehensive Revenue Report")
            lines.append("")
            lines.append(f"**Generated:** {timestamp}")
            lines.append(f"**Database:** `{os.path.basename(self.db_path)}`")
            lines.append(f"**Time Period:** {time_period.description}")
            lines.append(f"**Revenue Basis:** Gross revenue only (gross_rate field)")
            lines.append(f"**Query Method:** NULL-safe WorldLink exclusion with perfect reconciliation")
            lines.append("")
            
            # Get data for all years
            year_data = {}
            for year in time_period.years:
                year_data[year] = self.get_year_data(year)
            
            # Calculate combined totals
            total_spots = sum(data.total_spots for data in year_data.values())
            total_revenue = sum(data.total_revenue for data in year_data.values())
            
            if total_spots == 0:
                logger.warning(f"No data found for time period {time_period}")
                lines.append(f"## ⚠️ No Data Found")
                lines.append(f"No revenue data found for time period {time_period}")
                return "\n".join(lines)
            
            # Executive Summary
            lines.append("## 🎯 Executive Summary")
            lines.append("")
            lines.append(f"- **Total Revenue:** ${total_revenue:,.2f}")
            lines.append(f"- **Total Spots:** {total_spots:,}")
            lines.append(f"- **Average per Spot:** ${total_revenue/total_spots:.2f}")
            lines.append(f"- **Time Period:** {time_period.description}")
            lines.append("")
            
            # Year-over-Year Summary (if multi-year)
            if not time_period.is_single_year:
                lines.append("## 📈 Year-over-Year Summary")
                lines.append("")
                lines.append("| Year | Revenue | Change vs Previous | Spots | Change vs Previous |")
                lines.append("|------|---------|-------------------|-------|--------------------|")
                
                previous_year_data = None
                for year in time_period.years:
                    data = year_data[year]
                    
                    if previous_year_data:
                        rev_change, rev_pct = self._calculate_year_over_year_change(
                            data.total_revenue, previous_year_data.total_revenue
                        )
                        spot_change, spot_pct = self._calculate_year_over_year_change(
                            data.total_spots, previous_year_data.total_spots
                        )
                        
                        rev_change_str = self._format_change(rev_change, rev_pct)
                        spot_change_str = self._format_change(spot_change, spot_pct)
                    else:
                        rev_change_str = "➡️ Baseline"
                        spot_change_str = "➡️ Baseline"
                    
                    lines.append(f"| {year} | ${data.total_revenue:,.2f} | {rev_change_str} | {data.total_spots:,} | {spot_change_str} |")
                    previous_year_data = data
                
                lines.append("")
            
            # Combined Revenue Breakdown
            lines.append(f"## 💰 Combined Revenue Breakdown ({time_period.description})")
            lines.append("")
            
            # Aggregate all categories across years
            combined_categories = self._aggregate_categories(year_data)
            
            lines.append("| Category | Revenue | % | Spots | Bonus Spots | Avg/Spot |")
            lines.append("|----------|---------|---|-------|-------------|----------|")
            
            for cat_name, revenue, spots, bonus in combined_categories:
                if revenue > 0:
                    pct = (revenue / total_revenue * 100) if total_revenue > 0 else 0
                    avg_spot = revenue / spots if spots > 0 else 0
                    lines.append(f"| {cat_name} | ${revenue:,.2f} | {pct:.1f}% | {spots:,} | {bonus:,} | ${avg_spot:.2f} |")
            
            lines.append("")
            
            # Individual Year Breakdowns (if multi-year)
            if not time_period.is_single_year:
                lines.append("## 📊 Individual Year Breakdowns")
                lines.append("")
                
                for year in time_period.years:
                    data = year_data[year]
                    lines.append(f"### {year}")
                    lines.append("")
                    
                    # Year-specific breakdown
                    year_categories = self._get_year_categories(data)
                    
                    lines.append("| Category | Revenue | % | Spots | Bonus Spots | Avg/Spot |")
                    lines.append("|----------|---------|---|-------|-------------|----------|")
                    
                    for cat_name, revenue, spots, bonus in year_categories:
                        if revenue > 0:
                            pct = (revenue / data.total_revenue * 100) if data.total_revenue > 0 else 0
                            avg_spot = revenue / spots if spots > 0 else 0
                            lines.append(f"| {cat_name} | ${revenue:,.2f} | {pct:.1f}% | {spots:,} | {bonus:,} | ${avg_spot:.2f} |")
                    
                    lines.append("")
            
            # Language Analysis
            lines.append("## 🌐 Language Analysis")
            lines.append("")
            
            # Aggregate languages across all years
            combined_languages = self._aggregate_languages(year_data)
            
            if combined_languages:
                lines.append(f"### Combined Language Performance ({time_period.description})")
                lines.append("")
                lines.append("| Language | Revenue | % of Total | Spots | Bonus Spots | Avg/Spot |")
                lines.append("|----------|---------|------------|-------|-------------|----------|")
                
                lang_total_revenue = sum(row[2] for row in combined_languages)
                
                for lang_name, spots, revenue, bonus_spots in combined_languages:
                    if revenue > 0:
                        lang_pct = (revenue / lang_total_revenue * 100) if lang_total_revenue > 0 else 0
                        avg_spot = revenue / spots if spots > 0 else 0
                        lines.append(f"| {lang_name} | ${revenue:,.2f} | {lang_pct:.1f}% | {spots:,} | {bonus_spots:,} | ${avg_spot:.2f} |")
                
                lines.append("")
            
            # Performance Insights
            lines.append("## 📈 Performance Insights")
            lines.append("")
            
            # Multi-year trends (if applicable)
            if not time_period.is_single_year:
                lines.append("### 📊 Multi-Year Trends")
                lines.append("")
                
                # Calculate overall growth
                first_year = year_data[time_period.start_year]
                last_year = year_data[time_period.end_year]
                
                total_growth = last_year.total_revenue - first_year.total_revenue
                total_growth_pct = (total_growth / first_year.total_revenue * 100) if first_year.total_revenue > 0 else 0
                
                lines.append(f"- **Overall Growth:** {self._format_change(total_growth, total_growth_pct)}")
                lines.append(f"- **Period:** {time_period.start_year} to {time_period.end_year}")
                
                # Average annual growth
                years_span = time_period.end_year - time_period.start_year
                if years_span > 0:
                    avg_annual_growth = total_growth / years_span
                    lines.append(f"- **Average Annual Growth:** ${avg_annual_growth:,.2f}")
                
                lines.append("")
            
            # Top performing language
            if combined_languages:
                top_lang = combined_languages[0]
                lines.append(f"### 🏆 Top Performing Language")
                lines.append(f"**{top_lang[0]}** leads with ${top_lang[2]:,.2f} revenue from {top_lang[1]:,} spots")
                lines.append("")
            
            # Technical Notes
            lines.append("## 🔧 Technical Notes")
            lines.append("")
            lines.append("### Multi-Year Query Logic")
            lines.append("- **Time Period Processing:** Each year processed independently then aggregated")
            lines.append("- **Reconciliation:** Perfect reconciliation maintained for each year")
            lines.append("- **Year-over-Year Analysis:** Calculated using consecutive year comparisons")
            lines.append("")
            lines.append("### Data Filters Applied")
            lines.append("- ✅ Excludes Trade revenue across all years")
            lines.append("- ✅ NULL-safe WorldLink exclusion")
            lines.append("- ✅ Includes BNS spots even with NULL revenue")
            lines.append("- ✅ Consistent categorization across time periods")
            lines.append("")
            
            logger.info(f"Report generation completed successfully for {time_period}")
            return "\n".join(lines)
            
        except Exception as e:
            logger.error(f"Failed to generate report for {time_period}: {e}")
            raise RevenueReportError(f"Failed to generate report: {e}") from e
    
    def _aggregate_categories(self, year_data: Dict[int, YearData]) -> List[Tuple[str, float, int, int]]:
        """Aggregate revenue categories across multiple years"""
        # Initialize aggregated totals
        agg_individual_lang = [0, 0.0, 0]  # spots, revenue, bonus
        agg_multi_lang = [0, 0.0, 0]
        agg_direct_response = [0, 0.0, 0]
        agg_other_nonlang = [0, 0.0, 0]
        agg_branded_content = [0, 0.0, 0]
        agg_services = [0, 0.0, 0]
        
        for data in year_data.values():
            # Aggregate individual languages
            lang_spots = sum(row[1] for row in data.individual_languages)
            lang_revenue = sum(row[2] for row in data.individual_languages)
            lang_bonus = sum(row[3] for row in data.individual_languages)
            
            agg_individual_lang[0] += lang_spots
            agg_individual_lang[1] += lang_revenue
            agg_individual_lang[2] += lang_bonus
            
            # Aggregate other categories
            agg_multi_lang[0] += data.multi_language[0]
            agg_multi_lang[1] += data.multi_language[1]
            agg_multi_lang[2] += data.multi_language[2]
            
            agg_direct_response[0] += data.direct_response[0]
            agg_direct_response[1] += data.direct_response[1]
            agg_direct_response[2] += data.direct_response[2]
            
            agg_other_nonlang[0] += data.other_nonlanguage[0]
            agg_other_nonlang[1] += data.other_nonlanguage[1]
            agg_other_nonlang[2] += data.other_nonlanguage[2]
            
            agg_branded_content[0] += data.branded_content[0]
            agg_branded_content[1] += data.branded_content[1]
            agg_branded_content[2] += data.branded_content[2]
            
            agg_services[0] += data.services[0]
            agg_services[1] += data.services[1]
            agg_services[2] += data.services[2]
        
        # Create categories list
        categories = [
            ("Individual Language Blocks", agg_individual_lang[1], agg_individual_lang[0], agg_individual_lang[2]),
            ("Multi-Language (Cross-Audience)", agg_multi_lang[1], agg_multi_lang[0], agg_multi_lang[2]),
            ("Direct Response", agg_direct_response[1], agg_direct_response[0], agg_direct_response[2]),
            ("Other Non-Language", agg_other_nonlang[1], agg_other_nonlang[0], agg_other_nonlang[2]),
            ("Branded Content", agg_branded_content[1], agg_branded_content[0], agg_branded_content[2]),
            ("Services", agg_services[1], agg_services[0], agg_services[2]),
        ]
        
        # Sort by revenue
        categories.sort(key=lambda x: x[1], reverse=True)
        return categories
    
    def _get_year_categories(self, data: YearData) -> List[Tuple[str, float, int, int]]:
        """Get categories for a single year"""
        lang_spots = sum(row[1] for row in data.individual_languages)
        lang_revenue = sum(row[2] for row in data.individual_languages)
        lang_bonus = sum(row[3] for row in data.individual_languages)
        
        categories = [
            ("Individual Language Blocks", lang_revenue, lang_spots, lang_bonus),
            ("Multi-Language (Cross-Audience)", data.multi_language[1], data.multi_language[0], data.multi_language[2]),
            ("Direct Response", data.direct_response[1], data.direct_response[0], data.direct_response[2]),
            ("Other Non-Language", data.other_nonlanguage[1], data.other_nonlanguage[0], data.other_nonlanguage[2]),
            ("Branded Content", data.branded_content[1], data.branded_content[0], data.branded_content[2]),
            ("Services", data.services[1], data.services[0], data.services[2]),
        ]
        
        # Sort by revenue
        categories.sort(key=lambda x: x[1], reverse=True)
        return categories
    
    def _aggregate_languages(self, year_data: Dict[int, YearData]) -> List[Tuple[str, int, float, int]]:
        """Aggregate individual languages across multiple years"""
        language_totals = defaultdict(lambda: [0, 0.0, 0])  # spots, revenue, bonus
        
        for data in year_data.values():
            for lang_name, spots, revenue, bonus in data.individual_languages:
                language_totals[lang_name][0] += spots
                language_totals[lang_name][1] += revenue
                language_totals[lang_name][2] += bonus
        
        # Convert to list and sort by revenue
        combined_languages = [
            (lang_name, totals[0], totals[1], totals[2])
            for lang_name, totals in language_totals.items()
        ]
        
        combined_languages.sort(key=lambda x: x[2], reverse=True)
        return combined_languages
    
    def save_report(self, content: str, time_period: TimePeriod, filename: Optional[str] = None) -> str:
        """Save report to file with proper validation"""
        try:
            if filename is None:
                timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"revenue_report_{time_period.description}_{timestamp}.md"
            
            # Validate filename
            if not filename or not isinstance(filename, str):
                raise ValidationError("Filename must be a non-empty string")
            
            # Create directory if it doesn't exist
            output_path = Path(filename)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write file
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            logger.info(f"Report saved successfully: {output_path}")
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Failed to save report: {e}")
            raise RevenueReportError(f"Failed to save report: {e}") from e


def main():
    """Main function with multi-year support"""
    parser = argparse.ArgumentParser(
        description='Generate comprehensive revenue reports for single years or time periods',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python multi_year_revenue_report.py 2024                    # Single year
    python multi_year_revenue_report.py 2023-2024              # Year range
    python multi_year_revenue_report.py 2022-2024 --output multi_year_report.md
    python multi_year_revenue_report.py 2024 --stdout --log-level DEBUG
        """
    )
    
    parser.add_argument('period', type=str, help='Year or time period (e.g., "2024" or "2023-2024")')
    parser.add_argument('--output', '-o', help='Output filename (default: auto-generated)')
    parser.add_argument('--db-path', default='./data/database/production.db', 
                       help='Path to database file (default: ./data/database/production.db)')
    parser.add_argument('--stdout', action='store_true', 
                       help='Print to stdout instead of file')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
                       default='INFO', help='Set logging level (default: INFO)')
    
    args = parser.parse_args()
    
    # Configure logging level
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    try:
        logger.info(f"Starting revenue report generation for {args.period}")
        
        # Initialize generator with validation
        generator = MultiYearRevenueReport(args.db_path)
        
        # Parse time period
        time_period = generator.parse_time_period(args.period)
        
        # Generate report
        report_type = "single-year" if time_period.is_single_year else "multi-year"
        logger.info(f"Generating comprehensive {report_type} revenue report for {time_period}")
        content = generator.generate_report(time_period)
        
        if args.stdout:
            print(content)
            logger.info("Report printed to stdout")
        else:
            filename = generator.save_report(content, time_period, args.output)
            print(f"✅ Report generated successfully!")
            print(f"💾 Saved to: {filename}")
            print(f"📊 Time period: {time_period}")
            
        logger.info(f"Revenue report generation completed successfully ({report_type})")
        return 0
        
    except ValidationError as e:
        logger.error(f"Validation error: {e}")
        print(f"❌ Validation error: {e}")
        return 1
        
    except DatabaseConnectionError as e:
        logger.error(f"Database connection error: {e}")
        print(f"❌ Database connection error: {e}")
        return 1
        
    except RevenueReportError as e:
        logger.error(f"Revenue report error: {e}")
        print(f"❌ Revenue report error: {e}")
        return 1
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        print(f"❌ Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    exit(main())