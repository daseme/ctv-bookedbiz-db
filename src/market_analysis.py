#!/usr/bin/env python3
"""
Market Analysis System - Updated for New Language Assignment System
==================================================================

Updated to use the new language assignment system with spot_language_assignments table
instead of the old time block system.

Key Updates:
- Uses spot_language_assignments table
- Only analyzes spots with direct language mapping
- Based on Internal Ad Sales + COM/BNS spots
- Excludes business rule default English spots
- Combines CHI, CMP, and MSP markets into single CMP category
"""

import sqlite3
import sys
import os
from typing import Dict, List, Set, Any, Optional, Tuple
from dataclasses import dataclass

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


@dataclass
class LanguageResult:
    """Result structure for language analysis"""

    name: str
    revenue: float
    percentage: float
    paid_spots: int
    bonus_spots: int
    total_spots: int
    avg_per_spot: float


@dataclass
class MarketLanguageResult:
    """Result structure for market-language cross-tabulation"""

    language: str
    market: str
    revenue: float
    percentage_of_language: float
    percentage_of_market: float
    paid_spots: int
    bonus_spots: int
    total_spots: int
    avg_per_spot: float


@dataclass
class MarketSummary:
    """Result structure for market summary"""

    market: str
    revenue: float
    percentage_of_total: float
    top_language: str
    top_language_percentage: float
    unique_languages: int


class UpdatedMarketAnalysisEngine:
    """
    Updated market analysis engine using the new language assignment system.
    Focuses on language performance across markets using direct language mappings.
    Now combines CHI, CMP, and MSP markets into single CMP category.
    """

    def __init__(self, db_path: str = "data/database/production.db"):
        self.db_path = db_path
        self.db_connection = None

    def __enter__(self):
        self.db_connection = sqlite3.connect(self.db_path)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.db_connection:
            self.db_connection.close()

    def parse_year_range(self, year_input: str) -> Tuple[List[str], List[str]]:
        """
        Parse year input to handle both single years and ranges.

        Args:
            year_input: "2024" or "2023-2024" or "2022-2024"

        Returns:
            Tuple of (full_years, year_suffixes)
            e.g., (["2023", "2024"], ["23", "24"])
        """
        if "-" in year_input:
            # Handle range like "2023-2024"
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
            # Single year
            full_years = [year_input]
            year_suffixes = [year_input[-2:]]

        return full_years, year_suffixes

    def build_year_filter(self, year_suffixes: List[str]) -> Tuple[str, List[str]]:
        """
        Build SQL filter for multiple year suffixes.

        Args:
            year_suffixes: List of 2-digit year suffixes like ["23", "24"]

        Returns:
            Tuple of (SQL condition, parameters)
        """
        if len(year_suffixes) == 1:
            return "s.broadcast_month LIKE ?", [f"%-{year_suffixes[0]}"]
        else:
            conditions = []
            params = []
            for suffix in year_suffixes:
                conditions.append("s.broadcast_month LIKE ?")
                params.append(f"%-{suffix}")
            return f"({' OR '.join(conditions)})", params

    def get_language_performance_summary(
        self, year_input: str = "2024"
    ) -> List[LanguageResult]:
        """
        Get language performance summary using new language assignment system.
        Only includes spots with direct language mapping.
        """
        full_years, year_suffixes = self.parse_year_range(year_input)
        year_filter, year_params = self.build_year_filter(year_suffixes)

        # Use NEW language assignment system
        query = f"""
        SELECT 
            CASE 
                WHEN l.language_name IN ('Mandarin', 'Cantonese') THEN 'Chinese'
                WHEN l.language_name IN ('Tagalog', 'Filipino') THEN 'Filipino'
                WHEN l.language_name = 'Hmong' THEN 'Hmong'
                WHEN l.language_name IN ('Hindi', 'Punjabi', 'Bengali', 'Gujarati') OR l.language_name = 'South Asian' THEN 'South Asian'
                WHEN l.language_name = 'Vietnamese' THEN 'Vietnamese'
                WHEN l.language_name = 'Korean' THEN 'Korean'
                WHEN l.language_name = 'Japanese' THEN 'Japanese'
                WHEN l.language_name = 'English' THEN 'English'
                ELSE 'Other: ' || COALESCE(l.language_name, 'Unknown')
            END as language,
            SUM(COALESCE(s.gross_rate, 0)) as revenue,
            COUNT(CASE WHEN s.spot_type != 'BNS' OR s.spot_type IS NULL THEN 1 END) as paid_spots,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bonus_spots,
            COUNT(*) as total_spots
        FROM spots s
        JOIN spot_language_assignments sla ON s.spot_id = sla.spot_id
        LEFT JOIN languages l ON UPPER(sla.language_code) = UPPER(l.language_code)
        WHERE {year_filter}
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        -- NEW SYSTEM: Only spots with direct language assignments
        AND sla.assignment_method = 'direct_mapping'
        AND s.revenue_type = 'Internal Ad Sales'
        AND s.spot_type IN ('COM', 'BNS')
        GROUP BY CASE 
            WHEN l.language_name IN ('Mandarin', 'Cantonese') THEN 'Chinese'
            WHEN l.language_name IN ('Tagalog', 'Filipino') THEN 'Filipino'
            WHEN l.language_name = 'Hmong' THEN 'Hmong'
            WHEN l.language_name IN ('Hindi', 'Punjabi', 'Bengali', 'Gujarati') OR l.language_name = 'South Asian' THEN 'South Asian'
            WHEN l.language_name = 'Vietnamese' THEN 'Vietnamese'
            WHEN l.language_name = 'Korean' THEN 'Korean'
            WHEN l.language_name = 'Japanese' THEN 'Japanese'
            WHEN l.language_name = 'English' THEN 'English'
            ELSE 'Other: ' || COALESCE(l.language_name, 'Unknown')
        END
        HAVING SUM(COALESCE(s.gross_rate, 0)) > 0 OR COUNT(*) > 0
        ORDER BY SUM(COALESCE(s.gross_rate, 0)) DESC
        """

        cursor = self.db_connection.cursor()
        cursor.execute(query, year_params)

        results = []
        total_revenue = 0

        for row in cursor.fetchall():
            language, revenue, paid_spots, bonus_spots, total_spots = row
            total_revenue += revenue
            results.append(
                LanguageResult(
                    name=language,
                    revenue=revenue,
                    percentage=0,  # Will be calculated below
                    paid_spots=paid_spots,
                    bonus_spots=bonus_spots,
                    total_spots=total_spots,
                    avg_per_spot=revenue / total_spots if total_spots > 0 else 0,
                )
            )

        # Calculate percentages
        for result in results:
            result.percentage = (
                (result.revenue / total_revenue) * 100 if total_revenue > 0 else 0
            )

        return results

    def get_language_market_breakdown(
        self, year_input: str = "2024"
    ) -> List[MarketLanguageResult]:
        """
        Get language performance broken down by market using new assignment system.
        Combines CHI, CMP, and MSP markets into single CMP category.
        """
        full_years, year_suffixes = self.parse_year_range(year_input)
        year_filter, year_params = self.build_year_filter(year_suffixes)

        # Use NEW language assignment system with market breakdown and CMP consolidation
        query = f"""
        SELECT 
            CASE 
                WHEN l.language_name IN ('Mandarin', 'Cantonese') THEN 'Chinese'
                WHEN l.language_name IN ('Tagalog', 'Filipino') THEN 'Filipino'
                WHEN l.language_name = 'Hmong' THEN 'Hmong'
                WHEN l.language_name IN ('Hindi', 'Punjabi', 'Bengali', 'Gujarati') OR l.language_name = 'South Asian' THEN 'South Asian'
                WHEN l.language_name = 'Vietnamese' THEN 'Vietnamese'
                WHEN l.language_name = 'Korean' THEN 'Korean'
                WHEN l.language_name = 'Japanese' THEN 'Japanese'
                WHEN l.language_name = 'English' THEN 'English'
                ELSE 'Other: ' || COALESCE(l.language_name, 'Unknown')
            END as language,
            CASE 
                WHEN UPPER(COALESCE(s.market_name, 'Unknown Market')) IN ('CHI', 'CMP', 'MSP') THEN 'CMP'
                ELSE COALESCE(s.market_name, 'Unknown Market')
            END as market,
            SUM(COALESCE(s.gross_rate, 0)) as revenue,
            COUNT(CASE WHEN s.spot_type != 'BNS' OR s.spot_type IS NULL THEN 1 END) as paid_spots,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bonus_spots,
            COUNT(*) as total_spots
        FROM spots s
        JOIN spot_language_assignments sla ON s.spot_id = sla.spot_id
        LEFT JOIN languages l ON UPPER(sla.language_code) = UPPER(l.language_code)
        WHERE {year_filter}
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        -- NEW SYSTEM: Only spots with direct language assignments
        AND sla.assignment_method = 'direct_mapping'
        AND s.revenue_type = 'Internal Ad Sales'
        AND s.spot_type IN ('COM', 'BNS')
        GROUP BY 
            CASE 
                WHEN l.language_name IN ('Mandarin', 'Cantonese') THEN 'Chinese'
                WHEN l.language_name IN ('Tagalog', 'Filipino') THEN 'Filipino'
                WHEN l.language_name = 'Hmong' THEN 'Hmong'
                WHEN l.language_name IN ('Hindi', 'Punjabi', 'Bengali', 'Gujarati') OR l.language_name = 'South Asian' THEN 'South Asian'
                WHEN l.language_name = 'Vietnamese' THEN 'Vietnamese'
                WHEN l.language_name = 'Korean' THEN 'Korean'
                WHEN l.language_name = 'Japanese' THEN 'Japanese'
                WHEN l.language_name = 'English' THEN 'English'
                ELSE 'Other: ' || COALESCE(l.language_name, 'Unknown')
            END,
            CASE 
                WHEN UPPER(COALESCE(s.market_name, 'Unknown Market')) IN ('CHI', 'CMP', 'MSP') THEN 'CMP'
                ELSE COALESCE(s.market_name, 'Unknown Market')
            END
        HAVING SUM(COALESCE(s.gross_rate, 0)) > 0 OR COUNT(*) > 0
        ORDER BY 
            CASE 
                WHEN l.language_name IN ('Mandarin', 'Cantonese') THEN 'Chinese'
                WHEN l.language_name IN ('Tagalog', 'Filipino') THEN 'Filipino'
                WHEN l.language_name = 'Hmong' THEN 'Hmong'
                WHEN l.language_name IN ('Hindi', 'Punjabi', 'Bengali', 'Gujarati') OR l.language_name = 'South Asian' THEN 'South Asian'
                WHEN l.language_name = 'Vietnamese' THEN 'Vietnamese'
                WHEN l.language_name = 'Korean' THEN 'Korean'
                WHEN l.language_name = 'Japanese' THEN 'Japanese'
                WHEN l.language_name = 'English' THEN 'English'
                ELSE 'Other: ' || COALESCE(l.language_name, 'Unknown')
            END,
            SUM(COALESCE(s.gross_rate, 0)) DESC
        """

        cursor = self.db_connection.cursor()
        cursor.execute(query, year_params)

        # Build results and calculate totals
        results = []
        language_totals = {}
        market_totals = {}

        for row in cursor.fetchall():
            language, market, revenue, paid_spots, bonus_spots, total_spots = row

            # Track totals for percentage calculations
            if language not in language_totals:
                language_totals[language] = 0
            language_totals[language] += revenue

            if market not in market_totals:
                market_totals[market] = 0
            market_totals[market] += revenue

            results.append(
                MarketLanguageResult(
                    language=language,
                    market=market,
                    revenue=revenue,
                    percentage_of_language=0,  # Will be calculated below
                    percentage_of_market=0,  # Will be calculated below
                    paid_spots=paid_spots,
                    bonus_spots=bonus_spots,
                    total_spots=total_spots,
                    avg_per_spot=revenue / total_spots if total_spots > 0 else 0,
                )
            )

        # Calculate percentages
        for result in results:
            if language_totals[result.language] > 0:
                result.percentage_of_language = (
                    result.revenue / language_totals[result.language]
                ) * 100
            if market_totals[result.market] > 0:
                result.percentage_of_market = (
                    result.revenue / market_totals[result.market]
                ) * 100

        return results

    def get_market_summary(self, year_input: str = "2024") -> List[MarketSummary]:
        """
        Get market summary showing total revenue and top language per market.
        Includes CMP consolidation (CHI + CMP + MSP).
        """
        market_language_data = self.get_language_market_breakdown(year_input)

        # Group by market
        market_data = {}
        total_revenue = 0

        for item in market_language_data:
            market = item.market
            if market not in market_data:
                market_data[market] = {
                    "revenue": 0,
                    "languages": {},
                    "unique_languages": 0,
                }

            market_data[market]["revenue"] += item.revenue
            market_data[market]["languages"][item.language] = item.revenue
            total_revenue += item.revenue

        # Build market summaries
        results = []
        for market, data in market_data.items():
            # Find top language
            top_language = (
                max(data["languages"].items(), key=lambda x: x[1])
                if data["languages"]
                else ("Unknown", 0)
            )

            results.append(
                MarketSummary(
                    market=market,
                    revenue=data["revenue"],
                    percentage_of_total=(data["revenue"] / total_revenue) * 100
                    if total_revenue > 0
                    else 0,
                    top_language=top_language[0],
                    top_language_percentage=(top_language[1] / data["revenue"]) * 100
                    if data["revenue"] > 0
                    else 0,
                    unique_languages=len(data["languages"]),
                )
            )

        # Sort by revenue descending
        results.sort(key=lambda x: x.revenue, reverse=True)

        return results

    def get_assignment_method_breakdown(
        self, year_input: str = "2024"
    ) -> Dict[str, Any]:
        """Get breakdown of assignment methods for context"""
        full_years, year_suffixes = self.parse_year_range(year_input)
        year_filter, year_params = self.build_year_filter(year_suffixes)

        query = f"""
        SELECT 
            sla.assignment_method,
            COUNT(*) as spots,
            SUM(COALESCE(s.gross_rate, 0)) as revenue
        FROM spots s
        JOIN spot_language_assignments sla ON s.spot_id = sla.spot_id
        WHERE {year_filter}
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        GROUP BY sla.assignment_method
        ORDER BY SUM(COALESCE(s.gross_rate, 0)) DESC
        """

        cursor = self.db_connection.cursor()
        cursor.execute(query, year_params)

        results = {}
        total_spots = 0
        total_revenue = 0

        for row in cursor.fetchall():
            method, spots, revenue = row
            results[method] = {"spots": spots, "revenue": revenue}
            total_spots += spots
            total_revenue += revenue

        # Add percentages
        for method, data in results.items():
            data["spot_percentage"] = (
                (data["spots"] / total_spots) * 100 if total_spots > 0 else 0
            )
            data["revenue_percentage"] = (
                (data["revenue"] / total_revenue) * 100 if total_revenue > 0 else 0
            )

        return {
            "methods": results,
            "total_spots": total_spots,
            "total_revenue": total_revenue,
        }

    def generate_market_analysis_report(self, year_input: str = "2024") -> str:
        """Generate comprehensive market analysis report using new system with CMP consolidation"""

        # Parse year range for display
        full_years, year_suffixes = self.parse_year_range(year_input)
        year_display = (
            f"{full_years[0]}-{full_years[-1]}"
            if len(full_years) > 1
            else full_years[0]
        )

        # Get all analyses
        language_summary = self.get_language_performance_summary(year_input)
        market_breakdown = self.get_language_market_breakdown(year_input)
        market_summary = self.get_market_summary(year_input)
        assignment_breakdown = self.get_assignment_method_breakdown(year_input)

        # Generate tables
        language_table = self._format_language_summary_table(
            language_summary, year_display
        )
        market_breakdown_table = self._format_market_breakdown_table(
            market_breakdown, year_display
        )
        market_summary_table = self._format_market_summary_table(
            market_summary, year_display
        )
        assignment_table = self._format_assignment_method_table(
            assignment_breakdown, year_display
        )

        # Generate insights
        insights = self._generate_market_insights(
            language_summary, market_breakdown, market_summary
        )

        # Generate report
        return f"""# Market Analysis Report - {year_display}

*Generated with NEW language assignment system using spot_language_assignments table*
*Markets: CHI, CMP, and MSP consolidated into CMP category*

## 📊 Analysis Overview

- **Years Analyzed**: {", ".join(full_years)}
- **Total Languages**: {len(language_summary)}
- **Total Markets**: {len(market_summary)}
- **Market Consolidation**: CHI + CMP + MSP → CMP
- **Data Source**: NEW language assignment system (spot_language_assignments table)
- **Analysis Focus**: Direct language mapping only (assignment_method = 'direct_mapping')
- **Scope**: Internal Ad Sales + COM/BNS spots only

{language_table}

{market_breakdown_table}

{market_summary_table}

{assignment_table}

{insights}

{self._generate_updated_methodology()}
"""

    def _format_language_summary_table(
        self, results: List[LanguageResult], year_display: str
    ) -> str:
        """Format language summary table"""

        if not results:
            return f"""## 🌐 Language Performance Summary
### Direct Language Mapping Performance ({year_display})

*No direct language mapping data found for the specified year(s)*

💡 This could mean:
- Language assignments haven't been processed yet
- No Internal Ad Sales spots have direct language targeting
- Run language assignment processing first: `python cli_01_language_assignment.py --process-all-categories`
"""

        # Calculate totals
        total_revenue = sum(r.revenue for r in results)
        total_paid_spots = sum(r.paid_spots for r in results)
        total_bonus_spots = sum(r.bonus_spots for r in results)
        total_all_spots = sum(r.total_spots for r in results)
        total_avg_per_spot = (
            total_revenue / total_all_spots if total_all_spots > 0 else 0
        )

        # Build the table
        table = f"""## 🌐 Language Performance Summary
### Direct Language Mapping Performance ({year_display})
| Language | Revenue | % of Total | Paid Spots | BNS Spots | Total Spots | Avg/Spot |
|----------|---------|------------|-----------|-----------|-------------|----------|
"""

        for result in results:
            table += f"| {result.name} | ${result.revenue:,.2f} | {result.percentage:.1f}% | {result.paid_spots:,} | {result.bonus_spots:,} | {result.total_spots:,} | ${result.avg_per_spot:.2f} |\n"

        # Add total row
        table += "|----------|---------|------------|-----------|-----------|-------------|----------|\n"
        table += f"| **TOTAL** | **${total_revenue:,.2f}** | **100.0%** | **{total_paid_spots:,}** | **{total_bonus_spots:,}** | **{total_all_spots:,}** | **${total_avg_per_spot:.2f}** |\n"

        return table

    def _format_market_breakdown_table(
        self, results: List[MarketLanguageResult], year_display: str
    ) -> str:
        """Format market breakdown table"""

        if not results:
            return f"""## 📍 Language Performance by Market
### Revenue Distribution Across Markets ({year_display})

*No market breakdown data found for the specified year(s)*
"""

        # Group by language for display
        language_data = {}
        market_totals = {}

        for result in results:
            if result.language not in language_data:
                language_data[result.language] = {}
            language_data[result.language][result.market] = result

            if result.market not in market_totals:
                market_totals[result.market] = 0
            market_totals[result.market] += result.revenue

        # Get sorted markets by revenue
        sorted_markets = sorted(market_totals.items(), key=lambda x: x[1], reverse=True)
        market_order = [market for market, _ in sorted_markets]

        # Build the table
        table = (
            f"""## 📍 Language Performance by Market
### Revenue Distribution Across Markets ({year_display})
*Note: CHI, CMP, and MSP markets consolidated into CMP category*

| Language | """
            + " | ".join(market_order)
            + """ | Total |
|----------|"""
            + "|".join(["-" * max(len(market), 8) for market in market_order])
            + """|-------|
"""
        )

        # Calculate language totals for percentage calculations
        language_totals = {}
        for language, markets in language_data.items():
            language_totals[language] = sum(
                result.revenue for result in markets.values()
            )

        # Sort languages by total revenue
        sorted_languages = sorted(
            language_totals.items(), key=lambda x: x[1], reverse=True
        )

        for language, total_revenue in sorted_languages:
            row = f"| {language} | "

            for market in market_order:
                if market in language_data[language]:
                    result = language_data[language][market]
                    row += f"${result.revenue:,.0f} ({result.percentage_of_language:.1f}%) | "
                else:
                    row += "- | "

            row += f"${total_revenue:,.0f} |"
            table += row + "\n"

        # Add total row
        table += (
            "|----------|"
            + "|".join(["-" * max(len(market), 8) for market in market_order])
            + "|-------|\n"
        )
        total_row = "| **TOTAL** | "

        for market in market_order:
            total_row += f"**${market_totals[market]:,.0f}** | "

        grand_total = sum(market_totals.values())
        total_row += f"**${grand_total:,.0f}** |"
        table += total_row + "\n"

        return table

    def _format_market_summary_table(
        self, results: List[MarketSummary], year_display: str
    ) -> str:
        """Format market summary table"""

        if not results:
            return f"""## 📊 Market Share Analysis
### Market Performance Summary ({year_display})

*No market summary data found for the specified year(s)*
"""

        table = f"""## 📊 Market Share Analysis
### Market Performance Summary ({year_display})
*Note: CMP includes consolidated CHI + CMP + MSP markets*

| Market | Revenue | % of Total | Top Language | Top Lang % | Languages |
|--------|---------|------------|--------------|------------|-----------|
"""

        for result in results:
            table += f"| {result.market} | ${result.revenue:,.0f} | {result.percentage_of_total:.1f}% | {result.top_language} | {result.top_language_percentage:.1f}% | {result.unique_languages} |\n"

        return table

    def _format_assignment_method_table(
        self, assignment_data: Dict[str, Any], year_display: str
    ) -> str:
        """Format assignment method breakdown table"""

        methods = assignment_data.get("methods", {})
        total_spots = assignment_data.get("total_spots", 0)
        total_revenue = assignment_data.get("total_revenue", 0)

        if not methods:
            return f"""## 🔧 Assignment Method Context
### Language Assignment Methods ({year_display})

*No assignment method data found*
"""

        table = f"""## 🔧 Assignment Method Context
### Language Assignment Methods ({year_display})
| Assignment Method | Spots | % of Spots | Revenue | % of Revenue |
|-------------------|-------|------------|---------|-------------|
"""

        # Sort by revenue descending
        sorted_methods = sorted(
            methods.items(), key=lambda x: x[1]["revenue"], reverse=True
        )

        for method, data in sorted_methods:
            method_display = {
                "direct_mapping": "Direct Language Mapping",
                "business_rule_default_english": "Business Rule Default English",
                "auto_default_com_bb": "Business Rule Default English (COM/BB)",
                "default_english": "Default English (Fallback)",
                "business_review_required": "Business Review Required",
                "undetermined_flagged": "Undetermined Language",
                "invalid_code_flagged": "Invalid Language Code",
                "error_fallback": "Error Fallback",
            }.get(method, method)

            table += f"| {method_display} | {data['spots']:,} | {data['spot_percentage']:.1f}% | ${data['revenue']:,.2f} | {data['revenue_percentage']:.1f}% |\n"

        # Add total row
        table += "|-------------------|-------|------------|---------|-------------|\n"
        table += f"| **TOTAL** | **{total_spots:,}** | **100.0%** | **${total_revenue:,.2f}** | **100.0%** |\n"

        # Add note about what's included in market analysis
        direct_mapping_data = methods.get("direct_mapping", {"spots": 0, "revenue": 0})
        table += f"""

### Market Analysis Scope
- **Market Analysis Includes**: Only "Direct Language Mapping" spots ({direct_mapping_data["spots"]:,} spots, ${direct_mapping_data["revenue"]:,.2f})
- **Excluded from Market Analysis**: Business rule defaults, review required, undetermined languages
- **Reason**: Market analysis focuses on spots with confirmed language targeting
"""

        return table

    def _generate_market_insights(
        self,
        language_summary: List[LanguageResult],
        market_breakdown: List[MarketLanguageResult],
        market_summary: List[MarketSummary],
    ) -> str:
        """Generate key insights from the market analysis"""

        if not language_summary or not market_breakdown or not market_summary:
            return """## 📊 Key Insights

*Insufficient data to generate insights - no direct language mapping spots found*

💡 **Next Steps:**
- Run language assignment processing: `python cli_01_language_assignment.py --process-all-categories`
- Verify Internal Ad Sales spots have language codes
- Check that language assignments completed successfully
"""

        # Calculate key metrics
        total_revenue = sum(r.revenue for r in language_summary)
        top_language = language_summary[0] if language_summary else None
        top_market = market_summary[0] if market_summary else None

        # Find CMP performance specifically
        cmp_market = next((m for m in market_summary if m.market == "CMP"), None)
        cmp_performance = (
            f"CMP (CHI+CMP+MSP): ${cmp_market.revenue:,.0f} ({cmp_market.percentage_of_total:.1f}%)"
            if cmp_market
            else "CMP data not found"
        )

        # Find market concentration
        top_2_markets_revenue = sum(r.revenue for r in market_summary[:2])
        market_concentration = (
            (top_2_markets_revenue / total_revenue) * 100 if total_revenue > 0 else 0
        )

        # Count dominant languages per market
        language_dominance = {}
        for market in market_summary:
            top_lang = market.top_language
            if top_lang not in language_dominance:
                language_dominance[top_lang] = 0
            language_dominance[top_lang] += 1

        dominant_language = (
            max(language_dominance.items(), key=lambda x: x[1])
            if language_dominance
            else ("Unknown", 0)
        )

        insights = f"""## 📊 Key Insights

### Language-Targeted Market Performance
- **Total Revenue**: ${total_revenue:,.2f} from direct language targeting campaigns
- **Top Language**: {top_language.name if top_language else "Unknown"} with ${top_language.revenue:,.2f} ({top_language.percentage:.1f}%)
- **Top Market**: {top_market.market if top_market else "Unknown"} with ${top_market.revenue:,.2f} ({top_market.percentage_of_total:.1f}%)
- **{cmp_performance}**
- **Market Concentration**: Top 2 markets account for {market_concentration:.1f}% of language-targeted revenue

### Geographic Language Distribution
- **Active Markets**: {len(market_summary)} markets with direct language targeting
- **Language Leadership**: {dominant_language[0]} dominates in {dominant_language[1]} market(s)
- **Market Diversity**: Average of {sum(m.unique_languages for m in market_summary) / len(market_summary):.1f} languages per market

### Strategic Language Patterns
- **Language Portfolio**: {len(language_summary)} distinct language groups with direct targeting
- **Revenue Concentration**: Top 3 languages account for {sum(r.percentage for r in language_summary[:3]):.1f}% of language-targeted revenue
- **Market Penetration**: Languages active across {len(set(mb.market for mb in market_breakdown))} different markets

### CMP Market Consolidation Impact
- **Market Grouping**: CHI, CMP, and MSP markets now consolidated into single CMP category
- **Geographic Clarity**: Simplified market analysis with combined Midwest markets
- **Revenue Aggregation**: CMP represents combined performance across Chicago, core CMP, and Minneapolis-St. Paul regions

### NEW SYSTEM Benefits
- **Data Quality**: Only includes confirmed language targeting (direct_mapping)
- **Business Clarity**: Excludes business rule defaults and uncertain assignments
- **Assignment Transparency**: Shows how each spot was categorized
- **Better Accuracy**: Removes ambiguous time block associations

### Business Intelligence
- **Geographic Strategy**: {"High" if market_concentration > 70 else "Moderate" if market_concentration > 50 else "Balanced"} geographic concentration
- **Language Focus**: {"Specialized" if language_summary[0].percentage > 40 else "Diversified" if language_summary[0].percentage < 25 else "Moderate"} language concentration
- **Market Reach**: {"Excellent" if len(market_summary) > 8 else "Good" if len(market_summary) > 5 else "Developing"} market penetration
"""

        return insights

    def _generate_updated_methodology(self) -> str:
        """Generate updated methodology section"""
        return """---

## 📋 Updated Market Analysis Methodology

### NEW Language Assignment System
- **Data Source**: `spot_language_assignments` table (not time blocks)
- **Assignment Method**: Only `assignment_method = 'direct_mapping'` spots included
- **Business Rules**: Based on revenue_type + spot_type combinations
- **Quality Control**: Confidence levels and review flagging included

### Market Consolidation (NEW)
- **CMP Market**: Combines CHI + CMP + MSP markets into single "CMP" category
- **Rationale**: Unified view of Midwest market performance
- **SQL Implementation**: `CASE WHEN UPPER(market_name) IN ('CHI', 'CMP', 'MSP') THEN 'CMP'`
- **Other Markets**: Remain unchanged

### Data Scope (Updated)
- **Included Spots**: Internal Ad Sales + COM/BNS spots with direct language assignments
- **Language Source**: `spots.language_code` mapped to `languages` table
- **Market Source**: `spots.market_name` field with CMP consolidation
- **Excluded**: Business rule defaults, review required, undetermined languages

### Analysis Components


#### Assignment Method Context
- **Purpose**: Shows how spots were categorized in new system
- **Transparency**: Breakdown by assignment method
- **Scope**: Market analysis uses only direct_mapping spots

### Business Rules Applied
1. **Direct Response Sales** → Default English (excluded from market analysis)
2. **Paid Programming** → Default English (excluded from market analysis)
3. **Branded Content** → Default English (excluded from market analysis)
4. **Internal Ad Sales + COM/BNS** → Language assignment required → Market analysis
5. **Other Combinations** → Review required (excluded from market analysis)

### Multiyear Support
- **Year Ranges**: Supports "2023-2024" format for multiyear analysis
- **Aggregation**: Combines data across all years in range
- **Consistency**: Same assignment and consolidation rules applied to all years

---

*Market Analysis System v2.1 - Updated for NEW Language Assignment System + CMP Market Consolidation*"""


def main():
    """Main function with command line interface"""
    import argparse

    parser = argparse.ArgumentParser(
        description="Updated Market Analysis System - NEW Language Assignment System + CMP Market Consolidation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Updated Market Analysis Examples:
  # Single year analysis with CMP consolidation
  python market_analysis.py --year 2024
  
  # Two year analysis
  python market_analysis.py --year 2023-2024
  
  # Save report to file
  python market_analysis.py --year 2024 --output market_report.md
  
  # Language summary only
  python market_analysis.py --year 2024 --language-summary-only
  
  # Assignment method breakdown
  python market_analysis.py --year 2024 --assignment-methods-only

Note: CHI, CMP, and MSP markets are automatically consolidated into CMP category
        """,
    )

    parser.add_argument(
        "--year",
        default="2024",
        help="Year to analyze - supports single year (2024) or range (2023-2024)",
    )

    parser.add_argument(
        "--output", metavar="FILE", help="Save report to file (e.g., market_report.md)"
    )

    parser.add_argument(
        "--db-path",
        default="data/database/production.db",
        help="Database path (default: data/database/production.db)",
    )

    parser.add_argument(
        "--language-summary-only",
        action="store_true",
        help="Show only language summary",
    )

    parser.add_argument(
        "--market-breakdown-only",
        action="store_true",
        help="Show only market breakdown",
    )

    parser.add_argument(
        "--assignment-methods-only",
        action="store_true",
        help="Show only assignment method breakdown",
    )

    args = parser.parse_args()

    try:
        with UpdatedMarketAnalysisEngine(args.db_path) as engine:
            if args.language_summary_only:
                # Show language summary only
                language_summary = engine.get_language_performance_summary(args.year)
                full_years, _ = engine.parse_year_range(args.year)
                year_display = (
                    f"{full_years[0]}-{full_years[-1]}"
                    if len(full_years) > 1
                    else full_years[0]
                )

                print(
                    "🌐 Language Performance Summary (NEW SYSTEM + CMP CONSOLIDATION):"
                )
                print("=" * 65)
                print(f"Years: {year_display}")
                print(f"Total Languages: {len(language_summary)}")
                print("Markets: CHI, CMP, MSP → CMP")

                if language_summary:
                    total_revenue = sum(r.revenue for r in language_summary)
                    print(f"Total Revenue: ${total_revenue:,.2f}")
                    print()

                    for lang in language_summary:
                        print(
                            f"{lang.name}: ${lang.revenue:,.2f} ({lang.percentage:.1f}%) - {lang.total_spots:,} spots"
                        )
                else:
                    print("No direct language mapping data found.")
                    print("💡 Run language assignment processing first.")

            elif args.market_breakdown_only:
                # Show market breakdown only
                market_breakdown = engine.get_language_market_breakdown(args.year)
                full_years, _ = engine.parse_year_range(args.year)
                year_display = (
                    f"{full_years[0]}-{full_years[-1]}"
                    if len(full_years) > 1
                    else full_years[0]
                )

                print(
                    "📍 Language Performance by Market (NEW SYSTEM + CMP CONSOLIDATION):"
                )
                print("=" * 70)
                print(f"Years: {year_display}")
                print(f"Total Language-Market Combinations: {len(market_breakdown)}")
                print("Markets: CHI, CMP, MSP → CMP")
                print()

                for item in market_breakdown:
                    print(
                        f"{item.language} in {item.market}: ${item.revenue:,.2f} ({item.percentage_of_language:.1f}% of language)"
                    )

            elif args.assignment_methods_only:
                # Show assignment method breakdown only
                assignment_breakdown = engine.get_assignment_method_breakdown(args.year)
                full_years, _ = engine.parse_year_range(args.year)
                year_display = (
                    f"{full_years[0]}-{full_years[-1]}"
                    if len(full_years) > 1
                    else full_years[0]
                )

                print("🔧 Assignment Method Breakdown (NEW SYSTEM):")
                print("=" * 50)
                print(f"Years: {year_display}")
                print(f"Total Spots: {assignment_breakdown['total_spots']:,}")
                print(f"Total Revenue: ${assignment_breakdown['total_revenue']:,.2f}")
                print()

                for method, data in assignment_breakdown["methods"].items():
                    print(
                        f"{method}: {data['spots']:,} spots (${data['revenue']:,.2f}) - {data['spot_percentage']:.1f}%"
                    )

            else:
                # Generate full report
                report = engine.generate_market_analysis_report(args.year)

                if args.output:
                    # Create directory if it doesn't exist
                    import os

                    os.makedirs(
                        os.path.dirname(args.output), exist_ok=True
                    ) if os.path.dirname(args.output) else None

                    with open(args.output, "w") as f:
                        f.write(report)

                    # Parse year for display
                    full_years, _ = engine.parse_year_range(args.year)
                    year_display = (
                        f"{full_years[0]}-{full_years[-1]}"
                        if len(full_years) > 1
                        else full_years[0]
                    )

                    print(f"✅ Updated market analysis report saved to {args.output}")
                    print(f"📅 Years analyzed: {year_display}")
                    print(f"📄 File size: {os.path.getsize(args.output):,} bytes")
                    print(f"🔧 System: NEW language assignment system")
                    print(f"📍 Markets: CHI + CMP + MSP → CMP consolidated")
                else:
                    print(report)

    except ValueError as e:
        print(f"❌ Input Error: {str(e)}")
        print("💡Help: Use format like: --year 2024 or --year 2023-2024")
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
