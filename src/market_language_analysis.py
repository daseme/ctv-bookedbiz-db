#!/usr/bin/env python3
"""
Market Analysis System - MULTIYEAR SUPPORT
==========================================

This system provides market-focused analysis showing:
1. Language performance summary (first grid)
2. Language performance across markets (second grid)

MULTIYEAR FEATURES:
==================

1. **Multiyear Arguments**: Support for "2023-2024", "2022-2023", etc.
2. **Combined Analysis**: Aggregates market data across multiple years
3. **Geographic Insights**: Shows language distribution across markets
4. **Market Share Analysis**: Identifies dominant languages per market
5. **Flexible Input**: Single year (2024) or range (2023-2024)

Usage Examples:
  python ./src/market_analysis.py --year 2024              # Single year
  python ./src/market_analysis.py --year 2023-2024         # Two years
  python ./src/market_analysis.py --year 2022-2024         # Three years
  python ./src/market_analysis.py --year 2023-2024 --output market_report.md

Save this as: src/market_analysis.py
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

class MarketAnalysisEngine:
    """
    Market analysis engine with multiyear support,
    focusing on language performance across markets.
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
        if '-' in year_input:
            # Handle range like "2023-2024"
            start_year, end_year = year_input.split('-')
            start_year = int(start_year)
            end_year = int(end_year)
            
            if start_year > end_year:
                raise ValueError(f"Start year {start_year} cannot be greater than end year {end_year}")
            
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
    
    def get_language_performance_summary(self, year_input: str = "2024") -> List[LanguageResult]:
        """
        Get language performance summary (first grid).
        This is similar to the second grid in unified analysis.
        """
        full_years, year_suffixes = self.parse_year_range(year_input)
        
        # Get individual language spots (same logic as unified analysis)
        individual_lang_spots = self._get_individual_language_spot_ids(year_suffixes)
        
        if not individual_lang_spots:
            return []
        
        spot_ids_list = list(individual_lang_spots)
        placeholders = ','.join(['?' for _ in spot_ids_list])
        
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
        LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        LEFT JOIN language_blocks lb ON slb.block_id = lb.block_id
        LEFT JOIN languages l ON lb.language_id = l.language_id
        WHERE s.spot_id IN ({placeholders})
        AND slb.block_id IS NOT NULL
        AND slb.campaign_type = 'language_specific'
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
        cursor.execute(query, spot_ids_list)
        
        results = []
        total_revenue = 0
        
        for row in cursor.fetchall():
            language, revenue, paid_spots, bonus_spots, total_spots = row
            total_revenue += revenue
            results.append(LanguageResult(
                name=language,
                revenue=revenue,
                percentage=0,  # Will be calculated below
                paid_spots=paid_spots,
                bonus_spots=bonus_spots,
                total_spots=total_spots,
                avg_per_spot=revenue / total_spots if total_spots > 0 else 0
            ))
        
        # Calculate percentages
        for result in results:
            result.percentage = (result.revenue / total_revenue) * 100 if total_revenue > 0 else 0
        
        return results
    
    def get_language_market_breakdown(self, year_input: str = "2024") -> List[MarketLanguageResult]:
        """
        Get language performance broken down by market (second grid).
        This shows how each language performs across different markets.
        """
        full_years, year_suffixes = self.parse_year_range(year_input)
        
        # Get individual language spots
        individual_lang_spots = self._get_individual_language_spot_ids(year_suffixes)
        
        if not individual_lang_spots:
            return []
        
        spot_ids_list = list(individual_lang_spots)
        placeholders = ','.join(['?' for _ in spot_ids_list])
        
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
            COALESCE(m.market_code, 'Unknown') as market,
            SUM(COALESCE(s.gross_rate, 0)) as revenue,
            COUNT(CASE WHEN s.spot_type != 'BNS' OR s.spot_type IS NULL THEN 1 END) as paid_spots,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bonus_spots,
            COUNT(*) as total_spots
        FROM spots s
        LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        LEFT JOIN language_blocks lb ON slb.block_id = lb.block_id
        LEFT JOIN languages l ON lb.language_id = l.language_id
        LEFT JOIN markets m ON s.market_id = m.market_id
        WHERE s.spot_id IN ({placeholders})
        AND slb.block_id IS NOT NULL
        AND slb.campaign_type = 'language_specific'
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
            COALESCE(m.market_code, 'Unknown')
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
        cursor.execute(query, spot_ids_list)
        
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
            
            results.append(MarketLanguageResult(
                language=language,
                market=market,
                revenue=revenue,
                percentage_of_language=0,  # Will be calculated below
                percentage_of_market=0,   # Will be calculated below
                paid_spots=paid_spots,
                bonus_spots=bonus_spots,
                total_spots=total_spots,
                avg_per_spot=revenue / total_spots if total_spots > 0 else 0
            ))
        
        # Calculate percentages
        for result in results:
            if language_totals[result.language] > 0:
                result.percentage_of_language = (result.revenue / language_totals[result.language]) * 100
            if market_totals[result.market] > 0:
                result.percentage_of_market = (result.revenue / market_totals[result.market]) * 100
        
        return results
    
    def get_market_summary(self, year_input: str = "2024") -> List[MarketSummary]:
        """
        Get market summary showing total revenue and top language per market.
        """
        market_language_data = self.get_language_market_breakdown(year_input)
        
        # Group by market
        market_data = {}
        total_revenue = 0
        
        for item in market_language_data:
            market = item.market
            if market not in market_data:
                market_data[market] = {
                    'revenue': 0,
                    'languages': {},
                    'unique_languages': 0
                }
            
            market_data[market]['revenue'] += item.revenue
            market_data[market]['languages'][item.language] = item.revenue
            total_revenue += item.revenue
        
        # Build market summaries
        results = []
        for market, data in market_data.items():
            # Find top language
            top_language = max(data['languages'].items(), key=lambda x: x[1]) if data['languages'] else ('Unknown', 0)
            
            results.append(MarketSummary(
                market=market,
                revenue=data['revenue'],
                percentage_of_total=(data['revenue'] / total_revenue) * 100 if total_revenue > 0 else 0,
                top_language=top_language[0],
                top_language_percentage=(top_language[1] / data['revenue']) * 100 if data['revenue'] > 0 else 0,
                unique_languages=len(data['languages'])
            ))
        
        # Sort by revenue descending
        results.sort(key=lambda x: x.revenue, reverse=True)
        
        return results
    
    def _get_individual_language_spot_ids(self, year_suffixes: List[str]) -> Set[int]:
        """Get Individual Language spot IDs for multiple years (same logic as unified analysis)"""
        year_filter, year_params = self.build_year_filter(year_suffixes)
        
        query = f"""
        SELECT DISTINCT s.spot_id
        FROM spots s
        LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        LEFT JOIN agencies a ON s.agency_id = a.agency_id
        WHERE {year_filter}
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
        AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
        AND s.revenue_type != 'Paid Programming'
        AND NOT (slb.spot_id IS NULL AND s.spot_type = 'PRD')
        AND NOT (slb.spot_id IS NULL AND s.spot_type = 'SVC')
        AND slb.campaign_type = 'language_specific'
        """
        
        cursor = self.db_connection.cursor()
        cursor.execute(query, year_params)
        return set(row[0] for row in cursor.fetchall())
    
    def generate_market_analysis_report(self, year_input: str = "2024") -> str:
        """Generate comprehensive market analysis report"""
        
        # Parse year range for display
        full_years, year_suffixes = self.parse_year_range(year_input)
        year_display = f"{full_years[0]}-{full_years[-1]}" if len(full_years) > 1 else full_years[0]
        
        # Get all analyses
        language_summary = self.get_language_performance_summary(year_input)
        market_breakdown = self.get_language_market_breakdown(year_input)
        market_summary = self.get_market_summary(year_input)
        
        # Generate first grid (language performance summary)
        language_table = self._format_language_summary_table(language_summary, year_display)
        
        # Generate second grid (language by market breakdown)
        market_breakdown_table = self._format_market_breakdown_table(market_breakdown, year_display)
        
        # Generate market summary
        market_summary_table = self._format_market_summary_table(market_summary, year_display)
        
        # Generate insights
        insights = self._generate_market_insights(language_summary, market_breakdown, market_summary)
        
        # Generate report
        return f"""# Market Analysis Report - {year_display}

*Generated with multiyear support, focusing on language performance across markets*

## 📊 Analysis Overview

- **Years Analyzed**: {', '.join(full_years)}
- **Total Languages**: {len(language_summary)}
- **Total Markets**: {len(market_summary)}
- **Analysis Focus**: Individual Language Blocks only (excludes ROS, Multi-Language, Direct Response, etc.)

{language_table}

{market_breakdown_table}

{market_summary_table}

{insights}

{self._generate_market_methodology()}
"""
    
    def _format_language_summary_table(self, results: List[LanguageResult], year_display: str) -> str:
        """Format language summary table (first grid)"""
        
        if not results:
            return """## 🌐 Language Performance Summary
### Individual Language Performance ({year_display})

*No individual language data found for the specified year(s)*
"""
        
        # Calculate totals
        total_revenue = sum(r.revenue for r in results)
        total_paid_spots = sum(r.paid_spots for r in results)
        total_bonus_spots = sum(r.bonus_spots for r in results)
        total_all_spots = sum(r.total_spots for r in results)
        total_avg_per_spot = total_revenue / total_all_spots if total_all_spots > 0 else 0
        
        # Build the table
        table = f"""## 🌐 Language Performance Summary
### Individual Language Performance ({year_display})
| Language | Revenue | % of Total | Paid Spots | BNS Spots | Total Spots | Avg/Spot |
|----------|---------|------------|-----------|-----------|-------------|----------|
"""
        
        for result in results:
            table += f"| {result.name} | ${result.revenue:,.2f} | {result.percentage:.1f}% | {result.paid_spots:,} | {result.bonus_spots:,} | {result.total_spots:,} | ${result.avg_per_spot:.2f} |\n"
        
        # Add total row
        table += "|----------|---------|------------|-----------|-----------|-------------|----------|\n"
        table += f"| **TOTAL** | **${total_revenue:,.2f}** | **100.0%** | **{total_paid_spots:,}** | **{total_bonus_spots:,}** | **{total_all_spots:,}** | **${total_avg_per_spot:.2f}** |\n"
        
        return table
    
    def _format_market_breakdown_table(self, results: List[MarketLanguageResult], year_display: str) -> str:
        """Format market breakdown table (second grid)"""
        
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
        table = f"""## 📍 Language Performance by Market
### Revenue Distribution Across Markets ({year_display})
| Language | """ + " | ".join(market_order) + """ | Total |
|----------|""" + "|".join(["-" * max(len(market), 8) for market in market_order]) + """|-------|
"""
        
        # Calculate language totals for percentage calculations
        language_totals = {}
        for language, markets in language_data.items():
            language_totals[language] = sum(result.revenue for result in markets.values())
        
        # Sort languages by total revenue
        sorted_languages = sorted(language_totals.items(), key=lambda x: x[1], reverse=True)
        
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
        table += "|----------|" + "|".join(["-" * max(len(market), 8) for market in market_order]) + "|-------|\n"
        total_row = "| **TOTAL** | "
        
        for market in market_order:
            total_row += f"**${market_totals[market]:,.0f}** | "
        
        grand_total = sum(market_totals.values())
        total_row += f"**${grand_total:,.0f}** |"
        table += total_row + "\n"
        
        return table
    
    def _format_market_summary_table(self, results: List[MarketSummary], year_display: str) -> str:
        """Format market summary table"""
        
        if not results:
            return f"""## 📊 Market Share Analysis
### Market Performance Summary ({year_display})

*No market summary data found for the specified year(s)*
"""
        
        table = f"""## 📊 Market Share Analysis
### Market Performance Summary ({year_display})
| Market | Revenue | % of Total | Top Language | Top Lang % | Languages |
|--------|---------|------------|--------------|------------|-----------|
"""
        
        for result in results:
            table += f"| {result.market} | ${result.revenue:,.0f} | {result.percentage_of_total:.1f}% | {result.top_language} | {result.top_language_percentage:.1f}% | {result.unique_languages} |\n"
        
        return table
    
    def _generate_market_insights(self, language_summary: List[LanguageResult], 
                                 market_breakdown: List[MarketLanguageResult],
                                 market_summary: List[MarketSummary]) -> str:
        """Generate key insights from the market analysis"""
        
        if not language_summary or not market_breakdown or not market_summary:
            return """## 📊 Key Insights

*Insufficient data to generate insights*
"""
        
        # Calculate key metrics
        total_revenue = sum(r.revenue for r in language_summary)
        top_language = language_summary[0] if language_summary else None
        top_market = market_summary[0] if market_summary else None
        
        # Find market concentration
        top_2_markets_revenue = sum(r.revenue for r in market_summary[:2])
        market_concentration = (top_2_markets_revenue / total_revenue) * 100 if total_revenue > 0 else 0
        
        # Count dominant languages per market
        language_dominance = {}
        for market in market_summary:
            top_lang = market.top_language
            if top_lang not in language_dominance:
                language_dominance[top_lang] = 0
            language_dominance[top_lang] += 1
        
        dominant_language = max(language_dominance.items(), key=lambda x: x[1]) if language_dominance else ('Unknown', 0)
        
        insights = f"""## 📊 Key Insights

### Market Performance Highlights
- **Total Revenue**: ${total_revenue:,.2f} across all individual language campaigns
- **Top Language**: {top_language.name if top_language else 'Unknown'} with ${top_language.revenue:,.2f} ({top_language.percentage:.1f}%)
- **Top Market**: {top_market.market if top_market else 'Unknown'} with ${top_market.revenue:,.2f} ({top_market.percentage_of_total:.1f}%)
- **Market Concentration**: Top 2 markets account for {market_concentration:.1f}% of total revenue

### Geographic Distribution
- **Total Markets**: {len(market_summary)} markets with individual language campaigns
- **Language Dominance**: {dominant_language[0]} is the top language in {dominant_language[1]} market(s)
- **Market Diversity**: Average of {sum(m.unique_languages for m in market_summary) / len(market_summary):.1f} languages per market

### Language Performance Patterns
- **Language Diversity**: {len(language_summary)} distinct language groups active
- **Revenue Distribution**: Top 3 languages account for {sum(r.percentage for r in language_summary[:3]):.1f}% of total revenue
- **Market Reach**: Languages appear across {len(set(mb.market for mb in market_breakdown))} different markets

### Strategic Observations
- **Geographic Concentration**: {"High" if market_concentration > 70 else "Moderate" if market_concentration > 50 else "Low"} concentration in top markets
- **Language Specialization**: {"High" if language_summary[0].percentage > 40 else "Moderate" if language_summary[0].percentage > 25 else "Balanced"} language market specialization
- **Market Penetration**: {"Excellent" if len(market_summary) > 8 else "Good" if len(market_summary) > 5 else "Limited"} market penetration across regions
"""
        
        return insights
    
    def _generate_market_methodology(self) -> str:
        """Generate methodology section"""
        return """---

## 📋 Market Analysis Methodology

### Data Scope
- **Focus**: Individual Language Blocks only (language-specific campaigns)
- **Exclusions**: ROS, Multi-Language, Direct Response, Paid Programming, Services, Branded Content, Packages
- **Years**: Supports single year or multiyear ranges (e.g., 2023-2024)
- **Revenue**: Gross rate revenue only, excludes Trade revenue

### Analysis Components

#### First Grid: Language Performance Summary
- **Purpose**: Shows overall performance of each language across all markets
- **Metrics**: Revenue, spot counts, averages, percentages
- **Grouping**: Languages consolidated (e.g., Mandarin + Cantonese = Chinese)

#### Second Grid: Language by Market Breakdown
- **Purpose**: Shows how each language performs in each market
- **Metrics**: Revenue with percentages of language total and market total
- **Cross-tabulation**: Language rows × Market columns

#### Market Summary
- **Purpose**: Shows market-level performance and dominant languages
- **Metrics**: Total revenue, top language per market, language diversity
- **Insights**: Geographic concentration and language specialization

### Key Calculations
- **Percentages**: Based on individual language revenue totals only
- **Market Share**: Each market's share of total individual language revenue
- **Language Dominance**: Top language's percentage within each market
- **Concentration**: Top markets' share of total revenue

### Multiyear Support
- **Year Ranges**: Supports "2023-2024" format for multiyear analysis
- **Aggregation**: Combines data across all years in range
- **Consistency**: Same classification rules applied to all years

---

*Market Analysis System v1.0 - Multiyear Support*"""


def main():
    """Main function with command line interface"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Market Analysis System - Multiyear Support",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Market Analysis Examples:
  # Single year analysis
  python market_analysis.py --year 2024
  
  # Two year analysis
  python market_analysis.py --year 2023-2024
  
  # Three year analysis
  python market_analysis.py --year 2022-2024
  
  # Save multiyear report to file
  python market_analysis.py --year 2023-2024 --output market_report.md
  
  # Language summary only
  python market_analysis.py --year 2024 --language-summary-only
        """
    )
    
    parser.add_argument("--year", default="2024", 
                       help="Year to analyze - supports single year (2024) or range (2023-2024)")
    
    parser.add_argument("--output", metavar="FILE", 
                       help="Save report to file (e.g., market_report.md). "
                            "If not specified, output goes to console.")
    
    parser.add_argument("--db-path", default="data/database/production.db", 
                       help="Database path (default: data/database/production.db)")
    
    parser.add_argument("--language-summary-only", action="store_true", 
                       help="Show only language summary (first grid)")
    
    parser.add_argument("--market-breakdown-only", action="store_true", 
                       help="Show only market breakdown (second grid)")
    
    args = parser.parse_args()
    
    try:
        with MarketAnalysisEngine(args.db_path) as engine:
            if args.language_summary_only:
                # Show language summary only
                language_summary = engine.get_language_performance_summary(args.year)
                full_years, _ = engine.parse_year_range(args.year)
                year_display = f"{full_years[0]}-{full_years[-1]}" if len(full_years) > 1 else full_years[0]
                
                print("🌐 Language Performance Summary:")
                print("=" * 50)
                print(f"Years: {year_display}")
                print(f"Total Languages: {len(language_summary)}")
                total_revenue = sum(r.revenue for r in language_summary)
                print(f"Total Revenue: ${total_revenue:,.2f}")
                print()
                
                for lang in language_summary:
                    print(f"{lang.name}: ${lang.revenue:,.2f} ({lang.percentage:.1f}%) - {lang.total_spots:,} spots")
                    
            elif args.market_breakdown_only:
                # Show market breakdown only
                market_breakdown = engine.get_language_market_breakdown(args.year)
                full_years, _ = engine.parse_year_range(args.year)
                year_display = f"{full_years[0]}-{full_years[-1]}" if len(full_years) > 1 else full_years[0]
                
                print("📍 Language Performance by Market:")
                print("=" * 50)
                print(f"Years: {year_display}")
                print(f"Total Language-Market Combinations: {len(market_breakdown)}")
                print()
                
                for item in market_breakdown:
                    print(f"{item.language} in {item.market}: ${item.revenue:,.2f} ({item.percentage_of_language:.1f}% of language)")
                    
            else:
                # Generate full report
                report = engine.generate_market_analysis_report(args.year)
                
                if args.output:
                    # Create directory if it doesn't exist
                    import os
                    os.makedirs(os.path.dirname(args.output), exist_ok=True) if os.path.dirname(args.output) else None
                    
                    with open(args.output, 'w') as f:
                        f.write(report)
                    
                    # Parse year for display
                    full_years, _ = engine.parse_year_range(args.year)
                    year_display = f"{full_years[0]}-{full_years[-1]}" if len(full_years) > 1 else full_years[0]
                    
                    print(f"✅ Market analysis report saved to {args.output}")
                    print(f"📅 Years analyzed: {year_display}")
                    print(f"📄 File size: {os.path.getsize(args.output):,} bytes")
                else:
                    print(report)
    
    except ValueError as e:
        print(f"❌ Input Error: {str(e)}")
        print("💡 Use format like: --year 2024 or --year 2023-2024")
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()