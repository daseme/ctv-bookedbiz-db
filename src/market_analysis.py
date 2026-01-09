#!/usr/bin/env python3
"""
Market Analysis System - Direct Language Code Query
====================================================

Updated to query spots.language_code directly instead of using the
spot_language_assignments table. This eliminates sync issues after
month closures and simplifies the data model.

Key Changes from Previous Version:
- Queries spots.language_code directly (no JOIN to spot_language_assignments)
- Applies language grouping logic in SQL
- Combines CHI, CMP, and MSP markets into single CMP category
- Filters: Internal Ad Sales + COM/BNS spots (same business logic)
"""

import sqlite3
import sys
import os
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass

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
    Market analysis engine querying spots.language_code directly.
    No dependency on spot_language_assignments table.
    """

    # Language code to group mapping
    LANGUAGE_GROUPS = {
        'M': 'Chinese',      # Mandarin
        'C': 'Chinese',      # Cantonese
        'M/C': 'Chinese',    # Mandarin/Cantonese
        'V': 'Vietnamese',
        'T': 'Filipino',     # Tagalog
        'K': 'Korean',
        'J': 'Japanese',
        'SA': 'South Asian',
        'HM': 'Hmong',
        'E': 'English',
        'EN': 'English',
        'ENG': 'English',
        'P': 'Portuguese',
    }

    def __init__(self, db_path: str = "data/database/production.db"):
        self.db_path = db_path
        self.db_connection = None

    def __enter__(self):
        self.db_connection = sqlite3.connect(self.db_path)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.db_connection:
            self.db_connection.close()

    def _build_language_group_case(self) -> str:
        """Build SQL CASE statement for language grouping."""
        return """
            CASE UPPER(TRIM(s.language_code))
                WHEN 'M' THEN 'Chinese'
                WHEN 'C' THEN 'Chinese'
                WHEN 'M/C' THEN 'Chinese'
                WHEN 'V' THEN 'Vietnamese'
                WHEN 'T' THEN 'Filipino'
                WHEN 'K' THEN 'Korean'
                WHEN 'J' THEN 'Japanese'
                WHEN 'SA' THEN 'South Asian'
                WHEN 'HM' THEN 'Hmong'
                WHEN 'E' THEN 'English'
                WHEN 'EN' THEN 'English'
                WHEN 'ENG' THEN 'English'
                WHEN 'P' THEN 'Portuguese'
                WHEN 'L' THEN 'Undetermined'
                WHEN '' THEN 'Unknown'
                WHEN NULL THEN 'Unknown'
                ELSE 'Other: ' || COALESCE(UPPER(TRIM(s.language_code)), 'Unknown')
            END
        """

    def _build_market_case(self) -> str:
        """Build SQL CASE statement for market consolidation (CHI+CMP+MSP -> CMP)."""
        return """
            CASE 
                WHEN UPPER(COALESCE(s.market_name, 'Unknown')) IN ('CHI', 'CMP', 'MSP') THEN 'CMP'
                ELSE COALESCE(s.market_name, 'Unknown')
            END
        """

    def parse_year_range(self, year_input: str) -> Tuple[List[str], List[str]]:
        """Parse year input to handle both single years and ranges."""
        if "-" in year_input and len(year_input) > 4:
            start_year, end_year = year_input.split("-")
            start_year = int(start_year)
            end_year = int(end_year)
            if start_year > end_year:
                raise ValueError(f"Start year {start_year} cannot be greater than end year {end_year}")
            full_years = [str(year) for year in range(start_year, end_year + 1)]
            year_suffixes = [year[-2:] for year in full_years]
        else:
            full_years = [year_input]
            year_suffixes = [year_input[-2:]]
        return full_years, year_suffixes

    def build_year_filter(self, year_suffixes: List[str]) -> Tuple[str, List[str]]:
        """Build SQL filter for multiple year suffixes."""
        if len(year_suffixes) == 1:
            return "s.broadcast_month LIKE ?", [f"%-{year_suffixes[0]}"]
        else:
            conditions = ["s.broadcast_month LIKE ?" for _ in year_suffixes]
            params = [f"%-{suffix}" for suffix in year_suffixes]
            return f"({' OR '.join(conditions)})", params

    def get_language_performance_summary(self, year_input: str = "2024") -> List[LanguageResult]:
        """Get language performance summary querying spots.language_code directly."""
        full_years, year_suffixes = self.parse_year_range(year_input)
        year_filter, year_params = self.build_year_filter(year_suffixes)

        language_group = self._build_language_group_case()

        query = f"""
        SELECT 
            {language_group} as language_group,
            SUM(COALESCE(s.gross_rate, 0)) as revenue,
            COUNT(CASE WHEN s.spot_type != 'BNS' OR s.spot_type IS NULL THEN 1 END) as paid_spots,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bonus_spots,
            COUNT(*) as total_spots
        FROM spots s
        WHERE {year_filter}
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND s.revenue_type = 'Internal Ad Sales'
        AND s.spot_type IN ('COM', 'BNS')
        AND s.language_code IS NOT NULL
        AND TRIM(s.language_code) != ''
        GROUP BY {language_group}
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
            results.append(LanguageResult(
                name=language,
                revenue=revenue,
                percentage=0,
                paid_spots=paid_spots,
                bonus_spots=bonus_spots,
                total_spots=total_spots,
                avg_per_spot=revenue / total_spots if total_spots > 0 else 0,
            ))

        for result in results:
            result.percentage = (result.revenue / total_revenue) * 100 if total_revenue > 0 else 0

        return results

    def get_language_market_breakdown(self, year_input: str = "2024") -> List[MarketLanguageResult]:
        """Get language performance broken down by market."""
        full_years, year_suffixes = self.parse_year_range(year_input)
        year_filter, year_params = self.build_year_filter(year_suffixes)

        language_group = self._build_language_group_case()
        market_case = self._build_market_case()

        query = f"""
        SELECT 
            {language_group} as language_group,
            {market_case} as market,
            SUM(COALESCE(s.gross_rate, 0)) as revenue,
            COUNT(CASE WHEN s.spot_type != 'BNS' OR s.spot_type IS NULL THEN 1 END) as paid_spots,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bonus_spots,
            COUNT(*) as total_spots
        FROM spots s
        WHERE {year_filter}
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND s.revenue_type = 'Internal Ad Sales'
        AND s.spot_type IN ('COM', 'BNS')
        AND s.language_code IS NOT NULL
        AND TRIM(s.language_code) != ''
        GROUP BY {language_group}, {market_case}
        HAVING SUM(COALESCE(s.gross_rate, 0)) > 0 OR COUNT(*) > 0
        ORDER BY {language_group}, SUM(COALESCE(s.gross_rate, 0)) DESC
        """

        cursor = self.db_connection.cursor()
        cursor.execute(query, year_params)

        results = []
        language_totals = {}
        market_totals = {}

        for row in cursor.fetchall():
            language, market, revenue, paid_spots, bonus_spots, total_spots = row

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
                percentage_of_language=0,
                percentage_of_market=0,
                paid_spots=paid_spots,
                bonus_spots=bonus_spots,
                total_spots=total_spots,
                avg_per_spot=revenue / total_spots if total_spots > 0 else 0,
            ))

        for result in results:
            if language_totals[result.language] > 0:
                result.percentage_of_language = (result.revenue / language_totals[result.language]) * 100
            if market_totals[result.market] > 0:
                result.percentage_of_market = (result.revenue / market_totals[result.market]) * 100

        return results

    def get_market_summary(self, year_input: str = "2024") -> List[MarketSummary]:
        """Get market summary showing total revenue and top language per market."""
        market_language_data = self.get_language_market_breakdown(year_input)

        market_data = {}
        total_revenue = 0

        for item in market_language_data:
            market = item.market
            if market not in market_data:
                market_data[market] = {"revenue": 0, "languages": {}, "unique_languages": 0}

            market_data[market]["revenue"] += item.revenue
            market_data[market]["languages"][item.language] = item.revenue
            total_revenue += item.revenue

        results = []
        for market, data in market_data.items():
            top_language = max(data["languages"].items(), key=lambda x: x[1]) if data["languages"] else ("Unknown", 0)

            results.append(MarketSummary(
                market=market,
                revenue=data["revenue"],
                percentage_of_total=(data["revenue"] / total_revenue) * 100 if total_revenue > 0 else 0,
                top_language=top_language[0],
                top_language_percentage=(top_language[1] / data["revenue"]) * 100 if data["revenue"] > 0 else 0,
                unique_languages=len(data["languages"]),
            ))

        results.sort(key=lambda x: x.revenue, reverse=True)
        return results

    def get_revenue_context(self, year_input: str = "2024") -> Dict[str, float]:
        """Get revenue totals at each filter level for context."""
        full_years, year_suffixes = self.parse_year_range(year_input)
        year_filter, year_params = self.build_year_filter(year_suffixes)

        cursor = self.db_connection.cursor()
        
        # Total gross (excluding Trade)
        cursor.execute(f"""
            SELECT SUM(COALESCE(gross_rate, 0))
            FROM spots s
            WHERE {year_filter}
            AND (revenue_type != 'Trade' OR revenue_type IS NULL)
        """, year_params)
        total_gross = cursor.fetchone()[0] or 0

        # Internal Ad Sales only
        cursor.execute(f"""
            SELECT SUM(COALESCE(gross_rate, 0))
            FROM spots s
            WHERE {year_filter}
            AND revenue_type = 'Internal Ad Sales'
        """, year_params)
        internal_ad_sales = cursor.fetchone()[0] or 0

        # Internal Ad Sales + COM/BNS (what the report analyzes)
        cursor.execute(f"""
            SELECT SUM(COALESCE(gross_rate, 0))
            FROM spots s
            WHERE {year_filter}
            AND revenue_type = 'Internal Ad Sales'
            AND spot_type IN ('COM', 'BNS')
        """, year_params)
        report_scope = cursor.fetchone()[0] or 0

        # Other revenue types breakdown
        cursor.execute(f"""
            SELECT revenue_type, SUM(COALESCE(gross_rate, 0)) as revenue
            FROM spots s
            WHERE {year_filter}
            AND (revenue_type != 'Trade' OR revenue_type IS NULL)
            AND revenue_type != 'Internal Ad Sales'
            GROUP BY revenue_type
            ORDER BY revenue DESC
        """, year_params)
        other_types = {row[0]: row[1] for row in cursor.fetchall()}

        return {
            "total_gross": total_gross,
            "internal_ad_sales": internal_ad_sales,
            "report_scope": report_scope,
            "excluded_from_report": total_gross - report_scope,
            "other_revenue_types": other_types,
        }

    def get_language_code_distribution(self, year_input: str = "2024") -> Dict[str, Any]:
        """Get raw language code distribution for diagnostics."""
        full_years, year_suffixes = self.parse_year_range(year_input)
        year_filter, year_params = self.build_year_filter(year_suffixes)

        query = f"""
        SELECT 
            UPPER(TRIM(s.language_code)) as code,
            COUNT(*) as spots,
            SUM(COALESCE(s.gross_rate, 0)) as revenue
        FROM spots s
        WHERE {year_filter}
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND s.revenue_type = 'Internal Ad Sales'
        AND s.spot_type IN ('COM', 'BNS')
        GROUP BY UPPER(TRIM(s.language_code))
        ORDER BY SUM(COALESCE(s.gross_rate, 0)) DESC
        """

        cursor = self.db_connection.cursor()
        cursor.execute(query, year_params)

        results = {}
        total_spots = 0
        total_revenue = 0

        for row in cursor.fetchall():
            code, spots, revenue = row
            code = code if code else "NULL/EMPTY"
            results[code] = {"spots": spots, "revenue": revenue}
            total_spots += spots
            total_revenue += revenue

        return {
            "codes": results,
            "total_spots": total_spots,
            "total_revenue": total_revenue,
        }

    def generate_market_analysis_report(self, year_input: str = "2024") -> str:
        """Generate comprehensive market analysis report."""
        full_years, year_suffixes = self.parse_year_range(year_input)
        year_display = f"{full_years[0]}-{full_years[-1]}" if len(full_years) > 1 else full_years[0]

        language_summary = self.get_language_performance_summary(year_input)
        market_breakdown = self.get_language_market_breakdown(year_input)
        market_summary = self.get_market_summary(year_input)
        code_distribution = self.get_language_code_distribution(year_input)
        revenue_context = self.get_revenue_context(year_input)

        language_table = self._format_language_summary_table(language_summary, year_display)
        market_breakdown_table = self._format_market_breakdown_table(market_breakdown, year_display)
        market_summary_table = self._format_market_summary_table(market_summary, year_display)
        code_table = self._format_code_distribution_table(code_distribution, year_display)
        context_table = self._format_revenue_context(revenue_context, year_display)
        insights = self._generate_market_insights(language_summary, market_breakdown, market_summary)

        return f"""# Market Analysis Report - {year_display}

*Direct query on spots.language_code - no dependency on spot_language_assignments*
*Markets: CHI, CMP, and MSP consolidated into CMP category*

{context_table}

## 📊 Analysis Overview

- **Years Analyzed**: {", ".join(full_years)}
- **Total Languages**: {len(language_summary)}
- **Total Markets**: {len(market_summary)}
- **Market Consolidation**: CHI + CMP + MSP → CMP
- **Data Source**: spots.language_code (direct query)
- **Scope**: Internal Ad Sales + COM/BNS spots

{language_table}

{market_breakdown_table}

{market_summary_table}

{code_table}

{insights}

{self._generate_methodology()}
"""

    def _format_language_summary_table(self, results: List[LanguageResult], year_display: str) -> str:
        """Format language summary table."""
        if not results:
            return f"""## 🌐 Language Performance Summary
### Language-Targeted Revenue ({year_display})

*No language data found for the specified year(s)*
"""

        total_revenue = sum(r.revenue for r in results)
        total_paid_spots = sum(r.paid_spots for r in results)
        total_bonus_spots = sum(r.bonus_spots for r in results)
        total_all_spots = sum(r.total_spots for r in results)
        total_avg = total_revenue / total_all_spots if total_all_spots > 0 else 0

        table = f"""## 🌐 Language Performance Summary
### Language-Targeted Revenue ({year_display})

| Language | Revenue | % of Total | Paid Spots | BNS Spots | Total Spots | Avg/Spot |
|----------|---------|------------|-----------|-----------|-------------|----------|
"""
        for result in results:
            table += f"| {result.name} | ${result.revenue:,.2f} | {result.percentage:.1f}% | {result.paid_spots:,} | {result.bonus_spots:,} | {result.total_spots:,} | ${result.avg_per_spot:.2f} |\n"

        table += "|----------|---------|------------|-----------|-----------|-------------|----------|\n"
        table += f"| **TOTAL** | **${total_revenue:,.2f}** | **100.0%** | **{total_paid_spots:,}** | **{total_bonus_spots:,}** | **{total_all_spots:,}** | **${total_avg:.2f}** |\n"

        return table

    def _format_market_breakdown_table(self, results: List[MarketLanguageResult], year_display: str) -> str:
        """Format market breakdown table."""
        if not results:
            return f"""## 📍 Language Performance by Market
### Revenue Distribution ({year_display})

*No market breakdown data found*
"""

        language_data = {}
        market_totals = {}

        for result in results:
            if result.language not in language_data:
                language_data[result.language] = {}
            language_data[result.language][result.market] = result

            if result.market not in market_totals:
                market_totals[result.market] = 0
            market_totals[result.market] += result.revenue

        sorted_markets = sorted(market_totals.items(), key=lambda x: x[1], reverse=True)
        market_order = [market for market, _ in sorted_markets]

        language_totals = {}
        for language, markets in language_data.items():
            language_totals[language] = sum(r.revenue for r in markets.values())

        sorted_languages = sorted(language_totals.items(), key=lambda x: x[1], reverse=True)

        table = f"""## 📍 Language Performance by Market
### Revenue Distribution ({year_display})
*Note: CHI, CMP, and MSP markets consolidated into CMP*

| Language | """ + " | ".join(market_order) + """ | Total |
|----------|""" + "|".join(["-" * max(len(m), 8) for m in market_order]) + """|-------|
"""

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

        table += "|----------|" + "|".join(["-" * max(len(m), 8) for m in market_order]) + "|-------|\n"
        total_row = "| **TOTAL** | "
        for market in market_order:
            total_row += f"**${market_totals[market]:,.0f}** | "
        grand_total = sum(market_totals.values())
        total_row += f"**${grand_total:,.0f}** |"
        table += total_row + "\n"

        return table

    def _format_revenue_context(self, context: Dict[str, Any], year_display: str) -> str:
        """Format revenue context section showing what's included vs excluded."""
        total = context["total_gross"]
        internal = context["internal_ad_sales"]
        scope = context["report_scope"]
        excluded = context["excluded_from_report"]
        other_types = context["other_revenue_types"]

        pct_of_total = (scope / total * 100) if total > 0 else 0

        table = f"""## 💰 Revenue Context ({year_display})

This report analyzes **Internal Ad Sales (COM/BNS spots)** which represents the multicultural advertising segment.

| Category | Revenue | % of Total |
|----------|---------|------------|
| **Total Gross Revenue** | ${total:,.2f} | 100.0% |
| Internal Ad Sales | ${internal:,.2f} | {internal/total*100:.1f}% |
| **This Report (Internal Ad Sales COM/BNS)** | **${scope:,.2f}** | **{pct_of_total:.1f}%** |

### Revenue Not Included in This Report

| Revenue Type | Amount | Notes |
|--------------|--------|-------|
"""
        for rev_type, amount in sorted(other_types.items(), key=lambda x: x[1], reverse=True):
            notes = {
                "Direct Response Sales": "National direct response advertising",
                "Paid Programming": "Infomercials and paid content",
                "Branded Content": "Sponsored content partnerships",
                "Other": "Miscellaneous revenue",
            }.get(rev_type, "")
            table += f"| {rev_type} | ${amount:,.2f} | {notes} |\n"

        non_com_bns = internal - scope
        if non_com_bns > 0:
            table += f"| Internal Ad Sales (non-COM/BNS) | ${non_com_bns:,.2f} | PRG, PKG, other spot types |\n"

        table += f"| **Total Excluded** | **${excluded:,.2f}** | |\n"

        return table

    def _format_market_summary_table(self, results: List[MarketSummary], year_display: str) -> str:
        """Format market summary table."""
        if not results:
            return ""

        table = f"""## 📊 Market Share Analysis
### Market Performance Summary ({year_display})
*CMP includes consolidated CHI + CMP + MSP markets*

| Market | Revenue | % of Total | Top Language | Top Lang % | Languages |
|--------|---------|------------|--------------|------------|-----------|
"""
        for result in results:
            table += f"| {result.market} | ${result.revenue:,.0f} | {result.percentage_of_total:.1f}% | {result.top_language} | {result.top_language_percentage:.1f}% | {result.unique_languages} |\n"

        return table

    def _format_code_distribution_table(self, code_data: Dict[str, Any], year_display: str) -> str:
        """Format raw language code distribution table."""
        codes = code_data.get("codes", {})
        if not codes:
            return ""

        table = f"""## 🔤 Raw Language Code Distribution
### Source Codes ({year_display})

| Code | Spots | Revenue | Group |
|------|-------|---------|-------|
"""
        for code, data in sorted(codes.items(), key=lambda x: x[1]["revenue"], reverse=True):
            group = self.LANGUAGE_GROUPS.get(code.upper(), "Other")
            table += f"| {code} | {data['spots']:,} | ${data['revenue']:,.2f} | {group} |\n"

        table += f"\n**Total**: {code_data['total_spots']:,} spots, ${code_data['total_revenue']:,.2f} revenue\n"
        return table

    def _generate_market_insights(self, language_summary, market_breakdown, market_summary) -> str:
        """Generate key insights."""
        if not language_summary or not market_summary:
            return "## 📊 Key Insights\n\n*Insufficient data to generate insights*\n"

        total_revenue = sum(r.revenue for r in language_summary)
        top_language = language_summary[0] if language_summary else None
        top_market = market_summary[0] if market_summary else None

        cmp_market = next((m for m in market_summary if m.market == "CMP"), None)
        cmp_info = f"CMP (CHI+CMP+MSP): ${cmp_market.revenue:,.0f} ({cmp_market.percentage_of_total:.1f}%)" if cmp_market else "CMP data not found"

        top_2_revenue = sum(r.revenue for r in market_summary[:2])
        concentration = (top_2_revenue / total_revenue) * 100 if total_revenue > 0 else 0

        return f"""## 📊 Key Insights

### Revenue Overview
- **Total Language-Targeted Revenue**: ${total_revenue:,.2f}
- **Top Language**: {top_language.name if top_language else "N/A"} (${top_language.revenue:,.2f}, {top_language.percentage:.1f}%)
- **Top Market**: {top_market.market if top_market else "N/A"} (${top_market.revenue:,.2f}, {top_market.percentage_of_total:.1f}%)
- **{cmp_info}**

### Market Concentration
- **Top 2 Markets**: {concentration:.1f}% of total revenue
- **Active Markets**: {len(market_summary)}
- **Average Languages per Market**: {sum(m.unique_languages for m in market_summary) / len(market_summary):.1f}

### Language Distribution
- **Language Groups**: {len(language_summary)}
- **Top 3 Languages**: {sum(r.percentage for r in language_summary[:3]):.1f}% of revenue
"""

    def _generate_methodology(self) -> str:
        """Generate methodology section."""
        return """---

## 📋 Methodology

### Data Source
- **Table**: `spots` (direct query on `language_code` column)
- **No dependency** on `spot_language_assignments` table
- **Survives month closures** without needing re-processing

### Filters Applied
- `revenue_type = 'Internal Ad Sales'`
- `spot_type IN ('COM', 'BNS')`
- `language_code IS NOT NULL`
- Excludes Trade revenue

### Language Grouping
| Code(s) | Group |
|---------|-------|
| M, C, M/C | Chinese |
| V | Vietnamese |
| T | Filipino |
| K | Korean |
| J | Japanese |
| SA | South Asian |
| HM | Hmong |
| E, EN, ENG | English |
| P | Portuguese |

### Market Consolidation
- CHI, CMP, MSP → CMP (combined Midwest markets)

---
*Market Analysis v3.0 - Direct Language Code Query*
"""


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Market Analysis - Direct Language Code Query",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--year", default="2025", help="Year(s) to analyze (e.g., 2025 or 2024-2025)")
    parser.add_argument("--output", metavar="FILE", help="Save report to file")
    parser.add_argument("--db-path", default="data/database/production.db", help="Database path")
    parser.add_argument("--language-summary-only", action="store_true", help="Show only language summary")
    parser.add_argument("--codes-only", action="store_true", help="Show only raw code distribution")

    args = parser.parse_args()

    try:
        with MarketAnalysisEngine(args.db_path) as engine:
            if args.language_summary_only:
                results = engine.get_language_performance_summary(args.year)
                print(f"\n🌐 Language Performance ({args.year}):")
                print("=" * 60)
                for r in results:
                    print(f"{r.name}: ${r.revenue:,.2f} ({r.percentage:.1f}%) - {r.total_spots:,} spots")

            elif args.codes_only:
                data = engine.get_language_code_distribution(args.year)
                print(f"\n🔤 Language Code Distribution ({args.year}):")
                print("=" * 60)
                for code, info in sorted(data["codes"].items(), key=lambda x: x[1]["revenue"], reverse=True):
                    print(f"{code}: {info['spots']:,} spots, ${info['revenue']:,.2f}")

            else:
                report = engine.generate_market_analysis_report(args.year)
                if args.output:
                    with open(args.output, "w") as f:
                        f.write(report)
                    print(f"✅ Report saved to {args.output}")
                else:
                    print(report)

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()