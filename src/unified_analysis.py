#!/usr/bin/env python3
"""
Refactored Unified Analysis System
==================================

Examples:
  python unified_analysis.py --year 2024
  python unified_analysis.py --year 2023-2024
  python unified_analysis.py --year 2024 --assignment-methods-only
  python unified_analysis.py --year 2024 --export-review review_2024.csv
  python unified_analysis.py --year 2024 --export-review review.csv --review-type undetermined
  python src/unified_analysis.py --year 2024 python unified_analysis.py --year 2024
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Protocol, Any
from enum import Enum
import sqlite3


# ============================================================================
# Domain Models
# ============================================================================


@dataclass
class AnalysisResult:
    """Core result model for all analysis types"""

    name: str
    revenue: float = 0.0
    percentage: float = 0.0
    paid_spots: int = 0
    bonus_spots: int = 0
    total_spots: int = 0
    avg_per_spot: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def revenue_formatted(self) -> str:
        return f"${self.revenue:,.2f}"

    @property
    def percentage_formatted(self) -> str:
        return f"{self.percentage:.1f}%"


@dataclass
class YearRange:
    """Encapsulates year range logic"""

    full_years: List[str]
    suffixes: List[str]
    display: str

    @classmethod
    def from_input(cls, year_input: str) -> "YearRange":
        if "-" in year_input:
            start_year, end_year = map(int, year_input.split("-"))
            if start_year > end_year:
                raise ValueError(
                    f"Start year {start_year} cannot be greater than end year {end_year}"
                )
            full_years = [str(y) for y in range(start_year, end_year + 1)]
        else:
            full_years = [year_input]

        suffixes = [y[-2:] for y in full_years]
        display = (
            f"{full_years[0]}-{full_years[-1]}"
            if len(full_years) > 1
            else full_years[0]
        )

        return cls(full_years=full_years, suffixes=suffixes, display=display)


class BusinessCategory(Enum):
    """Business categories with their rules"""

    INTERNAL_AD_SALES = ("Internal Ad Sales", "revenue_type = 'Internal Ad Sales'")
    DIRECT_RESPONSE = (
        "Direct Response Sales",
        "revenue_type = 'Direct Response Sales'",
    )
    PAID_PROGRAMMING = ("Paid Programming", "revenue_type = 'Paid Programming'")
    BRANDED_CONTENT = ("Branded Content", "revenue_type = 'Branded Content'")
    OTHER_REVIEW = ("Other/Review Required", "revenue_type = 'Other'")

    def __init__(self, display_name: str, sql_condition: str):
        self.display_name = display_name
        self.sql_condition = sql_condition


class LanguageGroup(Enum):
    """Language groupings for analysis"""

    CHINESE = ("Chinese", ["Mandarin", "Cantonese", "Mandarin/Cantonese"])
    FILIPINO = ("Filipino", ["Tagalog", "Filipino"])
    VIETNAMESE = ("Vietnamese", ["Vietnamese"])
    HMONG = ("Hmong", ["Hmong"])
    SOUTH_ASIAN = ("South Asian", ["Hindi", "Punjabi", "South Asian"])
    KOREAN = ("Korean", ["Korean"])
    JAPANESE = ("Japanese", ["Japanese"])
    ENGLISH = ("English", ["English"])

    def __init__(self, display_name: str, language_names: List[str]):
        self.display_name = display_name
        self.language_names = language_names


# ============================================================================
# Data Access Layer
# ============================================================================


class DatabaseConnection(Protocol):
    """Protocol for database connections"""

    def cursor(self) -> sqlite3.Cursor: ...
    def close(self) -> None: ...


class QueryBuilder:
    """Builds reusable SQL query components"""

    @staticmethod
    def build_year_filter(suffixes: List[str]) -> Tuple[str, List[str]]:
        """Build year filter SQL and parameters"""
        if len(suffixes) == 1:
            return "s.broadcast_month LIKE ?", [f"%-{suffixes[0]}"]

        conditions = ["s.broadcast_month LIKE ?" for _ in suffixes]
        params = [f"%-{suffix}" for suffix in suffixes]
        return f"({' OR '.join(conditions)})", params

    @staticmethod
    def get_base_where_clause(year_filter: str) -> str:
        """Standard WHERE clause for spot filtering"""
        return f"""
        WHERE {year_filter}
          AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
          AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        """


class SpotRepository:
    """Repository for spot-related data access"""

    def __init__(self, db: DatabaseConnection):
        self.db = db
        self.query_builder = QueryBuilder()

    def get_base_totals(self, year_range: YearRange) -> Dict[str, Any]:
        """Get base totals for the specified year range"""
        year_filter, params = self.query_builder.build_year_filter(year_range.suffixes)
        base_where = self.query_builder.get_base_where_clause(year_filter)

        query = f"""
        SELECT 
            SUM(COALESCE(s.gross_rate, 0)) AS revenue,
            COUNT(CASE WHEN s.spot_type IS NULL OR s.spot_type <> 'BNS' THEN 1 END) AS paid_spots,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) AS bonus_spots,
            COUNT(*) AS total_spots
        FROM spots s
        {base_where}
        """

        cursor = self.db.cursor()
        cursor.execute(query, params)
        row = cursor.fetchone()

        return {
            "revenue": float(row["revenue"] or 0),
            "paid_spots": int(row["paid_spots"] or 0),
            "bonus_spots": int(row["bonus_spots"] or 0),
            "total_spots": int(row["total_spots"] or 0),
        }

    def get_category_data(
        self, category: BusinessCategory, year_range: YearRange
    ) -> AnalysisResult:
        """Get data for a specific business category"""
        year_filter, params = self.query_builder.build_year_filter(year_range.suffixes)
        base_where = self.query_builder.get_base_where_clause(year_filter)

        query = f"""
        SELECT 
            SUM(COALESCE(s.gross_rate, 0)) AS revenue,
            COUNT(CASE WHEN s.spot_type IS NULL OR s.spot_type <> 'BNS' THEN 1 END) AS paid_spots,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) AS bonus_spots,
            COUNT(*) AS total_spots
        FROM spots s
        {base_where}
        AND {category.sql_condition}
        """

        cursor = self.db.cursor()
        cursor.execute(query, params)
        row = cursor.fetchone()

        revenue = float(row["revenue"] or 0)
        total_spots = int(row["total_spots"] or 0)

        return AnalysisResult(
            name=category.display_name,
            revenue=revenue,
            paid_spots=int(row["paid_spots"] or 0),
            bonus_spots=int(row["bonus_spots"] or 0),
            total_spots=total_spots,
            avg_per_spot=revenue / total_spots if total_spots else 0,
        )

    def get_language_data(self, year_range: YearRange) -> List[AnalysisResult]:
        """Get language analysis data for Internal Ad Sales spots"""
        year_filter, params = self.query_builder.build_year_filter(year_range.suffixes)

        # Build language case statement
        language_cases = []
        for lang_group in LanguageGroup:
            names_condition = " OR ".join(
                [f"l.language_name = '{name}'" for name in lang_group.language_names]
            )
            language_cases.append(
                f"WHEN ({names_condition}) THEN '{lang_group.display_name}'"
            )

        language_case_sql = (
            "CASE " + " ".join(language_cases) + " ELSE 'Other: Undetermined' END"
        )

        query = f"""
        SELECT 
            {language_case_sql} AS language,
            SUM(COALESCE(s.gross_rate,0)) AS revenue,
            COUNT(CASE WHEN s.spot_type IS NULL OR s.spot_type <> 'BNS' THEN 1 END) AS paid_spots,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) AS bonus_spots,
            COUNT(*) AS total_spots
        FROM spots s
        JOIN spot_language_assignments sla ON s.spot_id = sla.spot_id
        LEFT JOIN languages l ON UPPER(sla.language_code) = UPPER(l.language_code)
        WHERE {year_filter}
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        AND s.revenue_type = 'Internal Ad Sales'
        GROUP BY 1
        HAVING SUM(COALESCE(s.gross_rate,0)) > 0 OR COUNT(*) > 0
        ORDER BY revenue DESC
        """

        cursor = self.db.cursor()
        cursor.execute(query, params)

        results = []
        for row in cursor.fetchall():
            revenue = float(row["revenue"] or 0)
            total_spots = int(row["total_spots"] or 0)

            results.append(
                AnalysisResult(
                    name=row["language"],
                    revenue=revenue,
                    paid_spots=int(row["paid_spots"] or 0),
                    bonus_spots=int(row["bonus_spots"] or 0),
                    total_spots=total_spots,
                    avg_per_spot=revenue / total_spots if total_spots else 0,
                )
            )

        return results


# ============================================================================
# Business Logic Layer
# ============================================================================


class AnalysisService:
    """Core business logic for analysis operations"""

    def __init__(self, spot_repository: SpotRepository):
        self.spot_repository = spot_repository

    def get_category_analysis(self, year_input: str) -> List[AnalysisResult]:
        """Get mutually exclusive category analysis"""
        year_range = YearRange.from_input(year_input)
        results = []

        for category in BusinessCategory:
            result = self.spot_repository.get_category_data(category, year_range)
            results.append(result)

        # Calculate percentages
        total_revenue = sum(r.revenue for r in results)
        for result in results:
            result.percentage = (
                (result.revenue / total_revenue * 100) if total_revenue else 0
            )

        return results

    def get_language_analysis(self, year_input: str) -> List[AnalysisResult]:
        """Get language analysis for Internal Ad Sales"""
        year_range = YearRange.from_input(year_input)
        results = self.spot_repository.get_language_data(year_range)

        # Calculate percentages
        total_revenue = sum(r.revenue for r in results)
        for result in results:
            result.percentage = (
                (result.revenue / total_revenue * 100) if total_revenue else 0
            )

        return results

    def validate_reconciliation(self, year_input: str) -> Dict[str, Any]:
        """Validate that categories reconcile with base totals"""
        year_range = YearRange.from_input(year_input)

        base_totals = self.spot_repository.get_base_totals(year_range)
        category_results = self.get_category_analysis(year_input)

        category_totals = {
            "revenue": sum(r.revenue for r in category_results),
            "paid_spots": sum(r.paid_spots for r in category_results),
            "bonus_spots": sum(r.bonus_spots for r in category_results),
            "total_spots": sum(r.total_spots for r in category_results),
        }

        revenue_diff = abs(base_totals["revenue"] - category_totals["revenue"])
        spot_diff = abs(base_totals["total_spots"] - category_totals["total_spots"])

        return {
            "base_totals": base_totals,
            "category_totals": category_totals,
            "revenue_difference": revenue_diff,
            "spot_difference": spot_diff,
            "perfect_reconciliation": revenue_diff < 1.0 and spot_diff < 1,
            "years": year_range.full_years,
            "year_display": year_range.display,
        }


# ============================================================================
# Presentation Layer
# ============================================================================


class ReportFormatter:
    """Handles formatting of analysis results into reports"""

    def format_analysis_table(
        self,
        results: List[AnalysisResult],
        title: str,
        subtitle: str,
        year_display: str,
    ) -> str:
        """Format analysis results as a markdown table"""
        total_revenue = sum(r.revenue for r in results)
        total_paid_spots = sum(r.paid_spots for r in results)
        total_bonus_spots = sum(r.bonus_spots for r in results)
        total_all_spots = sum(r.total_spots for r in results)
        total_avg_per_spot = total_revenue / total_all_spots if total_all_spots else 0.0

        lines = [
            f"## {title}",
            f"### {subtitle} ({year_display})",
            "| Category | Revenue | % of Total | Paid Spots | BNS Spots | Total Spots | Avg/Spot |",
            "|----------|---------|------------|-----------|-----------|-------------|----------|",
        ]

        for result in results:
            lines.append(
                f"| {result.name} | {result.revenue_formatted} | {result.percentage_formatted} | "
                f"{result.paid_spots:,} | {result.bonus_spots:,} | {result.total_spots:,} | "
                f"${result.avg_per_spot:.2f} |"
            )

        lines.extend(
            [
                "|----------|---------|------------|-----------|-----------|-------------|----------|",
                f"| **TOTAL** | **${total_revenue:,.2f}** | **100.0%** | **{total_paid_spots:,}** | "
                f"**{total_bonus_spots:,}** | **{total_all_spots:,}** | **${total_avg_per_spot:.2f}** |",
            ]
        )

        return "\n".join(lines)

    def format_reconciliation_summary(self, validation: Dict[str, Any]) -> str:
        """Format reconciliation validation results"""
        return f"""## 🎯 Reconciliation

- **Years**: {", ".join(validation["years"])}
- **Base Revenue**: ${validation["base_totals"]["revenue"]:,.2f}
- **Category Total**: ${validation["category_totals"]["revenue"]:,.2f}
- **Revenue Δ**: ${validation["revenue_difference"]:,.2f}
- **Spots Δ**: {validation["spot_difference"]:,}
- **Perfect?**: {"✅ YES" if validation["perfect_reconciliation"] else "❌ NO"}
"""

    def generate_full_report(
        self, year_input: str, analysis_service: AnalysisService
    ) -> str:
        """Generate the complete analysis report"""
        year_range = YearRange.from_input(year_input)

        # Get analysis results
        categories = analysis_service.get_category_analysis(year_input)
        languages = analysis_service.get_language_analysis(year_input)
        validation = analysis_service.validate_reconciliation(year_input)

        # Format sections
        reconciliation = self.format_reconciliation_summary(validation)
        category_table = self.format_analysis_table(
            categories,
            "📊 Business Rule Category Breakdown",
            "Revenue Categories",
            year_range.display,
        )
        language_table = self.format_analysis_table(
            languages,
            "🌐 Language-Targeted Advertising",
            "Internal Ad Sales by Language",
            year_range.display,
        )

        return f"""# Language-Targeted Advertising Analysis - {year_range.display}

{reconciliation}

{category_table}

{language_table}

## 📋 System Notes

**Key Change**: Language-Targeted Advertising now includes **ALL** Internal Ad Sales spots.

- Previously: Internal Ad Sales + spot_type IN ('COM', 'BNS', 'BB')
- Now: **ALL** Internal Ad Sales spots (regardless of spot_type)

This change simplifies the business logic and moves all Internal Ad Sales spots
into the Language-Targeted Advertising category.

## FAQ

- **Why are PKG/CRD/AV spots no longer in review?**  
  They are now classified as Internal Ad Sales since they have revenue_type = 'Internal Ad Sales'.

- **What changed about Language-Targeted Advertising?**  
  It now includes ALL Internal Ad Sales spots, not just COM/BNS/BB. This simplifies the business logic.
"""


# ============================================================================
# Application Layer / Facade
# ============================================================================


class AnalysisEngine:
    """Main facade for the analysis system"""

    def __init__(self, db_path: str = "data/database/production.db"):
        self.db_path = db_path
        self._db: Optional[DatabaseConnection] = None

    def __enter__(self):
        self._db = sqlite3.connect(self.db_path)
        self._db.row_factory = sqlite3.Row

        # Initialize components with dependency injection
        self._spot_repository = SpotRepository(self._db)
        self._analysis_service = AnalysisService(self._spot_repository)
        self._report_formatter = ReportFormatter()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._db:
            self._db.close()

    def generate_report(self, year_input: str = "2024") -> str:
        """Generate the full analysis report"""
        return self._report_formatter.generate_full_report(
            year_input, self._analysis_service
        )

    def get_category_analysis(self, year_input: str = "2024") -> List[AnalysisResult]:
        """Get business category analysis"""
        return self._analysis_service.get_category_analysis(year_input)

    def get_language_analysis(self, year_input: str = "2024") -> List[AnalysisResult]:
        """Get language analysis"""
        return self._analysis_service.get_language_analysis(year_input)

    def validate_reconciliation(self, year_input: str = "2024") -> Dict[str, Any]:
        """Validate data reconciliation"""
        return self._analysis_service.validate_reconciliation(year_input)


# ============================================================================
# CLI Interface
# ============================================================================


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Clean Language-Targeted Advertising Analysis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--year", default="2024", help="Year or range, e.g. 2024 or 2023-2024"
    )
    parser.add_argument(
        "--db-path", default="data/database/production.db", help="SQLite DB path"
    )
    parser.add_argument("--output", help="Write report to file")
    parser.add_argument(
        "--validate-only", action="store_true", help="Only run reconciliation"
    )

    args = parser.parse_args()

    try:
        with AnalysisEngine(args.db_path) as engine:
            if args.validate_only:
                validation = engine.validate_reconciliation(args.year)
                print("🧪 Validation Results")
                print("=" * 50)
                print(f"Years: {', '.join(validation['years'])}")
                print(f"Base Revenue: ${validation['base_totals']['revenue']:,.2f}")
                print(
                    f"Category Total: ${validation['category_totals']['revenue']:,.2f}"
                )
                print(
                    f"Perfect: {'YES' if validation['perfect_reconciliation'] else 'NO'}"
                )
                return

            report = engine.generate_report(args.year)

            if args.output:
                import os

                os.makedirs(os.path.dirname(args.output), exist_ok=True)
                with open(args.output, "w", encoding="utf-8") as f:
                    f.write(report)
                print(f"✅ Report saved to {args.output}")
            else:
                print(report)

    except Exception as e:
        print(f"❌ Error: {e}")
        raise


if __name__ == "__main__":
    main()
