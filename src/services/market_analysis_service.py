"""
Market Analysis Service - Web integration for market analysis engine.
Wraps the MarketAnalysisEngine for Flask route consumption.
"""

import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from datetime import date
from src.utils.language_constants import LanguageConstants

logger = logging.getLogger(__name__)


@dataclass
class MarketAnalysisData:
    """Data container for market analysis results."""

    selected_year: str
    available_years: List[str]
    revenue_context: Dict[str, Any]
    language_summary: List[Dict[str, Any]]
    market_breakdown: List[Dict[str, Any]]
    market_summary: List[Dict[str, Any]]
    code_distribution: Dict[str, Any]
    metadata: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class MarketAnalysisService:
    """Service for market analysis data retrieval and formatting."""


    def __init__(self, db_connection):
        self.db = db_connection
        self.logger = logging.getLogger(self.__class__.__name__)

    def get_available_years(self) -> List[str]:
        """Get list of years with data."""
        with self.db.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT DISTINCT '20' || SUBSTR(broadcast_month, -2) as year
                FROM spots
                WHERE broadcast_month IS NOT NULL
                AND (revenue_type != 'Trade' OR revenue_type IS NULL)
                ORDER BY year DESC
            """)
            return [row[0] for row in cursor.fetchall()]

    def get_revenue_context(self, year: str) -> Dict[str, Any]:
        """Get revenue totals at each filter level."""
        with self.db.connection() as conn:
            cursor = conn.cursor()
            suffix = year[-2:]

            cursor.execute(
                """
                SELECT SUM(COALESCE(gross_rate, 0))
                FROM spots
                WHERE broadcast_month LIKE ?
                AND (revenue_type != 'Trade' OR revenue_type IS NULL)
            """,
                (f"%-{suffix}",),
            )
            total_gross = cursor.fetchone()[0] or 0

            cursor.execute(
                """
                SELECT SUM(COALESCE(gross_rate, 0))
                FROM spots
                WHERE broadcast_month LIKE ?
                AND revenue_type = 'Internal Ad Sales'
            """,
                (f"%-{suffix}",),
            )
            internal_ad_sales = cursor.fetchone()[0] or 0

            cursor.execute(
                """
                SELECT SUM(COALESCE(gross_rate, 0))
                FROM spots
                WHERE broadcast_month LIKE ?
                AND revenue_type = 'Internal Ad Sales'
                AND spot_type IN ('COM', 'BNS')
            """,
                (f"%-{suffix}",),
            )
            report_scope = cursor.fetchone()[0] or 0

            cursor.execute(
                """
                SELECT revenue_type, SUM(COALESCE(gross_rate, 0)) as revenue
                FROM spots
                WHERE broadcast_month LIKE ?
                AND (revenue_type != 'Trade' OR revenue_type IS NULL)
                AND revenue_type != 'Internal Ad Sales'
                GROUP BY revenue_type
                ORDER BY revenue DESC
            """,
                (f"%-{suffix}",),
            )
            other_types = {row[0]: row[1] for row in cursor.fetchall()}

            return {
                "total_gross": total_gross,
                "internal_ad_sales": internal_ad_sales,
                "report_scope": report_scope,
                "excluded_from_report": total_gross - report_scope,
                "other_revenue_types": other_types,
                "report_percentage": (report_scope / total_gross * 100)
                if total_gross > 0
                else 0,
            }

    def _build_language_case_sql(self) -> str:
        """Build SQL CASE statement for language grouping."""
        return """
            CASE UPPER(TRIM(language_code))
                WHEN 'M' THEN 'Chinese'
                WHEN 'C' THEN 'Chinese'
                WHEN 'M/C' THEN 'Chinese'
                WHEN 'V' THEN 'Vietnamese'
                WHEN 'T' THEN 'Filipino'
                WHEN 'K' THEN 'Korean'
                WHEN 'J' THEN 'Japanese'
                WHEN 'SA' THEN 'South Asian'
                WHEN 'P' THEN 'South Asian'
                WHEN 'HM' THEN 'Hmong'
                WHEN 'E' THEN 'English'
                WHEN 'EN' THEN 'English'
                WHEN 'ENG' THEN 'English'
                ELSE 'Other'
            END
        """

    def get_language_summary(self, year: str) -> List[Dict[str, Any]]:
        """Get language performance summary."""
        with self.db.connection() as conn:
            cursor = conn.cursor()
            suffix = year[-2:]
            lang_case = self._build_language_case_sql()

            query = f"""
            SELECT 
                {lang_case} as language_group,
                SUM(COALESCE(gross_rate, 0)) as revenue,
                COUNT(CASE WHEN spot_type != 'BNS' OR spot_type IS NULL THEN 1 END) as paid_spots,
                COUNT(CASE WHEN spot_type = 'BNS' THEN 1 END) as bonus_spots,
                COUNT(*) as total_spots
            FROM spots
            WHERE broadcast_month LIKE ?
            AND (revenue_type != 'Trade' OR revenue_type IS NULL)
            AND revenue_type = 'Internal Ad Sales'
            AND spot_type IN ('COM', 'BNS')
            AND language_code IS NOT NULL
            AND TRIM(language_code) != ''
            GROUP BY {lang_case}
            HAVING SUM(COALESCE(gross_rate, 0)) > 0
            ORDER BY SUM(COALESCE(gross_rate, 0)) DESC
            """

            cursor.execute(query, (f"%-{suffix}",))
            rows = cursor.fetchall()

        results = []
        total_revenue = 0

        for row in rows:
            lang, revenue, paid, bonus, total = row
            total_revenue += revenue
            results.append(
                {
                    "language": lang,
                    "revenue": revenue,
                    "paid_spots": paid,
                    "bonus_spots": bonus,
                    "total_spots": total,
                    "avg_per_spot": revenue / total if total > 0 else 0,
                }
            )

        for r in results:
            r["percentage"] = (
                (r["revenue"] / total_revenue * 100) if total_revenue > 0 else 0
            )

        return results

    def get_market_breakdown(self, year: str) -> List[Dict[str, Any]]:
        """Get language performance by market."""
        with self.db.connection() as conn:
            cursor = conn.cursor()
            suffix = year[-2:]
            lang_case = self._build_language_case_sql()

            query = f"""
            SELECT 
                {lang_case} as language_group,
                CASE 
                    WHEN UPPER(COALESCE(market_name, 'Unknown')) IN ('CHI', 'CMP', 'MSP') THEN 'CMP'
                    ELSE COALESCE(market_name, 'Unknown')
                END as market,
                SUM(COALESCE(gross_rate, 0)) as revenue,
                COUNT(*) as total_spots
            FROM spots
            WHERE broadcast_month LIKE ?
            AND (revenue_type != 'Trade' OR revenue_type IS NULL)
            AND revenue_type = 'Internal Ad Sales'
            AND spot_type IN ('COM', 'BNS')
            AND language_code IS NOT NULL
            AND TRIM(language_code) != ''
            GROUP BY language_group, market
            HAVING SUM(COALESCE(gross_rate, 0)) > 0
            ORDER BY language_group, revenue DESC
            """

            cursor.execute(query, (f"%-{suffix}",))
            rows = cursor.fetchall()

        results = []
        lang_totals = {}
        market_totals = {}

        for row in rows:
            lang, market, revenue, spots = row
            lang_totals[lang] = lang_totals.get(lang, 0) + revenue
            market_totals[market] = market_totals.get(market, 0) + revenue
            results.append(
                {
                    "language": lang,
                    "market": market,
                    "revenue": revenue,
                    "total_spots": spots,
                }
            )

        for r in results:
            r["pct_of_language"] = (
                (r["revenue"] / lang_totals[r["language"]] * 100)
                if lang_totals.get(r["language"], 0) > 0
                else 0
            )
            r["pct_of_market"] = (
                (r["revenue"] / market_totals[r["market"]] * 100)
                if market_totals.get(r["market"], 0) > 0
                else 0
            )

        return results

    def get_market_summary(self, year: str) -> List[Dict[str, Any]]:
        """Get market-level summary."""
        breakdown = self.get_market_breakdown(year)

        market_data = {}
        total_revenue = 0

        for item in breakdown:
            market = item["market"]
            if market not in market_data:
                market_data[market] = {"revenue": 0, "languages": {}}
            market_data[market]["revenue"] += item["revenue"]
            market_data[market]["languages"][item["language"]] = item["revenue"]
            total_revenue += item["revenue"]

        results = []
        for market, data in market_data.items():
            top_lang = (
                max(data["languages"].items(), key=lambda x: x[1])
                if data["languages"]
                else ("Unknown", 0)
            )
            results.append(
                {
                    "market": market,
                    "revenue": data["revenue"],
                    "pct_of_total": (data["revenue"] / total_revenue * 100)
                    if total_revenue > 0
                    else 0,
                    "top_language": top_lang[0],
                    "top_language_pct": (top_lang[1] / data["revenue"] * 100)
                    if data["revenue"] > 0
                    else 0,
                    "unique_languages": len(data["languages"]),
                }
            )

        results.sort(key=lambda x: x["revenue"], reverse=True)
        return results

    def get_code_distribution(self, year: str) -> Dict[str, Any]:
        """Get raw language code distribution."""
        with self.db.connection() as conn:
            cursor = conn.cursor()
            suffix = year[-2:]

            cursor.execute(
                """
                SELECT 
                    UPPER(TRIM(language_code)) as code,
                    COUNT(*) as spots,
                    SUM(COALESCE(gross_rate, 0)) as revenue
                FROM spots
                WHERE broadcast_month LIKE ?
                AND (revenue_type != 'Trade' OR revenue_type IS NULL)
                AND revenue_type = 'Internal Ad Sales'
                AND spot_type IN ('COM', 'BNS')
                GROUP BY UPPER(TRIM(language_code))
                ORDER BY revenue DESC
            """,
                (f"%-{suffix}",),
            )
            rows = cursor.fetchall()

        codes = []
        total_spots = 0
        total_revenue = 0

        for row in rows:
            code, spots, revenue = row
            code = code if code else "EMPTY"
            group = LanguageConstants.get_language_group(code)
            codes.append(
                {
                    "code": code,
                    "spots": spots,
                    "revenue": revenue,
                    "group": group,
                }
            )
            total_spots += spots
            total_revenue += revenue

        return {
            "codes": codes,
            "total_spots": total_spots,
            "total_revenue": total_revenue,
        }

    def get_market_analysis_data(
        self, year: Optional[str] = None
    ) -> MarketAnalysisData:
        """Get complete market analysis data for web display."""
        import time

        start_time = time.time()

        available_years = self.get_available_years()
        if not year:
            year = str(date.today().year)

        if year not in available_years and available_years:
            year = available_years[0]

        revenue_context = self.get_revenue_context(year)
        language_summary = self.get_language_summary(year)
        market_breakdown = self.get_market_breakdown(year)
        market_summary = self.get_market_summary(year)
        code_distribution = self.get_code_distribution(year)

        processing_time = (time.time() - start_time) * 1000

        return MarketAnalysisData(
            selected_year=year,
            available_years=available_years,
            revenue_context=revenue_context,
            language_summary=language_summary,
            market_breakdown=market_breakdown,
            market_summary=market_summary,
            code_distribution=code_distribution,
            metadata={
                "processing_time_ms": processing_time,
                "report_type": "market_analysis",
                "data_source": "spots.language_code",
                "filters": "Internal Ad Sales + COM/BNS",
            },
        )

    def get_csv_data(self, year: str, report_type: str) -> List[Dict[str, Any]]:
        """Get data formatted for CSV export."""
        if report_type == "language_summary":
            return self.get_language_summary(year)
        elif report_type == "market_breakdown":
            return self.get_market_breakdown(year)
        elif report_type == "market_summary":
            return self.get_market_summary(year)
        elif report_type == "code_distribution":
            return self.get_code_distribution(year).get("codes", [])
        else:
            return []
