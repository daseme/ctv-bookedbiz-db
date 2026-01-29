#!/usr/bin/env python3
"""
Updated Unified Analysis System
==================================================================

This system now defines Language-Targeted Advertising as simply:
- revenue_type = 'Internal Ad Sales' (regardless of spot_type)

Previously it was:
- revenue_type = 'Internal Ad Sales' AND spot_type IN ('COM','BNS','BB')

This change simplifies the business logic and moves all Internal Ad Sales spots
into the Language-Targeted Advertising category.

Key changes:
- Language-Targeted Advertising now includes ALL Internal Ad Sales spots
- Other/Review Required category no longer includes Internal Ad Sales spots with unusual spot types
- All related queries updated consistently

"""

import sqlite3
import csv
import sys
import os
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass

# Ensure relative imports resolve if needed
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


@dataclass
class UnifiedResult:
    """Unified result structure for both language and category analysis"""

    name: str
    revenue: float
    percentage: float
    paid_spots: int
    bonus_spots: int
    total_spots: int
    avg_per_spot: float
    details: Optional[Dict[str, Any]] = None


class UpdatedUnifiedAnalysisEngine:
    """
    Updated unified analysis engine using the new language assignment system
    with multiyear support and simplified business rule categories.

    Language-Targeted Advertising now includes ALL Internal Ad Sales spots.
    """

    def __init__(self, db_path: str = "data/database/production.db"):
        self.db_path = db_path
        self.db_connection: Optional[sqlite3.Connection] = None

    def __enter__(self):
        self.db_connection = sqlite3.connect(self.db_path)
        self.db_connection.row_factory = sqlite3.Row
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.db_connection:
            self.db_connection.close()

    # ---------------- Year helpers ----------------

    def parse_year_range(self, year_input: str) -> Tuple[List[str], List[str]]:
        """
        Parse year input "2024" or "2023-2024" -> (["2023","2024"], ["23","24"])
        """
        if "-" in year_input:
            start_year, end_year = year_input.split("-")
            start_year = int(start_year)
            end_year = int(end_year)
            if start_year > end_year:
                raise ValueError(
                    f"Start year {start_year} cannot be greater than end year {end_year}"
                )
            full_years = [str(y) for y in range(start_year, end_year + 1)]
            suffixes = [y[-2:] for y in full_years]
        else:
            full_years = [year_input]
            suffixes = [year_input[-2:]]
        return full_years, suffixes

    def build_year_filter(self, suffixes: List[str]) -> Tuple[str, List[str]]:
        if len(suffixes) == 1:
            return "s.broadcast_month LIKE ?", [f"%-{suffixes[0]}"]
        conds, params = [], []
        for suf in suffixes:
            conds.append("s.broadcast_month LIKE ?")
            params.append(f"%-{suf}")
        return f"({' OR '.join(conds)})", params

    # ---------------- Base totals ----------------

    def get_base_totals(self, year_input: str = "2024") -> Dict[str, Any]:
        full_years, suffixes = self.parse_year_range(year_input)
        year_filter, params = self.build_year_filter(suffixes)
        q = f"""
        SELECT 
            SUM(COALESCE(s.gross_rate, 0))                 AS revenue,
            COUNT(CASE WHEN s.spot_type IS NULL OR s.spot_type <> 'BNS' THEN 1 END) AS paid_spots,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END)                           AS bonus_spots,
            COUNT(*) AS total_spots
        FROM spots s
        WHERE {year_filter}
          AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
          AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        """
        cur = self.db_connection.cursor()
        cur.execute(q, params)
        r = cur.fetchone()
        return {
            "revenue": float(r["revenue"] or 0),
            "paid_spots": int(r["paid_spots"] or 0),
            "bonus_spots": int(r["bonus_spots"] or 0),
            "total_spots": int(r["total_spots"] or 0),
            "years": full_years,
            "year_range": year_input,
        }

    def get_business_category_assignment_method_crosstab(
        self, year_input: str = "2024"
    ):
        """
        Language-Targeted Advertising now includes ALL Internal Ad Sales spots.
        """
        full_years, year_suffixes = self.parse_year_range(year_input)
        year_filter, year_params = self.build_year_filter(year_suffixes)

        sql = f"""
        WITH scoped AS (
            SELECT
                s.spot_id,
                s.gross_rate,
                s.revenue_type,
                UPPER(COALESCE(s.spot_type, '')) AS spot_type,
                CASE
                    WHEN s.revenue_type = 'Direct Response Sales' THEN 'Direct Response Sales'
                    WHEN s.revenue_type = 'Paid Programming'      THEN 'Paid Programming'
                    WHEN s.revenue_type = 'Branded Content'       THEN 'Branded Content'
                    WHEN s.revenue_type = 'Internal Ad Sales'     THEN 'Internal Ad Sales'
                    ELSE 'Other/Review Required'
                END AS business_category
            FROM spots s
            WHERE {year_filter}
            AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
            AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        )
        SELECT
            scoped.business_category AS business_category,
            CASE
                WHEN sla.assignment_method = 'direct_mapping'
                    THEN 'Direct Language Mapping'
                WHEN sla.assignment_method = 'business_rule_default_english'
                    THEN 'Business Rule Default English'
                WHEN sla.assignment_method = 'auto_default_com_bb'
                    THEN 'Auto Default English (COM/BB)'
                WHEN sla.assignment_method = 'default_english'
                    THEN 'Default English (Fallback)'
                WHEN sla.assignment_method = 'business_review_required'
                    THEN 'Business Review Required'
                WHEN sla.assignment_method = 'undetermined_flagged'
                    THEN 'Undetermined (Needs Review)'
                WHEN sla.assignment_method = 'invalid_code_flagged'
                    THEN 'Invalid Code Flagged'
                WHEN sla.assignment_method IS NULL
                    THEN 'No Assignment Record'
                ELSE 'Other/Unknown'
            END AS method_label,
            COUNT(*) AS spots,
            SUM(COALESCE(scoped.gross_rate,0)) AS revenue,
            SUM(CASE WHEN COALESCE(sla.requires_review,0) = 1 THEN 1 ELSE 0 END) AS review_count
        FROM scoped
        LEFT JOIN spot_language_assignments sla
            ON scoped.spot_id = sla.spot_id
        GROUP BY business_category, method_label
        ORDER BY business_category, method_label;
        """

        cur = self.db_connection.cursor()
        cur.execute(sql, year_params)
        rows = cur.fetchall()

        method_order = [
            "Direct Language Mapping",
            "Business Rule Default English",
            "Auto Default English (COM/BB)",
            "Default English (Fallback)",
            "Business Review Required",
            "Undetermined (Needs Review)",
            "Invalid Code Flagged",
            "No Assignment Record",
            "Other/Unknown",
        ]
        category_order = [
            "Internal Ad Sales",
            "Direct Response Sales",
            "Paid Programming",
            "Branded Content",
            "Other/Review Required",
        ]

        table = {}
        methods_seen = set()
        for business_category, method_label, spots, revenue, review_count in rows:
            table.setdefault(business_category, {})[method_label] = {
                "spots": spots or 0,
                "revenue": revenue or 0.0,
                "review_count": review_count or 0,
            }
            methods_seen.add(method_label)

        cols = [m for m in method_order if m in methods_seen]
        rows_ordered = [c for c in category_order if c in table] + [
            c for c in table.keys() if c not in category_order
        ]

        return {"rows": rows_ordered, "cols": cols, "cells": table}

    # ---------------- Categories (mutually exclusive) ----------------

    def get_mutually_exclusive_categories(
        self, year_input: str = "2024"
    ) -> List[UnifiedResult]:
        """
        Categories based on business rules:
        1) Internal Ad Sales spots
        2) Direct Response Sales
        3) Paid Programming
        4) Branded Content
        6) Other/Review Required (only revenue_type = 'Other' now)
        """
        _, suffixes = self.parse_year_range(year_input)
        yf, yp = self.build_year_filter(suffixes)

        cur = self.db_connection.cursor()

        def fetch(q: str) -> Tuple[float, int, int, int]:
            cur.execute(q, yp)
            row = cur.fetchone()
            return (
                float(row["revenue"] or 0),
                int(row["paid_spots"] or 0),
                int(row["bonus_spots"] or 0),
                int(row["total_spots"] or 0),
            )

        base_where = f"""
        WHERE {yf}
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        """

        cats: List[UnifiedResult] = []

        # 1) Internal Ad Sales
        q4 = f"""
        SELECT 
            SUM(COALESCE(s.gross_rate, 0)) AS revenue,
            COUNT(CASE WHEN s.spot_type IS NULL OR s.spot_type <> 'BNS' THEN 1 END) AS paid_spots,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) AS bonus_spots,
            COUNT(*) AS total_spots
        FROM spots s
        {base_where}
        AND s.revenue_type = 'Internal Ad Sales'
        """
        rev, paid, bns, tot = fetch(q4)
        cats.append(
            UnifiedResult(
                "Internal Ad Sales", rev, 0, paid, bns, tot, rev / tot if tot else 0
            )
        )

        # 2) Direct Response Sales
        q1 = f"""
        SELECT 
            SUM(COALESCE(s.gross_rate, 0)) AS revenue,
            COUNT(CASE WHEN s.spot_type IS NULL OR s.spot_type <> 'BNS' THEN 1 END) AS paid_spots,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) AS bonus_spots,
            COUNT(*) AS total_spots
        FROM spots s
        {base_where}
        AND s.revenue_type = 'Direct Response Sales'
        """
        rev, paid, bns, tot = fetch(q1)
        cats.append(
            UnifiedResult(
                "Direct Response Sales", rev, 0, paid, bns, tot, rev / tot if tot else 0
            )
        )

        # 3) Paid Programming
        q2 = f"""
        SELECT 
            SUM(COALESCE(s.gross_rate, 0)) AS revenue,
            COUNT(CASE WHEN s.spot_type IS NULL OR s.spot_type <> 'BNS' THEN 1 END) AS paid_spots,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) AS bonus_spots,
            COUNT(*) AS total_spots
        FROM spots s
        {base_where}
        AND s.revenue_type = 'Paid Programming'
        """
        rev, paid, bns, tot = fetch(q2)
        cats.append(
            UnifiedResult(
                "Paid Programming", rev, 0, paid, bns, tot, rev / tot if tot else 0
            )
        )

        # 4) Branded Content
        q3 = f"""
        SELECT 
            SUM(COALESCE(s.gross_rate, 0)) AS revenue,
            COUNT(CASE WHEN s.spot_type IS NULL OR s.spot_type <> 'BNS' THEN 1 END) AS paid_spots,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) AS bonus_spots,
            COUNT(*) AS total_spots
        FROM spots s
        {base_where}
        AND s.revenue_type = 'Branded Content'
        """
        rev, paid, bns, tot = fetch(q3)
        cats.append(
            UnifiedResult(
                "Branded Content", rev, 0, paid, bns, tot, rev / tot if tot else 0
            )
        )

        # 5) Other/Review Required (only revenue_type = 'Other' now)
        q6 = f"""
        SELECT 
            SUM(COALESCE(s.gross_rate, 0)) AS revenue,
            COUNT(CASE WHEN s.spot_type IS NULL OR s.spot_type <> 'BNS' THEN 1 END) AS paid_spots,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) AS bonus_spots,
            COUNT(*) AS total_spots
        FROM spots s
        {base_where}
        AND s.revenue_type = 'Other'
        """

        rev, paid, bns, tot = fetch(q6)
        cats.append(
            UnifiedResult(
                "Other/Review Required", rev, 0, paid, bns, tot, rev / tot if tot else 0
            )
        )

        # Calculate percentages
        total_rev = sum(c.revenue for c in cats)
        for c in cats:
            c.percentage = (c.revenue / total_rev * 100) if total_rev else 0
        return cats

    # ---------------- Language analysis ----------------

    def get_unified_language_analysis(
        self, year_input: str = "2024"
    ) -> List[UnifiedResult]:
        """
        Simplified language categorization: Only main business-relevant categories,
        everything else goes to "Other: Undetermined"
        """
        _, suffixes = self.parse_year_range(year_input)
        yf, yp = self.build_year_filter(suffixes)
        cur = self.db_connection.cursor()
        q = f"""
        SELECT 
            CASE 
                -- Main Business Categories (Pure/Clean Language Groups)
                WHEN l.language_name IN ('Mandarin','Cantonese','Mandarin/Cantonese') THEN 'Chinese'
                WHEN l.language_name IN ('Tagalog','Filipino') THEN 'Filipino'
                WHEN l.language_name = 'Vietnamese' THEN 'Vietnamese'
                WHEN l.language_name = 'Hmong' THEN 'Hmong'
                WHEN l.language_name IN ('Hindi','Punjabi','South Asian') THEN 'South Asian'
                WHEN l.language_name = 'Korean' THEN 'Korean'
                WHEN l.language_name = 'Japanese' THEN 'Japanese'
                WHEN l.language_name = 'English' THEN 'English'
                
                -- Everything Else = Undetermined
                -- This includes:
                -- - Multi-language codes that span groups (M,V; C,M,Hm; M,C,V; etc.)
                -- - 'L' (undetermined)
                -- - Unknown codes (13, 3, 5, A, F)
                -- - Languages not in main categories (Russian, Spanish)
                ELSE 'Other: Undetermined'
            END AS language,
            SUM(COALESCE(s.gross_rate,0)) AS revenue,
            COUNT(CASE WHEN s.spot_type IS NULL OR s.spot_type <> 'BNS' THEN 1 END) AS paid_spots,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) AS bonus_spots,
            COUNT(*) AS total_spots
        FROM spots s
        JOIN spot_language_assignments sla ON s.spot_id = sla.spot_id
        LEFT JOIN languages l ON UPPER(sla.language_code) = UPPER(l.language_code)
        WHERE {yf}
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        AND s.revenue_type = 'Internal Ad Sales'
        GROUP BY 1
        HAVING SUM(COALESCE(s.gross_rate,0)) > 0 OR COUNT(*) > 0
        ORDER BY revenue DESC
        """
        cur.execute(q, yp)
        out: List[UnifiedResult] = []
        for row in cur.fetchall():
            lang = row["language"]
            rev = float(row["revenue"] or 0)
            paid = int(row["paid_spots"] or 0)
            bns = int(row["bonus_spots"] or 0)
            tot = int(row["total_spots"] or 0)
            out.append(
                UnifiedResult(lang, rev, 0, paid, bns, tot, rev / tot if tot else 0)
            )
        total_rev = sum(x.revenue for x in out)
        for x in out:
            x.percentage = (x.revenue / total_rev * 100) if total_rev else 0
        return out

    # ---------------- Assignment method analysis ----------------

    def get_assignment_method_analysis(
        self, year_input: str = "2024"
    ) -> List[UnifiedResult]:
        """
        Include the new auto_default_com_bb method in the mapping.
        """
        _, suffixes = self.parse_year_range(year_input)
        yf, yp = self.build_year_filter(suffixes)
        cur = self.db_connection.cursor()
        q = f"""
        SELECT 
            CASE 
                WHEN sla.assignment_method = 'business_rule_default_english' THEN 'Business Rule Default English'
                WHEN sla.assignment_method = 'auto_default_com_bb'          THEN 'Auto Default English (COM/BB)'
                WHEN sla.assignment_method = 'direct_mapping'               THEN 'Direct Language Mapping'
                WHEN sla.assignment_method = 'business_review_required'     THEN 'Business Review Required'
                WHEN sla.assignment_method = 'undetermined_flagged'         THEN 'Undetermined (Needs Review)'
                WHEN sla.assignment_method = 'default_english'              THEN 'Default English (Fallback)'
                ELSE 'Other: ' || COALESCE(sla.assignment_method, 'Unknown')
            END as assignment_method,
            SUM(COALESCE(s.gross_rate, 0)) as revenue,
            COUNT(CASE WHEN s.spot_type IS NULL OR s.spot_type <> 'BNS' THEN 1 END) as paid_spots,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bonus_spots,
            COUNT(*) as total_spots,
            AVG(sla.confidence) as avg_confidence,
            COUNT(CASE WHEN sla.requires_review = 1 THEN 1 END) as review_count
        FROM spots s
        JOIN spot_language_assignments sla ON s.spot_id = sla.spot_id
        WHERE {yf}
          AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
          AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        GROUP BY 1
        ORDER BY revenue DESC
        """
        cur.execute(q, yp)
        out: List[UnifiedResult] = []
        for row in cur.fetchall():
            out.append(
                UnifiedResult(
                    name=row["assignment_method"],
                    revenue=float(row["revenue"] or 0),
                    percentage=0,
                    paid_spots=int(row["paid_spots"] or 0),
                    bonus_spots=int(row["bonus_spots"] or 0),
                    total_spots=int(row["total_spots"] or 0),
                    avg_per_spot=(float(row["revenue"] or 0) / int(row["total_spots"]))
                    if int(row["total_spots"])
                    else 0,
                    details={
                        "avg_confidence": float(row["avg_confidence"] or 0),
                        "review_count": int(row["review_count"] or 0),
                    },
                )
            )
        total_rev = sum(r.revenue for r in out)
        for r in out:
            r.percentage = (r.revenue / total_rev * 100) if total_rev else 0
        return out

    # ---------------- Reconciliation ----------------

    def validate_reconciliation(self, year_input: str = "2024") -> Dict[str, Any]:
        base = self.get_base_totals(year_input)
        cats = self.get_mutually_exclusive_categories(year_input)
        cat_tot = {
            "revenue": sum(c.revenue for c in cats),
            "paid_spots": sum(c.paid_spots for c in cats),
            "bonus_spots": sum(c.bonus_spots for c in cats),
            "total_spots": sum(c.total_spots for c in cats),
        }
        return {
            "base_totals": base,
            "category_totals": cat_tot,
            "revenue_difference": abs(base["revenue"] - cat_tot["revenue"]),
            "spot_difference": abs(base["total_spots"] - cat_tot["total_spots"]),
            "perfect_reconciliation": (
                abs(base["revenue"] - cat_tot["revenue"]) < 1.0
                and abs(base["total_spots"] - cat_tot["total_spots"]) < 1
            ),
            "new_assignment_system": True,
            "multiyear_support": True,
            "years_analyzed": base["years"],
            "language_targeted_advertising_change": "Now includes ALL Internal Ad Sales spots (not just COM/BNS/BB)",
        }

    # ---------------- Review export (Python version of the bash script) ----------------

    def _review_filter_sql(self, review_type: str) -> str:
        t = (review_type or "all").lower()
        if t == "business":
            return "AND sla.assignment_method = 'business_review_required'"
        if t == "undetermined":
            return "AND sla.assignment_method = 'undetermined_flagged'"
        if t == "invalid":
            return "AND sla.language_status = 'invalid'"
        if t == "high-value":
            return "AND sla.requires_review = 1 AND s.gross_rate > 500"
        if t == "low-confidence":
            return "AND sla.confidence < 0.5"
        if t == "fallback":
            return "AND sla.assignment_method IN ('default_english','business_rule_default_english','auto_default_com_bb')"
        # all - updated to remove Internal Ad Sales spot type filters
        return (
            "AND ("
            " sla.assignment_method IN ('business_review_required','undetermined_flagged','invalid_code_flagged')"
            " OR sla.requires_review = 1"
            " OR sla.confidence < 0.5"
            " OR sla.spot_id IS NULL"
            " OR s.revenue_type = 'Other'"
            " OR s.revenue_type NOT IN ('Direct Response Sales','Paid Programming','Branded Content','Internal Ad Sales')"
            ")"
        )

    def export_review_required(
        self, year_input: str, output_csv: str, review_type: str = "all"
    ) -> int:
        """
        Export review-required lines to CSV using updated rules.
        Returns the number of rows written.
        """
        _, suffixes = self.parse_year_range(year_input)
        yf, yp = self.build_year_filter(suffixes)
        filter_sql = self._review_filter_sql(review_type)
        cur = self.db_connection.cursor()

        q = f"""
        SELECT 
            s.spot_id,
            s.bill_code,
            COALESCE(c.normalized_name, 'Unknown') AS customer_name,
            COALESCE(a.agency_name, 'No Agency')  AS agency_name,
            s.gross_rate,
            s.station_net,
            s.spot_type,
            s.revenue_type,
            s.time_in,
            s.time_out,
            -- duration in minutes best-effort
            CASE 
                WHEN s.time_in IS NULL OR s.time_out IS NULL THEN NULL
                WHEN s.time_in <= s.time_out THEN 
                    (CAST(substr(s.time_out,1,2) AS INTEGER) - CAST(substr(s.time_in,1,2) AS INTEGER)) * 60 +
                    (CAST(substr(s.time_out,4,2) AS INTEGER) - CAST(substr(s.time_in,4,2) AS INTEGER))
                ELSE 
                    (24*60) - (
                        (CAST(substr(s.time_in,1,2) AS INTEGER) - CAST(substr(s.time_out,1,2) AS INTEGER)) * 60 +
                        (CAST(substr(s.time_in,4,2) AS INTEGER) - CAST(substr(s.time_out,4,2) AS INTEGER))
                    )
            END AS duration_minutes,
            s.day_of_week,
            s.air_date,
            s.broadcast_month,
            s.sales_person,
            s.language_code AS original_language_code,
            s.comments      AS program_comments,
            s.market_name,
            sla.language_code       AS assigned_language,
            l.language_name         AS assigned_language_name,
            sla.assignment_method,
            sla.language_status,
            sla.confidence,
            sla.requires_review,
            sla.notes               AS assignment_notes,
            sla.assigned_date,
            CASE 
                WHEN s.revenue_type = 'Direct Response Sales' THEN 'Direct Response Sales'
                WHEN s.revenue_type = 'Paid Programming'      THEN 'Paid Programming'
                WHEN s.revenue_type = 'Branded Content'       THEN 'Branded Content'
                WHEN s.revenue_type = 'Internal Ad Sales'     THEN 'Internal Ad Sales'
                ELSE 'Other/Review Required'
            END AS business_category,
            CASE 
                WHEN sla.assignment_method = 'business_review_required' THEN 'Unusual Revenue/Spot Type Combination'
                WHEN sla.assignment_method = 'undetermined_flagged'     THEN 'Language Code L - Needs Manual Determination'
                WHEN sla.assignment_method IN ('default_english','business_rule_default_english','auto_default_com_bb')
                     THEN 'Defaulted to English'
                WHEN s.revenue_type = 'Other'                           THEN 'Revenue Type: Other - Needs Classification'
                WHEN sla.language_status = 'invalid'                    THEN 'Invalid Language Code'
                WHEN sla.confidence < 0.5                               THEN 'Low Confidence Assignment'
                WHEN sla.requires_review = 1                            THEN 'System Flagged for Review'
                WHEN sla.spot_id IS NULL                                THEN 'No Language Assignment Record'
                ELSE 'Other Review Required'
            END AS review_reason,
            CASE 
                WHEN s.gross_rate > 1000 THEN 'High Priority ($1000+)'
                WHEN s.gross_rate > 500  THEN 'Medium Priority ($500-1000)'
                WHEN s.gross_rate > 100  THEN 'Low Priority ($100-500)'
                ELSE 'Very Low Priority (<$100)'
            END AS priority_level
        FROM spots s
        LEFT JOIN spot_language_assignments sla ON s.spot_id = sla.spot_id
        LEFT JOIN languages l ON UPPER(sla.language_code) = UPPER(l.language_code)
        LEFT JOIN customers c ON s.customer_id = c.customer_id
        LEFT JOIN agencies  a ON s.agency_id   = a.agency_id
        WHERE {yf}
          AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
          AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
          {filter_sql}
        ORDER BY 
            CASE 
                WHEN s.gross_rate > 1000 THEN 1
                WHEN s.gross_rate > 500  THEN 2
                WHEN s.gross_rate > 100  THEN 3
                ELSE 4
            END,
            s.gross_rate DESC,
            COALESCE(sla.assigned_date, s.load_date) DESC
        """
        cur.execute(q, yp)
        rows = cur.fetchall()
        if not rows:
            # still create a file with header
            headers = [d[0] for d in cur.description]
            with open(output_csv, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(headers)
            return 0

        headers = rows[0].keys()
        with open(output_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(headers)
            for r in rows:
                w.writerow([r[h] for h in headers])
        return len(rows)

    # ---------------- Report formatting ----------------

    def _format_table(
        self, results: List[UnifiedResult], title: str, subtitle: str, year_display: str
    ) -> str:
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
        for r in results:
            lines.append(
                f"| {r.name} | ${r.revenue:,.2f} | {r.percentage:.1f}% | {r.paid_spots:,} | {r.bonus_spots:,} | {r.total_spots:,} | ${r.avg_per_spot:.2f} |"
            )
        lines.append(
            "|----------|---------|------------|-----------|-----------|-------------|----------|"
        )
        lines.append(
            f"| **TOTAL** | **${total_revenue:,.2f}** | **100.0%** | **{total_paid_spots:,}** | **{total_bonus_spots:,}** | **{total_all_spots:,}** | **${total_avg_per_spot:.2f}** |"
        )
        return "\n".join(lines)

    def _format_assignment_method_table(
        self, results: List[UnifiedResult], title: str, subtitle: str, year_display: str
    ) -> str:
        total_revenue = sum(r.revenue for r in results)
        total_spots = sum(r.total_spots for r in results)
        total_review = sum(
            r.details.get("review_count", 0) for r in results if r.details
        )

        lines = [
            f"## {title}",
            f"### {subtitle} ({year_display})",
            "| Assignment Method | Revenue | % of Total | Total Spots | Avg Confidence | Review Count |",
            "|-------------------|---------|------------|-------------|----------------|--------------|",
        ]
        for r in results:
            avg_conf = r.details.get("avg_confidence", 0) if r.details else 0
            rev_cnt = r.details.get("review_count", 0) if r.details else 0
            lines.append(
                f"| {r.name} | ${r.revenue:,.2f} | {r.percentage:.1f}% | {r.total_spots:,} | {avg_conf:.2f} | {rev_cnt:,} |"
            )
        lines.append(
            "|-------------------|---------|------------|-------------|----------------|--------------|"
        )
        lines.append(
            f"| **TOTAL** | **${total_revenue:,.2f}** | **100.0%** | **{total_spots:,}** | **N/A** | **{total_review:,}** |"
        )
        return "\n".join(lines)

    def _format_crosstab_counts_table(
        self, xtab, title: str, subtitle: str, year_display: str
    ) -> str:
        """
        Render a Markdown table where each cell shows:
        spots (rev $K)  — e.g., "1,234 ($567k)"
        and a separate trailing column per row for Review Count.
        """
        rows = xtab["rows"]
        cols = xtab["cols"]
        cells = xtab["cells"]

        # Header
        header = (
            "| Business Category | "
            + " | ".join(cols)
            + " | Total Spots | Review Count |\n"
        )
        sep = (
            "|"
            + "------------------|"
            + "|".join(["---------------------------" for _ in cols])
            + "|-------------|--------------|\n"
        )

        lines = [f"## {title}", f"### {subtitle} ({year_display})", header, sep]

        grand_total_spots = 0
        grand_total_review = 0

        for cat in rows:
            row_total_spots = 0
            row_total_review = 0
            cells_markdown = []
            for m in cols:
                cell = cells.get(cat, {}).get(
                    m, {"spots": 0, "revenue": 0.0, "review_count": 0}
                )
                spots = int(cell["spots"])
                rev_k = cell["revenue"] / 1000.0
                row_total_spots += spots
                row_total_review += int(cell["review_count"])
                cells_markdown.append(f"{spots:,} (${rev_k:,.0f}k)")
            grand_total_spots += row_total_spots
            grand_total_review += row_total_review
            lines.append(
                f"| {cat} | "
                + " | ".join(cells_markdown)
                + f" | {row_total_spots:,} | {row_total_review:,} |"
            )

        # Totals row (column totals for spots)
        col_totals = []
        for m in cols:
            total_spots = sum(
                cells.get(cat, {}).get(m, {"spots": 0})["spots"] for cat in rows
            )
            total_rev = sum(
                cells.get(cat, {}).get(m, {"revenue": 0.0})["revenue"] for cat in rows
            )
            col_totals.append(f"**{total_spots:,} (${total_rev / 1000.0:,.0f}k)**")

        lines.append(
            "| **TOTAL** | "
            + " | ".join(col_totals)
            + f" | **{grand_total_spots:,}** | **{grand_total_review:,}** |"
        )

        return "\n".join(lines)

    def _generate_updated_system_notes(self) -> str:
        return """## 📋 Language Assignment System Notes

**Key Change**: Language-Targeted Advertising now includes **ALL** Internal Ad Sales spots, regardless of spot type.

- Previously: Internal Ad Sales + spot_type IN ('COM', 'BNS', 'BB')
- Now: **ALL** Internal Ad Sales spots (COM, BNS, BB, PKG, CRD, AV, PRG, SVC, etc.)

Other Notes:
- 'L' (undetermined) always requires manual review.
- Valid non-'L' language codes avoid review via direct mapping.
- Other/Review Required category now only includes revenue_type = 'Other'.

"""

    def _generate_updated_faq_section(self) -> str:
        return """## FAQ 

- **Why are PKG/CRD/AV spots no longer in review?**  
  They are now classified as Internal Ad Sales since they have revenue_type = 'Internal Ad Sales'.

- **What changed about Language-Targeted Advertising?**  
  It now includes ALL Internal Ad Sales spots, not just COM/BNS/BB. This simplifies the business logic.

- **Why do some spots still fall into review?**  
  Only spots with revenue_type = 'Other', invalid language codes, low confidence assignments, or system flags.

- **Does the analysis use spot categories?**  
  The new system focuses primarily on revenue_type for business categorization, with spot_type being less critical.
"""

    def generate_updated_unified_tables(self, year_input: str = "2024") -> str:
        full_years, _ = self.parse_year_range(year_input)
        year_display = (
            f"{full_years[0]}-{full_years[-1]}"
            if len(full_years) > 1
            else full_years[0]
        )

        categories = self.get_mutually_exclusive_categories(year_input)
        languages = self.get_unified_language_analysis(year_input)
        validation = self.validate_reconciliation(year_input)

        category_table = self._format_table(
            categories,
            "📊 Business Rule Category Breakdown",
            "Revenue Categories",
            year_display,
        )
        language_table = self._format_table(
            languages,
            "🌐 Language-Targeted Advertising",
            "Internal Ad Sales by Language",
            year_display,
        )

        return f"""# Language-Targeted Advertising Analysis - {year_display}

    ## 🎯 Reconciliation

    - **Years**: {", ".join(validation["base_totals"]["years"])}
    - **Base Revenue**: ${validation["base_totals"]["revenue"]:,.2f}
    - **Category Total**: ${validation["category_totals"]["revenue"]:,.2f}
    - **Revenue Δ**: ${validation["revenue_difference"]:,.2f}
    - **Spots Δ**: {validation["spot_difference"]:,}
    - **Perfect?**: {"✅ YES" if validation["perfect_reconciliation"] else "❌ NO"}

    {category_table}

    {language_table}

    {self._generate_updated_system_notes()}

    {self._generate_updated_faq_section()}
    """


def main():
    import argparse

    p = argparse.ArgumentParser(
        description="Updated Unified Analysis - Language-Targeted Advertising Fix",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python unified_analysis.py --year 2024
  python unified_analysis.py --year 2023-2024
  python unified_analysis.py --year 2024 --assignment-methods-only
  python unified_analysis.py --year 2024 --export-review review_2024.csv
  python unified_analysis.py --year 2024 --export-review review.csv --review-type undetermined
  python src/unified_analysis.py --year 2024 --output reports/updated_unified_2024.md
        """,
    )
    p.add_argument(
        "--year", default="2024", help="Year or range, e.g. 2024 or 2023-2024"
    )
    p.add_argument(
        "--db-path", default="data/database/production.db", help="SQLite DB path"
    )
    p.add_argument("--output", help="Write full report (Markdown) to this file")
    p.add_argument(
        "--validate-only", action="store_true", help="Only run reconciliation checks"
    )
    p.add_argument(
        "--assignment-methods-only",
        action="store_true",
        help="Print only assignment method analysis",
    )
    # New: Python export of review lines
    p.add_argument(
        "--export-review", metavar="CSV", help="Export review-required lines to CSV"
    )
    p.add_argument(
        "--review-type",
        default="all",
        choices=[
            "all",
            "business",
            "undetermined",
            "invalid",
            "high-value",
            "low-confidence",
            "fallback",
        ],
        help="Filter for --export-review (default: all)",
    )
    args = p.parse_args()

    try:
        with UpdatedUnifiedAnalysisEngine(args.db_path) as eng:
            if args.export_review:
                count = eng.export_review_required(
                    args.year, args.export_review, args.review_type
                )
                print(
                    f"✅ Exported {count} review-required rows to {args.export_review}"
                )
                return

            if args.validate_only:
                v = eng.validate_reconciliation(args.year)
                print("🧪 Validation")
                print("=" * 50)
                print(f"Years: {', '.join(v['base_totals']['years'])}")
                print(f"Base Revenue: ${v['base_totals']['revenue']:,.2f}")
                print(f"Category Total: ${v['category_totals']['revenue']:,.2f}")
                print(f"Revenue Δ: ${v['revenue_difference']:,.2f}")
                print(f"Perfect: {'YES' if v['perfect_reconciliation'] else 'NO'}")
                print(f"Change: {v['language_targeted_advertising_change']}")
                return

            if args.assignment_methods_only:
                rows = eng.get_assignment_method_analysis(args.year)
                print("🔧 Assignment Method Analysis")
                print("=" * 50)
                for r in rows:
                    avg_conf = r.details.get("avg_confidence", 0) if r.details else 0
                    rev_cnt = r.details.get("review_count", 0) if r.details else 0
                    print(
                        f"{r.name}: ${r.revenue:,.2f} ({r.percentage:.1f}%) - {r.total_spots:,} spots"
                    )
                    print(f"  Confidence: {avg_conf:.2f}, Review Count: {rev_cnt:,}")
                return

            report = eng.generate_updated_unified_tables(args.year)
            if args.output:
                d = os.path.dirname(args.output)
                if d:
                    os.makedirs(d, exist_ok=True)
                with open(args.output, "w", encoding="utf-8") as f:
                    f.write(report)
                print(f"✅ Report saved to {args.output}")
            else:
                print(report)

    except ValueError as e:
        print(f"❌ Input Error: {e}")
        print("💡 Use --year 2024 or --year 2023-2024")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
