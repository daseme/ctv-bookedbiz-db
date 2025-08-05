#!/usr/bin/env python3
"""
Simple Language Analysis Table Generator
========================================

This script generates the exact language analysis table format
requested, including Hmong and all bonus spot details.

Usage:
    python language_table_generator.py --year 2024
    python language_table_generator.py --year 2024 --output report.md
    python language_table_generator.py --year 2024 --check-hmong

Requirements:
    - Place in same directory as query_builders.py
    - Database at data/database/production.db
"""

import sqlite3
import argparse
from typing import List, Dict, Any
from dataclasses import dataclass


@dataclass
class LanguageResult:
    """Language analysis result"""
    language: str
    revenue: float
    percentage: float
    paid_spots: int
    bonus_spots: int
    total_spots: int
    avg_per_spot: float


class SimpleLanguageAnalyzer:
    """Simple analyzer focused on generating the requested table"""
    
    def __init__(self, db_path: str = "data/database/production.db"):
        self.db_path = db_path
    
    def generate_language_table(self, year: str = "2024") -> str:
        """Generate the exact table format requested"""
        
        with sqlite3.connect(self.db_path) as db:
            cursor = db.cursor()
            
            # Get all language results in one query
            results = self._get_all_language_results(cursor, year)
            
            # Get Chinese Prime Time separately
            cpt_result = self._get_chinese_prime_time_result(cursor, year)
            if cpt_result:
                results.append(cpt_result)
            
            # Calculate totals for percentage
            total_revenue = sum(r.revenue for r in results)
            
            # Calculate percentages
            for result in results:
                result.percentage = (result.revenue / total_revenue) * 100 if total_revenue > 0 else 0
            
            # Sort by revenue descending
            results.sort(key=lambda x: x.revenue, reverse=True)
            
            # Generate the table
            return self._format_table(results, year)
    
    def _get_all_language_results(self, cursor, year: str) -> List[LanguageResult]:
        """Get all individual language results including Hmong"""
        year_suffix = year[-2:]
        
        query = """
        SELECT 
            CASE 
                WHEN l.language_name IN ('Mandarin', 'Cantonese') THEN 'Chinese'
                WHEN l.language_name = 'Hmong' THEN 'Hmong'
                ELSE COALESCE(l.language_name, 'Unknown Language')
            END as language,
            SUM(COALESCE(s.gross_rate, 0)) as revenue,
            COUNT(CASE WHEN s.spot_type != 'BNS' OR s.spot_type IS NULL THEN 1 END) as paid_spots,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bonus_spots,
            COUNT(*) as total_spots
        FROM spots s
        LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        LEFT JOIN language_blocks lb ON slb.block_id = lb.block_id
        LEFT JOIN languages l ON lb.language_id = l.language_id
        LEFT JOIN agencies a ON s.agency_id = a.agency_id
        WHERE s.broadcast_month LIKE ?
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
        AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
        AND ((slb.spans_multiple_blocks = 0 AND slb.block_id IS NOT NULL) OR 
             (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NOT NULL))
        GROUP BY CASE 
            WHEN l.language_name IN ('Mandarin', 'Cantonese') THEN 'Chinese'
            WHEN l.language_name = 'Hmong' THEN 'Hmong'
            ELSE COALESCE(l.language_name, 'Unknown Language')
        END
        HAVING SUM(COALESCE(s.gross_rate, 0)) > 0 OR COUNT(*) > 0
        ORDER BY SUM(COALESCE(s.gross_rate, 0)) DESC
        """
        
        cursor.execute(query, [f"%-{year_suffix}"])
        
        results = []
        for row in cursor.fetchall():
            language, revenue, paid_spots, bonus_spots, total_spots = row
            avg_per_spot = revenue / total_spots if total_spots > 0 else 0
            
            results.append(LanguageResult(
                language=language,
                revenue=revenue,
                percentage=0,  # Will be calculated later
                paid_spots=paid_spots,
                bonus_spots=bonus_spots,
                total_spots=total_spots,
                avg_per_spot=avg_per_spot
            ))
        
        return results
    
    def _get_chinese_prime_time_result(self, cursor, year: str) -> LanguageResult:
        """Get Chinese Prime Time as separate category"""
        year_suffix = year[-2:]
        
        query = """
        SELECT 
            SUM(COALESCE(s.gross_rate, 0)) as revenue,
            COUNT(CASE WHEN s.spot_type != 'BNS' OR s.spot_type IS NULL THEN 1 END) as paid_spots,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bonus_spots,
            COUNT(*) as total_spots
        FROM spots s
        LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        LEFT JOIN agencies a ON s.agency_id = a.agency_id
        WHERE s.broadcast_month LIKE ?
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
        AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
        AND (slb.spans_multiple_blocks = 1 OR 
             (slb.spans_multiple_blocks = 0 AND slb.block_id IS NULL) OR 
             (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NULL))
        AND (
            -- Chinese Prime Time M-F 7pm-11:59pm
            (s.time_in >= '19:00:00' AND s.time_out <= '23:59:59' 
             AND s.day_of_week IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'))
            OR
            -- Chinese Weekend 8pm-11:59pm  
            (s.time_in >= '20:00:00' AND s.time_out <= '23:59:59'
             AND s.day_of_week IN ('Saturday', 'Sunday'))
        )
        """
        
        cursor.execute(query, [f"%-{year_suffix}"])
        row = cursor.fetchone()
        
        if row and row[0]:
            revenue, paid_spots, bonus_spots, total_spots = row
            avg_per_spot = revenue / total_spots if total_spots > 0 else 0
            
            return LanguageResult(
                language="Chinese Prime Time",
                revenue=revenue,
                percentage=0,  # Will be calculated later
                paid_spots=paid_spots,
                bonus_spots=bonus_spots,
                total_spots=total_spots,
                avg_per_spot=avg_per_spot
            )
        
        return None
    
    def _format_table(self, results: List[LanguageResult], year: str) -> str:
        """Format the results into the requested table format"""
        
        # Calculate totals
        total_revenue = sum(r.revenue for r in results)
        total_paid_spots = sum(r.paid_spots for r in results)
        total_bonus_spots = sum(r.bonus_spots for r in results)
        total_all_spots = sum(r.total_spots for r in results)
        total_avg_per_spot = total_revenue / total_all_spots if total_all_spots > 0 else 0
        
        # Build the table
        table = f"""## 🌐 Language Analysis
### Combined Language Performance ({year})
| Language | Revenue | % of Total | Spots | Bonus Spots | Total Spots | Avg/Spot |
|----------|---------|------------|-------|-------------|-------------|----------|
"""
        
        for result in results:
            table += f"| {result.language} | ${result.revenue:,.2f} | {result.percentage:.1f}% | {result.paid_spots:,} | {result.bonus_spots:,} | {result.total_spots:,} | ${result.avg_per_spot:.2f} |\n"
        
        # Add total row
        table += "|----------|---------|------------|-------|-------------|-------------|----------|\n"
        table += f"| **TOTAL** | **${total_revenue:,.2f}** | **100.0%** | **{total_paid_spots:,}** | **{total_bonus_spots:,}** | **{total_all_spots:,}** | **${total_avg_per_spot:.2f}** |\n"
        
        return table
    
    def check_hmong_status(self, year: str = "2024") -> Dict[str, Any]:
        """Check if Hmong is included in the analysis"""
        
        with sqlite3.connect(self.db_path) as db:
            cursor = db.cursor()
            year_suffix = year[-2:]
            
            # Check for Hmong spots
            query = """
            SELECT 
                COUNT(*) as total_spots,
                SUM(COALESCE(s.gross_rate, 0)) as revenue,
                COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bonus_spots
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            LEFT JOIN language_blocks lb ON slb.block_id = lb.block_id
            LEFT JOIN languages l ON lb.language_id = l.language_id
            WHERE s.broadcast_month LIKE ?
            AND l.language_name = 'Hmong'
            AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
            AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
            """
            
            cursor.execute(query, [f"%-{year_suffix}"])
            result = cursor.fetchone()
            
            if result and result[0] > 0:
                return {
                    'found': True,
                    'total_spots': result[0],
                    'revenue': result[1],
                    'bonus_spots': result[2],
                    'avg_per_spot': result[1] / result[0] if result[0] > 0 else 0
                }
            else:
                # Check if Hmong exists in database at all
                cursor.execute("SELECT COUNT(*) FROM languages WHERE language_name = 'Hmong'")
                exists = cursor.fetchone()[0] > 0
                
                return {
                    'found': False,
                    'exists_in_db': exists,
                    'message': 'Hmong language found in database but no spots for this year' if exists else 'Hmong language not found in database'
                }


def main():
    parser = argparse.ArgumentParser(description="Generate Language Analysis Table")
    parser.add_argument("--year", default="2024", help="Year to analyze")
    parser.add_argument("--output", help="Output file path")
    parser.add_argument("--check-hmong", action="store_true", help="Check Hmong inclusion status")
    parser.add_argument("--db-path", default="data/database/production.db", help="Database path")
    
    args = parser.parse_args()
    
    analyzer = SimpleLanguageAnalyzer(args.db_path)
    
    if args.check_hmong:
        print(f"🔍 Checking Hmong status for {args.year}...")
        hmong_status = analyzer.check_hmong_status(args.year)
        
        if hmong_status['found']:
            print(f"✅ Hmong found:")
            print(f"   Total spots: {hmong_status['total_spots']:,}")
            print(f"   Revenue: ${hmong_status['revenue']:,.2f}")
            print(f"   Bonus spots: {hmong_status['bonus_spots']:,}")
            print(f"   Avg per spot: ${hmong_status['avg_per_spot']:.2f}")
        else:
            print(f"❌ Hmong not found: {hmong_status['message']}")
        print()
    
    print(f"📊 Generating language analysis table for {args.year}...")
    table = analyzer.generate_language_table(args.year)
    
    if args.output:
        with open(args.output, 'w') as f:
            f.write(table)
        print(f"✅ Table saved to {args.output}")
    else:
        print(table)


if __name__ == "__main__":
    main()