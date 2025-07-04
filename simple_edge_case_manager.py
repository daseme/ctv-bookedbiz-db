#!/usr/bin/env python3
"""
Simplified Edge Case Management (Post-Business Rules)
===================================================

A streamlined approach to managing the remaining 86,743 unassigned spots
after business rules eliminated 47.5% of false positives.
"""

import sqlite3
import argparse
from typing import List, Dict, Any, Optional
from datetime import datetime


class SimpleEdgeCaseManager:
    """
    Simplified edge case manager focused on true unassigned spots.
    
    This replaces the complex edge case system with a streamlined approach
    that focuses on the real work: managing the remaining unassigned spots.
    """
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
    
    def connect(self):
        """Connect to database."""
        self.conn = sqlite3.connect(self.db_path)
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
    
    def get_unassigned_stats(self) -> Dict[str, Any]:
        """Get statistics about unassigned spots."""
        cursor = self.conn.cursor()
        
        # Overall stats for ALL unassigned spots
        cursor.execute("""
            SELECT 
                COUNT(*) as total_unassigned,
                COALESCE(SUM(s.gross_rate), 0) as total_revenue,
                COALESCE(AVG(s.gross_rate), 0) as avg_revenue,
                COUNT(DISTINCT s.market_id) as markets_affected,
                COUNT(DISTINCT s.customer_id) as customers_affected
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            WHERE slb.spot_id IS NULL
              AND s.market_id IS NOT NULL
              AND s.time_in IS NOT NULL
              AND s.time_out IS NOT NULL
        """)
        
        overall = cursor.fetchone()
        
        # Stats for ALL unassigned spots with revenue (including incomplete data)
        cursor.execute("""
            SELECT 
                COUNT(*) as all_valuable_unassigned,
                COALESCE(SUM(s.gross_rate), 0) as all_valuable_revenue
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            WHERE slb.spot_id IS NULL
              AND s.gross_rate > 0
        """)
        
        all_valuable = cursor.fetchone()
        
        # Stats for COMPLETE unassigned spots with revenue
        cursor.execute("""
            SELECT 
                COUNT(*) as valuable_unassigned,
                COALESCE(SUM(s.gross_rate), 0) as valuable_revenue,
                COALESCE(AVG(s.gross_rate), 0) as avg_valuable_revenue
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            WHERE slb.spot_id IS NULL
              AND s.market_id IS NOT NULL
              AND s.time_in IS NOT NULL
              AND s.time_out IS NOT NULL
              AND s.gross_rate > 0
        """)
        
        valuable = cursor.fetchone()
        
        # Check what's causing the filtering
        cursor.execute("""
            SELECT 
                'Missing market_id' as issue,
                COUNT(*) as count,
                COALESCE(SUM(s.gross_rate), 0) as lost_revenue
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            WHERE slb.spot_id IS NULL
              AND s.gross_rate > 0
              AND s.market_id IS NULL
            
            UNION ALL
            
            SELECT 
                'Missing time_in' as issue,
                COUNT(*) as count,
                COALESCE(SUM(s.gross_rate), 0) as lost_revenue
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            WHERE slb.spot_id IS NULL
              AND s.gross_rate > 0
              AND s.time_in IS NULL
            
            UNION ALL
            
            SELECT 
                'Missing time_out' as issue,
                COUNT(*) as count,
                COALESCE(SUM(s.gross_rate), 0) as lost_revenue
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            WHERE slb.spot_id IS NULL
              AND s.gross_rate > 0
              AND s.time_out IS NULL
        """)
        
        data_issues = [
            {'issue': row[0], 'count': row[1], 'lost_revenue': row[2] or 0}
            for row in cursor.fetchall()
        ]
        
        # By revenue tier (only for complete valuable spots)
        if valuable[0] > 0:
            cursor.execute("""
                SELECT 
                    CASE 
                        WHEN s.gross_rate > 2000 THEN 'Very High (>$2000)'
                        WHEN s.gross_rate > 1000 THEN 'High ($1000-$2000)'
                        WHEN s.gross_rate > 500 THEN 'Medium ($500-$1000)'
                        WHEN s.gross_rate > 100 THEN 'Low ($100-$500)'
                        ELSE 'Very Low ($0-$100)'
                    END as revenue_tier,
                    COUNT(*) as count,
                    SUM(s.gross_rate) as revenue
                FROM spots s
                LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
                WHERE slb.spot_id IS NULL
                  AND s.market_id IS NOT NULL
                  AND s.time_in IS NOT NULL
                  AND s.time_out IS NOT NULL
                  AND s.gross_rate > 0
                GROUP BY revenue_tier
                ORDER BY revenue DESC
            """)
            
            by_revenue = [{'tier': row[0], 'count': row[1], 'revenue': row[2] or 0} for row in cursor.fetchall()]
        else:
            by_revenue = []
        
        # By sector (only for complete valuable spots)
        if valuable[0] > 0:
            cursor.execute("""
                SELECT 
                    sec.sector_code,
                    COUNT(*) as count,
                    SUM(s.gross_rate) as revenue,
                    AVG(s.gross_rate) as avg_revenue
                FROM spots s
                LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
                LEFT JOIN customers c ON s.customer_id = c.customer_id
                LEFT JOIN sectors sec ON c.sector_id = sec.sector_id
                WHERE slb.spot_id IS NULL
                  AND s.market_id IS NOT NULL
                  AND s.time_in IS NOT NULL
                  AND s.time_out IS NOT NULL
                  AND s.gross_rate > 0
                GROUP BY sec.sector_code
                ORDER BY revenue DESC
                LIMIT 10
            """)
            
            by_sector = [
                {'sector': row[0] or 'Unknown', 'count': row[1], 'revenue': row[2] or 0, 'avg_revenue': row[3] or 0}
                for row in cursor.fetchall()
            ]
        else:
            by_sector = []
        
        return {
            'total_unassigned': overall[0] or 0,
            'total_revenue': overall[1] or 0,
            'avg_revenue': overall[2] or 0,
            'markets_affected': overall[3] or 0,
            'customers_affected': overall[4] or 0,
            'all_valuable_unassigned': all_valuable[0] or 0,
            'all_valuable_revenue': all_valuable[1] or 0,
            'valuable_unassigned': valuable[0] or 0,
            'valuable_revenue': valuable[1] or 0,
            'avg_valuable_revenue': valuable[2] or 0,
            'zero_revenue_spots': (overall[0] or 0) - (valuable[0] or 0),
            'incomplete_valuable_spots': (all_valuable[0] or 0) - (valuable[0] or 0),
            'data_issues': data_issues,
            'by_revenue_tier': by_revenue,
            'by_sector': by_sector
        }
    
    def get_high_value_unassigned(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get high-value unassigned spots for manual review."""
        cursor = self.conn.cursor()
        
        cursor.execute("""
            SELECT 
                s.spot_id,
                s.bill_code,
                s.air_date,
                s.time_in,
                s.time_out,
                s.gross_rate,
                m.market_code,
                m.market_name,
                c.normalized_name as customer_name,
                sec.sector_code,
                sec.sector_name,
                spot_lang.language_name as spot_language,
                CAST((strftime('%s', s.time_out) - strftime('%s', s.time_in)) / 60 AS INTEGER) as duration_minutes
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            LEFT JOIN markets m ON s.market_id = m.market_id
            LEFT JOIN customers c ON s.customer_id = c.customer_id
            LEFT JOIN sectors sec ON c.sector_id = sec.sector_id
            LEFT JOIN languages spot_lang ON s.language_id = spot_lang.language_id
            WHERE slb.spot_id IS NULL
              AND s.market_id IS NOT NULL
              AND s.time_in IS NOT NULL
              AND s.time_out IS NOT NULL
              AND s.gross_rate > 0
            ORDER BY s.gross_rate DESC
            LIMIT ?
        """, (limit,))
        
        return [
            {
                'spot_id': row[0],
                'bill_code': row[1],
                'air_date': row[2],
                'time_in': row[3],
                'time_out': row[4],
                'gross_rate': row[5],  # Don't default to 0 here since we're filtering for > 0
                'market_code': row[6],
                'market_name': row[7],
                'customer_name': row[8],
                'sector_code': row[9],
                'sector_name': row[10],
                'spot_language': row[11],
                'duration_minutes': row[12] or 0
            }
            for row in cursor.fetchall()
        ]
    
    def identify_automation_opportunities(self) -> List[Dict[str, Any]]:
        """Identify patterns that could become new business rules."""
        cursor = self.conn.cursor()
        
        # Look for sector patterns (only valuable spots)
        cursor.execute("""
            SELECT 
                sec.sector_code,
                sec.sector_name,
                COUNT(*) as spot_count,
                SUM(s.gross_rate) as total_revenue,
                AVG(s.gross_rate) as avg_revenue,
                AVG(CASE 
                    WHEN s.time_in IS NOT NULL AND s.time_out IS NOT NULL 
                    THEN CAST((strftime('%s', s.time_out) - strftime('%s', s.time_in)) / 60 AS INTEGER)
                    ELSE 0
                END) as avg_duration
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            LEFT JOIN customers c ON s.customer_id = c.customer_id
            LEFT JOIN sectors sec ON c.sector_id = sec.sector_id
            WHERE slb.spot_id IS NULL
              AND s.market_id IS NOT NULL
              AND s.time_in IS NOT NULL
              AND s.time_out IS NOT NULL
              AND sec.sector_code IS NOT NULL
              AND sec.sector_code NOT IN ('MEDIA', 'GOV', 'POLITICAL', 'NPO')  -- Exclude existing rules
              AND s.gross_rate > 0  -- Only valuable spots
            GROUP BY sec.sector_code, sec.sector_name
            HAVING COUNT(*) > 5  -- At least 5 spots
            ORDER BY total_revenue DESC
        """)
        
        opportunities = []
        for row in cursor.fetchall():
            opportunities.append({
                'type': 'sector_rule',
                'sector_code': row[0],
                'sector_name': row[1],
                'spot_count': row[2],
                'total_revenue': row[3] or 0,
                'avg_revenue': row[4] or 0,
                'avg_duration': row[5] if row[5] is not None else 0,
                'recommended_rule': f"Auto-assign all {row[1]} spots (avg: ${row[4] or 0:.0f})",
                'confidence': 'high' if row[2] > 20 else 'medium'
            })
        
        # Look for duration patterns (only valuable spots)
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN duration_minutes >= 960 THEN '16+ hours'
                    WHEN duration_minutes >= 600 THEN '10-16 hours'
                    WHEN duration_minutes >= 360 THEN '6-10 hours'
                    WHEN duration_minutes >= 240 THEN '4-6 hours'
                    ELSE '2-4 hours'
                END as duration_range,
                COUNT(*) as spot_count,
                SUM(gross_rate) as total_revenue,
                AVG(gross_rate) as avg_revenue
            FROM (
                SELECT 
                    s.spot_id,
                    s.gross_rate,
                    CAST((strftime('%s', s.time_out) - strftime('%s', s.time_in)) / 60 AS INTEGER) as duration_minutes
                FROM spots s
                LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
                WHERE slb.spot_id IS NULL
                  AND s.market_id IS NOT NULL
                  AND s.time_in IS NOT NULL
                  AND s.time_out IS NOT NULL
                  AND s.gross_rate > 0  -- Only valuable spots
                  AND CAST((strftime('%s', s.time_out) - strftime('%s', s.time_in)) / 60 AS INTEGER) >= 120
            ) duration_analysis
            GROUP BY duration_range
            HAVING COUNT(*) > 5  -- At least 5 spots
            ORDER BY total_revenue DESC
        """)
        
        for row in cursor.fetchall():
            opportunities.append({
                'type': 'duration_rule',
                'duration_range': row[0],
                'spot_count': row[1],
                'total_revenue': row[2] or 0,
                'avg_revenue': row[3] or 0,
                'recommended_rule': f"Auto-assign all {row[0]} content (avg: ${row[3] or 0:.0f})",
                'confidence': 'high' if row[1] > 20 else 'medium'
            })
        
        return opportunities
        
    def analyze_valuable_unassigned(self) -> Dict[str, Any]:
        """Analyze why valuable spots weren't captured by business rules."""
        cursor = self.conn.cursor()
        
        # Check ALL valuable spots (including incomplete ones)
        cursor.execute("""
            SELECT 
                sec.sector_code,
                CASE 
                    WHEN s.market_id IS NULL THEN 'Missing market_id - cannot assign'
                    WHEN s.time_in IS NULL THEN 'Missing time_in - cannot assign'
                    WHEN s.time_out IS NULL THEN 'Missing time_out - cannot assign'
                    WHEN sec.sector_code = 'MEDIA' THEN 'Should be caught by MEDIA rule'
                    WHEN sec.sector_code = 'GOV' THEN 'Should be caught by GOV rule'
                    WHEN sec.sector_code = 'POLITICAL' THEN 'Should be caught by POLITICAL rule'
                    WHEN sec.sector_code = 'NPO' AND s.time_in IS NOT NULL AND s.time_out IS NOT NULL 
                         AND CAST((strftime('%s', s.time_out) - strftime('%s', s.time_in)) / 60 AS INTEGER) >= 300 
                         THEN 'Should be caught by NPO 5+ hour rule'
                    WHEN s.time_in IS NOT NULL AND s.time_out IS NOT NULL 
                         AND CAST((strftime('%s', s.time_out) - strftime('%s', s.time_in)) / 60 AS INTEGER) >= 720 
                         THEN 'Should be caught by 12+ hour rule'
                    ELSE 'Not covered by existing rules'
                END as rule_status,
                COUNT(*) as spot_count,
                SUM(s.gross_rate) as total_revenue,
                AVG(s.gross_rate) as avg_revenue,
                AVG(CAST((strftime('%s', s.time_out) - strftime('%s', s.time_in)) / 60 AS INTEGER)) as avg_duration
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            LEFT JOIN customers c ON s.customer_id = c.customer_id
            LEFT JOIN sectors sec ON c.sector_id = sec.sector_id
            WHERE slb.spot_id IS NULL
              AND s.gross_rate > 0
            GROUP BY sec.sector_code, rule_status
            ORDER BY total_revenue DESC
        """)
        
        analysis = []
        for row in cursor.fetchall():
            analysis.append({
                'sector_code': row[0] or 'Unknown',
                'rule_status': row[1],
                'spot_count': row[2],
                'total_revenue': row[3] or 0,
                'avg_revenue': row[4] or 0,
                'avg_duration': row[5] or 0
            })
        
        return {
            'total_valuable_unassigned': sum(item['spot_count'] for item in analysis),
            'total_valuable_revenue': sum(item['total_revenue'] for item in analysis),
            'analysis': analysis
        }

    def print_valuable_analysis(self):
        """Print analysis of why valuable spots weren't captured."""
        analysis = self.analyze_valuable_unassigned()
        
        print("🔍 VALUABLE SPOTS ANALYSIS")
        print("=" * 50)
        print(f"Total valuable unassigned: {analysis['total_valuable_unassigned']:,}")
        print(f"Total revenue: ${analysis['total_valuable_revenue']:,.2f}")
        print()
        
        print("📋 BREAKDOWN BY RULE STATUS:")
        for item in analysis['analysis']:
            avg_duration = item['avg_duration'] or 0
            print(f"  • {item['sector_code']} - {item['rule_status']}")
            print(f"    - {item['spot_count']:,} spots, ${item['total_revenue']:,.2f} revenue")
            print(f"    - Avg: ${item['avg_revenue']:,.2f}, Duration: {avg_duration:.0f} min")
            print()
    
    def create_assignment_batch(self, criteria: str, limit: int = 100) -> List[int]:
        """Create a batch of spots for manual assignment based on criteria."""
        cursor = self.conn.cursor()
        
        base_query = """
            SELECT s.spot_id
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            LEFT JOIN customers c ON s.customer_id = c.customer_id
            LEFT JOIN sectors sec ON c.sector_id = sec.sector_id
            WHERE slb.spot_id IS NULL
              AND s.market_id IS NOT NULL
              AND s.time_in IS NOT NULL
              AND s.time_out IS NOT NULL
        """
        
        if criteria == 'high_value':
            base_query += " AND s.gross_rate > 100 ORDER BY s.gross_rate DESC"
        elif criteria == 'specific_sector':
            base_query += " AND sec.sector_code IN ('HEALTH', 'AUTO', 'RETAIL') AND s.gross_rate > 0 ORDER BY s.gross_rate DESC"
        elif criteria == 'long_duration':
            base_query += " AND CAST((strftime('%s', s.time_out) - strftime('%s', s.time_in)) / 60 AS INTEGER) > 360 AND s.gross_rate > 0 ORDER BY s.gross_rate DESC"
        elif criteria == 'valuable_only':
            base_query += " AND s.gross_rate > 0 ORDER BY s.gross_rate DESC"
        else:
            base_query += " ORDER BY COALESCE(s.gross_rate, 0) DESC"
        
        base_query += f" LIMIT {limit}"
        
        cursor.execute(base_query)
        return [row[0] for row in cursor.fetchall()]
    
    def print_summary(self):
        """Print a summary of unassigned spots."""
        stats = self.get_unassigned_stats()
        
        print("🎯 UNASSIGNED SPOTS SUMMARY")
        print("=" * 50)
        print(f"Total unassigned spots: {stats['total_unassigned']:,}")
        print(f"Total revenue: ${stats['total_revenue']:,.0f}")
        print(f"Average revenue: ${stats['avg_revenue']:,.0f}")
        print(f"Markets affected: {stats['markets_affected']}")
        print(f"Customers affected: {stats['customers_affected']}")
        
        print(f"\n📊 BY REVENUE TIER:")
        for tier in stats['by_revenue_tier']:
            revenue = tier['revenue'] or 0
            print(f"  • {tier['tier']}: {tier['count']:,} spots (${revenue:,.0f})")
        
        print(f"\n🏢 TOP SECTORS:")
        for sector in stats['by_sector'][:5]:
            revenue = sector['revenue'] or 0
            print(f"  • {sector['sector']}: {sector['count']:,} spots (${revenue:,.0f})")
        
        # Show automation opportunities
        opportunities = self.identify_automation_opportunities()
        if opportunities:
            print(f"\n🚀 AUTOMATION OPPORTUNITIES:")
            for opp in opportunities[:3]:
                print(f"  • {opp['type']}: {opp.get('sector_name', opp.get('duration_range', 'N/A'))}")
                print(f"    - {opp['spot_count']} spots worth ${opp['total_revenue']:,.0f}")
                print(f"    - {opp['recommended_rule']}")


def main():
    """Main CLI function."""
    parser = argparse.ArgumentParser(description="Simplified Edge Case Management")
    parser.add_argument("--db", default="./data/database/production.db", help="Database path")
    parser.add_argument("--stats", action="store_true", help="Show statistics")
    parser.add_argument("--high-value", type=int, help="List N high-value unassigned spots")
    parser.add_argument("--analyze-valuable", action="store_true", help="Analyze why valuable spots weren't captured by business rules")
    parser.add_argument("--opportunities", action="store_true", help="Show automation opportunities")
    parser.add_argument("--batch", choices=['high_value', 'specific_sector', 'long_duration', 'valuable_only'], 
                       help="Create assignment batch")
    parser.add_argument("--limit", type=int, default=100, help="Limit for batch creation")
    
    args = parser.parse_args()
    
    manager = SimpleEdgeCaseManager(args.db)
    manager.connect()
    
    try:
        if args.stats:
            manager.print_summary()
        elif args.high_value:
            spots = manager.get_high_value_unassigned(args.high_value)
            print(f"\n💰 TOP {args.high_value} HIGH-VALUE UNASSIGNED SPOTS:")
            print("-" * 80)
            if spots:
                for spot in spots:
                    gross_rate = spot['gross_rate'] or 0
                    duration = spot['duration_minutes'] or 0
                    customer = spot['customer_name'] or 'Unknown'
                    market = spot['market_name'] or 'Unknown'
                    sector = spot['sector_name'] or 'Unknown'
                    print(f"Spot {spot['spot_id']}: ${gross_rate:,.2f} - {customer[:30]}...")
                    print(f"  {market} | {sector} | {duration}min")
            else:
                print("No high-value unassigned spots found (all spots with revenue > 0 have been processed)")
        elif args.analyze_valuable:
            manager.print_valuable_analysis()
        elif args.opportunities:
            opportunities = manager.identify_automation_opportunities()
            print(f"\n🚀 AUTOMATION OPPORTUNITIES:")
            print("-" * 50)
            for opp in opportunities:
                total_revenue = opp['total_revenue'] or 0
                print(f"• {opp['type'].upper()}: {opp.get('sector_name', opp.get('duration_range', 'N/A'))}")
                print(f"  - {opp['spot_count']} spots worth ${total_revenue:,.0f}")
                print(f"  - {opp['recommended_rule']}")
                print(f"  - Confidence: {opp['confidence']}")
                print()
        elif args.batch:
            spot_ids = manager.create_assignment_batch(args.batch, args.limit)
            print(f"Created batch of {len(spot_ids)} spots for '{args.batch}' criteria:")
            print(f"Spot IDs: {spot_ids[:10]}..." if len(spot_ids) > 10 else f"Spot IDs: {spot_ids}")
        else:
            manager.print_summary()
    
    finally:
        manager.close()


if __name__ == "__main__":
    main()