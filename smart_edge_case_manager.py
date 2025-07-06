#!/usr/bin/env python3
"""
Smart Edge Case Manager (Post-Business Rules Analysis)
====================================================

A refined approach to managing genuine edge cases after understanding that:
- 96.3% of billable content is correctly assigned
- Most "unassigned" spots are correctly excluded (bonus spots, zero revenue, etc.)
- True edge cases requiring manual review are <4% of billable content

This tool helps identify and manage the genuine edge cases while validating
that the business rules system is working correctly.
"""

import sqlite3
import argparse
from typing import List, Dict, Any, Optional
from datetime import datetime


class SmartEdgeCaseManager:
    """
    Smart edge case manager that understands the difference between:
    - Correctly excluded spots (bonus, zero revenue, billing entries)
    - Genuine edge cases requiring manual review
    - System validation and health checks
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
    
    def get_system_health(self) -> Dict[str, Any]:
        """Get system health metrics to validate business rules are working."""
        cursor = self.conn.cursor()
        
        # Overall system metrics
        cursor.execute("""
            SELECT 
                COUNT(*) as total_spots,
                COUNT(CASE WHEN slb.spot_id IS NOT NULL THEN 1 END) as assigned_spots,
                COUNT(CASE WHEN slb.spot_id IS NULL THEN 1 END) as unassigned_spots,
                COUNT(CASE WHEN slb.business_rule_applied IS NOT NULL THEN 1 END) as business_rule_spots
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        """)
        
        overall = cursor.fetchone()
        
        # Bonus spot exclusion (should be 100%)
        cursor.execute("""
            SELECT 
                COUNT(*) as total_bns,
                COUNT(CASE WHEN slb.spot_id IS NULL THEN 1 END) as excluded_bns,
                ROUND(COUNT(CASE WHEN slb.spot_id IS NULL THEN 1 END) * 100.0 / COUNT(*), 2) as exclusion_rate
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            WHERE s.spot_type = 'BNS'
        """)
        
        bns_stats = cursor.fetchone()
        
        # Commercial spot assignment rate for revenue spots
        cursor.execute("""
            SELECT 
                COUNT(*) as total_revenue_com,
                COUNT(CASE WHEN slb.spot_id IS NOT NULL THEN 1 END) as assigned_revenue_com,
                ROUND(COUNT(CASE WHEN slb.spot_id IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as assignment_rate
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            WHERE s.spot_type = 'COM' AND s.gross_rate > 0
        """)
        
        com_stats = cursor.fetchone()
        
        # Zero revenue exclusion
        cursor.execute("""
            SELECT 
                COUNT(*) as total_zero_revenue,
                COUNT(CASE WHEN slb.spot_id IS NULL THEN 1 END) as excluded_zero_revenue,
                ROUND(COUNT(CASE WHEN slb.spot_id IS NULL THEN 1 END) * 100.0 / COUNT(*), 2) as exclusion_rate
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            WHERE s.gross_rate = 0 OR s.gross_rate IS NULL
        """)
        
        zero_rev_stats = cursor.fetchone()
        
        return {
            'total_spots': overall[0] or 0,
            'assigned_spots': overall[1] or 0,
            'unassigned_spots': overall[2] or 0,
            'business_rule_spots': overall[3] or 0,
            'overall_assignment_rate': round((overall[1] or 0) * 100.0 / (overall[0] or 1), 2),
            'business_rule_rate': round((overall[3] or 0) * 100.0 / (overall[0] or 1), 2),
            'bns_total': bns_stats[0] or 0,
            'bns_excluded': bns_stats[1] or 0,
            'bns_exclusion_rate': bns_stats[2] or 0,
            'com_revenue_total': com_stats[0] or 0,
            'com_revenue_assigned': com_stats[1] or 0,
            'com_assignment_rate': com_stats[2] or 0,
            'zero_rev_total': zero_rev_stats[0] or 0,
            'zero_rev_excluded': zero_rev_stats[1] or 0,
            'zero_rev_exclusion_rate': zero_rev_stats[2] or 0
        }
    
    def get_genuine_edge_cases(self) -> Dict[str, Any]:
        """Get genuine edge cases that need manual review."""
        cursor = self.conn.cursor()
        
        # Genuine edge cases: COM spots with revenue, complete data, not assigned
        cursor.execute("""
            SELECT 
                COUNT(*) as genuine_edge_cases,
                COALESCE(SUM(s.gross_rate), 0) as total_revenue,
                COALESCE(AVG(s.gross_rate), 0) as avg_revenue,
                COUNT(DISTINCT s.market_id) as markets_affected,
                COUNT(DISTINCT s.customer_id) as customers_affected
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            WHERE slb.spot_id IS NULL
              AND s.spot_type = 'COM'
              AND s.gross_rate > 0
              AND s.market_id IS NOT NULL
              AND s.time_in IS NOT NULL
              AND s.time_out IS NOT NULL
        """)
        
        edge_cases = cursor.fetchone()
        
        # Calculate percentage of billable content
        cursor.execute("""
            SELECT COUNT(*) as total_billable_com
            FROM spots s
            WHERE s.spot_type = 'COM' AND s.gross_rate > 0
        """)
        
        total_billable = cursor.fetchone()[0] or 0
        
        # Break down by revenue tier
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
              AND s.spot_type = 'COM'
              AND s.gross_rate > 0
              AND s.market_id IS NOT NULL
              AND s.time_in IS NOT NULL
              AND s.time_out IS NOT NULL
            GROUP BY revenue_tier
            ORDER BY revenue DESC
        """)
        
        by_revenue = [{'tier': row[0], 'count': row[1], 'revenue': row[2] or 0} for row in cursor.fetchall()]
        
        # Break down by sector
        cursor.execute("""
            SELECT 
                COALESCE(sec.sector_code, 'NO_SECTOR') as sector_code,
                COALESCE(sec.sector_name, 'No Sector Assigned') as sector_name,
                COUNT(*) as count,
                SUM(s.gross_rate) as revenue,
                AVG(s.gross_rate) as avg_revenue
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            LEFT JOIN customers c ON s.customer_id = c.customer_id
            LEFT JOIN sectors sec ON c.sector_id = sec.sector_id
            WHERE slb.spot_id IS NULL
              AND s.spot_type = 'COM'
              AND s.gross_rate > 0
              AND s.market_id IS NOT NULL
              AND s.time_in IS NOT NULL
              AND s.time_out IS NOT NULL
            GROUP BY sec.sector_code, sec.sector_name
            ORDER BY revenue DESC
        """)
        
        by_sector = [
            {
                'sector_code': row[0],
                'sector_name': row[1],
                'count': row[2],
                'revenue': row[3] or 0,
                'avg_revenue': row[4] or 0
            }
            for row in cursor.fetchall()
        ]
        
        return {
            'genuine_edge_cases': edge_cases[0] or 0,
            'total_revenue': edge_cases[1] or 0,
            'avg_revenue': edge_cases[2] or 0,
            'markets_affected': edge_cases[3] or 0,
            'customers_affected': edge_cases[4] or 0,
            'total_billable_com': total_billable,
            'edge_case_percentage': round((edge_cases[0] or 0) * 100.0 / (total_billable or 1), 2),
            'by_revenue_tier': by_revenue,
            'by_sector': by_sector
        }
    
    def get_exclusion_breakdown(self) -> Dict[str, Any]:
        """Get detailed breakdown of why spots are excluded."""
        cursor = self.conn.cursor()
        
        exclusion_categories = []
        
        # Bonus spots
        cursor.execute("""
            SELECT 
                COUNT(*) as count,
                COALESCE(AVG(s.gross_rate), 0) as avg_revenue
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            WHERE slb.spot_id IS NULL AND s.spot_type = 'BNS'
        """)
        
        bns_count, bns_avg = cursor.fetchone()
        exclusion_categories.append({
            'category': 'Bonus Spots (BNS)',
            'count': bns_count or 0,
            'avg_revenue': bns_avg or 0,
            'status': 'Correctly Excluded',
            'reason': 'Not actual airtime'
        })
        
        # Zero revenue COM spots
        cursor.execute("""
            SELECT 
                COUNT(*) as count,
                0 as avg_revenue
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            WHERE slb.spot_id IS NULL 
              AND s.spot_type = 'COM' 
              AND (s.gross_rate = 0 OR s.gross_rate IS NULL)
        """)
        
        zero_com_count, _ = cursor.fetchone()
        exclusion_categories.append({
            'category': 'Zero Revenue COM Spots',
            'count': zero_com_count or 0,
            'avg_revenue': 0,
            'status': 'Correctly Excluded',
            'reason': 'Inventory/planning/billing entries'
        })
        
        # Missing data
        cursor.execute("""
            SELECT 
                COUNT(*) as count,
                COALESCE(AVG(s.gross_rate), 0) as avg_revenue
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            WHERE slb.spot_id IS NULL 
              AND (s.market_id IS NULL OR s.time_in IS NULL OR s.time_out IS NULL)
        """)
        
        missing_count, missing_avg = cursor.fetchone()
        exclusion_categories.append({
            'category': 'Missing Critical Data',
            'count': missing_count or 0,
            'avg_revenue': missing_avg or 0,
            'status': 'Correctly Excluded',
            'reason': 'Cannot assign without market/time data'
        })
        
        # Production/editing work
        cursor.execute("""
            SELECT 
                COUNT(*) as count,
                COALESCE(AVG(s.gross_rate), 0) as avg_revenue
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            WHERE slb.spot_id IS NULL 
              AND (s.bill_code LIKE '%EDIT%' OR s.bill_code LIKE '%PROD%' 
                   OR s.bill_code LIKE '%DEMO%' OR s.bill_code LIKE '%TEST%')
        """)
        
        prod_count, prod_avg = cursor.fetchone()
        exclusion_categories.append({
            'category': 'Production/Editing Work',
            'count': prod_count or 0,
            'avg_revenue': prod_avg or 0,
            'status': 'Correctly Excluded',
            'reason': 'Internal work, not airtime'
        })
        
        return {
            'exclusion_categories': exclusion_categories,
            'total_excluded': sum(cat['count'] for cat in exclusion_categories)
        }
    
    def get_edge_case_details(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get detailed list of genuine edge cases."""
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
                COALESCE(sec.sector_code, 'NO_SECTOR') as sector_code,
                COALESCE(sec.sector_name, 'No Sector Assigned') as sector_name,
                spot_lang.language_name as spot_language,
                CAST((strftime('%s', s.time_out) - strftime('%s', s.time_in)) / 60 AS INTEGER) as duration_minutes,
                CASE 
                    WHEN sec.sector_code = 'MEDIA' THEN 'Should match MEDIA rule'
                    WHEN sec.sector_code = 'GOV' THEN 'Should match GOV rule'
                    WHEN sec.sector_code = 'POLITICAL' THEN 'Should match POLITICAL rule'
                    WHEN sec.sector_code = 'NPO' AND CAST((strftime('%s', s.time_out) - strftime('%s', s.time_in)) / 60 AS INTEGER) >= 300 
                         THEN 'Should match NPO 5+ hour rule'
                    WHEN CAST((strftime('%s', s.time_out) - strftime('%s', s.time_in)) / 60 AS INTEGER) >= 720 
                         THEN 'Should match 12+ hour rule'
                    ELSE 'No applicable business rule'
                END as rule_analysis
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            LEFT JOIN markets m ON s.market_id = m.market_id
            LEFT JOIN customers c ON s.customer_id = c.customer_id
            LEFT JOIN sectors sec ON c.sector_id = sec.sector_id
            LEFT JOIN languages spot_lang ON s.language_id = spot_lang.language_id
            WHERE slb.spot_id IS NULL
              AND s.spot_type = 'COM'
              AND s.gross_rate > 0
              AND s.market_id IS NOT NULL
              AND s.time_in IS NOT NULL
              AND s.time_out IS NOT NULL
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
                'gross_rate': row[5],
                'market_code': row[6],
                'market_name': row[7],
                'customer_name': row[8],
                'sector_code': row[9],
                'sector_name': row[10],
                'spot_language': row[11],
                'duration_minutes': row[12] or 0,
                'rule_analysis': row[13]
            }
            for row in cursor.fetchall()
        ]
    
    def identify_business_rule_gaps(self) -> List[Dict[str, Any]]:
        """Identify potential gaps in business rules."""
        cursor = self.conn.cursor()
        
        # Look for spots that should be caught by existing rules but aren't
        cursor.execute("""
            SELECT 
                'MEDIA Rule Gap' as gap_type,
                COUNT(*) as spot_count,
                SUM(s.gross_rate) as revenue
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            LEFT JOIN customers c ON s.customer_id = c.customer_id
            LEFT JOIN sectors sec ON c.sector_id = sec.sector_id
            WHERE slb.spot_id IS NULL
              AND s.spot_type = 'COM'
              AND s.gross_rate > 0
              AND s.market_id IS NOT NULL
              AND s.time_in IS NOT NULL
              AND s.time_out IS NOT NULL
              AND sec.sector_code = 'MEDIA'
            
            UNION ALL
            
            SELECT 
                'GOV Rule Gap' as gap_type,
                COUNT(*) as spot_count,
                SUM(s.gross_rate) as revenue
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            LEFT JOIN customers c ON s.customer_id = c.customer_id
            LEFT JOIN sectors sec ON c.sector_id = sec.sector_id
            WHERE slb.spot_id IS NULL
              AND s.spot_type = 'COM'
              AND s.gross_rate > 0
              AND s.market_id IS NOT NULL
              AND s.time_in IS NOT NULL
              AND s.time_out IS NOT NULL
              AND sec.sector_code = 'GOV'
            
            UNION ALL
            
            SELECT 
                'POLITICAL Rule Gap' as gap_type,
                COUNT(*) as spot_count,
                SUM(s.gross_rate) as revenue
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            LEFT JOIN customers c ON s.customer_id = c.customer_id
            LEFT JOIN sectors sec ON c.sector_id = sec.sector_id
            WHERE slb.spot_id IS NULL
              AND s.spot_type = 'COM'
              AND s.gross_rate > 0
              AND s.market_id IS NOT NULL
              AND s.time_in IS NOT NULL
              AND s.time_out IS NOT NULL
              AND sec.sector_code = 'POLITICAL'
        """)
        
        gaps = []
        for row in cursor.fetchall():
            if row[1] > 0:  # Only include if there are actual gaps
                gaps.append({
                    'gap_type': row[0],
                    'spot_count': row[1],
                    'revenue': row[2] or 0,
                    'severity': 'High' if row[1] > 10 else 'Medium' if row[1] > 5 else 'Low'
                })
        
        return gaps
    
    def print_system_health(self):
        """Print system health assessment."""
        health = self.get_system_health()
        
        print("🏥 SYSTEM HEALTH ASSESSMENT")
        print("=" * 50)
        print(f"Total spots: {health['total_spots']:,}")
        print(f"Overall assignment rate: {health['overall_assignment_rate']:.1f}%")
        print(f"Business rule automation: {health['business_rule_rate']:.1f}%")
        print()
        
        # Health indicators
        print("📊 HEALTH INDICATORS:")
        
        # BNS exclusion rate
        bns_status = "✅ Excellent" if health['bns_exclusion_rate'] >= 99 else "⚠️ Needs Review"
        print(f"  • Bonus Spot Exclusion: {health['bns_exclusion_rate']:.1f}% {bns_status}")
        
        # COM assignment rate
        com_status = "✅ Excellent" if health['com_assignment_rate'] >= 95 else "⚠️ Needs Review"
        print(f"  • COM Revenue Assignment: {health['com_assignment_rate']:.1f}% {com_status}")
        
        # Zero revenue exclusion
        zero_status = "✅ Excellent" if health['zero_rev_exclusion_rate'] >= 95 else "⚠️ Needs Review"
        print(f"  • Zero Revenue Exclusion: {health['zero_rev_exclusion_rate']:.1f}% {zero_status}")
        
        print()
        print("🎯 SUMMARY:")
        if all([health['bns_exclusion_rate'] >= 99, health['com_assignment_rate'] >= 95, health['zero_rev_exclusion_rate'] >= 95]):
            print("  ✅ System is performing excellently!")
        else:
            print("  ⚠️ System needs attention in some areas.")
    
    def print_edge_case_analysis(self):
        """Print analysis of genuine edge cases."""
        edge_cases = self.get_genuine_edge_cases()
        
        print("🔍 GENUINE EDGE CASE ANALYSIS")
        print("=" * 50)
        print(f"Genuine edge cases: {edge_cases['genuine_edge_cases']:,}")
        print(f"Total billable COM spots: {edge_cases['total_billable_com']:,}")
        print(f"Edge case percentage: {edge_cases['edge_case_percentage']:.1f}%")
        print(f"Total revenue: ${edge_cases['total_revenue']:,.2f}")
        print()
        
        if edge_cases['genuine_edge_cases'] == 0:
            print("🎉 No genuine edge cases found!")
            print("   All billable commercial spots are correctly assigned.")
            return
        
        if edge_cases['edge_case_percentage'] <= 5:
            print("✅ Edge case percentage is within acceptable range (<5%)")
        else:
            print("⚠️ Edge case percentage is higher than expected (>5%)")
        
        print(f"\n📊 BY REVENUE TIER:")
        for tier in edge_cases['by_revenue_tier']:
            print(f"  • {tier['tier']}: {tier['count']} spots (${tier['revenue']:,.2f})")
        
        print(f"\n🏢 BY SECTOR:")
        for sector in edge_cases['by_sector']:
            print(f"  • {sector['sector_name']}: {sector['count']} spots (${sector['revenue']:,.2f})")
    
    def print_exclusion_summary(self):
        """Print summary of exclusion categories."""
        exclusions = self.get_exclusion_breakdown()
        
        print("📋 EXCLUSION BREAKDOWN")
        print("=" * 50)
        print(f"Total excluded spots: {exclusions['total_excluded']:,}")
        print()
        
        for category in exclusions['exclusion_categories']:
            print(f"• {category['category']}: {category['count']:,} spots")
            print(f"  - Status: {category['status']}")
            print(f"  - Reason: {category['reason']}")
            print(f"  - Avg Revenue: ${category['avg_revenue']:.2f}")
            print()


def main():
    """Main CLI function."""
    parser = argparse.ArgumentParser(description="Smart Edge Case Manager")
    parser.add_argument("--db", default="./data/database/production.db", help="Database path")
    parser.add_argument("--health", action="store_true", help="Show system health")
    parser.add_argument("--edge-cases", action="store_true", help="Analyze genuine edge cases")
    parser.add_argument("--exclusions", action="store_true", help="Show exclusion breakdown")
    parser.add_argument("--details", type=int, help="Show N detailed edge cases")
    parser.add_argument("--gaps", action="store_true", help="Identify business rule gaps")
    
    args = parser.parse_args()
    
    manager = SmartEdgeCaseManager(args.db)
    manager.connect()
    
    try:
        if args.health:
            manager.print_system_health()
        elif args.edge_cases:
            manager.print_edge_case_analysis()
        elif args.exclusions:
            manager.print_exclusion_summary()
        elif args.details:
            details = manager.get_edge_case_details(args.details)
            print(f"\n🔍 TOP {args.details} GENUINE EDGE CASES:")
            print("-" * 100)
            if details:
                for spot in details:
                    print(f"Spot {spot['spot_id']}: ${spot['gross_rate']:,.2f} - {spot['customer_name'][:30]}...")
                    print(f"  {spot['market_name']} | {spot['sector_name']} | {spot['duration_minutes']}min")
                    print(f"  Analysis: {spot['rule_analysis']}")
                    print()
            else:
                print("🎉 No genuine edge cases found!")
        elif args.gaps:
            gaps = manager.identify_business_rule_gaps()
            print(f"\n🔍 BUSINESS RULE GAPS:")
            print("-" * 50)
            if gaps:
                for gap in gaps:
                    print(f"• {gap['gap_type']}: {gap['spot_count']} spots, ${gap['revenue']:,.2f}")
                    print(f"  Severity: {gap['severity']}")
                    print()
            else:
                print("✅ No business rule gaps found!")
        else:
            # Default: show overview
            manager.print_system_health()
            print()
            manager.print_edge_case_analysis()
            print()
            manager.print_exclusion_summary()
    
    finally:
        manager.close()


if __name__ == "__main__":
    main()