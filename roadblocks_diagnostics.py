#!/usr/bin/env python3
"""
Roadblocks Diagnostics Script
=============================

This script helps diagnose why the roadblocks category is showing $0.00
by running various queries to understand the data structure.

Usage:
    python roadblocks_diagnostics.py --year 2024
"""

import sqlite3
import argparse
from typing import Dict, List, Any

class RoadblocksDiagnostics:
    """Diagnostic tool for roadblocks category issues"""
    
    def __init__(self, db_path: str = "data/database/production.db"):
        self.db_path = db_path
    
    def run_diagnostics(self, year: str = "2024") -> Dict[str, Any]:
        """Run all diagnostic queries"""
        year_suffix = year[-2:]
        
        with sqlite3.connect(self.db_path) as db:
            cursor = db.cursor()
            
            results = {}
            
            # 1. Check spots with no language assignment
            print("🔍 1. Checking spots with no language assignment...")
            cursor.execute("""
                SELECT 
                    COUNT(*) as spots_with_no_assignment,
                    SUM(COALESCE(s.gross_rate, 0)) as total_revenue
                FROM spots s
                LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
                WHERE s.broadcast_month LIKE ?
                AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
                AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
                AND slb.spot_id IS NULL
            """, [f"%-{year_suffix}"])
            
            row = cursor.fetchone()
            results['no_assignment'] = {
                'spots': row[0] or 0,
                'revenue': row[1] or 0
            }
            print(f"   Spots with no language assignment: {results['no_assignment']['spots']:,} (${results['no_assignment']['revenue']:,.2f})")
            
            # 2. Check campaign_type field
            print("\n🔍 2. Checking campaign_type distribution...")
            cursor.execute("""
                SELECT 
                    COALESCE(slb.campaign_type, 'NULL') as campaign_type,
                    COUNT(*) as spots,
                    SUM(COALESCE(s.gross_rate, 0)) as revenue
                FROM spots s
                LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
                WHERE s.broadcast_month LIKE ?
                AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
                AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
                GROUP BY slb.campaign_type
                ORDER BY revenue DESC
            """, [f"%-{year_suffix}"])
            
            campaign_types = []
            for row in cursor.fetchall():
                campaign_types.append({
                    'campaign_type': row[0],
                    'spots': row[1],
                    'revenue': row[2]
                })
                print(f"   {row[0]}: {row[1]:,} spots (${row[2]:,.2f})")
            
            results['campaign_types'] = campaign_types
            
            # 3. Check long-form content
            print("\n🔍 3. Checking long-form content (> 30 minutes)...")
            cursor.execute("""
                SELECT 
                    COUNT(*) as long_form_spots,
                    SUM(COALESCE(s.gross_rate, 0)) as total_revenue
                FROM spots s
                LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
                WHERE s.broadcast_month LIKE ?
                AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
                AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
                AND s.length_seconds IS NOT NULL 
                AND CAST(s.length_seconds AS INTEGER) > 1800
            """, [f"%-{year_suffix}"])
            
            row = cursor.fetchone()
            results['long_form'] = {
                'spots': row[0] or 0,
                'revenue': row[1] or 0
            }
            print(f"   Long-form content: {results['long_form']['spots']:,} spots (${results['long_form']['revenue']:,.2f})")
            
            # 4. Check sponsorship indicators
            print("\n🔍 4. Checking sponsorship indicators...")
            cursor.execute("""
                SELECT 
                    COUNT(*) as sponsorship_spots,
                    SUM(COALESCE(s.gross_rate, 0)) as total_revenue
                FROM spots s
                LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
                WHERE s.broadcast_month LIKE ?
                AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
                AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
                AND (s.program LIKE '%sponsor%' OR s.program LIKE '%Sponsor%')
            """, [f"%-{year_suffix}"])
            
            row = cursor.fetchone()
            results['sponsorship'] = {
                'spots': row[0] or 0,
                'revenue': row[1] or 0
            }
            print(f"   Sponsorship indicators: {results['sponsorship']['spots']:,} spots (${results['sponsorship']['revenue']:,.2f})")
            
            # 5. Check late night/early morning
            print("\n🔍 5. Checking late night/early morning spots...")
            cursor.execute("""
                SELECT 
                    COUNT(*) as late_night_spots,
                    SUM(COALESCE(s.gross_rate, 0)) as total_revenue
                FROM spots s
                LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
                WHERE s.broadcast_month LIKE ?
                AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
                AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
                AND (s.time_in >= '23:00:00' OR s.time_in <= '06:00:00')
            """, [f"%-{year_suffix}"])
            
            row = cursor.fetchone()
            results['late_night'] = {
                'spots': row[0] or 0,
                'revenue': row[1] or 0
            }
            print(f"   Late night/early morning: {results['late_night']['spots']:,} spots (${results['late_night']['revenue']:,.2f})")
            
            # 6. Check non-language_specific campaign types
            print("\n🔍 6. Checking non-language_specific campaign types...")
            cursor.execute("""
                SELECT 
                    slb.campaign_type,
                    slb.customer_intent,
                    COUNT(*) as spots,
                    SUM(COALESCE(s.gross_rate, 0)) as revenue
                FROM spots s
                LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
                WHERE s.broadcast_month LIKE ?
                AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
                AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
                AND slb.campaign_type IS NOT NULL
                AND slb.campaign_type != 'language_specific'
                GROUP BY slb.campaign_type, slb.customer_intent
                ORDER BY revenue DESC
            """, [f"%-{year_suffix}"])
            
            non_language_campaigns = []
            for row in cursor.fetchall():
                non_language_campaigns.append({
                    'campaign_type': row[0],
                    'customer_intent': row[1],
                    'spots': row[2],
                    'revenue': row[3]
                })
                print(f"   {row[0]} ({row[1]}): {row[2]:,} spots (${row[3]:,.2f})")
            
            results['non_language_campaigns'] = non_language_campaigns
            
            # 7. Sample of unassigned spots
            print("\n🔍 7. Sample of highest-revenue unassigned spots...")
            cursor.execute("""
                SELECT 
                    s.spot_id,
                    s.bill_code,
                    s.air_date,
                    s.time_in,
                    s.time_out,
                    s.length_seconds,
                    s.program,
                    s.spot_type,
                    s.gross_rate,
                    COALESCE(c.normalized_name, 'Unknown') as customer_name,
                    COALESCE(a.agency_name, 'Unknown') as agency_name
                FROM spots s
                LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
                LEFT JOIN agencies a ON s.agency_id = a.agency_id
                LEFT JOIN customers c ON s.customer_id = c.customer_id
                WHERE s.broadcast_month LIKE ?
                AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
                AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
                AND slb.spot_id IS NULL
                AND s.gross_rate > 0
                ORDER BY s.gross_rate DESC
                LIMIT 10
            """, [f"%-{year_suffix}"])
            
            unassigned_samples = []
            for row in cursor.fetchall():
                sample = {
                    'spot_id': row[0],
                    'bill_code': row[1],
                    'air_date': row[2],
                    'time_in': row[3],
                    'time_out': row[4],
                    'length_seconds': row[5],
                    'program': row[6],
                    'spot_type': row[7],
                    'gross_rate': row[8],
                    'customer_name': row[9],
                    'agency_name': row[10]
                }
                unassigned_samples.append(sample)
                print(f"   {row[1]} | ${row[8]:,.2f} | {row[6]} | {row[9]}")
            
            results['unassigned_samples'] = unassigned_samples
            
            # 8. Check if there are any spots that might be "roadblocks" by campaign_type
            print("\n🔍 8. Looking for potential roadblocks patterns...")
            
            # Check if there are any spots that might be roadblocks
            potential_roadblocks = []
            for campaign in non_language_campaigns:
                if campaign['campaign_type'] in ['roadblock', 'sponsorship', 'broadcast_sponsorship']:
                    potential_roadblocks.append(campaign)
            
            if potential_roadblocks:
                print("   Found potential roadblocks by campaign_type:")
                for rb in potential_roadblocks:
                    print(f"     {rb['campaign_type']}: {rb['spots']:,} spots (${rb['revenue']:,.2f})")
            else:
                print("   No obvious roadblocks found by campaign_type")
            
            results['potential_roadblocks'] = potential_roadblocks
            
            return results
    
    def generate_recommendations(self, results: Dict[str, Any]) -> List[str]:
        """Generate recommendations based on diagnostic results"""
        recommendations = []
        
        # Check if there are unassigned spots that should be roadblocks
        if results['no_assignment']['spots'] > 0:
            recommendations.append(f"📍 Found {results['no_assignment']['spots']:,} spots with no language assignment (${results['no_assignment']['revenue']:,.2f})")
            recommendations.append("   These might be roadblocks that aren't being captured by current criteria")
        
        # Check campaign types
        non_language_types = [ct for ct in results['campaign_types'] if ct['campaign_type'] not in ['language_specific', 'NULL']]
        if non_language_types:
            recommendations.append("📍 Found non-language_specific campaign types:")
            for ct in non_language_types:
                recommendations.append(f"   {ct['campaign_type']}: {ct['spots']:,} spots (${ct['revenue']:,.2f})")
            recommendations.append("   Consider using campaign_type to identify roadblocks instead of current criteria")
        
        # Check if current roadblocks criteria are too restrictive
        total_potential = (results['long_form']['spots'] + results['sponsorship']['spots'] + 
                          results['late_night']['spots'])
        if total_potential == 0:
            recommendations.append("📍 Current roadblocks criteria (long-form, sponsorship, late night) found 0 spots")
            recommendations.append("   The criteria might be too restrictive or roadblocks are identified differently")
        
        # Check for potential roadblocks by campaign_type
        if results['potential_roadblocks']:
            recommendations.append("📍 Found potential roadblocks by campaign_type - consider updating the roadblocks query")
        
        return recommendations


def main():
    parser = argparse.ArgumentParser(description="Diagnose roadblocks category issues")
    parser.add_argument("--year", default="2024", help="Year to analyze")
    parser.add_argument("--db-path", default="data/database/production.db", help="Database path")
    
    args = parser.parse_args()
    
    print(f"🚧 Roadblocks Diagnostics for {args.year}")
    print("=" * 60)
    
    diagnostics = RoadblocksDiagnostics(args.db_path)
    results = diagnostics.run_diagnostics(args.year)
    
    print("\n" + "=" * 60)
    print("📋 RECOMMENDATIONS")
    print("=" * 60)
    
    recommendations = diagnostics.generate_recommendations(results)
    for rec in recommendations:
        print(rec)
    
    print("\n" + "=" * 60)
    print("🔧 NEXT STEPS")
    print("=" * 60)
    print("1. Review the campaign_type field - it might be the key to identifying roadblocks")
    print("2. Check if roadblocks are identified by campaign_type != 'language_specific'")
    print("3. Consider updating the roadblocks query to use campaign_type instead of current criteria")
    print("4. Review the unassigned spots samples to understand what should be roadblocks")


if __name__ == "__main__":
    main()