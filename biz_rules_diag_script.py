#!/usr/bin/env python3
"""
Business Rules Diagnostics Script
==================================

Run comprehensive diagnostics on business rules and edge cases.
Usage: python3 business_rules_diagnostics.py
"""

import sqlite3
import os
import sys
from datetime import datetime
import json

def get_database_connection():
    """Get database connection"""
    db_path = "./data/database/production.db"
    
    if not os.path.exists(db_path):
        print(f"❌ Database not found at {db_path}")
        print("Please run this script from the project root directory")
        sys.exit(1)
    
    return sqlite3.connect(db_path)

def run_query(conn, query_name, query, description=""):
    """Run a query and format results"""
    print(f"\n{'='*60}")
    print(f"📊 {query_name}")
    if description:
        print(f"   {description}")
    print('='*60)
    
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        
        if not results:
            print("   No results found")
            return
        
        # Get column names
        columns = [desc[0] for desc in cursor.description]
        
        # Print header
        header = " | ".join(f"{col:>15}" for col in columns)
        print(f"   {header}")
        print(f"   {'-'*len(header)}")
        
        # Print results
        for row in results:
            formatted_row = " | ".join(f"{str(val):>15}" for val in row)
            print(f"   {formatted_row}")
        
        print(f"\n   📈 Total rows: {len(results)}")
        
    except Exception as e:
        print(f"   ❌ Error running query: {e}")

def main():
    """Run all diagnostic queries"""
    
    print("🔍 Business Rules Diagnostics")
    print("=" * 50)
    print(f"Started at: {datetime.now()}")
    
    conn = get_database_connection()
    
    # Query 1: Worldlink spots analysis
    query1 = """
    SELECT 
        s.bill_code,
        s.spot_type,
        s.program,
        s.media,
        s.gross_rate,
        s.revenue_type,
        CASE 
            WHEN s.bill_code LIKE '%EDIT%' THEN 'Contains EDIT'
            WHEN s.bill_code LIKE '%PROD%' THEN 'Contains PROD'
            WHEN s.bill_code LIKE '%DEMO%' THEN 'Contains DEMO'
            WHEN s.bill_code LIKE '%TEST%' THEN 'Contains TEST'
            ELSE 'No exclusion keywords'
        END as exclusion_check,
        COUNT(*) as spot_count
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    WHERE slb.spot_id IS NULL
    AND s.bill_code LIKE 'Worldlink%'
    GROUP BY s.bill_code, s.spot_type, s.program, s.media, s.gross_rate, s.revenue_type
    ORDER BY spot_count DESC
    LIMIT 20;
    """
    
    run_query(conn, "Worldlink Spots Analysis", query1, 
              "Analyze unassigned Worldlink spots to understand exclusion patterns")
    
    # Query 2: Worldlink sectors
    query2 = """
    SELECT 
        c.normalized_name,
        COALESCE(sec.sector_code, 'NO_SECTOR') as sector_code,
        COALESCE(sec.sector_name, 'No Sector Assigned') as sector_name,
        COUNT(*) as unassigned_spots,
        SUM(COALESCE(s.gross_rate, 0)) as total_revenue,
        MIN(s.air_date) as earliest_date,
        MAX(s.air_date) as latest_date
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN customers c ON s.customer_id = c.customer_id
    LEFT JOIN sectors sec ON c.sector_id = sec.sector_id
    WHERE slb.spot_id IS NULL
    AND s.bill_code LIKE 'Worldlink%'
    GROUP BY c.normalized_name, sec.sector_code, sec.sector_name
    ORDER BY unassigned_spots DESC;
    """
    
    run_query(conn, "Worldlink Spots by Sector", query2,
              "Check what sectors these unassigned Worldlink spots belong to")
    
    # Query 3: Exclusion patterns
    query3 = """
    SELECT 
        'Zero Revenue' as exclusion_reason,
        COUNT(*) as spots_excluded
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    WHERE slb.spot_id IS NULL
    AND (s.gross_rate = 0 OR s.gross_rate IS NULL)

    UNION ALL

    SELECT 
        'Contains EDIT keyword' as exclusion_reason,
        COUNT(*) as spots_excluded
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    WHERE slb.spot_id IS NULL
    AND s.bill_code LIKE '%EDIT%'

    UNION ALL

    SELECT 
        'Contains PROD keyword' as exclusion_reason,
        COUNT(*) as spots_excluded
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    WHERE slb.spot_id IS NULL
    AND s.bill_code LIKE '%PROD%'

    UNION ALL

    SELECT 
        'Missing critical data' as exclusion_reason,
        COUNT(*) as spots_excluded
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    WHERE slb.spot_id IS NULL
    AND (s.market_id IS NULL OR s.time_in IS NULL OR s.time_out IS NULL);
    """
    
    run_query(conn, "Exclusion Patterns Summary", query3,
              "See how many spots are excluded by each rule")
    
    # Query 4: Duration analysis
    query4 = """
    SELECT 
        CASE 
            WHEN CAST((strftime('%s', s.time_out) - strftime('%s', s.time_in)) / 60 AS INTEGER) >= 720 THEN '12+ hours'
            WHEN CAST((strftime('%s', s.time_out) - strftime('%s', s.time_in)) / 60 AS INTEGER) >= 300 THEN '5-12 hours'
            WHEN CAST((strftime('%s', s.time_out) - strftime('%s', s.time_in)) / 60 AS INTEGER) >= 60 THEN '1-5 hours'
            WHEN CAST((strftime('%s', s.time_out) - strftime('%s', s.time_in)) / 60 AS INTEGER) >= 30 THEN '30min-1hour'
            WHEN CAST((strftime('%s', s.time_out) - strftime('%s', s.time_in)) / 60 AS INTEGER) >= 0 THEN 'Under 30min'
            ELSE 'Invalid duration'
        END as duration_range,
        COUNT(*) as unassigned_spots,
        ROUND(AVG(CAST((strftime('%s', s.time_out) - strftime('%s', s.time_in)) / 60 AS INTEGER)), 2) as avg_duration_minutes
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    WHERE slb.spot_id IS NULL
    GROUP BY duration_range
    ORDER BY avg_duration_minutes DESC;
    """
    
    run_query(conn, "Duration Analysis of Unassigned Spots", query4,
              "Analyze duration patterns for unassigned spots")
    
    # Query 5: Recent Worldlink details
    query5 = """
    SELECT 
        s.spot_id,
        s.bill_code,
        c.normalized_name,
        COALESCE(sec.sector_code, 'NO_SECTOR') as sector_code,
        s.air_date,
        s.time_in,
        s.time_out,
        CAST((strftime('%s', s.time_out) - strftime('%s', s.time_in)) / 60 AS INTEGER) as duration_minutes,
        s.gross_rate,
        s.revenue_type,
        s.market_id,
        s.spot_type
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN customers c ON s.customer_id = c.customer_id
    LEFT JOIN sectors sec ON c.sector_id = sec.sector_id
    WHERE slb.spot_id IS NULL
    AND s.bill_code LIKE 'Worldlink%'
    AND s.air_date >= '2025-01-01'
    ORDER BY s.air_date DESC, s.spot_id DESC
    LIMIT 10;
    """
    
    run_query(conn, "Recent Worldlink Spot Details", query5,
              "Recent Worldlink spots with full details")
    
    # Query 6: Business rule effectiveness by sector
    query6 = """
    SELECT 
        COALESCE(sec.sector_code, 'NO_SECTOR') as sector_code,
        COALESCE(sec.sector_name, 'No Sector Assigned') as sector_name,
        COUNT(DISTINCT s.spot_id) as total_spots,
        COUNT(DISTINCT CASE WHEN slb.spot_id IS NOT NULL THEN s.spot_id END) as assigned_spots,
        COUNT(DISTINCT CASE WHEN slb.business_rule_applied IS NOT NULL THEN s.spot_id END) as business_rule_spots,
        ROUND(COUNT(DISTINCT CASE WHEN slb.spot_id IS NOT NULL THEN s.spot_id END) * 100.0 / COUNT(DISTINCT s.spot_id), 2) as assignment_rate,
        ROUND(COUNT(DISTINCT CASE WHEN slb.business_rule_applied IS NOT NULL THEN s.spot_id END) * 100.0 / COUNT(DISTINCT s.spot_id), 2) as business_rule_rate
    FROM spots s
    LEFT JOIN customers c ON s.customer_id = c.customer_id
    LEFT JOIN sectors sec ON c.sector_id = sec.sector_id
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    GROUP BY sec.sector_code, sec.sector_name
    ORDER BY total_spots DESC;
    """
    
    run_query(conn, "Business Rule Effectiveness by Sector", query6,
              "How effective are business rules for each sector")
    
    # Query 7: Business rule gaps
    query7 = """
    WITH rule_gaps AS (
        SELECT 
            s.spot_id,
            s.bill_code,
            COALESCE(sec.sector_code, 'NO_SECTOR') as sector_code,
            CAST((strftime('%s', s.time_out) - strftime('%s', s.time_in)) / 60 AS INTEGER) as duration_minutes,
            s.gross_rate,
            CASE 
                WHEN sec.sector_code = 'MEDIA' THEN 'Should be DIRECT_RESPONSE_SALES'
                WHEN sec.sector_code = 'GOV' THEN 'Should be GOVERNMENT_PUBLIC_SERVICE'
                WHEN sec.sector_code = 'POLITICAL' THEN 'Should be POLITICAL_CAMPAIGNS'
                WHEN sec.sector_code = 'NPO' AND CAST((strftime('%s', s.time_out) - strftime('%s', s.time_in)) / 60 AS INTEGER) >= 300 THEN 'Should be NONPROFIT_AWARENESS'
                WHEN CAST((strftime('%s', s.time_out) - strftime('%s', s.time_in)) / 60 AS INTEGER) >= 720 THEN 'Should be EXTENDED_CONTENT_BLOCKS'
                ELSE 'No applicable rule'
            END as expected_rule
        FROM spots s
        LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        LEFT JOIN customers c ON s.customer_id = c.customer_id
        LEFT JOIN sectors sec ON c.sector_id = sec.sector_id
        WHERE slb.spot_id IS NULL
        AND s.gross_rate > 0
    )
    SELECT 
        expected_rule,
        COUNT(*) as spots_affected,
        ROUND(AVG(gross_rate), 2) as avg_revenue,
        ROUND(SUM(gross_rate), 2) as total_revenue
    FROM rule_gaps
    WHERE expected_rule != 'No applicable rule'
    GROUP BY expected_rule
    ORDER BY spots_affected DESC;
    """
    
    run_query(conn, "Business Rule Gaps Analysis", query7,
              "Spots that should be caught by business rules but aren't")
    
    # Query 8: Current business rule performance
    query8 = """
    SELECT 
        COALESCE(business_rule_applied, 'Manual Assignment') as rule_applied,
        COUNT(*) as spots_affected,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM spot_language_blocks), 2) as percentage,
        ROUND(AVG(intent_confidence), 3) as avg_confidence,
        COUNT(CASE WHEN requires_attention = 1 THEN 1 END) as flagged_count,
        COUNT(CASE WHEN requires_attention = 0 THEN 1 END) as auto_resolved_count
    FROM spot_language_blocks
    GROUP BY business_rule_applied
    ORDER BY spots_affected DESC;
    """
    
    run_query(conn, "Current Business Rule Performance", query8,
              "How current business rules are performing")
    
    # Query 9: Overall assignment status
    query9 = """
    SELECT 
        'Total Spots in Database' as metric,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM spots), 2) as percentage
    FROM spots
    
    UNION ALL
    
    SELECT 
        'Assigned to Language Blocks' as metric,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM spots), 2) as percentage
    FROM spots s
    JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    
    UNION ALL
    
    SELECT 
        'Unassigned (Need Review)' as metric,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM spots), 2) as percentage
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    WHERE slb.spot_id IS NULL
    
    UNION ALL
    
    SELECT 
        'Auto-assigned by Business Rules' as metric,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM spots), 2) as percentage
    FROM spots s
    JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    WHERE slb.business_rule_applied IS NOT NULL;
    """
    
    run_query(conn, "Overall Assignment Status", query9,
              "High-level view of assignment coverage")
    
    conn.close()
    
    print(f"\n✅ Diagnostics completed at: {datetime.now()}")
    print("\n📋 Summary:")
    print("   - Check the 'Worldlink Spots Analysis' to see if they contain EDIT keywords")
    print("   - Review 'Exclusion Patterns' to understand why spots aren't assigned")
    print("   - Look at 'Business Rule Gaps' to find potential improvements")
    print("   - Monitor 'Overall Assignment Status' for system effectiveness")

if __name__ == "__main__":
    main()