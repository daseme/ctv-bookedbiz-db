"""
Chinese Prime Time Revenue Migration
===================================

This implements the Chinese Prime Time revenue category using BaseQueryBuilder.
Expected result: ~$699,550.49 according to your guide.

This category includes:
- Multi-language spots (spans_multiple_blocks = 1 OR block_id IS NULL)
- During Chinese Prime Time hours:
  * Weekdays (M-F): 7pm-11:59pm
  * Weekends (Sat-Sun): 8pm-11:59pm
- Excludes WorldLink
- Represents cross-audience targeting during Chinese premium viewing hours
"""

import sqlite3
from query_builders import ChinesePrimeTimeQueryBuilder, BaseQueryBuilder, validate_query_migration

def get_chinese_prime_time_revenue(year="2024", db_connection=None):
    """
    Get Chinese Prime Time revenue
    
    Returns:
        QueryResult: Revenue, spot count, and execution details
    """
    builder = ChinesePrimeTimeQueryBuilder(year)
    builder.add_chinese_prime_time_conditions().add_multi_language_conditions()
    
    return builder.execute_revenue_query(db_connection)

def validate_chinese_prime_time_migration(db_connection, year="2024"):
    """
    Validate that our new Chinese Prime Time query matches the original
    
    This uses the exact query from your Revenue-Querying-By-Language-Guide.md
    """
    
    # Original query from your guide
    old_query = f"""
    SELECT SUM(COALESCE(s.gross_rate, 0)) as revenue
    FROM spots s
    JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    WHERE s.broadcast_month LIKE '%-{year[-2:]}'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND (
        -- Chinese Prime Time M-F 7pm-11:59pm
        (s.time_in >= '19:00:00' AND s.time_out <= '23:59:59' 
         AND s.day_of_week IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'))
        OR
        -- Chinese Weekend 8pm-11:59pm  
        (s.time_in >= '20:00:00' AND s.time_out <= '23:59:59'
         AND s.day_of_week IN ('Saturday', 'Sunday'))
    )
    AND (slb.spans_multiple_blocks = 1 OR 
         (slb.spans_multiple_blocks = 0 AND slb.block_id IS NULL) OR 
         (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NULL))
    AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
    AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
    """
    
    # New query using builder
    builder = ChinesePrimeTimeQueryBuilder(year)
    builder.add_chinese_prime_time_conditions().add_multi_language_conditions()
    
    # Validate
    return validate_query_migration(old_query, builder, db_connection)

def analyze_chinese_prime_time_patterns(year="2024", db_connection=None):
    """
    Analyze Chinese Prime Time patterns in detail
    
    Returns:
        dict: Detailed breakdown by day type, time slots, etc.
    """
    
    # Weekday vs Weekend breakdown
    weekday_query = f"""
    SELECT 
        'Weekday' as day_type,
        COUNT(*) as spots,
        SUM(COALESCE(s.gross_rate, 0)) as revenue,
        AVG(COALESCE(s.gross_rate, 0)) as avg_rate
    FROM spots s
    JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    WHERE s.broadcast_month LIKE '%-{year[-2:]}'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND (s.time_in >= '19:00:00' AND s.time_out <= '23:59:59' 
         AND s.day_of_week IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'))
    AND (slb.spans_multiple_blocks = 1 OR 
         (slb.spans_multiple_blocks = 0 AND slb.block_id IS NULL) OR 
         (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NULL))
    AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
    AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
    
    UNION ALL
    
    SELECT 
        'Weekend' as day_type,
        COUNT(*) as spots,
        SUM(COALESCE(s.gross_rate, 0)) as revenue,
        AVG(COALESCE(s.gross_rate, 0)) as avg_rate
    FROM spots s
    JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    WHERE s.broadcast_month LIKE '%-{year[-2:]}'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND (s.time_in >= '20:00:00' AND s.time_out <= '23:59:59'
         AND s.day_of_week IN ('Saturday', 'Sunday'))
    AND (slb.spans_multiple_blocks = 1 OR 
         (slb.spans_multiple_blocks = 0 AND slb.block_id IS NULL) OR 
         (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NULL))
    AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
    AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
    """
    
    cursor = db_connection.cursor()
    cursor.execute(weekday_query)
    day_breakdown = cursor.fetchall()
    
    # Language code breakdown (to see cross-audience patterns)
    language_code_query = f"""
    SELECT 
        COALESCE(s.language_code, 'Unknown') as language_code,
        COUNT(*) as spots,
        SUM(COALESCE(s.gross_rate, 0)) as revenue,
        ROUND(COUNT(*) * 100.0 / (
            SELECT COUNT(*) FROM spots s2
            JOIN spot_language_blocks slb2 ON s2.spot_id = slb2.spot_id
            LEFT JOIN agencies a2 ON s2.agency_id = a2.agency_id
            WHERE s2.broadcast_month LIKE '%-{year[-2:]}'
            AND (s2.revenue_type != 'Trade' OR s2.revenue_type IS NULL)
            AND (s2.gross_rate IS NOT NULL OR s2.station_net IS NOT NULL OR s2.spot_type = 'BNS')
            AND (
                (s2.time_in >= '19:00:00' AND s2.time_out <= '23:59:59' 
                 AND s2.day_of_week IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'))
                OR
                (s2.time_in >= '20:00:00' AND s2.time_out <= '23:59:59'
                 AND s2.day_of_week IN ('Saturday', 'Sunday'))
            )
            AND (slb2.spans_multiple_blocks = 1 OR 
                 (slb2.spans_multiple_blocks = 0 AND slb2.block_id IS NULL) OR 
                 (slb2.spans_multiple_blocks IS NULL AND slb2.block_id IS NULL))
            AND COALESCE(a2.agency_name, '') NOT LIKE '%WorldLink%'
            AND COALESCE(s2.bill_code, '') NOT LIKE '%WorldLink%'
        ), 2) as spot_percentage
    FROM spots s
    JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    WHERE s.broadcast_month LIKE '%-{year[-2:]}'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND (
        (s.time_in >= '19:00:00' AND s.time_out <= '23:59:59' 
         AND s.day_of_week IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'))
        OR
        (s.time_in >= '20:00:00' AND s.time_out <= '23:59:59'
         AND s.day_of_week IN ('Saturday', 'Sunday'))
    )
    AND (slb.spans_multiple_blocks = 1 OR 
         (slb.spans_multiple_blocks = 0 AND slb.block_id IS NULL) OR 
         (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NULL))
    AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
    AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
    GROUP BY s.language_code
    ORDER BY SUM(COALESCE(s.gross_rate, 0)) DESC
    """
    
    cursor.execute(language_code_query)
    language_breakdown = cursor.fetchall()
    
    return {
        'day_breakdown': day_breakdown,
        'language_breakdown': language_breakdown
    }

def test_chinese_prime_time_migration(year="2024"):
    """Complete test of Chinese Prime Time migration"""
    
    print(f"🧪 Testing Chinese Prime Time Migration for {year}")
    print("=" * 60)
    
    # Connect to database
    conn = sqlite3.connect('data/database/production.db')
    
    try:
        # 1. Validate migration
        print("\n1. Validating Migration:")
        if validate_chinese_prime_time_migration(conn, year):
            print("   ✅ Migration validation passed!")
        else:
            print("   ❌ Migration validation failed!")
            return False
        
        # 2. Get total revenue
        print("\n2. Chinese Prime Time Revenue:")
        result = get_chinese_prime_time_revenue(year, conn)
        
        print(f"   Total Revenue: ${result.revenue:,.2f}")
        print(f"   Total Spots: {result.spot_count:,}")
        print(f"   Average Rate: ${result.revenue/result.spot_count:,.2f}" if result.spot_count > 0 else "   Average Rate: N/A")
        print(f"   Execution Time: {result.execution_time:.3f}s")
        
        # 3. Compare with documented results
        print(f"\n3. Validation Against Your Guide:")
        documented_chinese_prime_total = 699550.49  # From your guide
        difference = abs(result.revenue - documented_chinese_prime_total)
        
        print(f"   Expected (from guide): ${documented_chinese_prime_total:,.2f}")
        print(f"   Actual (new query): ${result.revenue:,.2f}")
        print(f"   Difference: ${difference:,.2f}")
        
        if difference < 1.0:
            print(f"   ✅ PERFECT MATCH! (Difference < $1.00)")
            perfect_match = True
        else:
            print(f"   ❌ DIFFERENCE FOUND! (Difference: ${difference:,.2f})")
            perfect_match = False
            
        # 4. Analyze patterns
        print(f"\n4. Chinese Prime Time Analysis:")
        patterns = analyze_chinese_prime_time_patterns(year, conn)
        
        print(f"   Day Type Breakdown:")
        weekday_total = 0
        weekend_total = 0
        
        for day_type, spots, revenue, avg_rate in patterns['day_breakdown']:
            pct = (revenue / result.revenue) * 100
            print(f"   {day_type:<8}: ${revenue:>12,.2f} ({spots:>6,} spots, ${avg_rate:>7.2f} avg) {pct:>5.1f}%")
            
            if day_type == 'Weekday':
                weekday_total = revenue
            else:
                weekend_total = revenue
        
        print(f"\n   Language Code Breakdown (Top 5):")
        for i, (lang_code, spots, revenue, spot_pct) in enumerate(patterns['language_breakdown'][:5]):
            rev_pct = (revenue / result.revenue) * 100
            print(f"   {i+1}. {lang_code:<8}: ${revenue:>12,.2f} ({spots:>6,} spots, {spot_pct:>5.1f}% of total) {rev_pct:>5.1f}%")
        
        # 5. Show strategic insights
        print(f"\n5. Strategic Insights:")
        print(f"   • Weekday Prime Time: ${weekday_total:,.2f} ({weekday_total/result.revenue*100:.1f}%)")
        print(f"   • Weekend Prime Time: ${weekend_total:,.2f} ({weekend_total/result.revenue*100:.1f}%)")
        print(f"   • Cross-audience strategy: Multi-language spots targeting Chinese viewing hours")
        print(f"   • Premium time slot: Higher rates during Chinese cultural prime time")
        
        # 6. Show generated query
        print(f"\n6. Generated Query:")
        builder = ChinesePrimeTimeQueryBuilder(year)
        builder.add_chinese_prime_time_conditions().add_multi_language_conditions()
        print("   " + builder.build_select_revenue_query().replace('\n', '\n   '))
        
        return perfect_match
        
    finally:
        conn.close()

if __name__ == "__main__":
    # Run the complete test
    print("🚀 Chinese Prime Time Revenue Migration Test")
    print("=" * 50)
    
    # Test the migration
    success = test_chinese_prime_time_migration("2024")
    
    if success:
        print(f"\n✅ Chinese Prime Time Migration Test Complete!")
        print(f"Ready to proceed with next category migration!")
    else:
        print(f"\n❌ Migration test failed - investigate before proceeding!")