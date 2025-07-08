"""
Multi-Language (Cross-Audience) Revenue Migration
=================================================

This implements the Multi-Language (Cross-Audience) revenue category using BaseQueryBuilder.
Expected result: ~$407,960.30 according to your guide.

This category includes:
- Multi-language spots (spans_multiple_blocks = 1 OR block_id IS NULL)
- EXCLUDES Chinese Prime Time (the key complexity)
- Excludes WorldLink
- Represents Filipino-led cross-cultural advertising (45.6% of category)
- Cross-audience content outside Chinese prime viewing hours
"""

import sqlite3
from query_builders import BaseQueryBuilder, validate_query_migration

class MultiLanguageQueryBuilder(BaseQueryBuilder):
    """Builder for Multi-Language (Cross-Audience) revenue queries"""
    
    def __init__(self, year: str = "2024"):
        super().__init__(year)
        self.apply_standard_filters()
        self.exclude_worldlink()
        self.add_language_block_join()
    
    def add_multi_language_conditions(self) -> 'MultiLanguageQueryBuilder':
        """Add conditions for multi-language spots"""
        self.add_filter("(slb.spans_multiple_blocks = 1 OR "
                       "(slb.spans_multiple_blocks = 0 AND slb.block_id IS NULL) OR "
                       "(slb.spans_multiple_blocks IS NULL AND slb.block_id IS NULL))")
        return self
    
    def exclude_chinese_prime_time(self) -> 'MultiLanguageQueryBuilder':
        """Exclude Chinese Prime Time hours - this is the key complexity"""
        self.add_filter("""NOT (
            -- Exclude Chinese Prime Time M-F 7pm-11:59pm
            (s.time_in >= '19:00:00' AND s.time_out <= '23:59:59' 
             AND s.day_of_week IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'))
            OR
            -- Exclude Chinese Weekend 8pm-11:59pm  
            (s.time_in >= '20:00:00' AND s.time_out <= '23:59:59'
             AND s.day_of_week IN ('Saturday', 'Sunday'))
        )""")
        return self
    
    def exclude_nkb_overnight_shopping(self) -> 'MultiLanguageQueryBuilder':
        """Exclude NKB spots that belong to Overnight Shopping category"""
        self.add_customer_join()
        self.add_filter("COALESCE(c.normalized_name, '') NOT LIKE '%NKB%'")
        self.add_filter("COALESCE(s.bill_code, '') NOT LIKE '%NKB%'")
        # Agency join already added by exclude_worldlink()
        self.add_filter("COALESCE(a.agency_name, '') NOT LIKE '%NKB%'")
        return self

def get_multi_language_revenue(year="2024", db_connection=None):
    """
    Get Multi-Language (Cross-Audience) revenue
    
    Returns:
        QueryResult: Revenue, spot count, and execution details
    """
    builder = MultiLanguageQueryBuilder(year)
    builder.add_multi_language_conditions().exclude_chinese_prime_time().exclude_nkb_overnight_shopping()
    
    return builder.execute_revenue_query(db_connection)

def validate_multi_language_migration(db_connection, year="2024"):
    """
    Validate that our new Multi-Language query matches the original
    
    This uses the exact query from your Revenue-Querying-By-Language-Guide.md
    """
    
    # Original query from your guide (updated to exclude NKB spots)
    old_query = f"""
    SELECT SUM(COALESCE(s.gross_rate, 0)) as revenue
    FROM spots s
    JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    LEFT JOIN customers c ON s.customer_id = c.customer_id
    WHERE s.broadcast_month LIKE '%-{year[-2:]}'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND (slb.spans_multiple_blocks = 1 OR 
         (slb.spans_multiple_blocks = 0 AND slb.block_id IS NULL) OR 
         (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NULL))
    AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
    AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
    -- EXCLUDE Chinese Prime Time
    AND NOT (
        (s.time_in >= '19:00:00' AND s.time_out <= '23:59:59' 
         AND s.day_of_week IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'))
        OR
        (s.time_in >= '20:00:00' AND s.time_out <= '23:59:59'
         AND s.day_of_week IN ('Saturday', 'Sunday'))
    )
    -- EXCLUDE NKB spots (they go to overnight shopping)
    AND COALESCE(c.normalized_name, '') NOT LIKE '%NKB%'
    AND COALESCE(s.bill_code, '') NOT LIKE '%NKB%'
    AND COALESCE(a.agency_name, '') NOT LIKE '%NKB%'
    """
    
    # New query using builder
    builder = MultiLanguageQueryBuilder(year)
    builder.add_multi_language_conditions().exclude_chinese_prime_time().exclude_nkb_overnight_shopping()
    
    # Validate
    return validate_query_migration(old_query, builder, db_connection)

def analyze_multi_language_patterns(year="2024", db_connection=None):
    """
    Analyze Multi-Language (Cross-Audience) patterns in detail
    
    Returns:
        dict: Detailed breakdown showing Filipino leadership and cross-audience patterns
    """
    
    # Language code breakdown to show Filipino leadership
    language_code_query = f"""
    SELECT 
        COALESCE(s.language_code, 'Unknown') as language_code,
        COUNT(*) as spots,
        SUM(COALESCE(s.gross_rate, 0)) as revenue,
        ROUND(COUNT(*) * 100.0 / (
            SELECT COUNT(*) FROM spots s2
            JOIN spot_language_blocks slb2 ON s2.spot_id = slb2.spot_id
            LEFT JOIN agencies a2 ON s2.agency_id = a2.agency_id
            LEFT JOIN customers c2 ON s2.customer_id = c2.customer_id
            WHERE s2.broadcast_month LIKE '%-{year[-2:]}'
            AND (s2.revenue_type != 'Trade' OR s2.revenue_type IS NULL)
            AND (s2.gross_rate IS NOT NULL OR s2.station_net IS NOT NULL OR s2.spot_type = 'BNS')
            AND (slb2.spans_multiple_blocks = 1 OR 
                 (slb2.spans_multiple_blocks = 0 AND slb2.block_id IS NULL) OR 
                 (slb2.spans_multiple_blocks IS NULL AND slb2.block_id IS NULL))
            AND COALESCE(a2.agency_name, '') NOT LIKE '%WorldLink%'
            AND COALESCE(s2.bill_code, '') NOT LIKE '%WorldLink%'
            AND NOT (
                (s2.time_in >= '19:00:00' AND s2.time_out <= '23:59:59' 
                 AND s2.day_of_week IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'))
                OR
                (s2.time_in >= '20:00:00' AND s2.time_out <= '23:59:59'
                 AND s2.day_of_week IN ('Saturday', 'Sunday'))
            )
            AND COALESCE(c2.normalized_name, '') NOT LIKE '%NKB%'
            AND COALESCE(s2.bill_code, '') NOT LIKE '%NKB%'
            AND COALESCE(a2.agency_name, '') NOT LIKE '%NKB%'
        ), 2) as spot_percentage,
        ROUND(SUM(COALESCE(s.gross_rate, 0)) * 100.0 / (
            SELECT SUM(COALESCE(s3.gross_rate, 0)) FROM spots s3
            JOIN spot_language_blocks slb3 ON s3.spot_id = slb3.spot_id
            LEFT JOIN agencies a3 ON s3.agency_id = a3.agency_id
            LEFT JOIN customers c3 ON s3.customer_id = c3.customer_id
            WHERE s3.broadcast_month LIKE '%-{year[-2:]}'
            AND (s3.revenue_type != 'Trade' OR s3.revenue_type IS NULL)
            AND (s3.gross_rate IS NOT NULL OR s3.station_net IS NOT NULL OR s3.spot_type = 'BNS')
            AND (slb3.spans_multiple_blocks = 1 OR 
                 (slb3.spans_multiple_blocks = 0 AND slb3.block_id IS NULL) OR 
                 (slb3.spans_multiple_blocks IS NULL AND slb3.block_id IS NULL))
            AND COALESCE(a3.agency_name, '') NOT LIKE '%WorldLink%'
            AND COALESCE(s3.bill_code, '') NOT LIKE '%WorldLink%'
            AND NOT (
                (s3.time_in >= '19:00:00' AND s3.time_out <= '23:59:59' 
                 AND s3.day_of_week IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'))
                OR
                (s3.time_in >= '20:00:00' AND s3.time_out <= '23:59:59'
                 AND s3.day_of_week IN ('Saturday', 'Sunday'))
            )
            AND COALESCE(c3.normalized_name, '') NOT LIKE '%NKB%'
            AND COALESCE(s3.bill_code, '') NOT LIKE '%NKB%'
            AND COALESCE(a3.agency_name, '') NOT LIKE '%NKB%'
        ), 2) as revenue_percentage
    FROM spots s
    JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    LEFT JOIN customers c ON s.customer_id = c.customer_id
    WHERE s.broadcast_month LIKE '%-{year[-2:]}'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND (slb.spans_multiple_blocks = 1 OR 
         (slb.spans_multiple_blocks = 0 AND slb.block_id IS NULL) OR 
         (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NULL))
    AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
    AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
    AND NOT (
        (s.time_in >= '19:00:00' AND s.time_out <= '23:59:59' 
         AND s.day_of_week IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'))
        OR
        (s.time_in >= '20:00:00' AND s.time_out <= '23:59:59'
         AND s.day_of_week IN ('Saturday', 'Sunday'))
    )
    AND COALESCE(c.normalized_name, '') NOT LIKE '%NKB%'
    AND COALESCE(s.bill_code, '') NOT LIKE '%NKB%'
    AND COALESCE(a.agency_name, '') NOT LIKE '%NKB%'
    GROUP BY s.language_code
    ORDER BY SUM(COALESCE(s.gross_rate, 0)) DESC
    """
    
    cursor = db_connection.cursor()
    cursor.execute(language_code_query)
    language_breakdown = cursor.fetchall()
    
    # Day of week breakdown
    day_of_week_query = f"""
    SELECT 
        s.day_of_week,
        COUNT(*) as spots,
        SUM(COALESCE(s.gross_rate, 0)) as revenue,
        AVG(COALESCE(s.gross_rate, 0)) as avg_rate
    FROM spots s
    JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    LEFT JOIN customers c ON s.customer_id = c.customer_id
    WHERE s.broadcast_month LIKE '%-{year[-2:]}'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND (slb.spans_multiple_blocks = 1 OR 
         (slb.spans_multiple_blocks = 0 AND slb.block_id IS NULL) OR 
         (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NULL))
    AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
    AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
    AND NOT (
        (s.time_in >= '19:00:00' AND s.time_out <= '23:59:59' 
         AND s.day_of_week IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'))
        OR
        (s.time_in >= '20:00:00' AND s.time_out <= '23:59:59'
         AND s.day_of_week IN ('Saturday', 'Sunday'))
    )
    AND COALESCE(c.normalized_name, '') NOT LIKE '%NKB%'
    AND COALESCE(s.bill_code, '') NOT LIKE '%NKB%'
    AND COALESCE(a.agency_name, '') NOT LIKE '%NKB%'
    GROUP BY s.day_of_week
    ORDER BY 
        CASE s.day_of_week 
            WHEN 'Monday' THEN 1
            WHEN 'Tuesday' THEN 2
            WHEN 'Wednesday' THEN 3
            WHEN 'Thursday' THEN 4
            WHEN 'Friday' THEN 5
            WHEN 'Saturday' THEN 6
            WHEN 'Sunday' THEN 7
        END
    """
    
    cursor.execute(day_of_week_query)
    day_breakdown = cursor.fetchall()
    
    return {
        'language_breakdown': language_breakdown,
        'day_breakdown': day_breakdown
    }

def verify_nkb_exclusion_fix(year="2024", db_connection=None):
    """
    Verify that the $66,700 difference was caused by NKB spots
    
    Returns:
        dict: Analysis of NKB spots that were incorrectly included
    """
    
    # Query to find NKB spots in multi-language category (without NKB exclusion)
    nkb_spots_query = f"""
    SELECT 
        COUNT(*) as nkb_spots,
        SUM(COALESCE(s.gross_rate, 0)) as nkb_revenue,
        c.normalized_name as customer_name,
        s.bill_code
    FROM spots s
    JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    LEFT JOIN customers c ON s.customer_id = c.customer_id
    WHERE s.broadcast_month LIKE '%-{year[-2:]}'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND (slb.spans_multiple_blocks = 1 OR 
         (slb.spans_multiple_blocks = 0 AND slb.block_id IS NULL) OR 
         (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NULL))
    AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
    AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
    AND NOT (
        (s.time_in >= '19:00:00' AND s.time_out <= '23:59:59' 
         AND s.day_of_week IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'))
        OR
        (s.time_in >= '20:00:00' AND s.time_out <= '23:59:59'
         AND s.day_of_week IN ('Saturday', 'Sunday'))
    )
    -- Only NKB spots
    AND (
        COALESCE(c.normalized_name, '') LIKE '%NKB%' 
        OR COALESCE(s.bill_code, '') LIKE '%NKB%'
        OR COALESCE(a.agency_name, '') LIKE '%NKB%'
    )
    GROUP BY c.normalized_name, s.bill_code
    ORDER BY SUM(COALESCE(s.gross_rate, 0)) DESC
    """
    
    cursor = db_connection.cursor()
    cursor.execute(nkb_spots_query)
    nkb_details = cursor.fetchall()
    
    # Get total NKB revenue that was incorrectly included
    total_nkb_revenue = sum(row[1] for row in nkb_details)
    total_nkb_spots = sum(row[0] for row in nkb_details)
    
    return {
        'total_nkb_revenue': total_nkb_revenue,
        'total_nkb_spots': total_nkb_spots,
        'nkb_details': nkb_details
    }

def test_multi_language_migration(year="2024"):
    """Complete test of Multi-Language migration"""
    
    print(f"🧪 Testing Multi-Language (Cross-Audience) Migration for {year}")
    print("=" * 60)
    
    # Connect to database
    conn = sqlite3.connect('data/database/production.db')
    
    try:
        # 0. First, verify the NKB exclusion fix
        print("\n0. Verifying NKB Exclusion Fix:")
        nkb_analysis = verify_nkb_exclusion_fix(year, conn)
        print(f"   NKB spots that were incorrectly included: {nkb_analysis['total_nkb_spots']:,}")
        print(f"   NKB revenue that was incorrectly included: ${nkb_analysis['total_nkb_revenue']:,.2f}")
        
        if abs(nkb_analysis['total_nkb_revenue'] - 66700.00) < 1.0:
            print(f"   ✅ Confirmed: The $66,700 difference was from NKB spots!")
        else:
            print(f"   ❓ Unexpected: NKB revenue doesn't match expected $66,700")
            
        # 1. Validate migration
        print("\n1. Validating Migration:")
        if validate_multi_language_migration(conn, year):
            print("   ✅ Migration validation passed!")
        else:
            print("   ❌ Migration validation failed!")
            return False
        
        # 2. Get total revenue
        print("\n2. Multi-Language (Cross-Audience) Revenue:")
        result = get_multi_language_revenue(year, conn)
        
        print(f"   Total Revenue: ${result.revenue:,.2f}")
        print(f"   Total Spots: {result.spot_count:,}")
        print(f"   Average Rate: ${result.revenue/result.spot_count:,.2f}" if result.spot_count > 0 else "   Average Rate: N/A")
        print(f"   Execution Time: {result.execution_time:.3f}s")
        
        # 3. Compare with documented results
        print(f"\n3. Validation Against Your Guide:")
        documented_multi_language_total = 407960.30  # From your guide
        difference = abs(result.revenue - documented_multi_language_total)
        
        print(f"   Expected (from guide): ${documented_multi_language_total:,.2f}")
        print(f"   Actual (new query): ${result.revenue:,.2f}")
        print(f"   Difference: ${difference:,.2f}")
        
        if difference < 1.0:
            print(f"   ✅ PERFECT MATCH! (Difference < $1.00)")
            perfect_match = True
        else:
            print(f"   ❌ DIFFERENCE FOUND! (Difference: ${difference:,.2f})")
            perfect_match = False
            
        # 4. Analyze patterns
        print(f"\n4. Cross-Audience Analysis:")
        patterns = analyze_multi_language_patterns(year, conn)
        
        print(f"   Language Code Breakdown (showing Filipino leadership):")
        filipino_found = False
        for lang_code, spots, revenue, spot_pct, rev_pct in patterns['language_breakdown'][:8]:
            if lang_code == 'T':  # Tagalog = Filipino
                filipino_found = True
                print(f"   {lang_code:<8} (Filipino): ${revenue:>12,.2f} ({spots:>6,} spots, {spot_pct:>5.1f}% of spots, {rev_pct:>5.1f}% of revenue)")
            else:
                print(f"   {lang_code:<8}: ${revenue:>12,.2f} ({spots:>6,} spots, {spot_pct:>5.1f}% of spots, {rev_pct:>5.1f}% of revenue)")
        
        # 5. Day of week breakdown
        print(f"\n   Day of Week Breakdown:")
        weekend_total = 0
        weekday_total = 0
        
        for day, spots, revenue, avg_rate in patterns['day_breakdown']:
            pct = (revenue / result.revenue) * 100
            print(f"   {day:<9}: ${revenue:>12,.2f} ({spots:>6,} spots, ${avg_rate:>6.2f} avg) {pct:>5.1f}%")
            
            if day in ['Saturday', 'Sunday']:
                weekend_total += revenue
            else:
                weekday_total += revenue
        
        # 6. Strategic insights
        print(f"\n5. Strategic Insights:")
        print(f"   • Filipino Cross-Audience Leadership: Look for 'T' (Tagalog) dominance")
        print(f"   • Weekday Revenue: ${weekday_total:,.2f} ({weekday_total/result.revenue*100:.1f}%)")
        print(f"   • Weekend Revenue: ${weekend_total:,.2f} ({weekend_total/result.revenue*100:.1f}%)")
        print(f"   • Excludes Chinese Prime Time: Shows non-Chinese cross-audience strategy")
        print(f"   • Excludes NKB Overnight Shopping: Clean separation of categories")
        print(f"   • Transition Time Focus: Cross-cultural advertising outside premium hours")
        
        # 7. Show generated query
        print(f"\n6. Generated Query:")
        builder = MultiLanguageQueryBuilder(year)
        builder.add_multi_language_conditions().exclude_chinese_prime_time().exclude_nkb_overnight_shopping()
        print("   " + builder.build_select_revenue_query().replace('\n', '\n   '))
        
        return perfect_match
        
    finally:
        conn.close()

if __name__ == "__main__":
    # Run the complete test
    print("🚀 Multi-Language (Cross-Audience) Revenue Migration Test")
    print("=" * 60)
    
    # Test the migration
    success = test_multi_language_migration("2024")
    
    if success:
        print(f"\n✅ Multi-Language Migration Test Complete!")
        print(f"Ready to proceed with remaining categories!")
    else:
        print(f"\n❌ Migration test failed - investigate before proceeding!")