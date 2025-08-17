"""
Overnight Shopping Revenue Migration
===================================

This implements the Overnight Shopping revenue category using BaseQueryBuilder.
Expected result: ~$66,700.00 according to your guide.

This category includes:
- No language assignment (slb.spot_id IS NULL)
- Not PRD/SVC spot types (regular spots)
- Excludes WorldLink
- ONLY NKB spots (NKB:Shop LC overnight programming)
- 7-day operation starting 6:00:00+ (early morning programming)
"""

import sqlite3
from query_builders import BaseQueryBuilder, validate_query_migration

class OvernightShoppingQueryBuilder(BaseQueryBuilder):
    """Builder for Overnight Shopping revenue queries"""
    
    def __init__(self, year: str = "2024"):
        super().__init__(year)
        self.apply_standard_filters()
        self.exclude_worldlink()
        self.add_customer_join()
    
    def add_no_language_assignment_condition(self) -> 'OvernightShoppingQueryBuilder':
        """Add condition for spots with no language assignment"""
        # LEFT JOIN so we can check for NULL
        self.add_left_join("spot_language_blocks slb", "s.spot_id = slb.spot_id")
        self.add_filter("slb.spot_id IS NULL")
        return self
    
    def exclude_prd_svc_spots(self) -> 'OvernightShoppingQueryBuilder':
        """Exclude PRD and SVC spot types"""
        self.add_filter("(s.spot_type NOT IN ('PRD', 'SVC') OR s.spot_type IS NULL OR s.spot_type = '')")
        return self
    
    def include_only_nkb_spots(self) -> 'OvernightShoppingQueryBuilder':
        """Include ONLY NKB spots (overnight shopping programming)"""
        self.add_filter("""(
            COALESCE(c.normalized_name, '') LIKE '%NKB%' 
            OR COALESCE(s.bill_code, '') LIKE '%NKB%'
            OR COALESCE(a.agency_name, '') LIKE '%NKB%'
        )""")
        return self

def get_overnight_shopping_revenue(year="2024", db_connection=None):
    """
    Get Overnight Shopping revenue
    
    Returns:
        QueryResult: Revenue, spot count, and execution details
    """
    builder = OvernightShoppingQueryBuilder(year)
    builder.add_no_language_assignment_condition().exclude_prd_svc_spots().include_only_nkb_spots()
    
    return builder.execute_revenue_query(db_connection)

def validate_overnight_shopping_migration(db_connection, year="2024"):
    """
    Validate that our new Overnight Shopping query matches the original
    
    This uses the exact query from your Revenue-Querying-By-Language-Guide.md
    """
    
    # Original query from your guide
    old_query = f"""
    SELECT SUM(COALESCE(s.gross_rate, 0)) as revenue
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    LEFT JOIN customers c ON s.customer_id = c.customer_id
    WHERE s.broadcast_month LIKE '%-{year[-2:]}'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND slb.spot_id IS NULL
    AND (s.spot_type NOT IN ('PRD', 'SVC') OR s.spot_type IS NULL OR s.spot_type = '')
    AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
    AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
    -- ONLY NKB spots
    AND (
        COALESCE(c.normalized_name, '') LIKE '%NKB%' 
        OR COALESCE(s.bill_code, '') LIKE '%NKB%'
        OR COALESCE(a.agency_name, '') LIKE '%NKB%'
    )
    """
    
    # New query using builder
    builder = OvernightShoppingQueryBuilder(year)
    builder.add_no_language_assignment_condition().exclude_prd_svc_spots().include_only_nkb_spots()
    
    # Validate
    return validate_query_migration(old_query, builder, db_connection)

def analyze_overnight_shopping_patterns(year="2024", db_connection=None):
    """
    Analyze Overnight Shopping patterns in detail
    
    Returns:
        dict: Detailed breakdown of NKB overnight shopping programming
    """
    
    # Time of day analysis
    time_analysis_query = f"""
    SELECT 
        CASE 
            WHEN s.time_in >= '06:00:00' AND s.time_in < '12:00:00' THEN 'Morning (6am-12pm)'
            WHEN s.time_in >= '12:00:00' AND s.time_in < '18:00:00' THEN 'Afternoon (12pm-6pm)'
            WHEN s.time_in >= '18:00:00' AND s.time_in < '24:00:00' THEN 'Evening (6pm-12am)'
            ELSE 'Overnight (12am-6am)'
        END as time_period,
        COUNT(*) as spots,
        SUM(COALESCE(s.gross_rate, 0)) as revenue,
        AVG(COALESCE(s.gross_rate, 0)) as avg_rate,
        MIN(s.time_in) as earliest_time,
        MAX(s.time_out) as latest_time
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    LEFT JOIN customers c ON s.customer_id = c.customer_id
    WHERE s.broadcast_month LIKE '%-{year[-2:]}'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND slb.spot_id IS NULL
    AND (s.spot_type NOT IN ('PRD', 'SVC') OR s.spot_type IS NULL OR s.spot_type = '')
    AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
    AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
    AND (
        COALESCE(c.normalized_name, '') LIKE '%NKB%' 
        OR COALESCE(s.bill_code, '') LIKE '%NKB%'
        OR COALESCE(a.agency_name, '') LIKE '%NKB%'
    )
    GROUP BY 
        CASE 
            WHEN s.time_in >= '06:00:00' AND s.time_in < '12:00:00' THEN 'Morning (6am-12pm)'
            WHEN s.time_in >= '12:00:00' AND s.time_in < '18:00:00' THEN 'Afternoon (12pm-6pm)'
            WHEN s.time_in >= '18:00:00' AND s.time_in < '24:00:00' THEN 'Evening (6pm-12am)'
            ELSE 'Overnight (12am-6am)'
        END
    ORDER BY SUM(COALESCE(s.gross_rate, 0)) DESC
    """
    
    cursor = db_connection.cursor()
    cursor.execute(time_analysis_query)
    time_breakdown = cursor.fetchall()
    
    # Day of week analysis
    day_analysis_query = f"""
    SELECT 
        s.day_of_week,
        COUNT(*) as spots,
        SUM(COALESCE(s.gross_rate, 0)) as revenue,
        AVG(COALESCE(s.gross_rate, 0)) as avg_rate
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    LEFT JOIN customers c ON s.customer_id = c.customer_id
    WHERE s.broadcast_month LIKE '%-{year[-2:]}'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND slb.spot_id IS NULL
    AND (s.spot_type NOT IN ('PRD', 'SVC') OR s.spot_type IS NULL OR s.spot_type = '')
    AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
    AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
    AND (
        COALESCE(c.normalized_name, '') LIKE '%NKB%' 
        OR COALESCE(s.bill_code, '') LIKE '%NKB%'
        OR COALESCE(a.agency_name, '') LIKE '%NKB%'
    )
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
    
    cursor.execute(day_analysis_query)
    day_breakdown = cursor.fetchall()
    
    # Customer/bill code analysis
    customer_analysis_query = f"""
    SELECT 
        COALESCE(c.normalized_name, 'Unknown') as customer_name,
        s.bill_code,
        COUNT(*) as spots,
        SUM(COALESCE(s.gross_rate, 0)) as revenue
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    LEFT JOIN customers c ON s.customer_id = c.customer_id
    WHERE s.broadcast_month LIKE '%-{year[-2:]}'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND slb.spot_id IS NULL
    AND (s.spot_type NOT IN ('PRD', 'SVC') OR s.spot_type IS NULL OR s.spot_type = '')
    AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
    AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
    AND (
        COALESCE(c.normalized_name, '') LIKE '%NKB%' 
        OR COALESCE(s.bill_code, '') LIKE '%NKB%'
        OR COALESCE(a.agency_name, '') LIKE '%NKB%'
    )
    GROUP BY c.normalized_name, s.bill_code
    ORDER BY SUM(COALESCE(s.gross_rate, 0)) DESC
    """
    
    cursor.execute(customer_analysis_query)
    customer_breakdown = cursor.fetchall()
    
    return {
        'time_breakdown': time_breakdown,
        'day_breakdown': day_breakdown,
        'customer_breakdown': customer_breakdown
    }

def test_overnight_shopping_migration(year="2024"):
    """Complete test of Overnight Shopping migration"""
    
    print(f"🧪 Testing Overnight Shopping Migration for {year}")
    print("=" * 60)
    
    # Connect to database
    conn = sqlite3.connect('data/database/production.db')
    
    try:
        # 1. Validate migration
        print("\n1. Validating Migration:")
        if validate_overnight_shopping_migration(conn, year):
            print("   ✅ Migration validation passed!")
        else:
            print("   ❌ Migration validation failed!")
            return False
        
        # 2. Get total revenue
        print("\n2. Overnight Shopping Revenue:")
        result = get_overnight_shopping_revenue(year, conn)
        
        print(f"   Total Revenue: ${result.revenue:,.2f}")
        print(f"   Total Spots: {result.spot_count:,}")
        print(f"   Average Rate: ${result.revenue/result.spot_count:,.2f}" if result.spot_count > 0 else "   Average Rate: N/A")
        print(f"   Execution Time: {result.execution_time:.3f}s")
        
        # 3. Compare with documented results
        print(f"\n3. Validation Against Your Guide:")
        documented_overnight_shopping_total = 66700.00  # From your guide
        difference = abs(result.revenue - documented_overnight_shopping_total)
        
        print(f"   Expected (from guide): ${documented_overnight_shopping_total:,.2f}")
        print(f"   Actual (new query): ${result.revenue:,.2f}")
        print(f"   Difference: ${difference:,.2f}")
        
        if difference < 1.0:
            print(f"   ✅ PERFECT MATCH! (Difference < $1.00)")
            perfect_match = True
        else:
            print(f"   ❌ DIFFERENCE FOUND! (Difference: ${difference:,.2f})")
            perfect_match = False
            
        # 4. Analyze patterns
        print(f"\n4. Overnight Shopping Programming Analysis:")
        patterns = analyze_overnight_shopping_patterns(year, conn)
        
        print(f"   Time Period Breakdown:")
        for time_period, spots, revenue, avg_rate, earliest, latest in patterns['time_breakdown']:
            pct = (revenue / result.revenue) * 100 if result.revenue > 0 else 0
            print(f"   {time_period:<20}: ${revenue:>10,.2f} ({spots:>3,} spots, ${avg_rate:>6.2f} avg) {pct:>5.1f}%")
            print(f"   {'':>21}  Time range: {earliest} - {latest}")
        
        print(f"\n   Day of Week Breakdown (7-day operation):")
        for day, spots, revenue, avg_rate in patterns['day_breakdown']:
            pct = (revenue / result.revenue) * 100 if result.revenue > 0 else 0
            print(f"   {day:<9}: ${revenue:>10,.2f} ({spots:>3,} spots, ${avg_rate:>6.2f} avg) {pct:>5.1f}%")
        
        print(f"\n   Customer/Bill Code Breakdown:")
        for customer, bill_code, spots, revenue in patterns['customer_breakdown']:
            pct = (revenue / result.revenue) * 100 if result.revenue > 0 else 0
            print(f"   {customer:<20} | {bill_code:<20}: ${revenue:>10,.2f} ({spots:>3,} spots) {pct:>5.1f}%")
        
        # 5. Strategic insights
        print(f"\n5. Strategic Insights:")
        print(f"   • NKB:Shop LC Programming: Dedicated shopping channel content")
        print(f"   • 7-Day Operation: Consistent programming across all days")
        print(f"   • Early Morning Focus: Programming starts at 6:00:00+ (note time periods)")
        print(f"   • Separate Business Model: Shopping channel vs. traditional advertising")
        print(f"   • Revenue Contribution: {result.revenue/4076255.94*100:.1f}% of total revenue")
        
        # 6. Show generated query
        print(f"\n6. Generated Query:")
        builder = OvernightShoppingQueryBuilder(year)
        builder.add_no_language_assignment_condition().exclude_prd_svc_spots().include_only_nkb_spots()
        print("   " + builder.build_select_revenue_query().replace('\n', '\n   '))
        
        return perfect_match
        
    finally:
        conn.close()

if __name__ == "__main__":
    # Run the complete test
    print("🚀 Overnight Shopping Revenue Migration Test")
    print("=" * 50)
    
    # Test the migration
    success = test_overnight_shopping_migration("2024")
    
    if success:
        print(f"\n✅ Overnight Shopping Migration Test Complete!")
        print(f"Ready for Branded Content (PRD) category!")
    else:
        print(f"\n❌ Migration test failed - investigate before proceeding!")