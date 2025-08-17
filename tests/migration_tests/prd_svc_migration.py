"""
Branded Content (PRD) and Services (SVC) Migration
==================================================

This implements the final two revenue categories using BaseQueryBuilder:
- Branded Content (PRD): ~$52,592.29 
- Services (SVC): ~$12,000.00

Both categories are simple:
- No language assignment (slb.spot_id IS NULL)
- Specific spot_type (PRD or SVC)
- No WorldLink exclusion needed (they don't use WorldLink)
"""

import sqlite3
from query_builders import BaseQueryBuilder, validate_query_migration

class BrandedContentQueryBuilder(BaseQueryBuilder):
    """Builder for Branded Content (PRD) revenue queries"""
    
    def __init__(self, year: str = "2024"):
        super().__init__(year)
        self.apply_standard_filters()
        # Note: No WorldLink exclusion needed for PRD
    
    def add_no_language_assignment_condition(self) -> 'BrandedContentQueryBuilder':
        """Add condition for spots with no language assignment"""
        self.add_left_join("spot_language_blocks slb", "s.spot_id = slb.spot_id")
        self.add_filter("slb.spot_id IS NULL")
        return self
    
    def add_prd_spot_type_condition(self) -> 'BrandedContentQueryBuilder':
        """Add condition for PRD spot type"""
        self.add_filter("s.spot_type = 'PRD'")
        return self

class ServicesQueryBuilder(BaseQueryBuilder):
    """Builder for Services (SVC) revenue queries"""
    
    def __init__(self, year: str = "2024"):
        super().__init__(year)
        self.apply_standard_filters()
        # Note: No WorldLink exclusion needed for SVC
    
    def add_no_language_assignment_condition(self) -> 'ServicesQueryBuilder':
        """Add condition for spots with no language assignment"""
        self.add_left_join("spot_language_blocks slb", "s.spot_id = slb.spot_id")
        self.add_filter("slb.spot_id IS NULL")
        return self
    
    def add_svc_spot_type_condition(self) -> 'ServicesQueryBuilder':
        """Add condition for SVC spot type"""
        self.add_filter("s.spot_type = 'SVC'")
        return self

def get_branded_content_revenue(year="2024", db_connection=None):
    """Get Branded Content (PRD) revenue"""
    builder = BrandedContentQueryBuilder(year)
    builder.add_no_language_assignment_condition().add_prd_spot_type_condition()
    return builder.execute_revenue_query(db_connection)

def get_services_revenue(year="2024", db_connection=None):
    """Get Services (SVC) revenue"""
    builder = ServicesQueryBuilder(year)
    builder.add_no_language_assignment_condition().add_svc_spot_type_condition()
    return builder.execute_revenue_query(db_connection)

def validate_branded_content_migration(db_connection, year="2024"):
    """Validate Branded Content (PRD) migration"""
    
    # Original query from your guide
    old_query = f"""
    SELECT SUM(COALESCE(s.gross_rate, 0)) as revenue
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    WHERE s.broadcast_month LIKE '%-{year[-2:]}'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND slb.spot_id IS NULL
    AND s.spot_type = 'PRD'
    """
    
    # New query using builder
    builder = BrandedContentQueryBuilder(year)
    builder.add_no_language_assignment_condition().add_prd_spot_type_condition()
    
    return validate_query_migration(old_query, builder, db_connection)

def validate_services_migration(db_connection, year="2024"):
    """Validate Services (SVC) migration"""
    
    # Original query from your guide
    old_query = f"""
    SELECT SUM(COALESCE(s.gross_rate, 0)) as revenue
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    WHERE s.broadcast_month LIKE '%-{year[-2:]}'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND slb.spot_id IS NULL
    AND s.spot_type = 'SVC'
    """
    
    # New query using builder
    builder = ServicesQueryBuilder(year)
    builder.add_no_language_assignment_condition().add_svc_spot_type_condition()
    
    return validate_query_migration(old_query, builder, db_connection)

def analyze_prd_svc_patterns(year="2024", db_connection=None):
    """Analyze PRD and SVC patterns"""
    
    # PRD analysis
    prd_query = f"""
    SELECT 
        'PRD' as category,
        COALESCE(c.normalized_name, 'Unknown') as customer_name,
        s.bill_code,
        COUNT(*) as spots,
        SUM(COALESCE(s.gross_rate, 0)) as revenue
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN customers c ON s.customer_id = c.customer_id
    WHERE s.broadcast_month LIKE '%-{year[-2:]}'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND slb.spot_id IS NULL
    AND s.spot_type = 'PRD'
    GROUP BY c.normalized_name, s.bill_code
    ORDER BY SUM(COALESCE(s.gross_rate, 0)) DESC
    """
    
    # SVC analysis
    svc_query = f"""
    SELECT 
        'SVC' as category,
        COALESCE(c.normalized_name, 'Unknown') as customer_name,
        s.bill_code,
        COUNT(*) as spots,
        SUM(COALESCE(s.gross_rate, 0)) as revenue
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN customers c ON s.customer_id = c.customer_id
    WHERE s.broadcast_month LIKE '%-{year[-2:]}'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND slb.spot_id IS NULL
    AND s.spot_type = 'SVC'
    GROUP BY c.normalized_name, s.bill_code
    ORDER BY SUM(COALESCE(s.gross_rate, 0)) DESC
    """
    
    cursor = db_connection.cursor()
    
    cursor.execute(prd_query)
    prd_breakdown = cursor.fetchall()
    
    cursor.execute(svc_query)
    svc_breakdown = cursor.fetchall()
    
    return {
        'prd_breakdown': prd_breakdown,
        'svc_breakdown': svc_breakdown
    }

def test_prd_svc_migration(year="2024"):
    """Complete test of PRD and SVC migrations"""
    
    print(f"🧪 Testing Branded Content (PRD) and Services (SVC) Migration for {year}")
    print("=" * 70)
    
    # Connect to database
    conn = sqlite3.connect('data/database/production.db')
    
    try:
        # 1. Validate PRD migration
        print("\n1. Validating Branded Content (PRD) Migration:")
        if validate_branded_content_migration(conn, year):
            print("   ✅ PRD migration validation passed!")
        else:
            print("   ❌ PRD migration validation failed!")
            return False
        
        # 2. Validate SVC migration
        print("\n2. Validating Services (SVC) Migration:")
        if validate_services_migration(conn, year):
            print("   ✅ SVC migration validation passed!")
        else:
            print("   ❌ SVC migration validation failed!")
            return False
        
        # 3. Get PRD revenue
        print("\n3. Branded Content (PRD) Revenue:")
        prd_result = get_branded_content_revenue(year, conn)
        
        print(f"   Total Revenue: ${prd_result.revenue:,.2f}")
        print(f"   Total Spots: {prd_result.spot_count:,}")
        print(f"   Average Rate: ${prd_result.revenue/prd_result.spot_count:,.2f}" if prd_result.spot_count > 0 else "   Average Rate: N/A")
        print(f"   Execution Time: {prd_result.execution_time:.3f}s")
        
        # 4. Get SVC revenue
        print("\n4. Services (SVC) Revenue:")
        svc_result = get_services_revenue(year, conn)
        
        print(f"   Total Revenue: ${svc_result.revenue:,.2f}")
        print(f"   Total Spots: {svc_result.spot_count:,}")
        print(f"   Average Rate: ${svc_result.revenue/svc_result.spot_count:,.2f}" if svc_result.spot_count > 0 else "   Average Rate: N/A")
        print(f"   Execution Time: {svc_result.execution_time:.3f}s")
        
        # 5. Compare with documented results
        print(f"\n5. Validation Against Your Guide:")
        documented_prd_total = 52592.29  # From your guide
        documented_svc_total = 12000.00  # From your guide
        
        prd_difference = abs(prd_result.revenue - documented_prd_total)
        svc_difference = abs(svc_result.revenue - documented_svc_total)
        
        print(f"   PRD Expected: ${documented_prd_total:,.2f}")
        print(f"   PRD Actual: ${prd_result.revenue:,.2f}")
        print(f"   PRD Difference: ${prd_difference:,.2f}")
        
        print(f"   SVC Expected: ${documented_svc_total:,.2f}")
        print(f"   SVC Actual: ${svc_result.revenue:,.2f}")
        print(f"   SVC Difference: ${svc_difference:,.2f}")
        
        prd_match = prd_difference < 1.0
        svc_match = svc_difference < 1.0
        
        if prd_match and svc_match:
            print(f"   ✅ BOTH PERFECT MATCHES! (Differences < $1.00)")
            perfect_match = True
        elif prd_match:
            print(f"   ✅ PRD PERFECT MATCH, ❌ SVC difference found")
            perfect_match = False
        elif svc_match:
            print(f"   ❌ PRD difference found, ✅ SVC PERFECT MATCH")
            perfect_match = False
        else:
            print(f"   ❌ DIFFERENCES FOUND in both categories")
            perfect_match = False
            
        # 6. Analyze patterns
        print(f"\n6. PRD and SVC Analysis:")
        patterns = analyze_prd_svc_patterns(year, conn)
        
        print(f"   Branded Content (PRD) Breakdown:")
        if patterns['prd_breakdown']:
            for category, customer, bill_code, spots, revenue in patterns['prd_breakdown'][:5]:
                print(f"   {customer:<30} | {bill_code:<20}: ${revenue:>10,.2f} ({spots:>3,} spots)")
        else:
            print("   No PRD spots found")
        
        print(f"\n   Services (SVC) Breakdown:")
        if patterns['svc_breakdown']:
            for category, customer, bill_code, spots, revenue in patterns['svc_breakdown'][:5]:
                print(f"   {customer:<30} | {bill_code:<20}: ${revenue:>10,.2f} ({spots:>3,} spots)")
        else:
            print("   No SVC spots found")
        
        # 7. Strategic insights
        print(f"\n7. Strategic Insights:")
        print(f"   • PRD (Branded Content): Internal production work, not traditional advertising")
        print(f"   • SVC (Services): Station services and announcements")
        print(f"   • No Language Assignment: These don't target specific language audiences")
        print(f"   • Revenue Contribution: PRD {prd_result.revenue/4076255.94*100:.1f}%, SVC {svc_result.revenue/4076255.94*100:.1f}% of total")
        print(f"   • Combined: ${prd_result.revenue + svc_result.revenue:,.2f} ({(prd_result.revenue + svc_result.revenue)/4076255.94*100:.1f}%)")
        
        # 8. Show generated queries
        print(f"\n8. Generated Queries:")
        
        print(f"   PRD Query:")
        prd_builder = BrandedContentQueryBuilder(year)
        prd_builder.add_no_language_assignment_condition().add_prd_spot_type_condition()
        print("   " + prd_builder.build_select_revenue_query().replace('\n', '\n   '))
        
        print(f"\n   SVC Query:")
        svc_builder = ServicesQueryBuilder(year)
        svc_builder.add_no_language_assignment_condition().add_svc_spot_type_condition()
        print("   " + svc_builder.build_select_revenue_query().replace('\n', '\n   '))
        
        return perfect_match
        
    finally:
        conn.close()

if __name__ == "__main__":
    # Run the complete test
    print("🚀 Branded Content (PRD) and Services (SVC) Migration Test")
    print("=" * 60)
    
    # Test the migration
    success = test_prd_svc_migration("2024")
    
    if success:
        print(f"\n✅ PRD and SVC Migration Test Complete!")
        print(f"All revenue categories migrated successfully!")
    else:
        print(f"\n❌ Migration test failed - investigate before proceeding!")