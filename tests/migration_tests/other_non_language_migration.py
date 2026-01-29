"""
Other Non-Language Revenue Migration
===================================

This implements the Other Non-Language revenue category using BaseQueryBuilder.
Expected result: ~$58,733.77 according to your guide.

This category includes:
- No language assignment (slb.spot_id IS NULL)
- Not PRD/SVC spot types (regular spots)
- Excludes WorldLink
- Excludes NKB (they go to Overnight Shopping)
- Miscellaneous spots requiring investigation
"""

import sqlite3
from query_builders import BaseQueryBuilder, validate_query_migration


class OtherNonLanguageQueryBuilder(BaseQueryBuilder):
    """Builder for Other Non-Language revenue queries"""

    def __init__(self, year: str = "2024"):
        super().__init__(year)
        self.apply_standard_filters()
        self.exclude_worldlink()
        self.add_customer_join()

    def add_no_language_assignment_condition(self) -> "OtherNonLanguageQueryBuilder":
        """Add condition for spots with no language assignment"""
        # LEFT JOIN so we can check for NULL
        self.add_left_join("spot_language_blocks slb", "s.spot_id = slb.spot_id")
        self.add_filter("slb.spot_id IS NULL")
        return self

    def exclude_prd_svc_spots(self) -> "OtherNonLanguageQueryBuilder":
        """Exclude PRD and SVC spot types"""
        self.add_filter(
            "(s.spot_type NOT IN ('PRD', 'SVC') OR s.spot_type IS NULL OR s.spot_type = '')"
        )
        return self

    def exclude_nkb_spots(self) -> "OtherNonLanguageQueryBuilder":
        """Exclude NKB spots (they go to overnight shopping)"""
        self.add_filter("COALESCE(c.normalized_name, '') NOT LIKE '%NKB%'")
        self.add_filter("COALESCE(s.bill_code, '') NOT LIKE '%NKB%'")
        # Agency join already added by exclude_worldlink()
        self.add_filter("COALESCE(a.agency_name, '') NOT LIKE '%NKB%'")
        return self


def get_other_non_language_revenue(year="2024", db_connection=None):
    """
    Get Other Non-Language revenue

    Returns:
        QueryResult: Revenue, spot count, and execution details
    """
    builder = OtherNonLanguageQueryBuilder(year)
    builder.add_no_language_assignment_condition().exclude_prd_svc_spots().exclude_nkb_spots()

    return builder.execute_revenue_query(db_connection)


def validate_other_non_language_migration(db_connection, year="2024"):
    """
    Validate that our new Other Non-Language query matches the original

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
    -- EXCLUDE NKB spots (they go to overnight shopping)
    AND COALESCE(c.normalized_name, '') NOT LIKE '%NKB%'
    AND COALESCE(s.bill_code, '') NOT LIKE '%NKB%'
    AND COALESCE(a.agency_name, '') NOT LIKE '%NKB%'
    """

    # New query using builder
    builder = OtherNonLanguageQueryBuilder(year)
    builder.add_no_language_assignment_condition().exclude_prd_svc_spots().exclude_nkb_spots()

    # Validate
    return validate_query_migration(old_query, builder, db_connection)


def analyze_other_non_language_patterns(year="2024", db_connection=None):
    """
    Analyze Other Non-Language patterns in detail

    Returns:
        dict: Detailed breakdown of miscellaneous non-language spots
    """

    # Customer breakdown
    customer_query = f"""
    SELECT 
        COALESCE(c.normalized_name, 'Unknown Customer') as customer_name,
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
    AND COALESCE(c.normalized_name, '') NOT LIKE '%NKB%'
    AND COALESCE(s.bill_code, '') NOT LIKE '%NKB%'
    AND COALESCE(a.agency_name, '') NOT LIKE '%NKB%'
    GROUP BY c.normalized_name
    ORDER BY SUM(COALESCE(s.gross_rate, 0)) DESC
    """

    cursor = db_connection.cursor()
    cursor.execute(customer_query)
    customer_breakdown = cursor.fetchall()

    # Spot type breakdown
    spot_type_query = f"""
    SELECT 
        COALESCE(s.spot_type, 'NULL/Empty') as spot_type,
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
    AND COALESCE(c.normalized_name, '') NOT LIKE '%NKB%'
    AND COALESCE(s.bill_code, '') NOT LIKE '%NKB%'
    AND COALESCE(a.agency_name, '') NOT LIKE '%NKB%'
    GROUP BY s.spot_type
    ORDER BY SUM(COALESCE(s.gross_rate, 0)) DESC
    """

    cursor.execute(spot_type_query)
    spot_type_breakdown = cursor.fetchall()

    # Agency breakdown (non-WorldLink agencies)
    agency_query = f"""
    SELECT 
        COALESCE(a.agency_name, 'No Agency') as agency_name,
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
    AND COALESCE(c.normalized_name, '') NOT LIKE '%NKB%'
    AND COALESCE(s.bill_code, '') NOT LIKE '%NKB%'
    AND COALESCE(a.agency_name, '') NOT LIKE '%NKB%'
    GROUP BY a.agency_name
    ORDER BY SUM(COALESCE(s.gross_rate, 0)) DESC
    """

    cursor.execute(agency_query)
    agency_breakdown = cursor.fetchall()

    return {
        "customer_breakdown": customer_breakdown,
        "spot_type_breakdown": spot_type_breakdown,
        "agency_breakdown": agency_breakdown,
    }


def test_other_non_language_migration(year="2024"):
    """Complete test of Other Non-Language migration"""

    print(f"🧪 Testing Other Non-Language Migration for {year}")
    print("=" * 60)

    # Connect to database
    conn = sqlite3.connect("data/database/production.db")

    try:
        # 1. Validate migration
        print("\n1. Validating Migration:")
        if validate_other_non_language_migration(conn, year):
            print("   ✅ Migration validation passed!")
        else:
            print("   ❌ Migration validation failed!")
            return False

        # 2. Get total revenue
        print("\n2. Other Non-Language Revenue:")
        result = get_other_non_language_revenue(year, conn)

        print(f"   Total Revenue: ${result.revenue:,.2f}")
        print(f"   Total Spots: {result.spot_count:,}")
        print(
            f"   Average Rate: ${result.revenue / result.spot_count:,.2f}"
            if result.spot_count > 0
            else "   Average Rate: N/A"
        )
        print(f"   Execution Time: {result.execution_time:.3f}s")

        # 3. Compare with documented results
        print("\n3. Validation Against Your Guide:")
        documented_other_non_language_total = 58733.77  # From your guide
        difference = abs(result.revenue - documented_other_non_language_total)

        print(f"   Expected (from guide): ${documented_other_non_language_total:,.2f}")
        print(f"   Actual (new query): ${result.revenue:,.2f}")
        print(f"   Difference: ${difference:,.2f}")

        if difference < 1.0:
            print("   ✅ PERFECT MATCH! (Difference < $1.00)")
            perfect_match = True
        else:
            print(f"   ❌ DIFFERENCE FOUND! (Difference: ${difference:,.2f})")
            perfect_match = False

        # 4. Analyze patterns
        print("\n4. Miscellaneous Non-Language Analysis:")
        patterns = analyze_other_non_language_patterns(year, conn)

        print("   Top Customers (needing investigation):")
        for customer, spots, revenue, avg_rate in patterns["customer_breakdown"][:5]:
            pct = (revenue / result.revenue) * 100 if result.revenue > 0 else 0
            print(
                f"   {customer:<30}: ${revenue:>10,.2f} ({spots:>3,} spots, ${avg_rate:>6.2f} avg) {pct:>5.1f}%"
            )

        print("\n   Spot Type Breakdown:")
        for spot_type, spots, revenue, avg_rate in patterns["spot_type_breakdown"]:
            pct = (revenue / result.revenue) * 100 if result.revenue > 0 else 0
            print(
                f"   {spot_type:<12}: ${revenue:>10,.2f} ({spots:>3,} spots, ${avg_rate:>6.2f} avg) {pct:>5.1f}%"
            )

        print("\n   Agency Breakdown:")
        for agency, spots, revenue, avg_rate in patterns["agency_breakdown"][:5]:
            pct = (revenue / result.revenue) * 100 if result.revenue > 0 else 0
            print(
                f"   {agency:<30}: ${revenue:>10,.2f} ({spots:>3,} spots, ${avg_rate:>6.2f} avg) {pct:>5.1f}%"
            )

        # 5. Strategic insights
        print("\n5. Strategic Insights:")
        print("   • No Language Assignment: These spots lack programming grid coverage")
        print(
            "   • Investigation Required: Identify why these spots have no language assignment"
        )
        print(
            "   • Potential Grid Gaps: May indicate missing programming schedule coverage"
        )
        print(
            f"   • Revenue Impact: Small but consistent revenue stream ({result.revenue / 4076255.94 * 100:.1f}% of total)"
        )

        # 6. Show generated query
        print("\n6. Generated Query:")
        builder = OtherNonLanguageQueryBuilder(year)
        builder.add_no_language_assignment_condition().exclude_prd_svc_spots().exclude_nkb_spots()
        print("   " + builder.build_select_revenue_query().replace("\n", "\n   "))

        return perfect_match

    finally:
        conn.close()


if __name__ == "__main__":
    # Run the complete test
    print("🚀 Other Non-Language Revenue Migration Test")
    print("=" * 50)

    # Test the migration
    success = test_other_non_language_migration("2024")

    if success:
        print("\n✅ Other Non-Language Migration Test Complete!")
        print("Ready for Overnight Shopping category!")
    else:
        print("\n❌ Migration test failed - investigate before proceeding!")
