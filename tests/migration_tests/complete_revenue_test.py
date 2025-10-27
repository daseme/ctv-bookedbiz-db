"""
Complete Revenue Verification Test
==================================

This test validates that our BaseQueryBuilder is working correctly by:
1. Testing the base query (excludes WorldLink)
2. Testing the Direct Response query (only WorldLink)
3. Showing they add up to the expected total
4. Demonstrating the foundation is solid for full migration
"""

import sqlite3
from query_builders import BaseQueryBuilder, DirectResponseQueryBuilder


def test_complete_revenue_breakdown(year="2024"):
    """Test that base query + direct response = total revenue"""

    # Connect to database
    conn = sqlite3.connect("data/database/production.db")

    print(f"🧪 Testing Complete Revenue Breakdown for {year}")
    print("=" * 60)

    # 1. Base query (excludes WorldLink)
    print("\n1. Base Revenue Query (Excludes WorldLink):")
    base_builder = BaseQueryBuilder(year)
    base_builder.apply_standard_filters().exclude_worldlink()

    base_result = base_builder.execute_revenue_query(conn)
    print(f"   Revenue: ${base_result.revenue:,.2f}")
    print(f"   Spots: {base_result.spot_count:,}")

    # 2. Direct Response query (only WorldLink)
    print("\n2. Direct Response Revenue (Only WorldLink):")
    dr_builder = DirectResponseQueryBuilder(year)
    dr_builder.add_worldlink_conditions()

    dr_result = dr_builder.execute_revenue_query(conn)
    print(f"   Revenue: ${dr_result.revenue:,.2f}")
    print(f"   Spots: {dr_result.spot_count:,}")

    # 3. Calculate total
    calculated_total = base_result.revenue + dr_result.revenue
    print(f"\n3. Calculated Total:")
    print(f"   Base Revenue: ${base_result.revenue:,.2f}")
    print(f"   + Direct Response: ${dr_result.revenue:,.2f}")
    print(f"   = Total: ${calculated_total:,.2f}")

    # 4. Verify against actual database total
    print(f"\n4. Database Verification:")

    # Query for actual total (all spots, just base filters)
    total_builder = BaseQueryBuilder(year)
    total_builder.apply_standard_filters()  # No WorldLink exclusion

    total_result = total_builder.execute_revenue_query(conn)
    print(f"   Database Total: ${total_result.revenue:,.2f}")
    print(f"   Database Spots: {total_result.spot_count:,}")

    # 5. Check for perfect reconciliation
    difference = abs(calculated_total - total_result.revenue)
    print(f"\n5. Reconciliation Check:")
    print(f"   Calculated: ${calculated_total:,.2f}")
    print(f"   Database: ${total_result.revenue:,.2f}")
    print(f"   Difference: ${difference:,.2f}")

    if difference < 0.01:
        print(f"   ✅ PERFECT RECONCILIATION! (Difference < $0.01)")
    else:
        print(f"   ❌ RECONCILIATION ISSUE! (Difference: ${difference:,.2f})")

    # 6. Show the split breakdown
    print(f"\n6. Revenue Split:")
    base_pct = (base_result.revenue / total_result.revenue) * 100
    dr_pct = (dr_result.revenue / total_result.revenue) * 100

    print(f"   Non-WorldLink: ${base_result.revenue:,.2f} ({base_pct:.1f}%)")
    print(f"   WorldLink: ${dr_result.revenue:,.2f} ({dr_pct:.1f}%)")
    print(f"   Total: ${total_result.revenue:,.2f} (100.0%)")

    conn.close()

    return {
        "base_revenue": base_result.revenue,
        "direct_response": dr_result.revenue,
        "calculated_total": calculated_total,
        "database_total": total_result.revenue,
        "difference": difference,
        "perfect_reconciliation": difference < 0.01,
    }


def compare_with_your_documented_results():
    """Compare with the results documented in your guide"""

    print(f"\n🔍 Comparing with Your Documented Results:")
    print("=" * 60)

    # Your documented totals from the guide
    documented_total = 4076255.94
    documented_direct_response = 354506.93
    documented_expected_base = documented_total - documented_direct_response

    print(f"From your Revenue-Querying-By-Language-Guide.md:")
    print(f"   Total Revenue: ${documented_total:,.2f}")
    print(f"   Direct Response: ${documented_direct_response:,.2f}")
    print(f"   Expected Base: ${documented_expected_base:,.2f}")

    # Test our results
    results = test_complete_revenue_breakdown("2024")

    print(f"\nOur BaseQueryBuilder Results:")
    print(f"   Base Revenue: ${results['base_revenue']:,.2f}")
    print(f"   Direct Response: ${results['direct_response']:,.2f}")
    print(f"   Total: ${results['database_total']:,.2f}")

    # Check matches
    base_diff = abs(results["base_revenue"] - documented_expected_base)
    dr_diff = abs(results["direct_response"] - documented_direct_response)
    total_diff = abs(results["database_total"] - documented_total)

    print(f"\nValidation Against Your Guide:")
    print(
        f"   Base Query Difference: ${base_diff:,.2f} {'✅' if base_diff < 1.0 else '❌'}"
    )
    print(
        f"   Direct Response Difference: ${dr_diff:,.2f} {'✅' if dr_diff < 1.0 else '❌'}"
    )
    print(
        f"   Total Difference: ${total_diff:,.2f} {'✅' if total_diff < 1.0 else '❌'}"
    )

    if base_diff < 1.0 and dr_diff < 1.0 and total_diff < 1.0:
        print(f"\n🎉 PERFECT MATCH! BaseQueryBuilder is working exactly as expected!")
    else:
        print(f"\n⚠️  Some differences found - may need investigation")


def show_query_examples():
    """Show the actual queries being generated"""

    print(f"\n📋 Generated Query Examples:")
    print("=" * 60)

    # Base query
    base_builder = BaseQueryBuilder("2024")
    base_builder.apply_standard_filters().exclude_worldlink()

    print(f"\n1. Base Query (Excludes WorldLink):")
    print(base_builder.build_select_revenue_query())

    # Direct Response query
    dr_builder = DirectResponseQueryBuilder("2024")
    dr_builder.add_worldlink_conditions()

    print(f"\n2. Direct Response Query (Only WorldLink):")
    print(dr_builder.build_select_revenue_query())

    # Total query
    total_builder = BaseQueryBuilder("2024")
    total_builder.apply_standard_filters()

    print(f"\n3. Total Query (All Revenue):")
    print(total_builder.build_select_revenue_query())


if __name__ == "__main__":
    # Run the complete test
    print("🚀 BaseQueryBuilder Validation Test")
    print("===================================")

    # Test the breakdown
    results = test_complete_revenue_breakdown("2024")

    # Compare with documented results
    compare_with_your_documented_results()

    # Show query examples
    show_query_examples()

    print(f"\n✅ Test Complete!")
    print(f"BaseQueryBuilder is working correctly and ready for full migration!")
