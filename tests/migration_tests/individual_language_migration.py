"""
Individual Language Revenue Migration
====================================

This implements the Individual Language revenue category using BaseQueryBuilder.
Expected result: ~$2,424,212 according to your guide.

This category includes:
- Single language targeting (spans_multiple_blocks = 0)
- Has language block assignment (block_id IS NOT NULL)
- Excludes WorldLink
- Combines Mandarin + Cantonese as "Chinese"
"""

import sqlite3
from query_builders import (
    IndividualLanguageQueryBuilder,
    BaseQueryBuilder,
    validate_query_migration,
)


def get_individual_language_revenue(year="2024", db_connection=None):
    """
    Get individual language revenue breakdown

    Returns:
        dict: Language breakdown with revenue, spots, and bonus counts
    """
    builder = IndividualLanguageQueryBuilder(year)
    builder.add_individual_language_conditions()

    # Get the language summary
    query = builder.build_language_summary_query()

    cursor = db_connection.cursor()
    cursor.execute(query)

    results = []
    total_revenue = 0
    total_spots = 0
    total_bonus = 0

    for row in cursor.fetchall():
        language_data = {
            "language": row[0],
            "spots": row[1],
            "revenue": row[2],
            "bonus_spots": row[3],
        }
        results.append(language_data)
        total_revenue += row[2]
        total_spots += row[1]
        total_bonus += row[3]

    return {
        "languages": results,
        "total_revenue": total_revenue,
        "total_spots": total_spots,
        "total_bonus_spots": total_bonus,
    }


def get_individual_language_total_only(year="2024", db_connection=None):
    """Get just the total revenue for individual languages (faster)"""
    builder = IndividualLanguageQueryBuilder(year)
    builder.add_individual_language_conditions()

    result = builder.execute_revenue_query(db_connection)
    return result


def validate_individual_language_migration(db_connection, year="2024"):
    """
    Validate that our new Individual Language query matches the original

    This uses the exact query from your Revenue-Querying-By-Language-Guide.md
    """

    # Original query from your guide (modified to just get total)
    old_query = f"""
    SELECT SUM(COALESCE(s.gross_rate, 0)) as revenue
    FROM spots s
    JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN language_blocks lb ON slb.block_id = lb.block_id
    LEFT JOIN languages l ON lb.language_id = l.language_id
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    WHERE s.broadcast_month LIKE '%-{year[-2:]}'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND ((slb.spans_multiple_blocks = 0 AND slb.block_id IS NOT NULL) OR 
         (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NOT NULL))
    AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
    AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
    """

    # New query using builder
    builder = IndividualLanguageQueryBuilder(year)
    builder.add_individual_language_conditions()

    # Validate
    return validate_query_migration(old_query, builder, db_connection)


def test_individual_language_migration(year="2024"):
    """Complete test of Individual Language migration"""

    print(f"🧪 Testing Individual Language Migration for {year}")
    print("=" * 60)

    # Connect to database
    conn = sqlite3.connect("data/database/production.db")

    try:
        # 1. Validate migration
        print("\n1. Validating Migration:")
        if validate_individual_language_migration(conn, year):
            print("   ✅ Migration validation passed!")
        else:
            print("   ❌ Migration validation failed!")
            return False

        # 2. Get detailed results
        print("\n2. Individual Language Breakdown:")
        detailed_results = get_individual_language_revenue(year, conn)

        print(f"   {'Language':<15} {'Revenue':<15} {'Spots':<8} {'Bonus':<8} {'%':<8}")
        print("   " + "-" * 60)

        for lang_data in detailed_results["languages"]:
            pct = (lang_data["revenue"] / detailed_results["total_revenue"]) * 100
            print(
                f"   {lang_data['language']:<15} ${lang_data['revenue']:>12,.2f} "
                f"{lang_data['spots']:>6,} {lang_data['bonus_spots']:>6,} {pct:>6.1f}%"
            )

        print("   " + "-" * 60)
        print(
            f"   {'TOTAL':<15} ${detailed_results['total_revenue']:>12,.2f} "
            f"{detailed_results['total_spots']:>6,} {detailed_results['total_bonus_spots']:>6,} {'100.0%':>6}"
        )

        # 3. Compare with documented results
        print(f"\n3. Validation Against Your Guide:")
        documented_individual_total = 2424212.16  # From your guide
        difference = abs(
            detailed_results["total_revenue"] - documented_individual_total
        )

        print(f"   Expected (from guide): ${documented_individual_total:,.2f}")
        print(f"   Actual (new query): ${detailed_results['total_revenue']:,.2f}")
        print(f"   Difference: ${difference:,.2f}")

        if difference < 1.0:
            print(f"   ✅ PERFECT MATCH! (Difference < $1.00)")
        else:
            print(f"   ❌ DIFFERENCE FOUND! (Difference: ${difference:,.2f})")

        # 4. Show top languages
        print(f"\n4. Top Languages by Revenue:")
        top_languages = sorted(
            detailed_results["languages"], key=lambda x: x["revenue"], reverse=True
        )[:5]

        for i, lang in enumerate(top_languages, 1):
            pct = (lang["revenue"] / detailed_results["total_revenue"]) * 100
            print(f"   {i}. {lang['language']}: ${lang['revenue']:,.2f} ({pct:.1f}%)")

        # 5. Show generated query
        print(f"\n5. Generated Query:")
        builder = IndividualLanguageQueryBuilder(year)
        builder.add_individual_language_conditions()
        print("   " + builder.build_select_revenue_query().replace("\n", "\n   "))

        return True

    finally:
        conn.close()


def show_chinese_combination_effect(year="2024"):
    """Show how Mandarin + Cantonese combination affects results"""

    print(f"\n🔍 Chinese Language Combination Analysis for {year}")
    print("=" * 60)

    conn = sqlite3.connect("data/database/production.db")

    try:
        # Query without Chinese combination
        query_separate = f"""
        SELECT 
            COALESCE(l.language_name, 'Unknown Language') as language,
            COUNT(*) as spots,
            SUM(COALESCE(s.gross_rate, 0)) as revenue
        FROM spots s
        JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        LEFT JOIN language_blocks lb ON slb.block_id = lb.block_id
        LEFT JOIN languages l ON lb.language_id = l.language_id
        LEFT JOIN agencies a ON s.agency_id = a.agency_id
        WHERE s.broadcast_month LIKE '%-{year[-2:]}'
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        AND ((slb.spans_multiple_blocks = 0 AND slb.block_id IS NOT NULL) OR 
             (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NOT NULL))
        AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
        AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
        AND l.language_name IN ('Mandarin', 'Cantonese')
        GROUP BY l.language_name
        ORDER BY SUM(COALESCE(s.gross_rate, 0)) DESC
        """

        cursor = conn.cursor()
        cursor.execute(query_separate)
        separate_results = cursor.fetchall()

        print("   Separate Chinese Languages:")
        mandarin_total = 0
        cantonese_total = 0

        for row in separate_results:
            print(f"   {row[0]:<12}: ${row[2]:>12,.2f} ({row[1]:,} spots)")
            if row[0] == "Mandarin":
                mandarin_total = row[2]
            elif row[0] == "Cantonese":
                cantonese_total = row[2]

        combined_total = mandarin_total + cantonese_total
        print(f"   {'Combined':<12}: ${combined_total:>12,.2f}")

        # Show in context of full results
        full_results = get_individual_language_revenue(year, conn)
        chinese_from_full = next(
            (
                lang["revenue"]
                for lang in full_results["languages"]
                if lang["language"] == "Chinese"
            ),
            0,
        )

        print(f"\n   Validation:")
        print(f"   Manual calculation: ${combined_total:,.2f}")
        print(f"   Builder result: ${chinese_from_full:,.2f}")
        print(f"   Difference: ${abs(combined_total - chinese_from_full):,.2f}")

        if abs(combined_total - chinese_from_full) < 0.01:
            print(f"   ✅ Chinese combination working correctly!")
        else:
            print(f"   ❌ Chinese combination issue!")

    finally:
        conn.close()


if __name__ == "__main__":
    # Run the complete test
    print("🚀 Individual Language Revenue Migration Test")
    print("=" * 50)

    # Test the migration
    success = test_individual_language_migration("2024")

    if success:
        # Show Chinese combination effect
        show_chinese_combination_effect("2024")

        print(f"\n✅ Individual Language Migration Test Complete!")
        print(f"Ready to proceed with next category migration!")
    else:
        print(f"\n❌ Migration test failed - investigate before proceeding!")
