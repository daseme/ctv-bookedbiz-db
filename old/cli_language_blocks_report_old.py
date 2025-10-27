#!/usr/bin/env python3
"""
Enhanced Language Block Report with Business Intelligence
========================================================

Comprehensive report that includes business pattern recognition and
separates legitimate language targeting from broad-reach campaigns.
"""

import sqlite3
import os
from datetime import datetime
from collections import defaultdict


def generate_enhanced_language_block_report():
    """Generate enhanced language block report with business intelligence"""

    db_path = "data/database/production.db"
    if not os.path.exists(db_path):
        print("✗ Database not found. Run: python db_sync.py download")
        return False

    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row

        print("=" * 80)
        print("ENHANCED LANGUAGE BLOCK REPORT WITH BUSINESS INTELLIGENCE")
        print("=" * 80)
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()

        # 1. Overall Revenue Summary (ALL SPOTS)
        print("📊 OVERALL REVENUE SUMMARY")
        print("-" * 50)

        total_summary_query = """
        SELECT 
            COUNT(DISTINCT spot_id) as total_spots,
            SUM(gross_rate) as total_revenue,
            SUM(station_net) as total_net_revenue,
            COUNT(DISTINCT broadcast_month) as total_months,
            COUNT(DISTINCT customer_id) as unique_customers,
            COUNT(DISTINCT market_id) as total_markets,
            MIN(broadcast_month) as earliest_month,
            MAX(broadcast_month) as latest_month
        FROM spots
        WHERE (revenue_type != 'Trade' OR revenue_type IS NULL)
          AND gross_rate > 0
        """

        cursor = conn.execute(total_summary_query)
        summary = cursor.fetchone()

        if summary:
            print(f"Total Revenue: ${summary['total_revenue']:,.2f}")
            print(f"Total Spots: {summary['total_spots']:,}")
            print(
                f"Reporting Period: {summary['earliest_month']} to {summary['latest_month']}"
            )
            print(f"Active Markets: {summary['total_markets']}")
            print(f"Unique Customers: {summary['unique_customers']}")
            if summary["total_spots"] > 0:
                print(
                    f"Avg Revenue/Spot: ${summary['total_revenue'] / summary['total_spots']:,.2f}"
                )

        # 2. NEW: Business Pattern Analysis
        print(f"\n🧠 BUSINESS PATTERN ANALYSIS")
        print("-" * 40)

        # WorldLink Infomercials
        worldlink_query = """
        SELECT 
            COUNT(DISTINCT s.spot_id) as spots,
            SUM(s.gross_rate) as revenue,
            AVG(s.gross_rate) as avg_revenue
        FROM spots s
        WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
          AND s.gross_rate > 0
          AND s.bill_code LIKE '%WorldLink%'
        """

        cursor = conn.execute(worldlink_query)
        worldlink = cursor.fetchone()

        # Government Roadblocks
        gov_roadblock_query = """
        SELECT 
            COUNT(DISTINCT s.spot_id) as spots,
            SUM(s.gross_rate) as revenue,
            AVG(s.gross_rate) as avg_revenue
        FROM spots s
        LEFT JOIN customers c ON s.customer_id = c.customer_id
        WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
          AND s.gross_rate > 0
          AND c.normalized_name IN ('Department of Health', 'Cal Fire', 'California State Library', 'CA Community Colleges', 'Sacramento County Water Agency', 'CMS', 'Sac Yolo Mosquito')
          AND s.time_in = '6:00:00'
          AND s.time_out = '23:59:00'
        """

        cursor = conn.execute(gov_roadblock_query)
        gov_roadblock = cursor.fetchone()

        # Political Ads
        political_query = """
        SELECT 
            COUNT(DISTINCT s.spot_id) as spots,
            SUM(s.gross_rate) as revenue,
            AVG(s.gross_rate) as avg_revenue
        FROM spots s
        WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
          AND s.gross_rate > 0
          AND s.bill_code LIKE '%Intersection%'
        """

        cursor = conn.execute(political_query)
        political = cursor.fetchone()

        # Language-Targeted Spots
        language_targeted_query = """
        SELECT 
            COUNT(DISTINCT s.spot_id) as spots,
            SUM(s.gross_rate) as revenue,
            AVG(s.gross_rate) as avg_revenue
        FROM spots s
        JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        LEFT JOIN customers c ON s.customer_id = c.customer_id
        WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
          AND s.gross_rate > 0
          AND s.bill_code NOT LIKE '%WorldLink%'
          AND s.bill_code NOT LIKE '%Intersection%'
          AND NOT (
              c.normalized_name IN ('Department of Health', 'Cal Fire', 'California State Library', 'CA Community Colleges', 'Sacramento County Water Agency', 'CMS', 'Sac Yolo Mosquito')
              AND s.time_in = '6:00:00'
              AND s.time_out = '23:59:00'
          )
        """

        cursor = conn.execute(language_targeted_query)
        language_targeted = cursor.fetchone()

        # Unassigned Spots
        unassigned_query = """
        SELECT 
            COUNT(DISTINCT s.spot_id) as spots,
            SUM(s.gross_rate) as revenue,
            AVG(s.gross_rate) as avg_revenue
        FROM spots s
        LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        LEFT JOIN customers c ON s.customer_id = c.customer_id
        WHERE slb.spot_id IS NULL
          AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
          AND s.gross_rate > 0
          AND s.bill_code NOT LIKE '%WorldLink%'
          AND s.bill_code NOT LIKE '%Intersection%'
          AND NOT (
              c.normalized_name IN ('Department of Health', 'Cal Fire', 'California State Library', 'CA Community Colleges', 'Sacramento County Water Agency', 'CMS', 'Sac Yolo Mosquito')
              AND s.time_in = '6:00:00'
              AND s.time_out = '23:59:00'
          )
        """

        cursor = conn.execute(unassigned_query)
        unassigned = cursor.fetchone()

        # Display Business Pattern Analysis
        print("BUSINESS PATTERN BREAKDOWN:")
        print(
            f"{'Category':25} {'Spots':>10} {'Revenue':>15} {'Avg Rev':>10} {'Purpose':>20}"
        )
        print("-" * 90)

        if worldlink and worldlink["spots"]:
            print(
                f"{'WorldLink Infomercials':25} {worldlink['spots']:>10,} ${worldlink['revenue']:>14,.0f} ${worldlink['avg_revenue']:>9,.0f} {'Broad Reach DR':>20}"
            )

        if gov_roadblock and gov_roadblock["spots"]:
            print(
                f"{'Government Roadblocks':25} {gov_roadblock['spots']:>10,} ${gov_roadblock['revenue']:>14,.0f} ${gov_roadblock['avg_revenue']:>9,.0f} {'All Communities':>20}"
            )

        if political and political["spots"]:
            print(
                f"{'Political Campaigns':25} {political['spots']:>10,} ${political['revenue']:>14,.0f} ${political['avg_revenue']:>9,.0f} {'Broad Political':>20}"
            )

        if language_targeted and language_targeted["spots"]:
            print(
                f"{'Language-Targeted':25} {language_targeted['spots']:>10,} ${language_targeted['revenue']:>14,.0f} ${language_targeted['avg_revenue']:>9,.0f} {'Specific Language':>20}"
            )

        if unassigned and unassigned["spots"]:
            print(
                f"{'Unassigned/Grid Gaps':25} {unassigned['spots']:>10,} ${unassigned['revenue']:>14,.0f} ${unassigned['avg_revenue']:>9,.0f} {'Needs Assignment':>20}"
            )

        # Calculate total for percentage
        total_spots = summary["total_spots"]
        total_revenue = summary["total_revenue"]

        print(f"\n{'PERCENTAGE BREAKDOWN':25} {'% Spots':>10} {'% Revenue':>15}")
        print("-" * 55)

        if worldlink and worldlink["spots"]:
            spots_pct = (worldlink["spots"] / total_spots) * 100
            revenue_pct = (worldlink["revenue"] / total_revenue) * 100
            print(
                f"{'WorldLink Infomercials':25} {spots_pct:>9.1f}% {revenue_pct:>14.1f}%"
            )

        if gov_roadblock and gov_roadblock["spots"]:
            spots_pct = (gov_roadblock["spots"] / total_spots) * 100
            revenue_pct = (gov_roadblock["revenue"] / total_revenue) * 100
            print(
                f"{'Government Roadblocks':25} {spots_pct:>9.1f}% {revenue_pct:>14.1f}%"
            )

        if political and political["spots"]:
            spots_pct = (political["spots"] / total_spots) * 100
            revenue_pct = (political["revenue"] / total_revenue) * 100
            print(
                f"{'Political Campaigns':25} {spots_pct:>9.1f}% {revenue_pct:>14.1f}%"
            )

        if language_targeted and language_targeted["spots"]:
            spots_pct = (language_targeted["spots"] / total_spots) * 100
            revenue_pct = (language_targeted["revenue"] / total_revenue) * 100
            print(f"{'Language-Targeted':25} {spots_pct:>9.1f}% {revenue_pct:>14.1f}%")

        if unassigned and unassigned["spots"]:
            spots_pct = (unassigned["spots"] / total_spots) * 100
            revenue_pct = (unassigned["revenue"] / total_revenue) * 100
            print(
                f"{'Unassigned/Grid Gaps':25} {spots_pct:>9.1f}% {revenue_pct:>14.1f}%"
            )

        # 3. NEW: Edge Case Status Analysis
        print(f"\n🚨 EDGE CASE STATUS ANALYSIS")
        print("-" * 40)

        edge_case_status_query = """
        SELECT 
            CASE 
                WHEN slb.alert_reason LIKE '%WORLDLINK%' THEN 'WorldLink Resolved'
                WHEN slb.alert_reason LIKE '%GOVERNMENT%' THEN 'Government Resolved'
                WHEN slb.alert_reason LIKE '%POLITICAL%' THEN 'Political Resolved'
                WHEN slb.requires_attention = 1 THEN 'Requires Attention'
                WHEN slb.requires_attention = 0 THEN 'Standard Assignment'
                ELSE 'No Assignment'
            END as status,
            COUNT(DISTINCT s.spot_id) as spots,
            SUM(s.gross_rate) as revenue
        FROM spots s
        LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
          AND s.gross_rate > 0
        GROUP BY status
        ORDER BY spots DESC
        """

        cursor = conn.execute(edge_case_status_query)
        edge_cases = cursor.fetchall()

        print("EDGE CASE STATUS:")
        print(f"{'Status':25} {'Spots':>10} {'Revenue':>15}")
        print("-" * 55)
        for case in edge_cases:
            print(
                f"{case['status']:25} {case['spots']:>10,} ${case['revenue']:>14,.0f}"
            )

        # 4. Language-Specific Performance (Real Language Targeting)
        print(f"\n🌐 LANGUAGE-SPECIFIC PERFORMANCE")
        print("-" * 45)

        language_performance_query = """
        SELECT 
            l.language_name,
            COUNT(DISTINCT s.spot_id) as language_targeted_spots,
            SUM(s.gross_rate) as language_targeted_revenue,
            AVG(s.gross_rate) as avg_revenue,
            COUNT(DISTINCT s.customer_id) as unique_customers
        FROM spots s
        JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        JOIN languages l ON s.language_id = l.language_id
        LEFT JOIN customers c ON s.customer_id = c.customer_id
        WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
          AND s.gross_rate > 0
          AND s.bill_code NOT LIKE '%WorldLink%'
          AND s.bill_code NOT LIKE '%Intersection%'
          AND NOT (
              c.normalized_name IN ('Department of Health', 'Cal Fire', 'California State Library', 'CA Community Colleges', 'Sacramento County Water Agency', 'CMS', 'Sac Yolo Mosquito')
              AND s.time_in = '6:00:00'
              AND s.time_out = '23:59:00'
          )
        GROUP BY l.language_name
        ORDER BY language_targeted_revenue DESC
        """

        cursor = conn.execute(language_performance_query)
        lang_performance = cursor.fetchall()

        if lang_performance:
            print(
                f"{'Language':15} {'Spots':>8} {'Revenue':>12} {'Avg Rev':>10} {'Customers':>10}"
            )
            print("-" * 70)
            for lang in lang_performance:
                print(
                    f"{lang['language_name']:15} {lang['language_targeted_spots']:>8,} ${lang['language_targeted_revenue']:>11,.0f} ${lang['avg_revenue']:>9,.0f} {lang['unique_customers']:>10}"
                )

        # 5. Top Language Blocks (Real Performance)
        print(f"\n🏆 TOP PERFORMING LANGUAGE BLOCKS")
        print("-" * 45)

        block_performance_query = """
        SELECT 
            s.block_name,
            s.block_language_name,
            s.market_display_name,
            s.day_part,
            s.block_time_start || '-' || s.block_time_end as time_slot,
            COUNT(DISTINCT s.spot_id) as total_spots,
            SUM(s.gross_rate) as total_revenue,
            COUNT(DISTINCT s.customer_name) as unique_customers
        FROM spots_with_language_blocks_enhanced s
        WHERE s.gross_rate > 0 
          AND s.block_id IS NOT NULL
          AND s.bill_code NOT LIKE '%WorldLink%'
          AND s.bill_code NOT LIKE '%Intersection%'
        GROUP BY s.block_id, s.block_name, s.block_language_name, s.market_display_name
        ORDER BY total_revenue DESC
        LIMIT 10
        """

        cursor = conn.execute(block_performance_query)
        top_blocks = cursor.fetchall()

        if top_blocks:
            for i, block in enumerate(top_blocks, 1):
                print(f"{i:2d}. {block['block_name']} ({block['block_language_name']})")
                print(
                    f"     Market: {block['market_display_name']} | Time: {block['day_part']} {block['time_slot']}"
                )
                print(
                    f"     Revenue: ${block['total_revenue']:,.0f} | Spots: {block['total_spots']:,} | Customers: {block['unique_customers']}"
                )
                print()

        # 6. Programming Grid Analysis
        print(f"\n📅 PROGRAMMING GRID ANALYSIS")
        print("-" * 35)

        # Grid coverage analysis
        grid_coverage_query = """
        SELECT 
            CASE 
                WHEN s.time_in < '06:00:00' THEN 'Early Morning (Before 6AM)'
                WHEN s.time_out > '23:30:00' THEN 'Late Night (After 11:30PM)'
                WHEN s.time_in >= '06:00:00' AND s.time_out <= '23:30:00' THEN 'Standard Grid Hours'
                ELSE 'Mixed/Complex'
            END as time_category,
            COUNT(DISTINCT s.spot_id) as spots,
            SUM(s.gross_rate) as revenue
        FROM spots s
        LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        WHERE slb.spot_id IS NULL
          AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
          AND s.gross_rate > 0
        GROUP BY time_category
        ORDER BY spots DESC
        """

        cursor = conn.execute(grid_coverage_query)
        grid_coverage = cursor.fetchall()

        if grid_coverage:
            print("UNASSIGNED SPOTS BY TIME CATEGORY:")
            print(f"{'Time Category':30} {'Spots':>8} {'Revenue':>12}")
            print("-" * 55)
            for category in grid_coverage:
                print(
                    f"{category['time_category']:30} {category['spots']:>8,} ${category['revenue']:>11,.0f}"
                )

        # 7. Customer Intelligence
        print(f"\n👥 CUSTOMER INTELLIGENCE")
        print("-" * 30)

        # Top customers by category
        language_targeted_customers_query = """
        SELECT 
            c.normalized_name as customer_name,
            COUNT(DISTINCT s.spot_id) as spots,
            SUM(s.gross_rate) as revenue,
            COUNT(DISTINCT l.language_name) as languages_used
        FROM spots s
        JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        JOIN customers c ON s.customer_id = c.customer_id
        JOIN languages l ON s.language_id = l.language_id
        WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
          AND s.gross_rate > 0
          AND s.bill_code NOT LIKE '%WorldLink%'
          AND s.bill_code NOT LIKE '%Intersection%'
          AND NOT (
              c.normalized_name IN ('Department of Health', 'Cal Fire', 'California State Library', 'CA Community Colleges', 'Sacramento County Water Agency', 'CMS', 'Sac Yolo Mosquito')
              AND s.time_in = '6:00:00'
              AND s.time_out = '23:59:00'
          )
        GROUP BY c.normalized_name
        ORDER BY revenue DESC
        LIMIT 15
        """

        cursor = conn.execute(language_targeted_customers_query)
        targeted_customers = cursor.fetchall()

        if targeted_customers:
            print("TOP LANGUAGE-TARGETED CUSTOMERS:")
            print(f"{'Customer':30} {'Spots':>8} {'Revenue':>12} {'Languages':>10}")
            print("-" * 70)
            for customer in targeted_customers:
                print(
                    f"{customer['customer_name'][:30]:30} {customer['spots']:>8,} ${customer['revenue']:>11,.0f} {customer['languages_used']:>10}"
                )

        # 8. Key Insights (Updated)
        print(f"\n💡 KEY INSIGHTS & STRATEGIC RECOMMENDATIONS")
        print("-" * 50)

        # Calculate real penetration
        real_penetration_query = """
        SELECT 
            COUNT(DISTINCT CASE WHEN slb.spot_id IS NOT NULL THEN s.spot_id END) as assigned_spots,
            COUNT(DISTINCT s.spot_id) as total_spots,
            SUM(CASE WHEN slb.spot_id IS NOT NULL THEN s.gross_rate ELSE 0 END) as assigned_revenue,
            SUM(s.gross_rate) as total_revenue
        FROM spots s
        LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        LEFT JOIN customers c ON s.customer_id = c.customer_id
        WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
          AND s.gross_rate > 0
          AND s.bill_code NOT LIKE '%WorldLink%'
          AND s.bill_code NOT LIKE '%Intersection%'
          AND NOT (
              c.normalized_name IN ('Department of Health', 'Cal Fire', 'California State Library', 'CA Community Colleges', 'Sacramento County Water Agency', 'CMS', 'Sac Yolo Mosquito')
              AND s.time_in = '6:00:00'
              AND s.time_out = '23:59:00'
          )
        """

        cursor = conn.execute(real_penetration_query)
        real_penetration = cursor.fetchone()

        if real_penetration:
            assignment_rate = (
                real_penetration["assigned_spots"] / real_penetration["total_spots"]
            ) * 100
            revenue_rate = (
                real_penetration["assigned_revenue"] / real_penetration["total_revenue"]
            ) * 100
            unassigned_revenue = (
                real_penetration["total_revenue"] - real_penetration["assigned_revenue"]
            )

            print("🎯 REAL LANGUAGE TARGETING METRICS:")
            print(f"   • Assignment Rate: {assignment_rate:.1f}% of legitimate spots")
            print(f"   • Revenue Coverage: {revenue_rate:.1f}% of targetable revenue")
            print(
                f"   • Growth Opportunity: ${unassigned_revenue:,.0f} in unassigned revenue"
            )
            print()

        print("📈 BUSINESS INTELLIGENCE SUMMARY:")
        print(
            "   • Broad-reach campaigns (infomercials, roadblocks, political) work by design"
        )
        print("   • Language targeting focuses on specific customer segments")
        print("   • Programming grid gaps represent expansion opportunities")
        print("   • Customer behavior varies significantly by campaign type")
        print()

        print("🎯 STRATEGIC RECOMMENDATIONS:")
        print("   • Implement business rule automation for known patterns")
        print("   • Focus language block expansion on high-value targeted customers")
        print("   • Analyze programming grid gaps for premium time slot opportunities")
        print("   • Develop customer-specific assignment strategies")
        print("   • Create business intelligence dashboards for campaign type analysis")

        print(f"\n{'=' * 80}")
        print("✓ Enhanced Language Block Report Complete!")
        print("🧠 This report now includes business intelligence insights")
        print("🚀 Ready for strategic decision-making and automation")
        print(f"{'=' * 80}")

        return True

    except Exception as e:
        print(f"✗ Error generating report: {e}")
        import traceback

        print(traceback.format_exc())
        return False
    finally:
        if "conn" in locals():
            conn.close()


if __name__ == "__main__":
    generate_enhanced_language_block_report()
