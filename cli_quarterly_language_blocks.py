#!/usr/bin/env python3
"""
2024 vs 2025 Quarterly Language Block Report
Focused analysis comparing 2024 and 2025 performance by quarters
"""

import sqlite3
import os
from datetime import datetime
from collections import defaultdict

def generate_quarterly_language_block_report():
    """Generate quarterly comparison report for 2024 vs 2025"""
    
    db_path = "data/database/production.db"
    if not os.path.exists(db_path):
        print("✗ Database not found. Run: python db_sync.py download")
        return False
    
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        
        print("="*90)
        print("2024 vs 2025 QUARTERLY LANGUAGE BLOCK REPORT")
        print("="*90)
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # 1. Overall 2024 vs 2025 Summary
        print("📊 2024 vs 2025 SUMMARY")
        print("-" * 40)
        
        yearly_summary_query = """
        SELECT 
            CASE 
                WHEN s.broadcast_month LIKE '2024%' THEN '2024'
                WHEN s.broadcast_month LIKE '2025%' THEN '2025'
            END as year,
            COUNT(DISTINCT s.spot_id) as total_spots,
            SUM(s.gross_rate) as total_revenue,
            SUM(s.station_net) as total_net_revenue,
            COUNT(DISTINCT s.customer_id) as unique_customers,
            COUNT(DISTINCT s.market_id) as markets_active,
            -- Language block stats
            COUNT(DISTINCT CASE WHEN slb.spot_id IS NOT NULL THEN s.spot_id END) as spots_with_blocks,
            SUM(CASE WHEN slb.spot_id IS NOT NULL THEN s.gross_rate ELSE 0 END) as revenue_with_blocks,
            COUNT(DISTINCT s.broadcast_month) as active_months
        FROM spots s
        LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL) 
          AND s.gross_rate > 0 
          AND s.broadcast_month IS NOT NULL
          AND (s.broadcast_month LIKE '2024%' OR s.broadcast_month LIKE '2025%')
        GROUP BY year
        ORDER BY year
        """
        
        cursor = conn.execute(yearly_summary_query)
        yearly_data = cursor.fetchall()
        
        if yearly_data:
            print(f"{'Year':6} {'Total Rev':>12} {'Block Rev':>12} {'Block %':>8} {'Spots':>10} {'Customers':>10} {'Markets':>8} {'Months':>7}")
            print("-" * 85)
            for year in yearly_data:
                block_pct = (year['revenue_with_blocks'] / year['total_revenue'] * 100) if year['total_revenue'] > 0 else 0
                print(f"{year['year']:6} ${year['total_revenue']:>11,.0f} ${year['revenue_with_blocks']:>11,.0f} {block_pct:>7.1f}% {year['total_spots']:>10,} {year['unique_customers']:>10} {year['markets_active']:>8} {year['active_months']:>7}")
        
        # 2. Quarterly Breakdown
        print(f"\n📅 QUARTERLY PERFORMANCE COMPARISON")
        print("-" * 50)
        
        quarterly_query = """
        SELECT 
            CASE 
                WHEN s.broadcast_month LIKE '2024%' THEN '2024'
                WHEN s.broadcast_month LIKE '2025%' THEN '2025'
            END as year,
            CASE 
                WHEN s.broadcast_month LIKE '%-01-%' OR s.broadcast_month LIKE '%-02-%' OR s.broadcast_month LIKE '%-03-%' THEN 'Q1'
                WHEN s.broadcast_month LIKE '%-04-%' OR s.broadcast_month LIKE '%-05-%' OR s.broadcast_month LIKE '%-06-%' THEN 'Q2'
                WHEN s.broadcast_month LIKE '%-07-%' OR s.broadcast_month LIKE '%-08-%' OR s.broadcast_month LIKE '%-09-%' THEN 'Q3'
                WHEN s.broadcast_month LIKE '%-10-%' OR s.broadcast_month LIKE '%-11-%' OR s.broadcast_month LIKE '%-12-%' THEN 'Q4'
            END as quarter,
            COUNT(DISTINCT s.spot_id) as quarterly_spots,
            SUM(s.gross_rate) as quarterly_revenue,
            SUM(s.station_net) as quarterly_net_revenue,
            COUNT(DISTINCT s.customer_id) as quarterly_customers,
            -- Language block stats
            COUNT(DISTINCT CASE WHEN slb.spot_id IS NOT NULL THEN s.spot_id END) as spots_with_blocks,
            SUM(CASE WHEN slb.spot_id IS NOT NULL THEN s.gross_rate ELSE 0 END) as revenue_with_blocks,
            COUNT(DISTINCT s.broadcast_month) as months_in_quarter
        FROM spots s
        LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL) 
          AND s.gross_rate > 0 
          AND s.broadcast_month IS NOT NULL
          AND (s.broadcast_month LIKE '2024%' OR s.broadcast_month LIKE '2025%')
        GROUP BY year, quarter
        ORDER BY year, quarter
        """
        
        cursor = conn.execute(quarterly_query)
        quarterly_data = cursor.fetchall()
        
        if quarterly_data:
            print(f"{'Year':5} {'Qtr':4} {'Total Rev':>12} {'Block Rev':>12} {'Block %':>8} {'Spots':>8} {'Customers':>10} {'Months':>7}")
            print("-" * 80)
            
            # Group by year for better readability
            for year_group in ['2024', '2025']:
                year_quarters = [q for q in quarterly_data if q['year'] == year_group]
                if year_quarters:
                    print(f"\n{year_group} Performance:")
                    for quarter in year_quarters:
                        block_pct = (quarter['revenue_with_blocks'] / quarter['quarterly_revenue'] * 100) if quarter['quarterly_revenue'] > 0 else 0
                        print(f"  {quarter['quarter']:4} ${quarter['quarterly_revenue']:>11,.0f} ${quarter['revenue_with_blocks']:>11,.0f} {block_pct:>7.1f}% {quarter['quarterly_spots']:>8,} {quarter['quarterly_customers']:>10} {quarter['months_in_quarter']:>7}")
        
        # 3. Quarter-over-Quarter Growth Analysis
        print(f"\n📈 QUARTER-OVER-QUARTER GROWTH ANALYSIS")
        print("-" * 45)
        
        # Calculate Q/Q growth rates
        growth_query = """
        WITH quarterly_summary AS (
            SELECT 
                CASE 
                    WHEN s.broadcast_month LIKE '2024%' THEN '2024'
                    WHEN s.broadcast_month LIKE '2025%' THEN '2025'
                END as year,
                CASE 
                    WHEN s.broadcast_month LIKE '%-01-%' OR s.broadcast_month LIKE '%-02-%' OR s.broadcast_month LIKE '%-03-%' THEN 'Q1'
                    WHEN s.broadcast_month LIKE '%-04-%' OR s.broadcast_month LIKE '%-05-%' OR s.broadcast_month LIKE '%-06-%' THEN 'Q2'
                    WHEN s.broadcast_month LIKE '%-07-%' OR s.broadcast_month LIKE '%-08-%' OR s.broadcast_month LIKE '%-09-%' THEN 'Q3'
                    WHEN s.broadcast_month LIKE '%-10-%' OR s.broadcast_month LIKE '%-11-%' OR s.broadcast_month LIKE '%-12-%' THEN 'Q4'
                END as quarter,
                SUM(s.gross_rate) as quarterly_revenue,
                SUM(CASE WHEN slb.spot_id IS NOT NULL THEN s.gross_rate ELSE 0 END) as quarterly_block_revenue
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL) 
              AND s.gross_rate > 0 
              AND s.broadcast_month IS NOT NULL
              AND (s.broadcast_month LIKE '2024%' OR s.broadcast_month LIKE '2025%')
            GROUP BY year, quarter
        )
        SELECT 
            current.year || '-' || current.quarter as period,
            current.quarterly_revenue,
            current.quarterly_block_revenue,
            CASE 
                WHEN prior.quarterly_revenue > 0 
                THEN ROUND(((current.quarterly_revenue - prior.quarterly_revenue) / prior.quarterly_revenue * 100), 1)
                ELSE NULL
            END as revenue_growth_pct,
            CASE 
                WHEN prior.quarterly_block_revenue > 0 
                THEN ROUND(((current.quarterly_block_revenue - prior.quarterly_block_revenue) / prior.quarterly_block_revenue * 100), 1)
                ELSE NULL
            END as block_revenue_growth_pct
        FROM quarterly_summary current
        LEFT JOIN quarterly_summary prior ON (
            (current.year = '2024' AND current.quarter = 'Q2' AND prior.year = '2024' AND prior.quarter = 'Q1') OR
            (current.year = '2024' AND current.quarter = 'Q3' AND prior.year = '2024' AND prior.quarter = 'Q2') OR
            (current.year = '2024' AND current.quarter = 'Q4' AND prior.year = '2024' AND prior.quarter = 'Q3') OR
            (current.year = '2025' AND current.quarter = 'Q1' AND prior.year = '2024' AND prior.quarter = 'Q4') OR
            (current.year = '2025' AND current.quarter = 'Q2' AND prior.year = '2025' AND prior.quarter = 'Q1') OR
            (current.year = '2025' AND current.quarter = 'Q3' AND prior.year = '2025' AND prior.quarter = 'Q2') OR
            (current.year = '2025' AND current.quarter = 'Q4' AND prior.year = '2025' AND prior.quarter = 'Q3')
        )
        ORDER BY current.year, current.quarter
        """
        
        cursor = conn.execute(growth_query)
        growth_data = cursor.fetchall()
        
        if growth_data:
            print(f"{'Period':8} {'Revenue':>12} {'Block Rev':>12} {'Rev Growth':>12} {'Block Growth':>12}")
            print("-" * 70)
            for period in growth_data:
                rev_growth = f"{period['revenue_growth_pct']:+.1f}%" if period['revenue_growth_pct'] is not None else "N/A"
                block_growth = f"{period['block_revenue_growth_pct']:+.1f}%" if period['block_revenue_growth_pct'] is not None else "N/A"
                print(f"{period['period']:8} ${period['quarterly_revenue']:>11,.0f} ${period['quarterly_block_revenue']:>11,.0f} {rev_growth:>12} {block_growth:>12}")
        
        # 4. Language Performance by Year
        print(f"\n🌐 LANGUAGE PERFORMANCE: 2024 vs 2025")
        print("-" * 45)
        
        language_comparison_query = """
        SELECT 
            l.language_name,
            -- 2024 Performance
            SUM(CASE WHEN s.broadcast_month LIKE '2024%' THEN s.gross_rate ELSE 0 END) as revenue_2024,
            SUM(CASE WHEN s.broadcast_month LIKE '2024%' AND slb.spot_id IS NOT NULL THEN s.gross_rate ELSE 0 END) as block_revenue_2024,
            COUNT(DISTINCT CASE WHEN s.broadcast_month LIKE '2024%' THEN s.spot_id END) as spots_2024,
            -- 2025 Performance  
            SUM(CASE WHEN s.broadcast_month LIKE '2025%' THEN s.gross_rate ELSE 0 END) as revenue_2025,
            SUM(CASE WHEN s.broadcast_month LIKE '2025%' AND slb.spot_id IS NOT NULL THEN s.gross_rate ELSE 0 END) as block_revenue_2025,
            COUNT(DISTINCT CASE WHEN s.broadcast_month LIKE '2025%' THEN s.spot_id END) as spots_2025
        FROM spots s
        LEFT JOIN languages l ON s.language_id = l.language_id
        LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL) 
          AND s.gross_rate > 0
          AND l.language_name IS NOT NULL
          AND (s.broadcast_month LIKE '2024%' OR s.broadcast_month LIKE '2025%')
        GROUP BY l.language_name
        HAVING (revenue_2024 > 10000 OR revenue_2025 > 10000)
        ORDER BY (revenue_2024 + revenue_2025) DESC
        """
        
        cursor = conn.execute(language_comparison_query)
        language_data = cursor.fetchall()
        
        if language_data:
            print(f"{'Language':15} {'2024 Rev':>12} {'2024 Block%':>11} {'2025 Rev':>12} {'2025 Block%':>11} {'YoY Growth':>12}")
            print("-" * 90)
            for lang in language_data:
                block_pct_2024 = (lang['block_revenue_2024'] / lang['revenue_2024'] * 100) if lang['revenue_2024'] > 0 else 0
                block_pct_2025 = (lang['block_revenue_2025'] / lang['revenue_2025'] * 100) if lang['revenue_2025'] > 0 else 0
                
                # Calculate year-over-year growth
                if lang['revenue_2024'] > 0 and lang['revenue_2025'] > 0:
                    yoy_growth = ((lang['revenue_2025'] - lang['revenue_2024']) / lang['revenue_2024'] * 100)
                    yoy_display = f"{yoy_growth:+.1f}%"
                else:
                    yoy_display = "N/A"
                
                print(f"{lang['language_name']:15} ${lang['revenue_2024']:>11,.0f} {block_pct_2024:>10.1f}% ${lang['revenue_2025']:>11,.0f} {block_pct_2025:>10.1f}% {yoy_display:>12}")
        
        # 5. Top Customers by Year
        print(f"\n👥 TOP CUSTOMERS: 2024 vs 2025")
        print("-" * 35)
        
        customer_comparison_query = """
        SELECT 
            c.normalized_name as customer_name,
            -- 2024 Performance
            SUM(CASE WHEN s.broadcast_month LIKE '2024%' THEN s.gross_rate ELSE 0 END) as revenue_2024,
            SUM(CASE WHEN s.broadcast_month LIKE '2024%' AND slb.spot_id IS NOT NULL THEN s.gross_rate ELSE 0 END) as block_revenue_2024,
            -- 2025 Performance  
            SUM(CASE WHEN s.broadcast_month LIKE '2025%' THEN s.gross_rate ELSE 0 END) as revenue_2025,
            SUM(CASE WHEN s.broadcast_month LIKE '2025%' AND slb.spot_id IS NOT NULL THEN s.gross_rate ELSE 0 END) as block_revenue_2025
        FROM spots s
        LEFT JOIN customers c ON s.customer_id = c.customer_id
        LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL) 
          AND s.gross_rate > 0
          AND c.normalized_name IS NOT NULL
          AND (s.broadcast_month LIKE '2024%' OR s.broadcast_month LIKE '2025%')
        GROUP BY c.normalized_name
        HAVING (revenue_2024 + revenue_2025) > 50000
        ORDER BY (revenue_2024 + revenue_2025) DESC
        LIMIT 15
        """
        
        cursor = conn.execute(customer_comparison_query)
        customer_data = cursor.fetchall()
        
        if customer_data:
            print(f"{'Customer':25} {'2024 Rev':>12} {'2024 Block%':>11} {'2025 Rev':>12} {'2025 Block%':>11}")
            print("-" * 80)
            for customer in customer_data:
                block_pct_2024 = (customer['block_revenue_2024'] / customer['revenue_2024'] * 100) if customer['revenue_2024'] > 0 else 0
                block_pct_2025 = (customer['block_revenue_2025'] / customer['revenue_2025'] * 100) if customer['revenue_2025'] > 0 else 0
                
                print(f"{customer['customer_name'][:24]:25} ${customer['revenue_2024']:>11,.0f} {block_pct_2024:>10.1f}% ${customer['revenue_2025']:>11,.0f} {block_pct_2025:>10.1f}%")
        
        # 6. Market Performance by Year
        print(f"\n🏢 MARKET PERFORMANCE: 2024 vs 2025")
        print("-" * 40)
        
        market_comparison_query = """
        SELECT 
            m.market_code,
            m.market_name,
            -- 2024 Performance
            SUM(CASE WHEN s.broadcast_month LIKE '2024%' THEN s.gross_rate ELSE 0 END) as revenue_2024,
            SUM(CASE WHEN s.broadcast_month LIKE '2024%' AND slb.spot_id IS NOT NULL THEN s.gross_rate ELSE 0 END) as block_revenue_2024,
            -- 2025 Performance  
            SUM(CASE WHEN s.broadcast_month LIKE '2025%' THEN s.gross_rate ELSE 0 END) as revenue_2025,
            SUM(CASE WHEN s.broadcast_month LIKE '2025%' AND slb.spot_id IS NOT NULL THEN s.gross_rate ELSE 0 END) as block_revenue_2025
        FROM spots s
        LEFT JOIN markets m ON s.market_id = m.market_id
        LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL) 
          AND s.gross_rate > 0
          AND m.market_code IS NOT NULL
          AND (s.broadcast_month LIKE '2024%' OR s.broadcast_month LIKE '2025%')
        GROUP BY m.market_code, m.market_name
        HAVING (revenue_2024 + revenue_2025) > 25000
        ORDER BY (revenue_2024 + revenue_2025) DESC
        """
        
        cursor = conn.execute(market_comparison_query)
        market_data = cursor.fetchall()
        
        if market_data:
            print(f"{'Market':10} {'2024 Rev':>12} {'2024 Block%':>11} {'2025 Rev':>12} {'2025 Block%':>11} {'YoY Growth':>12}")
            print("-" * 85)
            for market in market_data:
                block_pct_2024 = (market['block_revenue_2024'] / market['revenue_2024'] * 100) if market['revenue_2024'] > 0 else 0
                block_pct_2025 = (market['block_revenue_2025'] / market['revenue_2025'] * 100) if market['revenue_2025'] > 0 else 0
                
                # Calculate year-over-year growth
                if market['revenue_2024'] > 0 and market['revenue_2025'] > 0:
                    yoy_growth = ((market['revenue_2025'] - market['revenue_2024']) / market['revenue_2024'] * 100)
                    yoy_display = f"{yoy_growth:+.1f}%"
                else:
                    yoy_display = "N/A"
                
                print(f"{market['market_code']:10} ${market['revenue_2024']:>11,.0f} {block_pct_2024:>10.1f}% ${market['revenue_2025']:>11,.0f} {block_pct_2025:>10.1f}% {yoy_display:>12}")
        
        # 7. Key Insights
        print(f"\n💡 KEY INSIGHTS & TRENDS")
        print("-" * 30)
        
        # Calculate overall trends
        insights_query = """
        SELECT 
            -- 2024 totals
            SUM(CASE WHEN s.broadcast_month LIKE '2024%' THEN s.gross_rate ELSE 0 END) as total_2024,
            SUM(CASE WHEN s.broadcast_month LIKE '2024%' AND slb.spot_id IS NOT NULL THEN s.gross_rate ELSE 0 END) as block_2024,
            -- 2025 totals
            SUM(CASE WHEN s.broadcast_month LIKE '2025%' THEN s.gross_rate ELSE 0 END) as total_2025,
            SUM(CASE WHEN s.broadcast_month LIKE '2025%' AND slb.spot_id IS NOT NULL THEN s.gross_rate ELSE 0 END) as block_2025,
            -- Q4 2024 vs Q1 2025 (most recent comparison)
            SUM(CASE WHEN s.broadcast_month LIKE '2024%' AND (s.broadcast_month LIKE '%-10-%' OR s.broadcast_month LIKE '%-11-%' OR s.broadcast_month LIKE '%-12-%') THEN s.gross_rate ELSE 0 END) as q4_2024,
            SUM(CASE WHEN s.broadcast_month LIKE '2025%' AND (s.broadcast_month LIKE '%-01-%' OR s.broadcast_month LIKE '%-02-%' OR s.broadcast_month LIKE '%-03-%') THEN s.gross_rate ELSE 0 END) as q1_2025
        FROM spots s
        LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL) 
          AND s.gross_rate > 0
          AND (s.broadcast_month LIKE '2024%' OR s.broadcast_month LIKE '2025%')
        """
        
        cursor = conn.execute(insights_query)
        insights = cursor.fetchone()
        
        if insights:
            block_pct_2024 = (insights['block_2024'] / insights['total_2024'] * 100) if insights['total_2024'] > 0 else 0
            block_pct_2025 = (insights['block_2025'] / insights['total_2025'] * 100) if insights['total_2025'] > 0 else 0
            
            print(f"🎯 YEAR-OVER-YEAR COMPARISON:")
            print(f"   • 2024: ${insights['total_2024']:,.0f} total | {block_pct_2024:.1f}% in language blocks")
            print(f"   • 2025: ${insights['total_2025']:,.0f} total | {block_pct_2025:.1f}% in language blocks")
            
            if insights['total_2024'] > 0 and insights['total_2025'] > 0:
                yoy_growth = ((insights['total_2025'] - insights['total_2024']) / insights['total_2024'] * 100)
                print(f"   • Overall YoY Growth: {yoy_growth:+.1f}%")
            
            if block_pct_2025 > block_pct_2024:
                print(f"   • ✅ Language block adoption improving (+{block_pct_2025 - block_pct_2024:.1f}%)")
            else:
                print(f"   • ⚠️  Language block adoption declining ({block_pct_2025 - block_pct_2024:.1f}%)")
        
        print(f"\n🚀 OPPORTUNITIES:")
        print(f"   • 2024 had {block_pct_2024:.1f}% language block penetration")
        print(f"   • 2025 has {block_pct_2025:.1f}% language block penetration")
        print(f"   • ${insights['total_2025'] - insights['block_2025']:,.0f} unassigned 2025 revenue")
        
        print(f"\n{'='*90}")
        print("✓ 2024 vs 2025 Quarterly Language Block Report Complete!")
        print("📊 This report focuses on quarterly trends and year-over-year comparisons")
        print("💡 Use quarterly data to identify seasonal patterns and growth opportunities")
        print(f"{'='*90}")
        
        return True
        
    except Exception as e:
        print(f"✗ Error generating report: {e}")
        import traceback
        print(traceback.format_exc())
        return False
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    generate_quarterly_language_block_report()