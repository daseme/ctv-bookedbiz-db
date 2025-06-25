#!/usr/bin/env python3
"""
Simple Language Block Report
Generate actionable reports from your language block data
"""

import sqlite3
import os
from datetime import datetime

def generate_language_block_report():
    """Generate a comprehensive language block report"""
    
    db_path = "data/database/production.db"
    if not os.path.exists(db_path):
        print("✗ Database not found. Run: python db_sync.py download")
        return False
    
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        
        print("="*60)
        print("LANGUAGE BLOCK PERFORMANCE REPORT")
        print("="*60)
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # 1. Overall Summary
        print("📊 OVERALL SUMMARY")
        print("-" * 30)
        
        summary_query = """
        SELECT 
            COUNT(DISTINCT block_id) as total_blocks,
            COUNT(DISTINCT language_name) as total_languages,
            COUNT(DISTINCT market_code) as total_markets,
            SUM(total_revenue) as total_revenue,
            SUM(total_spots) as total_spots,
            AVG(total_revenue) as avg_revenue_per_block_month
        FROM language_block_revenue_summary
        """
        
        cursor = conn.execute(summary_query)
        summary = cursor.fetchone()
        
        if summary:
            print(f"Total Language Blocks: {summary['total_blocks']}")
            print(f"Languages Served: {summary['total_languages']}")  
            print(f"Markets Covered: {summary['total_markets']}")
            print(f"Total Revenue: ${summary['total_revenue']:,.2f}")
            print(f"Total Spots: {summary['total_spots']:,}")
            print(f"Avg Revenue/Block/Month: ${summary['avg_revenue_per_block_month']:,.2f}")
        
        # 2. Top Performing Blocks by Revenue
        print(f"\n🏆 TOP 10 LANGUAGE BLOCKS BY REVENUE")
        print("-" * 50)
        
        top_blocks_query = """
        SELECT 
            block_name,
            language_name,
            market_display_name,
            day_part,
            time_start || '-' || time_end as time_slot,
            SUM(total_revenue) as total_revenue,
            SUM(total_spots) as total_spots,
            COUNT(DISTINCT year_month) as active_months
        FROM language_block_revenue_summary
        GROUP BY block_id, block_name, language_name, market_display_name
        ORDER BY total_revenue DESC
        LIMIT 10
        """
        
        cursor = conn.execute(top_blocks_query)
        top_blocks = cursor.fetchall()
        
        for i, block in enumerate(top_blocks, 1):
            print(f"{i:2d}. {block['block_name']} ({block['language_name']})")
            print(f"     Market: {block['market_display_name']}")
            print(f"     Time: {block['day_part']} {block['time_slot']}")
            print(f"     Revenue: ${block['total_revenue']:,.2f} | Spots: {block['total_spots']:,} | Active: {block['active_months']} months")
            print()
        
        # 3. Language Performance
        print(f"🌐 LANGUAGE PERFORMANCE")
        print("-" * 30)
        
        language_query = """
        SELECT 
            language_name,
            COUNT(DISTINCT block_id) as block_count,
            SUM(total_revenue) as total_revenue,
            SUM(total_spots) as total_spots,
            AVG(total_revenue) as avg_revenue_per_block
        FROM language_block_revenue_summary
        GROUP BY language_name
        ORDER BY total_revenue DESC
        """
        
        cursor = conn.execute(language_query)
        languages = cursor.fetchall()
        
        for lang in languages:
            revenue_per_spot = lang['total_revenue'] / lang['total_spots'] if lang['total_spots'] > 0 else 0
            print(f"{lang['language_name']:15} | Blocks: {lang['block_count']:2d} | Revenue: ${lang['total_revenue']:>8,.0f} | Spots: {lang['total_spots']:>5,} | $/Spot: ${revenue_per_spot:>5.2f}")
        
        # 4. Market Performance
        print(f"\n🏢 MARKET PERFORMANCE")
        print("-" * 30)
        
        market_query = """
        SELECT 
            market_display_name,
            market_code,
            COUNT(DISTINCT block_id) as block_count,
            COUNT(DISTINCT language_name) as language_count,
            SUM(total_revenue) as total_revenue,
            SUM(total_spots) as total_spots
        FROM language_block_revenue_summary
        GROUP BY market_display_name, market_code
        ORDER BY total_revenue DESC
        """
        
        cursor = conn.execute(market_query)
        markets = cursor.fetchall()
        
        for market in markets:
            print(f"{market['market_code']:4} {market['market_display_name']:20} | Blocks: {market['block_count']:2d} | Languages: {market['language_count']:2d} | Revenue: ${market['total_revenue']:>8,.0f}")
        
        # 5. Time Slot Analysis
        print(f"\n⏰ TIME SLOT PERFORMANCE")
        print("-" * 30)
        
        time_query = """
        SELECT 
            day_part,
            COUNT(DISTINCT block_id) as block_count,
            SUM(total_revenue) as total_revenue,
            SUM(total_spots) as total_spots,
            AVG(total_revenue) as avg_revenue_per_block
        FROM language_block_revenue_summary
        GROUP BY day_part
        ORDER BY total_revenue DESC
        """
        
        cursor = conn.execute(time_query)
        timeslots = cursor.fetchall()
        
        for slot in timeslots:
            if slot['day_part'] and slot['day_part'].strip():
                print(f"{slot['day_part']:20} | Blocks: {slot['block_count']:2d} | Revenue: ${slot['total_revenue']:>8,.0f} | Avg/Block: ${slot['avg_revenue_per_block']:>6,.0f}")
            elif slot['total_revenue'] > 0:  # Show data even if day_part is null
                print(f"{'(Unspecified)':20} | Blocks: {slot['block_count']:2d} | Revenue: ${slot['total_revenue']:>8,.0f} | Avg/Block: ${slot['avg_revenue_per_block']:>6,.0f}")
        
        # 6. Recent Activity
        print(f"\n📅 RECENT ACTIVITY (Last 6 Months)")
        print("-" * 40)
        
        recent_query = """
        SELECT 
            year_month,
            COUNT(DISTINCT block_id) as active_blocks,
            SUM(total_revenue) as monthly_revenue,
            SUM(total_spots) as monthly_spots
        FROM language_block_revenue_summary
        WHERE year_month >= date('now', '-6 months', 'start of month')
        GROUP BY year_month
        ORDER BY year_month DESC
        """
        
        cursor = conn.execute(recent_query)
        recent = cursor.fetchall()
        
        for month in recent:
            if month['year_month'] and month['year_month'].strip():
                print(f"{month['year_month']:8} | Active Blocks: {month['active_blocks']:3d} | Revenue: ${month['monthly_revenue']:>8,.0f} | Spots: {month['monthly_spots']:>5,}")
            elif month['monthly_revenue'] and month['monthly_revenue'] > 0:
                print(f"{'Unknown':8} | Active Blocks: {month['active_blocks']:3d} | Revenue: ${month['monthly_revenue']:>8,.0f} | Spots: {month['monthly_spots']:>5,}")
        
        # 7. Customer Analysis
        print(f"\n👥 TOP CUSTOMERS BY LANGUAGE BLOCK REVENUE")
        print("-" * 45)
        
        customer_query = """
        SELECT 
            customer_name,
            COUNT(DISTINCT block_id) as blocks_used,
            COUNT(DISTINCT language_name) as languages_targeted,
            SUM(total_revenue) as total_revenue,
            SUM(total_spots) as total_spots
        FROM language_block_revenue_summary
        WHERE customer_name IS NOT NULL
        GROUP BY customer_name
        ORDER BY total_revenue DESC
        LIMIT 15
        """
        
        cursor = conn.execute(customer_query)
        customers = cursor.fetchall()
        
        for i, customer in enumerate(customers, 1):
            print(f"{i:2d}. {customer['customer_name']:30} | Revenue: ${customer['total_revenue']:>8,.0f} | Blocks: {customer['blocks_used']:2d} | Languages: {customer['languages_targeted']:2d}")
        
        # 8. Performance Insights
        print(f"\n💡 KEY INSIGHTS")
        print("-" * 20)
        
        # Find most profitable language per spot
        profit_query = """
        SELECT 
            language_name,
            SUM(total_revenue) / SUM(total_spots) as revenue_per_spot,
            SUM(total_spots) as total_spots
        FROM language_block_revenue_summary
        GROUP BY language_name
        HAVING SUM(total_spots) >= 50
        ORDER BY revenue_per_spot DESC
        LIMIT 1
        """
        
        cursor = conn.execute(profit_query)
        most_profitable = cursor.fetchone()
        
        if most_profitable:
            print(f"• Most profitable language: {most_profitable['language_name']} (${most_profitable['revenue_per_spot']:.2f}/spot)")
        
        # Find busiest time slot
        busy_query = """
        SELECT 
            day_part,
            SUM(total_spots) as total_spots
        FROM language_block_revenue_summary
        WHERE day_part IS NOT NULL
        GROUP BY day_part
        ORDER BY total_spots DESC
        LIMIT 1
        """
        
        cursor = conn.execute(busy_query)
        busiest = cursor.fetchone()
        
        if busiest:
            print(f"• Busiest time slot: {busiest['day_part']} ({busiest['total_spots']:,} spots)")
        
        # Find growth opportunities
        opportunity_query = """
        SELECT 
            language_name,
            COUNT(DISTINCT market_code) as current_markets,
            SUM(total_revenue) as current_revenue
        FROM language_block_revenue_summary
        GROUP BY language_name
        HAVING current_markets < 5 AND current_revenue > 10000
        ORDER BY current_revenue DESC
        LIMIT 3
        """
        
        cursor = conn.execute(opportunity_query)
        opportunities = cursor.fetchall()
        
        if opportunities:
            print(f"• Expansion opportunities:")
            for opp in opportunities:
                print(f"  - {opp['language_name']}: ${opp['current_revenue']:,.0f} revenue in only {opp['current_markets']} markets")
        
        print(f"\n{'='*60}")
        print("✓ Language Block Report Complete!")
        print("💡 Use this data to optimize programming and sales strategies")
        print(f"{'='*60}")
        
        return True
        
    except Exception as e:
        print(f"✗ Error generating report: {e}")
        return False
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    generate_language_block_report()