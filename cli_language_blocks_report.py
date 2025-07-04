#!/usr/bin/env python3
"""
Language Block Report CLI Tool
Integrated with existing ctv-bookedbiz-db system
"""

import os
import sys
import sqlite3
import argparse
from datetime import datetime
from pathlib import Path

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

try:
    from services.container import get_container
    from services.report_data_service import ReportDataService
except ImportError as e:
    print(f"Warning: Could not import services: {e}")
    print("Running in standalone mode with direct database connection")

def get_database_connection():
    """Get database connection using existing service or direct connection"""
    try:
        # Try to use existing service container
        container = get_container()
        report_service = container.get('report_data_service')
        if report_service:
            return report_service.get_connection()
    except Exception as e:
        print(f"Service container not available: {e}")
    
    # Fallback to direct database connection
    db_paths = [
        "data/database/production.db",
        "data/production.db", 
        "production.db",
        "database.db"
    ]
    
    for db_path in db_paths:
        if os.path.exists(db_path):
            print(f"Using database: {db_path}")
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            return conn
    
    raise FileNotFoundError("Database not found. Tried: " + ", ".join(db_paths))

def format_currency(amount):
    """Format currency values"""
    if amount is None:
        return "$0.00"
    return f"${amount:,.2f}"

def format_number(num):
    """Format numbers with commas"""
    if num is None:
        return "0"
    return f"{num:,}"

def print_section_header(title, width=60):
    """Print a formatted section header"""
    print(f"\n{title}")
    print("-" * len(title))

def print_overall_summary(conn):
    """Print overall summary statistics"""
    print("="*60)
    print("LANGUAGE BLOCK PERFORMANCE REPORT")
    print("="*60)
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    print_section_header("📊 OVERALL SUMMARY")
    
    query = """
    SELECT 
        COUNT(DISTINCT block_id) as total_blocks,
        COUNT(DISTINCT language_name) as total_languages,
        COUNT(DISTINCT market_code) as total_markets,
        SUM(total_revenue) as total_revenue,
        SUM(total_spots) as total_spots,
        AVG(total_revenue) as avg_revenue_per_block_month,
        MIN(first_air_date) as data_start_date,
        MAX(last_air_date) as data_end_date
    FROM language_block_revenue_summary
    """
    
    cursor = conn.execute(query)
    summary = cursor.fetchone()
    
    if summary:
        print(f"Total Language Blocks: {format_number(summary['total_blocks'])}")
        print(f"Languages Served: {format_number(summary['total_languages'])}")
        print(f"Markets Covered: {format_number(summary['total_markets'])}")
        print(f"Total Revenue: {format_currency(summary['total_revenue'])}")
        print(f"Total Spots: {format_number(summary['total_spots'])}")
        print(f"Avg Revenue/Block/Month: {format_currency(summary['avg_revenue_per_block_month'])}")
        if summary['data_start_date'] and summary['data_end_date']:
            print(f"Data Range: {summary['data_start_date']} to {summary['data_end_date']}")
    else:
        print("No language block data found")

def print_top_performers(conn, limit=10):
    """Print top performing language blocks"""
    print_section_header(f"🏆 TOP {limit} LANGUAGE BLOCKS BY REVENUE")
    
    query = """
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
    GROUP BY block_id, block_name, language_name, market_display_name, day_part, time_start, time_end
    ORDER BY total_revenue DESC
    LIMIT ?
    """
    
    cursor = conn.execute(query, (limit,))
    top_blocks = cursor.fetchall()
    
    if not top_blocks:
        print("No top performer data found")
        return
    
    for i, block in enumerate(top_blocks, 1):
        print(f"{i:2d}. {block['block_name']} ({block['language_name']})")
        print(f"     Market: {block['market_display_name']}")
        if block['day_part'] and block['time_slot']:
            print(f"     Time: {block['day_part']} {block['time_slot']}")
        print(f"     Revenue: {format_currency(block['total_revenue'])} | Spots: {format_number(block['total_spots'])} | Active: {block['active_months']} months")
        print()

def print_language_performance(conn):
    """Print language performance analysis"""
    print_section_header("🌐 LANGUAGE PERFORMANCE")
    
    query = """
    SELECT 
        language_name,
        COUNT(DISTINCT block_id) as block_count,
        SUM(total_revenue) as total_revenue,
        SUM(total_spots) as total_spots,
        AVG(total_revenue) as avg_revenue_per_block,
        CASE 
            WHEN SUM(total_spots) > 0 THEN SUM(total_revenue) / SUM(total_spots)
            ELSE 0
        END as revenue_per_spot
    FROM language_block_revenue_summary
    GROUP BY language_name
    ORDER BY total_revenue DESC
    """
    
    cursor = conn.execute(query)
    languages = cursor.fetchall()
    
    if not languages:
        print("No language performance data found")
        return
    
    print(f"{'Language':<15} | {'Blocks':<6} | {'Revenue':<12} | {'Spots':<7} | {'$/Spot':<8}")
    print("-" * 60)
    
    for lang in languages:
        print(f"{lang['language_name']:<15} | {lang['block_count']:>6} | {format_currency(lang['total_revenue']):>12} | {format_number(lang['total_spots']):>7} | {format_currency(lang['revenue_per_spot']):>8}")

def print_market_performance(conn):
    """Print market performance analysis"""
    print_section_header("🏢 MARKET PERFORMANCE")
    
    query = """
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
    
    cursor = conn.execute(query)
    markets = cursor.fetchall()
    
    if not markets:
        print("No market performance data found")
        return
    
    for market in markets:
        print(f"{market['market_code']:4} {market['market_display_name']:<20} | Blocks: {market['block_count']:2d} | Languages: {market['language_count']:2d} | Revenue: {format_currency(market['total_revenue'])}")

def print_time_slot_analysis(conn):
    """Print time slot performance analysis"""
    print_section_header("⏰ TIME SLOT PERFORMANCE")
    
    query = """
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
    
    cursor = conn.execute(query)
    timeslots = cursor.fetchall()
    
    if not timeslots:
        print("No time slot data found")
        return
    
    for slot in timeslots:
        day_part = slot['day_part'] if slot['day_part'] and slot['day_part'].strip() else "(Unspecified)"
        if slot['total_revenue'] and slot['total_revenue'] > 0:
            print(f"{day_part:<20} | Blocks: {slot['block_count']:2d} | Revenue: {format_currency(slot['total_revenue'])} | Avg/Block: {format_currency(slot['avg_revenue_per_block'])}")

def print_available_data_info(conn):
    """Print information about what data is available"""
    print_section_header("📋 DATA AVAILABILITY")
    
    try:
        # Check what columns exist in the main table
        schema_query = "PRAGMA table_info(language_block_revenue_summary)"
        cursor = conn.execute(schema_query)
        columns = cursor.fetchall()
        
        print("Available columns in language_block_revenue_summary:")
        for col in columns:
            print(f"  - {col['name']} ({col['type']})")
        
        # Check for any customer-related tables
        customer_tables_query = """
        SELECT name FROM sqlite_master 
        WHERE type='table' AND (name LIKE '%customer%' OR name LIKE '%client%')
        """
        cursor = conn.execute(customer_tables_query)
        customer_tables = cursor.fetchall()
        
        if customer_tables:
            print(f"\nCustomer-related tables found:")
            for table in customer_tables:
                print(f"  - {table['name']}")
        else:
            print(f"\nNo customer-specific tables found in database")
            
    except Exception as e:
        print(f"Error checking data availability: {e}")

def print_insights(conn):
    """Print key insights and recommendations"""
    print_section_header("💡 KEY INSIGHTS")
    
    # Most profitable language per spot
    query1 = """
    SELECT 
        language_name,
        SUM(total_revenue) / SUM(total_spots) as revenue_per_spot,
        SUM(total_spots) as total_spots,
        SUM(total_revenue) as total_revenue
    FROM language_block_revenue_summary
    GROUP BY language_name
    HAVING SUM(total_spots) >= 50
    ORDER BY revenue_per_spot DESC
    LIMIT 1
    """
    
    cursor = conn.execute(query1)
    most_profitable = cursor.fetchone()
    
    if most_profitable:
        print(f"• Most profitable language: {most_profitable['language_name']} ({format_currency(most_profitable['revenue_per_spot'])}/spot)")
    
    # Busiest time slot
    query2 = """
    SELECT 
        day_part,
        SUM(total_spots) as total_spots
    FROM language_block_revenue_summary
    WHERE day_part IS NOT NULL AND day_part != ''
    GROUP BY day_part
    ORDER BY total_spots DESC
    LIMIT 1
    """
    
    cursor = conn.execute(query2)
    busiest = cursor.fetchone()
    
    if busiest:
        print(f"• Busiest time slot: {busiest['day_part']} ({format_number(busiest['total_spots'])} spots)")
    
    # Growth opportunities
    query3 = """
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
    
    cursor = conn.execute(query3)
    opportunities = cursor.fetchall()
    
    if opportunities:
        print("• Expansion opportunities:")
        for opp in opportunities:
            print(f"  - {opp['language_name']}: {format_currency(opp['current_revenue'])} revenue in only {opp['current_markets']} markets")

def print_recent_activity(conn):
    """Print recent activity trends"""
    print_section_header("📅 RECENT ACTIVITY (Last 6 Months)")
    
    query = """
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
    
    cursor = conn.execute(query)
    recent = cursor.fetchall()
    
    if not recent:
        print("No recent activity data found")
        return
    
    for month in recent:
        month_str = month['year_month'] if month['year_month'] and month['year_month'].strip() else "Unknown"
        if month['monthly_revenue'] and month['monthly_revenue'] > 0:
            print(f"{month_str:<8} | Active Blocks: {month['active_blocks']:3d} | Revenue: {format_currency(month['monthly_revenue'])} | Spots: {format_number(month['monthly_spots'])}")

def generate_report(sections=None, top_limit=10, customer_limit=15):
    """Generate the complete language block report"""
    try:
        conn = get_database_connection()
        
        # Default sections if none specified
        if sections is None:
            sections = ['summary', 'top_performers', 'language', 'market', 'time_slot', 'recent', 'insights', 'data_info']
        
        # Print requested sections
        if 'summary' in sections:
            print_overall_summary(conn)
        
        if 'top_performers' in sections:
            print_top_performers(conn, top_limit)
        
        if 'language' in sections:
            print_language_performance(conn)
        
        if 'market' in sections:
            print_market_performance(conn)
        
        if 'time_slot' in sections:
            print_time_slot_analysis(conn)
        
        if 'data_info' in sections:
            print_available_data_info(conn)
        
        if 'recent' in sections:
            print_recent_activity(conn)
        
        if 'insights' in sections:
            print_insights(conn)
        
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

def main():
    """Main CLI function"""
    parser = argparse.ArgumentParser(description="Generate Language Block Performance Reports")
    parser.add_argument('--sections', nargs='+', 
                       choices=['summary', 'top_performers', 'language', 'market', 'time_slot', 'recent', 'insights', 'data_info'],
                       help='Specific sections to include in report')
    parser.add_argument('--top-limit', type=int, default=10, 
                       help='Number of top performers to show (default: 10)')
    parser.add_argument('--customer-limit', type=int, default=15,
                       help='Number of top customers to show (default: 15)')
    parser.add_argument('--output', '-o', help='Output file (default: stdout)')
    
    args = parser.parse_args()
    
    # Redirect output if specified
    if args.output:
        import sys
        with open(args.output, 'w') as f:
            old_stdout = sys.stdout
            sys.stdout = f
            try:
                success = generate_report(args.sections, args.top_limit, args.customer_limit)
            finally:
                sys.stdout = old_stdout
            
            if success:
                print(f"✓ Report generated successfully: {args.output}")
            else:
                print(f"✗ Error generating report")
    else:
        generate_report(args.sections, args.top_limit, args.customer_limit)

if __name__ == "__main__":
    main()