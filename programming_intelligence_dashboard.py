#!/usr/bin/env python3
"""
Programming Intelligence Dashboard - Business Rules & Analytics System
=====================================================================

A comprehensive dashboard for analyzing programming composition, revenue density,
and strategic intelligence from the dual-purpose Business Rules Assignment System.

This system provides BOTH:
1. Assignment automation (88.3% coverage)
2. Programming analytics (content mix insights)

FIXED: Proper date parsing for ISO format broadcast_month fields (e.g., "2025-05-15 00:00:00")

COMMAND LINE FLAGS:
==================

Database Connection:
  --db PATH                 Database path (default: ./data/database/production.db)

System Overview & Diagnostics:
  --overview                Show high-level system performance metrics
                           • Total spots analyzed and assignment coverage
                           • Business rule automation rate
                           • Content type distribution (COM/BNS/PRG)
                           • Languages and markets covered

  --diagnose               Diagnose date format issues in broadcast_month field
                           • Shows actual vs expected date formats
                           • Identifies parsing problems
                           • Useful for troubleshooting date-related errors

Programming Analysis:
  --composition            Show programming composition analysis by language block
                           • Content mix (Commercial/Bonus/Program ratios)
                           • Revenue density by time slot
                           • Language-specific performance metrics

  --language LANG          Filter programming analysis by language
                           • Use with --composition for targeted analysis
                           • Examples: --language "Vietnamese", --language "Mandarin"

Revenue Intelligence:
  --revenue               Show revenue density analysis by language and day part
                           • High/Medium/Low revenue categorization
                           • Bonus content percentage analysis
                           • Performance by time slot

  --optimize              Identify programming optimization opportunities
                           • High priority: Segments with >40% bonus content
                           • Medium priority: Low revenue density segments
                           • Low priority: Underutilized inventory

  --top N                 Show top N performing language blocks by revenue
                           • Revenue per spot analysis
                           • Content mix breakdown
                           • Time slot performance

Trend Analysis:
  --trends                Show content mix trends with smart date grouping
                           • Current year: Monthly detail (2025-05, 2025-04, etc.)
                           • Historical years: Annual summary (2024, 2023, etc.)
                           • Content mix evolution over time

  --breakdown             Show simplified year breakdown with proper date parsing
                           • Grouped by year and month
                           • Spot counts and revenue by period
                           • Multi-language performance

  --yearly                Show year-over-year comparison for strategic planning
                           • Revenue growth analysis
                           • Content mix changes
                           • Language performance trends

  --monthly               Show current year monthly progression
                           • Month-by-month performance
                           • Seasonal trend identification
                           • Languages active by month

Comprehensive Reports:
  --all                   Show comprehensive dashboard with all key metrics
                           • Combines overview, top performers, and trends
                           • Executive summary format
                           • Complete system analysis

Usage Examples:
==============
python3 programming_intelligence_dashboard.py --overview
python3 programming_intelligence_dashboard.py --composition --language "Vietnamese"
python3 programming_intelligence_dashboard.py --revenue --optimize
python3 programming_intelligence_dashboard.py --trends --top 10
python3 programming_intelligence_dashboard.py --all

Key Insights Available:
======================
• "Vietnamese blocks: $62.95/spot, 28.8% bonus content"
• "Mandarin shift: 1.0% to 74.9% bonus content (2024 vs 2025)"
• "Tagalog premium: $78.84/spot, 0% bonus content"
• Programming optimization opportunities
• Revenue density by language and time slot
• Content mix trends and strategic analysis
"""

import sqlite3
import argparse
from typing import List, Dict, Any, Optional
from datetime import datetime
import json


class ProgrammingIntelligenceDashboard:
    """
    Dashboard for extracting programming intelligence and strategic insights
    from the Business Rules Assignment System.
    """
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
    
    def connect(self):
        """Connect to database."""
        self.conn = sqlite3.connect(self.db_path)
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
    
    
    def get_content_mix_trends(self) -> List[Dict[str, Any]]:
        """Get content mix trends over time with smart date grouping - FIXED for ISO dates."""
        cursor = self.conn.cursor()
        
        # Get current year for comparison
        current_year = datetime.now().year
        
        cursor.execute("""
            SELECT 
                -- Extract year from ISO date format like "2025-05-15 00:00:00"
                CAST(substr(s.broadcast_month, 1, 4) AS INTEGER) as year_num,
                -- Extract month from ISO date format  
                CAST(substr(s.broadcast_month, 6, 2) AS INTEGER) as month_num,
                -- Create time period based on year
                CASE 
                    WHEN CAST(substr(s.broadcast_month, 1, 4) AS INTEGER) = ? THEN 
                        substr(s.broadcast_month, 1, 7)  -- Show as "2025-05" for current year
                    ELSE 
                        substr(s.broadcast_month, 1, 4)  -- Show as "2024" for other years
                END as time_period,
                l.language_name,
                COUNT(*) as total_spots,
                COUNT(CASE WHEN s.spot_type = 'COM' THEN 1 END) as commercial_spots,
                COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bonus_spots,
                ROUND(COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) * 100.0 / COUNT(*), 1) as bonus_percentage,
                ROUND(AVG(s.gross_rate), 2) as avg_revenue_per_spot,
                ROUND(SUM(s.gross_rate), 2) as total_revenue,
                CASE 
                    WHEN CAST(substr(s.broadcast_month, 1, 4) AS INTEGER) = ? THEN 'monthly'
                    ELSE 'yearly'
                END as period_type
            FROM spots s
            JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            JOIN language_blocks lb ON slb.block_id = lb.block_id
            JOIN languages l ON lb.language_id = l.language_id
            WHERE s.broadcast_month IS NOT NULL
            GROUP BY time_period, l.language_name
            HAVING COUNT(*) >= 20
            ORDER BY 
                year_num DESC,
                month_num DESC,
                l.language_name
        """, (current_year, current_year))
        
        return [
            {
                'time_period': row[2],
                'language': row[3],
                'total_spots': row[4],
                'commercial_spots': row[5],
                'bonus_spots': row[6],
                'bonus_percentage': row[7],
                'avg_revenue_per_spot': row[8] or 0,
                'total_revenue': row[9] or 0,
                'period_type': row[10]
            }
            for row in cursor.fetchall()
        ]
    
    def get_current_year_monthly_progression(self) -> List[Dict[str, Any]]:
        """Get current year monthly progression for trend analysis - FIXED for ISO dates."""
        cursor = self.conn.cursor()
        
        # Get current year
        current_year = datetime.now().year
        
        cursor.execute("""
            SELECT 
                substr(s.broadcast_month, 1, 7) as year_month,  -- Extract "2025-05" format
                COUNT(*) as total_spots,
                COUNT(CASE WHEN s.spot_type = 'COM' THEN 1 END) as commercial_spots,
                COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bonus_spots,
                ROUND(COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) * 100.0 / COUNT(*), 1) as bonus_percentage,
                ROUND(AVG(s.gross_rate), 2) as avg_revenue_per_spot,
                ROUND(SUM(s.gross_rate), 2) as total_revenue,
                COUNT(DISTINCT l.language_name) as languages_active
            FROM spots s
            JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            JOIN language_blocks lb ON slb.block_id = lb.block_id
            JOIN languages l ON lb.language_id = l.language_id
            WHERE s.broadcast_month IS NOT NULL
            AND CAST(substr(s.broadcast_month, 1, 4) AS INTEGER) = ?
            GROUP BY year_month
            ORDER BY year_month
        """, (current_year,))
        
        return [
            {
                'broadcast_month': row[0],
                'total_spots': row[1],
                'commercial_spots': row[2],
                'bonus_spots': row[3],
                'bonus_percentage': row[4],
                'avg_revenue_per_spot': row[5] or 0,
                'total_revenue': row[6] or 0,
                'languages_active': row[7]
            }
            for row in cursor.fetchall()
        ]

    def get_year_over_year_comparison(self) -> List[Dict[str, Any]]:
        """Get year-over-year comparison for strategic planning - FIXED for ISO dates."""
        cursor = self.conn.cursor()
        
        cursor.execute("""
            SELECT 
                substr(s.broadcast_month, 1, 4) as year,  -- Extract year from ISO date
                l.language_name,
                COUNT(*) as total_spots,
                COUNT(CASE WHEN s.spot_type = 'COM' THEN 1 END) as commercial_spots,
                COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bonus_spots,
                ROUND(COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) * 100.0 / COUNT(*), 1) as bonus_percentage,
                ROUND(AVG(s.gross_rate), 2) as avg_revenue_per_spot,
                ROUND(SUM(s.gross_rate), 2) as total_revenue
            FROM spots s
            JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            JOIN language_blocks lb ON slb.block_id = lb.block_id
            JOIN languages l ON lb.language_id = l.language_id
            WHERE s.broadcast_month IS NOT NULL
            GROUP BY year, l.language_name
            HAVING COUNT(*) >= 50  -- Only years with significant data
            ORDER BY year DESC, total_revenue DESC
        """)
        
        return [
            {
                'year': row[0],
                'language': row[1],
                'total_spots': row[2],
                'commercial_spots': row[3],
                'bonus_spots': row[4],
                'bonus_percentage': row[5],
                'avg_revenue_per_spot': row[6] or 0,
                'total_revenue': row[7] or 0
            }
            for row in cursor.fetchall()
        ]

    def get_simplified_year_breakdown(self) -> List[Dict[str, Any]]:
        """Get a simplified year breakdown with proper ISO date parsing."""
        cursor = self.conn.cursor()
        
        cursor.execute("""
            SELECT 
                broadcast_month,
                substr(broadcast_month, 1, 4) as year_part,  -- Extract year from ISO date
                substr(broadcast_month, 1, 7) as year_month,  -- Extract "2025-05" format
                COUNT(*) as spot_count,
                COUNT(DISTINCT l.language_name) as languages,
                ROUND(SUM(s.gross_rate), 2) as total_revenue,
                ROUND(AVG(s.gross_rate), 2) as avg_revenue
            FROM spots s
            JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            JOIN language_blocks lb ON slb.block_id = lb.block_id
            JOIN languages l ON lb.language_id = l.language_id
            WHERE s.broadcast_month IS NOT NULL
            GROUP BY year_month
            ORDER BY year_month DESC
        """)
        
        return [
            {
                'broadcast_month': row[0],
                'year_part': row[1],
                'year_month': row[2],
                'spot_count': row[3],
                'languages': row[4],
                'total_revenue': row[5] or 0,
                'avg_revenue': row[6] or 0
            }
            for row in cursor.fetchall()
        ]
    
    # ADD these missing methods to your ProgrammingIntelligenceDashboard class:

    def print_date_diagnosis(self):
        """Print diagnosis of date format issues."""
        print("🔍 DATE FORMAT DIAGNOSIS")
        print("=" * 60)
        
        diagnosis = self.diagnose_date_format()
        
        if not diagnosis:
            print("No broadcast_month data found!")
            return
        
        print("Sample broadcast_month values:")
        print("Format: broadcast_month | extracted_year | extracted_month | length | count")
        print("-" * 70)
        
        for item in diagnosis:
            print(f"{item['broadcast_month']:<15} | {item['extracted_year']:<13} | {item['extracted_month']:<15} | {item['field_length']:<6} | {item['count']}")
        
        # Analyze patterns
        year_parts = [item['extracted_year'] for item in diagnosis]
        unique_years = set(year_parts)
        
        print(f"\nUnique year parts found: {sorted(unique_years)}")
        print(f"Expected years: 24, 25, 26")
        
        if '00' in unique_years:
            print("⚠️  WARNING: Found '00' as year part - this suggests incorrect date format!")
        
        if not any(year in unique_years for year in ['24', '25', '26']):
            print("⚠️  WARNING: Expected years (24, 25, 26) not found!")

    def diagnose_date_format(self) -> List[Dict[str, Any]]:
        """Diagnose the actual format of broadcast_month values."""
        cursor = self.conn.cursor()
        
        cursor.execute("""
            SELECT 
                broadcast_month,
                substr(broadcast_month, -2) as extracted_year,
                substr(broadcast_month, 1, 3) as extracted_month,
                length(broadcast_month) as field_length,
                COUNT(*) as count
            FROM spots 
            WHERE broadcast_month IS NOT NULL 
            GROUP BY broadcast_month 
            ORDER BY broadcast_month DESC 
            LIMIT 20
        """)
        
        return [
            {
                'broadcast_month': row[0],
                'extracted_year': row[1],
                'extracted_month': row[2],
                'field_length': row[3],
                'count': row[4]
            }
            for row in cursor.fetchall()
        ]

    def print_optimization_opportunities(self):
        """Print optimization opportunities."""
        opportunities = self.get_optimization_opportunities()
        
        print("🚀 PROGRAMMING OPTIMIZATION OPPORTUNITIES")
        print("=" * 60)
        
        high_priority = [opp for opp in opportunities if opp['priority'] == 'High']
        medium_priority = [opp for opp in opportunities if opp['priority'] == 'Medium']
        
        if high_priority:
            print("🔴 HIGH PRIORITY OPPORTUNITIES:")
            for opp in high_priority:
                print(f"  • {opp['language']} - {opp['day_part']}")
                print(f"    - {opp['optimization_opportunity']}")
                print(f"    - Current: ${opp['avg_revenue_per_spot']:.2f}/spot, {opp['bonus_percentage']:.1f}% bonus")
                print(f"    - Total Revenue: ${opp['total_revenue']:,.2f}")
                print()
        
        if medium_priority:
            print("🟡 MEDIUM PRIORITY OPPORTUNITIES:")
            for opp in medium_priority[:3]:  # Show top 3
                print(f"  • {opp['language']} - {opp['day_part']}")
                print(f"    - {opp['optimization_opportunity']}")
                print(f"    - Current: ${opp['avg_revenue_per_spot']:.2f}/spot, {opp['bonus_percentage']:.1f}% bonus")
                print()

    def print_year_over_year_comparison(self):
        """Print year-over-year comparison."""
        comparison = self.get_year_over_year_comparison()
        
        print("📊 YEAR-OVER-YEAR COMPARISON")
        print("=" * 60)
        
        # Group by language for comparison
        languages = {}
        for item in comparison:
            lang = item['language']
            if lang not in languages:
                languages[lang] = []
            languages[lang].append(item)
        
        for language, years in languages.items():
            print(f"🌐 {language}:")
            years.sort(key=lambda x: x['year'], reverse=True)
            
            for year_data in years:
                print(f"  {year_data['year']}: {year_data['total_spots']:,} spots, "
                    f"${year_data['avg_revenue_per_spot']:.2f}/spot, "
                    f"{year_data['bonus_percentage']:.1f}% bonus, "
                    f"${year_data['total_revenue']:,.2f} total")
            
            # Calculate growth if we have multiple years
            if len(years) >= 2:
                current = years[0]
                previous = years[1]
                revenue_growth = ((current['total_revenue'] - previous['total_revenue']) / previous['total_revenue']) * 100
                print(f"  📈 Growth: {revenue_growth:+.1f}% revenue ({current['year']} vs {previous['year']})")
            
            print()

    def print_current_year_progression(self):
        """Print current year monthly progression."""
        progression = self.get_current_year_monthly_progression()
        
        print("📅 CURRENT YEAR MONTHLY PROGRESSION")
        print("=" * 60)
        
        if not progression:
            print("No current year data found!")
            return
        
        total_spots = sum(item['total_spots'] for item in progression)
        total_revenue = sum(item['total_revenue'] for item in progression)
        
        print(f"Year-to-Date Summary: {total_spots:,} spots, ${total_revenue:,.2f} total revenue")
        print()
        
        for month in progression:
            print(f"📊 {month['broadcast_month']}:")
            print(f"  • {month['total_spots']:,} spots ({month['languages_active']} languages active)")
            print(f"  • ${month['avg_revenue_per_spot']:.2f}/spot average")
            print(f"  • {month['bonus_percentage']:.1f}% bonus content")
            print(f"  • ${month['total_revenue']:,.2f} total revenue")
            print()

    
    def print_simplified_breakdown(self):
        """Print simplified year breakdown with proper formatting."""
        print("📅 SIMPLIFIED YEAR BREAKDOWN")
        print("=" * 60)
        
        breakdown = self.get_simplified_year_breakdown()
        
        if not breakdown:
            print("No data found!")
            return
        
        current_year = None
        year_total = 0
        
        for item in breakdown:
            year_part = item['year_part']
            year_month = item['year_month']
            
            # Group by year
            if current_year != year_part:
                if current_year is not None:
                    print(f"  {current_year} Total: {year_total:,} spots")
                    print()
                
                current_year = year_part
                year_total = 0
                print(f"📊 Year {year_part}:")
            
            year_total += item['spot_count']
            
            print(f"  {year_month}: {item['spot_count']:,} spots, "
                f"{item['languages']} languages, ${item['total_revenue']:,.2f} revenue")
        
        if current_year is not None:
            print(f"  {current_year} Total: {year_total:,} spots")
    
    def print_content_mix_trends(self, limit: int = 20):
        """Print content mix trends with FIXED date parsing."""
        print("📊 CONTENT MIX TRENDS (FIXED)")
        print("=" * 60)
        
        trends = self.get_content_mix_trends()  # Use the fixed method above
        
        if not trends:
            print("No content mix trend data found.")
            return
        
        # Group by period type for better display
        current_year_trends = [t for t in trends if t['period_type'] == 'monthly']
        historical_trends = [t for t in trends if t['period_type'] == 'yearly']
        
        if current_year_trends:
            print("📅 CURRENT YEAR (Monthly Detail):")
            for trend in current_year_trends[:limit//2]:
                print(f"  • {trend['time_period']} - {trend['language']}")
                print(f"    Total: {trend['total_spots']:,} spots")
                print(f"    Mix: {trend['commercial_spots']:,} Commercial, {trend['bonus_spots']:,} Bonus ({trend['bonus_percentage']:.1f}%)")
                print(f"    Revenue: ${trend['avg_revenue_per_spot']:.2f}/spot, ${trend['total_revenue']:,.2f} total")
                print()
        
        if historical_trends:
            print("📈 HISTORICAL YEARS (Annual Summary):")
            for trend in historical_trends[:limit//2]:
                print(f"  • {trend['time_period']} - {trend['language']}")
                print(f"    Total: {trend['total_spots']:,} spots")
                print(f"    Mix: {trend['commercial_spots']:,} Commercial, {trend['bonus_spots']:,} Bonus ({trend['bonus_percentage']:.1f}%)")
                print(f"    Revenue: ${trend['avg_revenue_per_spot']:.2f}/spot, ${trend['total_revenue']:,.2f} total")
                print()
    
    def get_system_overview(self) -> Dict[str, Any]:
        """Get high-level system performance overview."""
        cursor = self.conn.cursor()
        
        # Overall system metrics
        cursor.execute("""
            SELECT 
                COUNT(*) as total_spots,
                COUNT(CASE WHEN slb.spot_id IS NOT NULL THEN 1 END) as assigned_spots,
                COUNT(CASE WHEN slb.business_rule_applied IS NOT NULL THEN 1 END) as business_rule_spots,
                COUNT(DISTINCT l.language_name) as languages_covered,
                COUNT(DISTINCT lb.block_id) as unique_blocks,
                COUNT(DISTINCT m.market_name) as markets_covered
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            LEFT JOIN language_blocks lb ON slb.block_id = lb.block_id
            LEFT JOIN languages l ON lb.language_id = l.language_id
            LEFT JOIN markets m ON s.market_id = m.market_id
        """)
        
        overall = cursor.fetchone()
        
        # Content type distribution
        cursor.execute("""
            SELECT 
                s.spot_type,
                COUNT(*) as total_spots,
                COUNT(CASE WHEN slb.spot_id IS NOT NULL THEN 1 END) as assigned_spots,
                ROUND(COUNT(CASE WHEN slb.spot_id IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 1) as assignment_rate,
                ROUND(AVG(s.gross_rate), 2) as avg_revenue
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            GROUP BY s.spot_type
            ORDER BY COUNT(*) DESC
        """)
        
        content_types = [
            {
                'spot_type': row[0] or 'Unknown',
                'total_spots': row[1],
                'assigned_spots': row[2],
                'assignment_rate': row[3],
                'avg_revenue': row[4] or 0
            }
            for row in cursor.fetchall()
        ]
        
        return {
            'total_spots': overall[0] or 0,
            'assigned_spots': overall[1] or 0,
            'business_rule_spots': overall[2] or 0,
            'languages_covered': overall[3] or 0,
            'unique_blocks': overall[4] or 0,
            'markets_covered': overall[5] or 0,
            'assignment_rate': round((overall[1] or 0) * 100.0 / (overall[0] or 1), 1),
            'automation_rate': round((overall[2] or 0) * 100.0 / (overall[0] or 1), 1),
            'content_types': content_types
        }
    
  

    def get_programming_composition(self, language_filter: str = None) -> List[Dict[str, Any]]:
        """Get programming composition analysis by language block."""
        cursor = self.conn.cursor()
        
        base_query = """
            SELECT 
                l.language_name,
                lb.block_name,
                lb.day_part,
                lb.day_of_week,
                lb.time_start,
                lb.time_end,
                COUNT(*) as total_spots,
                COUNT(CASE WHEN s.spot_type = 'COM' THEN 1 END) as commercial_spots,
                COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bonus_spots,
                COUNT(CASE WHEN s.spot_type = 'PRG' THEN 1 END) as program_spots,
                COUNT(CASE WHEN s.spot_type NOT IN ('COM', 'BNS', 'PRG') THEN 1 END) as other_spots,
                ROUND(COUNT(CASE WHEN s.spot_type = 'COM' THEN 1 END) * 100.0 / COUNT(*), 1) as commercial_percentage,
                ROUND(COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) * 100.0 / COUNT(*), 1) as bonus_percentage,
                ROUND(AVG(s.gross_rate), 2) as avg_revenue_per_spot,
                ROUND(SUM(s.gross_rate), 2) as total_revenue,
                ROUND(SUM(CASE WHEN s.spot_type = 'COM' THEN s.gross_rate ELSE 0 END), 2) as commercial_revenue,
                ROUND(SUM(CASE WHEN s.spot_type = 'COM' THEN s.gross_rate ELSE 0 END) / NULLIF(COUNT(CASE WHEN s.spot_type = 'COM' THEN 1 END), 0), 2) as avg_commercial_rate
            FROM spots s
            JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            JOIN language_blocks lb ON slb.block_id = lb.block_id
            JOIN languages l ON lb.language_id = l.language_id
        """
        
        if language_filter:
            base_query += f" WHERE l.language_name LIKE '%{language_filter}%'"
        
        base_query += """
            GROUP BY l.language_name, lb.block_name, lb.day_part, lb.day_of_week, lb.time_start, lb.time_end
            HAVING COUNT(*) >= 10
            ORDER BY total_revenue DESC
        """
        
        cursor.execute(base_query)
        
        return [
            {
                'language': row[0],
                'block_name': row[1],
                'day_part': row[2],
                'day_of_week': row[3],
                'time_start': row[4],
                'time_end': row[5],
                'total_spots': row[6],
                'commercial_spots': row[7],
                'bonus_spots': row[8],
                'program_spots': row[9],
                'other_spots': row[10],
                'commercial_percentage': row[11],
                'bonus_percentage': row[12],
                'avg_revenue_per_spot': row[13] or 0,
                'total_revenue': row[14] or 0,
                'commercial_revenue': row[15] or 0,
                'avg_commercial_rate': row[16] or 0
            }
            for row in cursor.fetchall()
        ]

    def print_programming_composition(self, language_filter: str = None):
        """Print programming composition analysis."""
        composition = self.get_programming_composition(language_filter)
        
        filter_text = f" - {language_filter}" if language_filter else ""
        print(f"📺 PROGRAMMING COMPOSITION ANALYSIS{filter_text}")
        print("=" * 60)
        
        if not composition:
            print("No programming composition data found.")
            return
        
        for block in composition[:10]:  # Show top 10 blocks
            print(f"🎬 {block['language']} - {block['block_name']}")
            print(f"   Time: {block['day_of_week']} {block['time_start']}-{block['time_end']} ({block['day_part']})")
            print(f"   Total Spots: {block['total_spots']:,}")
            print(f"   Content Mix: {block['commercial_spots']:,} Commercial ({block['commercial_percentage']:.1f}%), {block['bonus_spots']:,} Bonus ({block['bonus_percentage']:.1f}%)")
            print(f"   Revenue: ${block['avg_revenue_per_spot']:.2f}/spot average, ${block['total_revenue']:,.2f} total")
            print(f"   Commercial Revenue: ${block['commercial_revenue']:,.2f} (${block['avg_commercial_rate']:.2f}/commercial spot)")
            print()

    def get_revenue_density_analysis(self) -> List[Dict[str, Any]]:
        """Get revenue density analysis by language and day part."""
        cursor = self.conn.cursor()
        
        cursor.execute("""
            SELECT 
                l.language_name,
                lb.day_part,
                COUNT(*) as total_spots,
                ROUND(COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) * 100.0 / COUNT(*), 1) as bonus_percentage,
                ROUND(AVG(s.gross_rate), 2) as avg_revenue_per_spot,
                ROUND(SUM(s.gross_rate), 2) as total_revenue,
                ROUND(SUM(CASE WHEN s.spot_type = 'COM' THEN s.gross_rate ELSE 0 END), 2) as commercial_revenue,
                COUNT(DISTINCT lb.block_id) as unique_blocks,
                CASE 
                    WHEN AVG(s.gross_rate) > 30 THEN 'High Revenue (>$30)'
                    WHEN AVG(s.gross_rate) > 20 THEN 'Medium Revenue ($20-$30)'
                    WHEN AVG(s.gross_rate) > 10 THEN 'Low Revenue ($10-$20)'
                    ELSE 'Very Low Revenue (<$10)'
                END as revenue_category,
                CASE 
                    WHEN COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) * 100.0 / COUNT(*) < 10 THEN 'Low Bonus (<10%)'
                    WHEN COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) * 100.0 / COUNT(*) < 25 THEN 'Medium Bonus (10-25%)'
                    ELSE 'High Bonus (>25%)'
                END as bonus_category
            FROM spots s
            JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            JOIN language_blocks lb ON slb.block_id = lb.block_id
            JOIN languages l ON lb.language_id = l.language_id
            GROUP BY l.language_name, lb.day_part
            HAVING COUNT(*) >= 20
            ORDER BY avg_revenue_per_spot DESC
        """)
        
        return [
            {
                'language': row[0],
                'day_part': row[1],
                'total_spots': row[2],
                'bonus_percentage': row[3],
                'avg_revenue_per_spot': row[4] or 0,
                'total_revenue': row[5] or 0,
                'commercial_revenue': row[6] or 0,
                'unique_blocks': row[7],
                'revenue_category': row[8],
                'bonus_category': row[9]
            }
            for row in cursor.fetchall()
        ]

    def print_revenue_density_analysis(self):
        """Print revenue density analysis."""
        analysis = self.get_revenue_density_analysis()
        
        print("💰 REVENUE DENSITY ANALYSIS")
        print("=" * 60)
        
        print("📈 TOP PERFORMING SEGMENTS:")
        for segment in analysis[:5]:
            print(f"  • {segment['language']} - {segment['day_part']}")
            print(f"    - ${segment['avg_revenue_per_spot']:.2f}/spot average")
            print(f"    - {segment['bonus_percentage']:.1f}% bonus content")
            print(f"    - {segment['total_spots']:,} total spots")
            print(f"    - {segment['revenue_category']} | {segment['bonus_category']}")
            print()

    def get_optimization_opportunities(self) -> List[Dict[str, Any]]:
        """Identify programming optimization opportunities."""
        cursor = self.conn.cursor()
        
        cursor.execute("""
            SELECT 
                l.language_name,
                lb.day_part,
                COUNT(*) as total_spots,
                ROUND(COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) * 100.0 / COUNT(*), 1) as bonus_percentage,
                ROUND(AVG(s.gross_rate), 2) as avg_revenue_per_spot,
                ROUND(SUM(s.gross_rate), 2) as total_revenue,
                CASE 
                    WHEN COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) * 100.0 / COUNT(*) > 40 THEN 'High bonus content - opportunity to increase paid advertising'
                    WHEN AVG(s.gross_rate) < 10 THEN 'Low revenue density - opportunity to optimize pricing'
                    WHEN COUNT(*) < 50 THEN 'Low utilization - opportunity to increase inventory'
                    WHEN AVG(s.gross_rate) > 30 AND COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) * 100.0 / COUNT(*) < 15 THEN 'High performance - model for other segments'
                    ELSE 'Well optimized segment'
                END as optimization_opportunity,
                CASE 
                    WHEN COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) * 100.0 / COUNT(*) > 40 THEN 'High'
                    WHEN AVG(s.gross_rate) < 10 THEN 'Medium'
                    WHEN COUNT(*) < 50 THEN 'Low'
                    ELSE 'Optimized'
                END as priority
            FROM spots s
            JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            JOIN language_blocks lb ON slb.block_id = lb.block_id
            JOIN languages l ON lb.language_id = l.language_id
            GROUP BY l.language_name, lb.day_part
            HAVING COUNT(*) >= 10
            ORDER BY 
                CASE 
                    WHEN COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) * 100.0 / COUNT(*) > 40 THEN 1
                    WHEN AVG(s.gross_rate) < 10 THEN 2
                    WHEN COUNT(*) < 50 THEN 3
                    ELSE 4
                END,
                total_revenue DESC
        """)
        
        return [
            {
                'language': row[0],
                'day_part': row[1],
                'total_spots': row[2],
                'bonus_percentage': row[3],
                'avg_revenue_per_spot': row[4] or 0,
                'total_revenue': row[5] or 0,
                'optimization_opportunity': row[6],
                'priority': row[7]
            }
            for row in cursor.fetchall()
        ]

    def get_top_performing_blocks(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get top performing language blocks by revenue density."""
        cursor = self.conn.cursor()
        
        cursor.execute("""
            SELECT 
                l.language_name,
                lb.block_name,
                lb.day_part,
                lb.day_of_week,
                lb.time_start,
                lb.time_end,
                COUNT(*) as total_spots,
                COUNT(CASE WHEN s.spot_type = 'COM' THEN 1 END) as commercial_spots,
                COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bonus_spots,
                ROUND(COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) * 100.0 / COUNT(*), 1) as bonus_percentage,
                ROUND(AVG(s.gross_rate), 2) as avg_revenue_per_spot,
                ROUND(SUM(s.gross_rate), 2) as total_revenue,
                ROUND(SUM(CASE WHEN s.spot_type = 'COM' THEN s.gross_rate ELSE 0 END), 2) as commercial_revenue
            FROM spots s
            JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            JOIN language_blocks lb ON slb.block_id = lb.block_id
            JOIN languages l ON lb.language_id = l.language_id
            GROUP BY l.language_name, lb.block_name, lb.day_part, lb.day_of_week, lb.time_start, lb.time_end
            HAVING COUNT(*) >= 50
            ORDER BY avg_revenue_per_spot DESC
            LIMIT ?
        """, (limit,))
        
        return [
            {
                'language': row[0],
                'block_name': row[1],
                'day_part': row[2],
                'day_of_week': row[3],
                'time_start': row[4],
                'time_end': row[5],
                'total_spots': row[6],
                'commercial_spots': row[7],
                'bonus_spots': row[8],
                'bonus_percentage': row[9],
                'avg_revenue_per_spot': row[10] or 0,
                'total_revenue': row[11] or 0,
                'commercial_revenue': row[12] or 0
            }
            for row in cursor.fetchall()
        ]

    def print_top_performers(self, limit: int = 5):
        """Print top performing blocks."""
        top_blocks = self.get_top_performing_blocks(limit)
        
        print(f"🏆 TOP {limit} PERFORMING LANGUAGE BLOCKS")
        print("=" * 60)
        
        for i, block in enumerate(top_blocks, 1):
            print(f"{i}. {block['language']} - {block['block_name']}")
            print(f"   Time: {block['day_of_week']} {block['time_start']}-{block['time_end']} ({block['day_part']})")
            print(f"   Performance: ${block['avg_revenue_per_spot']:.2f}/spot average")
            print(f"   Content Mix: {block['commercial_spots']:,} Commercial, {block['bonus_spots']:,} Bonus ({block['bonus_percentage']:.1f}%)")
            print(f"   Total Revenue: ${block['total_revenue']:,.2f}")
            print()

    def print_system_overview(self):
        """Print comprehensive system overview."""
        overview = self.get_system_overview()
        
        print("🎯 PROGRAMMING INTELLIGENCE DASHBOARD")
        print("=" * 60)
        print(f"Total Spots Analyzed: {overview['total_spots']:,}")
        print(f"Assignment Coverage: {overview['assignment_rate']:.1f}% ({overview['assigned_spots']:,} spots)")
        print(f"Business Rule Automation: {overview['automation_rate']:.1f}% ({overview['business_rule_spots']:,} spots)")
        print(f"Languages Covered: {overview['languages_covered']}")
        print(f"Unique Programming Blocks: {overview['unique_blocks']}")
        print(f"Markets Analyzed: {overview['markets_covered']}")
        print()
        
        print("📊 CONTENT TYPE DISTRIBUTION:")
        for content_type in overview['content_types']:
            type_name = content_type['spot_type']
            if type_name == 'COM':
                type_desc = "Commercial (Paid Advertising)"
            elif type_name == 'BNS':
                type_desc = "Bonus (Programming Content)"
            elif type_name == 'PRG':
                type_desc = "Program (Show Content)"
            else:
                type_desc = f"{type_name} Content"
            
            print(f"  • {type_desc}")
            print(f"    - {content_type['total_spots']:,} spots total")
            print(f"    - {content_type['assignment_rate']:.1f}% assigned ({content_type['assigned_spots']:,} spots)")
            print(f"    - ${content_type['avg_revenue']:.2f} average revenue")
            print()


def main():
    """Main CLI function."""
    parser = argparse.ArgumentParser(
        description="Programming Intelligence Dashboard - Extract strategic insights from your dual-purpose assignment system"
    )
    
    # Database Connection
    parser.add_argument("--db", default="./data/database/production.db", help="Database path")
    
    # System Overview & Diagnostics
    parser.add_argument("--overview", action="store_true", help="Show system overview")
    parser.add_argument("--diagnose", action="store_true", help="Diagnose date format issues")
    
    # Programming Analysis
    parser.add_argument("--composition", action="store_true", help="Show programming composition analysis")
    parser.add_argument("--language", type=str, help="Filter by language (e.g., 'Vietnamese', 'Mandarin')")
    
    # Revenue Intelligence
    parser.add_argument("--revenue", action="store_true", help="Show revenue density analysis")
    parser.add_argument("--optimize", action="store_true", help="Show optimization opportunities")
    parser.add_argument("--top", type=int, help="Show top N performing blocks")
    
    # Trend Analysis
    parser.add_argument("--trends", action="store_true", help="Show content mix trends (smart date grouping)")
    parser.add_argument("--breakdown", action="store_true", help="Show simplified year breakdown")
    parser.add_argument("--yearly", action="store_true", help="Show year-over-year comparison")
    parser.add_argument("--monthly", action="store_true", help="Show current year monthly progression")
    
    # Comprehensive Reports
    parser.add_argument("--all", action="store_true", help="Show comprehensive dashboard")
    
    args = parser.parse_args()
    
    dashboard = ProgrammingIntelligenceDashboard(args.db)
    dashboard.connect()
    
    try:
        # System Overview & Diagnostics
        if args.overview or args.all:
            dashboard.print_system_overview()
            if args.all:
                print()
        
        if args.diagnose:
            dashboard.print_date_diagnosis()
            print()
        
        # Programming Analysis
        if args.composition or args.all:
            dashboard.print_programming_composition(args.language)
            if args.all:
                print()
        
        # Revenue Intelligence
        if args.revenue or args.all:
            dashboard.print_revenue_density_analysis()
            if args.all:
                print()
        
        if args.optimize or args.all:
            dashboard.print_optimization_opportunities()
            if args.all:
                print()
        
        if args.top or args.all:
            limit = args.top if args.top else 5
            dashboard.print_top_performers(limit)
            if args.all:
                print()
        
        # Trend Analysis
        if args.trends or args.all:
            dashboard.print_content_mix_trends()
            if args.all:
                print()
        
        if args.breakdown:
            dashboard.print_simplified_breakdown()
            print()
        
        if args.yearly:
            dashboard.print_year_over_year_comparison()
            print()
        
        if args.monthly:
            dashboard.print_current_year_progression()
            print()
        
        # Default behavior if no flags specified
        if not any([args.overview, args.composition, args.revenue, args.optimize, args.top, args.trends, 
                   args.breakdown, args.yearly, args.monthly, args.all, args.diagnose]):
            # Default: show overview and key insights
            dashboard.print_system_overview()
            print()
            dashboard.print_top_performers(3)
            print()
            dashboard.print_current_year_progression()
            print()
            print("💡 TIP: Use --all for comprehensive dashboard, --trends for smart date grouping, or --help for all options")
    
    finally:
        dashboard.close()


if __name__ == "__main__":
    main()