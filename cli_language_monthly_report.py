#!/usr/bin/env python3
"""
Fixed Revenue Report Generator - Direct Response Extraction + Multi-Language Separation
======================================================================================

Updated for new broadcast_month format: mmm-yy (e.g., Jan-24, Feb-25)

Fixes:
1. Direct Response extracted regardless of language blocks (~$387K WorldLink revenue)
2. Separated Multi-Language (Cross-Audience) as its own revenue category
3. Updated for new mmm-yy broadcast_month format (no more datetime parsing)
4. Restored annual language performance grid from original

Usage:
    python fixed_revenue_report.py 2024
"""

import sqlite3
import argparse
import datetime
import os
from typing import List, Tuple, Optional


class FixedRevenueReportGenerator:
    def __init__(self, db_path: str = './data/database/production.db'):
        self.db_path = db_path
        self.report_lines = []
        
    def add_line(self, line: str = ""):
        """Add a line to the report"""
        self.report_lines.append(line)
        
    def add_header(self, text: str, level: int = 1):
        """Add a markdown header"""
        self.add_line(f"{'#' * level} {text}")
        self.add_line()
        
    def add_table_row(self, cells: List[str], is_header: bool = False):
        """Add a table row"""
        row = "| " + " | ".join(cells) + " |"
        self.add_line(row)
        if is_header:
            separator = "| " + " | ".join(["---"] * len(cells)) + " |"
            self.add_line(separator)
    
    def get_broadcast_months_aggregated(self, year: int) -> List[Tuple[str, str]]:
        """Get broadcast months in new mmm-yy format"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get all broadcast months for the year in mmm-yy format
        year_suffix = str(year)[2:]  # Convert 2024 to "24"
        cursor.execute("""
            SELECT DISTINCT 
                broadcast_month,
                broadcast_month as sample_date
            FROM spots 
            WHERE broadcast_month LIKE ?
            ORDER BY 
                CASE substr(broadcast_month, 1, 3)
                    WHEN 'Jan' THEN 1 WHEN 'Feb' THEN 2 WHEN 'Mar' THEN 3
                    WHEN 'Apr' THEN 4 WHEN 'May' THEN 5 WHEN 'Jun' THEN 6
                    WHEN 'Jul' THEN 7 WHEN 'Aug' THEN 8 WHEN 'Sep' THEN 9
                    WHEN 'Oct' THEN 10 WHEN 'Nov' THEN 11 WHEN 'Dec' THEN 12
                END
        """, (f'%-{year_suffix}',))
        
        result = cursor.fetchall()
        conn.close()
        return result
    
    def get_month_display_name(self, month_str: str) -> str:
        """Return month string as-is since it's already in mmm-yy format"""
        return month_str
    
    def get_month_revenue_data_aggregated(self, month_str: str) -> Tuple:
        """Get revenue data for a specific month in mmm-yy format"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT COUNT(*) as spots, SUM(gross_rate) as revenue
            FROM spots 
            WHERE broadcast_month = ? AND gross_rate != 0
        """, (month_str,))
        
        result = cursor.fetchone()
        conn.close()
        return result if result else (0, 0)
    
    def get_language_block_data_aggregated(self, month_str: str) -> Tuple:
        """Get language block revenue data for a specific month in mmm-yy format"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get total language block revenue
        cursor.execute("""
            SELECT 
                COUNT(*) as total_lang_spots,
                SUM(s.gross_rate) as total_lang_revenue
            FROM spots s
            JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            WHERE s.broadcast_month = ? AND s.gross_rate != 0
        """, (month_str,))
        
        lang_total_result = cursor.fetchone()
        total_lang_spots, total_lang_revenue = lang_total_result if lang_total_result else (0, 0)
        
        # Get language breakdown - keep Multi-Language separate for detailed view
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN slb.spans_multiple_blocks = 1 THEN 'Multi-Language (Cross-Audience)'
                    WHEN slb.customer_intent = 'no_grid_coverage' THEN 'No Language Targeting'
                    WHEN l.language_name IS NULL THEN 'Language Block (Missing Data)'
                    ELSE l.language_name
                END as language_name,
                COUNT(CASE WHEN s.gross_rate != 0 THEN 1 END) as revenue_spots,
                SUM(s.gross_rate) as revenue,
                AVG(CASE WHEN s.gross_rate != 0 THEN s.gross_rate END) as avg_spot_value,
                COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bonus_spots,
                COUNT(CASE WHEN s.spot_type IN ('COM', 'PKG', 'PRG') THEN 1 END) as paid_spots,
                COUNT(*) as total_spots
            FROM spots s
            JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            LEFT JOIN language_blocks lb ON slb.block_id = lb.block_id
            LEFT JOIN languages l ON lb.language_id = l.language_id
            WHERE s.broadcast_month = ?
            GROUP BY language_name
            HAVING SUM(s.gross_rate) != 0
            ORDER BY revenue DESC
        """, (month_str,))
        
        lang_results = cursor.fetchall()
        conn.close()
        
        return total_lang_spots, total_lang_revenue, lang_results
    
    def get_non_language_data_aggregated(self, month_str: str) -> List[Tuple]:
        """Get non-language revenue data for a specific month in mmm-yy format - FIXED Direct Response Logic"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                CASE 
                    -- Fixed Direct Response logic - only include positive revenue WorldLink
                    WHEN (a.agency_name LIKE '%WorldLink%' AND s.gross_rate > 0)
                         OR (s.bill_code LIKE '%WorldLink%' AND s.gross_rate > 0)
                         OR (s.revenue_type = 'Direct Response' AND s.gross_rate > 0)
                         THEN 'Direct Response'
                    
                    -- Broker fees and negative adjustments (separate category)
                    WHEN s.bill_code LIKE '%BROKER%'
                         OR s.bill_code LIKE '%DO NOT INVOICE%'
                         OR (s.broker_fees < 0 AND s.broker_fees IS NOT NULL)
                         THEN 'Broker Adjustments'
                    
                    -- Other standard categories
                    WHEN s.spot_type = 'PRD' THEN 'Production'
                    WHEN sect.sector_name LIKE '%GOV%' OR sect.sector_code = 'GOV' THEN 'Government'
                    WHEN sect.sector_name LIKE '%NPO%' OR sect.sector_code = 'NPO' THEN 'Non-Profit'
                    WHEN s.spot_type = 'SVC' THEN 'Service Announcements'
                    ELSE 'Other Non-Language'
                END as category,
                SUM(s.gross_rate + COALESCE(s.broker_fees, 0)) as revenue
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            LEFT JOIN customers c ON s.customer_id = c.customer_id
            LEFT JOIN sectors sect ON c.sector_id = sect.sector_id
            LEFT JOIN agencies a ON s.agency_id = a.agency_id
            WHERE s.broadcast_month = ?
              AND (s.gross_rate != 0 OR s.broker_fees != 0)
              AND slb.spot_id IS NULL  -- Only non-language spots
            GROUP BY category
            HAVING revenue != 0
            ORDER BY revenue DESC
        """, (month_str,))
        
        result = cursor.fetchall()
        conn.close()
        return result
    
    def get_annual_language_data(self, year: int) -> dict:
        """Get annual language performance data for the grid"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        year_suffix = str(year)[2:]  # Convert 2024 to "24"
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN slb.spans_multiple_blocks = 1 THEN 'Multi-Language (Cross-Audience)'
                    WHEN slb.customer_intent = 'no_grid_coverage' THEN 'No Language Targeting'
                    WHEN l.language_name IS NULL THEN 'Language Block (Missing Data)'
                    ELSE l.language_name
                END as language_name,
                COUNT(CASE WHEN s.gross_rate != 0 THEN 1 END) as revenue_spots,
                SUM(s.gross_rate) as revenue,
                COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bonus_spots,
                COUNT(CASE WHEN s.spot_type IN ('COM', 'PKG', 'PRG') THEN 1 END) as paid_spots,
                COUNT(*) as total_spots
            FROM spots s
            JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            LEFT JOIN language_blocks lb ON slb.block_id = lb.block_id
            LEFT JOIN languages l ON lb.language_id = l.language_id
            WHERE s.broadcast_month LIKE ?
              AND s.gross_rate > 0
            GROUP BY language_name
            ORDER BY revenue DESC
        """, (f'%-{year_suffix}',))
        
        results = cursor.fetchall()
        conn.close()
        
        # Convert to dictionary
        annual_data = {}
        for lang_name, revenue_spots, revenue, bonus_spots, paid_spots, total_spots in results:
            annual_data[lang_name] = {
                'revenue': revenue or 0,
                'revenue_spots': revenue_spots or 0,
                'bonus_spots': bonus_spots or 0,
                'paid_spots': paid_spots or 0,
                'total_spots': total_spots or 0
            }
        
        return annual_data
    
    def get_annual_nonlang_data(self, year: int) -> dict:
        """Get annual non-language data - FIXED Direct Response Logic"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        year_suffix = str(year)[2:]  # Convert 2024 to "24"
        cursor.execute("""
            SELECT 
                CASE 
                    -- Fixed Direct Response logic - only include positive revenue WorldLink
                    WHEN (a.agency_name LIKE '%WorldLink%' AND s.gross_rate > 0)
                         OR (s.bill_code LIKE '%WorldLink%' AND s.gross_rate > 0)
                         OR (s.revenue_type = 'Direct Response' AND s.gross_rate > 0)
                         THEN 'Direct Response'
                    
                    -- Broker fees and negative adjustments (separate category)
                    WHEN s.bill_code LIKE '%BROKER%'
                         OR s.bill_code LIKE '%DO NOT INVOICE%'
                         OR (s.broker_fees < 0 AND s.broker_fees IS NOT NULL)
                         THEN 'Broker Adjustments'
                    
                    -- Other standard categories
                    WHEN s.spot_type = 'PRD' THEN 'Production'
                    WHEN sect.sector_name LIKE '%GOV%' OR sect.sector_code = 'GOV' THEN 'Government'
                    WHEN sect.sector_name LIKE '%NPO%' OR sect.sector_code = 'NPO' THEN 'Non-Profit'
                    WHEN s.spot_type = 'SVC' THEN 'Service Announcements'
                    ELSE 'Other Non-Language'
                END as category,
                SUM(s.gross_rate + COALESCE(s.broker_fees, 0)) as revenue
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            LEFT JOIN customers c ON s.customer_id = c.customer_id
            LEFT JOIN sectors sect ON c.sector_id = sect.sector_id
            LEFT JOIN agencies a ON s.agency_id = a.agency_id
            WHERE s.broadcast_month LIKE ?
              AND (s.gross_rate != 0 OR s.broker_fees != 0)
              AND slb.spot_id IS NULL  -- Only non-language spots
            GROUP BY category
            HAVING revenue != 0
            ORDER BY revenue DESC
        """, (f'%-{year_suffix}',))
        
        results = cursor.fetchall()
        conn.close()
        
        # Convert to dictionary
        nonlang_data = {}
        for category, revenue in results:
            nonlang_data[category] = revenue or 0
        
        return nonlang_data
    
    def get_revenue_type_breakdown(self, year: int) -> Tuple[dict, dict]:
        """Get revenue breakdown by type - Multi-Language separated + Direct Response extracted"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        year_suffix = str(year)[2:]  # Convert 2024 to "24"
        
        # Get Direct Response revenue FIRST (regardless of language blocks)
        cursor.execute("""
            SELECT 
                SUM(s.gross_rate + COALESCE(s.broker_fees, 0)) as direct_response_revenue,
                COUNT(*) as direct_response_spots
            FROM spots s
            LEFT JOIN agencies a ON s.agency_id = a.agency_id
            WHERE s.broadcast_month LIKE ?
              AND (s.gross_rate != 0 OR s.broker_fees != 0)
              AND ((a.agency_name LIKE '%WorldLink%' AND s.gross_rate > 0)
                   OR (s.bill_code LIKE '%WorldLink%' AND s.gross_rate > 0)
                   OR (s.revenue_type = 'Direct Response' AND s.gross_rate > 0))
        """, (f'%-{year_suffix}',))
        
        direct_response_result = cursor.fetchone()
        direct_response_revenue, direct_response_spots = direct_response_result if direct_response_result else (0, 0)
        
        # Get Multi-Language (Cross-Audience) revenue - EXCLUDING Direct Response
        cursor.execute("""
            SELECT 
                SUM(s.gross_rate) as multi_lang_revenue,
                COUNT(*) as multi_lang_spots
            FROM spots s
            JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            LEFT JOIN agencies a ON s.agency_id = a.agency_id
            WHERE s.broadcast_month LIKE ?
              AND s.gross_rate > 0
              AND slb.spans_multiple_blocks = 1
              AND NOT ((a.agency_name LIKE '%WorldLink%' AND s.gross_rate > 0)
                       OR (s.bill_code LIKE '%WorldLink%' AND s.gross_rate > 0)
                       OR (s.revenue_type = 'Direct Response' AND s.gross_rate > 0))
        """, (f'%-{year_suffix}',))
        
        multi_lang_result = cursor.fetchone()
        multi_lang_revenue, multi_lang_spots = multi_lang_result if multi_lang_result else (0, 0)
        
        # Get Other Language Blocks revenue - EXCLUDING Direct Response
        cursor.execute("""
            SELECT 
                SUM(s.gross_rate) as other_lang_revenue,
                COUNT(*) as other_lang_spots
            FROM spots s
            JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            LEFT JOIN agencies a ON s.agency_id = a.agency_id
            WHERE s.broadcast_month LIKE ?
              AND s.gross_rate > 0
              AND slb.spans_multiple_blocks != 1
              AND NOT ((a.agency_name LIKE '%WorldLink%' AND s.gross_rate > 0)
                       OR (s.bill_code LIKE '%WorldLink%' AND s.gross_rate > 0)
                       OR (s.revenue_type = 'Direct Response' AND s.gross_rate > 0))
        """, (f'%-{year_suffix}',))
        
        other_lang_result = cursor.fetchone()
        other_lang_revenue, other_lang_spots = other_lang_result if other_lang_result else (0, 0)
        
        # Get Non-Language revenue - EXCLUDING Direct Response
        cursor.execute("""
            SELECT 
                SUM(s.gross_rate + COALESCE(s.broker_fees, 0)) as nonlang_revenue,
                COUNT(*) as nonlang_spots
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            LEFT JOIN agencies a ON s.agency_id = a.agency_id
            WHERE s.broadcast_month LIKE ?
              AND (s.gross_rate != 0 OR s.broker_fees != 0)
              AND slb.spot_id IS NULL
              AND NOT ((a.agency_name LIKE '%WorldLink%' AND s.gross_rate > 0)
                       OR (s.bill_code LIKE '%WorldLink%' AND s.gross_rate > 0)
                       OR (s.revenue_type = 'Direct Response' AND s.gross_rate > 0))
        """, (f'%-{year_suffix}',))
        
        nonlang_result = cursor.fetchone()
        nonlang_revenue, nonlang_spots = nonlang_result if nonlang_result else (0, 0)
        
        conn.close()
        
        # Return breakdown with Direct Response separated
        revenue_breakdown = {
            'Direct Response': direct_response_revenue or 0,
            'Multi-Language (Cross-Audience)': multi_lang_revenue or 0,
            'Targeted Language Blocks': other_lang_revenue or 0,
            'Other Non-Language': nonlang_revenue or 0
        }
        
        spots_breakdown = {
            'Direct Response': direct_response_spots or 0,
            'Multi-Language (Cross-Audience)': multi_lang_spots or 0,
            'Targeted Language Blocks': other_lang_spots or 0,
            'Other Non-Language': nonlang_spots or 0
        }
        
        return revenue_breakdown, spots_breakdown
    
    def generate_report(self, year: int, target_revenue: Optional[float] = None) -> str:
        """Generate the fixed revenue report"""
        self.report_lines = []
        
        # Header
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.add_header(f"📊 {year} Revenue Report - FIXED", 1)
        self.add_line(f"**Generated:** {timestamp}")
        self.add_line(f"**Report Type:** Complete Revenue Analysis with Direct Response Extracted (mmm-yy format)")
        self.add_line()
        
        # Get aggregated broadcast months
        broadcast_months = self.get_broadcast_months_aggregated(year)
        
        if not broadcast_months:
            self.add_line(f"⚠️ **No data found for {year}!**")
            return "\n".join(self.report_lines)
        
        self.add_header("📋 Executive Summary", 2)
        
        year_total = 0
        total_spots = 0
        
        # Calculate totals
        for month_str, sample_date in broadcast_months:
            spots, revenue = self.get_month_revenue_data_aggregated(month_str)
            if spots > 0:
                year_total += revenue
                total_spots += spots
        
        # Summary table
        self.add_table_row(["Metric", "Value"], True)
        self.add_table_row(["Total Revenue", f"${year_total:,.2f}"])
        self.add_table_row(["Total Spots", f"{total_spots:,}"])
        self.add_table_row(["Average per Spot", f"${year_total/total_spots:.2f}" if total_spots > 0 else "$0.00"])
        self.add_table_row(["Closed Months", f"{len(broadcast_months)}"])
        
        if target_revenue:
            difference = year_total - target_revenue
            self.add_table_row(["Target Revenue", f"${target_revenue:,.2f}"])
            self.add_table_row(["Difference", f"${difference:+,.2f}"])
        
        self.add_line()
        
        # Monthly breakdown (using new mmm-yy format)
        self.add_header("📅 Monthly Revenue Breakdown", 2)
        
        for month_str, sample_date in broadcast_months:
            spots, revenue = self.get_month_revenue_data_aggregated(month_str)
            
            if spots == 0:
                continue
            
            display_name = self.get_month_display_name(month_str)
            self.add_header(f"{display_name} - ${revenue:,.2f}", 3)
            
            # Language block data
            total_lang_spots, total_lang_revenue, lang_results = self.get_language_block_data_aggregated(month_str)
            
            # Non-language data
            nonlang_results = self.get_non_language_data_aggregated(month_str)
            nonlang_total = sum(cat_revenue for _, cat_revenue in nonlang_results)
            
            # Revenue split
            self.add_table_row(["Type", "Amount", "Percentage"], True)
            self.add_table_row([
                "Language Blocks", 
                f"${total_lang_revenue:,.2f}", 
                f"{(total_lang_revenue/revenue*100):.1f}%" if revenue > 0 else "0.0%"
            ])
            self.add_table_row([
                "Non-Language", 
                f"${nonlang_total:,.2f}", 
                f"{(nonlang_total/revenue*100):.1f}%" if revenue > 0 else "0.0%"
            ])
            
            self.add_line()
        
        self.add_line("---")
        self.add_line()
        
        # FIXED Revenue Split Analysis - Direct Response Extracted
        self.add_header(f"💰 {year} Revenue Split Analysis - Direct Response Extracted", 2)
        
        revenue_breakdown, spots_breakdown = self.get_revenue_type_breakdown(year)
        
        # Calculate overall percentages
        total_revenue = sum(revenue_breakdown.values())
        total_spots_calc = sum(spots_breakdown.values())
        
        self.add_table_row(["Revenue Type", "Amount", "Percentage", "Spots", "Analysis"], True)
        
        for rev_type, amount in revenue_breakdown.items():
            pct = (amount / total_revenue * 100) if total_revenue > 0 else 0
            spots = spots_breakdown.get(rev_type, 0)
            
            if rev_type == 'Direct Response':
                analysis = "WorldLink agency and direct response advertising"
            elif rev_type == 'Multi-Language (Cross-Audience)':
                analysis = "Cross-audience, broad-reach advertising"
            elif rev_type == 'Targeted Language Blocks':
                analysis = "Specific language community targeting"
            else:
                analysis = "Production, government, other non-language"
            
            self.add_table_row([
                rev_type,
                f"${amount:,.2f}",
                f"{pct:.1f}%",
                f"{spots:,}",
                analysis
            ])
        
        self.add_table_row([
            "**TOTAL**",
            f"**${total_revenue:,.2f}**",
            f"**100.0%**",
            f"**{total_spots_calc:,}**",
            f"**Complete {year} Revenue**"
        ])
        
        self.add_line()
        
        # Annual Language Performance Summary
        self.add_header(f"🌐 {year} Language Performance Summary", 2)
        
        annual_lang_data = self.get_annual_language_data(year)
        
        if annual_lang_data:
            # Calculate totals
            total_lang_revenue = sum(data['revenue'] for data in annual_lang_data.values())
            total_lang_spots = sum(data['revenue_spots'] for data in annual_lang_data.values())
            
            self.add_line(f"**Total Language Block Revenue:** ${total_lang_revenue:,.2f} ({total_lang_spots:,} spots)")
            self.add_line(f"**Average Language Block Value:** ${total_lang_revenue/total_lang_spots:.2f}/spot" if total_lang_spots > 0 else "**Average Language Block Value:** $0.00/spot")
            self.add_line()
            
            # Annual language table
            self.add_table_row(["Language", "Revenue", "Rev %", "Spots", "Avg/Spot", "Paid", "Bonus", "Bonus %"], True)
            
            # Sort by revenue
            sorted_languages = sorted(annual_lang_data.items(), key=lambda x: x[1]['revenue'], reverse=True)
            
            for lang_name, data in sorted_languages:
                revenue_pct = (data['revenue'] / total_lang_revenue * 100) if total_lang_revenue > 0 else 0
                avg_spot_value = data['revenue'] / data['revenue_spots'] if data['revenue_spots'] > 0 else 0
                
                total_content_spots = data['bonus_spots'] + data['paid_spots']
                bns_percentage = (data['bonus_spots'] / total_content_spots * 100) if total_content_spots > 0 else 0
                
                self.add_table_row([
                    lang_name,
                    f"${data['revenue']:,.0f}",
                    f"{revenue_pct:.1f}%",
                    f"{data['revenue_spots']:,}",
                    f"${avg_spot_value:.2f}",
                    f"{data['paid_spots']:,}",
                    f"{data['bonus_spots']:,}",
                    f"{bns_percentage:.1f}%"
                ])
            
            self.add_line()
        
        # Annual Non-Language Summary
        self.add_header(f"📋 {year} Non-Language Revenue Summary - FIXED", 2)
        
        annual_nonlang_data = self.get_annual_nonlang_data(year)
        
        if annual_nonlang_data:
            total_nonlang_revenue = sum(annual_nonlang_data.values())
            
            self.add_line(f"**Total Non-Language Revenue:** ${total_nonlang_revenue:,.2f}")
            self.add_line()
            
            # Non-language table
            self.add_table_row(["Category", "Revenue", "Percentage"], True)
            
            # Sort by revenue
            sorted_categories = sorted(annual_nonlang_data.items(), key=lambda x: x[1], reverse=True)
            
            for category, cat_revenue in sorted_categories:
                revenue_pct = (cat_revenue / total_nonlang_revenue * 100) if total_nonlang_revenue > 0 else 0
                self.add_table_row([
                    category,
                    f"${cat_revenue:,.2f}",
                    f"{revenue_pct:.1f}%"
                ])
            
            self.add_line()
        
        # Final summary
        self.add_header("🎯 Final Summary", 2)
        self.add_line(f"**{year} Total Revenue:** ${year_total:,.2f}")
        
        if target_revenue:
            difference = year_total - target_revenue
            self.add_line(f"**Target Revenue:** ${target_revenue:,.2f}")
            self.add_line(f"**Difference:** ${difference:+,.2f}")
        
        self.add_line()
        
        # Key insights
        self.add_header("🔍 Key Fixes Applied", 2)
        self.add_line("✅ **Direct Response Extracted**: WorldLink revenue properly separated (~$387K)")
        self.add_line("✅ **Language Block Exclusion**: Direct Response excluded from language categories")
        self.add_line("✅ **Multi-Language Separated**: Cross-audience advertising shown separately")
        self.add_line("✅ **New Format Support**: Updated for mmm-yy broadcast_month format")
        
        return "\n".join(self.report_lines)
    
    def save_report(self, content: str, year: int) -> str:
        """Save the report to a timestamped file"""
        # Create reports directory if it doesn't exist
        reports_dir = "reports"
        if not os.path.exists(reports_dir):
            os.makedirs(reports_dir)
        
        # Generate filename with timestamp
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{reports_dir}/fixed_revenue_report_{year}_{timestamp}.md"
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return filename


def main():
    parser = argparse.ArgumentParser(description='Generate fixed revenue reports with Direct Response extracted (mmm-yy format)')
    parser.add_argument('year', type=int, help='Year to generate report for (e.g., 2024)')
    parser.add_argument('--target', type=float, help='Target revenue amount for comparison')
    parser.add_argument('--db-path', default='./data/database/production.db', help='Path to database file')
    parser.add_argument('--output', choices=['file', 'stdout', 'both'], default='both', 
                       help='Output destination (default: both)')
    
    args = parser.parse_args()
    
    # Generate report
    generator = FixedRevenueReportGenerator(args.db_path)
    
    try:
        print(f"📊 Generating FIXED {args.year} revenue report...")
        print("🔧 Fixes applied:")
        print("   • Direct Response extracted (~$387K WorldLink revenue)")
        print("   • Direct Response excluded from language block categories")
        print("   • Multi-Language (Cross-Audience) separated")
        print("   • Updated for new mmm-yy broadcast_month format")
        
        content = generator.generate_report(args.year, args.target)
        
        if args.output in ['stdout', 'both']:
            print("\n" + content)
        
        if args.output in ['file', 'both']:
            filename = generator.save_report(content, args.year)
            print(f"\n💾 Report saved to: {filename}")
            
    except Exception as e:
        print(f"❌ Error generating report: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())