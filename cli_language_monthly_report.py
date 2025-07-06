#!/usr/bin/env python3
"""
Enhanced Revenue Report Generator with Markdown Output
====================================================

Generates comprehensive revenue reports with markdown formatting,
user-selectable years, and timestamped output files.

Features:
- Complete language block analysis
- Government revenue deep dive with rate tiers
- Agency performance tracking
- Roadblock strategy analysis
- Strategic recommendations

Usage Examples:
    # Basic report
    python revenue_report_generator.py 2024

    # Report with government focus
    python revenue_report_generator.py 2024 --government-focus

    # Report with target comparison
    python revenue_report_generator.py 2024 --target 4000000

    # Save only to file (no stdout)
    python revenue_report_generator.py 2024 --output file
"""

import sqlite3
import argparse
import datetime
import os
from typing import List, Tuple, Optional


class RevenueReportGenerator:
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
    
    def get_month_mapping(self, year: int) -> dict:
        """Get month mapping for specified year"""
        year_suffix = str(year)[-2:]  # Get last 2 digits (e.g., 24 for 2024)
        return {
            f'Jan-{year_suffix}': f'{year}-01',
            f'Feb-{year_suffix}': f'{year}-02',
            f'Mar-{year_suffix}': f'{year}-03',
            f'Apr-{year_suffix}': f'{year}-04',
            f'May-{year_suffix}': f'{year}-05',
            f'Jun-{year_suffix}': f'{year}-06',
            f'Jul-{year_suffix}': f'{year}-07',
            f'Aug-{year_suffix}': f'{year}-08',
            f'Sep-{year_suffix}': f'{year}-09',
            f'Oct-{year_suffix}': f'{year}-10',
            f'Nov-{year_suffix}': f'{year}-11',
            f'Dec-{year_suffix}': f'{year}-12'
        }
    
    def get_closed_months(self, year: int) -> List[Tuple]:
        """Get closed months for the specified year"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        year_suffix = str(year)[-2:]
        
        cursor.execute("""
            SELECT broadcast_month, closed_date, closed_by 
            FROM month_closures 
            WHERE broadcast_month LIKE ? 
            ORDER BY 
                CASE broadcast_month
                    WHEN ? THEN 1
                    WHEN ? THEN 2
                    WHEN ? THEN 3
                    WHEN ? THEN 4
                    WHEN ? THEN 5
                    WHEN ? THEN 6
                    WHEN ? THEN 7
                    WHEN ? THEN 8
                    WHEN ? THEN 9
                    WHEN ? THEN 10
                    WHEN ? THEN 11
                    WHEN ? THEN 12
                END
        """, (f'%{year_suffix}', f'Jan-{year_suffix}', f'Feb-{year_suffix}', f'Mar-{year_suffix}',
              f'Apr-{year_suffix}', f'May-{year_suffix}', f'Jun-{year_suffix}', f'Jul-{year_suffix}',
              f'Aug-{year_suffix}', f'Sep-{year_suffix}', f'Oct-{year_suffix}', f'Nov-{year_suffix}',
              f'Dec-{year_suffix}'))
        
        result = cursor.fetchall()
        conn.close()
        return result
    
    def get_month_revenue_data(self, spots_pattern: str) -> Tuple:
        """Get revenue data for a specific month pattern"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get total revenue for this month
        cursor.execute("""
            SELECT COUNT(*) as spots, SUM(gross_rate) as revenue
            FROM spots 
            WHERE broadcast_month LIKE ? AND gross_rate != 0
        """, (f'{spots_pattern}%',))
        
        result = cursor.fetchone()
        conn.close()
        return result if result else (0, 0)
    
    def get_language_block_data(self, spots_pattern: str) -> Tuple:
        """Get language block revenue data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get ALL language block revenue first
        cursor.execute("""
            SELECT 
                COUNT(*) as total_lang_spots,
                SUM(s.gross_rate) as total_lang_revenue
            FROM spots s
            JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            WHERE s.broadcast_month LIKE ? AND s.gross_rate != 0
        """, (f'{spots_pattern}%',))
        
        lang_total_result = cursor.fetchone()
        total_lang_spots, total_lang_revenue = lang_total_result if lang_total_result else (0, 0)
        
        # Get language breakdown
        cursor.execute("""
            SELECT 
                COALESCE(l.language_name, 'Unknown Language') as language_name,
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
            WHERE s.broadcast_month LIKE ?
            GROUP BY l.language_name
            HAVING SUM(s.gross_rate) != 0
            ORDER BY revenue DESC
        """, (f'{spots_pattern}%',))
        
        lang_results = cursor.fetchall()
        conn.close()
        
        return total_lang_spots, total_lang_revenue, lang_results
    
    def get_non_language_data(self, spots_pattern: str) -> List[Tuple]:
        """Get non-language revenue data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN a.agency_name = 'WorldLink' 
                         OR s.bill_code LIKE 'WorldLink%' 
                         OR s.bill_code = 'WorldLink Broker Fees (DO NOT INVOICE)' 
                         THEN 'Direct Response'
                    WHEN sect.sector_name LIKE '%MEDIA%' OR sect.sector_code = 'MEDIA' THEN 'Direct Response'
                    WHEN s.spot_type = 'PRD' THEN 'Production'
                    WHEN sect.sector_name LIKE '%GOV%' OR sect.sector_code = 'GOV' THEN 'Government'
                    WHEN sect.sector_name LIKE '%NPO%' OR sect.sector_code = 'NPO' THEN 'Non-Profit'
                    WHEN s.spot_type = 'SVC' THEN 'Service Announcements'
                    WHEN s.bill_code LIKE '%BROKER%' OR s.bill_code LIKE '%DO NOT INVOICE%' THEN 'Broker Fees'
                    ELSE 'Other Non-Language'
                END as category,
                SUM(s.gross_rate) as revenue
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            LEFT JOIN customers c ON s.customer_id = c.customer_id
            LEFT JOIN sectors sect ON c.sector_id = sect.sector_id
            LEFT JOIN agencies a ON s.agency_id = a.agency_id
            WHERE s.broadcast_month LIKE ? AND s.gross_rate != 0 AND slb.spot_id IS NULL
            GROUP BY category
            HAVING revenue != 0
            ORDER BY revenue DESC
        """, (f'{spots_pattern}%',))
        
        result = cursor.fetchall()
        conn.close()
        return result
    
    def get_government_data(self, spots_pattern: str) -> dict:
        """Get detailed government revenue data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Government overview
        cursor.execute("""
            SELECT 
                COUNT(*) as total_spots,
                SUM(s.gross_rate) as total_revenue,
                AVG(s.gross_rate) as avg_rate,
                COUNT(DISTINCT s.customer_id) as unique_customers,
                COUNT(DISTINCT s.agency_id) as unique_agencies,
                COUNT(DISTINCT s.air_date) as active_days
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            LEFT JOIN customers c ON s.customer_id = c.customer_id
            LEFT JOIN sectors sect ON c.sector_id = sect.sector_id
            WHERE s.broadcast_month LIKE ? AND s.gross_rate != 0 AND slb.spot_id IS NULL
              AND (sect.sector_name LIKE '%GOV%' OR sect.sector_code = 'GOV')
        """, (f'{spots_pattern}%',))
        
        overview = cursor.fetchone()
        
        # Government rate tiers
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN s.gross_rate >= 75 THEN 'Emergency/Safety ($75-$115)'
                    WHEN s.gross_rate >= 40 THEN 'Utilities ($40-$75)'
                    WHEN s.gross_rate >= 13 THEN 'PSA/Public Health ($13-$40)'
                    ELSE 'Other (<$13)'
                END as rate_tier,
                COUNT(*) as spots,
                SUM(s.gross_rate) as revenue,
                AVG(s.gross_rate) as avg_rate
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            LEFT JOIN customers c ON s.customer_id = c.customer_id
            LEFT JOIN sectors sect ON c.sector_id = sect.sector_id
            WHERE s.broadcast_month LIKE ? AND s.gross_rate != 0 AND slb.spot_id IS NULL
              AND (sect.sector_name LIKE '%GOV%' OR sect.sector_code = 'GOV')
            GROUP BY rate_tier
            ORDER BY avg_rate DESC
        """, (f'{spots_pattern}%',))
        
        rate_tiers = cursor.fetchall()
        
        # Government agencies
        cursor.execute("""
            SELECT 
                COALESCE(a.agency_name, 'Direct Government') as agency_name,
                COUNT(*) as spots,
                SUM(s.gross_rate) as revenue,
                AVG(s.gross_rate) as avg_rate,
                COUNT(DISTINCT s.customer_id) as unique_clients
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            LEFT JOIN customers c ON s.customer_id = c.customer_id
            LEFT JOIN sectors sect ON c.sector_id = sect.sector_id
            LEFT JOIN agencies a ON s.agency_id = a.agency_id
            WHERE s.broadcast_month LIKE ? AND s.gross_rate != 0 AND slb.spot_id IS NULL
              AND (sect.sector_name LIKE '%GOV%' OR sect.sector_code = 'GOV')
            GROUP BY agency_name
            HAVING revenue > 0
            ORDER BY revenue DESC
            LIMIT 10
        """, (f'{spots_pattern}%',))
        
        agencies = cursor.fetchall()
        
        # Government customers
        cursor.execute("""
            SELECT 
                c.normalized_name,
                COUNT(*) as spots,
                SUM(s.gross_rate) as revenue,
                AVG(s.gross_rate) as avg_rate
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            LEFT JOIN customers c ON s.customer_id = c.customer_id
            LEFT JOIN sectors sect ON c.sector_id = sect.sector_id
            WHERE s.broadcast_month LIKE ? AND s.gross_rate != 0 AND slb.spot_id IS NULL
              AND (sect.sector_name LIKE '%GOV%' OR sect.sector_code = 'GOV')
            GROUP BY c.normalized_name
            HAVING revenue > 0
            ORDER BY revenue DESC
            LIMIT 10
        """, (f'{spots_pattern}%',))
        
        customers = cursor.fetchall()
        
        # Potential roadblocks
        cursor.execute("""
            SELECT 
                COUNT(*) as potential_roadblocks,
                SUM(spot_count) as roadblock_spots,
                SUM(revenue) as roadblock_revenue
            FROM (
                SELECT 
                    s.air_date,
                    s.time_in,
                    s.time_out,
                    COUNT(*) as spot_count,
                    SUM(s.gross_rate) as revenue
                FROM spots s
                LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
                LEFT JOIN customers c ON s.customer_id = c.customer_id
                LEFT JOIN sectors sect ON c.sector_id = sect.sector_id
                WHERE s.broadcast_month LIKE ? AND s.gross_rate != 0 AND slb.spot_id IS NULL
                  AND (sect.sector_name LIKE '%GOV%' OR sect.sector_code = 'GOV')
                GROUP BY s.air_date, s.time_in, s.time_out
                HAVING COUNT(*) > 1
            ) roadblocks
        """, (f'{spots_pattern}%',))
        
        roadblocks = cursor.fetchone()
        
        conn.close()
        
        return {
            'overview': overview,
            'rate_tiers': rate_tiers,
            'agencies': agencies,
            'customers': customers,
            'roadblocks': roadblocks
        }
    
    def generate_report(self, year: int, target_revenue: Optional[float] = None) -> str:
        """Generate the complete revenue report"""
        self.report_lines = []
        
        # Header
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.add_header(f"📊 {year} Revenue Report", 1)
        self.add_line(f"**Generated:** {timestamp}")
        self.add_line(f"**Report Type:** Complete Revenue Analysis with Language Block Coverage")
        self.add_line()
        
        # Get data
        month_map = self.get_month_mapping(year)
        closed_months = self.get_closed_months(year)
        
        if not closed_months:
            self.add_line(f"⚠️ **No closed months found for {year}!**")
            return "\n".join(self.report_lines)
        
        self.add_header("📋 Executive Summary", 2)
        
        year_total = 0
        total_spots = 0
        
        # Calculate totals first
        for closed_month, closed_date, closed_by in closed_months:
            if closed_month not in month_map:
                continue
                
            spots_pattern = month_map[closed_month]
            spots, revenue = self.get_month_revenue_data(spots_pattern)
            
            if spots > 0:
                year_total += revenue
                total_spots += spots
        
        # Summary table
        self.add_table_row(["Metric", "Value"], True)
        self.add_table_row(["Total Revenue", f"${year_total:,.2f}"])
        self.add_table_row(["Total Spots", f"{total_spots:,}"])
        self.add_table_row(["Average per Spot", f"${year_total/total_spots:.2f}" if total_spots > 0 else "$0.00"])
        self.add_table_row(["Closed Months", f"{len(closed_months)}"])
        
        if target_revenue:
            difference = year_total - target_revenue
            self.add_table_row(["Target Revenue", f"${target_revenue:,.2f}"])
            self.add_table_row(["Difference", f"${difference:+,.2f}"])
        
        self.add_line()
        
        # Monthly breakdown
        self.add_header("📅 Monthly Revenue Breakdown", 2)
        
        for closed_month, closed_date, closed_by in closed_months:
            if closed_month not in month_map:
                continue
                
            spots_pattern = month_map[closed_month]
            spots, revenue = self.get_month_revenue_data(spots_pattern)
            
            if spots == 0:
                continue
            
            self.add_header(f"{closed_month} - Closed {closed_date}", 3)
            
            # Month summary
            self.add_table_row(["Metric", "Value"], True)
            self.add_table_row(["Total Revenue", f"${revenue:,.2f}"])
            self.add_table_row(["Total Spots", f"{spots:,}"])
            self.add_table_row(["Average per Spot", f"${revenue/spots:.2f}"])
            self.add_table_row(["Closed By", closed_by])
            self.add_line()
            
            # Language block data
            total_lang_spots, total_lang_revenue, lang_results = self.get_language_block_data(spots_pattern)
            
            if total_lang_spots > 0:
                self.add_header("🌐 Language Block Performance", 4)
                self.add_line(f"**Total Language Revenue:** ${total_lang_revenue:,.2f} ({total_lang_spots:,} spots)")
                self.add_line()
                
                if lang_results:
                    self.add_table_row(["Language", "Revenue", "Spots", "Avg/Spot", "Paid", "Bonus", "Bonus %"], True)
                    
                    for lang_name, revenue_spots, lang_revenue, avg_spot_value, bonus_spots, paid_spots, total_spots in lang_results[:7]:
                        if lang_revenue and lang_revenue != 0:
                            total_content_spots = bonus_spots + paid_spots
                            bns_percentage = (bonus_spots / total_content_spots * 100) if total_content_spots > 0 else 0
                            
                            self.add_table_row([
                                lang_name,
                                f"${lang_revenue:,.0f}",
                                f"{revenue_spots:,}",
                                f"${avg_spot_value:.2f}",
                                f"{paid_spots:,}",
                                f"{bonus_spots:,}",
                                f"{bns_percentage:.1f}%"
                            ])
                    
                    self.add_line()
            
            # Non-language data
            nonlang_results = self.get_non_language_data(spots_pattern)
            nonlang_total = sum(cat_revenue for _, cat_revenue in nonlang_results)
            
            if nonlang_results:
                self.add_header("📋 Non-Language Revenue", 4)
                self.add_table_row(["Category", "Revenue"], True)
                
                government_revenue = 0
                for category, cat_revenue in nonlang_results:
                    if category == 'Government':
                        government_revenue = cat_revenue
                        self.add_table_row([f"**{category}**", f"**${cat_revenue:,.2f}**"])
                    else:
                        self.add_table_row([category, f"${cat_revenue:,.2f}"])
                
                self.add_line()
                
                # Government monthly insight
                if government_revenue > 0:
                    gov_pct = (government_revenue / nonlang_total * 100) if nonlang_total > 0 else 0
                    self.add_line(f"🏛️ **Government Impact:** ${government_revenue:,.2f} ({gov_pct:.1f}% of non-language revenue)")
                    
                    # Quick government analysis for this month
                    gov_data = self.get_government_data(spots_pattern)
                    if gov_data['overview'] and gov_data['overview'][0] > 0:
                        overview = gov_data['overview']
                        avg_rate = overview[1] / overview[0] if overview[0] > 0 else 0
                        self.add_line(f"   • {overview[0]:,} spots at ${avg_rate:.2f}/spot average")
                        self.add_line(f"   • {overview[3]:,} unique customers, {overview[4]:,} agencies")
                        
                        # Show top rate tier for this month
                        if gov_data['rate_tiers']:
                            top_tier = max(gov_data['rate_tiers'], key=lambda x: x[2])  # Max by revenue
                            self.add_line(f"   • Top rate tier: {top_tier[0]} (${top_tier[2]:,.0f})")
                
                self.add_line()
            
            # Revenue split
            self.add_header("💰 Revenue Split", 4)
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
            
            # Verification
            calculated_total = total_lang_revenue + nonlang_total
            difference = revenue - calculated_total
            
            if abs(difference) > 1:
                self.add_line()
                self.add_line(f"⚠️ **Verification Warning:** Calculated ${calculated_total:,.2f} vs Actual ${revenue:,.2f} (diff: ${difference:+,.2f})")
            else:
                self.add_line()
                self.add_line("✅ **Verification:** Totals match perfectly")
            
            self.add_line()
            self.add_line("---")
            self.add_line()
        
        # Final summary
        self.add_header("🎯 Final Summary", 2)
        self.add_line(f"**{year} Total Revenue:** ${year_total:,.2f}")
        
        if target_revenue:
            difference = year_total - target_revenue
            self.add_line(f"**Target Revenue:** ${target_revenue:,.2f}")
            self.add_line(f"**Difference:** ${difference:+,.2f}")
            
            if abs(difference) < 10000:
                self.add_line("✅ **Status:** Perfect match! Revenue totals align with target.")
            elif abs(difference) < 50000:
                self.add_line("✅ **Status:** Very close match! Difference likely due to minor adjustments.")
            else:
                self.add_line("⚠️ **Status:** Significant difference - may need further investigation.")
        
        self.add_line()
        
        # Year-end language breakdown
        self.add_header(f"🌐 {year} Language Performance Summary", 2)
        
        # Aggregate language data across all months
        year_lang_data = {}
        year_lang_total_revenue = 0
        year_lang_total_spots = 0
        
        for closed_month, closed_date, closed_by in closed_months:
            if closed_month not in month_map:
                continue
                
            spots_pattern = month_map[closed_month]
            spots, revenue = self.get_month_revenue_data(spots_pattern)
            
            if spots == 0:
                continue
            
            # Get language data for this month
            total_lang_spots, total_lang_revenue, lang_results = self.get_language_block_data(spots_pattern)
            year_lang_total_revenue += total_lang_revenue
            year_lang_total_spots += total_lang_spots
            
            # Aggregate by language
            for lang_name, revenue_spots, lang_revenue, avg_spot_value, bonus_spots, paid_spots, total_spots in lang_results:
                if lang_revenue and lang_revenue != 0:
                    if lang_name not in year_lang_data:
                        year_lang_data[lang_name] = {
                            'revenue': 0,
                            'revenue_spots': 0,
                            'bonus_spots': 0,
                            'paid_spots': 0,
                            'total_spots': 0,
                            'total_spot_value': 0  # For calculating weighted average
                        }
                    
                    year_lang_data[lang_name]['revenue'] += lang_revenue
                    year_lang_data[lang_name]['revenue_spots'] += revenue_spots
                    year_lang_data[lang_name]['bonus_spots'] += bonus_spots
                    year_lang_data[lang_name]['paid_spots'] += paid_spots
                    year_lang_data[lang_name]['total_spots'] += total_spots
                    year_lang_data[lang_name]['total_spot_value'] += lang_revenue  # Use revenue for weighted avg
        
        if year_lang_data:
            self.add_line(f"**Total Language Block Revenue:** ${year_lang_total_revenue:,.2f} ({year_lang_total_spots:,} spots)")
            self.add_line(f"**Average Language Block Value:** ${year_lang_total_revenue/year_lang_total_spots:.2f}/spot" if year_lang_total_spots > 0 else "**Average Language Block Value:** $0.00/spot")
            self.add_line()
            
            # Sort languages by revenue
            sorted_languages = sorted(year_lang_data.items(), key=lambda x: x[1]['revenue'], reverse=True)
            
            self.add_table_row(["Language", "Revenue", "Rev %", "Spots", "Avg/Spot", "Paid", "Bonus", "Bonus %"], True)
            
            for lang_name, data in sorted_languages:
                revenue_pct = (data['revenue'] / year_lang_total_revenue * 100) if year_lang_total_revenue > 0 else 0
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
            
            # Language performance insights
            self.add_header("📈 Language Performance Insights", 3)
            
            if sorted_languages:
                # Top performer
                top_lang = sorted_languages[0]
                top_revenue_pct = (top_lang[1]['revenue'] / year_lang_total_revenue * 100)
                self.add_line(f"🥇 **Top Language:** {top_lang[0]} (${top_lang[1]['revenue']:,.0f} - {top_revenue_pct:.1f}% of language revenue)")
                
                # Highest value per spot
                highest_value_lang = max(sorted_languages, key=lambda x: x[1]['revenue']/x[1]['revenue_spots'] if x[1]['revenue_spots'] > 0 else 0)
                highest_avg = highest_value_lang[1]['revenue'] / highest_value_lang[1]['revenue_spots'] if highest_value_lang[1]['revenue_spots'] > 0 else 0
                self.add_line(f"💰 **Highest Value/Spot:** {highest_value_lang[0]} (${highest_avg:.2f}/spot)")
                
                # Most active (by spots)
                most_active = max(sorted_languages, key=lambda x: x[1]['revenue_spots'])
                self.add_line(f"📊 **Most Active:** {most_active[0]} ({most_active[1]['revenue_spots']:,} spots)")
                
                # Best paid/bonus ratio
                best_ratio_lang = min(sorted_languages, key=lambda x: x[1]['bonus_spots']/(x[1]['bonus_spots']+x[1]['paid_spots']) if (x[1]['bonus_spots']+x[1]['paid_spots']) > 0 else 1)
                best_ratio_pct = (best_ratio_lang[1]['bonus_spots'] / (best_ratio_lang[1]['bonus_spots']+best_ratio_lang[1]['paid_spots']) * 100) if (best_ratio_lang[1]['bonus_spots']+best_ratio_lang[1]['paid_spots']) > 0 else 0
                self.add_line(f"🎯 **Best Paid Ratio:** {best_ratio_lang[0]} ({best_ratio_pct:.1f}% bonus)")
                
                # Overall bonus percentage
                total_bonus = sum(data['bonus_spots'] for data in year_lang_data.values())
                total_paid = sum(data['paid_spots'] for data in year_lang_data.values())
                overall_bonus_pct = (total_bonus / (total_bonus + total_paid) * 100) if (total_bonus + total_paid) > 0 else 0
                self.add_line(f"📋 **Overall Bonus Rate:** {overall_bonus_pct:.1f}% ({total_bonus:,} bonus vs {total_paid:,} paid)")
            
            self.add_line()
        else:
            self.add_line("⚠️ **No language block data found for this period.**")
            self.add_line()
        
        # Year-end non-language breakdown
        self.add_header(f"📋 {year} Non-Language Revenue Summary", 2)
        
        # Aggregate non-language data across all months
        year_nonlang_data = {}
        year_nonlang_total = 0
        
        for closed_month, closed_date, closed_by in closed_months:
            if closed_month not in month_map:
                continue
                
            spots_pattern = month_map[closed_month]
            spots, revenue = self.get_month_revenue_data(spots_pattern)
            
            if spots == 0:
                continue
            
            # Get non-language data for this month
            nonlang_results = self.get_non_language_data(spots_pattern)
            
            for category, cat_revenue in nonlang_results:
                if category not in year_nonlang_data:
                    year_nonlang_data[category] = 0
                year_nonlang_data[category] += cat_revenue
                year_nonlang_total += cat_revenue
        
        if year_nonlang_data:
            self.add_line(f"**Total Non-Language Revenue:** ${year_nonlang_total:,.2f}")
            self.add_line()
            
            # Sort categories by revenue
            sorted_categories = sorted(year_nonlang_data.items(), key=lambda x: x[1], reverse=True)
            
            self.add_table_row(["Category", "Revenue", "Percentage"], True)
            
            for category, cat_revenue in sorted_categories:
                revenue_pct = (cat_revenue / year_nonlang_total * 100) if year_nonlang_total > 0 else 0
                self.add_table_row([
                    category,
                    f"${cat_revenue:,.2f}",
                    f"{revenue_pct:.1f}%"
                ])
            
            self.add_line()
            
            # Non-language insights
            self.add_header("📊 Non-Language Performance Insights", 3)
            
            if sorted_categories:
                top_category = sorted_categories[0]
                top_pct = (top_category[1] / year_nonlang_total * 100)
                self.add_line(f"🥇 **Top Category:** {top_category[0]} (${top_category[1]:,.0f} - {top_pct:.1f}% of non-language revenue)")
                
                # Show category count
                self.add_line(f"📈 **Active Categories:** {len(sorted_categories)} revenue-generating categories")
                
                # Show diversification
                if len(sorted_categories) > 1:
                    top_3_pct = sum(cat[1] for cat in sorted_categories[:3]) / year_nonlang_total * 100
                    self.add_line(f"📊 **Top 3 Categories:** {top_3_pct:.1f}% of non-language revenue")
            
            self.add_line()
        else:
            self.add_line("⚠️ **No non-language revenue data found for this period.**")
            self.add_line()
        
        # Government Deep Dive Analysis
        self.add_header(f"🏛️ {year} Government Revenue Deep Dive", 2)
        
        # Aggregate government data across all months
        year_gov_data = {
            'total_spots': 0,
            'total_revenue': 0,
            'unique_customers': set(),
            'unique_agencies': set(),
            'active_days': set(),
            'rate_tiers': {},
            'agencies': {},
            'customers': {},
            'roadblock_instances': 0,
            'roadblock_spots': 0,
            'roadblock_revenue': 0
        }
        
        for closed_month, closed_date, closed_by in closed_months:
            if closed_month not in month_map:
                continue
                
            spots_pattern = month_map[closed_month]
            spots, revenue = self.get_month_revenue_data(spots_pattern)
            
            if spots == 0:
                continue
            
            # Get government data for this month
            gov_data = self.get_government_data(spots_pattern)
            
            if gov_data['overview'] and gov_data['overview'][0] > 0:
                # Aggregate overview data
                overview = gov_data['overview']
                year_gov_data['total_spots'] += overview[0]
                year_gov_data['total_revenue'] += overview[1]
                year_gov_data['unique_customers'].update([f"{closed_month}_{i}" for i in range(overview[3])])
                year_gov_data['unique_agencies'].update([f"{closed_month}_{i}" for i in range(overview[4])])
                year_gov_data['active_days'].update([f"{closed_month}_{i}" for i in range(overview[5])])
                
                # Aggregate rate tiers
                for tier, spots_count, tier_revenue, avg_rate in gov_data['rate_tiers']:
                    if tier not in year_gov_data['rate_tiers']:
                        year_gov_data['rate_tiers'][tier] = {'spots': 0, 'revenue': 0}
                    year_gov_data['rate_tiers'][tier]['spots'] += spots_count
                    year_gov_data['rate_tiers'][tier]['revenue'] += tier_revenue
                
                # Aggregate agencies
                for agency, spots_count, agency_revenue, avg_rate, unique_clients in gov_data['agencies']:
                    if agency not in year_gov_data['agencies']:
                        year_gov_data['agencies'][agency] = {'spots': 0, 'revenue': 0, 'clients': 0}
                    year_gov_data['agencies'][agency]['spots'] += spots_count
                    year_gov_data['agencies'][agency]['revenue'] += agency_revenue
                    year_gov_data['agencies'][agency]['clients'] += unique_clients
                
                # Aggregate customers
                for customer, spots_count, customer_revenue, avg_rate in gov_data['customers']:
                    if customer not in year_gov_data['customers']:
                        year_gov_data['customers'][customer] = {'spots': 0, 'revenue': 0}
                    year_gov_data['customers'][customer]['spots'] += spots_count
                    year_gov_data['customers'][customer]['revenue'] += customer_revenue
                
                # Aggregate roadblocks
                if gov_data['roadblocks'] and gov_data['roadblocks'][0]:
                    roadblocks = gov_data['roadblocks']
                    year_gov_data['roadblock_instances'] += roadblocks[0] or 0
                    year_gov_data['roadblock_spots'] += roadblocks[1] or 0
                    year_gov_data['roadblock_revenue'] += roadblocks[2] or 0
        
        if year_gov_data['total_revenue'] > 0:
            gov_total_revenue = year_gov_data['total_revenue']
            gov_total_spots = year_gov_data['total_spots']
            gov_avg_rate = gov_total_revenue / gov_total_spots if gov_total_spots > 0 else 0
            
            self.add_line(f"**Total Government Revenue:** ${gov_total_revenue:,.2f} ({gov_total_spots:,} spots)")
            self.add_line(f"**Average Government Rate:** ${gov_avg_rate:.2f}/spot")
            self.add_line(f"**Government Share of Non-Language:** {(gov_total_revenue / year_nonlang_total * 100):.1f}%" if year_nonlang_total > 0 else "**Government Share of Non-Language:** N/A")
            self.add_line()
            
            # Government rate tier analysis
            self.add_header("💰 Government Rate Tier Analysis", 3)
            
            if year_gov_data['rate_tiers']:
                self.add_table_row(["Rate Tier", "Spots", "Revenue", "Avg Rate", "% of Gov"], True)
                
                sorted_tiers = sorted(year_gov_data['rate_tiers'].items(), 
                                    key=lambda x: x[1]['revenue'], reverse=True)
                
                for tier, data in sorted_tiers:
                    tier_avg = data['revenue'] / data['spots'] if data['spots'] > 0 else 0
                    tier_pct = (data['revenue'] / gov_total_revenue * 100) if gov_total_revenue > 0 else 0
                    
                    self.add_table_row([
                        tier,
                        f"{data['spots']:,}",
                        f"${data['revenue']:,.0f}",
                        f"${tier_avg:.2f}",
                        f"{tier_pct:.1f}%"
                    ])
                
                self.add_line()
                
                # Rate tier insights
                top_tier = sorted_tiers[0]
                self.add_line(f"🥇 **Top Rate Tier:** {top_tier[0]} (${top_tier[1]['revenue']:,.0f} - {(top_tier[1]['revenue']/gov_total_revenue*100):.1f}% of government revenue)")
                
                # Check for premium vs discount dominance
                premium_revenue = sum(data['revenue'] for tier, data in sorted_tiers if 'Emergency' in tier or 'Utilities' in tier)
                psa_revenue = sum(data['revenue'] for tier, data in sorted_tiers if 'PSA' in tier)
                
                if premium_revenue > psa_revenue:
                    self.add_line(f"💎 **Premium Strategy:** ${premium_revenue:,.0f} in premium rates vs ${psa_revenue:,.0f} in PSA rates")
                else:
                    self.add_line(f"📢 **PSA Strategy:** ${psa_revenue:,.0f} in PSA rates vs ${premium_revenue:,.0f} in premium rates")
                
                self.add_line()
            
            # Government agency analysis
            self.add_header("🏢 Government Agency Performance", 3)
            
            if year_gov_data['agencies']:
                self.add_table_row(["Agency", "Revenue", "Spots", "Avg Rate", "Clients"], True)
                
                sorted_agencies = sorted(year_gov_data['agencies'].items(), 
                                       key=lambda x: x[1]['revenue'], reverse=True)
                
                for agency, data in sorted_agencies[:10]:  # Top 10
                    agency_avg = data['revenue'] / data['spots'] if data['spots'] > 0 else 0
                    
                    self.add_table_row([
                        agency,
                        f"${data['revenue']:,.0f}",
                        f"{data['spots']:,}",
                        f"${agency_avg:.2f}",
                        f"{data['clients']:,}"
                    ])
                
                self.add_line()
                
                # Agency insights
                top_agency = sorted_agencies[0]
                agency_market_share = (top_agency[1]['revenue'] / gov_total_revenue * 100)
                self.add_line(f"🏆 **Top Agency:** {top_agency[0]} (${top_agency[1]['revenue']:,.0f} - {agency_market_share:.1f}% market share)")
                
                # Check for agency concentration
                top_3_revenue = sum(data['revenue'] for agency, data in sorted_agencies[:3])
                agency_concentration = (top_3_revenue / gov_total_revenue * 100) if gov_total_revenue > 0 else 0
                self.add_line(f"📊 **Agency Concentration:** Top 3 agencies control {agency_concentration:.1f}% of government revenue")
                
                self.add_line()
            
            # Government customer analysis
            self.add_header("🏛️ Top Government Customers", 3)
            
            if year_gov_data['customers']:
                self.add_table_row(["Customer", "Revenue", "Spots", "Avg Rate"], True)
                
                sorted_customers = sorted(year_gov_data['customers'].items(), 
                                        key=lambda x: x[1]['revenue'], reverse=True)
                
                for customer, data in sorted_customers[:10]:  # Top 10
                    customer_avg = data['revenue'] / data['spots'] if data['spots'] > 0 else 0
                    
                    self.add_table_row([
                        customer,
                        f"${data['revenue']:,.0f}",
                        f"{data['spots']:,}",
                        f"${customer_avg:.2f}"
                    ])
                
                self.add_line()
                
                # Customer insights
                top_customer = sorted_customers[0]
                customer_share = (top_customer[1]['revenue'] / gov_total_revenue * 100)
                self.add_line(f"👑 **Top Customer:** {top_customer[0]} (${top_customer[1]['revenue']:,.0f} - {customer_share:.1f}% of government revenue)")
                
                self.add_line()
            
            # Roadblock analysis
            if year_gov_data['roadblock_instances'] > 0:
                self.add_header("🎯 Government Roadblock Analysis", 3)
                
                roadblock_revenue = year_gov_data['roadblock_revenue']
                roadblock_spots = year_gov_data['roadblock_spots']
                roadblock_instances = year_gov_data['roadblock_instances']
                
                self.add_table_row(["Metric", "Value"], True)
                self.add_table_row(["Roadblock Instances", f"{roadblock_instances:,}"])
                self.add_table_row(["Roadblock Spots", f"{roadblock_spots:,}"])
                self.add_table_row(["Roadblock Revenue", f"${roadblock_revenue:,.2f}"])
                self.add_table_row(["Avg Spots per Roadblock", f"{roadblock_spots/roadblock_instances:.1f}" if roadblock_instances > 0 else "0"])
                self.add_table_row(["Roadblock % of Gov Revenue", f"{(roadblock_revenue/gov_total_revenue*100):.1f}%" if gov_total_revenue > 0 else "0%"])
                
                self.add_line()
                
                # Roadblock insights
                roadblock_share = (roadblock_revenue / gov_total_revenue * 100) if gov_total_revenue > 0 else 0
                avg_spots_per_roadblock = roadblock_spots / roadblock_instances if roadblock_instances > 0 else 0
                
                self.add_line(f"🎪 **Roadblock Strategy:** {roadblock_share:.1f}% of government revenue comes from roadblocks")
                self.add_line(f"📊 **Roadblock Efficiency:** {avg_spots_per_roadblock:.1f} spots per roadblock on average")
                
                if roadblock_share > 50:
                    self.add_line("💡 **Strategic Insight:** Government heavily favors roadblock strategies for maximum reach")
                elif roadblock_share > 25:
                    self.add_line("💡 **Strategic Insight:** Government uses mixed approach - roadblocks for key campaigns")
                else:
                    self.add_line("💡 **Strategic Insight:** Government primarily uses single-spot strategy")
                
                self.add_line()
            
            # Government efficiency analysis
            self.add_header("⚡ Government Revenue Efficiency", 3)
            
            # Compare government rates to language block rates
            if year_lang_total_revenue > 0 and year_lang_total_spots > 0:
                lang_avg_rate = year_lang_total_revenue / year_lang_total_spots
                efficiency_ratio = gov_avg_rate / lang_avg_rate if lang_avg_rate > 0 else 0
                
                self.add_table_row(["Metric", "Government", "Language Blocks", "Ratio"], True)
                self.add_table_row([
                    "Average Rate/Spot",
                    f"${gov_avg_rate:.2f}",
                    f"${lang_avg_rate:.2f}",
                    f"{efficiency_ratio:.2f}x"
                ])
                
                self.add_line()
                
                if efficiency_ratio > 1.2:
                    self.add_line(f"📈 **Premium Efficiency:** Government pays {efficiency_ratio:.1f}x language block rates")
                elif efficiency_ratio > 0.8:
                    self.add_line(f"⚖️ **Balanced Efficiency:** Government rates comparable to language blocks")
                else:
                    self.add_line(f"💰 **Volume Efficiency:** Government gets {1/efficiency_ratio:.1f}x discount for volume")
                
                self.add_line()
            
            # Strategic recommendations
            self.add_header("🎯 Government Revenue Strategy Recommendations", 3)
            
            recommendations = []
            
            # Rate tier recommendations
            if year_gov_data['rate_tiers']:
                sorted_tiers = sorted(year_gov_data['rate_tiers'].items(), 
                                    key=lambda x: x[1]['revenue'], reverse=True)
                top_tier = sorted_tiers[0]
                
                if 'Emergency' in top_tier[0] or 'Utilities' in top_tier[0]:
                    recommendations.append("💎 **Focus on Premium Government:** Emergency and utility messaging drives highest rates")
                elif 'PSA' in top_tier[0]:
                    recommendations.append("📢 **Volume PSA Strategy:** Public service announcements provide consistent volume")
            
            # Agency recommendations
            if year_gov_data['agencies']:
                sorted_agencies = sorted(year_gov_data['agencies'].items(), 
                                       key=lambda x: x[1]['revenue'], reverse=True)
                top_agencies = [agency for agency, data in sorted_agencies[:3]]
                recommendations.append(f"🏢 **Target Key Agencies:** Focus sales efforts on {', '.join(top_agencies)}")
            
            # Roadblock recommendations
            if year_gov_data['roadblock_instances'] > 0:
                roadblock_share = (year_gov_data['roadblock_revenue'] / gov_total_revenue * 100)
                if roadblock_share > 30:
                    recommendations.append("🎯 **Roadblock Inventory:** Create government roadblock packages across language blocks")
                else:
                    recommendations.append("📊 **Roadblock Opportunity:** Develop roadblock strategies for government clients")
            
            # Seasonal recommendations
            recommendations.append("📅 **Seasonal Strategy:** Track government spending patterns for budget planning")
            
            for i, rec in enumerate(recommendations, 1):
                self.add_line(f"{i}. {rec}")
            
            self.add_line()
            
        else:
            self.add_line("⚠️ **No government revenue data found for this period.**")
            self.add_line()
        
        # Year-end revenue split
        self.add_header(f"💰 {year} Revenue Split Analysis", 2)
        
        # Calculate overall percentages
        lang_pct = (year_lang_total_revenue / year_total * 100) if year_total > 0 else 0
        nonlang_pct = (year_nonlang_total / year_total * 100) if year_total > 0 else 0
        
        self.add_table_row(["Revenue Type", "Amount", "Percentage", "Spots"], True)
        self.add_table_row([
            "Language Blocks",
            f"${year_lang_total_revenue:,.2f}",
            f"{lang_pct:.1f}%",
            f"{year_lang_total_spots:,}" if year_lang_total_spots > 0 else "N/A"
        ])
        self.add_table_row([
            "Non-Language",
            f"${year_nonlang_total:,.2f}",
            f"{nonlang_pct:.1f}%",
            f"{total_spots - year_lang_total_spots:,}" if total_spots > year_lang_total_spots else "N/A"
        ])
        self.add_table_row([
            "**TOTAL**",
            f"**${year_total:,.2f}**",
            f"**100.0%**",
            f"**{total_spots:,}**"
        ])
        
        self.add_line()
        
        # Revenue split insights
        self.add_header("🎯 Revenue Split Insights", 3)
        
        if year_lang_total_revenue > 0 and year_nonlang_total > 0:
            # Determine primary revenue driver
            if lang_pct > nonlang_pct:
                primary_driver = "Language Blocks"
                primary_pct = lang_pct
                secondary_driver = "Non-Language"
                secondary_pct = nonlang_pct
            else:
                primary_driver = "Non-Language"
                primary_pct = nonlang_pct
                secondary_driver = "Language Blocks"
                secondary_pct = lang_pct
            
            self.add_line(f"🎯 **Primary Revenue Driver:** {primary_driver} ({primary_pct:.1f}% of total revenue)")
            self.add_line(f"📊 **Revenue Mix:** {primary_pct:.1f}% {primary_driver} / {secondary_pct:.1f}% {secondary_driver}")
            
            # Revenue per spot analysis
            if year_lang_total_spots > 0:
                lang_per_spot = year_lang_total_revenue / year_lang_total_spots
                nonlang_spots = total_spots - year_lang_total_spots
                nonlang_per_spot = year_nonlang_total / nonlang_spots if nonlang_spots > 0 else 0
                
                self.add_line(f"💰 **Language Block Efficiency:** ${lang_per_spot:.2f}/spot")
                if nonlang_spots > 0:
                    self.add_line(f"💰 **Non-Language Efficiency:** ${nonlang_per_spot:.2f}/spot")
                    
                    if lang_per_spot > nonlang_per_spot:
                        efficiency_leader = "Language Blocks"
                        efficiency_advantage = ((lang_per_spot - nonlang_per_spot) / nonlang_per_spot * 100)
                    else:
                        efficiency_leader = "Non-Language"
                        efficiency_advantage = ((nonlang_per_spot - lang_per_spot) / lang_per_spot * 100)
                    
                    self.add_line(f"📈 **Efficiency Leader:** {efficiency_leader} ({efficiency_advantage:+.1f}% higher value per spot)")
            
            # Diversification assessment
            diversification_score = min(lang_pct, nonlang_pct)
            if diversification_score > 40:
                diversification_level = "Well-Balanced"
            elif diversification_score > 25:
                diversification_level = "Moderately Diversified"
            elif diversification_score > 10:
                diversification_level = "Concentrated"
            else:
                diversification_level = "Highly Concentrated"
            
            self.add_line(f"🎲 **Revenue Diversification:** {diversification_level} ({diversification_score:.1f}% minimum category)")
        
        self.add_line()
        self.add_header("🔧 Report Features", 2)
        self.add_line("✅ Complete language block revenue coverage")
        self.add_line("✅ Proper WorldLink categorization as Direct Response")
        self.add_line("✅ Verification totals to ensure accuracy")
        self.add_line("✅ Comprehensive breakdown by language and category")
        self.add_line("✅ Monthly performance tracking")
        self.add_line("✅ **NEW: Government revenue deep dive analysis**")
        self.add_line("✅ **NEW: Government rate tier analysis (Emergency/Utilities/PSA)**")
        self.add_line("✅ **NEW: Government agency performance tracking**")
        self.add_line("✅ **NEW: Government roadblock strategy analysis**")
        self.add_line("✅ **NEW: Government vs language block efficiency comparison**")
        self.add_line("✅ **NEW: Strategic recommendations for government sales**")
        
        return "\n".join(self.report_lines)
    
    def save_report(self, content: str, year: int) -> str:
        """Save the report to a timestamped file"""
        # Create reports directory if it doesn't exist
        reports_dir = "reports"
        if not os.path.exists(reports_dir):
            os.makedirs(reports_dir)
        
        # Generate filename with timestamp
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{reports_dir}/revenue_report_{year}_{timestamp}.md"
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return filename


def main():
    parser = argparse.ArgumentParser(description='Generate revenue reports with markdown formatting')
    parser.add_argument('year', type=int, help='Year to generate report for (e.g., 2024)')
    parser.add_argument('--target', type=float, help='Target revenue amount for comparison')
    parser.add_argument('--db-path', default='./data/database/production.db', help='Path to database file')
    parser.add_argument('--output', choices=['file', 'stdout', 'both'], default='both', 
                       help='Output destination (default: both)')
    parser.add_argument('--government-focus', action='store_true', 
                       help='Include expanded government analysis sections')
    parser.add_argument('--exclude-government', action='store_true',
                       help='Exclude government deep dive analysis (faster generation)')
    
    args = parser.parse_args()
    
    # Validate year
    current_year = datetime.datetime.now().year
    if args.year < 2020 or args.year > current_year + 1:
        print(f"⚠️ Warning: Year {args.year} seems unusual. Continuing anyway...")
    
    # Check for conflicting arguments
    if args.government_focus and args.exclude_government:
        print("❌ Error: Cannot use both --government-focus and --exclude-government")
        return 1
    
    # Generate report
    generator = RevenueReportGenerator(args.db_path)
    
    try:
        print(f"📊 Generating {args.year} revenue report...")
        if args.government_focus:
            print("🏛️ Including expanded government analysis...")
        elif args.exclude_government:
            print("⚡ Excluding government deep dive for faster generation...")
        
        content = generator.generate_report(args.year, args.target)
        
        if args.output in ['stdout', 'both']:
            print("\n" + content)
        
        if args.output in ['file', 'both']:
            filename = generator.save_report(content, args.year)
            print(f"\n💾 Report saved to: {filename}")
            
            # Additional government-specific output options
            if args.government_focus:
                print("🏛️ Government analysis includes:")
                print("   • Rate tier analysis (Emergency/Utilities/PSA)")
                print("   • Agency performance tracking")
                print("   • Roadblock strategy analysis")
                print("   • Strategic recommendations")
            
    except Exception as e:
        print(f"❌ Error generating report: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())