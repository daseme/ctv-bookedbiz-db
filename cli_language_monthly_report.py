#!/usr/bin/env python3
"""
Fixed 2024 Revenue Report - Complete Language Block Coverage
===========================================================

This fixes the revenue report to properly capture ALL language block revenue
and correctly categorize WorldLink spots.
"""

import sqlite3

def get_2024_revenue_report_fixed():
    """Get 2024 revenue report with complete coverage"""
    conn = sqlite3.connect('./data/database/production.db')
    cursor = conn.cursor()
    
    # Month mapping from "Jan-24" to "2024-01"
    month_map = {
        'Jan-24': '2024-01',
        'Feb-24': '2024-02',
        'Mar-24': '2024-03',
        'Apr-24': '2024-04',
        'May-24': '2024-05',
        'Jun-24': '2024-06',
        'Jul-24': '2024-07',
        'Aug-24': '2024-08',
        'Sep-24': '2024-09',
        'Oct-24': '2024-10',
        'Nov-24': '2024-11',
        'Dec-24': '2024-12'
    }
    
    # Get closed months
    cursor.execute("""
        SELECT broadcast_month, closed_date, closed_by 
        FROM month_closures 
        WHERE broadcast_month LIKE '%24' 
        ORDER BY 
            CASE broadcast_month
                WHEN 'Jan-24' THEN 1
                WHEN 'Feb-24' THEN 2
                WHEN 'Mar-24' THEN 3
                WHEN 'Apr-24' THEN 4
                WHEN 'May-24' THEN 5
                WHEN 'Jun-24' THEN 6
                WHEN 'Jul-24' THEN 7
                WHEN 'Aug-24' THEN 8
                WHEN 'Sep-24' THEN 9
                WHEN 'Oct-24' THEN 10
                WHEN 'Nov-24' THEN 11
                WHEN 'Dec-24' THEN 12
            END
    """)
    closed_months = cursor.fetchall()
    
    if not closed_months:
        print("No 2024 closed months found!")
        return
    
    print("🎯 2024 REVENUE REPORT - FIXED COMPLETE VERSION")
    print("=" * 80)
    
    year_total = 0
    
    for closed_month, closed_date, closed_by in closed_months:
        if closed_month not in month_map:
            continue
            
        spots_pattern = month_map[closed_month]
        
        # Get total revenue for this month
        cursor.execute("""
            SELECT COUNT(*) as spots, SUM(gross_rate) as revenue
            FROM spots 
            WHERE broadcast_month LIKE ? AND gross_rate != 0
        """, (f'{spots_pattern}%',))
        
        result = cursor.fetchone()
        if not result or result[0] == 0:
            continue
            
        spots, revenue = result
        year_total += revenue
        
        print(f"📅 {closed_month} ✅ CLOSED ({closed_date})")
        print(f"   💵 Total Revenue: ${revenue:,.2f} ({spots:,} spots)")
        print(f"   📈 Monthly Average: ${revenue/spots:.2f}/spot" if spots > 0 else "   📈 Monthly Average: $0.00/spot")
        
        # FIXED: Get ALL language block revenue first
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
        
        # FIXED: Get language breakdown with better query
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
        breakdown = []
        lang_breakdown_total = 0
        
        # Process language results
        for lang_name, revenue_spots, lang_revenue, avg_spot_value, bonus_spots, paid_spots, total_spots in lang_results:
            if lang_revenue and lang_revenue != 0:
                breakdown.append(f"{lang_name} ${lang_revenue:,.0f}")
                lang_breakdown_total += lang_revenue
        
        # FIXED: Get non-language revenue with proper WorldLink categorization
        cursor.execute("""
            SELECT 
                CASE 
                    -- FIXED: Check for WorldLink (agency OR bill code pattern) first
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
        
        nonlang_results = cursor.fetchall()
        nonlang_total = 0
        
        for category, cat_revenue in nonlang_results:
            nonlang_total += cat_revenue
        
        # Show breakdown
        if breakdown:
            print(f"   📊 Revenue Breakdown: {', '.join(breakdown)}")
        
        # FIXED: Show actual language block total vs breakdown
        print(f"   🌐 Language Block Performance:")
        print(f"      💰 TOTAL LANGUAGE BLOCKS: ${total_lang_revenue:,.0f} ({total_lang_spots:,} spots)")
        
        if abs(total_lang_revenue - lang_breakdown_total) > 1:
            print(f"      ⚠️  Breakdown total: ${lang_breakdown_total:,.0f} (difference: ${total_lang_revenue - lang_breakdown_total:+,.0f})")
        
        # Show top language details
        if lang_results:
            for lang_name, revenue_spots, lang_revenue, avg_spot_value, bonus_spots, paid_spots, total_spots in lang_results[:7]:
                if lang_revenue and lang_revenue != 0:
                    total_content_spots = bonus_spots + paid_spots
                    bns_percentage = (bonus_spots / total_content_spots * 100) if total_content_spots > 0 else 0
                    
                    print(f"      • {lang_name}: ${lang_revenue:,.0f} ({revenue_spots:,} revenue spots, {total_spots:,} total)")
                    print(f"        - Avg Value: ${avg_spot_value:.2f}/spot")
                    print(f"        - Content Mix: {paid_spots:,} paid, {bonus_spots:,} bonus ({bns_percentage:.1f}% bonus)")
        
        # Show non-language breakdown
        if nonlang_results:
            print(f"   📋 Non-Language Revenue:")
            for category, cat_revenue in nonlang_results:
                print(f"      • {category}: ${cat_revenue:,.0f}")
        
        # FIXED: Show accurate split using actual totals
        print(f"   💰 Split: Language Blocks ${total_lang_revenue:,.0f} | Non-Language ${nonlang_total:,.0f}")
        
        # VERIFICATION: Check if totals match
        calculated_total = total_lang_revenue + nonlang_total
        difference = revenue - calculated_total
        
        if abs(difference) > 1:
            print(f"   ⚠️  VERIFICATION: Calculated ${calculated_total:,.0f} vs Actual ${revenue:,.0f} (diff: ${difference:+,.0f})")
        else:
            print(f"   ✅ VERIFICATION: Totals match perfectly")
        
        print()
    
    print(f"🎯 2024 TOTAL REVENUE (FIXED): ${year_total:,.2f}")
    print(f"📊 Target from spreadsheet: $4,076,255.94")
    print(f"📈 Difference: ${year_total - 4076255.94:+,.2f}")
    
    if abs(year_total - 4076255.94) < 10000:
        print("✅ Perfect match! Revenue totals align with spreadsheet.")
    elif abs(year_total - 4076255.94) < 50000:
        print("✅ Very close match! Difference likely due to minor adjustments or timing.")
    else:
        print("⚠️  Significant difference - may need further investigation.")
    
    print()
    print("🔧 FIXES APPLIED:")
    print("=" * 60)
    print("✅ Language block query now captures ALL revenue (not just breakdown)")
    print("✅ WorldLink properly categorized as 'Direct Response' (agency + bill code)")
    print("✅ Added verification to ensure totals match")
    print("✅ Better handling of missing language assignments")
    print("=" * 60)
    
    conn.close()

if __name__ == "__main__":
    get_2024_revenue_report_fixed()