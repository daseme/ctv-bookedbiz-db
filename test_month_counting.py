#!/usr/bin/env python3
"""
Test script to verify FIXED month counting logic works correctly
"""

import sqlite3
import os

def test_fixed_month_counting():
    """Test FIXED month counting logic for 2024 data"""
    
    db_path = "data/database/production.db"
    if not os.path.exists(db_path):
        print(f"❌ Database not found: {db_path}")
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("🧪 TESTING FIXED MONTH COUNTING LOGIC FOR 2024")
    print("=" * 50)
    
    # Test 1: Check FIXED month counting - should now show ≤12 months
    print("\n1. FIXED: Distinct MONTHS (yyyy-mm) for 2024:")
    query1 = """
    SELECT DISTINCT 
        SUBSTR(s.broadcast_month, 1, 7) as year_month,
        COUNT(*) as spot_count
    FROM spots s
    WHERE SUBSTR(s.broadcast_month, 1, 4) = '2024'
      AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
      AND s.gross_rate > 0
    GROUP BY SUBSTR(s.broadcast_month, 1, 7)
    ORDER BY year_month
    """
    
    cursor.execute(query1)
    results = cursor.fetchall()
    
    print(f"{'Year-Month':12} {'Spots':>8}")
    print("-" * 25)
    total_months_2024 = 0
    for row in results:
        print(f"{row[0]:12} {row[1]:>8}")
        total_months_2024 += 1
    
    print(f"\nTotal distinct MONTHS in 2024: {total_months_2024} (should be ≤12)")
    
    # Test 2: Test the FIXED Vietnamese Midday Block month counting
    print(f"\n2. FIXED Vietnamese Midday Block month counting:")
    query2 = """
    SELECT 
        lb.block_name,
        l.language_name,
        m.market_code,
        
        -- OLD method (counts distinct datetime values - WRONG)
        COUNT(DISTINCT s.broadcast_month) as old_method_count,
        
        -- NEW FIXED method (counts distinct yyyy-mm - CORRECT)
        COUNT(DISTINCT CASE 
            WHEN SUBSTR(s.broadcast_month, 1, 4) = '2024' 
            THEN SUBSTR(s.broadcast_month, 1, 7)
        END) as fixed_method_count,
        
        -- Revenue and spots for 2024
        SUM(CASE WHEN SUBSTR(s.broadcast_month, 1, 4) = '2024' THEN s.gross_rate ELSE 0 END) as revenue_2024,
        COUNT(CASE WHEN SUBSTR(s.broadcast_month, 1, 4) = '2024' THEN s.spot_id END) as spots_2024
        
    FROM spots s
    JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    JOIN language_blocks lb ON slb.block_id = lb.block_id
    LEFT JOIN languages l ON lb.language_id = l.language_id
    LEFT JOIN markets m ON s.market_id = m.market_id
    WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
      AND s.gross_rate > 0
      AND lb.block_name LIKE '%Vietnamese Midday%'
      AND m.market_code = 'SFO'
    GROUP BY lb.block_id, lb.block_name, l.language_name, m.market_code
    ORDER BY revenue_2024 DESC
    LIMIT 3
    """
    
    cursor.execute(query2)
    results = cursor.fetchall()
    
    print(f"{'Block':20} {'Lang':8} {'Mkt':4} {'Old':>5} {'FIXED':>7} {'2024 Revenue':>12} {'2024 Spots':>10}")
    print("-" * 75)
    
    for row in results:
        block_name = row[0][:19] if row[0] else 'Unknown'
        language = row[1][:7] if row[1] else 'Unknown'  
        market = row[2] if row[2] else 'N/A'
        old_count = row[3] or 0
        fixed_count = row[4] or 0
        revenue_2024 = row[5] or 0
        spots_2024 = row[6] or 0
        
        print(f"{block_name:20} {language:8} {market:4} {old_count:>5} {fixed_count:>7} ${revenue_2024:>11,.0f} {spots_2024:>10}")
    
    conn.close()
    
    print(f"\n✅ EXPECTED FIXED RESULTS:")
    print("• 'Total distinct MONTHS in 2024' should be ≤ 12 ✅")
    print("• 'FIXED' column should show ≤ 12 ✅") 
    print("• 'Old' column may still show >12 (that's the old wrong method)")
    print("\n🎯 If FIXED column shows ≤12, the month counting is now correct!")

if __name__ == "__main__":
    test_fixed_month_counting()