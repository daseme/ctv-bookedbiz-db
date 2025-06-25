#!/usr/bin/env python3
"""
Language Block Assignment Diagnosis
Investigate why 2025 spots aren't being assigned to language blocks
"""

import sqlite3
import os
from datetime import datetime

def diagnose_assignment_issues():
    """Diagnose language block assignment problems"""
    
    db_path = "data/database/production.db"
    if not os.path.exists(db_path):
        print("✗ Database not found. Run: python db_sync.py download")
        return False
    
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        
        print("="*80)
        print("LANGUAGE BLOCK ASSIGNMENT DIAGNOSIS")
        print("="*80)
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # 1. Check Programming Schedules Status
        print("🏗️ PROGRAMMING SCHEDULES STATUS")
        print("-" * 40)
        
        schedules_query = """
        SELECT 
            schedule_name,
            schedule_version,
            schedule_type,
            effective_start_date,
            effective_end_date,
            is_active,
            created_date
        FROM programming_schedules
        ORDER BY created_date DESC
        """
        
        cursor = conn.execute(schedules_query)
        schedules = cursor.fetchall()
        
        if schedules:
            print(f"{'Schedule':20} {'Version':12} {'Active':8} {'Start Date':12} {'End Date':12}")
            print("-" * 75)
            for schedule in schedules:
                active_status = "✅ Yes" if schedule['is_active'] else "❌ No"
                end_date = schedule['effective_end_date'] or "Ongoing"
                print(f"{schedule['schedule_name'][:19]:20} {schedule['schedule_version'][:11]:12} {active_status:8} {str(schedule['effective_start_date'])[:10]:12} {str(end_date)[:10]:12}")
        else:
            print("❌ No programming schedules found!")
        
        # 2. Check Language Blocks
        print(f"\n📅 LANGUAGE BLOCKS STATUS")
        print("-" * 30)
        
        blocks_query = """
        SELECT 
            COUNT(*) as total_blocks,
            COUNT(CASE WHEN is_active = 1 THEN 1 END) as active_blocks,
            COUNT(DISTINCT schedule_id) as schedules_with_blocks,
            COUNT(DISTINCT language_id) as languages_covered,
            MIN(created_date) as oldest_block,
            MAX(created_date) as newest_block
        FROM language_blocks
        """
        
        cursor = conn.execute(blocks_query)
        blocks_summary = cursor.fetchone()
        
        if blocks_summary:
            print(f"Total Language Blocks: {blocks_summary['total_blocks']}")
            print(f"Active Language Blocks: {blocks_summary['active_blocks']}")
            print(f"Schedules with Blocks: {blocks_summary['schedules_with_blocks']}")
            print(f"Languages Covered: {blocks_summary['languages_covered']}")
            print(f"Block Creation Range: {blocks_summary['oldest_block']} to {blocks_summary['newest_block']}")
        
        # 3. Check Market Assignments
        print(f"\n🗺️ MARKET SCHEDULE ASSIGNMENTS")
        print("-" * 35)
        
        market_assignments_query = """
        SELECT 
            m.market_code,
            m.market_name,
            ps.schedule_name,
            sma.effective_start_date,
            sma.effective_end_date,
            sma.assignment_priority
        FROM schedule_market_assignments sma
        JOIN markets m ON sma.market_id = m.market_id
        JOIN programming_schedules ps ON sma.schedule_id = ps.schedule_id
        WHERE sma.effective_end_date IS NULL OR sma.effective_end_date > date('now')
        ORDER BY m.market_code, sma.assignment_priority
        """
        
        cursor = conn.execute(market_assignments_query)
        market_assignments = cursor.fetchall()
        
        if market_assignments:
            print(f"{'Market':10} {'Schedule':25} {'Start Date':12} {'End Date':12} {'Priority':8}")
            print("-" * 75)
            for assignment in market_assignments:
                end_date = assignment['effective_end_date'] or "Ongoing"
                print(f"{assignment['market_code']:10} {assignment['schedule_name'][:24]:25} {str(assignment['effective_start_date'])[:10]:12} {str(end_date)[:10]:12} {assignment['assignment_priority']:8}")
        else:
            print("❌ No active market assignments found!")
        
        # 4. Check Recent Assignment Activity
        print(f"\n📊 ASSIGNMENT ACTIVITY TIMELINE")
        print("-" * 35)
        
        assignment_timeline_query = """
        SELECT 
            DATE(s.broadcast_month) as month,
            COUNT(DISTINCT s.spot_id) as total_spots,
            COUNT(DISTINCT slb.spot_id) as assigned_spots,
            ROUND(COUNT(DISTINCT slb.spot_id) * 100.0 / COUNT(DISTINCT s.spot_id), 1) as assignment_rate
        FROM spots s
        LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
          AND s.broadcast_month IS NOT NULL
          AND s.broadcast_month >= '2024-01-01'
        GROUP BY DATE(s.broadcast_month)
        ORDER BY month DESC
        LIMIT 15
        """
        
        cursor = conn.execute(assignment_timeline_query)
        timeline = cursor.fetchall()
        
        if timeline:
            print(f"{'Month':12} {'Total Spots':>12} {'Assigned':>10} {'Rate %':>8}")
            print("-" * 50)
            for month in timeline:
                print(f"{month['month']:12} {month['total_spots']:>12,} {month['assigned_spots']:>10,} {month['assignment_rate']:>7.1f}%")
        
        # 5. Check Assignment Methods
        print(f"\n🔧 ASSIGNMENT METHODS USED")
        print("-" * 30)
        
        methods_query = """
        SELECT 
            assignment_method,
            customer_intent,
            COUNT(*) as assignments,
            COUNT(DISTINCT spot_id) as unique_spots,
            MIN(assigned_date) as first_assignment,
            MAX(assigned_date) as last_assignment
        FROM spot_language_blocks
        GROUP BY assignment_method, customer_intent
        ORDER BY assignments DESC
        """
        
        cursor = conn.execute(methods_query)
        methods = cursor.fetchall()
        
        if methods:
            print(f"{'Method':20} {'Intent':20} {'Count':>8} {'First':>12} {'Last':>12}")
            print("-" * 75)
            for method in methods:
                first_date = str(method['first_assignment'])[:10] if method['first_assignment'] else "N/A"
                last_date = str(method['last_assignment'])[:10] if method['last_assignment'] else "N/A"
                print(f"{method['assignment_method'][:19]:20} {method['customer_intent'][:19]:20} {method['assignments']:>8,} {first_date:>12} {last_date:>12}")
        
        # 6. Identify the Problem
        print(f"\n🕵️ PROBLEM DIAGNOSIS")
        print("-" * 25)
        
        # Check for assignment gaps
        gap_analysis_query = """
        SELECT 
            'Last Assignment Date' as metric,
            MAX(s.broadcast_month) as value
        FROM spots s
        JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        
        UNION ALL
        
        SELECT 
            'Latest Spot Date' as metric,
            MAX(broadcast_month) as value
        FROM spots
        WHERE (revenue_type != 'Trade' OR revenue_type IS NULL)
        
        UNION ALL
        
        SELECT 
            'Days Since Last Assignment' as metric,
            CAST(julianday('now') - julianday(MAX(s.broadcast_month)) AS INTEGER) as value
        FROM spots s
        JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        """
        
        cursor = conn.execute(gap_analysis_query)
        diagnostics = cursor.fetchall()
        
        for diagnostic in diagnostics:
            print(f"• {diagnostic['metric']}: {diagnostic['value']}")
        
        # 7. Check for 2025 Data Issues
        print(f"\n🔍 2025 SPECIFIC ANALYSIS")
        print("-" * 30)
        
        data_2025_query = """
        SELECT 
            COUNT(DISTINCT s.spot_id) as total_2025_spots,
            COUNT(DISTINCT CASE WHEN slb.spot_id IS NOT NULL THEN s.spot_id END) as assigned_2025_spots,
            COUNT(DISTINCT s.language_id) as languages_in_2025,
            COUNT(DISTINCT s.market_id) as markets_in_2025,
            MIN(s.broadcast_month) as earliest_2025,
            MAX(s.broadcast_month) as latest_2025
        FROM spots s
        LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        WHERE s.broadcast_month LIKE '2025%'
          AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        """
        
        cursor = conn.execute(data_2025_query)
        data_2025 = cursor.fetchone()
        
        if data_2025:
            assignment_rate = (data_2025['assigned_2025_spots'] / data_2025['total_2025_spots'] * 100) if data_2025['total_2025_spots'] > 0 else 0
            print(f"2025 Spots: {data_2025['total_2025_spots']:,} total | {data_2025['assigned_2025_spots']:,} assigned ({assignment_rate:.1f}%)")
            print(f"2025 Coverage: {data_2025['languages_in_2025']} languages | {data_2025['markets_in_2025']} markets")
            print(f"2025 Date Range: {data_2025['earliest_2025']} to {data_2025['latest_2025']}")
        
        # 8. Recommendations
        print(f"\n💡 RECOMMENDATIONS")
        print("-" * 20)
        
        if not schedules:
            print("❌ NO PROGRAMMING SCHEDULES FOUND")
            print("   → Create programming schedules first")
        elif not market_assignments:
            print("❌ NO MARKET ASSIGNMENTS FOUND") 
            print("   → Assign programming schedules to markets")
        elif data_2025 and data_2025['assigned_2025_spots'] == 0:
            print("❌ NO 2025 SPOTS ASSIGNED TO LANGUAGE BLOCKS")
            print("   → Assignment process may have stopped")
            print("   → Check if assignment automation is running")
            print("   → Verify schedule effective dates cover 2025")
        else:
            print("✅ System components appear to be in place")
            print("   → Check assignment automation/triggers")
        
        print(f"\n{'='*80}")
        print("✓ Language Block Assignment Diagnosis Complete!")
        print("📋 Use this information to identify and fix assignment issues")
        print(f"{'='*80}")
        
        return True
        
    except Exception as e:
        print(f"✗ Error in diagnosis: {e}")
        import traceback
        print(traceback.format_exc())
        return False
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    diagnose_assignment_issues()