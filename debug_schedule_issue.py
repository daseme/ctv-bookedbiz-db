#!/usr/bin/env python3
"""
Debug Script for Language Block Service - Schedule ID Issue
===========================================================

This script diagnoses why schedule_id is coming back as NULL during assignment.
"""

import sqlite3
import sys
import os

# Add src to path
sys.path.insert(0, 'src')

def debug_schedule_resolution(db_path):
    """Debug the schedule resolution issue"""
    
    print("🔍 Debugging Schedule Resolution Issue")
    print("=" * 50)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Step 1: Check a few test spots
    print("\n1. Examining Test Spots:")
    
    # First, check basic spot data
    cursor.execute("SELECT COUNT(*) FROM spots")
    total_spots = cursor.fetchone()[0]
    print(f"   Total spots in database: {total_spots:,}")
    
    # Check spots with required fields
    cursor.execute("""
        SELECT COUNT(*) FROM spots s
        WHERE s.time_in IS NOT NULL 
          AND s.time_out IS NOT NULL 
          AND s.day_of_week IS NOT NULL
    """)
    spots_with_time = cursor.fetchone()[0]
    print(f"   Spots with time data: {spots_with_time:,}")
    
    # Check spots excluding trade
    cursor.execute("""
        SELECT COUNT(*) FROM spots s
        WHERE s.time_in IS NOT NULL 
          AND s.time_out IS NOT NULL 
          AND s.day_of_week IS NOT NULL
          AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    """)
    assignable_spots = cursor.fetchone()[0]
    print(f"   Assignable spots (non-Trade): {assignable_spots:,}")
    
    # Try to get sample spots with more relaxed criteria
    cursor.execute("""
        SELECT s.spot_id, s.market_id, s.air_date, s.day_of_week, s.time_in, s.time_out, 
               COALESCE(m.market_code, 'UNKNOWN') as market_code, s.revenue_type
        FROM spots s
        LEFT JOIN markets m ON s.market_id = m.market_id
        WHERE s.time_in IS NOT NULL 
          AND s.time_out IS NOT NULL 
          AND s.day_of_week IS NOT NULL
        LIMIT 5
    """)
    
    test_spots = cursor.fetchall()
    if test_spots:
        for spot in test_spots:
            spot_id, market_id, air_date, day_of_week, time_in, time_out, market_code, revenue_type = spot
            print(f"   Spot {spot_id}: Market {market_code} ({market_id}), {air_date}, {day_of_week} {time_in}-{time_out}, Rev: {revenue_type}")
    else:
        print("   ❌ No spots found with required time data!")
        
        # Check what's missing
        cursor.execute("SELECT COUNT(*) FROM spots WHERE time_in IS NULL")
        missing_time_in = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM spots WHERE time_out IS NULL") 
        missing_time_out = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM spots WHERE day_of_week IS NULL")
        missing_day = cursor.fetchone()[0]
        
        print(f"      Missing time_in: {missing_time_in:,}")
        print(f"      Missing time_out: {missing_time_out:,}")
        print(f"      Missing day_of_week: {missing_day:,}")
        
        # Show some sample data
        cursor.execute("SELECT spot_id, time_in, time_out, day_of_week, revenue_type FROM spots LIMIT 10")
        sample_spots = cursor.fetchall()
        print("      Sample spot data:")
        for spot in sample_spots:
            print(f"        Spot {spot[0]}: {spot[1]} to {spot[2]}, {spot[3]}, {spot[4]}")
        
        return  # Exit early if no test spots
    
    # Step 2: Check schedule market assignments
    print("\n2. Schedule Market Assignments:")
    cursor.execute("""
        SELECT sma.assignment_id, m.market_code, ps.schedule_name, 
               sma.effective_start_date, sma.effective_end_date, sma.assignment_priority
        FROM schedule_market_assignments sma
        JOIN markets m ON sma.market_id = m.market_id
        JOIN programming_schedules ps ON sma.schedule_id = ps.schedule_id
        ORDER BY m.market_code, sma.effective_start_date
    """)
    
    assignments = cursor.fetchall()
    for assignment in assignments:
        assignment_id, market_code, schedule_name, start_date, end_date, priority = assignment
        end_display = end_date or "ongoing"
        print(f"   {market_code} -> {schedule_name} ({start_date} to {end_display}, priority {priority})")
    
    # Step 3: Test schedule resolution for each test spot
    print("\n3. Testing Schedule Resolution:")
    for spot in test_spots:
        # Handle the tuple unpacking correctly
        if len(spot) == 8:
            spot_id, market_id, air_date, day_of_week, time_in, time_out, market_code, revenue_type = spot
        else:
            spot_id, market_id, air_date, day_of_week, time_in, time_out, market_code = spot
            revenue_type = "Unknown"
        
        print(f"\n   🎯 Testing Spot {spot_id} (Market: {market_code}, ID: {market_id}, Date: {air_date})")
        
        if market_id is None:
            print(f"      ❌ Cannot resolve schedule - market_id is NULL!")
            print(f"         This spot has no market assigned - this is the root issue!")
            continue
        
        # Run the exact query from our service
        cursor.execute("""
            SELECT ps.schedule_id, ps.schedule_name
            FROM programming_schedules ps
            JOIN schedule_market_assignments sma ON ps.schedule_id = sma.schedule_id
            WHERE sma.market_id = ?
              AND sma.effective_start_date <= ?
              AND (sma.effective_end_date IS NULL OR sma.effective_end_date >= ?)
              AND ps.is_active = 1
            ORDER BY sma.assignment_priority DESC, sma.effective_start_date DESC
            LIMIT 1
        """, (market_id, air_date, air_date))
        
        result = cursor.fetchone()
        if result:
            schedule_id, schedule_name = result
            print(f"      ✅ Found Schedule: {schedule_name} (ID: {schedule_id})")
            
            # Now test for blocks
            cursor.execute("""
                SELECT COUNT(*) 
                FROM language_blocks
                WHERE schedule_id = ? AND day_of_week = ? AND is_active = 1
            """, (schedule_id, day_of_week.lower()))
            
            block_count = cursor.fetchone()[0]
            print(f"      📋 Available blocks for {day_of_week}: {block_count}")
            
        else:
            print(f"      ❌ No schedule found!")
            
            # Debug why no schedule found
            print(f"         Checking market assignments for market_id {market_id}:")
            cursor.execute("""
                SELECT sma.effective_start_date, sma.effective_end_date, ps.schedule_name, ps.is_active
                FROM schedule_market_assignments sma
                JOIN programming_schedules ps ON sma.schedule_id = ps.schedule_id
                WHERE sma.market_id = ?
            """, (market_id,))
            
            market_assignments = cursor.fetchall()
            for assignment in market_assignments:
                start_date, end_date, schedule_name, is_active = assignment
                end_display = end_date or "NULL"
                active_display = "active" if is_active else "inactive"
                
                # Check date logic
                date_match = air_date >= start_date and (end_date is None or air_date <= end_date)
                print(f"         - {schedule_name} ({active_display}): {start_date} to {end_display} - Date match: {date_match}")
    
    # Check market assignment issue
    print(f"\n📊 Market ID Analysis:")
    cursor.execute("SELECT COUNT(*) FROM spots WHERE market_id IS NULL")
    null_market_count = cursor.fetchone()[0]
    print(f"   Spots with NULL market_id: {null_market_count:,}")
    
    cursor.execute("SELECT COUNT(DISTINCT market_id) FROM spots WHERE market_id IS NOT NULL")
    distinct_markets = cursor.fetchone()[0]
    print(f"   Distinct non-NULL market_ids: {distinct_markets}")
    
    if null_market_count > 0:
        print(f"   ❌ PROBLEM: {null_market_count:,} spots have no market assigned!")
        print(f"      This is why schedule resolution is failing.")
        
        # Check if there are spots with valid market_ids
        cursor.execute("""
            SELECT s.spot_id, s.market_id, m.market_code, s.air_date, s.day_of_week
            FROM spots s
            JOIN markets m ON s.market_id = m.market_id
            WHERE s.time_in IS NOT NULL 
              AND s.time_out IS NOT NULL 
              AND s.day_of_week IS NOT NULL
            LIMIT 5
        """)
        
        valid_market_spots = cursor.fetchall()
        if valid_market_spots:
            print(f"   ✅ Found {len(valid_market_spots)} spots with valid markets:")
            for spot in valid_market_spots:
                print(f"      Spot {spot[0]}: {spot[2]} ({spot[1]}) - {spot[3]} {spot[4]}")
        else:
            print(f"   ❌ No spots found with valid market assignments!")
    
    # Step 4: Check programming schedules status
    print("\n4. Programming Schedules Status:")
    cursor.execute("""
        SELECT schedule_id, schedule_name, schedule_type, effective_start_date, 
               effective_end_date, is_active
        FROM programming_schedules
        ORDER BY schedule_name
    """)
    
    schedules = cursor.fetchall()
    for schedule in schedules:
        schedule_id, name, type_, start_date, end_date, is_active = schedule
        end_display = end_date or "ongoing"
        status = "ACTIVE" if is_active else "INACTIVE"
        print(f"   {name} (ID: {schedule_id}): {type_} - {start_date} to {end_display} [{status}]")
    
    # Step 5: Check for data type issues (only if we have test spots)
    if test_spots:
        print("\n5. Data Type Check:")
        cursor.execute("SELECT air_date FROM spots WHERE spot_id = ? LIMIT 1", (test_spots[0][0],))
        air_date_sample = cursor.fetchone()[0]
        print(f"   Sample air_date type: {type(air_date_sample)} = '{air_date_sample}'")
        
        cursor.execute("SELECT effective_start_date FROM schedule_market_assignments LIMIT 1")
        start_date_sample = cursor.fetchone()[0]
        print(f"   Sample effective_start_date type: {type(start_date_sample)} = '{start_date_sample}'")
    else:
        print("\n5. Data Type Check: Skipped (no test spots available)")
    
    conn.close()


def fix_schedule_resolution_issue(db_path):
    """Provide specific fixes based on findings"""
    print("\n🔧 Suggested Fixes:")
    print("=" * 30)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Check if dates are strings vs dates
    cursor.execute("SELECT air_date FROM spots LIMIT 1")
    air_date = cursor.fetchone()[0]
    
    cursor.execute("SELECT effective_start_date FROM schedule_market_assignments LIMIT 1")
    start_date = cursor.fetchone()[0]
    
    print(f"Air date format: {air_date} (type: {type(air_date)})")
    print(f"Start date format: {start_date} (type: {type(start_date)})")
    
    # Test direct date comparison
    cursor.execute("""
        SELECT COUNT(*) FROM spots s
        JOIN schedule_market_assignments sma ON s.market_id = sma.market_id
        WHERE s.air_date >= sma.effective_start_date
        LIMIT 1
    """)
    
    comparison_works = cursor.fetchone()[0] > 0
    print(f"Date comparison works: {comparison_works}")
    
    if not comparison_works:
        print("\n❌ Issue: Date comparison failing")
        print("   Fix: Convert dates to same format in query")
        print("   Try: DATE(s.air_date) >= DATE(sma.effective_start_date)")
    
    conn.close()


if __name__ == "__main__":
    db_path = "data/database/production.db"
    
    if not os.path.exists(db_path):
        print(f"❌ Database not found: {db_path}")
        sys.exit(1)
    
    debug_schedule_resolution(db_path)
    fix_schedule_resolution_issue(db_path)