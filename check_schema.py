#!/usr/bin/env python3
"""
Quick script to check the actual database schema for the spots table.
"""

import sqlite3

def check_spots_schema():
    """Check what constraints are actually in the spots table."""
    
    conn = sqlite3.connect('data/database/test.db')
    
    try:
        # Get the CREATE TABLE statement for spots
        cursor = conn.execute("SELECT sql FROM sqlite_master WHERE name='spots'")
        result = cursor.fetchone()
        
        if result:
            schema = result[0]
            print("Current SPOTS table schema:")
            print("=" * 50)
            print(schema)
            print("=" * 50)
            
            # Check specifically for the problematic constraint
            if "CHECK (gross_rate >= 0 OR gross_rate IS NULL)" in schema:
                print("❌ PROBLEM: Database still has the negative value constraint!")
                print("   The schema update did not take effect.")
            else:
                print("✅ GOOD: No negative value constraint found.")
                
            # Check for other financial constraints
            constraints_found = []
            if "CHECK (station_net >= 0" in schema:
                constraints_found.append("station_net >= 0")
            if "CHECK (broker_fees >= 0" in schema:
                constraints_found.append("broker_fees >= 0")
            if "CHECK (spot_value >= 0" in schema:
                constraints_found.append("spot_value >= 0")
                
            if constraints_found:
                print(f"❌ Other financial constraints found: {', '.join(constraints_found)}")
            else:
                print("✅ No other problematic financial constraints found.")
                
        else:
            print("❌ ERROR: spots table not found in database!")
            
    except Exception as e:
        print(f"❌ ERROR checking schema: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_spots_schema()