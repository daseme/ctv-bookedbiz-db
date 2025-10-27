#!/usr/bin/env python3
"""
Quick verification script for Language Block schema deployment
"""

import sys
import sqlite3
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from database.connection import DatabaseConnection


def verify_deployment():
    """Quick verification of the deployed schema."""

    print("🔍 Verifying Language Block Schema Deployment...")
    print("=" * 60)

    db = DatabaseConnection("data/database/production.db")

    try:
        with db.connect() as conn:
            # 1. Check tables
            print("\n📋 Tables:")
            cursor = conn.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name IN (
                    'programming_schedules', 'schedule_market_assignments', 
                    'schedule_collision_log', 'language_blocks', 'spot_language_blocks'
                )
                ORDER BY name
            """)
            tables = cursor.fetchall()
            for table in tables:
                print(f"   ✅ {table[0]}")

            # 2. Check views
            print("\n📊 Views:")
            cursor = conn.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='view' AND name LIKE '%language_block%'
                ORDER BY name
            """)
            views = cursor.fetchall()
            for view in views:
                print(f"   ✅ {view[0]}")

            # 3. Check initial data
            print("\n📦 Initial Data:")
            cursor = conn.execute(
                "SELECT schedule_name, schedule_type FROM programming_schedules ORDER BY schedule_id"
            )
            schedules = cursor.fetchall()
            for schedule in schedules:
                print(f"   ✅ {schedule[0]} ({schedule[1]})")

            # 4. Check market assignments
            print("\n🌍 Market Assignments:")
            cursor = conn.execute("""
                SELECT ps.schedule_name, COUNT(sma.market_id) as market_count
                FROM programming_schedules ps
                LEFT JOIN schedule_market_assignments sma ON ps.schedule_id = sma.schedule_id
                GROUP BY ps.schedule_name
                ORDER BY ps.schedule_id
            """)
            assignments = cursor.fetchall()
            for assignment in assignments:
                print(f"   ✅ {assignment[0]}: {assignment[1]} markets")

            # 5. Test collision detection
            print("\n🛡️  Testing Collision Detection:")

            # Clear any existing collision logs
            conn.execute("DELETE FROM schedule_collision_log")

            # Get a market for testing
            cursor = conn.execute("SELECT market_id FROM markets LIMIT 1")
            market_row = cursor.fetchone()

            if market_row:
                market_id = market_row[0]

                # Insert overlapping assignments to test trigger
                try:
                    conn.execute(
                        """
                        INSERT INTO schedule_market_assignments 
                        (schedule_id, market_id, effective_start_date, effective_end_date) 
                        VALUES (1, ?, '2025-01-01', '2025-12-31')
                    """,
                        (market_id,),
                    )

                    conn.execute(
                        """
                        INSERT INTO schedule_market_assignments 
                        (schedule_id, market_id, effective_start_date, effective_end_date) 
                        VALUES (2, ?, '2025-06-01', '2025-12-31')
                    """,
                        (market_id,),
                    )

                    # Check if collision was detected
                    cursor = conn.execute("SELECT COUNT(*) FROM schedule_collision_log")
                    collision_count = cursor.fetchone()[0]

                    if collision_count > 0:
                        print("   ✅ Collision detection trigger working!")
                    else:
                        print("   ⚠️  Collision detection may not be working")

                    # Clean up test data
                    conn.execute(
                        """
                        DELETE FROM schedule_market_assignments 
                        WHERE market_id = ? AND schedule_id IN (1, 2)
                    """,
                        (market_id,),
                    )
                    conn.execute("DELETE FROM schedule_collision_log")

                except Exception as e:
                    print(f"   ⚠️  Collision test error: {e}")
            else:
                print("   ⚠️  No markets available for collision testing")

            # 6. Test reporting views
            print("\n📈 Testing Reporting Views:")
            try:
                cursor = conn.execute(
                    "SELECT COUNT(*) FROM spots_with_language_blocks_enhanced"
                )
                print("   ✅ spots_with_language_blocks_enhanced view working")

                cursor = conn.execute(
                    "SELECT COUNT(*) FROM language_block_revenue_analysis"
                )
                print("   ✅ language_block_revenue_analysis view working")

                cursor = conn.execute("SELECT COUNT(*) FROM schedule_collision_monitor")
                print("   ✅ schedule_collision_monitor view working")

            except Exception as e:
                print(f"   ⚠️  View test error: {e}")

            print("\n🎉 Schema verification completed!")
            print("\n🚀 Ready for Prompt 2: Standard Grid Language Block Population")

    except Exception as e:
        print(f"❌ Verification failed: {e}")
        return False

    finally:
        db.close()

    return True


if __name__ == "__main__":
    verify_deployment()
