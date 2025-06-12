#!/usr/bin/env python3
"""
Basic Integration Test for Language Block Service - Phase 1
==========================================================

This test validates the minimal service works with your real database and data.
Run this to verify the foundation is solid before moving to Phase 2.

Place this file in: tests/services/test_language_block_service_basic.py
"""

import sqlite3
import sys
import os
from datetime import datetime

# Add src to path so we can import our service
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

try:
    from services.language_block_service import LanguageBlockService, test_language_block_service
except ImportError as e:
    print(f"Import error: {e}")
    print("Make sure language_block_service.py is in src/services/")
    sys.exit(1)


class BasicLanguageBlockTest:
    """Basic integration test for Language Block Service"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.service = None
        self.test_results = {}
    
    def setup(self):
        """Setup test environment"""
        print("Setting up Language Block Service test...")
        
        # Connect to database
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.service = LanguageBlockService(self.conn)
            print("✅ Database connection established")
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            return False
        
        return True
    
    def teardown(self):
        """Cleanup test environment"""
        if hasattr(self, 'conn'):
            self.conn.close()
        print("✅ Test cleanup completed")
    
    def test_database_schema(self):
        """Test that required database tables exist and have data"""
        print("\n--- Testing Database Schema ---")
        
        required_tables = [
            'spots', 'markets', 'languages', 'programming_schedules',
            'schedule_market_assignments', 'language_blocks', 'spot_language_blocks'
        ]
        
        cursor = self.conn.cursor()
        
        for table in required_tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"✅ {table}: {count:,} records")
            except Exception as e:
                print(f"❌ {table}: Error - {e}")
                return False
        
        # Check for programming schedules
        cursor.execute("""
            SELECT schedule_name, COUNT(*) as blocks 
            FROM programming_schedules ps 
            LEFT JOIN language_blocks lb ON ps.schedule_id = lb.schedule_id 
            WHERE ps.is_active = 1 
            GROUP BY ps.schedule_name
        """)
        
        schedules = cursor.fetchall()
        if schedules:
            print("\n📋 Active Programming Schedules:")
            for schedule_name, block_count in schedules:
                print(f"   {schedule_name}: {block_count} blocks")
        else:
            print("❌ No active programming schedules found")
            return False
        
        return True
    
    def test_spot_data_quality(self):
        """Test spot data quality for assignment"""
        print("\n--- Testing Spot Data Quality ---")
        
        cursor = self.conn.cursor()
        
        # Total spots
        cursor.execute("SELECT COUNT(*) FROM spots")
        total_spots = cursor.fetchone()[0]
        print(f"📊 Total spots: {total_spots:,}")
        
        # Assignable spots (have required fields)
        cursor.execute("""
            SELECT COUNT(*) FROM spots 
            WHERE time_in IS NOT NULL 
              AND time_out IS NOT NULL 
              AND day_of_week IS NOT NULL
              AND (revenue_type != 'Trade' OR revenue_type IS NULL)
        """)
        assignable_spots = cursor.fetchone()[0]
        print(f"📊 Assignable spots: {assignable_spots:,} ({assignable_spots/total_spots*100:.1f}%)")
        
        # Already assigned spots
        cursor.execute("SELECT COUNT(*) FROM spot_language_blocks")
        assigned_spots = cursor.fetchone()[0]
        print(f"📊 Already assigned: {assigned_spots:,}")
        
        # Available for assignment
        cursor.execute("""
            SELECT COUNT(*) FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            WHERE slb.spot_id IS NULL
              AND s.time_in IS NOT NULL 
              AND s.time_out IS NOT NULL 
              AND s.day_of_week IS NOT NULL
              AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        """)
        unassigned_spots = cursor.fetchone()[0]
        print(f"📊 Available for assignment: {unassigned_spots:,}")
        
        if unassigned_spots == 0:
            print("⚠️  No unassigned spots found - may need to clear existing assignments for testing")
            return False
        
        return True
    
    def test_single_spot_assignment(self):
        """Test assigning a single spot"""
        print("\n--- Testing Single Spot Assignment ---")
        
        # Get one unassigned spot
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT s.spot_id, s.bill_code, s.air_date, s.time_in, s.time_out, m.market_code
            FROM spots s
            JOIN markets m ON s.market_id = m.market_id
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            WHERE slb.spot_id IS NULL
              AND s.market_id IS NOT NULL
              AND s.time_in IS NOT NULL 
              AND s.time_out IS NOT NULL 
              AND s.day_of_week IS NOT NULL
              AND (s.revenue_type NOT IN ('Trade', 'Branded Content') OR s.revenue_type IS NULL)
              AND (s.bill_code NOT LIKE '%PRODUCTION%' OR s.bill_code IS NULL)
            LIMIT 1
        """)
        
        spot_row = cursor.fetchone()
        if not spot_row:
            print("❌ No unassigned spots available for testing")
            return False
        
        spot_id, bill_code, air_date, time_in, time_out, market_code = spot_row
        print(f"🎯 Testing spot {spot_id} ({bill_code}) - {market_code} {air_date} {time_in}-{time_out}")
        
        # Attempt assignment
        try:
            result = self.service.assign_single_spot(spot_id)
            
            if result.success:
                print(f"✅ Assignment successful!")
                print(f"   Customer Intent: {result.customer_intent.value}")
                print(f"   Schedule ID: {result.schedule_id}")
                print(f"   Block ID: {result.block_id}")
                print(f"   Spans Multiple: {result.spans_multiple_blocks}")
                print(f"   Requires Attention: {result.requires_attention}")
                
                if result.alert_reason:
                    print(f"   Alert: {result.alert_reason}")
                
                # Verify assignment was saved
                cursor.execute("SELECT COUNT(*) FROM spot_language_blocks WHERE spot_id = ?", (spot_id,))
                if cursor.fetchone()[0] > 0:
                    print("✅ Assignment saved to database")
                else:
                    print("❌ Assignment not found in database")
                    return False
                
            else:
                print(f"❌ Assignment failed: {result.error_message}")
                return False
                
        except Exception as e:
            print(f"❌ Assignment error: {e}")
            return False
        
        return True
    
    def test_batch_assignment(self, limit: int = 10):
        """Test batch assignment with multiple spots"""
        print(f"\n--- Testing Batch Assignment ({limit} spots) ---")
        
        try:
            results = self.service.test_assignment(limit)
            
            print(f"📊 Batch Test Results:")
            print(f"   Spots Tested: {results['spots_tested']}")
            print(f"   Success Rate: {results['success_rate']:.1%}")
            print(f"   Stats: {results['stats']}")
            
            # Show sample assignments
            if results['spot_details']:
                print(f"\n📋 Sample Assignments:")
                for detail in results['spot_details'][:5]:  # Show first 5
                    block_info = detail['block_name'] or 'No Coverage'
                    attention = " ⚠️" if detail['requires_attention'] else ""
                    print(f"   Spot {detail['spot_id']} ({detail['bill_code']}): {detail['customer_intent']} -> {block_info}{attention}")
            
            # Success criteria
            success_rate = results['success_rate']
            if success_rate >= 0.8:  # 80% success rate minimum
                print(f"✅ Batch assignment successful (>{80}% success rate)")
                return True
            else:
                print(f"❌ Batch assignment below threshold ({success_rate:.1%} < 80%)")
                return False
                
        except Exception as e:
            print(f"❌ Batch assignment error: {e}")
            return False
    
    def test_assignment_quality(self):
        """Test the quality of assignments made"""
        print("\n--- Testing Assignment Quality ---")
        
        cursor = self.conn.cursor()
        
        # Get assignment breakdown
        cursor.execute("""
            SELECT customer_intent, COUNT(*) as count
            FROM spot_language_blocks 
            WHERE assigned_date >= datetime('now', '-1 hour')
            GROUP BY customer_intent
        """)
        
        intent_breakdown = cursor.fetchall()
        if intent_breakdown:
            print("📊 Recent Assignment Intent Breakdown:")
            total_recent = sum(count for _, count in intent_breakdown)
            for intent, count in intent_breakdown:
                percentage = count / total_recent * 100
                print(f"   {intent}: {count} ({percentage:.1f}%)")
        
        # Check for spots requiring attention
        cursor.execute("""
            SELECT COUNT(*) FROM spot_language_blocks 
            WHERE requires_attention = 1 AND assigned_date >= datetime('now', '-1 hour')
        """)
        attention_count = cursor.fetchone()[0]
        
        # Check for errors
        cursor.execute("""
            SELECT COUNT(*) FROM spot_language_blocks 
            WHERE notes LIKE '%error%' AND assigned_date >= datetime('now', '-1 hour')
        """)
        error_count = cursor.fetchone()[0]
        
        print(f"⚠️  Spots requiring attention: {attention_count}")
        print(f"❌ Assignment errors: {error_count}")
        
        return True
    
    def run_all_tests(self):
        """Run complete test suite"""
        print(f"🧪 Starting Language Block Service Integration Test")
        print(f"Database: {self.db_path}")
        print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        
        if not self.setup():
            return False
        
        try:
            tests = [
                ("Database Schema", self.test_database_schema),
                ("Spot Data Quality", self.test_spot_data_quality),
                ("Single Spot Assignment", self.test_single_spot_assignment),
                ("Batch Assignment", lambda: self.test_batch_assignment(10)),
                ("Assignment Quality", self.test_assignment_quality)
            ]
            
            passed = 0
            failed = 0
            
            for test_name, test_func in tests:
                try:
                    if test_func():
                        passed += 1
                        print(f"✅ {test_name}: PASSED")
                    else:
                        failed += 1
                        print(f"❌ {test_name}: FAILED")
                except Exception as e:
                    failed += 1
                    print(f"❌ {test_name}: ERROR - {e}")
            
            print("\n" + "=" * 60)
            print(f"🧪 Test Results: {passed} passed, {failed} failed")
            
            if failed == 0:
                print("✅ ALL TESTS PASSED - Language Block Service is ready for Phase 2!")
                return True
            else:
                print("❌ Some tests failed - review issues before proceeding")
                return False
                
        finally:
            self.teardown()


def main():
    """Main test runner"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Basic Language Block Service Integration Test')
    parser.add_argument('--db-path', required=True, help='Path to database file')
    parser.add_argument('--limit', type=int, default=10, help='Number of spots for batch test')
    
    args = parser.parse_args()
    
    # Verify database exists
    if not os.path.exists(args.db_path):
        print(f"❌ Database file not found: {args.db_path}")
        return False
    
    # Run tests
    test_runner = BasicLanguageBlockTest(args.db_path)
    return test_runner.run_all_tests()


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)