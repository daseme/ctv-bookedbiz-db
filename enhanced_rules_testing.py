#!/usr/bin/env python3
"""
Enhanced Business Rules Testing Script
=====================================

This script tests the enhanced business rules implementation to ensure:
1. Rules only apply when base logic returns 'indifferent'
2. Pattern matching works correctly  
3. Language hints are properly validated
4. Database tracking fields are populated
5. No regression in existing functionality

Usage:
    python enhanced_rules_testing.py --database data/database/production.db
    python enhanced_rules_testing.py --database data/database/production.db --create-test-data
"""

import sqlite3
import argparse
import sys
from datetime import datetime
from typing import Dict, List, Any


def create_test_data(db_path):
    """Create test spots for enhanced rule validation"""
    print("🧪 Creating test data for enhanced rules...")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Test data scenarios
    test_spots = [
        {
            'bill_code': 'TEST_TAGALOG_PATTERN',
            'time_in': '16:00:00',
            'time_out': '19:00:00',
            'language_code': 'T',
            'scenario': 'Tagalog Pattern - should trigger tagalog_pattern rule'
        },
        {
            'bill_code': 'TEST_CHINESE_PATTERN',
            'time_in': '19:00:00', 
            'time_out': '23:59:00',
            'language_code': 'M',
            'scenario': 'Chinese Pattern - should trigger chinese_pattern rule'
        },
        {
            'bill_code': 'TEST_ROS_DURATION',
            'time_in': '10:00:00',
            'time_out': '18:00:00',  # 8 hours
            'language_code': 'E',
            'scenario': 'ROS Duration - should trigger ros_duration rule'
        },
        {
            'bill_code': 'TEST_ROS_TIME',
            'time_in': '13:00:00',
            'time_out': '23:59:00',
            'language_code': 'E', 
            'scenario': 'ROS Time - should trigger ros_time rule'
        },
        {
            'bill_code': 'TEST_NO_PATTERN',
            'time_in': '10:00:00',
            'time_out': '12:00:00',
            'language_code': 'E',
            'scenario': 'No Pattern - should remain indifferent'
        },
        {
            'bill_code': 'TEST_CHINESE_WRONG_HINT',
            'time_in': '19:00:00',
            'time_out': '23:59:00', 
            'language_code': 'Hm',  # Hmong, not Chinese
            'scenario': 'Chinese Time + Wrong Hint - should remain multilanguage'
        }
    ]
    
    try:
        for i, spot in enumerate(test_spots):
            spot_id = 9000000 + i  # High spot_id to avoid conflicts
            
            cursor.execute("""
                INSERT OR REPLACE INTO spots (
                    spot_id, bill_code, air_date, day_of_week, time_in, time_out,
                    language_code, market_id, gross_rate, broadcast_month,
                    sales_person, revenue_type
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                spot_id,
                spot['bill_code'],
                '2024-01-15',  # Monday
                'monday',
                spot['time_in'],
                spot['time_out'],
                spot['language_code'],
                1,  # Default market
                100.00,  # Test rate
                'Jan-24',
                'TEST_AE',
                'Cash'
            ))
            
            print(f"   • Created test spot {spot_id}: {spot['scenario']}")
        
        conn.commit()
        print(f"✅ Created {len(test_spots)} test spots")
        return True
        
    except Exception as e:
        print(f"❌ Failed to create test data: {e}")
        return False
    finally:
        conn.close()


def test_enhanced_rules(db_path):
    """Test the enhanced business rules functionality"""
    print("🧪 Testing Enhanced Business Rules...")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Test 1: Check if migration was applied
    print("\n📊 Test 1: Migration Verification")
    try:
        cursor.execute("SELECT business_rule_applied FROM spot_language_blocks LIMIT 1")
        print("   ✅ Migration applied - new columns exist")
    except sqlite3.OperationalError:
        print("   ❌ Migration not applied - run migrate_enhanced_rules.py first")
        return False
    
    # Test 2: Check test data exists
    print("\n📊 Test 2: Test Data Verification")
    cursor.execute("SELECT COUNT(*) FROM spots WHERE spot_id >= 9000000")
    test_count = cursor.fetchone()[0]
    print(f"   • Test spots found: {test_count}")
    
    if test_count == 0:
        print("   ❌ No test data found - run with --create-test-data first")
        return False
    
    # Test 3: Show test spots before assignment
    print("\n📊 Test 3: Test Spots (Before Assignment)")
    cursor.execute("""
        SELECT spot_id, bill_code, time_in, time_out, language_code
        FROM spots 
        WHERE spot_id >= 9000000
        ORDER BY spot_id
    """)
    
    test_spots = cursor.fetchall()
    for spot in test_spots:
        print(f"   • Spot {spot[0]}: {spot[1]} | {spot[2]}-{spot[3]} | Lang: {spot[4]}")
    
    # Test 4: Check assignments with enhanced rules
    print("\n📊 Test 4: Enhanced Rule Assignments")
    cursor.execute("""
        SELECT 
            s.spot_id,
            s.bill_code,
            s.time_in,
            s.time_out,
            s.language_code,
            slb.customer_intent,
            slb.business_rule_applied,
            slb.campaign_type,
            slb.auto_resolved_date
        FROM spots s
        LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        WHERE s.spot_id >= 9000000
        ORDER BY s.spot_id
    """)
    
    assignments = cursor.fetchall()
    enhanced_count = 0
    
    for assignment in assignments:
        spot_id, bill_code, time_in, time_out, lang_code, intent, rule, campaign, auto_date = assignment
        
        if rule:
            enhanced_count += 1
            status = f"✅ Enhanced: {rule}"
        elif intent:
            status = f"⚠️ Standard: {intent}"
        else:
            status = "❌ Unassigned"
        
        print(f"   • {bill_code}: {status}")
    
    print(f"\n   Summary: {enhanced_count}/{len(assignments)} spots used enhanced rules")
    
    # Test 5: Enhanced rule analytics
    print("\n📊 Test 5: Enhanced Rule Analytics")
    try:
        cursor.execute("SELECT rule_type, spots_affected FROM enhanced_rule_analytics")
        analytics = cursor.fetchall()
        
        for rule_type, count in analytics:
            print(f"   • {rule_type}: {count} spots")
        
        print("   ✅ Analytics views working")
    except Exception as e:
        print(f"   ❌ Analytics failed: {e}")
    
    # Test 6: Pattern matching validation
    print("\n📊 Test 6: Pattern Matching Validation")
    
    # Expected results based on test data
    expected_rules = {
        'TEST_TAGALOG_PATTERN': 'tagalog_pattern',
        'TEST_CHINESE_PATTERN': 'chinese_pattern', 
        'TEST_ROS_DURATION': 'ros_duration',
        'TEST_ROS_TIME': 'ros_time',
        'TEST_NO_PATTERN': None,  # Should remain indifferent
        'TEST_CHINESE_WRONG_HINT': None  # Wrong language hint
    }
    
    validation_passed = True
    
    for assignment in assignments:
        spot_id, bill_code, time_in, time_out, lang_code, intent, rule, campaign, auto_date = assignment
        expected_rule = expected_rules.get(bill_code)
        
        if rule == expected_rule:
            print(f"   ✅ {bill_code}: Expected {expected_rule}, got {rule}")
        else:
            print(f"   ❌ {bill_code}: Expected {expected_rule}, got {rule}")
            validation_passed = False
    
    if validation_passed:
        print("   ✅ All pattern matching tests passed")
    else:
        print("   ❌ Some pattern matching tests failed")
    
    conn.close()
    return validation_passed


def cleanup_test_data(db_path):
    """Clean up test data"""
    print("🧹 Cleaning up test data...")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Remove test spot assignments
        cursor.execute("DELETE FROM spot_language_blocks WHERE spot_id >= 9000000")
        deleted_assignments = cursor.rowcount
        
        # Remove test spots
        cursor.execute("DELETE FROM spots WHERE spot_id >= 9000000")
        deleted_spots = cursor.rowcount
        
        conn.commit()
        print(f"   ✅ Deleted {deleted_spots} test spots and {deleted_assignments} assignments")
        
    except Exception as e:
        print(f"   ❌ Cleanup failed: {e}")
    finally:
        conn.close()


def show_production_stats(db_path):
    """Show production statistics for enhanced rules"""
    print("📊 Production Enhanced Rules Statistics")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Total assignments
        cursor.execute("SELECT COUNT(*) FROM spot_language_blocks")
        total = cursor.fetchone()[0]
        
        # Enhanced rule assignments
        cursor.execute("""
            SELECT 
                business_rule_applied,
                COUNT(*) as count,
                COUNT(*) * 100.0 / ? as percentage
            FROM spot_language_blocks
            WHERE business_rule_applied IS NOT NULL
            GROUP BY business_rule_applied
            ORDER BY count DESC
        """, (total,))
        
        enhanced_stats = cursor.fetchall()
        
        print(f"\n   Total assignments: {total:,}")
        print(f"   Enhanced rule assignments:")
        
        total_enhanced = 0
        for rule, count, percentage in enhanced_stats:
            print(f"     • {rule}: {count:,} ({percentage:.1f}%)")
            total_enhanced += count
        
        standard_count = total - total_enhanced
        standard_pct = (standard_count / total * 100) if total > 0 else 0
        
        print(f"     • standard_assignment: {standard_count:,} ({standard_pct:.1f}%)")
        
        print(f"\n   Enhanced rules impact: {total_enhanced:,} spots ({total_enhanced/total*100:.1f}%)")
        
    except Exception as e:
        print(f"   ❌ Statistics failed: {e}")
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Enhanced Business Rules Testing")
    parser.add_argument("--database", default="data/database/production.db", help="Database path")
    parser.add_argument("--create-test-data", action="store_true", help="Create test data")
    parser.add_argument("--cleanup", action="store_true", help="Clean up test data")
    parser.add_argument("--stats", action="store_true", help="Show production statistics")
    
    args = parser.parse_args()
    
    if args.create_test_data:
        success = create_test_data(args.database)
        return 0 if success else 1
    
    elif args.cleanup:
        cleanup_test_data(args.database)
        return 0
    
    elif args.stats:
        show_production_stats(args.database)
        return 0
    
    else:
        success = test_enhanced_rules(args.database)
        return 0 if success else 1


if __name__ == "__main__":
    exit(main())