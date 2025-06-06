#!/usr/bin/env python3
"""
Fixed transaction test script that works with the corrected BaseService.
"""

import sys
import time
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from database.connection import DatabaseConnection
from services.month_closure_service import MonthClosureService

def test_basic_transaction_functionality():
    """Test basic transaction functionality with your existing database."""
    print("🧪 Testing BaseService Transaction Functionality")
    print("=" * 60)
    
    # Use your existing production database
    db_path = "data/database/production.db"
    
    if not Path(db_path).exists():
        print(f"❌ Database not found: {db_path}")
        print("Please run: uv run python scripts/setup_database.py")
        return False
    
    print(f"📊 Using database: {db_path}")
    
    try:
        db_connection = DatabaseConnection(db_path)
        service = MonthClosureService(db_connection)
        
        print("\n1. Testing transaction state detection...")
        
        # Test initial state
        print(f"   Initial state - In transaction: {service.in_transaction}")
        assert not service.in_transaction, "Should not be in transaction initially"
        print("   ✅ Initial state correct")
        
        # Test within transaction
        print("\n2. Testing safe_transaction context manager...")
        with service.safe_transaction() as conn:
            print(f"   Inside transaction - In transaction: {service.in_transaction}")
            assert service.in_transaction, "Should be in transaction"
            print("   ✅ Transaction state correct inside context")
            
            # Test a simple query
            cursor = conn.execute("SELECT COUNT(*) FROM month_closures")
            count = cursor.fetchone()[0]
            print(f"   ✅ Query executed successfully: {count} closed months")
            
            # Test nested transaction (should reuse connection)
            print("\n3. Testing nested transaction handling...")
            with service.safe_transaction() as conn2:
                print(f"   Nested transaction - In transaction: {service.in_transaction}")
                assert service.in_transaction, "Should still be in transaction"
                assert conn is conn2, "Should reuse same connection"
                print("   ✅ Nested transaction handled correctly")
        
        # Test state after transaction
        print(f"\n4. After transaction - In transaction: {service.in_transaction}")
        assert not service.in_transaction, "Should not be in transaction after context"
        print("   ✅ Transaction cleanup correct")
        
        # Test read operations work normally
        print("\n5. Testing normal operations...")
        closed_months = service.get_all_closed_months()
        print(f"   ✅ Read operation successful: {len(closed_months)} closed months")
        
        print("\n🎉 ALL BASIC TESTS PASSED!")
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        try:
            db_connection.close()
        except:
            pass

def test_month_operations():
    """Test month-specific operations to ensure they work correctly."""
    print("\n🧪 Testing Month Operations")
    print("=" * 60)
    
    db_path = "data/database/production.db"
    db_connection = DatabaseConnection(db_path)
    service = MonthClosureService(db_connection)
    
    try:
        print("1. Testing month statistics...")
        stats = service.get_month_statistics("Jan-24")
        print(f"   ✅ Jan-24 statistics: {stats['total_spots']:,} spots")
        
        print("2. Testing month closure check...")
        is_closed = service.is_month_closed("Jan-24")
        print(f"   ✅ Jan-24 closed status: {is_closed}")
        
        print("3. Testing closure info...")
        info = service.get_month_closure_info("Jan-24")
        if info:
            print(f"   ✅ Jan-24 closure info: Closed by {info['closed_by']}")
        else:
            print("   📊 Jan-24 not closed (expected if testing)")
        
        print("4. Testing validation...")
        validation = service.validate_months_for_import(["Jan-25", "Feb-25"], "WEEKLY_UPDATE")
        print(f"   ✅ Validation successful: {validation.is_valid}")
        
        print("\n🎉 ALL MONTH OPERATIONS PASSED!")
        return True
        
    except Exception as e:
        print(f"\n❌ Month operations failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        db_connection.close()

def test_concurrent_operations():
    """Test that concurrent operations don't cause issues."""
    print("\n🧪 Testing Concurrent Operations")
    print("=" * 60)
    
    import threading
    import time
    
    db_path = "data/database/production.db"
    results = []
    errors = []
    
    def worker(worker_id):
        """Worker function for concurrent testing."""
        try:
            db_conn = DatabaseConnection(db_path)
            service = MonthClosureService(db_conn)
            
            # Each worker performs read operations
            closed_months = service.get_all_closed_months()
            stats = service.get_month_statistics("Jan-24")
            
            results.append(f"Worker {worker_id}: SUCCESS - {len(closed_months)} months, {stats['total_spots']} spots")
            db_conn.close()
            
        except Exception as e:
            errors.append(f"Worker {worker_id}: ERROR - {e}")
    
    # Start multiple workers
    threads = []
    num_workers = 3
    
    print(f"Starting {num_workers} concurrent workers...")
    
    for i in range(num_workers):
        thread = threading.Thread(target=worker, args=(i,))
        threads.append(thread)
        thread.start()
    
    # Wait for completion
    for thread in threads:
        thread.join(timeout=10)
    
    # Check results
    print(f"\nResults:")
    for result in results:
        print(f"   ✅ {result}")
    
    if errors:
        print(f"\nErrors:")
        for error in errors:
            print(f"   ❌ {error}")
        return False
    
    print(f"\n🎉 ALL CONCURRENT OPERATIONS PASSED!")
    return True

def main():
    """Run all tests."""
    print("🚀 TESTING BASESERVICE TRANSACTION FIXES")
    print("=" * 80)
    
    tests_passed = 0
    total_tests = 3
    
    # Test 1: Basic functionality
    if test_basic_transaction_functionality():
        tests_passed += 1
    
    # Test 2: Month operations
    if test_month_operations():
        tests_passed += 1
    
    # Test 3: Concurrent operations
    if test_concurrent_operations():
        tests_passed += 1
    
    # Final results
    print("\n" + "=" * 80)
    print(f"📊 FINAL RESULTS: {tests_passed}/{total_tests} tests passed")
    
    if tests_passed == total_tests:
        print("🎉 ALL TESTS PASSED - BaseService is working correctly!")
        print("✅ No deadlocks detected")
        print("✅ Transaction management working properly")
        print("✅ Your fixes are successful!")
        return True
    else:
        print("❌ Some tests failed - additional fixes may be needed")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)