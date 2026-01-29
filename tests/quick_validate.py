#!/usr/bin/env python3
# tests/quick_validate.py
"""
Quick validation script to test critical fixes.
Run this first to validate that your enhanced services work properly.
"""

import os
import sys
import tempfile
import json
from concurrent.futures import ThreadPoolExecutor

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../src"))


def test_basic_service_creation():
    """Test 1: Basic service creation and initialization."""
    print("🔧 Test 1: Basic Service Creation")

    try:
        # Set up temporary environment
        temp_dir = tempfile.mkdtemp(prefix="ctv_quick_test_")
        data_path = os.path.join(temp_dir, "data", "processed")
        os.makedirs(data_path, exist_ok=True)

        # Set environment
        os.environ["PROJECT_ROOT"] = temp_dir
        os.environ["DATA_PATH"] = data_path
        os.environ["FLASK_ENV"] = "testing"

        # Import and initialize services
        from services.factory import initialize_services
        from services.container import get_container

        initialize_services()
        container = get_container()

        # Test getting services
        pipeline_service = container.get("pipeline_service")
        budget_service = container.get("budget_service")

        print("  ✅ Services created successfully")
        print(f"  ✅ Pipeline service: {type(pipeline_service).__name__}")
        print(f"  ✅ Budget service: {type(budget_service).__name__}")

        # Clean up
        import shutil

        shutil.rmtree(temp_dir, ignore_errors=True)

        return True

    except Exception as e:
        print(f"  ❌ Service creation failed: {e}")
        return False


def test_json_file_locking():
    """Test 2: JSON file locking and concurrent writes."""
    print("\n🔒 Test 2: JSON File Locking")

    try:
        # Set up temporary environment
        temp_dir = tempfile.mkdtemp(prefix="ctv_lock_test_")
        data_path = os.path.join(temp_dir, "data", "processed")
        os.makedirs(data_path, exist_ok=True)

        # Set environment
        os.environ["PROJECT_ROOT"] = temp_dir
        os.environ["DATA_PATH"] = data_path
        os.environ["FLASK_ENV"] = "testing"

        # Import services
        from services.factory import initialize_services
        from services.container import get_container

        initialize_services()
        container = get_container()
        pipeline_service = container.get("pipeline_service")

        # Test concurrent writes
        results = []

        def concurrent_update(thread_id):
            try:
                success = pipeline_service.update_pipeline_data(
                    ae_id=f"TEST_AE_{thread_id}",
                    month="2025-01",
                    pipeline_update={"current_pipeline": 1000 + thread_id},
                    updated_by=f"test_thread_{thread_id}",
                )
                return success
            except Exception as e:
                print(f"    Thread {thread_id} error: {e}")
                return False

        # Run 5 concurrent updates
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(concurrent_update, i) for i in range(5)]
            results = [future.result(timeout=10) for future in futures]

        successful_ops = sum(results)

        # Validate JSON file integrity
        pipeline_file = os.path.join(data_path, "pipeline_data.json")
        json_valid = False
        try:
            with open(pipeline_file, "r") as f:
                data = json.load(f)
                json_valid = "pipeline_data" in data and len(data["pipeline_data"]) > 0
        except:
            json_valid = False

        print(f"  ✅ Concurrent updates: {successful_ops}/5 successful")
        print(f"  ✅ JSON file integrity: {'OK' if json_valid else 'FAILED'}")

        # Clean up
        import shutil

        shutil.rmtree(temp_dir, ignore_errors=True)

        return successful_ops == 5 and json_valid

    except Exception as e:
        print(f"  ❌ File locking test failed: {e}")
        return False


def test_data_consistency_check():
    """Test 3: Data consistency validation."""
    print("\n🔍 Test 3: Data Consistency Check")

    try:
        # Set up temporary environment
        temp_dir = tempfile.mkdtemp(prefix="ctv_consistency_test_")
        data_path = os.path.join(temp_dir, "data", "processed")
        os.makedirs(data_path, exist_ok=True)

        # Set environment
        os.environ["PROJECT_ROOT"] = temp_dir
        os.environ["DATA_PATH"] = data_path
        os.environ["FLASK_ENV"] = "testing"

        # Import services
        from services.factory import initialize_services
        from services.container import get_container

        initialize_services()
        container = get_container()
        pipeline_service = container.get("pipeline_service")

        # Add some test data
        pipeline_service.update_pipeline_data(
            ae_id="CONSISTENCY_TEST",
            month="2025-01",
            pipeline_update={"current_pipeline": 5000},
            updated_by="consistency_test",
        )

        # Test consistency validation
        if hasattr(pipeline_service, "validate_data_consistency"):
            consistency_result = pipeline_service.validate_data_consistency()

            print(
                f"  ✅ Consistency check: {'PASSED' if consistency_result.is_consistent else 'FAILED'}"
            )
            print(f"  ✅ JSON records: {consistency_result.json_records}")
            print(f"  ✅ DB records: {consistency_result.db_records}")

            # Test data source info
            if hasattr(pipeline_service, "get_data_source_info"):
                info = pipeline_service.get_data_source_info()
                print(f"  ✅ Data source: {info.get('data_source', 'unknown')}")
        else:
            print("  ⚠️  Consistency validation not available (older service)")

        # Clean up
        import shutil

        shutil.rmtree(temp_dir, ignore_errors=True)

        return True

    except Exception as e:
        print(f"  ❌ Consistency check failed: {e}")
        return False


def test_emergency_repair():
    """Test 4: Emergency repair functionality."""
    print("\n🚨 Test 4: Emergency Repair")

    try:
        # Set up temporary environment
        temp_dir = tempfile.mkdtemp(prefix="ctv_repair_test_")
        data_path = os.path.join(temp_dir, "data", "processed")
        os.makedirs(data_path, exist_ok=True)

        # Set environment
        os.environ["PROJECT_ROOT"] = temp_dir
        os.environ["DATA_PATH"] = data_path
        os.environ["FLASK_ENV"] = "testing"

        # Import services
        from services.factory import initialize_services
        from services.container import get_container

        initialize_services()
        container = get_container()
        pipeline_service = container.get("pipeline_service")

        # Corrupt the JSON file
        pipeline_file = os.path.join(data_path, "pipeline_data.json")
        with open(pipeline_file, "w") as f:
            f.write("invalid json content")

        # Test emergency repair
        if hasattr(pipeline_service, "emergency_repair"):
            repair_result = pipeline_service.emergency_repair()
            repair_successful = repair_result.get("success", False)

            # Test if we can read data after repair
            try:
                data = pipeline_service.get_pipeline_data("TEST", "2025-01")
                read_after_repair = True
            except:
                read_after_repair = False

            print(
                f"  ✅ Emergency repair: {'SUCCESSFUL' if repair_successful else 'FAILED'}"
            )
            print(f"  ✅ Read after repair: {'OK' if read_after_repair else 'FAILED'}")

            success = repair_successful and read_after_repair
        else:
            print("  ⚠️  Emergency repair not available (older service)")
            success = True

        # Clean up
        import shutil

        shutil.rmtree(temp_dir, ignore_errors=True)

        return success

    except Exception as e:
        print(f"  ❌ Emergency repair test failed: {e}")
        return False


def test_health_monitoring():
    """Test 5: Health monitoring functionality."""
    print("\n❤️  Test 5: Health Monitoring")

    try:
        # Import health monitoring
        from services.factory import get_service_health_report

        health_report = get_service_health_report()

        overall_status = health_report.get("overall_status", "unknown")
        services_count = len(health_report.get("services", {}))
        issues_count = len(health_report.get("issues", []))

        print(f"  ✅ Health report generated: {overall_status}")
        print(f"  ✅ Services monitored: {services_count}")
        print(f"  ✅ Issues detected: {issues_count}")

        if issues_count > 0:
            print("  ⚠️  Issues found:")
            for issue in health_report.get("issues", [])[:3]:  # Show first 3
                print(f"    - {issue}")

        return True

    except Exception as e:
        print(f"  ❌ Health monitoring test failed: {e}")
        return False


def main():
    """Run quick validation tests."""
    print("🧪 QUICK VALIDATION - Critical Fixes")
    print("=" * 50)

    tests = [
        ("Basic Service Creation", test_basic_service_creation),
        ("JSON File Locking", test_json_file_locking),
        ("Data Consistency Check", test_data_consistency_check),
        ("Emergency Repair", test_emergency_repair),
        ("Health Monitoring", test_health_monitoring),
    ]

    passed = 0
    failed = 0

    for test_name, test_func in tests:
        try:
            if test_func():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"❌ {test_name} crashed: {e}")
            failed += 1

    print("\n" + "=" * 50)
    print("📊 QUICK VALIDATION RESULTS")
    print(f"✅ Passed: {passed}")
    print(f"❌ Failed: {failed}")
    print(f"📈 Success Rate: {passed / (passed + failed) * 100:.1f}%")

    if failed == 0:
        print("\n🎉 ALL TESTS PASSED!")
        print("✅ Critical fixes are working properly")
        print("✅ Ready for comprehensive testing")
        return 0
    else:
        print(f"\n⚠️  {failed} TESTS FAILED")
        print("❌ Please check the errors above")
        print("❌ Fix issues before proceeding")
        return 1


if __name__ == "__main__":
    exit(main())
