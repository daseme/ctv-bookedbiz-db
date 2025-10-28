#!/usr/bin/env python3
# tests/test_pipeline_decay.py
"""
Test script for Pipeline Decay System.
Validates that the decay system works correctly with real-time adjustments.
"""

import os
import sys
import tempfile
import time
import json
from datetime import datetime, timedelta

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../src"))


def test_pipeline_decay_system():
    """Test the complete pipeline decay system."""
    print("🧪 PIPELINE DECAY SYSTEM TEST")
    print("=" * 50)

    try:
        # Set up test environment
        temp_dir = tempfile.mkdtemp(prefix="decay_test_")
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
        pipeline_service = container.get("pipeline_service")

        print("✅ Pipeline service initialized with decay system")

        # Test 1: Set initial calibration baseline
        print("\n📊 Test 1: Setting Calibration Baseline")

        ae_id = "AE_TEST_001"
        month = "2025-01"
        initial_pipeline = 50000

        success = pipeline_service.set_pipeline_calibration(
            ae_id=ae_id,
            month=month,
            pipeline_value=initial_pipeline,
            calibrated_by="test_user",
            session_id="TEST_SESSION_001",
        )

        if success:
            print(f"✅ Calibration baseline set: {initial_pipeline:,}")

            # Verify calibration
            decay_summary = pipeline_service.get_pipeline_decay_summary(ae_id, month)
            if decay_summary:
                print(
                    f"✅ Calibrated pipeline: {decay_summary['calibrated_pipeline']:,}"
                )
                print(f"✅ Current pipeline: {decay_summary['current_pipeline']:,}")
                print(f"✅ Calibration date: {decay_summary['calibration_date']}")
            else:
                print("❌ Could not retrieve decay summary")
                return False
        else:
            print("❌ Failed to set calibration baseline")
            return False

        # Test 2: Apply revenue booking (should decrease pipeline)
        print("\n💰 Test 2: Revenue Booking (Pipeline Decay)")

        booking_amount = 8000
        success = pipeline_service.apply_revenue_booking(
            ae_id=ae_id,
            month=month,
            amount=booking_amount,
            customer="BigCorp Media",
            description="Q1 campaign booked early",
        )

        if success:
            print(f"✅ Revenue booking applied: ${booking_amount:,}")

            # Check decay effect
            decay_summary = pipeline_service.get_pipeline_decay_summary(ae_id, month)
            if decay_summary:
                expected_pipeline = initial_pipeline - booking_amount
                actual_pipeline = decay_summary["current_pipeline"]

                print(
                    f"✅ Pipeline after booking: {actual_pipeline:,} (expected: {expected_pipeline:,})"
                )
                print(f"✅ Total decay: {decay_summary['total_decay']:+,}")
                print(f"✅ Decay events: {len(decay_summary['decay_events'])}")

                if actual_pipeline == expected_pipeline:
                    print("✅ Decay calculation correct")
                else:
                    print(
                        f"❌ Decay calculation incorrect: {actual_pipeline} != {expected_pipeline}"
                    )
                    return False
            else:
                print("❌ Could not retrieve decay summary after booking")
                return False
        else:
            print("❌ Failed to apply revenue booking")
            return False

        # Test 3: Apply revenue removal (should increase pipeline)
        print("\n🔄 Test 3: Revenue Removal (Pipeline Increase)")

        removal_amount = 3000
        success = pipeline_service.apply_revenue_removal(
            ae_id=ae_id,
            month=month,
            amount=removal_amount,
            customer="TechStart Inc",
            reason="Campaign cancelled",
        )

        if success:
            print(f"✅ Revenue removal applied: ${removal_amount:,}")

            # Check decay effect
            decay_summary = pipeline_service.get_pipeline_decay_summary(ae_id, month)
            if decay_summary:
                expected_pipeline = (
                    initial_pipeline - booking_amount + removal_amount
                )  # 50000 - 8000 + 3000 = 45000
                actual_pipeline = decay_summary["current_pipeline"]

                print(
                    f"✅ Pipeline after removal: {actual_pipeline:,} (expected: {expected_pipeline:,})"
                )
                print(f"✅ Total decay: {decay_summary['total_decay']:+,}")
                print(f"✅ Decay events: {len(decay_summary['decay_events'])}")

                if actual_pipeline == expected_pipeline:
                    print("✅ Decay calculation correct")
                else:
                    print(
                        f"❌ Decay calculation incorrect: {actual_pipeline} != {expected_pipeline}"
                    )
                    return False
            else:
                print("❌ Could not retrieve decay summary after removal")
                return False
        else:
            print("❌ Failed to apply revenue removal")
            return False

        # Test 4: Multiple rapid changes
        print("\n⚡ Test 4: Multiple Rapid Changes")

        changes = [
            ("booking", 2000, "Customer A"),
            ("booking", 1500, "Customer B"),
            ("removal", 1000, "Customer C cancelled"),
            ("booking", 3000, "Customer D"),
        ]

        for change_type, amount, description in changes:
            if change_type == "booking":
                success = pipeline_service.apply_revenue_booking(
                    ae_id=ae_id, month=month, amount=amount, description=description
                )
            else:
                success = pipeline_service.apply_revenue_removal(
                    ae_id=ae_id, month=month, amount=amount, reason=description
                )

            if not success:
                print(f"❌ Failed to apply {change_type}: {amount}")
                return False

        # Verify final state
        decay_summary = pipeline_service.get_pipeline_decay_summary(ae_id, month)
        if decay_summary:
            # Calculate expected: 50000 - 8000 + 3000 - 2000 - 1500 + 1000 - 3000 = 39500
            expected_final = 39500
            actual_final = decay_summary["current_pipeline"]

            print(f"✅ Final pipeline: {actual_final:,} (expected: {expected_final:,})")
            print(f"✅ Total decay events: {len(decay_summary['decay_events'])}")

            if actual_final == expected_final:
                print("✅ Multiple changes processed correctly")
            else:
                print(
                    f"❌ Multiple changes calculation incorrect: {actual_final} != {expected_final}"
                )
                return False

        # Test 5: Decay timeline and analytics
        print("\n📈 Test 5: Decay Timeline and Analytics")

        # Get decay timeline
        timeline = pipeline_service.get_decay_timeline(ae_id, month)
        print(f"✅ Decay timeline retrieved: {len(timeline)} events")

        # Get decay analytics
        analytics = pipeline_service.get_decay_analytics(ae_id, [month])
        if analytics and "monthly_summaries" in analytics:
            monthly_summary = analytics["monthly_summaries"].get(month)
            if monthly_summary:
                print(f"✅ Analytics generated:")
                print(
                    f"   Decay rate per day: {monthly_summary['decay_rate_per_day']:.2f}"
                )
                print(
                    f"   Decay percentage: {monthly_summary['decay_percentage']:.1f}%"
                )
                print(
                    f"   Days since calibration: {monthly_summary['days_since_calibration']}"
                )
            else:
                print("❌ Monthly summary not found in analytics")
                return False
        else:
            print("❌ Failed to get decay analytics")
            return False

        # Test 6: New calibration (should reset baseline)
        print("\n🔄 Test 6: New Calibration (Reset Baseline)")

        new_calibration = 40000
        success = pipeline_service.set_pipeline_calibration(
            ae_id=ae_id,
            month=month,
            pipeline_value=new_calibration,
            calibrated_by="test_user",
            session_id="TEST_SESSION_002",
        )

        if success:
            print(f"✅ New calibration set: {new_calibration:,}")

            # Verify reset
            decay_summary = pipeline_service.get_pipeline_decay_summary(ae_id, month)
            if decay_summary:
                if (
                    decay_summary["calibrated_pipeline"] == new_calibration
                    and decay_summary["current_pipeline"] == new_calibration
                ):
                    print("✅ Calibration baseline reset correctly")
                    print(f"✅ New baseline: {new_calibration:,}")
                else:
                    print(f"❌ Calibration reset failed")
                    print(f"   Calibrated: {decay_summary['calibrated_pipeline']}")
                    print(f"   Current: {decay_summary['current_pipeline']}")
                    return False
            else:
                print("❌ Could not retrieve decay summary after new calibration")
                return False
        else:
            print("❌ Failed to set new calibration")
            return False

        # Test 7: Enhanced monthly summary
        print("\n📊 Test 7: Enhanced Monthly Summary")

        monthly_summaries = pipeline_service.get_monthly_summary_with_decay(ae_id)
        if monthly_summaries:
            print(f"✅ Monthly summaries retrieved: {len(monthly_summaries)} months")

            # Find our test month
            test_month_summary = None
            for summary in monthly_summaries:
                if summary["month"] == month:
                    test_month_summary = summary
                    break

            if test_month_summary:
                print(f"✅ Test month summary found:")
                print(
                    f"   Current pipeline: {test_month_summary['current_pipeline']:,}"
                )
                print(
                    f"   Calibrated pipeline: {test_month_summary.get('calibrated_pipeline', 'N/A')}"
                )
                print(
                    f"   Decay events: {test_month_summary.get('decay_events_count', 0)}"
                )
                print(
                    f"   Has decay activity: {test_month_summary.get('has_decay_activity', False)}"
                )
            else:
                print("❌ Test month not found in monthly summaries")
                return False
        else:
            print("❌ Failed to get monthly summaries")
            return False

        # Test 8: File integrity check
        print("\n🔍 Test 8: File Integrity Check")

        # Check pipeline data file
        pipeline_file = os.path.join(data_path, "pipeline_data.json")
        if os.path.exists(pipeline_file):
            try:
                with open(pipeline_file, "r") as f:
                    pipeline_data = json.load(f)
                print("✅ Pipeline data file integrity: OK")
                print(f"   AEs tracked: {len(pipeline_data.get('pipeline_data', {}))}")
                print(
                    f"   Audit log entries: {len(pipeline_data.get('audit_log', []))}"
                )
            except json.JSONDecodeError:
                print("❌ Pipeline data file corrupted")
                return False
        else:
            print("❌ Pipeline data file not found")
            return False

        # Check decay data file
        decay_file = os.path.join(data_path, "pipeline_decay.json")
        if os.path.exists(decay_file):
            try:
                with open(decay_file, "r") as f:
                    decay_data = json.load(f)
                print("✅ Decay data file integrity: OK")
                print(f"   AEs tracked: {len(decay_data.get('decay_tracking', {}))}")

                # Count total events
                total_events = sum(
                    len(month_data.get("decay_events", []))
                    for ae_data in decay_data.get("decay_tracking", {}).values()
                    for month_data in ae_data.values()
                )
                print(f"   Total decay events: {total_events}")
            except json.JSONDecodeError:
                print("❌ Decay data file corrupted")
                return False
        else:
            print("❌ Decay data file not found")
            return False

        # Cleanup
        import shutil

        shutil.rmtree(temp_dir, ignore_errors=True)

        print("\n" + "=" * 50)
        print("🎉 ALL PIPELINE DECAY TESTS PASSED!")
        print("✅ Real-time pipeline adjustment working")
        print("✅ Calibration baseline management working")
        print("✅ Decay tracking and analytics working")
        print("✅ File integrity maintained")
        print("✅ System ready for production deployment")

        return True

    except Exception as e:
        print(f"\n❌ Pipeline decay test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_decay_system_integration():
    """Test integration with existing pipeline system."""
    print("\n🔗 DECAY SYSTEM INTEGRATION TEST")
    print("=" * 50)

    try:
        # Set up test environment
        temp_dir = tempfile.mkdtemp(prefix="decay_integration_test_")
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
        pipeline_service = container.get("pipeline_service")

        # Test that decay system is properly integrated
        print("✅ Testing decay system integration...")

        # Check for decay engine
        has_decay_engine = (
            hasattr(pipeline_service, "decay_engine")
            and pipeline_service.decay_engine is not None
        )
        print(f"✅ Decay engine available: {has_decay_engine}")

        # Check for decay methods
        decay_methods = [
            "apply_revenue_booking",
            "apply_revenue_removal",
            "set_pipeline_calibration",
            "get_pipeline_decay_summary",
            "get_decay_analytics",
            "get_decay_timeline",
        ]

        available_methods = []
        for method in decay_methods:
            if hasattr(pipeline_service, method):
                available_methods.append(method)

        print(
            f"✅ Decay methods available: {len(available_methods)}/{len(decay_methods)}"
        )

        for method in available_methods:
            print(f"   ✅ {method}")

        missing_methods = set(decay_methods) - set(available_methods)
        if missing_methods:
            print("❌ Missing decay methods:")
            for method in missing_methods:
                print(f"   ❌ {method}")
            return False

        # Test enhanced data source info
        data_source_info = pipeline_service.get_data_source_info()
        print(
            f"✅ Data source info includes decay system: {'decay_system_enabled' in data_source_info}"
        )

        if "decay_system" in data_source_info:
            decay_info = data_source_info["decay_system"]
            print(f"   Decay system status: {decay_info.get('status', 'unknown')}")
            print(f"   Decay file exists: {decay_info.get('decay_file_exists', False)}")

        # Cleanup
        import shutil

        shutil.rmtree(temp_dir, ignore_errors=True)

        print("✅ Decay system integration test passed!")
        return True

    except Exception as e:
        print(f"❌ Integration test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def main():
    """Run all pipeline decay tests."""
    print("🧪 PIPELINE DECAY SYSTEM VALIDATION")
    print("=" * 60)

    tests = [
        ("Core Decay System", test_pipeline_decay_system),
        ("Integration Test", test_decay_system_integration),
    ]

    passed = 0
    failed = 0

    for test_name, test_func in tests:
        print(f"\n🧪 Running: {test_name}")
        try:
            if test_func():
                passed += 1
                print(f"✅ {test_name}: PASSED")
            else:
                failed += 1
                print(f"❌ {test_name}: FAILED")
        except Exception as e:
            failed += 1
            print(f"💥 {test_name} crashed: {e}")

    print("\n" + "=" * 60)
    print("📊 PIPELINE DECAY TEST RESULTS")
    print(f"✅ Passed: {passed}")
    print(f"❌ Failed: {failed}")
    print(f"📈 Success Rate: {passed / (passed + failed) * 100:.1f}%")

    if failed == 0:
        print("\n🎉 ALL TESTS PASSED!")
        print("✅ Pipeline decay system is working perfectly")
        print("✅ Ready for production deployment")
        print("✅ Real-time pipeline adjustments functional")
        return 0
    else:
        print(f"\n⚠️  {failed} TESTS FAILED")
        print("❌ Please fix the issues above")
        return 1


if __name__ == "__main__":
    exit(main())
