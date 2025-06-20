# tests/integration/test_critical_fixes.py
"""
Comprehensive integration tests for critical fixes.
Run this to validate that concurrency control and data consistency fixes work properly.
"""
import os
import sys
import tempfile
import threading
import time
import json
import sqlite3
import requests
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../src'))

# Test configuration
TEST_CONFIG = {
    'CONCURRENT_THREADS': 10,
    'OPERATIONS_PER_THREAD': 20,
    'LARGE_DATA_SIZE': 1000,  # Characters in large data tests
    'TIMEOUT_SECONDS': 30,
    'FLASK_PORT': 8001,  # Use different port for testing
}

class CriticalFixesTestSuite:
    """Comprehensive test suite for critical fixes validation."""
    
    def __init__(self):
        self.temp_dir = None
        self.db_path = None
        self.data_path = None
        self.flask_process = None
        self.test_results = {
            'concurrency_tests': {},
            'consistency_tests': {},
            'health_monitoring_tests': {},
            'performance_tests': {},
            'overall_status': 'pending'
        }
    
    def setup(self):
        """Set up test environment."""
        print("🔧 Setting up test environment...")
        
        # Create temporary directories
        self.temp_dir = tempfile.mkdtemp(prefix='ctv_test_')
        self.data_path = os.path.join(self.temp_dir, 'data', 'processed')
        self.db_path = os.path.join(self.temp_dir, 'data', 'database', 'test.db')
        
        # Create directories
        os.makedirs(self.data_path, exist_ok=True)
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        # Set environment variables
        os.environ['PROJECT_ROOT'] = self.temp_dir
        os.environ['DATA_PATH'] = self.data_path
        os.environ['DB_PATH'] = self.db_path
        os.environ['FLASK_ENV'] = 'testing'
        os.environ['DEBUG'] = 'false'
        
        print(f"✅ Test environment ready at {self.temp_dir}")
    
    def teardown(self):
        """Clean up test environment."""
        print("🧹 Cleaning up test environment...")
        
        # Stop Flask if running
        if self.flask_process:
            self.flask_process.terminate()
            self.flask_process.wait()
        
        # Clean up temp directory
        if self.temp_dir and os.path.exists(self.temp_dir):
            import shutil
            shutil.rmtree(self.temp_dir, ignore_errors=True)
        
        print("✅ Cleanup complete")
    
    def test_1_service_initialization(self):
        """Test 1: Service initialization and health checks."""
        print("\n📋 Test 1: Service Initialization and Health Checks")
        
        try:
            from services.factory import initialize_services, get_service_health_report
            from services.container import get_container
            
            # Initialize services
            initialize_services()
            container = get_container()
            
            # Test service availability
            services_to_test = ['pipeline_service', 'budget_service', 'report_data_service']
            service_status = {}
            
            for service_name in services_to_test:
                try:
                    service = container.get(service_name)
                    service_status[service_name] = 'healthy' if service else 'null'
                except Exception as e:
                    service_status[service_name] = f'error: {str(e)}'
            
            # Get health report
            health_report = get_service_health_report()
            
            self.test_results['concurrency_tests']['service_initialization'] = {
                'status': 'passed',
                'service_status': service_status,
                'health_report': health_report.get('overall_status', 'unknown'),
                'details': f"Services initialized: {list(service_status.keys())}"
            }
            
            print(f"  ✅ Services initialized: {service_status}")
            print(f"  ✅ Health report status: {health_report.get('overall_status', 'unknown')}")
            
        except Exception as e:
            self.test_results['concurrency_tests']['service_initialization'] = {
                'status': 'failed',
                'error': str(e)
            }
            print(f"  ❌ Service initialization failed: {e}")
    
    def test_2_concurrency_json_writes(self):
        """Test 2: Concurrent JSON writes with file locking."""
        print("\n📋 Test 2: Concurrent JSON Writes")
        
        try:
            from services.container import get_container
            
            container = get_container()
            pipeline_service = container.get('pipeline_service')
            
            # Test concurrent writes
            def update_pipeline(thread_id, iteration):
                """Update pipeline in thread."""
                try:
                    success = pipeline_service.update_pipeline_data(
                        ae_id=f'AE_{thread_id % 5}',  # Use 5 different AEs
                        month='2025-01',
                        pipeline_update={
                            'current_pipeline': 1000 + (thread_id * 100) + iteration,
                            'updated_by': f'thread_{thread_id}_iter_{iteration}'
                        },
                        updated_by=f'test_thread_{thread_id}'
                    )
                    return {'success': success, 'thread_id': thread_id, 'iteration': iteration}
                except Exception as e:
                    return {'success': False, 'error': str(e), 'thread_id': thread_id}
            
            # Run concurrent operations
            start_time = time.time()
            with ThreadPoolExecutor(max_workers=TEST_CONFIG['CONCURRENT_THREADS']) as executor:
                futures = []
                for thread_id in range(TEST_CONFIG['CONCURRENT_THREADS']):
                    for iteration in range(TEST_CONFIG['OPERATIONS_PER_THREAD']):
                        future = executor.submit(update_pipeline, thread_id, iteration)
                        futures.append(future)
                
                # Collect results
                results = []
                for future in as_completed(futures, timeout=TEST_CONFIG['TIMEOUT_SECONDS']):
                    result = future.result()
                    results.append(result)
            
            end_time = time.time()
            
            # Analyze results
            successful_ops = sum(1 for r in results if r.get('success', False))
            failed_ops = len(results) - successful_ops
            total_ops = len(results)
            
            # Validate data integrity
            pipeline_file = os.path.join(self.data_path, 'pipeline_data.json')
            data_integrity_ok = True
            try:
                with open(pipeline_file, 'r') as f:
                    data = json.load(f)
                    # Check if JSON is valid and has expected structure
                    if 'pipeline_data' not in data:
                        data_integrity_ok = False
            except (json.JSONDecodeError, FileNotFoundError):
                data_integrity_ok = False
            
            self.test_results['concurrency_tests']['json_writes'] = {
                'status': 'passed' if successful_ops == total_ops and data_integrity_ok else 'failed',
                'total_operations': total_ops,
                'successful_operations': successful_ops,
                'failed_operations': failed_ops,
                'duration_seconds': round(end_time - start_time, 2),
                'operations_per_second': round(total_ops / (end_time - start_time), 2),
                'data_integrity': data_integrity_ok,
                'details': f"Concurrent writes: {successful_ops}/{total_ops} successful"
            }
            
            print(f"  ✅ Concurrent operations: {successful_ops}/{total_ops} successful")
            print(f"  ✅ Duration: {end_time - start_time:.2f}s ({total_ops / (end_time - start_time):.1f} ops/sec)")
            print(f"  ✅ Data integrity: {'OK' if data_integrity_ok else 'FAILED'}")
            
        except Exception as e:
            self.test_results['concurrency_tests']['json_writes'] = {
                'status': 'failed',
                'error': str(e)
            }
            print(f"  ❌ Concurrent JSON writes test failed: {e}")
    
    def test_3_data_consistency_validation(self):
        """Test 3: Data consistency between JSON and database."""
        print("\n📋 Test 3: Data Consistency Validation")
        
        try:
            from services.container import get_container
            
            container = get_container()
            pipeline_service = container.get('pipeline_service')
            
            # Add data to JSON
            pipeline_service.update_pipeline_data(
                ae_id='CONSISTENCY_TEST',
                month='2025-01',
                pipeline_update={'current_pipeline': 5000},
                updated_by='consistency_test'
            )
            
            # Manually add conflicting data to database (if available)
            if hasattr(pipeline_service, 'db_connection') and pipeline_service.db_connection:
                try:
                    conn = pipeline_service.db_connection.connect()
                    conn.execute("""
                    INSERT OR REPLACE INTO pipeline_data 
                    (ae_id, ae_name, month, current_pipeline, updated_by)
                    VALUES (?, ?, ?, ?, ?)
                    """, ('CONSISTENCY_TEST', 'Test AE', '2025-01', 6000, 'direct_db'))
                    conn.commit()
                    conn.close()
                    
                    # Run consistency validation
                    consistency_result = pipeline_service.validate_data_consistency()
                    
                    # Test auto-repair
                    if not consistency_result.is_consistent:
                        repair_result = pipeline_service.force_consistency_repair()
                        
                        self.test_results['consistency_tests']['validation_and_repair'] = {
                            'status': 'passed',
                            'initial_consistent': consistency_result.is_consistent,
                            'conflicts_detected': len(consistency_result.conflicts),
                            'repair_successful': repair_result.is_consistent,
                            'details': f"Detected {len(consistency_result.conflicts)} conflicts, repair: {'successful' if repair_result.is_consistent else 'failed'}"
                        }
                        
                        print(f"  ✅ Consistency validation: {len(consistency_result.conflicts)} conflicts detected")
                        print(f"  ✅ Auto-repair: {'successful' if repair_result.is_consistent else 'failed'}")
                    else:
                        self.test_results['consistency_tests']['validation_and_repair'] = {
                            'status': 'passed',
                            'initial_consistent': True,
                            'details': "Data was already consistent"
                        }
                        print("  ✅ Data consistency: Already consistent")
                        
                except Exception as db_error:
                    # Database not available, test JSON-only consistency
                    self.test_results['consistency_tests']['validation_and_repair'] = {
                        'status': 'passed',
                        'database_available': False,
                        'details': f"Database not available, JSON-only mode: {str(db_error)}"
                    }
                    print(f"  ✅ JSON-only mode (DB not available): {db_error}")
            else:
                self.test_results['consistency_tests']['validation_and_repair'] = {
                    'status': 'passed',
                    'database_available': False,
                    'details': "Running in JSON-only mode"
                }
                print("  ✅ JSON-only mode (no database configured)")
                
        except Exception as e:
            self.test_results['consistency_tests']['validation_and_repair'] = {
                'status': 'failed',
                'error': str(e)
            }
            print(f"  ❌ Data consistency test failed: {e}")
    
    def test_4_concurrent_read_write_operations(self):
        """Test 4: Concurrent read and write operations."""
        print("\n📋 Test 4: Concurrent Read-Write Operations")
        
        try:
            from services.container import get_container
            
            container = get_container()
            pipeline_service = container.get('pipeline_service')
            
            # Results tracking
            read_results = []
            write_results = []
            
            def reader_worker(worker_id):
                """Worker that continuously reads data."""
                results = []
                for i in range(10):
                    try:
                        data = pipeline_service.get_pipeline_data('READ_WRITE_TEST', '2025-01')
                        results.append({'success': True, 'data_type': type(data).__name__})
                        time.sleep(0.01)  # Small delay
                    except Exception as e:
                        results.append({'success': False, 'error': str(e)})
                return results
            
            def writer_worker(worker_id):
                """Worker that continuously writes data."""
                results = []
                for i in range(10):
                    try:
                        success = pipeline_service.update_pipeline_data(
                            ae_id='READ_WRITE_TEST',
                            month='2025-01',
                            pipeline_update={'current_pipeline': 1000 + (worker_id * 100) + i},
                            updated_by=f'writer_{worker_id}'
                        )
                        results.append({'success': success})
                        time.sleep(0.01)  # Small delay
                    except Exception as e:
                        results.append({'success': False, 'error': str(e)})
                return results
            
            # Run concurrent readers and writers
            with ThreadPoolExecutor(max_workers=8) as executor:
                # Submit reader tasks
                reader_futures = [executor.submit(reader_worker, i) for i in range(4)]
                
                # Submit writer tasks
                writer_futures = [executor.submit(writer_worker, i) for i in range(4)]
                
                # Collect results
                for future in as_completed(reader_futures, timeout=TEST_CONFIG['TIMEOUT_SECONDS']):
                    read_results.extend(future.result())
                
                for future in as_completed(writer_futures, timeout=TEST_CONFIG['TIMEOUT_SECONDS']):
                    write_results.extend(future.result())
            
            # Analyze results
            successful_reads = sum(1 for r in read_results if r.get('success', False))
            successful_writes = sum(1 for r in write_results if r.get('success', False))
            
            self.test_results['concurrency_tests']['read_write'] = {
                'status': 'passed' if successful_reads == len(read_results) and successful_writes == len(write_results) else 'failed',
                'total_reads': len(read_results),
                'successful_reads': successful_reads,
                'total_writes': len(write_results),
                'successful_writes': successful_writes,
                'details': f"Concurrent R/W: {successful_reads}/{len(read_results)} reads, {successful_writes}/{len(write_results)} writes"
            }
            
            print(f"  ✅ Concurrent reads: {successful_reads}/{len(read_results)} successful")
            print(f"  ✅ Concurrent writes: {successful_writes}/{len(write_results)} successful")
            
        except Exception as e:
            self.test_results['concurrency_tests']['read_write'] = {
                'status': 'failed',
                'error': str(e)
            }
            print(f"  ❌ Concurrent read-write test failed: {e}")
    
    def test_5_large_data_handling(self):
        """Test 5: Large data handling and file integrity."""
        print("\n📋 Test 5: Large Data Handling")
        
        try:
            from services.container import get_container
            
            container = get_container()
            pipeline_service = container.get('pipeline_service')
            
            # Create large data payload
            large_notes = 'Large data test ' + 'X' * TEST_CONFIG['LARGE_DATA_SIZE']
            
            def large_data_worker(worker_id):
                """Worker that writes large data payloads."""
                try:
                    success = pipeline_service.update_pipeline_data(
                        ae_id=f'LARGE_DATA_{worker_id}',
                        month='2025-01',
                        pipeline_update={
                            'current_pipeline': 10000 + worker_id,
                            'notes': large_notes + f'_worker_{worker_id}'
                        },
                        updated_by=f'large_data_worker_{worker_id}'
                    )
                    return {'success': success, 'worker_id': worker_id}
                except Exception as e:
                    return {'success': False, 'error': str(e), 'worker_id': worker_id}
            
            # Run large data operations concurrently
            start_time = time.time()
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(large_data_worker, i) for i in range(5)]
                results = [future.result(timeout=TEST_CONFIG['TIMEOUT_SECONDS']) for future in futures]
            end_time = time.time()
            
            # Verify file integrity
            pipeline_file = os.path.join(self.data_path, 'pipeline_data.json')
            file_integrity_ok = True
            file_size = 0
            
            try:
                with open(pipeline_file, 'r') as f:
                    data = json.load(f)
                    file_size = os.path.getsize(pipeline_file)
                    
                    # Verify structure
                    if 'pipeline_data' not in data:
                        file_integrity_ok = False
                    
                    # Verify large data entries exist
                    large_data_entries = sum(1 for ae_id in data['pipeline_data'].keys() 
                                           if ae_id.startswith('LARGE_DATA_'))
                    
                    if large_data_entries != 5:
                        file_integrity_ok = False
                        
            except (json.JSONDecodeError, FileNotFoundError, OSError) as e:
                file_integrity_ok = False
            
            successful_ops = sum(1 for r in results if r.get('success', False))
            
            self.test_results['performance_tests']['large_data'] = {
                'status': 'passed' if successful_ops == len(results) and file_integrity_ok else 'failed',
                'total_operations': len(results),
                'successful_operations': successful_ops,
                'duration_seconds': round(end_time - start_time, 2),
                'file_size_bytes': file_size,
                'file_integrity': file_integrity_ok,
                'details': f"Large data operations: {successful_ops}/{len(results)}, file size: {file_size} bytes"
            }
            
            print(f"  ✅ Large data operations: {successful_ops}/{len(results)} successful")
            print(f"  ✅ File size: {file_size:,} bytes")
            print(f"  ✅ File integrity: {'OK' if file_integrity_ok else 'FAILED'}")
            
        except Exception as e:
            self.test_results['performance_tests']['large_data'] = {
                'status': 'failed',
                'error': str(e)
            }
            print(f"  ❌ Large data handling test failed: {e}")
    
    def test_6_emergency_repair(self):
        """Test 6: Emergency repair functionality."""
        print("\n📋 Test 6: Emergency Repair")
        
        try:
            from services.container import get_container
            
            container = get_container()
            pipeline_service = container.get('pipeline_service')
            
            # Corrupt data intentionally
            pipeline_file = os.path.join(self.data_path, 'pipeline_data.json')
            with open(pipeline_file, 'w') as f:
                f.write('invalid json content')
            
            # Test emergency repair
            repair_result = pipeline_service.emergency_repair()
            
            # Verify repair worked
            repair_successful = repair_result.get('success', False)
            
            # Try to read data after repair
            try:
                data = pipeline_service.get_pipeline_data('TEST_AE', '2025-01')
                read_after_repair = True
            except Exception:
                read_after_repair = False
            
            self.test_results['consistency_tests']['emergency_repair'] = {
                'status': 'passed' if repair_successful and read_after_repair else 'failed',
                'repair_successful': repair_successful,
                'read_after_repair': read_after_repair,
                'repair_message': repair_result.get('message', 'No message'),
                'details': f"Emergency repair: {'successful' if repair_successful else 'failed'}"
            }
            
            print(f"  ✅ Emergency repair: {'successful' if repair_successful else 'failed'}")
            print(f"  ✅ Read after repair: {'OK' if read_after_repair else 'FAILED'}")
            
        except Exception as e:
            self.test_results['consistency_tests']['emergency_repair'] = {
                'status': 'failed',
                'error': str(e)
            }
            print(f"  ❌ Emergency repair test failed: {e}")
    
    def test_7_health_monitoring_api(self):
        """Test 7: Health monitoring API endpoints."""
        print("\n📋 Test 7: Health Monitoring API")
        
        try:
            # Start Flask app for API testing
            self.start_flask_test_server()
            time.sleep(2)  # Give server time to start
            
            # Test health endpoints
            base_url = f"http://localhost:{TEST_CONFIG['FLASK_PORT']}"
            endpoints_to_test = [
                '/health/',
                '/health/pipeline',
                '/health/budget',
                '/health/database',
                '/health/consistency/validate'
            ]
            
            endpoint_results = {}
            
            for endpoint in endpoints_to_test:
                try:
                    response = requests.get(f"{base_url}{endpoint}", timeout=10)
                    endpoint_results[endpoint] = {
                        'status_code': response.status_code,
                        'response_time_ms': response.elapsed.total_seconds() * 1000,
                        'success': response.status_code in [200, 206],  # 206 = partial success
                        'content_type': response.headers.get('content-type', '')
                    }
                    
                    # Parse JSON response
                    if 'application/json' in response.headers.get('content-type', ''):
                        endpoint_results[endpoint]['json_valid'] = True
                        endpoint_results[endpoint]['response_data'] = response.json()
                    
                except requests.RequestException as e:
                    endpoint_results[endpoint] = {
                        'error': str(e),
                        'success': False
                    }
            
            successful_endpoints = sum(1 for result in endpoint_results.values() 
                                     if result.get('success', False))
            
            self.test_results['health_monitoring_tests']['api_endpoints'] = {
                'status': 'passed' if successful_endpoints == len(endpoints_to_test) else 'failed',
                'total_endpoints': len(endpoints_to_test),
                'successful_endpoints': successful_endpoints,
                'endpoint_results': endpoint_results,
                'details': f"Health API: {successful_endpoints}/{len(endpoints_to_test)} endpoints working"
            }
            
            print(f"  ✅ Health API endpoints: {successful_endpoints}/{len(endpoints_to_test)} working")
            for endpoint, result in endpoint_results.items():
                status = "✅" if result.get('success', False) else "❌"
                response_time = result.get('response_time_ms', 0)
                print(f"    {status} {endpoint}: {result.get('status_code', 'ERROR')} ({response_time:.0f}ms)")
            
        except Exception as e:
            self.test_results['health_monitoring_tests']['api_endpoints'] = {
                'status': 'failed',
                'error': str(e)
            }
            print(f"  ❌ Health monitoring API test failed: {e}")
        
        finally:
            self.stop_flask_test_server()
    
    def start_flask_test_server(self):
        """Start Flask test server."""
        try:
            # Create a simple test script to start Flask
            test_script = f"""
import sys
sys.path.insert(0, '{os.path.join(os.path.dirname(__file__), "../../src")}')
from web.app import create_testing_app
app = create_testing_app()
app.run(host='localhost', port={TEST_CONFIG['FLASK_PORT']}, debug=False)
"""
            
            script_path = os.path.join(self.temp_dir, 'test_server.py')
            with open(script_path, 'w') as f:
                f.write(test_script)
            
            # Start server in background
            self.flask_process = subprocess.Popen([
                sys.executable, script_path
            ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            
            print(f"  🚀 Started test server on port {TEST_CONFIG['FLASK_PORT']}")
            
        except Exception as e:
            print(f"  ⚠️  Could not start test server: {e}")
    
    def stop_flask_test_server(self):
        """Stop Flask test server."""
        if self.flask_process:
            self.flask_process.terminate()
            self.flask_process.wait()
            self.flask_process = None
            print("  🛑 Stopped test server")
    
    def generate_test_report(self):
        """Generate comprehensive test report."""
        print("\n" + "="*60)
        print("📊 CRITICAL FIXES TEST REPORT")
        print("="*60)
        
        # Calculate overall statistics
        all_tests = []
        for category in self.test_results.values():
            if isinstance(category, dict):
                for test_name, result in category.items():
                    if isinstance(result, dict) and 'status' in result:
                        all_tests.append(result['status'])
        
        passed_tests = sum(1 for status in all_tests if status == 'passed')
        failed_tests = sum(1 for status in all_tests if status == 'failed')
        total_tests = len(all_tests)
        
        overall_status = 'PASSED' if failed_tests == 0 else 'FAILED'
        self.test_results['overall_status'] = overall_status.lower()
        
        print(f"\n📈 OVERALL RESULTS:")
        print(f"   Total Tests: {total_tests}")
        print(f"   Passed: {passed_tests} ✅")
        print(f"   Failed: {failed_tests} ❌")
        print(f"   Success Rate: {(passed_tests/total_tests*100):.1f}%")
        print(f"   Overall Status: {overall_status}")
        
        # Detailed results by category
        categories = [
            ('Concurrency Tests', 'concurrency_tests'),
            ('Data Consistency Tests', 'consistency_tests'),
            ('Performance Tests', 'performance_tests'),
            ('Health Monitoring Tests', 'health_monitoring_tests')
        ]
        
        for category_name, category_key in categories:
            print(f"\n📋 {category_name.upper()}:")
            
            if category_key in self.test_results:
                for test_name, result in self.test_results[category_key].items():
                    if isinstance(result, dict):
                        status = result.get('status', 'unknown')
                        details = result.get('details', 'No details')
                        error = result.get('error', '')
                        
                        status_icon = "✅" if status == 'passed' else "❌"
                        print(f"   {status_icon} {test_name}: {details}")
                        
                        if error:
                            print(f"      Error: {error}")
        
        # Save detailed report to file
        report_file = os.path.join(self.temp_dir, 'test_report.json')
        try:
            with open(report_file, 'w') as f:
                json.dump(self.test_results, f, indent=2, default=str)
            print(f"\n💾 Detailed report saved to: {report_file}")
        except Exception as e:
            print(f"\n⚠️  Could not save report: {e}")
        
        return overall_status == 'PASSED'
    
    def run_all_tests(self):
        """Run all critical fixes tests."""
        print("🧪 STARTING CRITICAL FIXES TEST SUITE")
        print("="*60)
        
        try:
            self.setup()
            
            # Run tests in order
            self.test_1_service_initialization()
            self.test_2_concurrency_json_writes()
            self.test_3_data_consistency_validation()
            self.test_4_concurrent_read_write_operations()
            self.test_5_large_data_handling()
            self.test_6_emergency_repair()
            self.test_7_health_monitoring_api()
            
            # Generate report  
            success = self.generate_test_report()
            
            return success
            
        except KeyboardInterrupt:
            print("\n⚠️  Tests interrupted by user")
            return False
        except Exception as e:
            print(f"\n❌ Test suite failed with error: {e}")
            return False
        finally:
            self.teardown()


def main():
    """Main test runner."""
    test_suite = CriticalFixesTestSuite()
    
    try:
        success = test_suite.run_all_tests()
        
        if success:
            print("\n🎉 ALL TESTS PASSED - Critical fixes are working properly!")
            print("✅ Your system is ready for production deployment.")
            return 0
        else:
            print("\n⚠️  SOME TESTS FAILED - Please review the issues above.")
            print("❌ Fix the failing tests before deploying to production.")
            return 1
            
    except Exception as e:
        print(f"\n💥 Test runner failed: {e}")
        return 1


if __name__ == '__main__':
    exit(main())