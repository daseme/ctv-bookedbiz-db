# tests/test_pipeline_concurrency.py
"""
Tests for pipeline service concurrency control and data consistency fixes.
"""
import unittest
import tempfile
import os
import json
import threading
import time
import sqlite3
from unittest.mock import Mock, patch
from concurrent.futures import ThreadPoolExecutor, as_completed

# Assuming the enhanced PipelineService is in the artifacts above
from src.services.pipeline_service import PipelineService, DataSourceType, ConsistencyCheckResult


class MockDatabaseConnection:
    """Mock database connection for testing."""
    
    def __init__(self, db_path=None):
        self.db_path = db_path or ':memory:'
        self._setup_schema()
    
    def _setup_schema(self):
        """Setup test database schema."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_data (
            pipeline_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ae_id TEXT NOT NULL,
            ae_name TEXT NOT NULL,
            territory TEXT,
            month TEXT NOT NULL,
            current_pipeline DECIMAL(12, 2) DEFAULT 0,
            expected_pipeline DECIMAL(12, 2) DEFAULT 0,
            calibrated_pipeline DECIMAL(12, 2) DEFAULT 0,
            calibration_date TIMESTAMP,
            calibration_session_id TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_by TEXT,
            review_session_id TEXT,
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            data_source TEXT DEFAULT 'database',
            UNIQUE(ae_id, month)
        )
        """)
        conn.commit()
        conn.close()
    
    def connect(self):
        """Return SQLite connection."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # Enable dict-like access
        return conn


class TestPipelineConcurrency(unittest.TestCase):
    """Test pipeline service concurrency control."""
    
    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.pipeline_file = os.path.join(self.temp_dir, 'pipeline_data.json')
        self.sessions_file = os.path.join(self.temp_dir, 'review_sessions.json')
        self.db_connection = MockDatabaseConnection()
        
        # Create service instance
        self.service = PipelineService(
            db_connection=self.db_connection,
            data_path=self.temp_dir,
            data_source=DataSourceType.JSON_PRIMARY
        )
    
    def tearDown(self):
        """Clean up test environment."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_concurrent_writes_json_safety(self):
        """Test that concurrent writes to JSON are safe."""
        
        def update_pipeline(ae_id, month, value, updated_by):
            """Update pipeline in a thread."""
            return self.service.update_pipeline_data(
                ae_id=ae_id,
                month=month,
                pipeline_update={'current_pipeline': value},
                updated_by=updated_by
            )
        
        # Run concurrent updates
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            
            # Submit 50 concurrent updates
            for i in range(50):
                future = executor.submit(
                    update_pipeline,
                    ae_id=f'AE_{i % 5}',  # 5 different AEs
                    month='2025-01',
                    value=1000 + i,
                    updated_by=f'user_{i}'
                )
                futures.append(future)
            
            # Wait for all to complete
            results = []
            for future in as_completed(futures):
                try:
                    result = future.result(timeout=5)
                    results.append(result)
                except Exception as e:
                    self.fail(f"Concurrent update failed: {e}")
        
        # Verify all updates succeeded
        successful_updates = sum(1 for result in results if result)
        self.assertEqual(successful_updates, 50, "Not all concurrent updates succeeded")
        
        # Verify data integrity
        data = self.service._read_json_safely(self.pipeline_file)
        self.assertIn('pipeline_data', data)
        self.assertEqual(len(data['pipeline_data']), 5, "Expected 5 AEs in data")
        
        # Verify audit log integrity
        self.assertIn('audit_log', data)
        self.assertEqual(len(data['audit_log']), 50, "Expected 50 audit entries")
    
    def test_file_locking_prevents_corruption(self):
        """Test that file locking prevents JSON corruption."""
        
        def write_large_data(thread_id):
            """Write large amount of data to test file locking."""
            large_update = {
                'current_pipeline': 50000,
                'expected_pipeline': 60000,
                'notes': f'Large update from thread {thread_id} ' + 'x' * 1000  # Large text
            }
            
            return self.service.update_pipeline_data(
                ae_id=f'AE_LARGE_{thread_id}',
                month='2025-01',
                pipeline_update=large_update,
                updated_by=f'thread_{thread_id}'
            )
        
        # Run concurrent large writes
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(write_large_data, i) for i in range(5)]
            
            # Wait for completion
            for future in as_completed(futures):
                result = future.result(timeout=10)
                self.assertTrue(result, "Large data write should succeed")
        
        # Verify file is not corrupted
        try:
            data = self.service._read_json_safely(self.pipeline_file)
            self.assertIsInstance(data, dict)
            self.assertIn('pipeline_data', data)
        except json.JSONDecodeError:
            self.fail("JSON file was corrupted by concurrent writes")
    
    def test_atomic_operations_rollback(self):
        """Test that failed operations don't leave partial state."""
        
        # Mock a failure during write
        original_write = self.service._write_json_safely
        
        def failing_write(file_path, data):
            if 'FAIL' in str(data):
                raise IOError("Simulated write failure")
            return original_write(file_path, data)
        
        self.service._write_json_safely = failing_write
        
        # First, successful update
        success = self.service.update_pipeline_data(
            ae_id='AE_001',
            month='2025-01',
            pipeline_update={'current_pipeline': 1000},
            updated_by='test'
        )
        self.assertTrue(success)
        
        # Then, failing update
        success = self.service.update_pipeline_data(
            ae_id='AE_002',
            month='2025-01',
            pipeline_update={'current_pipeline': 2000, 'notes': 'FAIL'},
            updated_by='test'
        )
        self.assertFalse(success)
        
        # Verify original data is intact
        data = self.service.get_pipeline_data('AE_001', '2025-01')
        self.assertEqual(data.get('current_pipeline'), 1000)
        
        # Verify failed data is not present
        data = self.service.get_pipeline_data('AE_002', '2025-01')
        self.assertEqual(data, {})
    
    def test_data_consistency_validation(self):
        """Test data consistency validation between JSON and DB."""
        
        # Add data to JSON
        self.service.update_pipeline_data(
            ae_id='AE_001',
            month='2025-01',
            pipeline_update={'current_pipeline': 5000},
            updated_by='test'
        )
        
        # Add different data directly to DB
        conn = self.db_connection.connect()
        conn.execute("""
        INSERT INTO pipeline_data (ae_id, ae_name, month, current_pipeline, updated_by)
        VALUES (?, ?, ?, ?, ?)
        """, ('AE_001', 'Test AE', '2025-01', 6000, 'direct_db'))
        conn.commit()
        conn.close()
        
        # Check consistency
        result = self.service.validate_data_consistency()
        
        self.assertFalse(result.is_consistent)
        self.assertEqual(len(result.conflicts), 1)
        self.assertEqual(result.conflicts[0]['ae_id'], 'AE_001')
        self.assertEqual(result.conflicts[0]['month'], '2025-01')
        self.assertEqual(result.conflicts[0]['json_value'], 5000)
        self.assertEqual(result.conflicts[0]['db_value'], 6000)
    
    def test_auto_repair_consistency(self):
        """Test automatic consistency repair."""
        
        # Create inconsistent data
        self.service.update_pipeline_data(
            ae_id='AE_001',
            month='2025-01',
            pipeline_update={'current_pipeline': 7000},
            updated_by='test'
        )
        
        # Add conflicting data to DB
        conn = self.db_connection.connect()
        conn.execute("""
        INSERT INTO pipeline_data (ae_id, ae_name, month, current_pipeline, updated_by)
        VALUES (?, ?, ?, ?, ?)
        """, ('AE_001', 'Test AE', '2025-01', 8000, 'direct_db'))
        conn.commit()
        conn.close()
        
        # Force repair (JSON_PRIMARY should win)
        result = self.service.force_consistency_repair()
        
        self.assertTrue(result.is_consistent)
        
        # Verify JSON value was synced to DB
        conn = self.db_connection.connect()
        cursor = conn.execute("""
        SELECT current_pipeline FROM pipeline_data 
        WHERE ae_id = ? AND month = ?
        """, ('AE_001', '2025-01'))
        row = cursor.fetchone()
        conn.close()
        
        self.assertEqual(row['current_pipeline'], 7000)
    
    def test_emergency_repair_functionality(self):
        """Test emergency repair functionality."""
        
        # Corrupt JSON file
        with open(self.pipeline_file, 'w') as f:
            f.write('invalid json content')
        
        # Run emergency repair
        result = self.service.emergency_repair()
        
        self.assertTrue(result['success'])
        
        # Verify file was recreated
        data = self.service._read_json_safely(self.pipeline_file)
        self.assertIn('pipeline_data', data)
        self.assertIn('schema_version', data)
    
    def test_thread_safety_read_write(self):
        """Test thread safety of concurrent reads and writes."""
        
        def reader_thread(iterations=10):
            """Read pipeline data repeatedly."""
            results = []
            for i in range(iterations):
                data = self.service.get_pipeline_data('AE_001', '2025-01')
                results.append(data)
                time.sleep(0.001)  # Small delay
            return results
        
        def writer_thread(iterations=10):
            """Write pipeline data repeatedly."""
            results = []
            for i in range(iterations):
                success = self.service.update_pipeline_data(
                    ae_id='AE_001',
                    month='2025-01',
                    pipeline_update={'current_pipeline': 1000 + i},
                    updated_by=f'writer_{i}'
                )
                results.append(success)
                time.sleep(0.001)  # Small delay
            return results
        
        # Run concurrent readers and writers
        with ThreadPoolExecutor(max_workers=6) as executor:
            # Submit reader threads
            reader_futures = [executor.submit(reader_thread) for _ in range(3)]
            
            # Submit writer threads  
            writer_futures = [executor.submit(writer_thread) for _ in range(3)]
            
            # Wait for completion
            all_futures = reader_futures + writer_futures
            
            for future in as_completed(all_futures):
                try:
                    result = future.result(timeout=10)
                    if future in writer_futures:
                        # All writes should succeed
                        self.assertTrue(all(result), "Some writes failed")
                    else:
                        # All reads should return dict (even if empty)
                        self.assertTrue(all(isinstance(r, dict) for r in result), "Some reads failed")
                except Exception as e:
                    self.fail(f"Thread execution failed: {e}")
    
    def test_data_source_switching(self):
        """Test switching between data sources."""
        
        # Start with JSON_PRIMARY
        self.assertEqual(self.service.data_source, DataSourceType.JSON_PRIMARY)
        
        # Add some data
        self.service.update_pipeline_data(
            ae_id='AE_001',
            month='2025-01',
            pipeline_update={'current_pipeline': 3000},
            updated_by='test'
        )
        
        # Create new service with DB_PRIMARY
        db_service = PipelineService(
            db_connection=self.db_connection,
            data_path=self.temp_dir,
            data_source=DataSourceType.DB_PRIMARY
        )
        
        # Data should be synced from JSON to DB
        data = db_service.get_pipeline_data('AE_001', '2025-01')
        self.assertEqual(data.get('current_pipeline'), 3000)
    
    def test_concurrent_consistency_checks(self):
        """Test concurrent consistency validation."""
        
        def run_consistency_check():
            """Run consistency check in thread."""
            return self.service.validate_data_consistency()
        
        def update_data(thread_id):
            """Update data while consistency checks run."""
            return self.service.update_pipeline_data(
                ae_id=f'AE_{thread_id}',
                month='2025-01',
                pipeline_update={'current_pipeline': 1000 * thread_id},
                updated_by=f'thread_{thread_id}'
            )
        
        # Run concurrent consistency checks and updates
        with ThreadPoolExecutor(max_workers=8) as executor:
            # Submit consistency check threads
            check_futures = [executor.submit(run_consistency_check) for _ in range(4)]
            
            # Submit update threads
            update_futures = [executor.submit(update_data, i) for i in range(4)]
            
            # Wait for completion
            for future in as_completed(check_futures + update_futures):
                try:
                    result = future.result(timeout=5)
                    if future in check_futures:
                        self.assertIsInstance(result, ConsistencyCheckResult)
                    else:
                        self.assertTrue(result, "Update should succeed")
                except Exception as e:
                    self.fail(f"Concurrent operation failed: {e}")


class TestDataSourceConfiguration(unittest.TestCase):
    """Test different data source configurations."""
    
    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_connection = MockDatabaseConnection()
    
    def tearDown(self):
        """Clean up test environment."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_json_only_source(self):
        """Test JSON-only data source."""
        service = PipelineService(
            data_path=self.temp_dir,
            data_source=DataSourceType.JSON_ONLY
        )
        
        # Should work without database
        success = service.update_pipeline_data(
            ae_id='AE_001',
            month='2025-01',
            pipeline_update={'current_pipeline': 1000},
            updated_by='test'
        )
        self.assertTrue(success)
        
        # Consistency check should pass (no DB to compare)
        result = service.validate_data_consistency()
        self.assertTrue(result.is_consistent)
    
    def test_db_only_source(self):
        """Test database-only data source."""
        service = PipelineService(
            db_connection=self.db_connection,
            data_source=DataSourceType.DB_ONLY
        )
        
        # Should work without JSON files
        success = service.update_pipeline_data(
            ae_id='AE_001',
            month='2025-01',
            pipeline_update={'current_pipeline': 2000},
            updated_by='test'
        )
        self.assertTrue(success)
        
        # Verify data in database
        data = service.get_pipeline_data('AE_001', '2025-01')
        self.assertEqual(data.get('current_pipeline'), 2000)
    
    def test_dual_source_primary_json(self):
        """Test JSON primary with database backup."""
        service = PipelineService(
            db_connection=self.db_connection,
            data_path=self.temp_dir,
            data_source=DataSourceType.JSON_PRIMARY
        )
        
        # Update should go to both sources
        success = service.update_pipeline_data(
            ae_id='AE_001',
            month='2025-01',
            pipeline_update={'current_pipeline': 3000},
            updated_by='test'
        )
        self.assertTrue(success)
        
        # Verify in both sources
        json_data = service._get_pipeline_data_from_json('AE_001', '2025-01')
        self.assertEqual(json_data.get('current_pipeline'), 3000)
        
        # Database should have synced data (after sync implementation)
        # This would need the sync methods to be fully implemented
    
    def test_get_data_source_info(self):
        """Test data source information reporting."""
        service = PipelineService(
            db_connection=self.db_connection,
            data_path=self.temp_dir,
            data_source=DataSourceType.JSON_PRIMARY
        )
        
        info = service.get_data_source_info()
        
        self.assertEqual(info['data_source'], 'json_primary')
        self.assertTrue(info['json_file_exists'])
        self.assertTrue(info['db_connection_available'])
        self.assertIn('consistency_status', info)
        self.assertIsInstance(info['consistency_status']['is_consistent'], bool)


if __name__ == '__main__':
    # Run tests
    unittest.main(verbosity=2)