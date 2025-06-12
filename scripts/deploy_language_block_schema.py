#!/usr/bin/env python3
"""
Language Block Schema Deployment Script
Safely deploys the complete schema with validation and rollback capabilities.
"""

import sys
import sqlite3
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from database.connection import DatabaseConnection

class LanguageBlockSchemaDeployer:
    """Deploys and validates Language Block schema safely."""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection
        self.schema_file = Path(__file__).parent.parent / "src/database/schema-language-blocks.sql"
        self.backup_file = None
        
    def deploy_schema(self, create_backup: bool = True, validate: bool = True) -> Dict[str, Any]:
        """Deploy the complete Language Block schema."""
        start_time = datetime.now()
        result = {
            'success': False,
            'tables_created': 0,
            'indexes_created': 0,
            'triggers_created': 0,
            'views_created': 0,
            'initial_records': 0,
            'errors': [],
            'backup_file': None
        }
        
        logging.info("Starting Language Block schema deployment...")
        
        try:
            # Step 1: Create backup if requested
            if create_backup:
                result['backup_file'] = self._create_backup()
                logging.info(f"Database backup created: {result['backup_file']}")
            
            # Step 2: Read and parse schema file
            schema_sql = self._read_schema_file()
            
            # Step 3: Deploy in transaction
            with self.db.transaction() as conn:
                # Execute the complete schema
                conn.executescript(schema_sql)
                
                # Count what was created
                result.update(self._count_created_objects(conn))
                
                logging.info("Schema deployment completed successfully")
            
            # Step 4: Validate deployment
            if validate:
                validation_result = self.validate_deployment()
                if not validation_result['success']:
                    result['errors'].extend(validation_result['errors'])
                    raise Exception("Schema validation failed")
                
                logging.info("Schema validation passed")
            
            result['success'] = True
            duration = (datetime.now() - start_time).total_seconds()
            logging.info(f"Deployment completed successfully in {duration:.2f} seconds")
            
        except Exception as e:
            error_msg = f"Schema deployment failed: {str(e)}"
            result['errors'].append(error_msg)
            logging.error(error_msg)
            
            # Restore backup if deployment failed
            if create_backup and result['backup_file']:
                self._restore_backup(result['backup_file'])
                logging.info("Database restored from backup")
        
        return result
    
    def _read_schema_file(self) -> str:
        """Read the schema SQL file."""
        if not self.schema_file.exists():
            raise FileNotFoundError(f"Schema file not found: {self.schema_file}")
        
        with open(self.schema_file, 'r') as f:
            return f.read()
    
    def _create_backup(self) -> str:
        """Create database backup before deployment."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = f"backup_before_language_blocks_{timestamp}.sql"
        
        # Create SQL dump of current database
        with self.db.connect() as conn:
            with open(backup_file, 'w') as f:
                for line in conn.iterdump():
                    f.write('%s\n' % line)
        
        return backup_file
    
    def _restore_backup(self, backup_file: str):
        """Restore database from backup file."""
        if not Path(backup_file).exists():
            logging.error(f"Backup file not found: {backup_file}")
            return
        
        try:
            with self.db.connect() as conn:
                with open(backup_file, 'r') as f:
                    conn.executescript(f.read())
            logging.info(f"Database restored from {backup_file}")
        except Exception as e:
            logging.error(f"Failed to restore backup: {e}")
    
    def _count_created_objects(self, conn: sqlite3.Connection) -> Dict[str, int]:
        """Count database objects created by schema."""
        results = {}
        
        # Count tables
        cursor = conn.execute("""
            SELECT COUNT(*) FROM sqlite_master 
            WHERE type='table' AND name IN (
                'programming_schedules', 'schedule_market_assignments', 
                'schedule_collision_log', 'language_blocks', 'spot_language_blocks'
            )
        """)
        results['tables_created'] = cursor.fetchone()[0]
        
        # Count indexes
        cursor = conn.execute("""
            SELECT COUNT(*) FROM sqlite_master 
            WHERE type='index' AND name LIKE 'idx_%'
        """)
        results['indexes_created'] = cursor.fetchone()[0]
        
        # Count triggers
        cursor = conn.execute("""
            SELECT COUNT(*) FROM sqlite_master 
            WHERE type='trigger' AND name LIKE '%collision%'
        """)
        results['triggers_created'] = cursor.fetchone()[0]
        
        # Count views
        cursor = conn.execute("""
            SELECT COUNT(*) FROM sqlite_master 
            WHERE type='view' AND name LIKE '%language_block%'
        """)
        results['views_created'] = cursor.fetchone()[0]
        
        # Count initial records
        cursor = conn.execute("SELECT COUNT(*) FROM programming_schedules")
        results['initial_records'] = cursor.fetchone()[0]
        
        return results
    
    def validate_deployment(self) -> Dict[str, Any]:
        """Comprehensive validation of deployed schema."""
        result = {
            'success': True,
            'validations': {},
            'errors': []
        }
        
        try:
            with self.db.connect() as conn:
                # Test 1: Verify all tables exist
                result['validations']['tables'] = self._validate_tables(conn)
                
                # Test 2: Verify foreign key constraints
                result['validations']['constraints'] = self._validate_constraints(conn)
                
                # Test 3: Test collision detection trigger
                result['validations']['triggers'] = self._validate_triggers(conn)
                
                # Test 4: Verify indexes are working
                result['validations']['indexes'] = self._validate_indexes(conn)
                
                # Test 5: Test reporting views
                result['validations']['views'] = self._validate_views(conn)
                
                # Test 6: Verify initial data
                result['validations']['initial_data'] = self._validate_initial_data(conn)
                
                # Check for any failures
                for validation_name, validation_result in result['validations'].items():
                    if not validation_result['success']:
                        result['success'] = False
                        result['errors'].extend(validation_result['errors'])
        
        except Exception as e:
            result['success'] = False
            result['errors'].append(f"Validation failed: {str(e)}")
        
        return result
    
    def _validate_tables(self, conn: sqlite3.Connection) -> Dict[str, Any]:
        """Validate all required tables exist with proper structure."""
        required_tables = [
            'programming_schedules', 'schedule_market_assignments', 
            'schedule_collision_log', 'language_blocks', 'spot_language_blocks'
        ]
        
        result = {'success': True, 'errors': []}
        
        for table in required_tables:
            cursor = conn.execute(f"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='{table}'")
            if cursor.fetchone()[0] == 0:
                result['success'] = False
                result['errors'].append(f"Table {table} not found")
        
        return result
    
    def _validate_constraints(self, conn: sqlite3.Connection) -> Dict[str, Any]:
        """Validate foreign key constraints are working."""
        result = {'success': True, 'errors': []}
        
        try:
            # Enable foreign key checking
            conn.execute("PRAGMA foreign_keys = ON")
            
            # Test constraint by trying to insert invalid foreign key
            try:
                conn.execute("""
                    INSERT INTO schedule_market_assignments 
                    (schedule_id, market_id, effective_start_date) 
                    VALUES (99999, 99999, '2025-01-01')
                """)
                # If this succeeds, foreign keys aren't working
                result['success'] = False
                result['errors'].append("Foreign key constraints not enforced")
            except sqlite3.IntegrityError:
                # This is expected - foreign key constraint working
                pass
            
        except Exception as e:
            result['success'] = False
            result['errors'].append(f"Constraint validation error: {str(e)}")
        
        return result
    
    def _validate_triggers(self, conn: sqlite3.Connection) -> Dict[str, Any]:
        """Test collision detection triggers."""
        result = {'success': True, 'errors': []}
        
        try:
            # Clear any existing collision logs
            conn.execute("DELETE FROM schedule_collision_log")
            
            # Create test schedules and markets if they don't exist
            conn.execute("""
                INSERT OR IGNORE INTO programming_schedules 
                (schedule_name, schedule_version, schedule_type, effective_start_date) 
                VALUES ('Test Grid 1', '1.0', 'test', '2025-01-01')
            """)
            conn.execute("""
                INSERT OR IGNORE INTO programming_schedules 
                (schedule_name, schedule_version, schedule_type, effective_start_date) 
                VALUES ('Test Grid 2', '1.0', 'test', '2025-01-01')
            """)
            
            # Get schedule IDs
            cursor = conn.execute("SELECT schedule_id FROM programming_schedules WHERE schedule_type = 'test' ORDER BY schedule_id LIMIT 2")
            schedule_ids = [row[0] for row in cursor.fetchall()]
            
            if len(schedule_ids) < 2:
                result['errors'].append("Could not create test schedules for trigger testing")
                result['success'] = False
                return result
            
            # Get a market ID (assuming markets table exists)
            cursor = conn.execute("SELECT market_id FROM markets LIMIT 1")
            market_row = cursor.fetchone()
            if not market_row:
                result['errors'].append("No markets available for trigger testing")
                result['success'] = False
                return result
            
            market_id = market_row[0]
            
            # Insert first assignment
            conn.execute("""
                INSERT INTO schedule_market_assignments 
                (schedule_id, market_id, effective_start_date, effective_end_date) 
                VALUES (?, ?, '2025-01-01', '2025-12-31')
            """, (schedule_ids[0], market_id))
            
            # Insert overlapping assignment (should trigger collision detection)
            conn.execute("""
                INSERT INTO schedule_market_assignments 
                (schedule_id, market_id, effective_start_date, effective_end_date) 
                VALUES (?, ?, '2025-06-01', '2025-12-31')
            """, (schedule_ids[1], market_id))
            
            # Check if collision was logged
            cursor = conn.execute("SELECT COUNT(*) FROM schedule_collision_log WHERE collision_type = 'market_overlap'")
            collision_count = cursor.fetchone()[0]
            
            if collision_count == 0:
                result['success'] = False
                result['errors'].append("Collision detection trigger not working")
            
            # Clean up test data
            conn.execute("DELETE FROM schedule_market_assignments WHERE schedule_id IN (?, ?)", schedule_ids)
            conn.execute("DELETE FROM programming_schedules WHERE schedule_type = 'test'")
            conn.execute("DELETE FROM schedule_collision_log")
            
        except Exception as e:
            result['success'] = False
            result['errors'].append(f"Trigger validation error: {str(e)}")
        
        return result
    
    def _validate_indexes(self, conn: sqlite3.Connection) -> Dict[str, Any]:
        """Verify indexes are created and working.""" 
        result = {'success': True, 'errors': []}
        
        try:
            # Check that indexes exist
            cursor = conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'")
            index_count = cursor.fetchone()[0]
            
            if index_count < 10:  # Expecting at least 10 indexes
                result['success'] = False
                result['errors'].append(f"Expected at least 10 indexes, found {index_count}")
            
            # Test index usage with EXPLAIN QUERY PLAN
            cursor = conn.execute("""
                EXPLAIN QUERY PLAN 
                SELECT * FROM language_blocks 
                WHERE schedule_id = 1 AND day_of_week = 'monday' AND is_active = 1
            """)
            
            plan = ' '.join([row[3] for row in cursor.fetchall()])
            if 'USING INDEX' not in plan:
                result['errors'].append("Language blocks query not using index")
        
        except Exception as e:
            result['success'] = False
            result['errors'].append(f"Index validation error: {str(e)}")
        
        return result
    
    def _validate_views(self, conn: sqlite3.Connection) -> Dict[str, Any]:
        """Test reporting views return data."""
        result = {'success': True, 'errors': []}
        
        try:
            # Test enhanced view
            cursor = conn.execute("SELECT COUNT(*) FROM spots_with_language_blocks_enhanced")
            # Should run without error
            
            # Test revenue analysis view
            cursor = conn.execute("SELECT COUNT(*) FROM language_block_revenue_analysis")
            # Should run without error
            
            # Test collision monitor view
            cursor = conn.execute("SELECT COUNT(*) FROM schedule_collision_monitor")
            # Should run without error
            
        except Exception as e:
            result['success'] = False
            result['errors'].append(f"View validation error: {str(e)}")
        
        return result
    
    def _validate_initial_data(self, conn: sqlite3.Connection) -> Dict[str, Any]:
        """Verify initial data was inserted correctly."""
        result = {'success': True, 'errors': []}
        
        try:
            # Check programming schedules
            cursor = conn.execute("SELECT COUNT(*) FROM programming_schedules WHERE schedule_name IN ('Standard Grid', 'Dallas Grid')")
            if cursor.fetchone()[0] != 2:
                result['success'] = False
                result['errors'].append("Standard Grid and Dallas Grid not created")
            
            # Check market assignments
            cursor = conn.execute("SELECT COUNT(*) FROM schedule_market_assignments")
            assignment_count = cursor.fetchone()[0]
            if assignment_count == 0:
                result['success'] = False
                result['errors'].append("No market assignments created")
        
        except Exception as e:
            result['success'] = False
            result['errors'].append(f"Initial data validation error: {str(e)}")
        
        return result

def main():
    """CLI interface for schema deployment."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Deploy Language Block Schema")
    parser.add_argument("--db-path", default="data/database/production.db", help="Database path")
    parser.add_argument("--no-backup", action="store_true", help="Skip backup creation")
    parser.add_argument("--no-validation", action="store_true", help="Skip validation")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    # Setup logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format='%(levelname)s: %(message)s')
    
    # Deploy schema
    db_connection = DatabaseConnection(args.db_path)
    deployer = LanguageBlockSchemaDeployer(db_connection)
    
    try:
        result = deployer.deploy_schema(
            create_backup=not args.no_backup,
            validate=not args.no_validation
        )
        
        # Display results
        print(f"\n{'='*60}")
        print(f"LANGUAGE BLOCK SCHEMA DEPLOYMENT")
        print(f"{'='*60}")
        
        if result['success']:
            print(f"✅ Deployment successful!")
            print(f"   Tables created: {result['tables_created']}")
            print(f"   Indexes created: {result['indexes_created']}")
            print(f"   Triggers created: {result['triggers_created']}")
            print(f"   Views created: {result['views_created']}")
            print(f"   Initial records: {result['initial_records']}")
            
            if result['backup_file']:
                print(f"   Backup created: {result['backup_file']}")
        else:
            print(f"❌ Deployment failed!")
            for error in result['errors']:
                print(f"   • {error}")
            sys.exit(1)
        
    finally:
        db_connection.close()

if __name__ == "__main__":
    main()