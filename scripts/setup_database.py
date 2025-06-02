#!/usr/bin/env python3
"""
Database setup script - creates tables and populates reference data.
Run this once to initialize the database.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from database.connection import DatabaseConnection
from database.schema import ALL_TABLES, MARKET_MAPPINGS, LANGUAGE_MAPPINGS, SECTOR_MAPPINGS
from database.reference_data import ReferenceDataManager

def setup_database(db_path: str):
    """Create database schema and populate reference data."""
    print(f"Setting up database: {db_path}")
    
    db_conn = DatabaseConnection(db_path)
    
    with db_conn.transaction() as conn:
        # 1. Create all tables
        print("Creating tables...")
        for table_name, table_sql in ALL_TABLES:
            print(f"  Creating {table_name}...")
            conn.execute(table_sql)
        
        # 2. Populate reference data
        print("Populating reference data...")
        ref_manager = ReferenceDataManager(db_conn)
        ref_manager.populate_reference_data()
        
        print("Database setup complete!")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Setup sales database")
    parser.add_argument("--db-path", default="data/database/production.db",
                       help="Database file path")
    
    args = parser.parse_args()
    setup_database(args.db_path)