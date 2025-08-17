#!/usr/bin/env python3
"""
SQLite Database Schema Extractor
Extracts complete schema and saves to timestamped SQL file.
"""

import sqlite3
import sys
import os
from datetime import datetime
from typing import Dict, List, Any

def connect_to_database(db_path: str) -> sqlite3.Connection:
    """Connect to SQLite database with error handling."""
    try:
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"Database file not found: {db_path}")
        
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row  # Enable column name access
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)

def generate_filename() -> str:
    """Generate timestamped filename in format: schema-YYMMDD-HHMMam/pm.sql"""
    now = datetime.now()
    
    # Format: YYMMDD
    date_part = now.strftime("%y%m%d")
    
    # Format: HHMMam/pm (12-hour format)
    time_part = now.strftime("%I%M%p").lower()
    
    return f"schema-{date_part}-{time_part}.sql"

def get_all_tables(conn: sqlite3.Connection) -> List[str]:
    """Get all table names from the database."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name
    """)
    return [row[0] for row in cursor.fetchall()]

def get_table_create_statement(conn: sqlite3.Connection, table_name: str) -> str:
    """Get the CREATE TABLE statement for a specific table."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT sql FROM sqlite_master 
        WHERE type='table' AND name=?
    """, (table_name,))
    
    result = cursor.fetchone()
    return result[0] if result else None

def get_table_indexes(conn: sqlite3.Connection, table_name: str) -> List[str]:
    """Get all CREATE INDEX statements for a specific table."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT sql FROM sqlite_master 
        WHERE type='index' AND tbl_name=? AND sql IS NOT NULL
        ORDER BY name
    """, (table_name,))
    
    return [row[0] for row in cursor.fetchall()]

def get_views(conn: sqlite3.Connection) -> List[str]:
    """Get all CREATE VIEW statements."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT sql FROM sqlite_master 
        WHERE type='view'
        ORDER BY name
    """)
    
    return [row[0] for row in cursor.fetchall()]

def get_triggers(conn: sqlite3.Connection) -> List[str]:
    """Get all CREATE TRIGGER statements."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT sql FROM sqlite_master 
        WHERE type='trigger'
        ORDER BY name
    """)
    
    return [row[0] for row in cursor.fetchall()]

def get_standalone_indexes(conn: sqlite3.Connection) -> List[str]:
    """Get all standalone CREATE INDEX statements."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT sql FROM sqlite_master 
        WHERE type='index' AND sql IS NOT NULL
        ORDER BY name
    """)
    
    return [row[0] for row in cursor.fetchall()]

def write_schema_file(db_path: str, output_file: str):
    """Extract schema and write to SQL file."""
    conn = connect_to_database(db_path)
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            # Write header
            f.write("-- SQLite Database Schema Export\n")
            f.write(f"-- Source Database: {db_path}\n")
            f.write(f"-- Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("-- \n")
            
            # Get SQLite version
            cursor = conn.cursor()
            cursor.execute("SELECT sqlite_version()")
            sqlite_version = cursor.fetchone()[0]
            f.write(f"-- SQLite Version: {sqlite_version}\n")
            f.write("-- \n\n")
            
            # Add diagnostic information for troubleshooting
            f.write("-- DIAGNOSTIC: Column type verification\n")
            f.write("-- Use these commands to verify column types:\n")
            f.write("-- PRAGMA table_info(spots);\n")
            f.write("-- SELECT typeof(broadcast_month) FROM spots LIMIT 5;\n")
            f.write("-- \n\n")
            
            # Enable foreign keys (good practice)
            f.write("PRAGMA foreign_keys = ON;\n\n")
            
            # Get all tables and their schemas
            tables = get_all_tables(conn)
            f.write(f"-- Tables ({len(tables)})\n")
            f.write("-- " + "="*60 + "\n\n")
            
            for table_name in tables:
                f.write(f"-- Table: {table_name}\n")
                f.write("-- " + "-"*40 + "\n")
                
                # Get and write CREATE TABLE statement
                create_statement = get_table_create_statement(conn, table_name)
                if create_statement:
                    f.write(f"{create_statement};\n\n")
                
                # Get and write table-specific indexes
                table_indexes = get_table_indexes(conn, table_name)
                if table_indexes:
                    f.write(f"-- Indexes for table: {table_name}\n")
                    for index_sql in table_indexes:
                        f.write(f"{index_sql};\n")
                    f.write("\n")
            
            # Get and write views
            views = get_views(conn)
            if views:
                f.write(f"-- Views ({len(views)})\n")
                f.write("-- " + "="*60 + "\n\n")
                for view_sql in views:
                    f.write(f"{view_sql};\n\n")
            
            # Get and write triggers
            triggers = get_triggers(conn)
            if triggers:
                f.write(f"-- Triggers ({len(triggers)})\n")
                f.write("-- " + "="*60 + "\n\n")
                for trigger_sql in triggers:
                    f.write(f"{trigger_sql};\n\n")
            
            # Write footer
            f.write("-- End of schema export\n")
            f.write(f"-- Total tables: {len(tables)}\n")
            f.write(f"-- Total views: {len(views)}\n")
            f.write(f"-- Total triggers: {len(triggers)}\n")
        
        # Print summary to console
        print(f"Schema successfully exported to: {output_file}")
        print(f"Database: {db_path}")
        print(f"Tables exported: {len(tables)}")
        if views:
            print(f"Views exported: {len(views)}")
        if triggers:
            print(f"Triggers exported: {len(triggers)}")
        print(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        print(f"Error writing schema file: {e}")
        return 1
    finally:
        conn.close()
    
    return 0

def main():
    """Main function."""
    db_path = "./data/database/production.db"
    
    # Allow custom database path as command line argument
    if len(sys.argv) > 1:
        db_path = sys.argv[1]
    
    if not os.path.exists(db_path):
        print(f"Error: Database file not found: {db_path}")
        print("Usage: python schema_extractor.py [database_path]")
        sys.exit(1)
    
    # Generate timestamped filename
    output_file = generate_filename()
    
    # Allow custom output filename as second argument
    if len(sys.argv) > 2:
        output_file = sys.argv[2]
    
    return write_schema_file(db_path, output_file)

if __name__ == "__main__":
    sys.exit(main())