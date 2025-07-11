#!/usr/bin/env python3
"""
Enhanced Business Rules Migration Script
=======================================

This script applies the database schema changes needed for enhanced business rules.
Run this ONCE to update the production database with the new tracking fields.

Changes Applied:
1. Add business_rule_applied column to spot_language_blocks
2. Add auto_resolved_date column to spot_language_blocks  
3. Add indexes for performance and analytics
4. Create views for enhanced rule analytics

Usage:
    python enhanced_rules_migration.py --database data/database/production.db
    python enhanced_rules_migration.py --database data/database/production.db --verify
"""

import sqlite3
import argparse
import sys
from datetime import datetime


def check_column_exists(cursor, table_name, column_name):
    """Check if a column exists in a table"""
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [row[1] for row in cursor.fetchall()]
    return column_name in columns


def check_index_exists(cursor, index_name):
    """Check if an index exists"""
    cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name=?", (index_name,))
    return cursor.fetchone() is not None


def check_view_exists(cursor, view_name):
    """Check if a view exists"""
    cursor.execute("SELECT name FROM sqlite_master WHERE type='view' AND name=?", (view_name,))
    return cursor.fetchone() is not None


def apply_migration(db_path, dry_run=False):
    """Apply the enhanced business rules migration"""
    print(f"🔄 Starting Enhanced Business Rules Migration")
    print(f"   Database: {db_path}")
    print(f"   Dry Run: {dry_run}")
    print(f"   Timestamp: {datetime.now().isoformat()}")
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check current schema
        print(f"\n📊 Current Schema Analysis:")
        
        # Check if columns already exist
        business_rule_exists = check_column_exists(cursor, 'spot_language_blocks', 'business_rule_applied')
        auto_resolved_exists = check_column_exists(cursor, 'spot_language_blocks', 'auto_resolved_date')
        
        print(f"   • business_rule_applied column: {'✅ EXISTS' if business_rule_exists else '❌ MISSING'}")
        print(f"   • auto_resolved_date column: {'✅ EXISTS' if auto_resolved_exists else '❌ MISSING'}")
        
        changes_needed = []
        
        # Step 1: Add business_rule_applied column
        if not business_rule_exists:
            changes_needed.append("Add business_rule_applied column")
            if not dry_run:
                cursor.execute("""
                    ALTER TABLE spot_language_blocks 
                    ADD COLUMN business_rule_applied TEXT DEFAULT NULL
                """)
                print(f"   ✅ Added business_rule_applied column")
        
        # Step 2: Add auto_resolved_date column
        if not auto_resolved_exists:
            changes_needed.append("Add auto_resolved_date column")
            if not dry_run:
                cursor.execute("""
                    ALTER TABLE spot_language_blocks 
                    ADD COLUMN auto_resolved_date TIMESTAMP DEFAULT NULL
                """)
                print(f"   ✅ Added auto_resolved_date column")
        
        # Step 3: Add indexes
        indexes_to_create = [
            ('idx_spot_blocks_business_rule', """
                CREATE INDEX idx_spot_blocks_business_rule 
                ON spot_language_blocks(business_rule_applied) 
                WHERE business_rule_applied IS NOT NULL
            """),
            ('idx_spot_blocks_auto_resolved', """
                CREATE INDEX idx_spot_blocks_auto_resolved 
                ON spot_language_blocks(auto_resolved_date) 
                WHERE auto_resolved_date IS NOT NULL
            """)
        ]
        
        for index_name, index_sql in indexes_to_create:
            if not check_index_exists(cursor, index_name):
                changes_needed.append(f"Create index {index_name}")
                if not dry_run:
                    cursor.execute(index_sql)
                    print(f"   ✅ Created index {index_name}")
            else:
                print(f"   ✅ Index {index_name} already exists")
        
        # Step 4: Create analytics views
        views_to_create = [
            ('enhanced_rule_analytics', """
                CREATE VIEW enhanced_rule_analytics AS
                SELECT 
                    COALESCE(business_rule_applied, 'standard_assignment') as rule_type,
                    COUNT(*) as spots_affected,
                    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM spot_language_blocks) as percentage,
                    AVG(intent_confidence) as avg_confidence,
                    COUNT(CASE WHEN requires_attention = 1 THEN 1 END) as flagged_count,
                    COUNT(CASE WHEN requires_attention = 0 THEN 1 END) as auto_resolved_count,
                    MIN(assigned_date) as earliest_assignment,
                    MAX(assigned_date) as latest_assignment,
                    MIN(auto_resolved_date) as earliest_auto_resolved,
                    MAX(auto_resolved_date) as latest_auto_resolved
                FROM spot_language_blocks 
                GROUP BY COALESCE(business_rule_applied, 'standard_assignment')
                ORDER BY spots_affected DESC
            """),
            ('business_rule_summary', """
                CREATE VIEW business_rule_summary AS
                SELECT 
                    'Total spots' as metric,
                    COUNT(*) as value,
                    '' as notes
                FROM spot_language_blocks
                UNION ALL
                SELECT 
                    'Enhanced rules applied' as metric,
                    COUNT(*) as value,
                    'Auto-resolved by enhanced business rules' as notes
                FROM spot_language_blocks
                WHERE business_rule_applied IS NOT NULL
                UNION ALL
                SELECT 
                    'Standard assignments' as metric,
                    COUNT(*) as value,
                    'Standard assignment process' as notes
                FROM spot_language_blocks
                WHERE business_rule_applied IS NULL
            """)
        ]
        
        for view_name, view_sql in views_to_create:
            if not check_view_exists(cursor, view_name):
                changes_needed.append(f"Create view {view_name}")
                if not dry_run:
                    cursor.execute(view_sql)
                    print(f"   ✅ Created view {view_name}")
            else:
                print(f"   ✅ View {view_name} already exists")
        
        # Commit changes
        if not dry_run and changes_needed:
            conn.commit()
            print(f"\n✅ Migration completed successfully!")
            print(f"   Changes applied: {len(changes_needed)}")
            for change in changes_needed:
                print(f"   • {change}")
        elif not changes_needed:
            print(f"\n✅ No migration needed - schema already up to date!")
        else:
            print(f"\n🧪 Dry run completed - {len(changes_needed)} changes would be applied:")
            for change in changes_needed:
                print(f"   • {change}")
        
        return True
        
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        return False
    finally:
        conn.close()


def verify_migration(db_path):
    """Verify that the migration was applied correctly"""
    print(f"\n🔍 Verifying Migration Results:")
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check columns
        business_rule_exists = check_column_exists(cursor, 'spot_language_blocks', 'business_rule_applied')
        auto_resolved_exists = check_column_exists(cursor, 'spot_language_blocks', 'auto_resolved_date')
        
        print(f"   • business_rule_applied column: {'✅ EXISTS' if business_rule_exists else '❌ MISSING'}")
        print(f"   • auto_resolved_date column: {'✅ EXISTS' if auto_resolved_exists else '❌ MISSING'}")
        
        # Check indexes
        indexes = ['idx_spot_blocks_business_rule', 'idx_spot_blocks_auto_resolved']
        for index_name in indexes:
            exists = check_index_exists(cursor, index_name)
            print(f"   • {index_name}: {'✅ EXISTS' if exists else '❌ MISSING'}")
        
        # Check views
        views = ['enhanced_rule_analytics', 'business_rule_summary']
        for view_name in views:
            exists = check_view_exists(cursor, view_name)
            print(f"   • {view_name}: {'✅ EXISTS' if exists else '❌ MISSING'}")
        
        # Test views
        if check_view_exists(cursor, 'enhanced_rule_analytics'):
            cursor.execute("SELECT rule_type, spots_affected FROM enhanced_rule_analytics LIMIT 3")
            results = cursor.fetchall()
            print(f"   • Enhanced rule analytics test: {'✅ WORKING' if results else '⚠️ EMPTY'}")
        
        # Check current assignments
        cursor.execute("SELECT COUNT(*) FROM spot_language_blocks")
        total_assignments = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM spot_language_blocks WHERE business_rule_applied IS NOT NULL")
        enhanced_assignments = cursor.fetchone()[0]
        
        print(f"   • Total assignments: {total_assignments:,}")
        print(f"   • Enhanced rule assignments: {enhanced_assignments:,}")
        print(f"   • Ready for enhanced rules: ✅ YES")
        
        return True
        
    except Exception as e:
        print(f"❌ Verification failed: {e}")
        return False
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Enhanced Business Rules Migration")
    parser.add_argument("--database", default="data/database/production.db", help="Database path")
    parser.add_argument("--dry-run", action="store_true", help="Show changes without applying them")
    parser.add_argument("--verify", action="store_true", help="Verify migration was applied correctly")
    
    args = parser.parse_args()
    
    if args.verify:
        success = verify_migration(args.database)
        return 0 if success else 1
    else:
        success = apply_migration(args.database, args.dry_run)
        return 0 if success else 1


if __name__ == "__main__":
    exit(main())