#!/usr/bin/env python3
"""
Debug script to diagnose weekly import issues.
"""

import sqlite3
import sys
from pathlib import Path

def debug_import_issues(db_path: str, excel_file: str = None):
    """Debug common import issues."""
    
    print("🔍 DEBUGGING IMPORT ISSUES")
    print("=" * 50)
    
    if not Path(db_path).exists():
        print(f"❌ Database not found: {db_path}")
        return
    
    conn = sqlite3.connect(db_path)
    
    try:
        # 1. Check recent import batches
        print("1. RECENT IMPORT BATCHES:")
        cursor = conn.execute("""
            SELECT batch_id, import_mode, status, records_imported, records_deleted, 
                   import_date, error_summary
            FROM import_batches 
            ORDER BY import_date DESC 
            LIMIT 5
        """)
        
        batches = cursor.fetchall()
        if batches:
            for batch in batches:
                status_icon = "✅" if batch[2] == "COMPLETED" else "❌" if batch[2] == "FAILED" else "⏳"
                print(f"   {status_icon} {batch[0]}: {batch[1]} - {batch[2]}")
                print(f"      Records: {batch[3]} imported, {batch[4]} deleted")
                print(f"      Date: {batch[5]}")
                if batch[6]:
                    print(f"      Error: {batch[6]}")
                print()
        else:
            print("   No import batches found")
        
        # 2. Check spots table status
        print("2. SPOTS TABLE STATUS:")
        cursor = conn.execute("SELECT COUNT(*) FROM spots")
        total_spots = cursor.fetchone()[0]
        print(f"   Total spots in database: {total_spots:,}")
        
        # Check by month
        cursor = conn.execute("""
            SELECT broadcast_month, COUNT(*) as count 
            FROM spots 
            GROUP BY broadcast_month 
            ORDER BY broadcast_month
        """)
        
        month_counts = cursor.fetchall()
        if month_counts:
            print("   Spots by month:")
            for month, count in month_counts:
                print(f"      {month}: {count:,} spots")
        else:
            print("   No spots found in database")
        
        # 3. Check recent import batch data
        print("\n3. RECENT BATCH DATA:")
        cursor = conn.execute("""
            SELECT import_batch_id, COUNT(*) as count
            FROM spots 
            WHERE import_batch_id IS NOT NULL
            GROUP BY import_batch_id 
            ORDER BY import_batch_id DESC
            LIMIT 5
        """)
        
        batch_data = cursor.fetchall()
        if batch_data:
            for batch_id, count in batch_data:
                print(f"   {batch_id}: {count:,} spots")
        else:
            print("   No spots with import_batch_id found")
        
        # 4. Check for transaction/commit issues
        print("\n4. DATABASE INTEGRITY CHECKS:")
        
        # Check for hanging transactions
        cursor = conn.execute("PRAGMA busy_timeout")
        timeout = cursor.fetchone()[0]
        print(f"   Database timeout: {timeout}ms")
        
        # Check database file size
        db_size = Path(db_path).stat().st_size / (1024 * 1024)  # MB
        print(f"   Database file size: {db_size:.2f} MB")
        
        # Check for locks
        cursor = conn.execute("PRAGMA lock_status")
        try:
            lock_status = cursor.fetchone()
            print(f"   Lock status: {lock_status}")
        except:
            print("   Lock status: Unable to determine")
        
        # 5. Check specific batch if provided
        if len(sys.argv) > 2:
            batch_id = sys.argv[2]
            print(f"\n5. CHECKING SPECIFIC BATCH: {batch_id}")
            
            cursor = conn.execute("""
                SELECT * FROM import_batches WHERE batch_id = ?
            """, (batch_id,))
            
            batch_info = cursor.fetchone()
            if batch_info:
                print(f"   Status: {batch_info[4]}")
                print(f"   Records imported: {batch_info[6]}")
                print(f"   Records deleted: {batch_info[7]}")
                print(f"   Error: {batch_info[10] or 'None'}")
                
                # Check actual spots with this batch_id
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM spots WHERE import_batch_id = ?
                """, (batch_id,))
                
                actual_spots = cursor.fetchone()[0]
                print(f"   Actual spots in database: {actual_spots}")
                
                if batch_info[6] and batch_info[6] != actual_spots:
                    print(f"   ⚠️  MISMATCH: Batch claims {batch_info[6]} but only {actual_spots} found!")
            else:
                print(f"   ❌ Batch not found: {batch_id}")
        
        # 6. Check market and foreign key constraints
        print("\n6. FOREIGN KEY CONSTRAINTS:")
        
        cursor = conn.execute("SELECT COUNT(*) FROM markets")
        market_count = cursor.fetchone()[0]
        print(f"   Markets in database: {market_count}")
        
        cursor = conn.execute("SELECT COUNT(*) FROM languages")
        language_count = cursor.fetchone()[0]
        print(f"   Languages in database: {language_count}")
        
        # Check for spots with missing foreign keys
        cursor = conn.execute("""
            SELECT COUNT(*) FROM spots s
            LEFT JOIN markets m ON s.market_id = m.market_id
            WHERE s.market_id IS NOT NULL AND m.market_id IS NULL
        """)
        orphaned_markets = cursor.fetchone()[0]
        
        cursor = conn.execute("""
            SELECT COUNT(*) FROM spots s
            LEFT JOIN languages l ON s.language_id = l.language_id
            WHERE s.language_id IS NOT NULL AND l.language_id IS NULL
        """)
        orphaned_languages = cursor.fetchone()[0]
        
        if orphaned_markets > 0:
            print(f"   ⚠️  {orphaned_markets} spots with invalid market_id")
        if orphaned_languages > 0:
            print(f"   ⚠️  {orphaned_languages} spots with invalid language_id")
        
        # 7. Test basic insert
        print("\n7. TESTING BASIC INSERT:")
        try:
            test_batch_id = f"debug_test_{int(__import__('time').time())}"
            cursor = conn.execute("""
                INSERT INTO spots (bill_code, broadcast_month, import_batch_id, created_at)
                VALUES (?, ?, ?, ?)
            """, ("TEST001", "Aug-25", test_batch_id, "2025-01-07 10:00:00"))
            
            conn.commit()
            
            # Verify it was inserted
            cursor = conn.execute("""
                SELECT COUNT(*) FROM spots WHERE import_batch_id = ?
            """, (test_batch_id,))
            
            test_count = cursor.fetchone()[0]
            if test_count == 1:
                print("   ✅ Basic insert/commit working")
                
                # Clean up test record
                conn.execute("DELETE FROM spots WHERE import_batch_id = ?", (test_batch_id,))
                conn.commit()
            else:
                print("   ❌ Basic insert failed - commit issue?")
                
        except Exception as e:
            print(f"   ❌ Basic insert failed: {e}")
        
        print(f"\n{'='*50}")
        print("RECOMMENDATIONS:")
        
        if total_spots == 0:
            print("• Database appears empty - check if imports are actually running")
        
        if batch_data and not any(count > 0 for _, count in batch_data):
            print("• Import batches exist but no spot records - transaction/commit issue")
        
        print("• Review the fixed import service code above")
        print("• Run with --verbose flag to see detailed logging")
        print("• Check database permissions and disk space")
        
    except Exception as e:
        print(f"❌ Debug failed: {e}")
    
    finally:
        conn.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python debug_import.py <db_path> [batch_id]")
        sys.exit(1)
    
    db_path = sys.argv[1]
    debug_import_issues(db_path)