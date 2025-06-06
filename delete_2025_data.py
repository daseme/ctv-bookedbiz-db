#!/usr/bin/env python3
"""Delete incomplete 2025 data to prepare for re-import with full columns."""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from database.connection import DatabaseConnection
from services.base_service import BaseService

class DataCleanupService(BaseService):
    def delete_2025_months(self):
        """Delete 2025 month data and closures for re-import."""
        months_2025 = ['Jan-25', 'Feb-25', 'Mar-25', 'Apr-25']
        
        print("🧹 Deleting incomplete 2025 data for re-import...")
        
        with self.safe_transaction() as conn:
            total_spots_deleted = 0
            
            for month in months_2025:
                # Delete spots for this month
                cursor = conn.execute("""
                    DELETE FROM spots 
                    WHERE strftime('%Y', air_date) = '2025'
                    AND broadcast_month LIKE ?
                """, (f"%{month.replace('-', '-')}%",))
                
                spots_deleted = cursor.rowcount
                total_spots_deleted += spots_deleted
                
                # Delete month closure record
                cursor = conn.execute("""
                    DELETE FROM month_closures 
                    WHERE broadcast_month = ?
                """, (month,))
                
                closure_deleted = cursor.rowcount
                
                print(f"  🗑️  {month}: {spots_deleted} spots deleted, closure {'removed' if closure_deleted else 'not found'}")
            
            print(f"✅ Total cleanup: {total_spots_deleted} spots deleted from 4 months")
            return total_spots_deleted

# Run the cleanup
if __name__ == "__main__":
    db_connection = DatabaseConnection("data/database/production.db")
    cleanup_service = DataCleanupService(db_connection)
    
    try:
        # Show before
        print("📊 Before cleanup:")
        with db_connection.transaction() as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM spots WHERE strftime('%Y', air_date) = '2025'")
            before_count = cursor.fetchone()[0]
            print(f"  2025 spots: {before_count:,}")
        
        # Do cleanup
        deleted = cleanup_service.delete_2025_months()
        
        # Show after
        print("\n📊 After cleanup:")
        with db_connection.transaction() as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM spots WHERE strftime('%Y', air_date) = '2025'")
            after_count = cursor.fetchone()[0]
            print(f"  2025 spots: {after_count:,}")
        
        print(f"\n🎯 Ready to re-import 2025 data with full 29 columns!")
        
    finally:
        db_connection.close()