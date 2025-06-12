# Quick test script - save as test_connection.py
import sqlite3
import sys
sys.path.append('src')

from services.language_block_service import test_language_block_service

# Update with your actual database path
db_path = "ctv_booked_biz.db"  # or wherever your db is
conn = sqlite3.connect(db_path)

try:
    # Test with just 3 spots
    results = test_language_block_service(conn, limit=3)
    print("🎉 Success! Service is working with your data.")
except Exception as e:
    print(f"❌ Issue found: {e}")
finally:
    conn.close()