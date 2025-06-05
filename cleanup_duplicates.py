# Create a clean Python script
import sqlite3

conn = sqlite3.connect("data/database/test.db")

# Delete the bad 2025 import
cursor = conn.execute("DELETE FROM spots WHERE import_batch_id = ?", ("full_2025_import",))
deleted_2025 = cursor.rowcount
print(f"Deleted {deleted_2025} bad 2025 records")

# Reopen the 2025 months (remove closures)
months_to_reopen = ("Jan-25", "Feb-25", "Mar-25", "Apr-25")
cursor = conn.execute("DELETE FROM month_closures WHERE broadcast_month IN (?, ?, ?, ?)", months_to_reopen)
reopened = cursor.rowcount
print(f"Reopened {reopened} months")

conn.commit()

# Verify cleanup
result = conn.execute("SELECT import_batch_id, COUNT(*) FROM spots GROUP BY import_batch_id ORDER BY COUNT(*) DESC").fetchall()
print("\nRemaining data after cleanup:")
for batch_id, count in result:
    batch_name = batch_id if batch_id else "null (2024 data)"
    print(f"  {batch_name}: {count:,} records")

# Check month closure status
closures = conn.execute("SELECT broadcast_month FROM month_closures ORDER BY broadcast_month").fetchall()
print("\nClosed months:")
for (month,) in closures:
    print(f"  {month}")

conn.close()