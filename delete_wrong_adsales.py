import sqlite3

conn = sqlite3.connect("/mnt/c/Users/Kurt/Crossings TV Dropbox/kurt olmstead/Financial/Sales/WeeklyReports/ctv-bookedbiz-db/data/database/production.db")
cursor = conn.cursor()

query = """
DELETE FROM spots
WHERE is_historical = 0
  AND strftime('%Y-%m', broadcast_month) = '2025-06'
  AND strftime('%d', broadcast_month) NOT IN ('01', '15');


"""

cursor.execute(query)
conn.commit()
print(f"{cursor.rowcount} rows deleted.")
conn.close()
