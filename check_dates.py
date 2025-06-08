import sqlite3
from datetime import datetime

def check_database_dates():
    conn = sqlite3.connect('data/database/production.db')
    cursor = conn.cursor()
    
    print("=== DATABASE DATE ANALYSIS ===\n")
    
    # Check overall date range
    cursor.execute('SELECT MIN(air_date), MAX(air_date), COUNT(*) FROM spots')
    min_date, max_date, total_spots = cursor.fetchone()
    print(f"Overall Database Range: {min_date} to {max_date} ({total_spots:,} total spots)")
    
    # Check by AE
    print("\n=== BY SALES PERSON ===")
    cursor.execute('''
        SELECT sales_person, MIN(air_date), MAX(air_date), COUNT(*), SUM(gross_rate)
        FROM spots 
        WHERE sales_person IS NOT NULL AND sales_person != ""
        GROUP BY sales_person 
        ORDER BY COUNT(*) DESC
    ''')
    
    for row in cursor.fetchall():
        ae, min_dt, max_dt, count, revenue = row
        print(f"  {ae}: {min_dt} to {max_dt}, {count:,} spots, ${revenue:,.0f} revenue")
    
    # Check monthly revenue for Charmaine Lane
    print("\n=== CHARMAINE LANE MONTHLY REVENUE ===")
    cursor.execute('''
        SELECT 
            strftime('%Y-%m', air_date) as month,
            COUNT(*) as spots,
            SUM(gross_rate) as revenue
        FROM spots 
        WHERE sales_person = "Charmaine Lane"
        GROUP BY strftime('%Y-%m', air_date)
        ORDER BY month DESC
        LIMIT 12
    ''')
    
    for row in cursor.fetchall():
        month, spots, revenue = row
        print(f"  {month}: {spots:,} spots, ${revenue:,.0f}")
    
    # Check what the app would be querying for
    print("\n=== WHAT APP IS TRYING TO QUERY (2025-06 onward) ===")
    cursor.execute('''
        SELECT 
            strftime('%Y-%m', air_date) as month,
            COUNT(*) as spots,
            SUM(gross_rate) as revenue
        FROM spots 
        WHERE sales_person = "Charmaine Lane"
          AND air_date >= "2025-06-01"
        GROUP BY strftime('%Y-%m', air_date)
        ORDER BY month
    ''')
    
    results = cursor.fetchall()
    if results:
        for row in results:
            month, spots, revenue = row
            print(f"  {month}: {spots:,} spots, ${revenue:,.0f}")
    else:
        print("  NO DATA FOUND for 2025-06 onward!")
        print("  This explains why booked_revenue is 0!")
    
    conn.close()

if __name__ == "__main__":
    check_database_dates() 