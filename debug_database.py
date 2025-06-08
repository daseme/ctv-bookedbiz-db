import sqlite3
import os
from datetime import datetime

def test_database_query():
    """Test the exact database query used by Flask."""
    print("=== DATABASE QUERY DEBUG ===\n")
    
    # Database path as used by Flask
    db_path = 'data/database/production.db'
    print(f"Database path: {db_path}")
    print(f"Database exists: {os.path.exists(db_path)}")
    
    if not os.path.exists(db_path):
        print("✗ Database file not found!")
        return
    
    conn = sqlite3.connect(db_path)
    
    # Test parameters
    ae_name = "Charmaine Lane"
    month = "2025-06"
    
    print(f"Testing query for AE: '{ae_name}', Month: '{month}'\n")
    
    # The exact query used by Flask
    hist_query = """
    SELECT ROUND(SUM(gross_rate), 2) as revenue
    FROM spots
    WHERE sales_person = ?
    AND strftime('%Y-%m', air_date) = ?
    AND gross_rate IS NOT NULL
    AND (revenue_type != 'Trade' OR revenue_type IS NULL)
    """
    
    print("Running Flask query:")
    print(hist_query)
    print(f"Parameters: ['{ae_name}', '{month}']")
    
    cursor = conn.execute(hist_query, (ae_name, month))
    result = cursor.fetchone()
    revenue = result[0] if result and result[0] else 0
    
    print(f"Result: ${revenue:,}")
    
    # Let's also test a broader query to see what data exists
    print(f"\n=== BROADER ANALYSIS ===")
    
    # Check all data for this AE
    broad_query = """
    SELECT 
        strftime('%Y-%m', air_date) as month,
        COUNT(*) as spots,
        SUM(gross_rate) as revenue,
        MIN(air_date) as min_date,
        MAX(air_date) as max_date
    FROM spots 
    WHERE sales_person = ?
    AND gross_rate IS NOT NULL
    GROUP BY strftime('%Y-%m', air_date)
    ORDER BY month DESC
    LIMIT 10
    """
    
    print(f"Recent months for {ae_name}:")
    cursor = conn.execute(broad_query, (ae_name,))
    for row in cursor.fetchall():
        month_str, spots, revenue, min_date, max_date = row
        print(f"  {month_str}: {spots:,} spots, ${revenue:,.0f} ({min_date} to {max_date})")
    
    # Check if there are any revenue_type filters affecting results
    print(f"\n=== REVENUE TYPE ANALYSIS ===")
    
    revenue_type_query = """
    SELECT 
        revenue_type,
        COUNT(*) as spots,
        SUM(gross_rate) as revenue
    FROM spots 
    WHERE sales_person = ?
    AND strftime('%Y-%m', air_date) = ?
    AND gross_rate IS NOT NULL
    GROUP BY revenue_type
    """
    
    cursor = conn.execute(revenue_type_query, (ae_name, month))
    print(f"Revenue types for {ae_name} in {month}:")
    for row in cursor.fetchall():
        rev_type, spots, revenue = row
        rev_type_str = rev_type or "NULL"
        print(f"  {rev_type_str}: {spots:,} spots, ${revenue:,.0f}")
        
    conn.close()

if __name__ == "__main__":
    test_database_query() 