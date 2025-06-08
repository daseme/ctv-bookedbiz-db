import sqlite3
import os
import json
from datetime import datetime

def test_working_approach():
    """Test using the exact same approach as the working reports."""
    print("=== TESTING WORKING REPORT APPROACH ===\n")
    
    # Use the exact same database path structure as working reports
    db_path = 'data/database/production.db'
    
    print(f"Database path: {db_path}")
    print(f"Database exists: {os.path.exists(db_path)}")
    
    if not os.path.exists(db_path):
        print("✗ Database not found!")
        return
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # Enable column access by name
    
    # Test AE information first
    print("\n=== AE INFORMATION ===")
    ae_query = """
    SELECT DISTINCT sales_person, COUNT(*) as spot_count, SUM(gross_rate) as total_revenue
    FROM spots 
    WHERE sales_person IS NOT NULL 
    AND sales_person != ''
    AND gross_rate IS NOT NULL
    AND (revenue_type != 'Trade' OR revenue_type IS NULL)
    GROUP BY sales_person
    ORDER BY total_revenue DESC
    """
    
    cursor = conn.execute(ae_query)
    ae_results = [dict(row) for row in cursor.fetchall()]
    
    print("Available AEs in database:")
    for ae in ae_results:
        print(f"  {ae['sales_person']}: {ae['spot_count']:,} spots, ${ae['total_revenue']:,.0f}")
    
    # Test the exact query that should work for Charmaine Lane in June 2025
    print("\n=== TESTING CHARMAINE LANE JUNE 2025 ===")
    
    # Query using broadcast_month (like working reports)
    revenue_query = """
    SELECT 
        strftime('%Y-%m', broadcast_month) as month,
        COUNT(*) as spot_count,
        ROUND(SUM(gross_rate), 2) as total_revenue
    FROM spots
    WHERE sales_person = ?
    AND strftime('%Y-%m', broadcast_month) = ?
    AND gross_rate IS NOT NULL
    AND (revenue_type != 'Trade' OR revenue_type IS NULL)
    GROUP BY strftime('%Y-%m', broadcast_month)
    """
    
    ae_name = "Charmaine Lane"
    month = "2025-06"
    
    print(f"Query: {revenue_query}")
    print(f"Parameters: ['{ae_name}', '{month}']")
    
    cursor = conn.execute(revenue_query, (ae_name, month))
    results = [dict(row) for row in cursor.fetchall()]
    
    if results:
        result = results[0]
        print(f"✓ SUCCESS: Found {result['spot_count']:,} spots with ${result['total_revenue']:,} revenue")
    else:
        print("✗ No results found!")
        
        # Debug: Check what months exist for this AE
        debug_query = """
        SELECT DISTINCT strftime('%Y-%m', broadcast_month) as month
        FROM spots
        WHERE sales_person = ?
        AND gross_rate IS NOT NULL
        ORDER BY month DESC
        LIMIT 10
        """
        
        cursor = conn.execute(debug_query, (ae_name,))
        available_months = [row[0] for row in cursor.fetchall()]
        print(f"Available months for {ae_name}: {available_months}")
    
    # Test budget loading
    print("\n=== TESTING BUDGET LOADING ===")
    
    budget_file = 'real_budget_data.json'
    print(f"Budget file: {budget_file}")
    print(f"Budget file exists: {os.path.exists(budget_file)}")
    
    if os.path.exists(budget_file):
        with open(budget_file, 'r') as f:
            budget_data = json.load(f)
        
        print(f"Budget AEs: {list(budget_data.get('budget_2025', {}).keys())}")
        
        if ae_name in budget_data.get('budget_2025', {}):
            budget_amount = budget_data['budget_2025'][ae_name].get(month, 0)
            print(f"✓ Budget for {ae_name}/{month}: ${budget_amount:,}")
        else:
            print(f"✗ No budget found for {ae_name}")
    
    # Test pipeline calculation
    print("\n=== TESTING PIPELINE CALCULATION ===")
    if results and os.path.exists(budget_file):
        revenue = results[0]['total_revenue']
        budget_amount = budget_data['budget_2025'][ae_name].get(month, 0)
        
        current_pipeline = budget_amount * 0.6 if budget_amount > 0 else 50000
        expected_pipeline = budget_amount * 0.7 if budget_amount > 0 else 60000
        
        print(f"Revenue (booked): ${revenue:,}")
        print(f"Budget: ${budget_amount:,}")
        print(f"Current Pipeline (60% of budget): ${current_pipeline:,}")
        print(f"Expected Pipeline (70% of budget): ${expected_pipeline:,}")
        
        pipeline_gap = current_pipeline - expected_pipeline
        budget_gap = (revenue + current_pipeline) - budget_amount
        
        print(f"Pipeline Gap: ${pipeline_gap:,}")
        print(f"Budget Gap: ${budget_gap:,}")
    
    conn.close()

if __name__ == "__main__":
    test_working_approach() 