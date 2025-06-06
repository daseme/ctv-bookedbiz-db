import sqlite3
import pandas as pd
import os

def test_database(db_name):
    """Test a specific database against Excel data"""
    
    # Database path
    db_path = f"data/database/{db_name}.db"
    excel_path = "data/raw/2024.xlsx"
    
    print(f"=== Testing {db_name.upper()} Database ===")
    print(f"Database: {db_path}")
    print(f"Excel file: {excel_path}")
    print()
    
    # Check if files exist
    if not os.path.exists(db_path):
        print(f"❌ Database not found: {db_path}")
        return None
    
    if not os.path.exists(excel_path):
        print(f"❌ Excel file not found: {excel_path}")
        return None
    
    # Read Excel data
    print("📊 Reading Excel data...")
    excel_df = pd.read_excel(excel_path)
    
    # Check column names
    print("Excel columns:", list(excel_df.columns))
    print()
    
    # Try to identify the correct column names
    revenue_col = None
    revenue_type_col = None
    month_col = None
    
    for col in excel_df.columns:
        col_lower = col.lower()
        if 'gross' in col_lower and 'rate' in col_lower:
            revenue_col = col
        elif 'revenue' in col_lower and 'type' in col_lower:
            revenue_type_col = col
        elif col_lower == 'month':
            month_col = col
    
    if not revenue_col:
        print("❌ Could not find revenue column (expected 'Gross Rate' or similar)")
        return None
    
    if not revenue_type_col:
        print("❌ Could not find revenue type column (expected 'Revenue Type' or similar)")
        return None
    
    print(f"Using columns: Revenue='{revenue_col}', Revenue Type='{revenue_type_col}', Month='{month_col}'")
    print()
    
    # Filter Excel data (excluding Trade revenue)
    excel_filtered = excel_df[excel_df[revenue_type_col] != 'Trade'].copy()
    
    excel_count = len(excel_filtered)
    excel_revenue = excel_filtered[revenue_col].sum()
    
    print(f"Excel 2024 data (excluding Trade):")
    print(f"  Records: {excel_count:,}")
    print(f"  Revenue: ${excel_revenue:,.2f}")
    print()
    
    # Read database data with read-only connection
    print("🗄️ Reading database data...")
    try:
        # Use URI with read-only flag
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        
        # Get 2024 data from database (excluding Trade revenue)
        db_query = """
        SELECT COUNT(*) as count, SUM(gross_rate) as revenue
        FROM spots 
        WHERE strftime('%Y', broadcast_month) = '2024'
        AND revenue_type != 'Trade'
        """
        
        db_result = pd.read_sql_query(db_query, conn)
        db_count = int(db_result['count'].iloc[0])
        db_revenue = float(db_result['revenue'].iloc[0])
        
        print(f"Database 2024 data (excluding Trade):")
        print(f"  Records: {db_count:,}")
        print(f"  Revenue: ${db_revenue:,.2f}")
        print()
        
        # Calculate differences
        count_diff = db_count - excel_count
        revenue_diff = db_revenue - excel_revenue
        
        print("=== COMPARISON RESULTS ===")
        print(f"Record count difference: {count_diff:,}")
        print(f"Revenue difference: ${revenue_diff:,.2f}")
        print()
        
        # Check if issue is resolved
        result = {}
        if count_diff == 0 and abs(revenue_diff) < 0.01:  # Allow for small rounding differences
            print("✅ SUCCESS! Database matches Excel data perfectly!")
            print("✅ The $80 discrepancy appears to be RESOLVED!")
            result['status'] = 'perfect'
        elif abs(revenue_diff) == 80.0:
            print("❌ The $80 discrepancy still EXISTS")
            print("❌ Database still differs from Excel by exactly $80")
            result['status'] = 'still_80_issue'
        else:
            print(f"⚠️ Database differs from Excel by ${revenue_diff:.2f}")
            if abs(revenue_diff) < 80:
                print("✅ This is better than the original $80 discrepancy")
                result['status'] = 'improved'
            else:
                print("❌ This is worse than the original $80 discrepancy")
                result['status'] = 'worse'
        
        result['count_diff'] = count_diff
        result['revenue_diff'] = revenue_diff
        result['db_count'] = db_count
        result['db_revenue'] = db_revenue
        
        conn.close()
        return result
        
    except Exception as e:
        print(f"❌ Error accessing database: {e}")
        return None

def main():
    """Test both databases and compare"""
    
    print("Let's test both your databases to see which one is better!\n")
    
    # Test production database
    prod_result = test_database("production")
    
    print("\n" + "="*60 + "\n")
    
    # Test original database  
    test_result = test_database("test")
    
    # Summary
    print("\n" + "="*60)
    print("=== SUMMARY ===")
    
    if prod_result and test_result:
        print(f"Production DB: {prod_result['revenue_diff']:+.2f} difference")
        print(f"Test DB: {test_result['revenue_diff']:+.2f} difference")
        print()
        
        if abs(prod_result['revenue_diff']) < abs(test_result['revenue_diff']):
            print("🎉 PRODUCTION database is BETTER!")
        elif abs(prod_result['revenue_diff']) > abs(test_result['revenue_diff']):
            print("🎉 TEST database is BETTER!")
        else:
            print("Both databases have the same accuracy")
            
        if prod_result['status'] == 'perfect':
            print("✅ PRODUCTION database is PERFECT - use this one!")
        elif test_result['status'] == 'perfect':
            print("✅ TEST database is PERFECT - use this one!")
    
    elif prod_result:
        print("Only production database is accessible")
        if prod_result['status'] == 'perfect':
            print("✅ PRODUCTION database is PERFECT!")
    elif test_result:
        print("Only test database is accessible")
        if test_result['status'] == 'perfect':
            print("✅ TEST database is PERFECT!")
    else:
        print("❌ Could not access either database")

if __name__ == "__main__":
    main() 