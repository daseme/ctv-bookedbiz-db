#!/usr/bin/env python3

import sys
import traceback

def test_flask_imports():
    """Test all the imports used in the management report route."""
    
    print("🔍 Testing Flask Route Imports...")
    
    try:
        print("1. Testing basic imports...")
        import sqlite3
        import json
        from datetime import datetime
        print("✅ Basic imports successful")
        
        print("2. Testing Flask imports...")
        from flask import Flask, render_template, request
        print("✅ Flask imports successful")
        
        print("3. Testing budget warehouse import...")
        from budget_warehouse import BudgetWarehouse
        print("✅ BudgetWarehouse import successful")
        
        print("4. Testing BudgetWarehouse instantiation...")
        warehouse = BudgetWarehouse()
        print("✅ BudgetWarehouse instantiation successful")
        
        print("5. Testing warehouse methods...")
        budgets = warehouse.get_company_budget_totals(2025)
        print(f"✅ Company budgets: {budgets}")
        
        ae_budgets = warehouse.get_quarterly_budget_summary(2025)
        print(f"✅ AE budgets loaded: {len(ae_budgets)} AEs")
        
        print("6. Testing AE config loading...")
        with open('ae_config.json', 'r') as f:
            ae_config = json.load(f)['ae_settings']
        active_aes = [ae for ae, config in ae_config.items() if config.get('active', True)]
        print(f"✅ Active AEs: {active_aes}")
        
        print("7. Testing database connection...")
        import os
        db_path = '../../data/database/production.db'
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        print("✅ Database connection successful")
        
        print("8. Testing basic query...")
        cursor = conn.execute("SELECT COUNT(*) as count FROM spots WHERE strftime('%Y', broadcast_month) = '2025'")
        result = cursor.fetchone()
        print(f"✅ 2025 spot count: {result['count']}")
        conn.close()
        
        print("\n✅ All imports and basic functionality working!")
        return True
        
    except Exception as e:
        print(f"❌ Error during testing: {e}")
        print("Full traceback:")
        traceback.print_exc()
        return False

def simulate_management_route():
    """Simulate the core logic of the management report route."""
    
    print("\n🎯 Simulating Management Report Route Logic...")
    
    try:
        from budget_warehouse import BudgetWarehouse
        import sqlite3
        import json
        from datetime import datetime
        
        # Initialize
        warehouse = BudgetWarehouse()
        conn = sqlite3.connect('../../data/database/production.db')
        conn.row_factory = sqlite3.Row
        
        year = '2025'
        print(f"Year: {year}")
        
        # Load AE config
        with open('ae_config.json', 'r') as f:
            ae_config = json.load(f)['ae_settings']
        active_aes = [ae for ae, config in ae_config.items() if config.get('active', True)]
        print(f"Active AEs: {active_aes}")
        
        # Test company budget data
        company_budgets = warehouse.get_company_budget_totals(int(year))
        print(f"Company budgets: {company_budgets}")
        
        # Test total revenue
        total_revenue_query = """
        SELECT ROUND(SUM(gross_rate), 2) as total_revenue
        FROM spots 
        WHERE strftime('%Y', broadcast_month) = ?
        AND (revenue_type != 'Trade' OR revenue_type IS NULL)
        """
        cursor = conn.execute(total_revenue_query, (year,))
        total_revenue_raw = cursor.fetchone()['total_revenue'] or 0
        print(f"Total revenue: ${total_revenue_raw:,.2f}")
        
        # Test quarterly data
        quarter = 1
        start_month = 1
        end_month = 3
        
        quarter_revenue_query = """
        SELECT ROUND(SUM(gross_rate), 2) as quarter_revenue
        FROM spots 
        WHERE strftime('%Y', broadcast_month) = ?
        AND cast(strftime('%m', broadcast_month) as integer) BETWEEN ? AND ?
        AND (revenue_type != 'Trade' OR revenue_type IS NULL)
        """
        
        cursor = conn.execute(quarter_revenue_query, (year, start_month, end_month))
        quarter_booked_revenue = cursor.fetchone()['quarter_revenue'] or 0
        print(f"Q1 booked revenue: ${quarter_booked_revenue:,.2f}")
        
        # Test budget from warehouse
        quarter_budget = company_budgets.get(quarter, 0)
        print(f"Q1 budget from warehouse: ${quarter_budget:,.2f}")
        
        conn.close()
        print("✅ Route simulation successful!")
        return True
        
    except Exception as e:
        print(f"❌ Route simulation failed: {e}")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("=" * 50)
    print("🧪 TESTING MANAGEMENT REPORT ROUTE")
    print("=" * 50)
    
    # Test imports
    import_success = test_flask_imports()
    
    if import_success:
        # Test route simulation
        route_success = simulate_management_route()
        
        if route_success:
            print("\n🎉 ALL TESTS PASSED!")
            print("The management report route should be working.")
            print("Issue may be elsewhere.")
        else:
            print("\n❌ ROUTE SIMULATION FAILED")
    else:
        print("\n❌ IMPORT TESTS FAILED")
    
    print("=" * 50) 