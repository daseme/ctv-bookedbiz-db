import sqlite3
from budget_warehouse import BudgetWarehouse
import json

def test_management_report_data():
    """Test the data that should be loaded in the management report."""
    
    print("🔍 Debugging Management Report Data Loading...")
    
    # Test database connection
    db_path = '../../data/database/production.db'
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        print("✅ Database connection successful")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return
    
    # Test budget warehouse
    try:
        warehouse = BudgetWarehouse()
        print("✅ Budget warehouse initialized")
        
        # Test budget data
        company_budgets = warehouse.get_company_budget_totals(2025)
        print(f"📊 Company budgets: {company_budgets}")
        
        ae_budgets = warehouse.get_quarterly_budget_summary(2025)
        print(f"📊 AE budgets: {ae_budgets}")
        
    except Exception as e:
        print(f"❌ Budget warehouse error: {e}")
        import traceback
        traceback.print_exc()
    
    
    # Test AE configuration
    try:
        with open('ae_config.json', 'r') as f:
            ae_config = json.load(f)['ae_settings']
        active_aes = [ae for ae, config in ae_config.items() if config.get('active', True)]
        print(f"✅ Active AEs: {active_aes}")
    except Exception as e:
        print(f"❌ AE config error: {e}")
    
    # Test actual revenue data
    try:
        # Test total revenue
        total_revenue_query = """
        SELECT ROUND(SUM(gross_rate), 2) as total_revenue
        FROM spots 
        WHERE strftime('%Y', broadcast_month) = ?
        AND (revenue_type != 'Trade' OR revenue_type IS NULL)
        """
        
        cursor = conn.execute(total_revenue_query, ('2025',))
        total_revenue = cursor.fetchone()['total_revenue'] or 0
        print(f"💰 2025 Total Revenue: ${total_revenue:,.2f}")
        
        # Test quarterly data
        for quarter in range(1, 5):
            start_month = (quarter - 1) * 3 + 1
            end_month = quarter * 3
            
            quarter_revenue_query = """
            SELECT ROUND(SUM(gross_rate), 2) as quarter_revenue
            FROM spots 
            WHERE strftime('%Y', broadcast_month) = ?
            AND cast(strftime('%m', broadcast_month) as integer) BETWEEN ? AND ?
            AND (revenue_type != 'Trade' OR revenue_type IS NULL)
            """
            
            cursor = conn.execute(quarter_revenue_query, ('2025', start_month, end_month))
            quarter_revenue = cursor.fetchone()['quarter_revenue'] or 0
            print(f"📅 Q{quarter} 2025 Revenue: ${quarter_revenue:,.2f}")
            
        # Test AE data
        print("\n👥 AE Revenue Data:")
        for ae_name in active_aes[:2]:  # Test first 2 AEs
            if ae_name == 'Charmaine Lane':
                ae_condition = "sales_person = ?"
                ae_params = [ae_name]
            elif ae_name == 'House':
                ae_condition = "sales_person IN (?, ?)"
                ae_params = ['House', 'HOUSE']
            else:
                ae_condition = "sales_person = ?"
                ae_params = [ae_name]
            
            ae_revenue_query = f"""
            SELECT ROUND(SUM(gross_rate), 2) as ae_revenue
            FROM spots 
            WHERE strftime('%Y', broadcast_month) = ?
            AND {ae_condition}
            AND (revenue_type != 'Trade' OR revenue_type IS NULL)
            """
            
            cursor = conn.execute(ae_revenue_query, ['2025'] + ae_params)
            ae_revenue = cursor.fetchone()['ae_revenue'] or 0
            print(f"  {ae_name}: ${ae_revenue:,.2f}")
            
    except Exception as e:
        print(f"❌ Revenue data error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        conn.close()
    
    print("\n✅ Debug complete!")

if __name__ == "__main__":
    test_management_report_data() 