import sqlite3
import json
from simple_app import get_ae_query_condition, load_ae_config

def test_ae_queries():
    """Test the AE query conditions."""
    
    print("🔍 Testing AE Query Conditions...")
    
    # Connect to database
    conn = sqlite3.connect('../../data/database/production.db')
    conn.row_factory = sqlite3.Row
    
    # Load AE config
    ae_config = load_ae_config()
    active_aes = [ae for ae, config in ae_config.items() if config.get('active', True)]
    
    print(f"Active AEs: {active_aes}")
    
    for ae_name in active_aes:
        print(f"\n--- Testing {ae_name} ---")
        
        # Get query condition
        ae_condition = get_ae_query_condition(ae_name)
        print(f"Query condition: {ae_condition}")
        
        # Test the query
        try:
            ae_revenue_query = f"""
            SELECT ROUND(SUM(gross_rate), 2) as total_revenue
            FROM spots 
            WHERE strftime('%Y', broadcast_month) = '2025'
            AND ({ae_condition})
            AND (revenue_type != 'Trade' OR revenue_type IS NULL)
            """
            
            cursor = conn.execute(ae_revenue_query)
            result = cursor.fetchone()
            revenue = result['total_revenue'] or 0
            print(f"Revenue: ${revenue:,.2f}")
            
            # Test customer count
            ae_customers_query = f"""
            SELECT COUNT(DISTINCT customer_id) as total_customers
            FROM spots 
            WHERE strftime('%Y', broadcast_month) = '2025'
            AND ({ae_condition})
            AND (revenue_type != 'Trade' OR revenue_type IS NULL)
            """
            
            cursor = conn.execute(ae_customers_query)
            result = cursor.fetchone()
            customers = result['total_customers'] or 0
            print(f"Customers: {customers}")
            
        except Exception as e:
            print(f"❌ Error with {ae_name}: {e}")
    
    conn.close()
    print("\n✅ Testing complete!")

if __name__ == "__main__":
    test_ae_queries() 