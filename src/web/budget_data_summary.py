from budget_warehouse import BudgetWarehouse
import sqlite3

def show_budget_data_location():
    """Show where budget data is stored and how to access it."""
    
    print("=" * 60)
    print("📊 BUDGET DATA LOCATION & ACCESS GUIDE")
    print("=" * 60)
    
    print("\n🗄️ DATABASE LOCATION:")
    print("   File: ../../data/database/production.db")
    print("   Tables: budget_versions, budget_data, budget (legacy)")
    
    print("\n🐍 PYTHON ACCESS:")
    print("   File: budget_warehouse.py") 
    print("   Class: BudgetWarehouse")
    
    print("\n💰 CURRENT BUDGET DATA (2025):")
    
    try:
        warehouse = BudgetWarehouse()
        
        # Company totals
        company_budgets = warehouse.get_company_budget_totals(2025)
        total_budget = sum(company_budgets.values())
        
        print(f"   Total Annual Budget: ${total_budget:,.0f}")
        print("   Quarterly Breakdown:")
        for quarter, budget in company_budgets.items():
            print(f"     Q{quarter}: ${budget:,.0f}")
        
        # AE breakdown
        print("\n👥 AE BUDGET BREAKDOWN:")
        ae_budgets = warehouse.get_quarterly_budget_summary(2025)
        for ae_name, quarters in ae_budgets.items():
            annual_total = sum(quarters.values())
            print(f"   {ae_name}: ${annual_total:,.0f}")
            for quarter, budget in quarters.items():
                print(f"     Q{quarter}: ${budget:,.0f}")
            print()
        
        print("📈 USAGE EXAMPLES:")
        print("   # Get company Q1 budget")
        print("   warehouse = BudgetWarehouse()")
        print("   q1_budget = warehouse.get_company_budget_totals(2025)[1]")
        print()
        print("   # Get Charmaine's Q1 budget")
        print("   ae_budgets = warehouse.get_quarterly_budget_summary(2025)")
        print("   charmaine_q1 = ae_budgets['Charmaine Lane'][1]")
        
        print("\n✅ Budget data is properly warehoused and accessible!")
        
    except Exception as e:
        print(f"❌ Error accessing budget data: {e}")
    
    print("=" * 60)

if __name__ == "__main__":
    show_budget_data_location() 