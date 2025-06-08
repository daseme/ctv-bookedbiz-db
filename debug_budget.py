import os
import json
from datetime import datetime

def test_budget_loading():
    """Test the budget loading functionality directly."""
    print("=== BUDGET LOADING DEBUG ===\n")
    
    # Test from the project root (where this script runs)
    ae_name = "Charmaine Lane"
    month = "2025-06"
    
    print(f"Current working directory: {os.getcwd()}")
    
    # Try different path options
    possible_paths = [
        'real_budget_data.json',
        './real_budget_data.json',
        'src/web/../../real_budget_data.json',
        os.path.join(os.path.dirname(__file__), 'real_budget_data.json')
    ]
    
    for path in possible_paths:
        print(f"\nTrying path: {path}")
        if os.path.exists(path):
            print(f"  ✓ File exists!")
            try:
                with open(path, 'r') as f:
                    budget_data = json.load(f)
                
                print(f"  Available AEs: {list(budget_data.get('budget_2025', {}).keys())}")
                
                if ae_name in budget_data.get('budget_2025', {}):
                    budget_amount = budget_data['budget_2025'][ae_name].get(month, 0)
                    print(f"  ✓ Found budget for {ae_name}/{month}: ${budget_amount:,}")
                else:
                    print(f"  ✗ AE '{ae_name}' not found in budget data")
                    
            except Exception as e:
                print(f"  ✗ Error loading: {e}")
        else:
            print(f"  ✗ File does not exist")
    
    # Test from src/web directory (where Flask runs)
    print(f"\n=== TESTING FROM src/web DIRECTORY ===")
    web_dir = os.path.join(os.getcwd(), 'src', 'web')
    
    if os.path.exists(web_dir):
        os.chdir(web_dir)
        print(f"Changed to: {os.getcwd()}")
        
        web_path = '../../real_budget_data.json'
        print(f"Testing path from web dir: {web_path}")
        
        if os.path.exists(web_path):
            print("  ✓ File exists from web directory!")
            try:
                with open(web_path, 'r') as f:
                    budget_data = json.load(f)
                
                if ae_name in budget_data.get('budget_2025', {}):
                    budget_amount = budget_data['budget_2025'][ae_name].get(month, 0)
                    print(f"  ✓ Budget for {ae_name}/{month}: ${budget_amount:,}")
                else:
                    print(f"  ✗ AE not found")
            except Exception as e:
                print(f"  ✗ Error: {e}")
        else:
            print("  ✗ File not found from web directory")

if __name__ == "__main__":
    test_budget_loading() 