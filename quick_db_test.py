import sqlite3
import pandas as pd

def quick_test():
    """Quick test of production database"""
    
    # Load Excel data
    print("Loading Excel data...")
    excel_df = pd.read_excel("data/raw/2024.xlsx")
    
    # Filter Excel (exclude Trade)
    excel_filtered = excel_df[excel_df['Revenue Type'] != 'Trade']
    excel_count = len(excel_filtered)
    excel_revenue = excel_filtered['Unit rate Gross'].sum()
    
    print(f"Excel: {excel_count:,} records, ${excel_revenue:,.2f}")
    
    # Try production database
    try:
        conn = sqlite3.connect("data/database/production.db")
        cursor = conn.execute("""
            SELECT COUNT(*) as count, SUM(gross_rate) as revenue
            FROM spots 
            WHERE strftime('%Y', broadcast_month) = '2024'
            AND revenue_type != 'Trade'
        """)
        
        result = cursor.fetchone()
        db_count, db_revenue = result[0], result[1]
        
        print(f"Production DB: {db_count:,} records, ${db_revenue:,.2f}")
        
        # Calculate difference
        count_diff = db_count - excel_count
        revenue_diff = db_revenue - excel_revenue
        
        print(f"\nDifference: {count_diff:,} records, ${revenue_diff:,.2f}")
        
        if abs(revenue_diff) < 0.01:
            print("🎉 PERFECT MATCH! $80 discrepancy is RESOLVED!")
        elif abs(revenue_diff) == 80.0:
            print("😞 Still have $80 discrepancy")
        else:
            print(f"⚠️  Different discrepancy: ${revenue_diff:.2f}")
            
        conn.close()
        
    except Exception as e:
        print(f"Database error: {e}")

if __name__ == "__main__":
    quick_test() 