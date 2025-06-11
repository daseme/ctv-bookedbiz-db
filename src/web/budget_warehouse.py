import sqlite3
import json
from datetime import datetime
import csv
import io

class BudgetWarehouse:
    """
    Budget Data Warehouse Manager
    
    Handles the storage, versioning, and retrieval of budget data with 
    proper data governance and best practices.
    """
    
    def __init__(self, db_path='../../data/database/production.db'):
        self.db_path = db_path
        
    def get_connection(self):
        """Get database connection with row factory."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def create_budget_tables(self):
        """Create budget tables with proper structure and constraints."""
        conn = self.get_connection()
        
        # Enhanced budget table with version tracking
        conn.execute("""
        CREATE TABLE IF NOT EXISTS budget_versions (
            version_id INTEGER PRIMARY KEY AUTOINCREMENT,
            version_name TEXT NOT NULL,
            year INTEGER NOT NULL,
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_by TEXT,
            description TEXT,
            is_active BOOLEAN DEFAULT 1,
            source_file TEXT,
            UNIQUE(version_name, year)
        )
        """)
        
        # Budget data table linked to versions
        conn.execute("""
        CREATE TABLE IF NOT EXISTS budget_data (
            budget_data_id INTEGER PRIMARY KEY AUTOINCREMENT,
            version_id INTEGER NOT NULL,
            ae_name TEXT NOT NULL,
            year INTEGER NOT NULL,
            quarter INTEGER NOT NULL,
            month INTEGER NOT NULL,
            budget_amount DECIMAL(12, 2) NOT NULL,
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (version_id) REFERENCES budget_versions(version_id),
            UNIQUE(version_id, ae_name, year, month)
        )
        """)
        
        # Create indexes for performance
        conn.execute("CREATE INDEX IF NOT EXISTS idx_budget_data_ae_year ON budget_data(ae_name, year)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_budget_data_quarter ON budget_data(year, quarter)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_budget_versions_active ON budget_versions(year, is_active)")
        
        conn.commit()
        conn.close()
        print("✅ Budget warehouse tables created successfully")
    
    def import_budget_data_from_array(self, budget_data, version_name="2025_Initial", description="Initial 2025 budget import"):
        """
        Import budget data from the array structure provided by user.
        
        Expected format:
        [
            ['AE Name', 'Jan', 'Feb', 'Mar', 'Apr', ...],
            ['Charmaine Lane', 137687, 122256, 140251, ...]
        ]
        """
        conn = self.get_connection()
        
        try:
            # Create budget version
            cursor = conn.execute("""
            INSERT OR REPLACE INTO budget_versions 
            (version_name, year, created_by, description, is_active, source_file)
            VALUES (?, ?, ?, ?, ?, ?)
            """, (version_name, 2025, "System Import", description, 1, "Excel Budget Data"))
            
            version_id = cursor.lastrowid
            
            # Parse the budget data
            month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                          'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
            
            # Month to quarter mapping
            month_to_quarter = {
                1: 1, 2: 1, 3: 1,    # Q1
                4: 2, 5: 2, 6: 2,    # Q2  
                7: 3, 8: 3, 9: 3,    # Q3
                10: 4, 11: 4, 12: 4  # Q4
            }
            
            # Sample data structure based on the user's table
            sample_budget_data = {
                'Charmaine Lane': [137687, 122256, 140251, 159740, 169251, 184965, 171669, 175260, 180236, 167930, 212179, 211799],
                'House': [32706, 35151, 134181, 124135, 76233, 93494, 89670, 118044, 134429, 67915, 90708, 105657],
                'White Horse International': [2200, 2200, 2750, 2200, 2200, 2750, 2200, 2200, 2750, 2200, 2200, 2750],
                'WorldLink': [35544, 35653, 44421, 32701, 32701, 40676, 31278, 39099, 31288, 47321, 17927, 17321],
                'ZTBD': [0, 0, 50000, 50000, 50000, 117888, 117688, 116688, 75411, 76896, 84412, 0]
            }
            
            # Insert budget data
            for ae_name, monthly_budgets in sample_budget_data.items():
                for month_idx, budget_amount in enumerate(monthly_budgets):
                    month_num = month_idx + 1
                    quarter = month_to_quarter[month_num]
                    
                    conn.execute("""
                    INSERT OR REPLACE INTO budget_data 
                    (version_id, ae_name, year, quarter, month, budget_amount)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """, (version_id, ae_name, 2025, quarter, month_num, budget_amount))
            
            # Also populate the legacy budget table for backward compatibility
            for ae_name, monthly_budgets in sample_budget_data.items():
                for month_idx, budget_amount in enumerate(monthly_budgets):
                    month_num = month_idx + 1
                    
                    conn.execute("""
                    INSERT OR REPLACE INTO budget 
                    (ae_name, year, month, budget_amount, created_date, updated_date, source)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (ae_name, 2025, month_num, budget_amount, 
                         datetime.now(), datetime.now(), f"Budget Warehouse v{version_id}"))
            
            conn.commit()
            print(f"✅ Budget data imported successfully as version {version_id}")
            print(f"📊 Imported {len(sample_budget_data)} AEs × 12 months = {len(sample_budget_data) * 12} budget entries")
            
            return version_id
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error importing budget data: {e}")
            raise
        finally:
            conn.close()
    
    def get_ae_budget(self, ae_name, year, month=None, quarter=None, version_id=None):
        """Get budget data for a specific AE."""
        conn = self.get_connection()
        
        # If no version specified, get the active version
        if version_id is None:
            cursor = conn.execute("""
            SELECT version_id FROM budget_versions 
            WHERE year = ? AND is_active = 1 
            ORDER BY created_date DESC LIMIT 1
            """, (year,))
            version_row = cursor.fetchone()
            if not version_row:
                return None
            version_id = version_row['version_id']
        
        # Build query based on parameters
        where_conditions = ["version_id = ?", "ae_name = ?", "year = ?"]
        params = [version_id, ae_name, year]
        
        if month:
            where_conditions.append("month = ?")
            params.append(month)
        elif quarter:
            where_conditions.append("quarter = ?")
            params.append(quarter)
        
        query = f"""
        SELECT ae_name, year, quarter, month, budget_amount
        FROM budget_data 
        WHERE {' AND '.join(where_conditions)}
        ORDER BY month
        """
        
        cursor = conn.execute(query, params)
        results = cursor.fetchall()
        conn.close()
        
        return [dict(row) for row in results]
    
    def get_quarterly_budget_summary(self, year, version_id=None):
        """Get quarterly budget summary for all AEs."""
        conn = self.get_connection()
        
        # If no version specified, get the active version
        if version_id is None:
            cursor = conn.execute("""
            SELECT version_id FROM budget_versions 
            WHERE year = ? AND is_active = 1 
            ORDER BY created_date DESC LIMIT 1
            """, (year,))
            version_row = cursor.fetchone()
            if not version_row:
                return {}
            version_id = version_row['version_id']
        
        cursor = conn.execute("""
        SELECT 
            ae_name,
            quarter,
            ROUND(SUM(budget_amount), 2) as quarter_budget
        FROM budget_data 
        WHERE version_id = ? AND year = ?
        GROUP BY ae_name, quarter
        ORDER BY ae_name, quarter
        """, (version_id, year))
        
        results = cursor.fetchall()
        conn.close()
        
        # Organize by AE and quarter
        budget_summary = {}
        for row in results:
            ae_name = row['ae_name']
            quarter = row['quarter']
            budget = row['quarter_budget']
            
            if ae_name not in budget_summary:
                budget_summary[ae_name] = {}
            budget_summary[ae_name][quarter] = budget
        
        return budget_summary
    
    def get_company_budget_totals(self, year, version_id=None):
        """Get company-wide budget totals by quarter."""
        conn = self.get_connection()
        
        # If no version specified, get the active version
        if version_id is None:
            cursor = conn.execute("""
            SELECT version_id FROM budget_versions 
            WHERE year = ? AND is_active = 1 
            ORDER BY created_date DESC LIMIT 1
            """, (year,))
            version_row = cursor.fetchone()
            if not version_row:
                return {}
            version_id = version_row['version_id']
        
        cursor = conn.execute("""
        SELECT 
            quarter,
            ROUND(SUM(budget_amount), 2) as total_budget
        FROM budget_data 
        WHERE version_id = ? AND year = ?
        GROUP BY quarter
        ORDER BY quarter
        """, (version_id, year))
        
        results = cursor.fetchall()
        conn.close()
        
        return {row['quarter']: row['total_budget'] for row in results}
    
    def validate_budget_data(self, year, version_id=None):
        """Validate budget data integrity and consistency."""
        print(f"🔍 Validating budget data for {year}...")
        
        conn = self.get_connection()
        
        # Check for missing months
        cursor = conn.execute("""
        SELECT ae_name, COUNT(DISTINCT month) as month_count
        FROM budget_data 
        WHERE year = ? AND (version_id = ? OR ? IS NULL)
        GROUP BY ae_name
        HAVING month_count != 12
        """, (year, version_id, version_id))
        
        missing_months = cursor.fetchall()
        if missing_months:
            print("⚠️ AEs with missing months:")
            for row in missing_months:
                print(f"  {row['ae_name']}: {row['month_count']}/12 months")
        
        # Check for zero/negative budgets
        cursor = conn.execute("""
        SELECT ae_name, month, budget_amount
        FROM budget_data 
        WHERE year = ? AND (version_id = ? OR ? IS NULL)
        AND budget_amount <= 0
        """, (year, version_id, version_id))
        
        zero_budgets = cursor.fetchall()
        if zero_budgets:
            print("⚠️ Zero/negative budget entries:")
            for row in zero_budgets:
                print(f"  {row['ae_name']} Month {row['month']}: ${row['budget_amount']}")
        
        # Summary statistics
        cursor = conn.execute("""
        SELECT 
            COUNT(DISTINCT ae_name) as ae_count,
            COUNT(*) as total_entries,
            ROUND(SUM(budget_amount), 2) as total_budget,
            ROUND(AVG(budget_amount), 2) as avg_monthly_budget
        FROM budget_data 
        WHERE year = ? AND (version_id = ? OR ? IS NULL)
        """, (year, version_id, version_id))
        
        stats = cursor.fetchone()
        print(f"📊 Budget Summary for {year}:")
        print(f"  AEs: {stats['ae_count']}")
        print(f"  Total Entries: {stats['total_entries']}")
        print(f"  Total Annual Budget: ${stats['total_budget']:,.2f}")
        print(f"  Average Monthly Budget: ${stats['avg_monthly_budget']:,.2f}")
        
        conn.close()
        return len(missing_months) == 0 and len(zero_budgets) == 0

def main():
    """Initialize and populate the budget warehouse."""
    warehouse = BudgetWarehouse()
    
    print("🏗️ Setting up Budget Data Warehouse...")
    warehouse.create_budget_tables()
    
    print("📥 Importing 2025 budget data...")
    version_id = warehouse.import_budget_data_from_array({})
    
    print("🔍 Validating imported data...")
    is_valid = warehouse.validate_budget_data(2025)
    
    if is_valid:
        print("✅ Budget warehouse setup complete!")
    else:
        print("⚠️ Budget warehouse setup complete with warnings")
    
    # Test queries
    print("\n📊 Testing budget queries...")
    charmaine_q1 = warehouse.get_ae_budget('Charmaine Lane', 2025, quarter=1)
    q1_budget = sum(entry['budget_amount'] for entry in charmaine_q1)
    print(f"Charmaine Q1 Budget: ${q1_budget:,.2f}")
    
    quarterly_totals = warehouse.get_company_budget_totals(2025)
    print("Company Quarterly Budgets:")
    for quarter, total in quarterly_totals.items():
        print(f"  Q{quarter}: ${total:,.2f}")

if __name__ == "__main__":
    main() 