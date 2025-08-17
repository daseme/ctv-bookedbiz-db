# src/services/budget_service.py
"""
Hybrid Budget Service - Combines file-based performance with database features
Maintains compatibility with existing code while adding advanced features
"""

import json
import os
import sqlite3
from typing import Dict, Any, Optional, List
from datetime import datetime, date
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class BudgetService:
    """
    Hybrid Budget Service combining file-based performance with database capabilities.
    
    Features:
    - Fast file-based lookups for current operations
    - Database backing for version control and history
    - Backward compatibility with existing API
    - Advanced features for budget management
    """
    
    def __init__(self, data_path: str, db_path: Optional[str] = None):
        """
        Initialize with data directory path and optional database path.
        
        Args:
            data_path: Path to data directory containing JSON files
            db_path: Optional database path for advanced features
        """
        self.data_path = data_path
        self.budget_file = os.path.join(data_path, 'real_budget_data.json')
        self.db_path = db_path
        self._budget_cache = None
        
        # Initialize database if path provided
        if self.db_path:
            self._initialize_database()
    
    def _initialize_database(self):
        """Initialize database tables for advanced budget features."""
        try:
            conn = sqlite3.connect(self.db_path)
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
            
            conn.commit()
            conn.close()
            logger.info("Budget database initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize budget database: {e}")
    
    def _load_budget_data(self) -> Dict[str, Any]:
        """Load budget data from file with caching (maintains existing functionality)."""
        if self._budget_cache is not None:
            return self._budget_cache
            
        try:
            with open(self.budget_file, 'r') as f:
                self._budget_cache = json.load(f)
            return self._budget_cache
        except FileNotFoundError:
            logger.error(f"Budget file not found: {self.budget_file}")
            return {"budget_2025": {}}
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in budget file: {e}")
            return {"budget_2025": {}}
    
    def clear_cache(self):
        """Clear the budget cache to force reload."""
        self._budget_cache = None
    
    # ============================================================================
    # EXISTING API - Maintains backward compatibility
    # ============================================================================
    
    def get_monthly_budget(self, ae_name: str, month: str) -> float:
        """Get budget for specific AE and month (YYYY-MM format) - EXISTING API."""
        budget_data = self._load_budget_data()
        budget_2025 = budget_data.get('budget_2025', {})
        
        # Direct lookup
        if ae_name in budget_2025:
            return budget_2025[ae_name].get(month, 0)
        
        # Handle name variations
        name_mappings = {
            'House': 'House',
            'Charmaine Lane': 'Charmaine Lane', 
            'WorldLink': 'WorldLink',
            'White Horse International': 'White Horse International'
        }
        
        mapped_name = name_mappings.get(ae_name)
        if mapped_name and mapped_name in budget_2025:
            return budget_2025[mapped_name].get(month, 0)
        
        return 0
    
    def get_annual_target(self, ae_name: str) -> float:
        """Get total annual budget target for AE - EXISTING API."""
        budget_data = self._load_budget_data()
        budget_2025 = budget_data.get('budget_2025', {})
        
        if ae_name in budget_2025:
            return sum(budget_2025[ae_name].values())
        
        return 1000000  # Default target
    
    def get_quarterly_budget(self, ae_name: str, quarter: int) -> float:
        """Get quarterly budget total for AE - EXISTING API."""
        quarter_months = {
            1: ['2025-01', '2025-02', '2025-03'],
            2: ['2025-04', '2025-05', '2025-06'], 
            3: ['2025-07', '2025-08', '2025-09'],
            4: ['2025-10', '2025-11', '2025-12']
        }
        
        months = quarter_months.get(quarter, [])
        return sum(self.get_monthly_budget(ae_name, month) for month in months)
    
    # ============================================================================
    # ENHANCED API - New features for budget management
    # ============================================================================
    
    def get_company_budget_totals(self, year: int = 2025) -> Dict[int, float]:
        """Get company-wide budget totals by quarter."""
        if year == 2025:
            # Use file-based data for 2025 (fast path)
            budget_data = self._load_budget_data()
            budget_2025 = budget_data.get('budget_2025', {})
            
            quarterly_totals = {}
            for quarter in range(1, 5):
                total = 0
                for ae_name in budget_2025.keys():
                    total += self.get_quarterly_budget(ae_name, quarter)
                quarterly_totals[quarter] = total
            
            return quarterly_totals
        
        # Use database for other years
        return self._get_company_budget_totals_db(year)
    
    def get_quarterly_budget_summary(self, year: int = 2025) -> Dict[str, Dict[int, float]]:
        """Get quarterly budget summary for all AEs."""
        if year == 2025:
            # Use file-based data for 2025 (fast path)
            budget_data = self._load_budget_data()
            budget_2025 = budget_data.get('budget_2025', {})
            
            summary = {}
            for ae_name in budget_2025.keys():
                summary[ae_name] = {}
                for quarter in range(1, 5):
                    summary[ae_name][quarter] = self.get_quarterly_budget(ae_name, quarter)
            
            return summary
        
        # Use database for other years
        return self._get_quarterly_budget_summary_db(year)
    
    def upload_budget_data(self, year: int, version_name: str, description: str, 
                          budget_data: Dict[str, Dict[int, float]], 
                          created_by: str = "System") -> int:
        """Upload new budget data with version control."""
        if not self.db_path:
            raise ValueError("Database path required for budget uploads")
        
        # Validate input data
        if not budget_data:
            raise ValueError("Budget data cannot be empty")
        
        if year < 2020 or year > 2040:
            raise ValueError("Year must be between 2020 and 2040")
        
        conn = sqlite3.connect(self.db_path)
        
        try:
            # Create budget version
            cursor = conn.execute("""
            INSERT INTO budget_versions 
            (version_name, year, created_by, description, is_active, source_file)
            VALUES (?, ?, ?, ?, ?, ?)
            """, (version_name, year, created_by, description, 1, "Web Interface"))
            
            version_id = cursor.lastrowid
            
            # Month to quarter mapping
            month_to_quarter = {
                1: 1, 2: 1, 3: 1,    # Q1
                4: 2, 5: 2, 6: 2,    # Q2  
                7: 3, 8: 3, 9: 3,    # Q3
                10: 4, 11: 4, 12: 4  # Q4
            }
            
            # Insert budget data
            for ae_name, monthly_budgets in budget_data.items():
                for month, budget_amount in monthly_budgets.items():
                    quarter = month_to_quarter.get(month, 1)
                    
                    conn.execute("""
                    INSERT INTO budget_data 
                    (version_id, ae_name, year, quarter, month, budget_amount)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """, (version_id, ae_name, year, quarter, month, budget_amount))
            
            conn.commit()
            logger.info(f"Budget data uploaded successfully: version_id={version_id}, year={year}")
            
            # If uploading 2025 data, also update the JSON file
            if year == 2025:
                self._sync_to_json_file(budget_data)
            
            return version_id
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error uploading budget data: {e}")
            raise
        finally:
            conn.close()
    
    def _sync_to_json_file(self, budget_data: Dict[str, Dict[int, float]]):
        """Sync database budget data to JSON file for fast access."""
        try:
            # Convert month numbers to YYYY-MM format
            json_data = {"budget_2025": {}}
            
            for ae_name, monthly_budgets in budget_data.items():
                json_data["budget_2025"][ae_name] = {}
                for month, amount in monthly_budgets.items():
                    month_key = f"2025-{month:02d}"
                    json_data["budget_2025"][ae_name][month_key] = amount
            
            # Write to file
            with open(self.budget_file, 'w') as f:
                json.dump(json_data, f, indent=2)
            
            # Clear cache to force reload
            self.clear_cache()
            
            logger.info("Budget data synced to JSON file successfully")
            
        except Exception as e:
            logger.error(f"Error syncing budget data to JSON: {e}")
    
    def _get_company_budget_totals_db(self, year: int) -> Dict[int, float]:
        """Get company budget totals from database."""
        if not self.db_path:
            return {}
        
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        
        try:
            cursor = conn.execute("""
            SELECT 
                quarter,
                ROUND(SUM(budget_amount), 2) as total_budget
            FROM budget_data 
            WHERE year = ?
            GROUP BY quarter
            ORDER BY quarter
            """, (year,))
            
            results = cursor.fetchall()
            return {row['quarter']: row['total_budget'] for row in results}
            
        except Exception as e:
            logger.error(f"Error getting company budget totals from DB: {e}")
            return {}
        finally:
            conn.close()
    
    def _get_quarterly_budget_summary_db(self, year: int) -> Dict[str, Dict[int, float]]:
        """Get quarterly budget summary from database."""
        if not self.db_path:
            return {}
        
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        
        try:
            cursor = conn.execute("""
            SELECT 
                ae_name,
                quarter,
                ROUND(SUM(budget_amount), 2) as quarter_budget
            FROM budget_data 
            WHERE year = ?
            GROUP BY ae_name, quarter
            ORDER BY ae_name, quarter
            """, (year,))
            
            results = cursor.fetchall()
            
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
            
        except Exception as e:
            logger.error(f"Error getting quarterly budget summary from DB: {e}")
            return {}
        finally:
            conn.close()
    
    def get_budget_versions(self, year: int) -> List[Dict[str, Any]]:
        """Get all budget versions for a year."""
        if not self.db_path:
            return []
        
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        
        try:
            cursor = conn.execute("""
            SELECT 
                version_id,
                version_name,
                year,
                created_date,
                created_by,
                description,
                is_active,
                source_file
            FROM budget_versions 
            WHERE year = ?
            ORDER BY created_date DESC
            """, (year,))
            
            results = cursor.fetchall()
            return [dict(row) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting budget versions: {e}")
            return []
        finally:
            conn.close()
    
    def validate_budget_data(self, year: int = 2025) -> Dict[str, Any]:
        """Validate budget data integrity."""
        validation_results = {
            'is_valid': True,
            'warnings': [],
            'errors': [],
            'statistics': {}
        }
        
        try:
            if year == 2025:
                # Validate JSON file data
                budget_data = self._load_budget_data()
                budget_2025 = budget_data.get('budget_2025', {})
                
                if not budget_2025:
                    validation_results['errors'].append("No budget data found for 2025")
                    validation_results['is_valid'] = False
                    return validation_results
                
                # Check for missing months
                for ae_name, monthly_data in budget_2025.items():
                    expected_months = [f"2025-{m:02d}" for m in range(1, 13)]
                    actual_months = list(monthly_data.keys())
                    missing_months = set(expected_months) - set(actual_months)
                    
                    if missing_months:
                        validation_results['warnings'].append(
                            f"{ae_name} missing months: {sorted(missing_months)}"
                        )
                
                # Statistics
                total_aes = len(budget_2025)
                total_budget = sum(sum(ae_data.values()) for ae_data in budget_2025.values())
                avg_ae_budget = total_budget / total_aes if total_aes > 0 else 0
                
                validation_results['statistics'] = {
                    'ae_count': total_aes,
                    'total_budget': total_budget,
                    'avg_ae_budget': avg_ae_budget,
                    'data_source': 'JSON File'
                }
            
            else:
                # Validate database data for other years
                validation_results = self._validate_budget_data_db(year)
            
            return validation_results
            
        except Exception as e:
            logger.error(f"Error validating budget data: {e}")
            return {
                'is_valid': False,
                'error': str(e),
                'warnings': [],
                'errors': [str(e)]
            }
    
    def _validate_budget_data_db(self, year: int) -> Dict[str, Any]:
        """Validate budget data from database."""
        if not self.db_path:
            return {'is_valid': False, 'error': 'No database configured'}
        
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        
        try:
            validation_results = {
                'is_valid': True,
                'warnings': [],
                'errors': [],
                'statistics': {}
            }
            
            # Check for missing months
            cursor = conn.execute("""
            SELECT ae_name, COUNT(DISTINCT month) as month_count
            FROM budget_data 
            WHERE year = ?
            GROUP BY ae_name
            HAVING month_count != 12
            """, (year,))
            
            missing_months = cursor.fetchall()
            for row in missing_months:
                validation_results['warnings'].append(
                    f"{row['ae_name']} has only {row['month_count']}/12 months"
                )
            
            # Summary statistics
            cursor = conn.execute("""
            SELECT 
                COUNT(DISTINCT ae_name) as ae_count,
                COUNT(*) as total_entries,
                ROUND(SUM(budget_amount), 2) as total_budget,
                ROUND(AVG(budget_amount), 2) as avg_monthly_budget
            FROM budget_data 
            WHERE year = ?
            """, (year,))
            
            stats = cursor.fetchone()
            validation_results['statistics'] = dict(stats)
            validation_results['statistics']['data_source'] = 'Database'
            
            return validation_results
            
        except Exception as e:
            logger.error(f"Error validating budget data from DB: {e}")
            return {
                'is_valid': False,
                'error': str(e),
                'warnings': [],
                'errors': [str(e)]
            }
        finally:
            conn.close()