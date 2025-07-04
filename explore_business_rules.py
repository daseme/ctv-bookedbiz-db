#!/usr/bin/env python3
"""
Data Exploration Script for Language Block Business Rules
=========================================================

This script explores the database to understand patterns before implementing
business rules for language block assignment.

Usage:
    python explore_business_rules.py
    python explore_business_rules.py --query worldlink
    python explore_business_rules.py --query government  
    python explore_business_rules.py --query sectors
"""

import sqlite3
import argparse
import sys
from typing import List, Dict, Any
from datetime import datetime


class DataExplorer:
    def __init__(self, db_path: str = "./data/database/production.db"):
        self.db_path = db_path
        self.conn = None
        
    def connect(self):
        """Connect to the database"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            print(f"✅ Connected to database: {self.db_path}")
            return True
        except Exception as e:
            print(f"❌ Failed to connect to database: {e}")
            return False
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
    
    def execute_query(self, query: str, description: str = "") -> List[Dict[str, Any]]:
        """Execute a query and return results"""
        if not self.conn:
            print("❌ No database connection")
            return []
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            
            # Get results
            rows = cursor.fetchall()
            
            # Convert to list of dictionaries
            results = []
            for row in rows:
                results.append(dict(zip(columns, row)))
            
            return results
            
        except Exception as e:
            print(f"❌ Error executing query: {e}")
            return []
    
    def print_results(self, results: List[Dict[str, Any]], title: str, max_rows: int = 20):
        """Print query results in a formatted table"""
        if not results:
            print(f"\n📊 {title}")
            print("   No results found")
            return
        
        print(f"\n📊 {title}")
        print("=" * len(title))
        
        # Get column names and calculate widths
        columns = list(results[0].keys())
        col_widths = {}
        
        for col in columns:
            max_width = len(col)
            for row in results[:max_rows]:
                value_str = str(row[col]) if row[col] is not None else "NULL"
                max_width = max(max_width, len(value_str))
            col_widths[col] = min(max_width, 30)  # Cap at 30 chars
        
        # Print header
        header = " | ".join(col.ljust(col_widths[col]) for col in columns)
        print(header)
        print("-" * len(header))
        
        # Print rows
        for i, row in enumerate(results):
            if i >= max_rows:
                print(f"... ({len(results) - max_rows} more rows)")
                break
                
            row_str = " | ".join(
                str(row[col] if row[col] is not None else "NULL")[:col_widths[col]].ljust(col_widths[col])
                for col in columns
            )
            print(row_str)
        
        print(f"\nTotal rows: {len(results)}")
    
    def explore_worldlink_patterns(self):
        """Explore WorldLink infomercial patterns"""
        print("\n🔍 EXPLORING WORLDLINK PATTERNS")
        print("=" * 50)
        
        # Basic WorldLink stats
        query = """
        SELECT 
            COUNT(*) as total_spots,
            COUNT(DISTINCT bill_code) as unique_bill_codes,
            COUNT(DISTINCT customer_id) as unique_customers,
            ROUND(AVG(gross_rate), 2) as avg_revenue,
            ROUND(MIN(gross_rate), 2) as min_revenue,
            ROUND(MAX(gross_rate), 2) as max_revenue,
            COUNT(DISTINCT time_in || '-' || time_out) as unique_time_patterns,
            COUNT(DISTINCT CAST((strftime('%s', time_out) - strftime('%s', time_in)) / 60 AS INTEGER)) as unique_durations
        FROM spots 
        WHERE bill_code LIKE '%WorldLink%'
        """
        
        results = self.execute_query(query)
        self.print_results(results, "WorldLink Basic Statistics")
        
        # WorldLink customer patterns
        query = """
        SELECT 
            c.normalized_name,
            s.sector_name,
            COUNT(*) as spot_count,
            ROUND(AVG(sp.gross_rate), 2) as avg_revenue,
            COUNT(DISTINCT sp.bill_code) as unique_campaigns
        FROM spots sp
        JOIN customers c ON sp.customer_id = c.customer_id
        LEFT JOIN sectors s ON c.sector_id = s.sector_id
        WHERE sp.bill_code LIKE '%WorldLink%'
        GROUP BY c.normalized_name, s.sector_name
        ORDER BY spot_count DESC
        """
        
        results = self.execute_query(query)
        self.print_results(results, "WorldLink Customers by Sector")
        
        # WorldLink time patterns
        query = """
        SELECT 
            time_in,
            time_out,
            CAST((strftime('%s', time_out) - strftime('%s', time_in)) / 60 AS INTEGER) as duration_minutes,
            COUNT(*) as spot_count,
            ROUND(AVG(gross_rate), 2) as avg_revenue
        FROM spots 
        WHERE bill_code LIKE '%WorldLink%'
        GROUP BY time_in, time_out
        ORDER BY spot_count DESC
        LIMIT 10
        """
        
        results = self.execute_query(query)
        self.print_results(results, "WorldLink Time Patterns (Top 10)")
    
    def explore_government_patterns(self):
        """Explore government agency patterns"""
        print("\n🔍 EXPLORING GOVERNMENT PATTERNS")
        print("=" * 50)
        
        # Government-related customers
        query = """
        SELECT 
            c.normalized_name,
            s.sector_name,
            COUNT(*) as spot_count,
            ROUND(AVG(sp.gross_rate), 2) as avg_revenue,
            COUNT(DISTINCT sp.time_in || '-' || sp.time_out) as unique_time_patterns
        FROM spots sp
        JOIN customers c ON sp.customer_id = c.customer_id
        LEFT JOIN sectors s ON c.sector_id = s.sector_id
        WHERE c.normalized_name LIKE '%Department%'
           OR c.normalized_name LIKE '%Cal Fire%'
           OR c.normalized_name LIKE '%California%'
           OR c.normalized_name LIKE '%CMS%'
           OR c.normalized_name LIKE '%County%'
           OR c.normalized_name LIKE '%State%'
           OR c.normalized_name LIKE '%Government%'
           OR c.normalized_name LIKE '%Public%'
           OR c.normalized_name LIKE '%Agency%'
        GROUP BY c.normalized_name, s.sector_name
        ORDER BY spot_count DESC
        """
        
        results = self.execute_query(query)
        self.print_results(results, "Government-Related Customers")
        
        # Roadblock patterns (6:00 AM - 11:59 PM)
        query = """
        SELECT 
            c.normalized_name,
            s.sector_name,
            COUNT(*) as roadblock_spot_count,
            ROUND(AVG(sp.gross_rate), 2) as avg_revenue
        FROM spots sp
        JOIN customers c ON sp.customer_id = c.customer_id
        LEFT JOIN sectors s ON c.sector_id = s.sector_id
        WHERE sp.time_in = '06:00:00' 
          AND sp.time_out = '23:59:00'
        GROUP BY c.normalized_name, s.sector_name
        ORDER BY roadblock_spot_count DESC
        """
        
        results = self.execute_query(query)
        self.print_results(results, "Roadblock Pattern Users (6:00 AM - 11:59 PM)")
    
    def explore_intersection_patterns(self):
        """Explore Intersection (political) patterns"""
        print("\n🔍 EXPLORING INTERSECTION PATTERNS")
        print("=" * 50)
        
        # Basic Intersection stats
        query = """
        SELECT 
            COUNT(*) as total_spots,
            COUNT(DISTINCT customer_id) as unique_customers,
            ROUND(AVG(gross_rate), 2) as avg_revenue,
            COUNT(DISTINCT time_in || '-' || time_out) as unique_time_patterns
        FROM spots 
        WHERE bill_code LIKE '%Intersection%'
        """
        
        results = self.execute_query(query)
        self.print_results(results, "Intersection Basic Statistics")
        
        # Intersection customers
        query = """
        SELECT 
            c.normalized_name,
            s.sector_name,
            COUNT(*) as spot_count,
            ROUND(AVG(sp.gross_rate), 2) as avg_revenue
        FROM spots sp
        JOIN customers c ON sp.customer_id = c.customer_id
        LEFT JOIN sectors s ON c.sector_id = s.sector_id
        WHERE sp.bill_code LIKE '%Intersection%'
        GROUP BY c.normalized_name, s.sector_name
        ORDER BY spot_count DESC
        """
        
        results = self.execute_query(query)
        self.print_results(results, "Intersection Customers by Sector")
    
    def explore_sectors(self):
        """Explore sector classifications"""
        print("\n🔍 EXPLORING SECTOR CLASSIFICATIONS")
        print("=" * 50)
        
        query = """
        SELECT 
            s.sector_code,
            s.sector_name,
            s.sector_group,
            COUNT(DISTINCT c.customer_id) as unique_customers,
            COUNT(sp.spot_id) as total_spots,
            ROUND(AVG(sp.gross_rate), 2) as avg_revenue
        FROM sectors s
        LEFT JOIN customers c ON s.sector_id = c.sector_id
        LEFT JOIN spots sp ON c.customer_id = sp.customer_id
        WHERE s.is_active = 1
        GROUP BY s.sector_code, s.sector_name, s.sector_group
        ORDER BY total_spots DESC
        """
        
        results = self.execute_query(query)
        self.print_results(results, "Sector Analysis")
    
    def explore_duration_patterns(self):
        """Explore unusual duration patterns"""
        print("\n🔍 EXPLORING DURATION PATTERNS")
        print("=" * 50)
        
        query = """
        SELECT 
            CAST((strftime('%s', time_out) - strftime('%s', time_in)) / 60 AS INTEGER) as duration_minutes,
            COUNT(*) as spot_count,
            ROUND(AVG(gross_rate), 2) as avg_revenue,
            COUNT(DISTINCT customer_id) as unique_customers,
            COUNT(DISTINCT bill_code) as unique_campaigns
        FROM spots 
        WHERE time_in IS NOT NULL 
          AND time_out IS NOT NULL
          AND time_in != time_out
        GROUP BY duration_minutes
        HAVING duration_minutes > 60  -- More than 1 hour
        ORDER BY duration_minutes DESC
        """
        
        results = self.execute_query(query)
        self.print_results(results, "Long Duration Spots (>1 hour)")
    
    def run_all_explorations(self):
        """Run all exploration queries"""
        print(f"\n🚀 STARTING DATA EXPLORATION")
        print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        
        self.explore_sectors()
        self.explore_worldlink_patterns()
        self.explore_government_patterns()
        self.explore_intersection_patterns()
        self.explore_duration_patterns()
        
        print(f"\n✅ EXPLORATION COMPLETE")
        print("=" * 60)


def main():
    """Main CLI interface"""
    parser = argparse.ArgumentParser(description="Explore business rule patterns in the database")
    parser.add_argument("--database", default="./data/database/production.db", help="Database path")
    parser.add_argument("--query", choices=["worldlink", "government", "intersection", "sectors", "duration"], 
                       help="Run specific query type")
    
    args = parser.parse_args()
    
    explorer = DataExplorer(args.database)
    
    if not explorer.connect():
        return 1
    
    try:
        if args.query == "worldlink":
            explorer.explore_worldlink_patterns()
        elif args.query == "government":
            explorer.explore_government_patterns()
        elif args.query == "intersection":
            explorer.explore_intersection_patterns()
        elif args.query == "sectors":
            explorer.explore_sectors()
        elif args.query == "duration":
            explorer.explore_duration_patterns()
        else:
            # Run all explorations
            explorer.run_all_explorations()
            
        return 0
        
    except Exception as e:
        print(f"❌ Error during exploration: {e}")
        return 1
        
    finally:
        explorer.close()


if __name__ == "__main__":
    exit(main())