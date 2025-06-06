#!/usr/bin/env python3
"""
Manual Customer-Sector Assignment Tool - Step 2A
Interactive tool for efficiently assigning customers to sectors with smart suggestions,
bulk operations, and comprehensive management features.
"""

import sys
import sqlite3
import re
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import json

class CustomerSectorAssignmentTool:
    """Interactive tool for manual customer-sector assignments."""
    
    def __init__(self, db_path: str):
        """Initialize the assignment tool."""
        self.db_path = db_path
        self.db_connection = sqlite3.connect(db_path)
        self.db_connection.row_factory = sqlite3.Row
        
        # Smart suggestion patterns (more conservative than auto-assignment)
        self.suggestion_patterns = {
            'AUTO': ['toyota', 'ford', 'honda', 'bmw', 'mercedes', 'nissan', 'lexus', 'dealer', 'automotive'],
            'HEALTH': ['health', 'medical', 'hospital', 'clinic', 'care', 'medicare', 'dental', 'pharmacy'],
            'FIN': ['bank', 'credit', 'loan', 'finance', 'insurance', 'investment', 'mortgage'],
            'NPO': ['foundation', 'charity', 'nonprofit', 'food bank', 'community', 'church'],
            'EDU': ['school', 'university', 'college', 'education', 'academy', 'learning'],
            'GOV': ['city', 'county', 'state', 'government', 'municipal', 'department', 'office'],
            'RETAIL': ['store', 'shop', 'retail', 'mall', 'supermarket', 'grocery'],
            'TECH': ['technology', 'software', 'tech', 'digital', 'computer', 'internet'],
            'CASINO': ['casino', 'gaming', 'resort', 'casino resort', 'gaming resort', 'poker', 'slots'],
        }
        
        self.session_assignments = []  # Track assignments made in this session
    
    def get_sector_mappings(self) -> Dict[str, Dict]:
        """Get sector information."""
        cursor = self.db_connection.execute("""
            SELECT sector_id, sector_code, sector_name, sector_group 
            FROM sectors 
            WHERE is_active = 1 
            ORDER BY sector_code
        """)
        
        sectors = {}
        for row in cursor.fetchall():
            sectors[row['sector_code']] = {
                'id': row['sector_id'],
                'name': row['sector_name'],
                'group': row['sector_group']
            }
        
        return sectors
    
    def get_unassigned_customers(self, limit: int = None, search_filter: str = None) -> List[Dict]:
        """Get unassigned customers with optional filtering."""
        query = """
            SELECT customer_id, normalized_name, created_date, customer_type
            FROM customers 
            WHERE sector_id IS NULL AND is_active = 1
        """
        params = []
        
        if search_filter:
            query += " AND LOWER(normalized_name) LIKE ?"
            params.append(f"%{search_filter.lower()}%")
        
        query += " ORDER BY normalized_name"
        
        if limit:
            query += " LIMIT ?"
            params.append(limit)
        
        cursor = self.db_connection.execute(query, params)
        
        customers = []
        for row in cursor.fetchall():
            customers.append({
                'id': row['customer_id'],
                'name': row['normalized_name'],
                'created_date': row['created_date'],
                'type': row['customer_type']
            })
        
        return customers
    
    def suggest_sector(self, customer_name: str) -> List[Tuple[str, int, List[str]]]:
        """
        Suggest sectors for a customer based on name patterns.
        
        Returns:
            List of (sector_code, confidence_score, matched_keywords) tuples
        """
        customer_lower = customer_name.lower()
        suggestions = []
        
        for sector_code, keywords in self.suggestion_patterns.items():
            matched_keywords = [kw for kw in keywords if kw in customer_lower]
            
            if matched_keywords:
                # Calculate confidence score
                confidence = len(matched_keywords) * 10
                
                # Boost confidence for exact brand matches
                brand_keywords = ['toyota', 'ford', 'honda', 'bmw', 'mercedes', 'hospital', 'university']
                if any(brand in customer_lower for brand in brand_keywords):
                    confidence += 20
                
                suggestions.append((sector_code, confidence, matched_keywords))
        
        # Sort by confidence score
        suggestions.sort(key=lambda x: x[1], reverse=True)
        return suggestions[:3]  # Top 3 suggestions
    
    def assign_customer(self, customer_id: int, sector_code: str, assigned_by: str = "manual") -> bool:
        """Assign a customer to a sector."""
        sectors = self.get_sector_mappings()
        
        if sector_code not in sectors:
            print(f"❌ Invalid sector code: {sector_code}")
            return False
        
        sector_id = sectors[sector_code]['id']
        
        try:
            cursor = self.db_connection.execute("""
                UPDATE customers 
                SET sector_id = ?, updated_date = CURRENT_TIMESTAMP
                WHERE customer_id = ?
            """, (sector_id, customer_id))
            
            self.db_connection.commit()
            
            if cursor.rowcount == 1:
                # Track assignment in session
                self.session_assignments.append({
                    'customer_id': customer_id,
                    'sector_code': sector_code,
                    'assigned_by': assigned_by,
                    'timestamp': datetime.now().isoformat()
                })
                return True
            else:
                print(f"❌ Customer {customer_id} not found")
                return False
                
        except Exception as e:
            print(f"❌ Error assigning customer: {e}")
            return False
    
    def bulk_assign(self, customer_ids: List[int], sector_code: str) -> Tuple[int, int]:
        """
        Bulk assign multiple customers to a sector.
        
        Returns:
            Tuple of (successful_assignments, failed_assignments)
        """
        sectors = self.get_sector_mappings()
        
        if sector_code not in sectors:
            print(f"❌ Invalid sector code: {sector_code}")
            return 0, len(customer_ids)
        
        sector_id = sectors[sector_code]['id']
        successful = 0
        failed = 0
        
        for customer_id in customer_ids:
            try:
                cursor = self.db_connection.execute("""
                    UPDATE customers 
                    SET sector_id = ?, updated_date = CURRENT_TIMESTAMP
                    WHERE customer_id = ? AND sector_id IS NULL
                """, (sector_id, customer_id))
                
                if cursor.rowcount == 1:
                    successful += 1
                    self.session_assignments.append({
                        'customer_id': customer_id,
                        'sector_code': sector_code,
                        'assigned_by': 'bulk',
                        'timestamp': datetime.now().isoformat()
                    })
                else:
                    failed += 1
                    
            except Exception as e:
                print(f"❌ Error assigning customer {customer_id}: {e}")
                failed += 1
        
        self.db_connection.commit()
        return successful, failed
    
    def undo_last_assignment(self) -> bool:
        """Undo the last assignment made in this session."""
        if not self.session_assignments:
            print("❌ No assignments to undo")
            return False
        
        last_assignment = self.session_assignments.pop()
        customer_id = last_assignment['customer_id']
        
        try:
            cursor = self.db_connection.execute("""
                UPDATE customers 
                SET sector_id = NULL, updated_date = CURRENT_TIMESTAMP
                WHERE customer_id = ?
            """, (customer_id,))
            
            self.db_connection.commit()
            
            if cursor.rowcount == 1:
                print(f"✅ Undone assignment for customer {customer_id}")
                return True
            else:
                print(f"❌ Failed to undo assignment for customer {customer_id}")
                return False
                
        except Exception as e:
            print(f"❌ Error undoing assignment: {e}")
            return False
    
    def get_assignment_stats(self) -> Dict:
        """Get current assignment statistics."""
        cursor = self.db_connection.execute("""
            SELECT 
                COUNT(*) as total_customers,
                COUNT(sector_id) as assigned_customers,
                COUNT(*) - COUNT(sector_id) as unassigned_customers
            FROM customers 
            WHERE is_active = 1
        """)
        
        stats = dict(cursor.fetchone())
        stats['assignment_rate'] = (stats['assigned_customers'] / stats['total_customers'] * 100) if stats['total_customers'] > 0 else 0
        stats['session_assignments'] = len(self.session_assignments)
        
        return stats
    
    def display_customer_details(self, customer: Dict, show_suggestions: bool = True):
        """Display detailed customer information."""
        print(f"\n📋 Customer Details:")
        print(f"  ID: {customer['id']}")
        print(f"  Name: {customer['name']}")
        print(f"  Type: {customer['type'] or 'Not specified'}")
        print(f"  Created: {customer['created_date']}")
        
        if show_suggestions:
            suggestions = self.suggest_sector(customer['name'])
            if suggestions:
                print(f"  💡 Suggested sectors:")
                for i, (sector_code, confidence, keywords) in enumerate(suggestions, 1):
                    sectors = self.get_sector_mappings()
                    sector_name = sectors[sector_code]['name']
                    print(f"    {i}. {sector_code} ({sector_name}) - {confidence}% confidence")
                    print(f"       Keywords: {', '.join(keywords[:3])}")
            else:
                print(f"  💡 No sector suggestions available")
    
    def interactive_assignment_mode(self):
        """Interactive mode for assigning customers one by one."""
        print("🎯 Interactive Assignment Mode")
        print("=" * 50)
        print("📋 Commands:")
        print("  [1-99] - Assign to numbered sector")
        print("  s      - Skip this customer")
        print("  u      - Undo last assignment")
        print("  q      - Quit interactive mode")
        print("  h      - Show help")
        
        customers = self.get_unassigned_customers()
        sectors = self.get_sector_mappings()
        
        if not customers:
            print("✅ No unassigned customers found!")
            return
        
        # Create ordered sector list (ensure consistent ordering)
        sector_list = sorted(sectors.items())
        
        print(f"\n📊 Found {len(customers)} unassigned customers")
        print(f"🎯 All Available Sectors:")
        for i, (code, info) in enumerate(sector_list, 1):
            print(f"  {i:2}. {code:<8} - {info['name']}")
        
        for i, customer in enumerate(customers):
            print(f"\n{'='*70}")
            print(f"👤 Customer {i+1}/{len(customers)}")
            
            self.display_customer_details(customer, show_suggestions=True)
            
            # Show sector options with numbers
            print(f"\n🎯 Sector Options:")
            for j, (code, info) in enumerate(sector_list, 1):
                print(f"  {j:2}. {code:<8} - {info['name']}")
            
            while True:
                command = input(f"\n➤ Action [1-{len(sector_list)}/s/u/q/h]: ").strip().lower()
                
                if command == 'q':
                    print("👋 Exiting interactive mode")
                    return
                elif command == 'h':
                    print("\n📋 Commands:")
                    print(f"  1-{len(sector_list)} - Assign to numbered sector")
                    print("  s     - Skip this customer")
                    print("  u     - Undo last assignment")
                    print("  q     - Quit")
                    print("  h     - Show this help")
                elif command == 'u':
                    self.undo_last_assignment()
                elif command == 's':
                    print("⏭️  Skipping customer...")
                    break
                elif command.isdigit():
                    sector_num = int(command)
                    if 1 <= sector_num <= len(sector_list):
                        sector_index = sector_num - 1
                        sector_code = sector_list[sector_index][0]
                        sector_name = sector_list[sector_index][1]['name']
                        
                        if self.assign_customer(customer['id'], sector_code):
                            print(f"✅ Assigned {customer['name']} to {sector_code} ({sector_name})")
                            break
                        else:
                            print("❌ Assignment failed")
                    else:
                        print(f"❌ Invalid sector number: {command} (valid range: 1-{len(sector_list)})")
                else:
                    print(f"❌ Invalid command. Use 1-{len(sector_list)}, s, u, q, or h")
        
        # Show final stats
        stats = self.get_assignment_stats()
        print(f"\n📊 Session Complete!")
        print(f"  Total customers: {stats['total_customers']}")
        print(f"  Assigned: {stats['assigned_customers']} ({stats['assignment_rate']:.1f}%)")
        print(f"  Session assignments: {stats['session_assignments']}")
        print(f"🎉 Great work!")
    
    def _show_all_sectors(self, sector_list):
        """Show all available sectors with numbers."""
        print(f"\n🎯 All Available Sectors:")
        print(f"{'Num':<4} {'Code':<10} {'Name'}")
        print(f"{'-'*4} {'-'*10} {'-'*30}")
        
        for i, (code, info) in enumerate(sector_list, 1):
            print(f"{i:<4} {code:<10} {info['name']}")
    
    def _parse_sector_selection(self, choice: str, sector_list) -> Optional[Tuple[str, str]]:
        """Parse user sector selection (number or code)."""
        choice = choice.strip().upper()
        
        # Try as number first
        if choice.isdigit():
            sector_index = int(choice) - 1
            if 0 <= sector_index < len(sector_list):
                code = sector_list[sector_index][0]
                name = sector_list[sector_index][1]['name']
                return (code, name)
        
        # Try as sector code
        for code, info in sector_list:
            if code == choice:
                return (code, info['name'])
        
        print(f"❌ Invalid selection: {choice}")
        return None
    
    def bulk_assignment_mode(self):
        """Bulk assignment mode for handling multiple customers at once."""
        print("📦 Bulk Assignment Mode")
        print("=" * 40)
        
        while True:
            print(f"\nBulk Assignment Options:")
            print(f"1. Assign by name pattern")
            print(f"2. Assign by customer IDs")
            print(f"3. Show unassigned customers")
            print(f"4. Return to main menu")
            
            choice = input("Select option [1-4]: ").strip()
            
            if choice == '1':
                self._bulk_assign_by_pattern()
            elif choice == '2':
                self._bulk_assign_by_ids()
            elif choice == '3':
                self._show_unassigned_customers()
            elif choice == '4':
                break
            else:
                print("Please select 1-4")
    
    def _bulk_assign_by_pattern(self):
        """Bulk assign customers matching a name pattern."""
        pattern = input("Enter name pattern (e.g., 'toyota', 'hospital', 'casino'): ").strip()
        if not pattern:
            return
        
        customers = self.get_unassigned_customers(search_filter=pattern)
        
        if not customers:
            print(f"No unassigned customers found matching '{pattern}'")
            return
        
        print(f"\nFound {len(customers)} customers matching '{pattern}':")
        for customer in customers[:10]:  # Show first 10
            print(f"  - {customer['name']}")
        
        if len(customers) > 10:
            print(f"  ... and {len(customers) - 10} more")
        
        sectors = self.get_sector_mappings()
        sector_list = list(sectors.items())
        
        print(f"\n🎯 Available sectors:")
        for i, (code, info) in enumerate(sector_list, 1):
            print(f"  {i:2}. {code:<8} - {info['name']}")
        
        choice = input("Select sector number (or 'cancel'): ").strip()
        
        if choice.lower() == 'cancel':
            return
        
        try:
            sector_index = int(choice) - 1
            if 0 <= sector_index < len(sector_list):
                sector_code = sector_list[sector_index][0]
                sector_name = sector_list[sector_index][1]['name']
            else:
                print(f"❌ Invalid sector number: {choice}")
                return
        except ValueError:
            print(f"❌ Invalid input: {choice}")
            return
        
        print(f"\n⚠️  About to assign {len(customers)} customers to {sector_code} ({sector_name})")
        confirm = input("Confirm bulk assignment? (yes/no): ").strip().lower()
        
        if confirm != 'yes':
            print("❌ Bulk assignment cancelled")
            return
        
        customer_ids = [c['id'] for c in customers]
        successful, failed = self.bulk_assign(customer_ids, sector_code)
        
        print(f"✅ Bulk assignment completed:")
        print(f"  Successful: {successful}")
        print(f"  Failed: {failed}")
        print(f"  Assigned to: {sector_code} ({sector_name})")
    
    def _bulk_assign_by_ids(self):
        """Bulk assign customers by specific IDs."""
        ids_input = input("Enter customer IDs (comma-separated): ").strip()
        if not ids_input:
            return
        
        try:
            customer_ids = [int(id.strip()) for id in ids_input.split(',')]
        except ValueError:
            print("❌ Invalid customer IDs format")
            return
        
        sectors = self.get_sector_mappings()
        print(f"\nAvailable sectors:")
        for code, info in sectors.items():
            print(f"  {code} - {info['name']}")
        
        sector_code = input("Assign to sector: ").strip().upper()
        
        if sector_code not in sectors:
            print(f"❌ Invalid sector code: {sector_code}")
            return
        
        successful, failed = self.bulk_assign(customer_ids, sector_code)
        
        print(f"✅ Bulk assignment completed:")
        print(f"  Successful: {successful}")
        print(f"  Failed: {failed}")
    
    def _show_unassigned_customers(self):
        """Show list of unassigned customers."""
        limit = int(input("Number of customers to show (default 20): ").strip() or "20")
        customers = self.get_unassigned_customers(limit=limit)
        
        if not customers:
            print("✅ No unassigned customers found!")
            return
        
        print(f"\nUnassigned Customers (showing {len(customers)}):")
        for customer in customers:
            suggestions = self.suggest_sector(customer['name'])
            suggested = suggestions[0][0] if suggestions else "None"
            print(f"  {customer['id']:<4} - {customer['name']:<40} (Suggested: {suggested})")
    
    def main_menu(self):
        """Main menu for the assignment tool."""
        while True:
            stats = self.get_assignment_stats()
            
            print(f"\n{'='*60}")
            print(f"🎯 Customer-Sector Assignment Tool")
            print(f"{'='*60}")
            print(f"📊 Current Status:")
            print(f"  Total customers: {stats['total_customers']}")
            print(f"  Assigned: {stats['assigned_customers']} ({stats['assignment_rate']:.1f}%)")
            print(f"  Unassigned: {stats['unassigned_customers']}")
            print(f"  Session assignments: {stats['session_assignments']}")
            
            print(f"\nOptions:")
            print(f"1. Interactive assignment (one-by-one)")
            print(f"2. Bulk assignment")
            print(f"3. Search customers")
            print(f"4. Show statistics")
            print(f"5. Export session data")
            print(f"6. Undo last assignment")
            print(f"7. Exit")
            
            choice = input("Select option [1-7]: ").strip()
            
            if choice == '1':
                self.interactive_assignment_mode()
            elif choice == '2':
                self.bulk_assignment_mode()
            elif choice == '3':
                self._search_customers()
            elif choice == '4':
                self._show_detailed_stats()
            elif choice == '5':
                self._export_session_data()
            elif choice == '6':
                self.undo_last_assignment()
            elif choice == '7':
                print("👋 Goodbye!")
                break
            else:
                print("Please select 1-7")
    
    def _search_customers(self):
        """Search and filter customers."""
        search_term = input("Enter search term: ").strip()
        if not search_term:
            return
        
        customers = self.get_unassigned_customers(search_filter=search_term)
        
        if not customers:
            print(f"No customers found matching '{search_term}'")
            return
        
        print(f"\nFound {len(customers)} customers matching '{search_term}':")
        for customer in customers:
            suggestions = self.suggest_sector(customer['name'])
            suggested = suggestions[0][0] if suggestions else "None"
            print(f"  {customer['id']:<4} - {customer['name']:<40} (Suggested: {suggested})")
    
    def _show_detailed_stats(self):
        """Show detailed assignment statistics."""
        cursor = self.db_connection.execute("""
            SELECT 
                s.sector_code,
                s.sector_name,
                COUNT(c.customer_id) as customer_count
            FROM sectors s
            LEFT JOIN customers c ON s.sector_id = c.sector_id
            WHERE s.is_active = 1
            GROUP BY s.sector_id, s.sector_code, s.sector_name
            ORDER BY customer_count DESC
        """)
        
        print(f"\n📊 Detailed Statistics:")
        print(f"{'Sector':<8} {'Name':<20} {'Customers':<12}")
        print(f"{'-'*8} {'-'*20} {'-'*12}")
        
        for row in cursor.fetchall():
            print(f"{row['sector_code']:<8} {row['sector_name']:<20} {row['customer_count']:<12}")
    
    def _export_session_data(self):
        """Export session assignment data."""
        if not self.session_assignments:
            print("No assignments made in this session")
            return
        
        filename = f"assignment_session_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        try:
            with open(filename, 'w') as f:
                json.dump(self.session_assignments, f, indent=2)
            
            print(f"✅ Session data exported to: {filename}")
            print(f"   {len(self.session_assignments)} assignments saved")
            
        except Exception as e:
            print(f"❌ Export failed: {e}")
    
    def close(self):
        """Close database connection."""
        if self.db_connection:
            self.db_connection.close()

def main():
    """Main function for the assignment tool."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Manual Customer-Sector Assignment Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/manual_assignment_tool.py --db-path data/database/test.db
        """
    )
    
    parser.add_argument("--db-path", default="data/database/production.db", help="Database path")
    
    args = parser.parse_args()
    
    # Check database exists
    if not Path(args.db_path).exists():
        print(f"❌ Database not found: {args.db_path}")
        sys.exit(1)
    
    try:
        tool = CustomerSectorAssignmentTool(args.db_path)
        tool.main_menu()
        
    except KeyboardInterrupt:
        print(f"\n👋 Goodbye!")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        try:
            tool.close()
        except:
            pass

if __name__ == "__main__":
    main()