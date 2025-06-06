#!/usr/bin/env python3
"""
Quick Pattern-Based Assignment Tool - Step 1.5
Auto-assigns customers to sectors based on clear name patterns.
This gives immediate value by handling the obvious cases first.
"""

import sys
import sqlite3
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import json

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


class QuickPatternAssigner:
    """Assigns customers to sectors based on name patterns."""
    
    def __init__(self, db_path: str):
        """Initialize with database connection."""
        self.db_path = db_path
        self.db_connection = sqlite3.connect(db_path)
        self.db_connection.row_factory = sqlite3.Row
        
        # More precise and conservative sector keyword mapping
        self.sector_patterns = {
            'AUTO': {
                'keywords': [
                    # Car brands (very specific)
                    'toyota', 'ford', 'honda', 'bmw', 'mercedes', 'nissan', 'hyundai', 
                    'kia', 'lexus', 'acura', 'infiniti', 'cadillac', 'chevrolet', 
                    'buick', 'gmc', 'jeep', 'chrysler', 'dodge', 'volkswagen', 
                    'audi', 'porsche', 'volvo', 'subaru', 'mazda', 'mitsubishi', 'tesla',
                    # Auto-specific terms (must be combined with other indicators)
                    'dealership', 'auto dealer', 'car dealer', 'automotive service',
                    'auto repair', 'car service', 'collision center', 'auto body',
                    'service center', 'auto parts', 'car wash', 'dealer association'
                ],
                'exclusions': [
                    'auto insurance', 'auto loan', 'water', 'agency', 'government', 
                    'county', 'city', 'public', 'care', 'healthcare', 'medicare'
                ],
                'whole_word_only': ['ram'],  # Only match as whole word, not substring
                'confidence': 'high'
            },
            'HEALTH': {
                'keywords': [
                    # Very specific healthcare terms
                    'hospital', 'medical center', 'clinic', 'urgent care', 'emergency room',
                    'family practice', 'pediatrics', 'cardiology', 'orthopedic', 
                    'dermatology', 'radiology', 'pharmacy', 'dental', 'dentist',
                    'physical therapy', 'rehabilitation', 'surgery center',
                    'healthcare', 'medical group', 'physicians', 'doctors',
                    'medicare benefits', 'health plan', 'medical benefits',
                    'healthcare foundation'
                ],
                'exclusions': ['auto', 'automotive', 'car'],
                'whole_word_only': ['care'],  # Only match "care" as whole word, not in "scare"
                'confidence': 'high'
            },
            'FIN': {
                'keywords': [
                    # Very specific financial terms
                    'bank', 'credit union', 'financial services', 'investment',
                    'insurance company', 'insurance agency', 'auto insurance',
                    'life insurance', 'mortgage', 'lending', 'loan',
                    'wealth management', 'financial planning', 'accounting firm',
                    'tax services', 'cpa', 'certified public accountant'
                ],
                'exclusions': ['food bank', 'community', 'foundation'],
                'confidence': 'high'
            },
            'NPO': {
                'keywords': [
                    # Very specific nonprofit indicators
                    'food bank', 'community foundation', 'charity', 'nonprofit',
                    'church', 'religious organization', 'salvation army',
                    'red cross', 'united way', 'ymca', 'ywca', 'habitat for humanity',
                    'community center', 'senior center', 'food pantry',
                    'homeless shelter', 'animal shelter', 'rescue mission'
                ],
                'exclusions': ['bank', 'financial', 'insurance'],
                'confidence': 'high'
            },
            'EDU': {
                'keywords': [
                    # Very specific educational terms
                    'school district', 'elementary school', 'middle school', 'high school',
                    'university', 'college', 'community college', 'academy',
                    'educational services', 'tutoring', 'learning center',
                    'training institute', 'vocational school', 'colleges'
                ],
                'exclusions': ['government', 'public', 'city', 'county'],
                'confidence': 'high'
            },
            'GOV': {
                'keywords': [
                    # Very specific government terms
                    'city of', 'county of', 'state of', 'federal', 
                    'police department', 'fire department', 'city hall',
                    'county office', 'municipal', 'government agency',
                    'public works', 'parks and recreation', 'dmv',
                    'department of', 'bureau of', 'office of',
                    'water agency', 'county water', 'public utility'
                ],
                'exclusions': ['company', 'corp', 'inc', 'llc', 'foundation'],
                'confidence': 'high'
            },
            'RETAIL': {
                'keywords': [
                    # Very specific retail terms (avoiding generic ones)
                    'grocery store', 'supermarket', 'department store',
                    'clothing store', 'furniture store', 'electronics store',
                    'home depot', 'walmart', 'target', 'costco', 'safeway',
                    'shopping center', 'retail chain'
                ],
                'exclusions': ['service', 'agency', 'government'],
                'confidence': 'medium'
            },
            'TECH': {
                'keywords': [
                    # Very specific tech terms
                    'software company', 'technology company', 'tech startup',
                    'software development', 'web development', 'app development',
                    'data analytics', 'cloud services', 'cybersecurity',
                    'it services', 'computer services', 'tech support'
                ],
                'exclusions': ['television network', 'tv network', 'broadcasting'],
                'confidence': 'medium'
            }
        }
        
        # Add a special category for media/entertainment that maps to existing sectors
        self.special_cases = {
            'MEDIA': {
                'keywords': [
                    'television', 'tv network', 'broadcasting', 'radio station',
                    'media company', 'entertainment', 'production company',
                    'cable company', 'satellite tv'
                ],
                'map_to_sector': 'OTHER',
                'confidence': 'high'
            }
        }
        
        self.assignment_results = {
            'timestamp': datetime.now().isoformat(),
            'assignments_made': [],
            'skipped_customers': [],
            'errors': [],
            'summary': {}
        }
    
    def get_sector_mappings(self) -> Dict[str, int]:
        """Get sector code to sector_id mappings."""
        cursor = self.db_connection.execute("""
            SELECT sector_code, sector_id 
            FROM sectors 
            WHERE is_active = 1
        """)
        
        mappings = {}
        for row in cursor.fetchall():
            mappings[row['sector_code']] = row['sector_id']
        
        return mappings
    
    def find_pattern_matches(self, customer_name: str) -> List[Tuple[str, List[str], str]]:
        """
        Find sector patterns in customer name with improved logic and whole-word matching.
        
        Returns:
            List of (sector_code, matched_keywords, confidence) tuples
        """
        customer_lower = customer_name.lower()
        matches = []
        
        # Check special cases first (like media companies)
        for special_category, pattern_info in getattr(self, 'special_cases', {}).items():
            matched_keywords = []
            for keyword in pattern_info['keywords']:
                if self._keyword_matches(customer_lower, keyword, whole_word_only=False):
                    matched_keywords.append(keyword)
            
            if matched_keywords:
                # Map to appropriate sector
                target_sector = pattern_info['map_to_sector']
                matches.append((
                    target_sector,
                    matched_keywords,
                    pattern_info['confidence']
                ))
        
        # Check regular sector patterns
        for sector_code, pattern_info in self.sector_patterns.items():
            matched_keywords = []
            
            # Check keywords with whole-word requirements
            for keyword in pattern_info['keywords']:
                whole_word_only = keyword in pattern_info.get('whole_word_only', [])
                if self._keyword_matches(customer_lower, keyword, whole_word_only):
                    matched_keywords.append(keyword)
            
            # Check exclusions
            exclusions = pattern_info.get('exclusions', [])
            has_exclusions = any(
                self._keyword_matches(customer_lower, exclusion, whole_word_only=False)
                for exclusion in exclusions
            )
            
            if matched_keywords and not has_exclusions:
                matches.append((
                    sector_code,
                    matched_keywords,
                    pattern_info['confidence']
                ))
        
        # Enhanced sorting logic
        matches.sort(key=lambda x: (
            2 if x[2] == 'high' else 1 if x[2] == 'medium' else 0,  # Confidence priority
            len(x[1]),  # Number of keyword matches
            -len(x[0])  # Prefer longer sector codes (more specific)
        ), reverse=True)
        
        return matches
    
    def _keyword_matches(self, text: str, keyword: str, whole_word_only: bool = False) -> bool:
        """
        Check if keyword matches in text, with optional whole-word requirement.
        
        Args:
            text: Text to search in (already lowercase)
            keyword: Keyword to search for (already lowercase)
            whole_word_only: If True, only match whole words
            
        Returns:
            True if keyword matches
        """
        if not whole_word_only:
            return keyword in text
        
        # Whole word matching using word boundaries
        import re
        pattern = r'\b' + re.escape(keyword) + r'\b'
        return bool(re.search(pattern, text))
    
    def get_unassigned_customers(self) -> List[Dict]:
        """Get all customers without sector assignments."""
        cursor = self.db_connection.execute("""
            SELECT customer_id, normalized_name, created_date
            FROM customers 
            WHERE sector_id IS NULL AND is_active = 1
            ORDER BY normalized_name
        """)
        
        customers = []
        for row in cursor.fetchall():
            customers.append({
                'customer_id': row['customer_id'],
                'name': row['normalized_name'],
                'created_date': row['created_date']
            })
        
        return customers
    
    def assign_customer_to_sector(self, customer_id: int, sector_id: int, 
                                 confidence: str, matched_keywords: List[str]) -> bool:
        """Assign a customer to a sector."""
        try:
            cursor = self.db_connection.execute("""
                UPDATE customers 
                SET sector_id = ?, updated_date = CURRENT_TIMESTAMP
                WHERE customer_id = ?
            """, (sector_id, customer_id))
            
            # CRITICAL: Commit the transaction
            self.db_connection.commit()
            
            if cursor.rowcount == 1:
                return True
            else:
                self.assignment_results['errors'].append(
                    f"Customer {customer_id} not found or not updated"
                )
                return False
                
        except Exception as e:
            # Rollback on error
            self.db_connection.rollback()
            self.assignment_results['errors'].append(
                f"Error assigning customer {customer_id}: {str(e)}"
            )
            return False
    
    def run_pattern_assignment(self, dry_run: bool = False, 
                             min_confidence: str = 'high') -> Dict:  # Changed default to 'high'
        """
        Run the pattern-based assignment process.
        
        Args:
            dry_run: If True, don't make actual assignments
            min_confidence: Minimum confidence level ('high' only for now)
        """
        print("🎯 Starting Conservative Pattern-Based Customer Assignment")
        print("=" * 60)
        print("⚠️  Using conservative matching to avoid misassignments")
        
        # Get sector mappings
        sector_mappings = self.get_sector_mappings()
        print(f"📊 Found {len(sector_mappings)} active sectors")
        
        # Get unassigned customers
        unassigned_customers = self.get_unassigned_customers()
        print(f"👥 Found {len(unassigned_customers)} unassigned customers")
        
        if dry_run:
            print("🔍 DRY RUN MODE - No actual assignments will be made")
        print()
        
        # Process each customer
        processed = 0
        assigned = 0
        skipped = 0
        
        # Only use high confidence for now
        confidence_levels = ['high']
        
        for customer in unassigned_customers:
            processed += 1
            customer_id = customer['customer_id']
            customer_name = customer['name']
            
            # Find pattern matches
            matches = self.find_pattern_matches(customer_name)
            
            # Filter by confidence level
            valid_matches = [
                match for match in matches 
                if match[2] in confidence_levels
            ]
            
            if not valid_matches:
                skipped += 1
                self.assignment_results['skipped_customers'].append({
                    'customer_id': customer_id,
                    'name': customer_name,
                    'reason': 'No high-confidence patterns found'
                })
                continue
            
            # Take the best match
            best_match = valid_matches[0]
            sector_code, matched_keywords, confidence = best_match
            
            # Additional validation: Skip if sector doesn't exist
            if sector_code not in sector_mappings:
                skipped += 1
                self.assignment_results['skipped_customers'].append({
                    'customer_id': customer_id,
                    'name': customer_name,
                    'reason': f'Sector {sector_code} not found in database'
                })
                continue
            
            # Additional validation: Skip very generic matches
            if self._is_too_generic(customer_name, matched_keywords):
                skipped += 1
                self.assignment_results['skipped_customers'].append({
                    'customer_id': customer_id,
                    'name': customer_name,
                    'reason': 'Match too generic, requires manual review'
                })
                continue
            
            sector_id = sector_mappings[sector_code]
            
            # Display the assignment
            print(f"  {'[DRY RUN]' if dry_run else '✓'} {customer_name} → {sector_code}")
            print(f"    Keywords: {', '.join(matched_keywords[:3])}{'...' if len(matched_keywords) > 3 else ''}")
            print(f"    Confidence: {confidence.upper()}")
            
            # Make the assignment (unless dry run)
            if not dry_run:
                success = self.assign_customer_to_sector(
                    customer_id, sector_id, confidence, matched_keywords
                )
                
                if success:
                    assigned += 1
                    self.assignment_results['assignments_made'].append({
                        'customer_id': customer_id,
                        'customer_name': customer_name,
                        'sector_code': sector_code,
                        'sector_id': sector_id,
                        'matched_keywords': matched_keywords,
                        'confidence': confidence
                    })
                else:
                    skipped += 1
            else:
                assigned += 1  # Count as assigned for dry run stats
        
        # Generate summary
        self.assignment_results['summary'] = {
            'total_processed': processed,
            'assignments_made': assigned,
            'customers_skipped': skipped,
            'assignment_rate': (assigned / processed * 100) if processed > 0 else 0,
            'dry_run': dry_run,
            'min_confidence': min_confidence
        }
        
        # Display results
        self._display_results()
        
        return self.assignment_results
    
    def _is_too_generic(self, customer_name: str, matched_keywords: List[str]) -> bool:
        """Check if the match is too generic and might be incorrect."""
        customer_lower = customer_name.lower()
        
        # Skip single-word generic matches
        generic_terms = ['network', 'systems', 'solutions', 'services', 'group', 'company']
        
        if len(matched_keywords) == 1 and matched_keywords[0] in generic_terms:
            return True
        
        # Skip if it contains contradictory terms
        contradictory_pairs = [
            (['network'], ['television', 'tv', 'broadcasting']),  # TV networks aren't tech
            (['bank'], ['food']),  # Food banks aren't financial
            (['benefits'], ['medicare', 'health']),  # Health benefits aren't auto
        ]
        
        for keywords, contradictory in contradictory_pairs:
            if any(kw in matched_keywords for kw in keywords):
                if any(term in customer_lower for term in contradictory):
                    return True
        
        return False
    
    def _display_results(self):
        """Display assignment results summary."""
        summary = self.assignment_results['summary']
        
        print("\n" + "=" * 60)
        print("📋 PATTERN ASSIGNMENT RESULTS")
        print("=" * 60)
        
        print(f"📊 Summary:")
        print(f"  Total customers processed: {summary['total_processed']}")
        print(f"  Assignments made: {summary['assignments_made']}")
        print(f"  Customers skipped: {summary['customers_skipped']}")
        print(f"  Assignment rate: {summary['assignment_rate']:.1f}%")
        
        if summary['dry_run']:
            print(f"  🔍 This was a DRY RUN - no actual changes made")
        
        # Show assignments by sector
        if self.assignment_results['assignments_made']:
            assignments_by_sector = {}
            for assignment in self.assignment_results['assignments_made']:
                sector = assignment['sector_code']
                if sector not in assignments_by_sector:
                    assignments_by_sector[sector] = []
                assignments_by_sector[sector].append(assignment['customer_name'])
            
            print(f"\n🎯 Assignments by Sector:")
            for sector_code, customers in assignments_by_sector.items():
                print(f"  {sector_code}: {len(customers)} customers")
                for customer in customers[:3]:  # Show first 3
                    print(f"    - {customer}")
                if len(customers) > 3:
                    print(f"    ... and {len(customers) - 3} more")
        
        # Show errors if any
        if self.assignment_results['errors']:
            print(f"\n❌ Errors ({len(self.assignment_results['errors'])}):")
            for error in self.assignment_results['errors'][:5]:  # Show first 5
                print(f"  • {error}")
        
        print(f"\n✅ Pattern assignment completed at {self.assignment_results['timestamp']}")
    
    def export_results(self, output_file: str = None) -> str:
        """Export assignment results to JSON file."""
        if not output_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"pattern_assignments_{timestamp}.json"
        
        try:
            with open(output_file, 'w') as f:
                json.dump(self.assignment_results, f, indent=2, default=str)
            
            print(f"\n💾 Assignment results exported to: {output_file}")
            return output_file
            
        except Exception as e:
            print(f"❌ Failed to export results: {e}")
            return ""
    
    def get_assignment_preview(self, limit: int = 10) -> List[Dict]:
        """Get a preview of potential assignments without making them."""
        unassigned_customers = self.get_unassigned_customers()
        sector_mappings = self.get_sector_mappings()
        
        preview = []
        for customer in unassigned_customers[:limit]:
            matches = self.find_pattern_matches(customer['name'])
            if matches:
                best_match = matches[0]
                sector_code, keywords, confidence = best_match
                
                preview.append({
                    'customer_name': customer['name'],
                    'suggested_sector': sector_code,
                    'confidence': confidence,
                    'keywords': keywords[:3],  # First 3 keywords
                    'all_matches': len(matches)
                })
        
        return preview
    
    def close(self):
        """Close database connection."""
        if self.db_connection:
            self.db_connection.close()


def main():
    """Main function for pattern-based assignment."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Quick Pattern-Based Customer Assignment",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/quick_pattern_assignment.py --db-path data/database/test.db --dry-run
  python scripts/quick_pattern_assignment.py --db-path data/database/test.db --confidence high
  python scripts/quick_pattern_assignment.py --db-path data/database/test.db --preview
        """
    )
    
    parser.add_argument("--db-path", default="data/database/production.db", 
                       help="Database path")
    parser.add_argument("--dry-run", action="store_true",
                       help="Preview assignments without making changes")
    parser.add_argument("--confidence", choices=['high', 'medium'], default='medium',
                       help="Minimum confidence level for assignments")
    parser.add_argument("--preview", action="store_true",
                       help="Show preview of potential assignments")
    parser.add_argument("--export", help="Export results to JSON file")
    parser.add_argument("--verbose", action="store_true",
                       help="Enable verbose logging")
    
    args = parser.parse_args()
    
    # Setup logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format='%(levelname)s: %(message)s')
    
    # Check database exists
    if not Path(args.db_path).exists():
        print(f"❌ Database not found: {args.db_path}")
        print(f"Run: uv run python scripts/setup_database.py --db-path {args.db_path}")
        sys.exit(1)
    
    try:
        assigner = QuickPatternAssigner(args.db_path)
        
        if args.preview:
            # Show preview of potential assignments
            print("🔍 Assignment Preview (First 10 Potential Assignments)")
            print("=" * 60)
            
            preview = assigner.get_assignment_preview(10)
            if preview:
                for item in preview:
                    print(f"  {item['customer_name']} → {item['suggested_sector']}")
                    print(f"    Confidence: {item['confidence'].upper()}")
                    print(f"    Keywords: {', '.join(item['keywords'])}")
                    print(f"    Total matches: {item['all_matches']}")
                    print()
            else:
                print("  No customers found with recognizable patterns")
        
        else:
            # Run the assignment process
            results = assigner.run_pattern_assignment(
                dry_run=args.dry_run,
                min_confidence=args.confidence
            )
            
            # Export results if requested
            if args.export:
                assigner.export_results(args.export)
            
            # Display next steps
            if not args.dry_run and results['summary']['assignments_made'] > 0:
                print(f"\n💡 Next Steps:")
                print(f"  • Run sector audit again to see updated assignment rate")
                print(f"  • Review assignments: python scripts/sector_audit.py --db-path {args.db_path}")
                print(f"  • Build manual assignment tool for remaining customers")
            elif args.dry_run:
                print(f"\n💡 To make these assignments permanent:")
                print(f"  python scripts/quick_pattern_assignment.py --db-path {args.db_path}")
        
    except Exception as e:
        print(f"❌ Assignment failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)
    
    finally:
        try:
            assigner.close()
        except:
            pass


if __name__ == "__main__":
    main()