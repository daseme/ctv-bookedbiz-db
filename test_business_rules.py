#!/usr/bin/env python3
"""
Business Rules Test CLI (Updated with Stakeholder Language)
==========================================================

Test the new sector-based business rules against your actual data.
Updated to use stakeholder-friendly terminology and "Direct Response Sales" logic.

Usage:
    python test_business_rules.py --estimate          # Estimate impact
    python test_business_rules.py --test 100          # Test on 100 spots
    python test_business_rules.py --validate          # Validate rules
    python test_business_rules.py --all               # Run all tests
"""

import sqlite3
import argparse
import sys
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum
from datetime import datetime
import json


class BusinessRuleType(Enum):
    """Business rule types with stakeholder-friendly names"""
    DIRECT_RESPONSE_SALES = "direct_response_sales"
    GOVERNMENT_PUBLIC_SERVICE = "government_public_service"
    POLITICAL_CAMPAIGNS = "political_campaigns"
    NONPROFIT_AWARENESS = "nonprofit_awareness"
    EXTENDED_CONTENT_BLOCKS = "extended_content_blocks"


class CustomerIntent(Enum):
    """Customer intent classification"""
    INDIFFERENT = "indifferent"
    LANGUAGE_SPECIFIC = "language_specific"
    TIME_SPECIFIC = "time_specific"
    NO_GRID_COVERAGE = "no_grid_coverage"


@dataclass
class BusinessRule:
    """Definition of a business rule"""
    rule_type: BusinessRuleType
    name: str
    description: str
    sector_codes: List[str]
    min_duration_minutes: Optional[int] = None
    max_duration_minutes: Optional[int] = None
    customer_intent: CustomerIntent = CustomerIntent.INDIFFERENT
    auto_resolve: bool = True
    priority: int = 1


@dataclass
class SpotData:
    """Spot data for business rule evaluation"""
    spot_id: int
    customer_id: int
    sector_code: Optional[str]
    sector_name: Optional[str]
    bill_code: str
    duration_minutes: int
    gross_rate: float
    customer_name: str
    time_in: str
    time_out: str


@dataclass
class RuleTestResult:
    """Result of testing a business rule"""
    spot_id: int
    rule_matched: Optional[BusinessRule] = None
    customer_intent: Optional[CustomerIntent] = None
    auto_resolved: bool = False
    confidence: float = 1.0
    notes: str = ""


class BusinessRulesValidator:
    """Validates business rules against actual data"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
        self.rules = self._initialize_rules()
    
    def connect(self) -> bool:
        """Connect to database"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            print(f"✅ Connected to database: {self.db_path}")
            return True
        except Exception as e:
            print(f"❌ Failed to connect: {e}")
            return False
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
    
    def _initialize_rules(self) -> List[BusinessRule]:
        """Initialize business rules with stakeholder-friendly terminology"""
        return [
            # Rule 1: Direct Response Sales (ALL MEDIA spots - 295K!)
            BusinessRule(
                rule_type=BusinessRuleType.DIRECT_RESPONSE_SALES,
                name="Direct Response Sales Auto-Assignment",
                description="Direct response sales (infomercials) - broad audience reach",
                sector_codes=["MEDIA"],
                # No duration constraint - ALL MEDIA spots auto-resolve
                priority=1
            ),
            
            # Rule 2: Government Public Service (all GOV spots)
            BusinessRule(
                rule_type=BusinessRuleType.GOVERNMENT_PUBLIC_SERVICE,
                name="Government Public Service Campaigns",
                description="Government public service campaigns - community-wide reach",
                sector_codes=["GOV"],
                priority=2
            ),
            
            # Rule 3: Political Campaigns (all POLITICAL spots)
            BusinessRule(
                rule_type=BusinessRuleType.POLITICAL_CAMPAIGNS,
                name="Political Campaign Advertising",
                description="Political campaigns - broad demographic reach",
                sector_codes=["POLITICAL"],
                priority=3
            ),
            
            # Rule 4: Nonprofit Awareness (5+ hours)
            BusinessRule(
                rule_type=BusinessRuleType.NONPROFIT_AWARENESS,
                name="Nonprofit Long-Duration Awareness",
                description="Nonprofit awareness campaigns (5+ hours) - community outreach",
                sector_codes=["NPO"],
                min_duration_minutes=300,  # 5+ hours
                priority=4
            ),
            
            # Rule 5: Extended Content Blocks (12+ hours)
            BusinessRule(
                rule_type=BusinessRuleType.EXTENDED_CONTENT_BLOCKS,
                name="Extended Content Blocks",
                description="Extended content (12+ hours) - spans multiple blocks",
                sector_codes=[],  # Any sector
                min_duration_minutes=720,  # 12+ hours
                priority=5
            )
        ]
    
    def get_spot_data(self, spot_id: int) -> Optional[SpotData]:
        """Get spot data for business rule evaluation"""
        cursor = self.conn.cursor()
        
        query = """
        SELECT 
            s.spot_id,
            s.customer_id,
            s.bill_code,
            s.time_in,
            s.time_out,
            s.gross_rate,
            c.normalized_name as customer_name,
            sec.sector_code,
            sec.sector_name,
            CAST((strftime('%s', s.time_out) - strftime('%s', s.time_in)) / 60 AS INTEGER) as duration_minutes
        FROM spots s
        LEFT JOIN customers c ON s.customer_id = c.customer_id
        LEFT JOIN sectors sec ON c.sector_id = sec.sector_id
        WHERE s.spot_id = ?
        """
        
        cursor.execute(query, (spot_id,))
        row = cursor.fetchone()
        
        if not row:
            return None
        
        return SpotData(
            spot_id=row[0],
            customer_id=row[1],
            bill_code=row[2],
            time_in=row[3],
            time_out=row[4],
            gross_rate=row[5] or 0.0,
            customer_name=row[6] or "Unknown",
            sector_code=row[7],
            sector_name=row[8],
            duration_minutes=row[9] or 0
        )
    
    def evaluate_spot(self, spot_data: SpotData) -> RuleTestResult:
        """Evaluate a spot against business rules"""
        for rule in sorted(self.rules, key=lambda r: r.priority):
            if self._spot_matches_rule(spot_data, rule):
                return RuleTestResult(
                    spot_id=spot_data.spot_id,
                    rule_matched=rule,
                    customer_intent=rule.customer_intent,
                    auto_resolved=rule.auto_resolve,
                    confidence=1.0,
                    notes=f"Matched rule: {rule.name}"
                )
        
        return RuleTestResult(
            spot_id=spot_data.spot_id,
            notes="No business rule matched"
        )
    
    def _spot_matches_rule(self, spot_data: SpotData, rule: BusinessRule) -> bool:
        """Check if a spot matches a business rule"""
        # Check sector codes
        if rule.sector_codes and spot_data.sector_code not in rule.sector_codes:
            return False
        
        # Check duration constraints
        if rule.min_duration_minutes and spot_data.duration_minutes < rule.min_duration_minutes:
            return False
        
        if rule.max_duration_minutes and spot_data.duration_minutes > rule.max_duration_minutes:
            return False
        
        return True
    
    def estimate_impact(self) -> Dict[str, Any]:
        """Estimate impact of business rules on all spots"""
        print(f"\n📊 ESTIMATING BUSINESS RULES IMPACT")
        print("=" * 50)
        
        estimates = {}
        cursor = self.conn.cursor()
        
        # Get total spots for context
        cursor.execute("SELECT COUNT(*) FROM spots")
        total_spots = cursor.fetchone()[0]
        
        print(f"Total spots in database: {total_spots:,}")
        
        # Test each rule
        for rule in self.rules:
            print(f"\n🔍 Testing rule: {rule.name}")
            
            # Build query based on rule criteria
            query = """
            SELECT COUNT(*) FROM spots s
            LEFT JOIN customers c ON s.customer_id = c.customer_id
            LEFT JOIN sectors sec ON c.sector_id = sec.sector_id
            WHERE 1=1
            """
            params = []
            
            if rule.sector_codes:
                placeholders = ','.join(['?' for _ in rule.sector_codes])
                query += f" AND sec.sector_code IN ({placeholders})"
                params.extend(rule.sector_codes)
            
            if rule.min_duration_minutes:
                query += " AND CAST((strftime('%s', s.time_out) - strftime('%s', s.time_in)) / 60 AS INTEGER) >= ?"
                params.append(rule.min_duration_minutes)
            
            if rule.max_duration_minutes:
                query += " AND CAST((strftime('%s', s.time_out) - strftime('%s', s.time_in)) / 60 AS INTEGER) <= ?"
                params.append(rule.max_duration_minutes)
            
            cursor.execute(query, params)
            count = cursor.fetchone()[0]
            percentage = (count / total_spots * 100) if total_spots > 0 else 0
            
            estimates[rule.rule_type.value] = {
                'count': count,
                'percentage': percentage,
                'description': rule.description
            }
            
            print(f"   • Estimated spots: {count:,} ({percentage:.1f}%)")
        
        # Calculate totals
        total_affected = sum(est['count'] for est in estimates.values())
        total_percentage = (total_affected / total_spots * 100) if total_spots > 0 else 0
        
        print(f"\n💡 SUMMARY:")
        print(f"   • Total spots affected: {total_affected:,} ({total_percentage:.1f}%)")
        print(f"   • Potential edge case reduction: {total_percentage:.1f}%")
        
        return estimates
    
    def test_rules(self, limit: int = 100) -> Dict[str, Any]:
        """Test business rules on a sample of spots"""
        print(f"\n🧪 TESTING BUSINESS RULES ON {limit} SPOTS")
        print("=" * 50)
        
        # Get sample spots
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT spot_id FROM spots 
            WHERE customer_id IS NOT NULL
            AND time_in IS NOT NULL
            AND time_out IS NOT NULL
            ORDER BY RANDOM()
            LIMIT ?
        """, (limit,))
        
        spot_ids = [row[0] for row in cursor.fetchall()]
        
        if not spot_ids:
            print("❌ No spots found to test")
            return {}
        
        # Test each spot
        results = []
        for spot_id in spot_ids:
            spot_data = self.get_spot_data(spot_id)
            if spot_data:
                result = self.evaluate_spot(spot_data)
                results.append((spot_data, result))
        
        # Analyze results
        stats = self._analyze_test_results(results)
        self._print_test_results(results, stats)
        
        return stats
    
    def _analyze_test_results(self, results: List[tuple]) -> Dict[str, Any]:
        """Analyze test results"""
        total = len(results)
        auto_resolved = sum(1 for _, result in results if result.auto_resolved)
        rule_matches = sum(1 for _, result in results if result.rule_matched)
        
        rule_counts = {}
        for _, result in results:
            if result.rule_matched:
                rule_type = result.rule_matched.rule_type.value
                rule_counts[rule_type] = rule_counts.get(rule_type, 0) + 1
        
        return {
            'total': total,
            'auto_resolved': auto_resolved,
            'rule_matches': rule_matches,
            'no_rule_matches': total - rule_matches,
            'auto_resolve_rate': auto_resolved / total if total > 0 else 0,
            'rule_match_rate': rule_matches / total if total > 0 else 0,
            'rule_counts': rule_counts
        }
    
    def _print_test_results(self, results: List[tuple], stats: Dict[str, Any]):
        """Print test results"""
        print(f"\n📊 TEST RESULTS:")
        print(f"   • Total spots tested: {stats['total']}")
        print(f"   • Rule matches: {stats['rule_matches']} ({stats['rule_match_rate']:.1%})")
        print(f"   • Auto-resolved: {stats['auto_resolved']} ({stats['auto_resolve_rate']:.1%})")
        print(f"   • No rule matched: {stats['no_rule_matches']}")
        
        if stats['rule_counts']:
            print(f"\n🎯 RULES TRIGGERED:")
            for rule_type, count in stats['rule_counts'].items():
                print(f"   • {rule_type}: {count} spots")
        
        # Show sample matches
        print(f"\n📋 SAMPLE MATCHES:")
        shown = 0
        for spot_data, result in results:
            if result.rule_matched and shown < 5:
                print(f"   • Spot {spot_data.spot_id} ({spot_data.customer_name[:30]}...)")
                print(f"     Sector: {spot_data.sector_code}, Duration: {spot_data.duration_minutes}min")
                print(f"     Rule: {result.rule_matched.name}")
                shown += 1
    
    def validate_rules(self) -> Dict[str, Any]:
        """Validate business rules configuration"""
        print(f"\n🔍 VALIDATING BUSINESS RULES")
        print("=" * 50)
        
        validation_results = {}
        
        # Check if sectors exist
        cursor = self.conn.cursor()
        cursor.execute("SELECT sector_code FROM sectors WHERE is_active = 1")
        active_sectors = {row[0] for row in cursor.fetchall()}
        
        print(f"Active sectors in database: {sorted(active_sectors)}")
        
        for rule in self.rules:
            print(f"\n✅ Validating rule: {rule.name}")
            
            issues = []
            
            # Check sector codes
            if rule.sector_codes:
                missing_sectors = [s for s in rule.sector_codes if s not in active_sectors]
                if missing_sectors:
                    issues.append(f"Missing sectors: {missing_sectors}")
                else:
                    print(f"   • Sectors valid: {rule.sector_codes}")
            else:
                print(f"   • Applies to all sectors")
            
            # Check duration constraints
            if rule.min_duration_minutes:
                print(f"   • Min duration: {rule.min_duration_minutes} minutes")
            if rule.max_duration_minutes:
                print(f"   • Max duration: {rule.max_duration_minutes} minutes")
            
            validation_results[rule.rule_type.value] = {
                'valid': len(issues) == 0,
                'issues': issues
            }
            
            if issues:
                print(f"   ❌ Issues found: {'; '.join(issues)}")
            else:
                print(f"   ✅ Rule is valid")
        
        return validation_results
    
    def run_all_tests(self):
        """Run all tests"""
        print(f"\n🚀 RUNNING ALL BUSINESS RULES TESTS")
        print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        
        # Validate rules
        validation = self.validate_rules()
        
        # Estimate impact
        estimates = self.estimate_impact()
        
        # Test on sample
        test_results = self.test_rules(100)
        
        print(f"\n✅ ALL TESTS COMPLETE")
        print("=" * 60)
        
        return {
            'validation': validation,
            'estimates': estimates,
            'test_results': test_results
        }


def main():
    """Main CLI function"""
    parser = argparse.ArgumentParser(description="Business Rules Test CLI (Updated)")
    parser.add_argument("--database", default="./data/database/production.db", help="Database path")
    parser.add_argument("--estimate", action="store_true", help="Estimate impact of rules")
    parser.add_argument("--test", type=int, default=100, help="Test rules on N spots")
    parser.add_argument("--validate", action="store_true", help="Validate rules configuration")
    parser.add_argument("--all", action="store_true", help="Run all tests")
    
    args = parser.parse_args()
    
    validator = BusinessRulesValidator(args.database)
    
    if not validator.connect():
        return 1
    
    try:
        if args.all:
            validator.run_all_tests()
        elif args.estimate:
            validator.estimate_impact()
        elif args.validate:
            validator.validate_rules()
        else:
            validator.test_rules(args.test)
        
        return 0
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    finally:
        validator.close()


if __name__ == "__main__":
    exit(main())