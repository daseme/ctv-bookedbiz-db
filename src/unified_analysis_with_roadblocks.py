#!/usr/bin/env python3
"""
Unified Analysis Integration Layer with Roadblocks
==================================================

This extends the existing unified_analysis.py to include roadblocks
while maintaining perfect reconciliation and clean architecture.

Key features:
- Thin integration layer (minimal code changes)
- Perfect reconciliation maintained
- Clean separation of concerns
- Roadblocks integrated in proper precedence order

Usage:
- Add this code to your existing unified_analysis.py
- Or save as src/unified_analysis_with_roadblocks.py
"""

import sqlite3
import sys
import os
from typing import Dict, List, Set, Any, Optional
from dataclasses import dataclass

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from roadblocks_analyzer import RoadblocksAnalyzer
    ROADBLOCKS_AVAILABLE = True
except ImportError:
    print("Warning: roadblocks_analyzer module not found. Roadblocks analysis will be skipped.")
    ROADBLOCKS_AVAILABLE = False

try:
    from query_builders import BaseQueryBuilder
except ImportError:
    print("Warning: query_builders module not found. Using basic BaseQueryBuilder.")
    class BaseQueryBuilder:
        def __init__(self, year: str = "2024"):
            self.year = year
        
        def apply_standard_filters(self):
            return self


@dataclass
class UnifiedResult:
    """Unified result structure for both language and category analysis"""
    name: str
    revenue: float
    percentage: float
    paid_spots: int
    bonus_spots: int
    total_spots: int
    avg_per_spot: float
    details: Optional[Dict[str, Any]] = None


class UnifiedAnalysisWithRoadblocks:
    """
    Enhanced unified analysis engine with roadblocks integration
    while maintaining perfect reconciliation.
    """
    
    def __init__(self, db_path: str = "data/database/production.db"):
        self.db_path = db_path
        self.db_connection = None
    
    def __enter__(self):
        self.db_connection = sqlite3.connect(self.db_path)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.db_connection:
            self.db_connection.close()
    
    def get_base_totals(self, year: str = "2024") -> Dict[str, Any]:
        """Get the authoritative base totals that all analyses should reconcile to"""
        year_suffix = year[-2:]
        
        query = """
        SELECT 
            SUM(COALESCE(s.gross_rate, 0)) as revenue,
            COUNT(CASE WHEN s.spot_type != 'BNS' OR s.spot_type IS NULL THEN 1 END) as paid_spots,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bonus_spots,
            COUNT(*) as total_spots
        FROM spots s
        WHERE s.broadcast_month LIKE ?
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        """
        
        cursor = self.db_connection.cursor()
        cursor.execute(query, [f"%-{year_suffix}"])
        result = cursor.fetchone()
        
        return {
            'revenue': result[0] or 0,
            'paid_spots': result[1] or 0,
            'bonus_spots': result[2] or 0,
            'total_spots': result[3] or 0
        }
    
    def get_mutually_exclusive_categories(self, year: str = "2024") -> List[UnifiedResult]:
        """
        Get mutually exclusive categories using proper precedence rules
        UPDATED: Now includes roadblocks in proper precedence order
        """
        year_suffix = year[-2:]
        
        # Get all base spots
        base_spots = self._get_base_spot_ids(year_suffix)
        remaining_spots = base_spots.copy()
        
        categories = []
        
        # 1. Direct Response (WorldLink - highest priority)
        direct_response_spots = self._get_direct_response_spot_ids(year_suffix) & remaining_spots
        categories.append(self._create_category_result(
            "Direct Response", direct_response_spots, year_suffix
        ))
        remaining_spots -= direct_response_spots
        
        # 2. Branded Content (PRD)
        branded_content_spots = self._get_branded_content_spot_ids(year_suffix) & remaining_spots
        categories.append(self._create_category_result(
            "Branded Content (PRD)", branded_content_spots, year_suffix
        ))
        remaining_spots -= branded_content_spots
        
        # 3. Services (SVC)
        services_spots = self._get_services_spot_ids(year_suffix) & remaining_spots
        categories.append(self._create_category_result(
            "Services (SVC)", services_spots, year_suffix
        ))
        remaining_spots -= services_spots
        
        # 4. Overnight Shopping (NKB)
        overnight_spots = self._get_overnight_shopping_spot_ids(year_suffix) & remaining_spots
        categories.append(self._create_category_result(
            "Overnight Shopping", overnight_spots, year_suffix
        ))
        remaining_spots -= overnight_spots
        
        # 5. Individual Language Blocks
        individual_lang_spots = self._get_individual_language_spot_ids(year_suffix) & remaining_spots
        categories.append(self._create_category_result(
            "Individual Language Blocks", individual_lang_spots, year_suffix
        ))
        remaining_spots -= individual_lang_spots
        
        # 6. Roadblocks (NEW - integrated in proper precedence order)
        roadblocks_spots = self._get_roadblocks_spot_ids(year_suffix) & remaining_spots
        categories.append(self._create_category_result(
            "Roadblocks", roadblocks_spots, year_suffix
        ))
        remaining_spots -= roadblocks_spots
        
        # 7. Chinese Prime Time
        chinese_prime_spots = self._get_chinese_prime_time_spot_ids(year_suffix) & remaining_spots
        categories.append(self._create_category_result(
            "Chinese Prime Time", chinese_prime_spots, year_suffix
        ))
        remaining_spots -= chinese_prime_spots
        
        # 8. Multi-Language (Cross-Audience)
        multi_lang_spots = self._get_multi_language_spot_ids(year_suffix) & remaining_spots
        categories.append(self._create_category_result(
            "Multi-Language (Cross-Audience)", multi_lang_spots, year_suffix
        ))
        remaining_spots -= multi_lang_spots
        
        # 9. Other Non-Language (everything else)
        categories.append(self._create_category_result(
            "Other Non-Language", remaining_spots, year_suffix
        ))
        
        # Calculate percentages
        total_revenue = sum(cat.revenue for cat in categories)
        for cat in categories:
            cat.percentage = (cat.revenue / total_revenue) * 100 if total_revenue > 0 else 0
        
        return categories
    
    def _get_base_spot_ids(self, year_suffix: str) -> Set[int]:
        """Get all base spot IDs"""
        query = """
        SELECT DISTINCT s.spot_id
        FROM spots s
        WHERE s.broadcast_month LIKE ?
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        """
        
        cursor = self.db_connection.cursor()
        cursor.execute(query, [f"%-{year_suffix}"])
        return set(row[0] for row in cursor.fetchall())
    
    def _get_direct_response_spot_ids(self, year_suffix: str) -> Set[int]:
        """Get Direct Response spot IDs"""
        query = """
        SELECT DISTINCT s.spot_id
        FROM spots s
        LEFT JOIN agencies a ON s.agency_id = a.agency_id
        WHERE s.broadcast_month LIKE ?
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        AND (COALESCE(a.agency_name, '') LIKE '%WorldLink%' OR 
             COALESCE(s.bill_code, '') LIKE '%WorldLink%')
        """
        
        cursor = self.db_connection.cursor()
        cursor.execute(query, [f"%-{year_suffix}"])
        return set(row[0] for row in cursor.fetchall())
    
    def _get_branded_content_spot_ids(self, year_suffix: str) -> Set[int]:
        """Get Branded Content spot IDs"""
        query = """
        SELECT DISTINCT s.spot_id
        FROM spots s
        LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        WHERE s.broadcast_month LIKE ?
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        AND slb.spot_id IS NULL
        AND s.spot_type = 'PRD'
        """
        
        cursor = self.db_connection.cursor()
        cursor.execute(query, [f"%-{year_suffix}"])
        return set(row[0] for row in cursor.fetchall())
    
    def _get_services_spot_ids(self, year_suffix: str) -> Set[int]:
        """Get Services spot IDs"""
        query = """
        SELECT DISTINCT s.spot_id
        FROM spots s
        LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        WHERE s.broadcast_month LIKE ?
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        AND slb.spot_id IS NULL
        AND s.spot_type = 'SVC'
        """
        
        cursor = self.db_connection.cursor()
        cursor.execute(query, [f"%-{year_suffix}"])
        return set(row[0] for row in cursor.fetchall())
    
    def _get_overnight_shopping_spot_ids(self, year_suffix: str) -> Set[int]:
        """Get Overnight Shopping spot IDs"""
        query = """
        SELECT DISTINCT s.spot_id
        FROM spots s
        LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        LEFT JOIN agencies a ON s.agency_id = a.agency_id
        LEFT JOIN customers c ON s.customer_id = c.customer_id
        WHERE s.broadcast_month LIKE ?
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
        AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
        AND slb.spot_id IS NULL
        AND (s.spot_type NOT IN ('PRD', 'SVC') OR s.spot_type IS NULL OR s.spot_type = '')
        AND (COALESCE(c.normalized_name, '') LIKE '%NKB%' OR 
             COALESCE(s.bill_code, '') LIKE '%NKB%')
        """
        
        cursor = self.db_connection.cursor()
        cursor.execute(query, [f"%-{year_suffix}"])
        return set(row[0] for row in cursor.fetchall())
    
    def _get_individual_language_spot_ids(self, year_suffix: str) -> Set[int]:
        """Get Individual Language spot IDs"""
        query = """
        SELECT DISTINCT s.spot_id
        FROM spots s
        LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        LEFT JOIN agencies a ON s.agency_id = a.agency_id
        WHERE s.broadcast_month LIKE ?
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
        AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
        AND ((slb.spans_multiple_blocks = 0 AND slb.block_id IS NOT NULL) OR 
             (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NOT NULL))
        """
        
        cursor = self.db_connection.cursor()
        cursor.execute(query, [f"%-{year_suffix}"])
        return set(row[0] for row in cursor.fetchall())
    
    def _get_roadblocks_spot_ids(self, year_suffix: str) -> Set[int]:
        """Get Roadblocks spot IDs using integrated roadblocks analyzer"""
        if not ROADBLOCKS_AVAILABLE:
            return set()
        
        try:
            analyzer = RoadblocksAnalyzer(self.db_connection)
            year = f"20{year_suffix}"
            return analyzer.get_spot_ids(year)
        except Exception as e:
            print(f"Warning: Error getting roadblocks spot IDs: {e}")
            return set()
    
    def _get_chinese_prime_time_spot_ids(self, year_suffix: str) -> Set[int]:
        """Get Chinese Prime Time spot IDs"""
        query = """
        SELECT DISTINCT s.spot_id
        FROM spots s
        LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        LEFT JOIN agencies a ON s.agency_id = a.agency_id
        WHERE s.broadcast_month LIKE ?
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
        AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
        AND (slb.spans_multiple_blocks = 1 OR 
             (slb.spans_multiple_blocks = 0 AND slb.block_id IS NULL) OR 
             (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NULL))
        AND (
            (s.time_in >= '19:00:00' AND s.time_out <= '23:59:59' 
             AND s.day_of_week IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'))
            OR
            (s.time_in >= '20:00:00' AND s.time_out <= '23:59:59'
             AND s.day_of_week IN ('Saturday', 'Sunday'))
        )
        """
        
        cursor = self.db_connection.cursor()
        cursor.execute(query, [f"%-{year_suffix}"])
        return set(row[0] for row in cursor.fetchall())
    
    def _get_multi_language_spot_ids(self, year_suffix: str) -> Set[int]:
        """Get Multi-Language spot IDs"""
        query = """
        SELECT DISTINCT s.spot_id
        FROM spots s
        LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        LEFT JOIN agencies a ON s.agency_id = a.agency_id
        LEFT JOIN customers c ON s.customer_id = c.customer_id
        WHERE s.broadcast_month LIKE ?
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
        AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
        AND (slb.spans_multiple_blocks = 1 OR 
             (slb.spans_multiple_blocks = 0 AND slb.block_id IS NULL) OR 
             (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NULL))
        AND NOT (
            (s.time_in >= '19:00:00' AND s.time_out <= '23:59:59' 
             AND s.day_of_week IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'))
            OR
            (s.time_in >= '20:00:00' AND s.time_out <= '23:59:59'
             AND s.day_of_week IN ('Saturday', 'Sunday'))
        )
        AND COALESCE(c.normalized_name, '') NOT LIKE '%NKB%'
        AND COALESCE(s.bill_code, '') NOT LIKE '%NKB%'
        """
        
        cursor = self.db_connection.cursor()
        cursor.execute(query, [f"%-{year_suffix}"])
        return set(row[0] for row in cursor.fetchall())
    
    def _create_category_result(self, name: str, spot_ids: Set[int], year_suffix: str) -> UnifiedResult:
        """Create a category result from spot IDs"""
        if not spot_ids:
            return UnifiedResult(
                name=name,
                revenue=0,
                percentage=0,
                paid_spots=0,
                bonus_spots=0,
                total_spots=0,
                avg_per_spot=0
            )
        
        # Convert set to list for SQL IN clause
        spot_ids_list = list(spot_ids)
        
        # Create placeholders for IN clause
        placeholders = ','.join(['?' for _ in spot_ids_list])
        
        query = f"""
        SELECT 
            SUM(COALESCE(s.gross_rate, 0)) as revenue,
            COUNT(CASE WHEN s.spot_type != 'BNS' OR s.spot_type IS NULL THEN 1 END) as paid_spots,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bonus_spots,
            COUNT(*) as total_spots
        FROM spots s
        WHERE s.spot_id IN ({placeholders})
        """
        
        cursor = self.db_connection.cursor()
        cursor.execute(query, spot_ids_list)
        result = cursor.fetchone()
        
        revenue = result[0] or 0
        paid_spots = result[1] or 0
        bonus_spots = result[2] or 0
        total_spots = result[3] or 0
        
        return UnifiedResult(
            name=name,
            revenue=revenue,
            percentage=0,  # Will be calculated later
            paid_spots=paid_spots,
            bonus_spots=bonus_spots,
            total_spots=total_spots,
            avg_per_spot=revenue / total_spots if total_spots > 0 else 0
        )
    
    def validate_reconciliation(self, year: str = "2024") -> Dict[str, Any]:
        """Validate perfect reconciliation including roadblocks"""
        base_totals = self.get_base_totals(year)
        category_results = self.get_mutually_exclusive_categories(year)
        
        category_totals = {
            'revenue': sum(cat.revenue for cat in category_results),
            'paid_spots': sum(cat.paid_spots for cat in category_results),
            'bonus_spots': sum(cat.bonus_spots for cat in category_results),
            'total_spots': sum(cat.total_spots for cat in category_results)
        }
        
        return {
            'base_totals': base_totals,
            'category_totals': category_totals,
            'revenue_difference': abs(base_totals['revenue'] - category_totals['revenue']),
            'spot_difference': abs(base_totals['total_spots'] - category_totals['total_spots']),
            'perfect_reconciliation': (
                abs(base_totals['revenue'] - category_totals['revenue']) < 1.0 and
                abs(base_totals['total_spots'] - category_totals['total_spots']) < 1
            ),
            'roadblocks_included': ROADBLOCKS_AVAILABLE
        }
    
    def generate_integrated_report(self, year: str = "2024") -> str:
        """Generate integrated report with roadblocks"""
        
        # Get category analysis
        category_results = self.get_mutually_exclusive_categories(year)
        
        # Validate reconciliation
        validation = self.validate_reconciliation(year)
        
        # Get roadblocks details if available
        roadblocks_details = ""
        if ROADBLOCKS_AVAILABLE:
            try:
                analyzer = RoadblocksAnalyzer(self.db_connection)
                summary = analyzer.get_summary(year)
                roadblocks_details = f"""
## 🚧 Roadblocks Category Details

### Performance Metrics
- **Total Revenue**: ${summary.total_revenue:,.2f}
- **Total Spots**: {summary.total_spots:,}
- **Paid Spots**: {summary.paid_spots:,} ({100 - summary.bns_percentage:.1f}%)
- **BNS Spots**: {summary.bns_spots:,} ({summary.bns_percentage:.1f}%)
- **Average per Spot**: ${summary.avg_per_spot:.2f}

### Business Context
- **{summary.bns_percentage:.1f}% are BNS spots** (bonus content, no revenue)
- **Public service focus**: High BNS rate indicates government/non-profit campaigns
- **Full day coverage**: Most roadblocks run 6:00am-11:59pm (18 hours)
"""
            except Exception as e:
                roadblocks_details = f"\n⚠️ Error generating roadblocks details: {e}\n"
        
        # Format category table
        category_table = self._format_category_table(category_results, year)
        
        # Generate report
        return f"""# Integrated Revenue Analysis with Roadblocks - {year}

*Generated with perfect reconciliation including roadblocks category*

## 🎯 Reconciliation Status

- **Base Revenue**: ${validation['base_totals']['revenue']:,.2f}
- **Category Total**: ${validation['category_totals']['revenue']:,.2f}
- **Revenue Difference**: ${validation['revenue_difference']:,.2f}
- **Spot Difference**: {validation['spot_difference']:,}
- **Perfect Reconciliation**: {'✅ YES' if validation['perfect_reconciliation'] else '❌ NO'}
- **Roadblocks Included**: {'✅ YES' if validation['roadblocks_included'] else '❌ NO'}

{category_table}

{roadblocks_details}

## 📋 Integration Notes

- **Roadblocks Category**: Integrated as 6th priority after Individual Language Blocks
- **Perfect Reconciliation**: All spots accounted for exactly once
- **BNS Tracking**: Bonus spots properly tracked in all categories
- **Precedence Rules**: Direct Response → PRD → SVC → NKB → Individual Language → **Roadblocks** → Chinese Prime Time → Multi-Language → Other Non-Language

## 🔧 Technical Architecture

- **Thin Integration**: Minimal changes to existing unified_analysis.py
- **Separation of Concerns**: Roadblocks logic in dedicated module
- **Maintained Compatibility**: Existing functionality unchanged
- **Clean Architecture**: Each component has single responsibility

---

*Generated by Integrated Unified Analysis System v4.1*"""
    
    def _format_category_table(self, results: List[UnifiedResult], year: str) -> str:
        """Format category results into a table"""
        
        # Calculate totals
        total_revenue = sum(r.revenue for r in results)
        total_paid_spots = sum(r.paid_spots for r in results)
        total_bonus_spots = sum(r.bonus_spots for r in results)
        total_all_spots = sum(r.total_spots for r in results)
        total_avg_per_spot = total_revenue / total_all_spots if total_all_spots > 0 else 0
        
        # Build the table
        table = f"""## 📊 Revenue Category Breakdown
### Integrated Category Performance with Roadblocks ({year})
| Category | Revenue | % of Total | Paid Spots | BNS Spots | Total Spots | Avg/Spot |
|----------|---------|------------|-----------|-----------|-------------|----------|
"""
        
        for result in results:
            table += f"| {result.name} | ${result.revenue:,.2f} | {result.percentage:.1f}% | {result.paid_spots:,} | {result.bonus_spots:,} | {result.total_spots:,} | ${result.avg_per_spot:.2f} |\n"
        
        # Add total row
        table += "|----------|---------|------------|-----------|-----------|-------------|----------|\n"
        table += f"| **TOTAL** | **${total_revenue:,.2f}** | **100.0%** | **{total_paid_spots:,}** | **{total_bonus_spots:,}** | **{total_all_spots:,}** | **${total_avg_per_spot:.2f}** |\n"
        
        return table


def main():
    """Test the integrated unified analysis system"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Integrated Unified Analysis with Roadblocks")
    parser.add_argument("--year", default="2024", help="Year to analyze")
    parser.add_argument("--output", help="Output file path")
    parser.add_argument("--db-path", default="data/database/production.db", help="Database path")
    parser.add_argument("--validate-only", action="store_true", help="Run validation only")
    
    args = parser.parse_args()
    
    try:
        with UnifiedAnalysisWithRoadblocks(args.db_path) as system:
            if args.validate_only:
                # Run validation test
                validation = system.validate_reconciliation(args.year)
                print("🧪 Integrated Analysis Validation Results:")
                print("=" * 50)
                print(f"✅ Base Revenue: ${validation['base_totals']['revenue']:,.2f}")
                print(f"✅ Category Total: ${validation['category_totals']['revenue']:,.2f}")
                print(f"✅ Revenue Difference: ${validation['revenue_difference']:,.2f}")
                print(f"✅ Perfect Reconciliation: {'YES' if validation['perfect_reconciliation'] else 'NO'}")
                print(f"✅ Roadblocks Included: {'YES' if validation['roadblocks_included'] else 'NO'}")
            else:
                # Generate integrated report
                report = system.generate_integrated_report(args.year)
                
                if args.output:
                    with open(args.output, 'w') as f:
                        f.write(report)
                    print(f"✅ Integrated report saved to {args.output}")
                else:
                    print(report)
    
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()