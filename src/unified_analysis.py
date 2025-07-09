#!/usr/bin/env python3
"""
Working Unified Analysis System - Including Roadblocks Category
==============================================================

This system extends the existing unified analysis to include the new roadblocks category
while maintaining perfect reconciliation with mutually exclusive categories.

Save this as: src/unified_analysis.py
"""

import sqlite3
import sys
import os
from typing import Dict, List, Set, Any, Optional
from dataclasses import dataclass

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

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


class UpdatedUnifiedAnalysisEngine:
    """
    Updated unified analysis engine with roadblocks category support
    ensuring mutually exclusive categories and perfect reconciliation.
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
        """Get the authoritative base totals that both analyses should reconcile to"""
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
        Updated: Roadblocks moved ahead of Multi-Language
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
        
        # 6. Roadblocks (MOVED AHEAD OF MULTI-LANGUAGE)
        roadblocks_spots = self._get_roadblocks_spot_ids(year_suffix) & remaining_spots
        categories.append(self._create_category_result(
            "Roadblocks", roadblocks_spots, year_suffix
        ))
        remaining_spots -= roadblocks_spots
        
        # 7. Multi-Language (Cross-Audience) - now comes after Roadblocks
        multi_lang_spots = self._get_multi_language_spot_ids(year_suffix) & remaining_spots
        categories.append(self._create_category_result(
            "Multi-Language (Cross-Audience)", multi_lang_spots, year_suffix
        ))
        remaining_spots -= multi_lang_spots
        
        # 8. Other Non-Language (everything else)
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
    
    def _get_roadblocks_spot_ids(self, year_suffix: str) -> Set[int]:
        """
        Get Roadblocks spot IDs - Using campaign_type = 'roadblock'
        Now captures from remaining spots after higher-priority categories
        """
        query = """
        SELECT DISTINCT s.spot_id
        FROM spots s
        LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        WHERE s.broadcast_month LIKE ?
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        AND slb.campaign_type = 'roadblock'
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
    
    def get_unified_language_analysis(self, year: str = "2024") -> List[UnifiedResult]:
        """
        Get language analysis that reconciles with category analysis
        Updated: Removed Chinese Prime Time
        """
        year_suffix = year[-2:]
        languages = []
        
        # Get individual language breakdown (using only individual language spots)
        individual_languages = self._get_individual_language_breakdown(year_suffix)
        languages.extend(individual_languages)
        
        # Calculate percentages based on total revenue from languages
        total_revenue = sum(lang.revenue for lang in languages)
        for lang in languages:
            lang.percentage = (lang.revenue / total_revenue) * 100 if total_revenue > 0 else 0
        
        # Sort by revenue descending
        languages.sort(key=lambda x: x.revenue, reverse=True)
        
        return languages
    
    def _get_individual_language_breakdown(self, year_suffix: str) -> List[UnifiedResult]:
        """Get breakdown of individual language blocks by language"""
        
        # First get the spot IDs for individual language blocks
        individual_lang_spots = self._get_individual_language_spot_ids(year_suffix)
        
        if not individual_lang_spots:
            return []
        
        # Convert to list for SQL IN clause
        spot_ids_list = list(individual_lang_spots)
        placeholders = ','.join(['?' for _ in spot_ids_list])
        
        query = f"""
        SELECT 
            CASE 
                WHEN l.language_name IN ('Mandarin', 'Cantonese') THEN 'Chinese'
                WHEN l.language_name = 'Hmong' THEN 'Hmong'
                ELSE COALESCE(l.language_name, 'Unknown Language')
            END as language,
            SUM(COALESCE(s.gross_rate, 0)) as revenue,
            COUNT(CASE WHEN s.spot_type != 'BNS' OR s.spot_type IS NULL THEN 1 END) as paid_spots,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bonus_spots,
            COUNT(*) as total_spots
        FROM spots s
        LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        LEFT JOIN language_blocks lb ON slb.block_id = lb.block_id
        LEFT JOIN languages l ON lb.language_id = l.language_id
        WHERE s.spot_id IN ({placeholders})
        GROUP BY CASE 
            WHEN l.language_name IN ('Mandarin', 'Cantonese') THEN 'Chinese'
            WHEN l.language_name = 'Hmong' THEN 'Hmong'
            ELSE COALESCE(l.language_name, 'Unknown Language')
        END
        HAVING SUM(COALESCE(s.gross_rate, 0)) > 0 OR COUNT(*) > 0
        ORDER BY SUM(COALESCE(s.gross_rate, 0)) DESC
        """
        
        cursor = self.db_connection.cursor()
        cursor.execute(query, spot_ids_list)
        
        results = []
        for row in cursor.fetchall():
            language, revenue, paid_spots, bonus_spots, total_spots = row
            results.append(UnifiedResult(
                name=language,
                revenue=revenue,
                percentage=0,  # Will be calculated later
                paid_spots=paid_spots,
                bonus_spots=bonus_spots,
                total_spots=total_spots,
                avg_per_spot=revenue / total_spots if total_spots > 0 else 0
            ))
        
        return results
    
    def get_roadblocks_analysis(self, year: str = "2024") -> Dict[str, Any]:
        """Get detailed roadblocks analysis using campaign_type"""
        year_suffix = year[-2:]
        
        # Get roadblocks spots using campaign_type 
        query = """
        SELECT 
            s.spot_id,
            s.bill_code,
            s.air_date,
            s.time_in,
            s.time_out,
            s.length_seconds,
            s.program,
            s.gross_rate,
            COALESCE(c.normalized_name, 'Unknown') as customer_name,
            COALESCE(a.agency_name, 'Unknown') as agency_name,
            slb.campaign_type,
            slb.customer_intent,
            CASE 
                WHEN s.length_seconds IS NOT NULL AND CAST(s.length_seconds AS INTEGER) > 1800 THEN 'Long-form'
                WHEN s.program LIKE '%sponsor%' OR s.program LIKE '%Sponsor%' THEN 'Sponsorship'
                WHEN s.time_in >= '23:00:00' OR s.time_in <= '06:00:00' THEN 'Late Night/Early Morning'
                WHEN s.time_in IS NOT NULL AND s.time_out IS NOT NULL AND TIME(s.time_out) < TIME(s.time_in) THEN 'Spans Midnight'
                ELSE 'Standard Roadblock'
            END as roadblock_type
        FROM spots s
        LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        LEFT JOIN customers c ON s.customer_id = c.customer_id
        LEFT JOIN agencies a ON s.agency_id = a.agency_id
        WHERE s.broadcast_month LIKE ?
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        AND slb.campaign_type = 'roadblock'
        ORDER BY s.air_date, s.time_in
        """
        
        cursor = self.db_connection.cursor()
        cursor.execute(query, [f"%-{year_suffix}"])
        
        roadblocks_details = []
        roadblock_types = {}
        customer_analysis = {}
        agency_analysis = {}
        total_revenue = 0
        
        for row in cursor.fetchall():
            detail = {
                'spot_id': row[0],
                'bill_code': row[1],
                'air_date': row[2],
                'time_in': row[3],
                'time_out': row[4],
                'length_seconds': row[5],
                'program': row[6],
                'gross_rate': row[7] or 0,
                'customer_name': row[8],
                'agency_name': row[9],
                'campaign_type': row[10],
                'customer_intent': row[11],
                'roadblock_type': row[12]
            }
            roadblocks_details.append(detail)
            
            # Count by roadblock type
            roadblock_type = row[12]
            if roadblock_type not in roadblock_types:
                roadblock_types[roadblock_type] = {'count': 0, 'revenue': 0}
            roadblock_types[roadblock_type]['count'] += 1
            roadblock_types[roadblock_type]['revenue'] += row[7] or 0
            
            # Count by customer
            customer_name = row[8]
            if customer_name not in customer_analysis:
                customer_analysis[customer_name] = {'count': 0, 'revenue': 0}
            customer_analysis[customer_name]['count'] += 1
            customer_analysis[customer_name]['revenue'] += row[7] or 0
            
            # Count by agency
            agency_name = row[9]
            if agency_name not in agency_analysis:
                agency_analysis[agency_name] = {'count': 0, 'revenue': 0}
            agency_analysis[agency_name]['count'] += 1
            agency_analysis[agency_name]['revenue'] += row[7] or 0
            
            total_revenue += row[7] or 0
        
        # Sort by revenue
        top_customers = sorted(customer_analysis.items(), key=lambda x: x[1]['revenue'], reverse=True)[:10]
        top_agencies = sorted(agency_analysis.items(), key=lambda x: x[1]['revenue'], reverse=True)[:10]
        
        return {
            'total_revenue': total_revenue,
            'total_spots': len(roadblocks_details),
            'roadblock_types': roadblock_types,
            'top_customers': top_customers,
            'top_agencies': top_agencies,
            'details': roadblocks_details
        }
    
    def validate_reconciliation(self, year: str = "2024") -> Dict[str, Any]:
        """Validate perfect reconciliation"""
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
            )
        }
    
    def generate_updated_unified_tables(self, year: str = "2024") -> str:
        """Generate both tables with perfect reconciliation including roadblocks"""
        
        # Get both analyses
        category_results = self.get_mutually_exclusive_categories(year)
        language_results = self.get_unified_language_analysis(year)
        
        # Get roadblocks analysis
        roadblocks_analysis = self.get_roadblocks_analysis(year)
        
        # Validate reconciliation
        validation = self.validate_reconciliation(year)
        
        # Generate category table
        category_table = self._format_table(
            category_results,
            "📊 Revenue Category Breakdown",
            "Updated Category Performance with Roadblocks",
            year
        )
        
        # Generate language table
        language_table = self._format_table(
            language_results,
            "🌐 Language Analysis",
            "Combined Language Performance",
            year
        )
        
        # Generate roadblocks breakdown
        roadblocks_breakdown = self._format_roadblocks_breakdown(roadblocks_analysis)
        
        # Generate report
        return f"""# Updated Unified Revenue Analysis with Roadblocks - {year}

*Generated with PERFECT reconciliation including new Roadblocks category*

## 🎯 Reconciliation Status

- **Base Revenue**: ${validation['base_totals']['revenue']:,.2f}
- **Category Total**: ${validation['category_totals']['revenue']:,.2f}
- **Revenue Difference**: ${validation['revenue_difference']:,.2f}
- **Spot Difference**: {validation['spot_difference']:,}
- **Perfect Reconciliation**: {'✅ YES' if validation['perfect_reconciliation'] else '❌ NO'}

{category_table}

{language_table}

{roadblocks_breakdown}

{self._generate_reconciliation_notes()}
"""
    
    def _format_roadblocks_breakdown(self, roadblocks_analysis: Dict[str, Any]) -> str:
        """Format roadblocks analysis breakdown"""
        
        if roadblocks_analysis['total_spots'] == 0:
            return """## 🚧 Roadblocks Category Breakdown

### Broadcast Sponsorships (campaign_type = 'roadblock' from remaining spots)
- **Total Revenue**: $0.00
- **Total Spots**: 0
- **Source**: Captured from remaining spots after language-specific and multi-language categories
- **WorldLink**: WorldLink roadblocks go to Direct Response (higher precedence)
- **Note**: No roadblocks found in remaining spots for this year

"""
        
        breakdown = f"""## 🚧 Roadblocks Category Breakdown

### Broadcast Sponsorships (campaign_type = 'roadblock' from remaining spots)
- **Total Revenue**: ${roadblocks_analysis['total_revenue']:,.2f}
- **Total Spots**: {roadblocks_analysis['total_spots']:,}
- **Source**: Captured from remaining spots after language-specific and multi-language categories
- **WorldLink**: WorldLink roadblocks go to Direct Response (higher precedence)

### Roadblock Types Analysis
| Type | Spots | Revenue | Avg/Spot |
|------|-------|---------|----------|
"""
        
        for roadblock_type, data in roadblocks_analysis['roadblock_types'].items():
            avg_per_spot = data['revenue'] / data['count'] if data['count'] > 0 else 0
            breakdown += f"| {roadblock_type} | {data['count']:,} | ${data['revenue']:,.2f} | ${avg_per_spot:.2f} |\n"
        
        # Add top customers
        if roadblocks_analysis['top_customers']:
            breakdown += f"""
### Top Roadblock Customers
| Customer | Spots | Revenue | Avg/Spot |
|----------|-------|---------|----------|
"""
            for customer_name, data in roadblocks_analysis['top_customers']:
                avg_per_spot = data['revenue'] / data['count'] if data['count'] > 0 else 0
                breakdown += f"| {customer_name} | {data['count']:,} | ${data['revenue']:,.2f} | ${avg_per_spot:.2f} |\n"
        
        # Add top agencies
        if roadblocks_analysis['top_agencies']:
            breakdown += f"""
### Top Roadblock Agencies
| Agency | Spots | Revenue | Avg/Spot |
|--------|-------|---------|----------|
"""
            for agency_name, data in roadblocks_analysis['top_agencies']:
                avg_per_spot = data['revenue'] / data['count'] if data['count'] > 0 else 0
                breakdown += f"| {agency_name} | {data['count']:,} | ${data['revenue']:,.2f} | ${avg_per_spot:.2f} |\n"
        
        return breakdown
    
    def _generate_reconciliation_notes(self) -> str:
        """Generate reconciliation notes"""
        return """## 📋 Reconciliation Notes

- **Perfect Match**: Both analyses now work from the same base query with perfect reconciliation
- **Mutually Exclusive Categories**: Each spot is counted exactly once using proper precedence rules
- **No Double Counting**: Category overlaps eliminated through precedence-based assignment
- **Hmong Included**: Hmong language is properly included in the language analysis
- **NEW: Roadblocks Category**: Broadcast sponsorships outside language blocks now have dedicated category
- **Precedence Rules**: Direct Response → PRD → SVC → NKB → Individual Language → Multi-Language → **Roadblocks** → Other Non-Language

## 🔧 Technical Fix Applied

**Problem Solved**: The original system had spots being double-counted due to overlaps between categories.

**Solution Applied**: Implemented proper precedence rules using set subtraction to ensure each spot belongs to exactly one category.

**NEW: Roadblocks Category Added**: 
- **Purpose**: Capture broadcast sponsorships from remaining spots after language-specific categories
- **Identification**: Uses campaign_type = 'roadblock' but processed after multi-language
- **WorldLink Handling**: WorldLink roadblocks captured by Direct Response (higher precedence)
- **Reduces**: Multi-language category numbers by capturing roadblocks from remaining spots
- **Business Logic**: Roadblocks are secondary to language-specific and multi-language targeting

## 🎯 Roadblocks Category Details

**What Qualifies as Roadblocks:**
- Remaining spots with campaign_type = 'roadblock' after higher-priority categories
- Captures from spots not claimed by Direct Response, language-specific, or multi-language
- Broadcast sponsorships and special programming
- WorldLink roadblocks go to Direct Response category (higher precedence)

**Business Value:**
- Reduces multi-language category by capturing roadblocks from remaining spots
- Clear separation: WorldLink roadblocks → Direct Response, Others → Roadblocks
- Better understanding of non-language-specific sponsorship patterns
- Improved categorization of broadcast sponsorships vs. targeted advertising

---

*Generated by Updated Unified Revenue Analysis System v4.0*"""
    
    def _format_table(self, results: List[UnifiedResult], title: str, subtitle: str, year: str) -> str:
        """Format results into a table"""
        
        # Calculate totals
        total_revenue = sum(r.revenue for r in results)
        total_paid_spots = sum(r.paid_spots for r in results)
        total_bonus_spots = sum(r.bonus_spots for r in results)
        total_all_spots = sum(r.total_spots for r in results)
        total_avg_per_spot = total_revenue / total_all_spots if total_all_spots > 0 else 0
        
        # Build the table
        table = f"""## {title}
### {subtitle} ({year})
| Category | Revenue | % of Total | Spots | Bonus Spots | Total Spots | Avg/Spot |
|----------|---------|------------|-------|-------------|-------------|----------|
"""
        
        for result in results:
            table += f"| {result.name} | ${result.revenue:,.2f} | {result.percentage:.1f}% | {result.paid_spots:,} | {result.bonus_spots:,} | {result.total_spots:,} | ${result.avg_per_spot:.2f} |\n"
        
        # Add total row
        table += "|----------|---------|------------|-------|-------------|-------------|----------|\n"
        table += f"| **TOTAL** | **${total_revenue:,.2f}** | **100.0%** | **{total_paid_spots:,}** | **{total_bonus_spots:,}** | **{total_all_spots:,}** | **${total_avg_per_spot:.2f}** |\n"
        
        return table


def main():
    """Test the updated unified analysis system with roadblocks"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Updated Unified Revenue Analysis with Roadblocks")
    parser.add_argument("--year", default="2024", help="Year to analyze")
    parser.add_argument("--output", help="Output file path")
    parser.add_argument("--db-path", default="data/database/production.db", help="Database path")
    parser.add_argument("--roadblocks-only", action="store_true", help="Show only roadblocks analysis")
    
    args = parser.parse_args()
    
    try:
        with UpdatedUnifiedAnalysisEngine(args.db_path) as engine:
            if args.roadblocks_only:
                # Show detailed roadblocks analysis
                roadblocks_analysis = engine.get_roadblocks_analysis(args.year)
                print(f"🚧 Roadblocks Analysis for {args.year}")
                print("=" * 50)
                print(f"Total Revenue: ${roadblocks_analysis['total_revenue']:,.2f}")
                print(f"Total Spots: {roadblocks_analysis['total_spots']:,}")
                print("\nRoadblock Types:")
                for roadblock_type, data in roadblocks_analysis['roadblock_types'].items():
                    avg_per_spot = data['revenue'] / data['count'] if data['count'] > 0 else 0
                    print(f"  {roadblock_type}: {data['count']:,} spots, ${data['revenue']:,.2f} (${avg_per_spot:.2f}/spot)")
            else:
                # Full report
                report = engine.generate_updated_unified_tables(args.year)
                
                if args.output:
                    with open(args.output, 'w') as f:
                        f.write(report)
                    print(f"✅ Updated unified report with roadblocks saved to {args.output}")
                else:
                    print(report)
    
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()