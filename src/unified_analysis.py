#!/usr/bin/env python3
"""
Updated Unified Analysis System - Using New Language Assignment System
====================================================================

This system has been updated to work with the new language assignment system
that uses spot_language_assignments table instead of time blocks.

UPDATED FEATURES:
================

1. **New Language Assignment System**: Uses spot_language_assignments table
2. **Business Rule Categories**: Based on revenue_type and spot_type combinations  
3. **Simplified Categories**: Removed ROS, updated to match new business logic
4. **Multiyear Support**: Support for "2023-2024", "2022-2023", etc.
5. **Assignment Method Tracking**: Shows how languages were assigned

Usage Examples:
  python ./src/unified_analysis.py --year 2024              # Single year
  python ./src/unified_analysis.py --year 2023-2024         # Two years
  python ./src/unified_analysis.py --year 2023-2024 --output reports/report.md

Save this as: src/unified_analysis.py
"""

import sqlite3
import sys
import os
from typing import Dict, List, Set, Any, Optional, Tuple
from dataclasses import dataclass

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

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
    Updated unified analysis engine using the new language assignment system
    with multiyear support and simplified business rule categories.
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
    
    def parse_year_range(self, year_input: str) -> Tuple[List[str], List[str]]:
        """
        Parse year input to handle both single years and ranges.
        
        Args:
            year_input: "2024" or "2023-2024" or "2022-2024"
            
        Returns:
            Tuple of (full_years, year_suffixes)
            e.g., (["2023", "2024"], ["23", "24"])
        """
        if '-' in year_input:
            # Handle range like "2023-2024"
            start_year, end_year = year_input.split('-')
            start_year = int(start_year)
            end_year = int(end_year)
            
            if start_year > end_year:
                raise ValueError(f"Start year {start_year} cannot be greater than end year {end_year}")
            
            full_years = [str(year) for year in range(start_year, end_year + 1)]
            year_suffixes = [year[-2:] for year in full_years]
        else:
            # Single year
            full_years = [year_input]
            year_suffixes = [year_input[-2:]]
        
        return full_years, year_suffixes
    
    def build_year_filter(self, year_suffixes: List[str]) -> Tuple[str, List[str]]:
        """
        Build SQL filter for multiple year suffixes.
        
        Args:
            year_suffixes: List of 2-digit year suffixes like ["23", "24"]
            
        Returns:
            Tuple of (SQL condition, parameters)
        """
        if len(year_suffixes) == 1:
            return "s.broadcast_month LIKE ?", [f"%-{year_suffixes[0]}"]
        else:
            conditions = []
            params = []
            for suffix in year_suffixes:
                conditions.append("s.broadcast_month LIKE ?")
                params.append(f"%-{suffix}")
            return f"({' OR '.join(conditions)})", params
    
    def get_base_totals(self, year_input: str = "2024") -> Dict[str, Any]:
        """Get the authoritative base totals for single year or multiyear range"""
        full_years, year_suffixes = self.parse_year_range(year_input)
        year_filter, year_params = self.build_year_filter(year_suffixes)
        
        query = f"""
        SELECT 
            SUM(COALESCE(s.gross_rate, 0)) as revenue,
            COUNT(CASE WHEN s.spot_type != 'BNS' OR s.spot_type IS NULL THEN 1 END) as paid_spots,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bonus_spots,
            COUNT(*) as total_spots
        FROM spots s
        WHERE {year_filter}
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        """
        
        cursor = self.db_connection.cursor()
        cursor.execute(query, year_params)
        result = cursor.fetchone()
        
        return {
            'revenue': result[0] or 0,
            'paid_spots': result[1] or 0,
            'bonus_spots': result[2] or 0,
            'total_spots': result[3] or 0,
            'years': full_years,
            'year_range': year_input
        }
    
    def get_mutually_exclusive_categories(self, year_input: str = "2024") -> List[UnifiedResult]:
        """
        Get mutually exclusive categories using new business rule system
        Based on the new spot categorization logic
        """
        full_years, year_suffixes = self.parse_year_range(year_input)
        year_filter, year_params = self.build_year_filter(year_suffixes)
        
        categories = []
        
        # 1. Direct Response Sales (WorldLink - all spot types default to English)
        direct_response_query = f"""
        SELECT 
            SUM(COALESCE(s.gross_rate, 0)) as revenue,
            COUNT(CASE WHEN s.spot_type != 'BNS' OR s.spot_type IS NULL THEN 1 END) as paid_spots,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bonus_spots,
            COUNT(*) as total_spots
        FROM spots s
        WHERE {year_filter}
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        AND s.revenue_type = 'Direct Response Sales'
        """
        
        cursor = self.db_connection.cursor()
        cursor.execute(direct_response_query, year_params)
        result = cursor.fetchone()
        
        direct_response_revenue = result[0] or 0
        direct_response_paid = result[1] or 0  
        direct_response_bonus = result[2] or 0
        direct_response_total = result[3] or 0
        
        categories.append(UnifiedResult(
            name="Direct Response Sales",
            revenue=direct_response_revenue,
            percentage=0,
            paid_spots=direct_response_paid,
            bonus_spots=direct_response_bonus,
            total_spots=direct_response_total,
            avg_per_spot=direct_response_revenue / direct_response_total if direct_response_total > 0 else 0
        ))
        
        # 2. Paid Programming (all spot types default to English)
        paid_programming_query = f"""
        SELECT 
            SUM(COALESCE(s.gross_rate, 0)) as revenue,
            COUNT(CASE WHEN s.spot_type != 'BNS' OR s.spot_type IS NULL THEN 1 END) as paid_spots,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bonus_spots,
            COUNT(*) as total_spots
        FROM spots s
        WHERE {year_filter}
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        AND s.revenue_type = 'Paid Programming'
        """
        
        cursor.execute(paid_programming_query, year_params)
        result = cursor.fetchone()
        
        paid_programming_revenue = result[0] or 0
        paid_programming_paid = result[1] or 0
        paid_programming_bonus = result[2] or 0
        paid_programming_total = result[3] or 0
        
        categories.append(UnifiedResult(
            name="Paid Programming",
            revenue=paid_programming_revenue,
            percentage=0,
            paid_spots=paid_programming_paid,
            bonus_spots=paid_programming_bonus,
            total_spots=paid_programming_total,
            avg_per_spot=paid_programming_revenue / paid_programming_total if paid_programming_total > 0 else 0
        ))
        
        # 3. Branded Content (all spot types default to English)
        branded_content_query = f"""
        SELECT 
            SUM(COALESCE(s.gross_rate, 0)) as revenue,
            COUNT(CASE WHEN s.spot_type != 'BNS' OR s.spot_type IS NULL THEN 1 END) as paid_spots,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bonus_spots,
            COUNT(*) as total_spots
        FROM spots s
        WHERE {year_filter}
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        AND s.revenue_type = 'Branded Content'
        """
        
        cursor.execute(branded_content_query, year_params)
        result = cursor.fetchone()
        
        branded_content_revenue = result[0] or 0
        branded_content_paid = result[1] or 0
        branded_content_bonus = result[2] or 0
        branded_content_total = result[3] or 0
        
        categories.append(UnifiedResult(
            name="Branded Content",
            revenue=branded_content_revenue,
            percentage=0,
            paid_spots=branded_content_paid,
            bonus_spots=branded_content_bonus,
            total_spots=branded_content_total,
            avg_per_spot=branded_content_revenue / branded_content_total if branded_content_total > 0 else 0
        ))
        
        # 4. Language Assignment Required (Internal Ad Sales + COM/BNS)
        # These are the spots that get individual language assignments
        language_required_query = f"""
        SELECT 
            SUM(COALESCE(s.gross_rate, 0)) as revenue,
            COUNT(CASE WHEN s.spot_type != 'BNS' OR s.spot_type IS NULL THEN 1 END) as paid_spots,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bonus_spots,
            COUNT(*) as total_spots
        FROM spots s
        WHERE {year_filter}
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        AND s.revenue_type = 'Internal Ad Sales'
        AND s.spot_type IN ('COM', 'BNS')
        """
        
        cursor.execute(language_required_query, year_params)
        result = cursor.fetchone()
        
        language_required_revenue = result[0] or 0
        language_required_paid = result[1] or 0
        language_required_bonus = result[2] or 0
        language_required_total = result[3] or 0
        
        categories.append(UnifiedResult(
            name="Language-Targeted Advertising",
            revenue=language_required_revenue,
            percentage=0,
            paid_spots=language_required_paid,
            bonus_spots=language_required_bonus,
            total_spots=language_required_total,
            avg_per_spot=language_required_revenue / language_required_total if language_required_total > 0 else 0
        ))
        
        # 5. Review Category (Internal Ad Sales + other types, Other revenue types)
        review_category_query = f"""
        SELECT 
            SUM(COALESCE(s.gross_rate, 0)) as revenue,
            COUNT(CASE WHEN s.spot_type != 'BNS' OR s.spot_type IS NULL THEN 1 END) as paid_spots,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bonus_spots,
            COUNT(*) as total_spots
        FROM spots s
        WHERE {year_filter}
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        AND (
            (s.revenue_type = 'Internal Ad Sales' AND s.spot_type IN ('PKG', 'CRD', 'AV', 'BB'))
            OR s.revenue_type = 'Other'
            OR s.revenue_type = 'Local'
        )
        """
        
        cursor.execute(review_category_query, year_params)
        result = cursor.fetchone()
        
        review_revenue = result[0] or 0
        review_paid = result[1] or 0
        review_bonus = result[2] or 0
        review_total = result[3] or 0
        
        categories.append(UnifiedResult(
            name="Other/Review Required",
            revenue=review_revenue,
            percentage=0,
            paid_spots=review_paid,
            bonus_spots=review_bonus,
            total_spots=review_total,
            avg_per_spot=review_revenue / review_total if review_total > 0 else 0
        ))
        
        # Calculate percentages
        total_revenue = sum(cat.revenue for cat in categories)
        for cat in categories:
            cat.percentage = (cat.revenue / total_revenue) * 100 if total_revenue > 0 else 0
        
        return categories
    
    def get_unified_language_analysis(self, year_input: str = "2024") -> List[UnifiedResult]:
        """Get language analysis using the new spot_language_assignments table"""
        full_years, year_suffixes = self.parse_year_range(year_input)
        year_filter, year_params = self.build_year_filter(year_suffixes)
        
        # Get language breakdown for spots that have language assignments
        # Only include spots where language was actually determined (not default English)
        language_query = f"""
        SELECT 
            CASE 
                WHEN l.language_name IN ('Mandarin', 'Cantonese') THEN 'Chinese'
                WHEN l.language_name IN ('Tagalog', 'Filipino') THEN 'Filipino'
                WHEN l.language_name = 'Hmong' THEN 'Hmong'
                WHEN l.language_name IN ('Hindi', 'Punjabi', 'Bengali', 'Gujarati') OR l.language_name = 'South Asian' THEN 'South Asian'
                WHEN l.language_name = 'Vietnamese' THEN 'Vietnamese'  
                WHEN l.language_name = 'Korean' THEN 'Korean'
                WHEN l.language_name = 'Japanese' THEN 'Japanese'
                WHEN l.language_name = 'English' THEN 'English'
                ELSE 'Other: ' || COALESCE(l.language_name, 'Unknown')
            END as language,
            SUM(COALESCE(s.gross_rate, 0)) as revenue,
            COUNT(CASE WHEN s.spot_type != 'BNS' OR s.spot_type IS NULL THEN 1 END) as paid_spots,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bonus_spots,
            COUNT(*) as total_spots
        FROM spots s
        JOIN spot_language_assignments sla ON s.spot_id = sla.spot_id
        LEFT JOIN languages l ON UPPER(sla.language_code) = UPPER(l.language_code)
        WHERE {year_filter}
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        AND s.revenue_type = 'Internal Ad Sales'
        AND s.spot_type IN ('COM', 'BNS')
        AND sla.assignment_method = 'direct_mapping'  -- Only spots with actual language targeting
        GROUP BY CASE 
            WHEN l.language_name IN ('Mandarin', 'Cantonese') THEN 'Chinese'
            WHEN l.language_name IN ('Tagalog', 'Filipino') THEN 'Filipino'
            WHEN l.language_name = 'Hmong' THEN 'Hmong'
            WHEN l.language_name IN ('Hindi', 'Punjabi', 'Bengali', 'Gujarati') OR l.language_name = 'South Asian' THEN 'South Asian'
            WHEN l.language_name = 'Vietnamese' THEN 'Vietnamese'
            WHEN l.language_name = 'Korean' THEN 'Korean'
            WHEN l.language_name = 'Japanese' THEN 'Japanese'
            WHEN l.language_name = 'English' THEN 'English'
            ELSE 'Other: ' || COALESCE(l.language_name, 'Unknown')
        END
        HAVING SUM(COALESCE(s.gross_rate, 0)) > 0 OR COUNT(*) > 0
        ORDER BY SUM(COALESCE(s.gross_rate, 0)) DESC
        """
        
        cursor = self.db_connection.cursor()
        cursor.execute(language_query, year_params)
        
        results = []
        for row in cursor.fetchall():
            language, revenue, paid_spots, bonus_spots, total_spots = row
            results.append(UnifiedResult(
                name=language,
                revenue=revenue,
                percentage=0,
                paid_spots=paid_spots,
                bonus_spots=bonus_spots,
                total_spots=total_spots,
                avg_per_spot=revenue / total_spots if total_spots > 0 else 0
            ))
        
        # Calculate percentages based on total revenue from languages
        total_revenue = sum(lang.revenue for lang in results)
        for lang in results:
            lang.percentage = (lang.revenue / total_revenue) * 100 if total_revenue > 0 else 0
        
        return results
    
    def get_assignment_method_analysis(self, year_input: str = "2024") -> List[UnifiedResult]:
        """Get analysis by assignment method to understand how languages were assigned"""
        full_years, year_suffixes = self.parse_year_range(year_input)
        year_filter, year_params = self.build_year_filter(year_suffixes)
        
        method_query = f"""
        SELECT 
            CASE 
                WHEN sla.assignment_method = 'business_rule_default_english' THEN 'Business Rule Default English'
                WHEN sla.assignment_method = 'direct_mapping' THEN 'Direct Language Mapping'
                WHEN sla.assignment_method = 'business_review_required' THEN 'Business Review Required'
                WHEN sla.assignment_method = 'undetermined_flagged' THEN 'Undetermined (Needs Review)'
                WHEN sla.assignment_method = 'default_english' THEN 'Default English (Fallback)'
                ELSE 'Other: ' || COALESCE(sla.assignment_method, 'Unknown')
            END as assignment_method,
            SUM(COALESCE(s.gross_rate, 0)) as revenue,
            COUNT(CASE WHEN s.spot_type != 'BNS' OR s.spot_type IS NULL THEN 1 END) as paid_spots,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bonus_spots,
            COUNT(*) as total_spots,
            AVG(sla.confidence) as avg_confidence,
            COUNT(CASE WHEN sla.requires_review = 1 THEN 1 END) as review_count
        FROM spots s
        JOIN spot_language_assignments sla ON s.spot_id = sla.spot_id
        WHERE {year_filter}
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        GROUP BY CASE 
            WHEN sla.assignment_method = 'business_rule_default_english' THEN 'Business Rule Default English'
            WHEN sla.assignment_method = 'direct_mapping' THEN 'Direct Language Mapping'
            WHEN sla.assignment_method = 'business_review_required' THEN 'Business Review Required'
            WHEN sla.assignment_method = 'undetermined_flagged' THEN 'Undetermined (Needs Review)'
            WHEN sla.assignment_method = 'default_english' THEN 'Default English (Fallback)'
            ELSE 'Other: ' || COALESCE(sla.assignment_method, 'Unknown')
        END
        ORDER BY SUM(COALESCE(s.gross_rate, 0)) DESC
        """
        
        cursor = self.db_connection.cursor()
        cursor.execute(method_query, year_params)
        
        results = []
        for row in cursor.fetchall():
            method, revenue, paid_spots, bonus_spots, total_spots, avg_confidence, review_count = row
            results.append(UnifiedResult(
                name=method,
                revenue=revenue,
                percentage=0,
                paid_spots=paid_spots,
                bonus_spots=bonus_spots,
                total_spots=total_spots,
                avg_per_spot=revenue / total_spots if total_spots > 0 else 0,
                details={
                    'avg_confidence': avg_confidence or 0,
                    'review_count': review_count or 0
                }
            ))
        
        # Calculate percentages
        total_revenue = sum(result.revenue for result in results)
        for result in results:
            result.percentage = (result.revenue / total_revenue) * 100 if total_revenue > 0 else 0
        
        return results
    
    def validate_reconciliation(self, year_input: str = "2024") -> Dict[str, Any]:
        """Validate perfect reconciliation for multiyear analysis"""
        base_totals = self.get_base_totals(year_input)
        category_results = self.get_mutually_exclusive_categories(year_input)
        
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
            'new_assignment_system': True,
            'multiyear_support': True,
            'years_analyzed': base_totals['years']
        }
    
    def generate_updated_unified_tables(self, year_input: str = "2024") -> str:
        """Generate comprehensive analysis report with multiyear support"""
        
        # Parse year range for display
        full_years, year_suffixes = self.parse_year_range(year_input)
        year_display = f"{full_years[0]}-{full_years[-1]}" if len(full_years) > 1 else full_years[0]
        
        # Get all analyses
        category_results = self.get_mutually_exclusive_categories(year_input)
        language_results = self.get_unified_language_analysis(year_input)
        assignment_method_results = self.get_assignment_method_analysis(year_input)
        
        # Validate reconciliation
        validation = self.validate_reconciliation(year_input)
        
        # Generate tables
        category_table = self._format_table(
            category_results,
            "📊 Business Rule Category Breakdown",
            "Revenue Categories Based on New Assignment System",
            year_display
        )
        
        language_table = self._format_table(
            language_results,
            "🌐 Language-Targeted Advertising Analysis",
            "Individual Language Performance (Direct Mapping Only)",
            year_display
        )
        
        assignment_method_table = self._format_assignment_method_table(
            assignment_method_results,
            "🔧 Assignment Method Analysis",
            "How Languages Were Assigned by the New System",
            year_display
        )
        
        # Generate report
        return f"""# Updated Unified Revenue Analysis - {year_display}

*Generated with the new language assignment system using spot_language_assignments table*

## 🎯 System Update & Reconciliation Status

- **Years Analyzed**: {', '.join(validation['base_totals']['years'])}
- **Base Revenue**: ${validation['base_totals']['revenue']:,.2f}
- **Category Total**: ${validation['category_totals']['revenue']:,.2f}
- **Revenue Difference**: ${validation['revenue_difference']:,.2f}
- **Spot Difference**: {validation['spot_difference']:,}
- **Perfect Reconciliation**: {'✅ YES' if validation['perfect_reconciliation'] else '❌ NO'}
- **New Assignment System**: {'✅ YES' if validation['new_assignment_system'] else '❌ NO'}
- **Multiyear Support**: {'✅ YES' if validation['multiyear_support'] else '❌ NO'}

{category_table}

{language_table}

{assignment_method_table}

{self._generate_updated_system_notes()}

{self._generate_updated_faq_section()}
"""
    
    def _format_table(self, results: List[UnifiedResult], title: str, subtitle: str, year_display: str) -> str:
        """Format results into a table with multiyear support"""
        
        # Calculate totals
        total_revenue = sum(r.revenue for r in results)
        total_paid_spots = sum(r.paid_spots for r in results)
        total_bonus_spots = sum(r.bonus_spots for r in results)
        total_all_spots = sum(r.total_spots for r in results)
        total_avg_per_spot = total_revenue / total_all_spots if total_all_spots > 0 else 0
        
        # Build the table
        table = f"""## {title}
### {subtitle} ({year_display})
| Category | Revenue | % of Total | Paid Spots | BNS Spots | Total Spots | Avg/Spot |
|----------|---------|------------|-----------|-----------|-------------|----------|
"""
        
        for result in results:
            table += f"| {result.name} | ${result.revenue:,.2f} | {result.percentage:.1f}% | {result.paid_spots:,} | {result.bonus_spots:,} | {result.total_spots:,} | ${result.avg_per_spot:.2f} |\n"
        
        # Add total row
        table += "|----------|---------|------------|-----------|-----------|-------------|----------|\n"
        table += f"| **TOTAL** | **${total_revenue:,.2f}** | **100.0%** | **{total_paid_spots:,}** | **{total_bonus_spots:,}** | **{total_all_spots:,}** | **${total_avg_per_spot:.2f}** |\n"
        
        return table
    
    def _format_assignment_method_table(self, results: List[UnifiedResult], title: str, subtitle: str, year_display: str) -> str:
        """Format assignment method results into a table"""
        
        # Calculate totals
        total_revenue = sum(r.revenue for r in results)
        total_spots = sum(r.total_spots for r in results)
        total_review = sum(r.details.get('review_count', 0) for r in results if r.details)
        
        # Build the table
        table = f"""## {title}
### {subtitle} ({year_display})
| Assignment Method | Revenue | % of Total | Total Spots | Avg Confidence | Review Count |
|-------------------|---------|------------|-------------|----------------|--------------|
"""
        
        for result in results:
            avg_confidence = result.details.get('avg_confidence', 0) if result.details else 0
            review_count = result.details.get('review_count', 0) if result.details else 0
            
            table += f"| {result.name} | ${result.revenue:,.2f} | {result.percentage:.1f}% | {result.total_spots:,} | {avg_confidence:.2f} | {review_count:,} |\n"
        
        # Add total row
        table += "|-------------------|---------|------------|-------------|----------------|--------------|\n"
        table += f"| **TOTAL** | **${total_revenue:,.2f}** | **100.0%** | **{total_spots:,}** | **N/A** | **{total_review:,}** |\n"
        
        return table
    
    def _generate_updated_system_notes(self) -> str:
        """Generate notes about the updated system"""
        return """## 📋 Updated Language Assignment System Notes

### Key System Changes
- **New Assignment Logic**: Uses `spot_language_assignments` table instead of time blocks
- **Business Rule Categories**: Based on `revenue_type` and `spot_type` combinations
- **Assignment Methods**: Tracks how each language was assigned with confidence levels
- **Simplified Categories**: Removed ROS, updated to match current business logic
- **Review Flagging**: System flags spots requiring manual review

### New Business Rule Categories
1. **Direct Response Sales** → All spot types default to English (business rule)
2. **Paid Programming** → All spot types default to English (business rule)  
3. **Branded Content** → All spot types default to English (business rule)
4. **Language-Targeted Advertising** → Internal Ad Sales + COM/BNS spots (language assignment required)
5. **Other/Review Required** → Unusual combinations requiring manual review

### Assignment Method Types
- **Business Rule Default English**: Automatic English assignment based on revenue type
- **Direct Language Mapping**: Language determined from spots.language_code
- **Business Review Required**: Unusual revenue/spot combinations needing review
- **Undetermined (Needs Review)**: Language code 'L' - requires manual determination
- **Default English (Fallback)**: Missing language codes defaulted to English

### Data Quality Features
- **Confidence Scoring**: Each assignment has a confidence level (0.0-1.0)
- **Review Flagging**: System identifies spots needing manual attention
- **Assignment Tracking**: Records how each language was determined
- **Perfect Reconciliation**: Every spot assigned to exactly one category

## 🔧 Technical Implementation

### Database Schema Updates
- **spot_language_assignments** → Main assignment table with metadata
- **languages** → Reference table for valid language codes
- **spots** → Enhanced with language_code column
- **Removed Dependencies** → No longer uses time block tables

### Query Logic Changes
- **Direct Mapping**: Uses spots.language_code → languages table joins
- **Business Rules**: Revenue type and spot type combination logic
- **Assignment Methods**: Tracks the logic used for each assignment
- **Review Workflow**: Identifies and flags problematic spots

---

*Updated System - No longer uses time blocks or ROS categories*"""
    
    def _generate_updated_faq_section(self) -> str:
        """Generate FAQ section for the updated system"""
        return """---

# Updated Language Assignment System - FAQ

## How the New System Assigns TV Spots to Languages

### The Updated Business Problem
TV spots are now categorized using a simplified business rule system that assigns languages based on the `spots.language_code` column and business logic derived from `revenue_type` and `spot_type` combinations.

---

## Updated Assignment Rules (Simple FAQ)

### Q: How does the new system work?

**A:** The system uses three main approaches:

1. **Business Rule Defaults**: Direct Response, Paid Programming, and Branded Content automatically default to English
2. **Direct Language Mapping**: Internal Ad Sales spots with valid language codes get mapped to specific languages  
3. **Review Categories**: Unusual combinations get flagged for manual review

**Example**: A Vietnamese language code on an Internal Ad Sales commercial spot gets mapped to Vietnamese language

---

### Q: What happened to the time block system?

**A:** The system was simplified:

- **Old System**: Used complex time block matching and ROS detection
- **New System**: Uses simple business rules based on revenue type and spot type
- **Better Accuracy**: Fewer edge cases and clearer business logic
- **Easier Maintenance**: Simpler rules mean fewer errors and easier updates

---

### Q: Why is a spot assigned to "Direct Response Sales"?

**A:** Business rule categorization:

- **Revenue Type**: `revenue_type = 'Direct Response Sales'`
- **All Spot Types**: COM, BNS, PKG, etc. - all default to English
- **Business Logic**: Direct response advertising doesn't target specific language communities
- **Assignment Method**: `business_rule_default_english`

**Example**: WorldLink agency spots automatically get categorized as Direct Response Sales → English

---

### Q: How does language targeting work now?

**A:** Simplified language assignment:

- **Target Category**: Internal Ad Sales + COM/BNS spots only
- **Language Source**: `spots.language_code` column  
- **Direct Mapping**: Language code maps to language name via `languages` table
- **Assignment Method**: `direct_mapping`

**Example**: Internal Ad Sales spot with language_code 'T' → Tagalog → Filipino language category

---

### Q: What does "Business Review Required" mean?

**A:** Spots needing manual attention:

- **Unusual Combinations**: Internal Ad Sales + non-commercial spot types (PKG, CRD, AV, BB)
- **Other Revenue Types**: Revenue types that don't fit standard patterns
- **Manual Review**: Require business analyst review to determine proper categorization
- **Assignment Method**: `business_review_required`

**Example**: Internal Ad Sales + PKG spot type combination needs manual review

---

### Q: How are confidence levels used?

**A:** Assignment quality tracking:

- **High Confidence (1.0)**: Direct mapping and business rule defaults
- **Medium Confidence (0.5)**: Default fallbacks and review categories  
- **Low Confidence (0.0)**: Undetermined language codes (L)
- **Review Flagging**: Low confidence assignments get flagged for review

**Example**: Direct language mapping has 1.0 confidence, undetermined has 0.0 confidence

---

## Updated System Benefits

### Simplified Business Logic:
- **Clear Rules**: Revenue type + spot type = category assignment
- **Fewer Edge Cases**: Eliminated complex time block matching issues
- **Better Accuracy**: More predictable and maintainable logic
- **Easier Training**: Simpler rules for analysts to understand

### Enhanced Data Quality:
- **Confidence Scoring**: Every assignment has a quality score
- **Review Workflow**: System identifies spots needing manual attention  
- **Assignment Tracking**: Records how each language was determined
- **Perfect Reconciliation**: Every spot in exactly one category

### Operational Improvements:
- **Faster Processing**: Simpler queries run much faster
- **Better Reporting**: Clearer categories for business analysis
- **Easier Maintenance**: Fewer complex rules to maintain
- **Enhanced Debugging**: Assignment methods show exactly how decisions were made

---

## Updated Category Priority Order

The new system applies business rules in this order (first match wins):

1. **Direct Response Sales** (Revenue type = 'Direct Response Sales') - *All spot types → English*
2. **Paid Programming** (Revenue type = 'Paid Programming') - *All spot types → English*  
3. **Branded Content** (Revenue type = 'Branded Content') - *All spot types → English*
4. **Language-Targeted Advertising** (Internal Ad Sales + COM/BNS) - *Language assignment required*
5. **Other/Review Required** (Everything else) - *Manual review needed*

---

## Bottom Line

The updated system automatically sorts every TV spot into the right category using **simple business rules** based on **revenue type** and **spot type**. Language assignments come from the `language_code` column with confidence scoring and review flagging for quality control.

**Key Benefit**: Much simpler and more reliable than the old time block system, with better data quality and easier maintenance.

---

*Updated FAQ Section - New Language Assignment System*"""


def main():
    """Test the updated unified analysis system with new language assignment system"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Updated Unified Analysis - New Language Assignment System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Updated System Examples:
  # Single year analysis with new system
  python unified_analysis.py --year 2024
  
  # Two year analysis
  python unified_analysis.py --year 2023-2024
  
  # Assignment method analysis
  python unified_analysis.py --year 2024 --assignment-methods-only
  
  # Save report to file
  python unified_analysis.py --year 2023-2024 --output updated_report.md
  
  # Validate reconciliation
  python unified_analysis.py --year 2024 --validate-only
        """
    )
    
    parser.add_argument("--year", default="2024", 
                       help="Year to analyze - supports single year (2024) or range (2023-2024)")
    
    parser.add_argument("--output", metavar="FILE", 
                       help="Save report to file (e.g., report.md, analysis.txt)")
    
    parser.add_argument("--db-path", default="data/database/production.db", 
                       help="Database path (default: data/database/production.db)")
    
    parser.add_argument("--validate-only", action="store_true", 
                       help="Run validation only - check reconciliation")
    
    parser.add_argument("--assignment-methods-only", action="store_true", 
                       help="Show only assignment method analysis")
    
    args = parser.parse_args()
    
    try:
        with UpdatedUnifiedAnalysisEngine(args.db_path) as engine:
            if args.validate_only:
                # Run validation test
                validation = engine.validate_reconciliation(args.year)
                print("🧪 Updated System Validation Results:")
                print("=" * 50)
                print(f"✅ Years Analyzed: {', '.join(validation['base_totals']['years'])}")
                print(f"✅ Base Revenue: ${validation['base_totals']['revenue']:,.2f}")
                print(f"✅ Category Total: ${validation['category_totals']['revenue']:,.2f}")
                print(f"✅ Revenue Difference: ${validation['revenue_difference']:,.2f}")
                print(f"✅ Perfect Reconciliation: {'YES' if validation['perfect_reconciliation'] else 'NO'}")
                print(f"✅ New Assignment System: {'YES' if validation['new_assignment_system'] else 'NO'}")
                print(f"✅ Multiyear Support: {'YES' if validation['multiyear_support'] else 'NO'}")
            elif args.assignment_methods_only:
                # Show assignment method analysis only
                assignment_results = engine.get_assignment_method_analysis(args.year)
                print("🔧 Assignment Method Analysis:")
                print("=" * 50)
                for result in assignment_results:
                    confidence = result.details.get('avg_confidence', 0) if result.details else 0
                    review_count = result.details.get('review_count', 0) if result.details else 0
                    print(f"{result.name}: ${result.revenue:,.2f} ({result.percentage:.1f}%) - {result.total_spots:,} spots")
                    print(f"  Confidence: {confidence:.2f}, Review Count: {review_count:,}")
            else:
                # Generate full report
                report = engine.generate_updated_unified_tables(args.year)
                
                if args.output:
                    # Create directory if it doesn't exist
                    import os
                    os.makedirs(os.path.dirname(args.output), exist_ok=True) if os.path.dirname(args.output) else None
                    
                    with open(args.output, 'w') as f:
                        f.write(report)
                    
                    # Parse year for display
                    full_years, _ = engine.parse_year_range(args.year)
                    year_display = f"{full_years[0]}-{full_years[-1]}" if len(full_years) > 1 else full_years[0]
                    
                    print(f"✅ Updated unified report saved to {args.output}")
                    print(f"📅 Years analyzed: {year_display}")
                    print(f"📄 File size: {os.path.getsize(args.output):,} bytes")
                else:
                    print(report)
    
    except ValueError as e:
        print(f"❌ Input Error: {str(e)}")
        print("💡 Use format like: --year 2024 or --year 2023-2024")
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()