#!/usr/bin/env python3
"""
Multi-Language Analyzer - Production Ready (Fixed)
==========================================

Production-ready multi-language (cross-audience) analysis module with proper integration
into the existing BaseQueryBuilder system.

Key features:
- Cross-audience spot analysis (spans multiple language blocks)
- Customer and agency analysis
- Time pattern analysis
- Language span analysis
- Integration with existing query builder foundation

Save as: src/multi_language_analyzer.py
"""

import sqlite3
from typing import Dict, List, Set, Any, Optional, Tuple
from dataclasses import dataclass

try:
    from .query_builders import BaseQueryBuilder
except ImportError:
    try:
        from query_builders import BaseQueryBuilder
    except ImportError:
        # Fallback if BaseQueryBuilder not available
        class BaseQueryBuilder:
            def __init__(self, year: str = "2024"):
                self.year = year
                self.year_suffix = year[-2:]
                self.filters = []
                self.joins = []
                self.base_table = "spots s"
                self._added_joins = set()
            
            def apply_standard_filters(self):
                self.add_filter(f"s.broadcast_month LIKE '%-{self.year_suffix}'")
                self.add_filter("(s.revenue_type != 'Trade' OR s.revenue_type IS NULL)")
                self.add_filter("(s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')")
                return self
            
            def add_filter(self, condition: str):
                self.filters.append(condition)
                return self
            
            def add_left_join(self, table: str, condition: str):
                join_key = table.split(' ')[0]
                if join_key not in self._added_joins:
                    self.joins.append(f"LEFT JOIN {table} ON {condition}")
                    self._added_joins.add(join_key)
                return self
            
            def build_from_clause(self) -> str:
                result = f"FROM {self.base_table}"
                if self.joins:
                    result += "\n" + "\n".join(self.joins)
                return result
            
            def build_where_clause(self) -> str:
                if not self.filters:
                    return ""
                return "WHERE " + " AND ".join(self.filters)


@dataclass
class MultiLanguageSummary:
    """Multi-language summary statistics"""
    total_spots: int
    paid_spots: int
    bns_spots: int
    total_revenue: float
    avg_per_spot: float
    bns_percentage: float
    unique_customers: int
    unique_agencies: int


@dataclass
class MultiLanguageTimePattern:
    """Time pattern analysis for multi-language spots"""
    time_range: str
    day_of_week: str
    spots: int
    paid_spots: int
    bns_spots: int
    revenue: float
    avg_per_spot: float
    unique_customers: int


@dataclass
class MultiLanguageCustomer:
    """Customer analysis for multi-language spots"""
    customer_name: str
    total_spots: int
    paid_spots: int
    bns_spots: int
    revenue: float
    avg_per_spot: float
    unique_days: int
    time_spread: str
    primary_agency: str


@dataclass
class MultiLanguageAgency:
    """Agency analysis for multi-language spots"""
    agency_name: str
    total_spots: int
    paid_spots: int
    bns_spots: int
    revenue: float
    avg_per_spot: float
    unique_customers: int
    customer_list: str


@dataclass
class LanguageSpanAnalysis:
    """Analysis of language combinations for multi-language spots"""
    span_type: str
    description: str
    spots: int
    revenue: float
    avg_per_spot: float
    unique_customers: int


class MultiLanguageQueryBuilder(BaseQueryBuilder):
    """Production-ready multi-language query builder"""
    
    def __init__(self, year: str = "2024"):
        super().__init__(year)
        self.apply_standard_filters()
        self.exclude_worldlink()
        self.add_language_block_join()
        self.add_customer_and_agency_joins()
    
    def exclude_worldlink(self):
        """Exclude WorldLink spots"""
        self.add_left_join("agencies a", "s.agency_id = a.agency_id")
        self.add_filter("COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'")
        self.add_filter("COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'")
        return self
    
    def add_language_block_join(self):
        """Add language block join"""
        self.add_left_join("spot_language_blocks slb", "s.spot_id = slb.spot_id")
        return self
    
    def add_customer_and_agency_joins(self):
        """Add customer and agency joins for analysis"""
        self.add_left_join("customers c", "s.customer_id = c.customer_id")
        # Agency join already added by exclude_worldlink()
        return self
    
    def add_multi_language_conditions(self):
        """Add multi-language specific conditions"""
        self.add_filter("""(
            slb.spans_multiple_blocks = 1 OR 
            (slb.spans_multiple_blocks = 0 AND slb.block_id IS NULL) OR 
            (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NULL)
        )""")
        return self
    
    def exclude_nkb_spots(self):
        """Exclude NKB spots (they go to overnight shopping)"""
        self.add_filter("COALESCE(c.normalized_name, '') NOT LIKE '%NKB%'")
        self.add_filter("COALESCE(s.bill_code, '') NOT LIKE '%NKB%'")
        self.add_filter("COALESCE(a.agency_name, '') NOT LIKE '%NKB%'")
        return self
    
    def exclude_roadblocks(self):
        """Exclude roadblocks (they have their own category)"""
        self.add_filter("COALESCE(slb.campaign_type, '') != 'roadblock'")
        return self
    
    def add_language_block_details_join(self):
        """Add language block details for span analysis"""
        self.add_left_join("language_blocks lb", "slb.block_id = lb.block_id")
        self.add_left_join("languages l", "lb.language_id = l.language_id")
        return self
    
    def build_summary_query(self) -> str:
        """Build multi-language summary query"""
        return f"""
        SELECT 
            COUNT(*) as total_spots,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bns_spots,
            COUNT(CASE WHEN s.spot_type != 'BNS' OR s.spot_type IS NULL THEN 1 END) as paid_spots,
            SUM(COALESCE(s.gross_rate, 0)) as total_revenue,
            AVG(COALESCE(s.gross_rate, 0)) as avg_per_spot,
            COUNT(DISTINCT c.customer_id) as unique_customers,
            COUNT(DISTINCT a.agency_id) as unique_agencies
        {self.build_from_clause()}
        {self.build_where_clause()}
        """
    
    def build_time_pattern_query(self) -> str:
        """Build time pattern analysis query"""
        return f"""
        SELECT 
            COALESCE(s.time_in, 'Unknown') || ' to ' || COALESCE(s.time_out, 'Unknown') as time_range,
            COALESCE(s.day_of_week, 'Unknown') as day_of_week,
            COUNT(*) as spots,
            COUNT(CASE WHEN s.spot_type != 'BNS' OR s.spot_type IS NULL THEN 1 END) as paid_spots,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bns_spots,
            SUM(COALESCE(s.gross_rate, 0)) as revenue,
            AVG(COALESCE(s.gross_rate, 0)) as avg_per_spot,
            COUNT(DISTINCT c.customer_id) as unique_customers
        {self.build_from_clause()}
        {self.build_where_clause()}
        GROUP BY COALESCE(s.time_in, 'Unknown'), COALESCE(s.time_out, 'Unknown'), COALESCE(s.day_of_week, 'Unknown')
        ORDER BY revenue DESC
        LIMIT 20
        """
    
    def build_customer_query(self) -> str:
        """Build customer analysis query"""
        return f"""
        SELECT 
            COALESCE(c.normalized_name, 'Unknown') as customer_name,
            COUNT(*) as total_spots,
            COUNT(CASE WHEN s.spot_type != 'BNS' OR s.spot_type IS NULL THEN 1 END) as paid_spots,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bns_spots,
            SUM(COALESCE(s.gross_rate, 0)) as revenue,
            AVG(COALESCE(s.gross_rate, 0)) as avg_per_spot,
            COUNT(DISTINCT s.day_of_week) as unique_days,
            MIN(s.time_in) as earliest_time,
            MAX(s.time_out) as latest_time,
            COALESCE(a.agency_name, 'Unknown') as primary_agency
        {self.build_from_clause()}
        {self.build_where_clause()}
        GROUP BY COALESCE(c.normalized_name, 'Unknown'), COALESCE(a.agency_name, 'Unknown')
        ORDER BY revenue DESC
        LIMIT 25
        """
    
    def build_agency_query(self) -> str:
        """Build agency analysis query (fixed GROUP_CONCAT)"""
        return f"""
        SELECT 
            COALESCE(a.agency_name, 'Unknown') as agency_name,
            COUNT(*) as total_spots,
            COUNT(CASE WHEN s.spot_type != 'BNS' OR s.spot_type IS NULL THEN 1 END) as paid_spots,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bns_spots,
            SUM(COALESCE(s.gross_rate, 0)) as revenue,
            AVG(COALESCE(s.gross_rate, 0)) as avg_per_spot,
            COUNT(DISTINCT c.customer_id) as unique_customers,
            GROUP_CONCAT(DISTINCT c.normalized_name) as customer_list
        {self.build_from_clause()}
        {self.build_where_clause()}
        GROUP BY COALESCE(a.agency_name, 'Unknown')
        ORDER BY revenue DESC
        LIMIT 15
        """
    
    def build_language_span_query(self) -> str:
        """Build language span analysis query"""
        return f"""
        SELECT 
            CASE 
                WHEN slb.spans_multiple_blocks = 1 THEN 'Multi-Block Span'
                WHEN slb.block_id IS NULL THEN 'No Block Assignment'
                ELSE 'Single Block'
            END as span_type,
            CASE 
                WHEN slb.spans_multiple_blocks = 1 THEN 'Customer chose to span multiple language blocks'
                WHEN slb.block_id IS NULL THEN 'No specific language block assignment'
                ELSE 'Single language block (should not appear here)'
            END as description,
            COUNT(*) as spots,
            SUM(COALESCE(s.gross_rate, 0)) as revenue,
            AVG(COALESCE(s.gross_rate, 0)) as avg_per_spot,
            COUNT(DISTINCT c.customer_id) as unique_customers
        {self.build_from_clause()}
        {self.build_where_clause()}
        GROUP BY slb.spans_multiple_blocks, slb.block_id IS NULL
        ORDER BY revenue DESC
        """
    
    def build_spot_ids_query(self) -> str:
        """Build query to get multi-language spot IDs for unified analysis integration"""
        return f"""
        SELECT DISTINCT s.spot_id
        {self.build_from_clause()}
        {self.build_where_clause()}
        """


class MultiLanguageAnalyzer:
    """Production-ready multi-language analyzer"""
    
    def __init__(self, db_connection):
        self.db_connection = db_connection
    
    def get_summary(self, year: str = "2024") -> MultiLanguageSummary:
        """Get multi-language summary statistics"""
        builder = MultiLanguageQueryBuilder(year)
        builder.add_multi_language_conditions().exclude_nkb_spots().exclude_roadblocks()
        
        query = builder.build_summary_query()
        cursor = self.db_connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        
        total_spots = result[0] or 0
        bns_spots = result[1] or 0
        paid_spots = result[2] or 0
        total_revenue = result[3] or 0
        avg_per_spot = result[4] or 0
        unique_customers = result[5] or 0
        unique_agencies = result[6] or 0
        
        bns_percentage = (bns_spots / total_spots) * 100 if total_spots > 0 else 0
        
        return MultiLanguageSummary(
            total_spots=total_spots,
            paid_spots=paid_spots,
            bns_spots=bns_spots,
            total_revenue=total_revenue,
            avg_per_spot=avg_per_spot,
            bns_percentage=bns_percentage,
            unique_customers=unique_customers,
            unique_agencies=unique_agencies
        )
    
    def get_time_patterns(self, year: str = "2024") -> List[MultiLanguageTimePattern]:
        """Get time pattern analysis"""
        builder = MultiLanguageQueryBuilder(year)
        builder.add_multi_language_conditions().exclude_nkb_spots().exclude_roadblocks()
        
        query = builder.build_time_pattern_query()
        cursor = self.db_connection.cursor()
        cursor.execute(query)
        
        patterns = []
        for row in cursor.fetchall():
            patterns.append(MultiLanguageTimePattern(
                time_range=row[0],
                day_of_week=row[1],
                spots=row[2],
                paid_spots=row[3],
                bns_spots=row[4],
                revenue=row[5],
                avg_per_spot=row[6],
                unique_customers=row[7]
            ))
        
        return patterns
    
    def get_customers(self, year: str = "2024") -> List[MultiLanguageCustomer]:
        """Get customer analysis"""
        builder = MultiLanguageQueryBuilder(year)
        builder.add_multi_language_conditions().exclude_nkb_spots().exclude_roadblocks()
        
        query = builder.build_customer_query()
        cursor = self.db_connection.cursor()
        cursor.execute(query)
        
        customers = []
        for row in cursor.fetchall():
            time_spread = f"{row[7]} to {row[8]}" if row[7] and row[8] else "Unknown"
            customers.append(MultiLanguageCustomer(
                customer_name=row[0],
                total_spots=row[1],
                paid_spots=row[2],
                bns_spots=row[3],
                revenue=row[4],
                avg_per_spot=row[5],
                unique_days=row[6],
                time_spread=time_spread,
                primary_agency=row[9]
            ))
        
        return customers
    
    def get_agencies(self, year: str = "2024") -> List[MultiLanguageAgency]:
        """Get agency analysis (fixed customer list processing)"""
        builder = MultiLanguageQueryBuilder(year)
        builder.add_multi_language_conditions().exclude_nkb_spots().exclude_roadblocks()
        
        query = builder.build_agency_query()
        cursor = self.db_connection.cursor()
        cursor.execute(query)
        
        agencies = []
        for row in cursor.fetchall():
            # Handle customer list - GROUP_CONCAT uses comma as default separator
            customer_list = row[7] if row[7] else "Unknown"
            
            # Replace default comma with comma-space for better formatting
            if customer_list and customer_list != "Unknown":
                customer_list = customer_list.replace(',', ', ')
            
            # Truncate customer list if too long
            if len(customer_list) > 100:
                customer_list = customer_list[:97] + "..."
            
            agencies.append(MultiLanguageAgency(
                agency_name=row[0],
                total_spots=row[1],
                paid_spots=row[2],
                bns_spots=row[3],
                revenue=row[4],
                avg_per_spot=row[5],
                unique_customers=row[6],
                customer_list=customer_list
            ))
        
        return agencies
    
    def get_language_spans(self, year: str = "2024") -> List[LanguageSpanAnalysis]:
        """Get language span analysis"""
        builder = MultiLanguageQueryBuilder(year)
        builder.add_multi_language_conditions().exclude_nkb_spots().exclude_roadblocks()
        
        query = builder.build_language_span_query()
        cursor = self.db_connection.cursor()
        cursor.execute(query)
        
        spans = []
        for row in cursor.fetchall():
            spans.append(LanguageSpanAnalysis(
                span_type=row[0],
                description=row[1],
                spots=row[2],
                revenue=row[3],
                avg_per_spot=row[4],
                unique_customers=row[5]
            ))
        
        return spans
    
    def get_spot_ids(self, year: str = "2024") -> Set[int]:
        """Get multi-language spot IDs for unified analysis integration"""
        builder = MultiLanguageQueryBuilder(year)
        builder.add_multi_language_conditions().exclude_nkb_spots().exclude_roadblocks()
        
        query = builder.build_spot_ids_query()
        cursor = self.db_connection.cursor()
        cursor.execute(query)
        
        return set(row[0] for row in cursor.fetchall())
    
    def get_campaign_intent_breakdown(self, year: str = "2024") -> Dict[str, Any]:
        """Get campaign intent breakdown"""
        builder = MultiLanguageQueryBuilder(year)
        builder.add_multi_language_conditions().exclude_nkb_spots().exclude_roadblocks()
        
        query = f"""
        SELECT 
            COALESCE(slb.customer_intent, 'Unknown') as intent,
            COUNT(*) as spots,
            SUM(COALESCE(s.gross_rate, 0)) as revenue,
            AVG(COALESCE(s.gross_rate, 0)) as avg_per_spot,
            COUNT(DISTINCT c.customer_id) as unique_customers
        {builder.build_from_clause()}
        {builder.build_where_clause()}
        GROUP BY COALESCE(slb.customer_intent, 'Unknown')
        ORDER BY revenue DESC
        """
        
        cursor = self.db_connection.cursor()
        cursor.execute(query)
        
        intent_breakdown = []
        for row in cursor.fetchall():
            intent_breakdown.append({
                'intent': row[0],
                'spots': row[1],
                'revenue': row[2],
                'avg_per_spot': row[3],
                'unique_customers': row[4]
            })
        
        return {
            'breakdown': intent_breakdown,
            'total_intents': len(intent_breakdown)
        }


class MultiLanguageReportGenerator:
    """Production-ready multi-language report generator"""
    
    def __init__(self, db_connection):
        self.analyzer = MultiLanguageAnalyzer(db_connection)
    
    def generate_report(self, year: str = "2024") -> str:
        """Generate comprehensive multi-language report"""
        summary = self.analyzer.get_summary(year)
        time_patterns = self.analyzer.get_time_patterns(year)
        customers = self.analyzer.get_customers(year)
        agencies = self.analyzer.get_agencies(year)
        language_spans = self.analyzer.get_language_spans(year)
        intent_breakdown = self.analyzer.get_campaign_intent_breakdown(year)
        
        return f"""# Multi-Language (Cross-Audience) Analysis Report - {year}

*Generated by Production Multi-Language Analyzer*

## 🌍 Executive Summary

### Performance Metrics
- **Total Revenue**: ${summary.total_revenue:,.2f}
- **Total Spots**: {summary.total_spots:,}
- **Paid Spots**: {summary.paid_spots:,} ({100 - summary.bns_percentage:.1f}%)
- **BNS Spots**: {summary.bns_spots:,} ({summary.bns_percentage:.1f}%)
- **Average per Spot**: ${summary.avg_per_spot:.2f}
- **Unique Customers**: {summary.unique_customers:,}
- **Unique Agencies**: {summary.unique_agencies:,}

### Key Insights
- **Cross-audience strategy**: Spots targeting multiple language communities
- **{summary.bns_percentage:.1f}% are BNS spots** (bonus content enhancing cross-cultural reach)
- **Customer diversity**: {summary.unique_customers:,} customers using cross-audience approach
- **Agency engagement**: {summary.unique_agencies:,} agencies managing cross-audience campaigns

{self._format_language_spans(language_spans)}

{self._format_time_patterns(time_patterns)}

{self._format_customers(customers)}

{self._format_agencies(agencies)}

{self._format_intent_breakdown(intent_breakdown)}

## 📊 Technical Details

**Multi-Language Definition:**
- **spans_multiple_blocks = 1**: Customer explicitly chose to span multiple language blocks
- **block_id IS NULL**: No specific language block assignment (cross-audience placement)
- **Excludes**: WorldLink (Direct Response), NKB (Overnight Shopping), Roadblocks

**Revenue Calculation:**
- **BNS Spots**: spot_type = 'BNS' (bonus content for community engagement)
- **Paid Spots**: Revenue-generating cross-audience advertising
- **Total Revenue**: Sum of paid spots only

**Business Value:**
- **Cross-cultural reach**: Advertising spans multiple language communities
- **Flexible scheduling**: Not tied to specific language programming blocks
- **Broader audience**: Maximizes reach across diverse viewing audience

---

*Generated by Production Multi-Language Analyzer v1.0*
"""
    
    def _format_language_spans(self, spans: List[LanguageSpanAnalysis]) -> str:
        """Format language spans section"""
        if not spans:
            return "### No language spans found."
        
        section = """## 🎯 Language Span Analysis

| Span Type | Description | Spots | Revenue | Avg/Spot | Customers |
|-----------|-------------|-------|---------|----------|-----------|
"""
        
        for span in spans:
            section += f"| {span.span_type} | {span.description} | {span.spots:,} | ${span.revenue:,.2f} | ${span.avg_per_spot:.2f} | {span.unique_customers:,} |\n"
        
        return section
    
    def _format_time_patterns(self, patterns: List[MultiLanguageTimePattern]) -> str:
        """Format time patterns section"""
        if not patterns:
            return "### No time patterns found."
        
        section = """## 📅 Time Pattern Analysis

| Time Range | Day of Week | Total Spots | Paid Spots | BNS Spots | Revenue | Avg/Spot | Customers |
|------------|-------------|-------------|------------|-----------|---------|----------|-----------|
"""
        
        for pattern in patterns:
            section += f"| {pattern.time_range} | {pattern.day_of_week} | {pattern.spots:,} | {pattern.paid_spots:,} | {pattern.bns_spots:,} | ${pattern.revenue:,.2f} | ${pattern.avg_per_spot:.2f} | {pattern.unique_customers:,} |\n"
        
        return section
    
    def _format_customers(self, customers: List[MultiLanguageCustomer]) -> str:
        """Format customers section"""
        if not customers:
            return "### No customers found."
        
        section = """## 👥 Customer Analysis

| Customer | Total Spots | Paid Spots | BNS Spots | Revenue | Avg/Spot | Days | Time Spread | Primary Agency |
|----------|-------------|------------|-----------|---------|----------|------|-------------|----------------|
"""
        
        for customer in customers:
            section += f"| {customer.customer_name} | {customer.total_spots:,} | {customer.paid_spots:,} | {customer.bns_spots:,} | ${customer.revenue:,.2f} | ${customer.avg_per_spot:.2f} | {customer.unique_days} | {customer.time_spread} | {customer.primary_agency} |\n"
        
        return section
    
    def _format_agencies(self, agencies: List[MultiLanguageAgency]) -> str:
        """Format agencies section"""
        if not agencies:
            return "### No agencies found."
        
        section = """## 🏢 Agency Analysis

| Agency | Total Spots | Paid Spots | BNS Spots | Revenue | Avg/Spot | Customers | Customer List |
|--------|-------------|------------|-----------|---------|----------|-----------|---------------|
"""
        
        for agency in agencies:
            section += f"| {agency.agency_name} | {agency.total_spots:,} | {agency.paid_spots:,} | {agency.bns_spots:,} | ${agency.revenue:,.2f} | ${agency.avg_per_spot:.2f} | {agency.unique_customers:,} | {agency.customer_list} |\n"
        
        return section
    
    def _format_intent_breakdown(self, intent_breakdown: Dict[str, Any]) -> str:
        """Format intent breakdown section"""
        if not intent_breakdown['breakdown']:
            return "### No intent data found."
        
        section = f"""## 🎯 Campaign Intent Analysis

**Total Intent Types**: {intent_breakdown['total_intents']}

| Intent | Spots | Revenue | Avg/Spot | Customers |
|--------|-------|---------|----------|-----------|
"""
        
        for intent in intent_breakdown['breakdown']:
            section += f"| {intent['intent']} | {intent['spots']:,} | ${intent['revenue']:,.2f} | ${intent['avg_per_spot']:.2f} | {intent['unique_customers']:,} |\n"
        
        section += """
**Intent Definitions:**
- **language_specific**: Customer wants specific language community reach
- **time_specific**: Customer targets specific time slots regardless of language
- **indifferent**: Customer flexible on language/time placement
- **no_grid_coverage**: Time slot not covered by programming grid
"""
        
        return section


def main():
    """Main function for testing multi-language analyzer"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Production Multi-Language Analyzer")
    parser.add_argument("--year", default="2024", help="Year to analyze")
    parser.add_argument("--output", help="Output file path")
    parser.add_argument("--db-path", default="data/database/production.db", help="Database path")
    parser.add_argument("--summary-only", action="store_true", help="Show summary only")
    
    args = parser.parse_args()
    
    try:
        with sqlite3.connect(args.db_path) as db:
            if args.summary_only:
                analyzer = MultiLanguageAnalyzer(db)
                summary = analyzer.get_summary(args.year)
                print(f"📊 Multi-Language Summary ({args.year}):")
                print(f"Total Spots: {summary.total_spots:,}")
                print(f"Paid Spots: {summary.paid_spots:,}")
                print(f"BNS Spots: {summary.bns_spots:,} ({summary.bns_percentage:.1f}%)")
                print(f"Total Revenue: ${summary.total_revenue:,.2f}")
                print(f"Average per Spot: ${summary.avg_per_spot:.2f}")
                print(f"Unique Customers: {summary.unique_customers:,}")
                print(f"Unique Agencies: {summary.unique_agencies:,}")
            else:
                report_generator = MultiLanguageReportGenerator(db)
                report = report_generator.generate_report(args.year)
                
                if args.output:
                    with open(args.output, 'w') as f:
                        f.write(report)
                    print(f"✅ Multi-language report saved to {args.output}")
                else:
                    print(report)
    
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()