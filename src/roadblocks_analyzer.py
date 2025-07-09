#!/usr/bin/env python3
"""
Roadblocks Analyzer - Production Ready
=====================================

Production-ready roadblocks analysis module with proper integration
into the existing BaseQueryBuilder system.

Key features:
- Full Day Roadblock classification (6:00am-11:59pm)
- BNS spot tracking and analysis
- Customer and time pattern analysis
- Integration with existing query builder foundation

Save as: src/roadblocks_analyzer.py
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
class RoadblockSummary:
    """Roadblock summary statistics"""
    total_spots: int
    paid_spots: int
    bns_spots: int
    total_revenue: float
    avg_per_spot: float
    bns_percentage: float


@dataclass
class RoadblockTimePattern:
    """Time pattern analysis for roadblocks"""
    time_range: str
    classification: str
    spots: int
    paid_spots: int
    bns_spots: int
    revenue: float
    avg_per_spot: float


@dataclass
class RoadblockCustomer:
    """Customer analysis for roadblocks"""
    customer_name: str
    total_spots: int
    paid_spots: int
    bns_spots: int
    revenue: float
    avg_per_spot: float
    days_of_week: int
    time_range: str


class RoadblocksQueryBuilder(BaseQueryBuilder):
    """Production-ready roadblocks query builder"""
    
    def __init__(self, year: str = "2024"):
        super().__init__(year)
        self.apply_standard_filters()
        self.add_language_block_join()
    
    def add_language_block_join(self):
        """Add language block join for campaign_type"""
        self.add_left_join("spot_language_blocks slb", "s.spot_id = slb.spot_id")
        return self
    
    def add_roadblock_conditions(self):
        """Add roadblock-specific conditions"""
        self.add_filter("slb.campaign_type = 'roadblock'")
        return self
    
    def add_customer_and_agency_joins(self):
        """Add customer and agency joins for analysis"""
        self.add_left_join("customers c", "s.customer_id = c.customer_id")
        self.add_left_join("agencies a", "s.agency_id = a.agency_id")
        return self
    
    def get_roadblocks_spot_ids(self) -> Set[int]:
        """Get all roadblock spot IDs for integration with unified analysis"""
        # This method will be used by unified_analysis.py for reconciliation
        pass
    
    def classify_time_range(self, time_in: str, time_out: str) -> str:
        """Classify roadblock time range based on actual patterns"""
        if time_in == "6:00:00" and time_out == "23:59:00":
            return "Full Day Roadblock (6:00am-11:59pm)"
        elif time_in == "6:00:00" and time_out == "23:00:00":
            return "Full Day Roadblock (6:00am-11:00pm)"
        elif time_in == "06:00" and time_out == "23:59":
            return "Full Day Roadblock (6:00am-11:59pm) - Alt Format"
        else:
            return f"Roadblock ({time_in} to {time_out})"
    
    def build_summary_query(self) -> str:
        """Build roadblocks summary query"""
        return f"""
        SELECT 
            COUNT(*) as total_spots,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bns_spots,
            COUNT(CASE WHEN s.spot_type != 'BNS' OR s.spot_type IS NULL THEN 1 END) as paid_spots,
            SUM(COALESCE(s.gross_rate, 0)) as total_revenue,
            AVG(COALESCE(s.gross_rate, 0)) as avg_per_spot
        {self.build_from_clause()}
        {self.build_where_clause()}
        """
    
    def build_time_pattern_query(self) -> str:
        """Build time pattern analysis query"""
        return f"""
        SELECT 
            s.time_in || ' to ' || s.time_out as time_range,
            CASE 
                WHEN s.time_in = '6:00:00' AND s.time_out = '23:59:00' THEN 'Full Day Roadblock (6:00am-11:59pm)'
                WHEN s.time_in = '6:00:00' AND s.time_out = '23:00:00' THEN 'Full Day Roadblock (6:00am-11:00pm)'
                WHEN s.time_in = '06:00' AND s.time_out = '23:59' THEN 'Full Day Roadblock (6:00am-11:59pm) - Alt Format'
                ELSE 'Roadblock (' || s.time_in || ' to ' || s.time_out || ')'
            END as classification,
            COUNT(*) as spots,
            COUNT(CASE WHEN s.spot_type != 'BNS' OR s.spot_type IS NULL THEN 1 END) as paid_spots,
            COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bns_spots,
            SUM(COALESCE(s.gross_rate, 0)) as revenue,
            AVG(COALESCE(s.gross_rate, 0)) as avg_per_spot
        {self.build_from_clause()}
        {self.build_where_clause()}
        GROUP BY s.time_in, s.time_out
        ORDER BY revenue DESC
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
            COUNT(DISTINCT s.day_of_week) as days_of_week,
            MIN(s.time_in) as earliest_time,
            MAX(s.time_out) as latest_time
        {self.build_from_clause()}
        {self.build_where_clause()}
        GROUP BY COALESCE(c.normalized_name, 'Unknown')
        ORDER BY revenue DESC
        LIMIT 20
        """
    
    def build_spot_ids_query(self) -> str:
        """Build query to get roadblock spot IDs for unified analysis integration"""
        return f"""
        SELECT DISTINCT s.spot_id
        {self.build_from_clause()}
        {self.build_where_clause()}
        """


class RoadblocksAnalyzer:
    """Production-ready roadblocks analyzer"""
    
    def __init__(self, db_connection):
        self.db_connection = db_connection
    
    def get_summary(self, year: str = "2024") -> RoadblockSummary:
        """Get roadblocks summary statistics"""
        builder = RoadblocksQueryBuilder(year)
        builder.add_roadblock_conditions()
        
        query = builder.build_summary_query()
        cursor = self.db_connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        
        total_spots = result[0] or 0
        bns_spots = result[1] or 0
        paid_spots = result[2] or 0
        total_revenue = result[3] or 0
        avg_per_spot = result[4] or 0
        
        bns_percentage = (bns_spots / total_spots) * 100 if total_spots > 0 else 0
        
        return RoadblockSummary(
            total_spots=total_spots,
            paid_spots=paid_spots,
            bns_spots=bns_spots,
            total_revenue=total_revenue,
            avg_per_spot=avg_per_spot,
            bns_percentage=bns_percentage
        )
    
    def get_time_patterns(self, year: str = "2024") -> List[RoadblockTimePattern]:
        """Get time pattern analysis"""
        builder = RoadblocksQueryBuilder(year)
        builder.add_roadblock_conditions()
        
        query = builder.build_time_pattern_query()
        cursor = self.db_connection.cursor()
        cursor.execute(query)
        
        patterns = []
        for row in cursor.fetchall():
            patterns.append(RoadblockTimePattern(
                time_range=row[0],
                classification=row[1],
                spots=row[2],
                paid_spots=row[3],
                bns_spots=row[4],
                revenue=row[5],
                avg_per_spot=row[6]
            ))
        
        return patterns
    
    def get_customers(self, year: str = "2024") -> List[RoadblockCustomer]:
        """Get customer analysis"""
        builder = RoadblocksQueryBuilder(year)
        builder.add_roadblock_conditions().add_customer_and_agency_joins()
        
        query = builder.build_customer_query()
        cursor = self.db_connection.cursor()
        cursor.execute(query)
        
        customers = []
        for row in cursor.fetchall():
            customers.append(RoadblockCustomer(
                customer_name=row[0],
                total_spots=row[1],
                paid_spots=row[2],
                bns_spots=row[3],
                revenue=row[4],
                avg_per_spot=row[5],
                days_of_week=row[6],
                time_range=f"{row[7]} to {row[8]}" if row[7] and row[8] else "Unknown"
            ))
        
        return customers
    
    def get_spot_ids(self, year: str = "2024") -> Set[int]:
        """Get roadblock spot IDs for unified analysis integration"""
        builder = RoadblocksQueryBuilder(year)
        builder.add_roadblock_conditions()
        
        query = builder.build_spot_ids_query()
        cursor = self.db_connection.cursor()
        cursor.execute(query)
        
        return set(row[0] for row in cursor.fetchall())
    
    def get_bns_breakdown(self, year: str = "2024") -> Dict[str, Any]:
        """Get detailed BNS breakdown"""
        builder = RoadblocksQueryBuilder(year)
        builder.add_roadblock_conditions().add_customer_and_agency_joins()
        
        query = f"""
        SELECT 
            COALESCE(c.normalized_name, 'Unknown') as customer_name,
            COUNT(*) as bns_spots,
            s.time_in || ' to ' || s.time_out as time_range
        {builder.build_from_clause()}
        {builder.build_where_clause()}
        AND s.spot_type = 'BNS'
        GROUP BY COALESCE(c.normalized_name, 'Unknown'), s.time_in, s.time_out
        ORDER BY bns_spots DESC
        LIMIT 10
        """
        
        cursor = self.db_connection.cursor()
        cursor.execute(query)
        
        bns_customers = []
        for row in cursor.fetchall():
            bns_customers.append({
                'customer_name': row[0],
                'bns_spots': row[1],
                'time_range': row[2]
            })
        
        return {
            'customers': bns_customers,
            'total_bns': sum(c['bns_spots'] for c in bns_customers)
        }


class RoadblocksReportGenerator:
    """Production-ready roadblocks report generator"""
    
    def __init__(self, db_connection):
        self.analyzer = RoadblocksAnalyzer(db_connection)
    
    def generate_report(self, year: str = "2024") -> str:
        """Generate comprehensive roadblocks report"""
        summary = self.analyzer.get_summary(year)
        time_patterns = self.analyzer.get_time_patterns(year)
        customers = self.analyzer.get_customers(year)
        bns_breakdown = self.analyzer.get_bns_breakdown(year)
        
        return f"""# Roadblocks Analysis Report - {year}

*Generated by Production Roadblocks Analyzer*

## 🚧 Executive Summary

### Performance Metrics
- **Total Revenue**: ${summary.total_revenue:,.2f}
- **Total Spots**: {summary.total_spots:,}
- **Paid Spots**: {summary.paid_spots:,} ({100 - summary.bns_percentage:.1f}%)
- **BNS Spots**: {summary.bns_spots:,} ({summary.bns_percentage:.1f}%)
- **Average per Spot**: ${summary.avg_per_spot:.2f}

### Key Insights
- **{summary.bns_percentage:.1f}% of roadblocks are BNS** (bonus spots with no revenue)
- **Only {100 - summary.bns_percentage:.1f}% generate revenue** (${summary.total_revenue:,.2f})
- **Public service focus**: High BNS rate indicates government/non-profit campaigns

{self._format_time_patterns(time_patterns)}

{self._format_customers(customers)}

{self._format_bns_breakdown(bns_breakdown)}

## 📊 Technical Details

**Classification Logic:**
- **6:00:00 to 23:59:00**: Full Day Roadblock (6:00am-11:59pm) - Main pattern
- **6:00:00 to 23:00:00**: Full Day Roadblock (6:00am-11:00pm) - Ends at 11pm
- **06:00 to 23:59**: Full Day Roadblock (6:00am-11:59pm) - Alternative time format

**Revenue Calculation:**
- **BNS Spots**: spot_type = 'BNS' (no revenue, bonus content)
- **Paid Spots**: All other spots that generate revenue
- **Total Revenue**: Sum of paid spots only

**Data Source**: campaign_type = 'roadblock' in spot_language_blocks table

---

*Generated by Production Roadblocks Analyzer v1.0*
"""
    
    def _format_time_patterns(self, patterns: List[RoadblockTimePattern]) -> str:
        """Format time patterns section"""
        if not patterns:
            return "### No time patterns found."
        
        section = """## 📅 Time Pattern Analysis

| Time Range | Classification | Total Spots | Paid Spots | BNS Spots | Revenue | Avg/Spot |
|------------|----------------|-------------|------------|-----------|---------|----------|
"""
        
        for pattern in patterns:
            section += f"| {pattern.time_range} | {pattern.classification} | {pattern.spots:,} | {pattern.paid_spots:,} | {pattern.bns_spots:,} | ${pattern.revenue:,.2f} | ${pattern.avg_per_spot:.2f} |\n"
        
        return section
    
    def _format_customers(self, customers: List[RoadblockCustomer]) -> str:
        """Format customers section"""
        if not customers:
            return "### No customers found."
        
        section = """## 👥 Customer Analysis

| Customer | Total Spots | Paid Spots | BNS Spots | Revenue | Avg/Spot | Days | Time Range |
|----------|-------------|------------|-----------|---------|----------|------|------------|
"""
        
        for customer in customers[:15]:  # Top 15 customers
            section += f"| {customer.customer_name} | {customer.total_spots:,} | {customer.paid_spots:,} | {customer.bns_spots:,} | ${customer.revenue:,.2f} | ${customer.avg_per_spot:.2f} | {customer.days_of_week} | {customer.time_range} |\n"
        
        return section
    
    def _format_bns_breakdown(self, bns_breakdown: Dict[str, Any]) -> str:
        """Format BNS breakdown section"""
        if not bns_breakdown['customers']:
            return "### No BNS spots found."
        
        section = f"""## 🎁 BNS Spots Analysis

**Total BNS Spots**: {bns_breakdown['total_bns']:,}

| Customer | BNS Spots | Time Range |
|----------|-----------|------------|
"""
        
        for customer in bns_breakdown['customers']:
            section += f"| {customer['customer_name']} | {customer['bns_spots']:,} | {customer['time_range']} |\n"
        
        section += """
**Note**: BNS spots are bonus content with no revenue but count toward total spot inventory.
"""
        
        return section


def main():
    """Main function for testing roadblocks analyzer"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Production Roadblocks Analyzer")
    parser.add_argument("--year", default="2024", help="Year to analyze")
    parser.add_argument("--output", help="Output file path")
    parser.add_argument("--db-path", default="data/database/production.db", help="Database path")
    parser.add_argument("--summary-only", action="store_true", help="Show summary only")
    
    args = parser.parse_args()
    
    try:
        with sqlite3.connect(args.db_path) as db:
            if args.summary_only:
                analyzer = RoadblocksAnalyzer(db)
                summary = analyzer.get_summary(args.year)
                print(f"📊 Roadblocks Summary ({args.year}):")
                print(f"Total Spots: {summary.total_spots:,}")
                print(f"Paid Spots: {summary.paid_spots:,}")
                print(f"BNS Spots: {summary.bns_spots:,} ({summary.bns_percentage:.1f}%)")
                print(f"Total Revenue: ${summary.total_revenue:,.2f}")
                print(f"Average per Spot: ${summary.avg_per_spot:.2f}")
            else:
                report_generator = RoadblocksReportGenerator(db)
                report = report_generator.generate_report(args.year)
                
                if args.output:
                    with open(args.output, 'w') as f:
                        f.write(report)
                    print(f"✅ Roadblocks report saved to {args.output}")
                else:
                    print(report)
    
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()