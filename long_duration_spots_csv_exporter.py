#!/usr/bin/env python3
"""
Misclassified Long Duration Spots CSV Exporter
==============================================

This script exports spots with long durations (like 19:00:00 to 1 day, 0:00:00) that are 
NOT currently classified as ROS but should be. These are the problematic spots causing 
the $774,911 reconciliation gap.

Focus: Spots that should be ROS but are misclassified as other categories.
Excludes: Spots already properly classified as ROS.

Usage:
    python long_duration_spots_csv_exporter.py --year 2022
    python long_duration_spots_csv_exporter.py --year 2022 --pattern "19:00:00"
    python long_duration_spots_csv_exporter.py --year 2022 --all-patterns
"""

import sqlite3
import csv
import sys
import os
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import re

class LongDurationSpotsExporter:
    """Export misclassified long duration spots to CSV for manual analysis
    
    Focuses on spots that should be ROS but are currently classified as other categories.
    These are the spots causing the reconciliation gap between category totals and 
    language breakdowns.
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
    
    def get_long_duration_spots(self, year: str = "2022", time_pattern: Optional[str] = None) -> List[Dict]:
        """Get MISCLASSIFIED spots with long durations that should be ROS but aren't"""
        
        # Focus on spots that are NOT already ROS but should be
        base_query = """
        SELECT 
            s.spot_id,
            s.bill_code,
            s.air_date,
            s.day_of_week,
            s.time_in,
            s.time_out,
            s.length_seconds,
            s.gross_rate,
            s.station_net,
            s.sales_person,
            s.revenue_type,
            s.broadcast_month,
            s.spot_type,
            s.program,
            s.format,
            s.priority,
            s.market_name,
            
            -- Customer information
            c.normalized_name as customer_name,
            c.customer_type,
            
            -- Agency information
            a.agency_name,
            
            -- Market information
            m.market_code,
            m.market_name as market_display_name,
            m.region,
            
            -- Language information
            l.language_code,
            l.language_name,
            
            -- Assignment information
            slb.campaign_type,
            slb.customer_intent,
            slb.assignment_method,
            slb.requires_attention,
            slb.alert_reason,
            slb.business_rule_applied,
            slb.block_id,
            slb.spans_multiple_blocks,
            slb.blocks_spanned,
            
            -- Programming schedule info
            ps.schedule_name,
            ps.schedule_type,
            
            -- Language block info (if assigned)
            lb.block_name,
            lb.block_type,
            lb.day_part,
            lb.time_start as block_time_start,
            lb.time_end as block_time_end,
            bl.language_name as block_language_name
            
        FROM spots s
        LEFT JOIN customers c ON s.customer_id = c.customer_id
        LEFT JOIN agencies a ON s.agency_id = a.agency_id
        LEFT JOIN markets m ON s.market_id = m.market_id
        LEFT JOIN languages l ON s.language_id = l.language_id
        LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        LEFT JOIN programming_schedules ps ON slb.schedule_id = ps.schedule_id
        LEFT JOIN language_blocks lb ON slb.block_id = lb.block_id
        LEFT JOIN languages bl ON lb.language_id = bl.language_id
        
        WHERE s.broadcast_month LIKE ?
        AND s.time_out LIKE '%day%'
        -- FOCUS ON MISCLASSIFIED SPOTS: Not already ROS, but should be
        AND slb.campaign_type != 'ros'  -- Exclude spots already classified as ROS
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'  -- Exclude Direct Response
        AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
        AND s.revenue_type != 'Paid Programming'  -- Exclude Paid Programming
        AND NOT (slb.spot_id IS NULL AND s.spot_type = 'PRD')  -- Exclude Branded Content
        AND NOT (slb.spot_id IS NULL AND s.spot_type = 'SVC')  -- Exclude Services
        """
        
        params = [f"%-{year[-2:]}"]
        
        # Add specific time pattern filter if provided
        if time_pattern:
            base_query += " AND s.time_in = ?"
            params.append(time_pattern)
        
        base_query += " ORDER BY s.gross_rate DESC, s.air_date, s.time_in"
        
        cursor = self.db_connection.cursor()
        cursor.execute(base_query, params)
        
        columns = [desc[0] for desc in cursor.description]
        results = []
        
        for row in cursor.fetchall():
            spot_data = dict(zip(columns, row))
            
            # Calculate duration and add analysis
            duration_info = self._analyze_spot_duration(spot_data['time_in'], spot_data['time_out'])
            spot_data.update(duration_info)
            
            results.append(spot_data)
        
        return results
    
    def _analyze_spot_duration(self, time_in: str, time_out: str) -> Dict:
        """Analyze MISCLASSIFIED spot duration and provide ROS classification recommendations"""
        
        analysis = {
            'calculated_duration_minutes': 0,
            'calculated_duration_hours': 0,
            'should_be_ros': False,
            'ros_reason': '',
            'likely_data_error': False,
            'recommended_end_time': None,
            'recommended_classification': 'unknown',
            'analysis_notes': ''
        }
        
        try:
            if not time_in or not time_out:
                analysis['analysis_notes'] = 'Missing time data'
                return analysis
            
            # Parse start time
            start_parts = time_in.split(':')
            if len(start_parts) >= 2:
                start_hours = int(start_parts[0])
                start_minutes = int(start_parts[1])
                start_total_minutes = start_hours * 60 + start_minutes
            else:
                analysis['analysis_notes'] = 'Invalid start time format'
                return analysis
            
            # Parse end time with day format
            if 'day' in time_out:
                # Parse "1 day, 0:00:00" format
                day_match = re.match(r'(\d+)\s*day.*?(\d{1,2}):(\d{2}):(\d{2})', time_out)
                if day_match:
                    days = int(day_match.group(1))
                    end_hours = int(day_match.group(2))
                    end_minutes = int(day_match.group(3))
                    
                    # Calculate total end minutes
                    end_total_minutes = (days * 24 * 60) + (end_hours * 60) + end_minutes
                    
                    # Calculate duration
                    duration_minutes = end_total_minutes - start_total_minutes
                    
                    analysis['calculated_duration_minutes'] = duration_minutes
                    analysis['calculated_duration_hours'] = duration_minutes / 60
                    
                    # Check if should be ROS (duration > 6 hours = 360 minutes)
                    if duration_minutes > 360:
                        analysis['should_be_ros'] = True
                        analysis['ros_reason'] = f'Duration > 6 hours ({duration_minutes/60:.1f} hours)'
                        analysis['recommended_classification'] = 'ros'
                    
                    # Determine if likely data error or legitimate long duration
                    if duration_minutes > 1440:  # More than 24 hours
                        analysis['likely_data_error'] = True
                        analysis['analysis_notes'] = f'Multi-day duration ({duration_minutes/60:.1f} hours) - likely data error, should be ROS'
                    elif start_hours >= 19 and end_hours == 0 and days == 1:
                        # Evening start to midnight next day - very suspicious
                        analysis['likely_data_error'] = True
                        analysis['recommended_end_time'] = '23:59:00'
                        analysis['analysis_notes'] = f'Evening start to midnight next day ({duration_minutes/60:.1f} hours) - probably should end at 23:59:00 and be ROS'
                    elif start_hours >= 20 and end_hours == 0 and days == 1:
                        # Late evening start to midnight next day - very suspicious
                        analysis['likely_data_error'] = True
                        analysis['recommended_end_time'] = '23:59:00'
                        analysis['analysis_notes'] = f'Late evening start to midnight next day ({duration_minutes/60:.1f} hours) - probably should end at 23:59:00 and be ROS'
                    elif start_hours >= 23 and end_hours == 0 and days == 1:
                        # Very late start to midnight next day - extremely suspicious
                        analysis['likely_data_error'] = True
                        analysis['recommended_end_time'] = '23:59:00'
                        analysis['analysis_notes'] = f'Very late start to midnight next day ({duration_minutes/60:.1f} hours) - almost certainly should end at 23:59:00'
                    elif duration_minutes > 720:  # More than 12 hours
                        analysis['analysis_notes'] = f'Very long duration ({duration_minutes/60:.1f} hours) - should be ROS, review if legitimate'
                    else:
                        analysis['analysis_notes'] = f'Long duration ({duration_minutes/60:.1f} hours) - should be ROS'
                    
                    # Additional ROS classification reasons
                    if analysis['should_be_ros']:
                        if start_hours >= 19 and 'day' in time_out:
                            analysis['ros_reason'] += ' + Evening start to next day pattern'
                        elif start_hours >= 13 and duration_minutes > 600:  # 10+ hours from afternoon
                            analysis['ros_reason'] += ' + Afternoon/evening long duration'
                    
                else:
                    analysis['analysis_notes'] = 'Could not parse day format'
            else:
                analysis['analysis_notes'] = 'Standard time format (not day format)'
        
        except Exception as e:
            analysis['analysis_notes'] = f'Error analyzing duration: {str(e)}'
        
        return analysis
    
    def get_duration_summary(self, year: str = "2022") -> Dict:
        """Get summary of MISCLASSIFIED duration patterns (focus on ROS classification issues)"""
        
        spots = self.get_long_duration_spots(year)
        
        summary = {
            'total_spots': len(spots),
            'total_revenue': sum(spot['gross_rate'] or 0 for spot in spots),
            'should_be_ros': 0,
            'ros_revenue': 0,
            'by_current_classification': {},
            'by_time_pattern': {},
            'by_customer': {},
            'by_market': {},
            'likely_errors': 0,
            'error_revenue': 0,
            'data_error_patterns': {}
        }
        
        for spot in spots:
            # Count spots that should be ROS
            if spot['should_be_ros']:
                summary['should_be_ros'] += 1
                summary['ros_revenue'] += spot['gross_rate'] or 0
            
            # Group by current classification (to show misclassification)
            current_type = spot['campaign_type'] or 'unknown'
            if current_type not in summary['by_current_classification']:
                summary['by_current_classification'][current_type] = {'count': 0, 'revenue': 0}
            summary['by_current_classification'][current_type]['count'] += 1
            summary['by_current_classification'][current_type]['revenue'] += spot['gross_rate'] or 0
            
            # Group by time pattern
            time_pattern = f"{spot['time_in']} - {spot['time_out']}"
            if time_pattern not in summary['by_time_pattern']:
                summary['by_time_pattern'][time_pattern] = {'count': 0, 'revenue': 0, 'should_be_ros': 0}
            summary['by_time_pattern'][time_pattern]['count'] += 1
            summary['by_time_pattern'][time_pattern]['revenue'] += spot['gross_rate'] or 0
            if spot['should_be_ros']:
                summary['by_time_pattern'][time_pattern]['should_be_ros'] += 1
            
            # Group by customer
            customer = spot['customer_name'] or 'Unknown'
            if customer not in summary['by_customer']:
                summary['by_customer'][customer] = {'count': 0, 'revenue': 0}
            summary['by_customer'][customer]['count'] += 1
            summary['by_customer'][customer]['revenue'] += spot['gross_rate'] or 0
            
            # Group by market
            market = spot['market_code'] or 'Unknown'
            if market not in summary['by_market']:
                summary['by_market'][market] = {'count': 0, 'revenue': 0}
            summary['by_market'][market]['count'] += 1
            summary['by_market'][market]['revenue'] += spot['gross_rate'] or 0
            
            # Count likely errors
            if spot['likely_data_error']:
                summary['likely_errors'] += 1
                summary['error_revenue'] += spot['gross_rate'] or 0
                
                # Track data error patterns
                if spot['recommended_end_time']:
                    pattern = f"{spot['time_in']} -> {spot['recommended_end_time']}"
                    if pattern not in summary['data_error_patterns']:
                        summary['data_error_patterns'][pattern] = {'count': 0, 'revenue': 0}
                    summary['data_error_patterns'][pattern]['count'] += 1
                    summary['data_error_patterns'][pattern]['revenue'] += spot['gross_rate'] or 0
        
        return summary
    
    def export_to_csv(self, year: str = "2022", time_pattern: Optional[str] = None, 
                     output_file: Optional[str] = None) -> str:
        """Export long duration spots to CSV"""
        
        spots = self.get_long_duration_spots(year, time_pattern)
        
        if not spots:
            print(f"No long duration spots found for {year}")
            return ""
        
        # Generate filename if not provided
        if not output_file:
            pattern_suffix = f"_{time_pattern.replace(':', '')}" if time_pattern else ""
            output_file = f"long_duration_spots_{year}{pattern_suffix}.csv"
        
        # Define CSV columns focused on misclassification analysis
        csv_columns = [
            'spot_id',
            'bill_code',
            'air_date',
            'day_of_week',
            'time_in',
            'time_out',
            'calculated_duration_hours',
            'should_be_ros',
            'ros_reason',
            'likely_data_error',
            'recommended_end_time',
            'recommended_classification',
            'analysis_notes',
            'current_campaign_type',
            'customer_intent',
            'alert_reason',
            'gross_rate',
            'station_net',
            'sales_person',
            'customer_name',
            'agency_name',
            'market_code',
            'market_display_name',
            'region',
            'language_code',
            'language_name',
            'revenue_type',
            'broadcast_month',
            'spot_type',
            'program',
            'format',
            'priority',
            'assignment_method',
            'requires_attention',
            'business_rule_applied',
            'block_id',
            'spans_multiple_blocks',
            'blocks_spanned',
            'schedule_name',
            'schedule_type',
            'block_name',
            'block_type',
            'day_part',
            'block_time_start',
            'block_time_end',
            'block_language_name'
        ]
        
        # Write CSV
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns, extrasaction='ignore')
            writer.writeheader()
            
            for spot in spots:
                # Clean up any None values and add current campaign type for clarity
                cleaned_spot = {k: v if v is not None else '' for k, v in spot.items()}
                cleaned_spot['current_campaign_type'] = spot['campaign_type']  # Make this clear
                writer.writerow(cleaned_spot)
        
        print(f"✅ Exported {len(spots):,} long duration spots to {output_file}")
        return output_file
    
    def export_all_patterns(self, year: str = "2022") -> List[str]:
        """Export CSV files for all common long duration patterns"""
        
        # Get summary first to identify patterns
        summary = self.get_duration_summary(year)
        
        exported_files = []
        
        # Export overall file
        overall_file = self.export_to_csv(year, output_file=f"long_duration_spots_{year}_all.csv")
        exported_files.append(overall_file)
        
        # Export by common time patterns
        common_patterns = [
            "19:00:00",
            "20:00:00", 
            "21:00:00",
            "22:00:00",
            "23:00:00",
            "23:30:00"
        ]
        
        for pattern in common_patterns:
            # Check if this pattern exists in the data
            pattern_exists = any(f"{pattern} - " in time_pattern for time_pattern in summary['by_time_pattern'].keys())
            
            if pattern_exists:
                pattern_file = self.export_to_csv(year, time_pattern=pattern, 
                                                output_file=f"long_duration_spots_{year}_{pattern.replace(':', '')}.csv")
                exported_files.append(pattern_file)
        
        return exported_files
    
    def generate_summary_report(self, year: str = "2022") -> str:
        """Generate a summary report focused on MISCLASSIFIED long duration spots"""
        
        summary = self.get_duration_summary(year)
        
        report = f"""# Misclassified Long Duration Spots Analysis Report - {year}

## 🎯 Focus: Spots that should be ROS but aren't

This report analyzes spots with long durations (especially "1 day" format) that are NOT currently classified as ROS but should be.

## 📊 Overview

- **Total Misclassified Spots**: {summary['total_spots']:,}
- **Total Revenue**: ${summary['total_revenue']:,.2f}
- **Should Be ROS**: {summary['should_be_ros']:,} ({(summary['should_be_ros'] / summary['total_spots'] * 100):.1f}%)
- **ROS Revenue**: ${summary['ros_revenue']:,.2f}
- **Likely Data Errors**: {summary['likely_errors']:,} ({(summary['likely_errors'] / summary['total_spots'] * 100):.1f}%)
- **Error Revenue**: ${summary['error_revenue']:,.2f}

## 🔍 Current Misclassification Breakdown

| Current Classification | Spots | Revenue | Should Be ROS |
|------------------------|-------|---------|---------------|
"""
        
        for classification, data in summary['by_current_classification'].items():
            report += f"| {classification} | {data['count']:,} | ${data['revenue']:,.2f} | ❌ Should be ROS |\n"
        
        report += f"""

## 🕐 Time Pattern Analysis (Misclassified Spots Only)

| Time Pattern | Spots | Revenue | Should Be ROS | Likely Error |
|--------------|-------|---------|---------------|--------------|
"""
        
        # Sort patterns by revenue
        sorted_patterns = sorted(summary['by_time_pattern'].items(), key=lambda x: x[1]['revenue'], reverse=True)
        
        for time_pattern, data in sorted_patterns[:15]:  # Top 15 patterns
            should_be_ros = data['should_be_ros']
            # Check if this pattern is likely an error
            likely_error = "🔴 YES" if ("19:00:00 - 1 day" in time_pattern or "20:00:00 - 1 day" in time_pattern or "23:30:00 - 1 day" in time_pattern) else "🟡 MAYBE"
            report += f"| {time_pattern} | {data['count']:,} | ${data['revenue']:,.2f} | {should_be_ros:,} | {likely_error} |\n"
        
        # Add data error patterns section
        if summary['data_error_patterns']:
            report += f"""

## 🔧 Recommended Data Corrections

| Current Pattern | Recommended Fix | Spots | Revenue |
|-----------------|-----------------|-------|---------|
"""
            
            sorted_errors = sorted(summary['data_error_patterns'].items(), key=lambda x: x[1]['revenue'], reverse=True)
            
            for error_pattern, data in sorted_errors:
                report += f"| {error_pattern} | Fix time data | {data['count']:,} | ${data['revenue']:,.2f} |\n"
        
        report += f"""

## 🏢 Top Customers with Misclassified Spots

| Customer | Spots | Revenue | Impact |
|----------|-------|---------|--------|
"""
        
        # Sort customers by revenue
        sorted_customers = sorted(summary['by_customer'].items(), key=lambda x: x[1]['revenue'], reverse=True)
        
        for customer, data in sorted_customers[:10]:  # Top 10 customers
            report += f"| {customer} | {data['count']:,} | ${data['revenue']:,.2f} | Should be ROS |\n"
        
        report += f"""

## 🗺️ Market Distribution

| Market | Spots | Revenue | Impact |
|--------|-------|---------|--------|
"""
        
        # Sort markets by revenue
        sorted_markets = sorted(summary['by_market'].items(), key=lambda x: x[1]['revenue'], reverse=True)
        
        for market, data in sorted_markets[:10]:  # Top 10 markets
            report += f"| {market} | {data['count']:,} | ${data['revenue']:,.2f} | Should be ROS |\n"
        
        report += f"""

## 🎯 Root Cause Analysis

### The Problem
These {summary['total_spots']:,} spots are currently misclassified because:

1. **They have long durations** (mostly 24+ hours)
2. **They're NOT classified as ROS** (Run on Schedule)
3. **They should be ROS** based on duration > 6 hours rule
4. **They're causing category reconciliation issues**

### Most Suspicious Patterns
- **19:00:00 to 1 day, 0:00:00** → 29 hours (probably should end at 23:59:00)
- **20:00:00 to 1 day, 0:00:00** → 28 hours (probably should end at 23:59:00)
- **23:30:00 to 1 day, 0:00:00** → 24.5 hours (probably should end at 23:59:00)

### Data Quality Issues
- **{summary['likely_errors']:,} spots** ({(summary['likely_errors'] / summary['total_spots'] * 100):.1f}%) are likely data entry errors
- **${summary['error_revenue']:,.2f}** in revenue affected by data errors
- **Most common issue**: Evening spots extending to "next day midnight" instead of "same day 23:59"

## 💡 Recommendations

### 1. Fix Data Errors First
```sql
-- Example: Fix 19:00:00 to 1 day spots that should end at 23:59:00
UPDATE spots 
SET time_out = '23:59:00' 
WHERE time_in = '19:00:00' 
AND time_out = '1 day, 0:00:00'
AND broadcast_month LIKE '%-{year[-2:]}';
```

### 2. Fix ROS Classification Logic
- Ensure duration calculation properly handles "1 day" format
- Apply ROS rules BEFORE language block assignment
- Update precedence rules to catch these patterns

### 3. Re-run Classification
```bash
# After fixing data and logic
python cli_01_assign_language_blocks.py --all-year {year}
```

### 4. Expected Results
After fixes, these spots should:
- **Be classified as ROS** (Run on Schedule)
- **Move from Individual Language Blocks** to **ROS category**
- **Resolve the $774,911 reconciliation gap**

## 📋 Action Items

1. **✅ Export CSV files** (this report)
2. **🔍 Manual review** of suspicious patterns
3. **🔧 Fix data errors** in time_out fields
4. **⚙️ Update ROS detection logic** 
5. **🔄 Re-run classification** process
6. **✅ Validate reconciliation** is resolved

---

*This report focuses on the {summary['total_spots']:,} spots causing the $774,911 reconciliation gap*
"""
        
        return report


def main():
    """Main function to run the long duration spots exporter"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Export misclassified long duration spots to CSV for manual review (spots that should be ROS but aren't)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Export all misclassified long duration spots for 2022
    python long_duration_spots_csv_exporter.py --year 2022
    
    # Export only misclassified 19:00:00 pattern spots  
    python long_duration_spots_csv_exporter.py --year 2022 --pattern "19:00:00"
    
    # Export all misclassified patterns to separate CSV files
    python long_duration_spots_csv_exporter.py --year 2022 --all-patterns
    
    # Generate summary report of misclassification issues
    python long_duration_spots_csv_exporter.py --year 2022 --summary --output summary_report.md
        """
    )
    
    parser.add_argument("--year", default="2022", help="Year to analyze")
    parser.add_argument("--pattern", help="Specific time pattern to export (e.g., '19:00:00')")
    parser.add_argument("--all-patterns", action="store_true", help="Export separate CSV files for each common pattern")
    parser.add_argument("--summary", action="store_true", help="Generate summary report instead of CSV")
    parser.add_argument("--output", help="Output file name (for CSV or summary report)")
    parser.add_argument("--db-path", default="data/database/production.db", help="Database path")
    
    args = parser.parse_args()
    
    try:
        with LongDurationSpotsExporter(args.db_path) as exporter:
            if args.summary:
                # Generate summary report
                report = exporter.generate_summary_report(args.year)
                
                if args.output:
                    with open(args.output, 'w') as f:
                        f.write(report)
                    print(f"✅ Summary report saved to {args.output}")
                else:
                    print(report)
            
            elif args.all_patterns:
                # Export all patterns
                exported_files = exporter.export_all_patterns(args.year)
                
                print(f"✅ Exported {len(exported_files)} CSV files:")
                for file in exported_files:
                    print(f"   - {file}")
                
                # Also generate summary
                summary_file = f"long_duration_summary_{args.year}.md"
                report = exporter.generate_summary_report(args.year)
                with open(summary_file, 'w') as f:
                    f.write(report)
                print(f"✅ Summary report saved to {summary_file}")
            
            else:
                # Export single CSV
                output_file = exporter.export_to_csv(args.year, args.pattern, args.output)
                
                # Show quick summary
                summary = exporter.get_duration_summary(args.year)
                print(f"📊 Quick Summary:")
                print(f"   Total spots: {summary['total_spots']:,}")
                print(f"   Total revenue: ${summary['total_revenue']:,.2f}")
                print(f"   Likely errors: {summary['likely_errors']:,}")
                print(f"   Error revenue: ${summary['error_revenue']:,.2f}")
    
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()