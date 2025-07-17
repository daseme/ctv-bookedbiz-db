#!/usr/bin/env python3
"""
Language Mismatch Diagnostic Tool
=================================

This tool helps identify why there's a discrepancy between the Individual Language Blocks
category total and the Language Analysis breakdown.

Usage:
    python diagnostic_tool.py --year 2022 --db-path data/database/production.db
"""

import sqlite3
import sys
from typing import Dict, List, Tuple, Any

class LanguageMismatchDiagnostic:
    """Diagnostic tool to identify language analysis discrepancies"""
    
    def __init__(self, db_path: str = "data/database/production.db"):
        self.db_path = db_path
        self.db_connection = None
    
    def __enter__(self):
        self.db_connection = sqlite3.connect(self.db_path)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.db_connection:
            self.db_connection.close()
    
    def get_individual_language_spot_ids(self, year: str = "2022") -> set:
        """Get the exact spot IDs used in Individual Language Blocks category"""
        year_suffix = year[-2:]
        
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
        AND s.revenue_type != 'Paid Programming'
        AND NOT (slb.spot_id IS NULL AND s.spot_type = 'PRD')
        AND NOT (slb.spot_id IS NULL AND s.spot_type = 'SVC')
        AND slb.campaign_type = 'language_specific'
        """
        
        cursor = self.db_connection.cursor()
        cursor.execute(query, [f"%-{year_suffix}"])
        return set(row[0] for row in cursor.fetchall())
    
    def get_language_breakdown_spot_ids(self, year: str = "2022") -> set:
        """Get the exact spot IDs used in Language Analysis breakdown"""
        year_suffix = year[-2:]
        
        query = """
        SELECT DISTINCT s.spot_id
        FROM spots s
        LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        LEFT JOIN language_blocks lb ON COALESCE(slb.block_id, slb.primary_block_id) = lb.block_id
        LEFT JOIN languages l ON lb.language_id = l.language_id
        WHERE s.broadcast_month LIKE ?
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        AND COALESCE((SELECT agency_name FROM agencies WHERE agency_id = s.agency_id), '') NOT LIKE '%WorldLink%'
        AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
        AND s.revenue_type != 'Paid Programming'
        AND NOT (slb.spot_id IS NULL AND s.spot_type = 'PRD')
        AND NOT (slb.spot_id IS NULL AND s.spot_type = 'SVC')
        AND slb.campaign_type = 'language_specific'
        AND (slb.block_id IS NOT NULL OR slb.primary_block_id IS NOT NULL)
        """
        
        cursor = self.db_connection.cursor()
        cursor.execute(query, [f"%-{year_suffix}"])
        return set(row[0] for row in cursor.fetchall())
    
    def analyze_missing_spots(self, year: str = "2022") -> Dict[str, Any]:
        """Analyze the spots that are missing from the language breakdown"""
        category_spots = self.get_individual_language_spot_ids(year)
        language_spots = self.get_language_breakdown_spot_ids(year)
        
        missing_spots = category_spots - language_spots
        
        if not missing_spots:
            return {
                'missing_count': 0,
                'message': 'No missing spots found - perfect match!'
            }
        
        # Analyze the missing spots
        missing_spots_list = list(missing_spots)
        placeholders = ','.join(['?' for _ in missing_spots_list])
        
        query = f"""
        SELECT 
            s.spot_id,
            s.gross_rate,
            s.bill_code,
            s.broadcast_month,
            s.spot_type,
            slb.campaign_type,
            slb.block_id,
            slb.primary_block_id,
            lb.block_name,
            l.language_name,
            CASE 
                WHEN slb.block_id IS NULL AND slb.primary_block_id IS NULL THEN 'No block assignment'
                WHEN lb.block_id IS NULL THEN 'Block not found'
                WHEN l.language_id IS NULL THEN 'Language not found'
                ELSE 'Other issue'
            END as issue_type
        FROM spots s
        LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        LEFT JOIN language_blocks lb ON COALESCE(slb.block_id, slb.primary_block_id) = lb.block_id
        LEFT JOIN languages l ON lb.language_id = l.language_id
        WHERE s.spot_id IN ({placeholders})
        ORDER BY s.gross_rate DESC
        """
        
        cursor = self.db_connection.cursor()
        cursor.execute(query, missing_spots_list)
        missing_details = cursor.fetchall()
        
        # Aggregate by issue type
        issue_summary = {}
        total_revenue = 0
        
        for row in missing_details:
            spot_id, gross_rate, bill_code, broadcast_month, spot_type, campaign_type, block_id, primary_block_id, block_name, language_name, issue_type = row
            
            if issue_type not in issue_summary:
                issue_summary[issue_type] = {
                    'count': 0,
                    'revenue': 0,
                    'examples': []
                }
            
            issue_summary[issue_type]['count'] += 1
            issue_summary[issue_type]['revenue'] += (gross_rate or 0)
            total_revenue += (gross_rate or 0)
            
            if len(issue_summary[issue_type]['examples']) < 3:
                issue_summary[issue_type]['examples'].append({
                    'spot_id': spot_id,
                    'bill_code': bill_code,
                    'gross_rate': gross_rate,
                    'block_id': block_id,
                    'primary_block_id': primary_block_id,
                    'block_name': block_name,
                    'language_name': language_name
                })
        
        return {
            'missing_count': len(missing_spots),
            'missing_revenue': total_revenue,
            'issue_summary': issue_summary,
            'total_category_spots': len(category_spots),
            'total_language_spots': len(language_spots)
        }
    
    def generate_diagnostic_report(self, year: str = "2022") -> str:
        """Generate a comprehensive diagnostic report"""
        analysis = self.analyze_missing_spots(year)
        
        if analysis['missing_count'] == 0:
            return f"""
# Language Analysis Diagnostic Report - {year}

## ✅ PERFECT MATCH FOUND!
- **Category Spots**: {analysis.get('total_category_spots', 0):,}
- **Language Breakdown Spots**: {analysis.get('total_language_spots', 0):,}
- **Missing Spots**: 0
- **Status**: No discrepancy detected

The Individual Language Blocks category and Language Analysis totals match perfectly.
"""
        
        report = f"""
# Language Analysis Diagnostic Report - {year}

## 🔍 DISCREPANCY DETECTED

### Summary
- **Category Spots**: {analysis['total_category_spots']:,}
- **Language Breakdown Spots**: {analysis['total_language_spots']:,}
- **Missing Spots**: {analysis['missing_count']:,}
- **Missing Revenue**: ${analysis['missing_revenue']:,.2f}

### Root Cause Analysis
"""
        
        for issue_type, details in analysis['issue_summary'].items():
            report += f"""
#### {issue_type}
- **Count**: {details['count']:,} spots
- **Revenue**: ${details['revenue']:,.2f}
- **Percentage**: {(details['count'] / analysis['missing_count']) * 100:.1f}% of missing spots

**Examples**:
"""
            for example in details['examples']:
                report += f"""
- Spot {example['spot_id']}: {example['bill_code']} - ${example['gross_rate']:.2f}
  - Block ID: {example['block_id']}
  - Primary Block ID: {example['primary_block_id']}
  - Block Name: {example['block_name']}
  - Language: {example['language_name']}
"""
        
        report += f"""
## 🛠️ RECOMMENDED FIXES

### 1. No Block Assignment Issue
If spots show "No block assignment":
- Check if `spot_language_blocks` table has proper assignments
- Verify that `block_id` or `primary_block_id` are populated
- Run block assignment process for missing spots

### 2. Block Not Found Issue
If spots show "Block not found":
- Check if `language_blocks` table has the referenced block IDs
- Verify foreign key relationships
- Check if blocks were deleted or marked inactive

### 3. Language Not Found Issue
If spots show "Language not found":
- Check if `languages` table has the referenced language IDs
- Verify language assignments in `language_blocks` table
- Check for orphaned language block records

### 4. Quick Fix Query
To identify the exact issue for each missing spot:

```sql
SELECT 
    s.spot_id,
    s.bill_code,
    s.gross_rate,
    slb.campaign_type,
    slb.block_id,
    slb.primary_block_id,
    lb.block_name,
    l.language_name,
    CASE 
        WHEN slb.spot_id IS NULL THEN 'No spot_language_blocks entry'
        WHEN slb.block_id IS NULL AND slb.primary_block_id IS NULL THEN 'No block assignment'
        WHEN lb.block_id IS NULL THEN 'Block not found'
        WHEN l.language_id IS NULL THEN 'Language not found'
        ELSE 'Unknown issue'
    END as diagnosis
FROM spots s
LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
LEFT JOIN language_blocks lb ON COALESCE(slb.block_id, slb.primary_block_id) = lb.block_id
LEFT JOIN languages l ON lb.language_id = l.language_id
WHERE s.spot_id IN ({', '.join(str(x) for x in list(analysis.get('missing_spots', []))[:10])})
```

## 📊 IMPACT ASSESSMENT

The discrepancy of {analysis['missing_count']:,} spots worth ${analysis['missing_revenue']:,.2f} represents:
- **Revenue Impact**: {(analysis['missing_revenue'] / (analysis['missing_revenue'] + 2752138.23)) * 100:.1f}% of expected language revenue
- **Spot Count Impact**: {(analysis['missing_count'] / analysis['total_category_spots']) * 100:.1f}% of category spots

This explains why your Individual Language Blocks category shows ${analysis['missing_revenue']:,.2f} more than the Language Analysis breakdown.
"""
        
        return report

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Language Mismatch Diagnostic Tool")
    parser.add_argument("--year", default="2022", help="Year to analyze")
    parser.add_argument("--db-path", default="data/database/production.db", help="Database path")
    parser.add_argument("--output", help="Save report to file")
    
    args = parser.parse_args()
    
    try:
        with LanguageMismatchDiagnostic(args.db_path) as diagnostic:
            report = diagnostic.generate_diagnostic_report(args.year)
            
            if args.output:
                with open(args.output, 'w') as f:
                    f.write(report)
                print(f"✅ Diagnostic report saved to {args.output}")
            else:
                print(report)
                
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()