#!/usr/bin/env python3
"""
Test to verify what the roadblocks detailed analysis should show
"""

import sqlite3

def test_roadblocks_precedence():
    """Test what roadblocks remain after precedence"""
    
    with sqlite3.connect("data/database/production.db") as db:
        cursor = db.cursor()
        
        # Get all roadblock spots
        cursor.execute("""
            SELECT DISTINCT s.spot_id
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            WHERE s.broadcast_month LIKE '%-24'
            AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
            AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
            AND slb.campaign_type = 'roadblock'
        """)
        all_roadblocks = set(row[0] for row in cursor.fetchall())
        print(f"All roadblocks: {len(all_roadblocks):,}")
        
        # Get Direct Response spots
        cursor.execute("""
            SELECT DISTINCT s.spot_id
            FROM spots s
            LEFT JOIN agencies a ON s.agency_id = a.agency_id
            WHERE s.broadcast_month LIKE '%-24'
            AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
            AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
            AND (COALESCE(a.agency_name, '') LIKE '%WorldLink%' OR 
                 COALESCE(s.bill_code, '') LIKE '%WorldLink%')
        """)
        direct_response = set(row[0] for row in cursor.fetchall())
        
        # Get Individual Language spots
        cursor.execute("""
            SELECT DISTINCT s.spot_id
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            LEFT JOIN agencies a ON s.agency_id = a.agency_id
            WHERE s.broadcast_month LIKE '%-24'
            AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
            AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
            AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
            AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
            AND ((slb.spans_multiple_blocks = 0 AND slb.block_id IS NOT NULL) OR 
                 (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NOT NULL))
        """)
        individual_language = set(row[0] for row in cursor.fetchall())
        
        # Get Multi-Language spots
        cursor.execute("""
            SELECT DISTINCT s.spot_id
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            LEFT JOIN agencies a ON s.agency_id = a.agency_id
            LEFT JOIN customers c ON s.customer_id = c.customer_id
            WHERE s.broadcast_month LIKE '%-24'
            AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
            AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
            AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
            AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
            AND (slb.spans_multiple_blocks = 1 OR 
                 (slb.spans_multiple_blocks = 0 AND slb.block_id IS NULL) OR 
                 (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NULL))
            AND COALESCE(c.normalized_name, '') NOT LIKE '%NKB%'
            AND COALESCE(s.bill_code, '') NOT LIKE '%NKB%'
        """)
        multi_language = set(row[0] for row in cursor.fetchall())
        
        # Apply precedence
        remaining = all_roadblocks.copy()
        
        dr_claimed = remaining & direct_response
        remaining -= dr_claimed
        print(f"Direct Response claimed: {len(dr_claimed):,}")
        
        il_claimed = remaining & individual_language
        remaining -= il_claimed
        print(f"Individual Language claimed: {len(il_claimed):,}")
        
        ml_claimed = remaining & multi_language
        remaining -= ml_claimed
        print(f"Multi-Language claimed: {len(ml_claimed):,}")
        
        print(f"Remaining for Roadblocks: {len(remaining):,}")
        
        # Get revenue for remaining spots
        if remaining:
            placeholders = ','.join(['?' for _ in remaining])
            cursor.execute(f"""
                SELECT SUM(COALESCE(s.gross_rate, 0))
                FROM spots s
                WHERE s.spot_id IN ({placeholders})
            """, list(remaining))
            revenue = cursor.fetchone()[0] or 0
            print(f"Revenue for remaining: ${revenue:,.2f}")
        else:
            print("Revenue for remaining: $0.00")

if __name__ == "__main__":
    test_roadblocks_precedence()