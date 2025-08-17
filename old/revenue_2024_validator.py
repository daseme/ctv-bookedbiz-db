#!/usr/bin/env python3
import sqlite3
import pandas as pd

DB_PATH = "./data/database/production.db"
CSV_PATH = "./data/final_2024_revenue_breakdown.csv"

query = '''
WITH revenue_breakdown AS (
  SELECT 'Direct Response' as category, COUNT(*) as spots, SUM(gross_rate) as revenue
  FROM spots s LEFT JOIN agencies a ON s.agency_id = a.agency_id
  WHERE s.broadcast_month LIKE '%-24'
    AND (s.revenue_type IS NULL OR s.revenue_type != 'Trade')
    AND s.gross_rate IS NOT NULL AND s.gross_rate != 0
    AND (
      a.agency_name = 'WorldLink'
      OR s.bill_code = 'WorldLink'
      OR s.bill_code = 'WorldLink Broker Fees (DO NOT INVOICE)'
    )

  UNION ALL
  SELECT 'Multi-Language (Cross-Audience)', COUNT(*), SUM(s.gross_rate)
  FROM spots s
  JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
  LEFT JOIN agencies a ON s.agency_id = a.agency_id
  WHERE s.broadcast_month LIKE '%-24'
    AND (s.revenue_type IS NULL OR s.revenue_type != 'Trade')
    AND s.gross_rate IS NOT NULL AND s.gross_rate != 0
    AND slb.spans_multiple_blocks = 1
    AND NOT (
      a.agency_name = 'WorldLink'
      OR s.bill_code = 'WorldLink'
      OR s.bill_code = 'WorldLink Broker Fees (DO NOT INVOICE)'
    )

  UNION ALL
  SELECT COALESCE(l.language_name, 'Unknown Language'), COUNT(*), SUM(s.gross_rate)
  FROM spots s
  JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
  LEFT JOIN language_blocks lb ON slb.block_id = lb.block_id
  LEFT JOIN languages l ON lb.language_id = l.language_id
  LEFT JOIN agencies a ON s.agency_id = a.agency_id
  WHERE s.broadcast_month LIKE '%-24'
    AND (s.revenue_type IS NULL OR s.revenue_type != 'Trade')
    AND s.gross_rate IS NOT NULL AND s.gross_rate != 0
    AND slb.spans_multiple_blocks = 0
    AND slb.block_id IS NOT NULL
    AND slb.requires_attention = 0
    AND NOT (
      a.agency_name = 'WorldLink'
      OR s.bill_code = 'WorldLink'
      OR s.bill_code = 'WorldLink Broker Fees (DO NOT INVOICE)'
    )
  GROUP BY l.language_name

  UNION ALL
  SELECT 'Language Block (Requires Review)', COUNT(*), SUM(gross_rate)
  FROM spots s
  JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
  WHERE s.broadcast_month LIKE '%-24'
    AND s.gross_rate IS NOT NULL AND s.gross_rate != 0
    AND s.spot_type != 'BNS'
    AND (s.revenue_type IS NULL OR s.revenue_type != 'Trade')
    AND slb.spans_multiple_blocks = 0
    AND (slb.block_id IS NULL OR slb.requires_attention = 1)

  UNION ALL
  SELECT 'Branded Content', COUNT(*), SUM(gross_rate)
  FROM spots s
  LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
  LEFT JOIN agencies a ON s.agency_id = a.agency_id
  WHERE s.broadcast_month LIKE '%-24'
    AND s.gross_rate IS NOT NULL AND s.gross_rate != 0
    AND (s.revenue_type IS NULL OR s.revenue_type != 'Trade')
    AND s.spot_type IN ('PRD', 'SVC')
    AND slb.spot_id IS NULL
    AND NOT (
      a.agency_name = 'WorldLink'
      OR s.bill_code = 'WorldLink'
      OR s.bill_code = 'WorldLink Broker Fees (DO NOT INVOICE)'
    )

  UNION ALL
  SELECT 'Package (PKG)', COUNT(*), SUM(gross_rate)
  FROM spots s
  LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
  LEFT JOIN agencies a ON s.agency_id = a.agency_id
  WHERE s.broadcast_month LIKE '%-24'
    AND s.gross_rate IS NOT NULL AND s.gross_rate != 0
    AND (s.revenue_type IS NULL OR s.revenue_type != 'Trade')
    AND s.spot_type = 'PKG'
    AND slb.spot_id IS NULL
    AND NOT (
      a.agency_name = 'WorldLink'
      OR s.bill_code = 'WorldLink'
      OR s.bill_code = 'WorldLink Broker Fees (DO NOT INVOICE)'
    )

  UNION ALL
  SELECT 'Other Non-Language', COUNT(*), SUM(gross_rate)
  FROM spots s
  LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
  LEFT JOIN agencies a ON s.agency_id = a.agency_id
  WHERE s.broadcast_month LIKE '%-24'
    AND s.gross_rate IS NOT NULL AND s.gross_rate != 0
    AND (s.revenue_type IS NULL OR s.revenue_type != 'Trade')
    AND s.spot_type NOT IN ('PRD', 'SVC', 'PKG', 'BNS')
    AND slb.spot_id IS NULL
    AND NOT (
      a.agency_name = 'WorldLink'
      OR s.bill_code = 'WorldLink'
      OR s.bill_code = 'WorldLink Broker Fees (DO NOT INVOICE)'
    )
)
SELECT category, spots, ROUND(revenue, 2) AS revenue
FROM revenue_breakdown
ORDER BY revenue DESC;
'''

conn = sqlite3.connect(DB_PATH)
df = pd.read_sql_query(query, conn)
conn.close()

df.to_csv(CSV_PATH, index=False)
print(f"✅ Revenue data exported to: {CSV_PATH}")
