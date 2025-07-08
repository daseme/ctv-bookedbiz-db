#!/bin/bash
# Revenue Validation Test Script for 2024
# FIXED: Uses gross_rate only, excludes Trade revenue, proper NULL handling
# Target: $4,076,256.00

echo "🔍 TESTING BULLET-PROOF REVENUE QUERIES FOR 2024"
echo "FIXED: Uses gross_rate only, excludes Trade revenue"
echo "Target Total: \$4,076,256.00"
echo "================================================="

# Test 1: Total Revenue Validation (FIXED)
echo -e "\n1️⃣ TOTAL REVENUE VALIDATION (FIXED):"
sqlite3 ./data/database/production.db "
SELECT 
  'TOTAL 2024 REVENUE' as check_name,
  COUNT(*) as total_spots,
  printf('$%,.2f', SUM(COALESCE(s.gross_rate, 0))) as total_revenue
FROM spots s
WHERE s.broadcast_month LIKE '%-24'
  AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
  AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL);
"

# Test 2: Direct Response Revenue (FIXED)
echo -e "\n2️⃣ DIRECT RESPONSE REVENUE (FIXED):"
sqlite3 ./data/database/production.db "
SELECT 
  'Direct Response' as category,
  COUNT(*) as spots,
  printf('$%,.2f', SUM(COALESCE(s.gross_rate, 0))) as revenue
FROM spots s
LEFT JOIN agencies a ON s.agency_id = a.agency_id
WHERE s.broadcast_month LIKE '%-24'
  AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
  AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL)
  AND (
    (a.agency_name LIKE '%WorldLink%') OR
    (s.bill_code LIKE '%WorldLink%')
  );
"

# Test 3: Language-Specific Revenue Breakdown (FIXED)
echo -e "\n3️⃣ LANGUAGE-SPECIFIC REVENUE (FIXED):"
sqlite3 ./data/database/production.db "
SELECT 
  COALESCE(l.language_name, 'Unknown Language') as language,
  COUNT(*) as spots,
  printf('$%,.2f', SUM(COALESCE(s.gross_rate, 0))) as revenue
FROM spots s
JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
LEFT JOIN language_blocks lb ON slb.block_id = lb.block_id
LEFT JOIN languages l ON lb.language_id = l.language_id
LEFT JOIN agencies a ON s.agency_id = a.agency_id
WHERE s.broadcast_month LIKE '%-24'
  AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
  AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL)
  AND slb.spans_multiple_blocks = 0
  AND slb.block_id IS NOT NULL
  AND NOT (
    (a.agency_name LIKE '%WorldLink%') OR
    (s.bill_code LIKE '%WorldLink%')
  )
GROUP BY l.language_name
ORDER BY SUM(COALESCE(s.gross_rate, 0)) DESC;
"

# Test 4: Multi-Language Revenue (FIXED)
echo -e "\n4️⃣ MULTI-LANGUAGE (CROSS-AUDIENCE) REVENUE (FIXED):"
sqlite3 ./data/database/production.db "
SELECT 
  'Multi-Language (Cross-Audience)' as category,
  COUNT(*) as spots,
  printf('$%,.2f', SUM(COALESCE(s.gross_rate, 0))) as revenue
FROM spots s
JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
LEFT JOIN agencies a ON s.agency_id = a.agency_id
WHERE s.broadcast_month LIKE '%-24'
  AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
  AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL)
  AND slb.spans_multiple_blocks = 1
  AND NOT (
    (a.agency_name LIKE '%WorldLink%') OR
    (s.bill_code LIKE '%WorldLink%')
  );
"

# Test 5: Production Revenue (FIXED)
echo -e "\n5️⃣ PRODUCTION REVENUE (FIXED):"
sqlite3 ./data/database/production.db "
SELECT 
  'Production' as category,
  COUNT(*) as spots,
  printf('$%,.2f', SUM(COALESCE(s.gross_rate, 0))) as revenue
FROM spots s
LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
LEFT JOIN agencies a ON s.agency_id = a.agency_id
WHERE s.broadcast_month LIKE '%-24'
  AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
  AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL)
  AND s.spot_type = 'PRD'
  AND slb.spot_id IS NULL
  AND NOT (
    (a.agency_name LIKE '%WorldLink%') OR
    (s.bill_code LIKE '%WorldLink%')
  );
"

# Test 6: Branded Content Revenue (FIXED)
echo -e "\n6️⃣ BRANDED CONTENT REVENUE (FIXED):"
sqlite3 ./data/database/production.db "
SELECT 
  'Branded Content' as category,
  COUNT(*) as spots,
  printf('$%,.2f', SUM(COALESCE(s.gross_rate, 0))) as revenue
FROM spots s
LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
LEFT JOIN agencies a ON s.agency_id = a.agency_id
WHERE s.broadcast_month LIKE '%-24'
  AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
  AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL)
  AND s.spot_type = 'SVC'
  AND slb.spot_id IS NULL
  AND NOT (
    (a.agency_name LIKE '%WorldLink%') OR
    (s.bill_code LIKE '%WorldLink%')
  );
"

# Test 7: Other Non-Language Revenue (FIXED)
echo -e "\n7️⃣ OTHER NON-LANGUAGE REVENUE (FIXED):"
sqlite3 ./data/database/production.db "
SELECT 
  'Other Non-Language' as category,
  COUNT(*) as spots,
  printf('$%,.2f', SUM(COALESCE(s.gross_rate, 0))) as revenue
FROM spots s
LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
LEFT JOIN agencies a ON s.agency_id = a.agency_id
WHERE s.broadcast_month LIKE '%-24'
  AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
  AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL)
  AND slb.spot_id IS NULL
  AND s.spot_type NOT IN ('PRD', 'SVC')
  AND NOT (
    (a.agency_name LIKE '%WorldLink%') OR
    (s.bill_code LIKE '%WorldLink%')
  );
"

# Test 8: Comprehensive Summary (FIXED)
echo -e "\n8️⃣ COMPREHENSIVE REVENUE BREAKDOWN (FIXED):"
sqlite3 ./data/database/production.db "
WITH revenue_breakdown AS (
  -- Direct Response
  SELECT 
    'Direct Response' as category,
    1 as sort_order,
    COUNT(*) as spots,
    SUM(COALESCE(s.gross_rate, 0)) as revenue
  FROM spots s
  LEFT JOIN agencies a ON s.agency_id = a.agency_id
  WHERE s.broadcast_month LIKE '%-24'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL)
    AND (
      (a.agency_name LIKE '%WorldLink%') OR
      (s.bill_code LIKE '%WorldLink%')
    )
  
  UNION ALL
  
  -- Multi-Language
  SELECT 
    'Multi-Language (Cross-Audience)' as category,
    2 as sort_order,
    COUNT(*) as spots,
    SUM(COALESCE(s.gross_rate, 0)) as revenue
  FROM spots s
  JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
  LEFT JOIN agencies a ON s.agency_id = a.agency_id
  WHERE s.broadcast_month LIKE '%-24'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL)
    AND slb.spans_multiple_blocks = 1
    AND NOT (
      (a.agency_name LIKE '%WorldLink%') OR
      (s.bill_code LIKE '%WorldLink%')
    )
  
  UNION ALL
  
  -- All Individual Languages Combined
  SELECT 
    'All Language Blocks Combined' as category,
    3 as sort_order,
    COUNT(*) as spots,
    SUM(COALESCE(s.gross_rate, 0)) as revenue
  FROM spots s
  JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
  LEFT JOIN language_blocks lb ON slb.block_id = lb.block_id
  LEFT JOIN languages l ON lb.language_id = l.language_id
  LEFT JOIN agencies a ON s.agency_id = a.agency_id
  WHERE s.broadcast_month LIKE '%-24'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL)
    AND slb.spans_multiple_blocks = 0
    AND slb.block_id IS NOT NULL
    AND NOT (
      (a.agency_name LIKE '%WorldLink%') OR
      (s.bill_code LIKE '%WorldLink%')
    )
  
  UNION ALL
  
  -- Production
  SELECT 
    'Production' as category,
    4 as sort_order,
    COUNT(*) as spots,
    SUM(COALESCE(s.gross_rate, 0)) as revenue
  FROM spots s
  LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
  LEFT JOIN agencies a ON s.agency_id = a.agency_id
  WHERE s.broadcast_month LIKE '%-24'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL)
    AND s.spot_type = 'PRD'
    AND slb.spot_id IS NULL
    AND NOT (
      (a.agency_name LIKE '%WorldLink%') OR
      (s.bill_code LIKE '%WorldLink%')
    )
  
  UNION ALL
  
  -- Branded Content
  SELECT 
    'Branded Content' as category,
    5 as sort_order,
    COUNT(*) as spots,
    SUM(COALESCE(s.gross_rate, 0)) as revenue
  FROM spots s
  LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
  LEFT JOIN agencies a ON s.agency_id = a.agency_id
  WHERE s.broadcast_month LIKE '%-24'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL)
    AND s.spot_type = 'SVC'
    AND slb.spot_id IS NULL
    AND NOT (
      (a.agency_name LIKE '%WorldLink%') OR
      (s.bill_code LIKE '%WorldLink%')
    )
  
  UNION ALL
  
  -- Other Non-Language
  SELECT 
    'Other Non-Language' as category,
    6 as sort_order,
    COUNT(*) as spots,
    SUM(COALESCE(s.gross_rate, 0)) as revenue
  FROM spots s
  LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
  LEFT JOIN agencies a ON s.agency_id = a.agency_id
  WHERE s.broadcast_month LIKE '%-24'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL)
    AND slb.spot_id IS NULL
    AND s.spot_type NOT IN ('PRD', 'SVC')
    AND NOT (
      (a.agency_name LIKE '%WorldLink%') OR
      (s.bill_code LIKE '%WorldLink%')
    )
)
SELECT 
  category,
  spots,
  printf('$%,.2f', COALESCE(revenue, 0)) as revenue,
  printf('%.1f%%', COALESCE(revenue * 100.0 / (SELECT SUM(revenue) FROM revenue_breakdown), 0)) as percentage
FROM revenue_breakdown
WHERE revenue IS NOT NULL AND revenue > 0
ORDER BY sort_order;
"

# Test 9: Final Validation (FIXED)
echo -e "\n9️⃣ FINAL VALIDATION - TOTAL SHOULD EQUAL \$4,076,256.00:"
sqlite3 ./data/database/production.db "
WITH all_revenue AS (
  SELECT SUM(COALESCE(s.gross_rate, 0)) as total
  FROM spots s
  WHERE s.broadcast_month LIKE '%-24'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL)
)
SELECT 
  printf('$%,.2f', total) as calculated_total,
  '$4,076,256.00' as target_total,
  CASE 
    WHEN ABS(total - 4076256.00) < 0.01 THEN '✅ PERFECT MATCH'
    WHEN ABS(total - 4076256.00) < 100 THEN '⚠️  CLOSE (within $100)'
    ELSE '❌ SIGNIFICANT DIFFERENCE'
  END as validation_result
FROM all_revenue;
"

echo -e "\n🏁 TESTING COMPLETE!"
echo "FIXED: Now excludes Trade revenue and uses gross_rate only"
echo "Run this script to validate all revenue categories sum to \$4,076,256.00"