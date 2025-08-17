
-- Show sample data from key tables
SELECT 'TOTAL SPOTS ANALYSIS:' as info;
TOTAL SPOTS ANALYSIS:
SELECT COUNT(*) as total_spots, 
       SUM(gross_rate) as total_revenue,
       MIN(broadcast_month) as earliest_month,
       MAX(broadcast_month) as latest_month,
       COUNT(DISTINCT broadcast_month) as distinct_months
FROM spots 
WHERE revenue_type != 'Trade' OR revenue_type IS NULL;
742038|5829218.77744971|2024-01-15 00:00:00|2026-02-01|58

SELECT 'BROADCAST MONTH FORMAT:' as info;
BROADCAST MONTH FORMAT:
SELECT DISTINCT broadcast_month 
FROM spots 
WHERE broadcast_month IS NOT NULL 
ORDER BY broadcast_month 
LIMIT 10;
2024-01-15 00:00:00
2024-02-15 00:00:00
2024-03-15 00:00:00
2024-04-15 00:00:00
2024-05-15 00:00:00
2024-06-15 00:00:00
2024-07-15 00:00:00
2024-08-15 00:00:00
2024-09-15 00:00:00
2024-10-01 00:00:00

SELECT 'LANGUAGE BLOCK ASSIGNMENT STATUS:' as info;
LANGUAGE BLOCK ASSIGNMENT STATUS:
SELECT 
    'With Language Blocks' as category,
    COUNT(*) as spots,
    SUM(gross_rate) as revenue
FROM spots s
JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)

UNION ALL

SELECT 
    'Without Language Blocks' as category,
    COUNT(*) as spots,
    SUM(gross_rate) as revenue
FROM spots s
LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
WHERE slb.spot_id IS NULL
  AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL);
With Language Blocks|17354|642854.72597136
Without Language Blocks|724684|5186364.05147831

.output
