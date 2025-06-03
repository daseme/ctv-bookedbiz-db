-- Monthly Revenue Summary with Total Row (Fixed Sorting)
SELECT
  CASE
    strftime('%m', broadcast_month)
    WHEN '01' THEN 'January'
    WHEN '02' THEN 'February'
    WHEN '03' THEN 'March'
    WHEN '04' THEN 'April'
    WHEN '05' THEN 'May'
    WHEN '06' THEN 'June'
    WHEN '07' THEN 'July'
    WHEN '08' THEN 'August'
    WHEN '09' THEN 'September'
    WHEN '10' THEN 'October'
    WHEN '11' THEN 'November'
    WHEN '12' THEN 'December'
  END as month_name,
  strftime('%m', broadcast_month) as sort_order,
  COUNT(*) as spot_count,
  ROUND(SUM(gross_rate), 2) as total_revenue,
  ROUND(AVG(gross_rate), 2) as avg_rate,
  MIN(gross_rate) as min_rate,
  MAX(gross_rate) as max_rate
FROM
  spots
WHERE
  broadcast_month IS NOT NULL
  AND gross_rate IS NOT NULL
  AND (
    revenue_type != 'Trade'
    OR revenue_type IS NULL
  )
GROUP BY
  strftime('%m', broadcast_month)
UNION ALL
  -- TOTAL ROW
SELECT
  '*** TOTAL ***' as month_name,
  '99' as sort_order,
  -- Sort to bottom
  COUNT(*) as spot_count,
  ROUND(SUM(gross_rate), 2) as total_revenue,
  ROUND(AVG(gross_rate), 2) as avg_rate,
  MIN(gross_rate) as min_rate,
  MAX(gross_rate) as max_rate
FROM
  spots
WHERE
  broadcast_month IS NOT NULL
  AND gross_rate IS NOT NULL
  AND (
    revenue_type != 'Trade'
    OR revenue_type IS NULL
  )
ORDER BY
  sort_order;