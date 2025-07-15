-- Test Impact of Adding Packages Category
-- Run this to see how many spots will move from Other Non-Language to Packages
-- Replace '24' with your year suffix

.headers on
.mode table

-- Step 1: Check current PKG spots in Other Non-Language
SELECT 'CURRENT PKG SPOTS IN OTHER NON-LANGUAGE' as analysis_type;

WITH base_spots AS (
    SELECT spot_id FROM spots s
    WHERE s.broadcast_month LIKE '%-24'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
),
direct_response AS (
    SELECT DISTINCT s.spot_id
    FROM spots s
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    WHERE s.broadcast_month LIKE '%-24'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND (COALESCE(a.agency_name, '') LIKE '%WorldLink%' OR COALESCE(s.bill_code, '') LIKE '%WorldLink%')
),
paid_programming AS (
    SELECT DISTINCT s.spot_id
    FROM spots s
    WHERE s.broadcast_month LIKE '%-24'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND s.revenue_type = 'Paid Programming'
),
branded_content AS (
    SELECT DISTINCT s.spot_id
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    WHERE s.broadcast_month LIKE '%-24'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND slb.spot_id IS NULL
    AND s.spot_type = 'PRD'
),
services AS (
    SELECT DISTINCT s.spot_id
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    WHERE s.broadcast_month LIKE '%-24'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND slb.spot_id IS NULL
    AND s.spot_type = 'SVC'
),
individual_language AS (
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
),
ros AS (
    SELECT DISTINCT s.spot_id
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    WHERE s.broadcast_month LIKE '%-24'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND slb.business_rule_applied IN ('ros_duration', 'ros_time')
    AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
    AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
    AND s.revenue_type != 'Paid Programming'
    AND NOT (slb.spot_id IS NULL AND s.spot_type = 'PRD')
    AND NOT (slb.spot_id IS NULL AND s.spot_type = 'SVC')
),
multi_language AS (
    SELECT DISTINCT s.spot_id
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    WHERE s.broadcast_month LIKE '%-24'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND slb.campaign_type = 'multi_language'
),
current_other_non_language AS (
    SELECT spot_id FROM base_spots
    EXCEPT
    SELECT spot_id FROM direct_response
    EXCEPT
    SELECT spot_id FROM paid_programming
    EXCEPT
    SELECT spot_id FROM branded_content
    EXCEPT
    SELECT spot_id FROM services
    EXCEPT
    SELECT spot_id FROM individual_language
    EXCEPT
    SELECT spot_id FROM ros
    EXCEPT
    SELECT spot_id FROM multi_language
)
SELECT 
    s.spot_type,
    CASE 
        WHEN s.time_in IS NULL AND s.time_out IS NULL THEN 'Both NULL'
        WHEN s.time_in IS NULL THEN 'time_in NULL'
        WHEN s.time_out IS NULL THEN 'time_out NULL'
        WHEN s.time_in = '' AND s.time_out = '' THEN 'Both Empty'
        WHEN s.time_in = '' THEN 'time_in Empty'
        WHEN s.time_out = '' THEN 'time_out Empty'
        ELSE 'Has Times'
    END as time_status,
    COUNT(*) as spots,
    SUM(COALESCE(s.gross_rate, 0)) as revenue,
    CASE 
        WHEN s.spot_type = 'PKG' AND (s.time_in IS NULL OR s.time_out IS NULL OR s.time_in = '' OR s.time_out = '') 
        THEN 'WILL MOVE TO PACKAGES'
        ELSE 'STAYS IN OTHER NON-LANGUAGE'
    END as impact
FROM current_other_non_language onl
JOIN spots s ON onl.spot_id = s.spot_id
GROUP BY s.spot_type, 
         CASE 
             WHEN s.time_in IS NULL AND s.time_out IS NULL THEN 'Both NULL'
             WHEN s.time_in IS NULL THEN 'time_in NULL'
             WHEN s.time_out IS NULL THEN 'time_out NULL'
             WHEN s.time_in = '' AND s.time_out = '' THEN 'Both Empty'
             WHEN s.time_in = '' THEN 'time_in Empty'
             WHEN s.time_out = '' THEN 'time_out Empty'
             ELSE 'Has Times'
         END,
         CASE 
             WHEN s.spot_type = 'PKG' AND (s.time_in IS NULL OR s.time_out IS NULL OR s.time_in = '' OR s.time_out = '') 
             THEN 'WILL MOVE TO PACKAGES'
             ELSE 'STAYS IN OTHER NON-LANGUAGE'
         END
ORDER BY revenue DESC;

-- Step 2: Summary of impact
SELECT '' as separator;
SELECT 'IMPACT SUMMARY' as analysis_type;

WITH base_spots AS (
    SELECT spot_id FROM spots s
    WHERE s.broadcast_month LIKE '%-24'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
),
direct_response AS (
    SELECT DISTINCT s.spot_id
    FROM spots s
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    WHERE s.broadcast_month LIKE '%-24'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND (COALESCE(a.agency_name, '') LIKE '%WorldLink%' OR COALESCE(s.bill_code, '') LIKE '%WorldLink%')
),
paid_programming AS (
    SELECT DISTINCT s.spot_id
    FROM spots s
    WHERE s.broadcast_month LIKE '%-24'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND s.revenue_type = 'Paid Programming'
),
branded_content AS (
    SELECT DISTINCT s.spot_id
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    WHERE s.broadcast_month LIKE '%-24'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND slb.spot_id IS NULL
    AND s.spot_type = 'PRD'
),
services AS (
    SELECT DISTINCT s.spot_id
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    WHERE s.broadcast_month LIKE '%-24'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND slb.spot_id IS NULL
    AND s.spot_type = 'SVC'
),
individual_language AS (
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
),
ros AS (
    SELECT DISTINCT s.spot_id
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    WHERE s.broadcast_month LIKE '%-24'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND slb.business_rule_applied IN ('ros_duration', 'ros_time')
    AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
    AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
    AND s.revenue_type != 'Paid Programming'
    AND NOT (slb.spot_id IS NULL AND s.spot_type = 'PRD')
    AND NOT (slb.spot_id IS NULL AND s.spot_type = 'SVC')
),
multi_language AS (
    SELECT DISTINCT s.spot_id
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    WHERE s.broadcast_month LIKE '%-24'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND slb.campaign_type = 'multi_language'
),
current_other_non_language AS (
    SELECT spot_id FROM base_spots
    EXCEPT
    SELECT spot_id FROM direct_response
    EXCEPT
    SELECT spot_id FROM paid_programming
    EXCEPT
    SELECT spot_id FROM branded_content
    EXCEPT
    SELECT spot_id FROM services
    EXCEPT
    SELECT spot_id FROM individual_language
    EXCEPT
    SELECT spot_id FROM ros
    EXCEPT
    SELECT spot_id FROM multi_language
),
new_packages AS (
    SELECT DISTINCT s.spot_id
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    WHERE s.broadcast_month LIKE '%-24'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND s.spot_type = 'PKG'
    AND (s.time_in IS NULL OR s.time_out IS NULL OR s.time_in = '' OR s.time_out = '')
    AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
    AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
    AND s.revenue_type != 'Paid Programming'
    AND NOT (slb.spot_id IS NULL AND s.spot_type = 'PRD')
    AND NOT (slb.spot_id IS NULL AND s.spot_type = 'SVC')
),
new_other_non_language AS (
    SELECT spot_id FROM current_other_non_language
    EXCEPT
    SELECT spot_id FROM new_packages
)
SELECT 
    'Current Other Non-Language' as category,
    (SELECT COUNT(*) FROM current_other_non_language) as spots,
    (SELECT SUM(COALESCE(gross_rate, 0)) FROM spots WHERE spot_id IN (SELECT spot_id FROM current_other_non_language)) as revenue
UNION ALL
SELECT 
    'New Packages Category' as category,
    (SELECT COUNT(*) FROM new_packages) as spots,
    (SELECT SUM(COALESCE(gross_rate, 0)) FROM spots WHERE spot_id IN (SELECT spot_id FROM new_packages)) as revenue
UNION ALL
SELECT 
    'New Other Non-Language' as category,
    (SELECT COUNT(*) FROM new_other_non_language) as spots,
    (SELECT SUM(COALESCE(gross_rate, 0)) FROM spots WHERE spot_id IN (SELECT spot_id FROM new_other_non_language)) as revenue
UNION ALL
SELECT 
    'Verification (Packages + Other)' as category,
    (SELECT COUNT(*) FROM new_packages) + (SELECT COUNT(*) FROM new_other_non_language) as spots,
    (SELECT SUM(COALESCE(gross_rate, 0)) FROM spots WHERE spot_id IN (SELECT spot_id FROM new_packages)) + 
    (SELECT SUM(COALESCE(gross_rate, 0)) FROM spots WHERE spot_id IN (SELECT spot_id FROM new_other_non_language)) as revenue;

-- Step 3: Sample records that will move to Packages
SELECT '' as separator;
SELECT 'SAMPLE RECORDS MOVING TO PACKAGES' as analysis_type;

WITH base_spots AS (
    SELECT spot_id FROM spots s
    WHERE s.broadcast_month LIKE '%-24'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
),
direct_response AS (
    SELECT DISTINCT s.spot_id
    FROM spots s
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    WHERE s.broadcast_month LIKE '%-24'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND (COALESCE(a.agency_name, '') LIKE '%WorldLink%' OR COALESCE(s.bill_code, '') LIKE '%WorldLink%')
),
paid_programming AS (
    SELECT DISTINCT s.spot_id
    FROM spots s
    WHERE s.broadcast_month LIKE '%-24'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND s.revenue_type = 'Paid Programming'
),
branded_content AS (
    SELECT DISTINCT s.spot_id
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    WHERE s.broadcast_month LIKE '%-24'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND slb.spot_id IS NULL
    AND s.spot_type = 'PRD'
),
services AS (
    SELECT DISTINCT s.spot_id
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    WHERE s.broadcast_month LIKE '%-24'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND slb.spot_id IS NULL
    AND s.spot_type = 'SVC'
),
individual_language AS (
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
),
ros AS (
    SELECT DISTINCT s.spot_id
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    WHERE s.broadcast_month LIKE '%-24'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND slb.business_rule_applied IN ('ros_duration', 'ros_time')
    AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
    AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
    AND s.revenue_type != 'Paid Programming'
    AND NOT (slb.spot_id IS NULL AND s.spot_type = 'PRD')
    AND NOT (slb.spot_id IS NULL AND s.spot_type = 'SVC')
),
multi_language AS (
    SELECT DISTINCT s.spot_id
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    WHERE s.broadcast_month LIKE '%-24'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND slb.campaign_type = 'multi_language'
),
new_packages AS (
    SELECT DISTINCT s.spot_id
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    WHERE s.broadcast_month LIKE '%-24'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND s.spot_type = 'PKG'
    AND (s.time_in IS NULL OR s.time_out IS NULL OR s.time_in = '' OR s.time_out = '')
    AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
    AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
    AND s.revenue_type != 'Paid Programming'
    AND NOT (slb.spot_id IS NULL AND s.spot_type = 'PRD')
    AND NOT (slb.spot_id IS NULL AND s.spot_type = 'SVC')
)
SELECT 
    s.spot_id,
    s.bill_code,
    COALESCE(c.normalized_name, 'Unknown') as customer_name,
    COALESCE(a.agency_name, 'No Agency') as agency_name,
    s.gross_rate,
    s.spot_type,
    s.revenue_type,
    s.time_in,
    s.time_out,
    s.broadcast_month
FROM new_packages np
JOIN spots s ON np.spot_id = s.spot_id
LEFT JOIN customers c ON s.customer_id = c.customer_id
LEFT JOIN agencies a ON s.agency_id = a.agency_id
ORDER BY s.gross_rate DESC
LIMIT 10;