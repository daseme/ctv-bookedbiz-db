-- Language Block Reporting Views (CORRECTED)
-- Using actual table structure with block_id instead of language_block_id

-- 1. Language Block Revenue Summary
CREATE VIEW IF NOT EXISTS language_block_revenue_summary AS
SELECT 
    lb.block_id,
    lb.block_name,
    lb.language_id,
    l.language_name,
    lb.schedule_id,
    ps.schedule_name,
    swlb.market_code,
    swlb.market_display_name,
    lb.block_type,
    lb.day_part,
    lb.time_start,
    lb.time_end,
    COUNT(DISTINCT swlb.spot_id) as total_spots,
    COUNT(DISTINCT swlb.customer_name) as unique_customers,
    SUM(swlb.station_net) as total_revenue,
    AVG(swlb.station_net) as avg_spot_revenue,
    MIN(swlb.air_date) as first_air_date,
    MAX(swlb.air_date) as last_air_date,
    strftime('%Y', swlb.air_date) as year,
    strftime('%Y-%m', swlb.air_date) as year_month,
    lb.is_active
FROM language_blocks lb
LEFT JOIN languages l ON lb.language_id = l.language_id
LEFT JOIN programming_schedules ps ON lb.schedule_id = ps.schedule_id
LEFT JOIN spots_with_language_blocks_enhanced swlb ON lb.block_id = swlb.block_id
WHERE lb.is_active = 1 AND swlb.spot_id IS NOT NULL
GROUP BY 
    lb.block_id,
    lb.block_name,
    lb.language_id,
    l.language_name,
    lb.schedule_id,
    ps.schedule_name,
    swlb.market_code,
    swlb.market_display_name,
    lb.block_type,
    lb.day_part,
    lb.time_start,
    lb.time_end,
    strftime('%Y', swlb.air_date),
    strftime('%Y-%m', swlb.air_date),
    lb.is_active;

-- 2. Language Block Dashboard
CREATE VIEW IF NOT EXISTS language_block_dashboard AS
SELECT 
    lbrs.block_id,
    lbrs.block_name,
    lbrs.language_name,
    lbrs.schedule_name,
    lbrs.market_code,
    lbrs.market_display_name,
    lbrs.block_type,
    lbrs.day_part,
    lbrs.time_start,
    lbrs.time_end,
    -- Current year metrics
    SUM(CASE WHEN lbrs.year = strftime('%Y', 'now') THEN lbrs.total_revenue ELSE 0 END) as current_year_revenue,
    SUM(CASE WHEN lbrs.year = strftime('%Y', 'now') THEN lbrs.total_spots ELSE 0 END) as current_year_spots,
    SUM(CASE WHEN lbrs.year = strftime('%Y', 'now') THEN lbrs.unique_customers ELSE 0 END) as current_year_customers,
    
    -- Previous year metrics
    SUM(CASE WHEN lbrs.year = CAST(strftime('%Y', 'now') - 1 AS TEXT) THEN lbrs.total_revenue ELSE 0 END) as previous_year_revenue,
    SUM(CASE WHEN lbrs.year = CAST(strftime('%Y', 'now') - 1 AS TEXT) THEN lbrs.total_spots ELSE 0 END) as previous_year_spots,
    
    -- Calculate year-over-year growth
    CASE 
        WHEN SUM(CASE WHEN lbrs.year = CAST(strftime('%Y', 'now') - 1 AS TEXT) THEN lbrs.total_revenue ELSE 0 END) > 0 THEN
            ROUND(
                ((SUM(CASE WHEN lbrs.year = strftime('%Y', 'now') THEN lbrs.total_revenue ELSE 0 END) - 
                  SUM(CASE WHEN lbrs.year = CAST(strftime('%Y', 'now') - 1 AS TEXT) THEN lbrs.total_revenue ELSE 0 END)) * 100.0 / 
                  SUM(CASE WHEN lbrs.year = CAST(strftime('%Y', 'now') - 1 AS TEXT) THEN lbrs.total_revenue ELSE 0 END)), 2
            )
        ELSE NULL
    END as yoy_revenue_growth_pct,
    
    -- Performance rating
    CASE 
        WHEN SUM(CASE WHEN lbrs.year = strftime('%Y', 'now') THEN lbrs.total_revenue ELSE 0 END) >= 100000 THEN 'Excellent'
        WHEN SUM(CASE WHEN lbrs.year = strftime('%Y', 'now') THEN lbrs.total_revenue ELSE 0 END) >= 50000 THEN 'Good'
        WHEN SUM(CASE WHEN lbrs.year = strftime('%Y', 'now') THEN lbrs.total_revenue ELSE 0 END) >= 10000 THEN 'Fair'
        ELSE 'Needs Attention'
    END as performance_rating,
    
    -- Last activity date
    MAX(lbrs.last_air_date) as last_activity_date
FROM language_block_revenue_summary lbrs
GROUP BY 
    lbrs.block_id,
    lbrs.block_name,
    lbrs.language_name,
    lbrs.schedule_name,
    lbrs.market_code,
    lbrs.market_display_name,
    lbrs.block_type,
    lbrs.day_part,
    lbrs.time_start,
    lbrs.time_end
ORDER BY current_year_revenue DESC;

-- 3. Top Language Blocks
CREATE VIEW IF NOT EXISTS top_language_blocks AS
SELECT 
    lbd.block_id,
    lbd.block_name,
    lbd.language_name,
    lbd.schedule_name,
    lbd.market_display_name,
    lbd.block_type,
    lbd.day_part,
    lbd.time_start,
    lbd.time_end,
    lbd.current_year_revenue,
    lbd.current_year_spots,
    lbd.current_year_customers,
    lbd.yoy_revenue_growth_pct,
    lbd.performance_rating,
    lbd.last_activity_date,
    -- Rank by revenue
    ROW_NUMBER() OVER (ORDER BY lbd.current_year_revenue DESC) as revenue_rank,
    -- Rank by growth
    ROW_NUMBER() OVER (ORDER BY lbd.yoy_revenue_growth_pct DESC) as growth_rank
FROM language_block_dashboard lbd
WHERE lbd.current_year_revenue > 0
ORDER BY lbd.current_year_revenue DESC
LIMIT 20;
