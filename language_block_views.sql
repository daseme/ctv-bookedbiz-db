-- Language Block Reporting Views
-- Create comprehensive reporting views for language block analysis

-- 1. Language Block Revenue Summary
CREATE VIEW IF NOT EXISTS language_block_revenue_summary AS
SELECT 
    lb.language_block_id,
    lb.block_name,
    lb.language_id,
    l.language_name,
    lb.market_id,
    m.market_name,
    lb.market_code,
    COUNT(DISTINCT s.spot_id) as total_spots,
    COUNT(DISTINCT s.customer_id) as unique_customers,
    SUM(s.revenue) as total_revenue,
    AVG(s.revenue) as avg_spot_revenue,
    MIN(s.air_date) as first_air_date,
    MAX(s.air_date) as last_air_date,
    strftime('%Y', s.air_date) as year,
    strftime('%Y-%m', s.air_date) as year_month,
    lb.is_active
FROM language_blocks lb
LEFT JOIN languages l ON lb.language_id = l.language_id
LEFT JOIN markets m ON lb.market_id = m.market_id
LEFT JOIN spots_with_language_blocks_enhanced slb ON lb.language_block_id = slb.language_block_id
LEFT JOIN spots s ON slb.spot_id = s.spot_id
WHERE lb.is_active = 1
GROUP BY 
    lb.language_block_id,
    lb.block_name,
    lb.language_id,
    l.language_name,
    lb.market_id,
    m.market_name,
    lb.market_code,
    strftime('%Y', s.air_date),
    strftime('%Y-%m', s.air_date),
    lb.is_active;

-- 2. Language Block Dashboard
CREATE VIEW IF NOT EXISTS language_block_dashboard AS
SELECT 
    lbrs.language_block_id,
    lbrs.block_name,
    lbrs.language_name,
    lbrs.market_name,
    lbrs.market_code,
    -- Current year metrics
    SUM(CASE WHEN lbrs.year = strftime('%Y', 'now') THEN lbrs.total_revenue ELSE 0 END) as current_year_revenue,
    SUM(CASE WHEN lbrs.year = strftime('%Y', 'now') THEN lbrs.total_spots ELSE 0 END) as current_year_spots,
    COUNT(CASE WHEN lbrs.year = strftime('%Y', 'now') THEN lbrs.unique_customers ELSE NULL END) as current_year_customers,
    
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
    lbrs.language_block_id,
    lbrs.block_name,
    lbrs.language_name,
    lbrs.market_name,
    lbrs.market_code
ORDER BY current_year_revenue DESC;
