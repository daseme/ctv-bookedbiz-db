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

-- 2. Monthly Language Block Performance
CREATE VIEW IF NOT EXISTS monthly_language_block_performance AS
SELECT 
    lbrs.language_block_id,
    lbrs.block_name,
    lbrs.language_name,
    lbrs.market_name,
    lbrs.market_code,
    lbrs.year,
    lbrs.year_month,
    lbrs.total_spots,
    lbrs.unique_customers,
    lbrs.total_revenue,
    lbrs.avg_spot_revenue,
    -- Calculate month-over-month growth
    LAG(lbrs.total_revenue) OVER (
        PARTITION BY lbrs.language_block_id 
        ORDER BY lbrs.year_month
    ) as prev_month_revenue,
    -- Calculate percentage change
    CASE 
        WHEN LAG(lbrs.total_revenue) OVER (
            PARTITION BY lbrs.language_block_id 
            ORDER BY lbrs.year_month
        ) > 0 THEN
            ROUND(
                ((lbrs.total_revenue - LAG(lbrs.total_revenue) OVER (
                    PARTITION BY lbrs.language_block_id 
                    ORDER BY lbrs.year_month
                )) * 100.0 / LAG(lbrs.total_revenue) OVER (
                    PARTITION BY lbrs.language_block_id 
                    ORDER BY lbrs.year_month
                )), 2
            )
        ELSE NULL
    END as revenue_growth_pct
FROM language_block_revenue_summary lbrs
WHERE lbrs.year_month IS NOT NULL
ORDER BY lbrs.language_block_id, lbrs.year_month;

-- 3. Language Block Customer Analysis
CREATE VIEW IF NOT EXISTS language_block_customer_analysis AS
SELECT 
    lb.language_block_id,
    lb.block_name,
    lb.language_id,
    l.language_name,
    lb.market_code,
    m.market_name,
    c.customer_id,
    c.customer_name,
    COUNT(s.spot_id) as total_spots,
    SUM(s.revenue) as total_revenue,
    AVG(s.revenue) as avg_spot_revenue,
    MIN(s.air_date) as first_air_date,
    MAX(s.air_date) as last_air_date,
    -- Calculate days between first and last spot
    CASE 
        WHEN MIN(s.air_date) != MAX(s.air_date) THEN
            CAST((julianday(MAX(s.air_date)) - julianday(MIN(s.air_date))) AS INTEGER)
        ELSE 0
    END as campaign_duration_days,
    -- Customer loyalty score (simplified)
    CASE 
        WHEN COUNT(s.spot_id) >= 50 THEN 'High'
        WHEN COUNT(s.spot_id) >= 20 THEN 'Medium'
        ELSE 'Low'
    END as customer_loyalty
FROM language_blocks lb
LEFT JOIN languages l ON lb.language_id = l.language_id
LEFT JOIN markets m ON lb.market_id = m.market_id
LEFT JOIN spots_with_language_blocks_enhanced slb ON lb.language_block_id = slb.language_block_id
LEFT JOIN spots s ON slb.spot_id = s.spot_id
LEFT JOIN customers c ON s.customer_id = c.customer_id
WHERE lb.is_active = 1 AND c.customer_id IS NOT NULL
GROUP BY 
    lb.language_block_id,
    lb.block_name,
    lb.language_id,
    l.language_name,
    lb.market_code,
    m.market_name,
    c.customer_id,
    c.customer_name
HAVING COUNT(s.spot_id) > 0
ORDER BY total_revenue DESC;

-- 4. Language Block Performance Dashboard
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

-- 5. Top Performing Language Blocks
CREATE VIEW IF NOT EXISTS top_language_blocks AS
SELECT 
    lbd.language_block_id,
    lbd.block_name,
    lbd.language_name,
    lbd.market_name,
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