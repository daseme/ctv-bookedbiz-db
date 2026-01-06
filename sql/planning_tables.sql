-- ============================================================================
-- Planning Tool Schema Migration
-- Run against production.db
-- ============================================================================

-- ============================================================================
-- 1. Revenue Entities
-- Canonical list of AEs, House, and Agencies that generate revenue
-- ============================================================================

CREATE TABLE IF NOT EXISTS revenue_entities (
    entity_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_name TEXT NOT NULL UNIQUE,
    entity_type TEXT NOT NULL DEFAULT 'AE' CHECK (entity_type IN ('AE', 'House', 'Agency')),
    is_active BOOLEAN DEFAULT 1,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_revenue_entities_name 
ON revenue_entities(entity_name);

CREATE INDEX IF NOT EXISTS idx_revenue_entities_active 
ON revenue_entities(is_active) WHERE is_active = 1;

-- ============================================================================
-- 2. Forecast
-- Adjusted expectations by AE/month (defaults to budget until changed)
-- ============================================================================

CREATE TABLE IF NOT EXISTS forecast (
    forecast_id INTEGER PRIMARY KEY AUTOINCREMENT,
    ae_name TEXT NOT NULL,
    year INTEGER NOT NULL CHECK (year >= 2000 AND year <= 2100),
    month INTEGER NOT NULL CHECK (month >= 1 AND month <= 12),
    forecast_amount DECIMAL(12,2) NOT NULL,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_by TEXT,
    notes TEXT,
    UNIQUE(ae_name, year, month)
);

CREATE INDEX IF NOT EXISTS idx_forecast_ae_year_month 
ON forecast(ae_name, year, month);

CREATE INDEX IF NOT EXISTS idx_forecast_year_month 
ON forecast(year, month);

-- ============================================================================
-- 3. Forecast History
-- Audit trail of forecast changes for tracking planning session decisions
-- ============================================================================

CREATE TABLE IF NOT EXISTS forecast_history (
    history_id INTEGER PRIMARY KEY AUTOINCREMENT,
    ae_name TEXT NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    previous_amount DECIMAL(12,2),
    new_amount DECIMAL(12,2) NOT NULL,
    changed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    changed_by TEXT,
    session_notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_forecast_history_ae 
ON forecast_history(ae_name, year, month);

CREATE INDEX IF NOT EXISTS idx_forecast_history_date 
ON forecast_history(changed_date);

-- ============================================================================
-- 4. Populate Revenue Entities from Existing Data
-- ============================================================================

-- Insert from current year budget and spots
INSERT OR IGNORE INTO revenue_entities (entity_name, entity_type)
SELECT DISTINCT ae_name, 'AE'
FROM (
    SELECT ae_name FROM budget WHERE year = 2025
    UNION
    SELECT DISTINCT sales_person AS ae_name FROM spots 
    WHERE broadcast_month LIKE '%-25'
      AND (revenue_type != 'Trade' OR revenue_type IS NULL)
      AND sales_person IS NOT NULL 
      AND sales_person != ''
);

-- Update known non-AE entities (adjust these based on your actual data)
UPDATE revenue_entities SET entity_type = 'House' WHERE entity_name = 'House';
UPDATE revenue_entities SET entity_type = 'Agency' WHERE entity_name = 'WorldLink';

-- ============================================================================
-- 5. Validation View: Unmatched Revenue
-- Shows spots revenue not tied to a known revenue entity
-- ============================================================================

CREATE VIEW IF NOT EXISTS v_unmatched_revenue AS
SELECT 
    s.sales_person,
    s.broadcast_month,
    COUNT(*) AS spot_count,
    SUM(s.gross_rate) AS total_revenue
FROM spots s
LEFT JOIN revenue_entities re ON re.entity_name = s.sales_person
WHERE re.entity_id IS NULL
  AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
  AND s.sales_person IS NOT NULL 
  AND s.sales_person != ''
GROUP BY s.sales_person, s.broadcast_month
ORDER BY total_revenue DESC;

-- ============================================================================
-- 6. Planning View: Consolidated Planning Data
-- Combines budget, forecast, and booked for the planning tool
-- ============================================================================

CREATE VIEW IF NOT EXISTS v_planning_data AS
WITH booked AS (
    SELECT 
        sales_person AS ae_name,
        CAST(SUBSTR(broadcast_month, 5, 2) AS INTEGER) + 2000 AS year,
        CASE SUBSTR(broadcast_month, 1, 3)
            WHEN 'Jan' THEN 1 WHEN 'Feb' THEN 2 WHEN 'Mar' THEN 3
            WHEN 'Apr' THEN 4 WHEN 'May' THEN 5 WHEN 'Jun' THEN 6
            WHEN 'Jul' THEN 7 WHEN 'Aug' THEN 8 WHEN 'Sep' THEN 9
            WHEN 'Oct' THEN 10 WHEN 'Nov' THEN 11 WHEN 'Dec' THEN 12
        END AS month,
        SUM(gross_rate) AS booked_amount
    FROM spots
    WHERE (revenue_type != 'Trade' OR revenue_type IS NULL)
      AND sales_person IS NOT NULL
    GROUP BY sales_person, broadcast_month
)
SELECT 
    re.entity_id,
    re.entity_name,
    re.entity_type,
    b.year,
    b.month,
    COALESCE(b.budget_amount, 0) AS budget,
    COALESCE(f.forecast_amount, b.budget_amount, 0) AS forecast,
    COALESCE(bk.booked_amount, 0) AS booked,
    COALESCE(f.forecast_amount, b.budget_amount, 0) - COALESCE(bk.booked_amount, 0) AS pipeline,
    COALESCE(f.forecast_amount, b.budget_amount, 0) - COALESCE(b.budget_amount, 0) AS variance_to_budget,
    f.updated_date AS forecast_updated,
    f.updated_by AS forecast_updated_by
FROM revenue_entities re
CROSS JOIN (SELECT DISTINCT year, month FROM budget) ym
LEFT JOIN budget b 
    ON b.ae_name = re.entity_name 
    AND b.year = ym.year 
    AND b.month = ym.month
LEFT JOIN forecast f 
    ON f.ae_name = re.entity_name 
    AND f.year = ym.year 
    AND f.month = ym.month
LEFT JOIN booked bk 
    ON bk.ae_name = re.entity_name 
    AND bk.year = ym.year 
    AND bk.month = ym.month
WHERE re.is_active = 1;

-- ============================================================================
-- End of Migration
-- ============================================================================