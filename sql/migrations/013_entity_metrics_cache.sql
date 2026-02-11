-- Migration 013: Materialized entity metrics cache
-- Pre-computes spots GROUP BY aggregates for fast address book loading.
-- Spots data only changes on daily/monthly import, so this cache is refreshed after each import.

CREATE TABLE IF NOT EXISTS entity_metrics (
    entity_type TEXT NOT NULL,          -- 'agency' or 'customer'
    entity_id INTEGER NOT NULL,
    markets TEXT,                        -- comma-separated distinct market names
    last_active TEXT,                    -- MAX(air_date)
    total_revenue REAL DEFAULT 0,       -- SUM(gross_rate) excluding Trade
    spot_count INTEGER DEFAULT 0,       -- COUNT(*)
    agency_spot_count INTEGER DEFAULT 0, -- COUNT(agency_id) for agency-client detection (customers only)
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (entity_type, entity_id)
);

-- Populate: agencies
INSERT OR IGNORE INTO entity_metrics (entity_type, entity_id, markets, last_active, total_revenue, spot_count)
SELECT
    'agency',
    agency_id,
    GROUP_CONCAT(DISTINCT CASE WHEN market_name != '' THEN market_name END),
    MAX(air_date),
    SUM(CASE WHEN revenue_type != 'Trade' OR revenue_type IS NULL THEN gross_rate ELSE 0 END),
    COUNT(*)
FROM spots
WHERE agency_id IS NOT NULL
GROUP BY agency_id;

-- Populate: customers
INSERT OR IGNORE INTO entity_metrics (entity_type, entity_id, markets, last_active, total_revenue, spot_count, agency_spot_count)
SELECT
    'customer',
    customer_id,
    GROUP_CONCAT(DISTINCT CASE WHEN market_name != '' THEN market_name END),
    MAX(air_date),
    SUM(CASE WHEN revenue_type != 'Trade' OR revenue_type IS NULL THEN gross_rate ELSE 0 END),
    COUNT(*),
    COUNT(agency_id)
FROM spots
WHERE customer_id IS NOT NULL
GROUP BY customer_id;
