-- Migration 007: AE Assignment History + Backfill
-- Creates ae_assignments table for CRM-style assignment tracking with history.
-- Backfills from 2025+ spot activity (most recent sales_person per entity).
-- Keeps assigned_ae on agencies/customers as denormalized current value.

-- ============================================================
-- 1. Create ae_assignments table
-- ============================================================
CREATE TABLE IF NOT EXISTS ae_assignments (
    assignment_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL CHECK (entity_type IN ('customer', 'agency')),
    entity_id INTEGER NOT NULL,
    ae_name TEXT NOT NULL,
    assigned_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ended_date TIMESTAMP,          -- NULL = current/active assignment
    created_by TEXT NOT NULL,       -- 'backfill', 'web_user', etc.
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_ae_assignments_lookup
    ON ae_assignments(entity_type, entity_id, ended_date);
CREATE INDEX IF NOT EXISTS idx_ae_assignments_ae
    ON ae_assignments(ae_name);

-- ============================================================
-- 2. Backfill agencies from 2025+ spot activity
-- ============================================================

-- Insert into ae_assignments for each agency with 2025+ spots
INSERT INTO ae_assignments (entity_type, entity_id, ae_name, assigned_date, created_by, notes)
SELECT
    'agency',
    sub.agency_id,
    sub.ae_name,
    sub.most_recent_date,
    'backfill',
    'Backfilled from most recent 2025+ spot activity'
FROM (
    SELECT
        s.agency_id,
        CASE s.sales_person
            WHEN 'Charmaine Lane - TRANSFER' THEN 'Charmaine Lane'
            WHEN 'Riley van Patten' THEN 'Riley Van Patten'
            ELSE s.sales_person
        END AS ae_name,
        MAX(s.air_date) AS most_recent_date
    FROM spots s
    WHERE s.agency_id IS NOT NULL
      AND s.air_date >= '2025-01-01'
      AND s.sales_person IS NOT NULL
      AND s.sales_person NOT IN ('House', 'Overseas Partnership', 'White Horse International')
      AND (s.revenue_type <> 'Trade' OR s.revenue_type IS NULL)
    GROUP BY s.agency_id
) sub;

-- Update denormalized assigned_ae on agencies table
UPDATE agencies
SET assigned_ae = (
    SELECT ae_name FROM ae_assignments
    WHERE entity_type = 'agency' AND entity_id = agencies.agency_id AND ended_date IS NULL
    LIMIT 1
)
WHERE agency_id IN (
    SELECT entity_id FROM ae_assignments WHERE entity_type = 'agency' AND ended_date IS NULL
);

-- ============================================================
-- 3. Backfill customers from 2025+ spot activity
-- ============================================================

-- Insert into ae_assignments for each customer with 2025+ spots
INSERT INTO ae_assignments (entity_type, entity_id, ae_name, assigned_date, created_by, notes)
SELECT
    'customer',
    sub.customer_id,
    sub.ae_name,
    sub.most_recent_date,
    'backfill',
    'Backfilled from most recent 2025+ spot activity'
FROM (
    SELECT
        s.customer_id,
        CASE s.sales_person
            WHEN 'Charmaine Lane - TRANSFER' THEN 'Charmaine Lane'
            WHEN 'Riley van Patten' THEN 'Riley Van Patten'
            ELSE s.sales_person
        END AS ae_name,
        MAX(s.air_date) AS most_recent_date
    FROM spots s
    WHERE s.customer_id IS NOT NULL
      AND s.air_date >= '2025-01-01'
      AND s.sales_person IS NOT NULL
      AND s.sales_person NOT IN ('House', 'Overseas Partnership', 'White Horse International')
      AND (s.revenue_type <> 'Trade' OR s.revenue_type IS NULL)
    GROUP BY s.customer_id
) sub;

-- Update denormalized assigned_ae on customers table
UPDATE customers
SET assigned_ae = (
    SELECT ae_name FROM ae_assignments
    WHERE entity_type = 'customer' AND entity_id = customers.customer_id AND ended_date IS NULL
    LIMIT 1
)
WHERE customer_id IN (
    SELECT entity_id FROM ae_assignments WHERE entity_type = 'customer' AND ended_date IS NULL
);

-- ============================================================
-- 4. Audit log
-- ============================================================
INSERT INTO canon_audit (actor, action, key, value, extra)
VALUES ('migration', 'SCHEMA_CHANGE', 'migration:007', 'ae_assignments_table',
        'Created ae_assignments table and backfilled from 2025+ spot activity');
