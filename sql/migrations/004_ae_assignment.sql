-- Migration 004: AE Assignment
-- Add assigned_ae column to agencies and customers tables

-- ============================================================
-- 1. Add assigned_ae to agencies
-- ============================================================

ALTER TABLE agencies ADD COLUMN assigned_ae TEXT;

CREATE INDEX IF NOT EXISTS idx_agencies_assigned_ae
ON agencies(assigned_ae);

-- ============================================================
-- 2. Add assigned_ae to customers
-- ============================================================

ALTER TABLE customers ADD COLUMN assigned_ae TEXT;

CREATE INDEX IF NOT EXISTS idx_customers_assigned_ae
ON customers(assigned_ae);

-- ============================================================
-- 3. Log migration to canon_audit
-- ============================================================

INSERT INTO canon_audit (actor, action, key, value, extra)
VALUES (
    'migration',
    'SCHEMA_CHANGE',
    'migration:004_ae_assignment',
    'complete',
    'Added assigned_ae column to agencies and customers tables.'
);
