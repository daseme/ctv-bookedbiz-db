-- Migration 003: CRM Enhancements
-- Add contact_role and last_contacted to entity_contacts
-- Create entity_activity table for interaction tracking
-- Create saved_filters table for filter presets

-- ============================================================
-- 1. Extend entity_contacts table
-- ============================================================

-- Add contact_role column
-- Values: 'decision_maker', 'account_manager', 'billing', 'technical', 'other'
ALTER TABLE entity_contacts ADD COLUMN contact_role TEXT;

-- Add last_contacted timestamp
ALTER TABLE entity_contacts ADD COLUMN last_contacted TIMESTAMP;

-- ============================================================
-- 2. Create entity_activity table
-- ============================================================

CREATE TABLE IF NOT EXISTS entity_activity (
    activity_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL CHECK (entity_type IN ('customer', 'agency')),
    entity_id INTEGER NOT NULL,
    activity_type TEXT NOT NULL CHECK (activity_type IN ('note', 'call', 'email', 'meeting', 'status_change')),
    activity_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    description TEXT,
    created_by TEXT NOT NULL,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    contact_id INTEGER,
    FOREIGN KEY (contact_id) REFERENCES entity_contacts(contact_id) ON DELETE SET NULL
);

-- Index for efficient lookup by entity
CREATE INDEX IF NOT EXISTS idx_entity_activity_lookup
ON entity_activity(entity_type, entity_id, activity_date DESC);

-- ============================================================
-- 3. Create saved_filters table
-- ============================================================

CREATE TABLE IF NOT EXISTS saved_filters (
    filter_id INTEGER PRIMARY KEY AUTOINCREMENT,
    filter_name TEXT NOT NULL,
    filter_type TEXT NOT NULL DEFAULT 'address_book',
    filter_config TEXT NOT NULL,  -- JSON containing all filter parameters
    created_by TEXT NOT NULL,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_shared BOOLEAN DEFAULT 0
);

-- Index for lookup by filter type
CREATE INDEX IF NOT EXISTS idx_saved_filters_type
ON saved_filters(filter_type, filter_name);

-- ============================================================
-- 4. Log migration to canon_audit
-- ============================================================

INSERT INTO canon_audit (actor, action, key, value, extra)
VALUES (
    'migration',
    'SCHEMA_CHANGE',
    'migration:003_crm_enhancements',
    'complete',
    'Added contact_role, last_contacted to entity_contacts. Created entity_activity and saved_filters tables.'
);
