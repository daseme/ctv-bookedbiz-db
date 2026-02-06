-- Migration 002: Create entity_contacts table
-- Purpose: Unified contacts for agencies and customers
-- Run this migration with: sqlite3 production.db < sql/migrations/002_create_entity_contacts.sql

CREATE TABLE IF NOT EXISTS entity_contacts (
    contact_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL CHECK (entity_type IN ('customer', 'agency')),
    entity_id INTEGER NOT NULL,
    contact_name TEXT NOT NULL,
    contact_title TEXT,
    email TEXT,
    phone TEXT,
    is_primary BOOLEAN DEFAULT 0,
    is_active BOOLEAN DEFAULT 1,
    created_by TEXT NOT NULL,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notes TEXT
);

-- Index for fast lookup by entity
CREATE INDEX IF NOT EXISTS idx_entity_contacts_lookup
ON entity_contacts(entity_type, entity_id, is_active);

-- Index for primary contact lookup
CREATE INDEX IF NOT EXISTS idx_entity_contacts_primary
ON entity_contacts(entity_type, entity_id, is_primary) WHERE is_active = 1;

-- Index for email search
CREATE INDEX IF NOT EXISTS idx_entity_contacts_email
ON entity_contacts(email) WHERE email IS NOT NULL;

-- Ensure only one primary contact per entity
CREATE TRIGGER IF NOT EXISTS trg_entity_contacts_primary_check
BEFORE INSERT ON entity_contacts
WHEN NEW.is_primary = 1
BEGIN
    UPDATE entity_contacts
    SET is_primary = 0, updated_date = CURRENT_TIMESTAMP
    WHERE entity_type = NEW.entity_type
      AND entity_id = NEW.entity_id
      AND is_primary = 1
      AND is_active = 1;
END;

CREATE TRIGGER IF NOT EXISTS trg_entity_contacts_primary_update
BEFORE UPDATE ON entity_contacts
WHEN NEW.is_primary = 1 AND (OLD.is_primary = 0 OR OLD.is_primary IS NULL)
BEGIN
    UPDATE entity_contacts
    SET is_primary = 0, updated_date = CURRENT_TIMESTAMP
    WHERE entity_type = NEW.entity_type
      AND entity_id = NEW.entity_id
      AND contact_id != NEW.contact_id
      AND is_primary = 1
      AND is_active = 1;
END;

-- Log migration
INSERT INTO canon_audit (actor, action, key, value, extra)
VALUES ('migration', 'SCHEMA_CHANGE', 'migration:002', 'create_entity_contacts', 'Created entity_contacts table with triggers');
