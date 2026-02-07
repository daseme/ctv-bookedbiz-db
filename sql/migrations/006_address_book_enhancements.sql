-- Migration 006: Address Book Enhancements
-- Purpose: Add PO number + EDI billing fields, create entity_addresses table
-- Run this migration with: sqlite3 .data/dev.db < sql/migrations/006_address_book_enhancements.sql

-- Add billing fields to agencies
ALTER TABLE agencies ADD COLUMN po_number TEXT;
ALTER TABLE agencies ADD COLUMN edi_billing INTEGER DEFAULT 0;

-- Add billing fields to customers
ALTER TABLE customers ADD COLUMN po_number TEXT;
ALTER TABLE customers ADD COLUMN edi_billing INTEGER DEFAULT 0;

-- Create entity_addresses table (mirrors entity_contacts pattern)
CREATE TABLE IF NOT EXISTS entity_addresses (
    address_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL CHECK (entity_type IN ('customer', 'agency')),
    entity_id INTEGER NOT NULL,
    address_label TEXT NOT NULL CHECK (address_label IN ('Billing', 'Shipping', 'PO Box', 'Office', 'Other')),
    address TEXT,
    city TEXT,
    state TEXT,
    zip TEXT,
    is_primary BOOLEAN DEFAULT 0,
    is_active BOOLEAN DEFAULT 1,
    created_by TEXT NOT NULL,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notes TEXT
);

-- Index for fast lookup by entity
CREATE INDEX IF NOT EXISTS idx_entity_addresses_lookup
ON entity_addresses(entity_type, entity_id, is_active);

-- Ensure only one primary address per entity (auto-demote previous)
CREATE TRIGGER IF NOT EXISTS trg_entity_addresses_primary_check
BEFORE INSERT ON entity_addresses
WHEN NEW.is_primary = 1
BEGIN
    UPDATE entity_addresses
    SET is_primary = 0, updated_date = CURRENT_TIMESTAMP
    WHERE entity_type = NEW.entity_type
      AND entity_id = NEW.entity_id
      AND is_primary = 1
      AND is_active = 1;
END;

CREATE TRIGGER IF NOT EXISTS trg_entity_addresses_primary_update
BEFORE UPDATE ON entity_addresses
WHEN NEW.is_primary = 1 AND (OLD.is_primary = 0 OR OLD.is_primary IS NULL)
BEGIN
    UPDATE entity_addresses
    SET is_primary = 0, updated_date = CURRENT_TIMESTAMP
    WHERE entity_type = NEW.entity_type
      AND entity_id = NEW.entity_id
      AND address_id != NEW.address_id
      AND is_primary = 1
      AND is_active = 1;
END;

-- Log migration
INSERT INTO canon_audit (actor, action, key, value, extra)
VALUES ('migration', 'SCHEMA_CHANGE', 'migration:006', 'address_book_enhancements', 'Added po_number, edi_billing to agencies/customers; created entity_addresses table');
