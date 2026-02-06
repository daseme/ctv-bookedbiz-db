-- Migration 001: Add address columns to agencies and customers tables
-- Purpose: Support contact/address information for entities
-- Run this migration with: sqlite3 production.db < sql/migrations/001_add_entity_addresses.sql

-- Add address columns to agencies table
ALTER TABLE agencies ADD COLUMN address TEXT;
ALTER TABLE agencies ADD COLUMN city TEXT;
ALTER TABLE agencies ADD COLUMN state TEXT;
ALTER TABLE agencies ADD COLUMN zip TEXT;

-- Add address columns to customers table
ALTER TABLE customers ADD COLUMN address TEXT;
ALTER TABLE customers ADD COLUMN city TEXT;
ALTER TABLE customers ADD COLUMN state TEXT;
ALTER TABLE customers ADD COLUMN zip TEXT;

-- Create indexes for common lookups
CREATE INDEX IF NOT EXISTS idx_agencies_city ON agencies(city);
CREATE INDEX IF NOT EXISTS idx_agencies_state ON agencies(state);
CREATE INDEX IF NOT EXISTS idx_customers_city ON customers(city);
CREATE INDEX IF NOT EXISTS idx_customers_state ON customers(state);

-- Log migration
INSERT INTO canon_audit (actor, action, key, value, extra)
VALUES ('migration', 'SCHEMA_CHANGE', 'migration:001', 'add_entity_addresses', 'Added address columns to agencies and customers tables');
