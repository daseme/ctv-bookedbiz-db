-- Migration 010: Multi-Sector Support for Customers
-- Creates customer_sectors junction table (many-to-many) while keeping
-- customers.sector_id as a denormalized cache of the primary sector.
-- Follows the ae_assignments pattern from migration 007.

-- ============================================================
-- 1. Create customer_sectors junction table
-- ============================================================
CREATE TABLE IF NOT EXISTS customer_sectors (
    customer_sector_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id         INTEGER NOT NULL,
    sector_id           INTEGER NOT NULL,
    is_primary          INTEGER NOT NULL DEFAULT 0,
    assigned_date       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    assigned_by         TEXT NOT NULL DEFAULT 'system',
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE CASCADE,
    FOREIGN KEY (sector_id)   REFERENCES sectors(sector_id) ON DELETE RESTRICT,
    UNIQUE(customer_id, sector_id)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_customer_sectors_customer
    ON customer_sectors(customer_id, is_primary DESC);
CREATE INDEX IF NOT EXISTS idx_customer_sectors_sector
    ON customer_sectors(sector_id);
CREATE INDEX IF NOT EXISTS idx_customer_sectors_primary
    ON customer_sectors(customer_id) WHERE is_primary = 1;

-- ============================================================
-- 2. Backfill from existing customers.sector_id
-- ============================================================
INSERT INTO customer_sectors (customer_id, sector_id, is_primary, assigned_by)
SELECT customer_id, sector_id, 1, 'migration_010'
FROM customers
WHERE sector_id IS NOT NULL AND is_active = 1;

-- ============================================================
-- 3. Triggers to keep customers.sector_id in sync
-- ============================================================

-- Trigger 1: INSERT with is_primary=1 → clear old primary, update cache
CREATE TRIGGER IF NOT EXISTS trg_customer_sectors_insert_primary
AFTER INSERT ON customer_sectors
WHEN NEW.is_primary = 1
BEGIN
    -- Clear any other primary for this customer
    UPDATE customer_sectors
    SET is_primary = 0
    WHERE customer_id = NEW.customer_id
      AND customer_sector_id != NEW.customer_sector_id
      AND is_primary = 1;
    -- Sync denormalized cache
    UPDATE customers
    SET sector_id = NEW.sector_id
    WHERE customer_id = NEW.customer_id;
END;

-- Trigger 2: UPDATE is_primary to 1 → clear old primary, update cache
CREATE TRIGGER IF NOT EXISTS trg_customer_sectors_update_primary
AFTER UPDATE OF is_primary ON customer_sectors
WHEN NEW.is_primary = 1 AND OLD.is_primary = 0
BEGIN
    -- Clear any other primary for this customer
    UPDATE customer_sectors
    SET is_primary = 0
    WHERE customer_id = NEW.customer_id
      AND customer_sector_id != NEW.customer_sector_id
      AND is_primary = 1;
    -- Sync denormalized cache
    UPDATE customers
    SET sector_id = NEW.sector_id
    WHERE customer_id = NEW.customer_id;
END;

-- Trigger 3: DELETE of primary row → promote oldest remaining or NULL
CREATE TRIGGER IF NOT EXISTS trg_customer_sectors_delete_primary
AFTER DELETE ON customer_sectors
WHEN OLD.is_primary = 1
BEGIN
    -- Promote the oldest remaining sector to primary
    UPDATE customer_sectors
    SET is_primary = 1
    WHERE customer_sector_id = (
        SELECT customer_sector_id FROM customer_sectors
        WHERE customer_id = OLD.customer_id
        ORDER BY assigned_date ASC
        LIMIT 1
    );
    -- If no sectors remain, clear the cache to NULL
    UPDATE customers
    SET sector_id = NULL
    WHERE customer_id = OLD.customer_id
      AND NOT EXISTS (
          SELECT 1 FROM customer_sectors WHERE customer_id = OLD.customer_id
      );
END;

-- ============================================================
-- 4. Audit log
-- ============================================================
INSERT INTO canon_audit (actor, action, key, value, extra)
VALUES ('migration', 'SCHEMA_CHANGE', 'migration:010', 'customer_sectors_table',
        'Created customer_sectors junction table, backfilled from customers.sector_id, added sync triggers');
