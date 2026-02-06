-- Migration 005: Alias Integrity
-- 1. Triggers to prevent inserting aliases pointing to nonexistent/inactive entities
-- 2. Deactivate stale aliases pointing to inactive customers
-- 3. Deactivate case-duplicate aliases

-- ============================================================
-- TRIGGER: Validate customer alias targets on INSERT
-- ============================================================
CREATE TRIGGER IF NOT EXISTS trg_alias_customer_integrity
BEFORE INSERT ON entity_aliases
WHEN NEW.entity_type = 'customer'
BEGIN
    SELECT RAISE(ABORT, 'Target customer does not exist or is inactive')
    WHERE NOT EXISTS (
        SELECT 1 FROM customers
        WHERE customer_id = NEW.target_entity_id
          AND is_active = 1
    );
END;

-- ============================================================
-- TRIGGER: Validate agency alias targets on INSERT
-- ============================================================
CREATE TRIGGER IF NOT EXISTS trg_alias_agency_integrity
BEFORE INSERT ON entity_aliases
WHEN NEW.entity_type = 'agency'
BEGIN
    SELECT RAISE(ABORT, 'Target agency does not exist or is inactive')
    WHERE NOT EXISTS (
        SELECT 1 FROM agencies
        WHERE agency_id = NEW.target_entity_id
          AND is_active = 1
    );
END;

-- ============================================================
-- CLEANUP: Deactivate active aliases pointing to inactive customers
-- (American Heart Association, Ad Council — 2 aliases identified in audit)
-- ============================================================
UPDATE entity_aliases
SET is_active = 0,
    updated_date = CURRENT_TIMESTAMP,
    notes = COALESCE(notes, '') || ' | Deactivated by migration 005: target customer inactive'
WHERE entity_type = 'customer'
  AND is_active = 1
  AND target_entity_id NOT IN (
      SELECT customer_id FROM customers WHERE is_active = 1
  );

-- ============================================================
-- CLEANUP: Deactivate case-duplicate aliases (keep lower alias_id)
-- Pairs: (203,205) (204,206) (217,218) — deactivate 205, 206, 218
-- ============================================================
UPDATE entity_aliases
SET is_active = 0,
    updated_date = CURRENT_TIMESTAMP,
    notes = COALESCE(notes, '') || ' | Deactivated by migration 005: case-duplicate alias'
WHERE alias_id IN (205, 206, 218)
  AND is_active = 1;
