-- Migration 022: Automatic spot backfill on alias creation
--
-- When a customer alias is created or reactivated, automatically set
-- spots.customer_id for matching bill_codes. This ensures all code
-- paths (canon_tools, migration scripts, etc.) trigger backfill
-- without application-level changes.

CREATE TRIGGER IF NOT EXISTS trg_backfill_spots_on_alias_insert
AFTER INSERT ON entity_aliases
WHEN NEW.entity_type = 'customer' AND NEW.is_active = 1
BEGIN
    UPDATE spots
    SET customer_id = NEW.target_entity_id
    WHERE bill_code = NEW.alias_name
      AND customer_id IS NULL;
END;

CREATE TRIGGER IF NOT EXISTS trg_backfill_spots_on_alias_update
AFTER UPDATE ON entity_aliases
WHEN NEW.entity_type = 'customer'
  AND NEW.is_active = 1
  AND (OLD.is_active = 0
       OR OLD.target_entity_id != NEW.target_entity_id)
BEGIN
    UPDATE spots
    SET customer_id = NEW.target_entity_id
    WHERE bill_code = NEW.alias_name
      AND customer_id IS NULL;
END;
