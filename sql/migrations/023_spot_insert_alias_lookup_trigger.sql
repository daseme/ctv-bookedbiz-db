-- Migration 023: Auto-set customer_id on spot insertion
--
-- Complements migration 022 (alias insert/update triggers). When a new
-- spot is inserted with customer_id NULL, look up an existing active
-- customer alias matching the bill_code and set customer_id automatically.

CREATE TRIGGER IF NOT EXISTS trg_set_customer_on_spot_insert
AFTER INSERT ON spots
WHEN NEW.customer_id IS NULL
BEGIN
    UPDATE spots
    SET customer_id = (
        SELECT target_entity_id
        FROM entity_aliases
        WHERE alias_name = NEW.bill_code
          AND entity_type = 'customer'
          AND is_active = 1
        LIMIT 1
    )
    WHERE spot_id = NEW.spot_id
      AND customer_id IS NULL
      AND EXISTS (
        SELECT 1
        FROM entity_aliases
        WHERE alias_name = NEW.bill_code
          AND entity_type = 'customer'
          AND is_active = 1
    );
END;
