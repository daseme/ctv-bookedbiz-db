-- Migration 015: Add commission_rate and order_rate_basis to customers
-- Per-customer commission overrides: NULL = "inherit from agency", set value = override.
-- CHECK constraints mirror migration 012 on agencies table.

ALTER TABLE customers ADD COLUMN commission_rate DECIMAL(5,2) DEFAULT NULL
    CHECK (commission_rate IS NULL OR (commission_rate >= 0 AND commission_rate <= 100));

ALTER TABLE customers ADD COLUMN order_rate_basis TEXT DEFAULT NULL
    CHECK (order_rate_basis IS NULL OR order_rate_basis IN ('gross', 'net'));

INSERT INTO canon_audit (actor, action, key, value, extra)
VALUES ('migration', 'SCHEMA_CHANGE', 'migration:015', 'customer_commission_fields',
        'Added commission_rate and order_rate_basis to customers table (NULL = inherit from agency)');
