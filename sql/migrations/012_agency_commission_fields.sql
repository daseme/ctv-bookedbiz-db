-- Migration 012: Add commission_rate and order_rate_basis to agencies
-- These fields track each agency's commission percentage and whether orders arrive as gross or net.
-- NULL means "never reviewed" (distinct from "confirmed at 0%" or "confirmed as gross").

ALTER TABLE agencies ADD COLUMN commission_rate DECIMAL(5,2) DEFAULT NULL
    CHECK (commission_rate IS NULL OR (commission_rate >= 0 AND commission_rate <= 100));

ALTER TABLE agencies ADD COLUMN order_rate_basis TEXT DEFAULT NULL
    CHECK (order_rate_basis IS NULL OR order_rate_basis IN ('gross', 'net'));

INSERT INTO canon_audit (actor, action, key, value, extra)
VALUES ('migration', 'SCHEMA_CHANGE', 'migration:012', 'agency_commission_fields',
        'Added commission_rate and order_rate_basis to agencies table (NULL = unreviewed)');
