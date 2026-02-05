-- Add new accounts and new dollars forecast columns to sector_expectations.
-- These are annual values per sector; we store them on every row (all 12 months)
-- so that any row for that sector carries the value (no need to pick "month 1").
-- Run once against existing databases.

ALTER TABLE sector_expectations ADD COLUMN new_accounts_forecast INTEGER DEFAULT 0;
ALTER TABLE sector_expectations ADD COLUMN new_dollars_forecast DECIMAL(12, 2) DEFAULT 0;
