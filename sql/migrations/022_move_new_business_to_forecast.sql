-- Migration 022: Move new business fields from sector_expectations to forecast
--
-- Context:
-- - new_accounts_forecast / new_dollars_forecast started life on sector_expectations
--   (per AE / sector / month).
-- - We now want these as per-AE / per-month fields that live alongside forecast,
--   not per sector.
--
-- This migration:
-- 1) Adds new_accounts_forecast and new_dollars_forecast to the forecast table.
-- 2) Removes those columns from sector_expectations.
--
-- NOTE: This migration does NOT attempt to preserve or backfill data; any values
-- currently stored on sector_expectations.*_forecast will be dropped. Run only
-- after you're comfortable discarding those prototype values.

-- 1) Add columns to forecast (per AE / year / month)
ALTER TABLE forecast
    ADD COLUMN new_accounts_forecast INTEGER DEFAULT 0;

ALTER TABLE forecast
    ADD COLUMN new_dollars_forecast DECIMAL(12, 2) DEFAULT 0;


-- 2) Drop columns from sector_expectations
-- SQLite supports DROP COLUMN in recent versions; this will remove the
-- prototype fields entirely from the sector-level table.
ALTER TABLE sector_expectations
    DROP COLUMN new_accounts_forecast;

ALTER TABLE sector_expectations
    DROP COLUMN new_dollars_forecast;

