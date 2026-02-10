-- Migration 008: Add affidavit_required to customers table
-- Tracks whether a customer/advertiser requires an affidavit
ALTER TABLE customers ADD COLUMN affidavit_required BOOLEAN DEFAULT 0;
