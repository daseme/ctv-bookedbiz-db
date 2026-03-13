-- 026_revenue_classification.sql
-- Add revenue classification (regular/irregular) to customers

ALTER TABLE customers ADD COLUMN revenue_class TEXT DEFAULT 'regular'
    CHECK (revenue_class IN ('regular', 'irregular'));

-- Seed political customers as irregular
UPDATE customers SET revenue_class = 'irregular'
WHERE sector_id IN (
    SELECT sector_id FROM sectors
    WHERE sector_code IN ('POLITICAL', 'POLITICAL-OUTREACH', 'POLITICALOUTREACH')
);
