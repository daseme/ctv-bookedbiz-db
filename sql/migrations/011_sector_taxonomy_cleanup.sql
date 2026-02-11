-- Migration 011: Sector Taxonomy Cleanup
-- Establishes clean sector_group hierarchy, renames inconsistent sectors,
-- deactivates duplicates, and reassigns misplaced customers.

-- ============================================================
-- 1. Update sector_group for all active sectors
-- ============================================================

-- Commercial group
UPDATE sectors SET sector_group = 'Commercial' WHERE sector_id IN (1, 2, 5, 8, 9, 10, 15, 17, 20);
-- sector 1=Automotive, 2=CPG, 5=Tech, 8=Retail, 9=Telco, 10=Media, 15=Casino, 17=Restaurant, 20=Travel

-- Financial group
UPDATE sectors SET sector_group = 'Financial' WHERE sector_id IN (3, 6);
-- sector 3=Insurance, 6=Financial Services

-- Healthcare group
UPDATE sectors SET sector_group = 'Healthcare' WHERE sector_id = 7;

-- Outreach group
UPDATE sectors SET sector_group = 'Outreach' WHERE sector_id IN (4, 11, 12, 13, 18);
-- sector 4=General Outreach, 11=Government, 12=Education, 13=Non-Profit, 18=Political Outreach

-- Political group
UPDATE sectors SET sector_group = 'Political' WHERE sector_id = 16;

-- Other group
UPDATE sectors SET sector_group = 'Other' WHERE sector_id IN (14, 21);
-- sector 14=Other, 21=Utility

-- ============================================================
-- 2. Rename inconsistent sectors
-- ============================================================

-- Rename "Outreach" → "General Outreach" (sector 4, still active, ~$942K budget)
UPDATE sectors SET sector_name = 'General Outreach' WHERE sector_id = 4;

-- Rename "Political-Outreach" → "Political Outreach" (sector 18, remove hyphen)
UPDATE sectors SET sector_name = 'Political Outreach' WHERE sector_id = 18;

-- ============================================================
-- 3. Deactivate duplicate sector 19 (POLITICALOUTREACH)
-- ============================================================
-- 0 customers, 0 budget expectations — safe to deactivate
UPDATE sectors SET is_active = 0 WHERE sector_id = 19;

-- ============================================================
-- 4. Reassign misplaced customers
-- ============================================================
-- Triggers on customer_sectors only fire on is_primary column changes,
-- so we must DELETE + INSERT (not UPDATE sector_id) to sync the cache.

-- Customer 331 (Imprenta:PG&E): Outreach(4) → Utility(21)
DELETE FROM customer_sectors WHERE customer_id = 331 AND sector_id = 4 AND is_primary = 1;
INSERT INTO customer_sectors (customer_id, sector_id, is_primary, assigned_by)
VALUES (331, 21, 1, 'migration_011');

-- Customer 333 (Innocean:UC Davis): Outreach(4) → Education(12)
DELETE FROM customer_sectors WHERE customer_id = 333 AND sector_id = 4 AND is_primary = 1;
INSERT INTO customer_sectors (customer_id, sector_id, is_primary, assigned_by)
VALUES (333, 12, 1, 'migration_011');

-- ============================================================
-- 5. Audit log
-- ============================================================
INSERT INTO canon_audit (actor, action, key, value, extra)
VALUES ('migration', 'SCHEMA_CHANGE', 'migration:011', 'sector_taxonomy_cleanup',
        'Updated sector_groups, renamed sectors 4+18, deactivated sector 19, reassigned customers 331+333');
