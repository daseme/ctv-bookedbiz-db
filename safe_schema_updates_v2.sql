-- ============================================
-- SAFE DATABASE SCHEMA UPDATES V2
-- Handles the CHECK constraint issue for assignment_method
-- ============================================

PRAGMA foreign_keys = ON;

-- 1. Add business_rule_applied column (SAFE - no constraints)
ALTER TABLE spot_language_blocks 
ADD COLUMN business_rule_applied TEXT DEFAULT NULL;

-- 2. Add auto_resolved_date column (SAFE - no constraints)
ALTER TABLE spot_language_blocks 
ADD COLUMN auto_resolved_date TIMESTAMP DEFAULT NULL;

-- 3. Create indexes for new columns (SAFE - performance improvement)
CREATE INDEX IF NOT EXISTS idx_spot_blocks_business_rule 
ON spot_language_blocks(business_rule_applied) 
WHERE business_rule_applied IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_spot_blocks_auto_resolved 
ON spot_language_blocks(auto_resolved_date) 
WHERE auto_resolved_date IS NOT NULL;

-- 4. Create business rule analytics view (SAFE - read-only)
CREATE VIEW IF NOT EXISTS business_rule_analytics AS
SELECT 
    COALESCE(business_rule_applied, 'no_rule') as rule_applied,
    COUNT(*) as spots_affected,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM spot_language_blocks) as percentage,
    AVG(intent_confidence) as avg_confidence,
    COUNT(CASE WHEN requires_attention = 1 THEN 1 END) as flagged_count,
    COUNT(CASE WHEN requires_attention = 0 THEN 1 END) as auto_resolved_count,
    MIN(assigned_date) as earliest_assignment,
    MAX(assigned_date) as latest_assignment
FROM spot_language_blocks 
GROUP BY COALESCE(business_rule_applied, 'no_rule')
ORDER BY spots_affected DESC;

-- 5. Create business rule summary view (SAFE - read-only)
CREATE VIEW IF NOT EXISTS business_rule_summary AS
SELECT 
    'Total spots' as metric,
    COUNT(*) as value,
    '' as notes
FROM spot_language_blocks
UNION ALL
SELECT 
    'Business rule applied' as metric,
    COUNT(*) as value,
    'Auto-resolved by business rules' as notes
FROM spot_language_blocks
WHERE business_rule_applied IS NOT NULL
UNION ALL
SELECT 
    'Manual assignments' as metric,
    COUNT(*) as value,
    'Standard assignment process' as notes
FROM spot_language_blocks
WHERE business_rule_applied IS NULL;

-- Verify the changes
SELECT 'Schema update V2 completed successfully - CHECK constraint preserved' as status;
