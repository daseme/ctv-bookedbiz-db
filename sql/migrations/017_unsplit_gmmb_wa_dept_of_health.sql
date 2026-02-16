-- Migration 017: Unsplit "GMMB:WA Dept of Health" → "WA Dept of Health" + assign GMMB agency
-- Customer 309 is a colon-prefixed customer that should be "WA Dept of Health" under GMMB agency.
-- Customer 230 ("WA Dept of Health") is an empty inactive duplicate blocking the rename.

-- Step 1: Retire the empty duplicate to free the name (UNIQUE constraint)
UPDATE customers
   SET normalized_name = 'WA Dept of Health #retired-230',
       notes = COALESCE(notes || '; ', '') || 'Renamed to free name for unsplit of customer 309'
 WHERE customer_id = 230;

-- Step 2: Rename the real customer (strip agency prefix)
UPDATE customers
   SET normalized_name = 'WA Dept of Health',
       agency_id = 50,
       is_active = 1,
       notes = COALESCE(notes || '; ', '') || 'Unsplit from GMMB:WA Dept of Health; assigned agency GMMB (id=50)'
 WHERE customer_id = 309;

-- Step 3: Ensure all spots have agency_id set (safety — they already do)
UPDATE spots
   SET agency_id = 50
 WHERE customer_id = 309
   AND agency_id IS NULL;

-- Step 4: Audit
INSERT INTO canon_audit (actor, action, key, value, extra)
VALUES ('claude', 'unsplit_customer',
        'customer:309',
        'GMMB:WA Dept of Health → WA Dept of Health',
        '{"customer_id":309,"old_name":"GMMB:WA Dept of Health","new_name":"WA Dept of Health","agency_id":50,"agency_name":"GMMB","retired_duplicate":{"customer_id":230,"old_name":"WA Dept of Health","new_name":"WA Dept of Health #retired-230"}}');
