-- Migration 016: Merge "GMMB SERVICES" customer into "GMMB" agency
-- GMMB SERVICES (customer_id=231) is actually the GMMB agency (agency_id=50).
-- 1 spot ($1,185, May-22) with NULL agency_id needs linking.

-- Link the spot to GMMB agency
UPDATE spots
   SET agency_id = 50
 WHERE customer_id = 231
   AND agency_id IS NULL;

-- Create agency alias so future imports resolve "GMMB SERVICES" to GMMB agency
INSERT INTO entity_aliases (alias_name, entity_type, target_entity_id, is_active, created_by, notes)
VALUES ('GMMB SERVICES', 'agency', 50, 1, 'claude',
        'Merged from customer 231 — GMMB SERVICES was actually the GMMB agency');

-- Ensure customer record is deactivated (already inactive, but be explicit)
UPDATE customers
   SET is_active = 0,
       notes = COALESCE(notes || '; ', '') || 'Merged into agency GMMB (id=50) — was misidentified as advertiser'
 WHERE customer_id = 231;

-- Audit trail
INSERT INTO canon_audit (actor, action, key, value, extra)
VALUES ('claude', 'merge_customer_to_agency',
        'customer:231→agency:50',
        'GMMB SERVICES → GMMB',
        '{"source_type":"customer","source_id":231,"source_name":"GMMB SERVICES","target_type":"agency","target_id":50,"target_name":"GMMB","spots_moved":1,"reason":"GMMB SERVICES is actually the GMMB agency"}');
