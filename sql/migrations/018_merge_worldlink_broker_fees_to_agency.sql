-- Migration 018: Merge "WorldLink Broker Fees (DO NOT INVOICE)" customer into "WorldLink" agency
-- WorldLink Broker Fees (customer_id=5) represents negative broker fee adjustments
-- that belong to the WorldLink agency (agency_id=5).
-- 567 spots (all negative, -$83,942 total) with NULL agency_id need linking.

-- Link all spots to WorldLink agency
UPDATE spots
   SET agency_id = 5
 WHERE customer_id = 5
   AND agency_id IS NULL;

-- Create agency alias so future imports resolve this name to WorldLink agency
INSERT INTO entity_aliases (alias_name, entity_type, target_entity_id, is_active, created_by, notes)
VALUES ('WorldLink Broker Fees (DO NOT INVOICE)', 'agency', 5, 1, 'claude',
        'Merged from customer 5 — broker fee adjustments belong to WorldLink agency');

-- Deactivate the customer record
UPDATE customers
   SET is_active = 0,
       notes = COALESCE(notes || '; ', '') || 'Merged into agency WorldLink (id=5) — broker fee adjustments, not a real advertiser'
 WHERE customer_id = 5;

-- Audit trail
INSERT INTO canon_audit (actor, action, key, value, extra)
VALUES ('claude', 'merge_customer_to_agency',
        'customer:5→agency:5',
        'WorldLink Broker Fees (DO NOT INVOICE) → WorldLink',
        '{"source_type":"customer","source_id":5,"source_name":"WorldLink Broker Fees (DO NOT INVOICE)","target_type":"agency","target_id":5,"target_name":"WorldLink","spots_moved":567,"reason":"Broker fee adjustments belong to WorldLink agency, not a separate advertiser"}');
