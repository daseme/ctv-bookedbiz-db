-- Migration 019: Link Solsken customers to Solsken agency
-- Solsken (agency_id=49) advertises for itself. Three customers had spots
-- with no agency linked: Solsken (205), Solsken:Direct (234),
-- Solsken Communications:My Sister's House (449). Total: 492 spots.

UPDATE spots
   SET agency_id = 49
 WHERE customer_id IN (205, 234, 449)
   AND agency_id IS NULL;

-- Audit trail
INSERT INTO canon_audit (actor, action, key, value, extra)
VALUES ('claude', 'link_customer_to_agency',
        'customers:205,234,449→agency:49',
        'Solsken customers → Solsken agency',
        '{"customers":[{"id":205,"name":"Solsken","spots":6},{"id":234,"name":"Solsken:Direct","spots":72},{"id":449,"name":"Solsken Communications:My Sister''s House","spots":414}],"agency_id":49,"agency_name":"Solsken","total_spots":492,"reason":"Agency advertises for itself"}');
