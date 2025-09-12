PRAGMA foreign_keys=ON;

-- Canonical customer map (extend later as needed)
CREATE TABLE IF NOT EXISTS customer_canonical_map (
  alias_name     TEXT PRIMARY KEY,
  canonical_name TEXT NOT NULL,
  updated_date   TEXT DEFAULT (datetime('now'))
);
INSERT INTO customer_canonical_map(alias_name, canonical_name) VALUES
  ('Mc''Donald''s','McDonald''s')
ON CONFLICT(alias_name) DO UPDATE
SET canonical_name=excluded.canonical_name, updated_date=datetime('now');

-- v_normalized_candidates
DROP VIEW IF EXISTS v_normalized_candidates;
CREATE VIEW v_normalized_candidates AS
WITH base AS (
  SELECT raw_text, cleaned, INSTR(cleaned, ':') AS pos1 FROM v_raw_clean
),
s1 AS (
  SELECT raw_text, cleaned, pos1,
         CASE WHEN pos1=0 THEN NULL ELSE SUBSTR(cleaned,1,pos1-1) END AS a1,
         CASE WHEN pos1=0 THEN cleaned ELSE SUBSTR(cleaned,pos1+1) END AS rest
  FROM base
),
s2 AS (
  SELECT raw_text, cleaned, a1, rest,
         CASE WHEN pos1=0 THEN 0 ELSE INSTR(rest, ':') END AS p2
  FROM s1
),
parts AS (
  SELECT raw_text, cleaned,
         a1 AS agency1_raw,
         CASE WHEN p2>0 THEN SUBSTR(rest,1,p2-1) ELSE NULL END AS agency2_raw,
         CASE WHEN a1 IS NULL THEN rest
              WHEN p2=0 THEN rest
              ELSE SUBSTR(rest,p2+1) END AS customer_raw
  FROM s2
),
canon AS (
  SELECT p.raw_text, p.cleaned,
         TRIM(COALESCE(a1.canonical_name, p.agency1_raw)) AS agency1,
         TRIM(COALESCE(a2.canonical_name, p.agency2_raw)) AS agency2,
         TRIM(p.customer_raw) AS customer_base
  FROM parts p
  LEFT JOIN agency_canonical_map a1 ON a1.alias_name=TRIM(p.agency1_raw)
  LEFT JOIN agency_canonical_map a2 ON a2.alias_name=TRIM(p.agency2_raw)
),
strip_prod AS (
  SELECT raw_text, cleaned, agency1, agency2,
         RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(customer_base||'|',
               ' PRODUCTION|','|'),' PROD|','|'),'- PRODUCTION|','|'),'- PROD|','|'),'|') AS customer
  FROM canon
),
cust_canon AS (
  SELECT s.raw_text, s.cleaned, s.agency1, s.agency2,
         TRIM(COALESCE(ccm.canonical_name, s.customer)) AS customer
  FROM strip_prod s
  LEFT JOIN customer_canonical_map ccm ON ccm.alias_name=s.customer
)
SELECT raw_text,
       cleaned AS cleaned_text,
       agency1, agency2, customer,
       CASE
         WHEN agency1 IS NULL OR agency1='' THEN customer
         WHEN agency2 IS NULL OR agency2='' THEN agency1||':'||customer
         ELSE agency1||':'||agency2||':'||customer
       END AS normalized_name
FROM cust_canon;

-- v_customer_normalization_audit (reuse the normalizer)
DROP VIEW IF EXISTS v_customer_normalization_audit;
CREATE VIEW v_customer_normalization_audit AS
WITH src_spots AS (
  SELECT DISTINCT bill_code AS raw_text, revenue_type
  FROM spots
  WHERE bill_code IS NOT NULL AND bill_code<>''
    AND revenue_type IN ('Internal Ad Sales','Branded Content')
),
src_manual AS ( SELECT r.raw_text, NULL AS revenue_type FROM raw_customer_inputs r ),
all_inputs AS ( SELECT raw_text FROM src_manual UNION SELECT raw_text FROM src_spots ),
norm AS ( SELECT n.* FROM v_normalized_candidates n JOIN all_inputs a USING(raw_text) ),
roll_revenue AS (
  SELECT raw_text, GROUP_CONCAT(DISTINCT revenue_type) AS revenue_types_seen_raw
  FROM src_spots GROUP BY raw_text
),
cust AS ( SELECT customer_id, normalized_name, created_date FROM customers ),
alias AS (
  SELECT alias_name, target_entity_id FROM entity_aliases
  WHERE entity_type='customer' AND is_active=1
)
SELECT n.raw_text, n.cleaned_text, n.agency1, n.agency2, n.customer, n.normalized_name,
       REPLACE(rr.revenue_types_seen_raw, ',', ', ') AS revenue_types_seen,
       CASE WHEN c.customer_id IS NOT NULL THEN 1 ELSE 0 END AS exists_in_customers,
       a.target_entity_id IS NOT NULL AS has_alias,
       CASE WHEN a.target_entity_id IS NOT NULL AND c.customer_id IS NOT NULL
                 AND a.target_entity_id<>c.customer_id THEN 1 ELSE 0 END AS alias_conflict,
       c.customer_id, c.created_date AS customer_created_date
FROM norm n
LEFT JOIN roll_revenue rr ON rr.raw_text=n.raw_text
LEFT JOIN cust c ON c.normalized_name=n.normalized_name
LEFT JOIN alias a ON a.alias_name=n.raw_text;

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_spots_billcode_rev ON spots(bill_code, revenue_type);
CREATE INDEX IF NOT EXISTS idx_alias_customer_active ON entity_aliases(alias_name, entity_type, is_active);
CREATE INDEX IF NOT EXISTS idx_customers_normalized_name ON customers(normalized_name);
