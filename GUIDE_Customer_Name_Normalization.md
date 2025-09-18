# Customer Name Normalization System

## System Overview

The customer normalization system resolves `spots.bill_code` entries to canonical customer records through a multi-layer approach combining exact matches, fuzzy matching, and alias resolution.

**Architecture**: `spots` → `v_customer_normalization_audit` → `customers` / `entity_aliases`

**Primary Goal**: Aggregate revenue accurately to canonical customer entities while handling name variations and data quality issues.

## Core Components

| Component | Function |
|-----------|----------|
| `v_customer_normalization_audit` | Main normalization view joining spots data with customer resolution |
| `v_normalized_candidates` | Underlying view handling bill_code parsing and canonical mapping |
| `agency_canonical_map` | Maps agency name variants to canonical forms |
| `customer_canonical_map` | Maps customer name variants to canonical forms |
| `entity_aliases` | User-approved aliases linking variants to target entities |
| `customers` | Master customer records |

## Data Flow

1. **Bill Code Parsing**: Extract agency and customer components from `bill_code`
2. **Canonical Mapping**: Apply agency/customer canonical maps for normalization
3. **Alias Resolution**: Match through `entity_aliases` for approved variants
4. **Direct Matching**: Fall back to exact matches in `customers` table
5. **Revenue Aggregation**: Sum revenue by resolved canonical customer

## Key Views Structure

### v_customer_normalization_audit
Combines normalized candidates with customer data and revenue type information.

**Critical Dependencies:**
- `v_normalized_candidates` (must return unique records per raw_text)
- `entity_aliases` (customer type only)
- `customers` table

### v_normalized_candidates  
Handles bill_code parsing through complex CTE logic:
- Parses agency:customer format
- Applies canonical mappings
- Strips PROD/PRODUCTION suffixes
- Constructs normalized names

## Data Quality Requirements

### Canonical Mapping Tables
**Critical**: Mapping tables must not contain case-insensitive duplicates.

**Example Problem:**
```sql
-- This causes duplicate records in views:
INSERT INTO agency_canonical_map VALUES ('iGraphix', 'iGraphix');
INSERT INTO agency_canonical_map VALUES ('iGRAPHIX', 'iGraphix');
```

**Solution**: Maintain single canonical entry per logical entity.

### Detection Query
```sql
-- Find case-insensitive duplicates in canonical maps
SELECT 
    LOWER(alias_name) as lowercase_alias,
    GROUP_CONCAT(alias_name, ', ') as variants,
    COUNT(*) as variant_count
FROM agency_canonical_map
GROUP BY LOWER(alias_name)
HAVING COUNT(*) > 1;
```

## Revenue Query Pattern

### Standard Customer Revenue Query
```sql
SELECT
    COALESCE(audit.customer_id, 0) AS customer_id,
    COALESCE(audit.normalized_name, s.bill_code) AS customer,
    ROUND(SUM(COALESCE(s.gross_rate, 0)), 2) AS gross_revenue,
    ROUND(SUM(COALESCE(s.station_net, 0)), 2) AS net_revenue
FROM spots s
LEFT JOIN v_customer_normalization_audit audit ON audit.raw_text = s.bill_code
WHERE s.broadcast_month = ?
GROUP BY 
    COALESCE(audit.customer_id, 0),
    COALESCE(audit.normalized_name, s.bill_code)
ORDER BY gross_revenue DESC;
```

**Note**: Direct JOIN to `v_customer_normalization_audit` assumes clean underlying data. Use DISTINCT subquery only if view returns duplicates.

## Customer Matching Tools

### CLI Analysis Tool
```bash
python -m src.cli.customer_names --db-path data/database/production.db \
  --export-unmatched --suggest-aliases
```

### Review Queue Management
```bash
python scripts/load_review_queue.py --db data/database/production.db --auto-approve
```

### Review UI
```bash
export DB_PATH=data/database/production.db
export APP_PIN=1234
python -m src.web.review_ui.app --host 0.0.0.0 --port 5088
```

## Match Classification

| Status | Criteria | Action |
|--------|----------|---------|
| `exact` | Direct match to `customers.normalized_name` | Use existing customer |
| `alias` | Match through `entity_aliases` | Use target entity |
| `high_confidence` | Fuzzy score ≥ 0.92 & revenue ≥ $2k | Auto-approve candidate |
| `review` | Fuzzy score ≥ 0.80 but below high confidence | Queue for manual review |
| `unknown` | No viable match found | Requires manual resolution |

## Troubleshooting

### Revenue Reporting Discrepancies
**Symptoms**: Dashboard shows $0 or incorrect totals for known customers.

**Common Causes**:
1. **View Duplicates**: Duplicate records in normalization views causing double-counting
2. **Customer ID Mismatches**: Spots reference different customer_id than normalization system expects
3. **Missing Aliases**: New customer name variants not yet mapped

### Diagnostic Queries
```sql
-- Check for view duplicates
SELECT raw_text, COUNT(*) 
FROM v_customer_normalization_audit 
GROUP BY raw_text 
HAVING COUNT(*) > 1;

-- Verify customer ID alignment
SELECT 
    s.bill_code,
    s.customer_id as spots_customer_id,
    audit.customer_id as audit_customer_id,
    COUNT(*) as affected_spots
FROM spots s
LEFT JOIN v_customer_normalization_audit audit ON audit.raw_text = s.bill_code
WHERE s.customer_id != audit.customer_id
GROUP BY s.bill_code, s.customer_id, audit.customer_id;
```

### Performance Considerations
- Views use complex CTE logic; avoid nested subqueries where possible
- Canonical mapping tables should have appropriate indexes on alias_name
- Large fuzzy matching operations should use blocking keys for performance

## Agency Normalization (Future)

**Current State**: Agency names preserved as raw text from bill_code parsing.

**Planned Enhancement**: Similar normalization system for agency names with:
- `agencies` master table
- Agency-specific canonical mapping
- `entity_aliases` support for agency type
- Fuzzy matching for agency name variants

**Implementation Approach**:
1. Create `src/services/agency_matching/normalization.py`
2. Adapt blocking matcher for agency-specific patterns
3. Extend review UI to support agency entity types
4. Add agency resolution to main revenue queries

## Dependencies

**Required Python Packages**:
- `rapidfuzz` (fuzzy string matching)
- `metaphone` (phonetic matching)  
- `Unidecode` (character normalization)
- `flask` (review UI)

## System Maintenance

- Run customer analysis after major data imports
- Monitor canonical mapping tables for duplicate entries
- Review unmatched customers periodically through UI
- Document approved alias patterns for consistency