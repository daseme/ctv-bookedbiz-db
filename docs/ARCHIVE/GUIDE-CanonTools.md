# Canon Tool System Documentation

## Overview
The Canon Tool system provides a unified web-based interface and backend API for normalizing and canonicalizing customer and agency data. It supports three main workflows:

1. **Agency Canonicalization**
   - Map agency aliases to canonical names.
   - Optionally create/update an `entity_alias` for the agency.

2. **Customer-tail Canonicalization**
   - Map customer "tail" (last segment of raw string) aliases to canonical customer-tail names.

3. **Raw → Normalized Customer Mapping**
   - Directly map an exact raw string to an existing `customers.normalized_name`.
   - Useful for one-off corrections.

---

## Data Sync Requirements

### Critical: Keeping Raw Inputs Current
The Canon Tool operates on `raw_customer_inputs`, but live dashboard data comes from `spots.bill_code`. Regular sync is required to ensure all customer names are available for normalization.

**Sync Command:**
```sql
INSERT OR IGNORE INTO raw_customer_inputs (raw_text)
SELECT DISTINCT bill_code 
FROM spots 
WHERE bill_code IS NOT NULL 
  AND bill_code != '' 
  AND bill_code NOT IN (SELECT raw_text FROM raw_customer_inputs);
```

**Frequency**: Run after major data imports or monthly to catch new customer names.

**Verification:**
```sql
-- Check sync status
SELECT 
    (SELECT COUNT(DISTINCT bill_code) FROM spots WHERE bill_code IS NOT NULL AND bill_code != '') as spots_customers,
    (SELECT COUNT(*) FROM raw_customer_inputs) as raw_inputs,
    (SELECT COUNT(DISTINCT bill_code) FROM spots WHERE bill_code IS NOT NULL AND bill_code != '') - 
    (SELECT COUNT(*) FROM raw_customer_inputs) as missing_count;
```

---

## Database Architecture

### Canonical Maps
- `agency_canonical_map(alias_name, canonical_name, updated_date)`
- `customer_canonical_map(alias_name, canonical_name, updated_date)`

### Aliases
- `entity_aliases(alias_name, entity_type, target_entity_id, confidence_score, created_by, notes, is_active, updated_date)`

### Normalization View Chain
The system processes data through a chain of views:

1. **`v_raw_clean`** → Processes `raw_customer_inputs` with basic text cleaning
2. **`v_normalized_candidates`** → Applies canonicalization rules and auto-normalization
3. **`v_customer_normalization_audit`** → Final mapping used by dashboards

### Auto-Normalization Features
Built into the view chain:

- **PRODUCTION/PROD Removal**: Automatically strips " PRODUCTION" and " PROD" suffixes (case-sensitive)
- **Agency:Customer Splitting**: Handles multiple colon-separated segments (Agency1:Agency2:Customer)
- **Canonical Mapping Integration**: Uses both agency and customer canonical maps automatically
- **Text Cleaning**: Removes special characters, normalizes spacing, handles business entity patterns

### Audit Trail
- `canon_audit(ts, actor, action, key, value, extra)`

### Indexes
- `idx_entity_aliases_customer` on `(alias_name, entity_type)`
- `idx_customers_normalized_name` on `(normalized_name)`

---

## Backend (Flask + SQLite)

### File
`src/web/routes/canon_tools.py`

### Blueprints
All endpoints are under `/api/canon`.

### Endpoints

#### `POST /api/canon/agency`
**Request Body**
```json
{ "alias": "iGRAPHIX", "canonical": "iGraphix", "create_entity_alias": true }
```

**Behavior**
- Upserts into `agency_canonical_map`.
- Optionally inserts/updates `entity_aliases`.
- Returns JSON with affected preview count.

#### `POST /api/canon/customer`
**Request Body**
```json
{ "alias": "Mc'Donald's", "canonical": "McDonald's" }
```

**Behavior**
- Upserts into `customer_canonical_map`.
- Updates preview hit count for raw tail matches.

#### `POST /api/canon/raw-to-customer`
**Request Body**
```json
{ "raw": "Admerasia Inc.:McDonald's", "normalized_name": "Admerasia:McDonald's" }
```

**Behavior**
- Validates that `normalized_name` exists in `customers`.
- Inserts/updates an entry in `entity_aliases` with `entity_type='customer'`.
- Prevents conflicting remaps (same raw → different customer).
- Returns JSON with mapped customer_id.

#### `GET /api/canon/suggest/normalized?q=...`
- Suggests up to 20 normalized customer names for autocomplete in the UI.

---

## Frontend (Template + JS)

### Interface Location
Template: `customer_normalization_manager.html`

### Buttons
- **Canonize Agency** → opens agency modal.
- **Canonize Customer** → opens customer-tail modal.
- **Map Raw → Normalized** → opens raw mapping modal.

### Modals
- **Agency Canon Modal**: alias + canonical + checkbox (entity alias).
- **Customer-tail Canon Modal**: alias (tail) + canonical (tail).
- **Raw → Normalized Modal**: raw string + target normalized_name with datalist suggestions.

### JavaScript Features
- Handles modal open/close actions.
- Posts form data to API endpoints.
- Displays status/error messages.
- Supports **Enter-to-submit**.
- Provides **autocomplete** for normalized names (via `/api/canon/suggest/normalized`).
- Implements **optimistic updates** in the table (updates DOM without full reload).

---

## Dashboard Integration

### Live Impact
Canon Tool changes immediately affect:
- Customer revenue dashboards via `v_customer_normalization_audit`
- Customer deduplication (multiple raw names → single normalized name)
- New customer detection (based on normalized names)
- Revenue aggregation (combines multiple customer variations)

### Dashboard Query Integration
Revenue dashboards use:
```sql
LEFT JOIN v_customer_normalization_audit audit ON audit.raw_text = s.bill_code
```

This provides:
- `audit.normalized_name` for display
- Automatic PRODUCTION/PROD removal
- Agency:customer splitting
- Canonical name mapping

### Verification After Changes
1. Check dashboard for combined customer entries
2. Verify revenue totals are properly aggregated  
3. Confirm new customer flags are accurate
4. Ensure customer count reflects deduplication

---

## Operational Workflows

### Complete Canonicalization Workflow
1. **Sync raw data**: Ensure `raw_customer_inputs` includes all `spots.bill_code` values
2. **Identify duplicates**: Review dashboard for duplicate customer entries
3. **Apply canon rules**: Use Canon Tool to map variations to canonical names
4. **Verify dashboard**: Confirm customers now appear as single entries with combined revenue
5. **Monitor**: Set up regular sync processes for ongoing data imports

### Agency Canonicalization Example
1. User identifies duplicate agencies: "iGRAPHIX" vs "iGraphix"
2. Clicks "Canonize Agency"
3. Enters alias `iGRAPHIX`, canonical `iGraphix`
4. Enables "create entity alias" checkbox
5. Submits → API maps and creates entity_alias
6. Dashboard immediately shows unified "iGraphix" entries

### Customer-tail Canonicalization Example
1. User sees variations: "Mc'Donald's", "McDonalds", "McDonald's"
2. Clicks "Canonize Customer" 
3. Maps each variation to canonical "McDonald's"
4. Dashboard combines all variations under "McDonald's"

### Raw → Normalized Mapping Example
1. User sees raw string `Admerasia Inc.:McDonald's` showing as separate from `Admerasia:McDonald's`
2. Clicks "Map Raw → Normalized"
3. Enters raw `Admerasia Inc.:McDonald's`, target `Admerasia:McDonald's`
4. Uses autocomplete to select valid normalized name
5. Submits → creates entity_alias mapping
6. Dashboard immediately combines the entries

---

## Troubleshooting

### Dashboard Shows Raw Names Instead of Normalized
**Symptoms**: Customers appear with PRODUCTION suffixes, exact raw variations
**Causes**: 
- Raw names missing from `raw_customer_inputs`
- Normalization views not processing the data
- Dashboard not using normalization audit view

**Solutions**:
1. Run sync query to add missing `bill_code` values
2. Check if normalization views are populated:
   ```sql
   SELECT COUNT(*) FROM v_customer_normalization_audit 
   WHERE raw_text LIKE '%PRODUCTION';
   ```
3. Verify dashboard joins with `v_customer_normalization_audit`

### Duplicate Customers After Canonicalization  
**Symptoms**: Same normalized customer appears multiple times
**Causes**:
- Different `customer_id` values for same normalized name
- Dashboard grouping by customer_id instead of normalized name
- Revenue type grouping creating separate rows

**Solutions**:
1. Check for different customer IDs:
   ```sql
   SELECT normalized_name, COUNT(DISTINCT customer_id) as id_count
   FROM v_customer_normalization_audit
   GROUP BY normalized_name
   HAVING id_count > 1;
   ```
2. Verify dashboard GROUP BY excludes customer_id
3. Ensure revenue types are combined per customer

### Canon Tool Changes Not Appearing
**Symptoms**: Changes made in Canon Tool don't affect dashboard
**Causes**:
- View chain not refreshing
- Caching issues
- Database transaction not committed

**Solutions**:
1. Check canonical maps were updated:
   ```sql
   SELECT * FROM agency_canonical_map WHERE alias_name = 'your_alias';
   SELECT * FROM customer_canonical_map WHERE alias_name = 'your_alias';
   ```
2. Verify entity_aliases entries:
   ```sql
   SELECT * FROM entity_aliases WHERE alias_name = 'your_raw_string';
   ```
3. Restart application to clear caches

### New Customer Names Not Appearing in Tool
**Symptoms**: Recent customer names don't show up for canonicalization
**Cause**: Raw inputs out of sync with live data

**Solution**:
1. Run sync query (see Data Sync Requirements section)
2. Verify new names appear in `raw_customer_inputs`
3. Check views are processing new data

---

## Maintenance

### Regular Tasks
- **Weekly**: Check for new duplicate customers in dashboard
- **Monthly**: Run raw data sync after major imports
- **Quarterly**: Review canonicalization coverage and effectiveness

### Monitoring Queries
```sql
-- Check normalization coverage
SELECT 
    COUNT(DISTINCT bill_code) as total_customers,
    COUNT(DISTINCT CASE WHEN normalized_name != bill_code THEN bill_code END) as normalized_customers,
    ROUND(COUNT(DISTINCT CASE WHEN normalized_name != bill_code THEN bill_code END) * 100.0 / COUNT(DISTINCT bill_code), 2) as normalization_pct
FROM spots s
LEFT JOIN v_customer_normalization_audit audit ON audit.raw_text = s.bill_code
WHERE s.bill_code IS NOT NULL;

-- Find most common unnormalized patterns
SELECT bill_code, COUNT(*) as frequency
FROM spots 
WHERE bill_code NOT IN (
    SELECT raw_text FROM v_customer_normalization_audit 
    WHERE normalized_name != raw_text
)
AND bill_code IS NOT NULL
GROUP BY bill_code
ORDER BY frequency DESC
LIMIT 20;
```

---

## Deployment Notes
- Requires Flask + SQLite 3.40+.
- App config must provide `DB_PATH`.
- Run with WAL journaling enabled for concurrency.
- Frontend template: `customer_normalization_manager.html`.
- Ensure blueprints registered in `app.py`:
  ```python
  from src.web.routes.canon_tools import canon_bp
  app.register_blueprint(canon_bp)
  ```

---

## Audit Trail Details
Every successful operation inserts into `canon_audit`:
- **actor**: IP of requester (or blank if unavailable)
- **action**: `agency_canon`, `customer_canon`, or `raw_map`
- **key**: alias or raw string
- **value**: canonical or normalized_name
- **extra**: metadata (hit count, customer_id, etc.)

**Query audit trail:**
```sql
SELECT ts, action, key, value, extra 
FROM canon_audit 
ORDER BY ts DESC 
LIMIT 50;
```

---

## Future Enhancements
- User authentication / permissions for sensitive operations
- Bulk import/export of mappings for large-scale updates
- UI search/filter for audit log review
- Admin review workflow for critical changes
- Automated conflict detection and resolution suggestions
- Integration with data import workflows for proactive normalization