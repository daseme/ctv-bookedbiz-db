# CTV Booked Business Database Schema

**Generated:** 2025-06-11T08:18:48.801930  
**Database:** production.db

CTV Booked Business Database - Revenue tracking and management system

## Tables Overview

### agencies

**Description:** Advertising agency master data  
**Row Count:** 65

| Column | Type | Null | PK | Default | Description |
|--------|------|------|----|---------|-----------|
| `agency_id` | INTEGER | Yes | PK |  | Column: agency_id |
| `agency_name` | TEXT | No |  |  | Column: agency_name |
| `created_date` | TIMESTAMP | Yes |  | CURRENT_TIMESTAMP | Column: created_date |
| `updated_date` | TIMESTAMP | Yes |  | CURRENT_TIMESTAMP | Column: updated_date |
| `is_active` | BOOLEAN | Yes |  | 1 | Column: is_active |
| `notes` | TEXT | Yes |  |  | Column: notes |

**Sample Data:**
- `agency_id`: 7, 36, 8...
- `agency_name`: 3 Olives Media, 3Fold, A Partnership...
- `created_date`: 2025-06-06 17:44:15, 2025-06-06 17:44:19, 2025-06-06 17:44:20...

---

### budget

**Description:** Annual budget allocations by AE and time period  
**Row Count:** 60

| Column | Type | Null | PK | Default | Description |
|--------|------|------|----|---------|-----------|
| `budget_id` | INTEGER | Yes | PK |  | Column: budget_id |
| `ae_name` | TEXT | No |  |  | Column: ae_name |
| `year` | INTEGER | No |  |  | Column: year |
| `month` | INTEGER | No |  |  | Column: month |
| `budget_amount` | DECIMAL(12, 2) | No |  |  | Column: budget_amount |
| `created_date` | TIMESTAMP | Yes |  | CURRENT_TIMESTAMP | Column: created_date |
| `updated_date` | TIMESTAMP | Yes |  | CURRENT_TIMESTAMP | Column: updated_date |
| `source` | TEXT | Yes |  |  | Column: source |

**Sample Data:**
- `budget_id`: 1, 2, 3...
- `ae_name`: Charmaine Lane, House, White Horse International...
- `year`: 2025

---

### budget_data

**Description:** Database table: budget_data  
**Row Count:** 60

| Column | Type | Null | PK | Default | Description |
|--------|------|------|----|---------|-----------|
| `budget_data_id` | INTEGER | Yes | PK |  | Column: budget_data_id |
| `version_id` | INTEGER | No |  |  | Column: version_id |
| `ae_name` | TEXT | No |  |  | Column: ae_name |
| `year` | INTEGER | No |  |  | Column: year |
| `quarter` | INTEGER | No |  |  | Column: quarter |
| `month` | INTEGER | No |  |  | Column: month |
| `budget_amount` | DECIMAL(12, 2) | No |  |  | Column: budget_amount |
| `created_date` | TIMESTAMP | Yes |  | CURRENT_TIMESTAMP | Column: created_date |

**Sample Data:**
- `budget_data_id`: 1, 2, 3...
- `version_id`: 1
- `ae_name`: Charmaine Lane, House, White Horse International...

---

### budget_versions

**Description:** Database table: budget_versions  
**Row Count:** 1

| Column | Type | Null | PK | Default | Description |
|--------|------|------|----|---------|-----------|
| `version_id` | INTEGER | Yes | PK |  | Column: version_id |
| `version_name` | TEXT | No |  |  | Column: version_name |
| `year` | INTEGER | No |  |  | Column: year |
| `created_date` | TIMESTAMP | Yes |  | CURRENT_TIMESTAMP | Column: created_date |
| `created_by` | TEXT | Yes |  |  | Column: created_by |
| `description` | TEXT | Yes |  |  | Column: description |
| `is_active` | BOOLEAN | Yes |  | 1 | Column: is_active |
| `source_file` | TEXT | Yes |  |  | Column: source_file |

**Sample Data:**
- `version_id`: 1
- `version_name`: 2025_Initial
- `year`: 2025

---

### customer_mappings

**Description:** Database table: customer_mappings  
**Row Count:** 0

| Column | Type | Null | PK | Default | Description |
|--------|------|------|----|---------|-----------|
| `mapping_id` | INTEGER | Yes | PK |  | Column: mapping_id |
| `original_name` | TEXT | No |  |  | Column: original_name |
| `customer_id` | INTEGER | No |  |  | Column: customer_id |
| `created_date` | TIMESTAMP | Yes |  | CURRENT_TIMESTAMP | Column: created_date |
| `created_by` | TEXT | Yes |  | 'system' | Column: created_by |
| `confidence_score` | REAL | Yes |  |  | Column: confidence_score |

---

### customers

**Description:** Customer master data with normalized names and sector classifications  
**Row Count:** 158

| Column | Type | Null | PK | Default | Description |
|--------|------|------|----|---------|-----------|
| `customer_id` | INTEGER | Yes | PK |  | Unique customer identifier (primary key) |
| `normalized_name` | TEXT | No |  |  | Standardized customer name for reporting |
| `sector_id` | INTEGER | Yes |  |  | Column: sector_id |
| `agency_id` | INTEGER | Yes |  |  | Column: agency_id |
| `created_date` | TIMESTAMP | Yes |  | CURRENT_TIMESTAMP | Column: created_date |
| `updated_date` | TIMESTAMP | Yes |  | CURRENT_TIMESTAMP | Column: updated_date |
| `customer_type` | TEXT | Yes |  |  | Column: customer_type |
| `is_active` | BOOLEAN | Yes |  | 1 | Column: is_active |
| `notes` | TEXT | Yes |  |  | Column: notes |

**Sample Data:**
- `customer_id`: 1, 2, 3...
- `normalized_name`: 4Imprint (Marketing Architects), AAAJ SoCal, AIDS Healthcare Foundation...
- `sector_id`: 1, 2, 3...

---

### import_batches

**Description:** Database table: import_batches  
**Row Count:** 6

| Column | Type | Null | PK | Default | Description |
|--------|------|------|----|---------|-----------|
| `batch_id` | TEXT | Yes | PK |  | Column: batch_id |
| `import_date` | TIMESTAMP | No |  | CURRENT_TIMESTAMP | Column: import_date |
| `import_mode` | TEXT | No |  |  | Column: import_mode |
| `source_file` | TEXT | No |  |  | Column: source_file |
| `broadcast_months_affected` | TEXT | Yes |  |  | Column: broadcast_months_affected |
| `records_imported` | INTEGER | Yes |  | 0 | Column: records_imported |
| `records_deleted` | INTEGER | Yes |  | 0 | Column: records_deleted |
| `status` | TEXT | No |  | 'RUNNING' | Column: status |
| `started_by` | TEXT | Yes |  |  | Column: started_by |
| `completed_at` | TIMESTAMP | Yes |  |  | Column: completed_at |
| `notes` | TEXT | Yes |  |  | Column: notes |
| `error_summary` | TEXT | Yes |  |  | Column: error_summary |

**Sample Data:**
- `batch_id`: historical_1749231709, historical_1749232743, historical_1749233540...
- `import_date`: 2025-06-06 17:42:43, 2025-06-06 17:59:23, 2025-06-06 18:12:40...
- `import_mode`: HISTORICAL, WEEKLY_UPDATE

---

### languages

**Description:** Database table: languages  
**Row Count:** 9

| Column | Type | Null | PK | Default | Description |
|--------|------|------|----|---------|-----------|
| `language_id` | INTEGER | Yes | PK |  | Column: language_id |
| `language_code` | TEXT | No |  |  | Column: language_code |
| `language_name` | TEXT | No |  |  | Column: language_name |
| `language_group` | TEXT | Yes |  |  | Column: language_group |
| `created_date` | TIMESTAMP | Yes |  | CURRENT_TIMESTAMP | Column: created_date |

**Sample Data:**
- `language_id`: 3, 1, 5...
- `language_code`: C, E, Hm...
- `language_name`: English, Mandarin, Cantonese...

---

### markets

**Description:** Geographic market definitions  
**Row Count:** 8

| Column | Type | Null | PK | Default | Description |
|--------|------|------|----|---------|-----------|
| `market_id` | INTEGER | Yes | PK |  | Column: market_id |
| `market_name` | TEXT | No |  |  | Column: market_name |
| `market_code` | TEXT | No |  |  | Column: market_code |
| `region` | TEXT | Yes |  |  | Column: region |
| `is_active` | BOOLEAN | Yes |  | 1 | Column: is_active |
| `created_date` | TIMESTAMP | Yes |  | CURRENT_TIMESTAMP | Column: created_date |

**Sample Data:**
- `market_id`: 6, 3, 13...
- `market_name`: CHI MSP, Central Valley, DALLAS...
- `market_code`: CMP, CVC, DAL...

---

### month_closures

**Description:** Monthly financial period closure tracking  
**Row Count:** 17

| Column | Type | Null | PK | Default | Description |
|--------|------|------|----|---------|-----------|
| `broadcast_month` | TEXT | Yes | PK |  | Month that was closed (Mmm-YY format) |
| `closed_date` | DATE | No |  |  | Column: closed_date |
| `closed_by` | TEXT | No |  |  | User who closed the month |
| `notes` | TEXT | Yes |  |  | Column: notes |
| `created_date` | TIMESTAMP | Yes |  | CURRENT_TIMESTAMP | Column: created_date |

**Sample Data:**
- `broadcast_month`: Apr-24, Apr-25, Aug-24...
- `closed_date`: 2025-06-06
- `closed_by`: Kurt

---

### pipeline

**Description:** Revenue pipeline tracking for forecasting and budget management  
**Row Count:** 0

| Column | Type | Null | PK | Default | Description |
|--------|------|------|----|---------|-----------|
| `pipeline_id` | INTEGER | Yes | PK |  | Column: pipeline_id |
| `ae_name` | TEXT | No |  |  | Account Executive name |
| `year` | INTEGER | No |  |  | Pipeline year |
| `month` | INTEGER | No |  |  | Pipeline month |
| `pipeline_amount` | DECIMAL(12, 2) | No |  |  | Forecasted revenue amount |
| `update_date` | DATE | No |  |  | Column: update_date |
| `is_current` | BOOLEAN | Yes |  | 1 | Whether this is the current active pipeline version |
| `created_date` | TIMESTAMP | Yes |  | CURRENT_TIMESTAMP | Column: created_date |
| `created_by` | TEXT | Yes |  |  | Column: created_by |
| `notes` | TEXT | Yes |  |  | Column: notes |

---

### sectors

**Description:** Business sector classifications and hierarchies  
**Row Count:** 16

| Column | Type | Null | PK | Default | Description |
|--------|------|------|----|---------|-----------|
| `sector_id` | INTEGER | Yes | PK |  | Column: sector_id |
| `sector_code` | TEXT | No |  |  | Column: sector_code |
| `sector_name` | TEXT | No |  |  | Column: sector_name |
| `sector_group` | TEXT | Yes |  |  | Column: sector_group |
| `is_active` | BOOLEAN | Yes |  | 1 | Column: is_active |
| `created_date` | TIMESTAMP | Yes |  | CURRENT_TIMESTAMP | Column: created_date |

**Sample Data:**
- `sector_id`: 1, 15, 2...
- `sector_code`: AUTO, CASINO, CPG...
- `sector_name`: Automotive, Consumer Packaged Goods, Insurance...

---

### spots

**Description:** Main revenue table containing all booked advertising spots with customer and sales data  
**Row Count:** 738,992

| Column | Type | Null | PK | Default | Description |
|--------|------|------|----|---------|-----------|
| `spot_id` | INTEGER | Yes | PK |  | Column: spot_id |
| `bill_code` | TEXT | No |  |  | Billing code for agency tracking |
| `air_date` | DATE | No |  |  | Column: air_date |
| `end_date` | DATE | Yes |  |  | Column: end_date |
| `day_of_week` | TEXT | Yes |  |  | Column: day_of_week |
| `time_in` | TEXT | Yes |  |  | Column: time_in |
| `time_out` | TEXT | Yes |  |  | Column: time_out |
| `length_seconds` | TEXT | Yes |  |  | Column: length_seconds |
| `media` | TEXT | Yes |  |  | Column: media |
| `program` | TEXT | Yes |  |  | Column: program |
| `language_code` | TEXT | Yes |  |  | Column: language_code |
| `format` | TEXT | Yes |  |  | Column: format |
| `sequence_number` | INTEGER | Yes |  |  | Column: sequence_number |
| `line_number` | INTEGER | Yes |  |  | Column: line_number |
| `spot_type` | TEXT | Yes |  |  | Column: spot_type |
| `estimate` | TEXT | Yes |  |  | Column: estimate |
| `gross_rate` | DECIMAL(12, 2) | Yes |  |  | Revenue amount for the advertising spot |
| `make_good` | TEXT | Yes |  |  | Column: make_good |
| `spot_value` | DECIMAL(12, 2) | Yes |  |  | Column: spot_value |
| `broadcast_month` | TEXT | Yes |  |  | Month when the ad was/will be broadcast |
| `broker_fees` | DECIMAL(12, 2) | Yes |  |  | Column: broker_fees |
| `priority` | INTEGER | Yes |  |  | Column: priority |
| `station_net` | DECIMAL(12, 2) | Yes |  |  | Column: station_net |
| `sales_person` | TEXT | Yes |  |  | Account Executive responsible for the sale |
| `revenue_type` | TEXT | Yes |  |  | Type of revenue (e.g., Trade, Cash) |
| `billing_type` | TEXT | Yes |  |  | Column: billing_type |
| `agency_flag` | TEXT | Yes |  |  | Column: agency_flag |
| `affidavit_flag` | TEXT | Yes |  |  | Column: affidavit_flag |
| `contract` | TEXT | Yes |  |  | Column: contract |
| `market_name` | TEXT | Yes |  |  | Column: market_name |
| `customer_id` | INTEGER | Yes |  |  | Unique customer identifier |
| `agency_id` | INTEGER | Yes |  |  | Advertising agency identifier |
| `market_id` | INTEGER | Yes |  |  | Geographic market identifier |
| `language_id` | INTEGER | Yes |  |  | Column: language_id |
| `load_date` | TIMESTAMP | Yes |  | CURRENT_TIMESTAMP | Column: load_date |
| `source_file` | TEXT | Yes |  |  | Column: source_file |
| `is_historical` | BOOLEAN | Yes |  | 0 | Column: is_historical |
| `effective_date` | DATE | Yes |  |  | Column: effective_date |
| `import_batch_id` | TEXT | Yes |  |  | Column: import_batch_id |

**Sample Data:**
- `spot_id`: 726675, 726676, 726677...
- `bill_code`: Acento:City Colleges of Chicago PRODUCTION, Hoffman Lewis:Toyota PRODUCTION, iGRAPHIX:Pechanga Resort Casino PROD...
- `air_date`: 11/25/2024, 11/26/2024, 11/27/2024...

---

### sqlite_sequence

**Description:** Database table: sqlite_sequence  
**Row Count:** 9

| Column | Type | Null | PK | Default | Description |
|--------|------|------|----|---------|-----------|
| `name` |  | Yes |  |  | Column: name |
| `seq` |  | Yes |  |  | Column: seq |

**Sample Data:**
- `name`: markets, languages, sectors...
- `seq`: 14, 9, 16...

---

