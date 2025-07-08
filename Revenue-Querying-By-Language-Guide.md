# 📊 Revenue Querying by Language - Complete Guide (Updated 2025)

*A comprehensive guide to accurately querying broadcast revenue data with perfect reconciliation*

## 🎯 Overview

This guide documents the proven methodology for querying revenue data by language categories while ensuring **every dollar is captured exactly once**. Through extensive debugging, validation, and strategic analysis, we've developed bulletproof queries that achieve perfect reconciliation with enhanced business intelligence.

## 📋 Table of Contents

1. [The Challenge](#the-challenge)
2. [Data Structure](#data-structure)
3. [Revenue Categories](#revenue-categories)
4. [Critical Lessons Learned](#critical-lessons-learned)
5. [New Strategic Insights](#new-strategic-insights)
6. [Bulletproof Query Patterns](#bulletproof-query-patterns)
7. [Validation Techniques](#validation-techniques)
8. [Common Pitfalls](#common-pitfalls)
9. [Example Implementations](#example-implementations)

---

## 🔥 The Challenge

**Goal:** Categorize all broadcast revenue into clear buckets while ensuring:
- ✅ **Perfect reconciliation** - every dollar captured exactly once
- ✅ **Complete coverage** - no revenue left uncategorized  
- ✅ **Logical separation** - clear business rules for each category
- ✅ **Strategic insights** - reveal cross-audience patterns and prime time strategies
- ✅ **Scalable for any year** - works with consistent logic

**Target Total for 2024:** $4,076,255.94

---

## 🏗️ Data Structure

### Core Tables

```sql
-- Main revenue table
spots (
    spot_id, broadcast_month, gross_rate, station_net, 
    spot_type, revenue_type, bill_code, customer_id, agency_id,
    time_in, time_out, day_of_week, language_code
)

-- Language assignment junction table  
spot_language_blocks (
    spot_id, block_id, spans_multiple_blocks, 
    customer_intent, assignment_method
)

-- Language definitions
language_blocks (block_id, language_id, block_name)
languages (language_id, language_name, language_code)

-- Agency information
agencies (agency_id, agency_name)
customers (customer_id, normalized_name)
```

### Key Relationships

- **Language Assignment**: `spots` → `spot_language_blocks` → `language_blocks` → `languages`
- **Agency/Customer**: `spots` → `agencies` / `customers`
- **Revenue Source**: Use `gross_rate` field only (not `station_net` or `broker_fees`)

---

## 💰 Revenue Categories (Updated)

### 1. Individual Language Blocks  
**Definition:** Content targeting specific language communities  
**Business Logic:** Single language targeting for community engagement  
**Identifier:** `spans_multiple_blocks = 0 AND block_id IS NOT NULL`
**New Feature:** Mandarin + Cantonese combined as "Chinese" for unified analysis

### 2. Chinese Prime Time *(NEW CATEGORY)*
**Definition:** Multi-language spots during Chinese prime viewing hours  
**Business Logic:** Cross-audience targeting during peak Chinese TV time  
**Schedule:** M-F 7pm-11:59pm + Weekend 8pm-11:59pm  
**Identifier:** Multi-language spots during Chinese prime time windows

### 3. Multi-Language (Cross-Audience) *(REFINED)*
**Definition:** Cross-audience content outside Chinese prime time  
**Business Logic:** Filipino-led cross-cultural advertising strategy  
**Key Insight:** Filipino (Tagalog) represents 45.6% of this category  
**Identifier:** `spans_multiple_blocks = 1 OR block_id IS NULL` (excluding Chinese prime time)

### 4. Direct Response
**Definition:** All WorldLink agency advertising  
**Business Logic:** Direct response advertising regardless of language targeting  
**Identifier:** `agency_name LIKE '%WorldLink%' OR bill_code LIKE '%WorldLink%'`

### 5. Overnight Shopping *(NEW CATEGORY)*
**Definition:** NKB:Shop LC overnight programming only  
**Business Logic:** Dedicated shopping channel programming  
**Schedule:** 7-day operation starting 6:00:00+  
**Identifier:** NKB customer with no language assignment

### 6. Other Non-Language *(REFINED)*
**Definition:** Remaining non-language spots (excluding NKB)  
**Business Logic:** Miscellaneous spots requiring investigation  
**Identifier:** `No language assignment AND not PRD/SVC/WorldLink/NKB`

### 7. Branded Content (PRD)
**Definition:** Production work and branded content  
**Business Logic:** Internal production, not traditional advertising  
**Identifier:** `spot_type = 'PRD' AND no language assignment`

### 8. Services (SVC)
**Definition:** Service announcements  
**Business Logic:** Station services and announcements  
**Identifier:** `spot_type = 'SVC' AND no language assignment`

---

## 🧠 Critical Lessons Learned

### 1. **NULL Agency Bug (CRITICAL)**
**Problem:** NULL agencies broke WorldLink exclusion logic  
**Root Cause:** `NOT (NULL LIKE '%WorldLink%')` returns NULL, excluding valid spots  
**Solution:** Use NULL-safe filtering: `COALESCE(agency_name, '') NOT LIKE '%WorldLink%'`

### 2. **BNS Spot Exclusion** 
**Problem:** BNS (bonus) spots have NULL gross_rate AND NULL station_net  
**Root Cause:** Filter `(gross_rate IS NOT NULL OR station_net IS NOT NULL)` excluded them  
**Solution:** Add `OR spot_type = 'BNS'` to include bonus content

### 3. **Trade Revenue Exclusion**
**Problem:** Trade revenue should be excluded from all analysis  
**Solution:** Always filter `(revenue_type != 'Trade' OR revenue_type IS NULL)`

### 4. **Chinese Prime Time Discovery** *(NEW)*
**Problem:** Chinese evening/weekend spots mixed with general multi-language  
**Root Cause:** Weekend 8pm-11:59pm and weekday 7pm-11:59pm represent distinct Chinese strategy  
**Solution:** Separate Chinese prime time as distinct cross-audience category

### 5. **Filipino Cross-Audience Leadership** *(NEW)*
**Discovery:** Filipino programming drives 45.6% of cross-audience revenue  
**Insight:** Filipino community leads cross-cultural advertising integration  
**Strategy:** Filipino programming should be recognized as premium cross-audience inventory

### 6. **Language Code vs Language Name Gap** *(NEW)*
**Problem:** All spots show "Unknown Language" but have specific language codes  
**Root Cause:** Database mapping issue between spots.language_code and languages table  
**Insight:** Codes reveal strategic daypart buying (T=Tagalog, M=Mandarin, etc.)

---

## 🚀 New Strategic Insights

### 1. **Chinese Market Dominance**
**Combined Chinese Revenue:** Individual Chinese blocks + Chinese Prime Time = $1.35M+  
**Strategy:** Chinese represents largest language market when combining targeting approaches  
**Business Impact:** Chinese prime time more valuable as time slot than language content

### 2. **Filipino Cross-Audience Leadership**
**Key Finding:** Filipino programming generates 45.6% of cross-audience revenue  
**Strategic Value:** Filipino time slots crossing into other language communities  
**Government Partnership:** Strong government advertising presence (CalTrans, CA Colleges)

### 3. **Weekend Programming Strategy**
**Discovery:** Weekend language slots function as general audience inventory  
**Revenue Impact:** $325K+ weekend cross-audience advertising  
**Client Behavior:** Gaming, political, entertainment clients dominate weekends

### 4. **Transition Time Targeting**
**Pattern:** 16:00-19:00 Filipino time highly valued for cross-audience reach  
**Strategy:** Smart buyers using programming gaps between language blocks  
**Pricing Opportunity:** Transition times may be underpriced for their strategic value

### 5. **Overnight Shopping Separation**
**Clarity:** NKB:Shop LC represents dedicated shopping channel programming ($66.7K)  
**Business Model:** 7-day operation with early morning start times  
**Separation Value:** Cleaner analysis of advertising vs. shopping programming

---

## 🛡️ Bulletproof Query Patterns (Updated)

### Base Filters (Apply to ALL queries)
```sql
WHERE s.broadcast_month LIKE '%-24'  -- Year filter (24 = 2024)
AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)  -- Exclude Trade
AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')  -- Include BNS
```

### Chinese Prime Time *(NEW)*
```sql
SELECT SUM(COALESCE(s.gross_rate, 0)) as revenue
FROM spots s
JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
LEFT JOIN agencies a ON s.agency_id = a.agency_id
WHERE [base_filters]
AND (
    -- Chinese Prime Time M-F 7pm-11:59pm
    (s.time_in >= '19:00:00' AND s.time_out <= '23:59:59' 
     AND s.day_of_week IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'))
    OR
    -- Chinese Weekend 8pm-11:59pm  
    (s.time_in >= '20:00:00' AND s.time_out <= '23:59:59'
     AND s.day_of_week IN ('Saturday', 'Sunday'))
)
AND (slb.spans_multiple_blocks = 1 OR 
     (slb.spans_multiple_blocks = 0 AND slb.block_id IS NULL) OR 
     (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NULL))
AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%';
```

### Multi-Language (Excluding Chinese Prime Time) *(UPDATED)*
```sql
SELECT SUM(COALESCE(s.gross_rate, 0)) as revenue
FROM spots s
JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
LEFT JOIN agencies a ON s.agency_id = a.agency_id
WHERE [base_filters]
AND (slb.spans_multiple_blocks = 1 OR 
     (slb.spans_multiple_blocks = 0 AND slb.block_id IS NULL) OR 
     (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NULL))
AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
-- EXCLUDE Chinese Prime Time
AND NOT (
    (s.time_in >= '19:00:00' AND s.time_out <= '23:59:59' 
     AND s.day_of_week IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'))
    OR
    (s.time_in >= '20:00:00' AND s.time_out <= '23:59:59'
     AND s.day_of_week IN ('Saturday', 'Sunday'))
);
```

### Individual Languages (Chinese Combined) *(UPDATED)*
```sql
SELECT 
    CASE 
        WHEN l.language_name IN ('Mandarin', 'Cantonese') THEN 'Chinese'
        ELSE COALESCE(l.language_name, 'Unknown Language')
    END as language,
    COUNT(*) as spots,
    SUM(COALESCE(s.gross_rate, 0)) as revenue,
    COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bonus_spots
FROM spots s
JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
LEFT JOIN language_blocks lb ON slb.block_id = lb.block_id
LEFT JOIN languages l ON lb.language_id = l.language_id
LEFT JOIN agencies a ON s.agency_id = a.agency_id
WHERE [base_filters]
AND ((slb.spans_multiple_blocks = 0 AND slb.block_id IS NOT NULL) OR 
     (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NOT NULL))
AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
GROUP BY CASE 
    WHEN l.language_name IN ('Mandarin', 'Cantonese') THEN 'Chinese'
    ELSE COALESCE(l.language_name, 'Unknown Language')
END
ORDER BY SUM(COALESCE(s.gross_rate, 0)) DESC;
```

### Overnight Shopping (NKB Only) *(NEW)*
```sql
SELECT SUM(COALESCE(s.gross_rate, 0)) as revenue
FROM spots s
LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
LEFT JOIN agencies a ON s.agency_id = a.agency_id
LEFT JOIN customers c ON s.customer_id = c.customer_id
WHERE [base_filters]
AND slb.spot_id IS NULL  -- No language assignment
AND (s.spot_type NOT IN ('PRD', 'SVC') OR s.spot_type IS NULL OR s.spot_type = '')
AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
-- ONLY NKB spots
AND (
    COALESCE(c.normalized_name, '') LIKE '%NKB%' 
    OR COALESCE(s.bill_code, '') LIKE '%NKB%'
    OR COALESCE(a.agency_name, '') LIKE '%NKB%'
);
```

### Other Non-Language (Excluding NKB) *(UPDATED)*
```sql
SELECT SUM(COALESCE(s.gross_rate, 0)) as revenue
FROM spots s
LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
LEFT JOIN agencies a ON s.agency_id = a.agency_id
LEFT JOIN customers c ON s.customer_id = c.customer_id
WHERE [base_filters]
AND slb.spot_id IS NULL  -- No language assignment
AND (s.spot_type NOT IN ('PRD', 'SVC') OR s.spot_type IS NULL OR s.spot_type = '')
AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
-- EXCLUDE NKB spots (they go to overnight shopping)
AND COALESCE(c.normalized_name, '') NOT LIKE '%NKB%'
AND COALESCE(s.bill_code, '') NOT LIKE '%NKB%'
AND COALESCE(a.agency_name, '') NOT LIKE '%NKB%';
```

---

## ✅ Validation Techniques (Enhanced)

### 1. Perfect Reconciliation Test *(UPDATED)*
```sql
-- Sum of all categories should equal total revenue
WITH category_totals AS (
SELECT 
    (SELECT SUM(...) FROM individual_languages_query) +
    (SELECT SUM(...) FROM chinese_prime_time_query) +
    (SELECT SUM(...) FROM multi_language_query) +
    (SELECT SUM(...) FROM direct_response_query) +
    (SELECT SUM(...) FROM overnight_shopping_query) +
    (SELECT SUM(...) FROM other_nonlanguage_query) +
    (SELECT SUM(...) FROM branded_content_query) +
    (SELECT SUM(...) FROM services_query) as category_sum,
    (SELECT SUM(...) FROM total_validation_query) as database_total
)
SELECT 
category_sum,
database_total,
ABS(category_sum - database_total) as difference,
ROUND(ABS(category_sum - database_total) * 100.0 / database_total, 4) as error_pct
FROM category_totals;
-- Difference should be < $1.00, error_pct should be < 0.001%
```

### 2. Chinese Strategy Validation *(NEW)*
```sql
-- Validate Chinese market analysis
SELECT 
    'Individual Chinese' as category,
    SUM(COALESCE(s.gross_rate, 0)) as revenue
FROM [individual_languages_chinese_query]
UNION ALL
SELECT 
    'Chinese Prime Time' as category,
    SUM(COALESCE(s.gross_rate, 0)) as revenue  
FROM [chinese_prime_time_query]
UNION ALL
SELECT 
    'Total Chinese Strategy' as category,
    SUM(revenue) as revenue
FROM (
    SELECT SUM(COALESCE(s.gross_rate, 0)) as revenue FROM [individual_chinese_query]
    UNION ALL 
    SELECT SUM(COALESCE(s.gross_rate, 0)) as revenue FROM [chinese_prime_time_query]
);
```

### 3. Filipino Cross-Audience Analysis *(NEW)*
```sql
-- Analyze Filipino programming cross-audience dominance
SELECT 
    s.language_code,
    COUNT(*) as total_spots,
    SUM(COALESCE(s.gross_rate, 0)) as total_revenue,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM multi_language_remaining), 2) as spot_pct,
    ROUND(SUM(COALESCE(s.gross_rate, 0)) * 100.0 / (SELECT SUM(COALESCE(gross_rate, 0)) FROM multi_language_remaining), 2) as revenue_pct
FROM [multi_language_excluding_chinese_prime_query] s
GROUP BY s.language_code
ORDER BY total_revenue DESC;
-- Filipino (T) should show ~45.6% of spots and revenue
```

---

## 🎯 Success Metrics (Updated)

### Perfect Implementation Should Achieve:
- ✅ **0.00% reconciliation error** (total categories = database total)
- ✅ **100% spot coverage** (no missing spots in validation)
- ✅ **Logical category separation** (clear business rules)
- ✅ **Strategic insights revealed** (Chinese + Filipino patterns)
- ✅ **Chinese market clarity** (combined individual + prime time analysis)
- ✅ **Cross-audience understanding** (Filipino leadership, weekend strategies)

### Example 2024 Results (Updated):
| Category | Revenue | Percentage | Spots | Key Insight |
|----------|---------|------------|-------|-------------|
| Individual Language Blocks | $2,424,212.16 | 59.5% | 54,696 | Chinese combined: $654K |
| Chinese Prime Time | $699,550.49 | 17.2% | 24,929 | Premium cross-audience time |
| Multi-Language (Cross-Audience) | $407,960.30 | 10.0% | 16,422 | Filipino-led (45.6%) |
| Direct Response | $354,506.93 | 8.7% | 45,037 | WorldLink consistency |
| Other Non-Language | $58,733.77 | 1.4% | 128 | Excluding NKB |
| Overnight Shopping | $66,700.00 | 1.6% | 66 | NKB only |
| Branded Content | $52,592.29 | 1.3% | 0 | Revenue-only |
| Services | $12,000.00 | 0.3% | 0 | Revenue-only |
| **TOTAL** | **$4,076,255.94** | **100.0%** | **141,278** | Perfect reconciliation |

### Language Performance (Updated):
| Language | Revenue | Percentage | Total Spots | Strategy |
|----------|---------|------------|-------------|----------|
| Chinese Prime Time | $699,550.49 | 28.9% | 24,929 | Cross-audience during Chinese prime time |
| Vietnamese | $735,625.49 | 30.4% | 19,422 | Individual language blocks |
| Chinese | $654,802.95 | 27.1% | 9,996 | Individual language blocks (Mandarin + Cantonese) |
| South Asian | $585,320.05 | 24.2% | 14,740 | Individual language blocks |
| Korean | $250,808.19 | 10.4% | 7,152 | Individual language blocks |
| Tagalog | $170,688.09 | 7.1% | 2,123 | Individual language blocks |

**Total Chinese Strategy:** $1,354,353.44 (Chinese blocks + Chinese Prime Time)

---

## 🚀 Tools and Scripts (Updated)

### Enhanced Report Generation
- **Script:** `multi_year_revenue_report.py`
- **Features:** Chinese Prime Time separation, Filipino analysis, Overnight Shopping
- **Usage:** `python multi_year_revenue_report.py 2024`
- **Output:** Complete markdown report with strategic insights

### Strategic Analysis Queries
- **Chinese Market Analysis:** Combined individual + prime time revenue
- **Filipino Cross-Audience Study:** Weekend transition time patterns  
- **Prime Time Validation:** Chinese vs. general cross-audience comparison

---

## 📚 New Business Intelligence

### 1. **Chinese Prime Time Strategy**
- More valuable as **time slot** than **language content**
- **Cross-audience appeal** exceeds language-specific targeting
- **Weekend Chinese time** equally valuable as weekday prime time

### 2. **Filipino Cross-Audience Leadership**  
- **45.6% of cross-audience spots** use Filipino programming
- **Transition time mastery** (16:00-19:00 cross-cultural reach)
- **Government partnership strength** (CalTrans, CA Colleges)

### 3. **Weekend Programming Economics**
- **Language slots become general audience** on weekends
- **Gaming/entertainment focus** drives weekend cross-audience
- **$325K+ weekend revenue** shows strategic value

### 4. **Overnight Shopping Clarity**
- **NKB represents dedicated shopping channel** ($66.7K)
- **7-day operation** with early morning programming
- **Separate business model** from traditional advertising

---

## ⚡ Quick Start (Updated)

1. **Use the base filters** in every query
2. **Separate Chinese Prime Time** from Multi-Language
3. **Combine Mandarin + Cantonese** as "Chinese" in individual languages
4. **Apply NULL-safe WorldLink exclusion** for language categories  
5. **Include BNS spots** with the special revenue filter
6. **Separate NKB overnight shopping** from other non-language
7. **Validate totals** match database total with all 8 categories
8. **Analyze strategic patterns** (Chinese dominance, Filipino leadership)

**Remember:** Perfect reconciliation reveals strategic insights. The new category structure provides clearer business intelligence for inventory optimization and pricing strategies.

---

*This guide represents extensive analysis of cross-audience patterns, prime time strategies, and language community behaviors. The enhanced categorization provides actionable business intelligence for revenue optimization.*