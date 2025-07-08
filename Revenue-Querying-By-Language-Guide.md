    # 📊 Revenue Querying by Language - Complete Guide

    *A comprehensive guide to accurately querying broadcast revenue data with perfect reconciliation*

    ## 🎯 Overview

    This guide documents the proven methodology for querying revenue data by language categories while ensuring **every dollar is captured exactly once**. Through extensive debugging and validation, we've developed bulletproof queries that achieve perfect reconciliation.

    ## 📋 Table of Contents

    1. [The Challenge](#the-challenge)
    2. [Data Structure](#data-structure)
    3. [Revenue Categories](#revenue-categories)
    4. [Critical Lessons Learned](#critical-lessons-learned)
    5. [Bulletproof Query Patterns](#bulletproof-query-patterns)
    6. [Validation Techniques](#validation-techniques)
    7. [Common Pitfalls](#common-pitfalls)
    8. [Example Implementations](#example-implementations)

    ---

    ## 🔥 The Challenge

    **Goal:** Categorize all broadcast revenue into clear buckets while ensuring:
    - ✅ **Perfect reconciliation** - every dollar captured exactly once
    - ✅ **Complete coverage** - no revenue left uncategorized  
    - ✅ **Logical separation** - clear business rules for each category
    - ✅ **Scalable for any year** - works with consistent logic

    **Target Total for 2024:** $4,076,255.94

    ---

    ## 🏗️ Data Structure

    ### Core Tables

    ```sql
    -- Main revenue table
    spots (
        spot_id, broadcast_month, gross_rate, station_net, 
        spot_type, revenue_type, bill_code, customer_id, agency_id
    )

    -- Language assignment junction table  
    spot_language_blocks (
        spot_id, block_id, spans_multiple_blocks, 
        customer_intent, assignment_method
    )

    -- Language definitions
    language_blocks (block_id, language_id, block_name)
    languages (language_id, language_name)

    -- Agency information
    agencies (agency_id, agency_name)
    ```

    ### Key Relationships

    - **Language Assignment**: `spots` → `spot_language_blocks` → `language_blocks` → `languages`
    - **Agency Relationship**: `spots` → `agencies`
    - **Revenue Source**: Use `gross_rate` field only (not `station_net` or `broker_fees`)

    ---

    ## 💰 Revenue Categories

    ### 1. Direct Response
    **Definition:** All WorldLink agency advertising  
    **Business Logic:** Direct response advertising regardless of language targeting  
    **Identifier:** `agency_name LIKE '%WorldLink%' OR bill_code LIKE '%WorldLink%'`

    ### 2. Individual Language Blocks  
    **Definition:** Content targeting specific language communities  
    **Business Logic:** Single language targeting for community engagement  
    **Identifier:** `spans_multiple_blocks = 0 AND block_id IS NOT NULL`

    ### 3. Multi-Language (Cross-Audience)
    **Definition:** Content spanning multiple language blocks  
    **Business Logic:** Broad-reach advertising crossing language boundaries  
    **Identifier:** `spans_multiple_blocks = 1 OR (spans_multiple_blocks = 0 AND block_id IS NULL)`

    ### 4. Branded Content (Production)
    **Definition:** Production work and branded content  
    **Business Logic:** Internal production, not traditional advertising  
    **Identifier:** `spot_type = 'PRD' AND no language assignment`

    ### 5. Services
    **Definition:** Service announcements and branded content  
    **Business Logic:** Station services and announcements  
    **Identifier:** `spot_type = 'SVC' AND no language assignment`

    ### 6. Other Non-Language
    **Definition:** All remaining content without language assignments  
    **Business Logic:** Catch-all for unassigned content  
    **Identifier:** `No language assignment AND not PRD/SVC AND not WorldLink`

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

    ### 4. **Double Exclusion Logic Error**
    **Problem:** WorldLink spots with language assignments were excluded from both categories  
    **Root Cause:** Language categories excluded WorldLink, Direct Response excluded language-assigned  
    **Solution:** Direct Response includes ALL WorldLink (regardless of language assignment)

    ### 5. **Edge Case Spot Types**
    **Problem:** Spots with NULL or empty spot_type fell through category filters  
    **Solution:** Include `OR spot_type IS NULL OR spot_type = ''` in Other Non-Language

    ### 6. **Revenue Field Consistency**
    **Critical:** Always use `gross_rate` only, never mix with `station_net` or `broker_fees`

    ---

    ## 🛡️ Bulletproof Query Patterns

    ### Base Filters (Apply to ALL queries)
    ```sql
    WHERE s.broadcast_month LIKE '%-24'  -- Year filter (24 = 2024)
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)  -- Exclude Trade
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')  -- Include BNS
    ```

    ### Direct Response (ALL WorldLink)
    ```sql
    SELECT SUM(COALESCE(s.gross_rate, 0)) as revenue
    FROM spots s
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    WHERE [base_filters]
    AND (
        (a.agency_name LIKE '%WorldLink%') OR
        (s.bill_code LIKE '%WorldLink%')
    );
    ```

    ### Language Categories (NULL-safe WorldLink exclusion)
    ```sql
    SELECT SUM(COALESCE(s.gross_rate, 0)) as revenue
    FROM spots s
    JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    WHERE [base_filters]
    -- NULL-safe WorldLink exclusion (CRITICAL)
    AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
    AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
    -- Language-specific filters here
    ```

    ### Individual Languages
    ```sql
    -- Add to language query above:
    AND ((slb.spans_multiple_blocks = 0 AND slb.block_id IS NOT NULL) OR 
        (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NOT NULL))
    ```

    ### Multi-Language  
    ```sql
    -- Add to language query above:
    AND (slb.spans_multiple_blocks = 1 OR 
        (slb.spans_multiple_blocks = 0 AND slb.block_id IS NULL) OR 
        (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NULL))
    ```

    ### Non-Language Categories
    ```sql
    SELECT SUM(COALESCE(s.gross_rate, 0)) as revenue
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    WHERE [base_filters]
    AND slb.spot_id IS NULL  -- No language assignment
    -- NULL-safe WorldLink exclusion
    AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
    AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
    -- Category-specific filters
    ```

    ---

    ## ✅ Validation Techniques

    ### 1. Total Revenue Validation
    ```sql
    -- This should equal your target total
    SELECT SUM(COALESCE(gross_rate, 0)) as total_revenue
    FROM spots 
    WHERE broadcast_month LIKE '%-24'
    AND (revenue_type != 'Trade' OR revenue_type IS NULL)
    AND (gross_rate IS NOT NULL OR station_net IS NOT NULL OR spot_type = 'BNS');
    ```

    ### 2. Perfect Reconciliation Test
    ```sql
    -- Sum of all categories should equal total revenue
    WITH category_totals AS (
    SELECT 
        (SELECT SUM(...) FROM direct_response_query) +
        (SELECT SUM(...) FROM language_categories_query) +
        (SELECT SUM(...) FROM non_language_categories_query) as category_sum,
        (SELECT SUM(...) FROM total_validation_query) as database_total
    )
    SELECT 
    category_sum,
    database_total,
    ABS(category_sum - database_total) as difference
    FROM category_totals;
    -- Difference should be < $1.00
    ```

    ### 3. Missing Spots Analysis
    ```sql
    -- Find spots not captured by any category
    SELECT COUNT(*), SUM(COALESCE(gross_rate, 0)), spot_type
    FROM spots s
    WHERE [base_filters]
    AND spot_id NOT IN (
        SELECT spot_id FROM direct_response_query
        UNION SELECT spot_id FROM language_query  
        UNION SELECT spot_id FROM non_language_query
    )
    GROUP BY spot_type;
    -- Should return no results or minimal edge cases
    ```

    ---

    ## ⚠️ Common Pitfalls

    ### 1. **NULL Handling**
    ❌ **Wrong:** `NOT (agency_name LIKE '%WorldLink%')`  
    ✅ **Right:** `COALESCE(agency_name, '') NOT LIKE '%WorldLink%'`

    ### 2. **BNS Exclusion**
    ❌ **Wrong:** `(gross_rate IS NOT NULL OR station_net IS NOT NULL)`  
    ✅ **Right:** `(gross_rate IS NOT NULL OR station_net IS NOT NULL OR spot_type = 'BNS')`

    ### 3. **Revenue Field Mixing**
    ❌ **Wrong:** `SUM(gross_rate + broker_fees)`  
    ✅ **Right:** `SUM(COALESCE(gross_rate, 0))`

    ### 4. **Double Exclusion**
    ❌ **Wrong:** Excluding WorldLink from language AND excluding language-assigned from Direct Response  
    ✅ **Right:** Direct Response gets ALL WorldLink regardless of language assignment

    ### 5. **Trade Revenue**
    ❌ **Wrong:** Including Trade revenue in analysis  
    ✅ **Right:** Always filter `(revenue_type != 'Trade' OR revenue_type IS NULL)`

    ### 6. **Edge Case Spot Types**
    ❌ **Wrong:** `spot_type NOT IN ('PRD', 'SVC')`  
    ✅ **Right:** `(spot_type NOT IN ('PRD', 'SVC') OR spot_type IS NULL OR spot_type = '')`

    ---

    ## 🔧 Example Implementations

    ### Complete Revenue Report Query
    ```sql
    WITH revenue_breakdown AS (
    -- Direct Response (ALL WorldLink)
    SELECT 'Direct Response' as category,
            SUM(COALESCE(s.gross_rate, 0)) as revenue
    FROM spots s
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    WHERE s.broadcast_month LIKE '%-24'
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        AND ((a.agency_name LIKE '%WorldLink%') OR (s.bill_code LIKE '%WorldLink%'))
    
    UNION ALL
    
    -- Multi-Language (Cross-Audience)
    SELECT 'Multi-Language (Cross-Audience)' as category,
            SUM(COALESCE(s.gross_rate, 0)) as revenue
    FROM spots s
    JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    WHERE s.broadcast_month LIKE '%-24'
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        AND (slb.spans_multiple_blocks = 1 OR 
            (slb.spans_multiple_blocks = 0 AND slb.block_id IS NULL) OR 
            (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NULL))
        AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
        AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
    
    -- Add other categories following same pattern...
    )
    SELECT category, revenue,
        ROUND(revenue * 100.0 / SUM(revenue) OVER(), 2) as percentage
    FROM revenue_breakdown
    ORDER BY revenue DESC;
    ```

    ### Language-Specific Breakdown
    ```sql
    SELECT 
    COALESCE(l.language_name, 'Unknown') as language,
    COUNT(*) as spots,
    SUM(COALESCE(s.gross_rate, 0)) as revenue,
    COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bonus_spots,
    ROUND(COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) * 100.0 / COUNT(*), 1) as bonus_pct
    FROM spots s
    JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN language_blocks lb ON slb.block_id = lb.block_id
    LEFT JOIN languages l ON lb.language_id = l.language_id
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    WHERE s.broadcast_month LIKE '%-24'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND ((slb.spans_multiple_blocks = 0 AND slb.block_id IS NOT NULL) OR 
        (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NOT NULL))
    AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
    AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
    GROUP BY l.language_name
    ORDER BY revenue DESC;
    ```

    ---

    ## 🎯 Success Metrics

    ### Perfect Implementation Should Achieve:
    - ✅ **0.00% reconciliation error** (total categories = database total)
    - ✅ **100% spot coverage** (no missing spots in validation)
    - ✅ **Logical category separation** (clear business rules)
    - ✅ **Consistent bonus reporting** (proper BNS inclusion)
    - ✅ **Multi-year reliability** (works for any year)

    ### Example 2024 Results:
    | Category | Revenue | Percentage | Spots |
    |----------|---------|------------|-------|
    | Individual Language Blocks | $2,424,212.16 | 59.5% | 45,685 |
    | Multi-Language (Cross-Audience) | $1,107,510.79 | 27.2% | 29,607 |
    | Direct Response | $354,506.93 | 8.7% | 41,858 |
    | Other Non-Language | $125,433.77 | 3.1% | 194 |
    | Branded Content | $52,592.29 | 1.3% | 78 |
    | Services | $12,000.00 | 0.3% | 14 |
    | **TOTAL** | **$4,076,255.94** | **100.0%** | **117,436** |

    ---

    ## 🚀 Tools and Scripts

    ### Automated Report Generation
    - **Script:** `comprehensive_revenue_report.py`
    - **Usage:** `python comprehensive_revenue_report.py 2024`
    - **Output:** Complete markdown report with validation

    ### Validation Scripts  
    - **Script:** `test_revenue_script.sh`
    - **Usage:** Quick validation of all categories
    - **Purpose:** Verify perfect reconciliation

    ### Debugging Queries
    - **Purpose:** Find missing revenue and edge cases
    - **Location:** See debugging sections in this guide

    ---

    ## 📚 References

    - **Database Schema:** `schema-250707-0206pm.sql`
    - **Business Rules Documentation:** `BUSINESS_RULES_GUIDE.md`
    - **Fixed Revenue Report Script:** `cli_language_monthly_report.py`

    ---

    ## ⚡ Quick Start

    1. **Use the base filters** in every query
    2. **Apply NULL-safe WorldLink exclusion** for language categories  
    3. **Include BNS spots** with the special revenue filter
    4. **Validate totals** match database total
    5. **Check for missing spots** with validation queries

    **Remember:** Perfect reconciliation is achievable and required. Any gaps indicate a logical error in the query design.

    ---

    *This guide represents months of debugging, validation, and refinement to achieve bulletproof revenue querying. Follow these patterns exactly for guaranteed accuracy.*