# Language Assignment System - How-To Guide


# After ingesting new spots, just run the workflow again
python cli_01_language_assignment.py --categorize-all
python cli_01_language_assignment.py --process-all-remaining
# Quick health check
python cli_01_language_assignment.py --status-by-category

## Overview

The Language Assignment System automatically categorizes and assigns languages to advertising spots based on business rules and revenue types. This guide explains the system logic for developers writing reporting code and analysts working with the data.

## Table of Contents

1. [System Architecture](#system-architecture)
2. [Business Rules & Categorization](#business-rules--categorization)
3. [Database Schema](#database-schema)
4. [Language Assignment Logic](#language-assignment-logic)
5. [Reporting & Analysis](#reporting--analysis)
6. [Common Queries](#common-queries)
7. [Understanding Review Categories](#understanding-review-categories)

---

## System Architecture

### Three-Stage Processing Pipeline

The system processes spots through three distinct stages:

Raw Spots → Categorization → Language Assignment → Final Assignment

1. **Categorization**: Spots are classified into business categories
2. **Language Assignment**: Language logic is applied based on category
3. **Final Assignment**: Results are stored with metadata

### Separation of Concerns

- **Business Logic**: Revenue type + spot type combinations determine processing category
- **Language Logic**: Language codes are processed only for relevant spots
- **Assignment Logic**: Results are stored with confidence levels and review flags

---

## Business Rules & Categorization

### Primary Categories

Every spot is categorized into one of three processing categories:

#### 1. Language Assignment Required (`language_assignment_required`)
**Purpose**: Spots that need actual language determination
**Business Rule**: `Internal Ad Sales` + Commercial spot types (`COM`, `BNS`)
**Count**: ~288,074 spots

```sql
SELECT COUNT(*) FROM spots 
WHERE revenue_type = 'Internal Ad Sales' 
AND spot_type IN ('COM', 'BNS')

2. Review Category (review_category)
Purpose: Spots requiring manual business review
Business Rules:

Internal Ad Sales + Non-commercial types (PKG, CRD, AV, BB)
Other revenue type + Any spot type
Local revenue type (legacy, should be reclassified)

Count: ~5,893 spots

SELECT COUNT(*) FROM spots 
WHERE (revenue_type = 'Internal Ad Sales' AND spot_type IN ('PKG', 'CRD', 'AV', 'BB'))
   OR (revenue_type = 'Other')
   OR (revenue_type = 'Local')

3. Default English (default_english)
Purpose: Spots that default to English by business rule
Business Rules:

Direct Response Sales (all spot types) - WorldLink, etc.
Paid Programming (all spot types) - Fujisankei, etc.
Branded Content (all spot types) - PRD content

Count: ~826,609 spots

SELECT COUNT(*) FROM spots 
WHERE revenue_type IN ('Direct Response Sales', 'Paid Programming', 'Branded Content')

def categorize_spot(revenue_type: str, spot_type: str) -> SpotCategory:
    # Language Assignment Required
    if revenue_type == 'Internal Ad Sales' and spot_type in ['COM', 'BNS']:
        return LANGUAGE_ASSIGNMENT_REQUIRED
    
    # Review Category  
    if revenue_type == 'Internal Ad Sales' and spot_type in ['PKG', 'CRD', 'AV', 'BB']:
        return REVIEW_CATEGORY
    if revenue_type == 'Other':
        return REVIEW_CATEGORY
    
    # Default English
    if revenue_type in ['Direct Response Sales', 'Paid Programming', 'Branded Content']:
        return DEFAULT_ENGLISH
    
    # Fallback
    return REVIEW_CATEGORY

Database Schema
Core Tables
spots table

-- Key fields for categorization
spot_id INTEGER PRIMARY KEY
revenue_type TEXT              -- 'Internal Ad Sales', 'Direct Response Sales', etc.
spot_type TEXT                 -- 'COM', 'BNS', 'PKG', etc.
language_code TEXT             -- 'E', 'M', 'T', 'L', etc.
spot_category TEXT             -- Added by system: categorization result
bill_code TEXT                 -- Client identification

spot_language_assignments table

spot_language_assignments table-- Final language assignments
assignment_id INTEGER PRIMARY KEY
spot_id INTEGER UNIQUE         -- Links to spots table
language_code TEXT             -- Final assigned language ('english', 'mandarin', etc.)
language_status TEXT           -- 'determined', 'undetermined', 'default', 'invalid'
confidence REAL                -- 0.0-1.0 confidence in assignment
assignment_method TEXT         -- How assignment was made
requires_review BOOLEAN        -- Flags spots needing manual attention
notes TEXT                     -- Explanation of assignment logic
assigned_date TIMESTAMP        -- When assignment was made

languages table

-- Valid language codes
language_id INTEGER PRIMARY KEY
language_code TEXT UNIQUE      -- 'E', 'M', 'T', etc.
language_name TEXT             -- 'English', 'Mandarin', 'Tagalog', etc.
language_group TEXT            -- 'Chinese', 'South Asian', etc.

Key Relationships

-- Join spots with their language assignments
SELECT s.*, sla.* 
FROM spots s
LEFT JOIN spot_language_assignments sla ON s.spot_id = sla.spot_id

-- Join with language names
SELECT s.*, sla.*, l.language_name
FROM spots s
LEFT JOIN spot_language_assignments sla ON s.spot_id = sla.spot_id  
LEFT JOIN languages l ON UPPER(s.language_code) = UPPER(l.language_code)

Language Assignment Logic
Assignment Methods Explained
business_rule_default_english (826,609 spots)

Logic: Spot category = default_english
Language: Always 'english'
Confidence: 1.0 (100% confident)
Review: FALSE (no review needed)
Use Case: Direct Response, Paid Programming, Branded Content

direct_mapping (287,497 spots)

Logic: Spot has valid language_code in languages table
Language: Maps directly from spots.language_code
Confidence: 1.0 (100% confident)
Review: FALSE (no review needed)
Use Case: Internal Ad Sales with clear language codes

business_review_required (5,893 spots)

Logic: Spot category = review_category
Language: 'english' (default)
Confidence: 0.5 (low confidence)
Review: TRUE (needs business review)
Use Case: Unusual revenue/spot type combinations

undetermined_flagged (498 spots)

Logic: Spot has language_code = 'L' (undetermined)
Language: 'L' (preserved)
Confidence: 0.0 (no confidence)
Review: TRUE (needs manual language determination)
Use Case: Spots where language wasn't determined during trafficking

default_english (79 spots)

Logic: Spot has missing/NULL language_code
Language: 'english' (fallback)
Confidence: 0.5 (medium confidence)
Review: FALSE (no review needed)
Use Case: Internal Ad Sales spots missing language codes

Language Status Values
determined

Meaning: Language was successfully determined
Assignment Methods: direct_mapping, business_rule_default_english
Confidence: Usually 1.0

undetermined

Meaning: Language could not be determined
Assignment Methods: undetermined_flagged
Confidence: 0.0
Always requires review

default

Meaning: Language was assigned by fallback rule
Assignment Methods: default_english, business_review_required
Confidence: Usually 0.5

invalid

Meaning: Language code was invalid or not found
Assignment Methods: invalid_code_flagged (rare)
Always requires review

Reporting & Analysis
Understanding Revenue by Language
Commercial Spots Only (Revenue-Generating)

-- Only spots that generate language-specific revenue
SELECT 
    sla.language_code,
    COUNT(*) as spot_count,
    SUM(s.gross_rate) as total_revenue
FROM spots s
JOIN spot_language_assignments sla ON s.spot_id = sla.spot_id
WHERE s.spot_category = 'language_assignment_required'  -- Commercial spots only
AND s.revenue_type = 'Internal Ad Sales'
AND s.spot_type IN ('COM', 'BNS')
GROUP BY sla.language_code
ORDER BY total_revenue DESC

All Spots by Language Assignment

-- All spots regardless of revenue implications
SELECT 
    sla.language_code,
    s.spot_category,
    COUNT(*) as spot_count,
    SUM(s.gross_rate) as total_revenue
FROM spots s
JOIN spot_language_assignments sla ON s.spot_id = sla.spot_id
GROUP BY sla.language_code, s.spot_category
ORDER BY total_revenue DESC

Language Performance Analysis
Language Revenue (Commercial Only)

SELECT 
    l.language_name,
    COUNT(sla.spot_id) as spot_count,
    SUM(s.gross_rate) as revenue,
    AVG(s.gross_rate) as avg_spot_rate,
    COUNT(DISTINCT s.customer_id) as unique_customers
FROM spot_language_assignments sla
JOIN spots s ON sla.spot_id = s.spot_id
JOIN languages l ON UPPER(sla.language_code) = UPPER(l.language_code)
WHERE s.spot_category = 'language_assignment_required'
AND sla.requires_review = 0  -- Exclude review spots
GROUP BY l.language_name, sla.language_code
ORDER BY revenue DESC

Assignment Quality Metrics

SELECT 
    sla.assignment_method,
    sla.language_status,
    COUNT(*) as count,
    AVG(sla.confidence) as avg_confidence,
    COUNT(CASE WHEN sla.requires_review = 1 THEN 1 END) as review_count
FROM spot_language_assignments sla
GROUP BY sla.assignment_method, sla.language_status
ORDER BY count DESC


Spots Requiring Attention
Language Review Required

-- Spots flagged for manual language determination
SELECT 
    s.spot_id,
    s.bill_code,
    s.language_code as original_code,
    sla.language_code as assigned_code,
    sla.notes,
    s.gross_rate
FROM spots s
JOIN spot_language_assignments sla ON s.spot_id = sla.spot_id
WHERE sla.assignment_method = 'undetermined_flagged'
ORDER BY s.gross_rate DESC

Business Review Required

-- Spots flagged for business category review
SELECT 
    s.spot_id,
    s.bill_code,
    s.revenue_type,
    s.spot_type,
    sla.notes,
    s.gross_rate
FROM spots s
JOIN spot_language_assignments sla ON s.spot_id = sla.spot_id
WHERE sla.assignment_method = 'business_review_required'
ORDER BY s.gross_rate DESC

Common Queries
Revenue Reporting by Language
Monthly Language Revenue (Commercial Spots)

SELECT 
    s.broadcast_month,
    sla.language_code,
    COUNT(*) as spot_count,
    SUM(s.gross_rate) as revenue,
    SUM(s.station_net) as net_revenue
FROM spots s
JOIN spot_language_assignments sla ON s.spot_id = sla.spot_id
WHERE s.spot_category = 'language_assignment_required'
AND sla.requires_review = 0
GROUP BY s.broadcast_month, sla.language_code
ORDER BY s.broadcast_month, revenue DESC

Language Performance by Sales Person

SELECT 
    s.sales_person,
    sla.language_code,
    COUNT(*) as spots,
    SUM(s.gross_rate) as revenue
FROM spots s
JOIN spot_language_assignments sla ON s.spot_id = sla.spot_id
WHERE s.spot_category = 'language_assignment_required'
AND sla.requires_review = 0
AND s.broadcast_month = 'Dec-24'  -- Example month
GROUP BY s.sales_person, sla.language_code
ORDER BY s.sales_person, revenue DESC

System Health Monitoring
Assignment Status Summary

SELECT 
    'Total Spots' as metric,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM spots), 1) as percentage
FROM spots
UNION ALL
SELECT 
    'Categorized',
    COUNT(*),
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM spots), 1)
FROM spots WHERE spot_category IS NOT NULL
UNION ALL
SELECT 
    'Language Assigned',
    COUNT(*),
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM spots), 1)
FROM spot_language_assignments
UNION ALL
SELECT 
    'Requiring Review',
    COUNT(*),
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM spot_language_assignments), 1)
FROM spot_language_assignments WHERE requires_review = 1


Processing Errors and Edge Cases

-- Spots that might have processing issues
SELECT 
    'Missing Category' as issue_type,
    COUNT(*) as count
FROM spots WHERE spot_category IS NULL
UNION ALL
SELECT 
    'Categorized but Unassigned',
    COUNT(*)
FROM spots s
LEFT JOIN spot_language_assignments sla ON s.spot_id = sla.spot_id
WHERE s.spot_category IS NOT NULL AND sla.spot_id IS NULL
UNION ALL
SELECT 
    'Low Confidence Assignments',
    COUNT(*)
FROM spot_language_assignments WHERE confidence < 0.5

Understanding Review Categories
Language Review vs Business Review
Language Review (undetermined_flagged)

Trigger: Spots with language_code = 'L'
Action Needed: Manual language determination
Process: Review spot content/context to determine actual language
Resolution: Update language assignment after determination

Business Review (business_review_required)

Trigger: Unusual revenue_type + spot_type combinations
Action Needed: Review business categorization
Process: Verify revenue type and spot type are correct
Resolution: Either reclassify the spot or adjust business rules

Review Workflow
High-Priority Reviews (by Revenue)

SELECT 
    sla.assignment_method,
    s.spot_id,
    s.bill_code,
    s.gross_rate,
    sla.notes
FROM spots s
JOIN spot_language_assignments sla ON s.spot_id = sla.spot_id
WHERE sla.requires_review = 1
AND s.gross_rate > 500  -- High-value spots first
ORDER BY s.gross_rate DESC

Review Resolution Tracking

-- Track review resolution over time
SELECT 
    DATE(sla.assigned_date) as assignment_date,
    sla.assignment_method,
    COUNT(*) as spots_flagged,
    SUM(s.gross_rate) as revenue_at_risk
FROM spots s
JOIN spot_language_assignments sla ON s.spot_id = sla.spot_id
WHERE sla.requires_review = 1
GROUP BY DATE(sla.assigned_date), sla.assignment_method
ORDER BY assignment_date DESC

Best Practices for Reporting
1. Always Filter by Business Context

-- For language revenue analysis, use only commercial spots
WHERE s.spot_category = 'language_assignment_required'
AND sla.requires_review = 0

-- High-confidence assignments only
WHERE sla.confidence >= 0.8

-- Exclude spots needing review from KPIs
WHERE sla.requires_review = 0

JOIN languages l ON UPPER(sla.language_code) = UPPER(l.language_code)

-- Different logic for different assignment methods
CASE 
    WHEN sla.assignment_method = 'business_rule_default_english' THEN 'Auto-English'
    WHEN sla.assignment_method = 'direct_mapping' THEN 'Language-Targeted'
    WHEN sla.assignment_method = 'business_review_required' THEN 'Needs Review'
    ELSE 'Other'
END as assignment_type

This system provides complete language assignment coverage across all spot inventory while maintaining clear business logic separation and data quality through confidence scoring and review flagging.
