# GUIDE-ASSIGNMENT-SYSTEM.md
# Assignment Automation, Business Rules & Programming Analytics

**Version:** 6.0  
**Last Updated:** 2025-07-15  
**Target Audience:** LLMs, Developers, Business Intelligence Teams  
**Status:** Production-Ready with Fixed Classification Logic

---

## 🎯 **Overview**

This guide documents the **Assignment Automation System** with **fixed classification logic** - a comprehensive system that delivers both assignment efficiency and programming analytics through the `campaign_type` field methodology.

### **What's New in Version 6.0**
- **Fixed Classification Logic:** Uses `campaign_type` field instead of legacy `spans_multiple_blocks` logic
- **ROS Terminology:** Consistent Run-of-Schedule terminology throughout
- **Packages Integration:** Support for PKG spots without time targeting
- **Programming Analytics:** Comprehensive content composition analysis
- **Perfect Integration:** Seamless integration with revenue analysis system

---

## 🔧 **CRITICAL: Fixed Classification Logic**

### **The Major Fix**
**Problem:** Legacy classification logic using `spans_multiple_blocks` and `business_rule_applied` caused massive misclassification  
**Solution:** Updated to use structured `campaign_type` field for accurate categorization  
**Impact:** Other Non-Language reduced from 5.7% to <1% of revenue

### **Classification Logic Updates**

| Category | ❌ Old Logic | ✅ New Logic | Impact |
|----------|-------------|-------------|--------|
| **Individual Language** | `spans_multiple_blocks = 0 AND block_id IS NOT NULL` | `campaign_type = 'language_specific'` | Captures language-targeted spots correctly |
| **ROS (Run on Schedule)** | `business_rule_applied IN ('ros_duration', 'ros_time')` | `campaign_type = 'ros'` | Properly identifies broadcast sponsorships |
| **Multi-Language** | `campaign_type = 'multi_language'` | `campaign_type = 'multi_language'` | Was already correct ✅ |
| **Other Non-Language** | Catch-all with legacy exclusions | True miscellaneous content only | Reduced to <1% of spots |

### **Expected Results After Fix**

**Individual Language Blocks:**
- **Before Fix:** 65.2% of revenue (thousands misclassified as Other Non-Language)
- **After Fix:** 72.4% of revenue (proper language targeting captured)

**Other Non-Language:**
- **Before Fix:** 5.7% of revenue (6,401 spots misclassified)
- **After Fix:** <1% of revenue (~59 spots of true miscellaneous content)

**ROS (Run on Schedule):**
- **Before Fix:** Inconsistent classification using business_rule_applied
- **After Fix:** Accurate classification using campaign_type = 'ros'

---

## 📊 **Business Rules Framework**

### **Core Business Rules (Updated)**

#### **Rule 0: Direct Response Agency Exclusion (HIGHEST PRIORITY)**
- **Scope:** All spots from direct response agencies (WorldLink, etc.)
- **Logic:** Direct response agencies target broad audiences, not specific language blocks
- **Result:** Prevents ~$XXX,XXX from being miscategorized as language block revenue
- **Campaign Type:** N/A (excluded from language assignment)

#### **Rule 1: Enhanced Pattern Recognition**
- **Tagalog Pattern:** 16:00-19:00 with language hint "T" → Tagalog Block
- **Chinese Pattern:** 19:00-23:59 with language hint "M" or "M/C" → Chinese Block  
- **ROS Duration:** > 4 hours duration → ROS (`campaign_type = 'ros'`)
- **ROS Time:** 13:00-23:59 time slot → ROS (`campaign_type = 'ros'`)

#### **Rule 2: Media Sector Broad Reach**
- **Scope:** MEDIA sector spots (all content types) - EXCLUDING direct response agencies
- **Logic:** Broad-reach campaigns require multi-language coverage
- **Result:** Majority of MEDIA sector automated assignments
- **Campaign Type:** `campaign_type = 'multi_language'`

#### **Rule 3: Nonprofit Awareness (Extended Duration)**
- **Scope:** NPO sector spots with 5+ hours duration - EXCLUDING direct response agencies
- **Logic:** Long-form awareness campaigns span multiple blocks
- **Result:** Extended NPO content automated assignments
- **Campaign Type:** `campaign_type = 'multi_language'`

#### **Rule 4: Extended Content Blocks**
- **Scope:** Any content type with 12+ hour duration - EXCLUDING direct response agencies
- **Logic:** Extended content inherently crosses multiple programming blocks
- **Result:** Long-form content automated assignments
- **Campaign Type:** `campaign_type = 'multi_language'`

#### **Rule 5: Government Public Service**
- **Scope:** Government sector spots (all content types) - EXCLUDING direct response agencies
- **Logic:** Public service content requires community-wide reach
- **Result:** Government content automated assignments
- **Campaign Type:** `campaign_type = 'multi_language'`

#### **Rule 6: Customer Intent Assignment Logic**
- **Single Block Overlap:** Always assign to that language block (`campaign_type = 'language_specific'`)
- **Multi-Block with Language Match:** Assign to matching language block (`campaign_type = 'language_specific'`)
- **Multi-Block Time-Specific:** Assign to primary block based on time overlap (`campaign_type = 'language_specific'`)
- **Multi-Block Indifferent:** Assign as ROS (`campaign_type = 'ros'`) or Multi-Language (`campaign_type = 'multi_language'`)

---

## 🎬 **Programming Analytics Capabilities**

### **Content Mix Analysis**
The system enables comprehensive programming composition analysis:

```sql
-- Example: Language block composition analysis
SELECT 
    l.language_name,
    COUNT(*) as total_spots,
    COUNT(CASE WHEN s.spot_type = 'COM' THEN 1 END) as commercial_spots,
    COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) as bonus_spots,
    ROUND(COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) * 100.0 / COUNT(*), 1) as bonus_percentage,
    ROUND(AVG(s.gross_rate), 2) as avg_revenue_per_spot
FROM spots s
JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
JOIN language_blocks lb ON slb.block_id = lb.block_id
JOIN languages l ON lb.language_id = l.language_id
WHERE s.broadcast_month LIKE '%-YY'
AND slb.campaign_type = 'language_specific'
GROUP BY l.language_name
ORDER BY total_spots DESC;
```

### **Revenue Density Insights**
```sql
-- Programming performance analysis
SELECT 
    l.language_name,
    lb.day_part,
    COUNT(*) as total_spots,
    ROUND(AVG(s.gross_rate), 2) as avg_revenue_per_spot,
    SUM(s.gross_rate) as total_revenue,
    ROUND(COUNT(CASE WHEN s.spot_type = 'BNS' THEN 1 END) * 100.0 / COUNT(*), 1) as bonus_percentage
FROM spots s
JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
JOIN language_blocks lb ON slb.block_id = lb.block_id
JOIN languages l ON lb.language_id = l.language_id
WHERE s.broadcast_month LIKE '%-YY'
AND slb.campaign_type = 'language_specific'
GROUP BY l.language_name, lb.day_part
ORDER BY avg_revenue_per_spot DESC;
```

### **Campaign Type Distribution**
```sql
-- Assignment system effectiveness
SELECT 
    slb.campaign_type,
    COUNT(*) as spots,
    SUM(s.gross_rate) as revenue,
    ROUND(AVG(s.gross_rate), 2) as avg_rate,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM spots WHERE broadcast_month LIKE '%-YY'), 2) as percentage
FROM spots s
JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
WHERE s.broadcast_month LIKE '%-YY'
GROUP BY slb.campaign_type
ORDER BY spots DESC;
```

---

## 🚀 **Assignment Automation Performance**

### **Realistic Success Metrics**
- **85-95% assignment coverage:** Excellent performance (typical target)
- **95-99% assignment coverage:** Outstanding performance
- **99%+ assignment coverage:** Exceptional performance (not always achievable)

### **Stage-Specific Performance**

**Stage 1 (Language Block Assignment):**
- **Target:** 85-95% of eligible spots assigned
- **Typical Result:** 85-95% assignment rate
- **Success Example:** XX,XXX out of XX,XXX spots assigned

**Stage 2 (Business Rules Enhancement):**
- **Target:** 5-15% additional automation of remaining spots
- **Typical Result:** Processes remaining unassigned spots
- **Success Example:** XXX remaining spots processed by business rules

### **What 5-15% Remaining Spots Means**
- **Normal:** 5-15% of spots require manual review
- **Expected:** Edge cases that don't fit standard patterns
- **Not a failure:** System working as designed
- **Manual Review:** High-value spots requiring human judgment

---

## 🔍 **Classification Troubleshooting**

### **Large Other Non-Language Category**
If Other Non-Language is unexpectedly large (>1% of spots):

#### **Step 1: Check campaign_type Field Population**
```sql
-- Verify campaign_type field is populated
SELECT 
    campaign_type,
    COUNT(*) as spots,
    SUM(s.gross_rate) as revenue
FROM spots s
JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
WHERE s.broadcast_month LIKE '%-YY'
GROUP BY campaign_type
ORDER BY spots DESC;
```

#### **Step 2: Look for Missing campaign_type**
```sql
-- Find spots with missing campaign_type
SELECT COUNT(*) as missing_campaign_type
FROM spots s
LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
WHERE s.broadcast_month LIKE '%-YY' 
AND slb.campaign_type IS NULL;
```

#### **Step 3: Check for Outdated Logic**
```bash
# Ensure export scripts use campaign_type, not spans_multiple_blocks
grep -r "spans_multiple_blocks" src/
# Should return minimal or no results

# Check for business_rule_applied usage in classification
grep -r "business_rule_applied.*IN.*ros" src/
# Should return minimal or no results
```

#### **Step 4: Reprocess Year if Needed**
```bash
# Force reprocessing with updated classification logic
python cli_01_assign_language_blocks.py --force-year YYYY
```

### **Individual Language Percentage Too Low**
If Individual Language is <65% of revenue:

#### **Verify campaign_type Usage**
```sql
-- Check Individual Language classification
SELECT 
    COUNT(*) as language_specific_spots,
    SUM(s.gross_rate) as revenue,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM spots WHERE broadcast_month LIKE '%-YY'), 2) as percentage
FROM spots s
JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
WHERE s.broadcast_month LIKE '%-YY'
AND slb.campaign_type = 'language_specific';
```

#### **Check for Legacy Logic Usage**
```sql
-- Verify not using old spans_multiple_blocks logic
SELECT COUNT(*) as legacy_logic_spots
FROM spots s
JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
WHERE s.broadcast_month LIKE '%-YY'
AND slb.spans_multiple_blocks = 0 
AND slb.block_id IS NOT NULL
AND slb.campaign_type != 'language_specific';
```

### **ROS Classification Issues**
If ROS spots are being misclassified:

#### **Verify ROS campaign_type Usage**
```sql
-- Check ROS classification
SELECT 
    COUNT(*) as ros_spots,
    SUM(s.gross_rate) as revenue
FROM spots s
JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
WHERE s.broadcast_month LIKE '%-YY'
AND slb.campaign_type = 'ros';
```

#### **Check for Legacy business_rule_applied Logic**
```sql
-- Verify not using old business_rule_applied logic
SELECT COUNT(*) as legacy_ros_spots
FROM spots s
JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
WHERE s.broadcast_month LIKE '%-YY'
AND slb.business_rule_applied IN ('ros_duration', 'ros_time')
AND slb.campaign_type != 'ros';
```

---

## 📈 **Programming Intelligence Dashboard**

### **System Overview**
```bash
# Comprehensive system performance summary
python3 programming_intelligence_dashboard.py --overview

# Expected Output:
# 🎯 ASSIGNMENT SYSTEM PERFORMANCE
# Total Spots Analyzed: XXX,XXX
# Assignment Coverage: XX.X% (XXX,XXX spots)
# Business Rule Automation: XX.X% (XXX,XXX spots)
# Languages Covered: XX
# Programming Blocks: XXX
# Campaign Types: language_specific, ros, multi_language
```

### **Programming Composition Analysis**
```bash
# Analyze content mix for specific language
python3 programming_intelligence_dashboard.py --composition --language Vietnamese

# Expected Output:
# 📺 PROGRAMMING COMPOSITION - Vietnamese
# 🎬 Vietnamese - Evening News
#    Time: Monday 18:00-19:00 (Prime)
#    Total Spots: XXX (campaign_type: language_specific)
#    Content Mix: XXX Commercial (XX%), XXX Bonus (XX%)
#    Revenue: $XX.XX/spot average, $XXX,XXX total
```

### **Revenue Density Analysis**
```bash
# Analyze revenue patterns across programming
python3 programming_intelligence_dashboard.py --revenue

# Expected Output:
# 💰 REVENUE DENSITY BY CAMPAIGN TYPE
# 📈 TOP PERFORMING SEGMENTS:
#   • Language-Specific: $XX.XX/spot average, XX% of total revenue
#   • ROS: $XX.XX/spot average, XX% of total revenue
#   • Multi-Language: $XX.XX/spot average, XX% of total revenue
```

### **Assignment Performance Metrics**
```bash
# Show assignment system effectiveness
python3 programming_intelligence_dashboard.py --assignment-metrics

# Expected Output:
# 🔧 ASSIGNMENT SYSTEM METRICS
# 📊 Campaign Type Distribution:
#   • language_specific: XXX,XXX spots (XX.X%)
#   • ros: XX,XXX spots (XX.X%)
#   • multi_language: XX,XXX spots (XX.X%)
# 🎯 Assignment Coverage: XX.X% automated
# 🔍 Manual Review Required: XXX spots (XX.X%)
```

---

## 🎯 **Integration with Revenue Analysis**

### **Perfect Integration Architecture**
The assignment system seamlessly integrates with revenue analysis:

```python
# Example integration query
def get_revenue_by_campaign_type(year_suffix: str):
    """Get revenue breakdown by campaign_type assignment"""
    query = """
    SELECT 
        slb.campaign_type,
        COUNT(*) as spots,
        SUM(s.gross_rate) as revenue,
        ROUND(AVG(s.gross_rate), 2) as avg_rate
    FROM spots s
    JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    WHERE s.broadcast_month LIKE ?
    AND slb.campaign_type IS NOT NULL
    GROUP BY slb.campaign_type
    ORDER BY SUM(s.gross_rate) DESC
    """
    # Integration with revenue categories ensures perfect reconciliation
```

### **Revenue Category Mapping**
```sql
-- How assignment system feeds revenue analysis
SELECT 
    CASE 
        WHEN slb.campaign_type = 'language_specific' THEN 'Individual Language Blocks'
        WHEN slb.campaign_type = 'ros' THEN 'ROSs'
        WHEN slb.campaign_type = 'multi_language' THEN 'Multi-Language (Cross-Audience)'
        ELSE 'Other Categories'
    END as revenue_category,
    COUNT(*) as spots,
    SUM(s.gross_rate) as revenue
FROM spots s
JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
WHERE s.broadcast_month LIKE '%-YY'
GROUP BY revenue_category
ORDER BY revenue DESC;
```

---

## 🔧 **Technical Implementation**

### **Core Assignment Service**
```python
# Updated AssignmentResult with campaign_type
@dataclass
class AssignmentResult:
    customer_intent: str
    assignment_rationale: str
    campaign_type: str  # NEW: 'language_specific', 'ros', 'multi_language'
    business_rule_applied: Optional[str] = None
    auto_resolved_date: Optional[datetime] = None
```

### **Enhanced Business Rules Application**
```python
def _apply_enhanced_business_rules(self, spot_data, language_blocks):
    """Apply enhanced business rules with campaign_type classification"""
    
    # Rule 1: Direct Response Exclusion (highest priority)
    if self._is_direct_response_agency(spot_data):
        return None  # Exclude from language assignment
    
    # Rule 2: Enhanced Pattern Recognition
    if self._matches_tagalog_pattern(spot_data):
        return AssignmentResult(
            customer_intent="language_specific",
            assignment_rationale="tagalog_pattern",
            campaign_type="language_specific",
            business_rule_applied="tagalog_pattern"
        )
    
    # Rule 3: ROS Duration/Time Detection
    if self._is_ros_by_duration(spot_data) or self._is_ros_by_time(spot_data):
        return AssignmentResult(
            customer_intent="indifferent",
            assignment_rationale="ros_pattern",
            campaign_type="ros",
            business_rule_applied="ros_duration" if self._is_ros_by_duration(spot_data) else "ros_time"
        )
    
    # Rule 4: Multi-Language Broad Reach
    if self._requires_multi_language_reach(spot_data):
        return AssignmentResult(
            customer_intent="indifferent",
            assignment_rationale="broad_reach",
            campaign_type="multi_language",
            business_rule_applied="broad_reach"
        )
    
    return None  # No enhanced rule applies
```

### **Campaign Type Population**
```python
def _save_assignment(self, spot_id: int, assignment: AssignmentResult):
    """Save assignment with campaign_type classification"""
    
    query = """
    INSERT OR REPLACE INTO spot_language_blocks 
    (spot_id, block_id, customer_intent, assignment_rationale, 
     campaign_type, business_rule_applied, auto_resolved_date)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    
    self.db_connection.execute(query, (
        spot_id,
        assignment.block_id,
        assignment.customer_intent,
        assignment.assignment_rationale,
        assignment.campaign_type,  # Critical for classification
        assignment.business_rule_applied,
        assignment.auto_resolved_date
    ))
```

---

## 📊 **Validation and Quality Assurance**

### **Assignment Coverage Validation**
```bash
# Daily assignment coverage check
python cli_01_assign_language_blocks.py --validate-coverage --year YYYY

# Expected output:
# ✅ Assignment Coverage: XX.X% (XXX,XXX out of XXX,XXX spots)
# ✅ Campaign Type Population: XX.X% (XXX,XXX spots with campaign_type)
# ✅ Business Rule Applications: XXX spots enhanced
```

### **Classification Accuracy Validation**
```bash
# Weekly classification accuracy check
python cli_classification_validator.py --year YYYY

# Expected output:
# ✅ Individual Language: XX.X% of revenue (target: 65-75%)
# ✅ ROS: XX.X% of revenue (target: 5-8%)
# ✅ Multi-Language: XX.X% of revenue (target: 1-3%)
# ✅ Other Non-Language: XX.X% of revenue (target: <1%)
```

### **Perfect Reconciliation Validation**
```bash
# Monthly reconciliation validation
python src/unified_analysis.py --year YYYY --validate-reconciliation

# Expected output:
# ✅ Perfect Reconciliation: YES (0.000000% error)
# ✅ All categories sum to 100.0%
# ✅ No double-counting detected
# ✅ No missing spots detected
```

---

## 🎯 **Success Metrics**

### **Assignment System Performance**
- ✅ **Assignment Coverage:** 85-95% (Excellent) to 99%+ (Exceptional)
- ✅ **Campaign Type Population:** >99% of assigned spots
- ✅ **Business Rule Enhancement:** 5-15% additional automation
- ✅ **Classification Accuracy:** Individual Language 65-75%, ROS 5-8%

### **Programming Analytics Delivered**
- ✅ **Content Mix Analysis:** Complete composition tracking by language block
- ✅ **Revenue Density Insights:** Performance analysis across programming segments
- ✅ **Strategic Intelligence:** Data-driven programming optimization opportunities
- ✅ **Real-time Monitoring:** Programming Intelligence Dashboard capabilities

### **Integration Quality**
- ✅ **Perfect Reconciliation:** 0.000000% error rate maintained
- ✅ **Revenue System Integration:** Seamless integration with revenue analysis
- ✅ **Export Compatibility:** All export scripts use campaign_type classification
- ✅ **Backward Compatibility:** Legacy systems supported during transition

### **Technical Excellence**
- ✅ **Structured Classification:** campaign_type field over legacy logic
- ✅ **Enhanced Business Rules:** Pattern recognition and automation
- ✅ **Comprehensive Validation:** Coverage, accuracy, and reconciliation checks
- ✅ **Operational Monitoring:** Real-time assignment system performance

---

## 📋 **Migration Checklist**

### **For Systems Using Legacy Classification**

#### **Phase 1: Assessment**
- [ ] Identify systems using `spans_multiple_blocks` logic
- [ ] Identify systems using `business_rule_applied` for classification
- [ ] Assess current Other Non-Language percentage
- [ ] Backup existing data

#### **Phase 2: Update Classification Logic**
- [ ] Update Individual Language query to use `campaign_type = 'language_specific'`
- [ ] Update ROS query to use `campaign_type = 'ros'`
- [ ] Update Multi-Language query to use `campaign_type = 'multi_language'`
- [ ] Update export scripts to use campaign_type logic

#### **Phase 3: Data Reprocessing**
- [ ] Reprocess historical data: `python cli_01_assign_language_blocks.py --force-year YYYY`
- [ ] Validate results with unified analysis
- [ ] Check Other Non-Language reduction
- [ ] Verify perfect reconciliation

#### **Phase 4: Validation**
- [ ] Run comprehensive validation suite
- [ ] Compare before/after metrics
- [ ] Validate export script outputs
- [ ] Update documentation and training

---

**Status:** ✅ Production-Ready with Fixed Classification Logic  
**Classification System:** ✅ campaign_type Field Methodology  
**Assignment Coverage:** ✅ 85-95% Target Range  
**Programming Analytics:** ✅ Comprehensive Intelligence Platform  
**Integration Quality:** ✅ Perfect Reconciliation Maintained  
**Business Rules:** ✅ Enhanced Pattern Recognition Deployed