# GUIDE-ASSIGNMENT-SYSTEM.md
# Assignment Automation, Business Rules & Programming Analytics

**Version:** 7.1  
**Last Updated:** 2025-07-17  
**Target Audience:** LLMs, Developers, Business Stakeholders  
**Status:** Production-Ready with Enhanced Language Family Classification

---

## 💼 Business Overview

**What This System Does:** Automatically assigns radio advertising spots to language programming blocks based on time slots and business rules. When an advertiser buys a spot at 4:00 PM, the system determines whether it should run during Tagalog, Mandarin, or other language programming blocks.

**Why It Matters:** 
- **Revenue Optimization:** Ensures spots reach the intended language audience
- **Operational Efficiency:** Eliminates manual assignment of 150,000+ spots annually
- **Accuracy:** 99%+ correct assignments through intelligent pattern recognition
- **Categories:** Individual Language Blocks (70-75%), ROSs (15-20%), Multi-Language (2-4%), Direct Response (3-5%)

---

## 🎯 Technical Overview

Enhanced Assignment System achieving 99%+ accuracy through language family analysis and proper time format handling.

### **Core Components**
- **Input:** Spots table with market_id, air_date, day_of_week, time_in, time_out
- **Process:** Match spots to language_blocks based on time overlap and business rules
- **Output:** spot_language_blocks table with campaign_type classification
- **Database:** SQLite with foreign key constraints

### **What's New in Version 7.1**
- **Language Family Classification:** Multi-block same-language spots correctly assigned as `language_specific`
- **Time Format Fix:** "1 day, 0:00:00" normalized to "00:00:00" for proper midnight handling
- **Chinese Family Detection:** Enhanced detection for 19:00-00:00 Chinese family spans
- **Database Constraint Compliance:** Proper NULL handling for multi-block assignments

---

## 🔧 **Enhanced Language Classification Logic**

### **Language Family Analysis**
```python
def _analyze_block_languages(self, blocks: List[LanguageBlock]) -> Dict[str, Any]:
    """Analyze languages across multiple blocks"""
    
    unique_languages = set(b.language_id for b in blocks)
    
    # Language families
    language_families = {
        'Chinese': {2, 3},      # Mandarin=2, Cantonese=3
        'Filipino': {4},        # Tagalog=4
        'South Asian': {6},     # South Asian=6
        'English': {1},         # English=1
        'Vietnamese': {7},      # Vietnamese=7
        'Korean': {8},          # Korean=8
        'Japanese': {9},        # Japanese=9
        'Hmong': {5}            # Hmong=5
    }
    
    # Classification logic
    if len(unique_languages) == 1:
        return {'classification': 'same_language', 'expected_campaign_type': 'language_specific'}
    
    for family_name, family_languages in language_families.items():
        if unique_languages.issubset(family_languages):
            return {'classification': 'same_family', 'expected_campaign_type': 'language_specific'}
    
    return {'classification': 'different_families', 'expected_campaign_type': 'multi_language'}
```

### **Time Format Normalization**
```python
def _normalize_time_out(self, time_out: str) -> str:
    """Convert '1 day, 0:00:00' to '00:00:00' for proper comparison"""
    if time_out and "day" in str(time_out) and "0:00:00" in str(time_out):
        return "00:00:00"
    return time_out
```

### **Chinese Family Span Detection**
```python
def _is_chinese_family_span(self, spot: SpotData, blocks: List[LanguageBlock]) -> bool:
    """Detect Chinese family spans (19:00-00:00)"""
    normalized_time_out = self._normalize_time_out(spot.time_out)
    time_pattern_match = (spot.time_in == "19:00:00" and normalized_time_out == "00:00:00")
    
    if not time_pattern_match:
        return False
    
    block_languages = set(b.language_id for b in blocks)
    chinese_languages = {2, 3}  # Mandarin + Cantonese
    
    return bool(block_languages & chinese_languages)
```

---

## 📊 **Assignment Decision Tree**

### **Multi-Block Assignment Logic**
```
IF multiple blocks overlap:
    IF same_language → campaign_type = 'language_specific'
    IF same_family → campaign_type = 'language_specific' 
    IF different_families:
        IF duration >= 1020min OR blocks >= 15 → campaign_type = 'ros'
        ELSE → campaign_type = 'multi_language'
```

### **Database Constraints**
```
IF spans_multiple_blocks = True → block_id MUST be NULL
IF spans_multiple_blocks = False → block_id MUST NOT be NULL
```

---

## 🚀 **Precedence Rules (Production)**

### **Rule Application Order**
1. **WorldLink Direct Response** → `campaign_type = 'direct_response'`
2. **Paid Programming** → `campaign_type = 'paid_programming'`
3. **ROS by Duration** (>360 min, excluding Chinese/Tagalog patterns) → `campaign_type = 'ros'`
4. **ROS by Time Pattern** → `campaign_type = 'ros'`
5. **Standard Language Block Assignment** → Apply language family analysis

### **Enhanced Pattern Detection**
- **Chinese Pattern:** 19:00:00 or 20:00:00 start + ends at 23:59:00 or midnight + language code M/C/M/C
- **Tagalog Pattern:** 16:00:00-19:00:00 or 17:00:00-19:00:00 + language code T
- **Chinese Family Span:** 19:00:00-00:00:00 covering Mandarin/Cantonese blocks

---

## 📈 **CLI Usage**

```bash
# Test assignment
python cli_01_assign_language_blocks.py --test 100

# Assign by year
python cli_01_assign_language_blocks.py --all-year 2025

# Force reassignment
python cli_01_assign_language_blocks.py --force-year 2024

# Check status
python cli_01_assign_language_blocks.py --status
```

---

## 🔍 **Revenue Category Mapping**

```sql
SELECT 
    CASE 
        WHEN slb.campaign_type = 'language_specific' THEN 'Individual Language Blocks'
        WHEN slb.campaign_type = 'ros' THEN 'ROSs'
        WHEN slb.campaign_type = 'multi_language' THEN 'Multi-Language (Cross-Audience)'
        WHEN slb.campaign_type = 'direct_response' THEN 'Direct Response'
        WHEN slb.campaign_type = 'paid_programming' THEN 'Paid Programming'
    END as revenue_category,
    COUNT(*) as spots,
    SUM(s.gross_rate) as revenue
FROM spots s
JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
WHERE s.broadcast_month LIKE '%-25'
GROUP BY revenue_category;
```

---

## 🔧 **Key Implementation Details**

### **Midnight Rollover Handling**
```python
def _calculate_spot_duration(self, time_in: str, time_out: str) -> int:
    normalized_time_out = self._normalize_time_out(time_out)
    start_minutes = self._time_to_minutes(time_in)
    
    if normalized_time_out == '00:00:00':
        end_minutes = 1440  # Next day midnight
    else:
        end_minutes = self._time_to_minutes(normalized_time_out)
    
    if end_minutes >= start_minutes:
        return end_minutes - start_minutes
    else:
        return (1440 - start_minutes) + end_minutes
```

### **Primary Block Selection (Chinese)**
```python
def _find_primary_chinese_block(self, blocks: List[LanguageBlock]) -> Optional[LanguageBlock]:
    """Priority: Mandarin Prime > Any Mandarin > Any Cantonese > First block"""
    mandarin_prime = None
    mandarin_blocks = []
    cantonese_blocks = []
    
    for block in blocks:
        if block.language_id == 2:  # Mandarin
            mandarin_blocks.append(block)
            if 'Prime' in block.block_name:
                mandarin_prime = block
        elif block.language_id == 3:  # Cantonese
            cantonese_blocks.append(block)
    
    return mandarin_prime or (mandarin_blocks[0] if mandarin_blocks else None) or \
           (cantonese_blocks[0] if cantonese_blocks else None) or blocks[0]
```

---

## 📊 **Validation Queries**

```sql
-- Check campaign_type distribution
SELECT campaign_type, COUNT(*), SUM(gross_rate)
FROM spots s JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
WHERE s.broadcast_month LIKE '%-25' 
GROUP BY campaign_type;

-- Verify constraint compliance
SELECT COUNT(*) as constraint_violations
FROM spot_language_blocks
WHERE (spans_multiple_blocks = 1 AND block_id IS NOT NULL)
   OR (spans_multiple_blocks = 0 AND block_id IS NULL);
```

---

## 🗄️ **Database Schema Requirements**

```sql
-- Core tables needed
CREATE TABLE spots (
    spot_id INTEGER PRIMARY KEY,
    market_id INTEGER,
    air_date TEXT,
    day_of_week TEXT,
    time_in TEXT,  -- Format: HH:MM:SS
    time_out TEXT, -- Format: HH:MM:SS or "1 day, HH:MM:SS"
    gross_rate REAL,
    broadcast_month TEXT,  -- Format: MMM-YY
    language_code TEXT,    -- Original hint: T, M, C, etc.
    agency_id INTEGER,
    bill_code TEXT,
    revenue_type TEXT
);

CREATE TABLE language_blocks (
    block_id INTEGER PRIMARY KEY,
    schedule_id INTEGER,
    day_of_week TEXT,
    time_start TEXT,
    time_end TEXT,
    language_id INTEGER,
    block_name TEXT,
    block_type TEXT,
    day_part TEXT,
    is_active INTEGER DEFAULT 1
);

CREATE TABLE spot_language_blocks (
    spot_id INTEGER PRIMARY KEY,
    schedule_id INTEGER,
    block_id INTEGER,  -- NULL when spans_multiple_blocks = 1
    customer_intent TEXT,
    spans_multiple_blocks BOOLEAN,
    blocks_spanned TEXT,  -- JSON array of block_ids
    primary_block_id INTEGER,
    campaign_type TEXT,  -- CRITICAL: language_specific, ros, multi_language, etc.
    business_rule_applied TEXT,
    auto_resolved_date TEXT
);
```

---

## 🛠️ **Common Modification Scenarios**

### **Adding New Language Family**
```python
# In _analyze_block_languages(), add to language_families dict:
'NewFamily': {10, 11},  # Add language_ids for the family
```

### **Adding New Business Rule**
```python
# In _apply_precedence_rules(), add before standard assignment:
if self._is_new_pattern(spot):
    return AssignmentResult(
        campaign_type='new_type',
        business_rule_applied='new_pattern_rule'
    )
```

### **Modifying Time Patterns**
```python
# Add to _is_ros_by_time() or create new pattern method:
if spot.time_in == "XX:XX:XX" and spot.time_out == "YY:YY:YY":
    return True
```

---

## 🐛 **Debugging & Error Patterns**

### **Common Issues**
1. **Constraint Violations:** Check spans_multiple_blocks vs block_id NULL state
2. **No Schedule Found:** Market may lack programming_schedules entry
3. **Time Overlap Failures:** Check time format normalization
4. **Pattern Not Applied:** Verify precedence rule order

### **Debug Queries**
```sql
-- Find constraint violations
SELECT * FROM spot_language_blocks 
WHERE (spans_multiple_blocks = 1 AND block_id IS NOT NULL);

-- Check unassigned spots
SELECT s.spot_id, s.time_in, s.time_out, s.market_id
FROM spots s LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
WHERE slb.spot_id IS NULL AND s.broadcast_month LIKE '%-25';

-- Verify business rules
SELECT business_rule_applied, COUNT(*), AVG(gross_rate)
FROM spot_language_blocks slb JOIN spots s ON slb.spot_id = s.spot_id
GROUP BY business_rule_applied;
```

---

## 🧪 **Testing Approach**

### **Unit Test Pattern**
```python
# Test specific assignment logic
spot = SpotData(
    spot_id=12345,
    time_in="19:00:00",
    time_out="1 day, 0:00:00",
    # ... other fields
)
result = service._analyze_base_assignment(spot, schedule_id, blocks)
assert result.campaign_type == 'language_specific'
```

### **Integration Test**
```bash
# Test small batch first
python cli_01_assign_language_blocks.py --test 10

# Verify results
sqlite3 production.db "SELECT campaign_type, COUNT(*) FROM spot_language_blocks GROUP BY campaign_type"
```

**Status:** ✅ Production v7.1 - Enhanced Language Family Classification