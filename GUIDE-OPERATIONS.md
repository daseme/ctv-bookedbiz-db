# GUIDE-OPERATIONS.md
# Critical Operational Procedures & System Management

**Version:** 2.0  
**Last Updated:** 2025-07-15  
**Target Audience:** LLMs, Operations Teams, System Administrators  
**Status:** Production-Critical Reference

---

## 🚨 **CRITICAL: Database Lock Management**

### **The #1 Operational Issue**
**Problem:** Stage 2 can appear "stuck" when it's actually waiting for database access  
**Root Cause:** Datasette, web applications, and other processes create read locks that prevent write operations  
**Impact:** Process appears hung for hours when it's actually waiting for database access

### **Pre-Pipeline Checklist (MANDATORY)**
Before running ANY large pipeline operations:

```bash
# 1. Stop Datasette
kill $(pgrep -f datasette)

# 2. Check active connections
lsof | grep production.db

# 3. Kill competing processes
ps aux | grep -E "(datasette|sqlite3|uvicorn)" | grep -v grep

# 4. Kill any competing processes found
pkill -f datasette
pkill -f sqlite3
pkill -f uvicorn

# 5. Verify exclusive access
lsof | grep production.db
# Should return EMPTY or only show your pipeline process
```

### **Lock Detection Commands**
```bash
# Check if database is locked
lsof | grep production.db

# Find all processes using the database
ps aux | grep -E "(datasette|sqlite3|uvicorn)"

# Check for SQLite processes specifically
ps aux | grep sqlite3

# Find processes by port (if Datasette runs on specific port)
lsof -i :8001
```

### **Recovery from Stuck Processes**
If any pipeline stage appears stuck:

1. **Check CPU usage:**
   ```bash
   top -p $(pgrep -f cli_02_assign_business_rules)
   ```

2. **Check database locks:**
   ```bash
   lsof | grep production.db
   ```

3. **If locks found - Kill competing processes:**
   ```bash
   pkill -f datasette
   pkill -f sqlite3
   pkill -f uvicorn
   ```

4. **Restart the stuck process:**
   - Process should immediately resume if it was waiting for database access
   - If still stuck, kill and restart the pipeline stage

---

## 📊 **Enhanced Progress Tracking (MANDATORY)**

### **All Pipeline Stages Must Include:**
- **Real-time progress bars:** `tqdm` with ETA and rate display
- **Batch-level logging:** Every 1000 spots processed
- **Memory monitoring:** Track system resource usage
- **Heartbeat logging:** Periodic "still alive" indicators

### **Expected Progress Output Format:**
```
🔧 Stage 2: 🚀 Starting business rules assignment
🔧 Stage 2: 📊 Found 1,847 spots to process
🔧 Business Rules: 54%|████████████▌      | 1.00k/1.85k spots [02:15<01:53, 7.4spots/s]
🔧 Stage 2: 📦 Processed 1,000/1,847 spots (54.1%) in 135.2s (7.4 spots/s)
🔧 Stage 2: ✅ Assignment complete in 248.7s!
```

### **Red Flags (Process Actually Stuck):**
- **0% CPU usage** for >5 minutes
- **No log output** for >10 minutes
- **Progress bar frozen** at same percentage
- **Multiple database lock processes** in `lsof | grep production.db`

---

## 🔍 **Systematic Debugging Procedures**

### **Stage 2 Troubleshooting Checklist**
Execute in order when issues occur:

1. **Check process status:**
   ```bash
   ps aux | grep cli_02_assign_business_rules
   ```

2. **Check CPU usage:**
   ```bash
   top -p $(pgrep -f cli_02_assign_business_rules)
   ```

3. **Check database locks:**
   ```bash
   lsof | grep production.db
   ```

4. **Check progress logs:**
   ```bash
   tail -f /path/to/logfile.log
   ```

5. **Check memory usage:**
   ```bash
   free -h
   ps aux --sort=-%mem | head -10
   ```

### **Common Issues & Solutions**

#### **Issue:** Process appears stuck
**Solution:** Check database locks, kill competing processes
```bash
lsof | grep production.db
pkill -f datasette
```

#### **Issue:** "too many values to unpack" error
**Solution:** Add defensive tuple unpacking with length checks
```python
# Instead of: a, b = some_function()
# Use: result = some_function()
# if len(result) == 2: a, b = result
```

#### **Issue:** No progress indicators
**Solution:** Add enhanced logging and progress bars
```python
from tqdm import tqdm
for item in tqdm(items, desc="Processing"):
    # process item
```

#### **Issue:** Revenue not appearing in reports
**Solution:** Check Direct Response extraction logic
```bash
python cli_language_monthly_report.py 2024 | grep "Direct Response"
```

---

## 📡 **Pipeline Monitoring (MANDATORY)**

### **Pre-Pipeline Monitoring Setup**
Before running large operations:

1. **Clear database locks:** Stop competing processes
2. **Enable progress tracking:** Ensure all stages have progress indicators
3. **Set up monitoring:** Terminal with `top` and `lsof` commands ready
4. **Plan duration:** Stage 1 ~5-10 minutes, Stage 2 ~5-10 minutes

### **During Pipeline Monitoring**
Every 10 minutes, check:

```bash
# Progress indicators
tail -f logfile.log

# CPU usage
top -p $(pgrep -f your_pipeline_process)

# Memory usage
free -h

# Database locks
lsof | grep production.db
```

### **Success Indicators**
- **Stage 1:** 85-95% assignment rate, 0 errors
- **Stage 2:** Steady progress, <1% errors
- **Overall:** Complete within expected timeframe

---

## 📈 **Realistic Success Metrics**

### **Assignment Coverage Expectations**
- **85-95% coverage:** Excellent performance (typical target)
- **95-99% coverage:** Outstanding performance
- **99%+ coverage:** Exceptional performance (not always achievable)

### **Stage-Specific Success Rates**

**Stage 1 (Language Block Assignment):**
- **Target:** 85-95% of spots assigned
- **Typical Result:** 85-95% assignment rate
- **Success Example:** 220,828 out of 221,245 spots (99.8%) ✅

**Stage 2 (Business Rules):**
- **Target:** 5-15% additional automation
- **Typical Result:** Processes remaining unassigned spots
- **Success Example:** 417 remaining spots processed by business rules

### **What 5-15% Remaining Spots Means**
- **Normal:** 5-15% of spots require manual review
- **Expected:** Edge cases that don't fit standard patterns
- **Not a failure:** System working as designed

---

## 🔧 **Database Format Dependencies**

### **broadcast_month Format Standardization**
**Old Format:** Mixed formats (YYYY-MM-DD, YYYY-MM-DD HH:MM:SS, mmm-yy)  
**New Format:** Standardized mmm-yy (Jan-24, Feb-25, etc.)  
**Impact:** All code using broadcast_month must be updated

### **Code Update Requirements**
When broadcast_month format changes:

1. **Update all LIKE patterns:**
   ```sql
   -- Old: WHERE broadcast_month LIKE '2024%'
   -- New: WHERE broadcast_month LIKE '%-24'
   ```

2. **Update date parsing:** Remove substr() extractions
3. **Update display logic:** No more date conversion needed
4. **Test thoroughly:** Verify all queries work with new format

### **Format Validation Query**
```sql
-- Verify current format
SELECT DISTINCT broadcast_month FROM spots ORDER BY broadcast_month DESC LIMIT 5;
-- Expected: Jul-25, Jun-25, May-25, etc.
```

---

## 🎯 **Performance Benchmarks**

### **Expected Processing Times**
- **Stage 1 (Language Assignment):** 5-10 minutes per 100K spots
- **Stage 2 (Business Rules):** 5-10 minutes per 100K spots
- **Database operations:** <1 second per 1K records
- **Export operations:** 2-5 minutes per 100K spots

### **Memory Usage Expectations**
- **Normal operation:** <2GB RAM usage
- **Large datasets:** <8GB RAM usage
- **Memory leak indicators:** Continuous growth >10GB

### **CPU Usage Patterns**
- **Active processing:** 80-100% CPU usage
- **Database waiting:** 5-15% CPU usage
- **Stuck process:** 0% CPU usage for >5 minutes

---

## 🚨 **Emergency Procedures**

### **Pipeline Failure Recovery**
1. **Identify failure point:** Check logs for last successful operation
2. **Clear database locks:** Kill competing processes
3. **Restart from checkpoint:** Use appropriate resume flags
4. **Monitor progress:** Ensure recovery is working

### **Data Corruption Detection**
```sql
-- Check for constraint violations
PRAGMA integrity_check;

-- Verify spot counts
SELECT COUNT(*) FROM spots WHERE broadcast_month LIKE '%-24';

-- Check assignment consistency
SELECT COUNT(*) FROM spot_language_blocks WHERE block_id IS NULL;
```

### **Rollback Procedures**
```bash
# Backup current state
cp production.db production_backup_$(date +%Y%m%d_%H%M%S).db

# Restore from backup
cp production_backup_YYYYMMDD_HHMMSS.db production.db

# Verify restoration
sqlite3 production.db "SELECT COUNT(*) FROM spots;"
```

---

## 🔍 **Diagnostic Commands**

### **Database Health Check**
```bash
# Database file integrity
sqlite3 production.db "PRAGMA integrity_check;"

# Table structure verification
sqlite3 production.db ".schema spots"

# Index usage analysis
sqlite3 production.db ".indexes"
```

### **Process Monitoring**
```bash
# Long-running processes
ps aux --sort=-time | head -10

# Memory usage by process
ps aux --sort=-%mem | head -10

# Database connections
lsof | grep production.db

# Network connections (if applicable)
netstat -tulpn | grep :8001
```

### **System Resource Monitoring**
```bash
# Disk usage
df -h

# Memory usage
free -h

# CPU usage
top -n 1 | head -15

# I/O statistics
iostat -x 1 1
```

---

## 📋 **Operational Checklist**

### **Daily Operations**
- [ ] Check database locks before starting work
- [ ] Verify no stuck processes from previous day
- [ ] Monitor system resources (CPU, memory, disk)
- [ ] Check log files for errors or warnings

### **Weekly Operations**
- [ ] Review performance metrics
- [ ] Check for database growth patterns
- [ ] Verify backup procedures
- [ ] Update documentation with any new issues

### **Monthly Operations**
- [ ] Performance benchmark comparison
- [ ] Database maintenance and optimization
- [ ] Review and update operational procedures
- [ ] Training updates for new operational patterns

---

## 🎯 **Success Criteria**

### **Operational Health Indicators**
- ✅ **Database locks:** Zero competing processes during operations
- ✅ **Progress tracking:** All stages show real-time progress
- ✅ **Error rates:** <1% of operations encounter errors
- ✅ **Performance:** Operations complete within expected timeframes
- ✅ **Recovery:** Issues resolved within 15 minutes

### **System Reliability Metrics**
- **Uptime:** >99% operational availability
- **Data integrity:** Zero data corruption incidents
- **Performance:** Consistent processing speeds
- **Monitoring:** Real-time visibility into all operations

---

**Status:** ✅ Production-Critical Reference  
**Next Review:** Monthly operational review  
**Owner:** Operations Team & System Administrators