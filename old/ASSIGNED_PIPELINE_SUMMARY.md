# Assigned Pipeline System Summary

## 🎯 **New Approach: Assigned Monthly Pipeline**

The system has been updated to reflect your current revenue management approach where **each month gets assigned a specific pipeline number** rather than deriving pipeline from budget calculations.

## 📊 **Key Changes**

### **1. Pipeline Calculation Logic**
- **Before**: `Pipeline = Budget - Booked Revenue`
- **After**: `Pipeline = Assigned Monthly Value`
- **Budget Gap**: Calculated separately as `(Booked + Pipeline) - Budget`

### **2. Quarterly Totals**
- **Before**: Derived from quarterly budget calculations
- **After**: **Sum of assigned monthly pipeline values**
- **Example Q2 2025**: April ($0) + May ($0) + June ($89,099) = **$89,099 total**

### **3. Past Months (April/May 2025)**
- **Pipeline**: $0 (closed months, revenue already booked)
- **Budget Gap**: Shows actual performance vs budget
- **Status**: Marked as "CLOSED"

## 🗂️ **Data Structure**

### **Pipeline Data File** (`data/processed/pipeline_data.json`)
```json
{
  "AE001": {
    "monthly_pipeline": {
      "2025-04": {
        "current_pipeline": 0.00,
        "notes": "Closed month - no pipeline"
      },
      "2025-05": {
        "current_pipeline": 0.00,
        "notes": "Closed month - no pipeline"
      },
      "2025-06": {
        "current_pipeline": 89099.00,
        "notes": "Assigned pipeline for June - strong month expected"
      }
    }
  }
}
```

## 🔧 **System Behavior**

### **Monthly Cards**
- **April/May**: Show $0 pipeline, budget gap analysis
- **June+**: Show assigned pipeline values
- **Budget Gap**: Always calculated as `(Booked + Pipeline) - Budget`

### **Quarterly Cards**
- **Q2 2025**: $89,099 pipeline (sum of assigned monthly values)
- **Budget Analysis**: Separate calculation showing performance vs targets
- **Status**: Based on assigned pipeline vs expected pipeline

## 📈 **Current Q2 2025 Example**

| Month | Assigned Pipeline | Booked Revenue | Budget | Budget Gap |
|-------|------------------|----------------|---------|------------|
| April | $0 | $108,418 | $159,740 | -$51,322 |
| May | $0 | $131,474 | $169,251 | -$37,777 |
| June | $89,099 | $148,378 | $184,965 | +$52,512 |
| **Q2 Total** | **$89,099** | **$388,270** | **$513,956** | **-$36,587** |

## ✅ **Benefits**

1. **Flexible Pipeline Management**: Assign specific amounts based on market conditions
2. **Accurate Quarterly Tracking**: Sum of real assigned values, not derived calculations
3. **Separate Budget Analysis**: Budget gap tracked independently
4. **Historical Accuracy**: Past months correctly show $0 pipeline
5. **Review Session Integration**: Pipeline assignments tracked with audit trails

## 🎯 **Usage**

- **Revenue Reviews**: Update assigned pipeline values per month per AE
- **Quarterly Planning**: Set pipeline targets based on market analysis
- **Budget Analysis**: Track performance against budget separately
- **Audit Trail**: All pipeline assignments logged with timestamps and notes 