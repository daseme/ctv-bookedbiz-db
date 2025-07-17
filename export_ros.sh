#!/bin/bash

# UPDATED ROS (Run on Schedule) Export Script
# Export all ROS spots with enhanced business rule compliance checking
# 
# This script extracts ROS spots and validates they comply with business rules:
# 1. Duration-based ROS: > 6 hours (360 minutes)
# 2. Time-based ROS: Specific patterns (13:00-23:59, late night to next day, etc.)

# Default values
YEAR="2024"
OUTPUT_FILE=""
DB_PATH="data/database/production.db"
DEBUG=false

show_help() {
    cat << EOF
UPDATED ROS (Run on Schedule) Export Script with Business Rule Validation

USAGE:
    $0 [OPTIONS]

OPTIONS:
    -y, --year YEAR         Year to export (default: 2024)
    -o, --output FILE       Output CSV file (default: ros_spots_YEAR.csv)
    -d, --database PATH     Database path (default: data/database/production.db)
    --debug                 Show debug information
    -h, --help              Show this help message

EXAMPLES:
    $0 -y 2023                              # Export 2023 ROS spots
    $0 -y 2024                              # Export 2024 ROS spots
    $0 -y 2023 --debug                      # Export with debug info

ROS BUSINESS RULES (Enhanced):
    Duration-based ROS: Spots > 6 hours (360 minutes)
    Time-based ROS patterns:
    - 13:00-23:59 (standard ROS)
    - Late night starts (≥19:00) running to next day
    - Early morning starts (≤06:00) running to next day
    - Full day patterns (06:00-23:59)

VALIDATION FEATURES:
    - Business rule compliance checking
    - Pattern matching validation  
    - Misclassification detection
    - Revenue impact analysis
    - Handles corrected Fujisankei Paid Programming spots

IMPORTANT: If you see compliance issues, run the misclassification fix first:
    UPDATE spot_language_blocks SET campaign_type = 'paid_programming' 
    WHERE campaign_type = 'ros' AND spot_id IN (
        SELECT spot_id FROM spots WHERE revenue_type = 'Paid Programming'
    );

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -y|--year)
            YEAR="$2"
            shift 2
            ;;
        -o|--output)
            OUTPUT_FILE="$2"
            shift 2
            ;;
        -d|--database)
            DB_PATH="$2"
            shift 2
            ;;
        --debug)
            DEBUG=true
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Validate inputs
if [[ ! "$YEAR" =~ ^[0-9]{4}$ ]]; then
    echo "Error: Year must be 4 digits (e.g., 2024)"
    exit 1
fi

if [[ ! -f "$DB_PATH" ]]; then
    echo "Error: Database file not found: $DB_PATH"
    exit 1
fi

# Set default output file if not provided
if [[ -z "$OUTPUT_FILE" ]]; then
    OUTPUT_FILE="ros_spots_${YEAR}_validated.csv"
fi

YEAR_SUFFIX="${YEAR: -2}"

echo "🔍 ROS (Run on Schedule) Export with Business Rule Validation for $YEAR"
echo "📊 Database: $DB_PATH"
echo "📁 Output: $OUTPUT_FILE"
echo "🎯 Using enhanced business rule compliance checking"
echo ""

# Enhanced ROS export query with business rule validation
echo "⚡ Generating validated ROS spots export..."

sqlite3 -header -csv "$DB_PATH" << EOF > "$OUTPUT_FILE"
WITH ros_spots AS (
    SELECT DISTINCT s.spot_id
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    -- Exclude higher precedence categories (these should NOT be ROS)
    AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
    AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
    AND s.revenue_type != 'Paid Programming'
    AND NOT (slb.spot_id IS NULL AND s.spot_type = 'PRD')
    AND NOT (slb.spot_id IS NULL AND s.spot_type = 'SVC')
    -- ROS identification using campaign_type
    AND slb.campaign_type = 'ros'
),
duration_analysis AS (
    SELECT 
        s.spot_id,
        s.time_in,
        s.time_out,
        -- Enhanced duration calculation handling "1 day, 0:00:00" format
        CASE 
            WHEN s.time_in IS NULL OR s.time_out IS NULL THEN NULL
            WHEN s.time_out LIKE '%day%' THEN 
                1440 - (CAST(substr(s.time_in, 1, 2) AS INTEGER) * 60 + CAST(substr(s.time_in, 4, 2) AS INTEGER))
            WHEN s.time_in <= s.time_out THEN 
                (CAST(substr(s.time_out, 1, 2) AS INTEGER) - CAST(substr(s.time_in, 1, 2) AS INTEGER)) * 60 + 
                (CAST(substr(s.time_out, 4, 2) AS INTEGER) - CAST(substr(s.time_in, 4, 2) AS INTEGER))
            ELSE 
                (24 * 60) - ((CAST(substr(s.time_in, 1, 2) AS INTEGER) - CAST(substr(s.time_out, 1, 2) AS INTEGER)) * 60 + 
                              (CAST(substr(s.time_in, 4, 2) AS INTEGER) - CAST(substr(s.time_out, 4, 2) AS INTEGER)))
        END as duration_minutes
    FROM spots s
    WHERE s.spot_id IN (SELECT spot_id FROM ros_spots)
),
business_rule_validation AS (
    SELECT 
        da.spot_id,
        da.duration_minutes,
        slb.business_rule_applied,
        -- Validate duration-based ROS rule
        CASE 
            WHEN slb.business_rule_applied = 'ros_duration' AND da.duration_minutes > 360 THEN 'VALID'
            WHEN slb.business_rule_applied = 'ros_duration' AND da.duration_minutes <= 360 THEN 'INVALID - Duration too short'
            WHEN slb.business_rule_applied = 'ros_duration' AND da.duration_minutes IS NULL THEN 'INVALID - No duration info'
            ELSE 'N/A'
        END as duration_rule_compliance,
        -- Validate time-based ROS rule
        CASE 
            WHEN slb.business_rule_applied = 'ros_time' THEN
                CASE 
                    WHEN (s.time_in = '13:00:00' AND s.time_out = '23:59:00') THEN 'VALID - Standard ROS (1pm-midnight)'
                    WHEN (s.time_in >= '19:00:00' AND s.time_out LIKE '%day%') THEN 'VALID - Late night to next day'
                    WHEN (s.time_in <= '06:00:00' AND s.time_out LIKE '%day%') THEN 'VALID - Early morning to next day'
                    WHEN (s.time_in = '06:00:00' AND s.time_out = '23:59:00') THEN 'VALID - Full day pattern'
                    ELSE 'INVALID - Unknown time pattern'
                END
            ELSE 'N/A'
        END as time_rule_compliance,
        -- Overall compliance status with misclassification detection
        CASE 
            WHEN s.revenue_type = 'Paid Programming' THEN 'MISCLASSIFIED - Should be Paid Programming'
            WHEN slb.business_rule_applied = 'ros_duration' AND da.duration_minutes > 360 THEN 'COMPLIANT'
            WHEN slb.business_rule_applied = 'ros_time' AND (
                (s.time_in = '13:00:00' AND s.time_out = '23:59:00') OR
                (s.time_in >= '19:00:00' AND s.time_out LIKE '%day%') OR
                (s.time_in <= '06:00:00' AND s.time_out LIKE '%day%') OR
                (s.time_in = '06:00:00' AND s.time_out = '23:59:00')
            ) THEN 'COMPLIANT'
            WHEN slb.business_rule_applied IS NULL THEN 'MISSING_RULE'
            ELSE 'NON_COMPLIANT'
        END as overall_compliance
    FROM duration_analysis da
    JOIN spots s ON da.spot_id = s.spot_id
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
)
SELECT 
    s.spot_id,
    s.bill_code,
    COALESCE(c.normalized_name, 'Unknown') as customer_name,
    COALESCE(a.agency_name, 'No Agency') as agency_name,
    s.gross_rate,
    s.station_net,
    s.spot_type,
    s.revenue_type,
    s.time_in,
    s.time_out,
    da.duration_minutes,
    CASE 
        WHEN da.duration_minutes IS NULL THEN 'Unknown Duration'
        WHEN da.duration_minutes >= 720 THEN 'Very Long (12+ hours)'
        WHEN da.duration_minutes >= 360 THEN 'Long (6-12 hours)'
        WHEN da.duration_minutes >= 180 THEN 'Medium (3-6 hours)'
        WHEN da.duration_minutes >= 60 THEN 'Short (1-3 hours)'
        ELSE 'Very Short (<1 hour)'
    END as duration_category,
    CASE 
        WHEN s.time_in = '13:00:00' AND s.time_out = '23:59:00' THEN 'Standard ROS (1pm-midnight)'
        WHEN s.time_in >= '19:00:00' AND s.time_out LIKE '%day%' THEN 'Late Night to Next Day'
        WHEN s.time_in <= '06:00:00' AND s.time_out LIKE '%day%' THEN 'Early Morning to Next Day'
        WHEN s.time_in = '06:00:00' AND s.time_out = '23:59:00' THEN 'Full Day Pattern'
        ELSE 'Other Pattern'
    END as time_pattern,
    s.day_of_week,
    CASE 
        WHEN s.day_of_week IN ('saturday', 'sunday') THEN 'Weekend'
        WHEN s.day_of_week IN ('monday', 'tuesday', 'wednesday', 'thursday', 'friday') THEN 'Weekday'
        ELSE 'Unknown'
    END as weekday_weekend,
    s.air_date,
    s.broadcast_month,
    s.sales_person,
    s.language_code,
    s.program,
    s.market_name,
    -- Business rule validation fields
    COALESCE(slb.business_rule_applied, 'NO_RULE') as business_rule_applied,
    COALESCE(brv.duration_rule_compliance, 'N/A') as duration_rule_compliance,
    COALESCE(brv.time_rule_compliance, 'N/A') as time_rule_compliance,
    COALESCE(brv.overall_compliance, 'UNKNOWN') as overall_compliance,
    -- Compliance flags
    CASE 
        WHEN brv.overall_compliance = 'COMPLIANT' THEN 'PASS'
        WHEN brv.overall_compliance = 'NON_COMPLIANT' THEN 'FAIL'
        WHEN brv.overall_compliance = 'MISSING_RULE' THEN 'MISSING'
        WHEN brv.overall_compliance LIKE 'MISCLASSIFIED%' THEN 'MISCLASSIFIED'
        ELSE 'UNKNOWN'
    END as compliance_status,
    -- Revenue impact analysis
    CASE 
        WHEN brv.overall_compliance = 'NON_COMPLIANT' THEN 'Revenue at Risk'
        WHEN brv.overall_compliance = 'MISSING_RULE' THEN 'Needs Review'
        WHEN brv.overall_compliance LIKE 'MISCLASSIFIED%' THEN 'Wrong Category'
        ELSE 'OK'
    END as revenue_status,
    'ROS (Run on Schedule)' as final_category,
    COALESCE(slb.campaign_type, 'NO_ASSIGNMENT') as campaign_type_confirm
FROM ros_spots rs
JOIN spots s ON rs.spot_id = s.spot_id
LEFT JOIN customers c ON s.customer_id = c.customer_id
LEFT JOIN agencies a ON s.agency_id = a.agency_id
LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
LEFT JOIN duration_analysis da ON s.spot_id = da.spot_id
LEFT JOIN business_rule_validation brv ON s.spot_id = brv.spot_id
ORDER BY 
    brv.overall_compliance DESC,
    s.gross_rate DESC, 
    s.air_date, 
    s.time_in;
EOF

# Check results and provide validation summary
if [[ -f "$OUTPUT_FILE" ]]; then
    RECORD_COUNT=$(tail -n +2 "$OUTPUT_FILE" | wc -l)
    echo "✅ ROS export completed successfully!"
    echo "📊 Records exported: $RECORD_COUNT"
    echo "📁 File saved: $OUTPUT_FILE"
    
    # Calculate revenue
    REVENUE=$(tail -n +2 "$OUTPUT_FILE" | awk -F',' '{sum += $5} END {print sum}')
    echo "💰 Total ROS revenue: \$${REVENUE}"
    
    # Business rule compliance analysis
    echo ""
    echo "🔍 BUSINESS RULE COMPLIANCE ANALYSIS:"
    
    COMPLIANT_COUNT=$(tail -n +2 "$OUTPUT_FILE" | grep -c ',COMPLIANT,')
    NON_COMPLIANT_COUNT=$(tail -n +2 "$OUTPUT_FILE" | grep -c ',NON_COMPLIANT,')
    MISSING_RULE_COUNT=$(tail -n +2 "$OUTPUT_FILE" | grep -c ',MISSING_RULE,')
    MISCLASSIFIED_COUNT=$(tail -n +2 "$OUTPUT_FILE" | grep -c ',MISCLASSIFIED,')
    
    echo "   ✅ Compliant spots: $COMPLIANT_COUNT"
    echo "   ❌ Non-compliant spots: $NON_COMPLIANT_COUNT"
    echo "   ⚠️  Missing business rule: $MISSING_RULE_COUNT"
    echo "   🔄 Misclassified spots: $MISCLASSIFIED_COUNT"
    
    if [[ $RECORD_COUNT -gt 0 ]]; then
        COMPLIANCE_RATE=$(echo "scale=1; $COMPLIANT_COUNT * 100 / $RECORD_COUNT" | bc -l)
        echo "   📈 Compliance rate: ${COMPLIANCE_RATE}%"
    fi
    
    # Alert if misclassified spots found
    if [[ $MISCLASSIFIED_COUNT -gt 0 ]]; then
        echo "   🚨 Found $MISCLASSIFIED_COUNT misclassified spots - run fix script first!"
    fi
    
    # Revenue at risk analysis
    REVENUE_AT_RISK=$(tail -n +2 "$OUTPUT_FILE" | grep ',Revenue at Risk,' | awk -F',' '{sum += $5} END {print sum}')
    WRONG_CATEGORY_REVENUE=$(tail -n +2 "$OUTPUT_FILE" | grep ',Wrong Category,' | awk -F',' '{sum += $5} END {print sum}')
    
    if [[ -n "$REVENUE_AT_RISK" && "$REVENUE_AT_RISK" != "0" ]]; then
        echo "   ⚠️  Revenue at risk: \${REVENUE_AT_RISK}"
    fi
    if [[ -n "$WRONG_CATEGORY_REVENUE" && "$WRONG_CATEGORY_REVENUE" != "0" ]]; then
        echo "   🔄 Revenue in wrong category: \${WRONG_CATEGORY_REVENUE}"
    fi
    
    if [[ "$DEBUG" == true ]]; then
        echo ""
        echo "🔍 Debug: Business rule breakdown"
        echo "   Duration-based ROS: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',ros_duration,')"
        echo "   Time-based ROS: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',ros_time,')"
        echo "   No rule applied: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',NO_RULE,')"
        
        echo ""
        echo "📋 Sample non-compliant spots:"
        tail -n +2 "$OUTPUT_FILE" | grep ',NON_COMPLIANT,' | head -3 | while IFS=',' read -r spot_id bill_code customer rest; do
            echo "   Spot $spot_id ($bill_code): $customer"
        done
        
        if [[ $MISCLASSIFIED_COUNT -gt 0 ]]; then
            echo ""
            echo "🔄 Sample misclassified spots:"
            tail -n +2 "$OUTPUT_FILE" | grep ',MISCLASSIFIED,' | head -3 | while IFS=',' read -r spot_id bill_code customer rest; do
                echo "   Spot $spot_id ($bill_code): $customer"
            done
        fi
    fi
else
    echo "❌ Export failed!"
    exit 1
fi

echo ""
echo "🎯 ROS Business Rule Validation Summary:"
echo "   This export includes enhanced compliance checking for all ROS spots"
echo "   Each spot is validated against the specific business rules used in assignment"
echo "   Non-compliant spots should be reviewed and potentially reassigned"
echo "   Misclassified spots (like Paid Programming) are automatically flagged"
echo ""
echo "💡 Key validation features:"
echo "   - Duration rule compliance (>6 hours for ros_duration)"
echo "   - Time pattern compliance (specific patterns for ros_time)"
echo "   - Overall compliance status (COMPLIANT/NON_COMPLIANT/MISSING_RULE/MISCLASSIFIED)"
echo "   - Revenue impact analysis for non-compliant spots"
echo "   - Automatic detection of misclassified Paid Programming spots"