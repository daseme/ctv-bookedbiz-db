#!/bin/bash

# Package Deals Export Script
# Export all Package spots (PKG type without time targeting)
# 
# This script extracts Package spots that meet the business criteria:
# - spot_type = 'PKG'
# - campaign_type IS NULL (no specific time targeting)
# - Excludes higher precedence categories

# Default values
YEAR="2024"
OUTPUT_FILE=""
DB_PATH="data/database/production.db"
DEBUG=false
CORE_FIELDS=false

show_help() {
    cat << EOF
Package Deals Export Script with Business Rule Validation

USAGE:
    $0 [OPTIONS]

OPTIONS:
    -y, --year YEAR         Year to export (default: 2024)
    -o, --output FILE       Output CSV file (default: package_spots_YEAR.csv)
    -d, --database PATH     Database path (default: data/database/production.db)
    -c, --core-fields       Export core fields only (simplified output)
    --debug                 Show debug information
    -h, --help              Show this help message

EXAMPLES:
    $0 -y 2023                              # Export 2023 package spots
    $0 -y 2024                              # Export 2024 package spots
    $0 -y 2024 -c                           # Export core fields only
    $0 -y 2023 --debug                      # Export with debug info

PACKAGE BUSINESS RULES:
    - spot_type = 'PKG' (Package deals)
    - campaign_type IS NULL (No specific time targeting)
    - Excludes Direct Response (WorldLink)
    - Excludes Paid Programming
    - Excludes PRD and SVC spots

VALIDATION FEATURES:
    - Package deal compliance checking
    - Misclassification detection
    - Revenue impact analysis
    - Campaign type validation

REVENUE CATEGORY CONTEXT:
    Packages represent bundled advertising deals without specific time targeting.
    Expected to be 1-3% of total revenue.

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
        -c|--core-fields)
            CORE_FIELDS=true
            shift
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
    OUTPUT_FILE="package_spots_${YEAR}.csv"
fi

YEAR_SUFFIX="${YEAR: -2}"

echo "📦 Package Deals Export for $YEAR"
echo "📊 Database: $DB_PATH"
echo "📁 Output: $OUTPUT_FILE"
echo "🎯 Extracting PKG spots without time targeting"
echo ""

# Generate the appropriate query based on core fields flag
if [[ "$CORE_FIELDS" == true ]]; then
    echo "⚡ Generating core fields export..."
    
    sqlite3 -header -csv "$DB_PATH" << EOF > "$OUTPUT_FILE"
WITH package_spots AS (
    SELECT DISTINCT s.spot_id
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    -- Exclude higher precedence categories
    AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
    AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
    AND s.revenue_type != 'Paid Programming'
    AND NOT (slb.spot_id IS NULL AND s.spot_type = 'PRD')
    AND NOT (slb.spot_id IS NULL AND s.spot_type = 'SVC')
    -- Package identification
    AND s.spot_type = 'PKG'
    AND (slb.campaign_type IS NULL OR slb.spot_id IS NULL)  -- Package deals without time targeting
)
SELECT 
    s.spot_id,
    s.bill_code,
    COALESCE(c.normalized_name, 'Unknown') as customer_name,
    s.gross_rate,
    s.station_net,
    s.spot_type,
    s.air_date,
    s.broadcast_month,
    'Packages' as final_category
FROM package_spots ps
JOIN spots s ON ps.spot_id = s.spot_id
LEFT JOIN customers c ON s.customer_id = c.customer_id
ORDER BY s.gross_rate DESC, s.air_date;
EOF

else
    echo "⚡ Generating full package spots export with validation..."
    
    sqlite3 -header -csv "$DB_PATH" << EOF > "$OUTPUT_FILE"
WITH package_spots AS (
    SELECT DISTINCT s.spot_id
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    -- Exclude higher precedence categories
    AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
    AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
    AND s.revenue_type != 'Paid Programming'
    AND NOT (slb.spot_id IS NULL AND s.spot_type = 'PRD')
    AND NOT (slb.spot_id IS NULL AND s.spot_type = 'SVC')
    -- Package identification
    AND s.spot_type = 'PKG'
    AND (slb.campaign_type IS NULL OR slb.spot_id IS NULL)  -- Package deals without time targeting
),
validation_analysis AS (
    SELECT 
        s.spot_id,
        -- Validate package deal criteria
        CASE 
            WHEN s.spot_type != 'PKG' THEN 'INVALID - Not PKG type'
            WHEN slb.campaign_type IS NOT NULL THEN 'INVALID - Has time targeting'
            WHEN s.revenue_type = 'Paid Programming' THEN 'MISCLASSIFIED - Should be Paid Programming'
            WHEN a.agency_name LIKE '%WorldLink%' OR s.bill_code LIKE '%WorldLink%' THEN 'MISCLASSIFIED - Should be Direct Response'
            WHEN s.spot_type = 'PRD' THEN 'MISCLASSIFIED - Should be Branded Content'
            WHEN s.spot_type = 'SVC' THEN 'MISCLASSIFIED - Should be Services'
            ELSE 'VALID'
        END as validation_status,
        -- Package characteristics
        CASE 
            WHEN s.time_in IS NOT NULL AND s.time_out IS NOT NULL THEN 'Has Time Info'
            ELSE 'No Time Info'
        END as time_info_status,
        CASE 
            WHEN s.gross_rate >= 1000 THEN 'High Value (≥$1000)'
            WHEN s.gross_rate >= 500 THEN 'Medium Value ($500-999)'
            WHEN s.gross_rate >= 100 THEN 'Low Value ($100-499)'
            ELSE 'Very Low Value (<$100)'
        END as value_category
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    WHERE s.spot_id IN (SELECT spot_id FROM package_spots)
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
    s.length_seconds,
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
    s.contract,
    s.media,
    s.format,
    -- Package characteristics
    va.time_info_status,
    va.value_category,
    CASE 
        WHEN s.length_seconds IS NULL THEN 'Unknown Length'
        WHEN CAST(s.length_seconds AS INTEGER) >= 1800 THEN 'Long Form (30+ min)'
        WHEN CAST(s.length_seconds AS INTEGER) >= 300 THEN 'Extended (5-30 min)'
        WHEN CAST(s.length_seconds AS INTEGER) >= 60 THEN 'Standard (1-5 min)'
        ELSE 'Short (<1 min)'
    END as length_category,
    -- Language block assignment info
    COALESCE(slb.campaign_type, 'NO_CAMPAIGN_TYPE') as campaign_type,
    COALESCE(slb.customer_intent, 'NO_ASSIGNMENT') as customer_intent,
    COALESCE(slb.business_rule_applied, 'NO_RULE') as business_rule_applied,
    -- Validation status
    va.validation_status,
    CASE 
        WHEN va.validation_status = 'VALID' THEN 'PASS'
        WHEN va.validation_status LIKE 'INVALID%' THEN 'FAIL'
        WHEN va.validation_status LIKE 'MISCLASSIFIED%' THEN 'MISCLASSIFIED'
        ELSE 'UNKNOWN'
    END as compliance_status,
    -- Revenue impact
    CASE 
        WHEN va.validation_status LIKE 'INVALID%' THEN 'Revenue at Risk'
        WHEN va.validation_status LIKE 'MISCLASSIFIED%' THEN 'Wrong Category'
        ELSE 'OK'
    END as revenue_status,
    'Packages' as final_category,
    -- Additional package metadata
    CASE 
        WHEN s.gross_rate > 0 THEN 'Paid'
        WHEN s.spot_type = 'BNS' THEN 'Bonus'
        ELSE 'Unknown'
    END as paid_bonus_status,
    CASE 
        WHEN s.contract IS NOT NULL AND s.contract != '' THEN 'Has Contract'
        ELSE 'No Contract'
    END as contract_status
FROM package_spots ps
JOIN spots s ON ps.spot_id = s.spot_id
LEFT JOIN customers c ON s.customer_id = c.customer_id
LEFT JOIN agencies a ON s.agency_id = a.agency_id
LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
LEFT JOIN validation_analysis va ON s.spot_id = va.spot_id
ORDER BY 
    va.validation_status,
    s.gross_rate DESC, 
    s.air_date;
EOF
fi

# Check results and provide summary
if [[ -f "$OUTPUT_FILE" ]]; then
    RECORD_COUNT=$(tail -n +2 "$OUTPUT_FILE" | wc -l)
    echo "✅ Package export completed successfully!"
    echo "📊 Records exported: $RECORD_COUNT"
    echo "📁 File saved: $OUTPUT_FILE"
    
    # Calculate revenue
    if [[ "$CORE_FIELDS" == true ]]; then
        REVENUE=$(tail -n +2 "$OUTPUT_FILE" | awk -F',' '{sum += $4} END {print sum}')
        echo "💰 Total Package revenue: \$${REVENUE}"
    else
        REVENUE=$(tail -n +2 "$OUTPUT_FILE" | awk -F',' '{sum += $5} END {print sum}')
        echo "💰 Total Package revenue: \$${REVENUE}"
        
        # Validation analysis
        echo ""
        echo "🔍 PACKAGE VALIDATION ANALYSIS:"
        
        VALID_COUNT=$(tail -n +2 "$OUTPUT_FILE" | grep -c ',VALID,')
        INVALID_COUNT=$(tail -n +2 "$OUTPUT_FILE" | grep -c ',INVALID')
        MISCLASSIFIED_COUNT=$(tail -n +2 "$OUTPUT_FILE" | grep -c ',MISCLASSIFIED')
        
        echo "   ✅ Valid package spots: $VALID_COUNT"
        echo "   ❌ Invalid spots: $INVALID_COUNT"
        echo "   🔄 Misclassified spots: $MISCLASSIFIED_COUNT"
        
        if [[ $RECORD_COUNT -gt 0 ]]; then
            VALIDITY_RATE=$(echo "scale=1; $VALID_COUNT * 100 / $RECORD_COUNT" | bc -l)
            echo "   📈 Validity rate: ${VALIDITY_RATE}%"
        fi
        
        # Value category breakdown
        echo ""
        echo "📊 VALUE DISTRIBUTION:"
        HIGH_VALUE=$(tail -n +2 "$OUTPUT_FILE" | grep -c 'High Value')
        MEDIUM_VALUE=$(tail -n +2 "$OUTPUT_FILE" | grep -c 'Medium Value')
        LOW_VALUE=$(tail -n +2 "$OUTPUT_FILE" | grep -c 'Low Value')
        VERY_LOW_VALUE=$(tail -n +2 "$OUTPUT_FILE" | grep -c 'Very Low Value')
        
        echo "   💎 High Value (≥\$1000): $HIGH_VALUE spots"
        echo "   💰 Medium Value (\$500-999): $MEDIUM_VALUE spots"
        echo "   💵 Low Value (\$100-499): $LOW_VALUE spots"
        echo "   🪙 Very Low Value (<\$100): $VERY_LOW_VALUE spots"
        
        # Length category analysis
        echo ""
        echo "📏 LENGTH DISTRIBUTION:"
        LONG_FORM=$(tail -n +2 "$OUTPUT_FILE" | grep -c 'Long Form')
        EXTENDED=$(tail -n +2 "$OUTPUT_FILE" | grep -c 'Extended')
        STANDARD=$(tail -n +2 "$OUTPUT_FILE" | grep -c 'Standard')
        SHORT=$(tail -n +2 "$OUTPUT_FILE" | grep -c 'Short')
        
        echo "   📺 Long Form (30+ min): $LONG_FORM spots"
        echo "   🎬 Extended (5-30 min): $EXTENDED spots"
        echo "   📹 Standard (1-5 min): $STANDARD spots"
        echo "   ⚡ Short (<1 min): $SHORT spots"
        
        if [[ "$DEBUG" == true ]]; then
            echo ""
            echo "🔍 Debug: Package characteristics"
            echo "   With time info: $(tail -n +2 "$OUTPUT_FILE" | grep -c 'Has Time Info')"
            echo "   Without time info: $(tail -n +2 "$OUTPUT_FILE" | grep -c 'No Time Info')"
            echo "   With contract: $(tail -n +2 "$OUTPUT_FILE" | grep -c 'Has Contract')"
            echo "   Without contract: $(tail -n +2 "$OUTPUT_FILE" | grep -c 'No Contract')"
            
            echo ""
            echo "📋 Sample package spots:"
            tail -n +2 "$OUTPUT_FILE" | grep ',VALID,' | head -3 | while IFS=',' read -r spot_id bill_code customer rest; do
                echo "   Spot $spot_id ($bill_code): $customer"
            done
            
            if [[ $MISCLASSIFIED_COUNT -gt 0 ]]; then
                echo ""
                echo "🔄 Sample misclassified spots:"
                tail -n +2 "$OUTPUT_FILE" | grep ',MISCLASSIFIED' | head -3 | while IFS=',' read -r fields; do
                    spot_id=$(echo "$fields" | cut -d',' -f1)
                    bill_code=$(echo "$fields" | cut -d',' -f2)
                    customer=$(echo "$fields" | cut -d',' -f3)
                    validation=$(echo "$fields" | grep -o 'MISCLASSIFIED[^,]*')
                    echo "   Spot $spot_id ($bill_code): $customer - $validation"
                done
            fi
        fi
        
        # Revenue at risk analysis
        REVENUE_AT_RISK=$(tail -n +2 "$OUTPUT_FILE" | grep ',Revenue at Risk,' | awk -F',' '{sum += $5} END {print sum}')
        WRONG_CATEGORY_REVENUE=$(tail -n +2 "$OUTPUT_FILE" | grep ',Wrong Category,' | awk -F',' '{sum += $5} END {print sum}')
        
        if [[ -n "$REVENUE_AT_RISK" && "$REVENUE_AT_RISK" != "0" ]]; then
            echo ""
            echo "⚠️  Revenue at risk: \$$REVENUE_AT_RISK"
        fi
        if [[ -n "$WRONG_CATEGORY_REVENUE" && "$WRONG_CATEGORY_REVENUE" != "0" ]]; then
            echo "🔄 Revenue in wrong category: \$$WRONG_CATEGORY_REVENUE"
        fi
    fi
    
    # Top customers analysis
    echo ""
    echo "🏆 TOP PACKAGE CUSTOMERS:"
    if [[ "$CORE_FIELDS" == true ]]; then
        tail -n +2 "$OUTPUT_FILE" | awk -F',' '{customers[$3] += $4} END {for (c in customers) print customers[c] "," c}' | sort -t',' -k1,1nr | head -5 | while IFS=',' read -r revenue customer; do
            echo "   $customer: \$$revenue"
        done
    else
        tail -n +2 "$OUTPUT_FILE" | awk -F',' '{customers[$3] += $5} END {for (c in customers) print customers[c] "," c}' | sort -t',' -k1,1nr | head -5 | while IFS=',' read -r revenue customer; do
            echo "   $customer: \$$revenue"
        done
    fi
    
else
    echo "❌ Export failed!"
    exit 1
fi

echo ""
echo "🎯 Package Export Summary:"
echo "   This export captures all PKG spots without specific time targeting"
echo "   These represent bundled advertising deals (expected 1-3% of revenue)"
echo "   Validation ensures spots are correctly categorized as Packages"
echo ""
echo "💡 Key insights:"
echo "   - PKG spots without campaign_type assignment are true package deals"
echo "   - High-value packages often represent bundled campaign deals"
echo "   - Package deals typically don't have specific time targeting requirements"
echo "   - Misclassified spots should be reviewed and potentially reassigned"