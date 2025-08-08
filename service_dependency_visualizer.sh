#!/bin/bash

# Script to generate a markdown visualization of service dependencies
# Run this from your project root directory

OUTPUT_FILE="service-dependencies.md"

# Auto-detect services directory location
SERVICES_DIR=""
if [ -d "services" ]; then
    SERVICES_DIR="services"
elif [ -d "src/services" ]; then
    SERVICES_DIR="src/services"
else
    echo "❌ Error: Could not find services directory!"
    exit 1
fi

echo "🔍 Analyzing services in $SERVICES_DIR..."
echo "📝 Generating $OUTPUT_FILE..."

# Initialize markdown file
cat > "$OUTPUT_FILE" << 'EOF'
# Service Dependency Analysis

> Generated on: 
EOF
echo "$(date)" >> "$OUTPUT_FILE"
cat >> "$OUTPUT_FILE" << 'EOF'

## 📊 Overview

EOF

# Get all service files
SERVICE_FILES=$(find "$SERVICES_DIR" -name "*.py" -not -name "__init__.py" | grep -v __pycache__ | sort)

declare -A USED_SERVICES
declare -A SERVICE_DEPENDENCIES
declare -A SERVICE_USAGE_DETAILS
ORPHANED_SERVICES=()

# Analyze each service
for service_file in $SERVICE_FILES; do
    service_name=$(basename "$service_file" .py)
    echo "Analyzing $service_name..."
    
    class_name=$(echo "$service_name" | sed 's/_/ /g' | sed 's/\b\(.\)/\u\1/g' | sed 's/ //g')
    
    dependent_files=()
    usage_details=""
    
    # Search patterns
    patterns=(
        "from services.$service_name import"
        "import services.$service_name"
        "from services import.*$service_name"
        "services\.$service_name"
        "from src\.services.$service_name import"
        "import src\.services.$service_name" 
        "from src\.services import.*$service_name"
        "src\.services\.$service_name"
        "$class_name"
        "\"$service_name\""
        "'$service_name'"
    )
    
    # Collect dependencies and usage details
    for pattern in "${patterns[@]}"; do
        matching_lines=$(grep -rn --include="*.py" --exclude-dir=__pycache__ --exclude="$service_file" "$pattern" . 2>/dev/null)
        if [ -n "$matching_lines" ]; then
            while IFS= read -r line; do
                file=$(echo "$line" | cut -d':' -f1)
                line_num=$(echo "$line" | cut -d':' -f2)
                content=$(echo "$line" | cut -d':' -f3-)
                
                if [[ ! " ${dependent_files[@]} " =~ " $file " ]]; then
                    dependent_files+=("$file")
                fi
                
                usage_details+="- \`$file:$line_num\`: \`$content\`"$'\n'
            done <<< "$matching_lines"
        fi
    done
    
    if [ ${#dependent_files[@]} -eq 0 ]; then
        ORPHANED_SERVICES+=("$service_file")
    else
        USED_SERVICES["$service_name"]="$service_file"
        SERVICE_DEPENDENCIES["$service_name"]=$(IFS='|'; echo "${dependent_files[*]}")
        SERVICE_USAGE_DETAILS["$service_name"]="$usage_details"
    fi
done

# Write overview statistics
cat >> "$OUTPUT_FILE" << EOF
| Metric | Count |
|--------|-------|
| 📦 Total Services | $(echo "$SERVICE_FILES" | wc -l) |
| ✅ Used Services | ${#USED_SERVICES[@]} |
| ❌ Orphaned Services | ${#ORPHANED_SERVICES[@]} |
| 📈 Usage Rate | $((${#USED_SERVICES[@]} * 100 / $(echo "$SERVICE_FILES" | wc -l)))% |

EOF

# Generate Mermaid diagram
echo "## 🗺️ Service Dependency Graph" >> "$OUTPUT_FILE"
echo "" >> "$OUTPUT_FILE"
echo '```mermaid' >> "$OUTPUT_FILE"
echo 'graph TD' >> "$OUTPUT_FILE"

# Add nodes and connections
for service_name in "${!USED_SERVICES[@]}"; do
    IFS='|' read -ra deps <<< "${SERVICE_DEPENDENCIES[$service_name]}"
    
    # Style the service node
    echo "    $service_name[\"📦 $service_name\"]" >> "$OUTPUT_FILE"
    echo "    classDef serviceNode fill:#e1f5fe,stroke:#01579b,stroke-width:2px" >> "$OUTPUT_FILE"
    echo "    class $service_name serviceNode" >> "$OUTPUT_FILE"
    
    # Add connections
    for dep in "${deps[@]}"; do
        dep_clean=$(echo "$dep" | sed 's/[^a-zA-Z0-9_]/_/g')
        echo "    $dep_clean[\"📄 $(basename "$dep")\"] --> $service_name" >> "$OUTPUT_FILE"
        echo "    classDef fileNode fill:#f3e5f5,stroke:#4a148c,stroke-width:1px" >> "$OUTPUT_FILE"
        echo "    class $dep_clean fileNode" >> "$OUTPUT_FILE"
    done
done

# Add orphaned services
for service in "${ORPHANED_SERVICES[@]}"; do
    service_name=$(basename "$service" .py)
    echo "    $service_name[\"💀 $service_name\"]" >> "$OUTPUT_FILE"
    echo "    classDef orphanNode fill:#ffebee,stroke:#d32f2f,stroke-width:2px,stroke-dasharray: 5 5" >> "$OUTPUT_FILE"
    echo "    class $service_name orphanNode" >> "$OUTPUT_FILE"
done

echo '```' >> "$OUTPUT_FILE"
echo "" >> "$OUTPUT_FILE"

# Legend
cat >> "$OUTPUT_FILE" << 'EOF'
### 🎨 Legend
- 📦 **Blue boxes**: Active services (used by other files)
- 📄 **Purple boxes**: Files that depend on services
- 💀 **Red dashed boxes**: Orphaned services (not used anywhere)

EOF

# Used Services Section
if [ ${#USED_SERVICES[@]} -gt 0 ]; then
    echo "## ✅ Used Services" >> "$OUTPUT_FILE"
    echo "" >> "$OUTPUT_FILE"
    
    for service_name in $(printf '%s\n' "${!USED_SERVICES[@]}" | sort); do
        IFS='|' read -ra deps <<< "${SERVICE_DEPENDENCIES[$service_name]}"
        
        echo "### 📦 \`$service_name\`" >> "$OUTPUT_FILE"
        echo "" >> "$OUTPUT_FILE"
        echo "**File:** \`${USED_SERVICES[$service_name]}\`" >> "$OUTPUT_FILE"
        echo "" >> "$OUTPUT_FILE"
        echo "**Used by ${#deps[@]} file(s):**" >> "$OUTPUT_FILE"
        echo "" >> "$OUTPUT_FILE"
        
        for dep in "${deps[@]}"; do
            echo "- 📄 \`$dep\`" >> "$OUTPUT_FILE"
        done
        
        echo "" >> "$OUTPUT_FILE"
        echo "<details>" >> "$OUTPUT_FILE"
        echo "<summary>👁️ View usage details</summary>" >> "$OUTPUT_FILE"
        echo "" >> "$OUTPUT_FILE"
        echo "${SERVICE_USAGE_DETAILS[$service_name]}" >> "$OUTPUT_FILE"
        echo "</details>" >> "$OUTPUT_FILE"
        echo "" >> "$OUTPUT_FILE"
        echo "---" >> "$OUTPUT_FILE"
        echo "" >> "$OUTPUT_FILE"
    done
fi

# Orphaned Services Section
if [ ${#ORPHANED_SERVICES[@]} -gt 0 ]; then
    echo "## ❌ Orphaned Services" >> "$OUTPUT_FILE"
    echo "" >> "$OUTPUT_FILE"
    echo "> ⚠️ **Warning**: Please manually verify these results before deletion!" >> "$OUTPUT_FILE"
    echo "> Some services might be used in ways not detected by this script." >> "$OUTPUT_FILE"
    echo "" >> "$OUTPUT_FILE"
    
    for service in "${ORPHANED_SERVICES[@]}"; do
        service_name=$(basename "$service" .py)
        echo "- 💀 \`$service\`" >> "$OUTPUT_FILE"
    done
    echo "" >> "$OUTPUT_FILE"
fi

# Dependency Matrix
echo "## 📋 Dependency Matrix" >> "$OUTPUT_FILE"
echo "" >> "$OUTPUT_FILE"
echo "| Service | Dependencies | Status |" >> "$OUTPUT_FILE"
echo "|---------|-------------|--------|" >> "$OUTPUT_FILE"

for service_file in $SERVICE_FILES; do
    service_name=$(basename "$service_file" .py)
    if [[ -n "${USED_SERVICES[$service_name]}" ]]; then
        IFS='|' read -ra deps <<< "${SERVICE_DEPENDENCIES[$service_name]}"
        dep_count=${#deps[@]}
        echo "| \`$service_name\` | $dep_count files | ✅ Used |" >> "$OUTPUT_FILE"
    else
        echo "| \`$service_name\` | 0 files | ❌ Orphaned |" >> "$OUTPUT_FILE"
    fi
done

echo "" >> "$OUTPUT_FILE"

# Cleanup suggestions
echo "## 🧹 Cleanup Suggestions" >> "$OUTPUT_FILE"
echo "" >> "$OUTPUT_FILE"

if [ ${#ORPHANED_SERVICES[@]} -gt 0 ]; then
    echo "### Services to Review for Deletion" >> "$OUTPUT_FILE"
    echo "" >> "$OUTPUT_FILE"
    echo '```bash' >> "$OUTPUT_FILE"
    echo "# Review these potentially unused services:" >> "$OUTPUT_FILE"
    for service in "${ORPHANED_SERVICES[@]}"; do
        echo "# $service" >> "$OUTPUT_FILE"
    done
    echo '```' >> "$OUTPUT_FILE"
    echo "" >> "$OUTPUT_FILE"
fi

# Most/least used services
echo "### 📊 Usage Statistics" >> "$OUTPUT_FILE"
echo "" >> "$OUTPUT_FILE"

# Sort services by dependency count
declare -a sorted_services
for service_name in "${!USED_SERVICES[@]}"; do
    IFS='|' read -ra deps <<< "${SERVICE_DEPENDENCIES[$service_name]}"
    sorted_services+=("${#deps[@]}:$service_name")
done

if [ ${#sorted_services[@]} -gt 0 ]; then
    # Most used
    most_used=$(printf '%s\n' "${sorted_services[@]}" | sort -nr | head -3)
    echo "**Most Used Services:**" >> "$OUTPUT_FILE"
    while IFS= read -r line; do
        count=$(echo "$line" | cut -d':' -f1)
        name=$(echo "$line" | cut -d':' -f2)
        echo "- 🔥 \`$name\` ($count dependencies)" >> "$OUTPUT_FILE"
    done <<< "$most_used"
    
    echo "" >> "$OUTPUT_FILE"
    
    # Least used
    least_used=$(printf '%s\n' "${sorted_services[@]}" | sort -n | head -3)
    echo "**Least Used Services:**" >> "$OUTPUT_FILE"
    while IFS= read -r line; do
        count=$(echo "$line" | cut -d':' -f1)
        name=$(echo "$line" | cut -d':' -f2)
        echo "- 📉 \`$name\` ($count dependencies)" >> "$OUTPUT_FILE"
    done <<< "$least_used"
fi

echo "" >> "$OUTPUT_FILE"
echo "---" >> "$OUTPUT_FILE"
echo "*Generated by Service Dependency Analyzer*" >> "$OUTPUT_FILE"

echo "✅ Generated $OUTPUT_FILE"
echo "🔍 Total services analyzed: $(echo "$SERVICE_FILES" | wc -l)"
echo "✅ Used services: ${#USED_SERVICES[@]}"
echo "❌ Orphaned services: ${#ORPHANED_SERVICES[@]}"
echo ""
echo "📖 Open $OUTPUT_FILE in your favorite markdown viewer to see the visual dependency graph!"