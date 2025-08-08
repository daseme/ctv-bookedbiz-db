#!/bin/bash

# Generate a simplified Mermaid diagram that won't exceed size limits
OUTPUT_FILE="service-dependencies-simple.md"

# Auto-detect services directory
SERVICES_DIR=""
if [ -d "services" ]; then
    SERVICES_DIR="services"
elif [ -d "src/services" ]; then
    SERVICES_DIR="src/services"
else
    echo "❌ Error: Could not find services directory!"
    exit 1
fi

echo "🎨 Generating simplified visualization..."

# Get service files
SERVICE_FILES=$(find "$SERVICES_DIR" -name "*.py" -not -name "__init__.py" | grep -v __pycache__ | sort)

declare -A USED_SERVICES
declare -A SERVICE_DEPENDENCIES
ORPHANED_SERVICES=()

# Quick analysis
for service_file in $SERVICE_FILES; do
    service_name=$(basename "$service_file" .py)
    dependent_files=()
    
    # Simple search for service usage
    matching_files=$(grep -r --include="*.py" --exclude-dir=__pycache__ --exclude="$service_file" -l "$service_name" . 2>/dev/null)
    if [ -n "$matching_files" ]; then
        while IFS= read -r file; do
            dependent_files+=("$file")
        done <<< "$matching_files"
    fi
    
    if [ ${#dependent_files[@]} -eq 0 ]; then
        ORPHANED_SERVICES+=("$service_file")
    else
        USED_SERVICES["$service_name"]="$service_file"
        SERVICE_DEPENDENCIES["$service_name"]=${#dependent_files[@]}
    fi
done

# Generate simplified markdown
cat > "$OUTPUT_FILE" << 'EOF'
# Simplified Service Dependencies

## 🎯 Quick Actions

### 🗑️ Safe to Delete (Orphaned Services)
```bash
# These services appear to be unused:
EOF

for service in "${ORPHANED_SERVICES[@]}"; do
    echo "rm $service  # $(basename "$service" .py)" >> "$OUTPUT_FILE"
done

cat >> "$OUTPUT_FILE" << 'EOF'
```

### 🔥 Most Connected Services
EOF

# Sort services by dependency count
sorted_services=()
for service_name in "${!USED_SERVICES[@]}"; do
    count=${SERVICE_DEPENDENCIES[$service_name]}
    sorted_services+=("$count:$service_name")
done

# Top 10 most used
top_services=$(printf '%s\n' "${sorted_services[@]}" | sort -nr | head -10)

echo "" >> "$OUTPUT_FILE"
echo "| Service | Dependencies | Status |" >> "$OUTPUT_FILE"
echo "|---------|-------------|---------|" >> "$OUTPUT_FILE"

while IFS= read -r line; do
    count=$(echo "$line" | cut -d':' -f1)
    name=$(echo "$line" | cut -d':' -f2)
    if [ "$count" -ge 5 ]; then
        status="🔥 Heavily Used"
    elif [ "$count" -ge 3 ]; then
        status="⚡ Well Used"
    else
        status="✅ Used"
    fi
    echo "| \`$name\` | $count files | $status |" >> "$OUTPUT_FILE"
done <<< "$top_services"

# Create simplified Mermaid for top services only
cat >> "$OUTPUT_FILE" << 'EOF'

## 🗺️ Core Service Dependencies (Top 8)

```mermaid
graph TD
    subgraph "🔥 Core Services"
EOF

# Add only top 8 services to avoid size limits
top_8=$(printf '%s\n' "${sorted_services[@]}" | sort -nr | head -8)

while IFS= read -r line; do
    count=$(echo "$line" | cut -d':' -f1)
    name=$(echo "$line" | cut -d':' -f2)
    
    echo "        $name[\"📦 $name<br/>($count deps)\"]" >> "$OUTPUT_FILE"
    
    # Add some example connections (first 3 files that use this service)
    example_files=$(grep -r --include="*.py" --exclude-dir=__pycache__ --exclude="${USED_SERVICES[$name]}" -l "$name" . 2>/dev/null | head -3)
    if [ -n "$example_files" ]; then
        while IFS= read -r file; do
            clean_name=$(basename "$file" .py | sed 's/[^a-zA-Z0-9]/_/g')
            echo "        $clean_name[\"📄 $(basename "$file")\"] --> $name" >> "$OUTPUT_FILE"
        done <<< "$example_files"
    fi
done <<< "$top_8"

cat >> "$OUTPUT_FILE" << 'EOF'
    end
    
    subgraph "💀 Orphaned Services"
EOF

# Add orphaned services
for service in "${ORPHANED_SERVICES[@]}"; do
    service_name=$(basename "$service" .py)
    echo "        ${service_name}_orphan[\"💀 $service_name\"]" >> "$OUTPUT_FILE"
done

cat >> "$OUTPUT_FILE" << 'EOF'
    end

    classDef coreService fill:#e3f2fd,stroke:#1976d2,stroke-width:2px
    classDef fileNode fill:#f3e5f5,stroke:#7b1fa2,stroke-width:1px
    classDef orphanService fill:#ffebee,stroke:#d32f2f,stroke-width:2px,stroke-dasharray: 5 5
```

## 📋 Complete Service List

### ✅ Used Services
EOF

for service_name in $(printf '%s\n' "${!USED_SERVICES[@]}" | sort); do
    count=${SERVICE_DEPENDENCIES[$service_name]}
    echo "- 📦 **$service_name** ($count dependencies)" >> "$OUTPUT_FILE"
done

echo "" >> "$OUTPUT_FILE"
echo "### ❌ Orphaned Services" >> "$OUTPUT_FILE"
for service in "${ORPHANED_SERVICES[@]}"; do
    service_name=$(basename "$service" .py)
    echo "- 💀 **$service_name** (\`$service\`)" >> "$OUTPUT_FILE"
done

cat >> "$OUTPUT_FILE" << 'EOF'

## 🛠️ Cleanup Commands

```bash
# Review orphaned services before deletion:
EOF

for service in "${ORPHANED_SERVICES[@]}"; do
    service_name=$(basename "$service" .py)
    echo "git log --oneline $service  # Check recent changes to $service_name" >> "$OUTPUT_FILE"
done

cat >> "$OUTPUT_FILE" << 'EOF'

# If confirmed unused, delete them:
EOF

for service in "${ORPHANED_SERVICES[@]}"; do
    echo "rm $service" >> "$OUTPUT_FILE"
done

echo '```' >> "$OUTPUT_FILE"

echo "✅ Generated $OUTPUT_FILE"
echo "📊 Summary:"
echo "   • Used services: ${#USED_SERVICES[@]}"
echo "   • Orphaned services: ${#ORPHANED_SERVICES[@]}"
echo "   • Top service has ${SERVICE_DEPENDENCIES[$(printf '%s\n' "${sorted_services[@]}" | sort -nr | head -1 | cut -d':' -f2)]} dependencies"