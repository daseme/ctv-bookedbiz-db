#!/bin/bash

# Script to find orphaned/unused service files
# Run this from your project root directory

echo "=== Finding Orphaned Services ==="
echo

# Auto-detect services directory location
SERVICES_DIR=""
if [ -d "services" ]; then
    SERVICES_DIR="services"
elif [ -d "src/services" ]; then
    SERVICES_DIR="src/services"
else
    echo "❌ Error: Could not find services directory!"
    echo "   Looked for: ./services and ./src/services"
    exit 1
fi

echo "📁 Found services directory at: $SERVICES_DIR"
echo

# Get all service files (excluding __init__.py and __pycache__)
SERVICE_FILES=$(find "$SERVICES_DIR" -name "*.py" -not -name "__init__.py" | grep -v __pycache__ | sort)

ORPHANED_SERVICES=()
USED_SERVICES=()

for service_file in $SERVICE_FILES; do
    # Extract service name (remove .py extension and path)
    service_name=$(basename "$service_file" .py)
    
    echo "Checking: $service_name"
    
    # Search for various ways this service might be referenced
    # 1. Direct imports: from services.service_name import ...
    # 2. Module imports: import services.service_name
    # 3. String references (for dynamic imports)
    # 4. Class name references (assuming service follows naming convention)
    
    # Convert snake_case to potential CamelCase class name
    class_name=$(echo "$service_name" | sed 's/_/ /g' | sed 's/\b\(.\)/\u\1/g' | sed 's/ //g')
    
    found=0
    
    # Search patterns (excluding the service file itself)
    # Handle both direct services/ and src/services/ structures
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
    
    for pattern in "${patterns[@]}"; do
        # Search entire codebase except the service file itself and __pycache__
        if grep -r --include="*.py" --exclude-dir=__pycache__ --exclude="$service_file" -q "$pattern" .; then
            found=1
            break
        fi
    done
    
    if [ $found -eq 0 ]; then
        ORPHANED_SERVICES+=("$service_file")
        echo "  ❌ ORPHANED: $service_name"
    else
        USED_SERVICES+=("$service_file")
        echo "  ✅ USED: $service_name"
    fi
    echo
done

echo "=== SUMMARY ==="
echo
echo "📊 Total services analyzed: $(echo "$SERVICE_FILES" | wc -l)"
echo "✅ Used services: ${#USED_SERVICES[@]}"
echo "❌ Potentially orphaned services: ${#ORPHANED_SERVICES[@]}"
echo

if [ ${#ORPHANED_SERVICES[@]} -gt 0 ]; then
    echo "🗑️  POTENTIALLY ORPHANED SERVICES:"
    for service in "${ORPHANED_SERVICES[@]}"; do
        echo "   - $service"
    done
    echo
    echo "⚠️  WARNING: Please manually verify these results before deletion!"
    echo "   Some services might be used in ways not detected by this script."
fi