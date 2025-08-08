#!/bin/bash

# Script to show detailed dependencies for each service with line numbers and context
# Run this from your project root directory

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

echo "=== Detailed Service Dependencies Analysis ==="
echo "📁 Services directory: $SERVICES_DIR"
echo

# Get all service files (excluding __init__.py and __pycache__)
SERVICE_FILES=$(find "$SERVICES_DIR" -name "*.py" -not -name "__init__.py" | grep -v __pycache__ | sort)

for service_file in $SERVICE_FILES; do
    service_name=$(basename "$service_file" .py)
    
    echo "🔍 ==============================================="
    echo "📦 SERVICE: $service_name"
    echo "   File: $service_file"
    echo "🔍 ==============================================="
    
    # Convert snake_case to potential CamelCase class name
    class_name=$(echo "$service_name" | sed 's/_/ /g' | sed 's/\b\(.\)/\u\1/g' | sed 's/ //g')
    
    found_any=false
    
    echo "🔎 Searching for import patterns..."
    
    # Import patterns
    import_patterns=(
        "from services.$service_name import"
        "import services.$service_name"
        "from services import.*$service_name"
        "from src\.services.$service_name import"
        "import src\.services.$service_name"
        "from src\.services import.*$service_name"
    )
    
    for pattern in "${import_patterns[@]}"; do
        results=$(grep -rn --include="*.py" --exclude-dir=__pycache__ --exclude="$service_file" "$pattern" . 2>/dev/null)
        if [ -n "$results" ]; then
            echo "  📥 IMPORTS ($pattern):"
            while IFS= read -r line; do
                echo "     $line"
            done <<< "$results"
            found_any=true
            echo
        fi
    done
    
    echo "🔎 Searching for usage patterns..."
    
    # Usage patterns
    usage_patterns=(
        "services\.$service_name"
        "src\.services\.$service_name"
    )
    
    for pattern in "${usage_patterns[@]}"; do
        results=$(grep -rn --include="*.py" --exclude-dir=__pycache__ --exclude="$service_file" "$pattern" . 2>/dev/null)
        if [ -n "$results" ]; then
            echo "  🔧 USAGE ($pattern):"
            while IFS= read -r line; do
                echo "     $line"
            done <<< "$results"
            found_any=true
            echo
        fi
    done
    
    echo "🔎 Searching for class name references..."
    
    # Class name references
    class_results=$(grep -rn --include="*.py" --exclude-dir=__pycache__ --exclude="$service_file" "$class_name" . 2>/dev/null)
    if [ -n "$class_results" ]; then
        echo "  🏛️  CLASS REFERENCES ($class_name):"
        while IFS= read -r line; do
            echo "     $line"
        done <<< "$class_results"
        found_any=true
        echo
    fi
    
    echo "🔎 Searching for string references..."
    
    # String references (for dynamic imports)
    string_patterns=(
        "\"$service_name\""
        "'$service_name'"
    )
    
    for pattern in "${string_patterns[@]}"; do
        results=$(grep -rn --include="*.py" --exclude-dir=__pycache__ --exclude="$service_file" "$pattern" . 2>/dev/null)
        if [ -n "$results" ]; then
            echo "  💬 STRING REFERENCES ($pattern):"
            while IFS= read -r line; do
                echo "     $line"
            done <<< "$results"
            found_any=true
            echo
        fi
    done
    
    if [ "$found_any" = false ]; then
        echo "  ❌ NO REFERENCES FOUND - POTENTIALLY ORPHANED"
    else
        echo "  ✅ SERVICE IS BEING USED"
    fi
    
    echo
    echo
done