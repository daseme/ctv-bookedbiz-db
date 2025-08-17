#!/bin/bash

# Quick command to generate and optionally open the service dependency visualization

echo "🚀 Generating service dependency visualization..."
echo ""

# Make the main script executable and run it
chmod +x service_dependency_visualizer.sh
./service_dependency_visualizer.sh

if [ $? -eq 0 ]; then
    echo ""
    echo "🎉 Success! Generated service-dependencies.md"
    echo ""
    echo "📖 View options:"
    echo "  1. Open in VS Code:     code service-dependencies.md"
    echo "  2. Open in browser:     (upload to GitHub/GitLab for best viewing)"
    echo "  3. View with cat:       cat service-dependencies.md"
    echo "  4. View with less:      less service-dependencies.md"
    echo ""
    
    # Offer to open automatically if common tools are available
    if command -v code &> /dev/null; then
        read -p "🤔 Open in VS Code? (y/n): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            code service-dependencies.md
        fi
    elif command -v cat &> /dev/null; then
        read -p "🤔 Display in terminal? (y/n): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            cat service-dependencies.md
        fi
    fi
else
    echo "❌ Failed to generate visualization"
    exit 1
fi