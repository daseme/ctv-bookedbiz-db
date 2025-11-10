#!/bin/bash
# scripts/update_yearly_recap.sh

YEAR=${1:-$(date +%Y)}
SOURCE_PATH="/mnt/k-drive/Sales/Yearly Reports/${YEAR} Cash Revenue Recap.xlsx"
DEST_PATH="data/raw/${YEAR}.xlsx"

echo "Updating ${YEAR} Cash Revenue Recap..."

# Check if K drive is accessible
if [ ! -d "/mnt/k-drive/Sales" ]; then
    echo "Error: K drive not accessible at /mnt/k-drive"
    echo "Try: sudo systemctl start mnt-k\\x2ddrive.mount"
    exit 1
fi

# Check if source file exists
if [ ! -f "$SOURCE_PATH" ]; then
    echo "Error: Source file not found: $SOURCE_PATH"
    exit 1
fi

# Copy the file
cp "$SOURCE_PATH" "$DEST_PATH"
if [ $? -eq 0 ]; then
    echo "Successfully updated: $DEST_PATH"
    ls -la "$DEST_PATH"
else
    echo "Error: Failed to copy file"
    exit 1
fi