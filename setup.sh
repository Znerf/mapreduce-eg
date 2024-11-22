#!/bin/bash

# Define source and destination
SOURCE_FILE="source.list"
DEST_FILE="/etc/apt/sources.list"

# Copy source.list to /etc/apt/sources.list
if [[ -f "$SOURCE_FILE" ]]; then
    echo "Copying $SOURCE_FILE to $DEST_FILE..."
    cp "$SOURCE_FILE" "$DEST_FILE"
else
    echo "Error: $SOURCE_FILE does not exist."
    exit 1
fi

# Update package lists
echo "Updating package lists..."
apt-get update

# Upgrade installed packages
echo "Upgrading installed packages..."
apt-get upgrade -y

# Install Python3
echo "Installing Python3..."
apt-get install -y python3

echo "All tasks completed successfully!"
