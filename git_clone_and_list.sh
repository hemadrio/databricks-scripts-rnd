#!/bin/bash

# Git Clone and List Files Script
# This script clones a public repository and lists all files inside it
# Usage: ./git_clone_and_list.sh <repository_url> [destination_directory]

set -e  # Exit on any error

# Function to display usage
usage() {
    echo "Usage: $0 <repository_url> [destination_directory]"
    echo "Example: $0 https://github.com/user/repo.git my-repo"
    echo "If destination_directory is not provided, it will use the repo name"
    exit 1
}

# Check if at least one argument is provided
if [ $# -lt 1 ]; then
    echo "Error: Repository URL is required"
    usage
fi

REPO_URL="$1"
DEST_DIR="$2"

# Extract repository name from URL if destination not provided
if [ -z "$DEST_DIR" ]; then
    DEST_DIR=$(basename "$REPO_URL" .git)
fi

echo "========================================="
echo "Git Clone and List Files Script"
echo "========================================="
echo "Repository URL: $REPO_URL"
echo "Destination Directory: $DEST_DIR"
echo "Current Working Directory: $(pwd)"
echo "========================================="

# Remove existing directory if it exists
if [ -d "$DEST_DIR" ]; then
    echo "Warning: Directory '$DEST_DIR' already exists. Removing it..."
    rm -rf "$DEST_DIR"
fi

# Clone the repository
echo "Cloning repository..."
git clone "$REPO_URL" "$DEST_DIR"

if [ $? -eq 0 ]; then
    echo "✅ Repository cloned successfully!"
else
    echo "❌ Failed to clone repository"
    exit 1
fi

# Change to the cloned directory
cd "$DEST_DIR"

echo ""
echo "========================================="
echo "REPOSITORY INFORMATION"
echo "========================================="
echo "Repository path: $(pwd)"
echo "Git remote URL: $(git remote get-url origin)"
echo "Current branch: $(git branch --show-current)"
echo "Latest commit: $(git log -1 --oneline)"

echo ""
echo "========================================="
echo "DIRECTORY STRUCTURE (tree view)"
echo "========================================="
# Check if tree command is available, otherwise use find
if command -v tree >/dev/null 2>&1; then
    tree -a
else
    echo "Tree command not available, using find instead:"
    find . -type f | sort
fi

echo ""
echo "========================================="
echo "ALL FILES LIST (detailed)"
echo "========================================="
ls -la

echo ""
echo "========================================="
echo "FILE COUNT SUMMARY"
echo "========================================="
echo "Total files: $(find . -type f | wc -l)"
echo "Total directories: $(find . -type d | wc -l)"

echo ""
echo "========================================="
echo "FILES BY EXTENSION"
echo "========================================="
find . -type f -name "*.*" | sed 's/.*\.//' | sort | uniq -c | sort -nr

echo ""
echo "========================================="
echo "HIDDEN FILES"
echo "========================================="
find . -name ".*" -type f | head -20

echo ""
echo "========================================="
echo "LARGEST FILES (top 10)"
echo "========================================="
find . -type f -exec ls -lh {} \; | awk '{print $5 " " $9}' | sort -hr | head -10

echo ""
echo "========================================="
echo "README FILES"
echo "========================================="
find . -iname "readme*" -type f

echo ""
echo "========================================="
echo "SCRIPT COMPLETED SUCCESSFULLY"
echo "========================================="
echo "Repository cloned to: $(pwd)"
echo "To navigate to the repository, run: cd $DEST_DIR"
