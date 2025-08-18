#!/usr/bin/env python3
"""
Git Clone and List Files Script
This script clones a hardcoded public repository and lists all files inside it
Usage: python git_clone_and_list.py
Note: Repository URL is hardcoded in the script - modify repo_url variable to change the target repo
"""

import os
import sys
import subprocess
# import argparse  # Not needed since we're using hardcoded values
from pathlib import Path
import shutil
from collections import defaultdict

def run_command(command, cwd=None):
    """Run a command and return the result"""
    try:
        result = subprocess.run(
            command, 
            shell=True, 
            capture_output=True, 
            text=True, 
            cwd=cwd,
            check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {command}")
        print(f"Error: {e.stderr}")
        return None

def get_file_size_formatted(size_bytes):
    """Convert bytes to human readable format"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} TB"

def get_repository_info(repo_path):
    """Get repository information"""
    info = {}
    info['remote_url'] = run_command("git remote get-url origin", cwd=repo_path)
    info['current_branch'] = run_command("git branch --show-current", cwd=repo_path)
    info['latest_commit'] = run_command("git log -1 --oneline", cwd=repo_path)
    return info

def analyze_files(repo_path):
    """Analyze files in the repository"""
    repo_path = Path(repo_path)
    
    # Count files and directories
    all_files = list(repo_path.rglob('*'))
    files = [f for f in all_files if f.is_file()]
    directories = [f for f in all_files if f.is_dir()]
    
    # Group files by extension
    extensions = defaultdict(int)
    file_sizes = []
    
    for file_path in files:
        # Get extension
        ext = file_path.suffix.lower() if file_path.suffix else 'no_extension'
        extensions[ext] += 1
        
        # Get file size
        try:
            size = file_path.stat().st_size
            file_sizes.append((file_path, size))
        except (OSError, FileNotFoundError):
            continue
    
    # Sort files by size (largest first)
    file_sizes.sort(key=lambda x: x[1], reverse=True)
    
    # Find README files
    readme_files = [f for f in files if 'readme' in f.name.lower()]
    
    # Find hidden files
    hidden_files = [f for f in files if f.name.startswith('.')]
    
    return {
        'total_files': len(files),
        'total_directories': len(directories),
        'extensions': dict(extensions),
        'largest_files': file_sizes[:10],
        'readme_files': readme_files,
        'hidden_files': hidden_files[:20],
        'all_files': files
    }

def print_tree_structure(repo_path, max_depth=3, current_depth=0, prefix=""):
    """Print directory tree structure"""
    if current_depth > max_depth:
        return
    
    repo_path = Path(repo_path)
    items = sorted(repo_path.iterdir(), key=lambda x: (x.is_file(), x.name.lower()))
    
    for i, item in enumerate(items):
        if item.name.startswith('.git'):
            continue
            
        is_last = i == len(items) - 1
        current_prefix = "└── " if is_last else "├── "
        
        print(f"{prefix}{current_prefix}{item.name}")
        
        if item.is_dir() and current_depth < max_depth:
            extension = "    " if is_last else "│   "
            print_tree_structure(item, max_depth, current_depth + 1, prefix + extension)

def main():
    # Hardcoded repository URL - using a smaller repo for faster cloning in Databricks
    # You can change this to any open source repo you want to clone
    repo_url = "https://github.com/octocat/Hello-World.git"  # Small test repo
    # repo_url = "https://github.com/apache/spark.git"  # Uncomment for Apache Spark (large repo)
    max_depth = 3
    
    # Use /tmp directory for Databricks compatibility
    base_dir = "/tmp"
    repo_name = os.path.basename(repo_url.rstrip('/')).replace('.git', '')
    dest_dir = os.path.join(base_dir, repo_name)
    
    # Ensure the base directory exists and is writable
    os.makedirs(base_dir, exist_ok=True)
    
    print("=" * 50)
    print("Git Clone and List Files Script (Python)")
    print("=" * 50)
    print(f"Repository URL: {repo_url}")
    print(f"Destination Directory: {dest_dir}")
    print(f"Current Working Directory: {os.getcwd()}")
    print("=" * 50)
    
    # Remove existing directory if it exists
    if os.path.exists(dest_dir):
        print(f"Warning: Directory '{dest_dir}' already exists. Removing it...")
        try:
            shutil.rmtree(dest_dir)
        except Exception as e:
            print(f"Error removing existing directory: {e}")
            # Try with a different name if removal fails
            import time
            timestamp = int(time.time())
            dest_dir = f"{dest_dir}_{timestamp}"
            print(f"Using alternative directory name: {dest_dir}")
    
    # Clone the repository
    print("Cloning repository...")
    print(f"Clone command: git clone {repo_url} {dest_dir}")
    
    # Ensure git is available
    git_check = run_command("git --version")
    if git_check is None:
        print("❌ Git is not available in this environment")
        sys.exit(1)
    else:
        print(f"Git version: {git_check}")
    
    clone_command = f"git clone --depth 1 {repo_url} {dest_dir}"  # Shallow clone for faster execution
    
    if run_command(clone_command) is not None:
        print("✅ Repository cloned successfully!")
    else:
        print("❌ Failed to clone repository")
        print("This might be due to:")
        print("- Network connectivity issues")
        print("- Git not being available")
        print("- Permission issues with the destination directory")
        print(f"- Issues with the repository URL: {repo_url}")
        sys.exit(1)
    
    # Change to the cloned directory
    repo_path = Path(dest_dir).resolve()
    
    print("\n" + "=" * 50)
    print("REPOSITORY INFORMATION")
    print("=" * 50)
    print(f"Repository path: {repo_path}")
    
    repo_info = get_repository_info(repo_path)
    print(f"Git remote URL: {repo_info.get('remote_url', 'N/A')}")
    print(f"Current branch: {repo_info.get('current_branch', 'N/A')}")
    print(f"Latest commit: {repo_info.get('latest_commit', 'N/A')}")
    
    # Analyze files
    print("\n" + "=" * 50)
    print("DIRECTORY STRUCTURE (tree view)")
    print("=" * 50)
    print_tree_structure(repo_path, max_depth=max_depth)
    
    analysis = analyze_files(repo_path)
    
    print("\n" + "=" * 50)
    print("FILE COUNT SUMMARY")
    print("=" * 50)
    print(f"Total files: {analysis['total_files']}")
    print(f"Total directories: {analysis['total_directories']}")
    
    print("\n" + "=" * 50)
    print("FILES BY EXTENSION")
    print("=" * 50)
    sorted_extensions = sorted(analysis['extensions'].items(), key=lambda x: x[1], reverse=True)
    for ext, count in sorted_extensions[:15]:  # Show top 15 extensions
        ext_display = ext if ext != 'no_extension' else '[no extension]'
        print(f"{ext_display:20} : {count:4d} files")
    
    print("\n" + "=" * 50)
    print("LARGEST FILES (top 10)")
    print("=" * 50)
    for file_path, size in analysis['largest_files']:
        relative_path = file_path.relative_to(repo_path)
        print(f"{get_file_size_formatted(size):>10} : {relative_path}")
    
    if analysis['readme_files']:
        print("\n" + "=" * 50)
        print("README FILES")
        print("=" * 50)
        for readme in analysis['readme_files']:
            relative_path = readme.relative_to(repo_path)
            print(f"📄 {relative_path}")
    
    if analysis['hidden_files']:
        print("\n" + "=" * 50)
        print("HIDDEN FILES (first 20)")
        print("=" * 50)
        for hidden in analysis['hidden_files']:
            relative_path = hidden.relative_to(repo_path)
            print(f"🔸 {relative_path}")
    
    print("\n" + "=" * 50)
    print("ALL FILES LIST")
    print("=" * 50)
    for file_path in sorted(analysis['all_files'], key=lambda x: str(x).lower()):
        relative_path = file_path.relative_to(repo_path)
        try:
            size = get_file_size_formatted(file_path.stat().st_size)
            print(f"{size:>10} : {relative_path}")
        except (OSError, FileNotFoundError):
            print(f"{'N/A':>10} : {relative_path}")
    
    print("\n" + "=" * 50)
    print("SCRIPT COMPLETED SUCCESSFULLY")
    print("=" * 50)
    print(f"Repository cloned to: {repo_path}")
    print(f"To navigate to the repository programmatically: os.chdir('{dest_dir}')")

if __name__ == "__main__":
    main()
