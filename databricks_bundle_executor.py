#!/usr/bin/env python3
"""
Databricks Bundle Executor Script - Final Version with Secret Resolution

This script is designed to be executed via Spark Task Manager to perform
databricks bundle operations (git clone + bundle validate/deploy).

It supports both Personal Access Token and Service Principal authentication
and integrates with the existing secret resolution API.

Usage via Spark Task Manager:
    python databricks_bundle_executor.py --git_url <url> --git_branch <branch> --yaml_path <path> --target_env <env> --operation <validate|deploy>

Author: DataOps Team
Version: 3.0 - Added Secret Resolution API Integration
"""

import os
import sys
import json
import subprocess
import tempfile
import shutil
import argparse
import logging
import requests
from typing import Dict, Any, Optional

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Databricks Bundle Executor Script')
    
    # Git parameters
    parser.add_argument('--git_url', required=True, help='Git repository URL')
    parser.add_argument('--git_branch', default='main', help='Git branch to clone')
    parser.add_argument('--git_token', help='Git personal access token for authentication')
    
    # Databricks parameters
    parser.add_argument('--yaml_path', required=True, help='Path to databricks.yml file')
    parser.add_argument('--target_env', default='dev', help='Target environment (dev, prod, staging)')
    parser.add_argument('--databricks_host', help='Databricks workspace host')
    parser.add_argument('--databricks_token', help='Databricks access token')
    
    # Connection configurations (JSON strings)
    parser.add_argument('--git_connection_config', help='Git connection configuration as JSON string')
    parser.add_argument('--databricks_connection_config', help='Databricks connection configuration as JSON string')
    
    # Operation parameters
    parser.add_argument('--operation', default='validate', choices=['validate', 'deploy', 'destroy', 'run'], 
                       help='Bundle operation to perform')
    
    # Optional parameters
    parser.add_argument('--timeout', type=int, default=600, help='Operation timeout in seconds')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    
    return parser.parse_args()

def get_env_or_arg(value: str, env_var: str) -> Optional[str]:
    """Get value from argument or environment variable"""
    if value:
        return value
    return os.environ.get(env_var)

def parse_connection_config(config_json: str) -> Dict[str, Any]:
    """Parse connection configuration JSON string"""
    try:
        if config_json:
            return json.loads(config_json)
    except json.JSONDecodeError as e:
        logger.warning(f"Failed to parse connection config: {e}")
    return {}

def resolve_databricks_secret_via_api(secret_expr: str) -> Optional[str]:
    """
    Resolve Databricks secret using Databricks Secrets API directly
    
    Args:
        secret_expr: Secret expression (could be dbutils.secrets.get() or direct value)
        
    Returns:
        Resolved secret value or None if failed
    """
    try:
        # If it's already a direct value (not dbutils.secrets.get), return as-is
        if 'dbutils.secrets.get' not in secret_expr:
            return secret_expr
        
        # Parse dbutils.secrets.get(scope=secret-test, key=client_id) format
        # Extract scope and key from the expression
        if 'scope=' in secret_expr and 'key=' in secret_expr:
            # Parse the expression
            scope_start = secret_expr.find('scope=') + 6
            scope_end = secret_expr.find(',', scope_start)
            if scope_end == -1:
                scope_end = secret_expr.find(')', scope_start)
            
            key_start = secret_expr.find('key=') + 4
            key_end = secret_expr.find(')', key_start)
            
            scope = secret_expr[scope_start:scope_end].strip().strip("'\"")
            key = secret_expr[key_start:key_end].strip().strip("'\"")
            
            logger.info(f"🔍 Resolving secret: {scope}/{key}")
            
            # Use Databricks CLI to get the secret directly
            try:
                import subprocess
                import json
                
                # Run databricks secrets get command
                result = subprocess.run(
                    ['databricks', 'secrets', 'get', '--scope', scope, '--key', key],
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                
                if result.returncode == 0:
                    # The output might be just the value or JSON format
                    value = result.stdout.strip()
                    logger.info(f"✅ Successfully resolved secret: {scope}/{key}")
                    return value
                else:
                    logger.error(f"❌ Databricks CLI failed: {result.stderr}")
                    
            except subprocess.TimeoutExpired:
                logger.error(f"❌ Timeout resolving secret: {scope}/{key}")
            except Exception as e:
                logger.error(f"❌ Subprocess error: {str(e)}")
                
        return None
        
    except Exception as e:
        logger.error(f"❌ Error resolving secret: {str(e)}")
        return None

def setup_databricks_authentication(db_config: Dict[str, Any], databricks_host: Optional[str], databricks_token: Optional[str]) -> Dict[str, str]:
    """
    Setup Databricks authentication based on connection configuration
    
    Args:
        db_config: Databricks connection configuration
        databricks_host: Databricks host from arguments
        databricks_token: Databricks token from arguments
        
    Returns:
        Dictionary of environment variables to set
    """
    env_vars = {}
    
    # Get host
    host = databricks_host or db_config.get('workspace_url') or db_config.get('databricks_instance_url')
    if host:
        env_vars['DATABRICKS_HOST'] = host.replace('https://', '').replace('http://', '')
        logger.info(f"🔧 Using Databricks host: {host}")
    
    # Determine authentication type
    auth_type = db_config.get('authentication_type', 'personal_access_token')
    
    if auth_type == 'service_principal':
        # Service Principal authentication
        client_id_expr = db_config.get('client_id')
        secret_expr = db_config.get('secret')
        
        if client_id_expr and secret_expr:
            # Resolve secrets using the API
            resolved_client_id = resolve_databricks_secret_via_api(client_id_expr)
            resolved_secret = resolve_databricks_secret_via_api(secret_expr)
            
            if resolved_client_id and resolved_secret:
                env_vars['DATABRICKS_CLIENT_ID'] = resolved_client_id
                env_vars['DATABRICKS_CLIENT_SECRET'] = resolved_secret
                logger.info("🔧 Using Service Principal authentication")
                logger.info(f"   Client ID: {resolved_client_id[:10]}...")
            else:
                logger.error("❌ Failed to resolve service principal credentials")
                return {}
        else:
            logger.error("❌ Service Principal authentication requires client_id and secret")
            return {}
    else:
        # Personal Access Token authentication
        token_expr = databricks_token or db_config.get('personal_access_token') or db_config.get('token')
        if token_expr:
            # Resolve token if it's a secret reference
            resolved_token = resolve_databricks_secret_via_api(token_expr)
            if resolved_token:
                env_vars['DATABRICKS_TOKEN'] = resolved_token
                logger.info("🔧 Using Personal Access Token authentication")
                logger.info(f"   Token: {resolved_token[:10]}...")
            else:
                logger.error("❌ Failed to resolve Personal Access Token")
                return {}
        else:
            logger.error("❌ Personal Access Token authentication requires token")
            return {}
    
    return env_vars

def execute_git_clone(git_url: str, git_branch: str, git_token: Optional[str], temp_dir: str) -> bool:
    """
    Execute git clone operation
    
    Args:
        git_url: Git repository URL
        git_branch: Git branch to clone
        git_token: Git personal access token
        temp_dir: Temporary directory for cloning
        
    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info(f"🌿 Starting git clone operation")
        logger.info(f"   URL: {git_url}")
        logger.info(f"   Branch: {git_branch}")
        logger.info(f"   Directory: {temp_dir}")
        
        # Handle authentication if token provided
        authenticated_url = git_url
        if git_token:
            if 'github.com' in git_url:
                # For GitHub, use token in URL
                authenticated_url = git_url.replace('https://', f'https://{git_token}@')
                logger.info("🔐 Using authenticated GitHub URL")
            elif 'gitlab.com' in git_url:
                # For GitLab, use token in URL
                authenticated_url = git_url.replace('https://', f'https://oauth2:{git_token}@')
                logger.info("🔐 Using authenticated GitLab URL")
            else:
                # For other providers, try token in URL
                authenticated_url = git_url.replace('https://', f'https://{git_token}@')
                logger.info("🔐 Using authenticated URL")
        
        # Execute git clone
        clone_cmd = f"git clone {authenticated_url} --depth 1 --branch {git_branch} {temp_dir}"
        logger.info(f"Executing: {clone_cmd}")
        
        clone_result = subprocess.run(
            clone_cmd, shell=True, capture_output=True, text=True, timeout=300
        )
        
        if clone_result.returncode != 0:
            logger.error(f"Git clone failed: {clone_result.stderr}")
            return False
        
        logger.info("✅ Git clone completed successfully")
        return True
        
    except subprocess.TimeoutExpired:
        logger.error("⏰ Git clone operation timed out")
        return False
    except Exception as e:
        logger.error(f"❌ Git clone operation failed: {str(e)}")
        return False

def execute_bundle_operation(operation: str, target_env: str, work_dir: str, 
                           env_vars: Dict[str, str]) -> bool:
    """
    Execute databricks bundle operation
    
    Args:
        operation: Bundle operation (validate, deploy, etc.)
        target_env: Target environment
        work_dir: Working directory
        env_vars: Environment variables for Databricks authentication
        
    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info(f"🚀 Starting databricks bundle {operation} operation")
        logger.info(f"   Target Environment: {target_env}")
        logger.info(f"   Working Directory: {work_dir}")
        logger.info(f"   Authentication: {list(env_vars.keys())}")
        
        # Build bundle command
        bundle_cmd = f"databricks bundle {operation} -t {target_env}"
        logger.info(f"Executing: {bundle_cmd}")
        
        # Set up environment variables
        env = os.environ.copy()
        env.update(env_vars)
        
        # Execute bundle command
        bundle_result = subprocess.run(
            bundle_cmd, shell=True, capture_output=True, text=True, timeout=600, 
            cwd=work_dir, env=env
        )
        
        if bundle_result.returncode != 0:
            logger.error(f"Bundle operation failed: {bundle_result.stderr}")
            logger.error(f"Return code: {bundle_result.returncode}")
            return False
        
        logger.info("✅ Bundle operation completed successfully")
        if bundle_result.stdout:
            logger.info(f"📄 Output: {bundle_result.stdout}")
        
        return True
        
    except subprocess.TimeoutExpired:
        logger.error("⏰ Bundle operation timed out")
        return False
    except Exception as e:
        logger.error(f"❌ Bundle operation failed: {str(e)}")
        return False

def main():
    """Main function"""
    try:
        # Parse arguments
        args = parse_arguments()
        
        # Set up logging level
        if args.verbose:
            logging.getLogger().setLevel(logging.DEBUG)
        
        logger.info("🚀 Starting Databricks Bundle Executor Script (v3.1)")
        logger.info(f"Operation: {args.operation}")
        logger.info(f"Target Environment: {args.target_env}")
        
        # Get values from arguments or environment variables
        git_url = args.git_url
        git_branch = args.git_branch
        git_token = get_env_or_arg(args.git_token, 'GIT_TOKEN')
        yaml_path = args.yaml_path
        target_env = args.target_env
        databricks_host = get_env_or_arg(args.databricks_host, 'DATABRICKS_HOST')
        databricks_token = get_env_or_arg(args.databricks_token, 'DATABRICKS_TOKEN')
        operation = args.operation
        
        # Parse connection configurations if provided
        git_config = {}
        db_config = {}
        
        if args.git_connection_config:
            git_config = parse_connection_config(args.git_connection_config)
            if not git_token and git_config.get('personal_access_token'):
                git_token = git_config['personal_access_token']
                logger.info("🔐 Using Git token from connection config")
        
        if args.databricks_connection_config:
            db_config = parse_connection_config(args.databricks_connection_config)
            logger.info("🔧 Using Databricks config from connection config")
        
        # Setup Databricks authentication
        env_vars = setup_databricks_authentication(db_config, databricks_host, databricks_token)
        if not env_vars:
            logger.error("❌ Failed to setup Databricks authentication")
            sys.exit(1)
        
        # Validate required parameters
        if not git_url:
            logger.error("❌ Git URL is required")
            sys.exit(1)
        
        if not yaml_path:
            logger.error("❌ YAML path is required")
            sys.exit(1)
        
        # Create temporary directory
        temp_dir = tempfile.mkdtemp(prefix=f"bundle_{operation}_")
        logger.info(f"📁 Created temporary directory: {temp_dir}")
        
        try:
            # Step 1: Execute git clone
            if not execute_git_clone(git_url, git_branch, git_token, temp_dir):
                logger.error("❌ Git clone failed")
                sys.exit(1)
            
            # Step 2: Navigate to yaml file directory
            yaml_dir = os.path.dirname(yaml_path)
            if yaml_dir:
                work_dir = os.path.join(temp_dir, yaml_dir)
                if os.path.exists(work_dir):
                    logger.info(f"📂 Changed to directory: {work_dir}")
                else:
                    logger.warning(f"⚠️ Directory not found: {work_dir}, using root")
                    work_dir = temp_dir
            else:
                work_dir = temp_dir
                logger.info(f"📂 Using root directory: {work_dir}")
            
            # Step 3: Execute bundle operation
            if not execute_bundle_operation(operation, target_env, work_dir, env_vars):
                logger.error("❌ Bundle operation failed")
                sys.exit(1)
            
            logger.info("🎉 All operations completed successfully!")
            
        finally:
            # Cleanup temporary directory
            if os.path.exists(temp_dir):
                try:
                    shutil.rmtree(temp_dir)
                    logger.info(f"🧹 Cleaned up temporary directory: {temp_dir}")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to cleanup temporary directory: {str(e)}")
        
    except KeyboardInterrupt:
        logger.info("⏹️ Operation interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Unexpected error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
