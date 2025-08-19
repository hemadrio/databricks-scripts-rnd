#!/usr/bin/env python3
"""
Databricks Bundle Executor Script - Serverless Job Compute Version

This script is designed to be executed on Databricks serverless job compute to perform
databricks bundle operations (git clone + bundle validate/deploy) using Databricks CLI only.

Optimized for serverless compute with CLI-only execution (no SDK or REST API).

Usage on Databricks Serverless Job Compute:
    python databricks_bundle_executor.py --git_url <url> --git_branch <branch> --yaml_path <path> --target_env <env> --operation <validate|deploy>

Author: DataOps Team
Version: 4.0 - Serverless Job Compute with CLI-only execution
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
import time
from typing import Dict, Any, Optional

# Set up logging optimized for serverless execution
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [SERVERLESS] %(message)s',
    force=True  # Override any existing logging configuration
)
logger = logging.getLogger(__name__)

# Configure for serverless environment
logger.propagate = False  # Prevent duplicate logs in serverless environment

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
    
    # Serverless-specific parameters
    parser.add_argument('--serverless-optimized', action='store_true', default=True,
                       help='Enable serverless job compute optimizations (default: True)')
    parser.add_argument('--cli-retries', type=int, default=3, 
                       help='Number of retry attempts for CLI commands (default: 3)')
    parser.add_argument('--cli-retry-delay', type=int, default=5, 
                       help='Delay between CLI retry attempts in seconds (default: 5)')
    
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

def setup_databricks_authentication_serverless(db_config: Dict[str, Any], databricks_host: Optional[str], databricks_token: Optional[str]) -> Dict[str, str]:
    """
    Setup Databricks authentication for serverless job compute
    Optimized for CLI-only execution with enhanced serverless compute support
    
    Args:
        db_config: Databricks connection configuration
        databricks_host: Databricks host from arguments
        databricks_token: Databricks token from arguments
        
    Returns:
        Dictionary of environment variables to set
    """
    env_vars = {}
    
    logger.info("🚀 Setting up authentication for Databricks serverless job compute")
    
    # Get host - prioritize explicit host argument
    host = databricks_host or db_config.get('workspace_url') or db_config.get('databricks_instance_url')
    if host:
        # Ensure host is in correct format for CLI
        clean_host = host.replace('https://', '').replace('http://', '')
        env_vars['DATABRICKS_HOST'] = clean_host
        logger.info(f"🔧 Using Databricks host: {clean_host}")
    else:
        logger.error("❌ Databricks host is required for serverless job compute")
        return {}
    
    # Determine authentication type - prefer service principal for serverless
    auth_type = db_config.get('authentication_type', 'personal_access_token')
    
    if auth_type == 'service_principal':
        # Service Principal authentication (recommended for serverless)
        client_id = db_config.get('client_id')
        secret = db_config.get('secret')
        
        if client_id and secret:
            env_vars['DATABRICKS_CLIENT_ID'] = client_id
            env_vars['DATABRICKS_CLIENT_SECRET'] = secret
            logger.info("🔧 Using Service Principal authentication (recommended for serverless)")
            logger.info(f"   Client ID: {client_id[:10]}...")
        else:
            logger.error("❌ Service Principal authentication requires client_id and secret")
            return {}
    else:
        # Personal Access Token authentication
        token = databricks_token or db_config.get('personal_access_token') or db_config.get('token')
        if token:
            env_vars['DATABRICKS_TOKEN'] = token
            logger.info("🔧 Using Personal Access Token authentication")
            logger.info(f"   Token: {token[:10]}...")
        else:
            logger.error("❌ Personal Access Token authentication requires token")
            return {}
    
    # Set additional serverless-specific environment variables
    env_vars['DATABRICKS_CLI_PROFILE'] = 'DEFAULT'
    env_vars['DATABRICKS_OUTPUT_FORMAT'] = 'json'
    
    # Disable interactive prompts for serverless execution
    env_vars['DATABRICKS_CLI_CONFIGURE_INTERACTIVE'] = 'false'
    
    logger.info("✅ Databricks authentication configured for serverless execution")
    # Log final environment setup for serverless
    logger.info(f"🔍 Final serverless environment configuration:")
    for key in env_vars:
        if 'TOKEN' in key or 'SECRET' in key:
            logger.info(f"   {key}: [REDACTED]")
        else:
            logger.info(f"   {key}: {env_vars[key]}")
    
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

def execute_bundle_operation_cli_only(operation: str, target_env: str, work_dir: str, 
                                     env_vars: Dict[str, str], timeout: int = 600, 
                                     retries: int = 3, retry_delay: int = 5) -> bool:
    """
    Execute databricks bundle operation using Databricks CLI only (optimized for serverless)
    
    Args:
        operation: Bundle operation (validate, deploy, destroy, run)
        target_env: Target environment
        work_dir: Working directory
        env_vars: Environment variables for Databricks authentication
        timeout: Command timeout in seconds
        retries: Number of retry attempts
        retry_delay: Delay between retries in seconds
        
    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info(f"🚀 Starting databricks bundle {operation} operation (CLI-only, serverless optimized)")
        logger.info(f"   Target Environment: {target_env}")
        logger.info(f"   Working Directory: {work_dir}")
        logger.info(f"   Authentication: {list(env_vars.keys())}")
        logger.info(f"   Timeout: {timeout}s")
        
        # Validate working directory and bundle files
        if not validate_bundle_directory(work_dir):
            return False
        
        # Set up environment variables for serverless execution
        env = os.environ.copy()
        env.update(env_vars)
        
        # Additional serverless-specific environment setup
        env['PYTHONUNBUFFERED'] = '1'  # Ensure real-time logging
        env['DATABRICKS_CLI_DO_NOT_TRACK'] = '1'  # Disable tracking
        
        # Build optimized bundle command for serverless
        base_cmd = f"databricks bundle {operation} --target {target_env}"
        
        # Add serverless-specific flags
        cmd_flags = [
            "--output json",  # Structured output
            "--progress-format json",  # JSON progress for parsing
            "--no-prompt"  # Disable interactive prompts
        ]
        
        bundle_cmd = f"{base_cmd} {' '.join(cmd_flags)}"
        logger.info(f"🗥️ Executing: {bundle_cmd}")
        
        # Execute bundle command with retry logic for serverless resilience
        last_error = None
        
        for attempt in range(retries):
            try:
                if attempt > 0:
                    logger.info(f"🔄 Retry attempt {attempt + 1}/{retries} for {operation}")
                    time.sleep(retry_delay)
                
                bundle_result = subprocess.run(
                    bundle_cmd, 
                    shell=True, 
                    capture_output=True, 
                    text=True, 
                    timeout=timeout, 
                    cwd=work_dir, 
                    env=env,
                    check=False  # Don't raise exception on non-zero exit
                )
                
                # Process command results
                if process_bundle_result(bundle_result, operation, target_env):
                    return True
                else:
                    # Log failure and prepare for retry if applicable
                    last_error = f"CLI command failed with exit code {bundle_result.returncode}"
                    if attempt < retries - 1:
                        logger.warning(f"⚠️ Attempt {attempt + 1} failed, retrying in {retry_delay}s...")
                    else:
                        logger.error(f"❌ All {retries} attempts failed for {operation}")
                    
            except subprocess.TimeoutExpired:
                last_error = f"Operation timed out after {timeout}s"
                logger.error(f"⏰ Bundle {operation} operation timed out after {timeout}s")
                if attempt < retries - 1:
                    logger.warning(f"⚠️ Timeout on attempt {attempt + 1}, retrying...")
                else:
                    logger.error("⚠️ Consider increasing timeout for large bundles")
                    
        logger.error(f"❌ Final failure after {retries} attempts: {last_error}")
        return False
            
    except Exception as e:
        logger.error(f"❌ Bundle operation failed with exception: {str(e)}")
        logger.error("🔍 Check Databricks CLI installation and authentication")
        return False

def validate_bundle_directory(work_dir: str) -> bool:
    """
    Validate that the working directory contains required bundle files
    
    Args:
        work_dir: Working directory to validate
        
    Returns:
        True if validation passes, False otherwise
    """
    try:
        # Check if directory exists
        if not os.path.exists(work_dir):
            logger.error(f"❌ Working directory does not exist: {work_dir}")
            return False
        
        # List files in working directory
        try:
            files = os.listdir(work_dir)
            logger.info(f"📁 Files in working directory: {files}")
        except Exception as e:
            logger.warning(f"⚠️ Could not list files in working directory: {str(e)}")
            return False
        
        # Check for required databricks.yml file
        yaml_path = os.path.join(work_dir, 'databricks.yml')
        if not os.path.exists(yaml_path):
            logger.error("❌ databricks.yml not found in working directory")
            logger.error(f"   Expected location: {yaml_path}")
            return False
        
        logger.info("✅ databricks.yml found in working directory")
        
        # Basic validation of databricks.yml content
        try:
            with open(yaml_path, 'r') as f:
                yaml_content = f.read()
            
            if not yaml_content.strip():
                logger.error("❌ databricks.yml is empty")
                return False
            
            # Check for required sections
            required_sections = ['bundle:', 'targets:']
            for section in required_sections:
                if section not in yaml_content:
                    logger.error(f"❌ Missing required section: {section}")
                    return False
            
            logger.info("✅ databricks.yml contains required sections")
            
        except Exception as e:
            logger.error(f"❌ Failed to read databricks.yml: {str(e)}")
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Bundle directory validation failed: {str(e)}")
        return False


def process_bundle_result(result: subprocess.CompletedProcess, operation: str, target_env: str) -> bool:
    """
    Process the result of a bundle CLI command execution
    
    Args:
        result: Completed subprocess result
        operation: Bundle operation that was executed
        target_env: Target environment
        
    Returns:
        True if successful, False otherwise
    """
    if result.returncode == 0:
        logger.info(f"✅ Bundle {operation} completed successfully for {target_env}")
        
        # Parse and log structured output if available
        if result.stdout:
            try:
                # Try to parse JSON output
                import json
                output_data = json.loads(result.stdout)
                logger.info("📄 Structured output:")
                logger.info(json.dumps(output_data, indent=2))
            except json.JSONDecodeError:
                # If not JSON, log as plain text
                logger.info(f"📄 Output: {result.stdout}")
        
        if result.stderr and result.stderr.strip():
            logger.info(f"📝 Warnings/Info: {result.stderr}")
        
        return True
    else:
        logger.error(f"❌ Bundle {operation} failed for {target_env} (exit code: {result.returncode})")
        
        # Enhanced error reporting
        if result.stderr:
            logger.error(f"🚫 Error details: {result.stderr}")
            
            # Provide specific guidance for common errors
            stderr_lower = result.stderr.lower()
            if 'authentication' in stderr_lower or 'unauthorized' in stderr_lower:
                logger.error("🔐 Authentication error detected:")
                logger.error("   - Check DATABRICKS_HOST environment variable")
                logger.error("   - Verify DATABRICKS_TOKEN or service principal credentials")
                logger.error("   - Ensure credentials have proper permissions")
            elif 'not found' in stderr_lower:
                logger.error("📁 Resource not found error detected:")
                logger.error("   - Verify target environment configuration")
                logger.error("   - Check workspace and cluster settings")
            elif 'timeout' in stderr_lower:
                logger.error("⏰ Timeout error detected:")
                logger.error("   - Consider increasing the timeout value")
                logger.error("   - Check network connectivity to Databricks")
        
        if result.stdout:
            logger.error(f"📄 Standard output: {result.stdout}")
        
        return False

def main():
    """Main function"""
    try:
        # Parse arguments
        args = parse_arguments()
        
        # Set up logging level
        if args.verbose:
            logging.getLogger().setLevel(logging.DEBUG)
        
        logger.info("🚀 Starting Databricks Bundle Executor Script - Serverless CLI-Only (v4.0)")
        logger.info(f"Operation: {args.operation}")
        logger.info(f"Target Environment: {args.target_env}")
        
        # Log serverless-specific configuration
        if hasattr(args, 'serverless_optimized') and args.serverless_optimized:
            logger.info("☁️ Serverless optimizations: ENABLED")
            logger.info(f"   CLI Retries: {args.cli_retries}")
            logger.info(f"   Retry Delay: {args.cli_retry_delay}s")
            logger.info(f"   Timeout: {args.timeout}s")
        
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
        
        # Setup Databricks authentication for serverless
        env_vars = setup_databricks_authentication_serverless(db_config, databricks_host, databricks_token)
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
            
            # Step 3: Execute bundle operation (CLI-only) with serverless optimizations
            if not execute_bundle_operation_cli_only(
                operation, target_env, work_dir, env_vars, 
                timeout=args.timeout,
                retries=args.cli_retries,
                retry_delay=args.cli_retry_delay
            ):
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
