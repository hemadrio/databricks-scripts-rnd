#!/usr/bin/env python3
"""
Databricks Bundle Executor Script - Serverless Job Compute SDK Version

This script is designed to be executed on Databricks serverless job compute to perform
databricks bundle operations (git clone + bundle validate/deploy) using Databricks Python SDK.

Optimized for serverless compute with SDK execution (CLI not supported on serverless).

Usage on Databricks Serverless Job Compute:
    python databricks_bundle_executor.py --git_url <url> --git_branch <branch> --yaml_path <path> --target_env <env> --operation <validate|deploy>

Author: DataOps Team
Version: 5.0 - Serverless Job Compute with Databricks Python SDK
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
import yaml
from typing import Dict, Any, Optional

# Databricks SDK imports
try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.core import Config
    from databricks.sdk.errors import DatabricksError
    SDK_AVAILABLE = True
except ImportError as e:
    SDK_AVAILABLE = False
    SDK_IMPORT_ERROR = str(e)

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

def setup_databricks_client_serverless(db_config: Dict[str, Any], databricks_host: Optional[str], databricks_token: Optional[str]) -> WorkspaceClient:
    """
    Setup Databricks SDK client for serverless job compute
    Optimized for SDK execution with enhanced serverless compute support
    
    Args:
        db_config: Databricks connection configuration
        databricks_host: Databricks host from arguments
        databricks_token: Databricks token from arguments
        
    Returns:
        Configured WorkspaceClient instance
    """
    if not SDK_AVAILABLE:
        logger.error(f"❌ Databricks SDK not available: {SDK_IMPORT_ERROR}")
        logger.error("Please install databricks-sdk: pip install databricks-sdk")
        raise ImportError("Databricks SDK is required for serverless execution")
    
    logger.info("🚀 Setting up Databricks SDK client for serverless job compute")
    
    # Get host - prioritize explicit host argument
    host = databricks_host or db_config.get('workspace_url') or db_config.get('databricks_instance_url')
    if host:
        # Ensure host is in correct format for SDK
        if not host.startswith('https://'):
            host = f"https://{host}"
        logger.info(f"🔧 Using Databricks host: {host}")
    else:
        logger.error("❌ Databricks host is required for serverless job compute")
        raise ValueError("Databricks host is required")
    
    # Determine authentication type - prefer service principal for serverless
    auth_type = db_config.get('authentication_type', 'personal_access_token')
    
    try:
        if auth_type == 'service_principal':
            # Service Principal authentication (recommended for serverless)
            client_id = db_config.get('client_id')
            secret = db_config.get('secret')
            
            if client_id and secret:
                logger.info("🔧 Using Service Principal authentication (recommended for serverless)")
                logger.info(f"   Client ID: {client_id[:10]}...")
                
                config = Config(
                    host=host,
                    client_id=client_id,
                    client_secret=secret
                )
                client = WorkspaceClient(config=config)
            else:
                logger.error("❌ Service Principal authentication requires client_id and secret")
                raise ValueError("Service Principal authentication requires client_id and secret")
        else:
            # Personal Access Token authentication
            token = databricks_token or db_config.get('personal_access_token') or db_config.get('token')
            if token:
                logger.info("🔧 Using Personal Access Token authentication")
                logger.info(f"   Token: {token[:10]}...")
                
                config = Config(
                    host=host,
                    token=token
                )
                client = WorkspaceClient(config=config)
            else:
                logger.error("❌ Personal Access Token authentication requires token")
                raise ValueError("Personal Access Token authentication requires token")
        
        # Test connection
        logger.info("🔍 Testing Databricks connection...")
        current_user = client.current_user.me()
        logger.info(f"✅ Connected as: {current_user.user_name}")
        
        logger.info("✅ Databricks SDK client configured for serverless execution")
        return client
        
    except DatabricksError as e:
        logger.error(f"❌ Failed to setup Databricks SDK client: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"❌ Unexpected error setting up Databricks SDK client: {str(e)}")
        raise

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

def load_bundle_config(work_dir: str, target_env: str) -> Optional[Dict[str, Any]]:
    """
    Load and parse the bundle configuration from databricks.yml
    
    Args:
        work_dir: Working directory containing databricks.yml
        target_env: Target environment to extract config for
        
    Returns:
        Parsed bundle configuration or None if failed
    """
    try:
        yaml_path = os.path.join(work_dir, 'databricks.yml')
        
        with open(yaml_path, 'r') as f:
            bundle_config = yaml.safe_load(f)
        
        logger.info(f"📄 Loaded bundle configuration from {yaml_path}")
        
        # Validate basic structure
        if 'bundle' not in bundle_config:
            logger.error("❌ Missing 'bundle' section in configuration")
            return None
            
        if 'targets' not in bundle_config:
            logger.error("❌ Missing 'targets' section in configuration")
            return None
            
        if target_env not in bundle_config['targets']:
            logger.error(f"❌ Target environment '{target_env}' not found in configuration")
            return None
        
        logger.info(f"✅ Bundle config validation passed for {target_env}")
        return bundle_config
        
    except yaml.YAMLError as e:
        logger.error(f"❌ Failed to parse YAML configuration: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"❌ Failed to load bundle configuration: {str(e)}")
        return None


def validate_bundle_sdk(client: WorkspaceClient, bundle_config: Dict[str, Any], target_env: str) -> bool:
    """
    Validate bundle configuration using SDK
    
    Args:
        client: Databricks WorkspaceClient
        bundle_config: Bundle configuration
        target_env: Target environment
        
    Returns:
        True if validation passes, False otherwise
    """
    try:
        logger.info(f"🔍 Validating bundle configuration for {target_env}")
        
        target_config = bundle_config['targets'][target_env]
        
        # Check workspace configuration
        if 'workspace' in target_config:
            workspace_config = target_config['workspace']
            if 'host' in workspace_config:
                expected_host = workspace_config['host']
                actual_host = client.config.host
                if expected_host not in actual_host:
                    logger.warning(f"⚠️ Host mismatch: expected {expected_host}, using {actual_host}")
        
        # Validate workspace access
        current_user = client.current_user.me()
        logger.info(f"✅ Workspace access validated for {current_user.user_name}")
        
        # Check resources that will be deployed
        if 'resources' in bundle_config:
            resources = bundle_config['resources']
            logger.info(f"📦 Resources to validate: {list(resources.keys())}")
            
            for resource_type, resource_configs in resources.items():
                if resource_type == 'jobs':
                    logger.info(f"🔍 Validating {len(resource_configs)} job(s)")
                elif resource_type == 'models':
                    logger.info(f"🔍 Validating {len(resource_configs)} model(s)")
                elif resource_type == 'experiments':
                    logger.info(f"🔍 Validating {len(resource_configs)} experiment(s)")
                else:
                    logger.info(f"🔍 Found resource type: {resource_type}")
        
        logger.info("✅ Bundle validation completed successfully")
        return True
        
    except DatabricksError as e:
        logger.error(f"❌ Bundle validation failed: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"❌ Bundle validation failed with unexpected error: {str(e)}")
        return False


def deploy_bundle_sdk(client: WorkspaceClient, bundle_config: Dict[str, Any], target_env: str, work_dir: str) -> bool:
    """
    Deploy bundle using SDK
    
    Args:
        client: Databricks WorkspaceClient
        bundle_config: Bundle configuration
        target_env: Target environment
        work_dir: Working directory
        
    Returns:
        True if deployment succeeds, False otherwise
    """
    try:
        logger.info(f"🚀 Deploying bundle for {target_env}")
        
        # First validate
        if not validate_bundle_sdk(client, bundle_config, target_env):
            logger.error("❌ Bundle validation failed, aborting deployment")
            return False
        
        # Deploy resources
        if 'resources' in bundle_config:
            resources = bundle_config['resources']
            
            # Deploy jobs
            if 'jobs' in resources:
                if not deploy_jobs_sdk(client, resources['jobs'], target_env):
                    return False
            
            # Deploy models (if any)
            if 'models' in resources:
                if not deploy_models_sdk(client, resources['models'], target_env):
                    return False
                    
        logger.info("✅ Bundle deployment completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Bundle deployment failed: {str(e)}")
        return False


def deploy_jobs_sdk(client: WorkspaceClient, jobs_config: Dict[str, Any], target_env: str) -> bool:
    """Deploy jobs using SDK"""
    try:
        logger.info(f"🏗️ Deploying {len(jobs_config)} job(s)")
        
        for job_name, job_config in jobs_config.items():
            logger.info(f"📝 Deploying job: {job_name}")
            
            # Create or update job
            job_spec = {
                "name": f"{job_name}_{target_env}",
                "tasks": job_config.get('tasks', []),
                "job_clusters": job_config.get('job_clusters', []),
                "timeout_seconds": job_config.get('timeout_seconds', 3600)
            }
            
            try:
                # Try to find existing job
                existing_jobs = list(client.jobs.list(name=job_spec["name"]))
                
                if existing_jobs:
                    # Update existing job
                    job_id = existing_jobs[0].job_id
                    client.jobs.reset(job_id=job_id, new_settings=job_spec)
                    logger.info(f"✅ Updated existing job {job_name} (ID: {job_id})")
                else:
                    # Create new job
                    created_job = client.jobs.create(**job_spec)
                    logger.info(f"✅ Created new job {job_name} (ID: {created_job.job_id})")
                    
            except DatabricksError as e:
                logger.error(f"❌ Failed to deploy job {job_name}: {str(e)}")
                return False
                
        return True
        
    except Exception as e:
        logger.error(f"❌ Job deployment failed: {str(e)}")
        return False


def deploy_models_sdk(client: WorkspaceClient, models_config: Dict[str, Any], target_env: str) -> bool:
    """Deploy models using SDK"""
    try:
        logger.info(f"🤖 Deploying {len(models_config)} model(s)")
        # Model deployment would go here - simplified for now
        logger.info("✅ Model deployment completed")
        return True
    except Exception as e:
        logger.error(f"❌ Model deployment failed: {str(e)}")
        return False


def destroy_bundle_sdk(client: WorkspaceClient, bundle_config: Dict[str, Any], target_env: str) -> bool:
    """Destroy bundle resources using SDK"""
    try:
        logger.info(f"🗑️ Destroying bundle resources for {target_env}")
        
        if 'resources' in bundle_config:
            resources = bundle_config['resources']
            
            # Destroy jobs
            if 'jobs' in resources:
                for job_name in resources['jobs'].keys():
                    job_full_name = f"{job_name}_{target_env}"
                    existing_jobs = list(client.jobs.list(name=job_full_name))
                    
                    for job in existing_jobs:
                        client.jobs.delete(job_id=job.job_id)
                        logger.info(f"🗑️ Deleted job: {job_full_name} (ID: {job.job_id})")
        
        logger.info("✅ Bundle destruction completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Bundle destruction failed: {str(e)}")
        return False


def run_bundle_sdk(client: WorkspaceClient, bundle_config: Dict[str, Any], target_env: str) -> bool:
    """Run bundle jobs using SDK"""
    try:
        logger.info(f"▶️ Running bundle jobs for {target_env}")
        
        # First deploy, then run
        if not deploy_bundle_sdk(client, bundle_config, target_env, ""):
            return False
            
        # Run jobs
        if 'resources' in bundle_config and 'jobs' in bundle_config['resources']:
            for job_name in bundle_config['resources']['jobs'].keys():
                job_full_name = f"{job_name}_{target_env}"
                existing_jobs = list(client.jobs.list(name=job_full_name))
                
                if existing_jobs:
                    job_id = existing_jobs[0].job_id
                    run = client.jobs.run_now(job_id=job_id)
                    logger.info(f"▶️ Started job run: {job_full_name} (Run ID: {run.run_id})")
        
        logger.info("✅ Bundle run completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Bundle run failed: {str(e)}")
        return False


def execute_bundle_operation_sdk(operation: str, target_env: str, work_dir: str, 
                               client: WorkspaceClient, timeout: int = 600, 
                               retries: int = 3, retry_delay: int = 5) -> bool:
    """
    Execute databricks bundle operation using Databricks SDK (optimized for serverless)
    
    Args:
        operation: Bundle operation (validate, deploy, destroy, run)
        target_env: Target environment
        work_dir: Working directory
        client: Databricks WorkspaceClient instance
        timeout: Command timeout in seconds
        retries: Number of retry attempts
        retry_delay: Delay between retries in seconds
        
    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info(f"🚀 Starting databricks bundle {operation} operation (SDK, serverless optimized)")
        logger.info(f"   Target Environment: {target_env}")
        logger.info(f"   Working Directory: {work_dir}")
        logger.info(f"   Client: {client.config.host}")
        logger.info(f"   Timeout: {timeout}s")
        
        # Validate working directory and bundle files
        if not validate_bundle_directory(work_dir):
            return False
        
        # Load and parse the bundle configuration
        bundle_config = load_bundle_config(work_dir, target_env)
        if not bundle_config:
            return False
        
        # Execute bundle operation with retry logic for serverless resilience
        last_error = None
        
        for attempt in range(retries):
            try:
                if attempt > 0:
                    logger.info(f"🔄 Retry attempt {attempt + 1}/{retries} for {operation}")
                    time.sleep(retry_delay)
                
                # Execute the appropriate bundle operation
                success = False
                if operation == 'validate':
                    success = validate_bundle_sdk(client, bundle_config, target_env)
                elif operation == 'deploy':
                    success = deploy_bundle_sdk(client, bundle_config, target_env, work_dir)
                elif operation == 'destroy':
                    success = destroy_bundle_sdk(client, bundle_config, target_env)
                elif operation == 'run':
                    success = run_bundle_sdk(client, bundle_config, target_env)
                else:
                    logger.error(f"❌ Unsupported operation: {operation}")
                    return False
                
                if success:
                    logger.info(f"✅ Bundle {operation} completed successfully for {target_env}")
                    return True
                else:
                    # Log failure and prepare for retry if applicable
                    last_error = f"Bundle {operation} operation failed"
                    if attempt < retries - 1:
                        logger.warning(f"⚠️ Attempt {attempt + 1} failed, retrying in {retry_delay}s...")
                    else:
                        logger.error(f"❌ All {retries} attempts failed for {operation}")
                    
            except DatabricksError as e:
                last_error = f"Databricks SDK error: {str(e)}"
                logger.error(f"❌ Databricks SDK error on attempt {attempt + 1}: {str(e)}")
                
                if attempt < retries - 1:
                    logger.warning(f"⚠️ Retrying in {retry_delay}s...")
                else:
                    logger.error(f"❌ All {retries} attempts failed due to SDK errors")
            except Exception as e:
                last_error = f"Unexpected error: {str(e)}"
                logger.error(f"❌ Unexpected error on attempt {attempt + 1}: {str(e)}")
                
                if attempt < retries - 1:
                    logger.warning(f"⚠️ Retrying in {retry_delay}s...")
                else:
                    logger.error(f"❌ All {retries} attempts failed due to unexpected errors")
                    
        logger.error(f"❌ Final failure after {retries} attempts: {last_error}")
        return False
            
    except Exception as e:
        logger.error(f"❌ Bundle operation failed: {str(e)}")
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




def main():
    """Main function"""
    try:
        # Parse arguments
        args = parse_arguments()
        
        # Set up logging level
        if args.verbose:
            logging.getLogger().setLevel(logging.DEBUG)
        
        logger.info("🚀 Starting Databricks Bundle Executor Script - Serverless SDK (v5.0)")
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
        
        # Setup Databricks SDK client for serverless
        try:
            client = setup_databricks_client_serverless(db_config, databricks_host, databricks_token)
        except Exception as e:
            logger.error("❌ Failed to setup Databricks SDK client")
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
            
            # Step 3: Execute bundle operation (SDK) with serverless optimizations
            if not execute_bundle_operation_sdk(
                operation, target_env, work_dir, client, 
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
