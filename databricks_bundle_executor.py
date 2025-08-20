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
Version: 8.13 - Simplified CLI Download with Git YAML
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
    parser.add_argument('--databricks_client_id', help='Databricks Service Principal client ID')
    parser.add_argument('--databricks_client_secret', help='Databricks Service Principal client secret')
    
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

def setup_databricks_authentication(db_config: Dict[str, Any], databricks_host: Optional[str], databricks_token: Optional[str], client_id: Optional[str] = None, client_secret: Optional[str] = None) -> Dict[str, str]:
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
    
    # Prioritize direct parameters over connection config
    if databricks_token:
        # Use provided PAT token directly
        env_vars['DATABRICKS_TOKEN'] = databricks_token
        logger.info("🔧 Using provided Personal Access Token authentication")
        logger.info(f"   Token: {databricks_token[:8]}...")
    elif client_id and client_secret:
        # Use provided Service Principal credentials directly
        env_vars['DATABRICKS_CLIENT_ID'] = client_id
        env_vars['DATABRICKS_CLIENT_SECRET'] = client_secret
        logger.info("🔧 Using provided Service Principal authentication")
        logger.info(f"   Client ID: {client_id[:8]}...")
        logger.info(f"   Client Secret: {client_secret[:8]}...")
    else:
        # Fall back to connection config authentication
        auth_type = db_config.get('authentication_type', 'personal_access_token')
        
        if auth_type == 'service_principal':
            # Service Principal authentication
            client_id = db_config.get('client_id')
            secret = db_config.get('secret')
            
            if client_id and secret:
                env_vars['DATABRICKS_CLIENT_ID'] = client_id
                env_vars['DATABRICKS_CLIENT_SECRET'] = secret
                logger.info("🔧 Using Service Principal authentication")
                logger.info(f"   Client ID: {client_id[:10]}...")
            else:
                logger.error("❌ Service Principal authentication requires client_id and secret")
                return {}
        else:
            # Personal Access Token authentication from config
            token = db_config.get('personal_access_token') or db_config.get('token')
            if token:
                env_vars['DATABRICKS_TOKEN'] = token
                logger.info("🔧 Using Personal Access Token authentication from config")
                logger.info(f"   Token: {token[:8]}...")
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

def create_databricks_config(env_vars: Dict[str, str]) -> str:
    """
    Create a temporary .databrickscfg file for non-interactive authentication
    
    Args:
        env_vars: Environment variables for Databricks authentication
        
    Returns:
        Path to the created config file
    """
    try:
        import tempfile
        import os
        
        # Create temporary config file
        config_fd, config_path = tempfile.mkstemp(suffix='.databrickscfg', prefix='databricks_')
        
        # Determine authentication method
        host = env_vars.get('DATABRICKS_HOST')
        token = env_vars.get('DATABRICKS_TOKEN')
        client_id = env_vars.get('DATABRICKS_CLIENT_ID')
        client_secret = env_vars.get('DATABRICKS_CLIENT_SECRET')
        
        config_content = "[DEFAULT]\n"
        
        if host:
            config_content += f"host = https://{host}\n"
        
        if token:
            # Personal Access Token authentication
            config_content += f"token = {token}\n"
        elif client_id and client_secret:
            # Service Principal authentication
            config_content += f"client_id = {client_id}\n"
            config_content += f"client_secret = {client_secret}\n"
        
        # Add serverless compute configuration
        config_content += "serverless_compute_id = auto\n"
        
        # Write config file
        with os.fdopen(config_fd, 'w') as f:
            f.write(config_content)
        
        logger.info(f"📝 Created Databricks config file: {config_path}")
        logger.info(f"📋 Config content preview:")
        for line in config_content.split('\n'):
            if line and not any(secret in line.lower() for secret in ['token', 'secret']):
                logger.info(f"   {line}")
            elif line:
                logger.info(f"   {line.split('=')[0]}= ***REDACTED***")
        
        return config_path
        
    except Exception as e:
        logger.error(f"❌ Failed to create Databricks config: {str(e)}")
        return None

# Environment inspection function removed - always use downloaded CLI

def execute_bundle_operation(operation: str, target_env: str, work_dir: str, 
                           env_vars: Dict[str, str]) -> bool:
    """
    Execute databricks bundle operation using downloaded CLI
    
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
        
        # Debug: List files in working directory
        try:
            files = os.listdir(work_dir)
            logger.info(f"📁 Files in working directory: {files}")
            if 'databricks.yml' in files:
                logger.info("✅ databricks.yml found in working directory")
            else:
                logger.warning("⚠️ databricks.yml not found in working directory")
        except Exception as e:
            logger.warning(f"⚠️ Could not list files in working directory: {str(e)}")
        
        # Set up environment variables
        env = os.environ.copy()
        env.update(env_vars)
        
        # Always download and use modern CLI
        logger.info("🔄 Downloading modern CLI with bundle support")
        return download_and_execute_bundle_operation(operation, target_env, work_dir, env_vars)
        
    except subprocess.TimeoutExpired:
        logger.error("⏰ Bundle operation timed out")
        logger.warning("⚠️ CLI timeout - attempting CLI download fallback...")
        logger.info("🔄 Downloading modern CLI with bundle support")
        
        # Fall back to downloading modern CLI after timeout
        return download_and_execute_bundle_operation(operation, target_env, work_dir, env_vars)
            
    except Exception as e:
        logger.error(f"❌ Bundle operation failed: {str(e)}")
        logger.warning("⚠️ General error - attempting CLI download fallback...")
        logger.info("🔄 Downloading modern CLI with bundle support")
        
        # Fall back to downloading modern CLI after general error
        return download_and_execute_bundle_operation(operation, target_env, work_dir, env_vars)

def download_and_execute_bundle_operation(operation: str, target_env: str, work_dir: str, 
                                        env_vars: Dict[str, str]) -> bool:
    """
    Download modern Databricks CLI and execute bundle operation
    
    Args:
        operation: Bundle operation (validate, deploy, etc.)
        target_env: Target environment
        work_dir: Working directory
        env_vars: Environment variables for Databricks authentication
        
    Returns:
        True if successful, False otherwise
    """
    import platform
    import requests
    import tempfile
    import shutil
    
    try:
        logger.info("📥 Downloading modern Databricks CLI with bundle support...")
        
        # Determine platform and download URL (using latest confirmed version v0.264.2)
        version = "0.264.2"
        system = platform.system().lower()
        if system == "linux":
            cli_url = f"https://github.com/databricks/cli/releases/download/v{version}/databricks_cli_{version}_linux_amd64.zip"
        elif system == "darwin":
            cli_url = f"https://github.com/databricks/cli/releases/download/v{version}/databricks_cli_{version}_darwin_amd64.zip"
        else:
            logger.error(f"❌ Unsupported platform: {system}")
            return False
        
        # Create temporary directory for CLI
        temp_cli_dir = tempfile.mkdtemp(prefix="databricks_cli_")
        logger.info(f"📁 Created temporary CLI directory: {temp_cli_dir}")
        
        try:
            # Download CLI
            zip_path = os.path.join(temp_cli_dir, "databricks.zip")
            logger.info(f"🌐 Downloading CLI from: {cli_url}")
            
            response = requests.get(cli_url, timeout=120)
            response.raise_for_status()
            
            with open(zip_path, 'wb') as f:
                f.write(response.content)
            
            logger.info(f"✅ CLI downloaded successfully ({len(response.content)} bytes)")
            
            # Extract CLI
            logger.info("📦 Extracting CLI...")
            result = subprocess.run(
                ["unzip", "-q", zip_path, "-d", temp_cli_dir],
                capture_output=True, text=True, timeout=30
            )
            
            if result.returncode != 0:
                logger.error(f"❌ Failed to extract CLI: {result.stderr}")
                return False
            
            # Set executable permissions (CLI binary name may vary)
            cli_binary_name = "databricks"
            cli_path = os.path.join(temp_cli_dir, cli_binary_name)
            
            # Check if the standard binary exists, if not, look for alternatives
            if not os.path.exists(cli_path):
                # Check for other possible binary names
                possible_names = ["databricks", f"databricks_cli_{version}_linux_amd64", f"databricks_cli_{version}_darwin_amd64"]
                for name in possible_names:
                    test_path = os.path.join(temp_cli_dir, name)
                    if os.path.exists(test_path):
                        cli_path = test_path
                        break
                else:
                    # List files in directory for debugging
                    files = os.listdir(temp_cli_dir)
                    logger.error(f"❌ CLI binary not found. Available files: {files}")
                    return False
            
            os.chmod(cli_path, 0o755)
            logger.info(f"🔧 CLI ready at: {cli_path}")
            
            # Test CLI
            version_result = subprocess.run(
                [cli_path, "version"], capture_output=True, text=True, timeout=30, env=env_vars
            )
            
            if version_result.returncode == 0:
                logger.info(f"✅ Modern CLI version: {version_result.stdout.strip()}")
            else:
                logger.warning(f"⚠️ CLI version check failed: {version_result.stderr}")
            
            # Execute bundle operation using the actual YAML from Git
            logger.info(f"🚀 Executing bundle {operation} with downloaded CLI...")
            logger.info(f"📂 Using Git repository YAML file")
            
            bundle_cmd = [cli_path, "bundle", operation]
            
            if target_env:
                bundle_cmd.extend(["-t", target_env])
            
            logger.info(f"Executing: {' '.join(bundle_cmd)}")
            
            # Set up environment
            env = os.environ.copy()
            env.update(env_vars)
            
            bundle_result = subprocess.run(
                bundle_cmd, capture_output=True, text=True, timeout=600,
                cwd=work_dir, env=env
            )
            
            if bundle_result.returncode == 0:
                logger.info("✅ Bundle operation completed successfully with downloaded CLI!")
                if bundle_result.stdout:
                    logger.info(f"📄 CLI Output:\n{bundle_result.stdout}")
                return True
            else:
                logger.error(f"❌ Bundle operation failed with return code: {bundle_result.returncode}")
                if bundle_result.stderr:
                    logger.error(f"CLI Error: {bundle_result.stderr}")
                if bundle_result.stdout:
                    logger.error(f"CLI Output: {bundle_result.stdout}")
                return False
                
        finally:
            # Cleanup temporary directory
            try:
                shutil.rmtree(temp_cli_dir)
                logger.info(f"🧹 Cleaned up temporary CLI directory: {temp_cli_dir}")
            except Exception as cleanup_error:
                logger.warning(f"⚠️ Failed to cleanup temp directory: {cleanup_error}")
        
    except Exception as e:
        logger.error(f"❌ CLI download and execution failed: {str(e)}")
        return False

# Hardcoded YAML test function removed - always use actual Git YAML

def execute_bundle_validation(yaml_content: str, target_env: str, env_vars: Dict[str, str]) -> bool:
    """
    Execute bundle validation using string parsing (no SDK)
    
    Args:
        yaml_content: Bundle YAML content
        target_env: Target environment
        env_vars: Environment variables
        
    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info("🔍 Performing REAL bundle validation via Databricks API...")
        
        # Get authentication details
        workspace_url = f"https://{env_vars.get('DATABRICKS_HOST')}"
        client_id = env_vars.get('DATABRICKS_CLIENT_ID')
        client_secret = env_vars.get('DATABRICKS_CLIENT_SECRET')
        token = env_vars.get('DATABRICKS_TOKEN')
        
        logger.info(f"🔗 Target workspace: {workspace_url}")
        
        # Get access token
        headers = {}
        if token:
            headers['Authorization'] = f'Bearer {token}'
            logger.info("🔐 Using Personal Access Token for API authentication")
        elif client_id and client_secret:
            logger.info("🔐 Getting OAuth token using Service Principal...")
            logger.info(f"🔗 Token URL: {workspace_url}/oidc/v1/token")
            logger.info(f"🔑 Client ID length: {len(client_id) if client_id else 0}")
            logger.info(f"🔑 Client Secret length: {len(client_secret) if client_secret else 0}")
            
            import requests
            
            token_url = f"{workspace_url}/oidc/v1/token"
            token_response = requests.post(token_url, data={
                'grant_type': 'client_credentials',
                'client_id': client_id,
                'client_secret': client_secret
            }, timeout=30)
            
            if token_response.status_code != 200:
                logger.error(f"❌ Failed to get OAuth token: {token_response.text}")
                logger.info("🔄 Falling back to hardcoded YAML validation test...")
                return execute_hardcoded_yaml_test(yaml_content, target_env, env_vars)
                
            access_token = token_response.json()['access_token']
            headers['Authorization'] = f'Bearer {access_token}'
            logger.info("✅ OAuth token obtained successfully")
        else:
            logger.error("❌ No valid authentication method found")
            logger.info("🔄 Falling back to hardcoded YAML validation test...")
            return execute_hardcoded_yaml_test(yaml_content, target_env, env_vars)
        
        # Test workspace connectivity
        logger.info("🔧 Testing workspace connectivity...")
        headers['Content-Type'] = 'application/json'
        
        import requests
        workspace_test_url = f"{workspace_url}/api/2.0/workspace/list"
        test_response = requests.get(workspace_test_url, headers=headers, params={'path': '/'}, timeout=30)
        
        logger.info(f"📊 Workspace test response: {test_response.status_code}")
        
        if test_response.status_code != 200:
            logger.error(f"❌ Workspace connectivity failed: {test_response.text}")
            return False
        
        logger.info("✅ Workspace connectivity verified!")
        
        # Parse YAML and validate by creating a test job
        try:
            import yaml
            bundle_config = yaml.safe_load(yaml_content)
            logger.info("✅ Bundle YAML parsed successfully")
            
            # Extract job configuration from bundle
            resources = bundle_config.get('resources', {})
            jobs = resources.get('jobs', {})
            
            if jobs:
                # Take the first job for validation
                job_name, job_config = next(iter(jobs.items()))
                logger.info(f"🔧 Validating job configuration via Databricks Jobs API: {job_name}")
                
                # Create a test job payload
                test_job_payload = {
                    'name': f"VALIDATION-TEST-{job_name}-{target_env}",
                    'tasks': job_config.get('tasks', []),
                    'timeout_seconds': 3600,
                    'max_concurrent_runs': 1
                }
                
                # Call Jobs API to validate the configuration
                jobs_url = f"{workspace_url}/api/2.1/jobs/create"
                logger.info("🚀 Creating test job to validate bundle configuration...")
                
                validation_response = requests.post(jobs_url, headers=headers, json=test_job_payload, timeout=60)
                logger.info(f"📊 Job validation response: {validation_response.status_code}")
                
                if validation_response.status_code == 200:
                    job_data = validation_response.json()
                    job_id = job_data.get('job_id')
                    logger.info(f"✅ Bundle validation successful! Test job created: {job_id}")
                    
                    # Clean up test job immediately
                    if job_id:
                        logger.info("🧹 Cleaning up test job...")
                        delete_url = f"{workspace_url}/api/2.1/jobs/delete"
                        delete_payload = {'job_id': job_id}
                        delete_response = requests.post(delete_url, headers=headers, json=delete_payload, timeout=30)
                        if delete_response.status_code == 200:
                            logger.info("✅ Test job cleaned up successfully")
                        else:
                            logger.warning(f"⚠️ Failed to clean up test job: {delete_response.text}")
                    
                    logger.info("✅ REAL Bundle validation completed successfully via Databricks API!")
                    return True
                else:
                    logger.error(f"❌ Bundle validation failed via Jobs API: {validation_response.text}")
                    return False
            else:
                logger.warning("⚠️ No jobs found in bundle, checking basic structure...")
                # Basic structure validation
                if 'bundle' in bundle_config and 'targets' in bundle_config:
                    logger.info("✅ Bundle has basic structure (bundle + targets)")
                    return True
                else:
                    logger.error("❌ Bundle missing required structure")
                    return False
                
        except Exception as yaml_error:
            logger.error(f"❌ YAML parsing/validation error: {str(yaml_error)}")
            return False
        
    except Exception as e:
        logger.error(f"❌ Validation failed: {str(e)}")
        return False

# Main execution  
if __name__ == "__main__":
    """Main execution function"""
    
    # Parse command line arguments  
    args = parse_arguments()
    
    # Set up logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    logger.info("🚀 Starting Databricks Bundle Executor Script (v8.13)")
    logger.info(f"Operation: {args.operation}")
    logger.info(f"Target Environment: {args.target_env}")
    
    # Get values from arguments or environment variables
    git_url = args.git_url
    git_branch = args.git_branch
    git_token = args.git_token
    yaml_path = args.yaml_path
    operation = args.operation
    target_env = args.target_env
    databricks_host = args.databricks_host
    databricks_token = args.databricks_token
    databricks_client_id = args.databricks_client_id
    databricks_client_secret = args.databricks_client_secret
    
    # Parse connection configurations if provided
    git_config = {}
    db_config = {}
    
    if args.git_connection_config:
        git_config = parse_connection_config(args.git_connection_config)
        logger.info("🔧 Using Git config from connection config")
        
        # Use token from connection config if not provided
        if not git_token and git_config.get('token'):
            git_token = git_config['token']
            logger.info("🔐 Using Git token from connection config")
        if not git_token and git_config.get('personal_access_token'):
            git_token = git_config['personal_access_token']
            logger.info("🔐 Using Git token from connection config")
    
    if args.databricks_connection_config:
        db_config = parse_connection_config(args.databricks_connection_config)
        logger.info("🔧 Using Databricks config from connection config")
    
    # Setup Databricks authentication
    env_vars = setup_databricks_authentication(db_config, databricks_host, databricks_token, databricks_client_id, databricks_client_secret)
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
        
        logger.info("✅ Bundle operation completed successfully!")
        
    finally:
        # Cleanup
        try:
            shutil.rmtree(temp_dir)
            logger.info(f"🧹 Cleaned up temporary directory: {temp_dir}")
        except Exception as cleanup_error:
            logger.warning(f"⚠️ Failed to cleanup temp directory: {cleanup_error}")
