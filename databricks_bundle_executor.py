"""
Databricks Bundle Operations - Spark Task Manager Integration

This module provides databricks bundle operations by leveraging existing
pipeline fetching and connection infrastructure, and executing operations
via Spark Task Manager.

Author: DataOps Team
Version: 1.0
"""

import json
import logging
import os
import uuid
from typing import Dict, List, Any, Optional

# Set up logging
logger = logging.getLogger(__name__)

class DatabricksBundleOperations:
    """
    Bundle operations class that leverages existing infrastructure and Spark Task Manager
    """
    
    def __init__(self, databricks_host: str = None, databricks_token: str = None):
        """
        Initialize bundle operations
        
        Args:
            databricks_host: Databricks workspace host (from environment if not provided)
            databricks_token: Databricks API token (from environment if not provided)
        """
        self.databricks_host = databricks_host or os.environ.get('DATABRICKS_HOST')
        self.databricks_token = databricks_token or os.environ.get('DATABRICKS_TOKEN')
        
        if not self.databricks_host or not self.databricks_token:
            raise ValueError("Databricks host and token are required")
    
    def _resolve_secrets_in_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resolve any dbutils.secrets.get() expressions in configuration
        
        Args:
            config: Configuration dictionary that may contain secret expressions
            
        Returns:
            Configuration with resolved secrets
        """
        try:
            resolved_config = {}
            for key, value in config.items():
                if isinstance(value, str) and 'dbutils.secrets.get' in value:
                    # Parse dbutils.secrets.get(scope=secret-test, key=client_id) format
                    if 'scope=' in value and 'key=' in value:
                        scope_start = value.find('scope=') + 6
                        scope_end = value.find(',', scope_start)
                        if scope_end == -1:
                            scope_end = value.find(')', scope_start)
                        
                        key_start = value.find('key=') + 4
                        key_end = value.find(')', key_start)
                        
                        scope = value[scope_start:scope_end].strip().strip("'\"")
                        secret_key = value[key_start:key_end].strip().strip("'\"")
                        
                        logger.info(f"🔍 Resolving secret: {scope}/{secret_key}")
                        
                        # Use the existing get_databricks_secret function
                        from app import get_databricks_secret
                        success, secret_value, error = get_databricks_secret(scope, secret_key)
                        
                        if success:
                            resolved_config[key] = secret_value
                            logger.info(f"✅ Successfully resolved secret: {scope}/{secret_key}")
                        else:
                            logger.error(f"❌ Failed to resolve secret: {error}")
                            resolved_config[key] = value  # Keep original value
                    else:
                        resolved_config[key] = value  # Keep original value
                else:
                    resolved_config[key] = value
            
            return resolved_config
            
        except Exception as e:
            logger.error(f"❌ Error resolving secrets in config: {str(e)}")
            return config  # Return original config if resolution fails
    
    def extract_bundle_parameters(self, plan_step: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract databricks bundle parameters from plan step configuration
        
        Args:
            plan_step: Plan step configuration dictionary
            
        Returns:
            Dictionary containing bundle operation parameters
        """
        try:
            # Debug: Check the type and content of plan_step
            logger.info(f"Plan step type: {type(plan_step)}")
            logger.info(f"Plan step content: {plan_step}")
            
            # Handle case where plan_step might be a string
            if isinstance(plan_step, str):
                logger.error(f"Plan step is a string, not a dictionary: {plan_step}")
                return {}
            
            tool = plan_step.get('tool', {})
            config = tool.get('configuration', {})
            
            # Extract git-related parameters
            git_params = {
                'git_url': config.get('gitUrl'),
                'git_branch': config.get('gitBranch'),
                'git_repository': config.get('gitRepository'),
                'git_tool_id': config.get('gitToolId'),
                'ssh_url': config.get('sshUrl'),
                'repo_id': config.get('repoId'),
                'service': config.get('service')
            }
            
            # Extract databricks-related parameters
            databricks_params = {
                'databricks_tool_id': config.get('toolConfigId'),
                'workspace': config.get('workspace'),
                'workspace_name': config.get('workspaceName'),
                'yaml_path': config.get('yamlPath'),
                'target_environment': config.get('targetEnvironmentName'),
                'account_id': config.get('accountId'),
                'data_ops_tool_type': config.get('dataOpsToolType')
            }
            
            # Extract step metadata
            metadata = {
                'step_name': config.get('stepName'),
                'step_id': plan_step.get('id'),
                'orchestration_type': plan_step.get('orchestration_type'),
                'tool_identifier': tool.get('tool_identifier')
            }
            
            bundle_params = {
                'git_params': git_params,
                'databricks_params': databricks_params,
                'metadata': metadata,
                'raw_config': config
            }
            
            logger.info(f"Extracted bundle parameters for step: {metadata.get('step_name')}")
            return bundle_params
            
        except Exception as e:
            logger.error(f"Error extracting bundle parameters: {str(e)}")
            return {}
    
    def execute_bundle_operation(self, pipeline_id: str, plan_id: str, 
                                operation_type: str = "validate",
                                get_connection_details_func=None,
                                get_pipeline_func=None,
                                spark_task_manager=None) -> Dict[str, Any]:
        """
        Execute databricks bundle operation using Spark Task Manager
        
        Args:
            pipeline_id: Pipeline unique identifier
            plan_id: Plan step unique identifier
            operation_type: Type of operation (validate, deploy, etc.)
            get_connection_details_func: Function to get connection details (from app.py)
            get_pipeline_func: Function to get pipeline details (from app.py)
            spark_task_manager: Spark Task Manager instance for executing the script
            
        Returns:
            Dictionary containing operation results and logs
        """
        operation_id = f"bundle_op_{uuid.uuid4().hex[:8]}"
        
        try:
            logger.info(f"Starting bundle operation {operation_id} for pipeline {pipeline_id}, plan {plan_id}")
            
            # Step 1: Get pipeline configuration using existing function
            if not get_pipeline_func:
                return {
                    'success': False,
                    'error': 'Pipeline fetching function not provided',
                    'operation_id': operation_id
                }
            
            pipeline_result = get_pipeline_func(pipeline_id)
            
            # Debug: Check what type of object we got back
            logger.info(f"Pipeline result type: {type(pipeline_result)}")
            if hasattr(pipeline_result, 'json'):
                logger.info("Pipeline result is a Response object, converting to JSON")
                pipeline_result = pipeline_result.json()
            
            if not pipeline_result or not pipeline_result.get('success'):
                error_msg = "Unknown error"
                if isinstance(pipeline_result, dict):
                    error_msg = pipeline_result.get("error", "Unknown error")
                return {
                    'success': False,
                    'error': f'Failed to fetch pipeline: {error_msg}',
                    'operation_id': operation_id
                }
            
            pipeline = pipeline_result.get('pipeline', {})
            workflow_str = pipeline.get('workflow', '{}')
            
            # Parse workflow JSON string if it's a string
            if isinstance(workflow_str, str):
                try:
                    workflow = json.loads(workflow_str)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse workflow JSON: {e}")
                    return {
                        'success': False,
                        'error': f'Failed to parse workflow JSON: {e}',
                        'operation_id': operation_id
                    }
            else:
                workflow = workflow_str
            
            plan = workflow.get('plan', [])
            
            # Step 2: Find the specific plan step
            plan_step = None
            for step in plan:
                if step.get('id') == plan_id:
                    plan_step = step
                    break
            
            if not plan_step:
                return {
                    'success': False,
                    'error': f'Plan step not found: {plan_id}',
                    'operation_id': operation_id
                }
            
            # Step 3: Extract bundle parameters
            bundle_params = self.extract_bundle_parameters(plan_step)
            if not bundle_params:
                return {
                    'success': False,
                    'error': 'Failed to extract bundle parameters',
                    'operation_id': operation_id
                }
            
            git_params = bundle_params['git_params']
            databricks_params = bundle_params['databricks_params']
            
            # Step 4: Get connection details using API endpoints
            databricks_connection = None
            git_connection = None
            
            # Function to get connection details via API
            def get_connection_via_api(connection_id):
                try:
                    import requests
                    response = requests.get(f"http://localhost:3001/api/connections/{connection_id}")
                    if response.status_code == 200:
                        result = response.json()
                        if result.get('success'):
                            return result.get('connection')
                    return None
                except Exception as e:
                    logger.error(f"Error getting connection via API: {str(e)}")
                    return None
            
            # Try to get connections from pipeline configuration
            if databricks_params.get('databricks_tool_id'):
                logger.info(f"Fetching Databricks connection for ID: {databricks_params['databricks_tool_id']}")
                databricks_connection = get_connection_via_api(databricks_params['databricks_tool_id'])
                logger.info(f"Databricks connection found: {databricks_connection is not None}")
            
            if git_params.get('git_tool_id'):
                logger.info(f"Fetching Git connection for ID: {git_params['git_tool_id']}")
                git_connection = get_connection_via_api(git_params['git_tool_id'])
                logger.info(f"Git connection found: {git_connection is not None}")
            
            # Fallback to hardcoded connection IDs if pipeline config has invalid ones
            if not databricks_connection:
                logger.warning("⚠️ Databricks connection not found, trying hardcoded fallback")
                fallback_databricks_id = "0cc45749ee02b1a52c9a36758a5a174d"
                databricks_connection = get_connection_via_api(fallback_databricks_id)
                logger.info(f"Hardcoded Databricks connection found: {databricks_connection is not None}")
                if databricks_connection:
                    logger.info(f"Hardcoded Databricks connection type: {type(databricks_connection)}")
            
            if not git_connection:
                logger.warning("⚠️ Git connection not found, trying hardcoded fallback")
                fallback_git_id = "43cce0c37af2e7ba13dc9cf36df6c679"
                git_connection = get_connection_via_api(fallback_git_id)
                logger.info(f"Hardcoded Git connection found: {git_connection is not None}")
                if git_connection:
                    logger.info(f"Hardcoded Git connection type: {type(git_connection)}")
            
            # Step 5: Execute bundle operation via Spark Task Manager
            operation_result = self._execute_via_spark_task_manager(
                operation_id, git_params, databricks_params, 
                git_connection, databricks_connection, operation_type, spark_task_manager
            )
            
            return {
                'success': True,
                'operation_id': operation_id,
                'pipeline_id': pipeline_id,
                'plan_id': plan_id,
                'bundle_params': bundle_params,
                'operation_result': operation_result
            }
            
        except Exception as e:
            logger.error(f"Bundle operation failed: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'operation_id': operation_id
            }
    
    def _execute_via_spark_task_manager(self, operation_id: str, git_params: Dict[str, Any], 
                                       databricks_params: Dict[str, Any], git_connection: Optional[Dict[str, Any]], 
                                       databricks_connection: Optional[Dict[str, Any]], 
                                       operation_type: str, spark_task_manager) -> Dict[str, Any]:
        """
        Execute bundle operation via Spark Task Manager
        
        Args:
            operation_id: Unique operation identifier
            git_params: Git-related parameters
            databricks_params: Databricks-related parameters
            git_connection: Git connection details
            databricks_connection: Databricks connection details
            operation_type: Type of operation to perform
            spark_task_manager: Spark Task Manager instance
            
        Returns:
            Operation results with logs
        """
        try:
            if not spark_task_manager:
                return {
                    'success': False,
                    'error': 'Spark Task Manager not provided',
                    'logs': ['❌ Spark Task Manager not available']
                }
            
            # Prepare script parameters
            script_params = [
                f"--git_url={git_params.get('git_url')}",
                f"--git_branch={git_params.get('git_branch', 'main')}",
                f"--yaml_path={databricks_params.get('yaml_path', '')}",
                f"--target_env={databricks_params.get('target_environment', 'dev')}",
                f"--operation={operation_type}"
            ]
            
            # Add Service Principal credentials via connection config
            service_principal_config = {
                "authentication_type": "service_principal",
                "client_id": "8f75bdd0-5588-4a53-a18e-3eb4aa8f7acd",
                "secret": "dose33be8af6c59c358a5b7a4cbf5519f3b6",
                "workspace_url": "https://dbc-3da7f034-dce2.cloud.databricks.com"
            }
            sp_config_json = json.dumps(service_principal_config)
            script_params.append(f"--databricks_connection_config={sp_config_json}")
            logger.info(f"✅ Added Service Principal config: {service_principal_config['client_id'][:8]}...")
            
            # Add connection configurations if available
            if git_connection:
                logger.info(f"Git connection type: {type(git_connection)}")
                if isinstance(git_connection, dict):
                    git_config = git_connection.get('configuration', {})
                    # Resolve any secrets in git config
                    git_config = self._resolve_secrets_in_config(git_config)
                    git_config_json = json.dumps(git_config)
                    script_params.append(f"--git_connection_config={git_config_json}")
                    logger.info(f"✅ Added Git connection config: {git_config.get('personal_access_token', 'NO_TOKEN')[:10]}...")
                else:
                    logger.error(f"❌ Git connection is not a dict: {git_connection}")
            else:
                logger.warning("⚠️ No Git connection found")
            
            if databricks_connection:
                logger.info(f"Databricks connection type: {type(databricks_connection)}")
                if isinstance(databricks_connection, dict):
                    db_config = databricks_connection.get('configuration', {})
                    # Resolve any secrets in databricks config
                    db_config = self._resolve_secrets_in_config(db_config)
                    db_config_json = json.dumps(db_config)
                    script_params.append(f"--databricks_connection_config={db_config_json}")
                    logger.info(f"✅ Added Databricks connection config: {db_config.get('workspace_url', 'NO_URL')}")
                else:
                    logger.error(f"❌ Databricks connection is not a dict: {databricks_connection}")
            else:
                logger.warning("⚠️ No Databricks connection found")
            
            # Add environment variables for Databricks CLI
            env_vars = {}
            if databricks_connection:
                db_config = databricks_connection.get('configuration', {})
                host = db_config.get('databricks_instance_url') or db_config.get('host')
                token = db_config.get('personal_access_token') or db_config.get('token')
                
                if host:
                    env_vars['DATABRICKS_HOST'] = host.replace('https://', '').replace('http://', '')
                if token:
                    env_vars['DATABRICKS_TOKEN'] = token
            
            # Execute via Spark Task Manager
            logger.info(f"🚀 Executing bundle operation via Spark Task Manager")
            logger.info(f"   Script: databricks_bundle_script.py")
            logger.info(f"   Parameters: {script_params}")
            
            # Use the existing Spark Task Manager to execute the script from Git repository
            try:
                result = spark_task_manager.create_spark_python_task(
                    python_file_path="databricks_bundle_executor.py",
                    parameters=script_params,
                    task_name=f"bundle_{operation_type}_{operation_id}",
                    timeout_seconds=600,
                    environment_key="Default",
                    git_source={
                        "git_url": "https://github.com/hemadrio/databricks-scripts-rnd",
                        "git_provider": "gitHub",
                        "git_branch": "main"
                    },
                    source_type="GIT"
                )
                
                # Handle the result properly
                if hasattr(result, 'json'):
                    # If it's a Response object, get the JSON
                    result_data = result.json()
                elif isinstance(result, dict):
                    # If it's already a dictionary
                    result_data = result
                else:
                    # Convert to string representation
                    result_data = str(result)
                
            except Exception as e:
                logger.error(f"Spark Task Manager execution failed: {str(e)}")
                return {
                    'success': False,
                    'error': f'Spark Task Manager execution failed: {str(e)}',
                    'logs': [f"❌ Spark Task Manager execution failed: {str(e)}"]
                }
            
            # Extract the run ID for monitoring
            run_id = None
            if isinstance(result_data, dict):
                run_id = result_data.get('run_id') or result_data.get('api_response', {}).get('run_id')
            
            return {
                'success': True,
                'spark_task_result': result_data,
                'run_id': run_id,
                'job_monitoring': {
                    'run_id': run_id,
                    'logs_endpoint': f"/api/bundle/logs/{run_id}" if run_id else None,
                    'status_endpoint': f"/api/bundle/status/{run_id}" if run_id else None
                },
                'logs': [
                    f"🚀 Bundle operation submitted via Spark Task Manager",
                    f"📋 Task Name: bundle_{operation_type}_{operation_id}",
                    f"🆔 Spark Run ID: {run_id}",
                    f"🔧 Parameters: {script_params}",
                    f"📄 Monitor logs at: /api/bundle/logs/{run_id}" if run_id else "📄 No run ID available for monitoring"
                ]
            }
            
        except Exception as e:
            logger.error(f"Spark Task Manager execution failed: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'logs': [f"❌ Spark Task Manager execution failed: {str(e)}"]
            }
        """
        Execute the actual bundle operation with git clone and databricks commands
        
        Args:
            operation_id: Unique operation identifier
            git_params: Git-related parameters
            databricks_params: Databricks-related parameters
            git_connection: Git connection details
            databricks_connection: Databricks connection details
            operation_type: Type of operation to perform
            
        Returns:
            Operation results with logs
        """
        logs = []
        temp_dir = None
        
        try:
            # Create temporary directory for git clone
            temp_dir = tempfile.mkdtemp(prefix=f"bundle_{operation_id}_")
            logs.append(f"📁 Created temporary directory: {temp_dir}")
            
            # Step 1: Git Clone
            git_url = git_params.get('git_url')
            git_branch = git_params.get('git_branch', 'main')
            
            if not git_url:
                raise Exception("Git URL not found in configuration")
            
            # Handle authentication if git connection provided
            if git_connection:
                git_config = git_connection.get('configuration', {})
                token = git_config.get('personal_access_token')
                if token:
                    # Use authenticated URL
                    if 'github.com' in git_url:
                        git_url = git_url.replace('https://', f'https://{token}@')
                    logs.append(f"🔐 Using authenticated git URL")
            
            # Execute git clone
            clone_cmd = f"git clone {git_url} --depth 1 --branch {git_branch} {temp_dir}"
            logs.append(f"🌿 Executing: {clone_cmd}")
            
            clone_result = subprocess.run(
                clone_cmd, shell=True, capture_output=True, text=True, timeout=300
            )
            
            if clone_result.returncode != 0:
                raise Exception(f"Git clone failed: {clone_result.stderr}")
            
            logs.append("✅ Git clone completed successfully")
            
            # Step 2: Navigate to yaml file directory
            yaml_path = databricks_params.get('yaml_path', '')
            if yaml_path:
                # Extract directory from yaml path
                yaml_dir = os.path.dirname(yaml_path)
                if yaml_dir:
                    work_dir = os.path.join(temp_dir, yaml_dir)
                    if os.path.exists(work_dir):
                        os.chdir(work_dir)
                        logs.append(f"📂 Changed to directory: {work_dir}")
                    else:
                        logs.append(f"⚠️ Directory not found: {work_dir}, using root")
                else:
                    work_dir = temp_dir
                    os.chdir(work_dir)
                    logs.append(f"📂 Using root directory: {work_dir}")
            else:
                work_dir = temp_dir
                os.chdir(work_dir)
                logs.append(f"📂 Using root directory: {work_dir}")
            
            # Step 3: Execute databricks bundle command
            target_env = databricks_params.get('target_environment', 'dev')
            
            if operation_type == "validate":
                bundle_cmd = f"databricks bundle validate -t {target_env}"
            elif operation_type == "deploy":
                bundle_cmd = f"databricks bundle deploy -t {target_env}"
            else:
                bundle_cmd = f"databricks bundle {operation_type} -t {target_env}"
            
            logs.append(f"🚀 Executing: {bundle_cmd}")
            
            # Set databricks environment variables if connection provided
            env = os.environ.copy()
            if databricks_connection:
                db_config = databricks_connection.get('configuration', {})
                host = db_config.get('databricks_instance_url') or db_config.get('host')
                token = db_config.get('personal_access_token') or db_config.get('token')
                
                if host:
                    env['DATABRICKS_HOST'] = host.replace('https://', '').replace('http://', '')
                if token:
                    env['DATABRICKS_TOKEN'] = token
                
                logs.append(f"🔧 Using Databricks connection: {databricks_connection.get('connection_name')}")
            
            bundle_result = subprocess.run(
                bundle_cmd, shell=True, capture_output=True, text=True, timeout=600, env=env
            )
            
            if bundle_result.returncode != 0:
                logs.append(f"❌ Bundle operation failed: {bundle_result.stderr}")
                return {
                    'success': False,
                    'logs': logs,
                    'stdout': bundle_result.stdout,
                    'stderr': bundle_result.stderr,
                    'return_code': bundle_result.returncode
                }
            
            logs.append("✅ Bundle operation completed successfully")
            logs.append(f"📄 Output: {bundle_result.stdout}")
            
            return {
                'success': True,
                'logs': logs,
                'stdout': bundle_result.stdout,
                'stderr': bundle_result.stderr,
                'return_code': bundle_result.returncode,
                'work_directory': work_dir
            }
            
        except subprocess.TimeoutExpired:
            logs.append("⏰ Operation timed out")
            return {
                'success': False,
                'logs': logs,
                'error': 'Operation timed out'
            }
        except Exception as e:
            logs.append(f"❌ Operation failed: {str(e)}")
            return {
                'success': False,
                'logs': logs,
                'error': str(e)
            }
        finally:
            # Cleanup temporary directory
            if temp_dir and os.path.exists(temp_dir):
                try:
                    shutil.rmtree(temp_dir)
                    logs.append(f"🧹 Cleaned up temporary directory: {temp_dir}")
                except Exception as e:
                    logs.append(f"⚠️ Failed to cleanup temporary directory: {str(e)}")


def get_bundle_operations(databricks_host: str = None, databricks_token: str = None) -> DatabricksBundleOperations:
    """
    Factory function to create DatabricksBundleOperations instance
    
    Args:
        databricks_host: Databricks workspace host
        databricks_token: Databricks API token
        
    Returns:
        DatabricksBundleOperations instance
    """
    return DatabricksBundleOperations(databricks_host=databricks_host, databricks_token=databricks_token)
