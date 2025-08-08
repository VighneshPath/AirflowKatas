"""
Solution for Exercise 2: Dynamic DAG Generation

This solution demonstrates how to create DAGs dynamically based on configuration,
enabling scalable and maintainable pipeline architectures for multiple clients.
"""

from pathlib import Path
import yaml
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
import json

# Default arguments for dynamically generated DAGs
default_args = {
    'owner': 'airflow-kata',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Client configurations
CLIENT_CONFIGS = {
    'acme_corp': {
        'description': 'ACME Corporation data pipeline',
        'schedule_interval': timedelta(hours=6),
        'data_sources': ['crm', 'sales', 'inventory'],
        'transformations': ['clean', 'normalize', 'enrich'],
        'destination': 'data_warehouse',
        'notification_email': 'acme@example.com',
        'business_rules': {
            'data_retention_days': 365,
            'quality_threshold': 0.95,
            'enable_real_time': True
        }
    },
    'startup_inc': {
        'description': 'Startup Inc data pipeline',
        'schedule_interval': timedelta(days=1),
        'data_sources': ['app_events', 'user_data'],
        'transformations': ['validate', 'aggregate'],
        'destination': 'analytics_db',
        'notification_email': 'data@startup.com',
        'business_rules': {
            'data_retention_days': 90,
            'quality_threshold': 0.85,
            'enable_real_time': False
        }
    },
    'enterprise_ltd': {
        'description': 'Enterprise Ltd data pipeline',
        'schedule_interval': timedelta(hours=2),
        'data_sources': ['erp', 'crm', 'hr', 'finance'],
        'transformations': ['clean', 'validate', 'normalize', 'enrich', 'aggregate'],
        'destination': 'enterprise_warehouse',
        'notification_email': 'dataops@enterprise.com',
        'business_rules': {
            'data_retention_days': 2555,  # 7 years
            'quality_threshold': 0.99,
            'enable_real_time': True
        }
    }
}


def validate_client_config(client_id, config):
    """Validate client configuration before DAG creation"""
    required_fields = ['description', 'schedule_interval',
                       'data_sources', 'destination']

    for field in required_fields:
        if field not in config:
            raise ValueError(
                f"Missing required field '{field}' for client {client_id}")

    # Validate data sources
    if not isinstance(config['data_sources'], list) or len(config['data_sources']) == 0:
        raise ValueError(
            f"data_sources must be a non-empty list for client {client_id}")

    # Validate transformations
    if 'transformations' in config:
        if not isinstance(config['transformations'], list):
            raise ValueError(
                f"transformations must be a list for client {client_id}")

    # Validate business rules
    if 'business_rules' in config:
        rules = config['business_rules']
        if 'quality_threshold' in rules:
            threshold = rules['quality_threshold']
            if not isinstance(threshold, (int, float)) or not 0 <= threshold <= 1:
                raise ValueError(
                    f"quality_threshold must be between 0 and 1 for client {client_id}")

    print(f"✓ Configuration validation passed for client {client_id}")
    return True


def create_client_dag(client_id, config):
    """
    Factory function to create client-specific DAGs

    Args:
        client_id (str): Unique client identifier
        config (dict): Client configuration dictionary

    Returns:
        DAG: Configured Airflow DAG
    """

    # Validate configuration first
    validate_client_config(client_id, config)

    dag_id = f"client_{client_id}_pipeline"

    # Data processing functions
    def extract_data(source_name, client_id, **context):
        """Extract data from specified source"""
        print(f"🔄 Extracting {source_name} data for client {client_id}")
        print(f"   - Connecting to {source_name} system")
        print(f"   - Applying client-specific filters")
        print(f"   - Extracting data for processing")
        print(
            f"✓ Data extraction from {source_name} completed for {client_id}")
        return f"extracted_{source_name}_data_for_{client_id}"

    def apply_transformation(transform_type, client_id, **context):
        """Apply specified transformation"""
        print(
            f"🔧 Applying {transform_type} transformation for client {client_id}")

        if transform_type == 'clean':
            print("   - Removing invalid records")
            print("   - Standardizing data formats")
        elif transform_type == 'normalize':
            print("   - Normalizing data structures")
            print("   - Converting data types")
        elif transform_type == 'enrich':
            print("   - Adding derived fields")
            print("   - Joining with reference data")
        elif transform_type == 'validate':
            print("   - Running data validation rules")
            print("   - Checking data integrity")
        elif transform_type == 'aggregate':
            print("   - Calculating aggregations")
            print("   - Creating summary statistics")

        print(f"✓ {transform_type} transformation completed for {client_id}")
        return f"transformed_data_{transform_type}_for_{client_id}"

    def validate_quality(threshold, client_id, **context):
        """Validate data quality against threshold"""
        import random

        # Simulate quality check
        quality_score = random.uniform(0.8, 1.0)

        print(f"🔍 Quality validation for client {client_id}")
        print(f"   - Quality score: {quality_score:.3f}")
        print(f"   - Threshold: {threshold}")

        if quality_score >= threshold:
            print(
                f"✓ Quality check passed for {client_id}: {quality_score:.3f} >= {threshold}")
            return {
                'status': 'passed',
                'score': quality_score,
                'client': client_id
            }
        else:
            print(
                f"✗ Quality check failed for {client_id}: {quality_score:.3f} < {threshold}")
            raise ValueError(
                f"Quality check failed: {quality_score:.3f} < {threshold}")

    def load_data(destination, client_id, **context):
        """Load data to specified destination"""
        print(f"📤 Loading data to {destination} for client {client_id}")
        print(f"   - Connecting to {destination}")
        print(f"   - Creating client-specific tables")
        print(f"   - Loading processed data")
        print(f"   - Updating metadata")
        print(f"✓ Data loading to {destination} completed for {client_id}")
        return f"loaded_to_{destination}_for_{client_id}"

    def cleanup_old_data(retention_days, client_id, **context):
        """Clean up old data based on retention policy"""
        print(
            f"🧹 Cleaning up data older than {retention_days} days for client {client_id}")
        print(f"   - Identifying records older than {retention_days} days")
        print(f"   - Archiving old records")
        print(f"   - Removing expired data")
        print(f"✓ Data cleanup completed for {client_id}")
        return f"cleanup_completed_for_{client_id}"

    def send_notification(email, client_id, status, **context):
        """Send notification to client"""
        print(
            f"📧 Sending {status} notification to {email} for client {client_id}")
        return f"notification_sent_to_{email}"

    def real_time_processing(client_id, **context):
        """Handle real-time processing if enabled"""
        print(f"⚡ Processing real-time data stream for client {client_id}")
        print("   - Connecting to streaming data source")
        print("   - Processing incremental updates")
        print("   - Updating real-time dashboards")
        print(f"✓ Real-time processing completed for {client_id}")
        return f"real_time_processed_for_{client_id}"

    # Failure callback functions
    def extraction_failure_callback(context):
        """Handle extraction failures"""
        task_id = context['task_instance'].task_id
        print(f"🚨 Extraction failure in {task_id} for client {client_id}")
        print("   - Alerting data engineering team")
        print("   - Checking source system status")

    def quality_failure_callback(context):
        """Handle quality check failures"""
        task_id = context['task_instance'].task_id
        print(f"🚨 Quality check failure in {task_id} for client {client_id}")
        print("   - Alerting data quality team")
        print("   - Initiating data investigation")

    def loading_failure_callback(context):
        """Handle loading failures"""
        task_id = context['task_instance'].task_id
        print(f"🚨 Loading failure in {task_id} for client {client_id}")
        print("   - Checking destination system status")
        print("   - Preparing for retry")

    # Create the DAG
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=config.get(
            'description', f'Data pipeline for client {client_id}'),
        schedule_interval=config.get('schedule_interval', timedelta(days=1)),
        catchup=config.get('catchup', False),
        tags=['dynamic', 'client-specific',
              f'client-{client_id}'] + config.get('tags', [])
    )

    with dag:
        start = DummyOperator(task_id='start')

        # Dynamic data source extraction tasks
        extraction_tasks = []
        for source in config.get('data_sources', []):
            extract_task = PythonOperator(
                task_id=f'extract_{source}',
                python_callable=extract_data,
                op_kwargs={'source_name': source, 'client_id': client_id},
                on_failure_callback=extraction_failure_callback
            )
            extraction_tasks.append(extract_task)

        # Data consolidation after extraction
        consolidate_sources = PythonOperator(
            task_id='consolidate_sources',
            python_callable=lambda client=client_id, **ctx:
                print(f"📋 Consolidating data from all sources for {client}")
        )

        # Dynamic transformation tasks
        transformation_tasks = []
        transformations = config.get('transformations', ['default'])

        for i, transform_type in enumerate(transformations):
            transform_task = PythonOperator(
                task_id=f'transform_{i+1}_{transform_type}',
                python_callable=apply_transformation,
                op_kwargs={'transform_type': transform_type,
                           'client_id': client_id}
            )
            transformation_tasks.append(transform_task)

        # Quality validation
        business_rules = config.get('business_rules', {})
        quality_threshold = business_rules.get('quality_threshold', 0.9)

        quality_check = PythonOperator(
            task_id='quality_validation',
            python_callable=validate_quality,
            op_kwargs={'threshold': quality_threshold, 'client_id': client_id},
            retries=2,
            on_failure_callback=quality_failure_callback
        )

        # Data loading
        destination = config.get('destination', 'default_warehouse')
        load_task = PythonOperator(
            task_id='load_data',
            python_callable=load_data,
            op_kwargs={'destination': destination, 'client_id': client_id},
            retries=3,
            on_failure_callback=loading_failure_callback
        )

        # Conditional real-time processing
        real_time_tasks = []
        if business_rules.get('enable_real_time', False):
            real_time_task = PythonOperator(
                task_id='real_time_processing',
                python_callable=real_time_processing,
                op_kwargs={'client_id': client_id}
            )
            real_time_tasks.append(real_time_task)

        # Data cleanup based on retention policy
        retention_days = business_rules.get('data_retention_days', 365)
        cleanup_task = PythonOperator(
            task_id='cleanup_old_data',
            python_callable=cleanup_old_data,
            op_kwargs={'retention_days': retention_days,
                       'client_id': client_id}
        )

        # Success notification
        notification_email = config.get(
            'notification_email', 'default@example.com')
        success_notification = PythonOperator(
            task_id='send_success_notification',
            python_callable=send_notification,
            op_kwargs={
                'email': notification_email,
                'client_id': client_id,
                'status': 'success'
            }
        )

        # Failure notification
        failure_notification = PythonOperator(
            task_id='send_failure_notification',
            python_callable=send_notification,
            op_kwargs={
                'email': notification_email,
                'client_id': client_id,
                'status': 'failure'
            },
            trigger_rule='one_failed'
        )

        end = DummyOperator(
            task_id='end', trigger_rule='none_failed_min_one_success')

        # Build dependencies
        start >> extraction_tasks >> consolidate_sources

        # Chain transformation tasks
        if len(transformation_tasks) == 1:
            consolidate_sources >> transformation_tasks[0] >> quality_check
        else:
            consolidate_sources >> transformation_tasks[0]
            for i in range(len(transformation_tasks) - 1):
                transformation_tasks[i] >> transformation_tasks[i + 1]
            transformation_tasks[-1] >> quality_check

        # Continue with loading and optional real-time processing
        quality_check >> load_task

        if real_time_tasks:
            load_task >> real_time_tasks[0] >> cleanup_task
        else:
            load_task >> cleanup_task

        cleanup_task >> success_notification >> end

        # Connect failure notification
        [consolidate_sources] + transformation_tasks + \
            [quality_check, load_task] >> failure_notification

    return dag


# Generate DAGs for all clients
for client_id, config in CLIENT_CONFIGS.items():
    try:
        dag_id = f"client_{client_id}_pipeline"
        globals()[dag_id] = create_client_dag(client_id, config)
        print(f"✓ Successfully created DAG for client: {client_id}")
    except Exception as e:
        print(f"✗ Failed to create DAG for client {client_id}: {str(e)}")

# Example of YAML-driven DAG generation (Bonus Challenge)


def load_yaml_config(file_path):
    """Load DAG configuration from YAML file"""
    try:
        config_path = Path(file_path)
        if config_path.exists():
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
        else:
            print(f"⚠ YAML config file not found: {file_path}")
            return None
    except Exception as e:
        print(f"✗ Error loading YAML config: {str(e)}")
        return None


def create_dag_from_yaml(dag_id, dag_config):
    """Create DAG from YAML configuration"""

    def yaml_task_callable(task_config, **context):
        """Generic callable for YAML-defined tasks"""
        task_name = task_config.get('name', 'unknown')
        task_type = task_config.get('type', 'unknown')
        print(f"🔄 Executing YAML task: {task_name} (type: {task_type})")

        if 'command' in task_config:
            print(f"   Command: {task_config['command']}")

        return task_config.get('return_value', f'{task_name}_completed')

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=dag_config.get(
            'description', f'YAML-generated DAG: {dag_id}'),
        schedule_interval=dag_config.get('schedule_interval', '@daily'),
        catchup=dag_config.get('catchup', False),
        tags=['yaml-generated'] + dag_config.get('tags', [])
    )

    with dag:
        tasks = {}

        # Create tasks based on YAML configuration
        for task_config in dag_config.get('tasks', []):
            task_name = task_config['name']
            task_type = task_config['type']

            if task_type == 'dummy':
                task = DummyOperator(task_id=task_name)
            elif task_type == 'bash':
                task = BashOperator(
                    task_id=task_name,
                    bash_command=task_config.get(
                        'command', 'echo "No command specified"')
                )
            elif task_type == 'python':
                task = PythonOperator(
                    task_id=task_name,
                    python_callable=yaml_task_callable,
                    op_kwargs={'task_config': task_config}
                )
            else:
                # Default to dummy operator for unknown types
                task = DummyOperator(task_id=task_name)

            tasks[task_name] = task

        # Set up dependencies based on YAML configuration
        dependencies = dag_config.get('dependencies', [])
        for dep in dependencies:
            upstream = dep.get('upstream')
            downstream = dep.get('downstream')
            if upstream in tasks and downstream in tasks:
                tasks[upstream] >> tasks[downstream]

        # If no dependencies specified, create linear chain
        if not dependencies and len(tasks) > 1:
            task_list = list(tasks.values())
            for i in range(len(task_list) - 1):
                task_list[i] >> task_list[i + 1]

    return dag


# Example YAML configuration (would normally be loaded from file)
YAML_CONFIG_EXAMPLE = {
    'dags': {
        'yaml_example_pipeline': {
            'description': 'Example YAML-driven data pipeline',
            'schedule_interval': '@daily',
            'catchup': False,
            'tags': ['yaml', 'example'],
            'tasks': [
                {'name': 'start', 'type': 'dummy'},
                {'name': 'extract_data', 'type': 'bash',
                    'command': 'echo "Extracting data from source"'},
                {'name': 'process_data', 'type': 'python',
                    'return_value': 'processing_complete'},
                {'name': 'load_data', 'type': 'bash',
                    'command': 'echo "Loading data to destination"'},
                {'name': 'end', 'type': 'dummy'}
            ],
            'dependencies': [
                {'upstream': 'start', 'downstream': 'extract_data'},
                {'upstream': 'extract_data', 'downstream': 'process_data'},
                {'upstream': 'process_data', 'downstream': 'load_data'},
                {'upstream': 'load_data', 'downstream': 'end'}
            ]
        }
    }
}

# Generate DAGs from YAML configuration
for dag_id, dag_config in YAML_CONFIG_EXAMPLE.get('dags', {}).items():
    try:
        globals()[dag_id] = create_dag_from_yaml(dag_id, dag_config)
        print(f"✓ Successfully created YAML DAG: {dag_id}")
    except Exception as e:
        print(f"✗ Failed to create YAML DAG {dag_id}: {str(e)}")

# Environment-aware DAG generation example
ENVIRONMENT_CONFIGS = {
    'dev': {
        'environment': 'dev',
        'schedule_interval': timedelta(minutes=30),
        'run_tests': True,
        'deploy': False,
        'resource_limits': {'cpu': '1', 'memory': '2Gi'}
    },
    'staging': {
        'environment': 'staging',
        'schedule_interval': timedelta(hours=6),
        'run_tests': True,
        'deploy': True,
        'resource_limits': {'cpu': '2', 'memory': '4Gi'}
    },
    'prod': {
        'environment': 'prod',
        'schedule_interval': timedelta(days=1),
        'run_tests': False,
        'deploy': True,
        'resource_limits': {'cpu': '4', 'memory': '8Gi'}
    }
}


def create_environment_dag(base_dag_id, env_config):
    """Create environment-specific DAG"""

    environment = env_config.get('environment', 'dev')
    dag_id = f"{base_dag_id}_{environment}"

    def environment_task(task_name, **context):
        """Environment-aware task execution"""
        env = context['dag_run'].conf.get('environment', environment)
        limits = env_config.get('resource_limits', {})

        print(f"🌍 Running {task_name} in {env} environment")
        print(f"   Resource limits: {limits}")
        print(f"   Task execution completed")
        return f"{task_name}_completed_in_{env}"

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=f"Environment-specific pipeline for {environment}",
        schedule_interval=env_config.get(
            'schedule_interval', timedelta(days=1)),
        catchup=False,
        tags=[f'env-{environment}', 'environment-specific']
    )

    with dag:
        start = DummyOperator(task_id='start')

        # Environment-specific configuration loading
        load_config = PythonOperator(
            task_id='load_config',
            python_callable=environment_task,
            op_kwargs={'task_name': 'load_config'}
        )

        # Main processing
        main_processing = PythonOperator(
            task_id='main_processing',
            python_callable=environment_task,
            op_kwargs={'task_name': 'main_processing'}
        )

        # Conditional testing
        test_tasks = []
        if env_config.get('run_tests', False):
            run_tests = PythonOperator(
                task_id='run_tests',
                python_callable=environment_task,
                op_kwargs={'task_name': 'run_tests'}
            )
            test_tasks.append(run_tests)

        # Conditional deployment
        deploy_tasks = []
        if env_config.get('deploy', False):
            deploy = PythonOperator(
                task_id='deploy',
                python_callable=environment_task,
                op_kwargs={'task_name': 'deploy'}
            )
            deploy_tasks.append(deploy)

        end = DummyOperator(task_id='end')

        # Build workflow
        start >> load_config >> main_processing

        if test_tasks and deploy_tasks:
            main_processing >> test_tasks[0] >> deploy_tasks[0] >> end
        elif test_tasks:
            main_processing >> test_tasks[0] >> end
        elif deploy_tasks:
            main_processing >> deploy_tasks[0] >> end
        else:
            main_processing >> end

    return dag


# Generate environment-specific DAGs
for env, config in ENVIRONMENT_CONFIGS.items():
    try:
        dag_id = f"data_pipeline_{env}"
        globals()[dag_id] = create_environment_dag('data_pipeline', config)
        print(f"✓ Successfully created environment DAG: {dag_id}")
    except Exception as e:
        print(f"✗ Failed to create environment DAG for {env}: {str(e)}")
