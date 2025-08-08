"""
Dynamic DAG Generation Examples for Airflow Advanced Patterns

This module demonstrates various techniques for generating DAGs dynamically
based on configuration, enabling scalable and maintainable pipeline architectures.
"""

import os
import yaml
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

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

# Example 1: Configuration-driven DAG Generation


def create_etl_dag(dag_id, config):
    """
    Factory function to create ETL DAGs based on configuration

    Args:
        dag_id (str): Unique identifier for the DAG
        config (dict): Configuration dictionary containing DAG parameters

    Returns:
        DAG: Configured Airflow DAG
    """

    def extract_data(**context):
        source = context['dag_run'].conf.get(
            'source', config.get('source', 'default'))
        print(f"Extracting data from {source}")
        return f"data_from_{source}"

    def transform_data(**context):
        transformation = config.get('transformation', 'default_transform')
        print(f"Applying transformation: {transformation}")
        return f"transformed_data_{transformation}"

    def load_data(**context):
        destination = config.get('destination', 'default_dest')
        print(f"Loading data to {destination}")
        return f"loaded_to_{destination}"

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=config.get('description', f'ETL pipeline for {dag_id}'),
        schedule_interval=config.get('schedule_interval', timedelta(days=1)),
        catchup=config.get('catchup', False),
        tags=['dynamic', 'etl'] + config.get('tags', [])
    )

    with dag:
        start = DummyOperator(task_id='start')

        extract = PythonOperator(
            task_id='extract',
            python_callable=extract_data
        )

        # Create multiple transformation tasks if specified
        transform_tasks = []
        transformations = config.get('transformations', ['default'])

        for i, transform_type in enumerate(transformations):
            transform_task = PythonOperator(
                task_id=f'transform_{i+1}',
                python_callable=transform_data,
                op_kwargs={'transform_type': transform_type}
            )
            transform_tasks.append(transform_task)

        load = PythonOperator(
            task_id='load',
            python_callable=load_data
        )

        end = DummyOperator(task_id='end')

        # Build dependencies
        start >> extract
        if len(transform_tasks) == 1:
            extract >> transform_tasks[0] >> load
        else:
            extract >> transform_tasks >> load
        load >> end

    return dag


# Configuration for multiple ETL pipelines
ETL_CONFIGS = {
    'customer_etl': {
        'description': 'Customer data ETL pipeline',
        'source': 'customer_db',
        'destination': 'data_warehouse',
        'transformations': ['clean', 'normalize', 'enrich'],
        'schedule_interval': timedelta(hours=6),
        'tags': ['customer', 'high-priority']
    },
    'product_etl': {
        'description': 'Product data ETL pipeline',
        'source': 'product_api',
        'destination': 'analytics_db',
        'transformations': ['validate', 'categorize'],
        'schedule_interval': timedelta(hours=12),
        'tags': ['product', 'medium-priority']
    },
    'sales_etl': {
        'description': 'Sales data ETL pipeline',
        'source': 'sales_system',
        'destination': 'reporting_db',
        'transformations': ['aggregate', 'calculate_metrics'],
        'schedule_interval': timedelta(hours=1),
        'tags': ['sales', 'real-time']
    }
}

# Generate DAGs from configuration
for dag_name, config in ETL_CONFIGS.items():
    globals()[dag_name] = create_etl_dag(dag_name, config)

# Example 2: YAML-driven DAG Generation


def load_yaml_config(file_path):
    """Load DAG configuration from YAML file"""
    try:
        with open(file_path, 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        # Return default config if file not found
        return {
            'dags': {
                'yaml_example_dag': {
                    'description': 'Example YAML-driven DAG',
                    'schedule_interval': '@daily',
                    'tasks': [
                        {'name': 'start', 'type': 'dummy'},
                        {'name': 'process', 'type': 'bash',
                            'command': 'echo "Processing data"'},
                        {'name': 'end', 'type': 'dummy'}
                    ]
                }
            }
        }


def create_dag_from_yaml(dag_id, dag_config):
    """Create DAG from YAML configuration"""

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

        # Create tasks based on configuration
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
                def python_task(**context):
                    print(
                        f"Executing Python task: {context['task_instance'].task_id}")
                    return task_config.get('return_value', 'success')

                task = PythonOperator(
                    task_id=task_name,
                    python_callable=python_task
                )
            else:
                # Default to dummy operator for unknown types
                task = DummyOperator(task_id=task_name)

            tasks[task_name] = task

        # Set up dependencies based on configuration
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


# Load YAML configuration and generate DAGs
yaml_config = load_yaml_config('/opt/airflow/dags/config/dag_config.yaml')
for dag_id, dag_config in yaml_config.get('dags', {}).items():
    globals()[dag_id] = create_dag_from_yaml(dag_id, dag_config)

# Example 3: Multi-tenant DAG Generation


def create_tenant_dag(tenant_id, tenant_config):
    """Create tenant-specific DAG"""

    def process_tenant_data(**context):
        tenant = context['dag_run'].conf.get('tenant_id', tenant_id)
        print(f"Processing data for tenant: {tenant}")
        return f"processed_data_for_{tenant}"

    def generate_tenant_report(**context):
        tenant = context['dag_run'].conf.get('tenant_id', tenant_id)
        print(f"Generating report for tenant: {tenant}")
        return f"report_for_{tenant}"

    dag_id = f"tenant_{tenant_id}_pipeline"

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=f"Data pipeline for tenant {tenant_id}",
        schedule_interval=tenant_config.get(
            'schedule_interval', timedelta(days=1)),
        catchup=False,
        tags=['multi-tenant', f'tenant-{tenant_id}'] +
        tenant_config.get('tags', [])
    )

    with dag:
        start = DummyOperator(task_id='start')

        # Tenant-specific data processing
        with TaskGroup(f"process_{tenant_id}_data") as processing_group:
            extract_tenant_data = PythonOperator(
                task_id='extract_data',
                python_callable=process_tenant_data
            )

            validate_data = BashOperator(
                task_id='validate_data',
                bash_command=f'echo "Validating data for {tenant_id}"'
            )

            transform_data = PythonOperator(
                task_id='transform_data',
                python_callable=process_tenant_data
            )

            extract_tenant_data >> validate_data >> transform_data

        # Tenant-specific reporting
        generate_report = PythonOperator(
            task_id='generate_report',
            python_callable=generate_tenant_report
        )

        # Conditional tasks based on tenant configuration
        if tenant_config.get('send_notifications', False):
            send_notification = BashOperator(
                task_id='send_notification',
                bash_command=f'echo "Sending notification to {tenant_id}"'
            )
            generate_report >> send_notification

        end = DummyOperator(task_id='end')

        start >> processing_group >> generate_report >> end

    return dag


# Tenant configurations
TENANT_CONFIGS = {
    'acme_corp': {
        'schedule_interval': timedelta(hours=6),
        'send_notifications': True,
        'tags': ['enterprise', 'high-volume']
    },
    'startup_inc': {
        'schedule_interval': timedelta(days=1),
        'send_notifications': False,
        'tags': ['startup', 'low-volume']
    },
    'global_ltd': {
        'schedule_interval': timedelta(hours=2),
        'send_notifications': True,
        'tags': ['global', 'real-time']
    }
}

# Generate tenant-specific DAGs
for tenant_id, config in TENANT_CONFIGS.items():
    globals()[f"tenant_{tenant_id}_dag"] = create_tenant_dag(tenant_id, config)

# Example 4: Database-driven DAG Generation


def create_table_processing_dag(table_name, table_config):
    """Create DAG for processing specific database table"""

    def process_table(**context):
        table = context['dag_run'].conf.get('table_name', table_name)
        operation = table_config.get('operation', 'sync')
        print(f"Processing table {table} with operation {operation}")
        return f"{operation}_completed_for_{table}"

    dag_id = f"process_{table_name}_table"

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=f"Processing pipeline for {table_name} table",
        schedule_interval=table_config.get(
            'schedule_interval', timedelta(days=1)),
        catchup=False,
        tags=['table-processing', f'table-{table_name}']
    )

    with dag:
        start = DummyOperator(task_id='start')

        # Pre-processing checks
        check_table = BashOperator(
            task_id='check_table_exists',
            bash_command=f'echo "Checking if table {table_name} exists"'
        )

        # Main processing
        process_data = PythonOperator(
            task_id='process_data',
            python_callable=process_table
        )

        # Post-processing tasks based on table configuration
        post_tasks = []

        if table_config.get('create_backup', False):
            backup_task = BashOperator(
                task_id='create_backup',
                bash_command=f'echo "Creating backup for {table_name}"'
            )
            post_tasks.append(backup_task)

        if table_config.get('update_statistics', False):
            stats_task = BashOperator(
                task_id='update_statistics',
                bash_command=f'echo "Updating statistics for {table_name}"'
            )
            post_tasks.append(stats_task)

        if table_config.get('send_alert', False):
            alert_task = BashOperator(
                task_id='send_completion_alert',
                bash_command=f'echo "Sending completion alert for {table_name}"'
            )
            post_tasks.append(alert_task)

        end = DummyOperator(task_id='end')

        # Build workflow
        start >> check_table >> process_data
        if post_tasks:
            process_data >> post_tasks >> end
        else:
            process_data >> end

    return dag


# Table processing configurations
TABLE_CONFIGS = {
    'users': {
        'operation': 'full_sync',
        'schedule_interval': timedelta(hours=12),
        'create_backup': True,
        'update_statistics': True,
        'send_alert': False
    },
    'orders': {
        'operation': 'incremental_sync',
        'schedule_interval': timedelta(hours=1),
        'create_backup': False,
        'update_statistics': True,
        'send_alert': True
    },
    'products': {
        'operation': 'full_refresh',
        'schedule_interval': timedelta(days=1),
        'create_backup': True,
        'update_statistics': False,
        'send_alert': False
    }
}

# Generate table processing DAGs
for table_name, config in TABLE_CONFIGS.items():
    globals()[f"process_{table_name}_dag"] = create_table_processing_dag(
        table_name, config)

# Example 5: Environment-aware DAG Generation


def create_environment_dag(base_dag_id, env_config):
    """Create environment-specific DAG"""

    environment = env_config.get('environment', 'dev')
    dag_id = f"{base_dag_id}_{environment}"

    def environment_task(**context):
        env = context['dag_run'].conf.get('environment', environment)
        print(f"Running task in {env} environment")
        return f"task_completed_in_{env}"

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=f"{base_dag_id} for {environment} environment",
        schedule_interval=env_config.get(
            'schedule_interval', timedelta(days=1)),
        catchup=False,
        tags=[f'env-{environment}', 'environment-specific']
    )

    with dag:
        start = DummyOperator(task_id='start')

        # Environment-specific configuration
        config_task = BashOperator(
            task_id='load_config',
            bash_command=f'echo "Loading {environment} configuration"'
        )

        # Main processing with environment-specific parameters
        main_task = PythonOperator(
            task_id='main_processing',
            python_callable=environment_task
        )

        # Environment-specific validation
        if env_config.get('run_tests', False):
            test_task = BashOperator(
                task_id='run_tests',
                bash_command=f'echo "Running tests in {environment}"'
            )
            main_task >> test_task

        # Environment-specific deployment
        if env_config.get('deploy', False):
            deploy_task = BashOperator(
                task_id='deploy',
                bash_command=f'echo "Deploying to {environment}"'
            )
            if env_config.get('run_tests', False):
                test_task >> deploy_task
            else:
                main_task >> deploy_task

        end = DummyOperator(task_id='end')

        start >> config_task >> main_task >> end

    return dag


# Environment configurations
ENVIRONMENT_CONFIGS = {
    'dev': {
        'environment': 'dev',
        'schedule_interval': timedelta(minutes=30),
        'run_tests': True,
        'deploy': False
    },
    'staging': {
        'environment': 'staging',
        'schedule_interval': timedelta(hours=6),
        'run_tests': True,
        'deploy': True
    },
    'prod': {
        'environment': 'prod',
        'schedule_interval': timedelta(days=1),
        'run_tests': False,
        'deploy': True
    }
}

# Generate environment-specific DAGs
for env, config in ENVIRONMENT_CONFIGS.items():
    globals()[f"data_pipeline_{env}"] = create_environment_dag(
        'data_pipeline', config)
