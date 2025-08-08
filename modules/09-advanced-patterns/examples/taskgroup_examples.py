"""
TaskGroup Examples for Airflow Advanced Patterns

This module demonstrates various TaskGroup patterns for organizing
complex workflows in a readable and maintainable way.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

# Default arguments for all DAGs
default_args = {
    'owner': 'airflow-kata',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def extract_data(**context):
    """Simulate data extraction"""
    print(f"Extracting data for {context['task_instance'].task_id}")
    return f"extracted_data_{context['task_instance'].task_id}"


def transform_data(**context):
    """Simulate data transformation"""
    print(f"Transforming data for {context['task_instance'].task_id}")
    return f"transformed_data_{context['task_instance'].task_id}"


def validate_data(**context):
    """Simulate data validation"""
    print(f"Validating data for {context['task_instance'].task_id}")
    return f"validated_data_{context['task_instance'].task_id}"


def load_data(**context):
    """Simulate data loading"""
    print(f"Loading data for {context['task_instance'].task_id}")
    return "load_complete"


# Example 1: Basic TaskGroup Usage
dag1 = DAG(
    'taskgroup_basic_example',
    default_args=default_args,
    description='Basic TaskGroup example showing ETL organization',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['advanced-patterns', 'taskgroup', 'basic']
)

with dag1:
    start = DummyOperator(task_id='start')

    # ETL TaskGroup
    with TaskGroup("etl_process") as etl_group:
        extract = PythonOperator(
            task_id='extract',
            python_callable=extract_data
        )

        transform = PythonOperator(
            task_id='transform',
            python_callable=transform_data
        )

        validate = PythonOperator(
            task_id='validate',
            python_callable=validate_data
        )

        load = PythonOperator(
            task_id='load',
            python_callable=load_data
        )

        # Define dependencies within the group
        extract >> transform >> validate >> load

    end = DummyOperator(task_id='end')

    # Define overall workflow
    start >> etl_group >> end

# Example 2: Nested TaskGroups
dag2 = DAG(
    'taskgroup_nested_example',
    default_args=default_args,
    description='Nested TaskGroups for complex workflow organization',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['advanced-patterns', 'taskgroup', 'nested']
)

with dag2:
    start = DummyOperator(task_id='start')

    # Main data processing group
    with TaskGroup("data_processing") as main_group:

        # Source A processing
        with TaskGroup("source_a") as source_a_group:
            extract_a = PythonOperator(
                task_id='extract',
                python_callable=extract_data
            )
            transform_a = PythonOperator(
                task_id='transform',
                python_callable=transform_data
            )
            extract_a >> transform_a

        # Source B processing
        with TaskGroup("source_b") as source_b_group:
            extract_b = PythonOperator(
                task_id='extract',
                python_callable=extract_data
            )
            transform_b = PythonOperator(
                task_id='transform',
                python_callable=transform_data
            )
            extract_b >> transform_b

        # Data consolidation
        with TaskGroup("consolidation") as consolidation_group:
            merge = PythonOperator(
                task_id='merge',
                python_callable=lambda: print("Merging data from sources")
            )
            validate_merged = PythonOperator(
                task_id='validate',
                python_callable=validate_data
            )
            load_final = PythonOperator(
                task_id='load',
                python_callable=load_data
            )
            merge >> validate_merged >> load_final

        # Define dependencies between groups
        [source_a_group, source_b_group] >> consolidation_group

    end = DummyOperator(task_id='end')
    start >> main_group >> end

# Example 3: Parallel TaskGroups
dag3 = DAG(
    'taskgroup_parallel_example',
    default_args=default_args,
    description='Parallel TaskGroups for independent processing streams',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['advanced-patterns', 'taskgroup', 'parallel']
)


def create_processing_group(group_id, source_name):
    """Factory function to create processing TaskGroups"""
    with TaskGroup(group_id) as group:
        extract = PythonOperator(
            task_id='extract',
            python_callable=extract_data,
            op_kwargs={'source': source_name}
        )

        # Parallel transformation tasks
        transform_clean = PythonOperator(
            task_id='transform_clean',
            python_callable=transform_data
        )

        transform_enrich = PythonOperator(
            task_id='transform_enrich',
            python_callable=transform_data
        )

        # Validation and loading
        validate = PythonOperator(
            task_id='validate',
            python_callable=validate_data
        )

        load = PythonOperator(
            task_id='load',
            python_callable=load_data
        )

        # Define internal dependencies
        extract >> [transform_clean, transform_enrich] >> validate >> load

    return group


with dag3:
    start = DummyOperator(task_id='start')

    # Create multiple parallel processing streams
    customer_processing = create_processing_group('customer_data', 'customers')
    order_processing = create_processing_group('order_data', 'orders')
    product_processing = create_processing_group('product_data', 'products')

    # Final consolidation
    with TaskGroup("reporting") as reporting_group:
        generate_report = PythonOperator(
            task_id='generate_report',
            python_callable=lambda: print("Generating consolidated report")
        )

        send_notification = BashOperator(
            task_id='send_notification',
            bash_command='echo "Report generation complete"'
        )

        generate_report >> send_notification

    end = DummyOperator(task_id='end')

    # Define overall workflow
    start >> [customer_processing, order_processing,
              product_processing] >> reporting_group >> end

# Example 4: Dynamic TaskGroup Creation
dag4 = DAG(
    'taskgroup_dynamic_example',
    default_args=default_args,
    description='Dynamic TaskGroup creation based on configuration',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['advanced-patterns', 'taskgroup', 'dynamic']
)

# Configuration for dynamic TaskGroup creation
PROCESSING_CONFIG = {
    'databases': ['postgres', 'mysql', 'mongodb'],
    'operations': ['backup', 'analyze', 'cleanup']
}


def create_database_operations(db_name):
    """Create a TaskGroup for database operations"""
    with TaskGroup(f"{db_name}_operations") as db_group:
        for operation in PROCESSING_CONFIG['operations']:
            task = BashOperator(
                task_id=f"{operation}_{db_name}",
                bash_command=f'echo "Performing {operation} on {db_name}"'
            )

            # Create dependencies between operations
            if operation != PROCESSING_CONFIG['operations'][0]:
                prev_operation = PROCESSING_CONFIG['operations'][
                    PROCESSING_CONFIG['operations'].index(operation) - 1
                ]
                prev_task_id = f"{prev_operation}_{db_name}"
                db_group[prev_task_id] >> task

    return db_group


with dag4:
    start = DummyOperator(task_id='start')

    # Dynamically create TaskGroups for each database
    db_groups = []
    for db in PROCESSING_CONFIG['databases']:
        db_group = create_database_operations(db)
        db_groups.append(db_group)

    # Health check group
    with TaskGroup("health_checks") as health_group:
        system_check = BashOperator(
            task_id='system_health',
            bash_command='echo "Checking system health"'
        )

        performance_check = BashOperator(
            task_id='performance_check',
            bash_command='echo "Checking performance metrics"'
        )

        [system_check, performance_check]

    end = DummyOperator(task_id='end')

    # Connect all groups
    start >> db_groups >> health_group >> end

# Example 5: TaskGroup with Error Handling
dag5 = DAG(
    'taskgroup_error_handling_example',
    default_args=default_args,
    description='TaskGroup with comprehensive error handling',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['advanced-patterns', 'taskgroup', 'error-handling']
)


def task_success_callback(context):
    """Callback for successful task completion"""
    print(f"Task {context['task_instance'].task_id} completed successfully")


def task_failure_callback(context):
    """Callback for task failure"""
    print(f"Task {context['task_instance'].task_id} failed")


def group_success_callback(context):
    """Callback for successful group completion"""
    print(f"TaskGroup completed successfully")


with dag5:
    start = DummyOperator(task_id='start')

    # Critical processing group with error handling
    with TaskGroup("critical_processing") as critical_group:
        # High-priority task with retries
        critical_extract = PythonOperator(
            task_id='critical_extract',
            python_callable=extract_data,
            retries=3,
            retry_delay=timedelta(minutes=2),
            on_success_callback=task_success_callback,
            on_failure_callback=task_failure_callback
        )

        # Validation with custom retry logic
        critical_validate = PythonOperator(
            task_id='critical_validate',
            python_callable=validate_data,
            retries=2,
            on_success_callback=task_success_callback,
            on_failure_callback=task_failure_callback
        )

        # Final processing
        critical_process = PythonOperator(
            task_id='critical_process',
            python_callable=transform_data,
            on_success_callback=task_success_callback,
            on_failure_callback=task_failure_callback
        )

        critical_extract >> critical_validate >> critical_process

    # Cleanup group (runs even if critical processing fails)
    with TaskGroup("cleanup") as cleanup_group:
        cleanup_temp = BashOperator(
            task_id='cleanup_temp_files',
            bash_command='echo "Cleaning up temporary files"',
            trigger_rule='all_done'  # Runs regardless of upstream success/failure
        )

        send_status = PythonOperator(
            task_id='send_status_update',
            python_callable=lambda: print("Sending status update"),
            trigger_rule='all_done'
        )

        [cleanup_temp, send_status]

    end = DummyOperator(task_id='end', trigger_rule='all_done')

    start >> critical_group >> cleanup_group >> end
