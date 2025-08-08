"""
Basic Branching Examples for Airflow

This module demonstrates fundamental branching patterns using BranchPythonOperator.
Examples progress from simple conditional logic to more complex data-driven decisions.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

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

# =============================================================================
# Example 1: Simple Time-Based Branching
# =============================================================================


def time_based_branch(**context):
    """
    Simple branching based on current hour.
    Demonstrates basic conditional logic in branching.
    """
    current_hour = datetime.now().hour
    print(f"Current hour: {current_hour}")

    if current_hour < 12:
        print("Choosing morning processing path")
        return 'morning_task'
    else:
        print("Choosing afternoon processing path")
        return 'afternoon_task'


def morning_processing(**context):
    """Morning-specific processing logic."""
    print("Executing morning processing routine")
    print("- Light data validation")
    print("- Quick summary reports")
    return "Morning processing completed"


def afternoon_processing(**context):
    """Afternoon-specific processing logic."""
    print("Executing afternoon processing routine")
    print("- Full data validation")
    print("- Comprehensive reports")
    print("- Data archival")
    return "Afternoon processing completed"


# DAG 1: Time-based branching
dag1 = DAG(
    'example_1_time_based_branching',
    default_args=default_args,
    description='Simple time-based branching example',
    schedule_interval=timedelta(hours=6),
    catchup=False,
    tags=['example', 'branching', 'basic']
)

# Tasks for DAG 1
start_task_1 = DummyOperator(task_id='start', dag=dag1)

time_branch = BranchPythonOperator(
    task_id='time_branch',
    python_callable=time_based_branch,
    dag=dag1
)

morning_task = PythonOperator(
    task_id='morning_task',
    python_callable=morning_processing,
    dag=dag1
)

afternoon_task = PythonOperator(
    task_id='afternoon_task',
    python_callable=afternoon_processing,
    dag=dag1
)

# Join point - executes regardless of which branch ran
join_task_1 = DummyOperator(
    task_id='join_processing',
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag1
)

end_task_1 = DummyOperator(task_id='end', dag=dag1)

# Define dependencies
start_task_1 >> time_branch
time_branch >> [morning_task, afternoon_task]
[morning_task, afternoon_task] >> join_task_1 >> end_task_1

# =============================================================================
# Example 2: Data-Driven Branching
# =============================================================================


def generate_sample_data(**context):
    """Generate sample data for branching decisions."""
    import random

    # Simulate different data scenarios
    scenarios = [
        {'record_count': 50, 'data_quality': 0.95, 'source': 'api'},
        {'record_count': 500, 'data_quality': 0.85, 'source': 'database'},
        {'record_count': 5000, 'data_quality': 0.75, 'source': 'file'},
        {'record_count': 50000, 'data_quality': 0.90, 'source': 'stream'}
    ]

    data = random.choice(scenarios)
    print(f"Generated data scenario: {data}")

    # Push to XCom for branching decision
    return data


def data_size_branch(**context):
    """
    Branch based on data size from previous task.
    Demonstrates XCom-based branching decisions.
    """
    # Pull data from previous task
    data = context['task_instance'].xcom_pull(task_ids='generate_data')
    record_count = data.get('record_count', 0)

    print(f"Processing {record_count} records")

    if record_count > 10000:
        print("Large dataset detected - using distributed processing")
        return 'large_dataset_processing'
    elif record_count > 1000:
        print("Medium dataset detected - using optimized processing")
        return 'medium_dataset_processing'
    else:
        print("Small dataset detected - using standard processing")
        return 'small_dataset_processing'


def small_dataset_processing(**context):
    """Process small datasets with standard methods."""
    data = context['task_instance'].xcom_pull(task_ids='generate_data')
    print(f"Standard processing for {data['record_count']} records")
    print("- Single-threaded processing")
    print("- In-memory operations")
    return {"status": "completed", "method": "standard", "records": data['record_count']}


def medium_dataset_processing(**context):
    """Process medium datasets with optimized methods."""
    data = context['task_instance'].xcom_pull(task_ids='generate_data')
    print(f"Optimized processing for {data['record_count']} records")
    print("- Multi-threaded processing")
    print("- Batch operations")
    return {"status": "completed", "method": "optimized", "records": data['record_count']}


def large_dataset_processing(**context):
    """Process large datasets with distributed methods."""
    data = context['task_instance'].xcom_pull(task_ids='generate_data')
    print(f"Distributed processing for {data['record_count']} records")
    print("- Distributed computing")
    print("- Chunked processing")
    print("- Parallel execution")
    return {"status": "completed", "method": "distributed", "records": data['record_count']}


# DAG 2: Data-driven branching
dag2 = DAG(
    'example_2_data_driven_branching',
    default_args=default_args,
    description='Data-driven branching based on dataset size',
    schedule_interval=timedelta(hours=4),
    catchup=False,
    tags=['example', 'branching', 'data-driven']
)

# Tasks for DAG 2
start_task_2 = DummyOperator(task_id='start', dag=dag2)

generate_data = PythonOperator(
    task_id='generate_data',
    python_callable=generate_sample_data,
    dag=dag2
)

data_branch = BranchPythonOperator(
    task_id='data_size_branch',
    python_callable=data_size_branch,
    dag=dag2
)

small_processing = PythonOperator(
    task_id='small_dataset_processing',
    python_callable=small_dataset_processing,
    dag=dag2
)

medium_processing = PythonOperator(
    task_id='medium_dataset_processing',
    python_callable=medium_dataset_processing,
    dag=dag2
)

large_processing = PythonOperator(
    task_id='large_dataset_processing',
    python_callable=large_dataset_processing,
    dag=dag2
)

join_task_2 = DummyOperator(
    task_id='join_processing',
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag2
)

end_task_2 = DummyOperator(task_id='end', dag=dag2)

# Define dependencies
start_task_2 >> generate_data >> data_branch
data_branch >> [small_processing, medium_processing, large_processing]
[small_processing, medium_processing, large_processing] >> join_task_2 >> end_task_2

# =============================================================================
# Example 3: Multiple Path Branching
# =============================================================================


def check_processing_requirements(**context):
    """
    Determine which processing steps are needed.
    Demonstrates multiple parallel path selection.
    """
    import random

    # Simulate different processing requirements
    requirements = {
        'data_validation': random.choice([True, False]),
        'data_enrichment': random.choice([True, False]),
        'notification_required': random.choice([True, False]),
        'backup_needed': random.choice([True, False])
    }

    print(f"Processing requirements: {requirements}")
    return requirements


def multi_path_branch(**context):
    """
    Select multiple tasks to run based on requirements.
    Returns a list of task_ids to execute in parallel.
    """
    requirements = context['task_instance'].xcom_pull(
        task_ids='check_requirements')

    tasks_to_run = []

    if requirements.get('data_validation'):
        tasks_to_run.append('data_validation_task')
        print("Adding data validation to execution path")

    if requirements.get('data_enrichment'):
        tasks_to_run.append('data_enrichment_task')
        print("Adding data enrichment to execution path")

    if requirements.get('notification_required'):
        tasks_to_run.append('send_notification_task')
        print("Adding notification to execution path")

    if requirements.get('backup_needed'):
        tasks_to_run.append('backup_data_task')
        print("Adding backup to execution path")

    if not tasks_to_run:
        print("No additional processing required")
        return 'no_additional_processing'

    print(f"Selected tasks: {tasks_to_run}")
    return tasks_to_run


def data_validation(**context):
    """Perform data validation."""
    print("Executing data validation")
    print("- Checking data integrity")
    print("- Validating business rules")
    return {"validation_status": "passed", "errors": 0}


def data_enrichment(**context):
    """Perform data enrichment."""
    print("Executing data enrichment")
    print("- Adding external data sources")
    print("- Calculating derived fields")
    return {"enrichment_status": "completed", "fields_added": 5}


def send_notification(**context):
    """Send processing notification."""
    print("Sending notification")
    print("- Preparing notification message")
    print("- Sending to stakeholders")
    return {"notification_sent": True, "recipients": 3}


def backup_data(**context):
    """Backup processed data."""
    print("Backing up data")
    print("- Creating backup archive")
    print("- Storing in backup location")
    return {"backup_status": "completed", "backup_size_mb": 150}


def no_additional_processing(**context):
    """Handle case when no additional processing is needed."""
    print("No additional processing required")
    return {"status": "skipped", "reason": "no_requirements_met"}


# DAG 3: Multiple path branching
dag3 = DAG(
    'example_3_multiple_path_branching',
    default_args=default_args,
    description='Multiple path branching with parallel task execution',
    schedule_interval=timedelta(hours=8),
    catchup=False,
    tags=['example', 'branching', 'multiple-paths']
)

# Tasks for DAG 3
start_task_3 = DummyOperator(task_id='start', dag=dag3)

check_requirements = PythonOperator(
    task_id='check_requirements',
    python_callable=check_processing_requirements,
    dag=dag3
)

multi_branch = BranchPythonOperator(
    task_id='multi_path_branch',
    python_callable=multi_path_branch,
    dag=dag3
)

validation_task = PythonOperator(
    task_id='data_validation_task',
    python_callable=data_validation,
    dag=dag3
)

enrichment_task = PythonOperator(
    task_id='data_enrichment_task',
    python_callable=data_enrichment,
    dag=dag3
)

notification_task = PythonOperator(
    task_id='send_notification_task',
    python_callable=send_notification,
    dag=dag3
)

backup_task = PythonOperator(
    task_id='backup_data_task',
    python_callable=backup_data,
    dag=dag3
)

no_processing_task = PythonOperator(
    task_id='no_additional_processing',
    python_callable=no_additional_processing,
    dag=dag3
)

join_task_3 = DummyOperator(
    task_id='join_processing',
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag3
)

end_task_3 = DummyOperator(task_id='end', dag=dag3)

# Define dependencies
start_task_3 >> check_requirements >> multi_branch
multi_branch >> [validation_task, enrichment_task,
                 notification_task, backup_task, no_processing_task]
[validation_task, enrichment_task, notification_task,
    backup_task, no_processing_task] >> join_task_3 >> end_task_3

# =============================================================================
# Example 4: Environment-Based Branching
# =============================================================================


def get_environment(**context):
    """
    Determine current environment.
    In real scenarios, this might come from Airflow Variables or environment variables.
    """
    import random

    # Simulate different environments
    environments = ['development', 'staging', 'production']
    env = random.choice(environments)

    print(f"Current environment: {env}")
    return {'environment': env, 'timestamp': datetime.now().isoformat()}


def environment_branch(**context):
    """Branch based on deployment environment."""
    env_data = context['task_instance'].xcom_pull(task_ids='get_environment')
    environment = env_data.get('environment')

    print(f"Branching for environment: {environment}")

    if environment == 'production':
        return 'production_processing'
    elif environment == 'staging':
        return 'staging_processing'
    else:
        return 'development_processing'


def production_processing(**context):
    """Production environment processing with full validation."""
    print("Production processing:")
    print("- Full data validation")
    print("- Complete error handling")
    print("- Comprehensive logging")
    print("- Performance monitoring")
    return {"env": "production", "validation_level": "full"}


def staging_processing(**context):
    """Staging environment processing with moderate validation."""
    print("Staging processing:")
    print("- Moderate data validation")
    print("- Standard error handling")
    print("- Debug logging")
    return {"env": "staging", "validation_level": "moderate"}


def development_processing(**context):
    """Development environment processing with minimal validation."""
    print("Development processing:")
    print("- Basic data validation")
    print("- Verbose logging")
    print("- Debug mode enabled")
    return {"env": "development", "validation_level": "basic"}


# DAG 4: Environment-based branching
dag4 = DAG(
    'example_4_environment_branching',
    default_args=default_args,
    description='Environment-based branching for different deployment stages',
    schedule_interval=timedelta(hours=12),
    catchup=False,
    tags=['example', 'branching', 'environment']
)

# Tasks for DAG 4
start_task_4 = DummyOperator(task_id='start', dag=dag4)

get_env = PythonOperator(
    task_id='get_environment',
    python_callable=get_environment,
    dag=dag4
)

env_branch = BranchPythonOperator(
    task_id='environment_branch',
    python_callable=environment_branch,
    dag=dag4
)

prod_task = PythonOperator(
    task_id='production_processing',
    python_callable=production_processing,
    dag=dag4
)

staging_task = PythonOperator(
    task_id='staging_processing',
    python_callable=staging_processing,
    dag=dag4
)

dev_task = PythonOperator(
    task_id='development_processing',
    python_callable=development_processing,
    dag=dag4
)

join_task_4 = DummyOperator(
    task_id='join_processing',
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag4
)

end_task_4 = DummyOperator(task_id='end', dag=dag4)

# Define dependencies
start_task_4 >> get_env >> env_branch
env_branch >> [prod_task, staging_task, dev_task]
[prod_task, staging_task, dev_task] >> join_task_4 >> end_task_4

if __name__ == "__main__":
    # This section runs when the file is executed directly
    # Useful for testing branching logic

    print("Testing branching functions...")

    # Test time-based branching
    print("\n1. Time-based branching:")
    result = time_based_branch()
    print(f"Result: {result}")

    # Test data-driven branching with sample data
    print("\n2. Data-driven branching:")
    sample_context = {
        'task_instance': type('MockTI', (), {
            'xcom_pull': lambda task_ids: {'record_count': 1500}
        })()
    }
    result = data_size_branch(**sample_context)
    print(f"Result: {result}")

    print("\nBranching examples ready for Airflow execution!")
