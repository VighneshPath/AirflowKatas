"""
[DAG Name] - [Brief Description]

This DAG demonstrates [key concepts being illustrated].
It implements [main functionality] and shows how to [learning objectives].

Author: [Your Name]
Created: [Date]
Module: [Module Number and Name]
Exercise: [Exercise Number and Name] (if applicable)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
# Add other imports as needed

# DAG Configuration
DAG_ID = '[descriptive_dag_id]'
DESCRIPTION = '[Clear description of what this DAG does]'
SCHEDULE_INTERVAL = timedelta(days=1)  # Adjust as appropriate
START_DATE = datetime(2024, 1, 1)
CATCHUP = False
MAX_ACTIVE_RUNS = 1
TAGS = ['kata', 'module-name', 'concept-tag']

# Default arguments for all tasks
default_args = {
    'owner': 'kata-student',
    'depends_on_past': False,
    'start_date': START_DATE,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # Add SLA if appropriate
    # 'sla': timedelta(hours=2),
}

# Create the DAG
dag = DAG(
    DAG_ID,
    default_args=default_args,
    description=DESCRIPTION,
    schedule_interval=SCHEDULE_INTERVAL,
    catchup=CATCHUP,
    max_active_runs=MAX_ACTIVE_RUNS,
    tags=TAGS,
)

# Task Functions


def example_python_function(**context):
    """
    Example Python function for PythonOperator.

    This function demonstrates [specific concept or pattern].

    Args:
        **context: Airflow context dictionary containing task instance,
                  execution date, and other runtime information.

    Returns:
        [Description of return value if applicable]
    """
    # Access context information
    task_instance = context['task_instance']
    execution_date = context['execution_date']

    # Log important information
    print(f"Executing task: {task_instance.task_id}")
    print(f"Execution date: {execution_date}")

    # Implement your logic here
    result = "example_result"

    # Return data for XCom if needed
    return result


def data_processing_function(**context):
    """
    Example data processing function.

    Demonstrates [specific data processing concept].
    """
    # Pull data from previous task if needed
    # previous_result = context['task_instance'].xcom_pull(task_ids='previous_task')

    # Process data
    processed_data = {
        'status': 'processed',
        'timestamp': datetime.now().isoformat(),
        'record_count': 100
    }

    # Log processing results
    print(f"Processed {processed_data['record_count']} records")

    return processed_data


def validation_function(**context):
    """
    Example validation function.

    Demonstrates [validation concept or error handling].
    """
    # Pull data from previous task
    data = context['task_instance'].xcom_pull(task_ids='data_processing_task')

    # Perform validation
    if not data or data.get('record_count', 0) == 0:
        raise ValueError("No data to validate")

    # Validation logic
    is_valid = data['record_count'] > 0

    if not is_valid:
        raise ValueError("Data validation failed")

    print("Data validation passed")
    return {'validation_status': 'passed'}


# Task Definitions

# Task 1: Example Python task
python_task = PythonOperator(
    task_id='python_task_example',
    python_callable=example_python_function,
    dag=dag,
    # Add task-specific configurations
    # pool='default_pool',
    # priority_weight=1,
)

# Task 2: Example Bash task
bash_task = BashOperator(
    task_id='bash_task_example',
    bash_command='''
    echo "Starting bash task..."
    echo "Current date: $(date)"
    echo "Task completed successfully"
    ''',
    dag=dag,
)

# Task 3: Data processing task
data_processing_task = PythonOperator(
    task_id='data_processing_task',
    python_callable=data_processing_function,
    dag=dag,
)

# Task 4: Validation task
validation_task = PythonOperator(
    task_id='validation_task',
    python_callable=validation_function,
    dag=dag,
)

# Task 5: Final task (example of different patterns)
final_task = BashOperator(
    task_id='final_task',
    bash_command='echo "Pipeline completed successfully"',
    dag=dag,
)

# Define task dependencies
# Method 1: Using >> operator (recommended)
python_task >> bash_task >> data_processing_task >> validation_task >> final_task

# Method 2: Using set_downstream (alternative)
# python_task.set_downstream(bash_task)
# bash_task.set_downstream(data_processing_task)

# Method 3: Using set_upstream (alternative)
# bash_task.set_upstream(python_task)

# Method 4: Using lists for fan-out/fan-in patterns
# start_task >> [parallel_task_1, parallel_task_2] >> end_task

# Optional: Add task groups for complex workflows
# from airflow.utils.task_group import TaskGroup
#
# with TaskGroup('processing_group', dag=dag) as processing_group:
#     group_task_1 = PythonOperator(
#         task_id='group_task_1',
#         python_callable=example_python_function,
#     )
#
#     group_task_2 = PythonOperator(
#         task_id='group_task_2',
#         python_callable=example_python_function,
#     )
#
#     group_task_1 >> group_task_2

# Optional: Add callbacks for error handling


def task_failure_callback(context):
    """
    Callback function called when a task fails.

    Args:
        context: Airflow context dictionary
    """
    task_instance = context['task_instance']
    print(f"Task {task_instance.task_id} failed!")
    # Add notification logic here (email, Slack, etc.)


def dag_success_callback(context):
    """
    Callback function called when the entire DAG succeeds.

    Args:
        context: Airflow context dictionary
    """
    print("DAG completed successfully!")
    # Add success notification logic here


def dag_failure_callback(context):
    """
    Callback function called when the DAG fails.

    Args:
        context: Airflow context dictionary
    """
    print("DAG failed!")
    # Add failure notification logic here


# Apply callbacks to specific tasks if needed
# python_task.on_failure_callback = task_failure_callback

# Apply callbacks to the entire DAG if needed
# dag.on_success_callback = dag_success_callback
# dag.on_failure_callback = dag_failure_callback

# Optional: Add SLA miss callback
def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """
    Callback function called when SLA is missed.

    Args:
        dag: The DAG object
        task_list: List of tasks that missed SLA
        blocking_task_list: List of tasks that are blocking
        slas: List of SLA objects
        blocking_tis: List of blocking task instances
    """
    print(f"SLA missed for tasks: {[task.task_id for task in task_list]}")
    # Add SLA miss notification logic here


# Apply SLA callback to DAG if needed
# dag.sla_miss_callback = sla_miss_callback

# Documentation and metadata
dag.doc_md = """
## [DAG Name] Documentation

### Purpose
[Explain the purpose and business value of this DAG]

### Schedule
- **Frequency**: [How often it runs]
- **Start Date**: [When it starts]
- **Catchup**: [Whether it catches up on missed runs]

### Tasks Overview
1. **python_task_example**: [Description of what this task does]
2. **bash_task_example**: [Description of what this task does]
3. **data_processing_task**: [Description of what this task does]
4. **validation_task**: [Description of what this task does]
5. **final_task**: [Description of what this task does]

### Dependencies
[Describe the task dependencies and why they're structured this way]

### Error Handling
[Describe error handling strategy and retry logic]

### Monitoring
[Describe how to monitor this DAG and what to watch for]

### Troubleshooting
[Common issues and how to resolve them]
"""

# Task-specific documentation (optional)
python_task.doc_md = """
### Python Task Documentation
[Detailed description of what this specific task does]
"""

bash_task.doc_md = """
### Bash Task Documentation
[Detailed description of what this specific task does]
"""
