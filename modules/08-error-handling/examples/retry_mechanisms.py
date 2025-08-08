"""
Error Handling Examples: Retry Mechanisms

This module demonstrates different retry strategies and configurations
for handling transient failures in Airflow tasks.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException
import random
import logging

# Configure logging
logger = logging.getLogger(__name__)

# Default arguments with basic retry configuration
default_args = {
    'owner': 'airflow-kata',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    'retry_mechanisms_examples',
    default_args=default_args,
    description='Examples of different retry strategies',
    schedule_interval=None,
    catchup=False,
    tags=['error-handling', 'retry', 'examples']
)


def simulate_transient_failure(**context):
    """
    Simulates a task that fails randomly but might succeed on retry.
    This represents common transient failures like network timeouts.
    """
    task_instance = context['task_instance']
    attempt_number = task_instance.try_number

    logger.info(f"Attempt #{attempt_number}")

    # Simulate 70% failure rate on first attempt, decreasing with retries
    failure_probability = max(0.1, 0.7 - (attempt_number - 1) * 0.2)

    if random.random() < failure_probability:
        logger.error(f"Simulated failure on attempt #{attempt_number}")
        raise AirflowException(
            f"Transient failure occurred (attempt {attempt_number})")

    logger.info(f"Task succeeded on attempt #{attempt_number}")
    return f"Success after {attempt_number} attempts"


def simulate_network_call(**context):
    """
    Simulates a network call that might fail due to timeouts or connectivity issues.
    """
    import time

    task_instance = context['task_instance']
    attempt_number = task_instance.try_number

    logger.info(f"Making network call (attempt #{attempt_number})")

    # Simulate network delay
    time.sleep(2)

    # Higher chance of success on later attempts
    if attempt_number == 1 and random.random() < 0.6:
        raise AirflowException("Network timeout")
    elif attempt_number == 2 and random.random() < 0.3:
        raise AirflowException("Connection refused")

    logger.info("Network call successful")
    return {"status": "success", "data": "network_response"}


def database_operation_with_retry(**context):
    """
    Simulates a database operation that might fail due to locks or connectivity.
    """
    task_instance = context['task_instance']
    attempt_number = task_instance.try_number

    logger.info(f"Executing database operation (attempt #{attempt_number})")

    # Simulate different types of database errors
    if attempt_number == 1 and random.random() < 0.4:
        raise AirflowException("Database connection timeout")
    elif attempt_number == 2 and random.random() < 0.2:
        raise AirflowException("Deadlock detected")

    logger.info("Database operation completed successfully")
    return {"rows_affected": 42, "execution_time": "1.2s"}


# Task with basic retry configuration (uses DAG defaults)
basic_retry_task = PythonOperator(
    task_id='basic_retry_example',
    python_callable=simulate_transient_failure,
    dag=dag
)

# Task with custom retry configuration
custom_retry_task = PythonOperator(
    task_id='custom_retry_example',
    python_callable=simulate_network_call,
    retries=5,
    retry_delay=timedelta(minutes=1),
    dag=dag
)

# Task with exponential backoff
exponential_backoff_task = PythonOperator(
    task_id='exponential_backoff_example',
    python_callable=database_operation_with_retry,
    retries=4,
    retry_delay=timedelta(seconds=30),
    retry_exponential_backoff=True,
    max_retry_delay=timedelta(minutes=10),
    dag=dag
)

# Bash task with retry configuration
bash_retry_task = BashOperator(
    task_id='bash_retry_example',
    bash_command="""
    # Simulate a command that might fail
    if [ $((RANDOM % 3)) -eq 0 ]; then
        echo "Command succeeded"
        exit 0
    else
        echo "Command failed"
        exit 1
    fi
    """,
    retries=2,
    retry_delay=timedelta(seconds=30),
    dag=dag
)

# Task that should not retry (for permanent failures)
no_retry_task = PythonOperator(
    task_id='no_retry_example',
    python_callable=lambda: exec(
        'raise ValueError("This is a permanent error - should not retry")'),
    retries=0,  # No retries for this task
    dag=dag
)

# Set up task dependencies
basic_retry_task >> custom_retry_task >> exponential_backoff_task
bash_retry_task >> no_retry_task
