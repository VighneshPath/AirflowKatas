"""
Solution: Exercise 1 - Retry Strategies

This solution demonstrates different retry strategies for handling
transient failures in Airflow tasks.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException, AirflowSkipException
import random
import logging
import time

# Configure logging
logger = logging.getLogger(__name__)

# DAG configuration
default_args = {
    'owner': 'airflow-kata',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=30)
}

dag = DAG(
    'retry_exercise_dag',
    default_args=default_args,
    description='Solution for retry strategies exercise',
    schedule_interval=None,
    catchup=False,
    tags=['error-handling', 'retry', 'exercise', 'solution']
)

# Task 1.1: Basic Retry Configuration


def file_processing_task(**context):
    """
    Simulates reading a file that might be temporarily unavailable.
    Fails 60% of the time on first attempt.
    """
    task_instance = context['task_instance']
    attempt_number = task_instance.try_number

    logger.info(f"File processing attempt #{attempt_number}")

    # Simulate file availability - 60% failure on first attempt, decreasing with retries
    failure_probability = max(0.1, 0.6 - (attempt_number - 1) * 0.15)

    if random.random() < failure_probability:
        logger.error(
            f"File not available on attempt #{attempt_number} (probability: {failure_probability:.2f})")
        raise AirflowException(
            f"File temporarily unavailable (attempt {attempt_number})")

    logger.info(f"File successfully processed on attempt #{attempt_number}")
    return {
        "status": "success",
        "attempt": attempt_number,
        "file_size": "1.2MB",
        "records_processed": 1500
    }


def api_call_task(**context):
    """
    Simulates an API call with network issues.
    Fails 40% on first attempt, 20% on second.
    """
    task_instance = context['task_instance']
    attempt_number = task_instance.try_number

    logger.info(f"API call attempt #{attempt_number}")

    # Simulate network issues with decreasing failure probability
    if attempt_number == 1 and random.random() < 0.4:
        logger.error("Network timeout on first attempt")
        raise AirflowException("API call failed: Network timeout")
    elif attempt_number == 2 and random.random() < 0.2:
        logger.error("Connection refused on second attempt")
        raise AirflowException("API call failed: Connection refused")

    logger.info(f"API call successful on attempt #{attempt_number}")
    return {
        "status": "success",
        "attempt": attempt_number,
        "response_code": 200,
        "data": {"users": 42, "orders": 156, "revenue": 12500.50}
    }

# Task 1.2: Exponential Backoff


def database_connection_task(**context):
    """
    Simulates a database operation with exponential backoff.
    Logs actual delay between attempts.
    """
    task_instance = context['task_instance']
    attempt_number = task_instance.try_number

    logger.info(f"Database connection attempt #{attempt_number}")

    # Log retry delay information
    if attempt_number > 1:
        # Calculate expected delay for this attempt (exponential backoff)
        base_delay = 30  # 30 seconds base delay
        expected_delay = min(
            base_delay * (2 ** (attempt_number - 2)), 600)  # Max 10 minutes
        logger.info(
            f"Retry delay before this attempt: ~{expected_delay} seconds")

    # Simulate database connection issues
    failure_probability = max(0.05, 0.5 - (attempt_number - 1) * 0.1)

    if random.random() < failure_probability:
        error_types = ["Connection timeout",
                       "Database locked", "Too many connections"]
        error = random.choice(error_types)
        logger.error(f"Database error on attempt #{attempt_number}: {error}")
        raise AirflowException(f"Database operation failed: {error}")

    logger.info(f"Database operation successful on attempt #{attempt_number}")
    return {
        "status": "success",
        "attempt": attempt_number,
        "query_time": "0.85s",
        "rows_affected": 234
    }

# Task 1.3: Conditional Retry Logic


def data_validation_task(**context):
    """
    Smart retry logic based on error type.
    Different retry behavior for different types of issues.
    """
    task_instance = context['task_instance']
    attempt_number = task_instance.try_number

    logger.info(f"Data validation attempt #{attempt_number}")

    # Simulate different types of data issues
    issue_type = random.choice(
        ['missing', 'format_error', 'corrupted', 'valid'])

    if issue_type == 'missing':
        # Transient issue - retry
        logger.warning("Data file missing - this might be temporary")
        raise AirflowException("Data file not found - retrying")

    elif issue_type == 'format_error':
        # Permanent issue - don't retry by skipping
        logger.error(
            "Data format is invalid - this won't be fixed by retrying")
        # In a real scenario, you might want to fail permanently
        # For this exercise, we'll skip to show the concept
        raise AirflowSkipException("Data format invalid - skipping task")

    elif issue_type == 'corrupted':
        # Might be fixed by retry, use exponential backoff
        if attempt_number <= 3:
            logger.warning(f"Data appears corrupted - retry #{attempt_number}")
            raise AirflowException(
                "Data corruption detected - retrying with backoff")
        else:
            logger.error("Data still corrupted after multiple attempts")
            raise AirflowException(
                "Data corruption persists - manual intervention required")

    else:  # valid
        logger.info(f"Data validation successful on attempt #{attempt_number}")
        return {
            "status": "valid",
            "attempt": attempt_number,
            "records_validated": 5000,
            "quality_score": 0.98
        }

# Task 1.4: Bash Command Retries


bash_health_check = BashOperator(
    task_id='system_health_check',
    bash_command="""
    echo "Checking system resources (attempt: $AIRFLOW_CTX_TRY_NUMBER)"
    
    # Check disk space (simulate occasional failure)
    DISK_USAGE=$(df / | tail -1 | awk '{print $5}' | sed 's/%//')
    echo "Disk usage: ${DISK_USAGE}%"
    
    # Check memory (simulate occasional failure)
    MEM_USAGE=$(free | grep Mem | awk '{printf("%.0f", $3/$2 * 100.0)}')
    echo "Memory usage: ${MEM_USAGE}%"
    
    # Simulate random failure for demonstration
    if [ $((RANDOM % 4)) -eq 0 ]; then
        echo "System health check failed - resources may be temporarily unavailable"
        exit 1
    fi
    
    echo "System health check passed"
    echo "Status: OK, Disk: ${DISK_USAGE}%, Memory: ${MEM_USAGE}%"
    """,
    retries=3,
    retry_delay=timedelta(seconds=45),
    dag=dag
)

# Configure tasks with different retry strategies

# Task 1.1: Basic retry configuration
file_processing = PythonOperator(
    task_id='file_processing_task',
    python_callable=file_processing_task,
    retries=3,
    retry_delay=timedelta(seconds=30),
    dag=dag
)

api_call = PythonOperator(
    task_id='api_call_task',
    python_callable=api_call_task,
    retries=4,
    retry_delay=timedelta(minutes=1),
    dag=dag
)

# Task 1.2: Exponential backoff
database_connection = PythonOperator(
    task_id='database_connection_task',
    python_callable=database_connection_task,
    retries=5,
    retry_delay=timedelta(seconds=30),
    retry_exponential_backoff=True,
    max_retry_delay=timedelta(minutes=10),
    dag=dag
)

# Task 1.3: Conditional retry logic
data_validation = PythonOperator(
    task_id='data_validation_task',
    python_callable=data_validation_task,
    retries=3,
    retry_delay=timedelta(seconds=30),
    retry_exponential_backoff=True,
    dag=dag
)

# Set up task dependencies
file_processing >> api_call >> database_connection >> data_validation >> bash_health_check

# Additional demonstration tasks


def demonstrate_retry_behavior(**context):
    """
    Demonstrates different retry scenarios for educational purposes.
    """
    task_instance = context['task_instance']
    attempt_number = task_instance.try_number
    task_id = context['task'].task_id

    logger.info(f"Task {task_id} - Attempt #{attempt_number}")

    # Different behavior based on task_id suffix
    if 'always_fail' in task_id:
        raise AirflowException(
            f"Task designed to always fail (attempt {attempt_number})")
    elif 'succeed_on_retry' in task_id:
        if attempt_number == 1:
            raise AirflowException("Failing on first attempt")
        else:
            logger.info("Succeeding on retry")
            return f"Success on attempt {attempt_number}"
    else:
        return f"Task completed on attempt {attempt_number}"


# Demonstration tasks
always_fail_task = PythonOperator(
    task_id='demo_always_fail',
    python_callable=demonstrate_retry_behavior,
    retries=2,
    retry_delay=timedelta(seconds=10),
    dag=dag
)

succeed_on_retry_task = PythonOperator(
    task_id='demo_succeed_on_retry',
    python_callable=demonstrate_retry_behavior,
    retries=2,
    retry_delay=timedelta(seconds=10),
    dag=dag
)

# Add demonstration tasks to the workflow
data_validation >> [always_fail_task, succeed_on_retry_task]
