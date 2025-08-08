"""
Solution: Exercise 2 - Failure Callbacks

This solution demonstrates comprehensive failure callback implementation
for notifications, cleanup, and monitoring.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
import logging
import json
import tempfile
import os
from typing import Dict, Any, List

# Configure logging
logger = logging.getLogger(__name__)

# Global variables to simulate external resources
TEMP_FILES = []
MOCK_CONNECTIONS = []

# DAG configuration
default_args = {
    'owner': 'airflow-kata',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'callback_exercise_dag',
    default_args=default_args,
    description='Solution for failure callbacks exercise',
    schedule_interval=None,
    catchup=False,
    tags=['error-handling', 'callbacks', 'exercise', 'solution']
)

# Task 2.1: Notification Callbacks


def send_email_alert(context: Dict[str, Any]) -> None:
    """
    Send email notification on task failure.
    In production, this would integrate with Airflow's email system.
    """
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    execution_date = context['execution_date']
    exception = context.get('exception', 'Unknown error')

    # Format email content
    email_subject = f"🚨 Airflow Task Failed: {dag_id}.{task_id}"
    email_body = f"""
    AIRFLOW TASK FAILURE NOTIFICATION
    
    Task Details:
    - DAG ID: {dag_id}
    - Task ID: {task_id}
    - Execution Date: {execution_date}
    - Try Number: {task_instance.try_number}
    - Max Tries: {task_instance.max_tries}
    - Duration: {task_instance.duration}
    
    Error Information:
    - Error Type: {type(exception).__name__}
    - Error Message: {str(exception)}
    
    Task Instance Details:
    - State: {task_instance.state}
    - Start Date: {task_instance.start_date}
    - End Date: {task_instance.end_date}
    
    Debugging Information:
    - Log URL: http://localhost:8080/log?dag_id={dag_id}&task_id={task_id}&execution_date={execution_date}
    - Task Instance URL: http://localhost:8080/task?dag_id={dag_id}&task_id={task_id}&execution_date={execution_date}
    
    Please investigate and resolve the issue promptly.
    
    Airflow Monitoring System
    """

    # Log the email (in production, send actual email)
    logger.error(f"EMAIL ALERT SENT:")
    logger.error(f"Subject: {email_subject}")
    logger.error(f"Body: {email_body}")

    # In production, you would use:
    # from airflow.utils.email import send_email
    # send_email(to=['admin@company.com'], subject=email_subject, html_content=email_body)


def send_slack_alert(context: Dict[str, Any]) -> None:
    """
    Send Slack notification with rich formatting.
    """
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    exception = context.get('exception', 'Unknown error')

    # Determine error severity
    error_type = type(exception).__name__
    if 'timeout' in str(exception).lower():
        severity = 'warning'
        color = 'warning'
        emoji = '⚠️'
    else:
        severity = 'error'
        color = 'danger'
        emoji = '🚨'

    # Create Slack message payload
    slack_message = {
        "text": f"{emoji} Airflow Task Failed",
        "attachments": [
            {
                "color": color,
                "title": f"Task Failure: {dag_id}.{task_id}",
                "fields": [
                    {
                        "title": "DAG",
                        "value": dag_id,
                        "short": True
                    },
                    {
                        "title": "Task",
                        "value": task_id,
                        "short": True
                    },
                    {
                        "title": "Attempt",
                        "value": f"{task_instance.try_number}/{task_instance.max_tries}",
                        "short": True
                    },
                    {
                        "title": "Severity",
                        "value": severity.upper(),
                        "short": True
                    },
                    {
                        "title": "Error Type",
                        "value": error_type,
                        "short": True
                    },
                    {
                        "title": "Duration",
                        "value": str(task_instance.duration) if task_instance.duration else "N/A",
                        "short": True
                    },
                    {
                        "title": "Error Message",
                        "value": str(exception)[:200] + "..." if len(str(exception)) > 200 else str(exception),
                        "short": False
                    }
                ],
                "actions": [
                    {
                        "type": "button",
                        "text": "View Logs",
                        "url": f"http://localhost:8080/log?dag_id={dag_id}&task_id={task_id}"
                    },
                    {
                        "type": "button",
                        "text": "View Task",
                        "url": f"http://localhost:8080/task?dag_id={dag_id}&task_id={task_id}"
                    }
                ],
                "footer": "Airflow Monitoring",
                "ts": int(datetime.now().timestamp())
            }
        ]
    }

    # Log the Slack message (in production, send to actual Slack)
    logger.error(f"SLACK ALERT SENT: {json.dumps(slack_message, indent=2)}")

    # In production, you would use:
    # import requests
    # webhook_url = Variable.get("slack_webhook_url")
    # requests.post(webhook_url, json=slack_message, timeout=10)

# Task 2.2: Resource Cleanup Callbacks


def cleanup_temp_files(context: Dict[str, Any]) -> None:
    """
    Clean up temporary files created during task execution.
    """
    task_instance = context['task_instance']
    task_id = context['task'].task_id

    logger.info(f"Starting cleanup for task: {task_id}")

    global TEMP_FILES
    cleaned_files = []

    try:
        # Clean up temporary files
        for file_path in TEMP_FILES:
            if os.path.exists(file_path):
                os.remove(file_path)
                cleaned_files.append(file_path)
                logger.info(f"Cleaned up temp file: {file_path}")

        # Clear the global list
        TEMP_FILES = []

        logger.info(
            f"Cleanup completed. Removed {len(cleaned_files)} temporary files")

    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        # Don't raise exception in callback


def cleanup_connections(context: Dict[str, Any]) -> None:
    """
    Clean up database connections and other resources.
    """
    task_instance = context['task_instance']
    task_id = context['task'].task_id

    logger.info(f"Starting connection cleanup for task: {task_id}")

    global MOCK_CONNECTIONS

    try:
        # Simulate closing database connections
        for connection in MOCK_CONNECTIONS:
            logger.info(f"Closing connection: {connection['id']}")
            # Simulate connection cleanup
            connection['status'] = 'closed'

        # Clear connections list
        closed_count = len(MOCK_CONNECTIONS)
        MOCK_CONNECTIONS = []

        logger.info(
            f"Connection cleanup completed. Closed {closed_count} connections")

        # Additional cleanup operations
        cleanup_operations = [
            "Released file locks",
            "Cleared memory buffers",
            "Cancelled pending operations",
            "Updated external system status"
        ]

        for operation in cleanup_operations:
            logger.info(f"Cleanup: {operation}")

    except Exception as e:
        logger.error(f"Error during connection cleanup: {e}")

# Task 2.3: Metrics and Logging Callbacks


def log_failure_metrics(context: Dict[str, Any]) -> None:
    """
    Log comprehensive failure metrics for monitoring systems.
    """
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    exception = context.get('exception')

    # Collect comprehensive metrics
    failure_metrics = {
        "timestamp": datetime.now().isoformat(),
        "event_type": "task_failure",
        "dag_id": dag_id,
        "task_id": task_id,
        "execution_date": context['execution_date'].isoformat(),
        "try_number": task_instance.try_number,
        "max_tries": task_instance.max_tries,
        "duration_seconds": task_instance.duration.total_seconds() if task_instance.duration else None,
        "start_date": task_instance.start_date.isoformat() if task_instance.start_date else None,
        "end_date": task_instance.end_date.isoformat() if task_instance.end_date else None,
        "error_details": {
            "error_type": type(exception).__name__ if exception else "Unknown",
            "error_message": str(exception) if exception else "No error message",
            "error_category": classify_error(exception) if exception else "unknown"
        },
        "task_metadata": {
            "operator": context['task'].__class__.__name__,
            "pool": getattr(context['task'], 'pool', None),
            "queue": getattr(context['task'], 'queue', None),
            "priority_weight": getattr(context['task'], 'priority_weight', None)
        },
        "system_context": {
            "dag_run_id": context.get('dag_run', {}).run_id if context.get('dag_run') else None,
            "is_retry": task_instance.try_number > 1
        }
    }

    # Log structured metrics
    logger.error(f"FAILURE_METRICS: {json.dumps(failure_metrics, indent=2)}")

    # In production, send to monitoring system
    # send_to_datadog(failure_metrics)
    # send_to_prometheus(failure_metrics)


def classify_error(exception) -> str:
    """
    Classify error type for better handling and metrics.
    """
    error_message = str(exception).lower()
    error_type = type(exception).__name__.lower()

    if 'timeout' in error_message or 'timeout' in error_type:
        return 'timeout'
    elif 'connection' in error_message or 'network' in error_message:
        return 'network'
    elif 'permission' in error_message or 'access' in error_message:
        return 'permission'
    elif 'file' in error_message and 'not found' in error_message:
        return 'missing_file'
    elif 'memory' in error_message or 'disk' in error_message:
        return 'resource'
    elif 'syntax' in error_message or 'parse' in error_message:
        return 'code_error'
    else:
        return 'unknown'


def classify_and_log_error(context: Dict[str, Any]) -> None:
    """
    Classify error and provide remediation suggestions.
    """
    exception = context.get('exception')
    task_id = context['task'].task_id

    error_category = classify_error(exception) if exception else 'unknown'

    # Define remediation suggestions
    remediation_suggestions = {
        'timeout': [
            'Check network connectivity',
            'Increase task timeout',
            'Verify external service availability'
        ],
        'network': [
            'Check network configuration',
            'Verify firewall rules',
            'Test connectivity to external services'
        ],
        'permission': [
            'Check file/directory permissions',
            'Verify service account access',
            'Review security policies'
        ],
        'missing_file': [
            'Verify file path is correct',
            'Check if upstream task completed',
            'Ensure file was created successfully'
        ],
        'resource': [
            'Check available memory/disk space',
            'Monitor system resource usage',
            'Consider scaling resources'
        ],
        'code_error': [
            'Review code syntax',
            'Check for recent code changes',
            'Validate input data format'
        ],
        'unknown': [
            'Review task logs for details',
            'Check for recent system changes',
            'Contact system administrator'
        ]
    }

    error_analysis = {
        "timestamp": datetime.now().isoformat(),
        "task_id": task_id,
        "error_category": error_category,
        "error_message": str(exception) if exception else "No error message",
        "remediation_suggestions": remediation_suggestions.get(error_category, []),
        "priority": "high" if error_category in ['resource', 'permission'] else "medium"
    }

    logger.error(f"ERROR_ANALYSIS: {json.dumps(error_analysis, indent=2)}")

# Task functions that demonstrate different failure scenarios


def email_notification_task(**context):
    """Task that fails to demonstrate email notifications."""
    # Simulate some work before failing
    logger.info("Processing data...")
    raise AirflowException(
        "Email notification demo: Task failed during data processing")


def slack_notification_task(**context):
    """Task that fails to demonstrate Slack notifications."""
    logger.info("Connecting to external API...")
    raise AirflowException("Slack notification demo: API connection timeout")


def file_cleanup_task(**context):
    """Task that creates temp files then fails to demonstrate cleanup."""
    global TEMP_FILES

    # Create temporary files
    for i in range(3):
        temp_file = tempfile.NamedTemporaryFile(
            delete=False, prefix=f'task_temp_{i}_')
        TEMP_FILES.append(temp_file.name)
        temp_file.write(b'Temporary data for testing')
        temp_file.close()
        logger.info(f"Created temp file: {temp_file.name}")

    # Simulate failure
    raise AirflowException(
        "File cleanup demo: Task failed after creating temporary files")


def connection_cleanup_task(**context):
    """Task that creates connections then fails to demonstrate cleanup."""
    global MOCK_CONNECTIONS

    # Create mock connections
    for i in range(2):
        connection = {
            'id': f'conn_{i}_{datetime.now().timestamp()}',
            'type': 'database',
            'status': 'open'
        }
        MOCK_CONNECTIONS.append(connection)
        logger.info(f"Created connection: {connection['id']}")

    # Simulate failure
    raise AirflowException(
        "Connection cleanup demo: Database operation failed")


def metrics_task(**context):
    """Task that fails to demonstrate metrics logging."""
    logger.info("Starting complex data processing...")
    # Simulate some processing time
    import time
    time.sleep(2)
    raise AirflowException("Metrics demo: Data validation failed")


def multi_callback_task(**context):
    """Task that demonstrates multiple callbacks working together."""
    global TEMP_FILES, MOCK_CONNECTIONS

    # Create resources that need cleanup
    temp_file = tempfile.NamedTemporaryFile(
        delete=False, prefix='multi_callback_')
    TEMP_FILES.append(temp_file.name)
    temp_file.close()

    MOCK_CONNECTIONS.append({
        'id': f'multi_conn_{datetime.now().timestamp()}',
        'type': 'api',
        'status': 'open'
    })

    logger.info("Multi-callback task processing...")
    raise AirflowException("Multi-callback demo: Critical system failure")

# Configure tasks with different callback combinations


# Task 2.1: Notification callbacks
email_task = PythonOperator(
    task_id='email_notification_example',
    python_callable=email_notification_task,
    on_failure_callback=send_email_alert,
    dag=dag
)

slack_task = PythonOperator(
    task_id='slack_notification_example',
    python_callable=slack_notification_task,
    on_failure_callback=send_slack_alert,
    dag=dag
)

# Task 2.2: Resource cleanup callbacks
file_cleanup = PythonOperator(
    task_id='file_cleanup_example',
    python_callable=file_cleanup_task,
    on_failure_callback=cleanup_temp_files,
    dag=dag
)

connection_cleanup = PythonOperator(
    task_id='connection_cleanup_example',
    python_callable=connection_cleanup_task,
    on_failure_callback=cleanup_connections,
    dag=dag
)

# Task 2.3: Metrics and logging callbacks
metrics_logging = PythonOperator(
    task_id='metrics_example',
    python_callable=metrics_task,
    on_failure_callback=log_failure_metrics,
    dag=dag
)

error_classification = PythonOperator(
    task_id='error_classification_example',
    python_callable=lambda: exec(
        'raise PermissionError("Access denied to data directory")'),
    on_failure_callback=classify_and_log_error,
    dag=dag
)

# Task 2.4: Multi-callback integration
multi_callback = PythonOperator(
    task_id='multi_callback_example',
    python_callable=multi_callback_task,
    on_failure_callback=[
        send_email_alert,
        send_slack_alert,
        cleanup_temp_files,
        cleanup_connections,
        log_failure_metrics,
        classify_and_log_error
    ],
    dag=dag
)

# Set up task dependencies
email_task >> slack_task >> file_cleanup >> connection_cleanup
metrics_logging >> error_classification >> multi_callback
