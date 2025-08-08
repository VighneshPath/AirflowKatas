"""
Error Handling Examples: Failure Callbacks

This module demonstrates how to implement failure callbacks for notifications,
cleanup, and custom error handling logic.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.models import Variable
import logging
import json
import requests
from typing import Dict, Any

# Configure logging
logger = logging.getLogger(__name__)


def send_email_notification(context: Dict[str, Any]) -> None:
    """
    Send email notification on task failure.
    In production, this would use Airflow's email functionality.
    """
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    execution_date = context['execution_date']

    # Log the notification (in production, send actual email)
    logger.error(f"""
    EMAIL NOTIFICATION:
    Subject: Airflow Task Failed - {dag_id}.{task_id}
    
    Task Details:
    - DAG: {dag_id}
    - Task: {task_id}
    - Execution Date: {execution_date}
    - Try Number: {task_instance.try_number}
    - Max Tries: {task_instance.max_tries}
    
    Error: {context.get('exception', 'Unknown error')}
    
    Please investigate and resolve the issue.
    """)


def send_slack_notification(context: Dict[str, Any]) -> None:
    """
    Send Slack notification on task failure.
    This example shows how to integrate with Slack webhooks.
    """
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id

    # In production, get webhook URL from Airflow Variables or Connections
    slack_webhook_url = "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK"

    message = {
        "text": f"🚨 Airflow Task Failed",
        "attachments": [
            {
                "color": "danger",
                "fields": [
                    {"title": "DAG", "value": dag_id, "short": True},
                    {"title": "Task", "value": task_id, "short": True},
                    {"title": "Try Number", "value": str(
                        task_instance.try_number), "short": True},
                    {"title": "Error", "value": str(context.get('exception', 'Unknown error'))[
                        :100], "short": False}
                ]
            }
        ]
    }

    # Log the notification (in production, send to actual Slack)
    logger.error(f"SLACK NOTIFICATION: {json.dumps(message, indent=2)}")

    # Uncomment for actual Slack integration:
    # try:
    #     response = requests.post(slack_webhook_url, json=message, timeout=10)
    #     response.raise_for_status()
    # except Exception as e:
    #     logger.error(f"Failed to send Slack notification: {e}")


def cleanup_resources(context: Dict[str, Any]) -> None:
    """
    Cleanup resources when a task fails.
    This is important for tasks that create temporary resources.
    """
    task_instance = context['task_instance']
    task_id = context['task'].task_id

    logger.info(f"Cleaning up resources for failed task: {task_id}")

    # Example cleanup operations
    cleanup_operations = [
        "Removing temporary files",
        "Closing database connections",
        "Releasing file locks",
        "Canceling pending operations",
        "Updating status in external systems"
    ]

    for operation in cleanup_operations:
        logger.info(f"Cleanup: {operation}")
        # Implement actual cleanup logic here

    logger.info("Resource cleanup completed")


def log_failure_metrics(context: Dict[str, Any]) -> None:
    """
    Log failure metrics for monitoring and analysis.
    """
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id

    failure_metrics = {
        "timestamp": datetime.now().isoformat(),
        "dag_id": dag_id,
        "task_id": task_id,
        "try_number": task_instance.try_number,
        "max_tries": task_instance.max_tries,
        "duration": str(task_instance.duration) if task_instance.duration else None,
        "error_type": type(context.get('exception', Exception())).__name__,
        "error_message": str(context.get('exception', 'Unknown error'))
    }

    # Log structured data for monitoring systems
    logger.error(f"FAILURE_METRICS: {json.dumps(failure_metrics)}")


def success_callback(context: Dict[str, Any]) -> None:
    """
    Callback executed when a task succeeds.
    Useful for notifications and metrics collection.
    """
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id

    logger.info(
        f"Task succeeded: {dag_id}.{task_id} (attempt {task_instance.try_number})")

    # Log success metrics
    success_metrics = {
        "timestamp": datetime.now().isoformat(),
        "dag_id": dag_id,
        "task_id": task_id,
        "try_number": task_instance.try_number,
        "duration": str(task_instance.duration) if task_instance.duration else None,
        "status": "success"
    }

    logger.info(f"SUCCESS_METRICS: {json.dumps(success_metrics)}")


def retry_callback(context: Dict[str, Any]) -> None:
    """
    Callback executed on each retry attempt.
    Useful for logging retry attempts and adjusting retry strategy.
    """
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id

    logger.warning(
        f"Task retry: {dag_id}.{task_id} (attempt {task_instance.try_number}/{task_instance.max_tries})")

    # Log retry metrics
    retry_metrics = {
        "timestamp": datetime.now().isoformat(),
        "dag_id": dag_id,
        "task_id": task_id,
        "try_number": task_instance.try_number,
        "max_tries": task_instance.max_tries,
        "error_message": str(context.get('exception', 'Unknown error'))
    }

    logger.warning(f"RETRY_METRICS: {json.dumps(retry_metrics)}")


# DAG configuration
default_args = {
    'owner': 'airflow-kata',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'failure_callbacks_examples',
    default_args=default_args,
    description='Examples of failure callbacks and notifications',
    schedule_interval=None,
    catchup=False,
    tags=['error-handling', 'callbacks', 'examples']
)


def task_that_fails(**context):
    """Task that always fails to demonstrate failure callbacks."""
    raise AirflowException("This task is designed to fail for demonstration")


def task_that_succeeds(**context):
    """Task that succeeds to demonstrate success callbacks."""
    logger.info("Task executing successfully")
    return "Task completed successfully"


def task_with_retry_logic(**context):
    """Task that fails on first attempt but succeeds on retry."""
    task_instance = context['task_instance']

    if task_instance.try_number == 1:
        raise AirflowException("First attempt failure")

    logger.info("Task succeeded on retry")
    return "Success after retry"


# Task with email notification callback
email_notification_task = PythonOperator(
    task_id='email_notification_example',
    python_callable=task_that_fails,
    on_failure_callback=send_email_notification,
    retries=1,
    dag=dag
)

# Task with Slack notification callback
slack_notification_task = PythonOperator(
    task_id='slack_notification_example',
    python_callable=task_that_fails,
    on_failure_callback=send_slack_notification,
    retries=1,
    dag=dag
)

# Task with resource cleanup callback
cleanup_task = PythonOperator(
    task_id='cleanup_example',
    python_callable=task_that_fails,
    on_failure_callback=cleanup_resources,
    retries=1,
    dag=dag
)

# Task with metrics logging callback
metrics_task = PythonOperator(
    task_id='metrics_example',
    python_callable=task_that_fails,
    on_failure_callback=log_failure_metrics,
    retries=1,
    dag=dag
)

# Task with success callback
success_task = PythonOperator(
    task_id='success_callback_example',
    python_callable=task_that_succeeds,
    on_success_callback=success_callback,
    dag=dag
)

# Task with retry callback
retry_task = PythonOperator(
    task_id='retry_callback_example',
    python_callable=task_with_retry_logic,
    on_retry_callback=retry_callback,
    on_failure_callback=send_email_notification,
    on_success_callback=success_callback,
    retries=2,
    dag=dag
)

# Task with multiple callbacks
multi_callback_task = PythonOperator(
    task_id='multi_callback_example',
    python_callable=task_that_fails,
    on_failure_callback=[send_email_notification,
                         cleanup_resources, log_failure_metrics],
    retries=1,
    dag=dag
)

# Set up task dependencies
email_notification_task >> slack_notification_task >> cleanup_task
metrics_task >> success_task >> retry_task >> multi_callback_task
