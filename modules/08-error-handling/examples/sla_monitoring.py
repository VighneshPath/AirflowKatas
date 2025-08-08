"""
Error Handling Examples: SLA Monitoring

This module demonstrates how to configure and monitor Service Level Agreements (SLAs)
for Airflow tasks and DAGs.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
import logging
import time
import json
from typing import Dict, Any

# Configure logging
logger = logging.getLogger(__name__)


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """
    Callback function executed when an SLA is missed.

    Args:
        dag: The DAG object
        task_list: List of tasks that missed their SLA
        blocking_task_list: List of tasks that are blocking the SLA tasks
        slas: List of SLA objects that were missed
        blocking_tis: List of TaskInstance objects that are blocking
    """
    logger.error("SLA MISS DETECTED!")

    # Log details about the SLA miss
    for task in task_list:
        logger.error(f"Task {task.task_id} missed its SLA")

    for sla in slas:
        logger.error(f"SLA missed: {sla}")

    # Create structured alert data
    alert_data = {
        "timestamp": datetime.now().isoformat(),
        "dag_id": dag.dag_id,
        "missed_tasks": [task.task_id for task in task_list],
        "blocking_tasks": [task.task_id for task in blocking_task_list],
        "sla_details": [str(sla) for sla in slas]
    }

    # Log structured alert (in production, send to monitoring system)
    logger.error(f"SLA_MISS_ALERT: {json.dumps(alert_data, indent=2)}")

    # In production, you might:
    # - Send email notifications
    # - Create incidents in PagerDuty
    # - Send alerts to Slack
    # - Update external monitoring dashboards


def fast_task(**context):
    """Task that completes quickly, well within SLA."""
    logger.info("Executing fast task")
    time.sleep(2)  # 2 seconds
    return "Fast task completed"


def medium_task(**context):
    """Task that takes moderate time, usually within SLA."""
    logger.info("Executing medium task")
    time.sleep(30)  # 30 seconds
    return "Medium task completed"


def slow_task(**context):
    """Task that takes a long time, might exceed SLA."""
    logger.info("Executing slow task")
    time.sleep(120)  # 2 minutes
    return "Slow task completed"


def variable_duration_task(**context):
    """Task with variable duration based on external factors."""
    import random

    # Simulate variable processing time (10 seconds to 5 minutes)
    duration = random.randint(10, 300)
    logger.info(f"Task will run for {duration} seconds")
    time.sleep(duration)
    return f"Variable task completed in {duration} seconds"


def data_processing_task(**context):
    """Simulate a data processing task that might take longer than expected."""
    logger.info("Starting data processing")

    # Simulate different processing scenarios
    import random
    scenario = random.choice(['light', 'medium', 'heavy'])

    if scenario == 'light':
        time.sleep(15)  # 15 seconds
        logger.info("Light processing completed")
    elif scenario == 'medium':
        time.sleep(60)  # 1 minute
        logger.info("Medium processing completed")
    else:  # heavy
        time.sleep(180)  # 3 minutes
        logger.info("Heavy processing completed")

    return f"Data processing completed ({scenario} load)"


# DAG configuration with SLA callback
default_args = {
    'owner': 'airflow-kata',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'sla_miss_callback': sla_miss_callback
}

dag = DAG(
    'sla_monitoring_examples',
    default_args=default_args,
    description='Examples of SLA configuration and monitoring',
    schedule_interval=timedelta(hours=1),  # Run every hour
    catchup=False,
    tags=['error-handling', 'sla', 'monitoring', 'examples']
)

# Task with short SLA (should always meet)
fast_sla_task = PythonOperator(
    task_id='fast_sla_task',
    python_callable=fast_task,
    sla=timedelta(seconds=30),  # 30 second SLA
    dag=dag
)

# Task with medium SLA (usually meets, sometimes misses)
medium_sla_task = PythonOperator(
    task_id='medium_sla_task',
    python_callable=medium_task,
    sla=timedelta(minutes=1),  # 1 minute SLA
    dag=dag
)

# Task with tight SLA (likely to miss)
tight_sla_task = PythonOperator(
    task_id='tight_sla_task',
    python_callable=slow_task,
    sla=timedelta(seconds=90),  # 90 second SLA (task takes 2 minutes)
    dag=dag
)

# Task with realistic SLA
realistic_sla_task = PythonOperator(
    task_id='realistic_sla_task',
    python_callable=variable_duration_task,
    sla=timedelta(minutes=6),  # 6 minute SLA
    dag=dag
)

# Bash task with SLA
bash_sla_task = BashOperator(
    task_id='bash_sla_task',
    bash_command='sleep 45 && echo "Bash task completed"',
    sla=timedelta(minutes=1),  # 1 minute SLA
    dag=dag
)

# Data processing task with business SLA
data_sla_task = PythonOperator(
    task_id='data_processing_sla',
    python_callable=data_processing_task,
    sla=timedelta(minutes=2),  # 2 minute SLA
    dag=dag
)

# Task without SLA for comparison
no_sla_task = PythonOperator(
    task_id='no_sla_task',
    python_callable=lambda: time.sleep(60) or "No SLA task completed",
    # No SLA configured
    dag=dag
)

# Create a chain of tasks with different SLA requirements
fast_sla_task >> medium_sla_task >> tight_sla_task
realistic_sla_task >> bash_sla_task >> data_sla_task >> no_sla_task

# Additional DAG for demonstrating DAG-level SLA monitoring
dag_sla_args = {
    'owner': 'airflow-kata',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'sla_miss_callback': sla_miss_callback
}

dag_sla = DAG(
    'dag_level_sla_example',
    default_args=dag_sla_args,
    description='Example of DAG-level SLA monitoring',
    schedule_interval=timedelta(hours=2),
    catchup=False,
    tags=['error-handling', 'sla', 'dag-level', 'examples']
)


def quick_setup(**context):
    """Quick setup task."""
    time.sleep(10)
    return "Setup completed"


def main_processing(**context):
    """Main processing task that might take variable time."""
    import random
    duration = random.randint(60, 300)  # 1-5 minutes
    time.sleep(duration)
    return f"Processing completed in {duration} seconds"


def cleanup_task(**context):
    """Cleanup task."""
    time.sleep(15)
    return "Cleanup completed"


# Tasks for DAG-level SLA demonstration
setup_task = PythonOperator(
    task_id='setup',
    python_callable=quick_setup,
    sla=timedelta(seconds=30),  # Individual task SLA
    dag=dag_sla
)

processing_task = PythonOperator(
    task_id='main_processing',
    python_callable=main_processing,
    sla=timedelta(minutes=4),  # Individual task SLA
    dag=dag_sla
)

cleanup = PythonOperator(
    task_id='cleanup',
    python_callable=cleanup_task,
    sla=timedelta(seconds=30),  # Individual task SLA
    dag=dag_sla
)

# Set up dependencies
setup_task >> processing_task >> cleanup

# Note: DAG-level SLA would be configured at the DAG level
# and would monitor the total time for the entire DAG execution
