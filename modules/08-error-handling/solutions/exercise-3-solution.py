"""
Solution: Exercise 3 - SLA Configuration and Monitoring

This solution demonstrates comprehensive SLA configuration and monitoring
for Airflow tasks and DAGs.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import logging
import time
import json
import random
from typing import Dict, Any

# Configure logging
logger = logging.getLogger(__name__)


def comprehensive_sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """
    Comprehensive SLA miss callback with detailed logging and alerting.

    Args:
        dag: The DAG object
        task_list: List of tasks that missed their SLA
        blocking_task_list: List of tasks that are blocking the SLA tasks
        slas: List of SLA objects that were missed
        blocking_tis: List of TaskInstance objects that are blocking
    """
    logger.error("=" * 60)
    logger.error("SLA MISS DETECTED!")
    logger.error("=" * 60)

    # Log basic SLA miss information
    for task in task_list:
        logger.error(f"Task {task.task_id} missed its SLA")

    for blocking_task in blocking_task_list:
        logger.error(f"Blocking task: {blocking_task.task_id}")

    # Create comprehensive alert data
    alert_data = {
        "timestamp": datetime.now().isoformat(),
        "event_type": "sla_miss",
        "dag_id": dag.dag_id,
        "missed_tasks": [
            {
                "task_id": task.task_id,
                "sla_seconds": getattr(task, 'sla', {}).total_seconds() if hasattr(task, 'sla') and task.sla else None,
                "operator_type": task.__class__.__name__
            } for task in task_list
        ],
        "blocking_tasks": [
            {
                "task_id": task.task_id,
                "state": getattr(task, 'state', 'unknown')
            } for task in blocking_task_list
        ],
        "sla_details": [str(sla) for sla in slas],
        "impact_analysis": analyze_sla_impact(task_list, blocking_task_list),
        "remediation_suggestions": generate_remediation_suggestions(task_list, blocking_task_list)
    }

    # Log structured alert
    logger.error(f"SLA_MISS_ALERT: {json.dumps(alert_data, indent=2)}")

    # Categorize SLA miss severity
    severity = categorize_sla_miss_severity(task_list, slas)
    logger.error(f"SLA Miss Severity: {severity}")

    # Send appropriate notifications based on severity
    if severity == "critical":
        send_critical_sla_alert(alert_data)
    elif severity == "high":
        send_high_priority_sla_alert(alert_data)
    else:
        send_standard_sla_alert(alert_data)


def analyze_sla_impact(task_list, blocking_task_list):
    """Analyze the impact of SLA misses."""
    return {
        "missed_task_count": len(task_list),
        "blocking_task_count": len(blocking_task_list),
        "potential_cascade_risk": len(blocking_task_list) > 0,
        "business_impact": "high" if len(task_list) > 2 else "medium"
    }


def generate_remediation_suggestions(task_list, blocking_task_list):
    """Generate specific remediation suggestions."""
    suggestions = []

    if blocking_task_list:
        suggestions.append("Check blocking tasks for performance issues")
        suggestions.append("Consider parallel execution where possible")

    if len(task_list) > 1:
        suggestions.append("Review overall workflow design")
        suggestions.append("Consider breaking down complex tasks")

    suggestions.extend([
        "Monitor system resource usage",
        "Check for external dependency issues",
        "Review SLA thresholds for realism"
    ])

    return suggestions


def categorize_sla_miss_severity(task_list, slas):
    """Categorize SLA miss severity based on impact."""
    if len(task_list) >= 3:
        return "critical"
    elif len(task_list) == 2:
        return "high"
    else:
        return "medium"


def send_critical_sla_alert(alert_data):
    """Send critical SLA alert (would integrate with PagerDuty, etc.)."""
    logger.error("CRITICAL SLA ALERT: Sending to PagerDuty and executive team")


def send_high_priority_sla_alert(alert_data):
    """Send high priority SLA alert."""
    logger.error("HIGH PRIORITY SLA ALERT: Sending to operations team")


def send_standard_sla_alert(alert_data):
    """Send standard SLA alert."""
    logger.error("STANDARD SLA ALERT: Logging for monitoring dashboard")


# DAG configuration with SLA callback
default_args = {
    'owner': 'airflow-kata',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'sla_miss_callback': comprehensive_sla_miss_callback
}

dag = DAG(
    'sla_exercise_dag',
    default_args=default_args,
    description='Solution for SLA configuration and monitoring exercise',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['error-handling', 'sla', 'exercise', 'solution']
)

# Task 3.1: Basic SLA Configuration


def fast_task(**context):
    """Task that completes quickly, should always meet SLA."""
    task_instance = context['task_instance']
    start_time = time.time()

    logger.info("Executing fast task")
    time.sleep(random.uniform(10, 15))  # 10-15 seconds

    execution_time = time.time() - start_time
    logger.info(f"Fast task completed in {execution_time:.2f} seconds")

    return {
        "status": "success",
        "execution_time": execution_time,
        "sla_status": "within_sla"
    }


def medium_task(**context):
    """Task with moderate execution time, usually within SLA."""
    task_instance = context['task_instance']
    start_time = time.time()

    logger.info("Executing medium task")
    time.sleep(random.uniform(45, 60))  # 45-60 seconds

    execution_time = time.time() - start_time
    logger.info(f"Medium task completed in {execution_time:.2f} seconds")

    return {
        "status": "success",
        "execution_time": execution_time,
        "sla_status": "within_sla" if execution_time < 90 else "sla_risk"
    }


def slow_task(**context):
    """Task that takes long time, likely to exceed SLA."""
    task_instance = context['task_instance']
    start_time = time.time()

    logger.info("Executing slow task (likely to miss SLA)")
    time.sleep(random.uniform(120, 180))  # 2-3 minutes

    execution_time = time.time() - start_time
    logger.info(f"Slow task completed in {execution_time:.2f} seconds")

    return {
        "status": "success",
        "execution_time": execution_time,
        "sla_status": "sla_miss_expected"
    }

# Task 3.2: SLA Miss Callback Implementation (already implemented above)

# Task 3.3: Variable SLA Configuration


def calculate_business_hours_sla(**context):
    """Calculate SLA based on business hours."""
    current_time = datetime.now()
    current_hour = current_time.hour
    is_weekend = current_time.weekday() >= 5  # Saturday = 5, Sunday = 6

    if is_weekend:
        # Weekend: more relaxed SLA
        sla_minutes = 120  # 2 hours
        logger.info("Weekend SLA applied: 2 hours")
    elif 9 <= current_hour <= 17:
        # Business hours: stricter SLA
        sla_minutes = 30  # 30 minutes
        logger.info("Business hours SLA applied: 30 minutes")
    else:
        # Off-hours: moderate SLA
        sla_minutes = 60  # 1 hour
        logger.info("Off-hours SLA applied: 1 hour")

    return timedelta(minutes=sla_minutes)


def business_hours_task(**context):
    """Task with time-based SLA configuration."""
    current_sla = calculate_business_hours_sla(context)
    logger.info(f"Current SLA: {current_sla}")

    # Simulate variable processing time
    processing_time = random.uniform(20, 90)  # 20 seconds to 1.5 minutes
    time.sleep(processing_time)

    logger.info(
        f"Business hours task completed in {processing_time:.2f} seconds")
    return {
        "status": "success",
        "processing_time": processing_time,
        "applied_sla": str(current_sla)
    }


def calculate_data_volume_sla(data_size_mb):
    """Calculate SLA based on data volume."""
    # Base SLA: 1 minute per MB, minimum 2 minutes
    base_sla_minutes = max(2, data_size_mb * 1)

    # Add buffer for large datasets
    if data_size_mb > 100:
        base_sla_minutes *= 1.5

    return timedelta(minutes=base_sla_minutes)


def data_volume_task(**context):
    """Task with data volume-based SLA."""
    # Simulate different data sizes
    data_size_mb = random.uniform(1, 150)
    calculated_sla = calculate_data_volume_sla(data_size_mb)

    logger.info(f"Processing {data_size_mb:.1f}MB of data")
    logger.info(f"Calculated SLA: {calculated_sla}")

    # Simulate processing time proportional to data size
    processing_time = data_size_mb * 0.5  # 0.5 seconds per MB
    time.sleep(min(processing_time, 120))  # Cap at 2 minutes for demo

    logger.info(f"Data volume task completed")
    return {
        "status": "success",
        "data_size_mb": data_size_mb,
        "processing_time": processing_time,
        "calculated_sla": str(calculated_sla)
    }

# Task 3.4: DAG-Level SLA Monitoring


def pipeline_setup(**context):
    """Setup task for pipeline."""
    logger.info("Pipeline setup starting")
    time.sleep(random.uniform(15, 25))  # 15-25 seconds
    logger.info("Pipeline setup completed")
    return "setup_complete"


def pipeline_processing(**context):
    """Main processing task."""
    logger.info("Main processing starting")
    time.sleep(random.uniform(45, 75))  # 45-75 seconds
    logger.info("Main processing completed")
    return "processing_complete"


def pipeline_cleanup(**context):
    """Cleanup task for pipeline."""
    logger.info("Pipeline cleanup starting")
    time.sleep(random.uniform(20, 35))  # 20-35 seconds
    logger.info("Pipeline cleanup completed")
    return "cleanup_complete"


def critical_path_task(**context):
    """Task on critical path with strict SLA."""
    logger.info("Critical path task starting")

    # This task is on the critical path, so it has a stricter SLA
    processing_time = random.uniform(25, 45)  # 25-45 seconds
    time.sleep(processing_time)

    logger.info(
        f"Critical path task completed in {processing_time:.2f} seconds")
    return {
        "status": "success",
        "processing_time": processing_time,
        "critical_path": True
    }

# Configure tasks with different SLA strategies


# Task 3.1: Basic SLA Configuration
fast_sla_task = PythonOperator(
    task_id='fast_sla_task',
    python_callable=fast_task,
    sla=timedelta(seconds=30),  # 30 second SLA
    dag=dag
)

medium_sla_task = PythonOperator(
    task_id='medium_sla_task',
    python_callable=medium_task,
    sla=timedelta(minutes=1, seconds=30),  # 90 second SLA
    dag=dag
)

slow_sla_task = PythonOperator(
    task_id='slow_sla_task',
    python_callable=slow_task,
    sla=timedelta(seconds=90),  # 90 second SLA (will likely miss)
    dag=dag
)

# Task 3.3: Variable SLA Configuration
business_hours_sla_task = PythonOperator(
    task_id='business_hours_sla_task',
    python_callable=business_hours_task,
    sla=timedelta(minutes=60),  # Default SLA, would be dynamic in production
    dag=dag
)

data_volume_sla_task = PythonOperator(
    task_id='data_volume_sla_task',
    python_callable=data_volume_task,
    sla=timedelta(minutes=5),  # Default SLA, would be dynamic in production
    dag=dag
)

# Task 3.4: DAG-Level SLA Monitoring
setup_task = PythonOperator(
    task_id='pipeline_setup',
    python_callable=pipeline_setup,
    sla=timedelta(seconds=30),
    dag=dag
)

processing_task = PythonOperator(
    task_id='pipeline_processing',
    python_callable=pipeline_processing,
    sla=timedelta(minutes=1, seconds=30),
    dag=dag
)

cleanup_task = PythonOperator(
    task_id='pipeline_cleanup',
    python_callable=pipeline_cleanup,
    sla=timedelta(seconds=45),
    dag=dag
)

critical_task = PythonOperator(
    task_id='critical_path_task',
    python_callable=critical_path_task,
    sla=timedelta(seconds=50),  # Strict SLA for critical path
    dag=dag
)

# Bash task with SLA for demonstration
bash_sla_task = BashOperator(
    task_id='bash_sla_example',
    bash_command="""
    echo "Starting bash task with SLA monitoring"
    sleep_time=$((RANDOM % 60 + 30))  # 30-90 seconds
    echo "Will sleep for $sleep_time seconds"
    sleep $sleep_time
    echo "Bash task completed after $sleep_time seconds"
    """,
    sla=timedelta(minutes=1),  # 1 minute SLA
    dag=dag
)

# Set up task dependencies for pipeline SLA monitoring
fast_sla_task >> medium_sla_task >> slow_sla_task
business_hours_sla_task >> data_volume_sla_task
setup_task >> processing_task >> cleanup_task
critical_task >> bash_sla_task

# Create a separate DAG for comprehensive pipeline SLA demonstration
pipeline_sla_args = {
    'owner': 'airflow-kata',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'sla_miss_callback': comprehensive_sla_miss_callback
}

pipeline_dag = DAG(
    'pipeline_sla_monitoring',
    default_args=pipeline_sla_args,
    description='Pipeline-level SLA monitoring demonstration',
    schedule_interval=timedelta(hours=2),
    catchup=False,
    tags=['error-handling', 'sla', 'pipeline', 'solution']
)


def etl_extract(**context):
    """ETL Extract phase."""
    time.sleep(random.uniform(30, 60))
    return "extract_complete"


def etl_transform(**context):
    """ETL Transform phase."""
    time.sleep(random.uniform(60, 120))
    return "transform_complete"


def etl_load(**context):
    """ETL Load phase."""
    time.sleep(random.uniform(45, 90))
    return "load_complete"


def etl_validate(**context):
    """ETL Validation phase."""
    time.sleep(random.uniform(20, 40))
    return "validation_complete"


# ETL Pipeline with individual and overall SLAs
extract_task = PythonOperator(
    task_id='etl_extract',
    python_callable=etl_extract,
    sla=timedelta(minutes=1, seconds=30),  # 90 seconds
    dag=pipeline_dag
)

transform_task = PythonOperator(
    task_id='etl_transform',
    python_callable=etl_transform,
    sla=timedelta(minutes=2, seconds=30),  # 150 seconds
    dag=pipeline_dag
)

load_task = PythonOperator(
    task_id='etl_load',
    python_callable=etl_load,
    sla=timedelta(minutes=2),  # 120 seconds
    dag=pipeline_dag
)

validate_task = PythonOperator(
    task_id='etl_validate',
    python_callable=etl_validate,
    sla=timedelta(seconds=50),  # 50 seconds
    dag=pipeline_dag
)

# Set up ETL pipeline dependencies
extract_task >> transform_task >> load_task >> validate_task

# Note: In a real implementation, you would also configure DAG-level SLA
# monitoring to track the total pipeline execution time
