"""
Solution for Exercise 1, Task 1: Peak Hours Processing

This DAG processes high-priority data during peak business hours
every 30 minutes between 8 AM and 6 PM on weekdays.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# DAG configuration
dag = DAG(
    'peak_hours_processing',
    description='Process high-priority data during peak business hours',
    schedule_interval='*/30 8-18 * * 1-5',  # Every 30 min, 8AM-6PM, weekdays
    start_date=datetime(2024, 1, 1),
    catchup=False,  # Don't process historical runs
    tags=['scheduling', 'peak-hours', 'high-priority']
)


def log_processing_time(**context):
    """Log the current processing time and context information"""
    execution_date = context['execution_date']
    current_time = datetime.now()

    print(f"=== Peak Hours Processing ===")
    print(f"Execution Date: {execution_date}")
    print(f"Current Time: {current_time}")
    print(f"Day of Week: {execution_date.strftime('%A')}")
    print(f"Hour: {execution_date.hour}")
    print(f"Processing high-priority data during peak hours")

    # Validate that we're actually in peak hours
    hour = execution_date.hour
    weekday = execution_date.weekday()  # Monday = 0

    if 8 <= hour <= 18 and weekday < 5:
        print("✓ Confirmed: Processing during peak business hours")
    else:
        print("⚠ Warning: Processing outside expected peak hours")

    return f"Processed at {current_time}"


# Task to log processing time
log_time = PythonOperator(
    task_id='log_processing_time',
    python_callable=log_processing_time,
    dag=dag
)

# Simulate high-priority data processing
process_priority_data = BashOperator(
    task_id='process_priority_data',
    bash_command='''
    echo "Starting high-priority data processing..."
    echo "Processing time: $(date)"
    echo "Simulating intensive processing..."
    sleep 5
    echo "High-priority processing completed successfully"
    ''',
    dag=dag
)

# Set task dependencies
log_time >> process_priority_data
