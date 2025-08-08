"""
File Sensor Examples

This module demonstrates various FileSensor configurations and patterns.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Default arguments for all DAGs
default_args = {
    'owner': 'airflow-kata',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Example 1: Basic FileSensor
dag1 = DAG(
    'file_sensor_basic',
    default_args=default_args,
    description='Basic FileSensor example',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['sensors', 'file', 'basic']
)


def create_test_file():
    """Create a test file for demonstration"""
    import os
    os.makedirs('/tmp/airflow-kata', exist_ok=True)
    with open('/tmp/airflow-kata/input.txt', 'w') as f:
        f.write('Test data for sensor example')
    print("Test file created at /tmp/airflow-kata/input.txt")


def process_file():
    """Process the file once it's detected"""
    with open('/tmp/airflow-kata/input.txt', 'r') as f:
        content = f.read()
    print(f"Processing file content: {content}")
    return f"Processed {len(content)} characters"


# Create test file (in real scenarios, this would be external)
create_file_task = PythonOperator(
    task_id='create_test_file',
    python_callable=create_test_file,
    dag=dag1
)

# Wait for file to appear
wait_for_file = FileSensor(
    task_id='wait_for_input_file',
    filepath='/tmp/airflow-kata/input.txt',
    fs_conn_id='fs_default',
    poke_interval=10,  # Check every 10 seconds
    timeout=120,       # Timeout after 2 minutes
    dag=dag1
)

# Process file once detected
process_file_task = PythonOperator(
    task_id='process_file',
    python_callable=process_file,
    dag=dag1
)

# Set dependencies
create_file_task >> wait_for_file >> process_file_task

# Example 2: FileSensor with wildcard patterns
dag2 = DAG(
    'file_sensor_patterns',
    default_args=default_args,
    description='FileSensor with pattern matching',
    schedule_interval=timedelta(hours=2),
    catchup=False,
    tags=['sensors', 'file', 'patterns']
)


def create_daily_files():
    """Create multiple files with date patterns"""
    import os
    from datetime import datetime

    os.makedirs('/tmp/airflow-kata/daily', exist_ok=True)

    # Create files with different patterns
    today = datetime.now().strftime('%Y%m%d')
    files = [
        f'/tmp/airflow-kata/daily/data_{today}.csv',
        f'/tmp/airflow-kata/daily/report_{today}.txt',
        f'/tmp/airflow-kata/daily/summary_{today}.json'
    ]

    for file_path in files:
        with open(file_path, 'w') as f:
            f.write(f'Sample data for {file_path}')

    print(f"Created {len(files)} files for date {today}")


create_daily_files_task = PythonOperator(
    task_id='create_daily_files',
    python_callable=create_daily_files,
    dag=dag2
)

# Wait for CSV file with date pattern
wait_for_csv = FileSensor(
    task_id='wait_for_daily_csv',
    filepath='/tmp/airflow-kata/daily/data_{{ ds_nodash }}.csv',
    fs_conn_id='fs_default',
    poke_interval=15,
    timeout=300,
    dag=dag2
)

# Wait for report file
wait_for_report = FileSensor(
    task_id='wait_for_daily_report',
    filepath='/tmp/airflow-kata/daily/report_{{ ds_nodash }}.txt',
    fs_conn_id='fs_default',
    poke_interval=15,
    timeout=300,
    dag=dag2
)

# Process all files once both are available
process_daily_files = BashOperator(
    task_id='process_daily_files',
    bash_command='''
    echo "Processing daily files for {{ ds }}"
    ls -la /tmp/airflow-kata/daily/
    wc -l /tmp/airflow-kata/daily/*{{ ds_nodash }}*
    ''',
    dag=dag2
)

# Set dependencies - both sensors must complete before processing
create_daily_files_task >> [wait_for_csv,
                            wait_for_report] >> process_daily_files

# Example 3: FileSensor with reschedule mode and error handling
dag3 = DAG(
    'file_sensor_advanced',
    default_args=default_args,
    description='Advanced FileSensor with reschedule mode',
    schedule_interval=timedelta(hours=6),
    catchup=False,
    tags=['sensors', 'file', 'advanced']
)


def sensor_success_callback(context):
    """Callback when sensor succeeds"""
    task_instance = context['task_instance']
    print(f"Sensor {task_instance.task_id} successfully detected file!")
    # In real scenarios: send notification, update monitoring, etc.


def sensor_failure_callback(context):
    """Callback when sensor fails"""
    task_instance = context['task_instance']
    print(f"Sensor {task_instance.task_id} failed after timeout!")
    # In real scenarios: send alert, log to monitoring system, etc.


def create_delayed_file():
    """Create file after a delay to simulate real-world scenario"""
    import time
    import os

    print("Waiting 30 seconds before creating file...")
    time.sleep(30)  # Simulate delay

    os.makedirs('/tmp/airflow-kata/delayed', exist_ok=True)
    with open('/tmp/airflow-kata/delayed/important_data.txt', 'w') as f:
        f.write('Critical data that arrives with delay')

    print("Delayed file created successfully")


create_delayed_file_task = PythonOperator(
    task_id='create_delayed_file',
    python_callable=create_delayed_file,
    dag=dag3
)

# Advanced sensor with reschedule mode and callbacks
wait_for_important_file = FileSensor(
    task_id='wait_for_important_file',
    filepath='/tmp/airflow-kata/delayed/important_data.txt',
    fs_conn_id='fs_default',
    mode='reschedule',  # Release worker slot between checks
    poke_interval=20,   # Check every 20 seconds
    timeout=180,        # 3 minute timeout
    retries=2,          # Retry twice on failure
    retry_delay=timedelta(seconds=30),
    on_success_callback=sensor_success_callback,
    on_failure_callback=sensor_failure_callback,
    dag=dag3
)

# Cleanup task
cleanup_files = BashOperator(
    task_id='cleanup_test_files',
    bash_command='''
    echo "Cleaning up test files..."
    rm -rf /tmp/airflow-kata/
    echo "Cleanup completed"
    ''',
    dag=dag3
)

# Set dependencies
create_delayed_file_task >> wait_for_important_file >> cleanup_files
