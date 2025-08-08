"""
Solution for Exercise 1: File Sensors

This solution demonstrates proper FileSensor implementation with different
timeout and retry configurations.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow-kata',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Task 1: Basic FileSensor Implementation
dag1 = DAG(
    'daily_sales_pipeline',
    default_args=default_args,
    description='Daily sales report processing pipeline',
    schedule_interval='0 9 * * *',  # Daily at 9:00 AM
    catchup=False,
    tags=['exercise', 'file-sensor', 'sales']
)


def create_test_files():
    """Create test files for sensor exercises"""
    import os
    from datetime import datetime

    # Create directory
    os.makedirs('/tmp/sales-data', exist_ok=True)

    # Create files with current date
    today = datetime.now().strftime('%Y%m%d')

    files = {
        f'/tmp/sales-data/sales_report_{today}.csv': 'date,product,amount\n2024-01-01,Widget,100\n2024-01-01,Gadget,200',
        f'/tmp/sales-data/customer_data_{today}.json': '{"customers": [{"id": 1, "name": "John Doe"}]}',
        f'/tmp/sales-data/product_catalog_{today}.xml': '<products><product id="1">Widget</product></products>'
    }

    for file_path, content in files.items():
        with open(file_path, 'w') as f:
            f.write(content)
        print(f"Created: {file_path}")


# Create test data
setup_test_data = PythonOperator(
    task_id='setup_test_data',
    python_callable=create_test_files,
    dag=dag1
)

# Basic FileSensor
wait_for_sales_report = FileSensor(
    task_id='wait_for_sales_report',
    filepath='/tmp/sales-data/sales_report_{{ ds_nodash }}.csv',
    fs_conn_id='fs_default',
    poke_interval=30,      # Check every 30 seconds
    timeout=300,           # 5 minute timeout
    dag=dag1
)

# Process the file
process_sales_report = BashOperator(
    task_id='process_sales_report',
    bash_command='''
    echo "Processing sales report for {{ ds }}"
    echo "File contents:"
    cat /tmp/sales-data/sales_report_{{ ds_nodash }}.csv
    echo "Processing completed successfully"
    ''',
    dag=dag1
)

# Set dependencies for Task 1
setup_test_data >> wait_for_sales_report >> process_sales_report

# Task 2: Multiple File Dependencies
dag2 = DAG(
    'multi_file_sales_pipeline',
    default_args=default_args,
    description='Sales pipeline with multiple file dependencies',
    schedule_interval='0 9 * * *',
    catchup=False,
    tags=['exercise', 'file-sensor', 'multi-file']
)

# Setup test data for multiple files
setup_multi_test_data = PythonOperator(
    task_id='setup_multi_test_data',
    python_callable=create_test_files,
    dag=dag2
)

# Multiple sensors with different timeouts
wait_for_sales_data = FileSensor(
    task_id='wait_for_sales_data',
    filepath='/tmp/sales-data/sales_report_{{ ds_nodash }}.csv',
    fs_conn_id='fs_default',
    poke_interval=30,
    timeout=600,           # 10 minutes (critical)
    dag=dag2
)

wait_for_customer_data = FileSensor(
    task_id='wait_for_customer_data',
    filepath='/tmp/sales-data/customer_data_{{ ds_nodash }}.json',
    fs_conn_id='fs_default',
    poke_interval=45,
    timeout=900,           # 15 minutes (important)
    dag=dag2
)

wait_for_product_catalog = FileSensor(
    task_id='wait_for_product_catalog',
    filepath='/tmp/sales-data/product_catalog_{{ ds_nodash }}.xml',
    fs_conn_id='fs_default',
    poke_interval=60,
    timeout=1200,          # 20 minutes (can be delayed)
    dag=dag2
)

# Process all files once available
process_all_files = BashOperator(
    task_id='process_all_files',
    bash_command='''
    echo "Processing all files for {{ ds }}"
    echo "=== Sales Report ==="
    cat /tmp/sales-data/sales_report_{{ ds_nodash }}.csv
    echo "=== Customer Data ==="
    cat /tmp/sales-data/customer_data_{{ ds_nodash }}.json
    echo "=== Product Catalog ==="
    cat /tmp/sales-data/product_catalog_{{ ds_nodash }}.xml
    echo "All files processed successfully"
    ''',
    dag=dag2
)

# Set dependencies for Task 2 - parallel sensors
setup_multi_test_data >> [wait_for_sales_data, wait_for_customer_data,
                          wait_for_product_catalog] >> process_all_files

# Task 3: Advanced Sensor Configuration
dag3 = DAG(
    'advanced_sales_pipeline',
    default_args=default_args,
    description='Advanced sales pipeline with robust sensor configuration',
    schedule_interval='0 9 * * *',
    catchup=False,
    sla_miss_callback=lambda context: print(
        f"SLA missed for {context['task_instance'].task_id}"),
    tags=['exercise', 'file-sensor', 'advanced']
)


def sensor_success_callback(context):
    """Callback when sensor succeeds"""
    task_instance = context['task_instance']
    execution_date = context['execution_date']
    print(
        f"SUCCESS: Sensor {task_instance.task_id} completed at {datetime.now()}")
    print(f"Execution date: {execution_date}")
    print(f"Duration: {task_instance.duration} seconds")

    # In production: send notification, update monitoring dashboard, etc.


def sensor_failure_callback(context):
    """Callback when sensor fails"""
    task_instance = context['task_instance']
    execution_date = context['execution_date']
    print(
        f"FAILURE: Sensor {task_instance.task_id} failed at {datetime.now()}")
    print(f"Execution date: {execution_date}")
    print(f"Exception: {context.get('exception', 'Unknown error')}")

    # In production: send alert, create incident ticket, etc.


def sla_miss_callback(context):
    """Callback when SLA is missed"""
    task_instance = context['task_instance']
    print(f"SLA MISS: Task {task_instance.task_id} exceeded 30-minute SLA")

    # In production: escalate to operations team


def create_delayed_test_files():
    """Create test files with delay to test advanced features"""
    import time
    import os
    from datetime import datetime

    # Create directory
    os.makedirs('/tmp/sales-data', exist_ok=True)

    print("Creating files with delays to test advanced sensors...")

    # Create files with staggered delays
    today = datetime.now().strftime('%Y%m%d')

    # Immediate file
    with open(f'/tmp/sales-data/sales_report_{today}.csv', 'w') as f:
        f.write('date,product,amount\n2024-01-01,Widget,100')
    print("Created sales report immediately")

    # Delayed files
    time.sleep(10)
    with open(f'/tmp/sales-data/customer_data_{today}.json', 'w') as f:
        f.write('{"customers": [{"id": 1, "name": "John Doe"}]}')
    print("Created customer data after 10 seconds")

    time.sleep(20)
    with open(f'/tmp/sales-data/product_catalog_{today}.xml', 'w') as f:
        f.write('<products><product id="1">Widget</product></products>')
    print("Created product catalog after 30 seconds total")


# Setup advanced test data
setup_advanced_test_data = PythonOperator(
    task_id='setup_advanced_test_data',
    python_callable=create_delayed_test_files,
    dag=dag3
)

# Advanced sensor with reschedule mode and callbacks
advanced_sales_sensor = FileSensor(
    task_id='advanced_sales_sensor',
    filepath='/tmp/sales-data/sales_report_{{ ds_nodash }}.csv',
    fs_conn_id='fs_default',
    mode='reschedule',     # Release worker slot between checks
    poke_interval=20,      # Check every 20 seconds
    timeout=600,           # 10 minute timeout
    retries=3,             # 3 retries with exponential backoff
    retry_delay=timedelta(seconds=30),
    exponential_backoff=True,
    on_success_callback=sensor_success_callback,
    on_failure_callback=sensor_failure_callback,
    sla=timedelta(minutes=30),  # 30-minute SLA
    dag=dag3
)

advanced_customer_sensor = FileSensor(
    task_id='advanced_customer_sensor',
    filepath='/tmp/sales-data/customer_data_{{ ds_nodash }}.json',
    fs_conn_id='fs_default',
    mode='reschedule',
    poke_interval=25,
    timeout=900,
    retries=3,
    retry_delay=timedelta(seconds=45),
    exponential_backoff=True,
    on_success_callback=sensor_success_callback,
    on_failure_callback=sensor_failure_callback,
    sla=timedelta(minutes=30),
    dag=dag3
)

advanced_catalog_sensor = FileSensor(
    task_id='advanced_catalog_sensor',
    filepath='/tmp/sales-data/product_catalog_{{ ds_nodash }}.xml',
    fs_conn_id='fs_default',
    mode='reschedule',
    poke_interval=30,
    timeout=1200,
    retries=3,
    retry_delay=timedelta(minutes=1),
    exponential_backoff=True,
    on_success_callback=sensor_success_callback,
    on_failure_callback=sensor_failure_callback,
    sla=timedelta(minutes=30),
    dag=dag3
)

# Advanced processing with monitoring


def process_with_monitoring():
    """Process files with detailed monitoring"""
    import os
    from datetime import datetime

    today = datetime.now().strftime('%Y%m%d')

    files_processed = 0
    total_size = 0

    file_paths = [
        f'/tmp/sales-data/sales_report_{today}.csv',
        f'/tmp/sales-data/customer_data_{today}.json',
        f'/tmp/sales-data/product_catalog_{today}.xml'
    ]

    for file_path in file_paths:
        if os.path.exists(file_path):
            size = os.path.getsize(file_path)
            total_size += size
            files_processed += 1
            print(f"Processed: {file_path} ({size} bytes)")

    print(
        f"Summary: {files_processed} files processed, {total_size} total bytes")

    # In production: send metrics to monitoring system
    return {
        'files_processed': files_processed,
        'total_size': total_size,
        'processing_time': datetime.now().isoformat()
    }


advanced_processing = PythonOperator(
    task_id='advanced_processing',
    python_callable=process_with_monitoring,
    sla=timedelta(minutes=30),
    dag=dag3
)

# Cleanup task
cleanup_files = BashOperator(
    task_id='cleanup_files',
    bash_command='''
    echo "Cleaning up test files..."
    rm -rf /tmp/sales-data/
    echo "Cleanup completed"
    ''',
    dag=dag3
)

# Set dependencies for Task 3
setup_advanced_test_data >> [advanced_sales_sensor, advanced_customer_sensor,
                             advanced_catalog_sensor] >> advanced_processing >> cleanup_files
