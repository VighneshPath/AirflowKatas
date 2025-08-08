"""
S3 Sensor Examples with Mock Setup

This module demonstrates S3KeySensor usage with a mock S3 environment.
In production, you would connect to real AWS S3 buckets.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Note: S3KeySensor requires airflow-providers-amazon-aws
# For this kata, we'll simulate S3 behavior with local files
# In production, uncomment the following imports:
# from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
# from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator

default_args = {
    'owner': 'airflow-kata',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Example 1: Mock S3 Sensor (simulating S3 with local filesystem)
dag1 = DAG(
    's3_sensor_mock_basic',
    default_args=default_args,
    description='Mock S3 sensor example using filesystem',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['sensors', 's3', 'mock', 'basic']
)


def create_mock_s3_structure():
    """Create mock S3 bucket structure using local filesystem"""
    import os

    # Create mock S3 bucket directories
    buckets = [
        '/tmp/mock-s3/data-lake/raw',
        '/tmp/mock-s3/data-lake/processed',
        '/tmp/mock-s3/backups',
        '/tmp/mock-s3/logs'
    ]

    for bucket in buckets:
        os.makedirs(bucket, exist_ok=True)

    print("Mock S3 structure created:")
    for bucket in buckets:
        print(f"  - {bucket}")


def upload_to_mock_s3():
    """Simulate uploading files to S3"""
    import os
    from datetime import datetime

    # Create sample data files
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    files = {
        f'/tmp/mock-s3/data-lake/raw/sales_data_{timestamp}.csv': 'id,product,amount\n1,widget,100\n2,gadget,200',
        f'/tmp/mock-s3/data-lake/raw/customer_data_{timestamp}.json': '{"customers": [{"id": 1, "name": "John"}]}',
        f'/tmp/mock-s3/logs/application_{timestamp}.log': 'INFO: Application started successfully'
    }

    for file_path, content in files.items():
        with open(file_path, 'w') as f:
            f.write(content)
        print(f"Uploaded: {file_path}")


def check_mock_s3_file(file_pattern: str) -> bool:
    """Mock S3 file checking function"""
    import glob
    import os

    # Use glob to check for files matching pattern
    matches = glob.glob(file_pattern)
    exists = len(matches) > 0

    print(f"Checking pattern: {file_pattern}")
    print(f"Found {len(matches)} matching files: {matches}")

    return exists


# Create mock S3 environment
setup_mock_s3 = PythonOperator(
    task_id='setup_mock_s3',
    python_callable=create_mock_s3_structure,
    dag=dag1
)

# Simulate file upload to S3
upload_files = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_mock_s3,
    dag=dag1
)

# Mock S3 sensor using Python sensor pattern


def wait_for_s3_file():
    """Custom sensor function to wait for S3-like file"""
    import time
    import glob

    file_pattern = '/tmp/mock-s3/data-lake/raw/sales_data_*.csv'
    timeout = 120  # 2 minutes
    poke_interval = 10  # 10 seconds

    start_time = time.time()

    while time.time() - start_time < timeout:
        matches = glob.glob(file_pattern)
        if matches:
            print(f"File found: {matches[0]}")
            return matches[0]

        print(f"File not found, waiting {poke_interval} seconds...")
        time.sleep(poke_interval)

    raise Exception(f"Timeout waiting for file matching {file_pattern}")


wait_for_sales_data = PythonOperator(
    task_id='wait_for_sales_data',
    python_callable=wait_for_s3_file,
    dag=dag1
)

# Process the S3 file once detected
process_s3_data = BashOperator(
    task_id='process_s3_data',
    bash_command='''
    echo "Processing S3 data files..."
    echo "Sales data files:"
    ls -la /tmp/mock-s3/data-lake/raw/sales_data_*
    echo "File contents:"
    cat /tmp/mock-s3/data-lake/raw/sales_data_*
    ''',
    dag=dag1
)

# Set dependencies
setup_mock_s3 >> upload_files >> wait_for_sales_data >> process_s3_data

# Example 2: Multiple S3 sensors with different configurations
dag2 = DAG(
    's3_sensor_multiple',
    default_args=default_args,
    description='Multiple S3 sensors with different patterns',
    schedule_interval=timedelta(hours=2),
    catchup=False,
    tags=['sensors', 's3', 'multiple']
)


def create_batch_files():
    """Create multiple files simulating batch processing"""
    import os
    from datetime import datetime

    timestamp = datetime.now().strftime('%Y%m%d')

    # Create different types of batch files
    files = {
        f'/tmp/mock-s3/data-lake/raw/orders_{timestamp}.csv': 'order_id,customer_id,total\n1,101,250.00\n2,102,175.50',
        f'/tmp/mock-s3/data-lake/raw/inventory_{timestamp}.json': '{"products": [{"id": 1, "stock": 100}]}',
        f'/tmp/mock-s3/data-lake/raw/transactions_{timestamp}.parquet': 'mock_parquet_data_placeholder',
        f'/tmp/mock-s3/backups/db_backup_{timestamp}.sql': 'CREATE TABLE mock_backup AS SELECT * FROM production;'
    }

    for file_path, content in files.items():
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w') as f:
            f.write(content)
        print(f"Created batch file: {file_path}")


def wait_for_multiple_files():
    """Wait for multiple S3 files to be available"""
    import time
    import glob

    required_patterns = [
        '/tmp/mock-s3/data-lake/raw/orders_*.csv',
        '/tmp/mock-s3/data-lake/raw/inventory_*.json',
        '/tmp/mock-s3/data-lake/raw/transactions_*.parquet'
    ]

    timeout = 180  # 3 minutes
    poke_interval = 15  # 15 seconds
    start_time = time.time()

    while time.time() - start_time < timeout:
        all_found = True
        found_files = []

        for pattern in required_patterns:
            matches = glob.glob(pattern)
            if matches:
                found_files.extend(matches)
            else:
                all_found = False
                break

        if all_found:
            print(f"All required files found: {found_files}")
            return found_files

        print(
            f"Waiting for all files... ({len(found_files)}/{len(required_patterns)} found)")
        time.sleep(poke_interval)

    raise Exception("Timeout waiting for all required S3 files")


create_batch_files_task = PythonOperator(
    task_id='create_batch_files',
    python_callable=create_batch_files,
    dag=dag2
)

wait_for_batch_files = PythonOperator(
    task_id='wait_for_batch_files',
    python_callable=wait_for_multiple_files,
    dag=dag2
)

# Process all files once available
process_batch_data = BashOperator(
    task_id='process_batch_data',
    bash_command='''
    echo "Processing batch data files..."
    echo "=== Orders ==="
    cat /tmp/mock-s3/data-lake/raw/orders_*.csv
    echo "=== Inventory ==="
    cat /tmp/mock-s3/data-lake/raw/inventory_*.json
    echo "=== File Summary ==="
    ls -la /tmp/mock-s3/data-lake/raw/
    ''',
    dag=dag2
)

create_batch_files_task >> wait_for_batch_files >> process_batch_data

# Example 3: Production-ready S3 sensor configuration (commented for kata)
"""
# Uncomment this section when using real AWS S3

dag3 = DAG(
    's3_sensor_production',
    default_args=default_args,
    description='Production S3 sensor example',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['sensors', 's3', 'production']
)

# Real S3KeySensor configuration
wait_for_s3_data = S3KeySensor(
    task_id='wait_for_daily_data',
    bucket_name='my-data-lake',
    bucket_key='raw/daily/{{ ds }}/data.csv',
    aws_conn_id='aws_default',
    mode='reschedule',  # Don't block worker slots
    poke_interval=300,  # Check every 5 minutes
    timeout=3600,       # 1 hour timeout
    retries=3,
    retry_delay=timedelta(minutes=10),
    dag=dag3
)

# Process S3 data
process_s3_file = S3CreateObjectOperator(
    task_id='process_and_store',
    s3_bucket='my-processed-data',
    s3_key='processed/{{ ds }}/processed_data.csv',
    data='Processed data content',
    aws_conn_id='aws_default',
    dag=dag3
)

wait_for_s3_data >> process_s3_file
"""

# Cleanup task for all examples
cleanup_dag = DAG(
    's3_sensor_cleanup',
    default_args=default_args,
    description='Cleanup mock S3 files',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['sensors', 's3', 'cleanup']
)

cleanup_mock_s3 = BashOperator(
    task_id='cleanup_mock_s3',
    bash_command='''
    echo "Cleaning up mock S3 files..."
    rm -rf /tmp/mock-s3/
    echo "Mock S3 cleanup completed"
    ''',
    dag=cleanup_dag
)
