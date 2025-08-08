"""
Solution for Exercise 2: S3 Sensors and Mock Cloud Storage

This solution demonstrates S3-like sensor implementation using mock cloud storage
with proper timeout and retry configurations.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
import os
import glob
import time
import json


class MockS3Sensor(BaseSensorOperator):
    """
    Custom sensor that simulates S3KeySensor behavior using local filesystem
    """

    def __init__(self, bucket_path: str, key_pattern: str, **kwargs):
        super().__init__(**kwargs)
        self.bucket_path = bucket_path
        self.key_pattern = key_pattern

    def poke(self, context: Context) -> bool:
        """Check if the file exists in mock S3 bucket"""
        full_path = os.path.join(self.bucket_path, self.key_pattern)

        # Handle templated patterns (like {{ ds_nodash }})
        full_path = self.render_template(full_path, context)

        # Use glob for pattern matching
        matches = glob.glob(full_path)

        if matches:
            self.log.info(f"Found file(s): {matches}")
            return True
        else:
            self.log.info(f"File not found: {full_path}")
            return False

    def render_template(self, template: str, context: Context) -> str:
        """Simple template rendering for date patterns"""
        ds_nodash = context['ds_nodash']
        return template.replace('{{ ds_nodash }}', ds_nodash)


default_args = {
    'owner': 'airflow-kata',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Task 1: Mock S3 Sensor Implementation
dag1 = DAG(
    'cloud_data_pipeline',
    default_args=default_args,
    description='Cloud data pipeline with mock S3 sensors',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['exercise', 's3-sensor', 'cloud']
)


def setup_mock_s3_environment():
    """Create mock S3 bucket structure and test files"""
    import os
    from datetime import datetime

    # Create bucket structure
    buckets = [
        '/tmp/mock-s3/data-bucket/raw',
        '/tmp/mock-s3/data-bucket/processed',
        '/tmp/mock-s3/data-bucket/archive',
        '/tmp/mock-s3/logs-bucket/application',
        '/tmp/mock-s3/logs-bucket/access'
    ]

    for bucket in buckets:
        os.makedirs(bucket, exist_ok=True)

    print("Mock S3 bucket structure created:")
    for bucket in buckets:
        print(f"  - {bucket}")


def create_daily_export():
    """Create daily export file in mock S3"""
    import os
    from datetime import datetime

    today = datetime.now().strftime('%Y%m%d')
    file_path = f'/tmp/mock-s3/data-bucket/raw/daily_export_{today}.json'

    # Simulate delay in file creation
    time.sleep(30)  # 30 second delay

    data = {
        "export_date": today,
        "records": [
            {"id": 1, "value": "sample_data_1",
                "timestamp": "2024-01-01T10:00:00Z"},
            {"id": 2, "value": "sample_data_2", "timestamp": "2024-01-01T10:01:00Z"}
        ],
        "metadata": {
            "total_records": 2,
            "export_time": datetime.now().isoformat()
        }
    }

    with open(file_path, 'w') as f:
        json.dump(data, f, indent=2)

    print(f"Created daily export: {file_path}")


# Setup mock S3 environment
setup_mock_s3 = PythonOperator(
    task_id='setup_mock_s3',
    python_callable=setup_mock_s3_environment,
    dag=dag1
)

# Create test data
create_export_data = PythonOperator(
    task_id='create_export_data',
    python_callable=create_daily_export,
    dag=dag1
)

# Mock S3 sensor
wait_for_daily_export = MockS3Sensor(
    task_id='wait_for_daily_export',
    bucket_path='/tmp/mock-s3/data-bucket/raw',
    key_pattern='daily_export_{{ ds_nodash }}.json',
    poke_interval=45,      # Check every 45 seconds
    timeout=600,           # 10 minute timeout
    retries=2,             # 2 retries
    retry_delay=timedelta(minutes=2),
    dag=dag1
)

# Process the S3 file
process_daily_export = BashOperator(
    task_id='process_daily_export',
    bash_command='''
    echo "Processing daily export for {{ ds }}"
    echo "File contents:"
    cat /tmp/mock-s3/data-bucket/raw/daily_export_{{ ds_nodash }}.json
    echo "Processing completed"
    ''',
    dag=dag1
)

# Set dependencies for Task 1
setup_mock_s3 >> create_export_data >> wait_for_daily_export >> process_daily_export

# Task 2: Multi-Bucket Sensor Coordination
dag2 = DAG(
    'multi_bucket_pipeline',
    default_args=default_args,
    description='Multi-bucket coordination pipeline',
    schedule_interval=timedelta(hours=2),
    catchup=False,
    tags=['exercise', 's3-sensor', 'multi-bucket']
)


def create_multi_bucket_files():
    """Create files in multiple buckets with different timing"""
    import os
    import threading
    from datetime import datetime

    # Create additional bucket structure
    buckets = [
        '/tmp/mock-s3/data-bucket/raw',
        '/tmp/mock-s3/config-bucket/daily',
        '/tmp/mock-s3/logs-bucket/validation'
    ]

    for bucket in buckets:
        os.makedirs(bucket, exist_ok=True)

    today = datetime.now().strftime('%Y%m%d')

    def create_transactions():
        time.sleep(10)  # 10 second delay
        file_path = f'/tmp/mock-s3/data-bucket/raw/transactions_{today}.csv'
        with open(file_path, 'w') as f:
            f.write(
                'id,amount,customer\n1,100.00,customer1\n2,250.50,customer2\n3,75.25,customer3')
        print(f"Created: {file_path}")

    def create_customers():
        time.sleep(20)  # 20 second delay
        file_path = f'/tmp/mock-s3/data-bucket/raw/customers_{today}.json'
        data = {
            "customers": [
                {"id": "customer1", "name": "John Doe",
                    "email": "john@example.com"},
                {"id": "customer2", "name": "Jane Smith",
                    "email": "jane@example.com"},
                {"id": "customer3", "name": "Bob Johnson",
                    "email": "bob@example.com"}
            ]
        }
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"Created: {file_path}")

    def create_config():
        time.sleep(5)  # 5 second delay (arrives early)
        file_path = f'/tmp/mock-s3/config-bucket/daily/processing_config_{today}.yaml'
        config = '''processing:
  batch_size: 1000
  timeout: 300
  retry_attempts: 3
validation:
  required_fields: ["id", "amount", "customer"]
  max_amount: 10000.00
'''
        with open(file_path, 'w') as f:
            f.write(config)
        print(f"Created: {file_path}")

    def create_validation():
        time.sleep(60)  # 60 second delay (generated after validation)
        file_path = f'/tmp/mock-s3/logs-bucket/validation/data_quality_{today}.log'
        log_content = f'''INFO: Data validation started at {datetime.now().isoformat()}
INFO: Validating transactions file
INFO: Found 3 transaction records
INFO: All records passed validation
INFO: Validating customer file
INFO: Found 3 customer records
INFO: All customers have required fields
INFO: Data validation completed successfully
'''
        with open(file_path, 'w') as f:
            f.write(log_content)
        print(f"Created: {file_path}")

    # Start all file creation threads
    threads = [
        threading.Thread(target=create_transactions),
        threading.Thread(target=create_customers),
        threading.Thread(target=create_config),
        threading.Thread(target=create_validation)
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    print("All multi-bucket files created")


# Setup and create files
setup_multi_bucket = PythonOperator(
    task_id='setup_multi_bucket',
    python_callable=create_multi_bucket_files,
    dag=dag2
)

# Multiple sensors with different configurations
wait_for_transactions = MockS3Sensor(
    task_id='wait_for_transactions',
    bucket_path='/tmp/mock-s3/data-bucket/raw',
    key_pattern='transactions_{{ ds_nodash }}.csv',
    poke_interval=30,
    timeout=900,           # 15 minutes (critical path)
    mode='reschedule',     # Release worker slot
    dag=dag2
)

wait_for_customers = MockS3Sensor(
    task_id='wait_for_customers',
    bucket_path='/tmp/mock-s3/data-bucket/raw',
    key_pattern='customers_{{ ds_nodash }}.json',
    poke_interval=45,
    timeout=1200,          # 20 minutes (can be delayed)
    mode='reschedule',
    dag=dag2
)

wait_for_config = MockS3Sensor(
    task_id='wait_for_config',
    bucket_path='/tmp/mock-s3/config-bucket/daily',
    key_pattern='processing_config_{{ ds_nodash }}.yaml',
    poke_interval=15,
    timeout=300,           # 5 minutes (should arrive early)
    mode='poke',           # Keep worker slot (short wait)
    dag=dag2
)

wait_for_validation = MockS3Sensor(
    task_id='wait_for_validation',
    bucket_path='/tmp/mock-s3/logs-bucket/validation',
    key_pattern='data_quality_{{ ds_nodash }}.log',
    poke_interval=60,
    timeout=1500,          # 25 minutes (generated after validation)
    mode='reschedule',
    dag=dag2
)

# Process all files once available
process_multi_bucket_data = BashOperator(
    task_id='process_multi_bucket_data',
    bash_command='''
    echo "Processing multi-bucket data for {{ ds }}"
    echo "=== Configuration ==="
    cat /tmp/mock-s3/config-bucket/daily/processing_config_{{ ds_nodash }}.yaml
    echo "=== Transactions ==="
    cat /tmp/mock-s3/data-bucket/raw/transactions_{{ ds_nodash }}.csv
    echo "=== Customers ==="
    cat /tmp/mock-s3/data-bucket/raw/customers_{{ ds_nodash }}.json
    echo "=== Validation Log ==="
    cat /tmp/mock-s3/logs-bucket/validation/data_quality_{{ ds_nodash }}.log
    echo "Multi-bucket processing completed"
    ''',
    dag=dag2
)

# Set dependencies - all sensors must complete
setup_multi_bucket >> [wait_for_transactions, wait_for_customers,
                       wait_for_config, wait_for_validation] >> process_multi_bucket_data

# Task 3: Advanced Cloud Sensor Patterns
dag3 = DAG(
    'advanced_cloud_sensors',
    default_args=default_args,
    description='Advanced cloud sensor patterns with enterprise features',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['exercise', 's3-sensor', 'advanced']
)


class EnterpriseS3Sensor(MockS3Sensor):
    """
    Advanced S3 sensor with enterprise features
    """

    def __init__(self,
                 performance_tracking: bool = True,
                 circuit_breaker_threshold: int = 5,
                 **kwargs):
        super().__init__(**kwargs)
        self.performance_tracking = performance_tracking
        self.circuit_breaker_threshold = circuit_breaker_threshold
        self.failure_count = 0
        self.performance_file = f'/tmp/sensor_performance_{self.task_id}.json'

    def poke(self, context: Context) -> bool:
        """Enhanced poke with performance tracking and circuit breaker"""
        start_time = time.time()

        try:
            # Check circuit breaker
            if self.failure_count >= self.circuit_breaker_threshold:
                self.log.warning(f"Circuit breaker open for {self.task_id}")
                time.sleep(60)  # Wait before trying again
                self.failure_count = 0  # Reset after wait

            result = super().poke(context)

            if result:
                self.failure_count = 0  # Reset on success
                if self.performance_tracking:
                    self.record_performance(time.time() - start_time, True)

            return result

        except Exception as e:
            self.failure_count += 1
            if self.performance_tracking:
                self.record_performance(time.time() - start_time, False)
            raise e

    def record_performance(self, duration: float, success: bool):
        """Record sensor performance metrics"""
        try:
            data = {'durations': [], 'success_rate': [], 'timestamps': []}

            if os.path.exists(self.performance_file):
                with open(self.performance_file, 'r') as f:
                    data = json.load(f)

            data['durations'].append(duration)
            data['success_rate'].append(1 if success else 0)
            data['timestamps'].append(datetime.now().isoformat())

            # Keep only last 100 records
            for key in data:
                data[key] = data[key][-100:]

            with open(self.performance_file, 'w') as f:
                json.dump(data, f)

        except Exception as e:
            self.log.warning(f"Error recording performance: {e}")


def advanced_sensor_success_callback(context):
    """Advanced success callback with detailed logging"""
    task_instance = context['task_instance']
    duration = task_instance.duration or 0

    print(f"ADVANCED SUCCESS: {task_instance.task_id}")
    print(f"Duration: {duration} seconds")
    print(f"Execution date: {context['execution_date']}")

    # In production: send metrics to monitoring system
    metrics = {
        'task_id': task_instance.task_id,
        'duration': duration,
        'success': True,
        'timestamp': datetime.now().isoformat()
    }

    # Save metrics
    os.makedirs('/tmp/sensor-metrics', exist_ok=True)
    with open(f'/tmp/sensor-metrics/{task_instance.task_id}_metrics.json', 'a') as f:
        f.write(json.dumps(metrics) + '\n')


def advanced_sensor_failure_callback(context):
    """Advanced failure callback with alerting"""
    task_instance = context['task_instance']
    exception = context.get('exception', 'Unknown error')

    print(f"ADVANCED FAILURE: {task_instance.task_id}")
    print(f"Exception: {exception}")
    print(f"Execution date: {context['execution_date']}")

    # In production: send alert to operations team
    alert = {
        'task_id': task_instance.task_id,
        'error': str(exception),
        'success': False,
        'timestamp': datetime.now().isoformat(),
        'severity': 'HIGH'
    }

    # Save alert
    os.makedirs('/tmp/sensor-alerts', exist_ok=True)
    with open(f'/tmp/sensor-alerts/{task_instance.task_id}_alerts.json', 'a') as f:
        f.write(json.dumps(alert) + '\n')


def sla_miss_callback(context):
    """SLA miss callback with escalation"""
    task_instance = context['task_instance']

    print(f"SLA MISS: {task_instance.task_id} exceeded 45-minute SLA")

    # In production: escalate to management
    escalation = {
        'task_id': task_instance.task_id,
        'sla_duration': '45 minutes',
        'actual_duration': task_instance.duration,
        'timestamp': datetime.now().isoformat(),
        'escalation_level': 'MANAGEMENT'
    }

    os.makedirs('/tmp/sla-escalations', exist_ok=True)
    with open('/tmp/sla-escalations/escalations.json', 'a') as f:
        f.write(json.dumps(escalation) + '\n')


def create_enterprise_test_data():
    """Create test data for enterprise sensor testing"""
    import os
    import threading
    from datetime import datetime

    os.makedirs('/tmp/mock-s3/enterprise-bucket/critical', exist_ok=True)

    today = datetime.now().strftime('%Y%m%d')

    def create_critical_data():
        # Simulate variable delay
        delay = 45  # 45 seconds to test timeouts
        time.sleep(delay)

        file_path = f'/tmp/mock-s3/enterprise-bucket/critical/critical_data_{today}.json'
        data = {
            "critical_metrics": {
                "revenue": 1000000,
                "transactions": 50000,
                "errors": 12
            },
            "timestamp": datetime.now().isoformat(),
            "status": "CRITICAL"
        }

        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2)

        print(f"Created critical data: {file_path}")

    # Start file creation
    threading.Thread(target=create_critical_data).start()


# Setup enterprise test environment
setup_enterprise_data = PythonOperator(
    task_id='setup_enterprise_data',
    python_callable=create_enterprise_test_data,
    dag=dag3
)

# Enterprise sensor with all advanced features
enterprise_sensor = EnterpriseS3Sensor(
    task_id='enterprise_critical_sensor',
    bucket_path='/tmp/mock-s3/enterprise-bucket/critical',
    key_pattern='critical_data_{{ ds_nodash }}.json',
    poke_interval=30,      # Start with 30 seconds
    timeout=180,           # 3 minute timeout for testing
    retries=3,
    retry_delay=timedelta(seconds=30),
    exponential_backoff=True,
    mode='reschedule',
    performance_tracking=True,
    circuit_breaker_threshold=3,
    on_success_callback=advanced_sensor_success_callback,
    on_failure_callback=advanced_sensor_failure_callback,
    sla=timedelta(minutes=45),  # 45-minute SLA
    dag=dag3
)

# Performance monitoring task


def analyze_sensor_performance():
    """Analyze sensor performance metrics"""
    import json
    import os

    performance_file = '/tmp/sensor_performance_enterprise_critical_sensor.json'

    if os.path.exists(performance_file):
        with open(performance_file, 'r') as f:
            data = json.load(f)

        durations = data.get('durations', [])
        success_rate = data.get('success_rate', [])

        if durations and success_rate:
            avg_duration = sum(durations) / len(durations)
            success_percentage = (sum(success_rate) / len(success_rate)) * 100

            print(f"Sensor Performance Analysis:")
            print(f"Average duration: {avg_duration:.2f} seconds")
            print(f"Success rate: {success_percentage:.1f}%")
            print(f"Total measurements: {len(durations)}")

            return {
                'avg_duration': avg_duration,
                'success_rate': success_percentage,
                'total_measurements': len(durations)
            }

    print("No performance data available yet")
    return {}


performance_analysis = PythonOperator(
    task_id='analyze_performance',
    python_callable=analyze_sensor_performance,
    dag=dag3
)

# Cleanup task
cleanup_enterprise = BashOperator(
    task_id='cleanup_enterprise',
    bash_command='''
    echo "Cleaning up enterprise test data..."
    rm -rf /tmp/mock-s3/
    rm -rf /tmp/sensor-metrics/
    rm -rf /tmp/sensor-alerts/
    rm -rf /tmp/sla-escalations/
    rm -f /tmp/sensor_performance_*.json
    echo "Enterprise cleanup completed"
    ''',
    dag=dag3
)

# Set dependencies for Task 3
setup_enterprise_data >> enterprise_sensor >> performance_analysis >> cleanup_enterprise
