"""
Custom Sensor Examples

This module demonstrates how to create custom sensors with proper polling logic,
error handling, and event-driven workflow patterns.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.base import BaseSensorOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.context import Context
import time
import json
import os
import random
import requests
from typing import Any, Dict, Optional

default_args = {
    'owner': 'airflow-kata',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Example 1: Database Record Sensor


class DatabaseRecordSensor(BaseSensorOperator):
    """
    Custom sensor that waits for specific records to appear in a database.
    For this example, we'll simulate database behavior with JSON files.
    """

    def __init__(self,
                 table_name: str,
                 condition: Dict[str, Any],
                 db_path: str = '/tmp/mock-db',
                 **kwargs):
        super().__init__(**kwargs)
        self.table_name = table_name
        self.condition = condition
        self.db_path = db_path

    def poke(self, context: Context) -> bool:
        """Check if records matching condition exist in the mock database"""
        table_file = os.path.join(self.db_path, f'{self.table_name}.json')

        if not os.path.exists(table_file):
            self.log.info(f"Table {self.table_name} does not exist yet")
            return False

        try:
            with open(table_file, 'r') as f:
                records = json.load(f)

            # Check if any record matches all conditions
            for record in records:
                if all(record.get(key) == value for key, value in self.condition.items()):
                    self.log.info(f"Found matching record: {record}")
                    return True

            self.log.info(f"No records match condition: {self.condition}")
            return False

        except Exception as e:
            self.log.error(f"Error checking database: {e}")
            return False

# Example 2: API Status Sensor


class APIStatusSensor(BaseSensorOperator):
    """
    Custom sensor that monitors API endpoint status and response content.
    """

    def __init__(self,
                 endpoint_url: str,
                 expected_status: int = 200,
                 expected_response_key: Optional[str] = None,
                 expected_response_value: Optional[Any] = None,
                 headers: Optional[Dict[str, str]] = None,
                 timeout: int = 30,
                 **kwargs):
        super().__init__(**kwargs)
        self.endpoint_url = endpoint_url
        self.expected_status = expected_status
        self.expected_response_key = expected_response_key
        self.expected_response_value = expected_response_value
        self.headers = headers or {}
        self.request_timeout = timeout

    def poke(self, context: Context) -> bool:
        """Check API endpoint status and response content"""
        try:
            self.log.info(f"Checking API endpoint: {self.endpoint_url}")

            response = requests.get(
                self.endpoint_url,
                headers=self.headers,
                timeout=self.request_timeout
            )

            # Check status code
            if response.status_code != self.expected_status:
                self.log.info(
                    f"Status code {response.status_code} != expected {self.expected_status}")
                return False

            # Check response content if specified
            if self.expected_response_key and self.expected_response_value:
                try:
                    response_data = response.json()
                    actual_value = response_data.get(
                        self.expected_response_key)

                    if actual_value != self.expected_response_value:
                        self.log.info(
                            f"Response value {actual_value} != expected {self.expected_response_value}")
                        return False

                except json.JSONDecodeError:
                    self.log.error("Response is not valid JSON")
                    return False

            self.log.info("API endpoint check successful")
            return True

        except requests.exceptions.RequestException as e:
            self.log.warning(f"API request failed: {e}")
            return False
        except Exception as e:
            self.log.error(f"Unexpected error: {e}")
            return False

# Example 3: Multi-Condition Sensor


class MultiConditionSensor(BaseSensorOperator):
    """
    Custom sensor that waits for multiple conditions to be met simultaneously.
    """

    def __init__(self,
                 conditions: Dict[str, callable],
                 require_all: bool = True,
                 **kwargs):
        super().__init__(**kwargs)
        self.conditions = conditions
        self.require_all = require_all

    def poke(self, context: Context) -> bool:
        """Check multiple conditions"""
        results = {}

        for condition_name, condition_func in self.conditions.items():
            try:
                result = condition_func(context)
                results[condition_name] = result
                self.log.info(f"Condition '{condition_name}': {result}")
            except Exception as e:
                self.log.error(
                    f"Error checking condition '{condition_name}': {e}")
                results[condition_name] = False

        if self.require_all:
            # All conditions must be True
            success = all(results.values())
            self.log.info(f"All conditions met: {success}")
        else:
            # At least one condition must be True
            success = any(results.values())
            self.log.info(f"Any condition met: {success}")

        return success

# Example 4: Adaptive Polling Sensor


class AdaptivePollingS3Sensor(BaseSensorOperator):
    """
    Custom sensor with adaptive polling that adjusts intervals based on historical data.
    """

    def __init__(self,
                 bucket_path: str,
                 key_pattern: str,
                 initial_interval: int = 60,
                 max_interval: int = 300,
                 min_interval: int = 30,
                 **kwargs):
        super().__init__(**kwargs)
        self.bucket_path = bucket_path
        self.key_pattern = key_pattern
        self.initial_interval = initial_interval
        self.max_interval = max_interval
        self.min_interval = min_interval
        self.history_file = f'/tmp/sensor_history_{self.task_id}.json'

    def get_adaptive_interval(self) -> int:
        """Calculate adaptive polling interval based on historical success patterns"""
        try:
            if os.path.exists(self.history_file):
                with open(self.history_file, 'r') as f:
                    history = json.load(f)

                recent_attempts = history.get(
                    'attempts', [])[-10:]  # Last 10 attempts

                if recent_attempts:
                    # Calculate success rate
                    success_rate = sum(
                        1 for attempt in recent_attempts if attempt['success']) / len(recent_attempts)

                    # Adjust interval based on success rate
                    if success_rate > 0.8:  # High success rate - can poll less frequently
                        interval = min(self.initial_interval *
                                       1.5, self.max_interval)
                    elif success_rate < 0.3:  # Low success rate - poll more frequently
                        interval = max(self.initial_interval *
                                       0.7, self.min_interval)
                    else:
                        interval = self.initial_interval

                    self.log.info(
                        f"Adaptive interval: {interval}s (success rate: {success_rate:.2f})")
                    return int(interval)

            return self.initial_interval

        except Exception as e:
            self.log.warning(f"Error calculating adaptive interval: {e}")
            return self.initial_interval

    def record_attempt(self, success: bool):
        """Record polling attempt for adaptive learning"""
        try:
            history = {'attempts': []}

            if os.path.exists(self.history_file):
                with open(self.history_file, 'r') as f:
                    history = json.load(f)

            history['attempts'].append({
                'timestamp': datetime.now().isoformat(),
                'success': success
            })

            # Keep only last 50 attempts
            history['attempts'] = history['attempts'][-50:]

            with open(self.history_file, 'w') as f:
                json.dump(history, f)

        except Exception as e:
            self.log.warning(f"Error recording attempt: {e}")

    def poke(self, context: Context) -> bool:
        """Check for file with adaptive polling"""
        import glob

        # Render template
        key_pattern = self.key_pattern.replace(
            '{{ ds_nodash }}', context['ds_nodash'])
        full_path = os.path.join(self.bucket_path, key_pattern)

        matches = glob.glob(full_path)
        success = len(matches) > 0

        if success:
            self.log.info(f"Found file(s): {matches}")
        else:
            self.log.info(f"File not found: {full_path}")

        # Record attempt for learning
        self.record_attempt(success)

        # Adjust next polling interval if not successful
        if not success:
            adaptive_interval = self.get_adaptive_interval()
            # Note: In real implementation, you'd need to modify the sensor's poke_interval
            # This is for demonstration purposes
            self.log.info(f"Next poll in {adaptive_interval} seconds")

        return success


# DAG 1: Database Record Sensor Example
dag1 = DAG(
    'custom_database_sensor',
    default_args=default_args,
    description='Custom database record sensor example',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['custom-sensor', 'database']
)


def setup_mock_database():
    """Create mock database with initial data"""
    import os

    os.makedirs('/tmp/mock-db', exist_ok=True)

    # Create initial tables
    tables = {
        'orders': [
            {'id': 1, 'customer_id': 101, 'status': 'pending', 'amount': 100.00},
            {'id': 2, 'customer_id': 102, 'status': 'completed', 'amount': 250.50}
        ],
        'payments': [
            {'id': 1, 'order_id': 2, 'status': 'completed', 'amount': 250.50}
        ]
    }

    for table_name, records in tables.items():
        with open(f'/tmp/mock-db/{table_name}.json', 'w') as f:
            json.dump(records, f, indent=2)

    print("Mock database created with initial data")


def add_new_payment():
    """Simulate adding a new payment record"""
    import time

    time.sleep(20)  # Wait 20 seconds before adding

    payments_file = '/tmp/mock-db/payments.json'

    with open(payments_file, 'r') as f:
        payments = json.load(f)

    # Add new payment for order 1
    new_payment = {
        'id': 2,
        'order_id': 1,
        'status': 'completed',
        'amount': 100.00
    }

    payments.append(new_payment)

    with open(payments_file, 'w') as f:
        json.dump(payments, f, indent=2)

    print(f"Added new payment: {new_payment}")


setup_db = PythonOperator(
    task_id='setup_mock_database',
    python_callable=setup_mock_database,
    dag=dag1
)

add_payment = PythonOperator(
    task_id='add_new_payment',
    python_callable=add_new_payment,
    dag=dag1
)

# Wait for payment to be completed for order 1
wait_for_payment = DatabaseRecordSensor(
    task_id='wait_for_payment_completion',
    table_name='payments',
    condition={'order_id': 1, 'status': 'completed'},
    poke_interval=10,
    timeout=120,
    dag=dag1
)

process_payment = BashOperator(
    task_id='process_payment',
    bash_command='''
    echo "Processing completed payment for order 1"
    cat /tmp/mock-db/payments.json
    ''',
    dag=dag1
)

setup_db >> add_payment >> wait_for_payment >> process_payment

# DAG 2: API Status Sensor Example
dag2 = DAG(
    'custom_api_sensor',
    default_args=default_args,
    description='Custom API status sensor example',
    schedule_interval=timedelta(hours=2),
    catchup=False,
    tags=['custom-sensor', 'api']
)


def start_mock_api_server():
    """Start a simple mock API server"""
    import threading
    import http.server
    import socketserver
    import json

    class MockAPIHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == '/health':
                # Simulate API becoming healthy after some time
                import time
                if time.time() % 60 < 30:  # Healthy for first 30 seconds of each minute
                    self.send_response(200)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    response = {'status': 'healthy',
                                'timestamp': datetime.now().isoformat()}
                    self.wfile.write(json.dumps(response).encode())
                else:
                    self.send_response(503)
                    self.end_headers()
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, format, *args):
            pass  # Suppress log messages

    def run_server():
        try:
            with socketserver.TCPServer(("", 8080), MockAPIHandler) as httpd:
                httpd.serve_forever()
        except Exception as e:
            print(f"Mock API server error: {e}")

    # Start server in background thread
    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()

    print("Mock API server started on port 8080")
    time.sleep(2)  # Give server time to start


start_api = PythonOperator(
    task_id='start_mock_api',
    python_callable=start_mock_api_server,
    dag=dag2
)

# Wait for API to be healthy
wait_for_api_health = APIStatusSensor(
    task_id='wait_for_api_health',
    endpoint_url='http://localhost:8080/health',
    expected_status=200,
    expected_response_key='status',
    expected_response_value='healthy',
    poke_interval=15,
    timeout=180,
    dag=dag2
)

process_api_data = BashOperator(
    task_id='process_api_data',
    bash_command='''
    echo "API is healthy, processing data..."
    curl -s http://localhost:8080/health | jq .
    ''',
    dag=dag2
)

start_api >> wait_for_api_health >> process_api_data

# DAG 3: Multi-Condition Sensor Example
dag3 = DAG(
    'custom_multi_condition_sensor',
    default_args=default_args,
    description='Multi-condition sensor example',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['custom-sensor', 'multi-condition']
)


def setup_multi_condition_environment():
    """Setup environment for multi-condition testing"""
    import os
    import threading
    import time

    os.makedirs('/tmp/multi-condition', exist_ok=True)

    def create_file_after_delay(filename, delay):
        time.sleep(delay)
        with open(f'/tmp/multi-condition/{filename}', 'w') as f:
            f.write(f'Content for {filename}')
        print(f"Created {filename} after {delay} seconds")

    # Create files with different delays
    threading.Thread(target=create_file_after_delay,
                     args=('file1.txt', 10)).start()
    threading.Thread(target=create_file_after_delay,
                     args=('file2.txt', 20)).start()
    threading.Thread(target=create_file_after_delay,
                     args=('file3.txt', 30)).start()

# Define condition functions


def check_file1_exists(context):
    return os.path.exists('/tmp/multi-condition/file1.txt')


def check_file2_exists(context):
    return os.path.exists('/tmp/multi-condition/file2.txt')


def check_file3_exists(context):
    return os.path.exists('/tmp/multi-condition/file3.txt')


def check_time_condition(context):
    # Example: only true during certain seconds of the minute
    return datetime.now().second < 45


setup_multi_env = PythonOperator(
    task_id='setup_multi_condition_env',
    python_callable=setup_multi_condition_environment,
    dag=dag3
)

# Wait for all files to exist
wait_for_all_conditions = MultiConditionSensor(
    task_id='wait_for_all_files',
    conditions={
        'file1_exists': check_file1_exists,
        'file2_exists': check_file2_exists,
        'file3_exists': check_file3_exists
    },
    require_all=True,  # All conditions must be met
    poke_interval=10,
    timeout=120,
    dag=dag3
)

# Wait for any file to exist
wait_for_any_condition = MultiConditionSensor(
    task_id='wait_for_any_file',
    conditions={
        'file1_exists': check_file1_exists,
        'file2_exists': check_file2_exists,
        'file3_exists': check_file3_exists
    },
    require_all=False,  # Any condition can be met
    poke_interval=5,
    timeout=60,
    dag=dag3
)

process_multi_condition = BashOperator(
    task_id='process_multi_condition',
    bash_command='''
    echo "Multi-condition check completed"
    ls -la /tmp/multi-condition/
    ''',
    dag=dag3
)

setup_multi_env >> [wait_for_all_conditions,
                    wait_for_any_condition] >> process_multi_condition

# Cleanup DAG
cleanup_dag = DAG(
    'custom_sensor_cleanup',
    default_args=default_args,
    description='Cleanup custom sensor test data',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['custom-sensor', 'cleanup']
)

cleanup_custom_sensors = BashOperator(
    task_id='cleanup_custom_sensors',
    bash_command='''
    echo "Cleaning up custom sensor test data..."
    rm -rf /tmp/mock-db/
    rm -rf /tmp/multi-condition/
    rm -f /tmp/sensor_history_*.json
    echo "Custom sensor cleanup completed"
    ''',
    dag=cleanup_dag
)
