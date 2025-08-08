"""
Solution for Exercise 4: Custom Sensors

This solution demonstrates how to build custom sensors with proper polling logic,
error handling, and event-driven workflow patterns.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.base import BaseSensorOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.context import Context
import json
import os
import time
import requests
import threading
import http.server
import socketserver
from typing import Any, Dict, List, Callable, Optional

default_args = {
    'owner': 'airflow-kata',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Task 1: Database Record Sensor


class OrderStatusSensor(BaseSensorOperator):
    """
    Custom sensor that monitors order status in mock database
    """

    def __init__(self,
                 order_id: int,
                 expected_status: str,
                 db_path: str = '/tmp/mock-ecommerce-db',
                 **kwargs):
        super().__init__(**kwargs)
        self.order_id = order_id
        self.expected_status = expected_status
        self.db_path = db_path

    def poke(self, context: Context) -> bool:
        """Check if order has reached expected status"""
        orders_file = os.path.join(self.db_path, 'orders.json')

        try:
            if not os.path.exists(orders_file):
                self.log.info(f"Orders database file not found: {orders_file}")
                return False

            with open(orders_file, 'r') as f:
                orders = json.load(f)

            # Find the specific order
            for order in orders:
                if order.get('id') == self.order_id:
                    current_status = order.get('status')
                    self.log.info(
                        f"Order {self.order_id} current status: {current_status}")

                    if current_status == self.expected_status:
                        self.log.info(
                            f"Order {self.order_id} reached expected status: {self.expected_status}")
                        return True
                    else:
                        self.log.info(
                            f"Order {self.order_id} status '{current_status}' != expected '{self.expected_status}'")
                        return False

            self.log.warning(f"Order {self.order_id} not found in database")
            return False

        except json.JSONDecodeError as e:
            self.log.error(f"Invalid JSON in orders file: {e}")
            return False
        except Exception as e:
            self.log.error(f"Error checking order status: {e}")
            return False

# Task 2: API Health Monitor Sensor


class APIHealthSensor(BaseSensorOperator):
    """
    Custom sensor that monitors API health and readiness
    """

    def __init__(self,
                 endpoint_url: str,
                 expected_status: int = 200,
                 expected_response: Optional[Dict[str, Any]] = None,
                 headers: Optional[Dict[str, str]] = None,
                 request_timeout: int = 30,
                 **kwargs):
        super().__init__(**kwargs)
        self.endpoint_url = endpoint_url
        self.expected_status = expected_status
        self.expected_response = expected_response or {}
        self.headers = headers or {}
        self.request_timeout = request_timeout

    def poke(self, context: Context) -> bool:
        """Check API health status"""
        try:
            self.log.info(f"Checking API health: {self.endpoint_url}")

            response = requests.get(
                self.endpoint_url,
                headers=self.headers,
                timeout=self.request_timeout
            )

            self.log.info(f"API response status: {response.status_code}")

            # Check status code
            if response.status_code != self.expected_status:
                self.log.info(
                    f"Status code {response.status_code} != expected {self.expected_status}")
                return False

            # Check response content if specified
            if self.expected_response:
                try:
                    response_data = response.json()
                    self.log.info(f"API response data: {response_data}")

                    # Check each expected key-value pair
                    for key, expected_value in self.expected_response.items():
                        actual_value = response_data.get(key)
                        if actual_value != expected_value:
                            self.log.info(
                                f"Response key '{key}': {actual_value} != expected {expected_value}")
                            return False

                except json.JSONDecodeError as e:
                    self.log.error(f"API response is not valid JSON: {e}")
                    return False

            self.log.info("API health check successful")
            return True

        except requests.exceptions.Timeout:
            self.log.warning(
                f"API request timeout after {self.request_timeout} seconds")
            return False
        except requests.exceptions.ConnectionError as e:
            self.log.warning(f"API connection error: {e}")
            return False
        except requests.exceptions.RequestException as e:
            self.log.error(f"API request error: {e}")
            return False
        except Exception as e:
            self.log.error(f"Unexpected error during API health check: {e}")
            return False

# Task 3: Multi-Source Data Readiness Sensor


class MultiSourceReadinessSensor(BaseSensorOperator):
    """
    Custom sensor that waits for multiple data sources to be ready
    """

    def __init__(self,
                 conditions: Dict[str, Callable],
                 require_all: bool = True,
                 condition_timeouts: Optional[Dict[str, int]] = None,
                 **kwargs):
        super().__init__(**kwargs)
        self.conditions = conditions
        self.require_all = require_all
        self.condition_timeouts = condition_timeouts or {}
        self.condition_start_times = {}

    def poke(self, context: Context) -> bool:
        """Check multiple readiness conditions"""
        results = {}
        current_time = time.time()

        for condition_name, condition_func in self.conditions.items():
            # Initialize start time for this condition if not set
            if condition_name not in self.condition_start_times:
                self.condition_start_times[condition_name] = current_time

            # Check if condition has timed out
            condition_timeout = self.condition_timeouts.get(condition_name)
            if condition_timeout:
                elapsed = current_time - \
                    self.condition_start_times[condition_name]
                if elapsed > condition_timeout:
                    self.log.warning(
                        f"Condition '{condition_name}' timed out after {elapsed:.1f} seconds")
                    results[condition_name] = False
                    continue

            try:
                result = condition_func(context)
                results[condition_name] = result
                self.log.info(f"Condition '{condition_name}': {result}")

                # Reset start time on success
                if result:
                    self.condition_start_times[condition_name] = current_time

            except Exception as e:
                self.log.error(
                    f"Error checking condition '{condition_name}': {e}")
                results[condition_name] = False

        # Determine overall result
        if self.require_all:
            success = all(results.values())
            self.log.info(f"All conditions required - Success: {success}")
            self.log.info(f"Condition results: {results}")
        else:
            success = any(results.values())
            self.log.info(f"Any condition sufficient - Success: {success}")
            self.log.info(f"Condition results: {results}")

        return success

# Task 4: Adaptive Polling Sensor


class AdaptiveFileSensor(BaseSensorOperator):
    """
    Custom sensor with adaptive polling based on historical patterns
    """

    def __init__(self,
                 filepath: str,
                 initial_interval: int = 60,
                 min_interval: int = 30,
                 max_interval: int = 300,
                 history_size: int = 20,
                 **kwargs):
        super().__init__(**kwargs)
        self.filepath = filepath
        self.initial_interval = initial_interval
        self.min_interval = min_interval
        self.max_interval = max_interval
        self.history_size = history_size
        self.history_file = f'/tmp/adaptive_sensor_history_{self.task_id}.json'

    def poke(self, context: Context) -> bool:
        """Check file existence with adaptive polling"""
        start_time = time.time()

        # Render template if needed
        filepath = self.filepath.replace(
            '{{ ds_nodash }}', context.get('ds_nodash', ''))

        try:
            file_exists = os.path.exists(filepath)
            duration = time.time() - start_time

            self.log.info(f"Checking file: {filepath} - Exists: {file_exists}")

            # Record this attempt
            self.record_attempt(file_exists, duration)

            if not file_exists:
                # Calculate and log next adaptive interval
                next_interval = self.calculate_adaptive_interval()
                self.log.info(
                    f"File not found. Next adaptive interval: {next_interval}s")

            return file_exists

        except Exception as e:
            duration = time.time() - start_time
            self.log.error(f"Error checking file {filepath}: {e}")
            self.record_attempt(False, duration)
            return False

    def calculate_adaptive_interval(self) -> int:
        """Calculate next polling interval based on historical data"""
        try:
            if not os.path.exists(self.history_file):
                return self.initial_interval

            with open(self.history_file, 'r') as f:
                history = json.load(f)

            attempts = history.get('attempts', [])
            if len(attempts) < 3:  # Need at least 3 attempts for pattern analysis
                return self.initial_interval

            # Calculate success rate from recent attempts
            recent_attempts = attempts[-self.history_size:]
            success_count = sum(
                1 for attempt in recent_attempts if attempt['success'])
            success_rate = success_count / len(recent_attempts)

            self.log.info(
                f"Historical success rate: {success_rate:.2f} ({success_count}/{len(recent_attempts)})")

            # Adjust interval based on success rate
            if success_rate > 0.8:
                # High success rate - can poll less frequently
                new_interval = min(
                    int(self.initial_interval * 1.5), self.max_interval)
                self.log.info(
                    f"High success rate, increasing interval to {new_interval}s")
            elif success_rate < 0.3:
                # Low success rate - poll more frequently
                new_interval = max(
                    int(self.initial_interval * 0.7), self.min_interval)
                self.log.info(
                    f"Low success rate, decreasing interval to {new_interval}s")
            else:
                # Medium success rate - use default interval
                new_interval = self.initial_interval
                self.log.info(
                    f"Medium success rate, using default interval {new_interval}s")

            return new_interval

        except Exception as e:
            self.log.warning(f"Error calculating adaptive interval: {e}")
            return self.initial_interval

    def record_attempt(self, success: bool, duration: float):
        """Record polling attempt for learning"""
        try:
            history = {'attempts': []}

            if os.path.exists(self.history_file):
                with open(self.history_file, 'r') as f:
                    history = json.load(f)

            # Add new attempt
            attempt = {
                'timestamp': datetime.now().isoformat(),
                'success': success,
                'duration': duration
            }

            history['attempts'].append(attempt)

            # Keep only recent attempts
            history['attempts'] = history['attempts'][-self.history_size:]

            # Save updated history
            with open(self.history_file, 'w') as f:
                json.dump(history, f, indent=2)

            self.log.debug(
                f"Recorded attempt: success={success}, duration={duration:.2f}s")

        except Exception as e:
            self.log.warning(f"Error recording attempt: {e}")


# DAG 1: Order Status Sensor
dag1 = DAG(
    'custom_order_status_sensor',
    default_args=default_args,
    description='Custom order status sensor example',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['custom-sensor', 'database', 'orders']
)


def setup_ecommerce_database():
    """Create mock e-commerce database"""
    import os
    import json
    import threading
    import time

    os.makedirs('/tmp/mock-ecommerce-db', exist_ok=True)

    # Initial orders
    orders = [
        {
            "id": 1,
            "customer_id": 101,
            "status": "pending",
            "total": 299.99,
            "created_at": "2024-01-01T10:00:00Z"
        },
        {
            "id": 2,
            "customer_id": 102,
            "status": "processing",
            "total": 149.50,
            "created_at": "2024-01-01T10:15:00Z"
        },
        {
            "id": 3,
            "customer_id": 103,
            "status": "confirmed",
            "total": 89.99,
            "created_at": "2024-01-01T10:30:00Z"
        }
    ]

    with open('/tmp/mock-ecommerce-db/orders.json', 'w') as f:
        json.dump(orders, f, indent=2)

    def update_order_status():
        """Simulate order status updates"""
        time.sleep(30)  # Wait 30 seconds

        try:
            # Update order 1 to shipped
            with open('/tmp/mock-ecommerce-db/orders.json', 'r') as f:
                orders = json.load(f)

            for order in orders:
                if order['id'] == 1:
                    order['status'] = 'shipped'
                    order['shipped_at'] = datetime.now().isoformat()
                    break

            with open('/tmp/mock-ecommerce-db/orders.json', 'w') as f:
                json.dump(orders, f, indent=2)

            print("Updated order 1 status to 'shipped'")

        except Exception as e:
            print(f"Error updating order status: {e}")

    # Start status update thread
    threading.Thread(target=update_order_status, daemon=True).start()
    print("E-commerce database setup completed")


setup_db = PythonOperator(
    task_id='setup_ecommerce_database',
    python_callable=setup_ecommerce_database,
    dag=dag1
)

# Wait for order to be shipped
wait_for_order_shipped = OrderStatusSensor(
    task_id='wait_for_order_shipped',
    order_id=1,
    expected_status='shipped',
    poke_interval=15,
    timeout=120,
    dag=dag1
)

# Process shipped order
process_shipped_order = BashOperator(
    task_id='process_shipped_order',
    bash_command='''
    echo "Processing shipped order..."
    echo "Order details:"
    cat /tmp/mock-ecommerce-db/orders.json | jq '.[] | select(.id == 1)'
    echo "Order processing completed"
    ''',
    dag=dag1
)

setup_db >> wait_for_order_shipped >> process_shipped_order

# DAG 2: API Health Sensor
dag2 = DAG(
    'custom_api_health_sensor',
    default_args=default_args,
    description='Custom API health sensor example',
    schedule_interval=timedelta(hours=2),
    catchup=False,
    tags=['custom-sensor', 'api', 'health']
)


def setup_mock_api_server():
    """Setup mock API server for health monitoring"""
    import threading
    import http.server
    import socketserver
    import json
    import time

    class HealthAPIHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == '/api/health':
                # Simulate API becoming healthy after some time
                # Healthy for first 60 seconds of each 2-minute cycle
                if time.time() % 120 < 60:
                    self.send_response(200)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    response = {
                        "status": "healthy",
                        "version": "1.0",
                        "timestamp": datetime.now().isoformat(),
                        "uptime": int(time.time() % 120)
                    }
                    self.wfile.write(json.dumps(response).encode())
                else:
                    self.send_response(503)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    response = {
                        "status": "maintenance",
                        "message": "API under maintenance",
                        "timestamp": datetime.now().isoformat()
                    }
                    self.wfile.write(json.dumps(response).encode())
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, format, *args):
            pass  # Suppress default logging

    def run_server():
        try:
            with socketserver.TCPServer(("", 8081), HealthAPIHandler) as httpd:
                print("Mock API server running on port 8081")
                httpd.serve_forever()
        except Exception as e:
            print(f"Mock API server error: {e}")

    # Start server in background
    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()
    time.sleep(2)  # Give server time to start
    print("Mock API server started on port 8081")


start_api_server = PythonOperator(
    task_id='start_mock_api_server',
    python_callable=setup_mock_api_server,
    dag=dag2
)

# Wait for API to be healthy
wait_for_api_healthy = APIHealthSensor(
    task_id='wait_for_api_healthy',
    endpoint_url='http://localhost:8081/api/health',
    expected_status=200,
    expected_response={'status': 'healthy', 'version': '1.0'},
    poke_interval=20,
    timeout=180,
    dag=dag2
)

# Process API data
process_api_data = BashOperator(
    task_id='process_api_data',
    bash_command='''
    echo "API is healthy, processing data..."
    echo "API Health Status:"
    curl -s http://localhost:8081/api/health | jq .
    echo "Data processing completed"
    ''',
    dag=dag2
)

start_api_server >> wait_for_api_healthy >> process_api_data

# DAG 3: Multi-Source Readiness Sensor
dag3 = DAG(
    'custom_multi_source_sensor',
    default_args=default_args,
    description='Multi-source data readiness sensor example',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['custom-sensor', 'multi-source', 'ml']
)


def setup_ml_data_sources():
    """Setup mock ML data sources"""
    import os
    import threading
    import time
    from datetime import datetime

    # Create directories
    directories = [
        '/tmp/ml-data/behavior',
        '/tmp/ml-data/catalog',
        '/tmp/ml-data/inventory'
    ]

    for directory in directories:
        os.makedirs(directory, exist_ok=True)

    def create_behavior_logs():
        time.sleep(20)  # 20 second delay
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = f'/tmp/ml-data/behavior/user_behavior_{timestamp}.log'
        with open(log_file, 'w') as f:
            f.write('user_id,action,timestamp\n')
            f.write('1,click,2024-01-01T10:00:00Z\n')
            f.write('2,purchase,2024-01-01T10:05:00Z\n')
            f.write('3,view,2024-01-01T10:10:00Z\n')
        print(f"Created behavior logs: {log_file}")

    def create_catalog_api():
        time.sleep(10)  # 10 second delay
        # Simulate catalog API by creating a status file
        with open('/tmp/ml-data/catalog/api_status.json', 'w') as f:
            json.dump({
                "catalog_version": 105,
                "last_updated": datetime.now().isoformat(),
                "status": "ready"
            }, f)
        print("Created catalog API status")

    def create_inventory_snapshot():
        time.sleep(40)  # 40 second delay
        snapshot_file = '/tmp/ml-data/inventory/daily_snapshot.json'
        with open(snapshot_file, 'w') as f:
            json.dump({
                "date": datetime.now().strftime('%Y-%m-%d'),
                "products": [
                    {"id": 1, "stock": 100, "reserved": 10},
                    {"id": 2, "stock": 50, "reserved": 5},
                    {"id": 3, "stock": 200, "reserved": 20}
                ],
                "total_products": 3,
                "snapshot_time": datetime.now().isoformat()
            }, f)
        print(f"Created inventory snapshot: {snapshot_file}")

    # Start data creation threads
    threading.Thread(target=create_behavior_logs, daemon=True).start()
    threading.Thread(target=create_catalog_api, daemon=True).start()
    threading.Thread(target=create_inventory_snapshot, daemon=True).start()

    print("ML data sources setup initiated")

# Define condition functions for multi-source sensor


def check_behavior_logs_ready(context):
    """Check if recent behavior logs are available"""
    import glob
    from datetime import datetime, timedelta

    log_pattern = '/tmp/ml-data/behavior/user_behavior_*.log'
    log_files = glob.glob(log_pattern)

    if not log_files:
        return False

    # Check if any log file is recent (within last hour)
    one_hour_ago = datetime.now() - timedelta(hours=1)

    for log_file in log_files:
        file_time = datetime.fromtimestamp(os.path.getmtime(log_file))
        if file_time > one_hour_ago:
            return True

    return False


def check_catalog_api_ready(context):
    """Check if catalog API is ready with version > 100"""
    try:
        status_file = '/tmp/ml-data/catalog/api_status.json'
        if not os.path.exists(status_file):
            return False

        with open(status_file, 'r') as f:
            status = json.load(f)

        version = status.get('catalog_version', 0)
        return version > 100

    except Exception:
        return False


def check_inventory_snapshot_ready(context):
    """Check if today's inventory snapshot is available"""
    try:
        snapshot_file = '/tmp/ml-data/inventory/daily_snapshot.json'
        if not os.path.exists(snapshot_file):
            return False

        with open(snapshot_file, 'r') as f:
            snapshot = json.load(f)

        snapshot_date = snapshot.get('date')
        today = datetime.now().strftime('%Y-%m-%d')

        return snapshot_date == today

    except Exception:
        return False


setup_ml_data = PythonOperator(
    task_id='setup_ml_data_sources',
    python_callable=setup_ml_data_sources,
    dag=dag3
)

# Wait for all ML data sources to be ready
wait_for_all_ml_sources = MultiSourceReadinessSensor(
    task_id='wait_for_all_ml_sources',
    conditions={
        'behavior_logs': check_behavior_logs_ready,
        'catalog_api': check_catalog_api_ready,
        'inventory_snapshot': check_inventory_snapshot_ready
    },
    require_all=True,
    condition_timeouts={
        'behavior_logs': 60,    # 1 minute timeout
        'catalog_api': 30,      # 30 second timeout
        'inventory_snapshot': 90  # 1.5 minute timeout
    },
    poke_interval=15,
    timeout=180,
    dag=dag3
)

# Process ML data
process_ml_data = BashOperator(
    task_id='process_ml_data',
    bash_command='''
    echo "All ML data sources are ready, starting processing..."
    echo "=== Behavior Logs ==="
    ls -la /tmp/ml-data/behavior/
    echo "=== Catalog Status ==="
    cat /tmp/ml-data/catalog/api_status.json
    echo "=== Inventory Snapshot ==="
    cat /tmp/ml-data/inventory/daily_snapshot.json
    echo "ML data processing completed"
    ''',
    dag=dag3
)

setup_ml_data >> wait_for_all_ml_sources >> process_ml_data

# DAG 4: Adaptive File Sensor
dag4 = DAG(
    'custom_adaptive_file_sensor',
    default_args=default_args,
    description='Adaptive file sensor with learning capabilities',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['custom-sensor', 'adaptive', 'file']
)


def create_adaptive_test_files():
    """Create test files with variable delays to test adaptive behavior"""
    import threading
    import time
    import random
    from datetime import datetime

    os.makedirs('/tmp/adaptive-files', exist_ok=True)

    def create_file_with_random_delay():
        # Random delay between 10-60 seconds to simulate variable arrival times
        delay = random.randint(10, 60)
        time.sleep(delay)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'/tmp/adaptive-files/data_{timestamp}.txt'

        with open(filename, 'w') as f:
            f.write(f'Test data created at {datetime.now().isoformat()}\n')
            f.write(f'Delay was {delay} seconds\n')

        print(f"Created adaptive test file: {filename} (after {delay}s delay)")

    # Create multiple files with different delays
    for i in range(3):
        threading.Thread(target=create_file_with_random_delay,
                         daemon=True).start()

    print("Adaptive test file creation initiated")


setup_adaptive_files = PythonOperator(
    task_id='setup_adaptive_test_files',
    python_callable=create_adaptive_test_files,
    dag=dag4
)

# Adaptive file sensor
wait_for_adaptive_file = AdaptiveFileSensor(
    task_id='wait_for_adaptive_file',
    filepath='/tmp/adaptive-files/data_*.txt',
    initial_interval=30,
    min_interval=15,
    max_interval=120,
    history_size=10,
    poke_interval=30,  # This will be adapted based on history
    timeout=300,
    dag=dag4
)

# Analyze adaptive performance


def analyze_adaptive_performance():
    """Analyze the performance of the adaptive sensor"""
    history_file = '/tmp/adaptive_sensor_history_wait_for_adaptive_file.json'

    if os.path.exists(history_file):
        with open(history_file, 'r') as f:
            history = json.load(f)

        attempts = history.get('attempts', [])

        if attempts:
            success_count = sum(
                1 for attempt in attempts if attempt['success'])
            total_attempts = len(attempts)
            success_rate = success_count / total_attempts

            avg_duration = sum(attempt['duration']
                               for attempt in attempts) / total_attempts

            print(f"Adaptive Sensor Performance Analysis:")
            print(f"Total attempts: {total_attempts}")
            print(f"Successful attempts: {success_count}")
            print(f"Success rate: {success_rate:.2%}")
            print(f"Average check duration: {avg_duration:.3f} seconds")

            # Show recent attempts
            print(f"Recent attempts:")
            for attempt in attempts[-5:]:
                print(
                    f"  {attempt['timestamp']}: success={attempt['success']}, duration={attempt['duration']:.3f}s")
        else:
            print("No adaptive sensor history available yet")
    else:
        print("Adaptive sensor history file not found")


analyze_performance = PythonOperator(
    task_id='analyze_adaptive_performance',
    python_callable=analyze_adaptive_performance,
    dag=dag4
)

# Process adaptive files
process_adaptive_files = BashOperator(
    task_id='process_adaptive_files',
    bash_command='''
    echo "Processing adaptive files..."
    echo "Found files:"
    ls -la /tmp/adaptive-files/
    echo "File contents:"
    cat /tmp/adaptive-files/data_*.txt
    echo "Adaptive file processing completed"
    ''',
    dag=dag4
)

setup_adaptive_files >> wait_for_adaptive_file >> [
    analyze_performance, process_adaptive_files]

# Cleanup DAG
cleanup_dag = DAG(
    'custom_sensor_cleanup',
    default_args=default_args,
    description='Cleanup custom sensor test data',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['custom-sensor', 'cleanup']
)

cleanup_all_custom_sensors = BashOperator(
    task_id='cleanup_all_custom_sensors',
    bash_command='''
    echo "Cleaning up all custom sensor test data..."
    rm -rf /tmp/mock-ecommerce-db/
    rm -rf /tmp/ml-data/
    rm -rf /tmp/adaptive-files/
    rm -f /tmp/adaptive_sensor_history_*.json
    echo "Custom sensor cleanup completed"
    ''',
    dag=cleanup_dag
)
