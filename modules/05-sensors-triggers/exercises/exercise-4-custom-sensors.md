# Exercise 4: Custom Sensors

## Objective

Learn to build custom sensors with proper polling logic, error handling, and event-driven workflow patterns.

## Background

While Airflow provides many built-in sensors, real-world scenarios often require custom sensors tailored to specific business needs. You'll learn to create sensors that monitor custom conditions, integrate with proprietary systems, and implement advanced polling strategies.

## Tasks

### Task 1: Database Record Sensor

Create a custom sensor that monitors database records for specific conditions.

**Scenario**: Your e-commerce platform processes orders through multiple stages. You need to wait for orders to reach specific states before triggering downstream processing.

**Requirements:**

1. Create a `OrderStatusSensor` that inherits from `BaseSensorOperator`
2. Monitor a mock database (JSON files) for order status changes
3. Wait for orders to reach 'shipped' status before processing
4. Include proper error handling for database connection issues
5. Log detailed information about polling attempts

**Mock Database Structure:**

```json
{
  "orders": [
    { "id": 1, "customer_id": 101, "status": "pending", "total": 299.99 },
    { "id": 2, "customer_id": 102, "status": "processing", "total": 149.5 }
  ]
}
```

### Task 2: API Health Monitor Sensor

Build a sensor that monitors external API health and readiness.

**Scenario**: Your data pipeline depends on a third-party API that provides real-time pricing data. The API occasionally goes down for maintenance, and you need to wait for it to be healthy before processing.

**Requirements:**

1. Create an `APIHealthSensor` that checks endpoint availability
2. Verify both HTTP status code and response content
3. Support custom headers and authentication
4. Implement timeout handling for slow responses
5. Include retry logic with exponential backoff
6. Log API response details for debugging

**API Health Check:**

- Endpoint: `/api/health`
- Expected status: 200
- Expected response: `{"status": "healthy", "version": "1.0"}`

### Task 3: Multi-Source Data Readiness Sensor

Create an advanced sensor that waits for multiple data sources to be ready simultaneously.

**Scenario**: Your ML pipeline requires data from three sources: user behavior logs, product catalog updates, and inventory snapshots. All three must be available and recent before model training can begin.

**Requirements:**

1. Create a `MultiSourceReadinessSensor` that checks multiple conditions
2. Support both "all conditions" and "any condition" modes
3. Each condition should be a separate callable function
4. Include individual condition status tracking
5. Implement condition-specific timeout handling
6. Provide detailed logging for each condition check

**Data Sources to Monitor:**

- User behavior logs: Files in `/tmp/ml-data/behavior/` newer than 1 hour
- Product catalog: API endpoint returning catalog version > 100
- Inventory snapshot: Database record with today's date

### Task 4: Adaptive Polling Sensor

Build an intelligent sensor that adapts its polling frequency based on historical patterns.

**Scenario**: Your sensor monitors file arrivals from various partners. Some partners are very reliable (files arrive within minutes), while others are unpredictable (files can take hours). You want to optimize polling frequency to reduce resource usage while maintaining responsiveness.

**Requirements:**

1. Create an `AdaptiveFileSensor` that learns from historical data
2. Track success/failure patterns and adjust polling intervals
3. Implement minimum and maximum polling intervals
4. Store historical data persistently between DAG runs
5. Include performance metrics collection
6. Support manual interval override for urgent scenarios

**Adaptive Logic:**

- High success rate (>80%): Increase interval by 50%
- Low success rate (<30%): Decrease interval by 30%
- Track last 20 polling attempts for pattern analysis

## Starter Code

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.base import BaseSensorOperator
from airflow.operators.python import PythonOperator
from airflow.utils.context import Context
import json
import os
import time
import requests
from typing import Any, Dict, List, Callable, Optional

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
        # Your implementation here
        pass

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
        # Your initialization here
        pass

    def poke(self, context: Context) -> bool:
        """Check API health status"""
        # Your implementation here
        pass

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
        # Your initialization here
        pass

    def poke(self, context: Context) -> bool:
        """Check multiple readiness conditions"""
        # Your implementation here
        pass

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
        # Your initialization here
        pass

    def poke(self, context: Context) -> bool:
        """Check file existence with adaptive polling"""
        # Your implementation here
        pass

    def calculate_adaptive_interval(self) -> int:
        """Calculate next polling interval based on historical data"""
        # Your implementation here
        pass

    def record_attempt(self, success: bool, duration: float):
        """Record polling attempt for learning"""
        # Your implementation here
        pass

# Your DAG implementations here
default_args = {
    'owner': 'your-name',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
```

## Test Data Setup Functions

```python
def setup_ecommerce_database():
    """Create mock e-commerce database"""
    import os
    import json
    import threading
    import time

    os.makedirs('/tmp/mock-ecommerce-db', exist_ok=True)

    # Initial orders
    orders = [
        {"id": 1, "customer_id": 101, "status": "pending", "total": 299.99, "created_at": "2024-01-01T10:00:00Z"},
        {"id": 2, "customer_id": 102, "status": "processing", "total": 149.50, "created_at": "2024-01-01T10:15:00Z"},
        {"id": 3, "customer_id": 103, "status": "confirmed", "total": 89.99, "created_at": "2024-01-01T10:30:00Z"}
    ]

    with open('/tmp/mock-ecommerce-db/orders.json', 'w') as f:
        json.dump(orders, f, indent=2)

    def update_order_status():
        """Simulate order status updates"""
        time.sleep(30)  # Wait 30 seconds

        # Update order 1 to shipped
        with open('/tmp/mock-ecommerce-db/orders.json', 'r') as f:
            orders = json.load(f)

        for order in orders:
            if order['id'] == 1:
                order['status'] = 'shipped'
                order['shipped_at'] = datetime.now().isoformat()

        with open('/tmp/mock-ecommerce-db/orders.json', 'w') as f:
            json.dump(orders, f, indent=2)

        print("Updated order 1 status to 'shipped'")

    # Start status update thread
    threading.Thread(target=update_order_status, daemon=True).start()
    print("E-commerce database setup completed")

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
                # Simulate API becoming healthy after 45 seconds
                if time.time() % 120 < 60:  # Healthy for first 60 seconds of each 2-minute cycle
                    self.send_response(200)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    response = {
                        "status": "healthy",
                        "version": "1.0",
                        "timestamp": datetime.now().isoformat()
                    }
                    self.wfile.write(json.dumps(response).encode())
                else:
                    self.send_response(503)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    response = {"status": "maintenance", "message": "API under maintenance"}
                    self.wfile.write(json.dumps(response).encode())
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, format, *args):
            pass  # Suppress default logging

    def run_server():
        try:
            with socketserver.TCPServer(("", 8081), HealthAPIHandler) as httpd:
                httpd.serve_forever()
        except Exception as e:
            print(f"Mock API server error: {e}")

    # Start server in background
    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()
    time.sleep(2)  # Give server time to start
    print("Mock API server started on port 8081")

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
        with open(f'/tmp/ml-data/behavior/user_behavior_{timestamp}.log', 'w') as f:
            f.write('user_id,action,timestamp\n1,click,2024-01-01T10:00:00Z\n2,purchase,2024-01-01T10:05:00Z')
        print("Created behavior logs")

    def create_inventory_snapshot():
        time.sleep(40)  # 40 second delay
        with open('/tmp/ml-data/inventory/snapshot.json', 'w') as f:
            json.dump({
                "date": datetime.now().strftime('%Y-%m-%d'),
                "products": [{"id": 1, "stock": 100}, {"id": 2, "stock": 50}]
            }, f)
        print("Created inventory snapshot")

    # Start data creation threads
    threading.Thread(target=create_behavior_logs, daemon=True).start()
    threading.Thread(target=create_inventory_snapshot, daemon=True).start()

    print("ML data sources setup initiated")
```

## Expected Behavior

1. **Task 1**: OrderStatusSensor should wait for order status to change to 'shipped'
2. **Task 2**: APIHealthSensor should monitor API health and wait for healthy status
3. **Task 3**: MultiSourceReadinessSensor should coordinate multiple data source checks
4. **Task 4**: AdaptiveFileSensor should adjust polling frequency based on success patterns

## Validation Criteria

- [ ] Custom sensors inherit from BaseSensorOperator correctly
- [ ] Poke methods implement proper condition checking logic
- [ ] Error handling is implemented for external system failures
- [ ] Logging provides detailed information about sensor state
- [ ] Historical data tracking works for adaptive sensors
- [ ] Sensors integrate properly with Airflow's retry and timeout mechanisms

## Advanced Features to Implement

### Error Handling Patterns

```python
def poke(self, context: Context) -> bool:
    try:
        # Your condition checking logic
        result = self.check_condition()
        return result
    except ConnectionError as e:
        self.log.warning(f"Connection error: {e}")
        return False  # Continue polling
    except ValueError as e:
        self.log.error(f"Configuration error: {e}")
        raise  # Stop sensor execution
    except Exception as e:
        self.log.error(f"Unexpected error: {e}")
        # Decide whether to continue or fail based on error type
        return False
```

### Performance Monitoring

```python
def poke(self, context: Context) -> bool:
    start_time = time.time()

    try:
        result = self.check_condition()
        duration = time.time() - start_time

        # Record performance metrics
        self.record_performance_metric(duration, result)

        return result
    except Exception as e:
        duration = time.time() - start_time
        self.record_performance_metric(duration, False, str(e))
        raise
```

### Circuit Breaker Pattern

```python
class CircuitBreakerSensor(BaseSensorOperator):
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 300, **kwargs):
        super().__init__(**kwargs)
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.circuit_open = False

    def poke(self, context: Context) -> bool:
        # Check if circuit breaker should reset
        if self.circuit_open and self.should_attempt_reset():
            self.circuit_open = False
            self.failure_count = 0

        if self.circuit_open:
            self.log.warning("Circuit breaker is open, skipping check")
            return False

        try:
            result = self.check_condition()
            if result:
                self.failure_count = 0  # Reset on success
            return result
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.failure_count >= self.failure_threshold:
                self.circuit_open = True
                self.log.error(f"Circuit breaker opened after {self.failure_count} failures")

            raise
```

## Troubleshooting Tips

1. **Sensor not triggering**: Check that the poke method returns boolean values correctly
2. **Timeout issues**: Verify that condition checking logic doesn't take too long
3. **Memory leaks**: Ensure historical data storage has size limits
4. **Resource exhaustion**: Use appropriate poke intervals and reschedule mode
5. **Error handling**: Distinguish between retryable and non-retryable errors

## Extension Challenges

1. **Distributed Sensing**: Create sensors that coordinate across multiple Airflow instances
2. **Machine Learning Integration**: Use ML models to predict optimal polling intervals
3. **Event-Driven Hybrid**: Combine polling with webhook/event notifications
4. **Sensor Composition**: Create sensors that combine multiple other sensors
5. **Performance Dashboard**: Build a monitoring dashboard for sensor performance

## Solution Location

Check your implementation against the solution in `solutions/exercise-4-solution.py`
