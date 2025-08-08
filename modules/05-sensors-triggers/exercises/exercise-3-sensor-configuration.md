# Exercise 3: Advanced Sensor Configuration

## Objective

Master advanced sensor configuration including timeout strategies, retry mechanisms, and performance optimization.

## Background

You're designing a production data pipeline that needs to handle various external dependencies with different reliability characteristics. Some data sources are highly reliable (arrive within minutes), while others are unpredictable (could take hours). You need to configure sensors appropriately for each scenario.

## Tasks

### Task 1: Timeout Strategy Implementation

Create sensors with different timeout strategies based on data source reliability:

1. **High Reliability Source**: Financial data (arrives within 5 minutes, 99.9% reliability)
2. **Medium Reliability Source**: Customer data (arrives within 30 minutes, 95% reliability)
3. **Low Reliability Source**: External partner data (arrives within 4 hours, 80% reliability)

**Requirements:**

- Configure appropriate timeouts for each reliability level
- Implement different retry strategies
- Use reschedule mode for long-running sensors
- Add monitoring and alerting for each category

### Task 2: Exponential Backoff and Retry Logic

Implement sophisticated retry mechanisms:

1. **Linear backoff** for high-reliability sources
2. **Exponential backoff** for medium-reliability sources
3. **Custom backoff** with jitter for low-reliability sources

**Requirements:**

- Implement custom retry logic with different backoff strategies
- Add maximum retry limits based on source reliability
- Include circuit breaker pattern for repeatedly failing sensors
- Log detailed retry information for monitoring

### Task 3: Sensor Performance Optimization

Create a performance-optimized sensor configuration:

1. **Resource management**: Optimize worker slot usage
2. **Batch processing**: Group related sensors efficiently
3. **Performance monitoring**: Track sensor metrics
4. **Auto-scaling**: Adjust sensor frequency based on historical patterns

**Requirements:**

- Use reschedule mode to prevent worker slot blocking
- Implement sensor pooling for related checks
- Create performance metrics collection
- Add adaptive poke intervals based on historical data

## Starter Code

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.context import Context
from airflow.sensors.base import BaseSensorOperator
import time
import random
import json
import os

class AdaptiveSensor(BaseSensorOperator):
    """
    Sensor with adaptive poke intervals based on historical performance
    """

    def __init__(self,
                 base_poke_interval: int = 60,
                 max_poke_interval: int = 300,
                 performance_file: str = None,
                 **kwargs):
        super().__init__(**kwargs)
        self.base_poke_interval = base_poke_interval
        self.max_poke_interval = max_poke_interval
        self.performance_file = performance_file or f'/tmp/sensor_performance_{self.task_id}.json'

    def get_adaptive_interval(self) -> int:
        """Calculate adaptive poke interval based on historical data"""
        try:
            if os.path.exists(self.performance_file):
                with open(self.performance_file, 'r') as f:
                    data = json.load(f)

                # Calculate average wait time from last 10 runs
                recent_waits = data.get('wait_times', [])[-10:]
                if recent_waits:
                    avg_wait = sum(recent_waits) / len(recent_waits)
                    # Adjust interval based on average wait time
                    adaptive_interval = min(int(avg_wait / 4), self.max_poke_interval)
                    return max(adaptive_interval, self.base_poke_interval)

            return self.base_poke_interval
        except Exception as e:
            self.log.warning(f"Error calculating adaptive interval: {e}")
            return self.base_poke_interval

    def record_performance(self, wait_time: int, success: bool):
        """Record sensor performance metrics"""
        try:
            data = {'wait_times': [], 'success_rate': []}

            if os.path.exists(self.performance_file):
                with open(self.performance_file, 'r') as f:
                    data = json.load(f)

            data['wait_times'].append(wait_time)
            data['success_rate'].append(1 if success else 0)

            # Keep only last 50 records
            data['wait_times'] = data['wait_times'][-50:]
            data['success_rate'] = data['success_rate'][-50:]

            with open(self.performance_file, 'w') as f:
                json.dump(data, f)

        except Exception as e:
            self.log.warning(f"Error recording performance: {e}")

class ExponentialBackoffSensor(BaseSensorOperator):
    """
    Sensor with exponential backoff retry logic
    """

    def __init__(self,
                 initial_interval: int = 30,
                 max_interval: int = 300,
                 backoff_multiplier: float = 2.0,
                 jitter: bool = True,
                 **kwargs):
        super().__init__(**kwargs)
        self.initial_interval = initial_interval
        self.max_interval = max_interval
        self.backoff_multiplier = backoff_multiplier
        self.jitter = jitter
        self.current_interval = initial_interval

    def get_next_interval(self) -> int:
        """Calculate next poke interval with exponential backoff"""
        interval = min(self.current_interval, self.max_interval)

        # Add jitter to prevent thundering herd
        if self.jitter:
            jitter_range = interval * 0.1  # 10% jitter
            interval += random.uniform(-jitter_range, jitter_range)

        # Update for next iteration
        self.current_interval = min(
            self.current_interval * self.backoff_multiplier,
            self.max_interval
        )

        return int(max(interval, 1))

    def reset_interval(self):
        """Reset interval to initial value on success"""
        self.current_interval = self.initial_interval

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

## Test Data Setup

```python
def create_reliability_test_environment():
    """Create test environment with different reliability patterns"""
    import os
    import threading
    import time
    from datetime import datetime

    # Create directories
    directories = [
        '/tmp/reliable-data',      # High reliability
        '/tmp/medium-data',        # Medium reliability
        '/tmp/unreliable-data'     # Low reliability
    ]

    for directory in directories:
        os.makedirs(directory, exist_ok=True)

    def create_reliable_files():
        """Create files with high reliability (quick arrival)"""
        time.sleep(random.uniform(10, 60))  # 10-60 seconds
        today = datetime.now().strftime('%Y%m%d')
        with open(f'/tmp/reliable-data/financial_{today}.csv', 'w') as f:
            f.write('timestamp,amount,currency\n2024-01-01,1000.00,USD')
        print("Reliable file created")

    def create_medium_files():
        """Create files with medium reliability"""
        time.sleep(random.uniform(300, 1800))  # 5-30 minutes
        today = datetime.now().strftime('%Y%m%d')
        with open(f'/tmp/medium-data/customer_{today}.json', 'w') as f:
            f.write('{"customers": [{"id": 1, "name": "Test Customer"}]}')
        print("Medium reliability file created")

    def create_unreliable_files():
        """Create files with low reliability (may fail)"""
        # 80% success rate
        if random.random() < 0.8:
            time.sleep(random.uniform(1800, 14400))  # 30 minutes to 4 hours
            today = datetime.now().strftime('%Y%m%d')
            with open(f'/tmp/unreliable-data/partner_{today}.xml', 'w') as f:
                f.write('<data><record>Partner data</record></data>')
            print("Unreliable file created")
        else:
            print("Unreliable file creation failed (simulated)")

    # Start file creation threads
    threading.Thread(target=create_reliable_files, daemon=True).start()
    threading.Thread(target=create_medium_files, daemon=True).start()
    threading.Thread(target=create_unreliable_files, daemon=True).start()

def setup_performance_monitoring():
    """Set up performance monitoring infrastructure"""
    import os

    # Create monitoring directory
    os.makedirs('/tmp/sensor-monitoring', exist_ok=True)

    # Initialize performance tracking files
    performance_files = [
        '/tmp/sensor-monitoring/reliable_sensor_metrics.json',
        '/tmp/sensor-monitoring/medium_sensor_metrics.json',
        '/tmp/sensor-monitoring/unreliable_sensor_metrics.json'
    ]

    for file_path in performance_files:
        if not os.path.exists(file_path):
            with open(file_path, 'w') as f:
                json.dump({'wait_times': [], 'success_rate': []}, f)

    print("Performance monitoring setup completed")
```

## Configuration Examples

### High Reliability Sensor Configuration

```python
high_reliability_sensor = FileSensor(
    task_id='wait_for_financial_data',
    filepath='/tmp/reliable-data/financial_{{ ds_nodash }}.csv',
    fs_conn_id='fs_default',
    poke_interval=30,      # Check every 30 seconds
    timeout=300,           # 5 minute timeout
    retries=2,             # Limited retries
    retry_delay=timedelta(minutes=1),
    mode='poke'            # Keep worker slot (short wait expected)
)
```

### Medium Reliability Sensor Configuration

```python
medium_reliability_sensor = ExponentialBackoffSensor(
    task_id='wait_for_customer_data',
    filepath='/tmp/medium-data/customer_{{ ds_nodash }}.json',
    initial_interval=60,   # Start with 1 minute
    max_interval=600,      # Max 10 minutes between checks
    backoff_multiplier=1.5,
    timeout=2400,          # 40 minute timeout
    retries=3,
    retry_delay=timedelta(minutes=5),
    mode='reschedule'      # Release worker slot
)
```

### Low Reliability Sensor Configuration

```python
low_reliability_sensor = AdaptiveSensor(
    task_id='wait_for_partner_data',
    filepath='/tmp/unreliable-data/partner_{{ ds_nodash }}.xml',
    base_poke_interval=300,  # Start with 5 minutes
    max_poke_interval=1800,  # Max 30 minutes between checks
    timeout=14400,           # 4 hour timeout
    retries=5,               # More retries for unreliable source
    retry_delay=timedelta(minutes=30),
    mode='reschedule'
)
```

## Expected Behavior

1. **Task 1**: Different sensors should use appropriate timeout strategies based on reliability
2. **Task 2**: Retry mechanisms should implement proper backoff strategies
3. **Task 3**: Sensors should optimize resource usage and collect performance metrics

## Validation Criteria

- [ ] Sensors use appropriate timeout values for their reliability category
- [ ] Retry logic implements correct backoff strategies
- [ ] Reschedule mode is used for long-running sensors
- [ ] Performance metrics are collected and stored
- [ ] Adaptive intervals adjust based on historical data
- [ ] Circuit breaker pattern prevents resource waste

## Monitoring and Alerting

Implement monitoring for:

1. **Sensor success rates** by category
2. **Average wait times** and trends
3. **Timeout frequency** and patterns
4. **Resource utilization** (worker slots)
5. **Performance degradation** alerts

## Troubleshooting Tips

1. **High timeout rates**: Adjust timeout values or investigate data source issues
2. **Worker slot exhaustion**: Use reschedule mode for long-running sensors
3. **Performance degradation**: Check adaptive interval calculations
4. **Memory issues**: Limit performance history retention

## Extension Challenges

1. **Machine learning optimization**: Use ML to predict optimal poke intervals
2. **Dynamic timeout adjustment**: Adjust timeouts based on time of day/week patterns
3. **Sensor health scoring**: Create composite health scores for sensor reliability
4. **Auto-remediation**: Implement automatic sensor configuration adjustments

## Solution Location

Check your implementation against the solution in `solutions/exercise-3-solution.py`
