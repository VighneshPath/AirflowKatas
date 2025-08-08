# Sensors & Triggers Concepts

## What are Sensors?

Sensors are a special type of operator that waits for a certain condition to be met before proceeding. They are designed to be long-running tasks that periodically check for external events or conditions. Sensors enable event-driven workflows in Airflow, allowing your DAGs to react to external changes rather than running on fixed schedules.

## Key Sensor Concepts

### Polling vs Event-Driven

- **Polling**: Sensors check conditions at regular intervals (poke_interval)
- **Event-Driven**: Some sensors can be triggered by external events (less common)

### Sensor Modes

1. **Poke Mode** (default): Sensor occupies a worker slot while waiting
2. **Reschedule Mode**: Sensor releases worker slot between checks

### Timeout and Retry Configuration

- **timeout**: Maximum time sensor will wait before failing
- **poke_interval**: Time between condition checks
- **retries**: Number of retry attempts on failure
- **exponential_backoff**: Increase wait time between retries

## Common Built-in Sensors

### FileSensor

Waits for a file to appear in the filesystem:

```python
from airflow.sensors.filesystem import FileSensor

file_sensor = FileSensor(
    task_id='wait_for_file',
    filepath='/path/to/file.txt',
    fs_conn_id='fs_default',
    poke_interval=30,
    timeout=300
)
```

### S3KeySensor

Waits for a key (file) to appear in an S3 bucket:

```python
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

s3_sensor = S3KeySensor(
    task_id='wait_for_s3_file',
    bucket_name='my-bucket',
    bucket_key='path/to/file.csv',
    aws_conn_id='aws_default',
    poke_interval=60,
    timeout=600
)
```

### HttpSensor

Waits for an HTTP endpoint to return a specific response:

```python
from airflow.providers.http.sensors.http import HttpSensor

http_sensor = HttpSensor(
    task_id='wait_for_api',
    http_conn_id='api_default',
    endpoint='health',
    poke_interval=30,
    timeout=300
)
```

## Sensor Best Practices

### 1. Choose Appropriate Timeouts

- Set realistic timeout values based on expected wait times
- Consider business requirements for maximum acceptable delays

### 2. Use Reschedule Mode for Long Waits

```python
sensor = FileSensor(
    task_id='long_wait_sensor',
    filepath='/path/to/file.txt',
    mode='reschedule',  # Releases worker slot
    poke_interval=300,  # Check every 5 minutes
    timeout=3600       # 1 hour timeout
)
```

### 3. Implement Proper Error Handling

```python
def sensor_failure_callback(context):
    print(f"Sensor {context['task_instance'].task_id} failed after timeout")
    # Send notification, log to monitoring system, etc.

sensor = FileSensor(
    task_id='monitored_sensor',
    filepath='/path/to/file.txt',
    on_failure_callback=sensor_failure_callback,
    retries=2,
    retry_delay=timedelta(minutes=5)
)
```

### 4. Monitor Sensor Performance

- Track sensor wait times and success rates
- Set up alerts for frequently failing sensors
- Consider alternative approaches for unreliable external systems

## Custom Sensors

You can create custom sensors by inheriting from `BaseSensorOperator`:

```python
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context

class CustomSensor(BaseSensorOperator):
    def __init__(self, condition_param: str, **kwargs):
        super().__init__(**kwargs)
        self.condition_param = condition_param

    def poke(self, context: Context) -> bool:
        # Implement your custom condition logic here
        # Return True when condition is met, False otherwise
        return self.check_condition()

    def check_condition(self) -> bool:
        # Your custom logic here
        pass
```

## Sensor vs Operator Decision

Use sensors when:

- Waiting for external events or conditions
- Need to poll external systems
- Workflow depends on external data availability

Use regular operators when:

- Performing active work or transformations
- Making one-time API calls
- Processing data that's already available

## Common Pitfalls

1. **Worker Slot Exhaustion**: Using poke mode with many long-running sensors
2. **Inappropriate Timeouts**: Too short (false failures) or too long (resource waste)
3. **Missing Error Handling**: Not handling sensor failures gracefully
4. **Inefficient Polling**: Too frequent checks causing unnecessary load
