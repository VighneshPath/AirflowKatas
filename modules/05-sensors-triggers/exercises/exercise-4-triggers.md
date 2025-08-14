# Exercise 4: Implementing Triggers and Deferrable Operators

## Objective

Learn to implement and use triggers for efficient, event-driven workflows in Airflow. You'll create both simple deferrable sensors and custom triggers to understand the modern approach to waiting for external events.

**Learning Goals:**

- [ ] Understand the difference between sensors and triggers
- [ ] Convert traditional sensors to deferrable mode
- [ ] Implement custom triggers for specific use cases
- [ ] Handle trigger events and errors properly
- [ ] Compare resource usage between approaches

## Prerequisites

**Required Knowledge:**

- [ ] Completed Exercise 1-3 (File Sensors, S3 Sensors, Configuration)
- [ ] Understanding of async/await concepts in Python
- [ ] Basic knowledge of Airflow architecture

**Required Setup:**

- [ ] Airflow 2.2+ with triggerer process running
- [ ] Understanding of worker slots and resource management

## Scenario

You're working for a data processing company that handles hundreds of concurrent file uploads from various clients. The current sensor-based approach is consuming too many worker slots and causing bottlenecks. You need to implement a trigger-based solution that can efficiently handle many concurrent file monitoring tasks without exhausting resources.

**Business Requirements:**

- Monitor multiple client upload directories simultaneously
- Handle up to 100 concurrent file waits efficiently
- Provide detailed logging and monitoring
- Maintain backward compatibility with existing workflows

## Requirements

### Functional Requirements

1. **Deferrable File Monitoring**: Convert existing file sensors to use triggers

   - Acceptance criteria: Sensors use deferrable=True and free worker slots
   - Expected outcome: Reduced worker slot consumption during waits

2. **Custom Multi-Directory Trigger**: Create a trigger that monitors multiple directories

   - Acceptance criteria: Single trigger monitors 5+ directories simultaneously
   - Expected outcome: Efficient multi-directory monitoring

3. **Event Processing**: Properly handle trigger events and pass data downstream

   - Acceptance criteria: Downstream tasks receive trigger results via XCom
   - Expected outcome: Seamless data flow from triggers to processing tasks

4. **Error Handling**: Implement robust error handling for trigger failures
   - Acceptance criteria: Graceful handling of timeouts and trigger errors
   - Expected outcome: Clear error messages and appropriate retry behavior

### Technical Requirements

- **Airflow Version**: Use Airflow 2.2+ features
- **Triggerer Process**: Ensure triggerer is running and configured
- **Async Implementation**: Use proper async/await patterns in triggers
- **Serialization**: Implement proper trigger serialization
- **Resource Monitoring**: Include logging to monitor resource usage

## Instructions

### Step 1: Setup and Environment Preparation

Create a new DAG file named `trigger_implementation_exercise.py` and set up the basic structure:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'trigger-exercise',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'trigger_implementation_exercise',
    default_args=default_args,
    description='Exercise 4: Implementing Triggers',
    schedule_interval=None,
    catchup=False,
    tags=['kata', 'exercise-4', 'triggers'],
)
```

**Setup Tasks:**

1. Create test directories and files
2. Set up monitoring infrastructure
3. Prepare cleanup procedures

### Step 2: Convert Traditional Sensors to Deferrable

Convert these traditional sensors to use triggers:

```python
# Traditional approach (DON'T USE - just for reference)
traditional_sensor = FileSensor(
    task_id='traditional_file_wait',
    filepath='/tmp/client_data/upload.csv',
    poke_interval=30,
    timeout=600,
    # This blocks a worker slot!
)
```

**Your Task:**

1. Create a deferrable version of the above sensor
2. Add proper timeout and interval configuration
3. Include logging to show the difference

**Hints:**

- Use `deferrable=True` parameter
- Consider appropriate poke_interval for triggers
- Add task documentation explaining the benefits

### Step 3: Implement Custom Multi-Directory Trigger

Create a custom trigger that monitors multiple directories simultaneously:

```python
from airflow.triggers.base import BaseTrigger, TriggerEvent
import asyncio
import os
from typing import Tuple, Dict, Any, AsyncIterator

class MultiDirectoryTrigger(BaseTrigger):
    """
    Custom trigger that monitors multiple directories for file creation.

    This trigger should:
    - Monitor multiple directories simultaneously
    - Return when ANY file appears in ANY directory
    - Include information about which file was found
    - Handle errors gracefully
    """

    def __init__(self, directories: list, file_pattern: str = "*.csv",
                 check_interval: int = 10):
        # TODO: Implement initialization
        pass

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        # TODO: Implement serialization
        pass

    async def run(self) -> AsyncIterator[TriggerEvent]:
        # TODO: Implement async monitoring logic
        pass
```

**Implementation Requirements:**

1. Monitor at least 5 directories simultaneously
2. Support file pattern matching (e.g., "_.csv", "_.json")
3. Return detailed information about found files
4. Handle directory access errors gracefully
5. Implement proper async/await patterns

### Step 4: Create Deferrable Sensor Using Custom Trigger

Implement a sensor that uses your custom trigger:

```python
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context

class MultiDirectorySensorAsync(BaseSensorOperator):
    """
    Deferrable sensor using the custom multi-directory trigger.
    """

    def __init__(self, directories: list, file_pattern: str = "*.csv", **kwargs):
        # TODO: Implement initialization
        pass

    def execute(self, context: Context):
        # TODO: Implement execution logic (defer to trigger)
        pass

    def execute_complete(self, context: Context, event: Dict[str, Any]):
        # TODO: Handle trigger completion
        pass
```

**Requirements:**

1. Properly defer to your custom trigger
2. Handle trigger events correctly
3. Return useful data for downstream tasks
4. Include fallback to synchronous mode if needed

### Step 5: Implement Event Processing and Data Flow

Create tasks that process trigger results:

```python
def process_trigger_results(**context):
    """
    Process results from trigger-based sensors.

    Should:
    - Extract trigger result data from XCom
    - Log detailed information about found files
    - Perform basic file validation
    - Return processed data for downstream tasks
    """
    # TODO: Implement processing logic
    pass

def monitor_resource_usage(**context):
    """
    Monitor and log resource usage differences.

    Should:
    - Compare worker slot usage
    - Log trigger efficiency metrics
    - Provide recommendations
    """
    # TODO: Implement monitoring logic
    pass
```

### Step 6: Add Comprehensive Error Handling

Implement error handling for various trigger scenarios:

1. **Timeout Handling**: What happens when triggers time out?
2. **Directory Access Errors**: Handle permission or missing directory issues
3. **Trigger Process Failures**: Handle triggerer process issues
4. **Serialization Errors**: Handle trigger serialization problems

```python
def handle_trigger_failure(context):
    """Callback for trigger failures."""
    # TODO: Implement failure handling
    pass

# Apply to your sensors
multi_dir_sensor = MultiDirectorySensorAsync(
    task_id='monitor_client_uploads',
    directories=['/tmp/client1', '/tmp/client2', '/tmp/client3'],
    on_failure_callback=handle_trigger_failure,
    # TODO: Add other configuration
)
```

### Step 7: Create Comparison and Monitoring Tasks

Implement tasks that demonstrate the benefits of triggers:

```python
def compare_approaches(**context):
    """
    Compare traditional sensors vs triggers.

    Should demonstrate:
    - Resource usage differences
    - Scalability improvements
    - Performance metrics
    """
    # TODO: Implement comparison logic
    pass

def generate_trigger_report(**context):
    """
    Generate a report on trigger usage and efficiency.
    """
    # TODO: Implement reporting logic
    pass
```

## Validation

### Self-Check Questions

Before submitting your solution, verify:

- [ ] **Deferrable Sensors**: Do your sensors use `deferrable=True`?
- [ ] **Custom Trigger**: Does your trigger implement all required methods?
- [ ] **Async Patterns**: Are you using proper async/await syntax?
- [ ] **Event Handling**: Do you properly handle trigger events?
- [ ] **Error Handling**: Are errors handled gracefully?
- [ ] **Serialization**: Can your triggers be properly serialized?
- [ ] **Resource Efficiency**: Do triggers free up worker slots?

### Testing Your Solution

1. **Trigger Functionality**:

   ```bash
   # Ensure triggerer is running
   airflow triggerer

   # Test your DAG
   airflow dags test trigger_implementation_exercise
   ```

2. **Resource Monitoring**: Check that deferrable sensors don't consume worker slots during waits

3. **Multi-Directory Monitoring**: Verify your custom trigger monitors multiple directories

4. **Event Processing**: Confirm downstream tasks receive trigger data

### Expected Output

**Success Indicators:**

- Deferrable sensors show "deferred" status in Airflow UI
- Custom trigger successfully monitors multiple directories
- Trigger events are properly processed by downstream tasks
- Resource usage is significantly lower than traditional sensors
- Error handling works for various failure scenarios

## Extension Challenges

### Challenge 1: Advanced Trigger Features

Enhance your trigger with advanced features:

```python
class AdvancedMultiDirectoryTrigger(BaseTrigger):
    """
    Enhanced trigger with additional features:
    - File size monitoring (wait for files to stop growing)
    - Multiple file pattern support
    - Priority-based directory monitoring
    - Configurable retry logic with exponential backoff
    """
    # TODO: Implement advanced features
```

### Challenge 2: Trigger Metrics and Monitoring

Implement comprehensive monitoring:

```python
def collect_trigger_metrics(**context):
    """
    Collect and report trigger performance metrics:
    - Average wait times
    - Success/failure rates
    - Resource utilization
    - Trigger queue depths
    """
    # TODO: Implement metrics collection
```

### Challenge 3: Trigger-Based Workflow Orchestration

Create a complex workflow using multiple triggers:

```python
# TODO: Design a workflow that uses multiple different triggers
# - File triggers for data arrival
# - Time triggers for scheduling
# - API triggers for external events
# - Custom business logic triggers
```

## Common Pitfalls

### Pitfall 1: Incorrect Async Patterns

**Problem**: Using blocking operations in trigger `run()` method
**Symptoms**: Triggerer process becomes unresponsive
**Solution**: Use `await` for all I/O operations, use `asyncio.sleep()` instead of `time.sleep()`

### Pitfall 2: Improper Serialization

**Problem**: Trigger cannot be serialized/deserialized
**Symptoms**: Trigger fails to start or resume after restart
**Solution**: Ensure all trigger parameters are serializable, implement proper `serialize()` method

### Pitfall 3: Missing Error Handling

**Problem**: Triggers fail without proper error reporting
**Symptoms**: Silent failures, unclear error messages
**Solution**: Implement comprehensive error handling in `run()` method, emit error events

### Pitfall 4: Resource Leaks

**Problem**: Triggers don't clean up resources properly
**Symptoms**: Memory leaks, file handle exhaustion
**Solution**: Use proper async context managers, clean up resources in finally blocks

## Troubleshooting

### Issue 1: Triggerer Not Running

**Symptoms:**

- Deferrable sensors never complete
- "No triggerer available" errors

**Solutions:**

1. Start triggerer process: `airflow triggerer`
2. Check triggerer configuration in airflow.cfg
3. Verify triggerer is healthy in Airflow UI

### Issue 2: Trigger Serialization Errors

**Symptoms:**

- Trigger fails to start
- Serialization-related error messages

**Solutions:**

1. Check that all trigger parameters are JSON-serializable
2. Verify `serialize()` method returns correct format
3. Test trigger serialization manually

### Issue 3: Async/Await Issues

**Symptoms:**

- Triggerer becomes unresponsive
- "coroutine was never awaited" warnings

**Solutions:**

1. Use `await` for all async operations
2. Use `asyncio.sleep()` instead of `time.sleep()`
3. Properly handle async exceptions

## Resources

### Relevant Documentation

- [Airflow Triggers Documentation](https://airflow.apache.org/docs/apache-airflow/stable/concepts/deferring.html)
- [Deferrable Operators Guide](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html)
- [Custom Triggers Tutorial](https://airflow.apache.org/docs/apache-airflow/stable/howto/create-custom-trigger.html)

### Code Examples

- Check `trigger_examples.py` for reference implementations
- Review built-in deferrable sensors in Airflow source code
- Study provider package trigger implementations

## Reflection Questions

After completing the exercise, consider:

1. **Efficiency**: How much more efficient are triggers compared to traditional sensors?
2. **Complexity**: What's the trade-off between efficiency and implementation complexity?
3. **Use Cases**: When would you choose triggers vs traditional sensors?
4. **Monitoring**: How would you monitor trigger health in production?
5. **Scaling**: How do triggers help with scaling Airflow deployments?

## Next Steps

### Immediate Next Steps

- [ ] Compare your solution with the provided solution
- [ ] Try the extension challenges
- [ ] Experiment with different trigger patterns

### Preparation for Next Module

- [ ] Understand how trigger results flow to downstream tasks
- [ ] Practice with XCom data passing from triggers
- [ ] Review async programming concepts

### Further Learning

- [ ] Explore provider package triggers (AWS, GCP, etc.)
- [ ] Study trigger implementation in Airflow source code
- [ ] Practice building triggers for your specific use cases

---

**Ready to build efficient, event-driven workflows?** Start with Step 1 and work through each section systematically. Remember, triggers are a powerful feature that can significantly improve your Airflow deployment's efficiency!

**Need help?** Check the troubleshooting section, review the trigger examples, or ask for guidance in the community discussions.
