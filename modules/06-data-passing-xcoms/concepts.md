# XCom Concepts: Inter-Task Communication in Airflow

## What are XComs?

XCom (short for "cross-communication") is Airflow's built-in mechanism for passing small amounts of data between tasks in a DAG. XComs enable tasks to share results, configuration, and state information, making it possible to build dynamic and data-driven workflows.

## Core Concepts

### XCom Storage

XComs are stored in Airflow's metadata database with the following key components:

- **DAG ID**: The DAG containing the tasks
- **Task ID**: The task that created the XCom
- **Execution Date**: When the DAG run occurred
- **Key**: Optional identifier for multiple XComs from the same task
- **Value**: The actual data (JSON-serialized)

### Push and Pull Operations

**Push**: Tasks can push data to XCom using:

```python
# Method 1: Return value (automatic push with key 'return_value')
def my_task():
    return {"result": "success", "count": 42}

# Method 2: Explicit push with custom key
def my_task(**context):
    context['task_instance'].xcom_push(key='custom_key', value='my_data')
```

**Pull**: Tasks can pull data from XCom using:

```python
def my_task(**context):
    # Pull return value from specific task
    data = context['task_instance'].xcom_pull(task_ids='previous_task')

    # Pull with custom key
    custom_data = context['task_instance'].xcom_pull(
        task_ids='previous_task',
        key='custom_key'
    )
```

## Data Types and Serialization

### Supported Data Types

XComs automatically serialize/deserialize these Python types:

- **Primitives**: `str`, `int`, `float`, `bool`, `None`
- **Collections**: `list`, `dict`, `tuple`
- **Nested structures**: Complex combinations of the above

```python
# Examples of valid XCom data
return "simple string"
return 42
return [1, 2, 3, "mixed", {"nested": "dict"}]
return {
    "status": "completed",
    "records_processed": 1500,
    "errors": [],
    "metadata": {
        "start_time": "2024-01-01T10:00:00",
        "duration_seconds": 45
    }
}
```

### Serialization Limitations

- Data must be JSON-serializable
- Custom objects need explicit serialization
- Large datasets should use alternative storage

```python
# This will fail - datetime objects aren't JSON-serializable
from datetime import datetime
return {"timestamp": datetime.now()}  # ❌

# This works - convert to string first
from datetime import datetime
return {"timestamp": datetime.now().isoformat()}  # ✅
```

## XCom Patterns

### 1. Simple Data Passing

```python
def extract_data():
    # Simulate data extraction
    return {"records": 100, "source": "database"}

def transform_data(**context):
    # Pull data from previous task
    raw_data = context['task_instance'].xcom_pull(task_ids='extract')

    # Transform and return
    return {
        "processed_records": raw_data["records"] * 2,
        "source": raw_data["source"],
        "transformation": "applied"
    }
```

### 2. Multiple XComs from One Task

```python
def multi_output_task(**context):
    ti = context['task_instance']

    # Push multiple values with different keys
    ti.xcom_push(key='count', value=150)
    ti.xcom_push(key='status', value='success')
    ti.xcom_push(key='errors', value=[])

    # Also return a value (stored with key 'return_value')
    return {"summary": "processing complete"}
```

### 3. XCom Templating

You can use XComs in task parameters through templating:

```python
# Pull XCom value into task parameter
bash_task = BashOperator(
    task_id='process_file',
    bash_command='echo "Processing {{ ti.xcom_pull(task_ids="get_filename") }}"'
)
```

### 4. Conditional XCom Usage

```python
def conditional_push(**context):
    success = process_data()  # Some processing logic

    if success:
        return {"status": "success", "next_action": "continue"}
    else:
        return {"status": "failed", "next_action": "retry"}

def conditional_pull(**context):
    result = context['task_instance'].xcom_pull(task_ids='conditional_task')

    if result["status"] == "success":
        print("Proceeding with next action")
    else:
        raise Exception("Previous task failed")
```

## Performance Considerations

### Size Limitations

- **Recommended**: Keep XComs under 1MB
- **Database impact**: Large XComs slow down the metadata database
- **Memory usage**: XComs are loaded into memory during task execution

### Best Practices for Large Data

Instead of passing large datasets through XComs:

```python
# ❌ Don't do this for large data
def bad_approach():
    large_dataset = load_million_records()
    return large_dataset  # This will cause problems

# ✅ Do this instead
def good_approach(**context):
    large_dataset = load_million_records()

    # Save to shared storage
    file_path = "/shared/data/processed_data.json"
    save_to_file(large_dataset, file_path)

    # Pass only the reference
    return {"data_location": file_path, "record_count": len(large_dataset)}
```

## XCom Lifecycle

### Automatic Cleanup

- XComs are automatically cleaned up based on Airflow configuration
- Default retention is typically 30 days
- Configure `max_db_retries` and cleanup settings in `airflow.cfg`

### Manual Cleanup

```python
def cleanup_xcoms(**context):
    ti = context['task_instance']

    # Clear specific XCom
    ti.xcom_pull(task_ids='old_task', key='temp_data')

    # Clear all XComs from a task
    ti.clear_xcom_data()
```

## Advanced XCom Features

### Custom XCom Backends

For specialized storage needs, you can implement custom XCom backends:

```python
from airflow.models.xcom import BaseXCom

class FileXComBackend(BaseXCom):
    @staticmethod
    def serialize_value(value):
        # Custom serialization logic
        pass

    @staticmethod
    def deserialize_value(result):
        # Custom deserialization logic
        pass
```

### XCom with TaskGroups

```python
from airflow.utils.task_group import TaskGroup

with TaskGroup("processing_group") as group:
    task1 = PythonOperator(task_id='step1', python_callable=step1_func)
    task2 = PythonOperator(task_id='step2', python_callable=step2_func)

# Access XComs from TaskGroup tasks
def downstream_task(**context):
    result = context['task_instance'].xcom_pull(
        task_ids='processing_group.step1'
    )
```

## Common Pitfalls and Solutions

### 1. Serialization Errors

**Problem**: Custom objects can't be serialized

```python
# This fails
class CustomObject:
    def __init__(self, value):
        self.value = value

return CustomObject("test")  # ❌
```

**Solution**: Convert to serializable format

```python
class CustomObject:
    def __init__(self, value):
        self.value = value

    def to_dict(self):
        return {"value": self.value}

obj = CustomObject("test")
return obj.to_dict()  # ✅
```

### 2. Missing Dependencies

**Problem**: Pulling XCom from task that hasn't run

```python
# If 'upstream_task' fails, this will return None
data = ti.xcom_pull(task_ids='upstream_task')
```

**Solution**: Handle None values and check dependencies

```python
data = ti.xcom_pull(task_ids='upstream_task')
if data is None:
    raise ValueError("Required data not available from upstream task")
```

### 3. XCom Key Conflicts

**Problem**: Multiple tasks pushing to same key

```python
# Both tasks push to default 'return_value' key
task1 = PythonOperator(task_id='task1', python_callable=func1)
task2 = PythonOperator(task_id='task2', python_callable=func2)
```

**Solution**: Use explicit keys

```python
def func1(**context):
    context['task_instance'].xcom_push(key='task1_result', value=data)

def func2(**context):
    context['task_instance'].xcom_push(key='task2_result', value=data)
```

## When NOT to Use XComs

XComs are not suitable for:

- **Large datasets** (> 1MB): Use shared storage instead
- **Binary data**: Use file storage with path references
- **Streaming data**: Use message queues or streaming platforms
- **Persistent state**: Use external databases or storage systems
- **Cross-DAG communication**: Use external storage or triggering mechanisms

## Summary

XComs provide a powerful mechanism for inter-task communication in Airflow workflows. Key takeaways:

- Use XComs for small, structured data passing between tasks
- Keep payloads small for optimal performance
- Handle serialization carefully with custom objects
- Consider alternatives for large datasets
- Implement proper error handling for missing XComs
- Use meaningful keys for complex workflows

Understanding XComs is essential for building dynamic, data-driven workflows in Airflow. They enable tasks to coordinate and share information, making your DAGs more flexible and powerful.
