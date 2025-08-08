# DAG Development Debugging Guide

This guide provides systematic approaches to debugging Apache Airflow DAGs during development.

## Table of Contents

- [Pre-Development Checklist](#pre-development-checklist)
- [DAG Syntax Debugging](#dag-syntax-debugging)
- [Task Debugging](#task-debugging)
- [Dependency Debugging](#dependency-debugging)
- [Scheduling Debugging](#scheduling-debugging)
- [Data Flow Debugging](#data-flow-debugging)
- [Performance Debugging](#performance-debugging)
- [Testing Strategies](#testing-strategies)
- [Debugging Tools and Commands](#debugging-tools-and-commands)

## Pre-Development Checklist

Before writing your DAG, ensure:

- [ ] Airflow environment is running and accessible
- [ ] Required Python packages are installed
- [ ] DAG directory is properly mounted
- [ ] You understand the business logic and data flow
- [ ] You have sample data for testing

## DAG Syntax Debugging

### Step 1: Basic Syntax Validation

Always start with basic Python syntax checking:

```bash
# Check syntax without running
python -m py_compile dags/your_dag.py

# Or run the file directly
python dags/your_dag.py
```

### Step 2: Common Syntax Issues

**Issue**: Import errors

```python
ImportError: No module named 'airflow.operators.python_operator'
```

**Debug approach**:

1. Check Airflow version compatibility
2. Use correct import paths:

```python
# Airflow 2.x
from airflow.operators.python import PythonOperator
# Not: from airflow.operators.python_operator import PythonOperator
```

**Issue**: DAG object not found

```python
NameError: name 'dag' is not defined
```

**Debug approach**:

1. Ensure DAG is defined before task creation
2. Use context manager or explicit dag parameter:

```python
# Method 1: Context manager
with DAG('my_dag', ...) as dag:
    task1 = PythonOperator(...)

# Method 2: Explicit parameter
dag = DAG('my_dag', ...)
task1 = PythonOperator(..., dag=dag)
```

### Step 3: DAG Validation Script

Create a validation script to check multiple DAGs:

```python
# scripts/validate_dags.py
import os
import sys
from airflow.models import DagBag

def validate_dags(dag_directory):
    """Validate all DAGs in directory"""
    dag_bag = DagBag(dag_folder=dag_directory, include_examples=False)

    if dag_bag.import_errors:
        print("DAG Import Errors:")
        for filename, error in dag_bag.import_errors.items():
            print(f"  {filename}: {error}")
        return False

    print(f"Successfully loaded {len(dag_bag.dags)} DAGs")
    for dag_id, dag in dag_bag.dags.items():
        print(f"  - {dag_id}: {len(dag.tasks)} tasks")

    return True

if __name__ == "__main__":
    dag_dir = sys.argv[1] if len(sys.argv) > 1 else "dags"
    validate_dags(dag_dir)
```

## Task Debugging

### Step 1: Isolate Task Logic

Test task functions independently:

```python
def my_task_function(**context):
    # Your task logic here
    print(f"Execution date: {context['ds']}")
    return "success"

# Test outside Airflow
if __name__ == "__main__":
    # Mock context for testing
    test_context = {
        'ds': '2024-01-01',
        'task_instance': None,
        # Add other context variables as needed
    }
    result = my_task_function(**test_context)
    print(f"Task result: {result}")
```

### Step 2: Add Comprehensive Logging

```python
import logging
from airflow.operators.python import PythonOperator

def debug_task(**context):
    logger = logging.getLogger(__name__)

    # Log context information
    logger.info(f"Task instance: {context['task_instance']}")
    logger.info(f"Execution date: {context['ds']}")
    logger.info(f"DAG run: {context['dag_run']}")

    try:
        # Your task logic
        result = perform_operation()
        logger.info(f"Task completed successfully: {result}")
        return result
    except Exception as e:
        logger.error(f"Task failed with error: {str(e)}")
        logger.exception("Full traceback:")
        raise

task = PythonOperator(
    task_id='debug_task',
    python_callable=debug_task,
    dag=dag
)
```

### Step 3: Use Airflow's Test Command

Test individual tasks without running the full DAG:

```bash
# Test a specific task
docker-compose exec airflow-webserver airflow tasks test \
    dag_id task_id 2024-01-01

# Test with specific configuration
docker-compose exec airflow-webserver airflow tasks test \
    dag_id task_id 2024-01-01 --cfg-path /path/to/airflow.cfg
```

## Dependency Debugging

### Step 1: Visualize Dependencies

Use Airflow's graph view and dependency checking:

```python
# Add to your DAG for debugging
def print_dag_structure(dag):
    """Print DAG structure for debugging"""
    print(f"DAG: {dag.dag_id}")
    print("Tasks:")
    for task in dag.tasks:
        print(f"  - {task.task_id}")
        print(f"    Upstream: {[t.task_id for t in task.upstream_list]}")
        print(f"    Downstream: {[t.task_id for t in task.downstream_list]}")

# Call after DAG definition
if __name__ == "__main__":
    print_dag_structure(dag)
```

### Step 2: Check for Circular Dependencies

```python
def check_circular_dependencies(dag):
    """Check for circular dependencies in DAG"""
    try:
        # This will raise an exception if circular dependencies exist
        dag.topological_sort()
        print("No circular dependencies found")
        return True
    except Exception as e:
        print(f"Circular dependency detected: {e}")
        return False
```

### Step 3: Dependency Patterns Debugging

Common dependency issues and solutions:

```python
# Issue: Tasks not running in expected order
# Debug: Check dependency definition

# Correct dependency patterns:
task1 >> task2 >> task3  # Linear
task1 >> [task2, task3] >> task4  # Fan-out then fan-in
[task1, task2] >> task3  # Multiple upstream

# Debug dependencies
print("Task1 downstream:", [t.task_id for t in task1.downstream_list])
print("Task3 upstream:", [t.task_id for t in task3.upstream_list])
```

## Scheduling Debugging

### Step 1: Understand Scheduling Logic

```python
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

# Debug scheduling parameters
dag = DAG(
    'debug_scheduling',
    start_date=days_ago(2),
    schedule_interval=timedelta(hours=1),
    catchup=False,  # Important for debugging
    max_active_runs=1,
)

# Add logging to understand execution dates
def log_execution_info(**context):
    print(f"Logical date: {context['ds']}")
    print(f"Execution date: {context['execution_date']}")
    print(f"Next execution date: {context['next_execution_date']}")
    print(f"Previous execution date: {context['prev_execution_date']}")
```

### Step 2: Test Scheduling Logic

```bash
# Check next DAG run
docker-compose exec airflow-webserver airflow dags next-execution dag_id

# List DAG runs
docker-compose exec airflow-webserver airflow dags list-runs -d dag_id

# Manually trigger DAG run
docker-compose exec airflow-webserver airflow dags trigger dag_id
```

### Step 3: Debug Catchup Behavior

```python
# Create test DAG to understand catchup
test_dag = DAG(
    'catchup_test',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=True,  # Will create runs for each day since start_date
    max_active_runs=3,
)
```

## Data Flow Debugging

### Step 1: XCom Debugging

```python
def debug_xcom_push(**context):
    """Debug XCom push operations"""
    ti = context['task_instance']

    # Push with explicit key
    ti.xcom_push(key='debug_data', value={'status': 'success', 'count': 42})

    # Push with return value (default key)
    return {'default_data': 'from_return'}

def debug_xcom_pull(**context):
    """Debug XCom pull operations"""
    ti = context['task_instance']

    # Pull with explicit key
    debug_data = ti.xcom_pull(key='debug_data', task_ids='push_task')
    print(f"Pulled debug_data: {debug_data}")

    # Pull default return value
    default_data = ti.xcom_pull(task_ids='push_task')
    print(f"Pulled default_data: {default_data}")

    # Pull from multiple tasks
    all_data = ti.xcom_pull(task_ids=['task1', 'task2'])
    print(f"All data: {all_data}")
```

### Step 2: Data Validation

```python
def validate_data(**context):
    """Validate data at each step"""
    ti = context['task_instance']

    # Get data from upstream task
    data = ti.xcom_pull(task_ids='upstream_task')

    # Validate data structure
    assert data is not None, "No data received from upstream task"
    assert isinstance(data, dict), f"Expected dict, got {type(data)}"
    assert 'required_field' in data, "Missing required field"

    print(f"Data validation passed: {data}")
    return data
```

## Performance Debugging

### Step 1: Task Duration Analysis

```python
def performance_debug_task(**context):
    """Task with performance monitoring"""
    import time
    start_time = time.time()

    try:
        # Your task logic here
        result = expensive_operation()

        duration = time.time() - start_time
        print(f"Task completed in {duration:.2f} seconds")

        # Log to XCom for analysis
        context['task_instance'].xcom_push(
            key='performance_metrics',
            value={'duration': duration, 'status': 'success'}
        )

        return result
    except Exception as e:
        duration = time.time() - start_time
        print(f"Task failed after {duration:.2f} seconds: {e}")
        raise
```

### Step 2: Memory Usage Monitoring

```python
import psutil
import os

def memory_debug_task(**context):
    """Monitor memory usage during task execution"""
    process = psutil.Process(os.getpid())

    # Initial memory usage
    initial_memory = process.memory_info().rss / 1024 / 1024  # MB
    print(f"Initial memory usage: {initial_memory:.2f} MB")

    # Your task logic
    result = memory_intensive_operation()

    # Final memory usage
    final_memory = process.memory_info().rss / 1024 / 1024  # MB
    print(f"Final memory usage: {final_memory:.2f} MB")
    print(f"Memory increase: {final_memory - initial_memory:.2f} MB")

    return result
```

## Testing Strategies

### Unit Testing DAGs

```python
# tests/test_dags.py
import pytest
from airflow.models import DagBag
from datetime import datetime

class TestDAGs:
    def setup_method(self):
        self.dagbag = DagBag(dag_folder='dags/', include_examples=False)

    def test_dag_loaded(self):
        """Test that DAG is loaded without import errors"""
        assert len(self.dagbag.import_errors) == 0
        assert 'my_dag' in self.dagbag.dags

    def test_dag_structure(self):
        """Test DAG structure and dependencies"""
        dag = self.dagbag.get_dag('my_dag')
        assert len(dag.tasks) == 3

        # Test specific task exists
        assert dag.get_task('task1') is not None

        # Test dependencies
        task1 = dag.get_task('task1')
        task2 = dag.get_task('task2')
        assert task2 in task1.downstream_list

    def test_task_count(self):
        """Test expected number of tasks"""
        dag = self.dagbag.get_dag('my_dag')
        assert len(dag.tasks) == 3
```

### Integration Testing

```python
# tests/test_dag_execution.py
from airflow.models import DagBag, TaskInstance
from airflow.utils.state import State
from datetime import datetime

def test_dag_execution():
    """Test DAG execution end-to-end"""
    dagbag = DagBag(dag_folder='dags/')
    dag = dagbag.get_dag('my_dag')

    # Create task instance
    execution_date = datetime(2024, 1, 1)
    ti = TaskInstance(
        task=dag.get_task('task1'),
        execution_date=execution_date
    )

    # Run task
    ti.run(ignore_dependencies=True)

    # Check result
    assert ti.state == State.SUCCESS
```

## Debugging Tools and Commands

### Essential Airflow CLI Commands

```bash
# List all DAGs
airflow dags list

# Show DAG details
airflow dags show dag_id

# List tasks in DAG
airflow tasks list dag_id

# Show task details
airflow tasks show dag_id task_id

# Test task execution
airflow tasks test dag_id task_id 2024-01-01

# Check DAG state
airflow dags state dag_id 2024-01-01

# Clear task instances
airflow tasks clear dag_id -t task_id

# Pause/unpause DAG
airflow dags pause dag_id
airflow dags unpause dag_id
```

### Docker-Specific Debugging

```bash
# Access Airflow container
docker-compose exec airflow-webserver bash

# Check container logs
docker-compose logs airflow-scheduler
docker-compose logs airflow-webserver
docker-compose logs airflow-worker

# Monitor container resources
docker stats

# Restart specific service
docker-compose restart airflow-scheduler
```

### Python Debugging in Tasks

```python
def debug_with_pdb(**context):
    """Use Python debugger in task"""
    import pdb

    # Set breakpoint
    pdb.set_trace()

    # Your task logic here
    result = some_operation()

    return result

# Note: PDB won't work in containerized environment
# Use logging and print statements instead
```

## Best Practices for Debugging

1. **Start Simple**: Begin with minimal DAG and add complexity gradually
2. **Use Logging**: Add comprehensive logging at each step
3. **Test Incrementally**: Test each task independently before integration
4. **Version Control**: Use git to track changes and revert if needed
5. **Documentation**: Document known issues and solutions
6. **Monitoring**: Set up alerts for DAG failures in production

## Common Debugging Scenarios

### Scenario 1: DAG Not Appearing in UI

**Debug steps**:

1. Check file syntax: `python dags/your_dag.py`
2. Verify DAG ID is unique
3. Check scheduler logs: `docker-compose logs airflow-scheduler`
4. Refresh DAGs in UI
5. Check file permissions

### Scenario 2: Task Stuck in Running State

**Debug steps**:

1. Check worker logs: `docker-compose logs airflow-worker`
2. Verify task is not in infinite loop
3. Check resource constraints (memory, CPU)
4. Look for deadlocks or blocking operations
5. Kill and restart task if necessary

### Scenario 3: XCom Data Not Passing

**Debug steps**:

1. Verify upstream task completed successfully
2. Check XCom data in Airflow UI (Admin → XComs)
3. Ensure data is JSON serializable
4. Verify task IDs in xcom_pull calls
5. Check for data size limits

Remember: Systematic debugging saves time. Always start with the simplest explanation and work your way up to more complex issues.
