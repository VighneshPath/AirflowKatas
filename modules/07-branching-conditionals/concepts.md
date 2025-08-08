# Branching & Conditionals in Airflow

## Overview

Branching in Apache Airflow allows you to create dynamic workflows where the execution path depends on runtime conditions, data values, or external factors. This enables you to build intelligent pipelines that can adapt their behavior based on the results of previous tasks or external conditions.

## Core Concepts

### What is Branching?

Branching is the ability to conditionally execute different paths in your DAG based on runtime decisions. Instead of executing all tasks in a linear fashion, branching allows your workflow to "choose" which tasks to run based on specific conditions.

### When to Use Branching

- **Data-driven decisions**: Execute different processing based on data characteristics
- **Environment-specific logic**: Different behavior for dev/staging/production
- **Error handling**: Alternative paths when certain conditions fail
- **Business logic**: Different workflows based on business rules
- **Resource optimization**: Skip expensive operations when not needed

## BranchPythonOperator

The `BranchPythonOperator` is the primary tool for implementing branching logic in Airflow.

### Basic Syntax

```python
from airflow.operators.python import BranchPythonOperator

def choose_branch(**context):
    # Your branching logic here
    if some_condition:
        return 'task_a'  # Return task_id to execute
    else:
        return 'task_b'  # Alternative task_id

branch_task = BranchPythonOperator(
    task_id='branching_task',
    python_callable=choose_branch
)
```

### Key Characteristics

1. **Return Value**: Must return a task_id (string) or list of task_ids
2. **Downstream Tasks**: Only returned tasks will execute
3. **Skipped Tasks**: Non-selected tasks are marked as "skipped"
4. **Context Access**: Full access to task context and XComs

## Branching Patterns

### Simple Binary Branch

```python
def simple_branch(**context):
    # Simple condition check
    current_hour = datetime.now().hour
    if current_hour < 12:
        return 'morning_task'
    else:
        return 'afternoon_task'
```

### Data-Driven Branching

```python
def data_driven_branch(**context):
    # Get data from previous task
    data = context['task_instance'].xcom_pull(task_ids='data_check')
    record_count = data.get('count', 0)

    if record_count > 1000:
        return 'large_dataset_processing'
    elif record_count > 100:
        return 'medium_dataset_processing'
    else:
        return 'small_dataset_processing'
```

### Multiple Path Branching

```python
def multi_path_branch(**context):
    # Return multiple task_ids for parallel execution
    conditions = context['task_instance'].xcom_pull(task_ids='condition_check')

    tasks_to_run = []
    if conditions.get('process_data'):
        tasks_to_run.append('data_processing')
    if conditions.get('send_notifications'):
        tasks_to_run.append('notification_task')
    if conditions.get('update_database'):
        tasks_to_run.append('database_update')

    return tasks_to_run if tasks_to_run else 'no_action_task'
```

## Advanced Branching Concepts

### Combining Branching with XComs

Branching becomes powerful when combined with XCom data from previous tasks:

```python
def xcom_based_branch(**context):
    # Pull data from multiple previous tasks
    validation_result = context['task_instance'].xcom_pull(task_ids='validate_data')
    config = context['task_instance'].xcom_pull(task_ids='load_config')

    if not validation_result.get('is_valid'):
        return 'error_handling_task'

    processing_mode = config.get('mode', 'standard')
    if processing_mode == 'fast':
        return 'fast_processing'
    elif processing_mode == 'thorough':
        return 'thorough_processing'
    else:
        return 'standard_processing'
```

### Nested Branching

You can create complex decision trees with multiple branching points:

```python
# First level branching
def primary_branch(**context):
    data_type = context['task_instance'].xcom_pull(task_ids='identify_data_type')
    if data_type == 'csv':
        return 'csv_branch'
    elif data_type == 'json':
        return 'json_branch'
    else:
        return 'unknown_format_branch'

# Second level branching for CSV processing
def csv_branch(**context):
    csv_size = context['task_instance'].xcom_pull(task_ids='check_csv_size')
    if csv_size > 1000000:  # 1MB
        return 'large_csv_processing'
    else:
        return 'small_csv_processing'
```

## Task Dependencies with Branching

### Joining After Branches

Use dummy operators or specific tasks to join execution paths:

```python
from airflow.operators.dummy import DummyOperator

# Branching tasks
branch_task >> [task_a, task_b, task_c]

# Join point - will execute regardless of which branch ran
join_task = DummyOperator(task_id='join_task')
[task_a, task_b, task_c] >> join_task
```

### Trigger Rules

Control how tasks behave after branching:

```python
from airflow.utils.trigger_rule import TriggerRule

join_task = PythonOperator(
    task_id='join_task',
    python_callable=join_function,
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
)
```

Common trigger rules for branching:

- `ALL_SUCCESS`: Default, requires all upstream tasks to succeed
- `ALL_FAILED`: Execute only if all upstream tasks failed
- `ALL_DONE`: Execute when all upstream tasks are done (success or failed)
- `ONE_SUCCESS`: Execute when at least one upstream task succeeds
- `ONE_FAILED`: Execute when at least one upstream task fails
- `NONE_FAILED`: Execute when no upstream tasks have failed
- `NONE_FAILED_MIN_ONE_SUCCESS`: At least one success and no failures

## Best Practices

### 1. Clear Branching Logic

```python
def clear_branch_logic(**context):
    """
    Determine processing path based on data characteristics.
    Returns appropriate task_id for the data size and type.
    """
    data_info = context['task_instance'].xcom_pull(task_ids='analyze_data')

    # Clear, documented decision logic
    size_mb = data_info.get('size_mb', 0)
    data_type = data_info.get('type', 'unknown')

    if data_type == 'unknown':
        return 'handle_unknown_data'

    if size_mb > 100:
        return f'large_{data_type}_processing'
    else:
        return f'small_{data_type}_processing'
```

### 2. Error Handling in Branches

```python
def safe_branch(**context):
    try:
        condition = context['task_instance'].xcom_pull(task_ids='condition_task')
        if condition is None:
            return 'handle_missing_data'

        return 'process_data' if condition.get('ready') else 'wait_task'

    except Exception as e:
        # Log error and return safe fallback
        print(f"Branching error: {e}")
        return 'error_recovery_task'
```

### 3. Testable Branching Functions

```python
def testable_branch(data_size=None, **context):
    """
    Branching function that can be easily tested.
    Accepts data_size parameter for testing, falls back to XCom in production.
    """
    if data_size is None:
        # Production: get from XCom
        data_size = context['task_instance'].xcom_pull(task_ids='check_size')

    # Testable logic
    if data_size > 1000:
        return 'large_processing'
    else:
        return 'small_processing'

# Easy to test
assert testable_branch(data_size=500) == 'small_processing'
assert testable_branch(data_size=1500) == 'large_processing'
```

## Common Patterns

### 1. Environment-Based Branching

```python
def environment_branch(**context):
    env = Variable.get("environment", default_var="dev")

    if env == "production":
        return 'production_processing'
    elif env == "staging":
        return 'staging_processing'
    else:
        return 'development_processing'
```

### 2. Time-Based Branching

```python
def time_based_branch(**context):
    current_time = datetime.now()

    if current_time.weekday() >= 5:  # Weekend
        return 'weekend_processing'
    elif current_time.hour < 9 or current_time.hour > 17:  # Off hours
        return 'off_hours_processing'
    else:
        return 'business_hours_processing'
```

### 3. Data Quality Branching

```python
def quality_branch(**context):
    quality_report = context['task_instance'].xcom_pull(task_ids='data_quality_check')

    error_rate = quality_report.get('error_rate', 1.0)
    completeness = quality_report.get('completeness', 0.0)

    if error_rate > 0.1:  # More than 10% errors
        return 'data_cleaning_task'
    elif completeness < 0.9:  # Less than 90% complete
        return 'data_enrichment_task'
    else:
        return 'standard_processing_task'
```

## Debugging Branching

### 1. Logging Branch Decisions

```python
def logged_branch(**context):
    condition = context['task_instance'].xcom_pull(task_ids='condition_check')

    if condition.get('process_immediately'):
        decision = 'immediate_processing'
        print(f"Branch decision: {decision} - Processing immediately due to high priority")
    else:
        decision = 'scheduled_processing'
        print(f"Branch decision: {decision} - Using scheduled processing")

    return decision
```

### 2. Branch Decision Tracking

```python
def tracked_branch(**context):
    decision_data = {
        'timestamp': datetime.now().isoformat(),
        'dag_run_id': context['dag_run'].run_id,
        'conditions': context['task_instance'].xcom_pull(task_ids='conditions')
    }

    if decision_data['conditions'].get('urgent'):
        decision = 'urgent_processing'
    else:
        decision = 'normal_processing'

    # Store decision for analysis
    decision_data['decision'] = decision
    context['task_instance'].xcom_push(key='branch_decision', value=decision_data)

    return decision
```

## Performance Considerations

### 1. Minimize Branching Logic Complexity

Keep branching functions lightweight and fast-executing to avoid DAG parsing delays.

### 2. Cache Expensive Operations

```python
def cached_branch(**context):
    # Check if decision was already made
    cached_decision = context['task_instance'].xcom_pull(
        key='cached_branch_decision',
        task_ids='branching_task'
    )

    if cached_decision:
        return cached_decision

    # Expensive operation
    decision = expensive_decision_logic()

    # Cache for potential retries
    context['task_instance'].xcom_push(
        key='cached_branch_decision',
        value=decision
    )

    return decision
```

### 3. Avoid Deep Nesting

Instead of deeply nested conditions, use early returns or lookup tables:

```python
def efficient_branch(**context):
    data = context['task_instance'].xcom_pull(task_ids='data_analysis')

    # Lookup table approach
    processing_map = {
        ('small', 'csv'): 'small_csv_task',
        ('small', 'json'): 'small_json_task',
        ('large', 'csv'): 'large_csv_task',
        ('large', 'json'): 'large_json_task'
    }

    key = (data.get('size'), data.get('format'))
    return processing_map.get(key, 'unknown_format_task')
```

## Summary

Branching in Airflow enables:

- **Dynamic workflows** that adapt to runtime conditions
- **Efficient resource usage** by skipping unnecessary tasks
- **Complex business logic** implementation in data pipelines
- **Error handling** and recovery patterns
- **Environment-specific** processing logic

Key tools:

- `BranchPythonOperator` for conditional logic
- XComs for data-driven decisions
- Trigger rules for post-branch task coordination
- Proper error handling and logging

Next, we'll explore practical examples and exercises to master these concepts!
