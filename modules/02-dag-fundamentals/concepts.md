# DAG Fundamentals: Configuration and Dependencies

## Understanding DAG Configuration

A DAG (Directed Acyclic Graph) is more than just a collection of tasks - it's a carefully configured workflow with specific behavior patterns. Every parameter you set affects how and when your workflow executes.

### The Anatomy of a DAG

```python
from datetime import datetime, timedelta
from airflow import DAG

dag = DAG(
    dag_id='example_dag',                    # Unique identifier
    description='Example DAG for learning',  # Human-readable description
    schedule_interval='@daily',              # When to run
    start_date=datetime(2024, 1, 1),        # When to start
    catchup=False,                          # Handle historical runs
    max_active_runs=1,                      # Concurrency control
    default_args={                          # Task-level defaults
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    },
    tags=['example', 'learning']            # Organization
)
```

Let's break down each component and understand why it matters.

## DAG Scheduling Deep Dive

### Schedule Interval: When Your DAG Runs

The `schedule_interval` parameter controls when Airflow creates new DAG runs. This is often the most misunderstood aspect of Airflow.

#### Key Principle: Airflow Runs at the END of Periods

If you set `schedule_interval='@daily'` with `start_date=datetime(2024, 1, 1)`:

- The first run happens on **January 2nd** for the period **January 1st**
- The second run happens on **January 3rd** for the period **January 2nd**

This seems counterintuitive but makes sense for data processing: you process yesterday's data today.

#### Schedule Interval Options

**Preset Schedules:**

```python
schedule_interval='@once'      # Run only once
schedule_interval='@hourly'    # Every hour
schedule_interval='@daily'     # Every day at midnight
schedule_interval='@weekly'    # Every Sunday at midnight
schedule_interval='@monthly'   # First day of month at midnight
schedule_interval='@yearly'    # January 1st at midnight
```

**Cron Expressions:**

```python
schedule_interval='0 2 * * *'      # Daily at 2 AM
schedule_interval='0 9 * * 1-5'    # Weekdays at 9 AM
schedule_interval='0 0 1 * *'      # First day of month at midnight
schedule_interval='*/15 * * * *'   # Every 15 minutes
```

**Timedelta Objects:**

```python
from datetime import timedelta

schedule_interval=timedelta(hours=6)    # Every 6 hours
schedule_interval=timedelta(days=1)     # Daily (same as @daily)
schedule_interval=timedelta(minutes=30) # Every 30 minutes
```

**No Schedule:**

```python
schedule_interval=None  # Manual triggering only
```

### Cron Expression Breakdown

Cron expressions have 5 fields: `minute hour day_of_month month day_of_week`

```
┌───────────── minute (0 - 59)
│ ┌─────────── hour (0 - 23)
│ │ ┌───────── day of month (1 - 31)
│ │ │ ┌─────── month (1 - 12)
│ │ │ │ ┌───── day of week (0 - 6) (Sunday to Saturday)
│ │ │ │ │
* * * * *
```

**Common Patterns:**

```python
'0 8 * * 1-5'    # Weekdays at 8 AM
'30 14 * * 6'    # Saturdays at 2:30 PM
'0 */4 * * *'    # Every 4 hours
'0 9 1,15 * *'   # 1st and 15th of month at 9 AM
'0 2 * * 0'      # Sundays at 2 AM
```

### Start Date and Execution Date

**Start Date**: The earliest date this DAG should run
**Execution Date**: The logical date for which the DAG run is processing data

```python
# DAG configured to run daily starting Jan 1, 2024
start_date=datetime(2024, 1, 1)
schedule_interval='@daily'

# Timeline:
# Jan 1: No run (waiting for period to complete)
# Jan 2: First run with execution_date=2024-01-01
# Jan 3: Second run with execution_date=2024-01-02
```

This design allows you to process data for specific time periods consistently.

### Catchup Behavior

When you enable a DAG that has a start date in the past, what should happen?

**Catchup=True (Default):**

```python
# If today is Jan 10 and you enable this DAG:
start_date=datetime(2024, 1, 1)
catchup=True

# Airflow will create runs for:
# Jan 1, Jan 2, Jan 3, ..., Jan 9
# All at once!
```

**Catchup=False:**

```python
# Same scenario, but:
catchup=False

# Airflow will only create a run for:
# Jan 9 (the most recent complete period)
```

**When to Use Each:**

- **Catchup=True**: When you need to process all historical data (ETL pipelines)
- **Catchup=False**: When you only care about recent data (monitoring, alerts)

## DAG Concurrency and Performance

### Max Active Runs

Controls how many instances of this DAG can run simultaneously:

```python
max_active_runs=1  # Only one run at a time (default)
max_active_runs=3  # Up to 3 concurrent runs
```

**Use Cases:**

- `max_active_runs=1`: Resource-intensive DAGs, data consistency requirements
- `max_active_runs>1`: Independent data processing, parallel historical runs

### Max Active Tasks

Controls task-level concurrency within a DAG run:

```python
max_active_tasks=16  # Up to 16 tasks running simultaneously (default)
```

### Default Arguments

Set common parameters for all tasks in the DAG:

```python
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}
```

**Key Parameters:**

- **retries**: How many times to retry failed tasks
- **retry_delay**: Wait time between retries
- **execution_timeout**: Maximum time a task can run
- **depends_on_past**: Whether task depends on previous run's success
- **email_on_failure/retry**: Notification settings

## Task Dependencies: The Art of Workflow Design

Dependencies define the order in which tasks execute. Airflow provides several ways to express these relationships.

### Basic Dependency Syntax

**Method 1: Bitshift Operators (Recommended)**

```python
task_a >> task_b  # task_a runs before task_b
task_b << task_a  # Same as above, different direction
```

**Method 2: set_downstream/set_upstream**

```python
task_a.set_downstream(task_b)
task_b.set_upstream(task_a)
```

**Method 3: depends_on_past in Task Definition**

```python
task_b = BashOperator(
    task_id='task_b',
    bash_command='echo "Task B"',
    depends_on_past=True,  # Depends on previous run
)
```

### Common Dependency Patterns

#### 1. Linear Dependencies (Sequential)

```python
# A → B → C → D
task_a >> task_b >> task_c >> task_d

# Or equivalently:
task_a >> [task_b] >> [task_c] >> [task_d]
```

**Use Case**: ETL pipelines where each step depends on the previous one.

#### 2. Fan-out Pattern (One-to-Many)

```python
# A → [B, C, D]
task_a >> [task_b, task_c, task_d]

# Or:
task_a >> task_b
task_a >> task_c
task_a >> task_d
```

**Use Case**: Parallel processing after data preparation.

#### 3. Fan-in Pattern (Many-to-One)

```python
# [A, B, C] → D
[task_a, task_b, task_c] >> task_d

# Or:
task_a >> task_d
task_b >> task_d
task_c >> task_d
```

**Use Case**: Aggregating results from parallel processes.

#### 4. Diamond Pattern (Fan-out + Fan-in)

```python
#     A
#   ↙   ↘
#  B     C
#   ↘   ↙
#     D

task_a >> [task_b, task_c] >> task_d
```

**Use Case**: Parallel processing with final aggregation.

#### 5. Complex Multi-level Dependencies

```python
#     A
#   ↙   ↘
#  B     C
#  ↓     ↓
#  D     E
#   ↘   ↙
#     F

task_a >> [task_b, task_c]
task_b >> task_d
task_c >> task_e
[task_d, task_e] >> task_f
```

### Advanced Dependency Concepts

#### Cross-DAG Dependencies

Sometimes tasks in one DAG depend on tasks in another DAG:

```python
from airflow.sensors.external_task import ExternalTaskSensor

wait_for_other_dag = ExternalTaskSensor(
    task_id='wait_for_other_dag',
    external_dag_id='upstream_dag',
    external_task_id='final_task',
    timeout=600,
    poke_interval=60,
)
```

#### Conditional Dependencies

Dependencies that only apply under certain conditions:

```python
from airflow.operators.python import BranchPythonOperator

def choose_branch(**context):
    if context['ds'] == '2024-01-01':  # New Year's Day
        return 'holiday_task'
    return 'normal_task'

branch_task = BranchPythonOperator(
    task_id='branch_task',
    python_callable=choose_branch,
)

branch_task >> [holiday_task, normal_task]
```

## Best Practices for DAG Design

### 1. Keep DAGs Simple and Focused

**Good:**

```python
# One clear purpose
dag_id='daily_sales_report'
```

**Avoid:**

```python
# Too many responsibilities
dag_id='daily_sales_report_and_inventory_update_and_email_alerts'
```

### 2. Use Meaningful Task IDs

**Good:**

```python
extract_sales_data = PythonOperator(
    task_id='extract_sales_data',
    python_callable=extract_sales,
)
```

**Avoid:**

```python
task1 = PythonOperator(
    task_id='task1',
    python_callable=some_function,
)
```

### 3. Group Related Tasks

```python
from airflow.utils.task_group import TaskGroup

with TaskGroup('data_processing') as processing_group:
    clean_data = PythonOperator(...)
    validate_data = PythonOperator(...)
    transform_data = PythonOperator(...)

    clean_data >> validate_data >> transform_data

extract_data >> processing_group >> load_data
```

### 4. Set Appropriate Timeouts

```python
long_running_task = PythonOperator(
    task_id='long_running_task',
    python_callable=complex_processing,
    execution_timeout=timedelta(hours=2),  # Prevent hanging
)
```

### 5. Use Tags for Organization

```python
dag = DAG(
    dag_id='sales_pipeline',
    tags=['sales', 'daily', 'production', 'etl'],
)
```

## Common Pitfalls and Solutions

### 1. Schedule Confusion

**Problem**: "My daily DAG isn't running when I expect"

**Solution**: Remember that `@daily` runs at the end of the day for that day's data.

```python
# If you want a DAG to run at 9 AM daily:
schedule_interval='0 9 * * *'  # Not @daily
```

### 2. Catchup Overwhelm

**Problem**: Enabling an old DAG creates hundreds of runs

**Solution**: Set `catchup=False` for most use cases:

```python
dag = DAG(
    dag_id='my_dag',
    catchup=False,  # Only run for recent periods
)
```

### 3. Circular Dependencies

**Problem**: Task A depends on Task B, which depends on Task A

**Solution**: Airflow will detect and prevent this, but design your workflow to avoid it:

```python
# This will cause an error:
task_a >> task_b >> task_a  # Circular!

# Instead, rethink your workflow:
task_a >> task_b >> task_c
```

### 4. Over-complex Dependencies

**Problem**: Dependencies that are hard to understand and maintain

**Solution**: Break complex workflows into smaller, focused DAGs:

```python
# Instead of one massive DAG:
# data_ingestion_dag → data_processing_dag → reporting_dag
```

## Debugging DAG Issues

### Common Symptoms and Causes

**DAG doesn't appear in UI:**

- Check for Python syntax errors
- Verify the file is in the `dags/` directory
- Check DAG parsing logs

**DAG appears but doesn't run:**

- Verify `start_date` is in the past
- Check `schedule_interval` configuration
- Ensure DAG is unpaused (toggle switch in UI)

**Tasks don't run in expected order:**

- Review dependency definitions
- Check for missing dependencies
- Verify task IDs are correct

**Too many historical runs:**

- Set `catchup=False`
- Use `max_active_runs=1` to limit concurrency

## Performance Considerations

### DAG File Parsing

Airflow parses all DAG files regularly. Keep parsing fast:

```python
# Good: Simple, fast parsing
dag = DAG('my_dag', ...)

# Avoid: Heavy computation during parsing
# Don't do database queries or API calls at the module level
```

### Task Design

Design tasks to be:

- **Idempotent**: Same result when run multiple times
- **Atomic**: Single, focused responsibility
- **Independent**: Minimal dependencies on external state

### Resource Management

```python
# Consider resource requirements
heavy_task = PythonOperator(
    task_id='heavy_processing',
    python_callable=cpu_intensive_function,
    pool='cpu_intensive_pool',  # Use resource pools
)
```

## What's Next

Now that you understand DAG configuration and dependencies, you're ready to:

1. Practice with the exercises in this module
2. Experiment with different scheduling patterns
3. Build workflows with complex dependency patterns
4. Learn about different operator types in Module 3

The concepts in this module form the foundation for all advanced Airflow usage. Master these, and you'll be able to design robust, maintainable workflows for any use case!
