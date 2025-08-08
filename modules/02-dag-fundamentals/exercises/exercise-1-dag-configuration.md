# Exercise 1: DAG Configuration Mastery

## Objective

Master DAG configuration by creating DAGs with different scheduling patterns, catchup behaviors, and performance settings. This exercise will help you understand how each configuration parameter affects DAG execution.

## Background

You're a data engineer at a growing e-commerce company. The business has different data processing needs that require various scheduling patterns and configurations. You need to create several DAGs to handle these requirements efficiently.

## Requirements

Create **three separate DAG files** with the following specifications:

### DAG 1: Daily Sales Report (`daily_sales_report.py`)

**Business Need**: Generate daily sales reports every morning at 6 AM for the previous day's data.

**Configuration Requirements**:

- **DAG ID**: `daily_sales_report`
- **Description**: "Generate daily sales report for previous day"
- **Schedule**: Daily at 6:00 AM
- **Start Date**: January 1, 2024
- **Catchup**: Disabled (only process recent data)
- **Max Active Runs**: 1 (prevent overlapping runs)
- **Owner**: "sales_team"
- **Retries**: 3 attempts with 5-minute delays
- **Email on Failure**: Enabled
- **Tags**: `['sales', 'daily', 'reports']`

**Tasks to Implement**:

1. `extract_sales_data` - Extract yesterday's sales data
2. `calculate_metrics` - Calculate daily sales metrics
3. `generate_report` - Create the sales report
4. `send_report` - Email report to stakeholders

**Dependencies**: Linear sequence (extract → calculate → generate → send)

### DAG 2: Hourly System Monitoring (`hourly_monitoring.py`)

**Business Need**: Monitor system health every hour during business hours (9 AM to 6 PM, weekdays only).

**Configuration Requirements**:

- **DAG ID**: `hourly_system_monitoring`
- **Description**: "Monitor system health during business hours"
- **Schedule**: Every hour from 9 AM to 6 PM, Monday through Friday
- **Start Date**: January 1, 2024
- **Catchup**: Disabled
- **Max Active Runs**: 3 (allow some overlap for monitoring)
- **Owner**: "ops_team"
- **Retries**: 1 attempt with 2-minute delay
- **Execution Timeout**: 15 minutes per task
- **Tags**: `['monitoring', 'hourly', 'system']`

**Tasks to Implement**:

1. `check_cpu_usage` - Monitor CPU utilization
2. `check_memory_usage` - Monitor memory utilization
3. `check_disk_space` - Monitor disk space
4. `check_network_connectivity` - Test network connections
5. `aggregate_metrics` - Combine all monitoring data

**Dependencies**: Parallel monitoring (tasks 1-4 run in parallel) → aggregate

### DAG 3: Monthly Data Archive (`monthly_archive.py`)

**Business Need**: Archive old data on the first day of each month, with ability to process historical months if needed.

**Configuration Requirements**:

- **DAG ID**: `monthly_data_archive`
- **Description**: "Archive previous month's data on first day of month"
- **Schedule**: First day of every month at 3:00 AM
- **Start Date**: February 1, 2024 (so first run processes January data)
- **Catchup**: Enabled (process historical months if DAG was disabled)
- **Max Active Runs**: 1 (archiving is resource-intensive)
- **Owner**: "data_team"
- **Retries**: 2 attempts with 30-minute delays
- **Execution Timeout**: 4 hours per task
- **Depends on Past**: True (each month depends on previous month's success)
- **Tags**: `['archive', 'monthly', 'maintenance']`

**Tasks to Implement**:

1. `identify_archive_data` - Find data older than 30 days
2. `backup_to_storage` - Create backup of data to be archived
3. `compress_data` - Compress archived data
4. `update_metadata` - Update data catalog with archive information
5. `cleanup_old_data` - Remove archived data from active storage

**Dependencies**: Linear sequence with all tasks dependent on previous completion

## Step-by-Step Implementation Guide

### Step 1: Set Up Your Files

Create three new files in the `dags/` directory:

- `dags/daily_sales_report.py`
- `dags/hourly_monitoring.py`
- `dags/monthly_archive.py`

### Step 2: Import Required Modules

Each file should start with:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
```

### Step 3: Create Python Functions

For each DAG, create the necessary Python functions for PythonOperator tasks:

```python
# Example for daily sales report
def extract_sales_data(**context):
    """Extract sales data for the execution date"""
    execution_date = context['execution_date']
    print(f"Extracting sales data for {execution_date.strftime('%Y-%m-%d')}")
    # Simulate data extraction
    return f"Extracted 1,234 sales records for {execution_date.strftime('%Y-%m-%d')}"

def calculate_metrics(**context):
    """Calculate daily sales metrics"""
    # Your implementation here
    pass

# Continue with other functions...
```

### Step 4: Configure Each DAG

Use the specifications above to configure each DAG properly:

```python
# Example DAG configuration
dag = DAG(
    dag_id='your_dag_id',
    description='Your description',
    schedule_interval='your_schedule',
    start_date=datetime(2024, 1, 1),
    catchup=False,  # or True based on requirements
    max_active_runs=1,
    default_args={
        'owner': 'your_team',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        # Add other default args as needed
    },
    tags=['your', 'tags']
)
```

### Step 5: Create Tasks and Dependencies

Implement the tasks and set up dependencies according to the specifications.

## Testing Your DAGs

### 1. Validate DAG Parsing

1. Place all three files in the `dags/` directory
2. Check the Airflow UI at http://localhost:8080
3. Verify all three DAGs appear without parsing errors
4. Check that the schedule intervals are displayed correctly

### 2. Test Configuration

For each DAG, verify:

- **Schedule Display**: Check that the schedule shows correctly in the UI
- **Next Run Time**: Verify the next scheduled run time makes sense
- **DAG Details**: Click on each DAG to see configuration details
- **Task Count**: Ensure all tasks are visible in the Graph view

### 3. Manual Test Runs

1. Trigger each DAG manually using the "Trigger DAG" button
2. Watch task execution in the Graph view
3. Check task logs to verify your functions are working
4. Ensure dependencies execute in the correct order

### 4. Schedule Verification

Use the Airflow CLI to verify schedules (optional):

```bash
# Check next few run dates for a DAG
docker exec -it <airflow_container> airflow dags next-execution daily_sales_report
```

## Success Criteria

Your implementation is complete when:

### Daily Sales Report DAG:

- [ ] Runs daily at 6:00 AM
- [ ] Has catchup disabled
- [ ] Shows 4 tasks in linear sequence
- [ ] Tasks complete successfully when triggered
- [ ] Proper error handling with 3 retries

### Hourly Monitoring DAG:

- [ ] Runs every hour from 9 AM to 6 PM on weekdays
- [ ] Allows up to 3 concurrent runs
- [ ] Shows 5 tasks (4 parallel + 1 aggregation)
- [ ] Parallel tasks execute simultaneously
- [ ] 15-minute execution timeout is configured

### Monthly Archive DAG:

- [ ] Runs on first day of each month at 3:00 AM
- [ ] Has catchup enabled
- [ ] Shows depends_on_past behavior
- [ ] 5 tasks execute in linear sequence
- [ ] 4-hour timeout and 30-minute retry delays configured

## Common Issues and Solutions

### Schedule Not Working as Expected

**Problem**: DAG shows wrong next run time

**Solution**:

- Remember Airflow runs at the END of periods
- Double-check your cron expression syntax
- Verify start_date is in the past

### Tasks Not Running in Parallel

**Problem**: Monitoring tasks run sequentially instead of parallel

**Solution**:

```python
# Correct parallel setup
[task1, task2, task3, task4] >> aggregation_task

# Not this:
task1 >> task2 >> task3 >> task4 >> aggregation_task
```

### Catchup Creating Too Many Runs

**Problem**: Monthly archive creates many historical runs

**Solution**: This is expected behavior when catchup=True. You can:

- Set a more recent start_date
- Use `max_active_runs=1` to process them sequentially
- Clear unwanted runs from the UI if needed

### Configuration Not Taking Effect

**Problem**: Changes to DAG configuration don't appear

**Solution**:

- Wait 30 seconds for Airflow to re-parse DAGs
- Check for Python syntax errors
- Restart the Airflow scheduler if needed

## Bonus Challenges

Once your basic DAGs work, try these enhancements:

### 1. Dynamic Scheduling

Modify the hourly monitoring to skip runs on company holidays using a custom timetable.

### 2. SLA Configuration

Add SLA monitoring to the daily sales report (should complete within 30 minutes).

### 3. Resource Pools

Create resource pools for CPU-intensive tasks in the monthly archive DAG.

### 4. Custom Retry Logic

Implement different retry strategies for different types of failures.

## Verification Questions

Test your understanding by answering these questions:

1. **Why does the daily sales report run at 6 AM but process the previous day's data?**

2. **What happens if you enable the monthly archive DAG on March 15th with catchup=True?**

3. **How many concurrent runs can happen for the hourly monitoring DAG during a busy day?**

4. **What's the difference between setting retries in default_args vs. on individual tasks?**

5. **Why might you want depends_on_past=True for the archive DAG but not the others?**

## Next Steps

After completing this exercise, you'll have hands-on experience with:

- Complex scheduling patterns using cron expressions
- Catchup behavior and its implications
- Concurrency control with max_active_runs
- Different retry and timeout strategies
- Task dependency patterns

This foundation prepares you for Exercise 2, where you'll explore advanced dependency patterns and workflow design!
