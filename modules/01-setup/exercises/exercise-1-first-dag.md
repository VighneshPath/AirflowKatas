# Exercise 1: Create Your First DAG

## Objective

Create a simple DAG that demonstrates your understanding of basic Airflow concepts including DAG definition, task creation, and dependency management.

## Background

You work for a small e-commerce company that wants to automate their daily data processing. As a first step, you need to create a simple workflow that:

1. Checks if the system is ready for processing
2. Backs up yesterday's data
3. Processes new orders
4. Generates a summary report
5. Sends a completion notification

## Requirements

Create a DAG named `my_first_dag` with the following specifications:

### DAG Configuration

- **DAG ID**: `my_first_dag`
- **Description**: "My first Airflow DAG for daily data processing"
- **Schedule**: Run once per day
- **Start Date**: January 1, 2024
- **Owner**: Your name
- **Retries**: 2 attempts with 10-minute delays
- **Tags**: `['exercise', 'beginner', 'daily']`

### Tasks to Implement

1. **system_check** (BashOperator)

   - Print "System health check: All systems operational"
   - Check disk space with `df -h`

2. **backup_data** (BashOperator)

   - Print "Backing up yesterday's data..."
   - Create a timestamp with `date`

3. **process_orders** (PythonOperator)

   - Create a Python function that:
     - Prints "Processing new orders..."
     - Simulates processing by printing order IDs 1001-1005
     - Returns the number of orders processed

4. **generate_report** (PythonOperator)

   - Create a Python function that:
     - Prints "Generating daily summary report..."
     - Prints current date and time
     - Returns "Report generated successfully"

5. **send_notification** (BashOperator)
   - Print "Daily processing completed successfully!"
   - Print "Notification sent to operations team"

### Task Dependencies

Set up the following execution flow:

```
system_check → backup_data → process_orders → generate_report → send_notification
```

## Step-by-Step Guide

### Step 1: Set Up Your File

Create a new file `dags/my_first_dag.py` and start with the necessary imports:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Your code goes here...
```

### Step 2: Define Python Functions

Create the Python functions for your PythonOperator tasks:

```python
def process_orders():
    """Process new orders and return count"""
    # Your implementation here
    pass

def generate_report():
    """Generate daily summary report"""
    # Your implementation here
    pass
```

### Step 3: Configure Default Arguments

Set up default arguments for your DAG:

```python
default_args = {
    # Fill in the required configuration
}
```

### Step 4: Create the DAG

Define your DAG with the specified configuration:

```python
dag = DAG(
    # Your DAG configuration here
)
```

### Step 5: Create Tasks

Define all five tasks using the appropriate operators:

```python
# Task 1: system_check
system_check = BashOperator(
    # Your task configuration
)

# Continue with other tasks...
```

### Step 6: Set Dependencies

Define the task execution order:

```python
# Set up your task dependencies here
```

## Testing Your DAG

### 1. Copy to DAGs Folder

Place your completed DAG file in the `dags/` directory of the kata project.

### 2. Check DAG Parsing

In the Airflow UI:

1. Go to http://localhost:8080
2. Look for your DAG in the list
3. Check that it appears without errors (no red indicators)

### 3. Trigger a Test Run

1. Click on your DAG name
2. Click the "Trigger DAG" button (play icon)
3. Watch the tasks execute in the Graph view

### 4. Verify Task Logs

1. Click on each task in the Graph view
2. Select "Log" to see the output
3. Verify that your print statements appear correctly

## Success Criteria

Your DAG is complete when:

- [ ] DAG appears in Airflow UI without parsing errors
- [ ] All 5 tasks are visible in the Graph view
- [ ] Tasks execute in the correct order
- [ ] Each task completes successfully (green status)
- [ ] Task logs show the expected output messages
- [ ] Python functions return the specified values

## Common Issues and Solutions

**DAG doesn't appear in UI:**

- Check for Python syntax errors
- Ensure the file is in the `dags/` directory
- Wait 30 seconds for Airflow to detect the new DAG

**Tasks fail to execute:**

- Check task logs for error messages
- Verify Python function names match the `python_callable` parameter
- Ensure bash commands are properly formatted

**Wrong execution order:**

- Review your dependency definitions
- Make sure you're using `>>` or `set_downstream()` correctly

## Bonus Challenges

Once your basic DAG works, try these enhancements:

1. **Add Error Handling**: Make one task fail and observe the behavior
2. **Parallel Execution**: Modify dependencies so some tasks run in parallel
3. **Task Context**: Use `**context` in your Python functions to access execution date
4. **Return Values**: Have one task return data and another task use it

## Next Steps

After completing this exercise, you'll have hands-on experience with:

- DAG structure and configuration
- Using BashOperator and PythonOperator
- Setting task dependencies
- Testing and debugging DAGs

This foundation prepares you for more advanced concepts in the next modules!
