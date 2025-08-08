# Exercise 1: Basic Branching with BranchPythonOperator

## Learning Objectives

By completing this exercise, you will:

- Implement basic branching logic using BranchPythonOperator
- Create conditional workflows based on simple criteria
- Understand task skipping behavior in branched workflows
- Practice joining execution paths after branching

## Prerequisites

- Completed Module 6: Data Passing & XComs
- Understanding of PythonOperator and task dependencies
- Basic knowledge of Python conditional logic

## Scenario

You're building a data processing pipeline for an e-commerce company. The pipeline needs to handle different types of data processing based on the day of the week:

- **Weekdays (Monday-Friday)**: Process transaction data with standard validation
- **Weekends (Saturday-Sunday)**: Process inventory data with enhanced validation

Additionally, the pipeline should handle different processing volumes:

- **Small datasets** (< 1000 records): Single-threaded processing
- **Large datasets** (≥ 1000 records): Multi-threaded processing

## Exercise Tasks

### Task 1: Day-of-Week Branching

Create a DAG that branches based on the current day of the week.

**Requirements:**

1. Create a function `day_of_week_branch()` that:

   - Gets the current day of the week
   - Returns `'weekday_processing'` for Monday-Friday
   - Returns `'weekend_processing'` for Saturday-Sunday

2. Implement processing functions:

   - `weekday_processing()`: Simulates transaction data processing
   - `weekend_processing()`: Simulates inventory data processing

3. Create a join point that executes regardless of which branch ran

**Starter Code:**

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'owner': 'your-name',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def day_of_week_branch(**context):
    """
    TODO: Implement day-of-week branching logic

    Returns:
        str: 'weekday_processing' or 'weekend_processing'
    """
    # Your code here
    pass

def weekday_processing(**context):
    """
    TODO: Implement weekday processing logic

    Should simulate:
    - Transaction data loading
    - Standard validation
    - Basic reporting
    """
    # Your code here
    pass

def weekend_processing(**context):
    """
    TODO: Implement weekend processing logic

    Should simulate:
    - Inventory data loading
    - Enhanced validation
    - Detailed reporting
    """
    # Your code here
    pass

# TODO: Create DAG and tasks
dag = DAG(
    'exercise_1_day_branching',
    default_args=default_args,
    description='Day-of-week branching exercise',
    schedule_interval=timedelta(hours=12),
    catchup=False,
    tags=['exercise', 'branching', 'basic']
)

# TODO: Define your tasks here

# TODO: Set up task dependencies
```

### Task 2: Data Volume Branching

Extend your DAG to include data volume-based branching.

**Requirements:**

1. Add a data generation task that creates sample data with random record counts
2. Create a volume-based branch that:
   - Returns `'small_volume_processing'` for < 1000 records
   - Returns `'large_volume_processing'` for ≥ 1000 records
3. Implement appropriate processing functions for each volume type

**Additional Code:**

```python
def generate_sample_data(**context):
    """
    TODO: Generate sample data with random record count

    Returns:
        dict: Data information including record count
    """
    # Your code here
    pass

def volume_based_branch(**context):
    """
    TODO: Branch based on data volume from previous task

    Returns:
        str: 'small_volume_processing' or 'large_volume_processing'
    """
    # Your code here
    pass

def small_volume_processing(**context):
    """TODO: Implement small volume processing"""
    # Your code here
    pass

def large_volume_processing(**context):
    """TODO: Implement large volume processing"""
    # Your code here
    pass

# TODO: Add these tasks to your DAG and set up dependencies
```

### Task 3: Combined Branching Logic

Create a more complex branching scenario that combines both day-of-week and volume considerations.

**Requirements:**

1. Create a function that considers both day type and volume
2. Implement four different processing paths:
   - `weekday_small_processing`
   - `weekday_large_processing`
   - `weekend_small_processing`
   - `weekend_large_processing`

**Challenge Code:**

```python
def combined_branch(**context):
    """
    TODO: Implement combined branching logic

    Consider both day of week and data volume to determine processing path.

    Returns:
        str: One of four processing task IDs
    """
    # Your code here
    pass

# TODO: Implement the four processing functions
# TODO: Create a new DAG with this combined logic
```

## Testing Your Implementation

### Test Cases

1. **Day Branching Test:**

   ```python
   # Test weekday
   import datetime
   mock_context = {'execution_date': datetime.datetime(2024, 1, 15)}  # Monday
   result = day_of_week_branch(**mock_context)
   assert result == 'weekday_processing'

   # Test weekend
   mock_context = {'execution_date': datetime.datetime(2024, 1, 13)}  # Saturday
   result = day_of_week_branch(**mock_context)
   assert result == 'weekend_processing'
   ```

2. **Volume Branching Test:**

   ```python
   # Test small volume
   mock_ti = type('MockTI', (), {'xcom_pull': lambda task_ids: {'record_count': 500}})()
   mock_context = {'task_instance': mock_ti}
   result = volume_based_branch(**mock_context)
   assert result == 'small_volume_processing'

   # Test large volume
   mock_ti = type('MockTI', (), {'xcom_pull': lambda task_ids: {'record_count': 1500}})()
   mock_context = {'task_instance': mock_ti}
   result = volume_based_branch(**mock_context)
   assert result == 'large_volume_processing'
   ```

### Validation Steps

1. **DAG Validation:**

   - Ensure your DAG parses without errors
   - Check that all task dependencies are correctly defined
   - Verify that join points use appropriate trigger rules

2. **Execution Testing:**

   - Run your DAG in the Airflow UI
   - Observe which tasks execute and which are skipped
   - Check the logs to see your branching decisions

3. **Edge Case Testing:**
   - Test with different execution dates
   - Verify behavior with various data volumes
   - Ensure proper error handling

## Expected Outputs

### Task 1 Output Example:

```
[2024-01-15 10:00:00] INFO - Current day: Monday (weekday)
[2024-01-15 10:00:00] INFO - Choosing weekday processing path
[2024-01-15 10:00:00] INFO - Weekday processing:
[2024-01-15 10:00:00] INFO - - Loading transaction data
[2024-01-15 10:00:00] INFO - - Applying standard validation
[2024-01-15 10:00:00] INFO - - Generating basic reports
```

### Task 2 Output Example:

```
[2024-01-15 10:00:00] INFO - Generated 750 sample records
[2024-01-15 10:00:00] INFO - Small volume detected: using single-threaded processing
[2024-01-15 10:00:00] INFO - Small volume processing:
[2024-01-15 10:00:00] INFO - - Single-threaded execution
[2024-01-15 10:00:00] INFO - - Memory-efficient processing
```

## Common Pitfalls

1. **Incorrect Return Values:**

   - Ensure branching functions return exact task IDs as strings
   - Task IDs are case-sensitive and must match exactly

2. **Missing Trigger Rules:**

   - Join tasks after branching need appropriate trigger rules
   - Use `TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS` for most cases

3. **XCom Access Errors:**

   - Check that previous tasks have completed before accessing XComs
   - Handle cases where XCom data might be None

4. **Date/Time Issues:**
   - Be aware of timezone considerations in date-based branching
   - Use execution_date from context for consistent behavior

## Bonus Challenges

1. **Add Logging:** Enhance your branching functions with detailed logging
2. **Error Handling:** Add try-catch blocks to handle edge cases
3. **Configuration:** Use Airflow Variables to make branching criteria configurable
4. **Metrics:** Track branching decisions for analysis

## Solution Verification

Your solution should demonstrate:

- ✅ Correct implementation of BranchPythonOperator
- ✅ Proper task dependency setup with branching
- ✅ Appropriate use of trigger rules for join points
- ✅ Clear logging and decision tracking
- ✅ Handling of different execution scenarios

## Next Steps

After completing this exercise:

1. Review the solution file to compare approaches
2. Experiment with different branching criteria
3. Move on to Exercise 2 for more complex branching patterns
4. Consider how branching might apply to your real-world use cases

## Help and Troubleshooting

### Common Issues:

**Issue:** Tasks not skipping properly
**Solution:** Verify that your branching function returns the exact task_id string

**Issue:** Join task not executing
**Solution:** Check that your join task has the correct trigger rule

**Issue:** XCom data not found
**Solution:** Ensure the previous task completed successfully and pushed data to XCom

### Getting Help:

1. Check the Airflow logs for detailed error messages
2. Review the basic branching examples in the examples directory
3. Test your branching functions independently before adding to DAG
4. Use the Airflow UI to visualize task execution flow

---

**Estimated Time:** 45-60 minutes

**Difficulty:** Beginner to Intermediate

Ready to implement your first branching workflow? Start with Task 1 and work your way through each requirement!
