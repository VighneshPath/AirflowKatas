# Exercise 1: Retry Strategies

## Objective

Learn to implement different retry strategies for handling transient failures in Airflow tasks.

## Background

In production environments, tasks often fail due to transient issues like network timeouts, temporary resource unavailability, or external service outages. Implementing appropriate retry strategies can significantly improve workflow reliability.

## Tasks

### Task 1.1: Basic Retry Configuration

Create a DAG called `retry_exercise_dag` with the following requirements:

1. **File Processing Task**: Create a Python task that simulates reading a file that might be temporarily unavailable

   - Task should fail 60% of the time on first attempt
   - Configure 3 retries with 30-second delays
   - Log each attempt number and outcome

2. **API Call Task**: Create a task that simulates an API call with network issues
   - Task should fail 40% of the time on first attempt, 20% on second
   - Configure 4 retries with 1-minute delays
   - Return mock API response data on success

### Task 1.2: Exponential Backoff

Extend your DAG with a task that uses exponential backoff:

1. **Database Connection Task**: Simulate a database operation that might fail due to connection issues
   - Configure exponential backoff starting with 30 seconds
   - Set maximum retry delay to 10 minutes
   - Use 5 retry attempts
   - Log the actual delay between attempts

### Task 1.3: Conditional Retry Logic

Create a task with smart retry logic:

1. **Data Validation Task**: Create a task that validates data quality
   - If data is missing: retry (transient issue)
   - If data format is invalid: don't retry (permanent issue)
   - If data is corrupted: retry with exponential backoff
   - Implement custom logic to determine retry behavior

### Task 1.4: Bash Command Retries

Add a Bash task with retry configuration:

1. **System Health Check**: Create a bash task that checks system resources
   - Command should check disk space and memory
   - Fail if resources are below threshold
   - Configure appropriate retry strategy
   - Include logging of system status

## Implementation Guidelines

### Python Task Template

```python
def your_task_function(**context):
    task_instance = context['task_instance']
    attempt_number = task_instance.try_number

    # Log attempt information
    logger.info(f"Attempt #{attempt_number}")

    # Your task logic here
    # Simulate failures based on attempt number

    return "Task result"
```

### Retry Configuration Examples

```python
# Basic retry
task = PythonOperator(
    task_id='basic_retry',
    python_callable=your_function,
    retries=3,
    retry_delay=timedelta(seconds=30)
)

# Exponential backoff
task = PythonOperator(
    task_id='exponential_backoff',
    python_callable=your_function,
    retries=5,
    retry_delay=timedelta(seconds=30),
    retry_exponential_backoff=True,
    max_retry_delay=timedelta(minutes=10)
)
```

## Expected Outcomes

After completing this exercise, you should have:

1. A working DAG with multiple retry strategies
2. Understanding of when to use different retry configurations
3. Experience with exponential backoff implementation
4. Knowledge of conditional retry logic
5. Practical experience with both Python and Bash task retries

## Testing Your Implementation

1. **Trigger your DAG** and observe the retry behavior in the Airflow UI
2. **Check the logs** to see retry attempts and delays
3. **Verify** that tasks eventually succeed after retries
4. **Test edge cases** by modifying failure probabilities

## Bonus Challenges

1. **Custom Retry Delay**: Implement a custom retry delay function
2. **Retry Metrics**: Add logging to track retry statistics
3. **External Dependency**: Create a task that retries based on external service availability
4. **Retry Notification**: Send notifications on retry attempts

## Common Pitfalls

- Setting too many retries for permanent failures
- Using fixed delays for all types of failures
- Not logging retry attempts for debugging
- Ignoring the impact of retries on downstream tasks
- Not considering resource consumption during retries

## Next Steps

Once you complete this exercise, you'll be ready to move on to Exercise 2: Failure Callbacks, where you'll learn to implement custom actions when tasks fail.
