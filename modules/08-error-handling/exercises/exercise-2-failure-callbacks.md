# Exercise 2: Failure Callbacks

## Objective

Learn to implement failure callbacks for notifications, cleanup, and custom error handling when Airflow tasks fail.

## Background

When tasks fail in production, you need more than just retry logic. Failure callbacks allow you to:

- Send notifications to relevant teams
- Clean up resources that were allocated
- Log detailed error information
- Update external systems about the failure
- Trigger compensating actions

## Tasks

### Task 2.1: Notification Callbacks

Create a DAG called `callback_exercise_dag` with notification callbacks:

1. **Email Notification Task**: Create a task that sends email notifications on failure

   - Implement `send_email_alert()` callback function
   - Include task details, error message, and execution context
   - Format the email with relevant debugging information
   - Test with a task that always fails

2. **Slack Notification Task**: Create a task with Slack webhook integration
   - Implement `send_slack_alert()` callback function
   - Format message with task status, error details, and links
   - Include different message formats for different error types
   - Use structured message format with attachments

### Task 2.2: Resource Cleanup Callbacks

Implement cleanup callbacks for resource management:

1. **File Cleanup Task**: Create a task that creates temporary files and cleans them up on failure

   - Task should create temporary files during execution
   - Implement `cleanup_temp_files()` callback
   - Ensure cleanup happens even when task fails
   - Log cleanup operations for verification

2. **Connection Cleanup Task**: Simulate database connections that need cleanup
   - Mock database connection creation
   - Implement `cleanup_connections()` callback
   - Handle cleanup of multiple resource types
   - Ensure no resource leaks on failure

### Task 2.3: Metrics and Logging Callbacks

Create callbacks for monitoring and observability:

1. **Failure Metrics Task**: Implement comprehensive failure logging

   - Create `log_failure_metrics()` callback
   - Log structured data including error type, duration, context
   - Include task performance metrics
   - Format logs for external monitoring systems

2. **Error Classification Task**: Implement intelligent error categorization
   - Create `classify_and_log_error()` callback
   - Categorize errors (transient, permanent, configuration, etc.)
   - Log different metrics based on error type
   - Suggest remediation actions based on error category

### Task 2.4: Multi-Callback Integration

Create a task that uses multiple callbacks:

1. **Production-Ready Task**: Combine multiple callback types
   - Use notification, cleanup, and metrics callbacks together
   - Implement callback chaining and error handling
   - Ensure callbacks don't interfere with each other
   - Handle callback failures gracefully

## Implementation Guidelines

### Callback Function Template

```python
def your_callback_function(context):
    """
    Callback function executed on task failure.

    Args:
        context: Dictionary containing task execution context
    """
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    exception = context.get('exception')

    # Your callback logic here
    logger.error(f"Callback executed for {dag_id}.{task_id}")

    # Important: Don't raise exceptions in callbacks
    try:
        # Callback implementation
        pass
    except Exception as e:
        logger.error(f"Callback failed: {e}")
```

### Task Configuration with Callbacks

```python
task_with_callbacks = PythonOperator(
    task_id='callback_example',
    python_callable=your_task_function,
    on_failure_callback=your_callback_function,
    on_success_callback=success_callback,
    on_retry_callback=retry_callback,
    retries=2,
    dag=dag
)
```

### Multiple Callbacks

```python
# Multiple callbacks as a list
task = PythonOperator(
    task_id='multi_callback',
    python_callable=your_function,
    on_failure_callback=[
        send_notification,
        cleanup_resources,
        log_metrics
    ],
    dag=dag
)
```

## Expected Outcomes

After completing this exercise, you should have:

1. Working notification callbacks for email and Slack
2. Resource cleanup callbacks that prevent leaks
3. Comprehensive failure logging and metrics collection
4. Understanding of callback execution order and error handling
5. Experience with multiple callback integration

## Testing Your Implementation

1. **Trigger tasks** that are designed to fail
2. **Verify callbacks execute** by checking logs
3. **Test notification formatting** and content
4. **Confirm resource cleanup** occurs properly
5. **Validate metrics logging** structure and content

## Callback Context Information

The context dictionary contains useful information:

```python
# Available context keys
context['dag']              # DAG object
context['task']             # Task object
context['task_instance']    # TaskInstance object
context['execution_date']   # Execution date
context['exception']        # Exception that caused failure
context['reason']           # Reason for callback execution
context['dag_run']          # DagRun object
```

## Best Practices

1. **Keep callbacks lightweight** - avoid heavy processing
2. **Handle callback errors** - don't let callbacks fail
3. **Use structured logging** - make logs searchable
4. **Include context information** - make alerts actionable
5. **Test callback logic** - ensure callbacks work as expected

## Bonus Challenges

1. **Conditional Callbacks**: Implement callbacks that execute based on error type
2. **Callback Metrics**: Track callback execution success/failure
3. **External Integration**: Integrate with monitoring systems (DataDog, New Relic)
4. **Callback Templates**: Create reusable callback templates
5. **Escalation Logic**: Implement alert escalation based on failure patterns

## Common Pitfalls

- Callbacks that raise exceptions and cause additional failures
- Heavy processing in callbacks that delays task cleanup
- Missing error handling in callback functions
- Callbacks that depend on external services without timeouts
- Not testing callback functionality in development

## Next Steps

Once you complete this exercise, you'll be ready to move on to Exercise 3: SLA Configuration, where you'll learn to set up and monitor service level agreements for your workflows.
