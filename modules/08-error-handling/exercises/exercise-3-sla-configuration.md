# Exercise 3: SLA Configuration and Monitoring

## Objective

Learn to configure and monitor Service Level Agreements (SLAs) for Airflow tasks and DAGs to ensure workflows meet performance expectations.

## Background

Service Level Agreements (SLAs) define the maximum acceptable time for task or DAG completion. SLA monitoring helps you:

- Identify performance degradation early
- Ensure workflows meet business requirements
- Trigger alerts when performance thresholds are exceeded
- Track system performance trends over time

## Tasks

### Task 3.1: Basic SLA Configuration

Create a DAG called `sla_exercise_dag` with basic SLA monitoring:

1. **Fast Task with SLA**: Create a task that should complete quickly

   - Task execution time: 10-15 seconds
   - SLA: 30 seconds
   - Should always meet SLA
   - Log execution time for verification

2. **Medium Task with SLA**: Create a task with moderate execution time

   - Task execution time: 45-60 seconds
   - SLA: 90 seconds
   - Should usually meet SLA
   - Include variable execution time simulation

3. **Slow Task with Tight SLA**: Create a task likely to miss SLA
   - Task execution time: 2-3 minutes
   - SLA: 90 seconds
   - Designed to demonstrate SLA miss behavior
   - Log when SLA is likely to be missed

### Task 3.2: SLA Miss Callback Implementation

Implement comprehensive SLA miss handling:

1. **Custom SLA Miss Callback**: Create a detailed SLA miss callback function

   - Log all SLA miss details (tasks, times, blocking tasks)
   - Send structured alerts with actionable information
   - Include performance metrics and trends
   - Categorize SLA misses by severity

2. **SLA Recovery Actions**: Implement automated recovery actions
   - Attempt to identify root cause of SLA miss
   - Trigger compensating actions where possible
   - Update external monitoring systems
   - Create incident tickets for manual investigation

### Task 3.3: Variable SLA Configuration

Create tasks with different SLA strategies:

1. **Business Hours SLA**: Implement time-based SLA configuration

   - Different SLAs for business hours vs off-hours
   - Weekend vs weekday SLA differences
   - Consider system load variations
   - Implement dynamic SLA calculation

2. **Data Volume-Based SLA**: Create SLA that varies with data size
   - Estimate SLA based on input data volume
   - Implement SLA scaling logic
   - Handle edge cases (very large or small datasets)
   - Log SLA calculations for analysis

### Task 3.4: DAG-Level SLA Monitoring

Implement end-to-end workflow SLA monitoring:

1. **Pipeline SLA**: Create a multi-task pipeline with overall SLA

   - Individual task SLAs: 30s, 60s, 45s
   - Overall pipeline SLA: 3 minutes
   - Monitor both individual and aggregate performance
   - Handle cascading SLA impacts

2. **Critical Path Analysis**: Implement critical path SLA monitoring
   - Identify tasks on the critical path
   - Set stricter SLAs for critical path tasks
   - Monitor non-critical task impact on overall SLA
   - Implement priority-based SLA handling

## Implementation Guidelines

### Basic SLA Configuration

```python
task_with_sla = PythonOperator(
    task_id='sla_task',
    python_callable=your_function,
    sla=timedelta(minutes=2),  # 2-minute SLA
    dag=dag
)
```

### SLA Miss Callback Template

```python
def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """
    Callback executed when SLA is missed.

    Args:
        dag: DAG object
        task_list: Tasks that missed SLA
        blocking_task_list: Tasks blocking SLA tasks
        slas: SLA objects that were missed
        blocking_tis: TaskInstance objects that are blocking
    """
    logger.error("SLA MISS DETECTED!")

    # Log SLA miss details
    for task in task_list:
        logger.error(f"Task {task.task_id} missed SLA")

    # Create structured alert
    alert_data = {
        "timestamp": datetime.now().isoformat(),
        "dag_id": dag.dag_id,
        "missed_tasks": [task.task_id for task in task_list],
        "blocking_tasks": [task.task_id for task in blocking_task_list]
    }

    # Send alert (implement your notification logic)
    send_sla_alert(alert_data)
```

### DAG Configuration with SLA Callback

```python
default_args = {
    'owner': 'airflow-kata',
    'start_date': datetime(2024, 1, 1),
    'sla_miss_callback': sla_miss_callback,
    # other default args
}

dag = DAG(
    'sla_monitoring_dag',
    default_args=default_args,
    # other DAG configuration
)
```

### Dynamic SLA Calculation

```python
def calculate_dynamic_sla(**context):
    """Calculate SLA based on current conditions."""
    current_hour = datetime.now().hour

    # Business hours: stricter SLA
    if 9 <= current_hour <= 17:
        return timedelta(minutes=30)
    else:
        return timedelta(hours=1)

# Use in task configuration
task = PythonOperator(
    task_id='dynamic_sla_task',
    python_callable=your_function,
    sla=calculate_dynamic_sla(),
    dag=dag
)
```

## Expected Outcomes

After completing this exercise, you should have:

1. Tasks with appropriate SLA configurations
2. Comprehensive SLA miss callback implementation
3. Understanding of different SLA strategies
4. Experience with DAG-level SLA monitoring
5. Knowledge of dynamic SLA calculation

## Testing Your Implementation

1. **Run tasks** and monitor SLA compliance in Airflow UI
2. **Trigger SLA misses** by creating tasks that exceed their SLAs
3. **Verify callback execution** when SLAs are missed
4. **Check alert formatting** and notification content
5. **Test different SLA scenarios** (business hours, data volume, etc.)

## SLA Monitoring Best Practices

1. **Set Realistic SLAs**: Base on historical performance data
2. **Include Buffer Time**: Account for system load variations
3. **Monitor Trends**: Track SLA performance over time
4. **Categorize Misses**: Different handling for different miss types
5. **Automate Responses**: Implement automated recovery where possible

## Monitoring and Alerting Integration

### Structured Logging for Monitoring

```python
# Log SLA metrics for external monitoring
sla_metrics = {
    "timestamp": datetime.now().isoformat(),
    "dag_id": dag_id,
    "task_id": task_id,
    "sla_seconds": sla.total_seconds(),
    "actual_duration": actual_duration,
    "sla_miss": actual_duration > sla.total_seconds(),
    "miss_margin": actual_duration - sla.total_seconds()
}

logger.info(f"SLA_METRICS: {json.dumps(sla_metrics)}")
```

### External System Integration

```python
def send_sla_alert(alert_data):
    """Send SLA alert to external monitoring system."""
    # Example integrations:
    # - Send to Slack webhook
    # - Create PagerDuty incident
    # - Update Grafana dashboard
    # - Send to custom monitoring API
    pass
```

## Bonus Challenges

1. **SLA Forecasting**: Predict SLA misses before they occur
2. **Adaptive SLAs**: Automatically adjust SLAs based on performance trends
3. **SLA Dashboard**: Create a dashboard showing SLA compliance metrics
4. **Cascade Analysis**: Analyze how SLA misses impact downstream tasks
5. **Performance Optimization**: Identify and fix common SLA miss causes

## Common Pitfalls

- Setting unrealistic SLAs that are frequently missed
- Not considering system load variations in SLA calculations
- Missing SLA callback implementations
- Not monitoring SLA trends over time
- Setting the same SLA for all tasks regardless of complexity

## Troubleshooting SLA Issues

1. **Check Task Logs**: Look for performance bottlenecks
2. **Monitor System Resources**: CPU, memory, disk usage
3. **Analyze Dependencies**: Identify blocking tasks
4. **Review Historical Data**: Look for performance trends
5. **Test in Isolation**: Run tasks independently to isolate issues

## Next Steps

Once you complete this exercise, you'll be ready to move on to the advanced monitoring and alerting exercises, where you'll learn to build comprehensive monitoring solutions for production Airflow deployments.
