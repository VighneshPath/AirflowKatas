# Advanced Scheduling & Dependencies Concepts

## Advanced Scheduling Patterns

### Cron Expressions in Airflow

Airflow uses cron expressions to define complex scheduling patterns. Understanding these patterns is crucial for building production workflows.

#### Basic Cron Syntax

```
* * * * *
│ │ │ │ │
│ │ │ │ └── Day of week (0-7, Sunday = 0 or 7)
│ │ │ └──── Month (1-12)
│ │ └────── Day of month (1-31)
│ └──────── Hour (0-23)
└────────── Minute (0-59)
```

#### Common Patterns

- `0 2 * * *` - Daily at 2:00 AM
- `0 2 * * 1` - Weekly on Monday at 2:00 AM
- `0 2 1 * *` - Monthly on the 1st at 2:00 AM
- `0 */4 * * *` - Every 4 hours
- `0 2 1 */3 *` - Quarterly on the 1st at 2:00 AM

### Catchup and Backfill

#### Catchup Behavior

When `catchup=True`, Airflow will automatically run missed DAG runs for the period between the start_date and current time.

```python
dag = DAG(
    'example_catchup',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=True  # Will run for all days since start_date
)
```

#### Backfill Scenarios

Backfill is useful for:

- Processing historical data
- Recovering from system outages
- Initial data loads
- Testing with historical periods

### Timezone Considerations

Airflow operates in UTC by default, but you can specify timezones:

```python
from pendulum import timezone

dag = DAG(
    'timezone_example',
    start_date=datetime(2024, 1, 1, tzinfo=timezone('US/Eastern')),
    schedule_interval='0 9 * * *',  # 9 AM Eastern
)
```

## Dependency Patterns

### Fan-Out Pattern

One task triggers multiple parallel tasks:

```
    Task A
   /  |  \
Task B Task C Task D
```

### Fan-In Pattern

Multiple tasks converge to a single task:

```
Task A   Task B   Task C
   \      |      /
      Task D
```

### Diamond Pattern

Combination of fan-out and fan-in:

```
    Task A
   /      \
Task B    Task C
   \      /
    Task D
```

### Conditional Dependencies

Tasks that run based on conditions or previous task outcomes.

## Best Practices

### Scheduling Best Practices

1. **Use appropriate intervals**: Don't over-schedule; consider system resources
2. **Set realistic start_dates**: Avoid unnecessary catchup runs
3. **Consider timezone implications**: Be explicit about timezone requirements
4. **Use max_active_runs**: Prevent resource exhaustion from parallel runs

### Dependency Best Practices

1. **Keep dependencies simple**: Complex dependency graphs are hard to debug
2. **Use meaningful task names**: Make dependencies self-documenting
3. **Avoid circular dependencies**: Airflow will reject DAGs with cycles
4. **Group related tasks**: Use TaskGroups for logical organization
5. **Consider failure scenarios**: Design dependencies to handle task failures gracefully

### Performance Considerations

1. **Minimize cross-DAG dependencies**: They can create bottlenecks
2. **Use appropriate pool configurations**: Control resource usage
3. **Consider task duration**: Balance parallelism with resource constraints
4. **Monitor dependency resolution time**: Complex graphs can slow DAG parsing
