# Exercise 4: Performance Tuning

## Objective

Learn to identify performance bottlenecks and implement optimization techniques to improve Airflow pipeline performance and resource utilization.

## Background

Your data engineering team has been running Airflow pipelines in production, but they're experiencing performance issues:

- DAGs are taking longer to complete than expected
- Memory usage is high and causing worker failures
- Database connections are being exhausted
- Some tasks are competing for resources inefficiently
- The scheduler is experiencing lag during peak hours

You need to implement comprehensive performance tuning to optimize the entire pipeline.

## Requirements

Create a DAG called `performance_tuning_pipeline` that demonstrates:

1. **Memory optimization** techniques for large dataset processing
2. **Connection pooling** and database optimization
3. **Resource pool management** for different task types
4. **Caching strategies** to avoid redundant computations
5. **Performance monitoring** and alerting
6. **Incremental processing** to reduce computational overhead

## Performance Scenarios

### Scenario 1: Memory-Intensive Processing

You have a task that processes large datasets (100GB+) but is causing out-of-memory errors.

**Requirements:**

- Implement chunked processing to manage memory usage
- Use generator patterns for memory-efficient iteration
- Implement proper cleanup after processing chunks
- Monitor memory usage throughout the process

### Scenario 2: Database Connection Bottlenecks

Multiple tasks are trying to access the database simultaneously, causing connection pool exhaustion.

**Requirements:**

- Implement connection pooling strategies
- Optimize database queries for performance
- Use appropriate connection timeouts
- Implement retry logic for connection failures

### Scenario 3: Resource Contention

CPU-intensive, memory-intensive, and I/O-intensive tasks are competing for the same resources.

**Requirements:**

- Create separate resource pools for different task types
- Implement intelligent task scheduling based on resource availability
- Monitor resource utilization across pools
- Implement dynamic resource allocation

### Scenario 4: Redundant Computations

The same expensive computations are being performed multiple times across different tasks.

**Requirements:**

- Implement caching for expensive computations
- Create shared data structures for reuse across tasks
- Implement incremental processing for time-series data
- Track cache hit rates and performance improvements

## Implementation Structure

### Main DAG Structure

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
import time
import psutil
import logging

default_args = {
    'owner': 'your-name',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'performance_tuning_pipeline',
    default_args=default_args,
    description='Performance tuning and optimization techniques',
    schedule_interval=timedelta(hours=4),
    catchup=False,
    max_active_runs=2,  # Limit concurrent runs
    tags=['exercise', 'performance', 'optimization']
)

# Your implementation here
```

## Detailed Implementation Requirements

### 1. Memory Optimization

Implement a function that processes large datasets efficiently:

```python
def memory_optimized_processing(dataset_size, chunk_size=10000, **context):
    """
    Process large dataset in memory-efficient chunks

    Args:
        dataset_size (int): Total number of records to process
        chunk_size (int): Size of each processing chunk
    """

    # Your implementation should:
    # - Process data in configurable chunks
    # - Monitor memory usage during processing
    # - Implement proper cleanup after each chunk
    # - Use generator patterns where possible
    # - Log memory statistics

    pass
```

### 2. Connection Pool Management

Implement database connection optimization:

```python
def optimized_database_operations(**context):
    """
    Demonstrate optimized database operations with connection pooling
    """

    # Your implementation should:
    # - Reuse database connections efficiently
    # - Implement connection timeout handling
    # - Use batch operations for multiple queries
    # - Monitor connection pool statistics
    # - Handle connection failures gracefully

    pass
```

### 3. Resource Pool Configuration

Create tasks that use different resource pools:

```python
# CPU-intensive task
cpu_intensive_task = PythonOperator(
    task_id='cpu_intensive_processing',
    python_callable=cpu_intensive_function,
    pool='cpu_pool',  # Use CPU resource pool
    pool_slots=2      # Reserve 2 slots
)

# Memory-intensive task
memory_intensive_task = PythonOperator(
    task_id='memory_intensive_processing',
    python_callable=memory_intensive_function,
    pool='memory_pool',  # Use memory resource pool
    pool_slots=1
)

# I/O-intensive task
io_intensive_task = PythonOperator(
    task_id='io_intensive_processing',
    python_callable=io_intensive_function,
    pool='io_pool',  # Use I/O resource pool
    pool_slots=3
)
```

### 4. Caching Implementation

Implement intelligent caching:

```python
def cached_computation(computation_key, expensive_function, **context):
    """
    Implement caching for expensive computations

    Args:
        computation_key (str): Unique key for the computation
        expensive_function (callable): Function to execute if cache miss
    """

    # Your implementation should:
    # - Check cache for existing results
    # - Execute expensive function on cache miss
    # - Store results in cache for future use
    # - Implement cache expiration logic
    # - Track cache hit/miss statistics

    pass
```

### 5. Performance Monitoring

Implement comprehensive performance monitoring:

```python
def performance_monitor(task_name, **context):
    """
    Monitor and log performance metrics for tasks

    Args:
        task_name (str): Name of the task being monitored
    """

    # Your implementation should:
    # - Monitor CPU usage
    # - Track memory consumption
    # - Measure execution time
    # - Log I/O statistics
    # - Generate performance reports

    pass
```

### 6. Incremental Processing

Implement incremental processing logic:

```python
def incremental_data_processing(**context):
    """
    Process only new/changed data since last run
    """

    # Your implementation should:
    # - Determine last processing timestamp
    # - Identify new/changed records
    # - Process only incremental data
    # - Update processing checkpoints
    # - Calculate time/resource savings

    pass
```

## Performance Metrics to Track

Your implementation should track and report these metrics:

### Memory Metrics

- Peak memory usage per task
- Memory usage patterns over time
- Memory cleanup effectiveness
- Out-of-memory incidents

### Database Metrics

- Connection pool utilization
- Query execution times
- Connection timeout incidents
- Database lock wait times

### Resource Pool Metrics

- Pool utilization rates
- Task queue depths per pool
- Resource contention incidents
- Pool efficiency ratios

### Caching Metrics

- Cache hit/miss ratios
- Cache storage utilization
- Time saved through caching
- Cache invalidation rates

### Overall Performance Metrics

- End-to-end pipeline execution time
- Task success rates
- Resource utilization efficiency
- Cost per processed record

## Testing Your Solution

### Performance Benchmarking

1. **Baseline Measurement**: Run without optimizations and record metrics
2. **Optimization Testing**: Apply each optimization and measure improvements
3. **Load Testing**: Test with varying data volumes and concurrent loads
4. **Resource Constraint Testing**: Test under limited resource conditions

### Monitoring Validation

1. **Metric Accuracy**: Verify that monitoring captures accurate performance data
2. **Alert Functionality**: Test that performance alerts trigger appropriately
3. **Dashboard Updates**: Ensure performance dashboards update in real-time
4. **Historical Tracking**: Verify long-term performance trend tracking

## Success Criteria

- [ ] Memory usage optimized with chunked processing
- [ ] Database connections efficiently managed with pooling
- [ ] Resource pools properly configured and utilized
- [ ] Caching reduces redundant computations significantly
- [ ] Performance monitoring provides comprehensive insights
- [ ] Incremental processing reduces overall processing time
- [ ] All optimizations work together without conflicts
- [ ] Performance improvements are measurable and documented

## Advanced Challenges

### 1. Dynamic Resource Allocation

Implement dynamic resource allocation based on current system load:

```python
def dynamic_resource_allocation(**context):
    """
    Dynamically allocate resources based on current system state
    """
    # Check current system resources
    # Adjust task parallelism accordingly
    # Modify resource pool sizes dynamically
    # Implement load balancing logic
```

### 2. Predictive Performance Optimization

Implement predictive optimization based on historical patterns:

```python
def predictive_optimization(**context):
    """
    Use historical data to predict and prevent performance issues
    """
    # Analyze historical performance patterns
    # Predict resource requirements
    # Proactively adjust configurations
    # Implement early warning systems
```

### 3. Multi-tier Caching Strategy

Implement a sophisticated caching hierarchy:

```python
def multi_tier_caching(**context):
    """
    Implement L1 (memory), L2 (local disk), L3 (distributed) caching
    """
    # Implement memory cache (L1)
    # Implement local disk cache (L2)
    # Implement distributed cache (L3)
    # Implement intelligent cache promotion/demotion
```

## Performance Optimization Checklist

### Pre-optimization Analysis

- [ ] Profile current performance bottlenecks
- [ ] Identify resource utilization patterns
- [ ] Analyze task execution times
- [ ] Review error logs for performance-related issues

### Memory Optimization

- [ ] Implement chunked processing for large datasets
- [ ] Use generator patterns for memory efficiency
- [ ] Implement proper cleanup and garbage collection
- [ ] Monitor memory usage patterns

### Database Optimization

- [ ] Configure connection pooling
- [ ] Optimize query performance
- [ ] Implement batch operations
- [ ] Add appropriate database indexes

### Resource Management

- [ ] Configure resource pools appropriately
- [ ] Balance resource allocation across task types
- [ ] Monitor resource utilization
- [ ] Implement resource contention handling

### Caching Strategy

- [ ] Identify cacheable computations
- [ ] Implement cache invalidation logic
- [ ] Monitor cache performance
- [ ] Optimize cache storage utilization

### Monitoring and Alerting

- [ ] Implement comprehensive performance monitoring
- [ ] Set up performance alerts and thresholds
- [ ] Create performance dashboards
- [ ] Establish performance baselines

## Common Performance Anti-patterns to Avoid

1. **Loading entire datasets into memory** - Use streaming/chunking instead
2. **Creating new database connections for each query** - Use connection pooling
3. **Running all tasks in the default pool** - Use specialized resource pools
4. **Recomputing the same expensive operations** - Implement caching
5. **Ignoring resource constraints** - Monitor and respect system limits
6. **Processing all data every time** - Implement incremental processing
7. **Not monitoring performance** - Implement comprehensive monitoring

## Next Steps

After completing this exercise, you'll be ready to implement enterprise-grade patterns and tackle real-world performance challenges in production Airflow environments.
