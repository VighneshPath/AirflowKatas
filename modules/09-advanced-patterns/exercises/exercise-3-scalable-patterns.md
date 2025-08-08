# Exercise 3: Scalable Workflow Patterns

## Objective

Implement scalable workflow patterns that can handle large-scale data processing efficiently and adapt to varying workloads.

## Background

Your data engineering team needs to process large datasets that vary in size throughout the day. During peak hours, you might have 10x more data than during off-peak times. You need to implement scalable patterns that can:

- Handle variable data volumes efficiently
- Scale processing based on available resources
- Implement fault tolerance and recovery mechanisms
- Optimize resource utilization

## Requirements

Create a DAG called `scalable_data_processing` that implements multiple scalable patterns:

1. **Fan-out/Fan-in Pattern** for parallel processing
2. **Dynamic Batch Processing** based on data volume
3. **Resource-Aware Scaling** based on system capacity
4. **Quality Gates** with automatic rollback capability
5. **Checkpoint System** for fault tolerance

## Pattern Implementations

### 1. Fan-out/Fan-in Pattern

Implement a pattern that:

- Splits large datasets into manageable chunks
- Processes chunks in parallel
- Consolidates results from all parallel tasks
- Handles failures in individual chunks gracefully

### 2. Dynamic Batch Processing

Create a system that:

- Determines optimal batch size based on data volume
- Creates the appropriate number of parallel processing tasks
- Balances load across available resources
- Monitors processing performance

### 3. Resource-Aware Scaling

Implement logic that:

- Checks available system resources (CPU, memory, workers)
- Adjusts processing strategy based on resource availability
- Scales up during low-resource periods
- Scales down during high-resource contention

### 4. Quality Gates with Rollback

Create a pipeline with:

- Quality checks at each processing stage
- Automatic rollback on quality failures
- Configurable quality thresholds
- Notification system for quality issues

### 5. Checkpoint System

Implement checkpointing that:

- Saves processing state at key points
- Enables restart from last successful checkpoint
- Handles partial failures gracefully
- Maintains data consistency

## Detailed Specifications

### Data Volume Simulation

Create functions to simulate varying data volumes:

```python
def get_data_volume(**context):
    """Simulate varying data volumes throughout the day"""
    import random
    from datetime import datetime

    hour = datetime.now().hour

    # Simulate peak hours (9-17) with higher volume
    if 9 <= hour <= 17:
        base_volume = random.randint(8000, 12000)
    else:
        base_volume = random.randint(1000, 3000)

    return {
        'total_records': base_volume,
        'estimated_processing_time': base_volume * 0.01,  # seconds
        'peak_hour': 9 <= hour <= 17
    }
```

### Resource Monitoring

Implement resource checking:

```python
def check_system_resources(**context):
    """Check available system resources"""
    import random

    # Simulate resource availability
    resources = {
        'cpu_usage_percent': random.randint(20, 80),
        'memory_usage_percent': random.randint(30, 70),
        'available_workers': random.randint(4, 12),
        'queue_length': random.randint(0, 20)
    }

    return resources
```

### Scaling Strategy

Implement intelligent scaling:

```python
def determine_scaling_strategy(data_volume, resources, **context):
    """Determine optimal scaling strategy"""

    total_records = data_volume['total_records']
    cpu_usage = resources['cpu_usage_percent']
    available_workers = resources['available_workers']

    # Calculate optimal batch size and parallel tasks
    if cpu_usage < 50 and available_workers > 8:
        # High resource availability - aggressive scaling
        parallel_tasks = min(available_workers, total_records // 500)
        batch_size = max(500, total_records // parallel_tasks)
    elif cpu_usage < 70 and available_workers > 4:
        # Medium resource availability - moderate scaling
        parallel_tasks = min(available_workers // 2, total_records // 1000)
        batch_size = max(1000, total_records // parallel_tasks)
    else:
        # Low resource availability - conservative scaling
        parallel_tasks = min(2, available_workers)
        batch_size = max(2000, total_records // parallel_tasks)

    return {
        'parallel_tasks': max(1, parallel_tasks),
        'batch_size': batch_size,
        'processing_mode': 'aggressive' if cpu_usage < 50 else 'conservative'
    }
```

## Implementation Structure

### Main DAG Structure

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    'owner': 'your-name',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'scalable_data_processing',
    default_args=default_args,
    description='Scalable data processing with multiple patterns',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['exercise', 'scalable', 'patterns']
)

# Your implementation here
```

### TaskGroup Structure

Organize your implementation using TaskGroups:

1. **Resource Assessment Group**

   - Check data volume
   - Monitor system resources
   - Determine scaling strategy

2. **Dynamic Processing Group**

   - Create processing tasks based on scaling strategy
   - Implement fan-out pattern
   - Handle parallel execution

3. **Quality Control Group**

   - Implement quality gates
   - Handle rollback scenarios
   - Validate processing results

4. **Consolidation Group**

   - Implement fan-in pattern
   - Consolidate results
   - Create final output

5. **Checkpoint Management Group**
   - Create processing checkpoints
   - Handle recovery scenarios
   - Clean up temporary data

## Quality Gate Implementation

Implement quality gates with rollback:

```python
def quality_gate_check(stage_name, threshold=0.95, **context):
    """Implement quality gate with configurable threshold"""
    import random

    # Simulate quality score
    quality_score = random.uniform(0.85, 1.0)

    print(f"Quality gate for {stage_name}: {quality_score:.3f} (threshold: {threshold})")

    if quality_score >= threshold:
        return {
            'status': 'passed',
            'score': quality_score,
            'stage': stage_name
        }
    else:
        raise ValueError(f"Quality gate failed for {stage_name}: {quality_score} < {threshold}")

def rollback_stage(stage_name, **context):
    """Rollback processing stage"""
    print(f"Rolling back stage: {stage_name}")

    # Implement rollback logic
    rollback_steps = [
        f"Stopping {stage_name} processing",
        f"Cleaning up {stage_name} temporary data",
        f"Restoring {stage_name} from last checkpoint",
        f"Notifying team about {stage_name} rollback"
    ]

    for step in rollback_steps:
        print(f"Rollback step: {step}")

    return f"rollback_complete_{stage_name}"
```

## Checkpoint System

Implement checkpointing for fault tolerance:

```python
def create_checkpoint(stage_name, data_info, **context):
    """Create checkpoint for processing stage"""
    checkpoint = {
        'stage': stage_name,
        'timestamp': context['ts'],
        'dag_run_id': context['dag_run'].run_id,
        'data_info': data_info,
        'status': 'checkpoint_created'
    }

    print(f"Creating checkpoint for {stage_name}: {checkpoint}")
    return checkpoint

def restore_from_checkpoint(stage_name, **context):
    """Restore processing from checkpoint"""
    print(f"Restoring {stage_name} from checkpoint")

    # Simulate checkpoint restoration
    restored_data = {
        'stage': stage_name,
        'restored_at': context['ts'],
        'status': 'restored'
    }

    return restored_data
```

## Testing Your Solution

1. **Volume Variation Testing**: Test with different simulated data volumes
2. **Resource Constraint Testing**: Test under various resource availability scenarios
3. **Failure Recovery Testing**: Test checkpoint and rollback mechanisms
4. **Quality Gate Testing**: Test with different quality thresholds
5. **Scaling Behavior Testing**: Verify appropriate scaling decisions

## Success Criteria

- [ ] DAG handles varying data volumes appropriately
- [ ] Scaling strategy adapts to resource availability
- [ ] Quality gates function correctly with rollback capability
- [ ] Checkpoint system enables fault recovery
- [ ] Fan-out/fan-in pattern processes data efficiently
- [ ] All patterns work together seamlessly
- [ ] Performance scales with available resources

## Performance Metrics

Track these metrics in your implementation:

```python
def track_performance_metrics(stage_name, start_time, end_time, records_processed, **context):
    """Track performance metrics for monitoring"""

    processing_time = end_time - start_time
    throughput = records_processed / processing_time if processing_time > 0 else 0

    metrics = {
        'stage': stage_name,
        'processing_time_seconds': processing_time,
        'records_processed': records_processed,
        'throughput_records_per_second': throughput,
        'timestamp': context['ts']
    }

    print(f"Performance metrics for {stage_name}: {metrics}")
    return metrics
```

## Advanced Challenges

1. **Adaptive Thresholds**: Implement quality thresholds that adapt based on historical performance
2. **Predictive Scaling**: Use historical data to predict optimal scaling strategies
3. **Multi-Stage Rollback**: Implement cascading rollback across multiple processing stages
4. **Resource Reservation**: Implement resource reservation for critical processing tasks
5. **Cost Optimization**: Add cost considerations to scaling decisions

## Best Practices

1. **Monitor Resource Usage**: Always check resource availability before scaling
2. **Implement Graceful Degradation**: Handle resource constraints gracefully
3. **Use Appropriate Batch Sizes**: Balance between parallelism and overhead
4. **Implement Comprehensive Logging**: Track all scaling decisions and their outcomes
5. **Test Edge Cases**: Test with extreme data volumes and resource constraints

## Common Pitfalls

- Don't create too many parallel tasks (can overwhelm the scheduler)
- Ensure quality gates don't create bottlenecks
- Handle checkpoint cleanup to avoid storage issues
- Don't ignore resource constraints when scaling
- Implement proper error handling for all scaling scenarios

## Next Steps

After completing this exercise, you'll be ready to implement optimization techniques and enterprise-grade patterns for production workflows.
