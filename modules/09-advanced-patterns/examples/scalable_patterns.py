"""
Scalable Workflow Patterns for Airflow Advanced Patterns

This module demonstrates patterns for building scalable, maintainable,
and efficient workflows that can handle enterprise-level requirements.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
import json

# Default arguments
default_args = {
    'owner': 'airflow-kata',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Example 1: Fan-out/Fan-in Pattern for Parallel Processing
dag1 = DAG(
    'scalable_fanout_fanin_pattern',
    default_args=default_args,
    description='Scalable fan-out/fan-in pattern for parallel data processing',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['scalable', 'parallel', 'fan-out-fan-in']
)


def split_data(**context):
    """Split data into chunks for parallel processing"""
    # Simulate splitting data into chunks
    chunks = [f"chunk_{i}" for i in range(1, 6)]  # 5 chunks
    print(f"Split data into {len(chunks)} chunks: {chunks}")
    return chunks


def process_chunk(chunk_id, **context):
    """Process individual data chunk"""
    print(f"Processing chunk: {chunk_id}")
    # Simulate processing time and return result
    return f"processed_{chunk_id}"


def consolidate_results(**context):
    """Consolidate results from all chunks"""
    # In real scenario, would pull results from XCom
    print("Consolidating results from all processed chunks")
    return "consolidated_results"


with dag1:
    start = DummyOperator(task_id='start')

    # Split data into chunks
    split_task = PythonOperator(
        task_id='split_data',
        python_callable=split_data
    )

    # Create parallel processing tasks
    process_tasks = []
    for i in range(1, 6):  # 5 parallel processing tasks
        task = PythonOperator(
            task_id=f'process_chunk_{i}',
            python_callable=process_chunk,
            op_kwargs={'chunk_id': f'chunk_{i}'}
        )
        process_tasks.append(task)

    # Consolidate results
    consolidate = PythonOperator(
        task_id='consolidate_results',
        python_callable=consolidate_results
    )

    end = DummyOperator(task_id='end')

    # Define dependencies: fan-out then fan-in
    start >> split_task >> process_tasks >> consolidate >> end

# Example 2: Batch Processing Pattern with Dynamic Scaling
dag2 = DAG(
    'scalable_batch_processing_pattern',
    default_args=default_args,
    description='Scalable batch processing with dynamic task creation',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['scalable', 'batch', 'dynamic']
)


def get_batch_config(**context):
    """Get batch processing configuration"""
    # In real scenario, this might come from a database or API
    config = {
        'batch_size': 1000,
        'total_records': 10000,
        'processing_type': 'standard'
    }
    print(f"Batch configuration: {config}")
    return config


def create_batch_tasks(dag, config):
    """Dynamically create batch processing tasks"""
    batch_size = config.get('batch_size', 1000)
    total_records = config.get('total_records', 10000)
    num_batches = (total_records + batch_size -
                   1) // batch_size  # Ceiling division

    batch_tasks = []
    for i in range(num_batches):
        start_record = i * batch_size
        end_record = min((i + 1) * batch_size, total_records)

        task = PythonOperator(
            task_id=f'process_batch_{i+1}',
            python_callable=lambda start=start_record, end=end_record, **ctx:
                print(f"Processing records {start} to {end}"),
            dag=dag
        )
        batch_tasks.append(task)

    return batch_tasks


with dag2:
    start = DummyOperator(task_id='start')

    # Get configuration
    config_task = PythonOperator(
        task_id='get_batch_config',
        python_callable=get_batch_config
    )

    # Pre-processing setup
    setup_task = BashOperator(
        task_id='setup_processing',
        bash_command='echo "Setting up batch processing environment"'
    )

    # Create batch processing tasks (in real scenario, this would be dynamic)
    batch_tasks = []
    for i in range(10):  # 10 batch tasks for example
        task = PythonOperator(
            task_id=f'process_batch_{i+1}',
            python_callable=lambda batch_num=i+1, **ctx:
                print(f"Processing batch {batch_num}")
        )
        batch_tasks.append(task)

    # Post-processing validation
    validate_task = PythonOperator(
        task_id='validate_results',
        python_callable=lambda: print("Validating batch processing results")
    )

    # Cleanup
    cleanup_task = BashOperator(
        task_id='cleanup',
        bash_command='echo "Cleaning up batch processing resources"'
    )

    end = DummyOperator(task_id='end')

    start >> config_task >> setup_task >> batch_tasks >> validate_task >> cleanup_task >> end

# Example 3: Pipeline Pattern with Checkpoints
dag3 = DAG(
    'scalable_pipeline_checkpoints',
    default_args=default_args,
    description='Scalable pipeline with checkpoints for fault tolerance',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['scalable', 'pipeline', 'checkpoints']
)


def create_checkpoint(stage_name, **context):
    """Create checkpoint for pipeline stage"""
    checkpoint_data = {
        'stage': stage_name,
        'timestamp': context['ts'],
        'dag_run_id': context['dag_run'].run_id
    }
    print(f"Creating checkpoint for stage: {stage_name}")
    print(f"Checkpoint data: {checkpoint_data}")
    return checkpoint_data


def validate_checkpoint(stage_name, **context):
    """Validate checkpoint exists and is valid"""
    print(f"Validating checkpoint for stage: {stage_name}")
    # In real scenario, would check external storage
    return True


def process_stage(stage_name, **context):
    """Process pipeline stage"""
    print(f"Processing pipeline stage: {stage_name}")
    return f"stage_{stage_name}_complete"


with dag3:
    start = DummyOperator(task_id='start')

    # Define pipeline stages
    stages = ['ingestion', 'validation',
              'transformation', 'enrichment', 'loading']

    previous_task = start

    for stage in stages:
        # Create TaskGroup for each stage
        with TaskGroup(f"{stage}_stage") as stage_group:
            # Check if checkpoint exists (for restart capability)
            check_checkpoint = PythonOperator(
                task_id=f'check_{stage}_checkpoint',
                python_callable=validate_checkpoint,
                op_kwargs={'stage_name': stage}
            )

            # Process the stage
            process_task = PythonOperator(
                task_id=f'process_{stage}',
                python_callable=process_stage,
                op_kwargs={'stage_name': stage}
            )

            # Create checkpoint after successful processing
            create_checkpoint_task = PythonOperator(
                task_id=f'create_{stage}_checkpoint',
                python_callable=create_checkpoint,
                op_kwargs={'stage_name': stage}
            )

            # Validate stage completion
            validate_stage = PythonOperator(
                task_id=f'validate_{stage}',
                python_callable=lambda stage=stage, **ctx:
                    print(f"Validating completion of {stage} stage")
            )

            # Define stage internal dependencies
            check_checkpoint >> process_task >> create_checkpoint_task >> validate_stage

        # Connect stages
        previous_task >> stage_group
        previous_task = stage_group

    end = DummyOperator(task_id='end')
    previous_task >> end

# Example 4: Resource-Aware Scaling Pattern
dag4 = DAG(
    'scalable_resource_aware_pattern',
    default_args=default_args,
    description='Resource-aware scaling pattern for optimal resource utilization',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['scalable', 'resource-aware', 'optimization']
)


def check_system_resources(**context):
    """Check available system resources"""
    # Simulate resource checking
    resources = {
        'cpu_usage': 45,  # percentage
        'memory_usage': 60,  # percentage
        'available_workers': 8,
        'queue_length': 12
    }
    print(f"System resources: {resources}")
    return resources


def determine_scaling_strategy(**context):
    """Determine optimal scaling strategy based on resources"""
    # In real scenario, would use actual resource data
    strategy = {
        'parallel_tasks': 4,
        'batch_size': 500,
        'processing_mode': 'standard'
    }
    print(f"Scaling strategy: {strategy}")
    return strategy


def create_resource_optimized_tasks(strategy):
    """Create tasks optimized for current resource availability"""
    parallel_tasks = strategy.get('parallel_tasks', 2)
    tasks = []

    for i in range(parallel_tasks):
        task = PythonOperator(
            task_id=f'optimized_task_{i+1}',
            python_callable=lambda task_id=i+1, **ctx:
                print(f"Executing optimized task {task_id}")
        )
        tasks.append(task)

    return tasks


with dag4:
    start = DummyOperator(task_id='start')

    # Resource monitoring
    with TaskGroup("resource_monitoring") as monitoring_group:
        check_resources = PythonOperator(
            task_id='check_resources',
            python_callable=check_system_resources
        )

        determine_strategy = PythonOperator(
            task_id='determine_strategy',
            python_callable=determine_scaling_strategy
        )

        check_resources >> determine_strategy

    # Adaptive processing based on resources
    with TaskGroup("adaptive_processing") as processing_group:
        # Light processing tasks (always run)
        light_tasks = []
        for i in range(2):
            task = PythonOperator(
                task_id=f'light_task_{i+1}',
                python_callable=lambda task_id=i+1, **ctx:
                    print(f"Executing light task {task_id}")
            )
            light_tasks.append(task)

        # Heavy processing tasks (resource-dependent)
        heavy_tasks = []
        for i in range(4):
            task = PythonOperator(
                task_id=f'heavy_task_{i+1}',
                python_callable=lambda task_id=i+1, **ctx:
                    print(f"Executing heavy task {task_id}"),
                # In real scenario, would use resource-based trigger rules
                trigger_rule='all_success'
            )
            heavy_tasks.append(task)

        # Parallel execution of light tasks, conditional heavy tasks
        light_tasks
        heavy_tasks

    # Resource cleanup
    cleanup = PythonOperator(
        task_id='cleanup_resources',
        python_callable=lambda: print("Cleaning up allocated resources")
    )

    end = DummyOperator(task_id='end')

    start >> monitoring_group >> processing_group >> cleanup >> end

# Example 5: Multi-Stage Pipeline with Quality Gates
dag5 = DAG(
    'scalable_quality_gates_pattern',
    default_args=default_args,
    description='Scalable pipeline with quality gates and automated rollback',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['scalable', 'quality-gates', 'rollback']
)


def quality_check(stage_name, threshold=0.95, **context):
    """Perform quality check with configurable threshold"""
    # Simulate quality score
    import random
    quality_score = random.uniform(0.8, 1.0)

    print(
        f"Quality check for {stage_name}: {quality_score:.3f} (threshold: {threshold})")

    if quality_score >= threshold:
        print(f"✓ Quality check passed for {stage_name}")
        return {'status': 'passed', 'score': quality_score}
    else:
        print(f"✗ Quality check failed for {stage_name}")
        raise ValueError(
            f"Quality check failed: {quality_score} < {threshold}")


def rollback_stage(stage_name, **context):
    """Rollback stage in case of quality failure"""
    print(f"Rolling back stage: {stage_name}")
    return f"rollback_complete_{stage_name}"


def process_with_quality_gate(stage_name, quality_threshold=0.95):
    """Create a processing stage with quality gate"""

    with TaskGroup(f"{stage_name}_with_quality") as stage_group:
        # Main processing
        process_task = PythonOperator(
            task_id=f'process_{stage_name}',
            python_callable=lambda stage=stage_name, **ctx:
                print(f"Processing {stage} stage")
        )

        # Quality gate
        quality_gate = PythonOperator(
            task_id=f'quality_check_{stage_name}',
            python_callable=quality_check,
            op_kwargs={'stage_name': stage_name,
                       'threshold': quality_threshold}
        )

        # Success path
        success_task = PythonOperator(
            task_id=f'mark_{stage_name}_success',
            python_callable=lambda stage=stage_name, **ctx:
                print(f"Stage {stage} completed successfully")
        )

        # Failure path (rollback)
        rollback_task = PythonOperator(
            task_id=f'rollback_{stage_name}',
            python_callable=rollback_stage,
            op_kwargs={'stage_name': stage_name},
            trigger_rule='one_failed'
        )

        # Define internal dependencies
        process_task >> quality_gate >> success_task
        quality_gate >> rollback_task

    return stage_group


with dag5:
    start = DummyOperator(task_id='start')

    # Create pipeline stages with quality gates
    ingestion_stage = process_with_quality_gate('ingestion', 0.98)
    validation_stage = process_with_quality_gate('validation', 0.95)
    transformation_stage = process_with_quality_gate('transformation', 0.90)
    loading_stage = process_with_quality_gate('loading', 0.99)

    # Final validation
    final_validation = PythonOperator(
        task_id='final_pipeline_validation',
        python_callable=lambda: print("Performing final pipeline validation")
    )

    # Success notification
    success_notification = BashOperator(
        task_id='send_success_notification',
        bash_command='echo "Pipeline completed successfully with all quality gates passed"'
    )

    # Failure notification
    failure_notification = BashOperator(
        task_id='send_failure_notification',
        bash_command='echo "Pipeline failed - check quality gate failures"',
        trigger_rule='one_failed'
    )

    end = DummyOperator(
        task_id='end', trigger_rule='none_failed_min_one_success')

    # Define pipeline flow
    start >> ingestion_stage >> validation_stage >> transformation_stage >> loading_stage
    loading_stage >> final_validation >> success_notification >> end

    # Connect failure paths
    [ingestion_stage, validation_stage, transformation_stage,
        loading_stage] >> failure_notification

# Example 6: Event-Driven Scalable Pattern
dag6 = DAG(
    'scalable_event_driven_pattern',
    default_args=default_args,
    description='Event-driven scalable pattern for reactive processing',
    schedule_interval=None,  # Triggered by events
    catchup=False,
    tags=['scalable', 'event-driven', 'reactive']
)


def process_event_batch(event_type, batch_size=100, **context):
    """Process batch of events of specific type"""
    print(f"Processing batch of {batch_size} {event_type} events")
    return f"processed_{batch_size}_{event_type}_events"


def route_events(**context):
    """Route events to appropriate processing tasks"""
    # Simulate event routing logic
    event_distribution = {
        'user_events': 150,
        'order_events': 75,
        'product_events': 50,
        'system_events': 25
    }
    print(f"Event distribution: {event_distribution}")
    return event_distribution


with dag6:
    start = DummyOperator(task_id='start')

    # Event routing
    route_task = PythonOperator(
        task_id='route_events',
        python_callable=route_events
    )

    # Dynamic event processing based on volume
    event_types = ['user_events', 'order_events',
                   'product_events', 'system_events']
    processing_groups = []

    for event_type in event_types:
        with TaskGroup(f"process_{event_type}") as event_group:
            # Small batch processing
            small_batch = PythonOperator(
                task_id=f'small_batch_{event_type}',
                python_callable=process_event_batch,
                op_kwargs={'event_type': event_type, 'batch_size': 50}
            )

            # Large batch processing (conditional)
            large_batch = PythonOperator(
                task_id=f'large_batch_{event_type}',
                python_callable=process_event_batch,
                op_kwargs={'event_type': event_type, 'batch_size': 200}
            )

            # Validation
            validate_processing = PythonOperator(
                task_id=f'validate_{event_type}',
                python_callable=lambda event=event_type, **ctx:
                    print(f"Validating {event} processing")
            )

            [small_batch, large_batch] >> validate_processing

        processing_groups.append(event_group)

    # Consolidation
    consolidate = PythonOperator(
        task_id='consolidate_event_processing',
        python_callable=lambda: print(
            "Consolidating all event processing results")
    )

    end = DummyOperator(task_id='end')

    start >> route_task >> processing_groups >> consolidate >> end
