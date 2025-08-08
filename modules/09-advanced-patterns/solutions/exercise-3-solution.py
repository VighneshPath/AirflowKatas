"""
Solution for Exercise 3: Scalable Workflow Patterns

This solution demonstrates scalable workflow patterns that can handle large-scale
data processing efficiently and adapt to varying workloads.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
import random
import time

default_args = {
    'owner': 'airflow-kata',
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

# Utility functions for data volume and resource simulation


def get_data_volume(**context):
    """Simulate varying data volumes throughout the day"""
    hour = datetime.now().hour

    # Simulate peak hours (9-17) with higher volume
    if 9 <= hour <= 17:
        base_volume = random.randint(8000, 12000)
        peak_multiplier = 1.5
    else:
        base_volume = random.randint(1000, 3000)
        peak_multiplier = 1.0

    total_records = int(base_volume * peak_multiplier)

    volume_info = {
        'total_records': total_records,
        'estimated_processing_time': total_records * 0.01,  # seconds
        'peak_hour': 9 <= hour <= 17,
        'hour': hour,
        'volume_category': 'high' if total_records > 8000 else 'medium' if total_records > 4000 else 'low'
    }

    print(f"📊 Data Volume Assessment:")
    print(f"   Total records: {total_records:,}")
    print(f"   Peak hour: {volume_info['peak_hour']}")
    print(f"   Volume category: {volume_info['volume_category']}")
    print(
        f"   Estimated processing time: {volume_info['estimated_processing_time']:.1f} seconds")

    return volume_info


def check_system_resources(**context):
    """Check available system resources"""
    # Simulate resource availability with some randomness
    resources = {
        'cpu_usage_percent': random.randint(20, 80),
        'memory_usage_percent': random.randint(30, 70),
        'available_workers': random.randint(4, 12),
        'queue_length': random.randint(0, 20),
        'disk_usage_percent': random.randint(40, 85),
        'network_bandwidth_mbps': random.randint(100, 1000)
    }

    # Determine resource availability level
    cpu_available = 100 - resources['cpu_usage_percent']
    memory_available = 100 - resources['memory_usage_percent']

    if cpu_available > 50 and memory_available > 50 and resources['available_workers'] > 8:
        resource_level = 'high'
    elif cpu_available > 30 and memory_available > 30 and resources['available_workers'] > 4:
        resource_level = 'medium'
    else:
        resource_level = 'low'

    resources['resource_level'] = resource_level

    print(f"🖥️ System Resource Assessment:")
    print(f"   CPU usage: {resources['cpu_usage_percent']}%")
    print(f"   Memory usage: {resources['memory_usage_percent']}%")
    print(f"   Available workers: {resources['available_workers']}")
    print(f"   Queue length: {resources['queue_length']}")
    print(f"   Resource level: {resource_level}")

    return resources


def determine_scaling_strategy(**context):
    """Determine optimal scaling strategy based on data volume and resources"""
    # Get data from previous tasks (in real scenario, would use XCom)
    data_volume = get_data_volume()
    resources = check_system_resources()

    total_records = data_volume['total_records']
    resource_level = resources['resource_level']
    available_workers = resources['available_workers']
    cpu_usage = resources['cpu_usage_percent']

    # Calculate optimal scaling parameters
    if resource_level == 'high' and total_records > 8000:
        # Aggressive scaling for high volume + high resources
        parallel_tasks = min(available_workers, max(8, total_records // 1000))
        batch_size = max(500, total_records // parallel_tasks)
        processing_mode = 'aggressive'
        quality_threshold = 0.95
    elif resource_level == 'medium' or (resource_level == 'high' and total_records <= 8000):
        # Moderate scaling
        parallel_tasks = min(available_workers // 2,
                             max(4, total_records // 2000))
        batch_size = max(1000, total_records // parallel_tasks)
        processing_mode = 'moderate'
        quality_threshold = 0.90
    else:
        # Conservative scaling for low resources
        parallel_tasks = min(2, available_workers)
        batch_size = max(2000, total_records // parallel_tasks)
        processing_mode = 'conservative'
        quality_threshold = 0.85

    strategy = {
        'parallel_tasks': max(1, parallel_tasks),
        'batch_size': batch_size,
        'processing_mode': processing_mode,
        'quality_threshold': quality_threshold,
        'enable_checkpoints': total_records > 5000,
        'enable_rollback': processing_mode in ['aggressive', 'moderate']
    }

    print(f"🎯 Scaling Strategy Determined:")
    print(f"   Parallel tasks: {strategy['parallel_tasks']}")
    print(f"   Batch size: {strategy['batch_size']}")
    print(f"   Processing mode: {strategy['processing_mode']}")
    print(f"   Quality threshold: {strategy['quality_threshold']}")
    print(f"   Checkpoints enabled: {strategy['enable_checkpoints']}")

    return strategy

# Fan-out/Fan-in Pattern Implementation


def split_data_for_processing(**context):
    """Split data into chunks for parallel processing (Fan-out)"""
    strategy = determine_scaling_strategy()
    data_volume = get_data_volume()

    total_records = data_volume['total_records']
    parallel_tasks = strategy['parallel_tasks']
    batch_size = strategy['batch_size']

    # Create chunk information
    chunks = []
    for i in range(parallel_tasks):
        start_record = i * batch_size
        end_record = min((i + 1) * batch_size, total_records)

        if start_record < total_records:
            chunk_info = {
                'chunk_id': f'chunk_{i+1}',
                'start_record': start_record,
                'end_record': end_record,
                'record_count': end_record - start_record,
                'processing_mode': strategy['processing_mode']
            }
            chunks.append(chunk_info)

    print(f"📦 Data Split for Fan-out Processing:")
    print(f"   Total chunks created: {len(chunks)}")
    for chunk in chunks:
        print(
            f"   {chunk['chunk_id']}: records {chunk['start_record']}-{chunk['end_record']} ({chunk['record_count']} records)")

    return chunks


def process_data_chunk(chunk_id, start_record, end_record, processing_mode, **context):
    """Process individual data chunk"""
    record_count = end_record - start_record

    print(f"🔄 Processing {chunk_id}:")
    print(
        f"   Records: {start_record} to {end_record} ({record_count} records)")
    print(f"   Mode: {processing_mode}")

    # Simulate processing time based on mode and record count
    if processing_mode == 'aggressive':
        processing_time = record_count * 0.001  # Fast processing
    elif processing_mode == 'moderate':
        processing_time = record_count * 0.002  # Medium processing
    else:
        processing_time = record_count * 0.005  # Slower but safer processing

    # Simulate actual processing
    time.sleep(min(processing_time, 2))  # Cap at 2 seconds for demo

    # Simulate occasional processing issues
    if random.random() < 0.05:  # 5% chance of retry-able error
        print(f"⚠️ Temporary processing issue in {chunk_id} - will retry")
        raise Exception(f"Temporary processing error in {chunk_id}")

    result = {
        'chunk_id': chunk_id,
        'records_processed': record_count,
        'processing_time': processing_time,
        'status': 'completed',
        'quality_score': random.uniform(0.85, 0.99)
    }

    print(f"✅ {chunk_id} processing completed:")
    print(f"   Records processed: {record_count}")
    print(f"   Quality score: {result['quality_score']:.3f}")

    return result


def consolidate_chunk_results(**context):
    """Consolidate results from all chunks (Fan-in)"""
    print("🔄 Consolidating results from all processed chunks...")

    # In real scenario, would pull results from XCom
    # Simulating consolidation process
    total_processed = 0
    total_chunks = 0
    quality_scores = []

    # Simulate getting results from parallel tasks
    for i in range(random.randint(3, 8)):  # Simulate variable number of chunks
        chunk_result = {
            'records_processed': random.randint(500, 2000),
            'quality_score': random.uniform(0.85, 0.99)
        }
        total_processed += chunk_result['records_processed']
        quality_scores.append(chunk_result['quality_score'])
        total_chunks += 1

    average_quality = sum(quality_scores) / \
        len(quality_scores) if quality_scores else 0

    consolidated_result = {
        'total_records_processed': total_processed,
        'total_chunks': total_chunks,
        'average_quality_score': average_quality,
        'consolidation_status': 'completed'
    }

    print(f"📋 Consolidation Results:")
    print(f"   Total records processed: {total_processed:,}")
    print(f"   Total chunks: {total_chunks}")
    print(f"   Average quality score: {average_quality:.3f}")

    return consolidated_result

# Quality Gates with Rollback Implementation


def quality_gate_check(stage_name, threshold=0.95, **context):
    """Implement quality gate with configurable threshold"""
    # Simulate quality assessment
    quality_score = random.uniform(0.80, 1.0)

    print(f"🔍 Quality Gate Check for {stage_name}:")
    print(f"   Quality score: {quality_score:.3f}")
    print(f"   Threshold: {threshold}")

    if quality_score >= threshold:
        print(f"✅ Quality gate PASSED for {stage_name}")
        return {
            'status': 'passed',
            'score': quality_score,
            'stage': stage_name,
            'timestamp': context['ts']
        }
    else:
        print(f"❌ Quality gate FAILED for {stage_name}")
        print(f"   Score {quality_score:.3f} is below threshold {threshold}")
        raise ValueError(
            f"Quality gate failed for {stage_name}: {quality_score:.3f} < {threshold}")


def rollback_stage(stage_name, **context):
    """Rollback processing stage in case of quality failure"""
    print(f"🔄 Initiating rollback for stage: {stage_name}")

    rollback_steps = [
        f"Stopping all {stage_name} processing tasks",
        f"Cleaning up {stage_name} temporary data",
        f"Restoring {stage_name} from last checkpoint",
        f"Resetting {stage_name} processing state",
        f"Notifying operations team about {stage_name} rollback"
    ]

    for i, step in enumerate(rollback_steps, 1):
        print(f"   Step {i}: {step}")
        time.sleep(0.1)  # Simulate rollback time

    rollback_result = {
        'stage': stage_name,
        'rollback_completed': True,
        'timestamp': context['ts'],
        'next_action': 'manual_review_required'
    }

    print(f"✅ Rollback completed for {stage_name}")
    return rollback_result

# Checkpoint System Implementation


def create_checkpoint(stage_name, **context):
    """Create checkpoint for processing stage"""
    checkpoint_data = {
        'stage': stage_name,
        'timestamp': context['ts'],
        'dag_run_id': context['dag_run'].run_id,
        'task_instance_id': context['task_instance'].task_id,
        'execution_date': context['execution_date'].isoformat(),
        'checkpoint_id': f"{stage_name}_{context['ts']}"
    }

    print(f"💾 Creating checkpoint for {stage_name}:")
    print(f"   Checkpoint ID: {checkpoint_data['checkpoint_id']}")
    print(f"   Timestamp: {checkpoint_data['timestamp']}")

    # Simulate checkpoint creation
    time.sleep(0.2)

    print(f"✅ Checkpoint created successfully for {stage_name}")
    return checkpoint_data


def restore_from_checkpoint(stage_name, **context):
    """Restore processing from checkpoint"""
    print(f"🔄 Restoring {stage_name} from checkpoint...")

    # Simulate checkpoint restoration
    restoration_steps = [
        f"Locating latest checkpoint for {stage_name}",
        f"Validating checkpoint integrity",
        f"Restoring {stage_name} processing state",
        f"Resuming {stage_name} from checkpoint"
    ]

    for i, step in enumerate(restoration_steps, 1):
        print(f"   Step {i}: {step}")
        time.sleep(0.1)

    restored_data = {
        'stage': stage_name,
        'restored_at': context['ts'],
        'status': 'restored',
        'resume_point': f"{stage_name}_checkpoint_resume"
    }

    print(f"✅ Successfully restored {stage_name} from checkpoint")
    return restored_data

# Performance Tracking


def track_performance_metrics(stage_name, **context):
    """Track performance metrics for monitoring"""
    start_time = time.time()

    # Simulate some processing
    processing_time = random.uniform(0.5, 2.0)
    time.sleep(processing_time)

    end_time = time.time()
    actual_processing_time = end_time - start_time

    # Simulate metrics
    records_processed = random.randint(1000, 5000)
    throughput = records_processed / \
        actual_processing_time if actual_processing_time > 0 else 0

    metrics = {
        'stage': stage_name,
        'processing_time_seconds': actual_processing_time,
        'records_processed': records_processed,
        'throughput_records_per_second': throughput,
        'timestamp': context['ts'],
        'memory_usage_mb': random.randint(100, 500),
        'cpu_usage_percent': random.randint(20, 80)
    }

    print(f"📈 Performance Metrics for {stage_name}:")
    print(f"   Processing time: {actual_processing_time:.2f} seconds")
    print(f"   Records processed: {records_processed:,}")
    print(f"   Throughput: {throughput:.1f} records/second")
    print(f"   Memory usage: {metrics['memory_usage_mb']} MB")
    print(f"   CPU usage: {metrics['cpu_usage_percent']}%")

    return metrics


# Build the DAG
with dag:
    start = DummyOperator(task_id='start')

    # Resource Assessment Group
    with TaskGroup("resource_assessment") as assessment_group:
        check_data_volume = PythonOperator(
            task_id='check_data_volume',
            python_callable=get_data_volume
        )

        check_resources = PythonOperator(
            task_id='check_system_resources',
            python_callable=check_system_resources
        )

        determine_strategy = PythonOperator(
            task_id='determine_scaling_strategy',
            python_callable=determine_scaling_strategy
        )

        [check_data_volume, check_resources] >> determine_strategy

    # Dynamic Processing Group (Fan-out/Fan-in)
    with TaskGroup("dynamic_processing") as processing_group:
        # Create checkpoint before processing
        create_processing_checkpoint = PythonOperator(
            task_id='create_processing_checkpoint',
            python_callable=create_checkpoint,
            op_kwargs={'stage_name': 'processing'}
        )

        # Split data for parallel processing
        split_data = PythonOperator(
            task_id='split_data_for_processing',
            python_callable=split_data_for_processing
        )

        # Create multiple parallel processing tasks (simulating dynamic creation)
        parallel_tasks = []
        for i in range(6):  # Create 6 potential parallel tasks
            task = PythonOperator(
                task_id=f'process_chunk_{i+1}',
                python_callable=process_data_chunk,
                op_kwargs={
                    'chunk_id': f'chunk_{i+1}',
                    'start_record': i * 1000,
                    'end_record': (i + 1) * 1000,
                    'processing_mode': 'moderate'
                },
                retries=2
            )
            parallel_tasks.append(task)

        # Consolidate results
        consolidate_results = PythonOperator(
            task_id='consolidate_chunk_results',
            python_callable=consolidate_chunk_results
        )

        # Track processing performance
        track_processing_metrics = PythonOperator(
            task_id='track_processing_metrics',
            python_callable=track_performance_metrics,
            op_kwargs={'stage_name': 'processing'}
        )

        # Define processing group dependencies
        create_processing_checkpoint >> split_data >> parallel_tasks >> consolidate_results >> track_processing_metrics

    # Quality Control Group
    with TaskGroup("quality_control") as quality_group:
        # Primary quality gate
        primary_quality_gate = PythonOperator(
            task_id='primary_quality_check',
            python_callable=quality_gate_check,
            op_kwargs={'stage_name': 'primary_processing', 'threshold': 0.90}
        )

        # Secondary quality gate with higher threshold
        secondary_quality_gate = PythonOperator(
            task_id='secondary_quality_check',
            python_callable=quality_gate_check,
            op_kwargs={'stage_name': 'secondary_processing', 'threshold': 0.95}
        )

        # Rollback task (triggered on quality failure)
        rollback_processing = PythonOperator(
            task_id='rollback_on_quality_failure',
            python_callable=rollback_stage,
            op_kwargs={'stage_name': 'processing'},
            trigger_rule='one_failed'
        )

        # Quality validation success
        quality_validation_success = PythonOperator(
            task_id='quality_validation_success',
            python_callable=lambda: print(
                "✅ All quality gates passed successfully"),
            trigger_rule='all_success'
        )

        # Define quality control dependencies
        primary_quality_gate >> secondary_quality_gate >> quality_validation_success
        [primary_quality_gate, secondary_quality_gate] >> rollback_processing

    # Consolidation Group
    with TaskGroup("consolidation") as consolidation_group:
        # Create checkpoint before final consolidation
        create_consolidation_checkpoint = PythonOperator(
            task_id='create_consolidation_checkpoint',
            python_callable=create_checkpoint,
            op_kwargs={'stage_name': 'consolidation'}
        )

        # Final data consolidation
        final_consolidation = PythonOperator(
            task_id='final_data_consolidation',
            python_callable=lambda: print(
                "🔄 Performing final data consolidation")
        )

        # Generate final output
        generate_output = PythonOperator(
            task_id='generate_final_output',
            python_callable=lambda: print(
                "📤 Generating final processed output")
        )

        # Track consolidation performance
        track_consolidation_metrics = PythonOperator(
            task_id='track_consolidation_metrics',
            python_callable=track_performance_metrics,
            op_kwargs={'stage_name': 'consolidation'}
        )

        create_consolidation_checkpoint >> final_consolidation >> generate_output >> track_consolidation_metrics

    # Checkpoint Management Group
    with TaskGroup("checkpoint_management") as checkpoint_group:
        # Validate all checkpoints
        validate_checkpoints = PythonOperator(
            task_id='validate_all_checkpoints',
            python_callable=lambda: print(
                "✅ Validating all processing checkpoints")
        )

        # Cleanup temporary data
        cleanup_temp_data = PythonOperator(
            task_id='cleanup_temporary_data',
            python_callable=lambda: print(
                "🧹 Cleaning up temporary processing data")
        )

        # Archive successful checkpoints
        archive_checkpoints = PythonOperator(
            task_id='archive_successful_checkpoints',
            python_callable=lambda: print("📦 Archiving successful checkpoints")
        )

        validate_checkpoints >> cleanup_temp_data >> archive_checkpoints

    # Final success/failure handling
    pipeline_success = PythonOperator(
        task_id='pipeline_success_notification',
        python_callable=lambda: print(
            "🎉 Scalable pipeline completed successfully!"),
        trigger_rule='all_success'
    )

    pipeline_failure = PythonOperator(
        task_id='pipeline_failure_notification',
        python_callable=lambda: print(
            "🚨 Scalable pipeline failed - check logs for details"),
        trigger_rule='one_failed'
    )

    end = DummyOperator(
        task_id='end',
        trigger_rule='none_failed_min_one_success'
    )

    # Define overall workflow dependencies
    start >> assessment_group >> processing_group >> quality_group >> consolidation_group >> checkpoint_group

    # Connect success and failure paths
    checkpoint_group >> pipeline_success >> end
    [assessment_group, processing_group, quality_group,
        consolidation_group, checkpoint_group] >> pipeline_failure

# Additional example: Resource-aware batch processing DAG
dag_batch = DAG(
    'resource_aware_batch_processing',
    default_args=default_args,
    description='Resource-aware batch processing with adaptive scaling',
    schedule_interval=timedelta(hours=2),
    catchup=False,
    tags=['scalable', 'batch', 'resource-aware']
)


def adaptive_batch_processing(**context):
    """Implement adaptive batch processing based on current resources"""
    resources = check_system_resources()
    data_volume = get_data_volume()

    # Adapt batch size based on resources
    if resources['resource_level'] == 'high':
        batch_size = min(2000, data_volume['total_records'] // 4)
        parallel_batches = min(8, resources['available_workers'])
    elif resources['resource_level'] == 'medium':
        batch_size = min(1500, data_volume['total_records'] // 3)
        parallel_batches = min(4, resources['available_workers'])
    else:
        batch_size = min(1000, data_volume['total_records'] // 2)
        parallel_batches = min(2, resources['available_workers'])

    print(f"🔄 Adaptive Batch Processing:")
    print(f"   Batch size: {batch_size}")
    print(f"   Parallel batches: {parallel_batches}")
    print(f"   Resource level: {resources['resource_level']}")

    # Simulate batch processing
    for batch_num in range(parallel_batches):
        start_record = batch_num * batch_size
        end_record = min((batch_num + 1) * batch_size,
                         data_volume['total_records'])

        if start_record < data_volume['total_records']:
            print(
                f"   Processing batch {batch_num + 1}: records {start_record}-{end_record}")
            time.sleep(0.1)  # Simulate processing

    return {
        'batches_processed': parallel_batches,
        'total_records': data_volume['total_records'],
        'batch_size': batch_size,
        'resource_level': resources['resource_level']
    }


with dag_batch:
    start_batch = DummyOperator(task_id='start')

    adaptive_processing = PythonOperator(
        task_id='adaptive_batch_processing',
        python_callable=adaptive_batch_processing
    )

    end_batch = DummyOperator(task_id='end')

    start_batch >> adaptive_processing >> end_batch
