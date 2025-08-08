"""
Advanced Branching Patterns for Airflow

This module demonstrates complex branching scenarios including:
- Nested branching with multiple decision points
- Combining branching with XComs for data-driven workflows
- Error handling and recovery patterns in branching
- Dynamic task generation based on branching decisions
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable

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

# =============================================================================
# Example 1: Nested Branching with Multiple Decision Points
# =============================================================================


def analyze_data_source(**context):
    """First level analysis - determine data source type."""
    import random

    sources = [
        {'type': 'database', 'size_mb': random.randint(
            10, 1000), 'quality': random.uniform(0.7, 0.95)},
        {'type': 'api', 'size_mb': random.randint(
            1, 100), 'quality': random.uniform(0.8, 0.99)},
        {'type': 'file', 'size_mb': random.randint(
            50, 2000), 'quality': random.uniform(0.6, 0.9)},
        {'type': 'stream', 'size_mb': random.randint(
            100, 5000), 'quality': random.uniform(0.75, 0.95)}
    ]

    data_info = random.choice(sources)
    print(f"Data source analysis: {data_info}")

    return data_info


def primary_data_branch(**context):
    """First level branching - route by data source type."""
    data_info = context['task_instance'].xcom_pull(
        task_ids='analyze_data_source')
    data_type = data_info.get('type')

    print(f"Primary branch decision: routing {data_type} data")

    # Route to appropriate secondary branching
    return f'{data_type}_secondary_branch'


def database_secondary_branch(**context):
    """Secondary branching for database sources."""
    data_info = context['task_instance'].xcom_pull(
        task_ids='analyze_data_source')
    size_mb = data_info.get('size_mb', 0)
    quality = data_info.get('quality', 0)

    print(f"Database secondary branch: {size_mb}MB, quality: {quality:.2f}")

    if quality < 0.8:
        return 'database_quality_improvement'
    elif size_mb > 500:
        return 'database_large_processing'
    else:
        return 'database_standard_processing'


def api_secondary_branch(**context):
    """Secondary branching for API sources."""
    data_info = context['task_instance'].xcom_pull(
        task_ids='analyze_data_source')
    quality = data_info.get('quality', 0)

    print(f"API secondary branch: quality: {quality:.2f}")

    if quality > 0.95:
        return 'api_premium_processing'
    else:
        return 'api_standard_processing'


def file_secondary_branch(**context):
    """Secondary branching for file sources."""
    data_info = context['task_instance'].xcom_pull(
        task_ids='analyze_data_source')
    size_mb = data_info.get('size_mb', 0)

    print(f"File secondary branch: {size_mb}MB")

    if size_mb > 1000:
        return 'file_batch_processing'
    else:
        return 'file_streaming_processing'


def stream_secondary_branch(**context):
    """Secondary branching for stream sources."""
    data_info = context['task_instance'].xcom_pull(
        task_ids='analyze_data_source')
    size_mb = data_info.get('size_mb', 0)

    print(f"Stream secondary branch: {size_mb}MB")

    if size_mb > 2000:
        return 'stream_distributed_processing'
    else:
        return 'stream_single_node_processing'

# Processing functions for each path


def database_quality_improvement(**context):
    print("Database quality improvement processing")
    return {"status": "completed", "method": "quality_improvement"}


def database_large_processing(**context):
    print("Database large dataset processing")
    return {"status": "completed", "method": "large_processing"}


def database_standard_processing(**context):
    print("Database standard processing")
    return {"status": "completed", "method": "standard"}


def api_premium_processing(**context):
    print("API premium processing")
    return {"status": "completed", "method": "premium"}


def api_standard_processing(**context):
    print("API standard processing")
    return {"status": "completed", "method": "standard"}


def file_batch_processing(**context):
    print("File batch processing")
    return {"status": "completed", "method": "batch"}


def file_streaming_processing(**context):
    print("File streaming processing")
    return {"status": "completed", "method": "streaming"}


def stream_distributed_processing(**context):
    print("Stream distributed processing")
    return {"status": "completed", "method": "distributed"}


def stream_single_node_processing(**context):
    print("Stream single node processing")
    return {"status": "completed", "method": "single_node"}


# DAG 1: Nested branching
dag1 = DAG(
    'advanced_1_nested_branching',
    default_args=default_args,
    description='Nested branching with multiple decision points',
    schedule_interval=timedelta(hours=6),
    catchup=False,
    tags=['advanced', 'branching', 'nested']
)

# Tasks for nested branching
start_1 = DummyOperator(task_id='start', dag=dag1)

analyze_source = PythonOperator(
    task_id='analyze_data_source',
    python_callable=analyze_data_source,
    dag=dag1
)

primary_branch = BranchPythonOperator(
    task_id='primary_data_branch',
    python_callable=primary_data_branch,
    dag=dag1
)

# Secondary branching tasks
db_branch = BranchPythonOperator(
    task_id='database_secondary_branch',
    python_callable=database_secondary_branch,
    dag=dag1
)

api_branch = BranchPythonOperator(
    task_id='api_secondary_branch',
    python_callable=api_secondary_branch,
    dag=dag1
)

file_branch = BranchPythonOperator(
    task_id='file_secondary_branch',
    python_callable=file_secondary_branch,
    dag=dag1
)

stream_branch = BranchPythonOperator(
    task_id='stream_secondary_branch',
    python_callable=stream_secondary_branch,
    dag=dag1
)

# Processing tasks
db_quality = PythonOperator(task_id='database_quality_improvement',
                            python_callable=database_quality_improvement, dag=dag1)
db_large = PythonOperator(task_id='database_large_processing',
                          python_callable=database_large_processing, dag=dag1)
db_standard = PythonOperator(task_id='database_standard_processing',
                             python_callable=database_standard_processing, dag=dag1)

api_premium = PythonOperator(
    task_id='api_premium_processing', python_callable=api_premium_processing, dag=dag1)
api_standard = PythonOperator(
    task_id='api_standard_processing', python_callable=api_standard_processing, dag=dag1)

file_batch = PythonOperator(
    task_id='file_batch_processing', python_callable=file_batch_processing, dag=dag1)
file_streaming = PythonOperator(
    task_id='file_streaming_processing', python_callable=file_streaming_processing, dag=dag1)

stream_distributed = PythonOperator(
    task_id='stream_distributed_processing', python_callable=stream_distributed_processing, dag=dag1)
stream_single = PythonOperator(task_id='stream_single_node_processing',
                               python_callable=stream_single_node_processing, dag=dag1)

join_1 = DummyOperator(
    task_id='join', trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, dag=dag1)
end_1 = DummyOperator(task_id='end', dag=dag1)

# Dependencies for nested branching
start_1 >> analyze_source >> primary_branch
primary_branch >> [db_branch, api_branch, file_branch, stream_branch]

db_branch >> [db_quality, db_large, db_standard]
api_branch >> [api_premium, api_standard]
file_branch >> [file_batch, file_streaming]
stream_branch >> [stream_distributed, stream_single]

[db_quality, db_large, db_standard, api_premium, api_standard,
 file_batch, file_streaming, stream_distributed, stream_single] >> join_1 >> end_1

# =============================================================================
# Example 2: XCom-Driven Complex Branching
# =============================================================================


def collect_processing_metrics(**context):
    """Collect various metrics that will drive branching decisions."""
    import random

    metrics = {
        'cpu_usage': random.uniform(0.1, 0.9),
        'memory_usage': random.uniform(0.2, 0.8),
        'disk_space': random.uniform(0.3, 0.95),
        'network_latency': random.uniform(10, 200),  # ms
        'error_rate': random.uniform(0.0, 0.1),
        'data_freshness': random.randint(1, 24),  # hours
        'priority_level': random.choice(['low', 'medium', 'high', 'critical'])
    }

    print(f"Collected metrics: {metrics}")
    return metrics


def resource_availability_check(**context):
    """Check resource availability for processing decisions."""
    import random

    resources = {
        'worker_nodes': random.randint(1, 10),
        'gpu_available': random.choice([True, False]),
        'storage_gb': random.randint(100, 1000),
        'processing_queue_length': random.randint(0, 50)
    }

    print(f"Resource availability: {resources}")
    return resources


def intelligent_processing_branch(**context):
    """
    Complex branching logic based on multiple XCom inputs.
    Demonstrates sophisticated decision-making in workflows.
    """
    # Pull data from multiple previous tasks
    metrics = context['task_instance'].xcom_pull(task_ids='collect_metrics')
    resources = context['task_instance'].xcom_pull(task_ids='check_resources')

    # Complex decision logic
    cpu_usage = metrics.get('cpu_usage', 0)
    memory_usage = metrics.get('memory_usage', 0)
    priority = metrics.get('priority_level', 'low')
    error_rate = metrics.get('error_rate', 0)
    worker_nodes = resources.get('worker_nodes', 1)
    gpu_available = resources.get('gpu_available', False)

    print(
        f"Decision factors: CPU={cpu_usage:.2f}, Memory={memory_usage:.2f}, Priority={priority}")
    print(f"Resources: Workers={worker_nodes}, GPU={gpu_available}")

    # Critical priority always gets premium processing
    if priority == 'critical':
        if gpu_available and worker_nodes >= 5:
            return 'critical_gpu_processing'
        else:
            return 'critical_cpu_processing'

    # High error rate requires special handling
    if error_rate > 0.05:
        return 'error_recovery_processing'

    # Resource-based decisions for normal priority
    if cpu_usage > 0.8 or memory_usage > 0.7:
        return 'resource_constrained_processing'

    if gpu_available and priority == 'high':
        return 'gpu_accelerated_processing'

    if worker_nodes >= 3:
        return 'distributed_processing'

    # Default processing
    return 'standard_processing'


def critical_gpu_processing(**context):
    """High-priority processing with GPU acceleration."""
    print("Critical GPU processing:")
    print("- GPU-accelerated algorithms")
    print("- Maximum resource allocation")
    print("- Real-time monitoring")
    return {"method": "critical_gpu", "resources": "maximum"}


def critical_cpu_processing(**context):
    """High-priority processing without GPU."""
    print("Critical CPU processing:")
    print("- Multi-threaded processing")
    print("- Priority queue handling")
    print("- Enhanced monitoring")
    return {"method": "critical_cpu", "resources": "high"}


def error_recovery_processing(**context):
    """Special processing for high error rate scenarios."""
    print("Error recovery processing:")
    print("- Enhanced error handling")
    print("- Data validation steps")
    print("- Recovery mechanisms")
    return {"method": "error_recovery", "validation": "enhanced"}


def resource_constrained_processing(**context):
    """Processing optimized for limited resources."""
    print("Resource-constrained processing:")
    print("- Memory-efficient algorithms")
    print("- Batch processing")
    print("- Resource monitoring")
    return {"method": "resource_constrained", "optimization": "memory"}


def gpu_accelerated_processing(**context):
    """GPU-accelerated processing for high priority tasks."""
    print("GPU-accelerated processing:")
    print("- GPU computation")
    print("- Parallel processing")
    print("- Performance optimization")
    return {"method": "gpu_accelerated", "acceleration": "gpu"}


def distributed_processing(**context):
    """Distributed processing across multiple workers."""
    print("Distributed processing:")
    print("- Multi-node execution")
    print("- Load balancing")
    print("- Fault tolerance")
    return {"method": "distributed", "nodes": "multiple"}


def standard_processing(**context):
    """Standard processing for normal conditions."""
    print("Standard processing:")
    print("- Single-node execution")
    print("- Standard algorithms")
    print("- Basic monitoring")
    return {"method": "standard", "resources": "normal"}


# DAG 2: XCom-driven complex branching
dag2 = DAG(
    'advanced_2_xcom_driven_branching',
    default_args=default_args,
    description='Complex branching driven by multiple XCom inputs',
    schedule_interval=timedelta(hours=4),
    catchup=False,
    tags=['advanced', 'branching', 'xcom-driven']
)

# Tasks for XCom-driven branching
start_2 = DummyOperator(task_id='start', dag=dag2)

collect_metrics = PythonOperator(
    task_id='collect_metrics',
    python_callable=collect_processing_metrics,
    dag=dag2
)

check_resources = PythonOperator(
    task_id='check_resources',
    python_callable=resource_availability_check,
    dag=dag2
)

intelligent_branch = BranchPythonOperator(
    task_id='intelligent_processing_branch',
    python_callable=intelligent_processing_branch,
    dag=dag2
)

# Processing tasks
critical_gpu = PythonOperator(
    task_id='critical_gpu_processing', python_callable=critical_gpu_processing, dag=dag2)
critical_cpu = PythonOperator(
    task_id='critical_cpu_processing', python_callable=critical_cpu_processing, dag=dag2)
error_recovery = PythonOperator(
    task_id='error_recovery_processing', python_callable=error_recovery_processing, dag=dag2)
resource_constrained = PythonOperator(
    task_id='resource_constrained_processing', python_callable=resource_constrained_processing, dag=dag2)
gpu_accelerated = PythonOperator(
    task_id='gpu_accelerated_processing', python_callable=gpu_accelerated_processing, dag=dag2)
distributed = PythonOperator(
    task_id='distributed_processing', python_callable=distributed_processing, dag=dag2)
standard = PythonOperator(task_id='standard_processing',
                          python_callable=standard_processing, dag=dag2)

join_2 = DummyOperator(
    task_id='join', trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, dag=dag2)
end_2 = DummyOperator(task_id='end', dag=dag2)

# Dependencies
start_2 >> [collect_metrics, check_resources] >> intelligent_branch
intelligent_branch >> [critical_gpu, critical_cpu, error_recovery, resource_constrained,
                       gpu_accelerated, distributed, standard]
[critical_gpu, critical_cpu, error_recovery, resource_constrained,
 gpu_accelerated, distributed, standard] >> join_2 >> end_2

# =============================================================================
# Example 3: Error Handling and Recovery Patterns
# =============================================================================


def risky_data_operation(**context):
    """Simulate a data operation that might fail."""
    import random

    # Simulate different outcomes
    outcomes = [
        {'status': 'success', 'records_processed': 1000, 'errors': 0},
        {'status': 'partial_success', 'records_processed': 750, 'errors': 250},
        {'status': 'failure', 'records_processed': 0,
            'errors': 1000, 'error_type': 'connection'},
        {'status': 'failure', 'records_processed': 100,
            'errors': 900, 'error_type': 'validation'}
    ]

    outcome = random.choice(outcomes)
    print(f"Operation outcome: {outcome}")

    # Simulate failure for demonstration
    if outcome['status'] == 'failure':
        # Don't actually raise exception, just return failure info
        print(f"Operation failed: {outcome.get('error_type', 'unknown')}")

    return outcome


def error_handling_branch(**context):
    """Branch based on operation results and error conditions."""
    try:
        result = context['task_instance'].xcom_pull(task_ids='risky_operation')

        if result is None:
            print("No result from previous task - assuming failure")
            return 'handle_missing_data'

        status = result.get('status')
        error_count = result.get('errors', 0)
        error_type = result.get('error_type')

        print(f"Handling result: status={status}, errors={error_count}")

        if status == 'success':
            return 'success_processing'
        elif status == 'partial_success':
            if error_count > 500:
                return 'major_error_recovery'
            else:
                return 'minor_error_recovery'
        elif status == 'failure':
            if error_type == 'connection':
                return 'connection_error_recovery'
            elif error_type == 'validation':
                return 'validation_error_recovery'
            else:
                return 'general_error_recovery'
        else:
            return 'unknown_status_handling'

    except Exception as e:
        print(f"Exception in branching logic: {e}")
        return 'exception_recovery'


def success_processing(**context):
    """Handle successful operations."""
    result = context['task_instance'].xcom_pull(task_ids='risky_operation')
    print(
        f"Success processing: {result['records_processed']} records processed")
    return {"status": "completed", "next_action": "continue_pipeline"}


def minor_error_recovery(**context):
    """Handle minor errors with simple recovery."""
    result = context['task_instance'].xcom_pull(task_ids='risky_operation')
    print(f"Minor error recovery: {result['errors']} errors to handle")
    print("- Logging errors")
    print("- Continuing with partial data")
    return {"status": "recovered", "action": "partial_processing"}


def major_error_recovery(**context):
    """Handle major errors with comprehensive recovery."""
    result = context['task_instance'].xcom_pull(task_ids='risky_operation')
    print(f"Major error recovery: {result['errors']} errors detected")
    print("- Detailed error analysis")
    print("- Data quality assessment")
    print("- Recovery strategy implementation")
    return {"status": "major_recovery", "action": "comprehensive_analysis"}


def connection_error_recovery(**context):
    """Handle connection-specific errors."""
    print("Connection error recovery:")
    print("- Retry connection with backoff")
    print("- Switch to backup data source")
    print("- Notify system administrators")
    return {"status": "connection_recovered", "action": "retry_with_backup"}


def validation_error_recovery(**context):
    """Handle data validation errors."""
    print("Validation error recovery:")
    print("- Data cleansing procedures")
    print("- Schema validation")
    print("- Data quality reporting")
    return {"status": "validation_recovered", "action": "data_cleansing"}


def general_error_recovery(**context):
    """Handle general errors."""
    print("General error recovery:")
    print("- Error logging and analysis")
    print("- Fallback procedures")
    print("- Manual intervention notification")
    return {"status": "general_recovery", "action": "fallback_procedures"}


def handle_missing_data(**context):
    """Handle missing data scenarios."""
    print("Missing data handling:")
    print("- Check data source availability")
    print("- Use cached data if available")
    print("- Schedule retry")
    return {"status": "missing_data_handled", "action": "retry_scheduled"}


def exception_recovery(**context):
    """Handle unexpected exceptions."""
    print("Exception recovery:")
    print("- System state analysis")
    print("- Emergency procedures")
    print("- Alert system administrators")
    return {"status": "exception_handled", "action": "emergency_procedures"}


def unknown_status_handling(**context):
    """Handle unknown status scenarios."""
    print("Unknown status handling:")
    print("- Status investigation")
    print("- Safe mode activation")
    print("- Manual review required")
    return {"status": "unknown_handled", "action": "manual_review"}


# DAG 3: Error handling and recovery patterns
dag3 = DAG(
    'advanced_3_error_handling_branching',
    default_args=default_args,
    description='Error handling and recovery patterns with branching',
    schedule_interval=timedelta(hours=8),
    catchup=False,
    tags=['advanced', 'branching', 'error-handling']
)

# Tasks for error handling
start_3 = DummyOperator(task_id='start', dag=dag3)

risky_operation = PythonOperator(
    task_id='risky_operation',
    python_callable=risky_data_operation,
    dag=dag3
)

error_branch = BranchPythonOperator(
    task_id='error_handling_branch',
    python_callable=error_handling_branch,
    dag=dag3
)

# Recovery tasks
success_task = PythonOperator(
    task_id='success_processing', python_callable=success_processing, dag=dag3)
minor_recovery = PythonOperator(
    task_id='minor_error_recovery', python_callable=minor_error_recovery, dag=dag3)
major_recovery = PythonOperator(
    task_id='major_error_recovery', python_callable=major_error_recovery, dag=dag3)
connection_recovery = PythonOperator(
    task_id='connection_error_recovery', python_callable=connection_error_recovery, dag=dag3)
validation_recovery = PythonOperator(
    task_id='validation_error_recovery', python_callable=validation_error_recovery, dag=dag3)
general_recovery = PythonOperator(
    task_id='general_error_recovery', python_callable=general_error_recovery, dag=dag3)
missing_data = PythonOperator(
    task_id='handle_missing_data', python_callable=handle_missing_data, dag=dag3)
exception_task = PythonOperator(
    task_id='exception_recovery', python_callable=exception_recovery, dag=dag3)
unknown_status = PythonOperator(
    task_id='unknown_status_handling', python_callable=unknown_status_handling, dag=dag3)

join_3 = DummyOperator(
    task_id='join', trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, dag=dag3)
end_3 = DummyOperator(task_id='end', dag=dag3)

# Dependencies
start_3 >> risky_operation >> error_branch
error_branch >> [success_task, minor_recovery, major_recovery, connection_recovery,
                 validation_recovery, general_recovery, missing_data, exception_task, unknown_status]
[success_task, minor_recovery, major_recovery, connection_recovery,
 validation_recovery, general_recovery, missing_data, exception_task, unknown_status] >> join_3 >> end_3

if __name__ == "__main__":
    print("Advanced branching patterns ready for Airflow execution!")
    print("\nDAGs included:")
    print("1. advanced_1_nested_branching - Nested decision points")
    print("2. advanced_2_xcom_driven_branching - Complex XCom-based decisions")
    print("3. advanced_3_error_handling_branching - Error recovery patterns")
