"""
Solution for Exercise 1: Basic Branching with BranchPythonOperator

This solution demonstrates:
- Day-of-week branching logic
- Data volume-based branching
- Combined branching scenarios
- Proper task dependency setup with trigger rules
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
import random

default_args = {
    'owner': 'airflow-kata-solution',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# =============================================================================
# Task 1 Solution: Day-of-Week Branching
# =============================================================================


def day_of_week_branch(**context):
    """
    Branch based on current day of the week.

    Returns:
        str: 'weekday_processing' or 'weekend_processing'
    """
    # Use execution_date from context for consistent behavior
    execution_date = context.get('execution_date', datetime.now())
    day_of_week = execution_date.weekday()  # 0=Monday, 6=Sunday

    print(f"Execution date: {execution_date}")
    print(
        f"Day of week: {day_of_week} ({'weekday' if day_of_week < 5 else 'weekend'})")

    if day_of_week < 5:  # Monday (0) through Friday (4)
        print("Choosing weekday processing path")
        return 'weekday_processing'
    else:  # Saturday (5) and Sunday (6)
        print("Choosing weekend processing path")
        return 'weekend_processing'


def weekday_processing(**context):
    """
    Weekday processing logic for transaction data.

    Simulates:
    - Transaction data loading
    - Standard validation
    - Basic reporting
    """
    print("=== Weekday Processing Started ===")
    print("Processing transaction data for business day")

    # Simulate transaction data processing
    transaction_count = random.randint(5000, 15000)
    print(f"Loading {transaction_count} transactions")

    # Standard validation
    print("Applying standard validation rules:")
    print("- Checking transaction amounts")
    print("- Validating merchant IDs")
    print("- Verifying payment methods")

    # Basic reporting
    print("Generating basic reports:")
    print("- Daily transaction summary")
    print("- Payment method breakdown")
    print("- Top merchants report")

    result = {
        'processing_type': 'weekday',
        'transactions_processed': transaction_count,
        'validation_level': 'standard',
        'reports_generated': 3,
        'processing_time_minutes': random.randint(15, 30)
    }

    print(f"Weekday processing completed: {result}")
    return result


def weekend_processing(**context):
    """
    Weekend processing logic for inventory data.

    Simulates:
    - Inventory data loading
    - Enhanced validation
    - Detailed reporting
    """
    print("=== Weekend Processing Started ===")
    print("Processing inventory data for weekend analysis")

    # Simulate inventory data processing
    inventory_items = random.randint(10000, 50000)
    print(f"Loading {inventory_items} inventory items")

    # Enhanced validation
    print("Applying enhanced validation rules:")
    print("- Stock level verification")
    print("- Price consistency checks")
    print("- Supplier data validation")
    print("- Seasonal trend analysis")

    # Detailed reporting
    print("Generating detailed reports:")
    print("- Inventory turnover analysis")
    print("- Stock level optimization")
    print("- Supplier performance metrics")
    print("- Seasonal demand forecasting")

    result = {
        'processing_type': 'weekend',
        'inventory_items_processed': inventory_items,
        'validation_level': 'enhanced',
        'reports_generated': 4,
        'processing_time_minutes': random.randint(45, 90)
    }

    print(f"Weekend processing completed: {result}")
    return result


# DAG 1: Day-of-week branching
dag1 = DAG(
    'exercise_1_day_branching_solution',
    default_args=default_args,
    description='Day-of-week branching exercise solution',
    schedule_interval=timedelta(hours=12),
    catchup=False,
    tags=['solution', 'branching', 'basic', 'day-based']
)

# Tasks for DAG 1
start_1 = DummyOperator(task_id='start', dag=dag1)

day_branch = BranchPythonOperator(
    task_id='day_of_week_branch',
    python_callable=day_of_week_branch,
    dag=dag1
)

weekday_task = PythonOperator(
    task_id='weekday_processing',
    python_callable=weekday_processing,
    dag=dag1
)

weekend_task = PythonOperator(
    task_id='weekend_processing',
    python_callable=weekend_processing,
    dag=dag1
)

# Join point - executes regardless of which branch ran
join_1 = DummyOperator(
    task_id='join_processing',
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag1
)

end_1 = DummyOperator(task_id='end', dag=dag1)

# Define dependencies
start_1 >> day_branch >> [weekday_task, weekend_task] >> join_1 >> end_1

# =============================================================================
# Task 2 Solution: Data Volume Branching
# =============================================================================


def generate_sample_data(**context):
    """
    Generate sample data with random record count.

    Returns:
        dict: Data information including record count and characteristics
    """
    # Simulate different data scenarios
    scenarios = [
        {'record_count': random.randint(
            100, 999), 'data_source': 'api', 'complexity': 'low'},
        {'record_count': random.randint(
            1000, 5000), 'data_source': 'database', 'complexity': 'medium'},
        {'record_count': random.randint(
            5001, 20000), 'data_source': 'file', 'complexity': 'high'},
        {'record_count': random.randint(
            50, 500), 'data_source': 'stream', 'complexity': 'low'}
    ]

    data = random.choice(scenarios)
    data['generation_timestamp'] = datetime.now().isoformat()
    data['data_quality_score'] = random.uniform(0.8, 0.99)

    print(f"Generated sample data: {data}")
    return data


def volume_based_branch(**context):
    """
    Branch based on data volume from previous task.

    Returns:
        str: 'small_volume_processing' or 'large_volume_processing'
    """
    # Pull data from previous task
    data = context['task_instance'].xcom_pull(task_ids='generate_data')

    if data is None:
        print("Warning: No data received from previous task, defaulting to small volume")
        return 'small_volume_processing'

    record_count = data.get('record_count', 0)
    data_source = data.get('data_source', 'unknown')

    print(f"Analyzing data volume: {record_count} records from {data_source}")

    if record_count >= 1000:
        print(
            f"Large dataset detected ({record_count} records) - using optimized processing")
        return 'large_volume_processing'
    else:
        print(
            f"Small dataset detected ({record_count} records) - using standard processing")
        return 'small_volume_processing'


def small_volume_processing(**context):
    """Process small datasets with standard methods."""
    data = context['task_instance'].xcom_pull(task_ids='generate_data')
    record_count = data.get('record_count', 0)
    data_source = data.get('data_source', 'unknown')

    print("=== Small Volume Processing ===")
    print(f"Processing {record_count} records from {data_source}")

    # Single-threaded processing simulation
    print("Processing approach:")
    print("- Single-threaded execution")
    print("- In-memory operations")
    print("- Standard validation rules")
    print("- Basic error handling")

    # Simulate processing time
    processing_time = record_count * 0.01  # 0.01 seconds per record
    print(f"Estimated processing time: {processing_time:.2f} seconds")

    result = {
        "status": "completed",
        "method": "standard",
        "records_processed": record_count,
        "processing_time_seconds": processing_time,
        "memory_usage_mb": record_count * 0.001,  # 1KB per record
        "cpu_cores_used": 1
    }

    print(f"Small volume processing result: {result}")
    return result


def large_volume_processing(**context):
    """Process large datasets with optimized methods."""
    data = context['task_instance'].xcom_pull(task_ids='generate_data')
    record_count = data.get('record_count', 0)
    data_source = data.get('data_source', 'unknown')

    print("=== Large Volume Processing ===")
    print(f"Processing {record_count} records from {data_source}")

    # Multi-threaded processing simulation
    print("Processing approach:")
    print("- Multi-threaded execution (4 threads)")
    print("- Batch operations (1000 records per batch)")
    print("- Enhanced validation with parallel checks")
    print("- Advanced error handling and recovery")

    # Calculate batches
    batch_size = 1000
    num_batches = (record_count + batch_size - 1) // batch_size
    print(f"Processing in {num_batches} batches of {batch_size} records")

    # Simulate processing time (faster due to parallelization)
    processing_time = (record_count * 0.01) / 4  # 4x faster with 4 threads
    print(f"Estimated processing time: {processing_time:.2f} seconds")

    result = {
        "status": "completed",
        "method": "optimized",
        "records_processed": record_count,
        "processing_time_seconds": processing_time,
        # 2KB per record (more overhead)
        "memory_usage_mb": record_count * 0.002,
        "cpu_cores_used": 4,
        "batches_processed": num_batches
    }

    print(f"Large volume processing result: {result}")
    return result


# DAG 2: Data volume branching
dag2 = DAG(
    'exercise_1_volume_branching_solution',
    default_args=default_args,
    description='Data volume branching exercise solution',
    schedule_interval=timedelta(hours=8),
    catchup=False,
    tags=['solution', 'branching', 'volume-based']
)

# Tasks for DAG 2
start_2 = DummyOperator(task_id='start', dag=dag2)

generate_data = PythonOperator(
    task_id='generate_data',
    python_callable=generate_sample_data,
    dag=dag2
)

volume_branch = BranchPythonOperator(
    task_id='volume_based_branch',
    python_callable=volume_based_branch,
    dag=dag2
)

small_processing = PythonOperator(
    task_id='small_volume_processing',
    python_callable=small_volume_processing,
    dag=dag2
)

large_processing = PythonOperator(
    task_id='large_volume_processing',
    python_callable=large_volume_processing,
    dag=dag2
)

join_2 = DummyOperator(
    task_id='join_processing',
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag2
)

end_2 = DummyOperator(task_id='end', dag=dag2)

# Define dependencies
start_2 >> generate_data >> volume_branch >> [
    small_processing, large_processing] >> join_2 >> end_2

# =============================================================================
# Task 3 Solution: Combined Branching Logic
# =============================================================================


def combined_branch(**context):
    """
    Implement combined branching logic considering both day of week and data volume.

    Returns:
        str: One of four processing task IDs
    """
    # Get day of week
    execution_date = context.get('execution_date', datetime.now())
    day_of_week = execution_date.weekday()
    is_weekday = day_of_week < 5

    # Get data volume
    data = context['task_instance'].xcom_pull(
        task_ids='generate_combined_data')
    if data is None:
        print("Warning: No data received, using default small weekday processing")
        return 'weekday_small_processing'

    record_count = data.get('record_count', 0)
    is_large_volume = record_count >= 1000

    # Combined decision logic
    day_type = 'weekday' if is_weekday else 'weekend'
    volume_type = 'large' if is_large_volume else 'small'

    print(f"Combined branching decision:")
    print(f"- Day type: {day_type} (day {day_of_week})")
    print(f"- Volume type: {volume_type} ({record_count} records)")

    # Determine processing path
    task_id = f'{day_type}_{volume_type}_processing'
    print(f"Selected processing path: {task_id}")

    return task_id


def generate_combined_data(**context):
    """Generate data for combined branching scenario."""
    data = {
        'record_count': random.randint(200, 8000),
        'data_source': random.choice(['api', 'database', 'file', 'stream']),
        'priority': random.choice(['low', 'medium', 'high']),
        'quality_score': random.uniform(0.7, 0.99),
        'generation_timestamp': datetime.now().isoformat()
    }

    print(f"Generated combined scenario data: {data}")
    return data


def weekday_small_processing(**context):
    """Process small datasets on weekdays."""
    data = context['task_instance'].xcom_pull(
        task_ids='generate_combined_data')

    print("=== Weekday Small Processing ===")
    print("Optimized for business day, small volume processing")
    print("- Fast transaction processing")
    print("- Real-time validation")
    print("- Immediate reporting")

    return {
        "processing_type": "weekday_small",
        "records": data.get('record_count', 0),
        "approach": "fast_realtime",
        "completion_time_minutes": random.randint(5, 15)
    }


def weekday_large_processing(**context):
    """Process large datasets on weekdays."""
    data = context['task_instance'].xcom_pull(
        task_ids='generate_combined_data')

    print("=== Weekday Large Processing ===")
    print("High-performance processing for business day, large volume")
    print("- Parallel transaction processing")
    print("- Batch validation")
    print("- Scheduled reporting")

    return {
        "processing_type": "weekday_large",
        "records": data.get('record_count', 0),
        "approach": "parallel_batch",
        "completion_time_minutes": random.randint(20, 45)
    }


def weekend_small_processing(**context):
    """Process small datasets on weekends."""
    data = context['task_instance'].xcom_pull(
        task_ids='generate_combined_data')

    print("=== Weekend Small Processing ===")
    print("Comprehensive processing for weekend, small volume")
    print("- Detailed analysis")
    print("- Enhanced validation")
    print("- Comprehensive reporting")

    return {
        "processing_type": "weekend_small",
        "records": data.get('record_count', 0),
        "approach": "comprehensive_detailed",
        "completion_time_minutes": random.randint(30, 60)
    }


def weekend_large_processing(**context):
    """Process large datasets on weekends."""
    data = context['task_instance'].xcom_pull(
        task_ids='generate_combined_data')

    print("=== Weekend Large Processing ===")
    print("Full-scale processing for weekend, large volume")
    print("- Distributed processing")
    print("- Complete validation suite")
    print("- Full analytical reporting")

    return {
        "processing_type": "weekend_large",
        "records": data.get('record_count', 0),
        "approach": "distributed_complete",
        "completion_time_minutes": random.randint(60, 120)
    }


# DAG 3: Combined branching logic
dag3 = DAG(
    'exercise_1_combined_branching_solution',
    default_args=default_args,
    description='Combined day and volume branching solution',
    schedule_interval=timedelta(hours=6),
    catchup=False,
    tags=['solution', 'branching', 'combined']
)

# Tasks for DAG 3
start_3 = DummyOperator(task_id='start', dag=dag3)

generate_combined_data_task = PythonOperator(
    task_id='generate_combined_data',
    python_callable=generate_combined_data,
    dag=dag3
)

combined_branch_task = BranchPythonOperator(
    task_id='combined_branch',
    python_callable=combined_branch,
    dag=dag3
)

weekday_small = PythonOperator(
    task_id='weekday_small_processing',
    python_callable=weekday_small_processing,
    dag=dag3
)

weekday_large = PythonOperator(
    task_id='weekday_large_processing',
    python_callable=weekday_large_processing,
    dag=dag3
)

weekend_small = PythonOperator(
    task_id='weekend_small_processing',
    python_callable=weekend_small_processing,
    dag=dag3
)

weekend_large = PythonOperator(
    task_id='weekend_large_processing',
    python_callable=weekend_large_processing,
    dag=dag3
)

join_3 = DummyOperator(
    task_id='join_processing',
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag3
)

end_3 = DummyOperator(task_id='end', dag=dag3)

# Define dependencies
start_3 >> generate_combined_data_task >> combined_branch_task
combined_branch_task >> [weekday_small,
                         weekday_large, weekend_small, weekend_large]
[weekday_small, weekday_large, weekend_small, weekend_large] >> join_3 >> end_3

# =============================================================================
# Testing Functions
# =============================================================================


def test_branching_functions():
    """Test all branching functions with various scenarios."""

    print("=== Testing Branching Functions ===")

    # Test day-of-week branching
    print("\n1. Testing day-of-week branching:")

    # Monday test
    monday_context = {'execution_date': datetime(2024, 1, 15)}  # Monday
    result = day_of_week_branch(**monday_context)
    print(f"Monday result: {result}")
    assert result == 'weekday_processing', f"Expected 'weekday_processing', got {result}"

    # Saturday test
    saturday_context = {'execution_date': datetime(2024, 1, 13)}  # Saturday
    result = day_of_week_branch(**saturday_context)
    print(f"Saturday result: {result}")
    assert result == 'weekend_processing', f"Expected 'weekend_processing', got {result}"

    # Test volume-based branching
    print("\n2. Testing volume-based branching:")

    # Small volume test
    small_ti = type(
        'MockTI', (), {'xcom_pull': lambda task_ids: {'record_count': 500}})()
    small_context = {'task_instance': small_ti}
    result = volume_based_branch(**small_context)
    print(f"Small volume result: {result}")
    assert result == 'small_volume_processing', f"Expected 'small_volume_processing', got {result}"

    # Large volume test
    large_ti = type(
        'MockTI', (), {'xcom_pull': lambda task_ids: {'record_count': 1500}})()
    large_context = {'task_instance': large_ti}
    result = volume_based_branch(**large_context)
    print(f"Large volume result: {result}")
    assert result == 'large_volume_processing', f"Expected 'large_volume_processing', got {result}"

    # Test combined branching
    print("\n3. Testing combined branching:")

    # Weekday small test
    weekday_small_ti = type(
        'MockTI', (), {'xcom_pull': lambda task_ids: {'record_count': 500}})()
    weekday_small_context = {
        'execution_date': datetime(2024, 1, 15),  # Monday
        'task_instance': weekday_small_ti
    }
    result = combined_branch(**weekday_small_context)
    print(f"Weekday small result: {result}")
    assert result == 'weekday_small_processing', f"Expected 'weekday_small_processing', got {result}"

    # Weekend large test
    weekend_large_ti = type(
        'MockTI', (), {'xcom_pull': lambda task_ids: {'record_count': 2000}})()
    weekend_large_context = {
        'execution_date': datetime(2024, 1, 13),  # Saturday
        'task_instance': weekend_large_ti
    }
    result = combined_branch(**weekend_large_context)
    print(f"Weekend large result: {result}")
    assert result == 'weekend_large_processing', f"Expected 'weekend_large_processing', got {result}"

    print("\n✅ All tests passed!")


if __name__ == "__main__":
    print("Exercise 1 Solution - Basic Branching with BranchPythonOperator")
    print("=" * 60)

    # Run tests
    test_branching_functions()

    print("\nSolution includes:")
    print("1. exercise_1_day_branching_solution - Day-of-week branching")
    print("2. exercise_1_volume_branching_solution - Data volume branching")
    print("3. exercise_1_combined_branching_solution - Combined branching logic")

    print("\nKey learning points demonstrated:")
    print("- Proper use of BranchPythonOperator")
    print("- XCom data passing for branching decisions")
    print("- Trigger rules for joining after branches")
    print("- Error handling and edge cases")
    print("- Comprehensive logging and decision tracking")
