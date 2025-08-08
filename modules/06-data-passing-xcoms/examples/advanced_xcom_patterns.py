"""
Advanced XCom Patterns - Complex Data Passing Scenarios

This module demonstrates advanced XCom usage patterns:
- Custom XCom backends for specialized storage
- Large data handling with file references
- Dynamic XCom key generation
- XCom cleanup and lifecycle management
- Performance optimization techniques
- Cross-DAG communication patterns
"""

from datetime import datetime, timedelta
import json
import tempfile
import os
from typing import Any, Dict, List, Optional
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models.xcom import BaseXCom
from airflow.utils.context import Context

# Default arguments for all DAGs
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
# Example 1: Large Data Handling with File References
# =============================================================================


def generate_large_dataset():
    """Generate a large dataset and store it as file reference"""
    # Simulate generating a large dataset
    large_data = {
        "records": [
            {
                "id": i,
                "name": f"Record_{i}",
                # Make it large
                "data": f"Large data content for record {i}" * 100,
                "timestamp": datetime.now().isoformat(),
                "metadata": {"processed": True, "version": 1.0}
            }
            for i in range(1000)  # 1000 records with large content
        ],
        "summary": {
            "total_records": 1000,
            "generated_at": datetime.now().isoformat(),
            "data_size_mb": 15.7
        }
    }

    # Save to temporary file instead of passing through XCom
    temp_file = tempfile.NamedTemporaryFile(
        mode='w', delete=False, suffix='.json')
    with temp_file as f:
        json.dump(large_data, f, indent=2)
        temp_path = temp_file.name

    print(f"Generated large dataset with {len(large_data['records'])} records")
    print(f"Saved to temporary file: {temp_path}")

    # Return only reference and metadata (small payload)
    return {
        "data_location": temp_path,
        "record_count": len(large_data['records']),
        "file_size_bytes": os.path.getsize(temp_path),
        "data_type": "json",
        "generated_at": datetime.now().isoformat()
    }


def process_large_dataset(**context):
    """Process large dataset using file reference"""
    # Pull the file reference from XCom
    file_ref = context['task_instance'].xcom_pull(
        task_ids='generate_large_data')

    if not file_ref or 'data_location' not in file_ref:
        raise ValueError("No file reference received from previous task")

    # Load data from file
    with open(file_ref['data_location'], 'r') as f:
        large_data = json.load(f)

    # Process the data
    processed_records = []
    for record in large_data['records'][:10]:  # Process first 10 for demo
        processed_record = {
            "original_id": record['id'],
            "processed_name": record['name'].upper(),
            "processing_timestamp": datetime.now().isoformat(),
            "status": "processed"
        }
        processed_records.append(processed_record)

    # Clean up the temporary file
    os.unlink(file_ref['data_location'])
    print(f"Cleaned up temporary file: {file_ref['data_location']}")

    # Return processing results (small payload)
    return {
        "processed_count": len(processed_records),
        "sample_processed": processed_records[:3],  # Just a sample
        "original_file_size": file_ref['file_size_bytes'],
        "processing_completed_at": datetime.now().isoformat()
    }


# Create DAG for large data handling
large_data_dag = DAG(
    'large_data_xcom_pattern',
    default_args=default_args,
    description='Handling large datasets with file references',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['xcom', 'advanced', 'large-data']
)

generate_large_task = PythonOperator(
    task_id='generate_large_data',
    python_callable=generate_large_dataset,
    dag=large_data_dag
)

process_large_task = PythonOperator(
    task_id='process_large_data',
    python_callable=process_large_dataset,
    dag=large_data_dag
)

generate_large_task >> process_large_task

# =============================================================================
# Example 2: Dynamic XCom Key Generation
# =============================================================================


def generate_dynamic_data(**context):
    """Generate data with dynamic XCom keys based on runtime conditions"""
    ti = context['task_instance']

    # Simulate different data sources
    data_sources = ['database_a', 'database_b', 'api_service', 'file_system']

    for source in data_sources:
        # Generate data for each source
        source_data = {
            "source": source,
            "records": [f"record_{i}_from_{source}" for i in range(5)],
            "extracted_at": datetime.now().isoformat(),
            "status": "success" if source != 'api_service' else "partial"  # Simulate failure
        }

        # Push with dynamic key
        key = f"data_from_{source}"
        ti.xcom_push(key=key, value=source_data)
        print(f"Pushed data for source: {source} with key: {key}")

    # Return metadata about what was pushed
    return {
        "sources_processed": data_sources,
        "total_sources": len(data_sources),
        "keys_created": [f"data_from_{source}" for source in data_sources],
        "processing_timestamp": datetime.now().isoformat()
    }


def aggregate_dynamic_data(**context):
    """Aggregate data from dynamically generated XCom keys"""
    ti = context['task_instance']

    # Get metadata about what keys were created
    metadata = ti.xcom_pull(task_ids='generate_dynamic_data')
    keys_to_pull = metadata['keys_created']

    aggregated_data = {
        "successful_sources": [],
        "failed_sources": [],
        "total_records": 0,
        "source_details": {}
    }

    # Pull data from each dynamic key
    for key in keys_to_pull:
        source_data = ti.xcom_pull(task_ids='generate_dynamic_data', key=key)

        if source_data and source_data['status'] == 'success':
            aggregated_data['successful_sources'].append(source_data['source'])
            aggregated_data['total_records'] += len(source_data['records'])
        else:
            aggregated_data['failed_sources'].append(
                source_data['source'] if source_data else key)

        # Store details for each source
        aggregated_data['source_details'][key] = {
            "record_count": len(source_data['records']) if source_data else 0,
            "status": source_data['status'] if source_data else 'missing',
            "extracted_at": source_data.get('extracted_at', 'unknown')
        }

    aggregated_data['aggregation_completed_at'] = datetime.now().isoformat()

    print(f"Aggregated data from {len(keys_to_pull)} sources")
    print(f"Successful: {len(aggregated_data['successful_sources'])}")
    print(f"Failed: {len(aggregated_data['failed_sources'])}")

    return aggregated_data


# Create DAG for dynamic XCom keys
dynamic_keys_dag = DAG(
    'dynamic_xcom_keys_pattern',
    default_args=default_args,
    description='Dynamic XCom key generation and aggregation',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['xcom', 'advanced', 'dynamic']
)

generate_dynamic_task = PythonOperator(
    task_id='generate_dynamic_data',
    python_callable=generate_dynamic_data,
    dag=dynamic_keys_dag
)

aggregate_dynamic_task = PythonOperator(
    task_id='aggregate_dynamic_data',
    python_callable=aggregate_dynamic_data,
    dag=dynamic_keys_dag
)

generate_dynamic_task >> aggregate_dynamic_task

# =============================================================================
# Example 3: XCom Lifecycle Management and Cleanup
# =============================================================================


def create_temporary_data(**context):
    """Create temporary data that needs cleanup"""
    ti = context['task_instance']

    # Create multiple temporary data items
    temp_data_items = []

    for i in range(3):
        temp_data = {
            "temp_id": f"temp_{i}",
            "data": f"Temporary data item {i}",
            "created_at": datetime.now().isoformat(),
            "cleanup_required": True
        }

        key = f"temp_data_{i}"
        ti.xcom_push(key=key, value=temp_data)
        temp_data_items.append(key)

    # Also create some permanent data
    permanent_data = {
        "permanent_id": "perm_001",
        "data": "This data should be kept",
        "created_at": datetime.now().isoformat(),
        "cleanup_required": False
    }
    ti.xcom_push(key='permanent_data', value=permanent_data)

    return {
        "temp_keys_created": temp_data_items,
        "permanent_keys_created": ['permanent_data'],
        "total_items": len(temp_data_items) + 1,
        "creation_timestamp": datetime.now().isoformat()
    }


def process_and_cleanup(**context):
    """Process data and clean up temporary XComs"""
    ti = context['task_instance']

    # Get information about created keys
    creation_info = ti.xcom_pull(task_ids='create_temporary_data')

    processed_items = []

    # Process temporary data
    for temp_key in creation_info['temp_keys_created']:
        temp_data = ti.xcom_pull(
            task_ids='create_temporary_data', key=temp_key)

        if temp_data:
            processed_item = {
                "original_id": temp_data['temp_id'],
                "processed_data": temp_data['data'].upper(),
                "processed_at": datetime.now().isoformat()
            }
            processed_items.append(processed_item)

            # Simulate cleanup by marking as processed
            print(f"Processed and cleaned up: {temp_key}")

    # Process permanent data (keep it)
    permanent_data = ti.xcom_pull(
        task_ids='create_temporary_data', key='permanent_data')
    if permanent_data:
        print(f"Permanent data preserved: {permanent_data['permanent_id']}")

    return {
        "processed_temp_items": len(processed_items),
        "permanent_items_preserved": 1,
        "cleanup_completed_at": datetime.now().isoformat(),
        "processed_sample": processed_items[:2]  # Sample of processed items
    }


# Create DAG for XCom lifecycle management
lifecycle_dag = DAG(
    'xcom_lifecycle_management',
    default_args=default_args,
    description='XCom lifecycle management and cleanup patterns',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['xcom', 'advanced', 'lifecycle']
)

create_temp_task = PythonOperator(
    task_id='create_temporary_data',
    python_callable=create_temporary_data,
    dag=lifecycle_dag
)

cleanup_task = PythonOperator(
    task_id='process_and_cleanup',
    python_callable=process_and_cleanup,
    dag=lifecycle_dag
)

create_temp_task >> cleanup_task

# =============================================================================
# Example 4: Performance-Optimized XCom Usage
# =============================================================================


def generate_batch_data(**context):
    """Generate data in optimized batches"""
    ti = context['task_instance']

    # Instead of pushing many small XComs, batch them together
    batch_size = 100
    total_records = 500

    batches = []
    for batch_num in range(0, total_records, batch_size):
        batch_data = {
            "batch_id": batch_num // batch_size,
            "records": [
                {
                    "id": i,
                    "value": f"data_{i}",
                    "batch": batch_num // batch_size
                }
                for i in range(batch_num, min(batch_num + batch_size, total_records))
            ],
            "batch_size": min(batch_size, total_records - batch_num),
            "created_at": datetime.now().isoformat()
        }
        batches.append(batch_data)

    # Push all batches as a single XCom instead of multiple XComs
    ti.xcom_push(key='data_batches', value=batches)

    # Push summary information
    summary = {
        "total_batches": len(batches),
        "total_records": total_records,
        "batch_size": batch_size,
        "generation_completed_at": datetime.now().isoformat()
    }

    print(
        f"Generated {len(batches)} batches with {total_records} total records")

    return summary


def process_batch_data(**context):
    """Process batched data efficiently"""
    ti = context['task_instance']

    # Pull batched data
    batches = ti.xcom_pull(task_ids='generate_batch_data', key='data_batches')
    summary = ti.xcom_pull(task_ids='generate_batch_data')

    if not batches:
        raise ValueError("No batch data received")

    processed_batches = []
    total_processed = 0

    # Process each batch
    for batch in batches:
        processed_batch = {
            "batch_id": batch['batch_id'],
            "processed_records": len(batch['records']),
            "processing_timestamp": datetime.now().isoformat(),
            "status": "completed"
        }
        processed_batches.append(processed_batch)
        total_processed += len(batch['records'])

    processing_result = {
        "batches_processed": len(processed_batches),
        "total_records_processed": total_processed,
        "processing_completed_at": datetime.now().isoformat(),
        "batch_details": processed_batches,
        "original_summary": summary
    }

    print(
        f"Processed {len(processed_batches)} batches with {total_processed} records")

    return processing_result


# Create DAG for performance-optimized XCom usage
performance_dag = DAG(
    'performance_optimized_xcom',
    default_args=default_args,
    description='Performance-optimized XCom usage patterns',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['xcom', 'advanced', 'performance']
)

generate_batch_task = PythonOperator(
    task_id='generate_batch_data',
    python_callable=generate_batch_data,
    dag=performance_dag
)

process_batch_task = PythonOperator(
    task_id='process_batch_data',
    python_callable=process_batch_data,
    dag=performance_dag
)

generate_batch_task >> process_batch_task

# =============================================================================
# Example 5: Conditional XCom Usage with Error Handling
# =============================================================================


def risky_data_operation(**context):
    """Simulate a risky operation that might fail"""
    import random

    ti = context['task_instance']

    # Simulate different outcomes
    outcomes = ['success', 'partial_success', 'failure']
    outcome = random.choice(outcomes)

    if outcome == 'success':
        result_data = {
            "status": "success",
            "data": {"processed_items": 100, "errors": []},
            "message": "All data processed successfully",
            "processing_time": 45.2
        }
        ti.xcom_push(key='success_data', value=result_data['data'])

    elif outcome == 'partial_success':
        result_data = {
            "status": "partial_success",
            "data": {"processed_items": 75, "errors": ["timeout on 25 items"]},
            "message": "Partial processing completed",
            "processing_time": 60.1
        }
        ti.xcom_push(key='partial_data', value=result_data['data'])
        ti.xcom_push(key='error_details', value=result_data['data']['errors'])

    else:  # failure
        result_data = {
            "status": "failure",
            "data": {"processed_items": 0, "errors": ["database connection failed", "timeout"]},
            "message": "Processing failed completely",
            "processing_time": 10.5
        }
        ti.xcom_push(key='error_details', value=result_data['data']['errors'])

    print(f"Operation completed with status: {outcome}")

    return result_data


def handle_conditional_results(**context):
    """Handle results based on what XComs are available"""
    ti = context['task_instance']

    # Get the main result
    main_result = ti.xcom_pull(task_ids='risky_operation')
    status = main_result['status']

    handling_result = {
        "original_status": status,
        "handling_timestamp": datetime.now().isoformat(),
        "actions_taken": []
    }

    # Handle based on available XComs
    if status == 'success':
        success_data = ti.xcom_pull(
            task_ids='risky_operation', key='success_data')
        if success_data:
            handling_result['actions_taken'].append("Processed success data")
            handling_result['items_processed'] = success_data['processed_items']

    elif status == 'partial_success':
        partial_data = ti.xcom_pull(
            task_ids='risky_operation', key='partial_data')
        error_details = ti.xcom_pull(
            task_ids='risky_operation', key='error_details')

        if partial_data:
            handling_result['actions_taken'].append("Processed partial data")
            handling_result['items_processed'] = partial_data['processed_items']

        if error_details:
            handling_result['actions_taken'].append("Logged error details")
            handling_result['error_count'] = len(error_details)

    else:  # failure
        error_details = ti.xcom_pull(
            task_ids='risky_operation', key='error_details')

        if error_details:
            handling_result['actions_taken'].append("Initiated error recovery")
            handling_result['error_count'] = len(error_details)
            handling_result['recovery_needed'] = True

    print(
        f"Handled {status} with {len(handling_result['actions_taken'])} actions")

    return handling_result


# Create DAG for conditional XCom usage
conditional_dag = DAG(
    'conditional_xcom_usage',
    default_args=default_args,
    description='Conditional XCom usage with error handling',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['xcom', 'advanced', 'conditional', 'error-handling']
)

risky_task = PythonOperator(
    task_id='risky_operation',
    python_callable=risky_data_operation,
    dag=conditional_dag
)

handler_task = PythonOperator(
    task_id='handle_results',
    python_callable=handle_conditional_results,
    dag=conditional_dag
)

risky_task >> handler_task
