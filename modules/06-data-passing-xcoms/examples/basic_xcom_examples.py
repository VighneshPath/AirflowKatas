"""
Basic XCom Examples - Data Passing Between Tasks

This module demonstrates fundamental XCom operations in Airflow:
- Simple push/pull operations
- Different data types
- Multiple XComs from single tasks
- XCom templating in task parameters
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

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
# Example 1: Simple XCom Push and Pull
# =============================================================================


def extract_user_count():
    """Simulate extracting user count from database"""
    # In real scenario, this would query a database
    user_count = 1250
    print(f"Extracted {user_count} users from database")

    # Return value is automatically pushed to XCom with key 'return_value'
    return user_count


def process_user_data(**context):
    """Process user data using count from previous task"""
    # Pull the user count from the previous task
    user_count = context['task_instance'].xcom_pull(task_ids='extract_users')

    if user_count is None:
        raise ValueError("No user count received from extract task")

    # Process the data
    processed_count = user_count * 2  # Some processing logic
    print(f"Processed {processed_count} user records")

    return {
        "original_count": user_count,
        "processed_count": processed_count,
        "status": "completed"
    }


# Create the DAG
simple_xcom_dag = DAG(
    'simple_xcom_example',
    default_args=default_args,
    description='Basic XCom push and pull operations',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['xcom', 'basic', 'example']
)

# Define tasks
extract_task = PythonOperator(
    task_id='extract_users',
    python_callable=extract_user_count,
    dag=simple_xcom_dag
)

process_task = PythonOperator(
    task_id='process_users',
    python_callable=process_user_data,
    dag=simple_xcom_dag
)

# Set dependencies
extract_task >> process_task

# =============================================================================
# Example 2: Multiple XComs with Custom Keys
# =============================================================================


def analyze_sales_data(**context):
    """Analyze sales data and push multiple results"""
    ti = context['task_instance']

    # Simulate sales analysis
    total_sales = 45000.50
    order_count = 150
    avg_order_value = total_sales / order_count
    top_product = "Widget Pro"

    # Push multiple values with custom keys
    ti.xcom_push(key='total_sales', value=total_sales)
    ti.xcom_push(key='order_count', value=order_count)
    ti.xcom_push(key='avg_order_value', value=round(avg_order_value, 2))
    ti.xcom_push(key='top_product', value=top_product)

    print(f"Analysis complete: {order_count} orders, ${total_sales} total")

    # Also return a summary (stored with key 'return_value')
    return {
        "analysis_status": "completed",
        "timestamp": datetime.now().isoformat()
    }


def generate_sales_report(**context):
    """Generate report using multiple XCom values"""
    ti = context['task_instance']

    # Pull multiple values using different keys
    total_sales = ti.xcom_pull(task_ids='analyze_sales', key='total_sales')
    order_count = ti.xcom_pull(task_ids='analyze_sales', key='order_count')
    avg_order = ti.xcom_pull(task_ids='analyze_sales', key='avg_order_value')
    top_product = ti.xcom_pull(task_ids='analyze_sales', key='top_product')

    # Pull the return value (summary)
    summary = ti.xcom_pull(task_ids='analyze_sales')

    # Generate report
    report = f"""
    Sales Report
    ============
    Total Sales: ${total_sales}
    Order Count: {order_count}
    Average Order Value: ${avg_order}
    Top Product: {top_product}
    
    Analysis Status: {summary['analysis_status']}
    Generated: {summary['timestamp']}
    """

    print(report)
    return {"report_generated": True, "report_length": len(report)}


# Create the DAG
multi_xcom_dag = DAG(
    'multi_xcom_example',
    default_args=default_args,
    description='Multiple XComs with custom keys',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['xcom', 'multiple', 'example']
)

# Define tasks
analyze_task = PythonOperator(
    task_id='analyze_sales',
    python_callable=analyze_sales_data,
    dag=multi_xcom_dag
)

report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_sales_report,
    dag=multi_xcom_dag
)

# Set dependencies
analyze_task >> report_task

# =============================================================================
# Example 3: XCom Templating in Task Parameters
# =============================================================================


def get_file_info():
    """Get information about file to process"""
    return {
        "filename": "sales_data_2024.csv",
        "path": "/data/input",
        "size_mb": 15.7,
        "format": "csv"
    }


def validate_file_info(**context):
    """Validate file information from XCom"""
    file_info = context['task_instance'].xcom_pull(task_ids='get_file_info')

    if file_info['size_mb'] > 100:
        raise ValueError(f"File too large: {file_info['size_mb']}MB")

    print(f"File validation passed: {file_info['filename']}")
    return {"validation_status": "passed"}


# Create the DAG
templating_dag = DAG(
    'xcom_templating_example',
    default_args=default_args,
    description='XCom templating in task parameters',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['xcom', 'templating', 'example']
)

# Task that provides file information
file_info_task = PythonOperator(
    task_id='get_file_info',
    python_callable=get_file_info,
    dag=templating_dag
)

# Task that validates the file
validate_task = PythonOperator(
    task_id='validate_file',
    python_callable=validate_file_info,
    dag=templating_dag
)

# Bash task that uses XCom value in command through templating
process_file_task = BashOperator(
    task_id='process_file',
    bash_command='''
    echo "Processing file: {{ ti.xcom_pull(task_ids='get_file_info')['filename'] }}"
    echo "File size: {{ ti.xcom_pull(task_ids='get_file_info')['size_mb'] }}MB"
    echo "File format: {{ ti.xcom_pull(task_ids='get_file_info')['format'] }}"
    echo "Processing completed successfully"
    ''',
    dag=templating_dag
)

# Set dependencies
file_info_task >> validate_task >> process_file_task

# =============================================================================
# Example 4: Complex Data Types
# =============================================================================


def extract_complex_data():
    """Extract complex nested data structure"""
    return {
        "metadata": {
            "extraction_time": datetime.now().isoformat(),
            "source": "production_db",
            "version": "1.2.3"
        },
        "data": {
            "users": [
                {"id": 1, "name": "Alice", "active": True},
                {"id": 2, "name": "Bob", "active": False},
                {"id": 3, "name": "Charlie", "active": True}
            ],
            "summary": {
                "total_users": 3,
                "active_users": 2,
                "inactive_users": 1
            }
        },
        "quality_metrics": {
            "completeness": 0.95,
            "accuracy": 0.98,
            "consistency": 0.92
        }
    }


def transform_complex_data(**context):
    """Transform the complex data structure"""
    raw_data = context['task_instance'].xcom_pull(task_ids='extract_complex')

    # Extract and transform data
    users = raw_data['data']['users']
    active_users = [user for user in users if user['active']]

    # Create transformed structure
    transformed = {
        "source_metadata": raw_data['metadata'],
        "active_users": active_users,
        "transformation_summary": {
            "original_count": len(users),
            "active_count": len(active_users),
            "filtered_count": len(users) - len(active_users),
            "transformation_time": datetime.now().isoformat()
        },
        "quality_score": sum(raw_data['quality_metrics'].values()) / len(raw_data['quality_metrics'])
    }

    print(f"Transformed {len(users)} users, {len(active_users)} active")
    return transformed


def load_transformed_data(**context):
    """Load the transformed data"""
    transformed_data = context['task_instance'].xcom_pull(
        task_ids='transform_complex')

    # Simulate loading to destination
    active_count = transformed_data['transformation_summary']['active_count']
    quality_score = transformed_data['quality_score']

    print(f"Loading {active_count} active users")
    print(f"Data quality score: {quality_score:.2f}")

    return {
        "load_status": "completed",
        "records_loaded": active_count,
        "quality_score": quality_score
    }


# Create the DAG
complex_data_dag = DAG(
    'complex_data_xcom_example',
    default_args=default_args,
    description='Complex data types in XComs',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['xcom', 'complex', 'etl', 'example']
)

# Define tasks
extract_complex_task = PythonOperator(
    task_id='extract_complex',
    python_callable=extract_complex_data,
    dag=complex_data_dag
)

transform_complex_task = PythonOperator(
    task_id='transform_complex',
    python_callable=transform_complex_data,
    dag=complex_data_dag
)

load_complex_task = PythonOperator(
    task_id='load_complex',
    python_callable=load_transformed_data,
    dag=complex_data_dag
)

# Set dependencies
extract_complex_task >> transform_complex_task >> load_complex_task

# =============================================================================
# Example 5: Error Handling with XComs
# =============================================================================


def risky_operation(**context):
    """Operation that might fail and needs to communicate status"""
    import random

    success = random.choice([True, False])  # Simulate random success/failure

    if success:
        result = {
            "status": "success",
            "data": {"processed_items": 100, "errors": []},
            "message": "Operation completed successfully"
        }
    else:
        result = {
            "status": "failed",
            "data": {"processed_items": 45, "errors": ["Connection timeout", "Invalid data format"]},
            "message": "Operation failed with errors"
        }

    print(f"Operation result: {result['status']}")
    return result


def handle_operation_result(**context):
    """Handle the result of the risky operation"""
    result = context['task_instance'].xcom_pull(task_ids='risky_operation')

    if result['status'] == 'success':
        print(f"Success! Processed {result['data']['processed_items']} items")
        return {"next_action": "continue_pipeline"}
    else:
        print(
            f"Failure! Only processed {result['data']['processed_items']} items")
        print(f"Errors: {result['data']['errors']}")

        # In a real scenario, you might want to trigger alerts or retry logic
        return {"next_action": "retry_operation", "error_count": len(result['data']['errors'])}


# Create the DAG
error_handling_dag = DAG(
    'error_handling_xcom_example',
    default_args=default_args,
    description='Error handling with XComs',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['xcom', 'error-handling', 'example']
)

# Define tasks
risky_task = PythonOperator(
    task_id='risky_operation',
    python_callable=risky_operation,
    dag=error_handling_dag
)

handler_task = PythonOperator(
    task_id='handle_result',
    python_callable=handle_operation_result,
    dag=error_handling_dag
)

# Set dependencies
risky_task >> handler_task
