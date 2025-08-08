"""
Solution: My First DAG - Daily Data Processing Workflow

This solution demonstrates the complete implementation of Exercise 1,
showing best practices for DAG structure, task definition, and dependency management.

Key Components Explained:
- DAG configuration with proper defaults
- Mix of BashOperator and PythonOperator tasks
- Linear task dependencies
- Error handling and logging
- Return values from Python functions
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def process_orders():
    """
    Process new orders and return count.

    In a real scenario, this would connect to a database or API
    to fetch and process actual orders. Here we simulate the process.
    """
    print("Processing new orders...")

    # Simulate processing orders 1001-1005
    order_ids = [1001, 1002, 1003, 1004, 1005]

    for order_id in order_ids:
        print(f"Processing order {order_id}")

    orders_processed = len(order_ids)
    print(f"Successfully processed {orders_processed} orders")

    # Return value can be accessed by downstream tasks via XCom
    return orders_processed


def generate_report():
    """
    Generate daily summary report.

    This function creates a simple report with current date/time.
    In production, this might generate actual reports with data analysis.
    """
    print("Generating daily summary report...")

    # Get current date and time
    current_time = datetime.now()
    print(f"Report generated at: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # In a real scenario, you might:
    # - Query database for daily statistics
    # - Create charts or visualizations
    # - Save report to file system or cloud storage

    report_status = "Report generated successfully"
    print(report_status)

    return report_status


# Default arguments applied to all tasks in this DAG
default_args = {
    'owner': 'airflow-student',           # Replace with your name
    'depends_on_past': False,             # Each run is independent
    'start_date': datetime(2024, 1, 1),   # When DAG should start
    'email_on_failure': False,            # No email alerts for this exercise
    'email_on_retry': False,              # No retry email alerts
    'retries': 2,                         # Retry failed tasks twice
    'retry_delay': timedelta(minutes=10),  # Wait 10 minutes between retries
}

# DAG Definition
dag = DAG(
    'my_first_dag',                                    # Unique DAG identifier
    # Apply defaults to all tasks
    default_args=default_args,
    description='My first Airflow DAG for daily data processing',
    schedule_interval=timedelta(days=1),               # Run once per day
    start_date=datetime(2024, 1, 1),                   # Start date
    catchup=False,                                     # Don't backfill past dates
    tags=['exercise', 'beginner', 'daily'],           # Organization tags
)

# Task 1: System Health Check
system_check = BashOperator(
    task_id='system_check',
    bash_command='''
    echo "System health check: All systems operational"
    echo "Checking disk space:"
    df -h
    echo "System check completed successfully"
    ''',
    dag=dag,
)

# Task 2: Backup Yesterday's Data
backup_data = BashOperator(
    task_id='backup_data',
    bash_command='''
    echo "Backing up yesterday's data..."
    echo "Backup started at: $(date)"
    echo "Backup completed successfully"
    ''',
    dag=dag,
)

# Task 3: Process New Orders
process_orders_task = PythonOperator(
    task_id='process_orders',
    python_callable=process_orders,
    dag=dag,
)

# Task 4: Generate Summary Report
generate_report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag,
)

# Task 5: Send Completion Notification
send_notification = BashOperator(
    task_id='send_notification',
    bash_command='''
    echo "Daily processing completed successfully!"
    echo "Notification sent to operations team"
    echo "Workflow finished at: $(date)"
    ''',
    dag=dag,
)

# Define Task Dependencies
# Creates linear execution: system_check → backup_data → process_orders → generate_report → send_notification
system_check >> backup_data >> process_orders_task >> generate_report_task >> send_notification

"""
DAG Execution Flow:

1. system_check
   - Verifies system health
   - Checks disk space
   - Must complete before backup starts

2. backup_data
   - Simulates backing up previous day's data
   - Shows timestamp of backup operation
   - Prerequisite for order processing

3. process_orders
   - Processes orders 1001-1005
   - Returns count of processed orders
   - Core business logic task

4. generate_report
   - Creates daily summary report
   - Includes timestamp and status
   - Depends on order processing completion

5. send_notification
   - Sends completion notification
   - Final step in the workflow
   - Confirms entire process finished

Key Learning Points from This Solution:

1. **DAG Structure**: Clear separation of configuration, task definition, and dependencies

2. **Task Types**: 
   - BashOperator for system commands and simple operations
   - PythonOperator for complex logic and data processing

3. **Error Handling**: 
   - Retry configuration in default_args
   - Each task can fail independently without affecting DAG structure

4. **Dependencies**: 
   - Linear flow ensures proper execution order
   - Each task waits for previous task to complete successfully

5. **Logging**: 
   - Print statements in Python functions appear in task logs
   - Bash echo commands provide execution feedback

6. **Return Values**: 
   - Python functions can return values stored in XCom
   - Useful for passing data between tasks (covered in later modules)

7. **Best Practices**:
   - Descriptive task IDs and function names
   - Comprehensive comments and docstrings
   - Proper error handling configuration
   - Meaningful log messages

This DAG demonstrates the fundamental pattern you'll use in most Airflow workflows:
preparation → processing → reporting → notification
"""
