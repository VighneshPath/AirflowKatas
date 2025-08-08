"""
BashOperator Examples

This file demonstrates various ways to use the BashOperator in Airflow.
Each example shows different command types and configurations.
"""

from datetime import datetime, timedelta
from airflow import DAG
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

# DAG definition
dag = DAG(
    'bash_operator_examples',
    default_args=default_args,
    description='Examples of BashOperator usage',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['examples', 'bash', 'operators'],
)

# Example 1: Simple echo command
simple_echo = BashOperator(
    task_id='simple_echo',
    bash_command='echo "Hello from BashOperator!"',
    dag=dag,
)

# Example 2: Multi-line bash command
multi_line_command = BashOperator(
    task_id='multi_line_command',
    bash_command='''
    echo "Starting multi-line command..."
    date
    echo "Current directory: $(pwd)"
    echo "Available disk space:"
    df -h
    echo "Command completed!"
    ''',
    dag=dag,
)

# Example 3: Command with environment variables
command_with_env = BashOperator(
    task_id='command_with_env',
    bash_command='echo "Hello $CUSTOM_NAME from $CUSTOM_LOCATION!"',
    env={
        'CUSTOM_NAME': 'Airflow User',
        'CUSTOM_LOCATION': 'Data Pipeline'
    },
    dag=dag,
)

# Example 4: File operations
file_operations = BashOperator(
    task_id='file_operations',
    bash_command='''
    # Create a temporary file
    echo "Creating temporary file..."
    echo "Task executed at $(date)" > /tmp/airflow_task_output.txt
    
    # Display file contents
    echo "File contents:"
    cat /tmp/airflow_task_output.txt
    
    # Clean up
    rm /tmp/airflow_task_output.txt
    echo "File cleaned up!"
    ''',
    dag=dag,
)

# Example 5: Command with error handling
command_with_error_handling = BashOperator(
    task_id='command_with_error_handling',
    bash_command='''
    set -e  # Exit on any error
    
    echo "Starting command with error handling..."
    
    # This command will succeed
    echo "Step 1: Success"
    
    # Check if a directory exists, create if not
    if [ ! -d "/tmp/airflow_test" ]; then
        echo "Creating test directory..."
        mkdir -p /tmp/airflow_test
    fi
    
    echo "Step 2: Directory check completed"
    
    # Clean up
    rmdir /tmp/airflow_test 2>/dev/null || true
    echo "Command completed successfully!"
    ''',
    dag=dag,
)

# Example 6: Command with retry configuration
command_with_retries = BashOperator(
    task_id='command_with_retries',
    bash_command='''
    # Simulate a command that might fail occasionally
    if [ $((RANDOM % 3)) -eq 0 ]; then
        echo "Command succeeded!"
        exit 0
    else
        echo "Command failed, will retry..."
        exit 1
    fi
    ''',
    retries=3,
    retry_delay=timedelta(seconds=30),
    dag=dag,
)

# Example 7: Using templated commands (Jinja2)
templated_command = BashOperator(
    task_id='templated_command',
    bash_command='''
    echo "Execution date: {{ ds }}"
    echo "DAG ID: {{ dag.dag_id }}"
    echo "Task ID: {{ task.task_id }}"
    echo "Previous execution date: {{ prev_ds }}"
    ''',
    dag=dag,
)

# Example 8: Command that processes data
data_processing_command = BashOperator(
    task_id='data_processing_command',
    bash_command='''
    echo "Starting data processing simulation..."
    
    # Create sample data
    echo -e "name,age,city\nAlice,25,New York\nBob,30,San Francisco\nCharlie,35,Chicago" > /tmp/sample_data.csv
    
    # Process the data (count lines, show first few lines)
    echo "Data file created. Processing..."
    echo "Total records (including header): $(wc -l < /tmp/sample_data.csv)"
    echo "First 3 lines:"
    head -3 /tmp/sample_data.csv
    
    # Clean up
    rm /tmp/sample_data.csv
    echo "Data processing completed!"
    ''',
    dag=dag,
)

# Set up task dependencies
simple_echo >> multi_line_command >> command_with_env
command_with_env >> [file_operations, command_with_error_handling]
[file_operations, command_with_error_handling] >> command_with_retries
command_with_retries >> templated_command >> data_processing_command
