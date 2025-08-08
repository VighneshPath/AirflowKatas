"""
Solution for Exercise 1: BashOperator Fundamentals

This file provides a complete solution for the BashOperator exercise,
demonstrating various bash command types and configurations.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Default arguments for the DAG
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
    'bash_exercise_dag',
    default_args=default_args,
    description='BashOperator exercise solution',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['exercise', 'bash'],
)

# Task 1: Simple hello world
hello_world = BashOperator(
    task_id='hello_world',
    bash_command='echo "Hello World from Airflow!"',
    dag=dag,
)

# Task 2: System information
system_info = BashOperator(
    task_id='system_info',
    bash_command='''
    echo "=== System Information ==="
    echo "Current date and time: $(date)"
    echo "Current working directory: $(pwd)"
    echo "Available disk space:"
    df -h
    echo "Current user: $(whoami)"
    echo "=========================="
    ''',
    dag=dag,
)

# Task 3: Create file
create_file = BashOperator(
    task_id='create_file',
    bash_command='''
    echo "Creating exercise file..."
    cat > /tmp/airflow_exercise.txt << EOF
Exercise completed at: $(date)
DAG: bash_exercise_dag
Task: create_file
EOF
    echo "File created successfully!"
    ''',
    dag=dag,
)

# Task 4: Read file
read_file = BashOperator(
    task_id='read_file',
    bash_command='''
    echo "Reading file contents:"
    echo "======================"
    cat /tmp/airflow_exercise.txt
    echo "======================"
    ''',
    dag=dag,
)

# Task 5: Cleanup
cleanup = BashOperator(
    task_id='cleanup',
    bash_command='''
    echo "Cleaning up exercise file..."
    if [ -f "/tmp/airflow_exercise.txt" ]; then
        rm /tmp/airflow_exercise.txt
        echo "File removed successfully!"
    else
        echo "File not found, nothing to clean up."
    fi
    ''',
    dag=dag,
)

# Task 6: Environment variables demo
env_demo = BashOperator(
    task_id='env_demo',
    bash_command='echo "Student $STUDENT_NAME is working on $EXERCISE_NAME"',
    env={
        'EXERCISE_NAME': 'Bash Operators',
        'STUDENT_NAME': 'Your Name'
    },
    dag=dag,
)

# Task 7: Error handling
error_handling = BashOperator(
    task_id='error_handling',
    bash_command='''
    set -e  # Exit on any error
    
    echo "Starting error handling demonstration..."
    
    # Create directory if it doesn't exist
    if [ ! -d "/tmp/test_dir" ]; then
        echo "Creating test directory..."
        mkdir -p /tmp/test_dir
    else
        echo "Test directory already exists"
    fi
    
    # Write test file
    echo "Creating test file..."
    echo "Test file created at $(date)" > /tmp/test_dir/test_file.txt
    
    # Verify file was created
    if [ -f "/tmp/test_dir/test_file.txt" ]; then
        echo "Test file created successfully!"
        echo "File contents:"
        cat /tmp/test_dir/test_file.txt
    else
        echo "Error: Test file was not created!"
        exit 1
    fi
    
    # Clean up
    echo "Cleaning up test directory..."
    rm -rf /tmp/test_dir
    echo "Cleanup completed successfully!"
    ''',
    dag=dag,
)

# Task 8: Conditional logic
conditional_logic = BashOperator(
    task_id='conditional_logic',
    bash_command='''
    # Get day of week (1=Monday, 7=Sunday)
    DAY_OF_WEEK=$(date +%u)
    
    echo "Today is day number: $DAY_OF_WEEK"
    
    if [ $DAY_OF_WEEK -ge 6 ]; then
        echo "It's the weekend! Time to relax and maybe learn some Airflow!"
    else
        echo "It's a weekday! Perfect time for productive Airflow development!"
    fi
    
    # Additional day-specific messages
    case $DAY_OF_WEEK in
        1) echo "Monday: Start of a new week of data pipelines!" ;;
        2) echo "Tuesday: Keep building those workflows!" ;;
        3) echo "Wednesday: Midweek momentum!" ;;
        4) echo "Thursday: Almost there!" ;;
        5) echo "Friday: Finish strong!" ;;
        6) echo "Saturday: Weekend coding session?" ;;
        7) echo "Sunday: Rest day or review day?" ;;
    esac
    ''',
    dag=dag,
)

# Task 9: Templated command
templated_command = BashOperator(
    task_id='templated_command',
    bash_command='''
    echo "=== Airflow Template Variables ==="
    echo "Execution date: {{ ds }}"
    echo "DAG run ID: {{ dag_run.run_id }}"
    echo "Task instance key: {{ task_instance_key_str }}"
    echo "DAG ID: {{ dag.dag_id }}"
    echo "Task ID: {{ task.task_id }}"
    echo "Previous execution date: {{ prev_ds }}"
    echo "Next execution date: {{ next_ds }}"
    echo "=================================="
    ''',
    dag=dag,
)

# Task 10: Retry demonstration
retry_demo = BashOperator(
    task_id='retry_demo',
    bash_command='''
    echo "Starting retry demonstration..."
    
    # Generate random number between 1 and 10
    RANDOM_NUM=$((RANDOM % 10 + 1))
    echo "Generated random number: $RANDOM_NUM"
    
    # Succeed if number is greater than 3 (70% success rate)
    if [ $RANDOM_NUM -gt 3 ]; then
        echo "Success! Random number $RANDOM_NUM is greater than 3"
        echo "Task completed successfully!"
        exit 0
    else
        echo "Failure! Random number $RANDOM_NUM is not greater than 3"
        echo "This task will retry..."
        exit 1
    fi
    ''',
    retries=2,
    retry_delay=timedelta(seconds=30),
    dag=dag,
)

# Set up task dependencies as specified in the exercise
hello_world >> system_info >> create_file >> read_file >> cleanup

# Additional tasks run in parallel after the main sequence
cleanup >> [env_demo, error_handling, conditional_logic]
[env_demo, error_handling, conditional_logic] >> templated_command >> retry_demo
