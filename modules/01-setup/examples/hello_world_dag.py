"""
Hello World DAG - Your First Airflow Workflow

This DAG demonstrates the basic structure of an Airflow workflow with simple tasks
that introduce core concepts like operators, dependencies, and task execution.

Key Learning Points:
- DAG definition and configuration
- Using BashOperator and PythonOperator
- Setting task dependencies
- Task execution order
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# Python function for PythonOperator
def greet_user():
    """
    A simple Python function that will be executed as a task.
    This demonstrates how to use PythonOperator to run Python code.
    """
    print("Hello from Airflow! 🚀")
    print("This is your first Python task in a DAG.")
    return "Greeting completed successfully!"


def print_execution_date(**context):
    """
    Demonstrates how to access Airflow context variables.
    The **context parameter gives access to execution date, DAG run info, etc.
    """
    execution_date = context['execution_date']
    print(f"This DAG run was executed on: {execution_date}")
    print(f"DAG ID: {context['dag'].dag_id}")
    print(f"Task ID: {context['task_instance'].task_id}")
    return f"Execution date: {execution_date}"


# Default arguments that apply to all tasks in this DAG
default_args = {
    'owner': 'airflow-student',           # Who owns this DAG
    'depends_on_past': False,             # Don't depend on previous DAG runs
    'start_date': datetime(2024, 1, 1),   # When this DAG should start running
    # Don't send emails on failure (for now)
    'email_on_failure': False,
    'email_on_retry': False,              # Don't send emails on retry
    'retries': 1,                         # Retry failed tasks once
    'retry_delay': timedelta(minutes=5),  # Wait 5 minutes before retrying
}

# DAG Definition
# This is the main workflow container that holds all our tasks
dag = DAG(
    'hello_world_dag',                    # Unique identifier for this DAG
    default_args=default_args,            # Apply default args to all tasks
    description='A simple Hello World DAG for learning Airflow basics',
    schedule_interval=timedelta(days=1),  # Run once per day
    start_date=datetime(2024, 1, 1),      # When to start running
    catchup=False,                        # Don't run for past dates
    tags=['tutorial', 'beginner'],       # Tags for organization
)

# Task 1: Simple Bash Command
# BashOperator executes bash commands in a subprocess
start_task = BashOperator(
    task_id='start_workflow',             # Unique task identifier within the DAG
    bash_command='echo "Starting Hello World workflow..."',
    dag=dag,                              # Associate this task with our DAG
)

# Task 2: Python Function Execution
# PythonOperator executes a Python function
greet_task = PythonOperator(
    task_id='greet_user',                 # Unique task identifier
    python_callable=greet_user,           # Function to execute
    dag=dag,                              # Associate with DAG
)

# Task 3: Access Airflow Context
# Demonstrates how to use Airflow's built-in context variables
context_task = PythonOperator(
    task_id='print_context',
    python_callable=print_execution_date,
    dag=dag,
)

# Task 4: Multiple Bash Commands
# Shows how to run multiple commands in sequence
info_task = BashOperator(
    task_id='system_info',
    bash_command='''
    echo "System Information:"
    echo "Current date: $(date)"
    echo "Current user: $(whoami)"
    echo "Python version: $(python --version)"
    echo "Airflow task completed successfully!"
    ''',
    dag=dag,
)

# Task 5: Final Cleanup
end_task = BashOperator(
    task_id='end_workflow',
    bash_command='echo "Hello World workflow completed! 🎉"',
    dag=dag,
)

# Define Task Dependencies
# This creates the execution order: start → greet → context → info → end
# The >> operator means "then" (start_task THEN greet_task)
start_task >> greet_task >> context_task >> info_task >> end_task

# Alternative ways to define the same dependencies:
# Method 1: Using set_downstream()
# start_task.set_downstream(greet_task)
# greet_task.set_downstream(context_task)
# context_task.set_downstream(info_task)
# info_task.set_downstream(end_task)

# Method 2: Using set_upstream()
# greet_task.set_upstream(start_task)
# context_task.set_upstream(greet_task)
# info_task.set_upstream(context_task)
# end_task.set_upstream(info_task)

# Method 3: Using lists for multiple dependencies
# start_task >> [greet_task, context_task] >> info_task >> end_task
# This would run greet_task and context_task in parallel after start_task

"""
DAG Structure Visualization:

start_workflow
      ↓
   greet_user
      ↓
  print_context
      ↓
   system_info
      ↓
  end_workflow

When you run this DAG:
1. start_workflow prints a welcome message
2. greet_user executes the Python function
3. print_context shows execution details
4. system_info displays system information
5. end_workflow prints completion message

Each task must complete successfully before the next one starts.
If any task fails, the workflow stops and you can investigate the logs.
"""
