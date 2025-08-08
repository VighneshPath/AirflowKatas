# Tasks & Operators Concepts

## What are Tasks and Operators?

In Airflow, **tasks** are the basic units of work within a DAG, and **operators** define what each task actually does. Think of operators as templates that define the type of work to be performed, while tasks are specific instances of those operators with particular configurations.

## Key Concepts

### Tasks vs Operators

- **Operator**: A class that defines a type of work (e.g., BashOperator, PythonOperator)
- **Task**: An instance of an operator with specific parameters and configuration
- **Task Instance**: A specific run of a task for a particular execution date

### Common Operator Types

#### 1. BashOperator

Executes bash commands on the system where Airflow is running.

```python
from airflow.operators.bash import BashOperator

bash_task = BashOperator(
    task_id='run_bash_command',
    bash_command='echo "Hello from Bash!"',
    dag=dag
)
```

#### 2. PythonOperator

Executes a Python function.

```python
from airflow.operators.python import PythonOperator

def my_python_function():
    print("Hello from Python!")
    return "Task completed"

python_task = PythonOperator(
    task_id='run_python_function',
    python_callable=my_python_function,
    dag=dag
)
```

#### 3. EmailOperator

Sends emails (useful for notifications).

```python
from airflow.operators.email import EmailOperator

email_task = EmailOperator(
    task_id='send_email',
    to=['admin@example.com'],
    subject='Airflow Task Completed',
    html_content='<p>Your task has completed successfully!</p>',
    dag=dag
)
```

## Task Configuration

### Common Task Parameters

All operators inherit from `BaseOperator` and share common parameters:

- **task_id**: Unique identifier for the task within the DAG
- **dag**: The DAG this task belongs to
- **depends_on_past**: Whether task depends on success of previous run
- **retries**: Number of retries on failure
- **retry_delay**: Delay between retries
- **start_date**: When this task should start running
- **end_date**: When this task should stop running
- **trigger_rule**: Conditions for task execution

### Example with Common Parameters

```python
task_with_config = BashOperator(
    task_id='configured_task',
    bash_command='echo "Configured task"',
    retries=3,
    retry_delay=timedelta(minutes=5),
    depends_on_past=False,
    dag=dag
)
```

## Operator Parameters

### BashOperator Specific Parameters

- **bash_command**: The bash command to execute
- **env**: Environment variables for the command
- **cwd**: Working directory for command execution

### PythonOperator Specific Parameters

- **python_callable**: The Python function to execute
- **op_args**: Positional arguments for the function
- **op_kwargs**: Keyword arguments for the function
- **provide_context**: Whether to pass Airflow context to function

## Best Practices

1. **Use descriptive task_ids**: Make them meaningful and unique
2. **Handle errors gracefully**: Use appropriate retry settings
3. **Keep tasks atomic**: Each task should do one thing well
4. **Use appropriate operators**: Choose the right tool for the job
5. **Document your tasks**: Add docstrings and comments

## Task States

Understanding task states is crucial:

- **None**: Task not yet scheduled
- **Scheduled**: Task scheduled for execution
- **Queued**: Task queued by executor
- **Running**: Task currently executing
- **Success**: Task completed successfully
- **Failed**: Task failed
- **Retry**: Task failed but will retry
- **Skipped**: Task was skipped
- **Up for retry**: Task failed and waiting to retry

## Next Steps

Now that you understand the concepts, let's practice with hands-on exercises using different operators!
