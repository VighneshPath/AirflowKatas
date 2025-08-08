# Exercise 1: BashOperator Fundamentals

## Objective

Learn to use BashOperator with different command types and configurations.

## Prerequisites

- Completed Module 2: DAG Fundamentals
- Basic understanding of bash commands

## Exercise Tasks

### Task 1: Create a Simple BashOperator DAG

Create a DAG named `bash_exercise_dag` with the following specifications:

**Requirements:**

- DAG ID: `bash_exercise_dag`
- Schedule: Daily
- Start date: January 1, 2024
- No catchup
- Tags: `['exercise', 'bash']`

**Tasks to implement:**

1. **hello_world**: Simple echo command that prints "Hello World from Airflow!"

2. **system_info**: Multi-line command that displays:

   - Current date and time
   - Current working directory
   - Available disk space (using `df -h`)
   - Current user

3. **create_file**: Create a file at `/tmp/airflow_exercise.txt` with content:

   ```
   Exercise completed at: [current timestamp]
   DAG: bash_exercise_dag
   Task: create_file
   ```

4. **read_file**: Read and display the contents of the file created in the previous task

5. **cleanup**: Remove the file created in step 3

**Task Dependencies:**

```
hello_world >> system_info >> create_file >> read_file >> cleanup
```

### Task 2: Environment Variables and Error Handling

Add these additional tasks to your DAG:

6. **env_demo**: Use environment variables in your bash command

   - Set custom environment variables: `EXERCISE_NAME="Bash Operators"` and `STUDENT_NAME="Your Name"`
   - Echo a message using these variables: "Student $STUDENT_NAME is working on $EXERCISE_NAME"

7. **error_handling**: Implement a bash command with proper error handling

   - Use `set -e` to exit on errors
   - Create a directory `/tmp/test_dir` if it doesn't exist
   - Write a test file in that directory
   - Verify the file was created
   - Clean up the directory and file

8. **conditional_logic**: Implement bash conditional logic
   - Check if it's a weekday or weekend (use `date +%u` - returns 1-7, where 6-7 are weekend)
   - Echo different messages based on the day type
   - Use bash if-else statements

### Task 3: Advanced BashOperator Features

9. **templated_command**: Use Airflow templating in your bash command

   - Display the execution date using `{{ ds }}`
   - Show the DAG run ID using `{{ dag_run.run_id }}`
   - Display the task instance key using `{{ task_instance_key_str }}`

10. **retry_demo**: Create a task that demonstrates retry functionality
    - Configure the task with 2 retries and a 30-second retry delay
    - Implement a bash command that randomly succeeds or fails
    - Use `$RANDOM` to generate random numbers for success/failure logic

## Implementation Guidelines

1. Create your DAG file in the `dags/` directory
2. Name your file `bash_exercise_solution.py`
3. Include proper imports and documentation
4. Use meaningful task IDs and descriptions
5. Add comments explaining complex bash commands

## Validation

Test your DAG by:

1. Placing it in the `dags/` directory
2. Checking the Airflow UI for parsing errors
3. Running the DAG manually to verify all tasks execute correctly

## Expected Output Structure

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Your implementation here
```

## Hints

- Use triple quotes (`'''`) for multi-line bash commands
- Remember to escape special characters if needed
- Use `&&` to chain commands that should all succeed
- Use `||` for fallback commands
- Test your bash commands in a terminal first

## Next Steps

After completing this exercise, move on to Exercise 2: PythonOperator Fundamentals.
