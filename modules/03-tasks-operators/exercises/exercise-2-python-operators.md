# Exercise 2: PythonOperator Fundamentals

## Objective

Master the PythonOperator by creating functions with different parameter types and configurations.

## Prerequisites

- Completed Exercise 1: BashOperator Fundamentals
- Basic Python programming knowledge

## Exercise Tasks

### Task 1: Create a PythonOperator DAG

Create a DAG named `python_exercise_dag` with the following specifications:

**Requirements:**

- DAG ID: `python_exercise_dag`
- Schedule: Daily
- Start date: January 1, 2024
- No catchup
- Tags: `['exercise', 'python']`

**Tasks to implement:**

1. **simple_greeting**: Create a function that prints a personalized greeting

   - Function should print current timestamp
   - Return a success message

2. **calculate_stats**: Function with positional arguments

   - Accept a list of numbers as an argument
   - Calculate and print: sum, average, min, max
   - Return a dictionary with the calculated statistics
   - Use `op_args` to pass `[10, 25, 30, 45, 50, 15, 35]`

3. **process_user_data**: Function with keyword arguments
   - Accept user information: name, age, department, salary
   - Print formatted user information
   - Calculate annual bonus (10% of salary)
   - Return user data with bonus included
   - Use `op_kwargs` to pass sample user data

### Task 2: Context and Data Processing

4. **context_explorer**: Function that uses Airflow context

   - Access and print execution_date, dag_id, task_id
   - Print the previous and next execution dates
   - Return a summary of context information

5. **data_transformer**: Data processing function

   - Generate sample data (list of dictionaries with: id, name, score, active)
   - Filter records where score > 75 and active = True
   - Sort by score (descending)
   - Print summary statistics
   - Return processed data

6. **file_processor**: Function that works with files
   - Create a CSV file with sample data in `/tmp/`
   - Read the file and process the data
   - Calculate summary statistics
   - Clean up the file
   - Return processing results

### Task 3: Advanced Python Functions

7. **error_handler**: Function with comprehensive error handling

   - Implement try-catch blocks
   - Simulate different types of errors (ValueError, FileNotFoundError)
   - Use random logic to trigger different error scenarios
   - Return appropriate error or success messages

8. **api_simulator**: Function that simulates API interactions

   - Create mock API response data
   - Process the response (transform, validate, filter)
   - Handle potential API errors
   - Return processed results

9. **report_generator**: Function that creates a summary report
   - Combine data from previous tasks (use mock data)
   - Generate a formatted report
   - Include timestamps, statistics, and summaries
   - Return the complete report

### Task 4: Function Chaining and Dependencies

10. **data_validator**: Function that validates data quality
    - Check for missing values, duplicates, invalid formats
    - Return validation results and data quality score
    - This should run before other processing tasks

**Task Dependencies:**

```
simple_greeting >> calculate_stats >> process_user_data
context_explorer >> data_transformer >> file_processor
data_validator >> [error_handler, api_simulator] >> report_generator
```

## Implementation Guidelines

1. Create your DAG file as `dags/python_exercise_solution.py`
2. Define all functions before the DAG definition
3. Use type hints where appropriate
4. Include docstrings for all functions
5. Add proper error handling and logging
6. Use meaningful variable names

## Function Templates

```python
def simple_greeting():
    """Simple function that prints a greeting"""
    # Your implementation here
    pass

def calculate_stats(numbers):
    """Calculate statistics for a list of numbers"""
    # Your implementation here
    pass

def process_user_data(name, age, department, salary):
    """Process user information and calculate bonus"""
    # Your implementation here
    pass

def context_explorer(**context):
    """Explore Airflow context variables"""
    # Your implementation here
    pass
```

## Expected Outputs

Your functions should produce clear, informative output such as:

```
=== Simple Greeting ===
Hello! Current time: 2024-01-15 10:30:45
Task executed successfully!

=== Calculate Stats ===
Processing numbers: [10, 25, 30, 45, 50, 15, 35]
Sum: 210
Average: 30.0
Min: 10
Max: 50

=== User Data Processing ===
Processing user: John Doe
Age: 30, Department: Engineering
Salary: $75,000
Annual Bonus: $7,500
```

## Validation

Test your implementation by:

1. Checking for syntax errors in the Airflow UI
2. Running individual tasks to verify output
3. Checking task logs for proper execution
4. Verifying return values are properly handled

## Common Pitfalls to Avoid

- Don't forget to import required modules at the top
- Remember that functions must be defined before the DAG
- Use `**context` parameter when accessing Airflow context
- Handle exceptions gracefully to prevent task failures
- Ensure functions return serializable data types

## Bonus Challenges

If you complete the basic tasks, try these additional challenges:

1. **Dynamic Task Generation**: Create a function that generates tasks dynamically
2. **Cross-Task Communication**: Use return values from one task in another
3. **External Library Integration**: Use pandas or requests in your functions
4. **Custom Logging**: Implement custom logging within your functions

## Next Steps

After completing this exercise, move on to Exercise 3: Mixed Operator Workflows.
