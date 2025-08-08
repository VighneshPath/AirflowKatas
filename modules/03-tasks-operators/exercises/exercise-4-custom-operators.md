# Exercise 4: Building Custom Operators

## Objective

Learn to create custom operators by implementing your own operator classes with proper inheritance, parameter handling, and best practices.

## Prerequisites

- Completed Exercise 1: BashOperator Fundamentals
- Completed Exercise 2: PythonOperator Fundamentals
- Understanding of Python classes and inheritance

## Exercise Overview

In this exercise, you'll create several custom operators that demonstrate different patterns and use cases. Custom operators are useful when you need reusable functionality that doesn't fit well into existing operators.

## Exercise Tasks

### Task 1: Create a Simple Custom Operator

Create a custom operator called `MathOperator` that performs mathematical operations on two numbers.

**Requirements:**

- Inherit from `BaseOperator`
- Accept parameters: `num1`, `num2`, `operation`
- Support operations: `add`, `subtract`, `multiply`, `divide`
- Include proper error handling for division by zero
- Return the calculation result
- Use `@apply_defaults` decorator

**Implementation Template:**

```python
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.context import Context

class MathOperator(BaseOperator):
    """Custom operator for mathematical operations"""

    @apply_defaults
    def __init__(
        self,
        num1: float,
        num2: float,
        operation: str,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        # Your initialization code here

    def execute(self, context: Context):
        # Your execution logic here
        pass
```

### Task 2: Create a Data Processing Custom Operator

Build a `CSVProcessorOperator` that reads, processes, and writes CSV data.

**Requirements:**

- Accept parameters: `input_file`, `output_file`, `processing_function`
- Read CSV data from input file
- Apply a processing function to each row
- Write processed data to output file
- Include error handling for file operations
- Return processing statistics (rows processed, errors, etc.)
- Support templating for file paths

**Processing Functions to Support:**

- `uppercase_names`: Convert name fields to uppercase
- `calculate_age`: Calculate age from birth_date
- `filter_active`: Keep only active records
- `add_timestamp`: Add processing timestamp to each row

### Task 3: Create an HTTP Client Custom Operator

Implement an `HTTPOperator` that makes HTTP requests with retry logic and response validation.

**Requirements:**

- Accept parameters: `url`, `method`, `headers`, `payload`, `expected_status_codes`
- Support GET, POST, PUT, DELETE methods
- Implement retry logic with exponential backoff
- Validate response status codes
- Parse JSON responses
- Include comprehensive logging
- Return response data and metadata

**Advanced Features:**

- Support for authentication (API keys, basic auth)
- Request timeout configuration
- Response caching (optional)
- Custom response validators

### Task 4: Create a Data Validation Custom Operator

Build a `DataQualityOperator` that performs comprehensive data quality checks.

**Requirements:**

- Accept parameters: `data_source`, `validation_rules`, `fail_on_error`
- Support multiple data sources (dict, list, file path)
- Implement validation rules:
  - `null_check`: Check for null/empty values
  - `type_check`: Validate data types
  - `range_check`: Validate numeric ranges
  - `format_check`: Validate string formats (email, phone, etc.)
  - `uniqueness_check`: Check for duplicate values
- Generate detailed validation reports
- Support both strict and lenient validation modes

### Task 5: Create a Notification Custom Operator

Implement a `NotificationOperator` that sends notifications through multiple channels.

**Requirements:**

- Accept parameters: `message`, `channels`, `severity`, `metadata`
- Support notification channels:
  - `console`: Print to console/logs
  - `email`: Send email (simulate)
  - `slack`: Send Slack message (simulate)
  - `webhook`: Send HTTP webhook
- Include message templating with Airflow context
- Support different severity levels (info, warning, error)
- Include retry logic for failed notifications

## Implementation Guidelines

### DAG Structure

Create a DAG named `custom_operators_exercise` that demonstrates all your custom operators:

```python
from datetime import datetime, timedelta
from airflow import DAG

default_args = {
    'owner': 'airflow-kata',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'custom_operators_exercise',
    default_args=default_args,
    description='Custom operators exercise',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['exercise', 'custom-operators'],
)
```

### Best Practices to Follow

1. **Proper Inheritance**: Always inherit from `BaseOperator`
2. **Parameter Validation**: Validate parameters in `__init__`
3. **Error Handling**: Use try-catch blocks and meaningful error messages
4. **Logging**: Use `self.log` for consistent logging
5. **Documentation**: Include docstrings and type hints
6. **Templating**: Use `template_fields` for Jinja2 templating support
7. **Return Values**: Return serializable data structures

### Example Task Usage

```python
# Math operator example
math_task = MathOperator(
    task_id='calculate_result',
    num1=10,
    num2=5,
    operation='multiply',
    dag=dag,
)

# CSV processor example
csv_task = CSVProcessorOperator(
    task_id='process_csv',
    input_file='/tmp/input.csv',
    output_file='/tmp/output.csv',
    processing_function='uppercase_names',
    dag=dag,
)

# HTTP operator example
http_task = HTTPOperator(
    task_id='api_call',
    url='https://api.example.com/data',
    method='GET',
    expected_status_codes=[200, 201],
    dag=dag,
)
```

## Testing Your Operators

### Unit Testing

Create unit tests for your operators:

```python
import unittest
from unittest.mock import patch, MagicMock

class TestMathOperator(unittest.TestCase):
    def test_addition(self):
        operator = MathOperator(
            task_id='test_add',
            num1=5,
            num2=3,
            operation='add'
        )
        result = operator.execute({})
        self.assertEqual(result, 8)

    def test_division_by_zero(self):
        operator = MathOperator(
            task_id='test_divide_zero',
            num1=5,
            num2=0,
            operation='divide'
        )
        with self.assertRaises(ValueError):
            operator.execute({})
```

### Integration Testing

Test your operators in the Airflow environment:

1. Place your DAG in the `dags/` directory
2. Check for parsing errors in the Airflow UI
3. Run individual tasks to verify functionality
4. Check task logs for proper execution

## Validation Checklist

- [ ] All operators inherit from `BaseOperator`
- [ ] Proper use of `@apply_defaults` decorator
- [ ] Parameter validation in `__init__` methods
- [ ] Comprehensive error handling in `execute` methods
- [ ] Meaningful logging throughout execution
- [ ] Return values are JSON serializable
- [ ] Template fields are properly defined
- [ ] Docstrings and type hints are included
- [ ] DAG parses without errors
- [ ] All tasks execute successfully

## Expected Outputs

Your custom operators should produce clear, informative output:

```
[2024-01-15 10:30:45] INFO - MathOperator: Performing multiply operation
[2024-01-15 10:30:45] INFO - MathOperator: 10 * 5 = 50
[2024-01-15 10:30:45] INFO - MathOperator: Operation completed successfully

[2024-01-15 10:30:46] INFO - CSVProcessorOperator: Processing file /tmp/input.csv
[2024-01-15 10:30:46] INFO - CSVProcessorOperator: Applied uppercase_names to 100 rows
[2024-01-15 10:30:46] INFO - CSVProcessorOperator: Output written to /tmp/output.csv
```

## Bonus Challenges

If you complete the basic requirements, try these advanced features:

1. **Sensor Custom Operator**: Create a custom sensor that waits for specific conditions
2. **Batch Processing**: Add support for processing multiple files or datasets
3. **Configuration Management**: Use Airflow Variables and Connections
4. **Metrics Collection**: Add custom metrics and monitoring
5. **Plugin Integration**: Package your operators as an Airflow plugin

## Common Pitfalls to Avoid

- **Forgetting `super().__init__()`**: Always call parent constructor
- **Non-serializable Returns**: Ensure return values can be JSON serialized
- **Missing Error Handling**: Always handle potential exceptions
- **Hardcoded Values**: Use parameters instead of hardcoded values
- **Poor Logging**: Use `self.log` instead of `print()` statements
- **Template Field Mistakes**: Don't forget to define `template_fields`

## Solution Structure

Your solution should include:

1. **Custom operator classes** with proper inheritance
2. **DAG file** demonstrating all operators
3. **Test data files** (CSV, JSON) for testing
4. **Unit tests** for critical functionality
5. **Documentation** explaining each operator's purpose

## Next Steps

After completing this exercise, you'll have a solid understanding of custom operators. Consider exploring:

1. **Airflow Plugins**: Package your operators for reuse
2. **Provider Packages**: Study how official providers implement operators
3. **Advanced Patterns**: Sensors, hooks, and complex operator interactions
4. **Performance Optimization**: Efficient resource usage in operators

This exercise prepares you for Module 4: Scheduling & Dependencies, where you'll learn to orchestrate complex workflows using your custom operators.
