"""
Custom Operator Examples

This file demonstrates how to create custom operators in Airflow.
It includes examples of different custom operator patterns and best practices.
"""

from datetime import datetime, timedelta
import json
import random
import time
from typing import Any, Dict, Optional

from airflow import DAG
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
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

# DAG definition
dag = DAG(
    'custom_operator_examples',
    default_args=default_args,
    description='Examples of custom operators',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['examples', 'custom', 'operators'],
)


# Example 1: Simple Custom Operator
class HelloWorldOperator(BaseOperator):
    """
    A simple custom operator that prints a customizable greeting message.

    This operator demonstrates the basic structure of a custom operator
    with configurable parameters.
    """

    # Define which fields can use Jinja templating
    template_fields = ('message', 'name')

    @apply_defaults
    def __init__(
        self,
        name: str = "World",
        message: str = "Hello",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.name = name
        self.message = message

    def execute(self, context: Context) -> str:
        """Execute the operator logic"""
        full_message = f"{self.message}, {self.name}!"
        self.log.info(f"Executing HelloWorldOperator: {full_message}")
        print(full_message)
        return full_message


# Example 2: Data Processing Custom Operator
class DataProcessorOperator(BaseOperator):
    """
    Custom operator for data processing with configurable operations.

    This operator demonstrates parameter validation, error handling,
    and returning structured data.
    """

    template_fields = ('input_data', 'operation')

    @apply_defaults
    def __init__(
        self,
        input_data: list,
        operation: str = "sum",
        multiplier: float = 1.0,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.input_data = input_data
        self.operation = operation
        self.multiplier = multiplier

        # Validate operation parameter
        valid_operations = ['sum', 'average', 'max', 'min', 'count']
        if operation not in valid_operations:
            raise ValueError(f"Operation must be one of {valid_operations}")

    def execute(self, context: Context) -> Dict[str, Any]:
        """Execute the data processing operation"""
        self.log.info(f"Processing data with operation: {self.operation}")

        if not self.input_data:
            raise ValueError("Input data cannot be empty")

        # Perform the requested operation
        if self.operation == 'sum':
            result = sum(self.input_data) * self.multiplier
        elif self.operation == 'average':
            result = (sum(self.input_data) /
                      len(self.input_data)) * self.multiplier
        elif self.operation == 'max':
            result = max(self.input_data) * self.multiplier
        elif self.operation == 'min':
            result = min(self.input_data) * self.multiplier
        elif self.operation == 'count':
            result = len(self.input_data) * self.multiplier

        output = {
            'operation': self.operation,
            'input_data': self.input_data,
            'result': result,
            'multiplier': self.multiplier,
            'processed_at': datetime.now().isoformat()
        }

        self.log.info(f"Operation result: {result}")
        return output


# Example 3: File Processing Custom Operator
class FileProcessorOperator(BaseOperator):
    """
    Custom operator for file processing operations.

    This operator demonstrates file handling, cleanup, and
    comprehensive error management.
    """

    template_fields = ('file_path', 'output_path')

    @apply_defaults
    def __init__(
        self,
        file_path: str,
        output_path: Optional[str] = None,
        operation: str = "word_count",
        cleanup: bool = True,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.file_path = file_path
        self.output_path = output_path or f"{file_path}.processed"
        self.operation = operation
        self.cleanup = cleanup

    def execute(self, context: Context) -> Dict[str, Any]:
        """Execute the file processing operation"""
        import os

        self.log.info(f"Processing file: {self.file_path}")

        try:
            # Create sample file if it doesn't exist
            if not os.path.exists(self.file_path):
                self.log.info("Creating sample file for processing")
                with open(self.file_path, 'w') as f:
                    f.write("This is a sample file for processing.\n")
                    f.write("It contains multiple lines of text.\n")
                    f.write("The custom operator will process this content.\n")

            # Read and process the file
            with open(self.file_path, 'r') as f:
                content = f.read()

            # Perform the requested operation
            if self.operation == 'word_count':
                words = content.split()
                result = len(words)
                processed_content = f"Word count: {result}\n"
            elif self.operation == 'line_count':
                lines = content.split('\n')
                result = len([line for line in lines if line.strip()])
                processed_content = f"Line count: {result}\n"
            elif self.operation == 'char_count':
                result = len(content)
                processed_content = f"Character count: {result}\n"
            else:
                raise ValueError(f"Unknown operation: {self.operation}")

            # Write processed content to output file
            with open(self.output_path, 'w') as f:
                f.write(processed_content)
                f.write(f"Processed at: {datetime.now().isoformat()}\n")
                f.write(f"Original content:\n{content}")

            output = {
                'file_path': self.file_path,
                'output_path': self.output_path,
                'operation': self.operation,
                'result': result,
                'file_size': os.path.getsize(self.file_path)
            }

            self.log.info(f"File processing completed. Result: {result}")

            # Cleanup if requested
            if self.cleanup and os.path.exists(self.file_path):
                os.remove(self.file_path)
                self.log.info("Original file cleaned up")

            return output

        except Exception as e:
            self.log.error(f"Error processing file: {str(e)}")
            raise


# Example 4: API Client Custom Operator
class APIClientOperator(BaseOperator):
    """
    Custom operator for API interactions.

    This operator demonstrates HTTP client patterns, retry logic,
    and response handling in custom operators.
    """

    template_fields = ('endpoint', 'payload')

    @apply_defaults
    def __init__(
        self,
        endpoint: str,
        method: str = "GET",
        payload: Optional[Dict] = None,
        timeout: int = 30,
        max_retries: int = 3,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.endpoint = endpoint
        self.method = method.upper()
        self.payload = payload or {}
        self.timeout = timeout
        self.max_retries = max_retries

    def execute(self, context: Context) -> Dict[str, Any]:
        """Execute the API call with retry logic"""
        self.log.info(f"Making {self.method} request to {self.endpoint}")

        # Simulate API call with random success/failure
        for attempt in range(self.max_retries + 1):
            try:
                self.log.info(
                    f"Attempt {attempt + 1} of {self.max_retries + 1}")

                # Simulate API call delay
                time.sleep(random.uniform(0.5, 2.0))

                # Simulate random API responses
                if random.random() < 0.8:  # 80% success rate
                    # Simulate successful response
                    response_data = {
                        'status': 'success',
                        'data': {
                            'id': random.randint(1, 1000),
                            'message': 'API call successful',
                            'timestamp': datetime.now().isoformat()
                        },
                        'metadata': {
                            'endpoint': self.endpoint,
                            'method': self.method,
                            'attempt': attempt + 1
                        }
                    }

                    self.log.info("API call successful")
                    return response_data
                else:
                    # Simulate API error
                    raise Exception(f"API call failed (attempt {attempt + 1})")

            except Exception as e:
                self.log.warning(
                    f"API call attempt {attempt + 1} failed: {str(e)}")
                if attempt == self.max_retries:
                    self.log.error("All API call attempts failed")
                    raise
                else:
                    # Wait before retry
                    time.sleep(2 ** attempt)  # Exponential backoff


# Example 5: Validation Custom Operator
class DataValidatorOperator(BaseOperator):
    """
    Custom operator for data validation.

    This operator demonstrates validation patterns, rule engines,
    and detailed reporting in custom operators.
    """

    template_fields = ('data', 'validation_rules')

    @apply_defaults
    def __init__(
        self,
        data: Any,
        validation_rules: Dict[str, Any],
        strict_mode: bool = False,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.data = data
        self.validation_rules = validation_rules
        self.strict_mode = strict_mode

    def execute(self, context: Context) -> Dict[str, Any]:
        """Execute data validation"""
        self.log.info("Starting data validation")

        validation_results = {
            'validation_id': f"VAL_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'data_type': type(self.data).__name__,
            'rules_applied': list(self.validation_rules.keys()),
            'results': {},
            'summary': {
                'total_rules': len(self.validation_rules),
                'passed': 0,
                'failed': 0,
                'warnings': 0
            }
        }

        # Apply validation rules
        for rule_name, rule_config in self.validation_rules.items():
            self.log.info(f"Applying rule: {rule_name}")

            try:
                result = self._apply_validation_rule(rule_name, rule_config)
                validation_results['results'][rule_name] = result

                if result['status'] == 'passed':
                    validation_results['summary']['passed'] += 1
                elif result['status'] == 'failed':
                    validation_results['summary']['failed'] += 1
                else:
                    validation_results['summary']['warnings'] += 1

            except Exception as e:
                self.log.error(f"Error applying rule {rule_name}: {str(e)}")
                validation_results['results'][rule_name] = {
                    'status': 'error',
                    'message': str(e)
                }
                validation_results['summary']['failed'] += 1

        # Calculate overall validation score
        total_rules = validation_results['summary']['total_rules']
        passed_rules = validation_results['summary']['passed']
        validation_results['summary']['score'] = (
            passed_rules / total_rules) * 100 if total_rules > 0 else 0

        self.log.info(
            f"Validation completed. Score: {validation_results['summary']['score']:.2f}%")

        # Fail task if in strict mode and validation failed
        if self.strict_mode and validation_results['summary']['failed'] > 0:
            raise ValueError(
                f"Validation failed in strict mode. {validation_results['summary']['failed']} rules failed.")

        return validation_results

    def _apply_validation_rule(self, rule_name: str, rule_config: Dict) -> Dict[str, Any]:
        """Apply a single validation rule"""
        rule_type = rule_config.get('type', 'unknown')

        if rule_type == 'not_null':
            if self.data is not None:
                return {'status': 'passed', 'message': 'Data is not null'}
            else:
                return {'status': 'failed', 'message': 'Data is null'}

        elif rule_type == 'type_check':
            expected_type = rule_config.get('expected_type', str)
            if isinstance(self.data, expected_type):
                return {'status': 'passed', 'message': f'Data is of type {expected_type.__name__}'}
            else:
                return {'status': 'failed', 'message': f'Expected {expected_type.__name__}, got {type(self.data).__name__}'}

        elif rule_type == 'range_check' and isinstance(self.data, (int, float)):
            min_val = rule_config.get('min', float('-inf'))
            max_val = rule_config.get('max', float('inf'))
            if min_val <= self.data <= max_val:
                return {'status': 'passed', 'message': f'Value {self.data} is within range [{min_val}, {max_val}]'}
            else:
                return {'status': 'failed', 'message': f'Value {self.data} is outside range [{min_val}, {max_val}]'}

        else:
            return {'status': 'warning', 'message': f'Unknown rule type: {rule_type}'}


# Create task instances using the custom operators

# Task 1: Simple greeting
hello_task = HelloWorldOperator(
    task_id='hello_custom_operator',
    name='Airflow Developer',
    message='Greetings',
    dag=dag,
)

# Task 2: Data processing
data_processor_task = DataProcessorOperator(
    task_id='process_numbers',
    input_data=[10, 20, 30, 40, 50],
    operation='average',
    multiplier=1.5,
    dag=dag,
)

# Task 3: File processing
file_processor_task = FileProcessorOperator(
    task_id='process_file',
    file_path='/tmp/sample_text.txt',
    operation='word_count',
    cleanup=True,
    dag=dag,
)

# Task 4: API client
api_client_task = APIClientOperator(
    task_id='call_api',
    endpoint='https://api.example.com/data',
    method='GET',
    timeout=30,
    max_retries=2,
    dag=dag,
)

# Task 5: Data validation
validation_task = DataValidatorOperator(
    task_id='validate_data',
    data=42,
    validation_rules={
        'not_null': {'type': 'not_null'},
        'type_check': {'type': 'type_check', 'expected_type': int},
        'range_check': {'type': 'range_check', 'min': 0, 'max': 100}
    },
    strict_mode=False,
    dag=dag,
)

# Set up task dependencies
hello_task >> data_processor_task >> file_processor_task
file_processor_task >> [api_client_task, validation_task]
