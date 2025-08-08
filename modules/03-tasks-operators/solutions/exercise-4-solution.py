"""
Solution for Exercise 4: Building Custom Operators

This file provides complete solutions for all custom operators requested
in the exercise, demonstrating best practices and proper implementation patterns.
"""

from datetime import datetime, timedelta
import json
import csv
import os
import time
import random
from typing import Any, Dict, List, Optional, Union
import re

from airflow import DAG
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.context import Context

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
    'custom_operators_exercise',
    default_args=default_args,
    description='Custom operators exercise solution',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['exercise', 'custom-operators'],
)


# Task 1: MathOperator
class MathOperator(BaseOperator):
    """
    Custom operator for mathematical operations on two numbers.

    Supports basic arithmetic operations with proper error handling.
    """

    template_fields = ('num1', 'num2', 'operation')

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
        self.num1 = float(num1)
        self.num2 = float(num2)
        self.operation = operation.lower()

        # Validate operation
        valid_operations = ['add', 'subtract', 'multiply', 'divide']
        if self.operation not in valid_operations:
            raise ValueError(f"Operation must be one of {valid_operations}")

    def execute(self, context: Context) -> Dict[str, Any]:
        """Execute the mathematical operation"""
        self.log.info(f"MathOperator: Performing {self.operation} operation")
        self.log.info(
            f"MathOperator: {self.num1} {self._get_symbol()} {self.num2}")

        try:
            if self.operation == 'add':
                result = self.num1 + self.num2
            elif self.operation == 'subtract':
                result = self.num1 - self.num2
            elif self.operation == 'multiply':
                result = self.num1 * self.num2
            elif self.operation == 'divide':
                if self.num2 == 0:
                    raise ValueError("Division by zero is not allowed")
                result = self.num1 / self.num2

            self.log.info(f"MathOperator: Result = {result}")

            return {
                'num1': self.num1,
                'num2': self.num2,
                'operation': self.operation,
                'result': result,
                'calculated_at': datetime.now().isoformat()
            }

        except Exception as e:
            self.log.error(f"MathOperator: Error during calculation: {str(e)}")
            raise

    def _get_symbol(self) -> str:
        """Get the mathematical symbol for the operation"""
        symbols = {
            'add': '+',
            'subtract': '-',
            'multiply': '*',
            'divide': '/'
        }
        return symbols.get(self.operation, '?')


# Task 2: CSVProcessorOperator
class CSVProcessorOperator(BaseOperator):
    """
    Custom operator for reading, processing, and writing CSV data.

    Supports various processing functions and includes comprehensive error handling.
    """

    template_fields = ('input_file', 'output_file', 'processing_function')

    @apply_defaults
    def __init__(
        self,
        input_file: str,
        output_file: str,
        processing_function: str,
        create_sample_data: bool = True,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.input_file = input_file
        self.output_file = output_file
        self.processing_function = processing_function
        self.create_sample_data = create_sample_data

        # Validate processing function
        valid_functions = ['uppercase_names',
                           'calculate_age', 'filter_active', 'add_timestamp']
        if processing_function not in valid_functions:
            raise ValueError(
                f"Processing function must be one of {valid_functions}")

    def execute(self, context: Context) -> Dict[str, Any]:
        """Execute CSV processing"""
        self.log.info(
            f"CSVProcessorOperator: Processing file {self.input_file}")

        try:
            # Create sample data if requested and file doesn't exist
            if self.create_sample_data and not os.path.exists(self.input_file):
                self._create_sample_csv()

            # Read CSV data
            data = self._read_csv()
            self.log.info(f"CSVProcessorOperator: Read {len(data)} rows")

            # Process data
            processed_data, stats = self._process_data(data)
            self.log.info(
                f"CSVProcessorOperator: Processed {len(processed_data)} rows")

            # Write processed data
            self._write_csv(processed_data)
            self.log.info(
                f"CSVProcessorOperator: Output written to {self.output_file}")

            return {
                'input_file': self.input_file,
                'output_file': self.output_file,
                'processing_function': self.processing_function,
                'rows_read': len(data),
                'rows_processed': len(processed_data),
                'processing_stats': stats,
                'processed_at': datetime.now().isoformat()
            }

        except Exception as e:
            self.log.error(
                f"CSVProcessorOperator: Error during processing: {str(e)}")
            raise

    def _create_sample_csv(self):
        """Create sample CSV data for testing"""
        sample_data = [
            {'name': 'alice johnson', 'birth_date': '1990-05-15',
                'active': 'true', 'salary': '60000'},
            {'name': 'bob smith', 'birth_date': '1985-12-03',
                'active': 'true', 'salary': '75000'},
            {'name': 'charlie brown', 'birth_date': '1992-08-22',
                'active': 'false', 'salary': '55000'},
            {'name': 'diana prince', 'birth_date': '1988-03-10',
                'active': 'true', 'salary': '80000'},
            {'name': 'eve adams', 'birth_date': '1995-11-07',
                'active': 'false', 'salary': '50000'}
        ]

        os.makedirs(os.path.dirname(self.input_file), exist_ok=True)
        with open(self.input_file, 'w', newline='') as csvfile:
            fieldnames = ['name', 'birth_date', 'active', 'salary']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(sample_data)

        self.log.info("CSVProcessorOperator: Created sample CSV data")

    def _read_csv(self) -> List[Dict]:
        """Read CSV file and return data as list of dictionaries"""
        data = []
        with open(self.input_file, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                data.append(row)
        return data

    def _write_csv(self, data: List[Dict]):
        """Write processed data to CSV file"""
        if not data:
            return

        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        with open(self.output_file, 'w', newline='') as csvfile:
            fieldnames = data[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)

    def _process_data(self, data: List[Dict]) -> tuple:
        """Process data according to the specified function"""
        processed_data = []
        stats = {'errors': 0, 'processed': 0}

        for row in data:
            try:
                if self.processing_function == 'uppercase_names':
                    row['name'] = row['name'].upper()
                elif self.processing_function == 'calculate_age':
                    birth_date = datetime.strptime(
                        row['birth_date'], '%Y-%m-%d')
                    age = (datetime.now() - birth_date).days // 365
                    row['age'] = str(age)
                elif self.processing_function == 'filter_active':
                    if row['active'].lower() != 'true':
                        continue  # Skip inactive records
                elif self.processing_function == 'add_timestamp':
                    row['processed_at'] = datetime.now().isoformat()

                processed_data.append(row)
                stats['processed'] += 1

            except Exception as e:
                self.log.warning(f"Error processing row: {str(e)}")
                stats['errors'] += 1

        return processed_data, stats


# Task 3: HTTPOperator
class HTTPOperator(BaseOperator):
    """
    Custom operator for making HTTP requests with retry logic and response validation.

    Supports multiple HTTP methods, authentication, and comprehensive error handling.
    """

    template_fields = ('url', 'headers', 'payload')

    @apply_defaults
    def __init__(
        self,
        url: str,
        method: str = 'GET',
        headers: Optional[Dict] = None,
        payload: Optional[Dict] = None,
        expected_status_codes: Optional[List[int]] = None,
        timeout: int = 30,
        max_retries: int = 3,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.url = url
        self.method = method.upper()
        self.headers = headers or {}
        self.payload = payload or {}
        self.expected_status_codes = expected_status_codes or [200]
        self.timeout = timeout
        self.max_retries = max_retries

        # Validate method
        valid_methods = ['GET', 'POST', 'PUT', 'DELETE', 'PATCH']
        if self.method not in valid_methods:
            raise ValueError(f"Method must be one of {valid_methods}")

    def execute(self, context: Context) -> Dict[str, Any]:
        """Execute HTTP request with retry logic"""
        self.log.info(
            f"HTTPOperator: Making {self.method} request to {self.url}")

        for attempt in range(self.max_retries + 1):
            try:
                self.log.info(
                    f"HTTPOperator: Attempt {attempt + 1} of {self.max_retries + 1}")

                # Simulate HTTP request (in real implementation, use requests library)
                response_data = self._simulate_http_request()

                # Validate response status
                if response_data['status_code'] not in self.expected_status_codes:
                    raise Exception(
                        f"Unexpected status code: {response_data['status_code']}")

                self.log.info(
                    f"HTTPOperator: Request successful (status: {response_data['status_code']})")

                return {
                    'url': self.url,
                    'method': self.method,
                    'status_code': response_data['status_code'],
                    'response_data': response_data['data'],
                    'headers_sent': self.headers,
                    'payload_sent': self.payload,
                    'attempt': attempt + 1,
                    'requested_at': datetime.now().isoformat()
                }

            except Exception as e:
                self.log.warning(
                    f"HTTPOperator: Attempt {attempt + 1} failed: {str(e)}")
                if attempt == self.max_retries:
                    self.log.error("HTTPOperator: All attempts failed")
                    raise
                else:
                    # Exponential backoff
                    wait_time = 2 ** attempt
                    self.log.info(
                        f"HTTPOperator: Waiting {wait_time} seconds before retry")
                    time.sleep(wait_time)

    def _simulate_http_request(self) -> Dict[str, Any]:
        """Simulate HTTP request (replace with actual HTTP client in production)"""
        # Simulate network delay
        time.sleep(random.uniform(0.1, 1.0))

        # Simulate random responses
        if random.random() < 0.8:  # 80% success rate
            return {
                'status_code': random.choice(self.expected_status_codes),
                'data': {
                    'message': 'Request successful',
                    'timestamp': datetime.now().isoformat(),
                    'method': self.method,
                    'url': self.url
                }
            }
        else:
            # Simulate error responses
            error_codes = [400, 401, 403, 404, 500, 502, 503]
            return {
                'status_code': random.choice(error_codes),
                'data': {'error': 'Simulated error response'}
            }


# Task 4: DataQualityOperator
class DataQualityOperator(BaseOperator):
    """
    Custom operator for comprehensive data quality checks.

    Supports multiple validation rules and detailed reporting.
    """

    template_fields = ('data_source', 'validation_rules')

    @apply_defaults
    def __init__(
        self,
        data_source: Union[str, List, Dict],
        validation_rules: Dict[str, Dict],
        fail_on_error: bool = False,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.data_source = data_source
        self.validation_rules = validation_rules
        self.fail_on_error = fail_on_error

    def execute(self, context: Context) -> Dict[str, Any]:
        """Execute data quality validation"""
        self.log.info("DataQualityOperator: Starting data quality validation")

        # Load data
        data = self._load_data()
        self.log.info(
            f"DataQualityOperator: Loaded data with {len(data) if isinstance(data, list) else 1} records")

        # Run validation rules
        validation_results = {
            'validation_id': f"DQ_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'data_source': str(self.data_source),
            'total_records': len(data) if isinstance(data, list) else 1,
            'rules_applied': list(self.validation_rules.keys()),
            'results': {},
            'summary': {
                'total_rules': len(self.validation_rules),
                'passed': 0,
                'failed': 0,
                'warnings': 0
            }
        }

        # Apply each validation rule
        for rule_name, rule_config in self.validation_rules.items():
            self.log.info(f"DataQualityOperator: Applying rule '{rule_name}'")

            try:
                result = self._apply_validation_rule(
                    data, rule_name, rule_config)
                validation_results['results'][rule_name] = result

                if result['status'] == 'passed':
                    validation_results['summary']['passed'] += 1
                elif result['status'] == 'failed':
                    validation_results['summary']['failed'] += 1
                else:
                    validation_results['summary']['warnings'] += 1

            except Exception as e:
                self.log.error(
                    f"DataQualityOperator: Error applying rule '{rule_name}': {str(e)}")
                validation_results['results'][rule_name] = {
                    'status': 'error',
                    'message': str(e)
                }
                validation_results['summary']['failed'] += 1

        # Calculate quality score
        total_rules = validation_results['summary']['total_rules']
        passed_rules = validation_results['summary']['passed']
        quality_score = (passed_rules / total_rules) * \
            100 if total_rules > 0 else 0
        validation_results['summary']['quality_score'] = round(
            quality_score, 2)

        self.log.info(
            f"DataQualityOperator: Validation completed. Quality score: {quality_score:.2f}%")

        # Fail if configured to do so and there are failures
        if self.fail_on_error and validation_results['summary']['failed'] > 0:
            raise ValueError(
                f"Data quality validation failed. {validation_results['summary']['failed']} rules failed.")

        return validation_results

    def _load_data(self) -> Union[List, Dict]:
        """Load data from various sources"""
        if isinstance(self.data_source, str):
            # Assume it's a file path - create sample data for demo
            return [
                {'id': 1, 'name': 'Alice', 'email': 'alice@example.com',
                    'age': 25, 'salary': 60000},
                {'id': 2, 'name': '', 'email': 'invalid-email',
                    'age': -5, 'salary': 75000},
                {'id': 3, 'name': 'Charlie', 'email': 'charlie@example.com',
                    'age': 35, 'salary': None},
                {'id': 1, 'name': 'Alice Duplicate',
                    'email': 'alice2@example.com', 'age': 28, 'salary': 65000}
            ]
        else:
            return self.data_source

    def _apply_validation_rule(self, data: Union[List, Dict], rule_name: str, rule_config: Dict) -> Dict[str, Any]:
        """Apply a single validation rule"""
        rule_type = rule_config.get('type')
        field = rule_config.get('field')

        if rule_type == 'null_check':
            return self._null_check(data, field)
        elif rule_type == 'type_check':
            expected_type = rule_config.get('expected_type', str)
            return self._type_check(data, field, expected_type)
        elif rule_type == 'range_check':
            min_val = rule_config.get('min', float('-inf'))
            max_val = rule_config.get('max', float('inf'))
            return self._range_check(data, field, min_val, max_val)
        elif rule_type == 'format_check':
            pattern = rule_config.get('pattern', '.*')
            return self._format_check(data, field, pattern)
        elif rule_type == 'uniqueness_check':
            return self._uniqueness_check(data, field)
        else:
            return {'status': 'warning', 'message': f'Unknown rule type: {rule_type}'}

    def _null_check(self, data: List[Dict], field: str) -> Dict[str, Any]:
        """Check for null/empty values"""
        null_count = 0
        total_count = len(data)

        for record in data:
            value = record.get(field)
            if value is None or value == '':
                null_count += 1

        if null_count == 0:
            return {'status': 'passed', 'message': f'No null values found in field "{field}"'}
        else:
            return {
                'status': 'failed',
                'message': f'Found {null_count} null values in field "{field}" out of {total_count} records'
            }

    def _type_check(self, data: List[Dict], field: str, expected_type: type) -> Dict[str, Any]:
        """Check data types"""
        type_errors = 0
        total_count = len(data)

        for record in data:
            value = record.get(field)
            if value is not None and not isinstance(value, expected_type):
                type_errors += 1

        if type_errors == 0:
            return {'status': 'passed', 'message': f'All values in field "{field}" are of type {expected_type.__name__}'}
        else:
            return {
                'status': 'failed',
                'message': f'Found {type_errors} type errors in field "{field}" out of {total_count} records'
            }

    def _range_check(self, data: List[Dict], field: str, min_val: float, max_val: float) -> Dict[str, Any]:
        """Check numeric ranges"""
        range_errors = 0
        total_count = len(data)

        for record in data:
            value = record.get(field)
            if value is not None and isinstance(value, (int, float)):
                if not (min_val <= value <= max_val):
                    range_errors += 1

        if range_errors == 0:
            return {'status': 'passed', 'message': f'All values in field "{field}" are within range [{min_val}, {max_val}]'}
        else:
            return {
                'status': 'failed',
                'message': f'Found {range_errors} range violations in field "{field}" out of {total_count} records'
            }

    def _format_check(self, data: List[Dict], field: str, pattern: str) -> Dict[str, Any]:
        """Check string formats using regex"""
        format_errors = 0
        total_count = len(data)

        for record in data:
            value = record.get(field)
            if value is not None and isinstance(value, str):
                if not re.match(pattern, value):
                    format_errors += 1

        if format_errors == 0:
            return {'status': 'passed', 'message': f'All values in field "{field}" match the required format'}
        else:
            return {
                'status': 'failed',
                'message': f'Found {format_errors} format violations in field "{field}" out of {total_count} records'
            }

    def _uniqueness_check(self, data: List[Dict], field: str) -> Dict[str, Any]:
        """Check for duplicate values"""
        values = []
        for record in data:
            value = record.get(field)
            if value is not None:
                values.append(value)

        unique_values = set(values)
        duplicates = len(values) - len(unique_values)

        if duplicates == 0:
            return {'status': 'passed', 'message': f'All values in field "{field}" are unique'}
        else:
            return {
                'status': 'failed',
                'message': f'Found {duplicates} duplicate values in field "{field}"'
            }


# Task 5: NotificationOperator
class NotificationOperator(BaseOperator):
    """
    Custom operator for sending notifications through multiple channels.

    Supports various notification channels with templating and retry logic.
    """

    template_fields = ('message', 'metadata')

    @apply_defaults
    def __init__(
        self,
        message: str,
        channels: List[str],
        severity: str = 'info',
        metadata: Optional[Dict] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.message = message
        self.channels = channels
        self.severity = severity.lower()
        self.metadata = metadata or {}

        # Validate severity
        valid_severities = ['info', 'warning', 'error']
        if self.severity not in valid_severities:
            raise ValueError(f"Severity must be one of {valid_severities}")

        # Validate channels
        valid_channels = ['console', 'email', 'slack', 'webhook']
        for channel in self.channels:
            if channel not in valid_channels:
                raise ValueError(
                    f"Channel '{channel}' not supported. Valid channels: {valid_channels}")

    def execute(self, context: Context) -> Dict[str, Any]:
        """Execute notification sending"""
        self.log.info(
            f"NotificationOperator: Sending {self.severity} notification to {len(self.channels)} channels")

        # Prepare templated message
        templated_message = self._prepare_message(context)

        notification_results = {
            'notification_id': f"NOTIF_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'message': templated_message,
            'severity': self.severity,
            'channels': self.channels,
            'results': {},
            'summary': {
                'total_channels': len(self.channels),
                'successful': 0,
                'failed': 0
            }
        }

        # Send notification to each channel
        for channel in self.channels:
            try:
                self.log.info(f"NotificationOperator: Sending to {channel}")
                result = self._send_to_channel(channel, templated_message)
                notification_results['results'][channel] = result

                if result['status'] == 'success':
                    notification_results['summary']['successful'] += 1
                else:
                    notification_results['summary']['failed'] += 1

            except Exception as e:
                self.log.error(
                    f"NotificationOperator: Failed to send to {channel}: {str(e)}")
                notification_results['results'][channel] = {
                    'status': 'error',
                    'message': str(e)
                }
                notification_results['summary']['failed'] += 1

        self.log.info(
            f"NotificationOperator: Sent to {notification_results['summary']['successful']} channels successfully")

        return notification_results

    def _prepare_message(self, context: Context) -> str:
        """Prepare message with Airflow context templating"""
        # Simple template replacement (in production, use Jinja2)
        templated_message = self.message

        # Replace common template variables
        replacements = {
            '{{ dag.dag_id }}': context['dag'].dag_id,
            '{{ task.task_id }}': context['task'].task_id,
            '{{ ds }}': context['ds'],
            '{{ execution_date }}': str(context['execution_date'])
        }

        for template, value in replacements.items():
            templated_message = templated_message.replace(template, value)

        return templated_message

    def _send_to_channel(self, channel: str, message: str) -> Dict[str, Any]:
        """Send notification to a specific channel"""
        if channel == 'console':
            return self._send_console(message)
        elif channel == 'email':
            return self._send_email(message)
        elif channel == 'slack':
            return self._send_slack(message)
        elif channel == 'webhook':
            return self._send_webhook(message)
        else:
            raise ValueError(f"Unknown channel: {channel}")

    def _send_console(self, message: str) -> Dict[str, Any]:
        """Send notification to console/logs"""
        severity_prefix = {
            'info': 'INFO',
            'warning': 'WARNING',
            'error': 'ERROR'
        }

        formatted_message = f"[{severity_prefix[self.severity]}] {message}"
        print(formatted_message)
        self.log.info(f"Console notification: {formatted_message}")

        return {
            'status': 'success',
            'channel': 'console',
            'message': 'Notification sent to console'
        }

    def _send_email(self, message: str) -> Dict[str, Any]:
        """Simulate sending email notification"""
        # In production, integrate with EmailOperator or SMTP
        self.log.info(f"Email notification (simulated): {message}")

        # Simulate potential email failure
        if random.random() < 0.1:  # 10% failure rate
            raise Exception("Email server temporarily unavailable")

        return {
            'status': 'success',
            'channel': 'email',
            'message': 'Email notification sent successfully'
        }

    def _send_slack(self, message: str) -> Dict[str, Any]:
        """Simulate sending Slack notification"""
        # In production, integrate with Slack API
        self.log.info(f"Slack notification (simulated): {message}")

        # Simulate potential Slack failure
        if random.random() < 0.05:  # 5% failure rate
            raise Exception("Slack API rate limit exceeded")

        return {
            'status': 'success',
            'channel': 'slack',
            'message': 'Slack notification sent successfully'
        }

    def _send_webhook(self, message: str) -> Dict[str, Any]:
        """Simulate sending webhook notification"""
        # In production, make actual HTTP request
        self.log.info(f"Webhook notification (simulated): {message}")

        # Simulate potential webhook failure
        if random.random() < 0.15:  # 15% failure rate
            raise Exception("Webhook endpoint returned 500 error")

        return {
            'status': 'success',
            'channel': 'webhook',
            'message': 'Webhook notification sent successfully'
        }


# Create task instances using the custom operators

# Task 1: Math operations
math_add_task = MathOperator(
    task_id='math_add',
    num1=15,
    num2=25,
    operation='add',
    dag=dag,
)

math_divide_task = MathOperator(
    task_id='math_divide',
    num1=100,
    num2=4,
    operation='divide',
    dag=dag,
)

# Task 2: CSV processing
csv_processor_task = CSVProcessorOperator(
    task_id='process_csv_data',
    input_file='/tmp/sample_input.csv',
    output_file='/tmp/processed_output.csv',
    processing_function='uppercase_names',
    dag=dag,
)

# Task 3: HTTP request
http_request_task = HTTPOperator(
    task_id='make_http_request',
    url='https://api.example.com/data',
    method='GET',
    expected_status_codes=[200, 201],
    max_retries=2,
    dag=dag,
)

# Task 4: Data quality validation
data_quality_task = DataQualityOperator(
    task_id='validate_data_quality',
    data_source='/tmp/data.csv',
    validation_rules={
        'name_not_null': {'type': 'null_check', 'field': 'name'},
        'age_range': {'type': 'range_check', 'field': 'age', 'min': 0, 'max': 120},
        'email_format': {'type': 'format_check', 'field': 'email', 'pattern': r'^[^@]+@[^@]+\.[^@]+$'},
        'id_unique': {'type': 'uniqueness_check', 'field': 'id'}
    },
    fail_on_error=False,
    dag=dag,
)

# Task 5: Send notifications
notification_task = NotificationOperator(
    task_id='send_notifications',
    message='Pipeline completed successfully for DAG {{ dag.dag_id }} on {{ ds }}',
    channels=['console', 'email', 'slack'],
    severity='info',
    metadata={'pipeline': 'custom_operators_exercise'},
    dag=dag,
)

# Set up task dependencies
math_add_task >> math_divide_task >> csv_processor_task
csv_processor_task >> [http_request_task, data_quality_task]
[http_request_task, data_quality_task] >> notification_task
