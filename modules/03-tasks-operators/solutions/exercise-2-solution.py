"""
Solution for Exercise 2: PythonOperator Fundamentals

This file provides a complete solution for the PythonOperator exercise,
demonstrating various Python function types and configurations.
"""

from datetime import datetime, timedelta
import json
import random
import csv
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

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
    'python_exercise_dag',
    default_args=default_args,
    description='PythonOperator exercise solution',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['exercise', 'python'],
)

# Task 1: Simple greeting function


def simple_greeting():
    """Simple function that prints a greeting"""
    current_time = datetime.now()
    print(f"=== Simple Greeting ===")
    print(f"Hello! Current time: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("Task executed successfully!")
    return "Simple greeting completed successfully"


simple_greeting_task = PythonOperator(
    task_id='simple_greeting',
    python_callable=simple_greeting,
    dag=dag,
)

# Task 2: Function with positional arguments


def calculate_stats(numbers):
    """Calculate statistics for a list of numbers"""
    print(f"=== Calculate Stats ===")
    print(f"Processing numbers: {numbers}")

    if not numbers:
        return {'error': 'No numbers provided'}

    total = sum(numbers)
    average = total / len(numbers)
    minimum = min(numbers)
    maximum = max(numbers)

    print(f"Sum: {total}")
    print(f"Average: {average:.2f}")
    print(f"Min: {minimum}")
    print(f"Max: {maximum}")

    stats = {
        'sum': total,
        'average': round(average, 2),
        'min': minimum,
        'max': maximum,
        'count': len(numbers)
    }

    return stats


calculate_stats_task = PythonOperator(
    task_id='calculate_stats',
    python_callable=calculate_stats,
    op_args=[[10, 25, 30, 45, 50, 15, 35]],
    dag=dag,
)

# Task 3: Function with keyword arguments


def process_user_data(name, age, department, salary):
    """Process user information and calculate bonus"""
    print(f"=== User Data Processing ===")
    print(f"Processing user: {name}")
    print(f"Age: {age}, Department: {department}")
    print(f"Salary: ${salary:,}")

    # Calculate annual bonus (10% of salary)
    annual_bonus = salary * 0.10
    print(f"Annual Bonus: ${annual_bonus:,}")

    user_data = {
        'name': name,
        'age': age,
        'department': department,
        'salary': salary,
        'annual_bonus': annual_bonus,
        'total_compensation': salary + annual_bonus,
        'processed_at': datetime.now().isoformat()
    }

    return user_data


process_user_data_task = PythonOperator(
    task_id='process_user_data',
    python_callable=process_user_data,
    op_args=['John Doe'],
    op_kwargs={
        'age': 30,
        'department': 'Engineering',
        'salary': 75000
    },
    dag=dag,
)

# Task 4: Function using Airflow context


def context_explorer(**context):
    """Function that uses Airflow context variables"""
    print(f"=== Context Explorer ===")

    # Access context variables
    execution_date = context['execution_date']
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id

    print(f"Execution Date: {execution_date}")
    print(f"DAG ID: {dag_id}")
    print(f"Task ID: {task_id}")

    # Access other context variables
    prev_execution_date = context.get('prev_execution_date')
    next_execution_date = context.get('next_execution_date')

    print(f"Previous execution date: {prev_execution_date or 'None'}")
    print(f"Next execution date: {next_execution_date or 'None'}")

    # Additional context information
    dag_run = context.get('dag_run')
    if dag_run:
        print(f"DAG Run ID: {dag_run.run_id}")
        print(f"DAG Run Type: {dag_run.run_type}")

    context_summary = {
        'execution_date': execution_date.isoformat(),
        'dag_id': dag_id,
        'task_id': task_id,
        'prev_execution_date': prev_execution_date.isoformat() if prev_execution_date else None,
        'next_execution_date': next_execution_date.isoformat() if next_execution_date else None,
        'dag_run_id': dag_run.run_id if dag_run else None
    }

    return context_summary


context_explorer_task = PythonOperator(
    task_id='context_explorer',
    python_callable=context_explorer,
    dag=dag,
)

# Task 5: Data processing function


def data_transformer():
    """Data processing function"""
    print(f"=== Data Transformer ===")

    # Generate sample data
    data = []
    for i in range(20):
        record = {
            'id': i + 1,
            'name': f'User_{i + 1}',
            'score': random.randint(50, 100),
            'active': random.choice([True, False]),
            'department': random.choice(['Engineering', 'Sales', 'Marketing', 'HR'])
        }
        data.append(record)

    print(f"Generated {len(data)} records")

    # Filter records where score > 75 and active = True
    high_performers = [
        record for record in data
        if record['active'] and record['score'] > 75
    ]

    # Sort by score (descending)
    high_performers.sort(key=lambda x: x['score'], reverse=True)

    print(f"Found {len(high_performers)} high performers:")
    for performer in high_performers[:5]:  # Show top 5
        print(
            f"  - {performer['name']}: {performer['score']} ({performer['department']})")

    # Calculate summary statistics
    if high_performers:
        scores = [p['score'] for p in high_performers]
        avg_score = sum(scores) / len(scores)
        print(f"Average high performer score: {avg_score:.2f}")

    return {
        'total_records': len(data),
        'high_performers_count': len(high_performers),
        'high_performers': high_performers,
        'average_high_performer_score': avg_score if high_performers else 0
    }


data_transformer_task = PythonOperator(
    task_id='data_transformer',
    python_callable=data_transformer,
    dag=dag,
)

# Task 6: File processor function


def file_processor():
    """Function that works with files"""
    print(f"=== File Processor ===")

    file_path = '/tmp/sample_data.csv'

    try:
        # Create sample CSV data
        sample_data = [
            {'name': 'Alice', 'age': 25, 'city': 'New York', 'salary': 60000},
            {'name': 'Bob', 'age': 30, 'city': 'San Francisco', 'salary': 80000},
            {'name': 'Charlie', 'age': 35, 'city': 'Chicago', 'salary': 70000},
            {'name': 'Diana', 'age': 28, 'city': 'Boston', 'salary': 65000},
            {'name': 'Eve', 'age': 32, 'city': 'Seattle', 'salary': 75000}
        ]

        # Write CSV file
        with open(file_path, 'w', newline='') as csvfile:
            fieldnames = ['name', 'age', 'city', 'salary']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(sample_data)

        print(f"Created CSV file with {len(sample_data)} records")

        # Read and process the file
        processed_data = []
        with open(file_path, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Convert numeric fields
                row['age'] = int(row['age'])
                row['salary'] = int(row['salary'])
                processed_data.append(row)

        # Calculate summary statistics
        total_salary = sum(person['salary'] for person in processed_data)
        avg_salary = total_salary / len(processed_data)
        avg_age = sum(person['age']
                      for person in processed_data) / len(processed_data)

        print(f"Processed {len(processed_data)} records")
        print(f"Average salary: ${avg_salary:,.2f}")
        print(f"Average age: {avg_age:.1f}")

        # Clean up the file
        os.remove(file_path)
        print("File cleaned up successfully")

        return {
            'records_processed': len(processed_data),
            'average_salary': round(avg_salary, 2),
            'average_age': round(avg_age, 1),
            'total_salary': total_salary
        }

    except Exception as e:
        print(f"Error processing file: {str(e)}")
        # Clean up file if it exists
        if os.path.exists(file_path):
            os.remove(file_path)
        raise


file_processor_task = PythonOperator(
    task_id='file_processor',
    python_callable=file_processor,
    dag=dag,
)

# Task 7: Error handling function


def error_handler():
    """Function with comprehensive error handling"""
    print(f"=== Error Handler ===")

    try:
        print("Starting risky operations...")

        # Simulate different types of operations that might fail
        operations = ['divide_by_zero',
                      'file_not_found', 'value_error', 'success']
        operation = random.choice(operations)

        print(f"Performing operation: {operation}")

        if operation == 'divide_by_zero':
            result = 10 / 0
        elif operation == 'file_not_found':
            with open('/nonexistent/file.txt', 'r') as f:
                content = f.read()
        elif operation == 'value_error':
            result = int('not_a_number')
        else:  # success
            result = sum(range(1, 101))  # Sum of 1 to 100
            print(f"Operation successful! Result: {result}")
            return {'status': 'success', 'result': result, 'operation': operation}

    except ZeroDivisionError as e:
        print(f"Division by zero error: {str(e)}")
        return {'status': 'error', 'error_type': 'division_by_zero', 'operation': operation}
    except FileNotFoundError as e:
        print(f"File not found error: {str(e)}")
        return {'status': 'error', 'error_type': 'file_not_found', 'operation': operation}
    except ValueError as e:
        print(f"Value error: {str(e)}")
        return {'status': 'error', 'error_type': 'value_error', 'operation': operation}
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return {'status': 'error', 'error_type': 'unexpected', 'operation': operation}


error_handler_task = PythonOperator(
    task_id='error_handler',
    python_callable=error_handler,
    dag=dag,
)

# Task 8: API simulator function


def api_simulator():
    """Function that simulates API interactions"""
    print(f"=== API Simulator ===")

    try:
        print("Simulating API call...")

        # Simulate API response with random data
        api_response = {
            # Mostly successful
            'status_code': random.choice([200, 200, 200, 500, 404]),
            'data': {
                'users': [
                    {'id': i, 'name': f'User_{i}', 'email': f'user{i}@example.com',
                        'active': random.choice([True, False])}
                    for i in range(1, random.randint(5, 15))
                ],
                'timestamp': datetime.now().isoformat()
            }
        }

        print(f"API returned status code: {api_response['status_code']}")

        if api_response['status_code'] != 200:
            raise Exception(
                f"API call failed with status code: {api_response['status_code']}")

        # Process the API response
        users = api_response['data']['users']
        active_users = [user for user in users if user['active']]

        print(f"Retrieved {len(users)} users, {len(active_users)} active")

        # Transform the data
        processed_users = []
        for user in active_users:
            processed_user = {
                'user_id': user['id'],
                'display_name': user['name'].upper(),
                'email_domain': user['email'].split('@')[1],
                'processed_at': datetime.now().isoformat()
            }
            processed_users.append(processed_user)

        print(f"Processed {len(processed_users)} active users")

        return {
            'api_status': 'success',
            'total_users': len(users),
            'active_users': len(active_users),
            'processed_users': processed_users
        }

    except Exception as e:
        print(f"API simulation error: {str(e)}")
        return {
            'api_status': 'error',
            'error_message': str(e)
        }


api_simulator_task = PythonOperator(
    task_id='api_simulator',
    python_callable=api_simulator,
    dag=dag,
)

# Task 9: Report generator function


def report_generator():
    """Function that creates a summary report"""
    print(f"=== Report Generator ===")

    # Generate mock data for the report
    report_data = {
        'report_id': f"RPT_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        'generated_at': datetime.now().isoformat(),
        'summary': {
            'total_tasks_executed': 9,
            'successful_tasks': random.randint(7, 9),
            'data_records_processed': random.randint(100, 500),
            'average_processing_time': round(random.uniform(1.5, 5.0), 2)
        },
        'metrics': {
            'user_data_processed': random.randint(50, 100),
            'files_created': random.randint(3, 8),
            'api_calls_made': random.randint(1, 5),
            'errors_handled': random.randint(0, 3)
        }
    }

    print(f"Generated report: {report_data['report_id']}")
    print("Summary:")
    for key, value in report_data['summary'].items():
        print(f"  {key.replace('_', ' ').title()}: {value}")

    print("Metrics:")
    for key, value in report_data['metrics'].items():
        print(f"  {key.replace('_', ' ').title()}: {value}")

    return report_data


report_generator_task = PythonOperator(
    task_id='report_generator',
    python_callable=report_generator,
    dag=dag,
)

# Task 10: Data validator function


def data_validator():
    """Function that validates data quality"""
    print(f"=== Data Validator ===")

    # Simulate data validation checks
    validation_results = {
        'validation_id': f"VAL_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        'checks_performed': [
            'null_value_check',
            'duplicate_check',
            'format_validation',
            'range_validation'
        ],
        'results': {}
    }

    # Simulate validation results
    for check in validation_results['checks_performed']:
        passed = random.choice([True, True, True, False])  # 75% pass rate
        validation_results['results'][check] = {
            'passed': passed,
            'issues_found': 0 if passed else random.randint(1, 5)
        }

    # Calculate overall quality score
    passed_checks = sum(
        1 for result in validation_results['results'].values() if result['passed'])
    total_checks = len(validation_results['checks_performed'])
    quality_score = (passed_checks / total_checks) * 100

    validation_results['quality_score'] = round(quality_score, 2)

    print(f"Data validation completed: {validation_results['validation_id']}")
    print(f"Quality score: {quality_score:.2f}%")
    print(f"Checks passed: {passed_checks}/{total_checks}")

    for check, result in validation_results['results'].items():
        status = "PASSED" if result['passed'] else f"FAILED ({result['issues_found']} issues)"
        print(f"  {check.replace('_', ' ').title()}: {status}")

    return validation_results


data_validator_task = PythonOperator(
    task_id='data_validator',
    python_callable=data_validator,
    dag=dag,
)

# Set up task dependencies as specified in the exercise
simple_greeting_task >> calculate_stats_task >> process_user_data_task
context_explorer_task >> data_transformer_task >> file_processor_task
data_validator_task >> [error_handler_task,
                        api_simulator_task] >> report_generator_task
