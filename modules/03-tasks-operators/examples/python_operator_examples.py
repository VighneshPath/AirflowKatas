"""
PythonOperator Examples

This file demonstrates various ways to use the PythonOperator in Airflow.
Each example shows different function definitions, parameters, and configurations.
"""

from datetime import datetime, timedelta
import json
import random
from airflow import DAG
from airflow.operators.python import PythonOperator

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
    'python_operator_examples',
    default_args=default_args,
    description='Examples of PythonOperator usage',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['examples', 'python', 'operators'],
)

# Example 1: Simple Python function


def simple_python_function():
    """Simple function that prints a message"""
    print("Hello from PythonOperator!")
    print(f"Current time: {datetime.now()}")
    return "Simple function completed"


simple_python_task = PythonOperator(
    task_id='simple_python_task',
    python_callable=simple_python_function,
    dag=dag,
)

# Example 2: Function with positional arguments


def function_with_args(name, age, city):
    """Function that accepts positional arguments"""
    message = f"Processing user: {name}, age {age}, from {city}"
    print(message)
    return {
        'name': name,
        'age': age,
        'city': city,
        'processed_at': datetime.now().isoformat()
    }


function_with_args_task = PythonOperator(
    task_id='function_with_args_task',
    python_callable=function_with_args,
    op_args=['Alice', 25, 'New York'],
    dag=dag,
)

# Example 3: Function with keyword arguments


def function_with_kwargs(user_id, **kwargs):
    """Function that accepts keyword arguments"""
    name = kwargs.get('name', 'Unknown')
    department = kwargs.get('department', 'General')
    salary = kwargs.get('salary', 0)

    print(f"User ID: {user_id}")
    print(f"Name: {name}")
    print(f"Department: {department}")
    print(f"Salary: ${salary:,}")

    return {
        'user_id': user_id,
        'name': name,
        'department': department,
        'salary': salary
    }


function_with_kwargs_task = PythonOperator(
    task_id='function_with_kwargs_task',
    python_callable=function_with_kwargs,
    op_args=[12345],
    op_kwargs={
        'name': 'Bob Smith',
        'department': 'Engineering',
        'salary': 75000
    },
    dag=dag,
)

# Example 4: Function using Airflow context


def function_with_context(**context):
    """Function that uses Airflow context variables"""
    # Access context variables
    execution_date = context['execution_date']
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id

    print(f"Execution Date: {execution_date}")
    print(f"DAG ID: {dag_id}")
    print(f"Task ID: {task_id}")

    # Access other context variables
    print(
        f"Previous execution date: {context.get('prev_execution_date', 'None')}")
    print(f"Next execution date: {context.get('next_execution_date', 'None')}")

    return {
        'execution_date': execution_date.isoformat(),
        'dag_id': dag_id,
        'task_id': task_id
    }


function_with_context_task = PythonOperator(
    task_id='function_with_context_task',
    python_callable=function_with_context,
    dag=dag,
)

# Example 5: Data processing function


def process_data():
    """Function that simulates data processing"""
    # Generate sample data
    data = []
    for i in range(10):
        record = {
            'id': i + 1,
            'name': f'User_{i + 1}',
            'score': random.randint(60, 100),
            'active': random.choice([True, False])
        }
        data.append(record)

    print(f"Generated {len(data)} records")

    # Process data - filter active users with high scores
    high_performers = [
        record for record in data
        if record['active'] and record['score'] >= 80
    ]

    print(f"Found {len(high_performers)} high performers:")
    for performer in high_performers:
        print(f"  - {performer['name']}: {performer['score']}")

    return {
        'total_records': len(data),
        'high_performers': len(high_performers),
        'high_performer_details': high_performers
    }


process_data_task = PythonOperator(
    task_id='process_data_task',
    python_callable=process_data,
    dag=dag,
)

# Example 6: Function with error handling


def function_with_error_handling():
    """Function that demonstrates error handling"""
    try:
        print("Starting risky operation...")

        # Simulate some processing
        numbers = [1, 2, 3, 4, 5]
        result = sum(numbers) / len(numbers)

        print(f"Average calculated: {result}")

        # Simulate potential error condition
        if random.random() < 0.3:  # 30% chance of "error"
            raise ValueError("Simulated processing error")

        print("Operation completed successfully!")
        return {'status': 'success', 'result': result}

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        # In a real scenario, you might want to handle this differently
        # For now, we'll return an error status instead of raising
        return {'status': 'error', 'error_message': str(e)}


function_with_error_handling_task = PythonOperator(
    task_id='function_with_error_handling_task',
    python_callable=function_with_error_handling,
    dag=dag,
)

# Example 7: Function that returns data for downstream tasks


def generate_report_data():
    """Function that generates data for downstream consumption"""
    report_data = {
        'report_id': f"RPT_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        'generated_at': datetime.now().isoformat(),
        'metrics': {
            'total_users': random.randint(1000, 5000),
            'active_users': random.randint(500, 2000),
            'revenue': round(random.uniform(10000, 50000), 2)
        },
        'status': 'completed'
    }

    print(f"Generated report: {report_data['report_id']}")
    print(f"Metrics: {json.dumps(report_data['metrics'], indent=2)}")

    return report_data


generate_report_task = PythonOperator(
    task_id='generate_report_task',
    python_callable=generate_report_data,
    dag=dag,
)

# Example 8: Function that processes external data


def simulate_api_call():
    """Function that simulates calling an external API"""
    print("Simulating API call...")

    # Simulate API response
    api_response = {
        'status_code': 200,
        'data': {
            'users': [
                {'id': 1, 'name': 'Alice', 'email': 'alice@example.com'},
                {'id': 2, 'name': 'Bob', 'email': 'bob@example.com'},
                {'id': 3, 'name': 'Charlie', 'email': 'charlie@example.com'}
            ],
            'total_count': 3,
            'page': 1
        },
        'timestamp': datetime.now().isoformat()
    }

    print(
        f"API call successful. Retrieved {len(api_response['data']['users'])} users")

    # Process the response
    processed_users = []
    for user in api_response['data']['users']:
        processed_user = {
            'user_id': user['id'],
            'display_name': user['name'].upper(),
            'email_domain': user['email'].split('@')[1],
            'processed_at': datetime.now().isoformat()
        }
        processed_users.append(processed_user)

    print(f"Processed {len(processed_users)} users")
    return {
        'original_count': api_response['data']['total_count'],
        'processed_users': processed_users
    }


simulate_api_call_task = PythonOperator(
    task_id='simulate_api_call_task',
    python_callable=simulate_api_call,
    dag=dag,
)

# Set up task dependencies
simple_python_task >> function_with_args_task >> function_with_kwargs_task
function_with_kwargs_task >> function_with_context_task >> process_data_task
process_data_task >> function_with_error_handling_task
function_with_error_handling_task >> [
    generate_report_task, simulate_api_call_task]
