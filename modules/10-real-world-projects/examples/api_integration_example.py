"""
API Integration Example
A simplified example demonstrating API integration patterns with Airflow.
"""

from datetime import datetime, timedelta
import requests
import json
import logging
from typing import Dict, Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor

# DAG Configuration
default_args = {
    'owner': 'api-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'simple_api_integration',
    default_args=default_args,
    description='Simple API integration example',
    schedule_interval=timedelta(hours=4),
    catchup=False,
    tags=['example', 'api'],
)


def fetch_user_data(**context) -> Dict[str, Any]:
    """Fetch user data from JSONPlaceholder API"""
    logging.info("Fetching user data from API...")

    try:
        # Using JSONPlaceholder as a free testing API
        response = requests.get('https://jsonplaceholder.typicode.com/users')
        response.raise_for_status()

        users = response.json()
        logging.info(f"Successfully fetched {len(users)} users")

        # Transform data for our needs
        simplified_users = []
        for user in users:
            simplified_user = {
                'id': user['id'],
                'name': user['name'],
                'email': user['email'],
                'city': user['address']['city'],
                'company': user['company']['name']
            }
            simplified_users.append(simplified_user)

        return simplified_users

    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch user data: {str(e)}")
        raise


def process_user_data(**context) -> Dict[str, Any]:
    """Process the fetched user data"""
    # Get data from previous task
    users = context['task_instance'].xcom_pull(task_ids='fetch_user_data')

    logging.info(f"Processing {len(users)} users...")

    # Perform some processing
    processed_data = {
        'total_users': len(users),
        'cities': list(set(user['city'] for user in users)),
        'companies': list(set(user['company'] for user in users)),
        'users_by_city': {}
    }

    # Group users by city
    for user in users:
        city = user['city']
        if city not in processed_data['users_by_city']:
            processed_data['users_by_city'][city] = []
        processed_data['users_by_city'][city].append(user['name'])

    logging.info(f"Processed data: {processed_data}")
    return processed_data


def validate_api_response(**context) -> bool:
    """Validate the API response data"""
    users = context['task_instance'].xcom_pull(task_ids='fetch_user_data')

    logging.info("Validating API response...")

    # Basic validation checks
    if not users:
        raise ValueError("No user data received")

    if len(users) < 5:
        logging.warning(f"Expected more users, got {len(users)}")

    # Check required fields
    required_fields = ['id', 'name', 'email']
    for user in users:
        for field in required_fields:
            if field not in user or not user[field]:
                raise ValueError(
                    f"Missing or empty field '{field}' in user data")

    logging.info("API response validation passed")
    return True


def send_summary_report(**context) -> str:
    """Send a summary report"""
    processed_data = context['task_instance'].xcom_pull(
        task_ids='process_user_data')

    report = f"""
    API Integration Summary Report
    ============================
    
    Date: {context['ds']}
    Total Users Processed: {processed_data['total_users']}
    Cities Represented: {len(processed_data['cities'])}
    Companies Represented: {len(processed_data['companies'])}
    
    Cities: {', '.join(processed_data['cities'])}
    Companies: {', '.join(processed_data['companies'])}
    """

    logging.info(f"Summary Report:\n{report}")

    # In production, this would send email or save to file
    return report


# API Health Check using HttpSensor
api_health_check = HttpSensor(
    task_id='check_api_health',
    http_conn_id='http_default',  # Configure this connection in Airflow
    endpoint='https://jsonplaceholder.typicode.com/users/1',
    timeout=30,
    poke_interval=10,
    dag=dag,
)

# Fetch data task
fetch_data_task = PythonOperator(
    task_id='fetch_user_data',
    python_callable=fetch_user_data,
    dag=dag,
)

# Validate data task
validate_task = PythonOperator(
    task_id='validate_api_response',
    python_callable=validate_api_response,
    dag=dag,
)

# Process data task
process_task = PythonOperator(
    task_id='process_user_data',
    python_callable=process_user_data,
    dag=dag,
)

# Report task
report_task = PythonOperator(
    task_id='send_summary_report',
    python_callable=send_summary_report,
    dag=dag,
)

# Alternative approach using SimpleHttpOperator
fetch_posts_task = SimpleHttpOperator(
    task_id='fetch_posts_via_http_operator',
    http_conn_id='http_default',
    endpoint='https://jsonplaceholder.typicode.com/posts',
    method='GET',
    headers={'Content-Type': 'application/json'},
    xcom_push=True,
    dag=dag,
)


def process_posts(**context):
    """Process posts fetched via HTTP operator"""
    posts_response = context['task_instance'].xcom_pull(
        task_ids='fetch_posts_via_http_operator')

    if posts_response:
        posts = json.loads(posts_response)
        logging.info(f"Processed {len(posts)} posts via HTTP operator")
        return len(posts)

    return 0


process_posts_task = PythonOperator(
    task_id='process_posts',
    python_callable=process_posts,
    dag=dag,
)

# Define task dependencies
api_health_check >> fetch_data_task
fetch_data_task >> validate_task
validate_task >> process_task
process_task >> report_task

# Parallel branch for posts
api_health_check >> fetch_posts_task >> process_posts_task
