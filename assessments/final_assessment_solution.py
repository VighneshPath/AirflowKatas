"""
Final Assessment Solution: Complete Data Pipeline

This solution demonstrates mastery of all Airflow concepts covered in the kata.
It implements a comprehensive e-commerce data pipeline with proper error handling,
monitoring, and real-world patterns.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.exceptions import AirflowException
import logging
import json
import random

# Default arguments with comprehensive error handling
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(hours=4),
}

# DAG definition with proper configuration
dag = DAG(
    'ecommerce_data_pipeline',
    default_args=default_args,
    description='Complete e-commerce data pipeline demonstrating all Airflow concepts',
    schedule_interval='0 2 * * *',  # Daily at 2 AM UTC
    catchup=False,
    max_active_runs=1,
    tags=['assessment', 'ecommerce', 'data-pipeline'],
)

# Utility functions for data processing


def extract_customer_data(**context):
    """Extract customer data - demonstrates PythonOperator usage."""
    logging.info("Extracting customer data...")

    # Simulate customer data extraction
    customer_data = {
        'total_customers': random.randint(1000, 5000),
        'new_customers': random.randint(50, 200),
        'extraction_time': datetime.now().isoformat()
    }

    logging.info(
        f"Extracted data for {customer_data['total_customers']} customers")
    return customer_data


def validate_data_format(**context):
    """Validate data format and structure."""
    logging.info("Validating data format...")

    # Pull data from previous task
    customer_data = context['task_instance'].xcom_pull(
        task_ids='extract_customer_data')

    if not customer_data or customer_data['total_customers'] < 100:
        raise AirflowException("Insufficient customer data")

    validation_result = {
        'format_valid': True,
        'record_count': customer_data['total_customers'],
        'validation_time': datetime.now().isoformat()
    }

    return validation_result


def data_quality_checks(**context):
    """Perform comprehensive data quality checks."""
    logging.info("Performing data quality checks...")

    validation_result = context['task_instance'].xcom_pull(
        task_ids='validate_data_format')

    # Simulate quality checks
    quality_score = random.uniform(0.7, 1.0)

    quality_result = {
        'quality_score': quality_score,
        'passed_checks': quality_score > 0.8,
        'issues_found': [] if quality_score > 0.9 else ['minor_inconsistencies'],
        'check_time': datetime.now().isoformat()
    }

    logging.info(f"Data quality score: {quality_score:.2f}")
    return quality_result


def check_business_rules(**context):
    """Check business rules and determine processing path."""
    quality_result = context['task_instance'].xcom_pull(
        task_ids='data_quality_checks')

    if quality_result['passed_checks']:
        return 'standard_processing'
    else:
        return 'enhanced_processing'


def clean_sales_data(**context):
    """Clean and standardize sales data."""
    logging.info("Cleaning sales data...")

    # Simulate data cleaning
    cleaned_data = {
        'records_processed': random.randint(5000, 15000),
        'records_cleaned': random.randint(4800, 14500),
        'cleaning_time': datetime.now().isoformat()
    }

    return cleaned_data


def calculate_metrics(**context):
    """Calculate daily KPIs and metrics."""
    logging.info("Calculating daily metrics...")

    sales_data = context['task_instance'].xcom_pull(
        task_ids='clean_sales_data')
    customer_data = context['task_instance'].xcom_pull(
        task_ids='extract_customer_data')

    metrics = {
        'total_revenue': random.uniform(50000, 200000),
        'avg_order_value': random.uniform(50, 150),
        'conversion_rate': random.uniform(0.02, 0.08),
        'customer_acquisition_cost': random.uniform(20, 80),
        'calculation_time': datetime.now().isoformat()
    }

    logging.info(
        f"Calculated metrics: Revenue ${metrics['total_revenue']:.2f}")
    return metrics


def send_notifications(**context):
    """Send completion notifications to stakeholders."""
    logging.info("Sending notifications...")

    metrics = context['task_instance'].xcom_pull(task_ids='calculate_metrics')

    notification = {
        'status': 'completed',
        'revenue': metrics['total_revenue'],
        'timestamp': datetime.now().isoformat(),
        'recipients': ['data-team@company.com', 'business-team@company.com']
    }

    logging.info("Notifications sent successfully")
    return notification


def failure_callback(context):
    """Custom failure callback for critical tasks."""
    logging.error(f"Task {context['task_instance'].task_id} failed!")
    # In production, this would send alerts to monitoring systems


# Task 1: Data Extraction Group
with TaskGroup('data_extraction', dag=dag) as extraction_group:

    # Extract sales data using BashOperator
    extract_sales_data = BashOperator(
        task_id='extract_sales_data',
        bash_command='''
        echo "Extracting sales data..."
        mkdir -p /tmp/sales_data
        echo "sales_data_$(date +%Y%m%d)" > /tmp/sales_data/daily_sales.csv
        echo "Sales data extracted successfully"
        ''',
        on_failure_callback=failure_callback,
    )

    # Extract customer data using PythonOperator
    extract_customer_data_task = PythonOperator(
        task_id='extract_customer_data',
        python_callable=extract_customer_data,
        on_failure_callback=failure_callback,
    )

# Task 2: Data Validation Group
with TaskGroup('data_validation', dag=dag) as validation_group:

    validate_data_format_task = PythonOperator(
        task_id='validate_data_format',
        python_callable=validate_data_format,
    )

    data_quality_checks_task = PythonOperator(
        task_id='data_quality_checks',
        python_callable=data_quality_checks,
    )

    validate_data_format_task >> data_quality_checks_task

# Task 3: Conditional Processing
check_business_rules_task = BranchPythonOperator(
    task_id='check_business_rules',
    python_callable=check_business_rules,
    dag=dag,
)

# Task 4: Standard Processing Path
with TaskGroup('standard_processing', dag=dag) as standard_group:

    clean_sales_data_task = PythonOperator(
        task_id='clean_sales_data',
        python_callable=clean_sales_data,
    )

    calculate_metrics_task = PythonOperator(
        task_id='calculate_metrics',
        python_callable=calculate_metrics,
    )

    clean_sales_data_task >> calculate_metrics_task

# Task 5: Enhanced Processing Path (for data quality issues)
with TaskGroup('enhanced_processing', dag=dag) as enhanced_group:

    enhanced_cleaning = PythonOperator(
        task_id='enhanced_cleaning',
        python_callable=clean_sales_data,  # Same function, could be enhanced
    )

    manual_review = BashOperator(
        task_id='manual_review',
        bash_command='echo "Flagging for manual review due to data quality issues"',
    )

    calculate_metrics_enhanced = PythonOperator(
        task_id='calculate_metrics_enhanced',
        python_callable=calculate_metrics,
    )

    enhanced_cleaning >> manual_review >> calculate_metrics_enhanced

# Task 6: API Integration and Database Operations
api_integration = BashOperator(
    task_id='api_integration',
    bash_command='''
    echo "Calling external APIs for data enrichment..."
    curl -s "https://api.exchangerate-api.com/v4/latest/USD" > /tmp/exchange_rates.json || echo "API call simulated"
    echo "API integration completed"
    ''',
    trigger_rule='none_failed_or_skipped',
    dag=dag,
)

database_operations = BashOperator(
    task_id='database_operations',
    bash_command='''
    echo "Writing results to database..."
    echo "INSERT INTO daily_metrics (date, revenue, orders) VALUES ($(date +%Y-%m-%d), 150000, 1200);" > /tmp/db_operations.sql
    echo "Database operations completed"
    ''',
    trigger_rule='none_failed_or_skipped',
    dag=dag,
)

# Task 7: Generate Reports
generate_reports = BashOperator(
    task_id='generate_reports',
    bash_command='''
    echo "Generating business reports..."
    mkdir -p /tmp/reports
    echo "Daily Sales Report - $(date)" > /tmp/reports/daily_report.txt
    echo "Revenue: $150,000" >> /tmp/reports/daily_report.txt
    echo "Orders: 1,200" >> /tmp/reports/daily_report.txt
    echo "Reports generated successfully"
    ''',
    trigger_rule='none_failed_or_skipped',
    dag=dag,
)

# Task 8: Send Notifications
send_notifications_task = PythonOperator(
    task_id='send_notifications',
    python_callable=send_notifications,
    trigger_rule='none_failed_or_skipped',
    dag=dag,
)

# Define task dependencies
extraction_group >> validation_group >> check_business_rules_task

check_business_rules_task >> [standard_group, enhanced_group]

[standard_group, enhanced_group] >> api_integration
api_integration >> database_operations
database_operations >> generate_reports
generate_reports >> send_notifications_task

# Add SLA miss callback


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Handle SLA misses."""
    logging.error(
        f"SLA missed for tasks: {[task.task_id for task in task_list]}")
    # In production, this would trigger alerts


dag.sla_miss_callback = sla_miss_callback
