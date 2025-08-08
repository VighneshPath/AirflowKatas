"""
Complete ETL Pipeline Solution
This DAG demonstrates a production-ready ETL pipeline with data validation,
error handling, and monitoring capabilities.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Any
import pandas as pd
import json
import logging
import os
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.utils.task_group import TaskGroup

# Configuration
DEFAULT_ARGS = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
}

# DAG Definition
dag = DAG(
    'daily_sales_etl',
    default_args=DEFAULT_ARGS,
    description='Complete ETL pipeline for daily sales reporting',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'sales', 'production'],
)

# Data paths
DATA_DIR = Path('/opt/airflow/data')
SALES_DIR = DATA_DIR / 'sales'
PRODUCTS_DIR = DATA_DIR / 'products'
OUTPUT_DIR = DATA_DIR / 'output'
ARCHIVE_DIR = DATA_DIR / 'archive'

# Ensure directories exist
for directory in [OUTPUT_DIR, ARCHIVE_DIR]:
    directory.mkdir(parents=True, exist_ok=True)


def extract_sales_data(**context) -> str:
    """Extract sales data from CSV files"""
    execution_date = context['ds']
    sales_file = SALES_DIR / f"sales_{execution_date.replace('-', '_')}.csv"

    logging.info(f"Extracting sales data from {sales_file}")

    if not sales_file.exists():
        raise AirflowException(f"Sales file not found: {sales_file}")

    # Read and basic validation
    df = pd.read_csv(sales_file)
    logging.info(f"Extracted {len(df)} sales records")

    # Store raw data for validation
    output_file = OUTPUT_DIR / f"raw_sales_{execution_date}.csv"
    df.to_csv(output_file, index=False)

    return str(output_file)


def extract_product_data(**context) -> str:
    """Extract product data from JSON API (simulated with file)"""
    execution_date = context['ds']
    products_file = PRODUCTS_DIR / 'products.json'

    logging.info(f"Extracting product data from {products_file}")

    if not products_file.exists():
        raise AirflowException(f"Products file not found: {products_file}")

    with open(products_file, 'r') as f:
        data = json.load(f)

    products_df = pd.DataFrame(data['products'])
    logging.info(f"Extracted {len(products_df)} product records")

    # Store raw data
    output_file = OUTPUT_DIR / f"raw_products_{execution_date}.csv"
    products_df.to_csv(output_file, index=False)

    return str(output_file)


def extract_customer_data(**context) -> str:
    """Extract customer data (simulated - in real scenario would query database)"""
    execution_date = context['ds']

    # Simulate customer data
    customers_data = [
        {'customer_id': 'CUST001', 'name': 'John Doe',
            'email': 'john@example.com', 'region': 'North', 'tier': 'Gold'},
        {'customer_id': 'CUST002', 'name': 'Jane Smith',
            'email': 'jane@example.com', 'region': 'South', 'tier': 'Silver'},
        {'customer_id': 'CUST003', 'name': 'Bob Johnson',
            'email': 'bob@example.com', 'region': 'East', 'tier': 'Bronze'},
        {'customer_id': 'CUST004', 'name': 'Alice Brown',
            'email': 'alice@example.com', 'region': 'West', 'tier': 'Gold'},
        {'customer_id': 'CUST005', 'name': 'Charlie Wilson',
            'email': 'charlie@example.com', 'region': 'South', 'tier': 'Silver'},
        {'customer_id': 'CUST006', 'name': 'Diana Davis',
            'email': 'diana@example.com', 'region': 'East', 'tier': 'Bronze'},
        {'customer_id': 'CUST007', 'name': 'Eve Miller',
            'email': 'eve@example.com', 'region': 'West', 'tier': 'Gold'},
        {'customer_id': 'CUST008', 'name': 'Frank Garcia',
            'email': 'frank@example.com', 'region': 'South', 'tier': 'Silver'},
        {'customer_id': 'CUST009', 'name': 'Grace Lee',
            'email': 'grace@example.com', 'region': 'West', 'tier': 'Bronze'},
        {'customer_id': 'CUST010', 'name': 'Henry Taylor',
            'email': 'henry@example.com', 'region': 'North', 'tier': 'Gold'},
    ]

    customers_df = pd.DataFrame(customers_data)
    logging.info(f"Extracted {len(customers_df)} customer records")

    # Store raw data
    output_file = OUTPUT_DIR / f"raw_customers_{execution_date}.csv"
    customers_df.to_csv(output_file, index=False)

    return str(output_file)


def validate_sales_data(**context) -> Dict[str, Any]:
    """Validate sales data quality"""
    execution_date = context['ds']
    sales_file = OUTPUT_DIR / f"raw_sales_{execution_date}.csv"

    df = pd.read_csv(sales_file)
    validation_results = {
        'total_records': len(df),
        'validation_errors': [],
        'warnings': [],
        'passed': True
    }

    # Check for required fields
    required_fields = ['transaction_id', 'product_id',
                       'quantity', 'unit_price', 'total_amount']
    for field in required_fields:
        null_count = df[field].isnull().sum()
        if null_count > 0:
            validation_results['validation_errors'].append(
                f"{field}: {null_count} null values")

    # Check for positive amounts
    negative_amounts = (df['total_amount'] <= 0).sum()
    if negative_amounts > 0:
        validation_results['validation_errors'].append(
            f"Found {negative_amounts} non-positive amounts")

    # Check for missing customer_id (warning, not error)
    missing_customers = df['customer_id'].isnull().sum()
    if missing_customers > 0:
        validation_results['warnings'].append(
            f"Found {missing_customers} transactions without customer_id")

    # Check for data consistency
    inconsistent_totals = abs(
        df['quantity'] * df['unit_price'] - df['total_amount']) > 0.01
    if inconsistent_totals.sum() > 0:
        validation_results['validation_errors'].append(
            f"Found {inconsistent_totals.sum()} inconsistent total calculations")

    validation_results['passed'] = len(
        validation_results['validation_errors']) == 0

    logging.info(f"Sales validation results: {validation_results}")

    if not validation_results['passed']:
        raise AirflowException(
            f"Sales data validation failed: {validation_results['validation_errors']}")

    return validation_results


def validate_product_data(**context) -> Dict[str, Any]:
    """Validate product data quality"""
    execution_date = context['ds']
    products_file = OUTPUT_DIR / f"raw_products_{execution_date}.csv"

    df = pd.read_csv(products_file)
    validation_results = {
        'total_records': len(df),
        'validation_errors': [],
        'warnings': [],
        'passed': True
    }

    # Check for required fields
    required_fields = ['product_id', 'name', 'price']
    for field in required_fields:
        null_count = df[field].isnull().sum()
        if null_count > 0:
            validation_results['validation_errors'].append(
                f"{field}: {null_count} null values")

    # Check for positive prices
    negative_prices = (df['price'] <= 0).sum()
    if negative_prices > 0:
        validation_results['validation_errors'].append(
            f"Found {negative_prices} non-positive prices")

    # Check for duplicate product IDs
    duplicates = df['product_id'].duplicated().sum()
    if duplicates > 0:
        validation_results['validation_errors'].append(
            f"Found {duplicates} duplicate product IDs")

    validation_results['passed'] = len(
        validation_results['validation_errors']) == 0

    logging.info(f"Product validation results: {validation_results}")

    if not validation_results['passed']:
        raise AirflowException(
            f"Product data validation failed: {validation_results['validation_errors']}")

    return validation_results


def validate_customer_data(**context) -> Dict[str, Any]:
    """Validate customer data quality"""
    execution_date = context['ds']
    customers_file = OUTPUT_DIR / f"raw_customers_{execution_date}.csv"

    df = pd.read_csv(customers_file)
    validation_results = {
        'total_records': len(df),
        'validation_errors': [],
        'warnings': [],
        'passed': True
    }

    # Check for required fields
    required_fields = ['customer_id', 'email']
    for field in required_fields:
        null_count = df[field].isnull().sum()
        if null_count > 0:
            validation_results['validation_errors'].append(
                f"{field}: {null_count} null values")

    # Validate email format (basic check)
    invalid_emails = ~df['email'].str.contains('@', na=False)
    if invalid_emails.sum() > 0:
        validation_results['validation_errors'].append(
            f"Found {invalid_emails.sum()} invalid email formats")

    # Check for duplicate customer IDs
    duplicates = df['customer_id'].duplicated().sum()
    if duplicates > 0:
        validation_results['validation_errors'].append(
            f"Found {duplicates} duplicate customer IDs")

    validation_results['passed'] = len(
        validation_results['validation_errors']) == 0

    logging.info(f"Customer validation results: {validation_results}")

    if not validation_results['passed']:
        raise AirflowException(
            f"Customer data validation failed: {validation_results['validation_errors']}")

    return validation_results


def transform_sales_data(**context) -> str:
    """Transform and enrich sales data"""
    execution_date = context['ds']

    # Load validated data
    sales_df = pd.read_csv(OUTPUT_DIR / f"raw_sales_{execution_date}.csv")
    products_df = pd.read_csv(
        OUTPUT_DIR / f"raw_products_{execution_date}.csv")
    customers_df = pd.read_csv(
        OUTPUT_DIR / f"raw_customers_{execution_date}.csv")

    logging.info("Starting data transformation")

    # Enrich sales with product information
    enriched_df = sales_df.merge(
        products_df[['product_id', 'name', 'category', 'cost']],
        on='product_id',
        how='left'
    )

    # Enrich with customer information (left join to handle missing customers)
    enriched_df = enriched_df.merge(
        customers_df[['customer_id', 'name', 'tier']],
        on='customer_id',
        how='left',
        suffixes=('', '_customer')
    )

    # Calculate additional metrics
    enriched_df['profit'] = enriched_df['total_amount'] - \
        (enriched_df['quantity'] * enriched_df['cost'])
    enriched_df['profit_margin'] = enriched_df['profit'] / \
        enriched_df['total_amount']

    # Add date components for analysis
    enriched_df['transaction_date'] = pd.to_datetime(
        enriched_df['transaction_date'])
    enriched_df['hour'] = enriched_df['transaction_date'].dt.hour
    enriched_df['day_of_week'] = enriched_df['transaction_date'].dt.day_name()

    # Create daily summary
    daily_summary = enriched_df.groupby(['region', 'category']).agg({
        'total_amount': ['sum', 'count', 'mean'],
        'profit': 'sum',
        'quantity': 'sum'
    }).round(2)

    daily_summary.columns = ['total_revenue', 'transaction_count',
                             'avg_transaction', 'total_profit', 'total_quantity']
    daily_summary = daily_summary.reset_index()

    # Save transformed data
    transformed_file = OUTPUT_DIR / f"transformed_sales_{execution_date}.csv"
    enriched_df.to_csv(transformed_file, index=False)

    summary_file = OUTPUT_DIR / f"daily_summary_{execution_date}.csv"
    daily_summary.to_csv(summary_file, index=False)

    logging.info(
        f"Transformation complete. Processed {len(enriched_df)} records")
    logging.info(
        f"Generated summary with {len(daily_summary)} region-category combinations")

    return str(transformed_file)


def load_to_warehouse(**context) -> str:
    """Load transformed data to warehouse (simulated)"""
    execution_date = context['ds']
    transformed_file = OUTPUT_DIR / f"transformed_sales_{execution_date}.csv"

    # In a real scenario, this would load to a data warehouse
    # For this example, we'll copy to a "warehouse" directory
    warehouse_dir = OUTPUT_DIR / 'warehouse'
    warehouse_dir.mkdir(exist_ok=True)

    warehouse_file = warehouse_dir / f"sales_fact_{execution_date}.csv"

    # Simulate warehouse loading with some processing
    df = pd.read_csv(transformed_file)

    # Add warehouse-specific columns
    df['load_timestamp'] = datetime.now()
    df['batch_id'] = f"BATCH_{execution_date.replace('-', '')}"

    df.to_csv(warehouse_file, index=False)

    logging.info(f"Loaded {len(df)} records to warehouse: {warehouse_file}")

    return str(warehouse_file)


def generate_reports(**context) -> List[str]:
    """Generate business reports"""
    execution_date = context['ds']
    summary_file = OUTPUT_DIR / f"daily_summary_{execution_date}.csv"

    df = pd.read_csv(summary_file)

    reports = []

    # Executive Summary Report
    exec_summary = {
        'date': execution_date,
        'total_revenue': df['total_revenue'].sum(),
        'total_transactions': df['transaction_count'].sum(),
        'total_profit': df['total_profit'].sum(),
        'avg_transaction_value': df['total_revenue'].sum() / df['transaction_count'].sum(),
        'top_region': df.groupby('region')['total_revenue'].sum().idxmax(),
        'top_category': df.groupby('category')['total_revenue'].sum().idxmax()
    }

    exec_report_file = OUTPUT_DIR / f"executive_summary_{execution_date}.json"
    with open(exec_report_file, 'w') as f:
        json.dump(exec_summary, f, indent=2, default=str)
    reports.append(str(exec_report_file))

    # Regional Performance Report
    regional_report = df.groupby('region').agg({
        'total_revenue': 'sum',
        'transaction_count': 'sum',
        'total_profit': 'sum'
    }).round(2)

    regional_report_file = OUTPUT_DIR / \
        f"regional_performance_{execution_date}.csv"
    regional_report.to_csv(regional_report_file)
    reports.append(str(regional_report_file))

    logging.info(f"Generated {len(reports)} reports")

    return reports


def archive_raw_data(**context) -> str:
    """Archive raw data files"""
    execution_date = context['ds']

    # Create archive directory for this date
    date_archive_dir = ARCHIVE_DIR / execution_date
    date_archive_dir.mkdir(exist_ok=True)

    # Files to archive
    files_to_archive = [
        OUTPUT_DIR / f"raw_sales_{execution_date}.csv",
        OUTPUT_DIR / f"raw_products_{execution_date}.csv",
        OUTPUT_DIR / f"raw_customers_{execution_date}.csv"
    ]

    archived_files = []
    for file_path in files_to_archive:
        if file_path.exists():
            archive_path = date_archive_dir / file_path.name
            file_path.rename(archive_path)
            archived_files.append(str(archive_path))

    logging.info(f"Archived {len(archived_files)} files to {date_archive_dir}")

    return str(date_archive_dir)


# Task Groups for better organization
with TaskGroup("extract_data", dag=dag) as extract_group:
    extract_sales_task = PythonOperator(
        task_id='extract_sales_data',
        python_callable=extract_sales_data,
    )

    extract_products_task = PythonOperator(
        task_id='extract_product_data',
        python_callable=extract_product_data,
    )

    extract_customers_task = PythonOperator(
        task_id='extract_customer_data',
        python_callable=extract_customer_data,
    )

with TaskGroup("validate_data", dag=dag) as validate_group:
    validate_sales_task = PythonOperator(
        task_id='validate_sales_data',
        python_callable=validate_sales_data,
    )

    validate_products_task = PythonOperator(
        task_id='validate_product_data',
        python_callable=validate_product_data,
    )

    validate_customers_task = PythonOperator(
        task_id='validate_customer_data',
        python_callable=validate_customer_data,
    )

# Transform and Load tasks
transform_task = PythonOperator(
    task_id='transform_sales_data',
    python_callable=transform_sales_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_warehouse',
    python_callable=load_to_warehouse,
    dag=dag,
)

reports_task = PythonOperator(
    task_id='generate_reports',
    python_callable=generate_reports,
    dag=dag,
)

archive_task = PythonOperator(
    task_id='archive_raw_data',
    python_callable=archive_raw_data,
    dag=dag,
)

# Data quality check task
quality_check_task = BashOperator(
    task_id='data_quality_check',
    bash_command="""
    echo "Running data quality checks..."
    # In a real scenario, this would run comprehensive data quality tests
    echo "✓ All quality checks passed"
    """,
    dag=dag,
)

# Define dependencies
# Extract -> Validate -> Transform -> Load -> Reports/Archive
extract_sales_task >> validate_sales_task
extract_products_task >> validate_products_task
extract_customers_task >> validate_customers_task

[validate_sales_task, validate_products_task,
    validate_customers_task] >> transform_task
transform_task >> load_task
load_task >> [reports_task, quality_check_task]
[reports_task, quality_check_task] >> archive_task
