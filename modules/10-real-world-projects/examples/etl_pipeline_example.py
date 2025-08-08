"""
ETL Pipeline Example
A simplified example demonstrating key ETL concepts and patterns.
"""

from datetime import datetime, timedelta
import pandas as pd
import logging
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# DAG Configuration
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'simple_etl_example',
    default_args=default_args,
    description='Simple ETL pipeline example',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['example', 'etl'],
)


def extract_data(**context):
    """Extract data from source"""
    logging.info("Extracting data from source...")

    # Simulate data extraction
    data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['Product A', 'Product B', 'Product C', 'Product D', 'Product E'],
        'price': [10.99, 25.50, 15.75, 30.00, 8.25],
        'category': ['Electronics', 'Clothing', 'Books', 'Electronics', 'Books']
    }

    df = pd.DataFrame(data)

    # Save extracted data
    output_path = '/tmp/extracted_data.csv'
    df.to_csv(output_path, index=False)

    logging.info(f"Extracted {len(df)} records to {output_path}")
    return output_path


def transform_data(**context):
    """Transform the extracted data"""
    logging.info("Transforming data...")

    # Load extracted data
    input_path = '/tmp/extracted_data.csv'
    df = pd.read_csv(input_path)

    # Apply transformations
    df['price_category'] = df['price'].apply(
        lambda x: 'Low' if x < 15 else 'Medium' if x < 25 else 'High'
    )

    # Add calculated fields
    df['discounted_price'] = df['price'] * 0.9  # 10% discount
    df['processed_date'] = datetime.now().strftime('%Y-%m-%d')

    # Save transformed data
    output_path = '/tmp/transformed_data.csv'
    df.to_csv(output_path, index=False)

    logging.info(f"Transformed {len(df)} records to {output_path}")
    return output_path


def load_data(**context):
    """Load data to destination"""
    logging.info("Loading data to destination...")

    # Load transformed data
    input_path = '/tmp/transformed_data.csv'
    df = pd.read_csv(input_path)

    # Simulate loading to data warehouse
    # In real scenario, this would insert into database
    warehouse_path = '/tmp/warehouse_data.csv'
    df.to_csv(warehouse_path, index=False)

    logging.info(f"Loaded {len(df)} records to warehouse at {warehouse_path}")
    return warehouse_path


def validate_data(**context):
    """Validate the loaded data"""
    logging.info("Validating loaded data...")

    warehouse_path = '/tmp/warehouse_data.csv'
    df = pd.read_csv(warehouse_path)

    # Perform validation checks
    checks = {
        'record_count': len(df),
        'null_values': df.isnull().sum().sum(),
        'duplicate_ids': df['id'].duplicated().sum(),
        'price_range_valid': ((df['price'] > 0) & (df['price'] < 1000)).all()
    }

    logging.info(f"Validation results: {checks}")

    # Raise exception if validation fails
    if checks['null_values'] > 0:
        raise ValueError("Data contains null values")
    if checks['duplicate_ids'] > 0:
        raise ValueError("Data contains duplicate IDs")
    if not checks['price_range_valid']:
        raise ValueError("Invalid price values detected")

    logging.info("All validation checks passed!")
    return checks


# Define tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    dag=dag,
)

cleanup_task = BashOperator(
    task_id='cleanup_temp_files',
    bash_command='rm -f /tmp/extracted_data.csv /tmp/transformed_data.csv',
    dag=dag,
)

# Define task dependencies
extract_task >> transform_task >> load_task >> validate_task >> cleanup_task
