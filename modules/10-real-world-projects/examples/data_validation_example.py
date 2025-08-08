"""
Data Validation Example
A simplified example demonstrating data quality validation patterns.
"""

from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Any

from airflow import DAG
from airflow.operators.python import PythonOperator

# DAG Configuration
default_args = {
    'owner': 'quality-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'simple_data_validation',
    default_args=default_args,
    description='Simple data validation example',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['example', 'validation'],
)

# Sample data with quality issues
SAMPLE_DATA = [
    {'id': 1, 'name': 'John Doe', 'email': 'john@example.com',
        'age': 25, 'salary': 50000},
    {'id': 2, 'name': 'Jane Smith', 'email': 'jane@example.com',
        'age': 30, 'salary': 60000},
    # Issues: empty name, invalid email, negative age, zero salary
    {'id': 3, 'name': '', 'email': 'invalid-email', 'age': -5, 'salary': 0},
    {'id': 4, 'name': 'Bob Johnson', 'email': 'bob@example.com',
        'age': 35, 'salary': None},  # Issue: null salary
    {'id': 5, 'name': 'Alice Brown', 'email': 'alice@example.com', 'age': 150,
        'salary': 1000000},  # Issues: unrealistic age, very high salary
]


def load_and_profile_data(**context) -> Dict[str, Any]:
    """Load data and generate basic profile"""
    logging.info("Loading and profiling data...")

    df = pd.DataFrame(SAMPLE_DATA)

    # Basic profiling
    profile = {
        'total_records': len(df),
        'columns': list(df.columns),
        'data_types': df.dtypes.to_dict(),
        'null_counts': df.isnull().sum().to_dict(),
        'unique_counts': df.nunique().to_dict()
    }

    # Convert numpy types to Python types for JSON serialization
    for key, value in profile['data_types'].items():
        profile['data_types'][key] = str(value)

    logging.info(f"Data profile: {profile}")

    # Store data for downstream tasks
    context['task_instance'].xcom_push(key='raw_data', value=SAMPLE_DATA)
    context['task_instance'].xcom_push(key='data_profile', value=profile)

    return profile


def validate_completeness(**context) -> Dict[str, Any]:
    """Check for missing/null values"""
    logging.info("Validating data completeness...")

    data = context['task_instance'].xcom_pull(key='raw_data')
    df = pd.DataFrame(data)

    # Define required fields
    required_fields = ['id', 'name', 'email']

    completeness_results = {
        'total_records': len(df),
        'validation_results': {},
        'issues_found': []
    }

    for field in required_fields:
        null_count = df[field].isnull().sum()
        empty_count = (df[field] == '').sum(
        ) if df[field].dtype == 'object' else 0

        completeness_results['validation_results'][field] = {
            'null_count': int(null_count),
            'empty_count': int(empty_count),
            'completeness_rate': float((len(df) - null_count - empty_count) / len(df) * 100)
        }

        if null_count > 0 or empty_count > 0:
            completeness_results['issues_found'].append(
                f"Field '{field}' has {null_count} null and {empty_count} empty values"
            )

    logging.info(f"Completeness validation results: {completeness_results}")
    return completeness_results


def validate_format(**context) -> Dict[str, Any]:
    """Validate data formats"""
    logging.info("Validating data formats...")

    data = context['task_instance'].xcom_pull(key='raw_data')
    df = pd.DataFrame(data)

    format_results = {
        'validation_results': {},
        'issues_found': []
    }

    # Email format validation
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    valid_emails = df['email'].str.match(email_pattern, na=False)
    invalid_email_count = (~valid_emails).sum()

    format_results['validation_results']['email_format'] = {
        'valid_count': int(valid_emails.sum()),
        'invalid_count': int(invalid_email_count),
        'validity_rate': float(valid_emails.sum() / len(df) * 100)
    }

    if invalid_email_count > 0:
        invalid_emails = df[~valid_emails]['email'].tolist()
        format_results['issues_found'].append(
            f"Found {invalid_email_count} invalid email formats: {invalid_emails}"
        )

    logging.info(f"Format validation results: {format_results}")
    return format_results


def validate_business_rules(**context) -> Dict[str, Any]:
    """Validate business rules"""
    logging.info("Validating business rules...")

    data = context['task_instance'].xcom_pull(key='raw_data')
    df = pd.DataFrame(data)

    business_rules_results = {
        'validation_results': {},
        'issues_found': []
    }

    # Age validation (must be between 18 and 100)
    valid_ages = (df['age'] >= 18) & (df['age'] <= 100)
    invalid_age_count = (~valid_ages).sum()

    business_rules_results['validation_results']['age_range'] = {
        'valid_count': int(valid_ages.sum()),
        'invalid_count': int(invalid_age_count),
        'validity_rate': float(valid_ages.sum() / len(df) * 100)
    }

    if invalid_age_count > 0:
        invalid_ages = df[~valid_ages][[
            'id', 'name', 'age']].to_dict('records')
        business_rules_results['issues_found'].append(
            f"Found {invalid_age_count} records with invalid ages: {invalid_ages}"
        )

    # Salary validation (must be positive and reasonable)
    valid_salaries = (df['salary'] > 0) & (df['salary'] <= 500000)
    invalid_salary_count = (~valid_salaries).sum()

    business_rules_results['validation_results']['salary_range'] = {
        'valid_count': int(valid_salaries.sum()),
        'invalid_count': int(invalid_salary_count),
        'validity_rate': float(valid_salaries.sum() / len(df) * 100)
    }

    if invalid_salary_count > 0:
        invalid_salaries = df[~valid_salaries][[
            'id', 'name', 'salary']].to_dict('records')
        business_rules_results['issues_found'].append(
            f"Found {invalid_salary_count} records with invalid salaries: {invalid_salaries}"
        )

    logging.info(
        f"Business rules validation results: {business_rules_results}")
    return business_rules_results


def detect_anomalies(**context) -> Dict[str, Any]:
    """Detect statistical anomalies"""
    logging.info("Detecting anomalies...")

    data = context['task_instance'].xcom_pull(key='raw_data')
    df = pd.DataFrame(data)

    anomaly_results = {
        'anomalies_detected': [],
        'statistics': {}
    }

    # Analyze salary distribution
    salary_data = df['salary'].dropna()
    if len(salary_data) > 0:
        mean_salary = salary_data.mean()
        std_salary = salary_data.std()

        anomaly_results['statistics']['salary'] = {
            'mean': float(mean_salary),
            'std': float(std_salary),
            'min': float(salary_data.min()),
            'max': float(salary_data.max())
        }

        # Detect outliers (values more than 2 standard deviations from mean)
        outliers = salary_data[(salary_data < mean_salary - 2*std_salary) |
                               (salary_data > mean_salary + 2*std_salary)]

        if len(outliers) > 0:
            outlier_records = df[df['salary'].isin(
                outliers)][['id', 'name', 'salary']].to_dict('records')
            anomaly_results['anomalies_detected'].append({
                'type': 'salary_outlier',
                'count': len(outliers),
                'records': outlier_records
            })

    logging.info(f"Anomaly detection results: {anomaly_results}")
    return anomaly_results


def generate_quality_summary(**context) -> Dict[str, Any]:
    """Generate overall data quality summary"""
    logging.info("Generating quality summary...")

    # Get results from all validation tasks
    profile = context['task_instance'].xcom_pull(
        task_ids='load_and_profile_data')
    completeness = context['task_instance'].xcom_pull(
        task_ids='validate_completeness')
    format_validation = context['task_instance'].xcom_pull(
        task_ids='validate_format')
    business_rules = context['task_instance'].xcom_pull(
        task_ids='validate_business_rules')
    anomalies = context['task_instance'].xcom_pull(task_ids='detect_anomalies')

    # Calculate overall quality score
    total_issues = (
        len(completeness['issues_found']) +
        len(format_validation['issues_found']) +
        len(business_rules['issues_found']) +
        len(anomalies['anomalies_detected'])
    )

    # Deduct 10 points per issue type
    quality_score = max(0, 100 - (total_issues * 10))

    summary = {
        'execution_date': context['ds'],
        'total_records': profile['total_records'],
        'overall_quality_score': quality_score,
        'validation_summary': {
            'completeness_issues': len(completeness['issues_found']),
            'format_issues': len(format_validation['issues_found']),
            'business_rule_violations': len(business_rules['issues_found']),
            'anomalies_detected': len(anomalies['anomalies_detected'])
        },
        'recommendations': []
    }

    # Generate recommendations
    if completeness['issues_found']:
        summary['recommendations'].append(
            "Address missing data in required fields")

    if format_validation['issues_found']:
        summary['recommendations'].append(
            "Fix data format issues, especially email formats")

    if business_rules['issues_found']:
        summary['recommendations'].append(
            "Review and correct business rule violations")

    if anomalies['anomalies_detected']:
        summary['recommendations'].append(
            "Investigate statistical anomalies for potential data errors")

    if quality_score >= 90:
        summary['quality_status'] = 'EXCELLENT'
    elif quality_score >= 70:
        summary['quality_status'] = 'GOOD'
    elif quality_score >= 50:
        summary['quality_status'] = 'FAIR'
    else:
        summary['quality_status'] = 'POOR'

    logging.info(f"Quality summary: {summary}")
    return summary


# Define tasks
profile_task = PythonOperator(
    task_id='load_and_profile_data',
    python_callable=load_and_profile_data,
    dag=dag,
)

completeness_task = PythonOperator(
    task_id='validate_completeness',
    python_callable=validate_completeness,
    dag=dag,
)

format_task = PythonOperator(
    task_id='validate_format',
    python_callable=validate_format,
    dag=dag,
)

business_rules_task = PythonOperator(
    task_id='validate_business_rules',
    python_callable=validate_business_rules,
    dag=dag,
)

anomaly_task = PythonOperator(
    task_id='detect_anomalies',
    python_callable=detect_anomalies,
    dag=dag,
)

summary_task = PythonOperator(
    task_id='generate_quality_summary',
    python_callable=generate_quality_summary,
    dag=dag,
)

# Define dependencies
profile_task >> [completeness_task, format_task,
                 business_rules_task, anomaly_task]
[completeness_task, format_task, business_rules_task, anomaly_task] >> summary_task
