"""
Data Validation Project Solution
This DAG demonstrates comprehensive data quality monitoring with
validation rules, profiling, scoring, and alerting capabilities.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
import numpy as np
import json
import logging
import re
from dataclasses import dataclass, asdict
from enum import Enum
import statistics
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.utils.task_group import TaskGroup

# Configuration
DEFAULT_ARGS = {
    'owner': 'data-quality-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'data_quality_monitoring',
    default_args=DEFAULT_ARGS,
    description='Comprehensive data quality monitoring and validation',
    schedule_interval='0 */4 * * *',  # Every 4 hours
    catchup=False,
    max_active_runs=1,
    tags=['data-quality', 'validation', 'monitoring'],
)

# Enums and Data Classes


class ValidationSeverity(Enum):
    CRITICAL = "CRITICAL"
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"


class QualityDimension(Enum):
    COMPLETENESS = "completeness"
    ACCURACY = "accuracy"
    CONSISTENCY = "consistency"
    VALIDITY = "validity"
    TIMELINESS = "timeliness"


@dataclass
class ValidationRule:
    rule_id: str
    name: str
    description: str
    rule_type: str
    severity: ValidationSeverity
    dimension: QualityDimension
    parameters: Dict[str, Any]
    enabled: bool = True


@dataclass
class ValidationResult:
    rule_id: str
    passed: bool
    failed_count: int
    total_count: int
    pass_rate: float
    error_message: Optional[str] = None
    failed_values: Optional[List[Any]] = None


@dataclass
class DataProfile:
    column_name: str
    data_type: str
    total_count: int
    null_count: int
    unique_count: int
    min_value: Any
    max_value: Any
    mean_value: Optional[float]
    std_dev: Optional[float]
    percentiles: Optional[Dict[str, float]]


@dataclass
class QualityScore:
    dimension: QualityDimension
    score: float
    rule_results: List[ValidationResult]


# Sample Data
SAMPLE_TRANSACTION_DATA = [
    {
        'transaction_id': 'TXN001',
        'customer_id': 'CUST001',
        'account_id': 'ACC001',
        'amount': 1500.00,
        'currency': 'USD',
        'transaction_date': '2024-01-15 10:30:00',
        'transaction_type': 'TRANSFER',
        'status': 'COMPLETED'
    },
    {
        'transaction_id': 'TXN002',
        'customer_id': 'CUST002',
        'account_id': 'ACC002',
        'amount': -250.00,
        'currency': 'USD',
        'transaction_date': '2024-01-15 11:15:00',
        'transaction_type': 'WITHDRAWAL',
        'status': 'COMPLETED'
    },
    {
        'transaction_id': 'TXN003',
        'customer_id': 'CUST001',
        'account_id': 'ACC001',
        'amount': 75.50,
        'currency': 'USD',
        'transaction_date': '2024-01-15 12:00:00',
        'transaction_type': 'PURCHASE',
        'status': 'PENDING'
    },
    {
        'transaction_id': 'TXN004',
        'customer_id': None,  # Missing customer ID
        'account_id': 'ACC003',
        'amount': 10000.00,
        'currency': 'USD',
        'transaction_date': '2024-01-15 13:45:00',
        'transaction_type': 'DEPOSIT',
        'status': 'COMPLETED'
    },
    {
        'transaction_id': 'TXN005',
        'customer_id': 'CUST003',
        'account_id': 'ACC004',
        'amount': 999999.99,  # Suspicious large amount
        'currency': 'USD',
        'transaction_date': '2024-01-15 14:20:00',
        'transaction_type': 'TRANSFER',
        'status': 'FAILED'
    },
    {
        'transaction_id': 'INVALID',  # Invalid format
        'customer_id': 'CUST004',
        'account_id': 'ACC005',
        'amount': 100.00,
        'currency': 'EUR',
        'transaction_date': '2024-01-15 15:00:00',
        'transaction_type': 'PURCHASE',
        'status': 'COMPLETED'
    }
]

SAMPLE_CUSTOMER_DATA = [
    {
        'customer_id': 'CUST001',
        'first_name': 'John',
        'last_name': 'Doe',
        'email': 'john.doe@email.com',
        'phone': '+1-555-0123',
        'date_of_birth': '1985-03-15',
        'account_status': 'ACTIVE',
        'risk_rating': 'LOW'
    },
    {
        'customer_id': 'CUST002',
        'first_name': 'Jane',
        'last_name': 'Smith',
        'email': 'jane.smith@email.com',
        'phone': '+1-555-0124',
        'date_of_birth': '1990-07-22',
        'account_status': 'ACTIVE',
        'risk_rating': 'MEDIUM'
    },
    {
        'customer_id': 'CUST003',
        'first_name': 'Bob',
        'last_name': 'Johnson',
        'email': 'invalid-email',  # Invalid email format
        'phone': '+1-555-0125',
        'date_of_birth': '2010-01-01',  # Suspicious young age
        'account_status': 'SUSPENDED',
        'risk_rating': 'HIGH'
    },
    {
        'customer_id': 'CUST004',
        'first_name': 'Alice',
        'last_name': 'Brown',
        'email': 'alice.brown@email.com',
        'phone': None,  # Missing phone
        'date_of_birth': '1975-11-30',
        'account_status': 'ACTIVE',
        'risk_rating': 'LOW'
    }
]

# Validation Rules Configuration
VALIDATION_RULES = [
    ValidationRule(
        rule_id='TXN_001',
        name='Transaction ID Format',
        description='Transaction ID must follow TXN### pattern',
        rule_type='regex',
        severity=ValidationSeverity.ERROR,
        dimension=QualityDimension.VALIDITY,
        parameters={'pattern': r'^TXN\d{3}$', 'column': 'transaction_id'}
    ),
    ValidationRule(
        rule_id='TXN_002',
        name='Amount Range Check',
        description='Transaction amount must be between -10000 and 50000',
        rule_type='range',
        severity=ValidationSeverity.WARNING,
        dimension=QualityDimension.VALIDITY,
        parameters={'min_value': -10000,
                    'max_value': 50000, 'column': 'amount'}
    ),
    ValidationRule(
        rule_id='TXN_003',
        name='Customer ID Required',
        description='Customer ID is required for all transactions',
        rule_type='not_null',
        severity=ValidationSeverity.ERROR,
        dimension=QualityDimension.COMPLETENESS,
        parameters={'column': 'customer_id'}
    ),
    ValidationRule(
        rule_id='TXN_004',
        name='Currency Code Validation',
        description='Currency must be valid ISO code',
        rule_type='enum',
        severity=ValidationSeverity.ERROR,
        dimension=QualityDimension.VALIDITY,
        parameters={'column': 'currency',
                    'valid_values': ['USD', 'EUR', 'GBP', 'JPY']}
    ),
    ValidationRule(
        rule_id='CUST_001',
        name='Email Format Validation',
        description='Email must be in valid format',
        rule_type='regex',
        severity=ValidationSeverity.ERROR,
        dimension=QualityDimension.VALIDITY,
        parameters={
            'pattern': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', 'column': 'email'}
    ),
    ValidationRule(
        rule_id='CUST_002',
        name='Age Validation',
        description='Customer must be at least 18 years old',
        rule_type='age_check',
        severity=ValidationSeverity.WARNING,
        dimension=QualityDimension.VALIDITY,
        parameters={'column': 'date_of_birth', 'min_age': 18}
    )
]


class ValidationEngine:
    """Core validation engine for executing rules"""

    def __init__(self, rules: List[ValidationRule]):
        self.rules = {rule.rule_id: rule for rule in rules}

    def validate_data(self, data: pd.DataFrame, rule_ids: List[str] = None) -> List[ValidationResult]:
        """Execute validation rules against data"""
        if rule_ids is None:
            rule_ids = list(self.rules.keys())

        results = []

        for rule_id in rule_ids:
            if rule_id not in self.rules:
                logging.warning(f"Rule {rule_id} not found")
                continue

            rule = self.rules[rule_id]
            if not rule.enabled:
                continue

            try:
                result = self._execute_rule(data, rule)
                results.append(result)
            except Exception as e:
                logging.error(f"Error executing rule {rule_id}: {str(e)}")
                results.append(ValidationResult(
                    rule_id=rule_id,
                    passed=False,
                    failed_count=len(data),
                    total_count=len(data),
                    pass_rate=0.0,
                    error_message=str(e)
                ))

        return results

    def _execute_rule(self, data: pd.DataFrame, rule: ValidationRule) -> ValidationResult:
        """Execute a single validation rule"""
        column = rule.parameters.get('column')

        if column not in data.columns:
            raise ValueError(f"Column {column} not found in data")

        series = data[column]
        total_count = len(series)

        if rule.rule_type == 'not_null':
            passed_mask = series.notna()

        elif rule.rule_type == 'regex':
            pattern = rule.parameters['pattern']
            passed_mask = series.astype(str).str.match(pattern, na=False)

        elif rule.rule_type == 'range':
            min_val = rule.parameters.get('min_value')
            max_val = rule.parameters.get('max_value')
            passed_mask = (series >= min_val) & (series <= max_val)

        elif rule.rule_type == 'enum':
            valid_values = rule.parameters['valid_values']
            passed_mask = series.isin(valid_values)

        elif rule.rule_type == 'age_check':
            min_age = rule.parameters['min_age']
            birth_dates = pd.to_datetime(series, errors='coerce')
            today = pd.Timestamp.now()
            ages = (today - birth_dates).dt.days / 365.25
            passed_mask = ages >= min_age

        else:
            raise ValueError(f"Unknown rule type: {rule.rule_type}")

        passed_count = passed_mask.sum()
        failed_count = total_count - passed_count
        pass_rate = passed_count / total_count if total_count > 0 else 0.0

        # Get failed values for reporting
        failed_values = None
        if failed_count > 0 and failed_count <= 10:  # Limit to 10 examples
            failed_values = series[~passed_mask].tolist()

        return ValidationResult(
            rule_id=rule.rule_id,
            passed=failed_count == 0,
            failed_count=failed_count,
            total_count=total_count,
            pass_rate=pass_rate,
            failed_values=failed_values
        )


class DataProfiler:
    """Data profiling engine for analyzing data characteristics"""

    @staticmethod
    def profile_dataframe(df: pd.DataFrame) -> List[DataProfile]:
        """Generate comprehensive data profile"""
        profiles = []

        for column in df.columns:
            series = df[column]
            profile = DataProfiler._profile_column(column, series)
            profiles.append(profile)

        return profiles

    @staticmethod
    def _profile_column(column_name: str, series: pd.Series) -> DataProfile:
        """Profile a single column"""
        total_count = len(series)
        null_count = series.isnull().sum()
        unique_count = series.nunique()

        # Determine data type
        if pd.api.types.is_numeric_dtype(series):
            data_type = 'numeric'
            min_value = series.min()
            max_value = series.max()
            mean_value = series.mean() if not series.empty else None
            std_dev = series.std() if not series.empty else None

            # Calculate percentiles for numeric data
            percentiles = {}
            if not series.empty and series.notna().any():
                percentiles = {
                    'p25': series.quantile(0.25),
                    'p50': series.quantile(0.50),
                    'p75': series.quantile(0.75),
                    'p90': series.quantile(0.90),
                    'p95': series.quantile(0.95)
                }

        elif pd.api.types.is_datetime64_any_dtype(series):
            data_type = 'datetime'
            min_value = series.min()
            max_value = series.max()
            mean_value = None
            std_dev = None
            percentiles = None

        else:
            data_type = 'categorical'
            min_value = None
            max_value = None
            mean_value = None
            std_dev = None
            percentiles = None

        return DataProfile(
            column_name=column_name,
            data_type=data_type,
            total_count=total_count,
            null_count=null_count,
            unique_count=unique_count,
            min_value=min_value,
            max_value=max_value,
            mean_value=mean_value,
            std_dev=std_dev,
            percentiles=percentiles
        )


class QualityScorer:
    """Calculate quality scores by dimension"""

    @staticmethod
    def calculate_scores(validation_results: List[ValidationResult], rules: List[ValidationRule]) -> List[QualityScore]:
        """Calculate quality scores by dimension"""
        rule_lookup = {rule.rule_id: rule for rule in rules}

        # Group results by dimension
        dimension_results = {}
        for result in validation_results:
            rule = rule_lookup.get(result.rule_id)
            if rule:
                dimension = rule.dimension
                if dimension not in dimension_results:
                    dimension_results[dimension] = []
                dimension_results[dimension].append(result)

        # Calculate scores for each dimension
        scores = []
        for dimension, results in dimension_results.items():
            score = QualityScorer._calculate_dimension_score(
                results, rule_lookup)
            scores.append(QualityScore(
                dimension=dimension,
                score=score,
                rule_results=results
            ))

        return scores

    @staticmethod
    def _calculate_dimension_score(results: List[ValidationResult], rule_lookup: Dict[str, ValidationRule]) -> float:
        """Calculate score for a single dimension"""
        if not results:
            return 100.0

        weighted_scores = []
        total_weight = 0

        for result in results:
            rule = rule_lookup.get(result.rule_id)
            if rule:
                # Weight by severity
                weight = {
                    ValidationSeverity.CRITICAL: 4,
                    ValidationSeverity.ERROR: 3,
                    ValidationSeverity.WARNING: 2,
                    ValidationSeverity.INFO: 1
                }.get(rule.severity, 1)

                score = result.pass_rate * 100
                weighted_scores.append(score * weight)
                total_weight += weight

        if total_weight == 0:
            return 100.0

        return sum(weighted_scores) / total_weight


def load_validation_rules(**context) -> List[Dict]:
    """Load validation rules configuration"""
    logging.info("Loading validation rules...")

    # Convert ValidationRule objects to dictionaries for XCom
    rules_dict = []
    for rule in VALIDATION_RULES:
        rule_dict = asdict(rule)
        # Convert enums to strings for JSON serialization
        rule_dict['severity'] = rule.severity.value
        rule_dict['dimension'] = rule.dimension.value
        rules_dict.append(rule_dict)

    logging.info(f"Loaded {len(rules_dict)} validation rules")

    context['task_instance'].xcom_push(
        key='validation_rules', value=rules_dict)
    return rules_dict


def profile_transaction_data(**context) -> List[Dict]:
    """Profile transaction data to understand characteristics"""
    logging.info("Profiling transaction data...")

    # Load sample data
    df = pd.DataFrame(SAMPLE_TRANSACTION_DATA)

    # Generate profiles
    profiler = DataProfiler()
    profiles = profiler.profile_dataframe(df)

    # Convert to dictionaries for XCom
    profiles_dict = []
    for profile in profiles:
        profile_dict = asdict(profile)
        # Handle non-serializable values
        for key, value in profile_dict.items():
            if pd.isna(value):
                profile_dict[key] = None
            elif isinstance(value, (np.integer, np.floating)):
                profile_dict[key] = float(value)
        profiles_dict.append(profile_dict)

    logging.info(f"Generated profiles for {len(profiles_dict)} columns")

    # Store data and profiles
    context['task_instance'].xcom_push(
        key='transaction_data', value=SAMPLE_TRANSACTION_DATA)
    context['task_instance'].xcom_push(
        key='transaction_profiles', value=profiles_dict)

    return profiles_dict


def profile_customer_data(**context) -> List[Dict]:
    """Profile customer data to understand characteristics"""
    logging.info("Profiling customer data...")

    # Load sample data
    df = pd.DataFrame(SAMPLE_CUSTOMER_DATA)

    # Generate profiles
    profiler = DataProfiler()
    profiles = profiler.profile_dataframe(df)

    # Convert to dictionaries for XCom
    profiles_dict = []
    for profile in profiles:
        profile_dict = asdict(profile)
        # Handle non-serializable values
        for key, value in profile_dict.items():
            if pd.isna(value):
                profile_dict[key] = None
            elif isinstance(value, (np.integer, np.floating)):
                profile_dict[key] = float(value)
        profiles_dict.append(profile_dict)

    logging.info(f"Generated profiles for {len(profiles_dict)} columns")

    # Store data and profiles
    context['task_instance'].xcom_push(
        key='customer_data', value=SAMPLE_CUSTOMER_DATA)
    context['task_instance'].xcom_push(
        key='customer_profiles', value=profiles_dict)

    return profiles_dict


def validate_transactions(**context) -> List[Dict]:
    """Validate transaction data against rules"""
    logging.info("Validating transaction data...")

    # Get data and rules
    transaction_data = context['task_instance'].xcom_pull(
        key='transaction_data')
    rules_dict = context['task_instance'].xcom_pull(key='validation_rules')

    # Convert back to objects
    rules = []
    for rule_dict in rules_dict:
        rule_dict['severity'] = ValidationSeverity(rule_dict['severity'])
        rule_dict['dimension'] = QualityDimension(rule_dict['dimension'])
        rules.append(ValidationRule(**rule_dict))

    # Filter rules for transactions
    transaction_rules = [
        rule for rule in rules if rule.rule_id.startswith('TXN_')]

    # Create DataFrame and validate
    df = pd.DataFrame(transaction_data)
    engine = ValidationEngine(transaction_rules)
    results = engine.validate_data(df)

    # Convert results to dictionaries
    results_dict = []
    for result in results:
        result_dict = asdict(result)
        results_dict.append(result_dict)

    logging.info(f"Validated transaction data with {len(results_dict)} rules")

    context['task_instance'].xcom_push(
        key='transaction_validation_results', value=results_dict)
    return results_dict


def validate_customers(**context) -> List[Dict]:
    """Validate customer data against rules"""
    logging.info("Validating customer data...")

    # Get data and rules
    customer_data = context['task_instance'].xcom_pull(key='customer_data')
    rules_dict = context['task_instance'].xcom_pull(key='validation_rules')

    # Convert back to objects
    rules = []
    for rule_dict in rules_dict:
        rule_dict['severity'] = ValidationSeverity(rule_dict['severity'])
        rule_dict['dimension'] = QualityDimension(rule_dict['dimension'])
        rules.append(ValidationRule(**rule_dict))

    # Filter rules for customers
    customer_rules = [
        rule for rule in rules if rule.rule_id.startswith('CUST_')]

    # Create DataFrame and validate
    df = pd.DataFrame(customer_data)
    engine = ValidationEngine(customer_rules)
    results = engine.validate_data(df)

    # Convert results to dictionaries
    results_dict = []
    for result in results:
        result_dict = asdict(result)
        results_dict.append(result_dict)

    logging.info(f"Validated customer data with {len(results_dict)} rules")

    context['task_instance'].xcom_push(
        key='customer_validation_results', value=results_dict)
    return results_dict


def generate_quality_report(**context) -> Dict[str, Any]:
    """Generate comprehensive data quality report"""
    logging.info("Generating quality report...")

    # Get all validation results and rules
    transaction_results = context['task_instance'].xcom_pull(
        key='transaction_validation_results')
    customer_results = context['task_instance'].xcom_pull(
        key='customer_validation_results')
    rules_dict = context['task_instance'].xcom_pull(key='validation_rules')

    # Combine all results
    all_results = []
    for result_dict in transaction_results + customer_results:
        all_results.append(ValidationResult(**result_dict))

    # Convert rules back to objects
    rules = []
    for rule_dict in rules_dict:
        rule_dict['severity'] = ValidationSeverity(rule_dict['severity'])
        rule_dict['dimension'] = QualityDimension(rule_dict['dimension'])
        rules.append(ValidationRule(**rule_dict))

    # Calculate quality scores
    scorer = QualityScorer()
    quality_scores = scorer.calculate_scores(all_results, rules)

    # Generate report
    report = {
        'report_timestamp': datetime.now().isoformat(),
        'execution_date': context['ds'],
        'summary': {
            'total_rules_executed': len(all_results),
            'rules_passed': sum(1 for r in all_results if r.passed),
            'rules_failed': sum(1 for r in all_results if not r.passed),
            'overall_pass_rate': sum(r.pass_rate for r in all_results) / len(all_results) if all_results else 0
        },
        'quality_scores': {},
        'validation_details': [],
        'critical_issues': [],
        'recommendations': []
    }

    # Add quality scores by dimension
    for score in quality_scores:
        report['quality_scores'][score.dimension.value] = {
            'score': round(score.score, 2),
            'rules_count': len(score.rule_results),
            'failed_rules': [r.rule_id for r in score.rule_results if not r.passed]
        }

    # Add validation details
    for result in all_results:
        detail = {
            'rule_id': result.rule_id,
            'passed': result.passed,
            'pass_rate': round(result.pass_rate * 100, 2),
            'failed_count': result.failed_count,
            'total_count': result.total_count
        }

        if result.failed_values:
            # Limit examples
            detail['sample_failed_values'] = result.failed_values[:5]

        report['validation_details'].append(detail)

    # Identify critical issues
    for result in all_results:
        rule = next((r for r in rules if r.rule_id == result.rule_id), None)
        if rule and not result.passed and rule.severity in [ValidationSeverity.CRITICAL, ValidationSeverity.ERROR]:
            report['critical_issues'].append({
                'rule_id': result.rule_id,
                'rule_name': rule.name,
                'severity': rule.severity.value,
                'failed_count': result.failed_count,
                'description': rule.description
            })

    # Generate recommendations
    if report['critical_issues']:
        report['recommendations'].append(
            "Address critical data quality issues immediately")

    low_scores = [
        dim for dim, score in report['quality_scores'].items() if score['score'] < 80]
    if low_scores:
        report['recommendations'].append(
            f"Focus on improving {', '.join(low_scores)} dimensions")

    logging.info(
        f"Generated quality report with {len(report['validation_details'])} validation results")

    # Save report
    report_file = f"/tmp/quality_report_{context['ds']}.json"
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)

    context['task_instance'].xcom_push(key='quality_report', value=report)
    return report


def send_quality_alerts(**context) -> List[Dict]:
    """Send quality alerts based on validation results"""
    logging.info("Processing quality alerts...")

    quality_report = context['task_instance'].xcom_pull(key='quality_report')

    alerts = []

    # Critical issues alerts
    for issue in quality_report['critical_issues']:
        alert = {
            'alert_id': f"ALERT_{issue['rule_id']}_{context['ds']}",
            'severity': issue['severity'],
            'title': f"Data Quality Issue: {issue['rule_name']}",
            'description': issue['description'],
            'failed_count': issue['failed_count'],
            'timestamp': datetime.now().isoformat(),
            'action_required': True
        }
        alerts.append(alert)

    # Quality score alerts
    for dimension, score_info in quality_report['quality_scores'].items():
        if score_info['score'] < 70:  # Critical threshold
            alert = {
                'alert_id': f"ALERT_SCORE_{dimension}_{context['ds']}",
                'severity': 'CRITICAL',
                'title': f"Low Quality Score: {dimension.title()}",
                'description': f"Quality score for {dimension} is {score_info['score']:.1f}%",
                'failed_rules': score_info['failed_rules'],
                'timestamp': datetime.now().isoformat(),
                'action_required': True
            }
            alerts.append(alert)
        elif score_info['score'] < 85:  # Warning threshold
            alert = {
                'alert_id': f"ALERT_SCORE_{dimension}_{context['ds']}",
                'severity': 'WARNING',
                'title': f"Quality Score Below Target: {dimension.title()}",
                'description': f"Quality score for {dimension} is {score_info['score']:.1f}%",
                'failed_rules': score_info['failed_rules'],
                'timestamp': datetime.now().isoformat(),
                'action_required': False
            }
            alerts.append(alert)

    # Log alerts (in production, send to alerting system)
    for alert in alerts:
        if alert['severity'] == 'CRITICAL':
            logging.error(
                f"CRITICAL ALERT: {alert['title']} - {alert['description']}")
        elif alert['severity'] == 'WARNING':
            logging.warning(
                f"WARNING ALERT: {alert['title']} - {alert['description']}")
        else:
            logging.info(
                f"INFO ALERT: {alert['title']} - {alert['description']}")

    logging.info(f"Generated {len(alerts)} quality alerts")

    return alerts


# Task Groups
with TaskGroup("data_profiling", dag=dag) as profiling_group:
    profile_transactions_task = PythonOperator(
        task_id='profile_transaction_data',
        python_callable=profile_transaction_data,
    )

    profile_customers_task = PythonOperator(
        task_id='profile_customer_data',
        python_callable=profile_customer_data,
    )

with TaskGroup("data_validation", dag=dag) as validation_group:
    validate_transactions_task = PythonOperator(
        task_id='validate_transactions',
        python_callable=validate_transactions,
    )

    validate_customers_task = PythonOperator(
        task_id='validate_customers',
        python_callable=validate_customers,
    )

# Main tasks
load_rules_task = PythonOperator(
    task_id='load_validation_rules',
    python_callable=load_validation_rules,
    dag=dag,
)

quality_report_task = PythonOperator(
    task_id='generate_quality_report',
    python_callable=generate_quality_report,
    dag=dag,
)

alerts_task = PythonOperator(
    task_id='send_quality_alerts',
    python_callable=send_quality_alerts,
    dag=dag,
)

# Dashboard update task (simulated)
dashboard_task = BashOperator(
    task_id='update_quality_dashboard',
    bash_command="""
    echo "Updating quality dashboard with latest metrics..."
    echo "Dashboard updated successfully"
    """,
    dag=dag,
)

# Define dependencies
load_rules_task >> [profile_transactions_task, profile_customers_task]
profile_transactions_task >> validate_transactions_task
profile_customers_task >> validate_customers_task
[validate_transactions_task, validate_customers_task] >> quality_report_task
quality_report_task >> [alerts_task, dashboard_task]
