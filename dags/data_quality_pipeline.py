from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.task_group import TaskGroup


# Python functions for the data quality pipeline

def initialize_quality_checks(**context):
    """Set up quality check parameters"""
    execution_date = context['execution_date']
    print(
        f"Initializing quality checks for {execution_date.strftime('%Y-%m-%d')}")

    # Define quality check configuration
    quality_config = {
        'completeness_threshold': 0.95,  # 95% completeness required
        'accuracy_threshold': 0.90,     # 90% accuracy required
        'consistency_threshold': 0.85,  # 85% consistency required
        'timeliness_threshold': 24,     # Data must be within 24 hours
        'overall_quality_threshold': 0.80,  # 80% overall quality required
        'check_timestamp': datetime.now().isoformat()
    }

    print("  - Quality check thresholds configured:")
    for metric, threshold in quality_config.items():
        if metric != 'check_timestamp':
            if isinstance(threshold, float):
                print(f"    {metric}: {threshold:.0%}")
            else:
                print(f"    {metric}: {threshold}")

    return quality_config

def prepare_validation_rules(**context):
    """Load validation rules and thresholds"""
    print("Preparing validation rules...")

    # Define validation rules for different data types
    validation_rules = {
        'customer_data': {
            'required_fields': ['customer_id', 'email', 'registration_date'],
            'email_format_regex': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
            'id_format': 'numeric',
            'date_range': {'min': '2020-01-01', 'max': datetime.now().strftime('%Y-%m-%d')}
        },
        'transaction_data': {
            'required_fields': ['transaction_id', 'customer_id', 'amount', 'timestamp'],
            'amount_range': {'min': 0.01, 'max': 10000.00},
            'currency_codes': ['USD', 'EUR', 'GBP', 'CAD'],
            'status_values': ['completed', 'pending', 'cancelled', 'refunded']
        },
        'product_data': {
            'required_fields': ['product_id', 'name', 'category', 'price'],
            'price_range': {'min': 0.01, 'max': 5000.00},
            'categories': ['Electronics', 'Clothing', 'Home', 'Sports', 'Books'],
            'name_length': {'min': 3, 'max': 100}
        },
        'inventory_data': {
            'required_fields': ['product_id', 'quantity', 'location', 'last_updated'],
            'quantity_range': {'min': 0, 'max': 10000},
            'locations': ['warehouse_a', 'warehouse_b', 'store_1', 'store_2'],
            'update_frequency_hours': 6
        }
    }

    print("  - Validation rules loaded:")
    for data_type, rules in validation_rules.items():
        print(f"    {data_type}: {len(rules)} rule categories")

    return validation_rules

def collect_data(data_type):
    """Factory function for data collection tasks"""
    def _collect(**context):
        execution_date = context['execution_date']
        print(
            f"Collecting {data_type} data for {execution_date.strftime('%Y-%m-%d')}")

        # Simulate data collection process
        print(f"  - Connecting to {data_type} source...")
        print(f"  - Querying {data_type} tables...")
        print(f"  - Extracting {data_type} records...")

        # Simulate collected data with some quality issues
        import random

        base_records = random.randint(10000, 50000)

        # Introduce some data quality issues randomly
        quality_issues = {
            'missing_records': random.randint(0, int(base_records * 0.05)),
            'duplicate_records': random.randint(0, int(base_records * 0.02)),
            'invalid_format': random.randint(0, int(base_records * 0.03)),
            'outdated_records': random.randint(0, int(base_records * 0.01))
        }

        clean_records = base_records - sum(quality_issues.values())

        collection_result = {
            'data_type': data_type,
            'total_records': base_records,
            'clean_records': clean_records,
            'quality_issues': quality_issues,
            'collection_timestamp': datetime.now().isoformat(),
            'source_system': f"{data_type}_system",
            'data_freshness_hours': random.uniform(0.5, 12.0)
        }

        print(f"  - {data_type} collection completed:")
        print(f"    Total records: {base_records:,}")
        print(f"    Clean records: {clean_records:,}")
        print(f"    Quality issues: {sum(quality_issues.values()):,}")
        print(
            f"    Data freshness: {collection_result['data_freshness_hours']:.1f} hours")

        return collection_result

    return _collect


def validate_data_quality(data_type):
    """Factory function for data quality validation"""
    def _validate(**context):
        # Get collected data and validation rules
        prefix = "data_collection."
        collection_data = context['task_instance'].xcom_pull(
            task_ids=f'{prefix}collect_{data_type}_data')
        validation_rules = context['task_instance'].xcom_pull(
            task_ids='prepare_validation_rules')
        quality_config = context['task_instance'].xcom_pull(
            task_ids='initialize_quality_checks')

        print(f"Validating {data_type} data quality...")

        # Calculate quality metrics
        total_records = collection_data['total_records']
        clean_records = collection_data['clean_records']
        quality_issues = collection_data['quality_issues']

        # Completeness: percentage of non-missing records
        completeness = (
            total_records - quality_issues['missing_records']) / total_records

        # Accuracy: percentage of records without format issues
        accuracy = (total_records -
                    quality_issues['invalid_format']) / total_records

        # Consistency: percentage of records without duplicates
        consistency = (total_records -
                       quality_issues['duplicate_records']) / total_records

        # Timeliness: based on data freshness
        timeliness = 1.0 if collection_data['data_freshness_hours'] <= quality_config['timeliness_threshold'] else 0.5

        # Overall quality score (weighted average)
        overall_quality = (completeness * 0.3 + accuracy *
                           0.3 + consistency * 0.2 + timeliness * 0.2)

        # Determine if validation passed
        validation_passed = (
            completeness >= quality_config['completeness_threshold'] and
            accuracy >= quality_config['accuracy_threshold'] and
            consistency >= quality_config['consistency_threshold'] and
            overall_quality >= quality_config['overall_quality_threshold']
        )

        validation_result = {
            'data_type': data_type,
            'total_records': total_records,
            'quality_metrics': {
                'completeness': completeness,
                'accuracy': accuracy,
                'consistency': consistency,
                'timeliness': timeliness,
                'overall_quality': overall_quality
            },
            'validation_passed': validation_passed,
            'quality_issues': quality_issues,
            'validation_timestamp': datetime.now().isoformat()
        }

        print(f"  - {data_type} quality validation results:")
        print(
            f"    Completeness: {completeness:.1%} (threshold: {quality_config['completeness_threshold']:.1%})")
        print(
            f"    Accuracy: {accuracy:.1%} (threshold: {quality_config['accuracy_threshold']:.1%})")
        print(
            f"    Consistency: {consistency:.1%} (threshold: {quality_config['consistency_threshold']:.1%})")
        print(f"    Timeliness: {timeliness:.1%}")
        print(
            f"    Overall Quality: {overall_quality:.1%} (threshold: {quality_config['overall_quality_threshold']:.1%})")
        print(
            f"    Validation: {'✓ PASSED' if validation_passed else '✗ FAILED'}")

        return validation_result

    return _validate


def assess_overall_quality(**context):
    """Evaluate overall data quality score"""
    print("Assessing overall data quality...")

    # Pull validation results from all data types
    data_types = ['customer', 'transaction', 'product', 'inventory']
    validation_results = {}

    for data_type in data_types:
        prefix = "primary_validation."
        task_id = f'{prefix}validate_{data_type}_quality'
        result = context['task_instance'].xcom_pull(task_ids=task_id)
        validation_results[data_type] = result

        status = "PASSED" if result['validation_passed'] else "FAILED"
        print(
            f"  - {data_type}: {result['quality_metrics']['overall_quality']:.1%} ({status})")

    # Calculate overall system quality
    total_records = sum(result['total_records']
                        for result in validation_results.values())
    weighted_quality = sum(
        result['quality_metrics']['overall_quality'] * result['total_records']
        for result in validation_results.values()
    ) / total_records

    # Count passed validations
    passed_validations = sum(
        1 for result in validation_results.values() if result['validation_passed'])
    validation_pass_rate = passed_validations / len(validation_results)

    # Determine overall assessment
    if weighted_quality >= 0.9 and validation_pass_rate >= 0.75:
        overall_status = 'HIGH_QUALITY'
    elif weighted_quality >= 0.7 and validation_pass_rate >= 0.5:
        overall_status = 'MEDIUM_QUALITY'
    else:
        overall_status = 'POOR_QUALITY'

    assessment_result = {
        'overall_quality_score': weighted_quality,
        'validation_pass_rate': validation_pass_rate,
        'overall_status': overall_status,
        'total_records_assessed': total_records,
        'data_type_results': validation_results,
        'assessment_timestamp': datetime.now().isoformat()
    }

    print(f"  - Overall Quality Assessment:")
    print(f"    Weighted Quality Score: {weighted_quality:.1%}")
    print(
        f"    Validation Pass Rate: {validation_pass_rate:.1%} ({passed_validations}/{len(validation_results)})")
    print(f"    Overall Status: {overall_status}")
    print(f"    Total Records: {total_records:,}")

    return assessment_result


def choose_processing_path(**context):
    """Determine processing path based on quality assessment"""
    assessment = context['task_instance'].xcom_pull(
        task_ids='assess_overall_quality')

    overall_status = assessment['overall_status']

    print(
        f"Choosing processing path based on quality status: {overall_status}")

    if overall_status == 'HIGH_QUALITY':
        print("  - Routing to high quality data processing")
        return 'process_high_quality_data'
    elif overall_status == 'MEDIUM_QUALITY':
        print("  - Routing to medium quality data processing with additional validation")
        return 'process_medium_quality_data'
    else:
        print("  - Routing to poor quality data quarantine and remediation")
        return 'quarantine_poor_quality_data'


def process_high_quality_data(**context):
    """Process data that passed all checks"""
    assessment = context['task_instance'].xcom_pull(
        task_ids='assess_overall_quality')

    print("Processing high quality data...")
    print(f"Quality score: {assessment['overall_quality_score']:.1%}")

    # Simulate high-quality data processing
    processing_steps = [
        'Loading data into production pipeline',
        'Applying standard transformations',
        'Generating business metrics',
        'Updating real-time dashboards',
        'Triggering downstream workflows'
    ]

    for step in processing_steps:
        print(f"  - {step}...")
        import time
        time.sleep(0.2)

    result = {
        'processing_type': 'high_quality',
        'records_processed': assessment['total_records_assessed'],
        'processing_success': True,
        'processing_timestamp': datetime.now().isoformat()
    }

    print("  - High quality data processing completed successfully!")
    return result


def process_medium_quality_data(**context):
    """Process data with additional validation"""
    assessment = context['task_instance'].xcom_pull(
        task_ids='assess_overall_quality')

    print("Processing medium quality data with additional validation...")
    print(f"Quality score: {assessment['overall_quality_score']:.1%}")

    # Simulate medium-quality data processing with extra steps
    processing_steps = [
        'Applying additional data cleansing rules',
        'Running enhanced validation checks',
        'Flagging suspicious records for review',
        'Processing clean subset of data',
        'Generating quality improvement recommendations'
    ]

    for step in processing_steps:
        print(f"  - {step}...")
        import time
        time.sleep(0.3)

    # Simulate some records being filtered out
    import random
    filtered_records = int(
        assessment['total_records_assessed'] * random.uniform(0.05, 0.15))
    processed_records = assessment['total_records_assessed'] - filtered_records

    result = {
        'processing_type': 'medium_quality',
        'records_input': assessment['total_records_assessed'],
        'records_filtered': filtered_records,
        'records_processed': processed_records,
        'processing_success': True,
        'processing_timestamp': datetime.now().isoformat()
    }

    print(f"  - Medium quality processing completed!")
    print(f"  - Records processed: {processed_records:,}")
    print(f"  - Records filtered: {filtered_records:,}")

    return result


def quarantine_poor_quality_data(**context):
    """Isolate data that failed checks"""
    assessment = context['task_instance'].xcom_pull(
        task_ids='assess_overall_quality')

    print("Quarantining poor quality data...")
    print(f"Quality score: {assessment['overall_quality_score']:.1%}")

    # Simulate quarantine process
    quarantine_steps = [
        'Moving data to quarantine storage',
        'Cataloging quality issues by type',
        'Creating data remediation tasks',
        'Notifying data stewards',
        'Scheduling quality improvement review'
    ]

    for step in quarantine_steps:
        print(f"  - {step}...")
        import time
        time.sleep(0.2)

    # Analyze quality issues across all data types
    total_issues = {}
    for data_type, result in assessment['data_type_results'].items():
        for issue_type, count in result['quality_issues'].items():
            total_issues[issue_type] = total_issues.get(issue_type, 0) + count

    result = {
        'processing_type': 'quarantine',
        'records_quarantined': assessment['total_records_assessed'],
        'quality_issues_summary': total_issues,
        'remediation_required': True,
        'quarantine_timestamp': datetime.now().isoformat()
    }

    print("  - Data quarantine completed!")
    print("  - Quality issues summary:")
    for issue_type, count in total_issues.items():
        print(f"    {issue_type}: {count:,} records")

    return result


def generate_quality_alerts(**context):
    """Create alerts for quality issues"""
    assessment = context['task_instance'].xcom_pull(
        task_ids='assess_overall_quality')

    print("Generating data quality alerts...")

    # Determine alert severity
    quality_score = assessment['overall_quality_score']
    if quality_score < 0.5:
        alert_level = 'CRITICAL'
    elif quality_score < 0.7:
        alert_level = 'WARNING'
    else:
        alert_level = 'INFO'

    # Create alerts for each data type with issues
    alerts = []
    for data_type, result in assessment['data_type_results'].items():
        if not result['validation_passed']:
            alert = {
                'data_type': data_type,
                'alert_level': alert_level,
                'quality_score': result['quality_metrics']['overall_quality'],
                'issues': result['quality_issues'],
                'alert_timestamp': datetime.now().isoformat()
            }
            alerts.append(alert)

    print(f"  - Generated {len(alerts)} quality alerts (Level: {alert_level})")

    # Simulate alert distribution
    if alerts:
        print("  - Sending alerts to:")
        print("    • Data quality team")
        print("    • Data stewards")
        print("    • Operations team")
        if alert_level == 'CRITICAL':
            print("    • Management team (critical alert)")

    return {
        'alerts_generated': len(alerts),
        'alert_level': alert_level,
        'alerts': alerts,
        'overall_quality_score': quality_score
    }


def cross_validate_data(**context):
    """Perform cross-dataset validation"""
    print("Performing cross-dataset validation...")

    # Get processing results from the chosen path
    ti = context['task_instance']

    # Try to get results from any of the processing paths
    processing_result = None
    for task_id in ['process_high_quality_data', 'process_medium_quality_data', 'quarantine_poor_quality_data']:
        try:
            result = ti.xcom_pull(task_ids=task_id)
            if result:
                processing_result = result
                break
        except:
            continue

    if not processing_result:
        print("  - No processing result found, skipping cross-validation")
        return {'cross_validation_skipped': True}

    # Simulate cross-validation checks
    validation_checks = [
        'Customer-Transaction ID consistency',
        'Product-Inventory ID alignment',
        'Transaction-Customer relationship integrity',
        'Inventory-Product catalog synchronization'
    ]

    cross_validation_results = {}
    overall_consistency = 0

    for check in validation_checks:
        print(f"  - Running {check}...")

        # Simulate validation result
        import random
        consistency_score = random.uniform(0.85, 1.0)
        overall_consistency += consistency_score

        cross_validation_results[check] = {
            'consistency_score': consistency_score,
            'passed': consistency_score > 0.9
        }

        status = "✓ PASSED" if consistency_score > 0.9 else "⚠ WARNING"
        print(f"    {status} ({consistency_score:.1%})")

    overall_consistency /= len(validation_checks)

    result = {
        'cross_validation_results': cross_validation_results,
        'overall_consistency': overall_consistency,
        'validation_passed': overall_consistency > 0.9,
        'cross_validation_timestamp': datetime.now().isoformat()
    }

    print(f"  - Cross-validation completed!")
    print(f"  - Overall consistency: {overall_consistency:.1%}")
    print(
        f"  - Validation: {'✓ PASSED' if result['validation_passed'] else '⚠ WARNING'}")

    return result


def validate_business_rules(**context):
    """Check business logic constraints"""
    print("Validating business rules...")

    # Define business rules to check
    business_rules = [
        'Customer registration date must be before first transaction',
        'Transaction amounts must be positive',
        'Product prices must be within reasonable ranges',
        'Inventory quantities cannot be negative',
        'Customer email addresses must be unique'
    ]

    rule_results = {}

    for rule in business_rules:
        print(f"  - Checking: {rule}")

        # Simulate business rule validation
        import random
        compliance_rate = random.uniform(0.90, 1.0)
        violations = random.randint(0, 50)

        rule_results[rule] = {
            'compliance_rate': compliance_rate,
            'violations_found': violations,
            'rule_passed': compliance_rate > 0.95 and violations < 10
        }

        status = "✓ PASSED" if rule_results[rule]['rule_passed'] else "✗ FAILED"
        print(
            f"    {status} (Compliance: {compliance_rate:.1%}, Violations: {violations})")

    # Calculate overall business rule compliance
    overall_compliance = sum(result['compliance_rate']
                             for result in rule_results.values()) / len(rule_results)
    rules_passed = sum(1 for result in rule_results.values()
                       if result['rule_passed'])

    validation_result = {
        'business_rule_results': rule_results,
        'overall_compliance': overall_compliance,
        'rules_passed': rules_passed,
        'total_rules': len(business_rules),
        # 80% of rules must pass
        'validation_passed': rules_passed >= len(business_rules) * 0.8,
        'business_validation_timestamp': datetime.now().isoformat()
    }

    print(f"  - Business rule validation completed!")
    print(f"  - Overall compliance: {overall_compliance:.1%}")
    print(f"  - Rules passed: {rules_passed}/{len(business_rules)}")

    return validation_result


def generate_quality_report(**context):
    """Create comprehensive quality report"""
    print("Generating comprehensive data quality report...")

    # Gather all results
    assessment = context['task_instance'].xcom_pull(
        task_ids='assess_overall_quality')
    cross_validation = context['task_instance'].xcom_pull(
        task_ids='cross_validate_data')
    business_validation = context['task_instance'].xcom_pull(
        task_ids='validate_business_rules')

    # Create comprehensive report
    report = {
        'report_date': datetime.now().strftime('%Y-%m-%d'),
        'overall_quality_score': assessment['overall_quality_score'],
        'data_types_assessed': len(assessment['data_type_results']),
        'total_records': assessment['total_records_assessed'],
        'validation_summary': {
            'primary_validation_pass_rate': assessment['validation_pass_rate'],
            'cross_validation_passed': cross_validation.get('validation_passed', False),
            'business_rules_passed': business_validation['validation_passed']
        },
        'recommendations': [],
        'report_timestamp': datetime.now().isoformat()
    }

    # Generate recommendations based on results
    if assessment['overall_quality_score'] < 0.8:
        report['recommendations'].append(
            'Implement additional data cleansing procedures')

    if not cross_validation.get('validation_passed', True):
        report['recommendations'].append(
            'Review cross-dataset consistency rules')

    if not business_validation['validation_passed']:
        report['recommendations'].append(
            'Strengthen business rule enforcement')

    print("  - Quality report generated:")
    print(f"    Overall Quality Score: {report['overall_quality_score']:.1%}")
    print(f"    Data Types Assessed: {report['data_types_assessed']}")
    print(f"    Total Records: {report['total_records']:,}")
    print(f"    Recommendations: {len(report['recommendations'])}")

    if report['recommendations']:
        print("  - Key recommendations:")
        for rec in report['recommendations']:
            print(f"    • {rec}")

    return report


def update_quality_metrics(**context):
    """Update quality tracking dashboard"""
    quality_report = context['task_instance'].xcom_pull(
        task_ids='generate_quality_report')

    print("Updating quality metrics dashboard...")

    # Simulate dashboard updates
    dashboard_updates = {
        'quality_score_trend': quality_report['overall_quality_score'],
        'validation_pass_rates': quality_report['validation_summary'],
        'total_records_processed': quality_report['total_records'],
        'recommendations_count': len(quality_report['recommendations']),
        'last_updated': datetime.now().isoformat()
    }

    print("  - Dashboard metrics updated:")
    for metric, value in dashboard_updates.items():
        if metric != 'last_updated':
            print(f"    {metric}: {value}")

    print("  - Quality trend charts refreshed")
    print("  - Alert thresholds updated")
    print("  - Historical data archived")

    return dashboard_updates


def archive_processed_data(**context):
    """Archive successfully processed data"""
    quality_report = context['task_instance'].xcom_pull(
        task_ids='generate_quality_report')

    print("Archiving processed data...")

    # Simulate archival process
    archive_summary = {
        'records_archived': quality_report['total_records'],
        'quality_score': quality_report['overall_quality_score'],
        'archive_location': f"s3://data-quality-archive/{datetime.now().strftime('%Y/%m/%d')}/",
        'retention_days': 90,
        'archive_timestamp': datetime.now().isoformat()
    }

    print(f"  - Archived {archive_summary['records_archived']:,} records")
    print(f"  - Archive location: {archive_summary['archive_location']}")
    print(f"  - Retention period: {archive_summary['retention_days']} days")
    print("  - Archive metadata updated")
    print("  - Data lineage recorded")

    return archive_summary


# DAG configuration
default_args = {
    'owner': 'data_quality_team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

dag = DAG(
    dag_id='data_quality_pipeline',
    description='Multi-stage data quality validation and processing pipeline',
    schedule_interval=timedelta(hours=6),  # Every 6 hours
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=2,
    default_args=default_args,
    tags=['data-quality', 'validation', 'multi-stage']
)

# Phase 1: Initial Setup
initialize_quality_checks_task = PythonOperator(
    task_id='initialize_quality_checks',
    python_callable=initialize_quality_checks,
    dag=dag
)

prepare_validation_rules_task = PythonOperator(
    task_id='prepare_validation_rules',
    python_callable=prepare_validation_rules,
    dag=dag
)

# Phase 2: Data Collection (Parallel)
with TaskGroup('data_collection', dag=dag) as data_collection_group:
    collect_customer_data_task = PythonOperator(
        task_id='collect_customer_data',
        python_callable=collect_data('customer')
    )

    collect_transaction_data_task = PythonOperator(
        task_id='collect_transaction_data',
        python_callable=collect_data('transaction')
    )

    collect_product_data_task = PythonOperator(
        task_id='collect_product_data',
        python_callable=collect_data('product')
    )

    collect_inventory_data_task = PythonOperator(
        task_id='collect_inventory_data',
        python_callable=collect_data('inventory')
    )

# Phase 3: Primary Validation (Parallel)
with TaskGroup('primary_validation', dag=dag) as primary_validation_group:
    validate_customer_quality_task = PythonOperator(
        task_id='validate_customer_quality',
        python_callable=validate_data_quality('customer')
    )

    validate_transaction_quality_task = PythonOperator(
        task_id='validate_transaction_quality',
        python_callable=validate_data_quality('transaction')
    )

    validate_product_quality_task = PythonOperator(
        task_id='validate_product_quality',
        python_callable=validate_data_quality('product')
    )

    validate_inventory_quality_task = PythonOperator(
        task_id='validate_inventory_quality',
        python_callable=validate_data_quality('inventory')
    )

# Phase 4: Quality Assessment
assess_overall_quality_task = PythonOperator(
    task_id='assess_overall_quality',
    python_callable=assess_overall_quality,
    dag=dag
)

# Phase 5: Conditional Processing
choose_processing_path_task = BranchPythonOperator(
    task_id='choose_processing_path',
    python_callable=choose_processing_path,
    dag=dag
)

# Conditional processing tasks
process_high_quality_data_task = PythonOperator(
    task_id='process_high_quality_data',
    python_callable=process_high_quality_data,
    dag=dag
)

process_medium_quality_data_task = PythonOperator(
    task_id='process_medium_quality_data',
    python_callable=process_medium_quality_data,
    dag=dag
)

quarantine_poor_quality_data_task = PythonOperator(
    task_id='quarantine_poor_quality_data',
    python_callable=quarantine_poor_quality_data,
    dag=dag
)

generate_quality_alerts_task = PythonOperator(
    task_id='generate_quality_alerts',
    python_callable=generate_quality_alerts,
    dag=dag
)

# Phase 6: Secondary Validation
cross_validate_data_task = PythonOperator(
    task_id='cross_validate_data',
    python_callable=cross_validate_data,
    dag=dag,
    # Run regardless of which processing path was taken
    trigger_rule='none_failed_or_skipped'
)

validate_business_rules_task = PythonOperator(
    task_id='validate_business_rules',
    python_callable=validate_business_rules,
    dag=dag
)

# Phase 7: Final Processing
generate_quality_report_task = PythonOperator(
    task_id='generate_quality_report',
    python_callable=generate_quality_report,
    dag=dag
)

update_quality_metrics_task = PythonOperator(
    task_id='update_quality_metrics',
    python_callable=update_quality_metrics,
    dag=dag
)

archive_processed_data_task = PythonOperator(
    task_id='archive_processed_data',
    python_callable=archive_processed_data,
    dag=dag
)

# Phase 1: Sequential setup
initialize_quality_checks_task >> prepare_validation_rules_task

# Phase 2: Fan-out to data collection
prepare_validation_rules_task >> data_collection_group

# Phase 3: Fan-out to primary validation
data_collection_group >> primary_validation_group

# Phase 4: Fan-in to quality assessment
primary_validation_group >> assess_overall_quality_task

# Phase 5: Conditional branching
assess_overall_quality_task >> choose_processing_path_task
choose_processing_path_task >> [process_high_quality_data_task,
                                process_medium_quality_data_task, quarantine_poor_quality_data_task]

# Quality alerts run in parallel with processing
assess_overall_quality_task >> generate_quality_alerts_task

# Phase 6: Secondary validation (runs after any processing path)
[process_high_quality_data_task, process_medium_quality_data_task,
    quarantine_poor_quality_data_task] >> cross_validate_data_task
cross_validate_data_task >> validate_business_rules_task

# Phase 7: Final processing
[validate_business_rules_task,
    generate_quality_alerts_task] >> generate_quality_report_task
generate_quality_report_task >> [
    update_quality_metrics_task, archive_processed_data_task]