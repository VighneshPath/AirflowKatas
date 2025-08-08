"""
Solution for Exercise 2: Data-Driven Branching

This solution demonstrates:
- Complex data quality assessment and branching
- Multi-source data processing with parallel paths
- Priority-based resource allocation
- Comprehensive data pipeline with multiple branching points
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
import random

default_args = {
    'owner': 'airflow-kata-solution',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# =============================================================================
# Task 1 Solution: Data Quality Assessment and Branching
# =============================================================================


def assess_data_quality(**context):
    """
    Comprehensive data quality assessment with realistic scenarios.

    Returns:
        dict: Quality metrics dictionary with detailed analysis
    """
    # Simulate different quality scenarios based on realistic patterns
    quality_scenarios = [
        # High quality scenario
        {
            'error_rate': random.uniform(0.01, 0.04),
            'completeness': random.uniform(0.96, 0.99),
            'consistency': random.uniform(0.92, 0.98),
            'timeliness': random.uniform(0.95, 0.99),
            'record_count': random.randint(5000, 15000),
            'scenario': 'high_quality'
        },
        # Medium quality scenario
        {
            'error_rate': random.uniform(0.06, 0.14),
            'completeness': random.uniform(0.86, 0.94),
            'consistency': random.uniform(0.80, 0.90),
            'timeliness': random.uniform(0.85, 0.94),
            'record_count': random.randint(2000, 8000),
            'scenario': 'medium_quality'
        },
        # Low quality scenario
        {
            'error_rate': random.uniform(0.16, 0.35),
            'completeness': random.uniform(0.70, 0.84),
            'consistency': random.uniform(0.60, 0.78),
            'timeliness': random.uniform(0.65, 0.83),
            'record_count': random.randint(1000, 5000),
            'scenario': 'low_quality'
        },
        # Critical quality scenario
        {
            'error_rate': random.uniform(0.51, 0.80),
            'completeness': random.uniform(0.30, 0.60),
            'consistency': random.uniform(0.20, 0.50),
            'timeliness': random.uniform(0.40, 0.70),
            'record_count': random.randint(500, 2000),
            'scenario': 'critical_quality'
        }
    ]

    # Select scenario with weighted probability (favor higher quality)
    weights = [0.4, 0.35, 0.2, 0.05]  # High, Medium, Low, Critical
    quality_data = random.choices(quality_scenarios, weights=weights)[0]

    # Add additional metadata
    quality_data.update({
        'assessment_timestamp': datetime.now().isoformat(),
        'data_source': random.choice(['customer_db', 'transaction_api', 'inventory_file', 'analytics_stream']),
        'assessment_duration_ms': random.randint(100, 500),
        'quality_score': (
            quality_data['completeness'] * 0.3 +
            (1 - quality_data['error_rate']) * 0.3 +
            quality_data['consistency'] * 0.2 +
            quality_data['timeliness'] * 0.2
        )
    })

    print(f"=== Data Quality Assessment ===")
    print(f"Scenario: {quality_data['scenario']}")
    print(f"Error Rate: {quality_data['error_rate']:.1%}")
    print(f"Completeness: {quality_data['completeness']:.1%}")
    print(f"Consistency: {quality_data['consistency']:.1%}")
    print(f"Timeliness: {quality_data['timeliness']:.1%}")
    print(f"Overall Quality Score: {quality_data['quality_score']:.3f}")
    print(f"Record Count: {quality_data['record_count']:,}")
    print(f"Data Source: {quality_data['data_source']}")

    return quality_data


def quality_based_branch(**context):
    """
    Sophisticated quality-based branching with multiple criteria.

    Returns:
        str: Task ID for appropriate processing path
    """
    quality_data = context['task_instance'].xcom_pull(
        task_ids='assess_quality')

    if quality_data is None:
        print("Warning: No quality data available, defaulting to critical handling")
        return 'critical_quality_handling'

    error_rate = quality_data.get('error_rate', 1.0)
    completeness = quality_data.get('completeness', 0.0)
    consistency = quality_data.get('consistency', 0.0)
    quality_score = quality_data.get('quality_score', 0.0)
    record_count = quality_data.get('record_count', 0)

    print(f"=== Quality-Based Branching Decision ===")
    print(f"Quality Score: {quality_score:.3f}")
    print(f"Error Rate: {error_rate:.1%}")
    print(f"Completeness: {completeness:.1%}")
    print(f"Consistency: {consistency:.1%}")

    # Critical issues - immediate attention required
    if error_rate > 0.5 or completeness < 0.5:
        print("🚨 Critical quality issues detected - routing to emergency handling")
        return 'critical_quality_handling'

    # High quality - optimized processing
    elif error_rate < 0.05 and completeness > 0.95 and consistency > 0.90:
        print("✅ High quality data - routing to optimized processing")
        return 'high_quality_processing'

    # Low quality - comprehensive cleaning needed
    elif error_rate >= 0.15 or completeness <= 0.85 or consistency <= 0.75:
        print("⚠️ Low quality data - routing to comprehensive cleaning")
        return 'low_quality_processing'

    # Medium quality - enhanced processing
    else:
        print("📊 Medium quality data - routing to enhanced processing")
        return 'medium_quality_processing'


def high_quality_processing(**context):
    """
    High quality data processing with performance optimization.
    """
    quality_data = context['task_instance'].xcom_pull(
        task_ids='assess_quality')

    print("=== High Quality Processing ===")
    print("Optimized processing for high-quality data")

    # Minimal validation overhead
    print("Processing approach:")
    print("- Streamlined validation (spot checks only)")
    print("- Performance-optimized algorithms")
    print("- Direct processing without cleansing")
    print("- Real-time analytics integration")

    # Simulate fast processing
    processing_time = quality_data['record_count'] * 0.001  # Very fast

    result = {
        'processing_type': 'high_quality',
        'records_processed': quality_data['record_count'],
        'processing_time_seconds': processing_time,
        'validation_steps': 2,
        'cleansing_steps': 0,
        'quality_improvement': 0.02,  # Minimal improvement needed
        'throughput_records_per_second': quality_data['record_count'] / max(processing_time, 1)
    }

    print(f"High quality processing completed: {result}")
    return result


def medium_quality_processing(**context):
    """
    Medium quality data processing with enhanced validation.
    """
    quality_data = context['task_instance'].xcom_pull(
        task_ids='assess_quality')

    print("=== Medium Quality Processing ===")
    print("Enhanced processing for medium-quality data")

    # Enhanced validation and cleansing
    print("Processing approach:")
    print("- Comprehensive validation checks")
    print("- Targeted data cleansing")
    print("- Quality improvement measures")
    print("- Enhanced error reporting")

    # Simulate moderate processing time
    processing_time = quality_data['record_count'] * 0.003

    result = {
        'processing_type': 'medium_quality',
        'records_processed': quality_data['record_count'],
        'processing_time_seconds': processing_time,
        'validation_steps': 5,
        'cleansing_steps': 3,
        'quality_improvement': 0.15,
        'throughput_records_per_second': quality_data['record_count'] / max(processing_time, 1)
    }

    print(f"Medium quality processing completed: {result}")
    return result


def low_quality_processing(**context):
    """
    Low quality data processing with comprehensive cleansing.
    """
    quality_data = context['task_instance'].xcom_pull(
        task_ids='assess_quality')

    print("=== Low Quality Processing ===")
    print("Comprehensive processing for low-quality data")

    # Comprehensive cleansing
    print("Processing approach:")
    print("- Extensive data validation")
    print("- Multi-stage data cleansing")
    print("- Error correction procedures")
    print("- Quality assurance checks")
    print("- Detailed quality reporting")

    # Simulate longer processing time
    processing_time = quality_data['record_count'] * 0.008

    result = {
        'processing_type': 'low_quality',
        'records_processed': quality_data['record_count'],
        'processing_time_seconds': processing_time,
        'validation_steps': 8,
        'cleansing_steps': 6,
        'quality_improvement': 0.35,
        'throughput_records_per_second': quality_data['record_count'] / max(processing_time, 1)
    }

    print(f"Low quality processing completed: {result}")
    return result


def critical_quality_handling(**context):
    """
    Critical quality issue handling with emergency procedures.
    """
    quality_data = context['task_instance'].xcom_pull(
        task_ids='assess_quality')

    print("=== Critical Quality Handling ===")
    print("🚨 Emergency procedures for critical quality issues")

    # Emergency procedures
    print("Emergency response:")
    print("- Data quarantine procedures")
    print("- Immediate stakeholder notification")
    print("- Root cause analysis initiation")
    print("- Alternative data source activation")
    print("- Manual review process trigger")

    result = {
        'processing_type': 'critical_handling',
        'records_quarantined': quality_data['record_count'],
        'alerts_sent': 3,
        'manual_review_required': True,
        'alternative_sources_activated': True,
        'estimated_resolution_hours': random.randint(4, 24)
    }

    print(f"Critical quality handling initiated: {result}")
    return result


# DAG 1: Quality-based branching
dag1 = DAG(
    'exercise_2_quality_branching_solution',
    default_args=default_args,
    description='Data quality assessment and branching solution',
    schedule_interval=timedelta(hours=6),
    catchup=False,
    tags=['solution', 'branching', 'quality-driven']
)

# Tasks for quality-based branching
start_1 = DummyOperator(task_id='start', dag=dag1)

assess_quality = PythonOperator(
    task_id='assess_quality',
    python_callable=assess_data_quality,
    dag=dag1
)

quality_branch = BranchPythonOperator(
    task_id='quality_branch',
    python_callable=quality_based_branch,
    dag=dag1
)

high_quality = PythonOperator(
    task_id='high_quality_processing',
    python_callable=high_quality_processing,
    dag=dag1
)

medium_quality = PythonOperator(
    task_id='medium_quality_processing',
    python_callable=medium_quality_processing,
    dag=dag1
)

low_quality = PythonOperator(
    task_id='low_quality_processing',
    python_callable=low_quality_processing,
    dag=dag1
)

critical_quality = PythonOperator(
    task_id='critical_quality_handling',
    python_callable=critical_quality_handling,
    dag=dag1
)

join_1 = DummyOperator(
    task_id='join_processing',
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag1
)

end_1 = DummyOperator(task_id='end', dag=dag1)

# Dependencies
start_1 >> assess_quality >> quality_branch
quality_branch >> [high_quality, medium_quality, low_quality, critical_quality]
[high_quality, medium_quality, low_quality, critical_quality] >> join_1 >> end_1

# =============================================================================
# Task 2 Solution: Multi-Source Data Processing
# =============================================================================


def identify_data_sources(**context):
    """
    Identify and categorize multiple data sources with realistic scenarios.

    Returns:
        dict: Comprehensive source analysis
    """
    # Simulate different source combinations
    source_scenarios = [
        {
            'source_types': ['database'],
            'source_priorities': {'database': 'high'},
            'processing_requirements': {'database': ['validation', 'transformation']}
        },
        {
            'source_types': ['database', 'api'],
            'source_priorities': {'database': 'high', 'api': 'medium'},
            'processing_requirements': {
                'database': ['validation', 'transformation', 'enrichment'],
                'api': ['rate_limiting', 'validation']
            }
        },
        {
            'source_types': ['api', 'file', 'stream'],
            'source_priorities': {'api': 'medium', 'file': 'low', 'stream': 'high'},
            'processing_requirements': {
                'api': ['rate_limiting', 'validation'],
                'file': ['parsing', 'validation', 'transformation'],
                'stream': ['real_time_processing', 'buffering']
            }
        },
        {
            'source_types': ['database', 'api', 'file', 'stream'],
            'source_priorities': {'database': 'high', 'api': 'medium', 'file': 'low', 'stream': 'high'},
            'processing_requirements': {
                'database': ['validation', 'transformation', 'enrichment'],
                'api': ['rate_limiting', 'validation', 'caching'],
                'file': ['parsing', 'validation', 'transformation'],
                'stream': ['real_time_processing', 'buffering', 'aggregation']
            }
        }
    ]

    source_data = random.choice(source_scenarios)

    # Add metadata
    source_data.update({
        'analysis_timestamp': datetime.now().isoformat(),
        'total_sources': len(source_data['source_types']),
        'high_priority_sources': sum(1 for p in source_data['source_priorities'].values() if p == 'high'),
        'estimated_processing_time_minutes': len(source_data['source_types']) * random.randint(10, 30)
    })

    print(f"=== Data Source Analysis ===")
    print(f"Detected sources: {source_data['source_types']}")
    print(f"Source priorities: {source_data['source_priorities']}")
    print(f"Total sources: {source_data['total_sources']}")
    print(f"High priority sources: {source_data['high_priority_sources']}")

    return source_data


def multi_source_branch(**context):
    """
    Multi-source branching with parallel processing support.

    Returns:
        list or str: Task ID(s) for source processing
    """
    source_data = context['task_instance'].xcom_pull(
        task_ids='identify_sources')

    if source_data is None:
        print("Warning: No source data available")
        return 'no_sources_found'

    source_types = source_data.get('source_types', [])

    if not source_types:
        print("No sources detected")
        return 'no_sources_found'

    # Map source types to task IDs
    source_task_mapping = {
        'database': 'database_source_processing',
        'api': 'api_source_processing',
        'file': 'file_source_processing',
        'stream': 'stream_source_processing'
    }

    tasks_to_run = []
    for source_type in source_types:
        if source_type in source_task_mapping:
            tasks_to_run.append(source_task_mapping[source_type])

    print(f"=== Multi-Source Branching Decision ===")
    print(f"Sources to process: {source_types}")
    print(f"Tasks to execute: {tasks_to_run}")

    if len(tasks_to_run) == 1:
        return tasks_to_run[0]
    elif len(tasks_to_run) > 1:
        print(f"Parallel processing: {len(tasks_to_run)} sources")
        return tasks_to_run
    else:
        return 'no_sources_found'


def database_source_processing(**context):
    """Process database sources with connection pooling and optimization."""
    source_data = context['task_instance'].xcom_pull(
        task_ids='identify_sources')

    print("=== Database Source Processing ===")
    print("Processing database sources with optimized queries")

    requirements = source_data['processing_requirements'].get('database', [])
    priority = source_data['source_priorities'].get('database', 'medium')

    print(f"Priority level: {priority}")
    print(f"Processing requirements: {requirements}")

    # Simulate database processing
    print("Database operations:")
    print("- Connection pool management")
    print("- Optimized query execution")
    print("- Transaction management")
    print("- Data validation and transformation")

    result = {
        'source_type': 'database',
        'priority': priority,
        'requirements_processed': requirements,
        'records_extracted': random.randint(5000, 20000),
        'processing_time_seconds': random.randint(30, 120),
        'connection_pool_size': 5
    }

    print(f"Database processing completed: {result}")
    return result


def api_source_processing(**context):
    """Process API sources with rate limiting and error handling."""
    source_data = context['task_instance'].xcom_pull(
        task_ids='identify_sources')

    print("=== API Source Processing ===")
    print("Processing API sources with rate limiting")

    requirements = source_data['processing_requirements'].get('api', [])
    priority = source_data['source_priorities'].get('api', 'medium')

    print(f"Priority level: {priority}")
    print(f"Processing requirements: {requirements}")

    # Simulate API processing
    print("API operations:")
    print("- Rate limiting compliance")
    print("- Retry logic with exponential backoff")
    print("- Response validation")
    print("- Data caching")

    result = {
        'source_type': 'api',
        'priority': priority,
        'requirements_processed': requirements,
        'api_calls_made': random.randint(100, 500),
        'processing_time_seconds': random.randint(60, 300),
        'rate_limit_hits': random.randint(0, 3)
    }

    print(f"API processing completed: {result}")
    return result


def file_source_processing(**context):
    """Process file sources with parsing and validation."""
    source_data = context['task_instance'].xcom_pull(
        task_ids='identify_sources')

    print("=== File Source Processing ===")
    print("Processing file sources with parsing and validation")

    requirements = source_data['processing_requirements'].get('file', [])
    priority = source_data['source_priorities'].get('file', 'medium')

    print(f"Priority level: {priority}")
    print(f"Processing requirements: {requirements}")

    # Simulate file processing
    print("File operations:")
    print("- File format detection")
    print("- Streaming file parsing")
    print("- Schema validation")
    print("- Data transformation")

    result = {
        'source_type': 'file',
        'priority': priority,
        'requirements_processed': requirements,
        'files_processed': random.randint(5, 50),
        'total_size_mb': random.randint(100, 1000),
        'processing_time_seconds': random.randint(45, 180)
    }

    print(f"File processing completed: {result}")
    return result


def stream_source_processing(**context):
    """Process streaming sources with real-time handling."""
    source_data = context['task_instance'].xcom_pull(
        task_ids='identify_sources')

    print("=== Stream Source Processing ===")
    print("Processing streaming sources with real-time handling")

    requirements = source_data['processing_requirements'].get('stream', [])
    priority = source_data['source_priorities'].get('stream', 'medium')

    print(f"Priority level: {priority}")
    print(f"Processing requirements: {requirements}")

    # Simulate stream processing
    print("Stream operations:")
    print("- Real-time data ingestion")
    print("- Stream buffering and batching")
    print("- Event processing")
    print("- Aggregation and windowing")

    result = {
        'source_type': 'stream',
        'priority': priority,
        'requirements_processed': requirements,
        'events_processed': random.randint(10000, 100000),
        'processing_time_seconds': random.randint(20, 90),
        'buffer_size_mb': random.randint(10, 100)
    }

    print(f"Stream processing completed: {result}")
    return result


def no_sources_found(**context):
    """Handle case when no sources are detected."""
    print("=== No Sources Found ===")
    print("No data sources detected for processing")

    return {
        'status': 'no_sources',
        'action': 'monitoring_mode',
        'next_check_minutes': 15
    }


# DAG 2: Multi-source processing
dag2 = DAG(
    'exercise_2_multi_source_solution',
    default_args=default_args,
    description='Multi-source data processing solution',
    schedule_interval=timedelta(hours=4),
    catchup=False,
    tags=['solution', 'branching', 'multi-source']
)

# Tasks for multi-source processing
start_2 = DummyOperator(task_id='start', dag=dag2)

identify_sources = PythonOperator(
    task_id='identify_sources',
    python_callable=identify_data_sources,
    dag=dag2
)

multi_source_branch_task = BranchPythonOperator(
    task_id='multi_source_branch',
    python_callable=multi_source_branch,
    dag=dag2
)

database_processing = PythonOperator(
    task_id='database_source_processing',
    python_callable=database_source_processing,
    dag=dag2
)

api_processing = PythonOperator(
    task_id='api_source_processing',
    python_callable=api_source_processing,
    dag=dag2
)

file_processing = PythonOperator(
    task_id='file_source_processing',
    python_callable=file_source_processing,
    dag=dag2
)

stream_processing = PythonOperator(
    task_id='stream_source_processing',
    python_callable=stream_source_processing,
    dag=dag2
)

no_sources = PythonOperator(
    task_id='no_sources_found',
    python_callable=no_sources_found,
    dag=dag2
)

join_2 = DummyOperator(
    task_id='join_processing',
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag2
)

end_2 = DummyOperator(task_id='end', dag=dag2)

# Dependencies
start_2 >> identify_sources >> multi_source_branch_task
multi_source_branch_task >> [
    database_processing, api_processing, file_processing, stream_processing, no_sources]
[database_processing, api_processing, file_processing,
    stream_processing, no_sources] >> join_2 >> end_2

if __name__ == "__main__":
    print("Exercise 2 Solution - Data-Driven Branching")
    print("=" * 50)

    print("\nSolution includes:")
    print("1. exercise_2_quality_branching_solution - Quality-based processing")
    print("2. exercise_2_multi_source_solution - Multi-source parallel processing")

    print("\nKey features demonstrated:")
    print("- Sophisticated quality assessment")
    print("- Multi-criteria branching decisions")
    print("- Parallel source processing")
    print("- Comprehensive error handling")
    print("- Realistic data scenarios")
