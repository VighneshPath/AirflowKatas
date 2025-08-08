"""
XCom and Branching Integration Examples

This module demonstrates advanced patterns that combine branching with XCom data passing
for sophisticated workflow orchestration. These examples show real-world scenarios
where branching decisions depend on complex data analysis from previous tasks.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
import random
import json

# Default arguments
default_args = {
    'owner': 'airflow-kata',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# =============================================================================
# Example 1: Data Pipeline with Quality-Based Routing
# =============================================================================


def extract_and_analyze_data(**context):
    """
    Extract data and perform comprehensive analysis for branching decisions.
    This function demonstrates how to prepare rich data for branching logic.
    """
    # Simulate data extraction from multiple sources
    data_sources = ['customer_db', 'transaction_api',
                    'inventory_files', 'analytics_stream']

    analysis_results = {}

    for source in data_sources:
        # Simulate different data characteristics per source
        source_data = {
            'record_count': random.randint(1000, 50000),
            'error_rate': random.uniform(0.01, 0.25),
            'completeness': random.uniform(0.70, 0.99),
            'freshness_hours': random.randint(1, 48),
            'schema_version': random.choice(['v1.0', 'v1.1', 'v2.0']),
            'data_size_mb': random.randint(10, 500)
        }

        # Calculate quality score
        source_data['quality_score'] = (
            (1 - source_data['error_rate']) * 0.4 +
            source_data['completeness'] * 0.3 +
            max(0, (48 - source_data['freshness_hours']) / 48) * 0.3
        )

        analysis_results[source] = source_data

    # Global analysis
    global_analysis = {
        'total_records': sum(data['record_count'] for data in analysis_results.values()),
        'average_quality': sum(data['quality_score'] for data in analysis_results.values()) / len(analysis_results),
        'max_error_rate': max(data['error_rate'] for data in analysis_results.values()),
        'min_completeness': min(data['completeness'] for data in analysis_results.values()),
        'analysis_timestamp': datetime.now().isoformat(),
        'sources_analyzed': len(analysis_results)
    }

    # Combine source-specific and global analysis
    complete_analysis = {
        'source_analysis': analysis_results,
        'global_analysis': global_analysis
    }

    print(f"=== Data Extraction and Analysis Complete ===")
    print(f"Sources analyzed: {len(analysis_results)}")
    print(f"Total records: {global_analysis['total_records']:,}")
    print(f"Average quality score: {global_analysis['average_quality']:.3f}")
    print(f"Max error rate: {global_analysis['max_error_rate']:.1%}")

    return complete_analysis


def intelligent_processing_branch(**context):
    """
    Sophisticated branching logic using comprehensive data analysis.
    Demonstrates multi-criteria decision making with XCom data.
    """
    analysis = context['task_instance'].xcom_pull(
        task_ids='extract_and_analyze')

    if analysis is None:
        print("No analysis data available - routing to error handling")
        return 'handle_missing_analysis'

    global_analysis = analysis['global_analysis']
    source_analysis = analysis['source_analysis']

    total_records = global_analysis['total_records']
    avg_quality = global_analysis['average_quality']
    max_error_rate = global_analysis['max_error_rate']
    min_completeness = global_analysis['min_completeness']

    print(f"=== Intelligent Processing Branch Decision ===")
    print(f"Decision factors:")
    print(f"- Total records: {total_records:,}")
    print(f"- Average quality: {avg_quality:.3f}")
    print(f"- Max error rate: {max_error_rate:.1%}")
    print(f"- Min completeness: {min_completeness:.1%}")

    # Complex decision logic
    if max_error_rate > 0.20 or min_completeness < 0.60:
        print("🚨 Critical data quality issues detected")
        return 'critical_data_recovery'

    elif avg_quality > 0.85 and total_records > 100000:
        print("✅ High quality, high volume - premium processing")
        return 'premium_high_volume_processing'

    elif avg_quality > 0.85:
        print("✅ High quality - optimized processing")
        return 'optimized_processing'

    elif total_records > 100000:
        print("📊 High volume - distributed processing")
        return 'distributed_processing'

    elif avg_quality < 0.60:
        print("⚠️ Low quality - enhanced cleaning")
        return 'enhanced_cleaning_processing'

    else:
        print("📋 Standard conditions - standard processing")
        return 'standard_processing'


def premium_high_volume_processing(**context):
    """Premium processing for high-quality, high-volume data."""
    analysis = context['task_instance'].xcom_pull(
        task_ids='extract_and_analyze')

    print("=== Premium High Volume Processing ===")
    print("Executing premium processing pipeline:")
    print("- GPU-accelerated algorithms")
    print("- Real-time quality monitoring")
    print("- Advanced analytics and ML")
    print("- Priority resource allocation")

    # Process each source with premium methods
    results = {}
    for source, data in analysis['source_analysis'].items():
        processing_result = {
            'records_processed': data['record_count'],
            'processing_method': 'premium_gpu',
            'quality_improvement': 0.05,
            # Very fast
            'processing_time_seconds': data['record_count'] * 0.0001,
            'ml_insights_generated': True
        }
        results[source] = processing_result
        print(
            f"- {source}: {data['record_count']:,} records processed with GPU acceleration")

    # Push detailed results for downstream tasks
    context['task_instance'].xcom_push(key='processing_results', value=results)
    context['task_instance'].xcom_push(
        key='processing_method', value='premium_high_volume')

    return {
        'status': 'completed',
        'method': 'premium_high_volume',
        'total_records': sum(r['records_processed'] for r in results.values()),
        'average_processing_time': sum(r['processing_time_seconds'] for r in results.values()) / len(results)
    }


def optimized_processing(**context):
    """Optimized processing for high-quality data."""
    analysis = context['task_instance'].xcom_pull(
        task_ids='extract_and_analyze')

    print("=== Optimized Processing ===")
    print("Executing optimized processing pipeline:")
    print("- Multi-threaded processing")
    print("- Streamlined validation")
    print("- Efficient algorithms")

    results = {}
    for source, data in analysis['source_analysis'].items():
        processing_result = {
            'records_processed': data['record_count'],
            'processing_method': 'optimized_cpu',
            'quality_improvement': 0.03,
            'processing_time_seconds': data['record_count'] * 0.0005,
            'threads_used': 4
        }
        results[source] = processing_result

    context['task_instance'].xcom_push(key='processing_results', value=results)
    context['task_instance'].xcom_push(
        key='processing_method', value='optimized')

    return {
        'status': 'completed',
        'method': 'optimized',
        'total_records': sum(r['records_processed'] for r in results.values())
    }


def distributed_processing(**context):
    """Distributed processing for high-volume data."""
    analysis = context['task_instance'].xcom_pull(
        task_ids='extract_and_analyze')

    print("=== Distributed Processing ===")
    print("Executing distributed processing pipeline:")
    print("- Multi-node cluster processing")
    print("- Load balancing")
    print("- Fault tolerance")

    results = {}
    for source, data in analysis['source_analysis'].items():
        # Simulate distributed processing
        num_nodes = min(8, max(2, data['record_count'] // 10000))
        processing_result = {
            'records_processed': data['record_count'],
            'processing_method': 'distributed',
            'nodes_used': num_nodes,
            'processing_time_seconds': data['record_count'] * 0.0003,
            'load_balanced': True
        }
        results[source] = processing_result
        print(
            f"- {source}: {data['record_count']:,} records across {num_nodes} nodes")

    context['task_instance'].xcom_push(key='processing_results', value=results)
    context['task_instance'].xcom_push(
        key='processing_method', value='distributed')

    return {
        'status': 'completed',
        'method': 'distributed',
        'total_records': sum(r['records_processed'] for r in results.values()),
        'total_nodes_used': sum(r['nodes_used'] for r in results.values())
    }


def enhanced_cleaning_processing(**context):
    """Enhanced cleaning for low-quality data."""
    analysis = context['task_instance'].xcom_pull(
        task_ids='extract_and_analyze')

    print("=== Enhanced Cleaning Processing ===")
    print("Executing enhanced cleaning pipeline:")
    print("- Comprehensive data validation")
    print("- Advanced error correction")
    print("- Quality improvement algorithms")

    results = {}
    for source, data in analysis['source_analysis'].items():
        # More intensive processing for cleaning
        processing_result = {
            'records_processed': data['record_count'],
            'processing_method': 'enhanced_cleaning',
            'quality_improvement': 0.25,  # Significant improvement
            # Slower due to cleaning
            'processing_time_seconds': data['record_count'] * 0.002,
            'errors_corrected': int(data['record_count'] * data['error_rate'] * 0.8),
            'cleaning_stages': 5
        }
        results[source] = processing_result

    context['task_instance'].xcom_push(key='processing_results', value=results)
    context['task_instance'].xcom_push(
        key='processing_method', value='enhanced_cleaning')

    return {
        'status': 'completed',
        'method': 'enhanced_cleaning',
        'total_records': sum(r['records_processed'] for r in results.values()),
        'total_errors_corrected': sum(r['errors_corrected'] for r in results.values())
    }


def standard_processing(**context):
    """Standard processing for normal conditions."""
    analysis = context['task_instance'].xcom_pull(
        task_ids='extract_and_analyze')

    print("=== Standard Processing ===")
    print("Executing standard processing pipeline")

    results = {}
    for source, data in analysis['source_analysis'].items():
        processing_result = {
            'records_processed': data['record_count'],
            'processing_method': 'standard',
            'quality_improvement': 0.10,
            'processing_time_seconds': data['record_count'] * 0.001
        }
        results[source] = processing_result

    context['task_instance'].xcom_push(key='processing_results', value=results)
    context['task_instance'].xcom_push(
        key='processing_method', value='standard')

    return {
        'status': 'completed',
        'method': 'standard',
        'total_records': sum(r['records_processed'] for r in results.values())
    }


def critical_data_recovery(**context):
    """Critical data recovery for severely compromised data."""
    analysis = context['task_instance'].xcom_pull(
        task_ids='extract_and_analyze')

    print("=== Critical Data Recovery ===")
    print("🚨 Initiating emergency data recovery procedures")
    print("- Data quarantine")
    print("- Root cause analysis")
    print("- Alternative data source activation")

    recovery_actions = {
        'quarantined_records': analysis['global_analysis']['total_records'],
        'alternative_sources_activated': 3,
        'recovery_time_estimate_hours': random.randint(4, 12),
        'manual_review_required': True,
        'stakeholders_notified': True
    }

    context['task_instance'].xcom_push(
        key='recovery_actions', value=recovery_actions)

    return {
        'status': 'critical_recovery_initiated',
        'method': 'emergency_recovery',
        'actions_taken': recovery_actions
    }


def handle_missing_analysis(**context):
    """Handle cases where analysis data is missing."""
    print("=== Handling Missing Analysis ===")
    print("No analysis data available - implementing fallback procedures")

    return {
        'status': 'fallback_processing',
        'method': 'safe_mode',
        'action': 'retry_analysis_scheduled'
    }


# DAG 1: Data pipeline with quality-based routing
dag1 = DAG(
    'xcom_branching_quality_pipeline',
    default_args=default_args,
    description='Data pipeline with XCom-driven quality-based routing',
    schedule_interval=timedelta(hours=4),
    catchup=False,
    tags=['advanced', 'xcom', 'branching', 'quality']
)

# Tasks for quality-based routing
start_1 = DummyOperator(task_id='start', dag=dag1)

extract_analyze = PythonOperator(
    task_id='extract_and_analyze',
    python_callable=extract_and_analyze_data,
    dag=dag1
)

intelligent_branch = BranchPythonOperator(
    task_id='intelligent_processing_branch',
    python_callable=intelligent_processing_branch,
    dag=dag1
)

# Processing tasks
premium_processing = PythonOperator(
    task_id='premium_high_volume_processing',
    python_callable=premium_high_volume_processing,
    dag=dag1
)

optimized_proc = PythonOperator(
    task_id='optimized_processing',
    python_callable=optimized_processing,
    dag=dag1
)

distributed_proc = PythonOperator(
    task_id='distributed_processing',
    python_callable=distributed_processing,
    dag=dag1
)

enhanced_cleaning = PythonOperator(
    task_id='enhanced_cleaning_processing',
    python_callable=enhanced_cleaning_processing,
    dag=dag1
)

standard_proc = PythonOperator(
    task_id='standard_processing',
    python_callable=standard_processing,
    dag=dag1
)

critical_recovery = PythonOperator(
    task_id='critical_data_recovery',
    python_callable=critical_data_recovery,
    dag=dag1
)

missing_analysis = PythonOperator(
    task_id='handle_missing_analysis',
    python_callable=handle_missing_analysis,
    dag=dag1
)

join_1 = DummyOperator(
    task_id='join_processing',
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag1
)

end_1 = DummyOperator(task_id='end', dag=dag1)

# Dependencies
start_1 >> extract_analyze >> intelligent_branch
intelligent_branch >> [premium_processing, optimized_proc, distributed_proc,
                       enhanced_cleaning, standard_proc, critical_recovery, missing_analysis]
[premium_processing, optimized_proc, distributed_proc, enhanced_cleaning,
 standard_proc, critical_recovery, missing_analysis] >> join_1 >> end_1

if __name__ == "__main__":
    print("XCom and Branching Integration Examples")
    print("=" * 40)

    print("\nThis module demonstrates:")
    print("1. Complex data analysis feeding branching decisions")
    print("2. Multi-criteria branching logic using XCom data")
    print("3. Sophisticated processing paths based on data characteristics")
    print("4. XCom data passing between branching and processing tasks")

    print("\nKey patterns:")
    print("- Rich data analysis for informed branching")
    print("- Multi-dimensional decision criteria")
    print("- Processing method selection based on data quality and volume")
    print("- Comprehensive error handling and recovery")
