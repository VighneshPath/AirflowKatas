"""
Dependency Pattern Examples for Airflow

This module demonstrates various dependency patterns including:
- Fan-out patterns (one-to-many)
- Fan-in patterns (many-to-one)
- Diamond patterns (combination)
- Conditional dependencies
- Complex dependency chains
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

# Example 1: Fan-Out Pattern
# One task triggers multiple parallel tasks
fanout_dag = DAG(
    'fanout_pattern_example',
    description='Demonstrates fan-out dependency pattern',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dependencies', 'fan-out', 'parallel']
)


def prepare_data(**context):
    """Prepare data for parallel processing"""
    print("Preparing data for parallel processing...")
    data_sets = ['dataset_a', 'dataset_b', 'dataset_c', 'dataset_d']
    print(f"Data sets prepared: {data_sets}")
    return data_sets


def process_dataset_a(**context):
    """Process dataset A"""
    print("Processing dataset A...")
    print("Applying transformations specific to dataset A")
    return "dataset_a_processed"


def process_dataset_b(**context):
    """Process dataset B"""
    print("Processing dataset B...")
    print("Applying transformations specific to dataset B")
    return "dataset_b_processed"


def process_dataset_c(**context):
    """Process dataset C"""
    print("Processing dataset C...")
    print("Applying transformations specific to dataset C")
    return "dataset_c_processed"


def process_dataset_d(**context):
    """Process dataset D"""
    print("Processing dataset D...")
    print("Applying transformations specific to dataset D")
    return "dataset_d_processed"


# Fan-out tasks
prepare_data_task = PythonOperator(
    task_id='prepare_data',
    python_callable=prepare_data,
    dag=fanout_dag
)

process_a = PythonOperator(
    task_id='process_dataset_a',
    python_callable=process_dataset_a,
    dag=fanout_dag
)

process_b = PythonOperator(
    task_id='process_dataset_b',
    python_callable=process_dataset_b,
    dag=fanout_dag
)

process_c = PythonOperator(
    task_id='process_dataset_c',
    python_callable=process_dataset_c,
    dag=fanout_dag
)

process_d = PythonOperator(
    task_id='process_dataset_d',
    python_callable=process_dataset_d,
    dag=fanout_dag
)

# Fan-out dependency: one task to many
prepare_data_task >> [process_a, process_b, process_c, process_d]

# Example 2: Fan-In Pattern
# Multiple tasks converge to a single task
fanin_dag = DAG(
    'fanin_pattern_example',
    description='Demonstrates fan-in dependency pattern',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dependencies', 'fan-in', 'convergence']
)


def extract_source_1(**context):
    """Extract data from source 1"""
    print("Extracting data from source 1...")
    return {'source': 'source_1', 'records': 1000}


def extract_source_2(**context):
    """Extract data from source 2"""
    print("Extracting data from source 2...")
    return {'source': 'source_2', 'records': 1500}


def extract_source_3(**context):
    """Extract data from source 3"""
    print("Extracting data from source 3...")
    return {'source': 'source_3', 'records': 800}


def consolidate_data(**context):
    """Consolidate data from all sources"""
    print("Consolidating data from all sources...")

    # Get data from all upstream tasks
    source_1_data = context['task_instance'].xcom_pull(
        task_ids='extract_source_1')
    source_2_data = context['task_instance'].xcom_pull(
        task_ids='extract_source_2')
    source_3_data = context['task_instance'].xcom_pull(
        task_ids='extract_source_3')

    total_records = (source_1_data['records'] +
                     source_2_data['records'] +
                     source_3_data['records'])

    print(f"Source 1: {source_1_data['records']} records")
    print(f"Source 2: {source_2_data['records']} records")
    print(f"Source 3: {source_3_data['records']} records")
    print(f"Total consolidated records: {total_records}")

    return {'total_records': total_records, 'sources': 3}


# Fan-in tasks
extract_1 = PythonOperator(
    task_id='extract_source_1',
    python_callable=extract_source_1,
    dag=fanin_dag
)

extract_2 = PythonOperator(
    task_id='extract_source_2',
    python_callable=extract_source_2,
    dag=fanin_dag
)

extract_3 = PythonOperator(
    task_id='extract_source_3',
    python_callable=extract_source_3,
    dag=fanin_dag
)

consolidate = PythonOperator(
    task_id='consolidate_data',
    python_callable=consolidate_data,
    dag=fanin_dag
)

# Fan-in dependency: many tasks to one
[extract_1, extract_2, extract_3] >> consolidate

# Example 3: Diamond Pattern
# Combination of fan-out and fan-in
diamond_dag = DAG(
    'diamond_pattern_example',
    description='Demonstrates diamond dependency pattern',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dependencies', 'diamond', 'complex']
)


def initialize_processing(**context):
    """Initialize the processing pipeline"""
    print("Initializing data processing pipeline...")
    return {'status': 'initialized', 'timestamp': datetime.now().isoformat()}


def transform_for_analytics(**context):
    """Transform data for analytics"""
    print("Transforming data for analytics...")
    print("Applying analytics-specific transformations...")
    return {'transform_type': 'analytics', 'records_processed': 2000}


def transform_for_reporting(**context):
    """Transform data for reporting"""
    print("Transforming data for reporting...")
    print("Applying reporting-specific transformations...")
    return {'transform_type': 'reporting', 'records_processed': 1800}


def finalize_processing(**context):
    """Finalize the processing pipeline"""
    print("Finalizing data processing pipeline...")

    # Get results from both transformation tasks
    analytics_result = context['task_instance'].xcom_pull(
        task_ids='transform_for_analytics')
    reporting_result = context['task_instance'].xcom_pull(
        task_ids='transform_for_reporting')

    total_processed = (analytics_result['records_processed'] +
                       reporting_result['records_processed'])

    print(
        f"Analytics processing: {analytics_result['records_processed']} records")
    print(
        f"Reporting processing: {reporting_result['records_processed']} records")
    print(f"Total records processed: {total_processed}")
    print("Processing pipeline completed successfully")

    return {'total_processed': total_processed, 'status': 'completed'}


# Diamond pattern tasks
initialize = PythonOperator(
    task_id='initialize_processing',
    python_callable=initialize_processing,
    dag=diamond_dag
)

analytics_transform = PythonOperator(
    task_id='transform_for_analytics',
    python_callable=transform_for_analytics,
    dag=diamond_dag
)

reporting_transform = PythonOperator(
    task_id='transform_for_reporting',
    python_callable=transform_for_reporting,
    dag=diamond_dag
)

finalize = PythonOperator(
    task_id='finalize_processing',
    python_callable=finalize_processing,
    dag=diamond_dag
)

# Diamond dependency pattern
initialize >> [analytics_transform, reporting_transform] >> finalize

# Example 4: Conditional Dependencies
# Tasks that run based on conditions
conditional_dag = DAG(
    'conditional_dependencies_example',
    description='Demonstrates conditional dependency patterns',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dependencies', 'conditional', 'branching']
)


def check_data_quality(**context):
    """Check data quality and determine processing path"""
    import random

    # Simulate data quality check
    quality_score = random.uniform(0.5, 1.0)
    print(f"Data quality score: {quality_score:.2f}")

    if quality_score >= 0.8:
        print("High quality data detected - proceeding with full processing")
        return 'full_processing'
    elif quality_score >= 0.6:
        print("Medium quality data detected - proceeding with standard processing")
        return 'standard_processing'
    else:
        print("Low quality data detected - proceeding with cleanup first")
        return 'cleanup_processing'


def full_processing(**context):
    """Full data processing for high quality data"""
    print("Executing full data processing...")
    print("Applying advanced transformations...")
    print("Running comprehensive validations...")
    return 'full_processing_completed'


def standard_processing(**context):
    """Standard data processing for medium quality data"""
    print("Executing standard data processing...")
    print("Applying standard transformations...")
    print("Running basic validations...")
    return 'standard_processing_completed'


def cleanup_processing(**context):
    """Cleanup processing for low quality data"""
    print("Executing data cleanup processing...")
    print("Cleaning data anomalies...")
    print("Applying data correction rules...")
    return 'cleanup_processing_completed'


def final_validation(**context):
    """Final validation regardless of processing path"""
    print("Performing final validation...")
    print("All processing paths completed successfully")
    return 'validation_completed'


# Conditional dependency tasks
quality_check = BranchPythonOperator(
    task_id='check_data_quality',
    python_callable=check_data_quality,
    dag=conditional_dag
)

full_proc = PythonOperator(
    task_id='full_processing',
    python_callable=full_processing,
    dag=conditional_dag
)

standard_proc = PythonOperator(
    task_id='standard_processing',
    python_callable=standard_processing,
    dag=conditional_dag
)

cleanup_proc = PythonOperator(
    task_id='cleanup_processing',
    python_callable=cleanup_processing,
    dag=conditional_dag
)

# Join task that runs after any of the processing paths
join_task = DummyOperator(
    task_id='join_processing_paths',
    # Run if upstream tasks succeed or are skipped
    trigger_rule='none_failed_or_skipped',
    dag=conditional_dag
)

final_val = PythonOperator(
    task_id='final_validation',
    python_callable=final_validation,
    dag=conditional_dag
)

# Conditional dependencies
quality_check >> [full_proc, standard_proc, cleanup_proc]
[full_proc, standard_proc, cleanup_proc] >> join_task >> final_val

# Example 5: Complex Dependency Chain
# Multiple dependency patterns combined
complex_dag = DAG(
    'complex_dependency_chain',
    description='Demonstrates complex dependency patterns',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dependencies', 'complex', 'chain']
)

# Stage 1: Data ingestion (fan-out)
ingest_start = DummyOperator(task_id='start_ingestion', dag=complex_dag)

ingest_api = BashOperator(
    task_id='ingest_from_api',
    bash_command='echo "Ingesting data from API..."',
    dag=complex_dag
)

ingest_db = BashOperator(
    task_id='ingest_from_database',
    bash_command='echo "Ingesting data from database..."',
    dag=complex_dag
)

ingest_files = BashOperator(
    task_id='ingest_from_files',
    bash_command='echo "Ingesting data from files..."',
    dag=complex_dag
)

# Stage 2: Data validation (parallel)
validate_api = BashOperator(
    task_id='validate_api_data',
    bash_command='echo "Validating API data..."',
    dag=complex_dag
)

validate_db = BashOperator(
    task_id='validate_db_data',
    bash_command='echo "Validating database data..."',
    dag=complex_dag
)

validate_files = BashOperator(
    task_id='validate_file_data',
    bash_command='echo "Validating file data..."',
    dag=complex_dag
)

# Stage 3: Data transformation (fan-in to fan-out)
merge_data = BashOperator(
    task_id='merge_all_data',
    bash_command='echo "Merging all validated data..."',
    dag=complex_dag
)

transform_customer = BashOperator(
    task_id='transform_customer_data',
    bash_command='echo "Transforming customer data..."',
    dag=complex_dag
)

transform_product = BashOperator(
    task_id='transform_product_data',
    bash_command='echo "Transforming product data..."',
    dag=complex_dag
)

transform_sales = BashOperator(
    task_id='transform_sales_data',
    bash_command='echo "Transforming sales data..."',
    dag=complex_dag
)

# Stage 4: Final processing (fan-in)
final_processing = BashOperator(
    task_id='final_data_processing',
    bash_command='echo "Final data processing and loading..."',
    dag=complex_dag
)

# Complex dependency chain
ingest_start >> [ingest_api, ingest_db, ingest_files]
ingest_api >> validate_api
ingest_db >> validate_db
ingest_files >> validate_files
[validate_api, validate_db, validate_files] >> merge_data
merge_data >> [transform_customer, transform_product, transform_sales]
[transform_customer, transform_product, transform_sales] >> final_processing
