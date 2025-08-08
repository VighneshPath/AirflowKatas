"""
Dependency Patterns Examples

This file demonstrates various task dependency patterns commonly used in Airflow.
Each pattern shows a different way to structure workflow execution.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


# Common default args for all examples
default_args = {
    'owner': 'examples_team',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


# Example 1: Linear Dependencies (Sequential)
linear_dag = DAG(
    dag_id='example_linear_dependencies',
    description='Sequential task execution: A → B → C → D',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=['example', 'linear', 'sequential']
)

# Create tasks for linear pattern
extract_data = BashOperator(
    task_id='extract_data',
    bash_command='echo "Step 1: Extracting data from source"',
    dag=linear_dag
)

clean_data = BashOperator(
    task_id='clean_data',
    bash_command='echo "Step 2: Cleaning extracted data"',
    dag=linear_dag
)

transform_data = BashOperator(
    task_id='transform_data',
    bash_command='echo "Step 3: Transforming cleaned data"',
    dag=linear_dag
)

load_data = BashOperator(
    task_id='load_data',
    bash_command='echo "Step 4: Loading transformed data"',
    dag=linear_dag
)

# Set linear dependencies
extract_data >> clean_data >> transform_data >> load_data


# Example 2: Fan-out Pattern (One-to-Many)
fanout_dag = DAG(
    dag_id='example_fanout_pattern',
    description='One task triggers multiple parallel tasks',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=['example', 'fanout', 'parallel']
)

# Initial task
prepare_data = BashOperator(
    task_id='prepare_data',
    bash_command='echo "Preparing data for parallel processing"',
    dag=fanout_dag
)

# Parallel processing tasks


def process_region(region_name):
    def _process(**context):
        print(f"Processing data for {region_name}")
        print(f"Execution date: {context['ds']}")
        return f"{region_name} processing completed"
    return _process


process_north = PythonOperator(
    task_id='process_north_region',
    python_callable=process_region('North'),
    dag=fanout_dag
)

process_south = PythonOperator(
    task_id='process_south_region',
    python_callable=process_region('South'),
    dag=fanout_dag
)

process_east = PythonOperator(
    task_id='process_east_region',
    python_callable=process_region('East'),
    dag=fanout_dag
)

process_west = PythonOperator(
    task_id='process_west_region',
    python_callable=process_region('West'),
    dag=fanout_dag
)

# Fan-out dependencies: one task to many
prepare_data >> [process_north, process_south, process_east, process_west]


# Example 3: Fan-in Pattern (Many-to-One)
fanin_dag = DAG(
    dag_id='example_fanin_pattern',
    description='Multiple parallel tasks converge to one task',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=['example', 'fanin', 'convergence']
)

# Parallel data collection tasks
collect_sales = BashOperator(
    task_id='collect_sales_data',
    bash_command='echo "Collecting sales data"',
    dag=fanin_dag
)

collect_inventory = BashOperator(
    task_id='collect_inventory_data',
    bash_command='echo "Collecting inventory data"',
    dag=fanin_dag
)

collect_customer = BashOperator(
    task_id='collect_customer_data',
    bash_command='echo "Collecting customer data"',
    dag=fanin_dag
)

# Aggregation task


def aggregate_all_data(**context):
    """Aggregate data from all parallel tasks"""
    print("Aggregating data from all sources:")
    print("- Sales data: Ready")
    print("- Inventory data: Ready")
    print("- Customer data: Ready")
    print("Creating comprehensive business report")
    return "All data aggregated successfully"


aggregate_data = PythonOperator(
    task_id='aggregate_data',
    python_callable=aggregate_all_data,
    dag=fanin_dag
)

# Fan-in dependencies: many tasks to one
[collect_sales, collect_inventory, collect_customer] >> aggregate_data


# Example 4: Diamond Pattern (Fan-out + Fan-in)
diamond_dag = DAG(
    dag_id='example_diamond_pattern',
    description='Diamond pattern: A → [B,C] → D',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=['example', 'diamond', 'complex']
)

# Start task
start_processing = BashOperator(
    task_id='start_processing',
    bash_command='echo "Starting diamond pattern processing"',
    dag=diamond_dag
)

# Parallel middle tasks
process_type_a = PythonOperator(
    task_id='process_type_a',
    python_callable=lambda: print("Processing Type A data"),
    dag=diamond_dag
)

process_type_b = PythonOperator(
    task_id='process_type_b',
    python_callable=lambda: print("Processing Type B data"),
    dag=diamond_dag
)

# End task
finalize_processing = BashOperator(
    task_id='finalize_processing',
    bash_command='echo "Finalizing diamond pattern processing"',
    dag=diamond_dag
)

# Diamond dependencies
start_processing >> [process_type_a, process_type_b] >> finalize_processing


# Example 5: Complex Multi-level Pattern
complex_dag = DAG(
    dag_id='example_complex_dependencies',
    description='Complex multi-level dependency pattern',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=['example', 'complex', 'multi-level']
)

# Level 1: Initial data gathering
gather_raw_data = BashOperator(
    task_id='gather_raw_data',
    bash_command='echo "Gathering raw data from multiple sources"',
    dag=complex_dag
)

# Level 2: Parallel validation
validate_format = PythonOperator(
    task_id='validate_format',
    python_callable=lambda: print("Validating data format"),
    dag=complex_dag
)

validate_quality = PythonOperator(
    task_id='validate_quality',
    python_callable=lambda: print("Validating data quality"),
    dag=complex_dag
)

# Level 3: Conditional processing based on validation
process_clean_data = BashOperator(
    task_id='process_clean_data',
    bash_command='echo "Processing clean, validated data"',
    dag=complex_dag
)

# Level 4: Parallel output generation
generate_reports = PythonOperator(
    task_id='generate_reports',
    python_callable=lambda: print("Generating business reports"),
    dag=complex_dag
)

update_dashboard = PythonOperator(
    task_id='update_dashboard',
    python_callable=lambda: print("Updating real-time dashboard"),
    dag=complex_dag
)

send_notifications = BashOperator(
    task_id='send_notifications',
    bash_command='echo "Sending completion notifications"',
    dag=complex_dag
)

# Level 5: Final cleanup
cleanup_temp_files = BashOperator(
    task_id='cleanup_temp_files',
    bash_command='echo "Cleaning up temporary files"',
    dag=complex_dag
)

# Complex dependencies
gather_raw_data >> [validate_format, validate_quality]
[validate_format, validate_quality] >> process_clean_data
process_clean_data >> [generate_reports, update_dashboard, send_notifications]
[generate_reports, update_dashboard, send_notifications] >> cleanup_temp_files


# Example 6: Task Groups for Organization
taskgroup_dag = DAG(
    dag_id='example_task_groups',
    description='Using TaskGroups to organize complex dependencies',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=['example', 'task-groups', 'organization']
)

# Initial task
start_pipeline = BashOperator(
    task_id='start_pipeline',
    bash_command='echo "Starting organized pipeline"',
    dag=taskgroup_dag
)

# Data ingestion group
with TaskGroup('data_ingestion', dag=taskgroup_dag) as ingestion_group:
    ingest_api_data = BashOperator(
        task_id='ingest_api_data',
        bash_command='echo "Ingesting data from API"'
    )

    ingest_file_data = BashOperator(
        task_id='ingest_file_data',
        bash_command='echo "Ingesting data from files"'
    )

    ingest_db_data = BashOperator(
        task_id='ingest_db_data',
        bash_command='echo "Ingesting data from database"'
    )

    # Dependencies within the group
    [ingest_api_data, ingest_file_data, ingest_db_data]

# Data processing group
with TaskGroup('data_processing', dag=taskgroup_dag) as processing_group:
    clean_data_task = PythonOperator(
        task_id='clean_data',
        python_callable=lambda: print("Cleaning ingested data")
    )

    validate_data_task = PythonOperator(
        task_id='validate_data',
        python_callable=lambda: print("Validating cleaned data")
    )

    transform_data_task = PythonOperator(
        task_id='transform_data',
        python_callable=lambda: print("Transforming validated data")
    )

    # Sequential processing within group
    clean_data_task >> validate_data_task >> transform_data_task

# Output generation group
with TaskGroup('output_generation', dag=taskgroup_dag) as output_group:
    create_reports = BashOperator(
        task_id='create_reports',
        bash_command='echo "Creating business reports"'
    )

    update_warehouse = BashOperator(
        task_id='update_warehouse',
        bash_command='echo "Updating data warehouse"'
    )

    send_alerts = BashOperator(
        task_id='send_alerts',
        bash_command='echo "Sending completion alerts"'
    )

    # Parallel output generation
    [create_reports, update_warehouse, send_alerts]

# Final task
complete_pipeline = BashOperator(
    task_id='complete_pipeline',
    bash_command='echo "Pipeline completed successfully"',
    dag=taskgroup_dag
)

# Group-level dependencies
start_pipeline >> ingestion_group >> processing_group >> output_group >> complete_pipeline


# Example 7: Conditional Dependencies with Mixed Patterns
mixed_dag = DAG(
    dag_id='example_mixed_patterns',
    description='Combining different dependency patterns in one DAG',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=['example', 'mixed', 'comprehensive']
)

# Start with linear sequence
init_system = BashOperator(
    task_id='init_system',
    bash_command='echo "Initializing system"',
    dag=mixed_dag
)

check_prerequisites = BashOperator(
    task_id='check_prerequisites',
    bash_command='echo "Checking prerequisites"',
    dag=mixed_dag
)

# Fan-out for parallel data collection
collect_source_1 = PythonOperator(
    task_id='collect_source_1',
    python_callable=lambda: print("Collecting from source 1"),
    dag=mixed_dag
)

collect_source_2 = PythonOperator(
    task_id='collect_source_2',
    python_callable=lambda: print("Collecting from source 2"),
    dag=mixed_dag
)

collect_source_3 = PythonOperator(
    task_id='collect_source_3',
    python_callable=lambda: print("Collecting from source 3"),
    dag=mixed_dag
)

# Fan-in for data merging
merge_data = BashOperator(
    task_id='merge_data',
    bash_command='echo "Merging data from all sources"',
    dag=mixed_dag
)

# Another fan-out for parallel processing
process_batch_1 = PythonOperator(
    task_id='process_batch_1',
    python_callable=lambda: print("Processing batch 1"),
    dag=mixed_dag
)

process_batch_2 = PythonOperator(
    task_id='process_batch_2',
    python_callable=lambda: print("Processing batch 2"),
    dag=mixed_dag
)

# Final linear sequence
quality_check = BashOperator(
    task_id='quality_check',
    bash_command='echo "Performing quality check"',
    dag=mixed_dag
)

publish_results = BashOperator(
    task_id='publish_results',
    bash_command='echo "Publishing final results"',
    dag=mixed_dag
)

# Mixed pattern dependencies
init_system >> check_prerequisites
check_prerequisites >> [collect_source_1, collect_source_2, collect_source_3]
[collect_source_1, collect_source_2, collect_source_3] >> merge_data
merge_data >> [process_batch_1, process_batch_2]
[process_batch_1, process_batch_2] >> quality_check >> publish_results
