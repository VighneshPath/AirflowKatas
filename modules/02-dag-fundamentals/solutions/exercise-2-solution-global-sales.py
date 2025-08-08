"""
Solution: Global Sales Analytics Pipeline

This solution demonstrates complex dependency patterns with parallel processing,
data consolidation, and multiple output generation phases.
Key concepts: fan-out, fan-in, multi-stage parallel processing, TaskGroups.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


# Python functions for the global sales analytics pipeline

def validate_source_data(**context):
    """Check data quality and availability"""
    execution_date = context['execution_date']
    print(f"Validating source data for {execution_date.strftime('%Y-%m-%d')}")

    # Simulate data validation checks
    data_sources = ['sales_db', 'customer_db',
                    'product_catalog', 'inventory_system']
    validation_results = {}

    for source in data_sources:
        print(f"  - Checking {source} availability...")
        print(f"  - Validating {source} data quality...")
        print(f"  - Verifying {source} data freshness...")

        # Simulate validation results
        import random
        is_valid = random.choice([True, True, True, False])  # 75% success rate
        record_count = random.randint(10000, 50000)

        validation_results[source] = {
            'is_valid': is_valid,
            'record_count': record_count,
            'last_updated': execution_date.strftime('%Y-%m-%d %H:%M:%S')
        }

        status = "✓ VALID" if is_valid else "✗ INVALID"
        print(f"    {source}: {status} ({record_count:,} records)")

    # Overall validation status
    all_valid = all(result['is_valid']
                    for result in validation_results.values())
    total_records = sum(result['record_count']
                        for result in validation_results.values())

    print(f"  - Overall validation: {'PASSED' if all_valid else 'FAILED'}")
    print(f"  - Total records available: {total_records:,}")

    if not all_valid:
        print("  - WARNING: Some data sources failed validation!")
        print("  - Proceeding with available data...")

    return {
        'validation_passed': all_valid,
        'source_results': validation_results,
        'total_records': total_records,
        'validation_timestamp': datetime.now().isoformat()
    }


def prepare_processing_environment(**context):
    """Set up temporary tables and connections"""
    validation_data = context['task_instance'].xcom_pull(
        task_ids='validate_source_data')

    print("Preparing processing environment...")
    print(
        f"Setting up environment for {validation_data['total_records']:,} records")

    # Simulate environment preparation
    setup_tasks = [
        'Creating temporary database schemas',
        'Establishing database connections',
        'Setting up processing queues',
        'Initializing cache systems',
        'Configuring parallel processing pools',
        'Loading configuration parameters'
    ]

    for task in setup_tasks:
        print(f"  - {task}...")
        import time
        time.sleep(0.2)  # Simulate setup time

    # Create processing configuration
    processing_config = {
        'temp_schema': 'temp_global_analytics',
        'connection_pool_size': 10,
        'parallel_workers': 4,
        'cache_size_mb': 1024,
        'processing_mode': 'parallel',
        'setup_timestamp': datetime.now().isoformat()
    }

    print("  - Environment preparation completed!")
    print(f"  - Temporary schema: {processing_config['temp_schema']}")
    print(f"  - Parallel workers: {processing_config['parallel_workers']}")
    print(f"  - Cache size: {processing_config['cache_size_mb']} MB")

    return processing_config


def process_region_data(region_name):
    """Factory function to create region-specific processors"""
    def _process(**context):
        execution_date = context['execution_date']
        validation_data = context['task_instance'].xcom_pull(
            task_ids='validate_source_data')

        print(
            f"Processing {region_name} sales data for {execution_date.strftime('%Y-%m-%d')}")

        # Simulate region-specific processing
        print(f"  - Loading {region_name} raw sales data...")
        print(f"  - Applying {region_name} currency conversions...")
        print(f"  - Calculating {region_name} regional metrics...")
        print(f"  - Applying {region_name} business rules...")
        print(f"  - Validating {region_name} processed results...")

        # Simulate processing results
        import random
        base_sales = random.randint(500000, 2000000)
        growth_rate = random.uniform(-5.0, 15.0)

        results = {
            'region': region_name,
            'processing_date': execution_date.strftime('%Y-%m-%d'),
            'total_sales': base_sales,
            'growth_rate_percent': growth_rate,
            'orders_processed': random.randint(5000, 20000),
            'customers_active': random.randint(1000, 5000),
            'top_product_category': random.choice(['Electronics', 'Clothing', 'Home', 'Sports']),
            'processing_time_seconds': random.uniform(30, 120)
        }

        print(f"  - {region_name} processing completed:")
        print(f"    Total Sales: ${results['total_sales']:,}")
        print(f"    Growth Rate: {results['growth_rate_percent']:+.1f}%")
        print(f"    Orders: {results['orders_processed']:,}")
        print(f"    Active Customers: {results['customers_active']:,}")
        print(f"    Top Category: {results['top_product_category']}")

        return results

    return _process


def merge_regional_data(**context):
    """Combine all regional results"""
    print("Merging regional sales data...")

    # Pull data from all regional processing tasks
    regions = ['north_america', 'europe', 'asia_pacific', 'latin_america']
    regional_data = {}

    for region in regions:
        task_id = f'process_{region}'
        data = context['task_instance'].xcom_pull(task_ids=task_id)
        regional_data[region] = data
        print(f"  - Retrieved {region} data: ${data['total_sales']:,}")

    # Calculate global aggregates
    global_totals = {
        'total_sales': sum(data['total_sales'] for data in regional_data.values()),
        'total_orders': sum(data['orders_processed'] for data in regional_data.values()),
        'total_customers': sum(data['customers_active'] for data in regional_data.values()),
        'average_growth_rate': sum(data['growth_rate_percent'] for data in regional_data.values()) / len(regional_data),
        'regions_processed': len(regional_data),
        'merge_timestamp': datetime.now().isoformat()
    }

    print(f"  - Global sales total: ${global_totals['total_sales']:,}")
    print(f"  - Global orders total: {global_totals['total_orders']:,}")
    print(f"  - Global customers total: {global_totals['total_customers']:,}")
    print(
        f"  - Average growth rate: {global_totals['average_growth_rate']:+.1f}%")

    # Identify top performing region
    top_region = max(regional_data.items(), key=lambda x: x[1]['total_sales'])
    print(
        f"  - Top performing region: {top_region[0]} (${top_region[1]['total_sales']:,})")

    return {
        'regional_data': regional_data,
        'global_totals': global_totals,
        'top_region': top_region[0],
        'merge_completed': True
    }


def calculate_global_metrics(**context):
    """Compute global KPIs"""
    merged_data = context['task_instance'].xcom_pull(
        task_ids='merge_regional_data')

    print("Calculating global KPIs and metrics...")

    global_totals = merged_data['global_totals']
    regional_data = merged_data['regional_data']

    # Calculate advanced metrics
    metrics = {
        'revenue_per_customer': global_totals['total_sales'] / global_totals['total_customers'],
        'average_order_value': global_totals['total_sales'] / global_totals['total_orders'],
        # Simplified calculation
        'customer_acquisition_rate': global_totals['average_growth_rate'] * 0.3,
        # Assume 1M total market
        'market_penetration': global_totals['total_customers'] / 1000000,
        'regional_diversity_index': len([r for r in regional_data.values() if r['total_sales'] > global_totals['total_sales'] * 0.15])
    }

    # Calculate regional performance rankings
    regional_rankings = sorted(
        regional_data.items(),
        key=lambda x: x[1]['total_sales'],
        reverse=True
    )

    print("  - Global KPIs calculated:")
    print(f"    Revenue per Customer: ${metrics['revenue_per_customer']:.2f}")
    print(f"    Average Order Value: ${metrics['average_order_value']:.2f}")
    print(
        f"    Customer Acquisition Rate: {metrics['customer_acquisition_rate']:.1f}%")
    print(f"    Market Penetration: {metrics['market_penetration']:.1%}")

    print("  - Regional Rankings:")
    for i, (region, data) in enumerate(regional_rankings, 1):
        print(
            f"    {i}. {region}: ${data['total_sales']:,} ({data['growth_rate_percent']:+.1f}%)")

    return {
        'global_metrics': metrics,
        'regional_rankings': regional_rankings,
        'calculation_timestamp': datetime.now().isoformat()
    }


def generate_executive_dashboard(**context):
    """Create C-level dashboard"""
    metrics_data = context['task_instance'].xcom_pull(
        task_ids='calculate_global_metrics')

    print("Generating executive dashboard...")

    # Create executive summary
    dashboard_data = {
        'dashboard_type': 'executive',
        'key_metrics': metrics_data['global_metrics'],
        'top_insights': [
            f"Global revenue: ${metrics_data['global_metrics']['revenue_per_customer'] * 100000:,.0f}",
            f"Top region: {metrics_data['regional_rankings'][0][0]}",
            f"Growth rate: {metrics_data['global_metrics']['customer_acquisition_rate']:+.1f}%"
        ],
        'charts_generated': [
            'Revenue Trend Chart',
            'Regional Performance Map',
            'Growth Rate Comparison',
            'Customer Acquisition Funnel'
        ]
    }

    print("  - Executive dashboard components:")
    for chart in dashboard_data['charts_generated']:
        print(f"    ✓ {chart}")

    print("  - Key insights for executives:")
    for insight in dashboard_data['top_insights']:
        print(f"    • {insight}")

    print("  - Dashboard published to executive portal")

    return dashboard_data


def generate_regional_reports(**context):
    """Create detailed regional reports"""
    merged_data = context['task_instance'].xcom_pull(
        task_ids='merge_regional_data')

    print("Generating detailed regional reports...")

    regional_data = merged_data['regional_data']
    reports_generated = []

    for region, data in regional_data.items():
        print(f"  - Creating {region} detailed report...")

        report = {
            'region': region,
            'report_type': 'detailed_regional',
            'metrics': data,
            'sections': [
                'Sales Performance Summary',
                'Customer Demographics',
                'Product Category Analysis',
                'Growth Trend Analysis',
                'Market Opportunities'
            ],
            'file_name': f"{region}_sales_report_{datetime.now().strftime('%Y%m%d')}.pdf"
        }

        reports_generated.append(report)
        print(f"    Generated: {report['file_name']}")

    print(f"  - Total reports generated: {len(reports_generated)}")
    print("  - Reports uploaded to regional manager portals")

    return {
        'reports': reports_generated,
        'total_reports': len(reports_generated),
        'generation_timestamp': datetime.now().isoformat()
    }


def update_data_warehouse(**context):
    """Load data into warehouse"""
    merged_data = context['task_instance'].xcom_pull(
        task_ids='merge_regional_data')
    metrics_data = context['task_instance'].xcom_pull(
        task_ids='calculate_global_metrics')

    print("Updating data warehouse with processed data...")

    # Simulate data warehouse updates
    warehouse_updates = {
        'fact_sales': merged_data['global_totals']['total_sales'],
        'dim_regions': len(merged_data['regional_data']),
        'fact_metrics': len(metrics_data['global_metrics']),
        'dim_time': 1,  # One time period
        'update_timestamp': datetime.now().isoformat()
    }

    print("  - Updating warehouse tables:")
    for table, record_count in warehouse_updates.items():
        if table != 'update_timestamp':
            print(f"    {table}: {record_count:,} records")

    print("  - Running data quality checks...")
    print("  - Updating warehouse metadata...")
    print("  - Refreshing materialized views...")
    print("  - Data warehouse update completed!")

    return warehouse_updates


def send_stakeholder_alerts(**context):
    """Notify relevant teams"""
    metrics_data = context['task_instance'].xcom_pull(
        task_ids='calculate_global_metrics')

    print("Sending stakeholder notifications...")

    # Define stakeholder groups
    stakeholders = {
        'executives': ['ceo@company.com', 'cfo@company.com'],
        'regional_managers': ['na-manager@company.com', 'eu-manager@company.com'],
        'analytics_team': ['analytics@company.com', 'data-science@company.com'],
        'sales_team': ['sales-director@company.com', 'sales-ops@company.com']
    }

    notifications_sent = []

    for group, emails in stakeholders.items():
        print(f"  - Notifying {group}...")

        notification = {
            'group': group,
            'recipients': emails,
            'subject': f'Global Sales Analytics - {datetime.now().strftime("%Y-%m-%d")}',
            'key_metrics': metrics_data['global_metrics'],
            'sent_timestamp': datetime.now().isoformat()
        }

        notifications_sent.append(notification)
        print(f"    Sent to {len(emails)} recipients")

    # Additional notifications
    print("  - Posting summary to Slack #sales-updates...")
    print("  - Updating team dashboards...")
    print("  - Creating calendar reminders for follow-ups...")

    print(f"  - Total notifications sent: {len(notifications_sent)}")

    return {
        'notifications': notifications_sent,
        'total_sent': len(notifications_sent),
        'notification_timestamp': datetime.now().isoformat()
    }


def validate_outputs(**context):
    """Verify all outputs were created successfully"""
    print("Validating all pipeline outputs...")

    # Pull results from all output generation tasks
    dashboard_data = context['task_instance'].xcom_pull(
        task_ids='generate_executive_dashboard')
    reports_data = context['task_instance'].xcom_pull(
        task_ids='generate_regional_reports')
    warehouse_data = context['task_instance'].xcom_pull(
        task_ids='update_data_warehouse')
    alerts_data = context['task_instance'].xcom_pull(
        task_ids='send_stakeholder_alerts')

    # Validate each output
    validations = {
        'executive_dashboard': dashboard_data is not None and len(dashboard_data['charts_generated']) > 0,
        'regional_reports': reports_data is not None and reports_data['total_reports'] > 0,
        'data_warehouse': warehouse_data is not None and 'fact_sales' in warehouse_data,
        'stakeholder_alerts': alerts_data is not None and alerts_data['total_sent'] > 0
    }

    print("  - Output validation results:")
    for output, is_valid in validations.items():
        status = "✓ VALID" if is_valid else "✗ INVALID"
        print(f"    {output}: {status}")

    all_valid = all(validations.values())

    if all_valid:
        print("  - ✓ All outputs validated successfully!")
    else:
        print("  - ✗ Some outputs failed validation!")
        failed_outputs = [output for output,
                          valid in validations.items() if not valid]
        print(f"    Failed outputs: {', '.join(failed_outputs)}")

    return {
        'validation_results': validations,
        'all_outputs_valid': all_valid,
        'validation_timestamp': datetime.now().isoformat()
    }


def cleanup_temp_resources(**context):
    """Clean up temporary resources"""
    processing_config = context['task_instance'].xcom_pull(
        task_ids='prepare_processing_environment')

    print("Cleaning up temporary resources...")

    # Simulate cleanup tasks
    cleanup_tasks = [
        f"Dropping temporary schema: {processing_config['temp_schema']}",
        "Closing database connections",
        "Clearing processing queues",
        "Flushing cache systems",
        "Releasing parallel processing pools",
        "Archiving processing logs"
    ]

    for task in cleanup_tasks:
        print(f"  - {task}...")
        import time
        time.sleep(0.1)  # Simulate cleanup time

    # Calculate resource usage summary
    cleanup_summary = {
        'temp_schema_dropped': True,
        'connections_closed': processing_config['connection_pool_size'],
        'cache_cleared_mb': processing_config['cache_size_mb'],
        'workers_released': processing_config['parallel_workers'],
        'cleanup_timestamp': datetime.now().isoformat()
    }

    print("  - Cleanup completed successfully!")
    print(f"  - Released {cleanup_summary['connections_closed']} connections")
    print(f"  - Cleared {cleanup_summary['cache_cleared_mb']} MB cache")
    print(f"  - Released {cleanup_summary['workers_released']} workers")

    return cleanup_summary


# DAG configuration
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

dag = DAG(
    dag_id='global_sales_analytics',
    description='Global sales data processing with parallel regional analysis',
    schedule_interval='0 4 * * *',  # Daily at 4:00 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=['sales', 'global', 'analytics', 'parallel']
)

# Phase 1: Data Preparation
validate_source_data_task = PythonOperator(
    task_id='validate_source_data',
    python_callable=validate_source_data,
    dag=dag
)

prepare_processing_environment_task = PythonOperator(
    task_id='prepare_processing_environment',
    python_callable=prepare_processing_environment,
    dag=dag
)

# Phase 2: Regional Processing (Parallel)
with TaskGroup('regional_processing', dag=dag) as regional_processing_group:
    process_north_america = PythonOperator(
        task_id='process_north_america',
        python_callable=process_region_data('North America')
    )

    process_europe = PythonOperator(
        task_id='process_europe',
        python_callable=process_region_data('Europe')
    )

    process_asia_pacific = PythonOperator(
        task_id='process_asia_pacific',
        python_callable=process_region_data('Asia Pacific')
    )

    process_latin_america = PythonOperator(
        task_id='process_latin_america',
        python_callable=process_region_data('Latin America')
    )

# Phase 3: Data Consolidation
merge_regional_data_task = PythonOperator(
    task_id='merge_regional_data',
    python_callable=merge_regional_data,
    dag=dag
)

calculate_global_metrics_task = PythonOperator(
    task_id='calculate_global_metrics',
    python_callable=calculate_global_metrics,
    dag=dag
)

# Phase 4: Output Generation (Parallel)
with TaskGroup('output_generation', dag=dag) as output_generation_group:
    generate_executive_dashboard_task = PythonOperator(
        task_id='generate_executive_dashboard',
        python_callable=generate_executive_dashboard
    )

    generate_regional_reports_task = PythonOperator(
        task_id='generate_regional_reports',
        python_callable=generate_regional_reports
    )

    update_data_warehouse_task = PythonOperator(
        task_id='update_data_warehouse',
        python_callable=update_data_warehouse
    )

    send_stakeholder_alerts_task = PythonOperator(
        task_id='send_stakeholder_alerts',
        python_callable=send_stakeholder_alerts
    )

# Phase 5: Final Validation
validate_outputs_task = PythonOperator(
    task_id='validate_outputs',
    python_callable=validate_outputs,
    dag=dag
)

cleanup_temp_resources_task = PythonOperator(
    task_id='cleanup_temp_resources',
    python_callable=cleanup_temp_resources,
    dag=dag
)

# Set up complex dependencies
# Phase 1: Sequential preparation
validate_source_data_task >> prepare_processing_environment_task

# Phase 2: Fan-out to regional processing
prepare_processing_environment_task >> regional_processing_group

# Phase 3: Fan-in to consolidation
regional_processing_group >> merge_regional_data_task >> calculate_global_metrics_task

# Phase 4: Fan-out to output generation
calculate_global_metrics_task >> output_generation_group

# Phase 5: Fan-in to final validation and cleanup
output_generation_group >> validate_outputs_task >> cleanup_temp_resources_task
