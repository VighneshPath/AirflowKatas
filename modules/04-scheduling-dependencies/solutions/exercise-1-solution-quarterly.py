"""
Solution for Exercise 1, Task 4: Quarterly Report Generation

This DAG generates quarterly business reports on the first day of each quarter
(Jan, Apr, Jul, Oct) at 6 AM with proper retry logic and task chaining.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# DAG configuration
dag = DAG(
    'quarterly_reports',
    description='Generate quarterly business reports',
    schedule_interval='0 6 1 1,4,7,10 *',  # First day of quarters at 6 AM
    start_date=datetime(2024, 1, 1),
    catchup=True,  # Process all historical quarters
    max_active_runs=1,  # One quarterly report at a time
    default_args={
        'retries': 3,
        'retry_delay': timedelta(minutes=10),
        'retry_exponential_backoff': True,
        'max_retry_delay': timedelta(hours=1),
    },
    tags=['scheduling', 'quarterly', 'reports', 'business']
)


def determine_quarter_info(**context):
    """Determine quarter information and validate execution"""
    execution_date = context['execution_date']
    year = execution_date.year
    month = execution_date.month

    # Determine quarter
    quarter = (month - 1) // 3 + 1
    quarter_months = {
        1: ['January', 'February', 'March'],
        2: ['April', 'May', 'June'],
        3: ['July', 'August', 'September'],
        4: ['October', 'November', 'December']
    }

    print(f"=== Quarterly Report Processing ===")
    print(f"Execution Date: {execution_date}")
    print(f"Year: {year}")
    print(f"Quarter: Q{quarter} {year}")
    print(f"Quarter Months: {', '.join(quarter_months[quarter])}")

    # Validate that we're on the first day of a quarter month
    if month in [1, 4, 7, 10] and execution_date.day == 1:
        print("✓ Confirmed: First day of quarter")
    else:
        print("⚠ Warning: Not first day of quarter")

    quarter_info = {
        'year': year,
        'quarter': quarter,
        'quarter_name': f'Q{quarter} {year}',
        'months': quarter_months[quarter]
    }

    return quarter_info


def extract_quarterly_data(**context):
    """Extract data for the entire quarter"""
    quarter_info = context['task_instance'].xcom_pull(
        task_ids='determine_quarter_info')

    print(f"=== Data Extraction - {quarter_info['quarter_name']} ===")
    print(f"Extracting data for months: {', '.join(quarter_info['months'])}")

    # Simulate data extraction
    print("Extracting sales data...")
    print("Extracting customer data...")
    print("Extracting financial data...")
    print("Extracting operational metrics...")
    print("Extracting market data...")

    # Simulate quarterly data
    quarterly_data = {
        'sales_revenue': 450000 * quarter_info['quarter'],  # Simulate growth
        'new_customers': 150 * quarter_info['quarter'],
        'total_orders': 1800 * quarter_info['quarter'],
        'market_share': 12.5 + (quarter_info['quarter'] * 0.5),
        'employee_count': 50 + (quarter_info['quarter'] * 5)
    }

    print(f"Extracted quarterly data: {quarterly_data}")
    print("Data extraction completed successfully")

    return quarterly_data


def process_quarterly_analysis(**context):
    """Process and analyze quarterly data"""
    quarter_info = context['task_instance'].xcom_pull(
        task_ids='determine_quarter_info')
    quarterly_data = context['task_instance'].xcom_pull(
        task_ids='extract_quarterly_data')

    print(f"=== Quarterly Analysis - {quarter_info['quarter_name']} ===")

    # Perform analysis calculations
    avg_order_value = quarterly_data['sales_revenue'] / \
        quarterly_data['total_orders']
    revenue_per_customer = quarterly_data['sales_revenue'] / \
        quarterly_data['new_customers']
    # Baseline comparison
    growth_rate = (quarterly_data['sales_revenue'] / 400000 - 1) * 100

    analysis_results = {
        **quarterly_data,
        'avg_order_value': avg_order_value,
        'revenue_per_customer': revenue_per_customer,
        'growth_rate': growth_rate
    }

    print(f"Average Order Value: ${avg_order_value:.2f}")
    print(f"Revenue per Customer: ${revenue_per_customer:.2f}")
    print(f"Growth Rate: {growth_rate:.1f}%")
    print("Quarterly analysis completed")

    return analysis_results


def generate_quarterly_report(**context):
    """Generate comprehensive quarterly report"""
    quarter_info = context['task_instance'].xcom_pull(
        task_ids='determine_quarter_info')
    analysis_results = context['task_instance'].xcom_pull(
        task_ids='process_quarterly_analysis')

    print(f"=== Report Generation - {quarter_info['quarter_name']} ===")

    # Generate different report sections
    print("Generating executive summary...")
    print("Creating financial performance section...")
    print("Building customer analytics section...")
    print("Compiling operational metrics...")
    print("Adding market analysis...")
    print("Creating visualizations and charts...")

    # Display comprehensive report
    print(f"\n{'='*50}")
    print(f"QUARTERLY BUSINESS REPORT - {quarter_info['quarter_name']}")
    print(f"{'='*50}")
    print(
        f"Report Period: {', '.join(quarter_info['months'])} {quarter_info['year']}")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    print("FINANCIAL PERFORMANCE:")
    print(f"  Sales Revenue: ${analysis_results['sales_revenue']:,}")
    print(f"  Growth Rate: {analysis_results['growth_rate']:.1f}%")
    print(f"  Average Order Value: ${analysis_results['avg_order_value']:.2f}")
    print()
    print("CUSTOMER METRICS:")
    print(f"  New Customers: {analysis_results['new_customers']:,}")
    print(f"  Total Orders: {analysis_results['total_orders']:,}")
    print(
        f"  Revenue per Customer: ${analysis_results['revenue_per_customer']:.2f}")
    print()
    print("OPERATIONAL METRICS:")
    print(f"  Employee Count: {analysis_results['employee_count']}")
    print(f"  Market Share: {analysis_results['market_share']:.1f}%")
    print(f"{'='*50}")

    print("Quarterly report generated successfully")
    return f"report_generated_{quarter_info['quarter_name'].replace(' ', '_')}"


# Task definitions
determine_quarter = PythonOperator(
    task_id='determine_quarter_info',
    python_callable=determine_quarter_info,
    dag=dag
)

extract_data = PythonOperator(
    task_id='extract_quarterly_data',
    python_callable=extract_quarterly_data,
    dag=dag
)

process_analysis = PythonOperator(
    task_id='process_quarterly_analysis',
    python_callable=process_quarterly_analysis,
    dag=dag
)

generate_report = PythonOperator(
    task_id='generate_quarterly_report',
    python_callable=generate_quarterly_report,
    dag=dag
)

# Additional tasks using BashOperator
validate_data_quality = BashOperator(
    task_id='validate_data_quality',
    bash_command='''
    echo "=== Data Quality Validation ==="
    echo "Checking data completeness..."
    echo "Validating data accuracy..."
    echo "Verifying data consistency..."
    echo "Running quality checks..."
    echo "✓ All data quality checks passed"
    ''',
    dag=dag
)

distribute_reports = BashOperator(
    task_id='distribute_reports',
    bash_command='''
    echo "=== Report Distribution ==="
    echo "Sending reports to executive team..."
    echo "Uploading to company dashboard..."
    echo "Archiving in document management system..."
    echo "Sending summary to stakeholders..."
    echo "Report distribution completed"
    ''',
    dag=dag
)

cleanup_temp_files = BashOperator(
    task_id='cleanup_temp_files',
    bash_command='''
    echo "=== Cleanup Process ==="
    echo "Removing temporary data files..."
    echo "Clearing processing cache..."
    echo "Optimizing storage..."
    echo "Cleanup completed successfully"
    ''',
    dag=dag
)

# Set task dependencies - Pipeline structure
determine_quarter >> extract_data >> validate_data_quality
validate_data_quality >> process_analysis >> generate_report
generate_report >> [distribute_reports, cleanup_temp_files]
