"""
Solution for Exercise 1, Task 3: Month-End Financial Processing

This DAG handles month-end financial calculations on the last 3 days
of every month at 10 PM with timezone handling.
"""

from datetime import datetime, timedelta
from calendar import monthrange
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from pendulum import timezone

# DAG configuration with timezone
dag = DAG(
    'month_end_financial',
    description='Month-end financial processing with timezone handling',
    schedule_interval='0 22 29-31 * *',  # Last 3 days of month at 10 PM
    start_date=datetime(2024, 1, 1, tzinfo=timezone('US/Eastern')),
    catchup=True,  # Process historical month-ends
    max_active_runs=1,  # Only one month-end process at a time
    default_args={
        'retries': 3,
        'retry_delay': timedelta(minutes=15),
    },
    tags=['scheduling', 'month-end', 'financial', 'timezone']
)


def validate_month_end(**context):
    """Validate that today is actually the last day of the month"""
    execution_date = context['execution_date']
    year = execution_date.year
    month = execution_date.month
    day = execution_date.day

    # Get the last day of the month
    last_day = monthrange(year, month)[1]

    print(f"=== Month-End Validation ===")
    print(f"Execution Date: {execution_date}")
    print(f"Year: {year}, Month: {month}, Day: {day}")
    print(f"Last day of {month}/{year}: {last_day}")
    print(f"Timezone: {execution_date.tzinfo}")

    if day == last_day:
        print("✓ Confirmed: Today is the last day of the month")
        print("Proceeding with month-end financial processing")
        return True
    else:
        print(f"⚠ Not the last day of month (last day: {last_day})")
        print("Skipping month-end processing")
        return False


def extract_financial_data(**context):
    """Extract financial data for the month"""
    execution_date = context['execution_date']
    month_year = execution_date.strftime('%B %Y')

    print(f"=== Financial Data Extraction - {month_year} ===")
    print("Extracting revenue data...")
    print("Extracting expense data...")
    print("Extracting account balances...")
    print("Extracting transaction summaries...")
    print("Financial data extraction completed")

    # Simulate extracted data
    financial_data = {
        'revenue': 150000,
        'expenses': 120000,
        'profit': 30000,
        'transactions': 1250
    }

    return financial_data


def process_financial_calculations(**context):
    """Process month-end financial calculations"""
    execution_date = context['execution_date']
    month_year = execution_date.strftime('%B %Y')

    # Get data from previous task
    financial_data = context['task_instance'].xcom_pull(
        task_ids='extract_financial_data')

    print(f"=== Financial Calculations - {month_year} ===")
    print(f"Processing data: {financial_data}")

    # Perform calculations
    profit_margin = (financial_data['profit'] /
                     financial_data['revenue']) * 100
    avg_transaction = financial_data['revenue'] / \
        financial_data['transactions']

    print(f"Calculated profit margin: {profit_margin:.2f}%")
    print(f"Average transaction value: ${avg_transaction:.2f}")
    print("Financial calculations completed")

    results = {
        **financial_data,
        'profit_margin': profit_margin,
        'avg_transaction': avg_transaction
    }

    return results


def generate_financial_reports(**context):
    """Generate month-end financial reports"""
    execution_date = context['execution_date']
    month_year = execution_date.strftime('%B %Y')

    # Get processed data
    results = context['task_instance'].xcom_pull(
        task_ids='process_financial_calculations')

    print(f"=== Financial Report Generation - {month_year} ===")
    print("Generating profit & loss statement...")
    print("Generating balance sheet...")
    print("Generating cash flow statement...")
    print("Generating executive summary...")

    print(f"\n--- {month_year} Financial Summary ---")
    print(f"Revenue: ${results['revenue']:,}")
    print(f"Expenses: ${results['expenses']:,}")
    print(f"Profit: ${results['profit']:,}")
    print(f"Profit Margin: {results['profit_margin']:.2f}%")
    print(f"Total Transactions: {results['transactions']:,}")
    print(f"Average Transaction: ${results['avg_transaction']:.2f}")

    print("Financial reports generated successfully")
    return f"reports_generated_for_{month_year.replace(' ', '_')}"


# Task definitions
validate_month_end_task = PythonOperator(
    task_id='validate_month_end',
    python_callable=validate_month_end,
    dag=dag
)

extract_data = PythonOperator(
    task_id='extract_financial_data',
    python_callable=extract_financial_data,
    dag=dag
)

process_calculations = PythonOperator(
    task_id='process_financial_calculations',
    python_callable=process_financial_calculations,
    dag=dag
)

generate_reports = PythonOperator(
    task_id='generate_financial_reports',
    python_callable=generate_financial_reports,
    dag=dag
)

# Archive and backup task
archive_data = BashOperator(
    task_id='archive_monthly_data',
    bash_command='''
    echo "=== Monthly Data Archival ==="
    echo "Archiving financial data for $(date +%B\ %Y)"
    echo "Creating backup of month-end reports..."
    echo "Compressing historical data..."
    echo "Monthly data archived successfully"
    ''',
    dag=dag
)

# Notification task
send_notifications = BashOperator(
    task_id='send_completion_notifications',
    bash_command='''
    echo "=== Month-End Processing Complete ==="
    echo "Sending notifications to finance team..."
    echo "Updating dashboard with latest figures..."
    echo "Month-end processing completed at $(date)"
    ''',
    dag=dag
)

# Set task dependencies
validate_month_end_task >> extract_data >> process_calculations >> generate_reports
generate_reports >> [archive_data, send_notifications]
