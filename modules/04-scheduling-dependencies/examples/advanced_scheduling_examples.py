"""
Advanced Scheduling Examples for Airflow

This module demonstrates complex scheduling patterns including:
- Cron expressions for various business requirements
- Catchup behavior and backfill scenarios
- Timezone handling
- Schedule interval variations
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from pendulum import timezone

# Example 1: Business Hours Processing
# Runs every hour during business hours (9 AM - 5 PM) on weekdays
business_hours_dag = DAG(
    'business_hours_processing',
    description='Process data during business hours only',
    schedule_interval='0 9-17 * * 1-5',  # 9 AM to 5 PM, Monday to Friday
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['scheduling', 'business-hours']
)

process_business_data = BashOperator(
    task_id='process_business_data',
    bash_command='echo "Processing business data at $(date)"',
    dag=business_hours_dag
)

# Example 2: End of Month Reporting
# Runs on the last day of each month at 11 PM
end_of_month_dag = DAG(
    'end_of_month_reporting',
    description='Generate monthly reports on the last day of the month',
    schedule_interval='0 23 28-31 * *',  # Last few days of month at 11 PM
    start_date=datetime(2024, 1, 1),
    catchup=True,  # Process historical months
    max_active_runs=1,  # Prevent overlapping runs
    tags=['scheduling', 'monthly', 'reporting']
)


def check_last_day_of_month(**context):
    """Check if today is the last day of the month"""
    from calendar import monthrange
    execution_date = context['execution_date']
    year = execution_date.year
    month = execution_date.month
    day = execution_date.day

    # Get the last day of the month
    last_day = monthrange(year, month)[1]

    if day == last_day:
        print(f"Today ({day}) is the last day of {month}/{year}")
        return True
    else:
        print(
            f"Today ({day}) is not the last day of {month}/{year} (last day: {last_day})")
        return False


check_month_end = PythonOperator(
    task_id='check_month_end',
    python_callable=check_last_day_of_month,
    dag=end_of_month_dag
)

generate_monthly_report = BashOperator(
    task_id='generate_monthly_report',
    bash_command='echo "Generating monthly report for $(date +%Y-%m)"',
    dag=end_of_month_dag
)

check_month_end >> generate_monthly_report

# Example 3: Quarterly Data Processing with Timezone
# Runs quarterly on the first day at 2 AM Eastern Time
quarterly_dag = DAG(
    'quarterly_data_processing',
    description='Process quarterly data in Eastern timezone',
    schedule_interval='0 2 1 */3 *',  # First day of quarter at 2 AM
    start_date=datetime(2024, 1, 1, tzinfo=timezone('US/Eastern')),
    catchup=True,  # Process all quarters since start
    tags=['scheduling', 'quarterly', 'timezone']
)


def log_quarterly_info(**context):
    """Log information about the quarterly run"""
    execution_date = context['execution_date']
    quarter = (execution_date.month - 1) // 3 + 1
    print(f"Processing Q{quarter} {execution_date.year} data")
    print(f"Execution date: {execution_date}")
    print(f"Timezone: {execution_date.tzinfo}")


quarterly_setup = PythonOperator(
    task_id='quarterly_setup',
    python_callable=log_quarterly_info,
    dag=quarterly_dag
)

process_quarterly_data = BashOperator(
    task_id='process_quarterly_data',
    bash_command='echo "Processing quarterly data for Q$(date +%q) $(date +%Y)"',
    dag=quarterly_dag
)

quarterly_setup >> process_quarterly_data

# Example 4: Catchup Demonstration
# Shows the difference between catchup=True and catchup=False
catchup_demo_dag = DAG(
    'catchup_demonstration',
    description='Demonstrates catchup behavior with historical runs',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),  # Start date in the past
    catchup=True,  # Will create runs for all days since start_date
    max_active_runs=3,  # Limit concurrent runs
    tags=['scheduling', 'catchup', 'demo']
)


def log_execution_info(**context):
    """Log execution date and data interval information"""
    execution_date = context['execution_date']
    data_interval_start = context['data_interval_start']
    data_interval_end = context['data_interval_end']

    print(f"Execution Date: {execution_date}")
    print(f"Data Interval Start: {data_interval_start}")
    print(f"Data Interval End: {data_interval_end}")
    print(f"Processing data for: {data_interval_start.strftime('%Y-%m-%d')}")


log_execution = PythonOperator(
    task_id='log_execution_info',
    python_callable=log_execution_info,
    dag=catchup_demo_dag
)

process_daily_data = BashOperator(
    task_id='process_daily_data',
    bash_command='echo "Processing daily data for {{ ds }}"',
    dag=catchup_demo_dag
)

log_execution >> process_daily_data

# Example 5: Custom Schedule with Backfill Considerations
# Runs every 6 hours but skips weekends
custom_schedule_dag = DAG(
    'custom_schedule_backfill',
    description='Custom scheduling with backfill considerations',
    schedule_interval='0 */6 * * 1-5',  # Every 6 hours, weekdays only
    start_date=datetime(2024, 1, 1),
    catchup=True,
    max_active_runs=2,
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    },
    tags=['scheduling', 'custom', 'backfill']
)


def validate_business_day(**context):
    """Validate that we're running on a business day"""
    execution_date = context['execution_date']
    weekday = execution_date.weekday()  # Monday = 0, Sunday = 6

    if weekday < 5:  # Monday to Friday
        print(f"Valid business day: {execution_date.strftime('%A, %Y-%m-%d')}")
        return True
    else:
        print(
            f"Weekend day, skipping: {execution_date.strftime('%A, %Y-%m-%d')}")
        return False


validate_day = PythonOperator(
    task_id='validate_business_day',
    python_callable=validate_business_day,
    dag=custom_schedule_dag
)

process_6hourly_data = BashOperator(
    task_id='process_6hourly_data',
    bash_command='echo "Processing 6-hourly data at $(date)"',
    dag=custom_schedule_dag
)

validate_day >> process_6hourly_data

# Example 6: Dynamic Scheduling Based on External Factors
# Demonstrates how to handle variable scheduling requirements
dynamic_schedule_dag = DAG(
    'dynamic_schedule_example',
    description='Example of handling dynamic scheduling requirements',
    schedule_interval='0 */2 * * *',  # Every 2 hours as base schedule
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['scheduling', 'dynamic', 'conditional']
)


def determine_processing_frequency(**context):
    """Determine processing frequency based on day of week"""
    execution_date = context['execution_date']
    weekday = execution_date.weekday()

    if weekday < 5:  # Weekdays
        frequency = "high"
        print("Weekday detected: High frequency processing")
    else:  # Weekends
        frequency = "low"
        print("Weekend detected: Low frequency processing")

    # Store the frequency for downstream tasks
    return frequency


determine_frequency = PythonOperator(
    task_id='determine_processing_frequency',
    python_callable=determine_processing_frequency,
    dag=dynamic_schedule_dag
)


def process_based_on_frequency(**context):
    """Process data based on determined frequency"""
    # Get the frequency from the previous task
    frequency = context['task_instance'].xcom_pull(
        task_ids='determine_processing_frequency')

    if frequency == "high":
        print("Performing intensive data processing")
        # Simulate high-frequency processing
    else:
        print("Performing light data processing")
        # Simulate low-frequency processing


process_data = PythonOperator(
    task_id='process_based_on_frequency',
    python_callable=process_based_on_frequency,
    dag=dynamic_schedule_dag
)

determine_frequency >> process_data
