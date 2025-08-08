"""
Basic Scheduling Examples

This file demonstrates different scheduling patterns and DAG configurations.
Each DAG shows a specific scheduling concept with clear examples.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# Example 1: Daily DAG with Catchup Disabled
daily_dag = DAG(
    dag_id='example_daily_schedule',
    description='Runs daily at 2 AM, no historical runs',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,  # Don't run historical instances
    max_active_runs=1,
    default_args={
        'owner': 'data_team',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    },
    tags=['example', 'daily', 'scheduling']
)

daily_task = BashOperator(
    task_id='daily_processing',
    bash_command='echo "Processing data for {{ ds }}"',
    dag=daily_dag
)


# Example 2: Hourly DAG with Limited Active Runs
hourly_dag = DAG(
    dag_id='example_hourly_schedule',
    description='Runs every hour during business hours',
    schedule_interval='0 9-17 * * 1-5',  # Every hour, 9 AM to 5 PM, weekdays only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=2,  # Allow 2 concurrent runs
    default_args={
        'owner': 'monitoring_team',
        'retries': 1,
        'retry_delay': timedelta(minutes=2),
        'execution_timeout': timedelta(minutes=30),
    },
    tags=['example', 'hourly', 'business-hours']
)

hourly_task = PythonOperator(
    task_id='hourly_check',
    python_callable=lambda: print(f"Hourly check at {datetime.now()}"),
    dag=hourly_dag
)


# Example 3: Weekly DAG with Catchup Enabled
def process_weekly_data(**context):
    """Process a week's worth of data"""
    execution_date = context['execution_date']
    print(f"Processing weekly data for week ending: {execution_date}")
    print(
        f"Data range: {execution_date - timedelta(days=6)} to {execution_date}")
    return f"Processed week ending {execution_date.strftime('%Y-%m-%d')}"


weekly_dag = DAG(
    dag_id='example_weekly_schedule',
    description='Runs weekly on Sundays, processes historical data',
    schedule_interval='0 3 * * 0',  # Sundays at 3 AM
    start_date=datetime(2024, 1, 7),  # First Sunday of 2024
    catchup=True,  # Process historical weeks if needed
    max_active_runs=1,
    default_args={
        'owner': 'analytics_team',
        'retries': 3,
        'retry_delay': timedelta(minutes=10),
        'email_on_failure': True,
        'email_on_retry': False,
    },
    tags=['example', 'weekly', 'analytics']
)

weekly_task = PythonOperator(
    task_id='weekly_processing',
    python_callable=process_weekly_data,
    dag=weekly_dag
)


# Example 4: Custom Interval DAG
custom_interval_dag = DAG(
    dag_id='example_custom_interval',
    description='Runs every 6 hours using timedelta',
    schedule_interval=timedelta(hours=6),  # Every 6 hours
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        'owner': 'ops_team',
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    },
    tags=['example', 'custom-interval']
)

custom_task = BashOperator(
    task_id='system_check',
    bash_command='''
    echo "System check at $(date)"
    echo "Uptime: $(uptime)"
    echo "Disk usage: $(df -h / | tail -1)"
    ''',
    dag=custom_interval_dag
)


# Example 5: Manual Trigger Only DAG
manual_dag = DAG(
    dag_id='example_manual_trigger',
    description='Only runs when manually triggered',
    schedule_interval=None,  # No automatic scheduling
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=3,  # Allow multiple manual runs
    default_args={
        'owner': 'admin_team',
        'retries': 0,  # No retries for manual tasks
    },
    tags=['example', 'manual', 'on-demand']
)


def manual_task_function(**context):
    """Function for manual execution"""
    trigger_time = datetime.now()
    execution_date = context['execution_date']
    print(f"Manual task triggered at: {trigger_time}")
    print(f"Execution date: {execution_date}")
    print("This task only runs when manually triggered!")
    return "Manual task completed"


manual_task = PythonOperator(
    task_id='manual_processing',
    python_callable=manual_task_function,
    dag=manual_dag
)


# Example 6: Complex Cron Schedule
complex_schedule_dag = DAG(
    dag_id='example_complex_schedule',
    description='Complex scheduling: 1st and 15th of month at 10:30 AM',
    schedule_interval='30 10 1,15 * *',  # 1st and 15th at 10:30 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        'owner': 'finance_team',
        'retries': 2,
        'retry_delay': timedelta(minutes=15),
        'execution_timeout': timedelta(hours=2),
    },
    tags=['example', 'complex-schedule', 'bi-monthly']
)


def bi_monthly_processing(**context):
    """Process bi-monthly financial data"""
    execution_date = context['execution_date']
    day = execution_date.day

    if day == 1:
        print("Processing month-start financial data")
        print("- Generating monthly reports")
        print("- Updating budget allocations")
    elif day == 15:
        print("Processing mid-month financial data")
        print("- Calculating mid-month metrics")
        print("- Preparing payroll data")

    return f"Bi-monthly processing completed for day {day}"


complex_task = PythonOperator(
    task_id='bi_monthly_processing',
    python_callable=bi_monthly_processing,
    dag=complex_schedule_dag
)


# Example 7: DAG with Depends on Past
depends_on_past_dag = DAG(
    dag_id='example_depends_on_past',
    description='Tasks depend on previous run success',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        'owner': 'data_pipeline_team',
        'depends_on_past': True,  # Tasks depend on previous run
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    },
    tags=['example', 'depends-on-past', 'sequential']
)

sequential_task = BashOperator(
    task_id='sequential_processing',
    bash_command='''
    echo "This task depends on the previous run's success"
    echo "Execution date: {{ ds }}"
    echo "Previous execution must have succeeded for this to run"
    ''',
    dag=depends_on_past_dag
)


# Example 8: DAG with SLA Configuration
sla_dag = DAG(
    dag_id='example_sla_configuration',
    description='DAG with SLA monitoring',
    schedule_interval='0 8 * * 1-5',  # Weekdays at 8 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        'owner': 'sla_team',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'sla': timedelta(hours=2),  # Task should complete within 2 hours
        'email_on_failure': True,
        'email_on_retry': False,
    },
    tags=['example', 'sla', 'monitoring']
)

sla_task = PythonOperator(
    task_id='sla_monitored_task',
    python_callable=lambda: print("This task has a 2-hour SLA"),
    dag=sla_dag
)
