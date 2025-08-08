"""
Solution for Exercise 1, Task 2: Weekend Maintenance

This DAG runs maintenance tasks every 4 hours on weekends only,
with catchup enabled to ensure maintenance consistency.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# DAG configuration
dag = DAG(
    'weekend_maintenance',
    description='Weekend maintenance tasks running every 4 hours',
    # Every 4 hours on Saturday (6) and Sunday (0)
    schedule_interval='0 */4 * * 0,6',
    start_date=datetime(2024, 1, 1),
    catchup=True,  # Process historical runs for maintenance consistency
    max_active_runs=2,  # Limit concurrent maintenance runs
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=10),
    },
    tags=['scheduling', 'weekend', 'maintenance']
)


def validate_weekend_execution(**context):
    """Validate that we're executing on a weekend"""
    execution_date = context['execution_date']
    weekday = execution_date.weekday()  # Monday = 0, Sunday = 6
    day_name = execution_date.strftime('%A')

    print(f"=== Weekend Maintenance Validation ===")
    print(f"Execution Date: {execution_date}")
    print(f"Day: {day_name} (weekday: {weekday})")

    # Weekend check: Saturday = 5, Sunday = 6
    if weekday in [5, 6]:
        print("✓ Confirmed: Executing on weekend")
        return True
    else:
        print("⚠ Warning: Not executing on weekend")
        return False


def perform_system_cleanup(**context):
    """Perform system cleanup tasks"""
    execution_date = context['execution_date']

    print(f"=== System Cleanup - {execution_date} ===")
    print("Cleaning temporary files...")
    print("Clearing cache directories...")
    print("Removing old log files...")
    print("Optimizing database indexes...")
    print("System cleanup completed successfully")

    return "cleanup_completed"


def perform_health_checks(**context):
    """Perform system health checks"""
    execution_date = context['execution_date']

    print(f"=== Health Checks - {execution_date} ===")
    print("Checking disk space...")
    print("Monitoring memory usage...")
    print("Validating database connections...")
    print("Testing API endpoints...")
    print("Checking service status...")
    print("All health checks passed")

    return "health_check_passed"


# Validation task
validate_weekend = PythonOperator(
    task_id='validate_weekend_execution',
    python_callable=validate_weekend_execution,
    dag=dag
)

# System cleanup task
system_cleanup = PythonOperator(
    task_id='system_cleanup',
    python_callable=perform_system_cleanup,
    dag=dag
)

# Health check task
health_checks = PythonOperator(
    task_id='health_checks',
    python_callable=perform_health_checks,
    dag=dag
)

# Additional maintenance tasks using BashOperator
disk_maintenance = BashOperator(
    task_id='disk_maintenance',
    bash_command='''
    echo "=== Disk Maintenance ==="
    echo "Current disk usage:"
    df -h | head -5
    echo "Cleaning up temporary files..."
    echo "Disk maintenance completed"
    ''',
    dag=dag
)

log_maintenance_summary = BashOperator(
    task_id='log_maintenance_summary',
    bash_command='''
    echo "=== Maintenance Summary ==="
    echo "Maintenance completed at: $(date)"
    echo "Next maintenance in 4 hours"
    echo "All systems operational"
    ''',
    dag=dag
)

# Set task dependencies
validate_weekend >> [system_cleanup, health_checks, disk_maintenance]
[system_cleanup, health_checks, disk_maintenance] >> log_maintenance_summary
