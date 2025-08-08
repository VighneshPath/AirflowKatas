"""
Solution: Hourly System Monitoring DAG

This solution demonstrates hourly scheduling during business hours with parallel monitoring tasks.
Key concepts: cron scheduling, parallel execution, fan-in pattern, execution timeouts.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# Python functions for system monitoring
def check_cpu_usage(**context):
    """Monitor CPU utilization"""
    print("Checking CPU usage...")

    # Simulate CPU monitoring
    import random
    cpu_usage = random.uniform(20, 80)  # Random CPU usage between 20-80%

    print(f"  - Current CPU usage: {cpu_usage:.1f}%")

    # Check against thresholds
    if cpu_usage > 70:
        print("  - WARNING: High CPU usage detected!")
        status = "WARNING"
    elif cpu_usage > 50:
        print("  - CAUTION: Moderate CPU usage")
        status = "CAUTION"
    else:
        print("  - OK: CPU usage is normal")
        status = "OK"

    return {
        'metric': 'cpu_usage',
        'value': cpu_usage,
        'unit': 'percent',
        'status': status,
        'threshold_warning': 50,
        'threshold_critical': 70
    }


def check_memory_usage(**context):
    """Monitor memory utilization"""
    print("Checking memory usage...")

    # Simulate memory monitoring
    import random
    memory_usage = random.uniform(30, 85)  # Random memory usage

    print(f"  - Current memory usage: {memory_usage:.1f}%")

    # Check against thresholds
    if memory_usage > 80:
        print("  - CRITICAL: High memory usage!")
        status = "CRITICAL"
    elif memory_usage > 60:
        print("  - WARNING: Elevated memory usage")
        status = "WARNING"
    else:
        print("  - OK: Memory usage is normal")
        status = "OK"

    return {
        'metric': 'memory_usage',
        'value': memory_usage,
        'unit': 'percent',
        'status': status,
        'threshold_warning': 60,
        'threshold_critical': 80
    }


def check_disk_space(**context):
    """Monitor disk space"""
    print("Checking disk space...")

    # Simulate disk space monitoring
    import random
    disk_usage = random.uniform(40, 90)  # Random disk usage
    available_gb = random.uniform(50, 500)  # Available space in GB

    print(f"  - Current disk usage: {disk_usage:.1f}%")
    print(f"  - Available space: {available_gb:.1f} GB")

    # Check against thresholds
    if disk_usage > 85:
        print("  - CRITICAL: Low disk space!")
        status = "CRITICAL"
    elif disk_usage > 70:
        print("  - WARNING: Disk space getting low")
        status = "WARNING"
    else:
        print("  - OK: Disk space is adequate")
        status = "OK"

    return {
        'metric': 'disk_usage',
        'value': disk_usage,
        'unit': 'percent',
        'available_gb': available_gb,
        'status': status,
        'threshold_warning': 70,
        'threshold_critical': 85
    }


def check_network_connectivity(**context):
    """Test network connections"""
    print("Checking network connectivity...")

    # Simulate network connectivity tests
    import random

    endpoints = [
        'database-server',
        'api-gateway',
        'external-service',
        'backup-server'
    ]

    results = {}
    overall_status = "OK"

    for endpoint in endpoints:
        # Simulate connection test
        response_time = random.uniform(10, 200)  # Response time in ms
        is_reachable = random.choice(
            [True, True, True, False])  # 75% success rate

        if is_reachable:
            if response_time > 100:
                endpoint_status = "SLOW"
                print(
                    f"  - {endpoint}: Reachable but slow ({response_time:.0f}ms)")
            else:
                endpoint_status = "OK"
                print(f"  - {endpoint}: OK ({response_time:.0f}ms)")
        else:
            endpoint_status = "FAILED"
            overall_status = "CRITICAL"
            print(f"  - {endpoint}: FAILED - Unreachable")

        results[endpoint] = {
            'reachable': is_reachable,
            'response_time_ms': response_time,
            'status': endpoint_status
        }

    return {
        'metric': 'network_connectivity',
        'endpoints': results,
        'status': overall_status,
        'total_endpoints': len(endpoints),
        'failed_endpoints': sum(1 for r in results.values() if not r['reachable'])
    }


def aggregate_metrics(**context):
    """Combine all monitoring data"""
    print("Aggregating system monitoring metrics...")

    # Pull data from all monitoring tasks
    cpu_data = context['task_instance'].xcom_pull(task_ids='check_cpu_usage')
    memory_data = context['task_instance'].xcom_pull(
        task_ids='check_memory_usage')
    disk_data = context['task_instance'].xcom_pull(task_ids='check_disk_space')
    network_data = context['task_instance'].xcom_pull(
        task_ids='check_network_connectivity')

    # Aggregate status
    all_statuses = [
        cpu_data['status'],
        memory_data['status'],
        disk_data['status'],
        network_data['status']
    ]

    # Determine overall system health
    if 'CRITICAL' in all_statuses:
        overall_status = 'CRITICAL'
    elif 'WARNING' in all_statuses:
        overall_status = 'WARNING'
    elif 'CAUTION' in all_statuses:
        overall_status = 'CAUTION'
    else:
        overall_status = 'OK'

    # Create summary
    summary = {
        'timestamp': datetime.now().isoformat(),
        'overall_status': overall_status,
        'metrics': {
            'cpu': cpu_data,
            'memory': memory_data,
            'disk': disk_data,
            'network': network_data
        }
    }

    print(f"  - Overall system status: {overall_status}")
    print(f"  - CPU: {cpu_data['value']:.1f}% ({cpu_data['status']})")
    print(f"  - Memory: {memory_data['value']:.1f}% ({memory_data['status']})")
    print(f"  - Disk: {disk_data['value']:.1f}% ({disk_data['status']})")
    print(
        f"  - Network: {network_data['failed_endpoints']}/{network_data['total_endpoints']} failed ({network_data['status']})")

    # Simulate actions based on status
    if overall_status == 'CRITICAL':
        print("  - ALERT: Sending critical system alert to ops team!")
        print("  - Creating incident ticket...")
    elif overall_status == 'WARNING':
        print("  - Sending warning notification to monitoring channel")

    print("  - Updating monitoring dashboard...")
    print("  - Storing metrics in time-series database...")

    return summary


# DAG configuration
default_args = {
    'owner': 'ops_team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=15),  # 15-minute timeout per task
}

dag = DAG(
    dag_id='hourly_system_monitoring',
    description='Monitor system health during business hours',
    schedule_interval='0 9-17 * * 1-5',  # Every hour, 9 AM to 5 PM, weekdays only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=3,  # Allow some overlap for monitoring
    default_args=default_args,
    tags=['monitoring', 'hourly', 'system']
)

# Task definitions - parallel monitoring tasks
check_cpu_task = PythonOperator(
    task_id='check_cpu_usage',
    python_callable=check_cpu_usage,
    dag=dag
)

check_memory_task = PythonOperator(
    task_id='check_memory_usage',
    python_callable=check_memory_usage,
    dag=dag
)

check_disk_task = PythonOperator(
    task_id='check_disk_space',
    python_callable=check_disk_space,
    dag=dag
)

check_network_task = PythonOperator(
    task_id='check_network_connectivity',
    python_callable=check_network_connectivity,
    dag=dag
)

# Aggregation task
aggregate_task = PythonOperator(
    task_id='aggregate_metrics',
    python_callable=aggregate_metrics,
    dag=dag
)

# Set up fan-in dependencies: parallel monitoring → aggregation
[check_cpu_task, check_memory_task, check_disk_task,
    check_network_task] >> aggregate_task
