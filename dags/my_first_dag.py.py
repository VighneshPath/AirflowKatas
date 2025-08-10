from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def process_orders():
    print("Processing orders...")
    order_ids = [1001, 1002, 1003, 1004, 1005]

    for order_id in order_ids:
        print(f"Processing order {order_id}")

    orders_processed = len(order_ids)
    print(f"Successfully processed {orders_processed} orders")
    return orders_processed

def generate_report():
    print("Generating daily summary report...")
    print("Current date and time:", datetime.now())
    return "Report generated successfully!"


default_args = {
    "owner": "vighnesh",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 2
}

dag = DAG(
    "my_first_dag1",
    description="A simple DAG to process orders and generate reports",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    tags=["example", "exercise-1", "daily"],
    catchup=False
    )

system_checks = BashOperator(
    task_id="system_checks",
    bash_command='''
    echo "System health check: All systems operational"
    echo "Checking disk space:"
    df -h
    echo "System check completed successfully"
    ''',
    dag=dag
)

backup_data = BashOperator(
    task_id="backup_data",
    bash_command='''
    echo "Backing up yesterday's data..."
    echo "Backup started at: $(date)"
    echo "Backup completed successfully"
    ''',
    dag=dag
)

send_notification = BashOperator(
    task_id = "send_notification",
    bash_command = '''
    echo "Daily processing completed successfully!"
    echo "Notification sent to operations team"
    ''',
    dag = dag
    )

process_orders_task = PythonOperator(
    task_id='process_orders',
    python_callable=process_orders,
    dag=dag,
)


generate_report_task = PythonOperator(
    task_id = "generate_report",
    python_callable = generate_report,
    dag = dag
)

system_checks >> backup_data >> process_orders_task >> generate_report_task >> send_notification