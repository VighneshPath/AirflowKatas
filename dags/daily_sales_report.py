from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def extract_sales_data(**context):
    """Extract yesterday's sales data"""
    execution_date = context['execution_date']
    print(f"Extracting sales data for {execution_date.strftime('%Y-%m-%d')}")

    # Simulate data extraction process
    print("  - Connecting to sales database...")
    print("  - Querying transaction tables...")
    print("  - Filtering for date range...")
    print("  - Validating data integrity...")

    # Simulate extracted data
    records_count = 1234
    total_revenue = 45678.90

    print(f"  - Extracted {records_count} sales records")
    print(f"  - Total revenue: ${total_revenue:,.2f}")

    # Return data for downstream tasks
    return {
        'records_count': records_count,
        'total_revenue': total_revenue,
        'extraction_date': execution_date.strftime('%Y-%m-%d')
    }


def calculate_metrics(**context):
    sales_data = context["task_instance"].xcom_pull(
        task_ids = 'extract_sales_data'
    )

    print("Calculating daily sales metrics")

    avg_order = sales_data['total_revenue'] / sales_data['records_count']

    metrics = {
        'total_revenue': sales_data['total_revenue'],
        'total_orders': sales_data['records_count'],
        'average_order_value': avg_order,
        'revenue_growth': 4.1,
        'top_category': 'electronics',
        'top_product_category': 'Electronics',
        'peak_hour': '2:00 PM'
    }

    return metrics


def generate_report(**context):
    metrics = context['task_instance'].xcom_pull(task_ids='calculate_metrics')
    execution_date = context['execution_date']

    print(f"Generating sales report for {execution_date.strftime('%Y-%m-%d')}")

    # Create report content
    report_content = f"""
    DAILY SALES REPORT
    Date: {execution_date.strftime('%Y-%m-%d')}
    
    SUMMARY METRICS:
    - Total Revenue: ${metrics['total_revenue']:,.2f}
    - Total Orders: {metrics['total_orders']:,}
    - Average Order Value: ${metrics['average_order_value']:.2f}
    - Revenue Growth: {metrics['revenue_growth']}%
    
    INSIGHTS:
    - Top Product Category: {metrics['top_product_category']}
    - Peak Sales Hour: {metrics['peak_hour']}
    
    Report generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    """

    print("  - Report generated successfully")
    print("  - Report preview:")
    print(report_content)

    return {
        'report_content': report_content,
        'report_date': execution_date.strftime('%Y-%m-%d'),
        'generation_time': datetime.now().isoformat()
    }

def send_report(**context):
    """Send report to stakeholders"""
    report_data = context['task_instance'].xcom_pull(
        task_ids='generate_report')

    print("Sending sales report to stakeholders...")

    # Simulate email sending
    recipients = ['sales-manager@company.com',
                  'ceo@company.com', 'analytics-team@company.com']

    print(f"  - Recipients: {', '.join(recipients)}")
    print(f"  - Subject: Daily Sales Report - {report_data['report_date']}")
    print("  - Attaching report file...")
    print("  - Email sent successfully!")

    # Simulate additional notifications
    print("  - Updating sales dashboard...")
    print("  - Posting to Slack #sales-updates channel...")
    print("  - Archiving report to shared drive...")

    return f"Report sent successfully for {report_data['report_date']}"


default_args = {
    'owner': 'vighnesh',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id = 'daily_sales_report',
    description='Generate daily sales report for previous day',
    start_date=datetime(2024, 2, 1),
    schedule_interval = '0 6 * * *',
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags = ['sales', 'daily', 'reports']
)

extract_sales_data_task = PythonOperator(
    task_id = 'extract_sales_data',
    python_callable = extract_sales_data,
    dag = dag
)

calculate_metrics_task = PythonOperator(
    task_id='calculate_metrics',
    python_callable=calculate_metrics,
    dag=dag
)

generate_report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag
)

send_report_task = PythonOperator(
    task_id='send_report',
    python_callable=send_report,
    dag=dag
)

extract_sales_data_task >> calculate_metrics_task >> generate_report_task >> send_report_task