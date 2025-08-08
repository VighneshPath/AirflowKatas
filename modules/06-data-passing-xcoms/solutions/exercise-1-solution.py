"""
Exercise 1 Solution: Basic XCom Operations

This solution demonstrates:
- Basic XCom push and pull operations
- Data passing between multiple tasks
- Error handling and validation
- Working with different data types in XComs
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# DAG configuration
default_args = {
    'owner': 'airflow-kata',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_order_processing',
    default_args=default_args,
    description='Daily order processing with XCom data passing',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['exercise', 'xcom', 'orders', 'solution']
)


def extract_orders():
    """Extract order data from simulated database"""
    # Simulate database extraction with realistic data
    order_details = [
        {"order_id": 1001, "amount": 89.99, "status": "completed"},
        {"order_id": 1002, "amount": 156.75, "status": "completed"},
        {"order_id": 1003, "amount": 45.00, "status": "pending"},
        {"order_id": 1004, "amount": 234.50, "status": "completed"},
        {"order_id": 1005, "amount": 67.25, "status": "pending"},
        {"order_id": 1006, "amount": 123.80, "status": "completed"},
        {"order_id": 1007, "amount": 98.40, "status": "completed"},
        {"order_id": 1008, "amount": 178.90, "status": "pending"},
    ]

    # Calculate totals
    total_orders = len(order_details)
    total_revenue = sum(order['amount'] for order in order_details)

    # Create the return data structure
    order_data = {
        "total_orders": total_orders,
        "total_revenue": round(total_revenue, 2),
        "order_details": order_details,
        "extraction_timestamp": datetime.now().isoformat()
    }

    print(
        f"Extracted {total_orders} orders with total revenue ${total_revenue:.2f}")

    # Return value is automatically pushed to XCom
    return order_data


def calculate_kpis(**context):
    """Calculate KPIs from order data"""
    # Pull data from the extract_orders task
    order_data = context['task_instance'].xcom_pull(task_ids='extract_orders')

    if not order_data:
        raise ValueError("No order data received from extract_orders task")

    # Extract order details for calculations
    order_details = order_data['order_details']
    total_orders = len(order_details)

    # Calculate KPIs
    completed_orders = [
        order for order in order_details if order['status'] == 'completed']
    pending_orders = [
        order for order in order_details if order['status'] == 'pending']

    # Average order value (all orders)
    total_amount = sum(order['amount'] for order in order_details)
    avg_order_value = round(total_amount / total_orders,
                            2) if total_orders > 0 else 0

    # Completion rate
    completion_rate = round(len(completed_orders) /
                            total_orders, 2) if total_orders > 0 else 0

    # Revenue from completed orders only
    completed_revenue = round(sum(order['amount']
                              for order in completed_orders), 2)

    # Number of pending orders
    pending_count = len(pending_orders)

    kpis = {
        "avg_order_value": avg_order_value,
        "completion_rate": completion_rate,
        "completed_revenue": completed_revenue,
        "pending_orders": pending_count,
        "calculation_timestamp": datetime.now().isoformat()
    }

    print(
        f"Calculated KPIs: Avg Order ${avg_order_value}, Completion Rate {completion_rate*100}%")

    return kpis


def generate_summary(**context):
    """Generate summary report"""
    ti = context['task_instance']

    # Pull data from both previous tasks
    order_data = ti.xcom_pull(task_ids='extract_orders')
    kpis = ti.xcom_pull(task_ids='calculate_kpis')

    if not order_data or not kpis:
        raise ValueError("Missing data from previous tasks")

    # Generate formatted report
    report = f"""Daily Order Summary
==================
Extraction Time: {order_data['extraction_timestamp']}
Calculation Time: {kpis['calculation_timestamp']}

Order Statistics:
- Total Orders: {order_data['total_orders']}
- Total Revenue: ${order_data['total_revenue']}
- Average Order Value: ${kpis['avg_order_value']}

Performance Metrics:
- Completion Rate: {kpis['completion_rate']*100:.1f}%
- Completed Revenue: ${kpis['completed_revenue']}
- Pending Orders: {kpis['pending_orders']}

Order Breakdown:
"""

    # Add order details to report
    for order in order_data['order_details']:
        status_symbol = "✓" if order['status'] == 'completed' else "⏳"
        report += f"- Order {order['order_id']}: ${order['amount']} {status_symbol}\n"

    summary_data = {
        "report": report,
        "report_length": len(report),
        "generated_at": datetime.now().isoformat()
    }

    print(f"Generated summary report ({len(report)} characters)")

    return summary_data


def validate_results(**context):
    """Validate all results"""
    ti = context['task_instance']

    # Pull data from all previous tasks
    order_data = ti.xcom_pull(task_ids='extract_orders')
    kpis = ti.xcom_pull(task_ids='calculate_kpis')
    summary = ti.xcom_pull(task_ids='generate_summary')

    validation_errors = []

    # Validate order data
    if not order_data:
        validation_errors.append("Missing order data")
    else:
        if order_data.get('total_orders', 0) <= 0:
            validation_errors.append("Total orders must be greater than 0")

        if order_data.get('total_revenue', 0) <= 0:
            validation_errors.append("Total revenue must be positive")

        if not order_data.get('order_details'):
            validation_errors.append("Order details are missing")

    # Validate KPIs
    if not kpis:
        validation_errors.append("Missing KPI data")
    else:
        completion_rate = kpis.get('completion_rate', -1)
        if not (0 <= completion_rate <= 1):
            validation_errors.append(
                f"Completion rate must be between 0 and 1, got {completion_rate}")

        if kpis.get('completed_revenue', 0) < 0:
            validation_errors.append("Completed revenue cannot be negative")

        if kpis.get('avg_order_value', 0) <= 0:
            validation_errors.append("Average order value must be positive")

    # Validate summary
    if not summary:
        validation_errors.append("Missing summary data")
    else:
        if not summary.get('report'):
            validation_errors.append("Summary report is empty")

        if summary.get('report_length', 0) <= 0:
            validation_errors.append("Report length must be positive")

    # Cross-validation: Check if numbers add up
    if order_data and kpis:
        # Verify that completed + pending = total
        total_from_kpis = len([o for o in order_data['order_details']
                              if o['status'] == 'completed']) + kpis['pending_orders']
        if total_from_kpis != order_data['total_orders']:
            validation_errors.append(
                f"Order count mismatch: {total_from_kpis} vs {order_data['total_orders']}")

    # Prepare validation result
    validation_result = {
        "validation_status": "passed" if not validation_errors else "failed",
        "errors": validation_errors,
        "error_count": len(validation_errors),
        "validated_at": datetime.now().isoformat(),
        "data_summary": {
            "orders_validated": order_data['total_orders'] if order_data else 0,
            "kpis_validated": len(kpis) if kpis else 0,
            "report_validated": bool(summary and summary.get('report'))
        }
    }

    if validation_errors:
        print(f"Validation FAILED with {len(validation_errors)} errors:")
        for error in validation_errors:
            print(f"  - {error}")
        # In a real scenario, you might want to raise an exception here
        # raise ValueError(f"Validation failed: {validation_errors}")
    else:
        print("Validation PASSED - All data is valid")

    return validation_result


# Define tasks
extract_task = PythonOperator(
    task_id='extract_orders',
    python_callable=extract_orders,
    dag=dag
)

calculate_task = PythonOperator(
    task_id='calculate_kpis',
    python_callable=calculate_kpis,
    dag=dag
)

summary_task = PythonOperator(
    task_id='generate_summary',
    python_callable=generate_summary,
    dag=dag
)

validate_task = PythonOperator(
    task_id='validate_results',
    python_callable=validate_results,
    dag=dag
)

# Set up task dependencies
extract_task >> calculate_task >> summary_task >> validate_task

# Alternative dependency syntax (both work the same way):
# extract_task.set_downstream(calculate_task)
# calculate_task.set_downstream(summary_task)
# summary_task.set_downstream(validate_task)
