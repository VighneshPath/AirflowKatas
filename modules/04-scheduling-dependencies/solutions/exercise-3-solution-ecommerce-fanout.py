"""
Solution for Exercise 3, Task 1: E-commerce Data Pipeline (Fan-Out Pattern)

This DAG demonstrates a fan-out pattern where one data preparation task
feeds multiple parallel processing tasks for different aspects of e-commerce data.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import random

# DAG configuration
dag = DAG(
    'ecommerce_fanout_pipeline',
    description='E-commerce data processing with fan-out pattern',
    schedule_interval='0 3 * * *',  # Daily at 3 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    },
    tags=['dependencies', 'fan-out', 'ecommerce']
)


def prepare_ecommerce_data(**context):
    """Prepare e-commerce data for parallel processing"""
    execution_date = context['execution_date']

    print(f"=== E-commerce Data Preparation - {execution_date.date()} ===")
    print("Extracting raw e-commerce data...")

    # Simulate data preparation
    raw_data = {
        'customers': [
            {'id': i, 'name': f'Customer_{i}', 'segment': random.choice(
                ['Premium', 'Standard', 'Basic'])}
            for i in range(1, 501)  # 500 customers
        ],
        'products': [
            {'id': i, 'name': f'Product_{i}', 'category': random.choice(
                ['Electronics', 'Clothing', 'Books', 'Home'])}
            for i in range(1, 201)  # 200 products
        ],
        'orders': [
            {
                'id': i,
                'customer_id': random.randint(1, 500),
                'product_id': random.randint(1, 200),
                'amount': round(random.uniform(10, 500), 2),
                'quantity': random.randint(1, 5)
            }
            for i in range(1, 1001)  # 1000 orders
        ],
        'inventory': [
            {
                'product_id': i,
                'stock_level': random.randint(0, 100),
                'reorder_point': random.randint(10, 30)
            }
            for i in range(1, 201)  # Inventory for all products
        ]
    }

    print(f"Prepared data summary:")
    print(f"  - Customers: {len(raw_data['customers'])}")
    print(f"  - Products: {len(raw_data['products'])}")
    print(f"  - Orders: {len(raw_data['orders'])}")
    print(f"  - Inventory items: {len(raw_data['inventory'])}")

    print("Data preparation completed successfully")
    return raw_data


def analyze_customers(**context):
    """Process customer behavior data"""
    print("=== Customer Analysis ===")

    # Get prepared data
    raw_data = context['task_instance'].xcom_pull(
        task_ids='prepare_ecommerce_data')
    customers = raw_data['customers']
    orders = raw_data['orders']

    print("Analyzing customer behavior patterns...")

    # Customer segmentation analysis
    segment_counts = {}
    for customer in customers:
        segment = customer['segment']
        segment_counts[segment] = segment_counts.get(segment, 0) + 1

    # Customer order analysis
    customer_orders = {}
    total_revenue_by_customer = {}

    for order in orders:
        customer_id = order['customer_id']
        customer_orders[customer_id] = customer_orders.get(customer_id, 0) + 1
        total_revenue_by_customer[customer_id] = total_revenue_by_customer.get(
            customer_id, 0) + order['amount']

    # Calculate metrics
    avg_orders_per_customer = sum(
        customer_orders.values()) / len(customer_orders)
    avg_revenue_per_customer = sum(
        total_revenue_by_customer.values()) / len(total_revenue_by_customer)

    print(f"Customer Analysis Results:")
    print(f"  - Customer segments: {segment_counts}")
    print(f"  - Average orders per customer: {avg_orders_per_customer:.2f}")
    print(f"  - Average revenue per customer: ${avg_revenue_per_customer:.2f}")
    print(f"  - Total active customers: {len(customer_orders)}")

    analysis_results = {
        'segment_distribution': segment_counts,
        'avg_orders_per_customer': avg_orders_per_customer,
        'avg_revenue_per_customer': avg_revenue_per_customer,
        'total_customers': len(customers),
        'active_customers': len(customer_orders)
    }

    print("Customer analysis completed")
    return analysis_results


def analyze_products(**context):
    """Process product performance data"""
    print("=== Product Analysis ===")

    # Get prepared data
    raw_data = context['task_instance'].xcom_pull(
        task_ids='prepare_ecommerce_data')
    products = raw_data['products']
    orders = raw_data['orders']

    print("Analyzing product performance...")

    # Product category analysis
    category_counts = {}
    for product in products:
        category = product['category']
        category_counts[category] = category_counts.get(category, 0) + 1

    # Product sales analysis
    product_sales = {}
    product_revenue = {}

    for order in orders:
        product_id = order['product_id']
        product_sales[product_id] = product_sales.get(
            product_id, 0) + order['quantity']
        product_revenue[product_id] = product_revenue.get(
            product_id, 0) + order['amount']

    # Top performing products
    top_products_by_sales = sorted(
        product_sales.items(), key=lambda x: x[1], reverse=True)[:5]
    top_products_by_revenue = sorted(
        product_revenue.items(), key=lambda x: x[1], reverse=True)[:5]

    print(f"Product Analysis Results:")
    print(f"  - Product categories: {category_counts}")
    print(f"  - Total products: {len(products)}")
    print(f"  - Products with sales: {len(product_sales)}")
    print(f"  - Top 5 products by sales volume: {top_products_by_sales}")
    print(f"  - Top 5 products by revenue: {top_products_by_revenue}")

    analysis_results = {
        'category_distribution': category_counts,
        'total_products': len(products),
        'products_with_sales': len(product_sales),
        'top_products_sales': top_products_by_sales,
        'top_products_revenue': top_products_by_revenue
    }

    print("Product analysis completed")
    return analysis_results


def analyze_orders(**context):
    """Process order and transaction data"""
    print("=== Order Analysis ===")

    # Get prepared data
    raw_data = context['task_instance'].xcom_pull(
        task_ids='prepare_ecommerce_data')
    orders = raw_data['orders']

    print("Analyzing order and transaction patterns...")

    # Order metrics
    total_orders = len(orders)
    total_revenue = sum(order['amount'] for order in orders)
    total_quantity = sum(order['quantity'] for order in orders)
    avg_order_value = total_revenue / total_orders
    avg_order_quantity = total_quantity / total_orders

    # Order value distribution
    order_values = [order['amount'] for order in orders]
    order_values.sort()

    # Percentiles
    p25_index = int(0.25 * len(order_values))
    p50_index = int(0.50 * len(order_values))
    p75_index = int(0.75 * len(order_values))

    percentiles = {
        '25th': order_values[p25_index],
        '50th': order_values[p50_index],
        '75th': order_values[p75_index]
    }

    print(f"Order Analysis Results:")
    print(f"  - Total orders: {total_orders:,}")
    print(f"  - Total revenue: ${total_revenue:,.2f}")
    print(f"  - Total items sold: {total_quantity:,}")
    print(f"  - Average order value: ${avg_order_value:.2f}")
    print(f"  - Average items per order: {avg_order_quantity:.2f}")
    print(f"  - Order value percentiles: {percentiles}")

    analysis_results = {
        'total_orders': total_orders,
        'total_revenue': total_revenue,
        'total_quantity': total_quantity,
        'avg_order_value': avg_order_value,
        'avg_order_quantity': avg_order_quantity,
        'value_percentiles': percentiles
    }

    print("Order analysis completed")
    return analysis_results


def analyze_inventory(**context):
    """Process inventory and stock data"""
    print("=== Inventory Analysis ===")

    # Get prepared data
    raw_data = context['task_instance'].xcom_pull(
        task_ids='prepare_ecommerce_data')
    inventory = raw_data['inventory']
    orders = raw_data['orders']

    print("Analyzing inventory levels and stock management...")

    # Inventory metrics
    total_items = len(inventory)
    total_stock = sum(item['stock_level'] for item in inventory)
    avg_stock_level = total_stock / total_items

    # Stock status analysis
    out_of_stock = [item for item in inventory if item['stock_level'] == 0]
    low_stock = [item for item in inventory if 0 <
                 item['stock_level'] <= item['reorder_point']]
    healthy_stock = [
        item for item in inventory if item['stock_level'] > item['reorder_point']]

    # Product demand analysis (from orders)
    product_demand = {}
    for order in orders:
        product_id = order['product_id']
        product_demand[product_id] = product_demand.get(
            product_id, 0) + order['quantity']

    # Identify high-demand, low-stock items
    critical_items = []
    for item in inventory:
        product_id = item['product_id']
        demand = product_demand.get(product_id, 0)
        if item['stock_level'] <= item['reorder_point'] and demand > 10:
            critical_items.append({
                'product_id': product_id,
                'stock_level': item['stock_level'],
                'demand': demand
            })

    print(f"Inventory Analysis Results:")
    print(f"  - Total inventory items: {total_items}")
    print(f"  - Total stock units: {total_stock:,}")
    print(f"  - Average stock level: {avg_stock_level:.2f}")
    print(f"  - Out of stock items: {len(out_of_stock)}")
    print(f"  - Low stock items: {len(low_stock)}")
    print(f"  - Healthy stock items: {len(healthy_stock)}")
    print(
        f"  - Critical items (high demand, low stock): {len(critical_items)}")

    analysis_results = {
        'total_items': total_items,
        'total_stock': total_stock,
        'avg_stock_level': avg_stock_level,
        'out_of_stock_count': len(out_of_stock),
        'low_stock_count': len(low_stock),
        'healthy_stock_count': len(healthy_stock),
        'critical_items': critical_items[:10]  # Top 10 critical items
    }

    print("Inventory analysis completed")
    return analysis_results


# Task definitions
prepare_data = PythonOperator(
    task_id='prepare_ecommerce_data',
    python_callable=prepare_ecommerce_data,
    dag=dag
)

customer_analysis = PythonOperator(
    task_id='analyze_customers',
    python_callable=analyze_customers,
    dag=dag
)

product_analysis = PythonOperator(
    task_id='analyze_products',
    python_callable=analyze_products,
    dag=dag
)

order_analysis = PythonOperator(
    task_id='analyze_orders',
    python_callable=analyze_orders,
    dag=dag
)

inventory_analysis = PythonOperator(
    task_id='analyze_inventory',
    python_callable=analyze_inventory,
    dag=dag
)

# Fan-out dependency pattern: one task feeds multiple parallel tasks
prepare_data >> [customer_analysis, product_analysis,
                 order_analysis, inventory_analysis]
