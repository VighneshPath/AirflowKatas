"""
Exercise 2 Solution: Working with Different Data Types in XComs

This solution demonstrates:
- Working with various data types (strings, numbers, lists, dictionaries)
- Multiple XCom keys from a single task
- Complex nested data structures
- XCom templating in BashOperator
- Proper serialization handling
"""

from datetime import datetime, timedelta
import random
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

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
    'customer_analytics_pipeline',
    default_args=default_args,
    description='Customer analytics with different data types in XComs',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['exercise', 'xcom', 'data-types', 'solution']
)


def load_customer_data():
    """Load customer data with mixed data types"""
    customers = [
        {
            "customer_id": 12345,
            "name": "Alice Johnson",
            "email": "alice@example.com",
            "age": 28,
            "is_premium": True,
            "signup_date": "2023-06-15",
            "preferences": ["electronics", "books"],
            "address": {
                "street": "123 Main St",
                "city": "Seattle",
                "state": "WA",
                "zip": "98101"
            },
            "loyalty_points": 1250,
            "last_login": "2024-01-14T15:30:00"
        },
        {
            "customer_id": 12346,
            "name": "Bob Smith",
            "email": "bob@example.com",
            "age": 35,
            "is_premium": False,
            "signup_date": "2023-08-22",
            "preferences": ["clothing", "sports"],
            "address": {
                "street": "456 Oak Ave",
                "city": "Portland",
                "state": "OR",
                "zip": "97201"
            },
            "loyalty_points": 450,
            "last_login": None  # Example of null value
        },
        {
            "customer_id": 12347,
            "name": "Carol Davis",
            "email": "carol@example.com",
            "age": 42,
            "is_premium": True,
            "signup_date": "2023-03-10",
            "preferences": ["electronics", "home", "books"],
            "address": {
                "street": "789 Pine St",
                "city": "San Francisco",
                "state": "CA",
                "zip": "94102"
            },
            "loyalty_points": 2100,
            "last_login": "2024-01-15T09:15:00"
        },
        {
            "customer_id": 12348,
            "name": "David Wilson",
            "email": "david@example.com",
            "age": 29,
            "is_premium": False,
            "signup_date": "2023-11-05",
            "preferences": ["sports", "electronics"],
            "address": {
                "street": "321 Elm Dr",
                "city": "Denver",
                "state": "CO",
                "zip": "80202"
            },
            "loyalty_points": 680,
            "last_login": "2024-01-13T20:45:00"
        },
        {
            "customer_id": 12349,
            "name": "Eva Martinez",
            "email": "eva@example.com",
            "age": 26,
            "is_premium": False,
            "signup_date": "2024-01-01",
            "preferences": ["books", "clothing"],
            "address": {
                "street": "654 Maple Ln",
                "city": "Austin",
                "state": "TX",
                "zip": "73301"
            },
            "loyalty_points": 150,
            "last_login": "2024-01-15T11:20:00"
        }
    ]

    customer_data = {
        "customers": customers,
        "metadata": {
            "total_customers": len(customers),
            "load_timestamp": datetime.now().isoformat(),
            "data_source": "customer_db",
            "schema_version": "2.1",
            "premium_count": sum(1 for c in customers if c['is_premium']),
            "avg_age": round(sum(c['age'] for c in customers) / len(customers), 1),
            "states_represented": list(set(c['address']['state'] for c in customers))
        }
    }

    print(f"Loaded {len(customers)} customers with mixed data types")
    print(f"Premium customers: {customer_data['metadata']['premium_count']}")

    return customer_data


def analyze_purchases(**context):
    """Analyze purchases and push multiple data types with different keys"""
    ti = context['task_instance']

    # Pull customer data
    customer_data = ti.xcom_pull(task_ids='load_customer_data')
    if not customer_data:
        raise ValueError("No customer data received")

    customers = customer_data['customers']

    # Generate realistic purchase data for each customer
    all_transactions = []
    category_totals = {}
    total_revenue = 0

    categories = ["electronics", "books", "clothing", "sports", "home"]

    for customer in customers:
        customer_transactions = []
        customer_total = 0

        # Generate 2-8 transactions per customer
        num_transactions = random.randint(2, 8)

        for _ in range(num_transactions):
            category = random.choice(customer.get('preferences', categories))
            amount = round(random.uniform(25.99, 299.99), 2)
            date = f"2024-01-{random.randint(1, 15):02d}"

            transaction = {
                "date": date,
                "amount": amount,
                "category": category
            }

            customer_transactions.append(transaction)
            customer_total += amount
            total_revenue += amount

            # Update category totals
            if category not in category_totals:
                category_totals[category] = {"count": 0, "revenue": 0}
            category_totals[category]["count"] += 1
            category_totals[category]["revenue"] += amount

        all_transactions.append({
            "customer_id": customer['customer_id'],
            "transactions": customer_transactions,
            "customer_total": round(customer_total, 2)
        })

    # Calculate summary statistics
    total_transactions = sum(len(ct['transactions'])
                             for ct in all_transactions)
    avg_transaction_value = round(total_revenue / total_transactions, 2)

    # Round category totals
    for category in category_totals:
        category_totals[category]["revenue"] = round(
            category_totals[category]["revenue"], 2)

    # 1. Push purchase summary
    purchase_summary = {
        "total_transactions": total_transactions,
        "total_revenue": round(total_revenue, 2),
        "avg_transaction_value": avg_transaction_value,
        "date_range": {
            "start": "2024-01-01",
            "end": "2024-01-15"
        }
    }
    ti.xcom_push(key='purchase_summary', value=purchase_summary)

    # 2. Push customer transactions
    ti.xcom_push(key='customer_transactions', value=all_transactions)

    # 3. Push category breakdown
    ti.xcom_push(key='category_breakdown', value=category_totals)

    print(
        f"Analyzed {total_transactions} transactions across {len(customers)} customers")
    print(f"Total revenue: ${total_revenue:.2f}")

    # 4. Return analysis metadata
    return {
        "analysis_completed": True,
        "processing_time_seconds": 2.5,
        "customers_analyzed": len(customers),
        "analysis_timestamp": datetime.now().isoformat(),
        "categories_found": list(category_totals.keys())
    }


def calculate_segments(**context):
    """Calculate customer segments based on purchase behavior"""
    ti = context['task_instance']

    # Pull data from multiple XCom keys
    purchase_summary = ti.xcom_pull(
        task_ids='analyze_purchases', key='purchase_summary')
    customer_transactions = ti.xcom_pull(
        task_ids='analyze_purchases', key='customer_transactions')
    category_breakdown = ti.xcom_pull(
        task_ids='analyze_purchases', key='category_breakdown')

    if not all([purchase_summary, customer_transactions, category_breakdown]):
        raise ValueError("Missing required data from analyze_purchases task")

    # Calculate segments based on customer spending
    segments = {
        "high_value": {
            "criteria": {"min_total_spent": 500, "min_transactions": 5},
            "customers": [],
            "count": 0,
            "total_value": 0
        },
        "regular": {
            "criteria": {"min_total_spent": 100, "max_total_spent": 499},
            "customers": [],
            "count": 0,
            "total_value": 0
        },
        "new": {
            "criteria": {"max_transactions": 2},
            "customers": [],
            "count": 0,
            "total_value": 0
        }
    }

    # Classify customers
    for customer_data in customer_transactions:
        customer_id = customer_data['customer_id']
        total_spent = customer_data['customer_total']
        transaction_count = len(customer_data['transactions'])

        if total_spent >= 500 and transaction_count >= 5:
            segment = "high_value"
        elif transaction_count <= 2:
            segment = "new"
        else:
            segment = "regular"

        segments[segment]["customers"].append(customer_id)
        segments[segment]["count"] += 1
        segments[segment]["total_value"] += total_spent

    # Calculate average values
    for segment_name, segment_data in segments.items():
        if segment_data["count"] > 0:
            segment_data["avg_value"] = round(
                segment_data["total_value"] / segment_data["count"], 2)
        else:
            segment_data["avg_value"] = 0

    # Calculate distribution percentages
    total_customers = sum(s["count"] for s in segments.values())
    segment_distribution = []

    for segment_name, segment_data in segments.items():
        percentage = round(
            (segment_data["count"] / total_customers) * 100, 1) if total_customers > 0 else 0
        segment_distribution.append({
            "segment": segment_name,
            "percentage": percentage
        })

    segmentation_result = {
        "segments": segments,
        "segment_distribution": segment_distribution,
        "segmentation_rules": {
            "version": "1.0",
            "last_updated": datetime.now().isoformat(),
            "total_segments": len(segments),
            "total_customers_segmented": total_customers
        }
    }

    print(
        f"Segmented {total_customers} customers into {len(segments)} segments")
    for segment_name, segment_data in segments.items():
        print(
            f"  {segment_name}: {segment_data['count']} customers (avg: ${segment_data['avg_value']})")

    return segmentation_result


def generate_insights(**context):
    """Generate insights combining different data types"""
    ti = context['task_instance']

    # Pull data from all previous tasks
    customer_data = ti.xcom_pull(task_ids='load_customer_data')
    purchase_summary = ti.xcom_pull(
        task_ids='analyze_purchases', key='purchase_summary')
    category_breakdown = ti.xcom_pull(
        task_ids='analyze_purchases', key='category_breakdown')
    segments = ti.xcom_pull(task_ids='calculate_segments')

    if not all([customer_data, purchase_summary, category_breakdown, segments]):
        raise ValueError("Missing required data from previous tasks")

    # Generate insights
    total_customers = customer_data['metadata']['total_customers']
    total_revenue = purchase_summary['total_revenue']
    avg_transaction = purchase_summary['avg_transaction_value']

    # Find top category
    top_category = max(category_breakdown.items(),
                       key=lambda x: x[1]['revenue'])
    top_category_name = top_category[0]
    top_category_revenue = top_category[1]['revenue']
    top_category_percentage = round(
        (top_category_revenue / total_revenue) * 100, 1)

    # High value segment analysis
    high_value_segment = segments['segments']['high_value']
    high_value_percentage = next(
        (s['percentage'] for s in segments['segment_distribution']
         if s['segment'] == 'high_value'),
        0
    )

    # Premium customer analysis
    premium_count = customer_data['metadata']['premium_count']
    premium_percentage = round((premium_count / total_customers) * 100, 1)

    top_insights = [
        f"{high_value_percentage}% of customers are high-value with average spend of ${high_value_segment['avg_value']}",
        f"{top_category_name.title()} category generates {top_category_percentage}% of total revenue",
        f"Premium customers represent {premium_percentage}% of the customer base",
        f"Average transaction value is ${avg_transaction} across all customers"
    ]

    recommendations = [
        f"Focus marketing on {top_category_name} for high-value segment",
        "Develop retention program for regular customers",
        "Create onboarding campaign for new customers",
        "Expand premium program to increase customer lifetime value"
    ]

    # Calculate metrics
    customer_lifetime_value = round(total_revenue / total_customers, 2)
    churn_risk_score = 0.15  # Simulated metric
    growth_potential = 0.85  # Simulated metric

    # Generate text report
    report_text = f"""Customer Analytics Report
========================
Generated: {datetime.now().isoformat()}

Customer Overview:
- Total Customers: {total_customers}
- Premium Customers: {premium_count} ({premium_percentage}%)
- Average Age: {customer_data['metadata']['avg_age']} years

Purchase Analysis:
- Total Revenue: ${total_revenue}
- Total Transactions: {purchase_summary['total_transactions']}
- Average Transaction: ${avg_transaction}

Top Category: {top_category_name.title()}
- Revenue: ${top_category_revenue} ({top_category_percentage}%)
- Transactions: {top_category[1]['count']}

Customer Segments:
"""

    for segment_name, segment_data in segments['segments'].items():
        percentage = next(
            (s['percentage'] for s in segments['segment_distribution']
             if s['segment'] == segment_name),
            0
        )
        report_text += f"- {segment_name.title()}: {segment_data['count']} customers ({percentage}%) - Avg: ${segment_data['avg_value']}\n"

    report_text += f"\nKey Insights:\n"
    for i, insight in enumerate(top_insights, 1):
        report_text += f"{i}. {insight}\n"

    insights_result = {
        "insights": {
            "top_insights": top_insights,
            "recommendations": recommendations
        },
        "metrics": {
            "customer_lifetime_value": customer_lifetime_value,
            "churn_risk_score": churn_risk_score,
            "growth_potential": growth_potential
        },
        "report_text": report_text,
        "generated_at": datetime.now().isoformat(),
        "data_sources": ["customer_data", "purchase_analysis", "segmentation"]
    }

    print(
        f"Generated {len(top_insights)} insights and {len(recommendations)} recommendations")

    return insights_result


# Define tasks
load_data_task = PythonOperator(
    task_id='load_customer_data',
    python_callable=load_customer_data,
    dag=dag
)

analyze_task = PythonOperator(
    task_id='analyze_purchases',
    python_callable=analyze_purchases,
    dag=dag
)

segment_task = PythonOperator(
    task_id='calculate_segments',
    python_callable=calculate_segments,
    dag=dag
)

insights_task = PythonOperator(
    task_id='generate_insights',
    python_callable=generate_insights,
    dag=dag
)

# Export task using XCom templating in BashOperator
export_task = BashOperator(
    task_id='export_results',
    bash_command='''
    echo "=== Customer Analytics Export ==="
    echo "Total Customers: {{ ti.xcom_pull(task_ids='load_customer_data')['metadata']['total_customers'] }}"
    echo "Premium Customers: {{ ti.xcom_pull(task_ids='load_customer_data')['metadata']['premium_count'] }}"
    echo "Total Revenue: ${{ ti.xcom_pull(task_ids='analyze_purchases', key='purchase_summary')['total_revenue'] }}"
    echo "Average Transaction: ${{ ti.xcom_pull(task_ids='analyze_purchases', key='purchase_summary')['avg_transaction_value'] }}"
    echo "High Value Customers: {{ ti.xcom_pull(task_ids='calculate_segments')['segments']['high_value']['count'] }}"
    echo "Customer Lifetime Value: ${{ ti.xcom_pull(task_ids='generate_insights')['metrics']['customer_lifetime_value'] }}"
    echo ""
    echo "Top Insight: {{ ti.xcom_pull(task_ids='generate_insights')['insights']['top_insights'][0] }}"
    echo "Top Recommendation: {{ ti.xcom_pull(task_ids='generate_insights')['insights']['recommendations'][0] }}"
    echo ""
    echo "Export completed at: $(date)"
    echo "=== End Export ==="
    ''',
    dag=dag
)

# Set up task dependencies
load_data_task >> analyze_task >> segment_task >> insights_task >> export_task
