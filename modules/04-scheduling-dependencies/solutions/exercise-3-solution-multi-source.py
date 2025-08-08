"""
Solution for Exercise 3, Task 2: Multi-Source Data Integration (Fan-In Pattern)

This DAG demonstrates a fan-in pattern where multiple data extraction tasks
feed into a single consolidation task for multi-source data integration.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import random

# DAG configuration
dag = DAG(
    'multi_source_integration',
    description='Multi-source data integration with fan-in pattern',
    schedule_interval='0 */6 * * *',  # Every 6 hours
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        'retries': 3,
        'retry_delay': timedelta(minutes=10),
    },
    tags=['dependencies', 'fan-in', 'integration']
)


def extract_crm_data(**context):
    """Extract customer data from CRM system"""
    execution_date = context['execution_date']

    print(f"=== CRM Data Extraction - {execution_date} ===")
    print("Connecting to CRM system...")
    print("Extracting customer data...")

    # Simulate CRM data extraction
    crm_data = {
        'source': 'CRM',
        'extraction_time': execution_date.isoformat(),
        'customers': [
            {
                'customer_id': f'CRM_{i}',
                'name': f'Customer {i}',
                'email': f'customer{i}@example.com',
                'segment': random.choice(['Enterprise', 'SMB', 'Startup']),
                'lifetime_value': round(random.uniform(1000, 50000), 2),
                'status': random.choice(['Active', 'Inactive', 'Prospect'])
            }
            for i in range(1, 301)  # 300 customers
        ]
    }

    # Data quality validation
    active_customers = [c for c in crm_data['customers']
                        if c['status'] == 'Active']
    avg_lifetime_value = sum(c['lifetime_value']
                             for c in crm_data['customers']) / len(crm_data['customers'])

    print(f"CRM Extraction Results:")
    print(f"  - Total customers extracted: {len(crm_data['customers'])}")
    print(f"  - Active customers: {len(active_customers)}")
    print(f"  - Average lifetime value: ${avg_lifetime_value:,.2f}")
    print(
        f"  - Data quality: {'PASS' if len(crm_data['customers']) > 0 else 'FAIL'}")

    # Add metadata
    crm_data['metadata'] = {
        'record_count': len(crm_data['customers']),
        'active_count': len(active_customers),
        'avg_lifetime_value': avg_lifetime_value,
        'quality_status': 'PASS' if len(crm_data['customers']) > 0 else 'FAIL'
    }

    print("CRM data extraction completed successfully")
    return crm_data


def extract_sales_data(**context):
    """Extract sales transactions from sales database"""
    execution_date = context['execution_date']

    print(f"=== Sales Database Extraction - {execution_date} ===")
    print("Connecting to sales database...")
    print("Extracting transaction data...")

    # Simulate sales data extraction
    sales_data = {
        'source': 'Sales_DB',
        'extraction_time': execution_date.isoformat(),
        'transactions': [
            {
                'transaction_id': f'TXN_{i}',
                'customer_id': f'CRM_{random.randint(1, 300)}',
                'product_id': f'PROD_{random.randint(1, 100)}',
                'amount': round(random.uniform(50, 2000), 2),
                'quantity': random.randint(1, 10),
                'transaction_date': execution_date.date().isoformat(),
                'sales_rep': f'Rep_{random.randint(1, 20)}',
                'region': random.choice(['North', 'South', 'East', 'West'])
            }
            for i in range(1, 501)  # 500 transactions
        ]
    }

    # Data quality validation
    total_revenue = sum(t['amount'] for t in sales_data['transactions'])
    total_quantity = sum(t['quantity'] for t in sales_data['transactions'])
    avg_transaction_value = total_revenue / len(sales_data['transactions'])
    unique_customers = len(set(t['customer_id']
                           for t in sales_data['transactions']))

    print(f"Sales Extraction Results:")
    print(
        f"  - Total transactions extracted: {len(sales_data['transactions'])}")
    print(f"  - Total revenue: ${total_revenue:,.2f}")
    print(f"  - Total quantity sold: {total_quantity:,}")
    print(f"  - Average transaction value: ${avg_transaction_value:.2f}")
    print(f"  - Unique customers: {unique_customers}")
    print(f"  - Data quality: {'PASS' if total_revenue > 0 else 'FAIL'}")

    # Add metadata
    sales_data['metadata'] = {
        'record_count': len(sales_data['transactions']),
        'total_revenue': total_revenue,
        'total_quantity': total_quantity,
        'avg_transaction_value': avg_transaction_value,
        'unique_customers': unique_customers,
        'quality_status': 'PASS' if total_revenue > 0 else 'FAIL'
    }

    print("Sales data extraction completed successfully")
    return sales_data


def extract_marketing_data(**context):
    """Extract campaign data from marketing platform"""
    execution_date = context['execution_date']

    print(f"=== Marketing Platform Extraction - {execution_date} ===")
    print("Connecting to marketing platform...")
    print("Extracting campaign data...")

    # Simulate marketing data extraction
    marketing_data = {
        'source': 'Marketing_Platform',
        'extraction_time': execution_date.isoformat(),
        'campaigns': [
            {
                'campaign_id': f'CAMP_{i}',
                'campaign_name': f'Campaign {i}',
                'channel': random.choice(['Email', 'Social', 'PPC', 'Display']),
                'budget': round(random.uniform(1000, 10000), 2),
                'spend': round(random.uniform(500, 8000), 2),
                'impressions': random.randint(10000, 100000),
                'clicks': random.randint(100, 5000),
                'conversions': random.randint(10, 200),
                'start_date': execution_date.date().isoformat(),
                'status': random.choice(['Active', 'Paused', 'Completed'])
            }
            for i in range(1, 51)  # 50 campaigns
        ]
    }

    # Data quality validation
    total_budget = sum(c['budget'] for c in marketing_data['campaigns'])
    total_spend = sum(c['spend'] for c in marketing_data['campaigns'])
    total_impressions = sum(c['impressions']
                            for c in marketing_data['campaigns'])
    total_clicks = sum(c['clicks'] for c in marketing_data['campaigns'])
    total_conversions = sum(c['conversions']
                            for c in marketing_data['campaigns'])

    # Calculate metrics
    avg_ctr = (total_clicks / total_impressions *
               100) if total_impressions > 0 else 0
    avg_conversion_rate = (total_conversions /
                           total_clicks * 100) if total_clicks > 0 else 0
    active_campaigns = [
        c for c in marketing_data['campaigns'] if c['status'] == 'Active']

    print(f"Marketing Extraction Results:")
    print(f"  - Total campaigns extracted: {len(marketing_data['campaigns'])}")
    print(f"  - Active campaigns: {len(active_campaigns)}")
    print(f"  - Total budget: ${total_budget:,.2f}")
    print(f"  - Total spend: ${total_spend:,.2f}")
    print(f"  - Total impressions: {total_impressions:,}")
    print(f"  - Total clicks: {total_clicks:,}")
    print(f"  - Total conversions: {total_conversions:,}")
    print(f"  - Average CTR: {avg_ctr:.2f}%")
    print(f"  - Average conversion rate: {avg_conversion_rate:.2f}%")
    print(
        f"  - Data quality: {'PASS' if len(marketing_data['campaigns']) > 0 else 'FAIL'}")

    # Add metadata
    marketing_data['metadata'] = {
        'record_count': len(marketing_data['campaigns']),
        'active_campaigns': len(active_campaigns),
        'total_budget': total_budget,
        'total_spend': total_spend,
        'total_impressions': total_impressions,
        'total_clicks': total_clicks,
        'total_conversions': total_conversions,
        'avg_ctr': avg_ctr,
        'avg_conversion_rate': avg_conversion_rate,
        'quality_status': 'PASS' if len(marketing_data['campaigns']) > 0 else 'FAIL'
    }

    print("Marketing data extraction completed successfully")
    return marketing_data


def consolidate_all_data(**context):
    """Consolidate data from all sources into a unified dataset"""
    execution_date = context['execution_date']

    print(f"=== Data Consolidation - {execution_date} ===")
    print("Retrieving data from all source systems...")

    # Get data from all upstream tasks
    crm_data = context['task_instance'].xcom_pull(task_ids='extract_crm_data')
    sales_data = context['task_instance'].xcom_pull(
        task_ids='extract_sales_data')
    marketing_data = context['task_instance'].xcom_pull(
        task_ids='extract_marketing_data')

    print("Validating data quality from all sources...")

    # Validate data availability
    sources_status = {
        'CRM': crm_data is not None and crm_data['metadata']['quality_status'] == 'PASS',
        'Sales': sales_data is not None and sales_data['metadata']['quality_status'] == 'PASS',
        'Marketing': marketing_data is not None and marketing_data['metadata']['quality_status'] == 'PASS'
    }

    print(f"Source validation results: {sources_status}")

    # Handle missing or invalid data
    if not all(sources_status.values()):
        print("⚠ Warning: Some data sources failed validation")
        failed_sources = [source for source,
                          status in sources_status.items() if not status]
        print(f"Failed sources: {failed_sources}")

    # Consolidate data
    print("Consolidating data from all sources...")

    consolidated_data = {
        'consolidation_timestamp': execution_date.isoformat(),
        'source_summary': {
            'crm': {
                'status': sources_status['CRM'],
                'record_count': crm_data['metadata']['record_count'] if crm_data else 0,
                'active_customers': crm_data['metadata']['active_count'] if crm_data else 0
            },
            'sales': {
                'status': sources_status['Sales'],
                'record_count': sales_data['metadata']['record_count'] if sales_data else 0,
                'total_revenue': sales_data['metadata']['total_revenue'] if sales_data else 0
            },
            'marketing': {
                'status': sources_status['Marketing'],
                'record_count': marketing_data['metadata']['record_count'] if marketing_data else 0,
                'active_campaigns': marketing_data['metadata']['active_campaigns'] if marketing_data else 0
            }
        }
    }

    # Cross-source analysis
    if sources_status['CRM'] and sources_status['Sales']:
        # Match customers between CRM and Sales
        crm_customer_ids = set(c['customer_id'] for c in crm_data['customers'])
        sales_customer_ids = set(t['customer_id']
                                 for t in sales_data['transactions'])

        matched_customers = crm_customer_ids.intersection(sales_customer_ids)
        crm_only_customers = crm_customer_ids - sales_customer_ids
        sales_only_customers = sales_customer_ids - crm_customer_ids

        consolidated_data['customer_analysis'] = {
            'total_crm_customers': len(crm_customer_ids),
            'total_sales_customers': len(sales_customer_ids),
            'matched_customers': len(matched_customers),
            'crm_only_customers': len(crm_only_customers),
            'sales_only_customers': len(sales_only_customers),
            'match_rate': len(matched_customers) / len(crm_customer_ids) * 100 if crm_customer_ids else 0
        }

    # Calculate overall metrics
    total_records = sum(
        consolidated_data['source_summary'][source]['record_count']
        for source in consolidated_data['source_summary']
    )

    successful_sources = sum(1 for status in sources_status.values() if status)
    data_completeness = successful_sources / len(sources_status) * 100

    consolidated_data['overall_metrics'] = {
        'total_records_processed': total_records,
        'successful_sources': successful_sources,
        'total_sources': len(sources_status),
        'data_completeness_percentage': data_completeness,
        'consolidation_status': 'SUCCESS' if data_completeness >= 66.7 else 'PARTIAL'
    }

    # Display consolidation results
    print(f"\n{'='*60}")
    print(f"DATA CONSOLIDATION SUMMARY")
    print(f"{'='*60}")
    print(f"Consolidation Time: {execution_date}")
    print(f"Total Records Processed: {total_records:,}")
    print(f"Successful Sources: {successful_sources}/{len(sources_status)}")
    print(f"Data Completeness: {data_completeness:.1f}%")
    print(
        f"Consolidation Status: {consolidated_data['overall_metrics']['consolidation_status']}")
    print()

    print("SOURCE BREAKDOWN:")
    for source, summary in consolidated_data['source_summary'].items():
        status_icon = "✓" if summary['status'] else "✗"
        print(
            f"  {status_icon} {source.upper()}: {summary['record_count']:,} records")

    if 'customer_analysis' in consolidated_data:
        print()
        print("CUSTOMER MATCHING ANALYSIS:")
        ca = consolidated_data['customer_analysis']
        print(f"  - CRM Customers: {ca['total_crm_customers']:,}")
        print(f"  - Sales Customers: {ca['total_sales_customers']:,}")
        print(f"  - Matched Customers: {ca['matched_customers']:,}")
        print(f"  - Match Rate: {ca['match_rate']:.1f}%")

    print(f"{'='*60}")

    print("Data consolidation completed successfully")
    return consolidated_data


# Task definitions
extract_crm = PythonOperator(
    task_id='extract_crm_data',
    python_callable=extract_crm_data,
    dag=dag
)

extract_sales = PythonOperator(
    task_id='extract_sales_data',
    python_callable=extract_sales_data,
    dag=dag
)

extract_marketing = PythonOperator(
    task_id='extract_marketing_data',
    python_callable=extract_marketing_data,
    dag=dag
)

consolidate_data = PythonOperator(
    task_id='consolidate_all_data',
    python_callable=consolidate_all_data,
    dag=dag
)

# Fan-in dependency pattern: multiple tasks feed into one consolidation task
[extract_crm, extract_sales, extract_marketing] >> consolidate_data
