"""
Solution for Exercise 1: TaskGroup Organization

This solution demonstrates how to organize complex workflows using TaskGroups
for better readability and maintainability in an e-commerce data processing pipeline.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

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
    'ecommerce_data_processing',
    default_args=default_args,
    description='E-commerce data processing with TaskGroups',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['exercise', 'taskgroups', 'ecommerce']
)

# Callback functions for error handling


def task_success_callback(context):
    """Callback for successful task completion"""
    task_id = context['task_instance'].task_id
    print(f"✓ Task {task_id} completed successfully")


def task_failure_callback(context):
    """Callback for task failure"""
    task_id = context['task_instance'].task_id
    print(f"✗ Task {task_id} failed - initiating recovery procedures")


def validation_failure_callback(context):
    """Special callback for validation failures"""
    task_id = context['task_instance'].task_id
    print(
        f"⚠ Data validation failed in {task_id} - alerting data quality team")

# Data processing functions


def extract_customer_data(**context):
    """Extract customer information from CRM system"""
    print("Extracting customer data from CRM system...")
    print("- Connecting to customer database")
    print("- Querying customer records")
    print("- Applying data privacy filters")
    print("✓ Customer data extraction completed")
    return "customer_data_extracted"


def validate_customer_data(**context):
    """Validate customer data quality"""
    print("Validating customer data quality...")
    print("- Checking for missing required fields")
    print("- Validating email formats")
    print("- Checking for duplicate records")
    print("✓ Customer data validation passed")
    return "customer_data_validated"


def transform_customer_data(**context):
    """Apply customer-specific transformations"""
    print("Transforming customer data...")
    print("- Standardizing address formats")
    print("- Calculating customer lifetime value")
    print("- Applying segmentation rules")
    print("✓ Customer data transformation completed")
    return "customer_data_transformed"


def load_customer_data(**context):
    """Load processed customer data"""
    print("Loading customer data to data warehouse...")
    print("- Creating staging tables")
    print("- Performing upsert operations")
    print("- Updating customer dimension table")
    print("✓ Customer data loading completed")
    return "customer_data_loaded"


def extract_order_data(**context):
    """Extract order information from order management system"""
    print("Extracting order data from OMS...")
    print("- Connecting to order database")
    print("- Querying recent orders")
    print("- Including order line items")
    print("✓ Order data extraction completed")
    return "order_data_extracted"


def validate_order_data(**context):
    """Validate order data integrity"""
    print("Validating order data integrity...")
    print("- Checking order totals")
    print("- Validating product references")
    print("- Verifying customer associations")
    print("✓ Order data validation passed")
    return "order_data_validated"


def enrich_order_data(**context):
    """Enrich orders with customer information"""
    print("Enriching order data with customer information...")
    print("- Joining with customer data")
    print("- Adding customer segment information")
    print("- Calculating order profitability")
    print("✓ Order data enrichment completed")
    return "order_data_enriched"


def load_order_data(**context):
    """Load processed order data"""
    print("Loading order data to data warehouse...")
    print("- Creating order fact table entries")
    print("- Updating inventory levels")
    print("- Triggering downstream processes")
    print("✓ Order data loading completed")
    return "order_data_loaded"


def extract_product_data(**context):
    """Extract product catalog from product management system"""
    print("Extracting product data from PIM...")
    print("- Connecting to product database")
    print("- Querying product catalog")
    print("- Including product attributes")
    print("✓ Product data extraction completed")
    return "product_data_extracted"


def validate_product_data(**context):
    """Validate product information"""
    print("Validating product data...")
    print("- Checking required product attributes")
    print("- Validating pricing information")
    print("- Verifying category assignments")
    print("✓ Product data validation passed")
    return "product_data_validated"


def categorize_products(**context):
    """Apply product categorization logic"""
    print("Categorizing products...")
    print("- Applying ML-based categorization")
    print("- Updating product hierarchies")
    print("- Assigning recommendation tags")
    print("✓ Product categorization completed")
    return "products_categorized"


def load_product_data(**context):
    """Load processed product data"""
    print("Loading product data to data warehouse...")
    print("- Updating product dimension table")
    print("- Creating product hierarchy entries")
    print("- Indexing for search optimization")
    print("✓ Product data loading completed")
    return "product_data_loaded"


def generate_customer_report(**context):
    """Generate customer analytics report"""
    print("Generating customer analytics report...")
    print("- Calculating customer metrics")
    print("- Creating customer segments analysis")
    print("- Generating retention insights")
    print("✓ Customer report generated")
    return "customer_report_generated"


def generate_sales_report(**context):
    """Generate sales analytics report"""
    print("Generating sales analytics report...")
    print("- Calculating sales metrics")
    print("- Analyzing sales trends")
    print("- Creating performance dashboards")
    print("✓ Sales report generated")
    return "sales_report_generated"


def generate_inventory_report(**context):
    """Generate inventory analytics report"""
    print("Generating inventory analytics report...")
    print("- Calculating inventory levels")
    print("- Identifying slow-moving products")
    print("- Forecasting inventory needs")
    print("✓ Inventory report generated")
    return "inventory_report_generated"


def consolidate_reports(**context):
    """Combine all reports into executive summary"""
    print("Consolidating all reports...")
    print("- Merging report data")
    print("- Creating executive summary")
    print("- Generating key insights")
    print("✓ Report consolidation completed")
    return "reports_consolidated"


with dag:
    start = DummyOperator(task_id='start')

    # Customer Data Processing TaskGroup
    with TaskGroup("customer_processing") as customer_group:
        extract_customer = PythonOperator(
            task_id='extract_customer_data',
            python_callable=extract_customer_data,
            on_success_callback=task_success_callback,
            on_failure_callback=task_failure_callback
        )

        validate_customer = PythonOperator(
            task_id='validate_customer_data',
            python_callable=validate_customer_data,
            retries=2,  # Extra retries for validation
            on_success_callback=task_success_callback,
            on_failure_callback=validation_failure_callback
        )

        transform_customer = PythonOperator(
            task_id='transform_customer_data',
            python_callable=transform_customer_data,
            on_success_callback=task_success_callback,
            on_failure_callback=task_failure_callback
        )

        load_customer = PythonOperator(
            task_id='load_customer_data',
            python_callable=load_customer_data,
            retries=3,  # Extra retries for loading
            on_success_callback=task_success_callback,
            on_failure_callback=task_failure_callback
        )

        # Define dependencies within customer processing group
        extract_customer >> validate_customer >> transform_customer >> load_customer

    # Order Data Processing TaskGroup
    with TaskGroup("order_processing") as order_group:
        extract_order = PythonOperator(
            task_id='extract_order_data',
            python_callable=extract_order_data,
            on_success_callback=task_success_callback,
            on_failure_callback=task_failure_callback
        )

        validate_order = PythonOperator(
            task_id='validate_order_data',
            python_callable=validate_order_data,
            retries=2,
            on_success_callback=task_success_callback,
            on_failure_callback=validation_failure_callback
        )

        enrich_order = PythonOperator(
            task_id='enrich_order_data',
            python_callable=enrich_order_data,
            on_success_callback=task_success_callback,
            on_failure_callback=task_failure_callback
        )

        load_order = PythonOperator(
            task_id='load_order_data',
            python_callable=load_order_data,
            retries=3,
            on_success_callback=task_success_callback,
            on_failure_callback=task_failure_callback
        )

        # Define dependencies within order processing group
        extract_order >> validate_order >> enrich_order >> load_order

    # Product Data Processing TaskGroup
    with TaskGroup("product_processing") as product_group:
        extract_product = PythonOperator(
            task_id='extract_product_data',
            python_callable=extract_product_data,
            on_success_callback=task_success_callback,
            on_failure_callback=task_failure_callback
        )

        validate_product = PythonOperator(
            task_id='validate_product_data',
            python_callable=validate_product_data,
            retries=2,
            on_success_callback=task_success_callback,
            on_failure_callback=validation_failure_callback
        )

        categorize_product = PythonOperator(
            task_id='categorize_products',
            python_callable=categorize_products,
            on_success_callback=task_success_callback,
            on_failure_callback=task_failure_callback
        )

        load_product = PythonOperator(
            task_id='load_product_data',
            python_callable=load_product_data,
            retries=3,
            on_success_callback=task_success_callback,
            on_failure_callback=task_failure_callback
        )

        # Define dependencies within product processing group
        extract_product >> validate_product >> categorize_product >> load_product

    # Reporting TaskGroup
    with TaskGroup("reporting") as reporting_group:
        customer_report = PythonOperator(
            task_id='generate_customer_report',
            python_callable=generate_customer_report,
            on_success_callback=task_success_callback,
            on_failure_callback=task_failure_callback
        )

        sales_report = PythonOperator(
            task_id='generate_sales_report',
            python_callable=generate_sales_report,
            on_success_callback=task_success_callback,
            on_failure_callback=task_failure_callback
        )

        inventory_report = PythonOperator(
            task_id='generate_inventory_report',
            python_callable=generate_inventory_report,
            on_success_callback=task_success_callback,
            on_failure_callback=task_failure_callback
        )

        consolidate = PythonOperator(
            task_id='consolidate_reports',
            python_callable=consolidate_reports,
            on_success_callback=task_success_callback,
            on_failure_callback=task_failure_callback
        )

        send_notification = BashOperator(
            task_id='send_report_notification',
            bash_command='echo "📧 Sending report completion notification to stakeholders"',
            on_success_callback=task_success_callback,
            on_failure_callback=task_failure_callback
        )

        # Define dependencies within reporting group
        # Reports can be generated in parallel, then consolidated
        [customer_report, sales_report,
            inventory_report] >> consolidate >> send_notification

    end = DummyOperator(task_id='end')

    # Define overall workflow dependencies
    # All processing groups run in parallel, then reporting runs after all complete
    start >> [customer_group, order_group,
              product_group] >> reporting_group >> end

# Additional TaskGroup example with nested groups (Bonus Challenge)
dag_nested = DAG(
    'ecommerce_nested_taskgroups',
    default_args=default_args,
    description='E-commerce processing with nested TaskGroups',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['exercise', 'taskgroups', 'nested', 'bonus']
)

with dag_nested:
    start_nested = DummyOperator(task_id='start')

    # Main processing group with nested sub-groups
    with TaskGroup("data_processing") as main_processing:

        # Source systems group
        with TaskGroup("source_systems") as sources_group:
            extract_crm = BashOperator(
                task_id='extract_from_crm',
                bash_command='echo "Extracting from CRM system"'
            )
            extract_oms = BashOperator(
                task_id='extract_from_oms',
                bash_command='echo "Extracting from Order Management System"'
            )
            extract_pim = BashOperator(
                task_id='extract_from_pim',
                bash_command='echo "Extracting from Product Information Management"'
            )

            [extract_crm, extract_oms, extract_pim]

        # Data quality group
        with TaskGroup("data_quality") as quality_group:
            schema_validation = PythonOperator(
                task_id='schema_validation',
                python_callable=lambda: print("Validating data schemas")
            )
            data_profiling = PythonOperator(
                task_id='data_profiling',
                python_callable=lambda: print("Profiling data quality")
            )
            anomaly_detection = PythonOperator(
                task_id='anomaly_detection',
                python_callable=lambda: print("Detecting data anomalies")
            )

            schema_validation >> [data_profiling, anomaly_detection]

        # Transformation group
        with TaskGroup("transformations") as transform_group:
            clean_data = PythonOperator(
                task_id='clean_data',
                python_callable=lambda: print("Cleaning data")
            )
            normalize_data = PythonOperator(
                task_id='normalize_data',
                python_callable=lambda: print("Normalizing data")
            )
            enrich_data = PythonOperator(
                task_id='enrich_data',
                python_callable=lambda: print("Enriching data")
            )

            clean_data >> normalize_data >> enrich_data

        # Define dependencies between nested groups
        sources_group >> quality_group >> transform_group

    # Reporting group with nested sub-groups
    with TaskGroup("advanced_reporting") as advanced_reporting:

        # Analytics group
        with TaskGroup("analytics") as analytics_group:
            customer_analytics = PythonOperator(
                task_id='customer_analytics',
                python_callable=lambda: print("Generating customer analytics")
            )
            sales_analytics = PythonOperator(
                task_id='sales_analytics',
                python_callable=lambda: print("Generating sales analytics")
            )
            product_analytics = PythonOperator(
                task_id='product_analytics',
                python_callable=lambda: print("Generating product analytics")
            )

            [customer_analytics, sales_analytics, product_analytics]

        # Visualization group
        with TaskGroup("visualizations") as viz_group:
            create_dashboards = PythonOperator(
                task_id='create_dashboards',
                python_callable=lambda: print(
                    "Creating interactive dashboards")
            )
            generate_charts = PythonOperator(
                task_id='generate_charts',
                python_callable=lambda: print("Generating statistical charts")
            )

            [create_dashboards, generate_charts]

        # Distribution group
        with TaskGroup("distribution") as dist_group:
            email_reports = BashOperator(
                task_id='email_reports',
                bash_command='echo "Emailing reports to stakeholders"'
            )
            update_portal = BashOperator(
                task_id='update_portal',
                bash_command='echo "Updating business intelligence portal"'
            )

            [email_reports, update_portal]

        # Define dependencies between reporting sub-groups
        analytics_group >> viz_group >> dist_group

    end_nested = DummyOperator(task_id='end')

    # Define overall nested workflow
    start_nested >> main_processing >> advanced_reporting >> end_nested
