# Exercise 1: TaskGroup Organization

## Objective

Learn to organize complex workflows using TaskGroups for better readability and maintainability.

## Background

You're building a data processing pipeline for an e-commerce platform that needs to process customer data, order data, and product data in parallel, then generate consolidated reports. The workflow is becoming complex and hard to read, so you need to organize it using TaskGroups.

## Requirements

Create a DAG called `ecommerce_data_processing` that:

1. **Uses TaskGroups** to organize related tasks
2. **Processes three data sources** in parallel (customers, orders, products)
3. **Includes data validation** within each processing group
4. **Generates reports** after all processing is complete
5. **Implements proper error handling** within TaskGroups

## Detailed Specifications

### Data Processing Groups

Each data source should have its own TaskGroup with the following tasks:

#### Customer Data Processing (`customer_processing`)

- `extract_customer_data`: Extract customer information
- `validate_customer_data`: Validate customer data quality
- `transform_customer_data`: Apply customer-specific transformations
- `load_customer_data`: Load processed customer data

#### Order Data Processing (`order_processing`)

- `extract_order_data`: Extract order information
- `validate_order_data`: Validate order data integrity
- `enrich_order_data`: Enrich orders with customer information
- `load_order_data`: Load processed order data

#### Product Data Processing (`product_processing`)

- `extract_product_data`: Extract product catalog
- `validate_product_data`: Validate product information
- `categorize_products`: Apply product categorization
- `load_product_data`: Load processed product data

### Reporting Group

Create a `reporting` TaskGroup with:

- `generate_customer_report`: Generate customer analytics
- `generate_sales_report`: Generate sales analytics
- `generate_inventory_report`: Generate inventory analytics
- `consolidate_reports`: Combine all reports
- `send_report_notification`: Send completion notification

### Error Handling

- Add retry logic to critical tasks
- Implement failure callbacks for data validation tasks
- Use appropriate trigger rules for report generation

## Implementation Guidelines

1. **Use meaningful TaskGroup IDs** that clearly indicate their purpose
2. **Implement proper dependencies** both within and between TaskGroups
3. **Add logging** to track processing progress
4. **Use PythonOperator** for data processing tasks
5. **Use BashOperator** for system operations like notifications

## Starter Code Structure

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    'owner': 'your-name',
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

# Your implementation here
```

## Expected Workflow Structure

```
start
├── customer_processing
│   ├── extract_customer_data
│   ├── validate_customer_data
│   ├── transform_customer_data
│   └── load_customer_data
├── order_processing
│   ├── extract_order_data
│   ├── validate_order_data
│   ├── enrich_order_data
│   └── load_order_data
├── product_processing
│   ├── extract_product_data
│   ├── validate_product_data
│   ├── categorize_products
│   └── load_product_data
└── reporting
    ├── generate_customer_report
    ├── generate_sales_report
    ├── generate_inventory_report
    ├── consolidate_reports
    └── send_report_notification
end
```

## Testing Your Solution

1. **Verify TaskGroup Structure**: Check that tasks are properly grouped in the Airflow UI
2. **Test Dependencies**: Ensure proper execution order within and between groups
3. **Validate Error Handling**: Test failure scenarios and recovery
4. **Check Logging**: Verify that all tasks produce meaningful logs

## Success Criteria

- [ ] DAG parses without errors
- [ ] All TaskGroups are properly defined and visible in UI
- [ ] Dependencies work correctly within and between groups
- [ ] Error handling is implemented for critical tasks
- [ ] All tasks execute successfully in the correct order
- [ ] Logging provides clear visibility into processing steps

## Bonus Challenges

1. **Nested TaskGroups**: Create sub-groups within the reporting TaskGroup for different report types
2. **Dynamic TaskGroups**: Make the number of processing tasks configurable
3. **Conditional Processing**: Add conditional logic to skip certain processing steps based on data volume
4. **Resource Management**: Add resource requirements to tasks within TaskGroups

## Common Pitfalls to Avoid

- Don't create overly deep nesting (more than 2-3 levels)
- Ensure TaskGroup IDs are unique and descriptive
- Don't forget to define dependencies between TaskGroups
- Avoid circular dependencies within TaskGroups
- Remember that TaskGroups are for organization, not execution control

## Next Steps

After completing this exercise, you'll be ready to tackle dynamic DAG generation and more advanced workflow patterns.
