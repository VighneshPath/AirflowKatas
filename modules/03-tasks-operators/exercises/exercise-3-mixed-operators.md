# Exercise 3: Mixed Operator Workflows

## Objective

Combine different operator types in a single workflow to create more complex and realistic data pipelines.

## Prerequisites

- Completed Exercise 1: BashOperator Fundamentals
- Completed Exercise 2: PythonOperator Fundamentals

## Exercise Tasks

### Task 1: Data Pipeline Workflow

Create a DAG named `mixed_operators_pipeline` that simulates a complete data processing pipeline:

**Requirements:**

- DAG ID: `mixed_operators_pipeline`
- Schedule: Daily at 6 AM
- Start date: January 1, 2024
- No catchup
- Tags: `['exercise', 'mixed', 'pipeline']`

**Pipeline Tasks:**

1. **setup_environment** (BashOperator)

   - Create necessary directories: `/tmp/data_pipeline/raw`, `/tmp/data_pipeline/processed`
   - Set appropriate permissions
   - Log the setup completion

2. **generate_sample_data** (PythonOperator)

   - Generate sample CSV data (customers, orders, products)
   - Save files to `/tmp/data_pipeline/raw/`
   - Return summary of generated data

3. **validate_data_files** (BashOperator)

   - Check if all expected files exist
   - Verify file sizes are greater than 0
   - Count lines in each file
   - Exit with error if validation fails

4. **process_customer_data** (PythonOperator)

   - Read customer CSV file
   - Clean and validate data (remove duplicates, validate emails)
   - Save processed data to `/tmp/data_pipeline/processed/`
   - Return processing statistics

5. **process_order_data** (PythonOperator)

   - Read order CSV file
   - Calculate order totals and statistics
   - Join with customer data
   - Save enriched data
   - Return processing results

6. **generate_report** (BashOperator)

   - Use bash commands to create a summary report
   - Count records in processed files
   - Calculate file sizes
   - Create a final report file with timestamp

7. **cleanup_temp_files** (BashOperator)
   - Remove temporary raw data files
   - Keep processed files and reports
   - Log cleanup actions

### Task 2: Monitoring and Alerting Workflow

Add monitoring tasks to your pipeline:

8. **check_disk_space** (BashOperator)

   - Check available disk space
   - Alert if space is below threshold (simulate with random check)
   - Log disk usage statistics

9. **validate_pipeline_results** (PythonOperator)

   - Verify all expected output files exist
   - Check data quality metrics
   - Generate validation report
   - Return success/failure status

10. **send_notification** (PythonOperator)
    - Simulate sending completion notification
    - Include pipeline statistics and status
    - Log notification details

### Task 3: Error Handling and Recovery

11. **backup_previous_run** (BashOperator)

    - Create backup of previous pipeline results
    - Use timestamped directories
    - Handle case where no previous run exists

12. **recovery_check** (PythonOperator)
    - Check if pipeline can recover from previous failures
    - Validate backup integrity
    - Return recovery status

**Task Dependencies:**

```
backup_previous_run >> setup_environment >> generate_sample_data
generate_sample_data >> validate_data_files
validate_data_files >> [process_customer_data, process_order_data]
[process_customer_data, process_order_data] >> generate_report
generate_report >> [check_disk_space, validate_pipeline_results]
[check_disk_space, validate_pipeline_results] >> send_notification
send_notification >> cleanup_temp_files
recovery_check >> setup_environment
```

## Implementation Guidelines

### Python Functions

```python
import csv
import json
import os
import random
from datetime import datetime

def generate_sample_data():
    """Generate sample CSV files for the pipeline"""
    # Create customer data
    customers = []
    for i in range(100):
        customer = {
            'customer_id': i + 1,
            'name': f'Customer_{i + 1}',
            'email': f'customer{i + 1}@example.com',
            'city': random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston']),
            'signup_date': f'2024-01-{random.randint(1, 28):02d}'
        }
        customers.append(customer)

    # Save customer data
    with open('/tmp/data_pipeline/raw/customers.csv', 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=customers[0].keys())
        writer.writeheader()
        writer.writerows(customers)

    # Create order data
    orders = []
    for i in range(200):
        order = {
            'order_id': i + 1,
            'customer_id': random.randint(1, 100),
            'product': f'Product_{random.randint(1, 20)}',
            'quantity': random.randint(1, 5),
            'price': round(random.uniform(10, 100), 2),
            'order_date': f'2024-01-{random.randint(1, 28):02d}'
        }
        orders.append(order)

    # Save order data
    with open('/tmp/data_pipeline/raw/orders.csv', 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=orders[0].keys())
        writer.writeheader()
        writer.writerows(orders)

    return {
        'customers_generated': len(customers),
        'orders_generated': len(orders),
        'files_created': ['customers.csv', 'orders.csv']
    }

def process_customer_data():
    """Process and clean customer data"""
    # Your implementation here
    pass

def process_order_data():
    """Process order data and calculate statistics"""
    # Your implementation here
    pass

def validate_pipeline_results():
    """Validate the pipeline execution results"""
    # Your implementation here
    pass

def send_notification():
    """Send pipeline completion notification"""
    # Your implementation here
    pass

def recovery_check():
    """Check if pipeline can recover from failures"""
    # Your implementation here
    pass
```

### Bash Commands

```bash
# Setup environment
mkdir -p /tmp/data_pipeline/raw /tmp/data_pipeline/processed /tmp/data_pipeline/backup
echo "Environment setup completed at $(date)"

# Validate data files
if [ ! -f "/tmp/data_pipeline/raw/customers.csv" ]; then
    echo "Error: customers.csv not found"
    exit 1
fi

# Generate report
echo "=== Pipeline Report ===" > /tmp/data_pipeline/report.txt
echo "Generated at: $(date)" >> /tmp/data_pipeline/report.txt
echo "Customer records: $(wc -l < /tmp/data_pipeline/processed/customers_clean.csv)" >> /tmp/data_pipeline/report.txt

# Cleanup
rm -f /tmp/data_pipeline/raw/*.csv
echo "Cleanup completed at $(date)"
```

## Advanced Features

### Task Configuration Examples

```python
# Task with custom retry configuration
process_customer_data_task = PythonOperator(
    task_id='process_customer_data',
    python_callable=process_customer_data,
    retries=2,
    retry_delay=timedelta(minutes=1),
    dag=dag,
)

# Bash task with environment variables
setup_environment_task = BashOperator(
    task_id='setup_environment',
    bash_command='''
    export PIPELINE_NAME="Mixed Operators Pipeline"
    export RUN_DATE="{{ ds }}"
    mkdir -p /tmp/data_pipeline/{raw,processed,backup}
    echo "Setup completed for $PIPELINE_NAME on $RUN_DATE"
    ''',
    dag=dag,
)
```

### Error Handling Patterns

```python
def robust_function():
    """Function with comprehensive error handling"""
    try:
        # Main processing logic
        result = perform_processing()
        return {'status': 'success', 'result': result}
    except FileNotFoundError as e:
        print(f"File not found: {e}")
        return {'status': 'error', 'error_type': 'file_not_found'}
    except Exception as e:
        print(f"Unexpected error: {e}")
        return {'status': 'error', 'error_type': 'unexpected'}
```

## Validation Checklist

- [ ] All tasks are properly defined with correct operators
- [ ] Task dependencies are set up correctly
- [ ] Python functions handle errors gracefully
- [ ] Bash commands include proper error checking
- [ ] Files are created and cleaned up appropriately
- [ ] Pipeline produces expected outputs
- [ ] DAG parses without errors in Airflow UI

## Expected Outputs

Your pipeline should produce:

1. Processed CSV files in `/tmp/data_pipeline/processed/`
2. A summary report in `/tmp/data_pipeline/report.txt`
3. Proper logging throughout the pipeline
4. Clean error handling and recovery

## Bonus Challenges

1. **Dynamic File Processing**: Handle variable numbers of input files
2. **Configuration Management**: Use Airflow Variables for configuration
3. **Data Quality Checks**: Implement comprehensive data validation
4. **Performance Monitoring**: Add timing and performance metrics
5. **Parallel Processing**: Process multiple files simultaneously

## Next Steps

After completing this exercise, you're ready to move on to Module 4: Scheduling & Dependencies, where you'll learn about more complex scheduling patterns and dependency management.
