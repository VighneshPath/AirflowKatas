# Exercise 1: Basic XCom Operations

## Learning Objectives

By completing this exercise, you will:

- Implement basic XCom push and pull operations
- Pass different data types between tasks
- Handle XCom data in downstream tasks
- Debug common XCom issues

## Scenario

You're building a data processing pipeline for an e-commerce company. The pipeline needs to:

1. Extract daily order statistics from a simulated database
2. Calculate key performance indicators (KPIs)
3. Generate a summary report
4. Validate the results

Each step needs to pass data to the next step using XComs.

## Your Task

Create a DAG called `daily_order_processing` that implements the following workflow:

```
extract_orders → calculate_kpis → generate_summary → validate_results
```

### Step 1: Extract Orders Task

Create a task called `extract_orders` that:

- Simulates extracting order data from a database
- Returns a dictionary with the following structure:

```python
{
    "total_orders": 245,
    "total_revenue": 12750.50,
    "order_details": [
        {"order_id": 1001, "amount": 89.99, "status": "completed"},
        {"order_id": 1002, "amount": 156.75, "status": "completed"},
        {"order_id": 1003, "amount": 45.00, "status": "pending"},
        # ... more orders (create at least 5)
    ],
    "extraction_timestamp": "2024-01-15T10:30:00"
}
```

**Requirements:**

- Use realistic order data (at least 5 orders)
- Include both completed and pending orders
- Use the current timestamp for extraction_timestamp

### Step 2: Calculate KPIs Task

Create a task called `calculate_kpis` that:

- Pulls the order data from the previous task
- Calculates the following KPIs:
  - Average order value
  - Completion rate (completed orders / total orders)
  - Revenue from completed orders only
  - Number of pending orders
- Returns the KPIs as a dictionary

**Expected output structure:**

```python
{
    "avg_order_value": 52.04,
    "completion_rate": 0.80,
    "completed_revenue": 10200.40,
    "pending_orders": 49,
    "calculation_timestamp": "2024-01-15T10:31:00"
}
```

### Step 3: Generate Summary Task

Create a task called `generate_summary` that:

- Pulls data from both `extract_orders` and `calculate_kpis` tasks
- Creates a formatted summary report
- Returns a dictionary with the report and metadata

**Expected output structure:**

```python
{
    "report": "Daily Order Summary\n==================\nTotal Orders: 245\n...",
    "report_length": 156,
    "generated_at": "2024-01-15T10:32:00"
}
```

### Step 4: Validate Results Task

Create a task called `validate_results` that:

- Pulls data from all previous tasks
- Performs validation checks:
  - Total orders > 0
  - Completion rate between 0 and 1
  - Revenue values are positive
  - Report is not empty
- Returns validation status and any errors found

## Implementation Guidelines

### DAG Configuration

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

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
    'daily_order_processing',
    default_args=default_args,
    description='Daily order processing with XCom data passing',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['exercise', 'xcom', 'orders']
)
```

### Task Function Templates

```python
def extract_orders():
    """Extract order data from simulated database"""
    # Your implementation here
    pass

def calculate_kpis(**context):
    """Calculate KPIs from order data"""
    # Pull data from previous task
    order_data = context['task_instance'].xcom_pull(task_ids='extract_orders')

    # Your implementation here
    pass

def generate_summary(**context):
    """Generate summary report"""
    # Pull data from multiple tasks
    order_data = context['task_instance'].xcom_pull(task_ids='extract_orders')
    kpis = context['task_instance'].xcom_pull(task_ids='calculate_kpis')

    # Your implementation here
    pass

def validate_results(**context):
    """Validate all results"""
    ti = context['task_instance']

    # Pull data from all previous tasks
    order_data = ti.xcom_pull(task_ids='extract_orders')
    kpis = ti.xcom_pull(task_ids='calculate_kpis')
    summary = ti.xcom_pull(task_ids='generate_summary')

    # Your implementation here
    pass
```

## Testing Your Implementation

### Test Data Validation

Your extract_orders function should return data that passes these checks:

```python
def test_extract_orders_output(order_data):
    assert isinstance(order_data, dict)
    assert 'total_orders' in order_data
    assert 'total_revenue' in order_data
    assert 'order_details' in order_data
    assert len(order_data['order_details']) >= 5
    assert all('order_id' in order for order in order_data['order_details'])
```

### Manual Testing Steps

1. **Deploy your DAG**: Save the file in the `dags/` directory
2. **Check DAG parsing**: Ensure no syntax errors in the Airflow UI
3. **Trigger a test run**: Run the DAG manually
4. **Inspect XComs**: Use the Airflow UI to view XCom values:
   - Go to DAG → Task Instance → XCom
   - Verify data is being passed correctly between tasks
5. **Check logs**: Review task logs for any errors or warnings

## Common Issues and Solutions

### Issue 1: XCom Returns None

**Problem**: Downstream task receives `None` instead of expected data

**Possible Causes:**

- Upstream task failed or didn't complete
- Wrong task_id in xcom_pull
- Task didn't return any value

**Solution:**

```python
def safe_xcom_pull(**context):
    data = context['task_instance'].xcom_pull(task_ids='upstream_task')
    if data is None:
        raise ValueError("No data received from upstream_task")
    return data
```

### Issue 2: Serialization Errors

**Problem**: "Object of type X is not JSON serializable"

**Solution**: Convert non-serializable objects to basic types:

```python
from datetime import datetime

# Instead of this:
return {"timestamp": datetime.now()}  # ❌

# Do this:
return {"timestamp": datetime.now().isoformat()}  # ✅
```

### Issue 3: Large Data Performance

**Problem**: Tasks are slow due to large XCom data

**Solution**: Keep XComs small and use references:

```python
# Instead of passing large data:
return large_dataset  # ❌

# Pass a summary and save data elsewhere:
save_to_file(large_dataset, '/tmp/data.json')
return {"data_location": "/tmp/data.json", "record_count": len(large_dataset)}  # ✅
```

## Success Criteria

Your implementation is successful when:

1. ✅ All tasks complete without errors
2. ✅ Each task receives expected data from upstream tasks
3. ✅ XCom data follows the specified structure
4. ✅ Validation task passes all checks
5. ✅ DAG can be triggered multiple times successfully

## Extension Challenges

Once you complete the basic exercise, try these extensions:

### Challenge 1: Add Error Handling

Modify your tasks to handle missing or invalid data gracefully:

```python
def robust_calculate_kpis(**context):
    order_data = context['task_instance'].xcom_pull(task_ids='extract_orders')

    if not order_data or 'order_details' not in order_data:
        return {"error": "Invalid order data", "kpis": None}

    # Continue with calculations...
```

### Challenge 2: Add Data Quality Checks

Enhance the validation task to check:

- Data freshness (extraction timestamp within last hour)
- Data completeness (no missing required fields)
- Data consistency (totals match detail sums)

### Challenge 3: Multiple XCom Keys

Modify the calculate_kpis task to push multiple values with different keys:

```python
def calculate_kpis_multi(**context):
    # ... calculations ...

    ti = context['task_instance']
    ti.xcom_push(key='avg_order_value', value=avg_value)
    ti.xcom_push(key='completion_rate', value=completion_rate)
    ti.xcom_push(key='revenue_metrics', value=revenue_data)

    return {"calculation_status": "completed"}
```

## Next Steps

After completing this exercise:

1. Review the solution file to compare approaches
2. Experiment with different data types in XComs
3. Try using XCom templating in BashOperator tasks
4. Move on to Exercise 2 for more advanced XCom patterns

## File Location

Save your implementation as: `dags/exercise_1_basic_xcoms.py`

---

**Estimated Time**: 30-35 minutes

**Difficulty**: Beginner

**Key Concepts**: XCom push/pull, data serialization, task dependencies, error handling
