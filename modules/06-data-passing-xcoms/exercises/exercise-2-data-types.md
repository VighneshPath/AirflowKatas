# Exercise 2: Working with Different Data Types in XComs

## Learning Objectives

By completing this exercise, you will:

- Work with various data types in XComs (strings, numbers, lists, dictionaries)
- Handle serialization and deserialization of complex data structures
- Use multiple XCom keys from a single task
- Implement XCom templating in different operator types
- Debug serialization issues with custom objects

## Scenario

You're building a customer analytics pipeline that processes different types of data:

1. **Customer Demographics**: Basic information (strings, numbers)
2. **Purchase History**: Lists of transactions with nested data
3. **Behavioral Metrics**: Complex nested dictionaries
4. **Segmentation Results**: Multiple outputs with different data types

The pipeline needs to handle all these data types efficiently using XComs.

## Your Task

Create a DAG called `customer_analytics_pipeline` that demonstrates working with different data types:

```
load_customer_data → analyze_purchases → calculate_segments → generate_insights → export_results
```

### Step 1: Load Customer Data Task

Create a task called `load_customer_data` that returns customer data with mixed data types:

**Expected output structure:**

```python
{
    "customers": [
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
            }
        },
        # ... at least 4 more customers with varied data
    ],
    "metadata": {
        "total_customers": 5,
        "load_timestamp": "2024-01-15T10:00:00",
        "data_source": "customer_db",
        "schema_version": "2.1"
    }
}
```

**Requirements:**

- Include at least 5 customers with realistic data
- Mix of premium and regular customers
- Various ages, preferences, and locations
- Include null/None values for some optional fields

### Step 2: Analyze Purchases Task

Create a task called `analyze_purchases` that:

- Pulls customer data from the previous task
- Generates purchase history for each customer
- Uses multiple XCom keys to push different types of analysis results

**Push the following data with specific keys:**

1. **Key: 'purchase_summary'** (dictionary):

```python
{
    "total_transactions": 45,
    "total_revenue": 12450.75,
    "avg_transaction_value": 276.68,
    "date_range": {
        "start": "2024-01-01",
        "end": "2024-01-15"
    }
}
```

2. **Key: 'customer_transactions'** (list of dictionaries):

```python
[
    {
        "customer_id": 12345,
        "transactions": [
            {"date": "2024-01-10", "amount": 89.99, "category": "electronics"},
            {"date": "2024-01-12", "amount": 45.50, "category": "books"}
        ],
        "customer_total": 135.49
    },
    # ... for each customer
]
```

3. **Key: 'category_breakdown'** (nested dictionary):

```python
{
    "electronics": {"count": 15, "revenue": 4500.25},
    "books": {"count": 12, "revenue": 890.50},
    "clothing": {"count": 18, "revenue": 7060.00}
}
```

4. **Return value** (analysis metadata):

```python
{
    "analysis_completed": True,
    "processing_time_seconds": 2.5,
    "customers_analyzed": 5,
    "analysis_timestamp": "2024-01-15T10:01:00"
}
```

### Step 3: Calculate Segments Task

Create a task called `calculate_segments` that:

- Pulls data from multiple XCom keys
- Calculates customer segments based on purchase behavior
- Returns segmentation results with complex nested data

**Expected output structure:**

```python
{
    "segments": {
        "high_value": {
            "criteria": {"min_total_spent": 500, "min_transactions": 5},
            "customers": [12345, 12347],
            "count": 2,
            "avg_value": 750.25
        },
        "regular": {
            "criteria": {"min_total_spent": 100, "max_total_spent": 499},
            "customers": [12346, 12348],
            "count": 2,
            "avg_value": 245.50
        },
        "new": {
            "criteria": {"max_transactions": 2},
            "customers": [12349],
            "count": 1,
            "avg_value": 89.99
        }
    },
    "segment_distribution": [
        {"segment": "high_value", "percentage": 40.0},
        {"segment": "regular", "percentage": 40.0},
        {"segment": "new", "percentage": 20.0}
    ],
    "segmentation_rules": {
        "version": "1.0",
        "last_updated": "2024-01-15T10:02:00",
        "total_segments": 3
    }
}
```

### Step 4: Generate Insights Task

Create a task called `generate_insights` that:

- Pulls data from all previous tasks
- Generates insights combining different data types
- Creates formatted text reports and structured data

**Expected output structure:**

```python
{
    "insights": {
        "top_insights": [
            "40% of customers are high-value with average spend of $750.25",
            "Electronics category generates 36% of total revenue",
            "Premium customers have 2.3x higher transaction value"
        ],
        "recommendations": [
            "Focus marketing on electronics for high-value segment",
            "Develop retention program for regular customers",
            "Create onboarding campaign for new customers"
        ]
    },
    "metrics": {
        "customer_lifetime_value": 456.78,
        "churn_risk_score": 0.15,
        "growth_potential": 0.85
    },
    "report_text": "Customer Analytics Report\n========================\n...",
    "generated_at": "2024-01-15T10:03:00"
}
```

### Step 5: Export Results Task

Create a task called `export_results` that:

- Uses XCom templating to access data in task parameters
- Demonstrates different ways to pull XCom data
- Validates all data types were handled correctly

This task should use a BashOperator with XCom templating to display results:

```python
from airflow.operators.bash import BashOperator

export_task = BashOperator(
    task_id='export_results',
    bash_command='''
    echo "=== Customer Analytics Export ==="
    echo "Total Customers: {{ ti.xcom_pull(task_ids='load_customer_data')['metadata']['total_customers'] }}"
    echo "Total Revenue: ${{ ti.xcom_pull(task_ids='analyze_purchases', key='purchase_summary')['total_revenue'] }}"
    echo "High Value Customers: {{ ti.xcom_pull(task_ids='calculate_segments')['segments']['high_value']['count'] }}"
    echo "Top Insight: {{ ti.xcom_pull(task_ids='generate_insights')['insights']['top_insights'][0] }}"
    echo "Export completed at: $(date)"
    ''',
    dag=dag
)
```

## Implementation Guidelines

### Handling Different Data Types

```python
def demonstrate_data_types():
    """Example of different data types in XComs"""
    return {
        # Primitive types
        "string_value": "Hello World",
        "integer_value": 42,
        "float_value": 3.14159,
        "boolean_value": True,
        "null_value": None,

        # Collection types
        "list_value": [1, 2, "three", {"four": 4}],
        "dict_value": {"nested": {"deeply": {"value": "found"}}},
        "tuple_value": (1, 2, 3),  # Will be converted to list

        # Complex nested structures
        "complex_structure": {
            "users": [
                {"id": 1, "active": True, "scores": [85, 92, 78]},
                {"id": 2, "active": False, "scores": [90, 88, 95]}
            ],
            "metadata": {
                "version": 1.0,
                "tags": ["production", "validated"],
                "config": {"timeout": 30, "retries": 3}
            }
        }
    }
```

### Multiple XCom Keys Pattern

```python
def push_multiple_values(**context):
    """Push multiple values with different keys"""
    ti = context['task_instance']

    # Calculate different metrics
    summary_data = {"total": 100, "average": 25.5}
    detail_data = [{"id": 1, "value": 10}, {"id": 2, "value": 20}]
    metadata = {"processed_at": datetime.now().isoformat()}

    # Push with different keys
    ti.xcom_push(key='summary', value=summary_data)
    ti.xcom_push(key='details', value=detail_data)
    ti.xcom_push(key='metadata', value=metadata)

    # Also return a value (stored with key 'return_value')
    return {"status": "completed", "keys_pushed": 3}
```

### Pulling Multiple XCom Values

```python
def pull_multiple_values(**context):
    """Pull multiple values from different keys"""
    ti = context['task_instance']

    # Pull from different keys
    summary = ti.xcom_pull(task_ids='previous_task', key='summary')
    details = ti.xcom_pull(task_ids='previous_task', key='details')
    metadata = ti.xcom_pull(task_ids='previous_task', key='metadata')

    # Pull return value (default key)
    status = ti.xcom_pull(task_ids='previous_task')

    # Use the data
    print(f"Status: {status}")
    print(f"Summary: {summary}")
    print(f"Details count: {len(details)}")
```

## Testing Your Implementation

### Data Type Validation

Test that your data structures are properly serializable:

```python
import json

def test_serialization(data):
    """Test if data can be JSON serialized"""
    try:
        json_str = json.dumps(data)
        restored = json.loads(json_str)
        print("✅ Serialization test passed")
        return True
    except (TypeError, ValueError) as e:
        print(f"❌ Serialization test failed: {e}")
        return False
```

### XCom Key Testing

Verify that multiple XCom keys work correctly:

```python
def test_xcom_keys(**context):
    """Test multiple XCom key functionality"""
    ti = context['task_instance']

    # Test pulling with different keys
    keys_to_test = ['purchase_summary', 'customer_transactions', 'category_breakdown']

    for key in keys_to_test:
        data = ti.xcom_pull(task_ids='analyze_purchases', key=key)
        if data is None:
            print(f"❌ Missing data for key: {key}")
        else:
            print(f"✅ Found data for key: {key} (type: {type(data)})")
```

## Common Serialization Issues

### Issue 1: DateTime Objects

```python
# ❌ This will fail
from datetime import datetime
return {"timestamp": datetime.now()}

# ✅ Convert to string
return {"timestamp": datetime.now().isoformat()}
```

### Issue 2: Custom Objects

```python
# ❌ This will fail
class Customer:
    def __init__(self, name):
        self.name = name

return {"customer": Customer("Alice")}

# ✅ Convert to dictionary
class Customer:
    def __init__(self, name):
        self.name = name

    def to_dict(self):
        return {"name": self.name}

customer = Customer("Alice")
return {"customer": customer.to_dict()}
```

### Issue 3: Sets and Tuples

```python
# ❌ Sets are not JSON serializable
return {"unique_ids": {1, 2, 3}}

# ✅ Convert to list
return {"unique_ids": list({1, 2, 3})}

# Note: Tuples are automatically converted to lists
return {"coordinates": (10, 20)}  # Becomes [10, 20]
```

## Success Criteria

Your implementation is successful when:

1. ✅ All tasks complete without serialization errors
2. ✅ Multiple XCom keys are used correctly
3. ✅ Complex nested data structures are handled properly
4. ✅ XCom templating works in BashOperator
5. ✅ All data types are preserved through serialization/deserialization
6. ✅ Tasks can pull data from multiple sources and keys

## Extension Challenges

### Challenge 1: Custom Serialization

Implement a helper function for handling custom objects:

```python
def serialize_custom_object(obj):
    """Convert custom objects to serializable format"""
    if hasattr(obj, 'to_dict'):
        return obj.to_dict()
    elif hasattr(obj, '__dict__'):
        return obj.__dict__
    else:
        return str(obj)
```

### Challenge 2: Data Validation

Add validation for each data type:

```python
def validate_data_types(data):
    """Validate that data contains expected types"""
    validations = {
        'customers': list,
        'metadata': dict,
        'total_customers': int
    }

    for key, expected_type in validations.items():
        if key in data and not isinstance(data[key], expected_type):
            raise TypeError(f"{key} should be {expected_type}, got {type(data[key])}")
```

### Challenge 3: Large Data Handling

Implement a pattern for handling large datasets:

```python
def handle_large_data(large_dataset):
    """Handle large datasets by saving to file and passing reference"""
    import tempfile
    import json

    # Save large data to temporary file
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
        json.dump(large_dataset, f)
        temp_path = f.name

    # Return reference instead of data
    return {
        "data_location": temp_path,
        "record_count": len(large_dataset),
        "data_type": "file_reference"
    }
```

## File Location

Save your implementation as: `dags/exercise_2_data_types.py`

---

**Estimated Time**: 35-40 minutes

**Difficulty**: Intermediate

**Key Concepts**: Data serialization, multiple XCom keys, complex data structures, XCom templating
