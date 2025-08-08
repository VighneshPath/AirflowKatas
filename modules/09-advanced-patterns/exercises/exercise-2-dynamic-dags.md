# Exercise 2: Dynamic DAG Generation

## Objective

Learn to create DAGs dynamically based on configuration, enabling scalable and maintainable pipeline architectures.

## Background

Your organization has multiple clients, each requiring similar data processing workflows but with different configurations (data sources, processing schedules, output formats). Instead of creating separate DAGs manually for each client, you need to implement dynamic DAG generation.

## Requirements

Create a dynamic DAG generation system that:

1. **Generates client-specific DAGs** from configuration
2. **Supports different processing schedules** per client
3. **Handles varying data sources** and destinations
4. **Implements client-specific business rules**
5. **Maintains code reusability** across all generated DAGs

## Configuration Structure

Create a configuration dictionary with the following structure:

```python
CLIENT_CONFIGS = {
    'acme_corp': {
        'description': 'ACME Corporation data pipeline',
        'schedule_interval': timedelta(hours=6),
        'data_sources': ['crm', 'sales', 'inventory'],
        'transformations': ['clean', 'normalize', 'enrich'],
        'destination': 'data_warehouse',
        'notification_email': 'acme@example.com',
        'business_rules': {
            'data_retention_days': 365,
            'quality_threshold': 0.95,
            'enable_real_time': True
        }
    },
    'startup_inc': {
        'description': 'Startup Inc data pipeline',
        'schedule_interval': timedelta(days=1),
        'data_sources': ['app_events', 'user_data'],
        'transformations': ['validate', 'aggregate'],
        'destination': 'analytics_db',
        'notification_email': 'data@startup.com',
        'business_rules': {
            'data_retention_days': 90,
            'quality_threshold': 0.85,
            'enable_real_time': False
        }
    },
    'enterprise_ltd': {
        'description': 'Enterprise Ltd data pipeline',
        'schedule_interval': timedelta(hours=2),
        'data_sources': ['erp', 'crm', 'hr', 'finance'],
        'transformations': ['clean', 'validate', 'normalize', 'enrich', 'aggregate'],
        'destination': 'enterprise_warehouse',
        'notification_email': 'dataops@enterprise.com',
        'business_rules': {
            'data_retention_days': 2555,  # 7 years
            'quality_threshold': 0.99,
            'enable_real_time': True
        }
    }
}
```

## Implementation Requirements

### 1. DAG Factory Function

Create a function `create_client_dag(client_id, config)` that:

- Takes client ID and configuration as parameters
- Returns a fully configured DAG object
- Implements all required processing steps
- Handles client-specific business rules

### 2. Dynamic Task Creation

Implement dynamic task creation based on configuration:

- **Data Source Tasks**: Create extraction tasks for each data source
- **Transformation Tasks**: Create transformation tasks based on the transformations list
- **Validation Tasks**: Implement quality checks using the quality threshold
- **Loading Tasks**: Create loading tasks for the specified destination

### 3. Business Rules Implementation

Handle client-specific business rules:

- **Data Retention**: Implement cleanup tasks based on retention policy
- **Quality Thresholds**: Create validation tasks with client-specific thresholds
- **Real-time Processing**: Add real-time tasks if enabled

### 4. Error Handling and Notifications

Implement client-specific error handling:

- Send notifications to client-specific email addresses
- Implement retry strategies based on client requirements
- Create client-specific failure callbacks

## Detailed Specifications

### DAG Structure

Each generated DAG should follow this pattern:

```
start
├── [Dynamic Data Source Tasks]
│   ├── extract_[source1]
│   ├── extract_[source2]
│   └── extract_[sourceN]
├── [Dynamic Transformation Tasks]
│   ├── [transformation1]
│   ├── [transformation2]
│   └── [transformationN]
├── quality_validation
├── load_to_[destination]
├── [Conditional Real-time Tasks]
└── cleanup_old_data
end
```

### Task Implementation Guidelines

1. **Extraction Tasks**:

   ```python
   def extract_data(source_name, client_id, **context):
       print(f"Extracting {source_name} data for {client_id}")
       return f"extracted_{source_name}_data"
   ```

2. **Transformation Tasks**:

   ```python
   def apply_transformation(transform_type, client_id, **context):
       print(f"Applying {transform_type} transformation for {client_id}")
       return f"transformed_data_{transform_type}"
   ```

3. **Quality Validation**:
   ```python
   def validate_quality(threshold, client_id, **context):
       # Simulate quality check
       quality_score = 0.96  # Mock score
       if quality_score >= threshold:
           print(f"Quality check passed for {client_id}: {quality_score}")
           return True
       else:
           raise ValueError(f"Quality check failed: {quality_score} < {threshold}")
   ```

## Starter Code

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'your-name',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def create_client_dag(client_id, config):
    """
    Factory function to create client-specific DAGs

    Args:
        client_id (str): Unique client identifier
        config (dict): Client configuration dictionary

    Returns:
        DAG: Configured Airflow DAG
    """

    # Your implementation here
    pass

# Generate DAGs for all clients
for client_id, config in CLIENT_CONFIGS.items():
    dag_id = f"client_{client_id}_pipeline"
    globals()[dag_id] = create_client_dag(client_id, config)
```

## Testing Your Solution

1. **Verify DAG Generation**: Check that all client DAGs are created
2. **Test Configuration Handling**: Ensure each DAG uses its specific configuration
3. **Validate Dynamic Tasks**: Confirm tasks are created based on configuration
4. **Check Business Rules**: Verify client-specific rules are implemented
5. **Test Error Scenarios**: Ensure proper error handling and notifications

## Success Criteria

- [ ] All client DAGs are generated successfully
- [ ] Each DAG has unique tasks based on client configuration
- [ ] Business rules are properly implemented per client
- [ ] Error handling and notifications work correctly
- [ ] DAGs can be parsed and executed without errors
- [ ] Configuration changes result in updated DAG structure

## Advanced Challenges

1. **YAML Configuration**: Load client configurations from YAML files
2. **Database-Driven**: Generate DAGs based on database records
3. **Template System**: Use Jinja2 templates for DAG generation
4. **Validation Framework**: Add configuration validation before DAG creation
5. **Hot Reloading**: Implement configuration change detection and DAG updates

## Configuration Validation

Implement a validation function to ensure configuration integrity:

```python
def validate_client_config(client_id, config):
    """Validate client configuration before DAG creation"""
    required_fields = ['description', 'schedule_interval', 'data_sources', 'destination']

    for field in required_fields:
        if field not in config:
            raise ValueError(f"Missing required field '{field}' for client {client_id}")

    # Add more validation logic
    return True
```

## Best Practices

1. **Use descriptive DAG IDs** that include client identifiers
2. **Implement proper error handling** for configuration issues
3. **Add comprehensive logging** for debugging dynamic generation
4. **Keep factory functions pure** (no side effects)
5. **Document configuration schema** clearly
6. **Test with various configurations** to ensure robustness

## Common Pitfalls

- Don't create too many dynamic DAGs (impacts scheduler performance)
- Ensure DAG IDs are unique across all generated DAGs
- Validate configuration before DAG creation
- Handle missing or invalid configuration gracefully
- Don't hardcode client-specific logic in the factory function

## Next Steps

After mastering dynamic DAG generation, you'll be ready to implement scalable workflow patterns and optimization techniques.
