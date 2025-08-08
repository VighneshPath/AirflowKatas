# Exercise 2: Data-Driven Branching

## Learning Objectives

By completing this exercise, you will:

- Implement complex data-driven branching logic
- Use XCom data to make dynamic workflow decisions
- Create multi-path branching with parallel execution
- Handle data quality scenarios with appropriate branching

## Prerequisites

- Completed Exercise 1: Basic Branching
- Understanding of XCom data passing
- Knowledge of data validation concepts
- Familiarity with Python data structures

## Scenario

You're building an advanced data processing pipeline for a financial services company. The pipeline must dynamically adapt its processing strategy based on:

1. **Data Quality Metrics**: Error rates, completeness, consistency
2. **Data Volume**: Small, medium, large datasets requiring different processing approaches
3. **Data Source**: Different validation and processing rules for different sources
4. **Business Priority**: Critical, high, normal, low priority processing

The pipeline should intelligently route data through appropriate processing paths and handle quality issues automatically.

## Exercise Tasks

### Task 1: Data Quality Assessment and Branching

Create a DAG that analyzes data quality and branches based on the results.

**Requirements:**

1. Implement a data quality assessment function
2. Create branching logic based on quality metrics
3. Implement different processing paths for various quality levels

**Starter Code:**

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
import random

default_args = {
    'owner': 'your-name',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def assess_data_quality(**context):
    """
    TODO: Implement data quality assessment

    Should analyze and return metrics for:
    - error_rate: Percentage of records with errors (0.0 to 1.0)
    - completeness: Percentage of complete records (0.0 to 1.0)
    - consistency: Data consistency score (0.0 to 1.0)
    - timeliness: Data freshness score (0.0 to 1.0)
    - record_count: Total number of records

    Returns:
        dict: Quality metrics dictionary
    """
    # Your code here - simulate realistic quality scenarios
    pass

def quality_based_branch(**context):
    """
    TODO: Implement quality-based branching logic

    Branch based on quality metrics:
    - High quality (error_rate < 0.05, completeness > 0.95): 'high_quality_processing'
    - Medium quality (error_rate < 0.15, completeness > 0.85): 'medium_quality_processing'
    - Low quality (error_rate >= 0.15 or completeness <= 0.85): 'low_quality_processing'
    - Critical issues (error_rate > 0.5): 'critical_quality_handling'

    Returns:
        str: Task ID for appropriate processing path
    """
    # Your code here
    pass

def high_quality_processing(**context):
    """
    TODO: Implement high quality data processing

    Should include:
    - Standard processing algorithms
    - Minimal validation overhead
    - Performance optimization
    """
    # Your code here
    pass

def medium_quality_processing(**context):
    """
    TODO: Implement medium quality data processing

    Should include:
    - Enhanced validation steps
    - Data cleansing procedures
    - Quality improvement measures
    """
    # Your code here
    pass

def low_quality_processing(**context):
    """
    TODO: Implement low quality data processing

    Should include:
    - Comprehensive data cleansing
    - Error correction procedures
    - Quality reporting
    """
    # Your code here
    pass

def critical_quality_handling(**context):
    """
    TODO: Implement critical quality issue handling

    Should include:
    - Data rejection procedures
    - Alert notifications
    - Manual review triggers
    """
    # Your code here
    pass

# TODO: Create DAG with quality-based branching
```

### Task 2: Multi-Source Data Processing

Extend your pipeline to handle different data sources with source-specific processing.

**Requirements:**

1. Create a data source identification function
2. Implement source-specific validation and processing
3. Handle multiple processing paths that can run in parallel

**Additional Code:**

```python
def identify_data_sources(**context):
    """
    TODO: Identify and categorize data sources

    Should return information about:
    - source_types: List of detected source types
    - source_priorities: Priority levels for each source
    - processing_requirements: Special requirements per source

    Returns:
        dict: Source analysis results
    """
    # Your code here - simulate multiple data sources
    pass

def multi_source_branch(**context):
    """
    TODO: Implement multi-source branching logic

    Should return a list of task IDs for parallel processing:
    - 'database_source_processing' if database sources detected
    - 'api_source_processing' if API sources detected
    - 'file_source_processing' if file sources detected
    - 'stream_source_processing' if streaming sources detected

    Returns:
        list or str: Task ID(s) for source processing
    """
    # Your code here
    pass

def database_source_processing(**context):
    """TODO: Process database sources"""
    # Your code here
    pass

def api_source_processing(**context):
    """TODO: Process API sources"""
    # Your code here
    pass

def file_source_processing(**context):
    """TODO: Process file sources"""
    # Your code here
    pass

def stream_source_processing(**context):
    """TODO: Process streaming sources"""
    # Your code here
    pass

# TODO: Add these tasks to your DAG
```

### Task 3: Priority-Based Resource Allocation

Create intelligent resource allocation based on business priority and system load.

**Requirements:**

1. Assess system resources and business priority
2. Implement priority-based processing allocation
3. Handle resource constraints with appropriate fallbacks

**Challenge Code:**

```python
def assess_system_resources(**context):
    """
    TODO: Assess current system resources

    Should return:
    - cpu_usage: Current CPU utilization (0.0 to 1.0)
    - memory_usage: Current memory utilization (0.0 to 1.0)
    - available_workers: Number of available worker nodes
    - queue_length: Current processing queue length

    Returns:
        dict: System resource metrics
    """
    # Your code here
    pass

def determine_business_priority(**context):
    """
    TODO: Determine business priority of current data

    Should analyze:
    - data_age: How recent is the data
    - business_impact: Potential business impact
    - sla_requirements: SLA constraints
    - customer_tier: Customer priority level

    Returns:
        dict: Priority assessment
    """
    # Your code here
    pass

def resource_priority_branch(**context):
    """
    TODO: Branch based on resources and priority

    Consider both system resources and business priority:
    - 'high_priority_processing': Critical data with sufficient resources
    - 'standard_priority_processing': Normal priority with adequate resources
    - 'low_priority_processing': Low priority or resource-constrained
    - 'deferred_processing': Insufficient resources, defer to later

    Returns:
        str: Appropriate processing task ID
    """
    # Your code here
    pass

# TODO: Implement processing functions for each priority level
```

### Task 4: Comprehensive Data Pipeline

Combine all branching logic into a comprehensive data processing pipeline.

**Requirements:**

1. Create a pipeline that uses all previous branching logic
2. Implement proper error handling and recovery
3. Add monitoring and reporting capabilities

## Testing Your Implementation

### Test Scenarios

Create test cases for various data scenarios:

```python
def test_quality_branching():
    """Test quality-based branching with different scenarios"""

    # Test high quality scenario
    high_quality_data = {
        'error_rate': 0.02,
        'completeness': 0.98,
        'consistency': 0.95,
        'record_count': 1000
    }

    # Test low quality scenario
    low_quality_data = {
        'error_rate': 0.25,
        'completeness': 0.75,
        'consistency': 0.60,
        'record_count': 800
    }

    # TODO: Implement test logic
    pass

def test_multi_source_branching():
    """Test multi-source branching scenarios"""

    # Test single source
    single_source = {
        'source_types': ['database'],
        'source_priorities': {'database': 'high'}
    }

    # Test multiple sources
    multi_source = {
        'source_types': ['database', 'api', 'file'],
        'source_priorities': {
            'database': 'high',
            'api': 'medium',
            'file': 'low'
        }
    }

    # TODO: Implement test logic
    pass
```

### Validation Checklist

- [ ] Quality assessment correctly categorizes data
- [ ] Branching logic handles all quality scenarios
- [ ] Multi-source processing works with various combinations
- [ ] Priority-based allocation considers both factors
- [ ] Error handling prevents pipeline failures
- [ ] All branches eventually join properly
- [ ] XCom data is properly passed between tasks

## Expected Outputs

### Quality Assessment Example:

```
[2024-01-15 10:00:00] INFO - Data Quality Assessment:
[2024-01-15 10:00:00] INFO - - Error Rate: 8.5%
[2024-01-15 10:00:00] INFO - - Completeness: 92.3%
[2024-01-15 10:00:00] INFO - - Consistency: 88.7%
[2024-01-15 10:00:00] INFO - - Record Count: 15,432
[2024-01-15 10:00:00] INFO - Quality Level: Medium - routing to enhanced processing
```

### Multi-Source Processing Example:

```
[2024-01-15 10:00:00] INFO - Detected Sources: ['database', 'api']
[2024-01-15 10:00:00] INFO - Parallel Processing Paths:
[2024-01-15 10:00:00] INFO - - Database source: High priority processing
[2024-01-15 10:00:00] INFO - - API source: Standard processing
```

## Advanced Features

### Bonus Challenges

1. **Dynamic Thresholds**: Make quality thresholds configurable via Airflow Variables
2. **Historical Analysis**: Use historical data to improve branching decisions
3. **Machine Learning**: Implement ML-based quality prediction
4. **Real-time Monitoring**: Add real-time quality monitoring and alerting

### Performance Optimization

1. **Caching**: Cache expensive quality assessments
2. **Parallel Assessment**: Run quality checks in parallel
3. **Incremental Processing**: Process only changed data
4. **Resource Pooling**: Implement resource pool management

## Common Pitfalls

1. **Complex Branching Logic**: Keep branching functions focused and testable
2. **XCom Size Limits**: Be mindful of XCom payload sizes for large datasets
3. **Error Propagation**: Ensure errors in one branch don't affect others
4. **Resource Deadlocks**: Avoid resource allocation conflicts

## Integration Patterns

### With Other Airflow Features

1. **Sensors**: Combine with sensors for event-driven branching
2. **SubDAGs**: Use SubDAGs for complex branching scenarios
3. **TaskGroups**: Organize related branching tasks
4. **Pools**: Manage resource allocation with Airflow pools

### With External Systems

1. **Monitoring Systems**: Integrate with external monitoring
2. **Data Quality Tools**: Connect with data quality platforms
3. **Notification Systems**: Send alerts based on branching decisions
4. **Metadata Stores**: Store branching decisions for analysis

## Solution Architecture

Your final solution should demonstrate:

```
Data Input
    ↓
Quality Assessment
    ↓
Quality Branch ──→ High Quality Processing
    ├──→ Medium Quality Processing
    ├──→ Low Quality Processing
    └──→ Critical Quality Handling
    ↓
Source Identification
    ↓
Multi-Source Branch ──→ Database Processing
    ├──→ API Processing
    ├──→ File Processing
    └──→ Stream Processing
    ↓
Resource Assessment
    ↓
Priority Branch ──→ High Priority Processing
    ├──→ Standard Priority Processing
    ├──→ Low Priority Processing
    └──→ Deferred Processing
    ↓
Results Aggregation
    ↓
Final Output
```

## Next Steps

After completing this exercise:

1. Review the solution to see alternative approaches
2. Experiment with different quality thresholds
3. Try integrating with real data sources
4. Move on to Exercise 3 for advanced conditional patterns

---

**Estimated Time:** 90-120 minutes

**Difficulty:** Intermediate to Advanced

This exercise will challenge you to think about real-world data processing scenarios and implement sophisticated branching logic. Take your time to understand each requirement before implementing!
