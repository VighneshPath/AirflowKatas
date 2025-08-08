# Exercise 3: Complex Conditional Workflows

## Learning Objectives

By completing this exercise, you will:

- Master nested branching with multiple decision levels
- Implement conditional workflows that combine branching with XComs
- Create dynamic task generation based on runtime conditions
- Build error recovery and fallback mechanisms in branched workflows

## Prerequisites

- Completed Exercise 2: Data-Driven Branching
- Advanced understanding of XCom data passing
- Knowledge of Airflow trigger rules and task dependencies
- Experience with complex Python data structures

## Scenario

You're designing a sophisticated data processing system for a global e-commerce platform. The system must handle:

1. **Multi-Region Processing**: Different processing rules for different geographical regions
2. **Dynamic Workflow Generation**: Create processing tasks based on detected data patterns
3. **Intelligent Error Recovery**: Automatic fallback mechanisms when processing fails
4. **Cross-Dependency Management**: Tasks that depend on results from multiple branches
5. **Real-Time Adaptation**: Workflows that adapt based on system performance and load

This exercise will challenge you to create the most complex branching scenarios you'll encounter in production systems.

## Exercise Tasks

### Task 1: Multi-Level Nested Branching

Create a workflow with three levels of branching decisions.

**Requirements:**

1. **Level 1**: Branch by geographical region (Americas, EMEA, APAC)
2. **Level 2**: Branch by data type within each region (transactions, inventory, customer)
3. **Level 3**: Branch by processing complexity within each data type (simple, complex, ML-enhanced)

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

def analyze_global_data(**context):
    """
    TODO: Analyze incoming data to determine region and characteristics

    Should return:
    - region: 'americas', 'emea', or 'apac'
    - data_types: List of detected data types ['transactions', 'inventory', 'customer']
    - complexity_indicators: Dict with complexity scores for each data type
    - volume_metrics: Data volume information
    - quality_scores: Quality assessment for each data type

    Returns:
        dict: Comprehensive data analysis
    """
    # Your code here - create realistic multi-region scenarios
    pass

def regional_branch(**context):
    """
    TODO: Level 1 branching - Route by geographical region

    Returns:
        str: Regional processing task ID
    """
    # Your code here
    pass

# Americas Region Processing
def americas_data_type_branch(**context):
    """
    TODO: Level 2 branching for Americas region

    Analyze data types present in Americas data and route accordingly.
    May return single task ID or list for parallel processing.

    Returns:
        str or list: Data type processing task ID(s)
    """
    # Your code here
    pass

def americas_transaction_complexity_branch(**context):
    """
    TODO: Level 3 branching for Americas transaction processing

    Returns:
        str: Complexity-appropriate processing task ID
    """
    # Your code here
    pass

def americas_inventory_complexity_branch(**context):
    """TODO: Level 3 branching for Americas inventory processing"""
    # Your code here
    pass

def americas_customer_complexity_branch(**context):
    """TODO: Level 3 branching for Americas customer processing"""
    # Your code here
    pass

# EMEA Region Processing
def emea_data_type_branch(**context):
    """TODO: Level 2 branching for EMEA region"""
    # Your code here
    pass

def emea_transaction_complexity_branch(**context):
    """TODO: Level 3 branching for EMEA transaction processing"""
    # Your code here
    pass

# APAC Region Processing
def apac_data_type_branch(**context):
    """TODO: Level 2 branching for APAC region"""
    # Your code here
    pass

def apac_transaction_complexity_branch(**context):
    """TODO: Level 3 branching for APAC transaction processing"""
    # Your code here
    pass

# TODO: Implement all processing functions for each path
# Example: americas_transaction_simple_processing, americas_transaction_complex_processing, etc.

# TODO: Create DAG with nested branching structure
```

### Task 2: Dynamic Task Generation with Conditional Logic

Create a workflow that dynamically generates processing tasks based on detected data patterns.

**Requirements:**

1. Analyze data to identify processing requirements
2. Dynamically determine which processing tasks are needed
3. Create conditional dependencies between generated tasks
4. Handle scenarios where no processing is required

**Advanced Code:**

```python
def analyze_processing_requirements(**context):
    """
    TODO: Analyze data to determine dynamic processing requirements

    Should identify:
    - required_validations: List of validation types needed
    - transformation_steps: Required data transformations
    - enrichment_sources: External data sources to integrate
    - output_formats: Required output formats
    - compliance_checks: Regulatory compliance requirements

    Returns:
        dict: Processing requirements analysis
    """
    # Your code here
    pass

def dynamic_workflow_branch(**context):
    """
    TODO: Generate dynamic workflow based on requirements

    This is the most complex branching function you'll write.
    It should:
    1. Analyze processing requirements
    2. Determine optimal task sequence
    3. Return list of tasks to execute in order
    4. Handle dependencies between dynamic tasks

    Returns:
        list: Ordered list of task IDs to execute
    """
    # Your code here
    pass

def conditional_validation_branch(**context):
    """
    TODO: Determine which validation tasks to run

    Based on data characteristics, decide which validations are needed:
    - 'schema_validation' for structure checks
    - 'business_rule_validation' for business logic
    - 'data_quality_validation' for quality checks
    - 'compliance_validation' for regulatory requirements

    Returns:
        list: Validation task IDs to execute
    """
    # Your code here
    pass

def conditional_transformation_branch(**context):
    """
    TODO: Determine which transformation tasks to run

    Returns:
        list: Transformation task IDs to execute
    """
    # Your code here
    pass

def conditional_enrichment_branch(**context):
    """
    TODO: Determine which enrichment tasks to run

    Returns:
        list: Enrichment task IDs to execute
    """
    # Your code here
    pass

# TODO: Implement all dynamic processing functions
```

### Task 3: Intelligent Error Recovery with Branching

Create sophisticated error handling that uses branching for recovery strategies.

**Requirements:**

1. Implement error detection and classification
2. Create recovery strategies based on error types
3. Implement fallback mechanisms with multiple levels
4. Add circuit breaker patterns for failing services

**Error Handling Code:**

```python
def execute_risky_processing(**context):
    """
    TODO: Simulate processing that might fail in various ways

    Should simulate different failure scenarios:
    - network_timeout: Connection issues
    - data_corruption: Data quality problems
    - resource_exhaustion: System resource issues
    - service_unavailable: External service failures
    - validation_failure: Data validation errors

    Returns:
        dict: Processing results or error information
    """
    # Your code here
    pass

def error_classification_branch(**context):
    """
    TODO: Classify errors and determine recovery strategy

    Analyze the error from previous task and determine:
    - error_type: Classification of the error
    - severity: How critical is the error
    - recovery_strategy: Which recovery approach to use
    - retry_feasible: Whether retry makes sense

    Returns:
        str: Recovery task ID based on error classification
    """
    # Your code here
    pass

def network_error_recovery(**context):
    """
    TODO: Handle network-related errors

    Should implement:
    - Exponential backoff retry
    - Alternative endpoint switching
    - Cached data fallback
    """
    # Your code here
    pass

def data_error_recovery(**context):
    """
    TODO: Handle data-related errors

    Should implement:
    - Data cleansing procedures
    - Alternative data source lookup
    - Partial processing with error reporting
    """
    # Your code here
    pass

def resource_error_recovery(**context):
    """
    TODO: Handle resource exhaustion errors

    Should implement:
    - Resource cleanup procedures
    - Processing queue management
    - Deferred processing scheduling
    """
    # Your code here
    pass

def service_error_recovery(**context):
    """
    TODO: Handle external service failures

    Should implement:
    - Service health checking
    - Alternative service routing
    - Graceful degradation
    """
    # Your code here
    pass

def validation_error_recovery(**context):
    """
    TODO: Handle validation failures

    Should implement:
    - Data correction attempts
    - Manual review triggering
    - Alternative validation methods
    """
    # Your code here
    pass

def critical_error_escalation(**context):
    """
    TODO: Handle critical errors that can't be recovered

    Should implement:
    - Alert notifications
    - Data preservation
    - Manual intervention triggers
    """
    # Your code here
    pass
```

### Task 4: Cross-Branch Data Aggregation

Create a complex workflow where results from multiple branches are combined intelligently.

**Requirements:**

1. Collect results from multiple parallel branches
2. Implement intelligent aggregation based on data quality
3. Handle scenarios where some branches fail
4. Create final processing based on aggregated results

**Aggregation Code:**

```python
def parallel_processing_coordinator(**context):
    """
    TODO: Coordinate multiple parallel processing branches

    Should:
    1. Initiate multiple processing paths
    2. Track processing status
    3. Coordinate result collection

    Returns:
        list: Task IDs for parallel processing
    """
    # Your code here
    pass

def collect_branch_results(**context):
    """
    TODO: Collect and analyze results from all branches

    Should:
    1. Gather XCom data from all completed branches
    2. Assess quality and completeness of results
    3. Identify any missing or failed processing
    4. Prepare data for aggregation decision

    Returns:
        dict: Comprehensive results analysis
    """
    # Your code here
    pass

def aggregation_strategy_branch(**context):
    """
    TODO: Determine how to aggregate results based on their quality

    Strategies:
    - 'complete_aggregation': All branches succeeded, full aggregation
    - 'partial_aggregation': Some branches failed, partial aggregation
    - 'weighted_aggregation': Weight results by quality scores
    - 'fallback_aggregation': Use cached or default data for missing results
    - 'retry_failed_branches': Retry failed branches before aggregation

    Returns:
        str: Aggregation strategy task ID
    """
    # Your code here
    pass

def complete_aggregation(**context):
    """TODO: Perform complete aggregation of all results"""
    # Your code here
    pass

def partial_aggregation(**context):
    """TODO: Perform partial aggregation with missing data handling"""
    # Your code here
    pass

def weighted_aggregation(**context):
    """TODO: Perform quality-weighted aggregation"""
    # Your code here
    pass

def fallback_aggregation(**context):
    """TODO: Perform aggregation with fallback data"""
    # Your code here
    pass

def retry_failed_branches(**context):
    """TODO: Retry failed branches before aggregation"""
    # Your code here
    pass
```

## Advanced Testing Scenarios

### Comprehensive Test Suite

```python
def test_nested_branching_scenarios():
    """Test all combinations of nested branching"""

    test_scenarios = [
        {
            'region': 'americas',
            'data_types': ['transactions'],
            'complexity': 'simple',
            'expected_path': 'americas_transaction_simple_processing'
        },
        {
            'region': 'emea',
            'data_types': ['transactions', 'inventory'],
            'complexity': 'complex',
            'expected_paths': ['emea_transaction_complex_processing', 'emea_inventory_complex_processing']
        },
        # Add more test scenarios
    ]

    # TODO: Implement comprehensive testing
    pass

def test_dynamic_workflow_generation():
    """Test dynamic task generation"""

    # Test scenarios with different requirements
    scenarios = [
        {
            'validations': ['schema', 'business_rules'],
            'transformations': ['normalize', 'enrich'],
            'expected_tasks': ['schema_validation', 'business_rule_validation', 'normalize_transformation', 'enrich_transformation']
        }
    ]

    # TODO: Implement dynamic workflow testing
    pass

def test_error_recovery_paths():
    """Test all error recovery scenarios"""

    error_scenarios = [
        {'error_type': 'network_timeout', 'expected_recovery': 'network_error_recovery'},
        {'error_type': 'data_corruption', 'expected_recovery': 'data_error_recovery'},
        # Add more scenarios
    ]

    # TODO: Implement error recovery testing
    pass
```

### Performance Testing

```python
def test_branching_performance():
    """Test performance of complex branching logic"""

    # TODO: Implement performance benchmarks
    # - Measure branching decision time
    # - Test with large XCom payloads
    # - Validate memory usage
    pass

def test_scalability():
    """Test scalability with many parallel branches"""

    # TODO: Test with increasing numbers of parallel branches
    pass
```

## Integration Challenges

### Real-World Integration

1. **Database Integration**: Connect branching decisions to database queries
2. **API Integration**: Use external APIs to inform branching decisions
3. **Message Queue Integration**: Handle branching with message queues
4. **Monitoring Integration**: Send branching metrics to monitoring systems

### Example Integration Code:

```python
def database_informed_branch(**context):
    """
    TODO: Use database query results to inform branching

    Should:
    1. Query database for current system state
    2. Use query results in branching logic
    3. Handle database connection failures
    """
    # Your code here
    pass

def api_informed_branch(**context):
    """
    TODO: Use external API data for branching decisions

    Should:
    1. Call external API for decision data
    2. Handle API failures gracefully
    3. Cache API responses when appropriate
    """
    # Your code here
    pass
```

## Expected Outputs

### Nested Branching Example:

```
[2024-01-15 10:00:00] INFO - Global Data Analysis Complete
[2024-01-15 10:00:00] INFO - Region: Americas, Data Types: ['transactions', 'inventory']
[2024-01-15 10:00:00] INFO - Level 1 Branch: Routing to Americas processing
[2024-01-15 10:00:00] INFO - Level 2 Branch: Processing transactions and inventory in parallel
[2024-01-15 10:00:00] INFO - Level 3 Branch (Transactions): Complex processing required
[2024-01-15 10:00:00] INFO - Level 3 Branch (Inventory): Simple processing sufficient
```

### Dynamic Workflow Example:

```
[2024-01-15 10:00:00] INFO - Processing Requirements Analysis:
[2024-01-15 10:00:00] INFO - - Validations needed: ['schema', 'business_rules', 'compliance']
[2024-01-15 10:00:00] INFO - - Transformations needed: ['normalize', 'enrich', 'aggregate']
[2024-01-15 10:00:00] INFO - - Output formats: ['json', 'parquet']
[2024-01-15 10:00:00] INFO - Dynamic workflow generated: 6 tasks in optimal sequence
```

### Error Recovery Example:

```
[2024-01-15 10:00:00] ERROR - Processing failed: Network timeout after 30 seconds
[2024-01-15 10:00:00] INFO - Error Classification: network_timeout, Severity: medium
[2024-01-15 10:00:00] INFO - Recovery Strategy: Exponential backoff with alternative endpoint
[2024-01-15 10:00:00] INFO - Recovery attempt 1/3: Switching to backup endpoint
[2024-01-15 10:00:00] INFO - Recovery successful: Processing completed via backup
```

## Success Criteria

Your solution should demonstrate:

- ✅ **Complex Nested Logic**: 3+ levels of branching with proper dependencies
- ✅ **Dynamic Task Generation**: Runtime task creation based on data analysis
- ✅ **Intelligent Error Handling**: Sophisticated error classification and recovery
- ✅ **Cross-Branch Coordination**: Proper aggregation of results from multiple branches
- ✅ **Performance Optimization**: Efficient branching logic that scales well
- ✅ **Comprehensive Testing**: Test coverage for all branching scenarios
- ✅ **Production Readiness**: Error handling, logging, and monitoring integration

## Common Pitfalls

1. **Circular Dependencies**: Avoid creating circular task dependencies in complex branching
2. **XCom Payload Size**: Be careful with large data structures in XComs
3. **Error Propagation**: Ensure errors in one branch don't cascade to others
4. **Resource Deadlocks**: Prevent resource conflicts in parallel branches
5. **Infinite Loops**: Avoid infinite retry loops in error recovery
6. **Memory Leaks**: Clean up resources in long-running branching logic

## Next Steps

After completing this exercise:

1. **Review and Optimize**: Analyze your solution for performance improvements
2. **Real-World Application**: Consider how these patterns apply to your use cases
3. **Advanced Patterns**: Explore SubDAGs and TaskGroups for complex workflows
4. **Production Deployment**: Plan for monitoring and maintenance of complex branching logic

---

**Estimated Time:** 3-4 hours

**Difficulty:** Advanced

This is the most challenging branching exercise. Take breaks, test incrementally, and don't hesitate to simplify if you get stuck. The goal is to understand the patterns, not to create the perfect solution on the first try!
