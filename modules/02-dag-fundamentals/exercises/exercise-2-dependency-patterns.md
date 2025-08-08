# Exercise 2: Advanced Dependency Patterns

## Objective

Master complex task dependency patterns by implementing real-world workflows that require sophisticated orchestration. You'll learn to design efficient, maintainable workflows using fan-out, fan-in, and mixed dependency patterns.

## Background

Your e-commerce company is expanding globally and needs more sophisticated data processing workflows. You need to design DAGs that can handle parallel processing, data convergence, and complex business logic while maintaining efficiency and reliability.

## Requirements

Create **two comprehensive DAG files** that demonstrate advanced dependency patterns:

### DAG 1: Global Sales Analytics Pipeline (`global_sales_analytics.py`)

**Business Need**: Process sales data from multiple regions in parallel, then create consolidated reports and dashboards.

**Workflow Pattern**:

```
Data Preparation
       ↓
Regional Processing (Parallel)
       ↓
Data Consolidation
       ↓
Output Generation (Parallel)
       ↓
Final Validation
```

**Configuration**:

- **DAG ID**: `global_sales_analytics`
- **Description**: "Global sales data processing with parallel regional analysis"
- **Schedule**: Daily at 4:00 AM
- **Start Date**: January 1, 2024
- **Catchup**: False
- **Max Active Runs**: 1
- **Tags**: `['sales', 'global', 'analytics', 'parallel']`

**Tasks to Implement**:

1. **Data Preparation Phase**:

   - `validate_source_data` - Check data quality and availability
   - `prepare_processing_environment` - Set up temporary tables and connections

2. **Regional Processing Phase (Parallel)**:

   - `process_north_america` - Process NA sales data
   - `process_europe` - Process European sales data
   - `process_asia_pacific` - Process APAC sales data
   - `process_latin_america` - Process LATAM sales data

3. **Data Consolidation Phase**:

   - `merge_regional_data` - Combine all regional results
   - `calculate_global_metrics` - Compute global KPIs

4. **Output Generation Phase (Parallel)**:

   - `generate_executive_dashboard` - Create C-level dashboard
   - `generate_regional_reports` - Create detailed regional reports
   - `update_data_warehouse` - Load data into warehouse
   - `send_stakeholder_alerts` - Notify relevant teams

5. **Final Validation Phase**:
   - `validate_outputs` - Verify all outputs were created successfully
   - `cleanup_temp_resources` - Clean up temporary resources

**Dependencies**:

- Data Preparation tasks run sequentially
- Regional Processing tasks run in parallel after preparation
- Consolidation tasks run after all regional processing completes
- Output Generation tasks run in parallel after consolidation
- Final Validation runs after all outputs complete

### DAG 2: Multi-Stage Data Quality Pipeline (`data_quality_pipeline.py`)

**Business Need**: Implement a comprehensive data quality pipeline with multiple validation stages, error handling, and conditional processing paths.

**Workflow Pattern**:

```
Initial Setup
     ↓
Data Collection (Parallel)
     ↓
Primary Validation (Parallel)
     ↓
Conditional Processing (Based on validation results)
     ↓
Secondary Validation
     ↓
Final Processing & Reporting
```

**Configuration**:

- **DAG ID**: `data_quality_pipeline`
- **Description**: "Multi-stage data quality validation and processing pipeline"
- **Schedule**: Every 6 hours
- **Start Date**: January 1, 2024
- **Catchup**: False
- **Max Active Runs**: 2
- **Tags**: `['data-quality', 'validation', 'multi-stage']`

**Tasks to Implement**:

1. **Initial Setup**:

   - `initialize_quality_checks` - Set up quality check parameters
   - `prepare_validation_rules` - Load validation rules and thresholds

2. **Data Collection Phase (Parallel)**:

   - `collect_customer_data` - Gather customer information
   - `collect_transaction_data` - Gather transaction records
   - `collect_product_data` - Gather product catalog data
   - `collect_inventory_data` - Gather inventory levels

3. **Primary Validation Phase (Parallel)**:

   - `validate_customer_quality` - Check customer data quality
   - `validate_transaction_quality` - Check transaction data quality
   - `validate_product_quality` - Check product data quality
   - `validate_inventory_quality` - Check inventory data quality

4. **Quality Assessment**:

   - `assess_overall_quality` - Evaluate overall data quality score

5. **Conditional Processing** (Use TaskGroup):

   - `process_high_quality_data` - Process data that passed all checks
   - `quarantine_poor_quality_data` - Isolate data that failed checks
   - `generate_quality_alerts` - Create alerts for quality issues

6. **Secondary Validation**:

   - `cross_validate_data` - Perform cross-dataset validation
   - `validate_business_rules` - Check business logic constraints

7. **Final Processing**:
   - `generate_quality_report` - Create comprehensive quality report
   - `update_quality_metrics` - Update quality tracking dashboard
   - `archive_processed_data` - Archive successfully processed data

**Dependencies**:

- Setup tasks run sequentially
- Data Collection tasks run in parallel
- Primary Validation tasks run in parallel after collection
- Quality Assessment runs after all primary validations
- Conditional Processing tasks run based on quality assessment
- Secondary Validation runs after conditional processing
- Final Processing tasks run after secondary validation

## Implementation Guidelines

### Step 1: Plan Your Dependencies

Before coding, draw out the dependency graph for each DAG:

```python
# Example dependency planning for Global Sales Analytics:
# Phase 1: validate_source_data >> prepare_processing_environment
# Phase 2: prepare_processing_environment >> [process_north_america, process_europe, process_asia_pacific, process_latin_america]
# Phase 3: [all regional processing] >> merge_regional_data >> calculate_global_metrics
# Phase 4: calculate_global_metrics >> [generate_executive_dashboard, generate_regional_reports, update_data_warehouse, send_stakeholder_alerts]
# Phase 5: [all output generation] >> validate_outputs >> cleanup_temp_resources
```

### Step 2: Implement Python Functions

Create realistic functions that simulate actual data processing:

```python
def process_region_data(region_name):
    """Factory function to create region-specific processors"""
    def _process(**context):
        execution_date = context['execution_date']
        print(f"Processing {region_name} sales data for {execution_date}")

        # Simulate processing steps
        print(f"  - Loading {region_name} raw data...")
        print(f"  - Applying {region_name}-specific business rules...")
        print(f"  - Calculating {region_name} metrics...")
        print(f"  - Validating {region_name} results...")

        # Simulate processing time and results
        import time
        time.sleep(2)  # Simulate processing time

        # Return processing results
        results = {
            'region': region_name,
            'records_processed': 10000 + hash(region_name) % 5000,
            'total_sales': 1000000 + hash(region_name) % 500000,
            'processing_time': 2.0
        }

        print(f"  - {region_name} processing completed: {results}")
        return results

    return _process

def validate_data_quality(data_type):
    """Factory function for data quality validation"""
    def _validate(**context):
        print(f"Validating {data_type} data quality...")

        # Simulate quality checks
        import random
        quality_score = random.uniform(0.7, 1.0)  # Random quality score

        print(f"  - Checking {data_type} completeness...")
        print(f"  - Checking {data_type} accuracy...")
        print(f"  - Checking {data_type} consistency...")

        result = {
            'data_type': data_type,
            'quality_score': quality_score,
            'passed_validation': quality_score > 0.8
        }

        print(f"  - {data_type} quality validation result: {result}")
        return result

    return _validate
```

### Step 3: Use TaskGroups for Organization

Organize complex workflows using TaskGroups:

```python
from airflow.utils.task_group import TaskGroup

# Example TaskGroup usage
with TaskGroup('regional_processing', dag=dag) as regional_group:
    process_na = PythonOperator(
        task_id='process_north_america',
        python_callable=process_region_data('North America')
    )

    process_eu = PythonOperator(
        task_id='process_europe',
        python_callable=process_region_data('Europe')
    )

    # Add other regional tasks...

    # No explicit dependencies needed within group for parallel execution
```

### Step 4: Implement Complex Dependencies

Use various dependency patterns:

```python
# Sequential within phases
task_a >> task_b >> task_c

# Parallel execution
task_prep >> [task_1, task_2, task_3, task_4]

# Fan-in after parallel processing
[task_1, task_2, task_3, task_4] >> task_consolidate

# Mixed patterns
task_start >> parallel_group >> consolidation_group >> [output_1, output_2] >> final_task
```

### Step 5: Add Error Handling and Monitoring

Include appropriate error handling:

```python
default_args = {
    'owner': 'data_engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
    'email_on_failure': True,
    'email_on_retry': False,
}

# For critical tasks, add specific configurations
critical_task = PythonOperator(
    task_id='critical_processing',
    python_callable=important_function,
    retries=3,  # Override default
    retry_delay=timedelta(minutes=10),
    execution_timeout=timedelta(hours=2),
)
```

## Testing Your Implementation

### 1. Dependency Visualization

1. Load both DAGs in Airflow UI
2. Use the Graph view to verify dependency patterns
3. Check that parallel tasks appear side-by-side
4. Verify sequential tasks appear in proper order
5. Ensure TaskGroups are properly organized

### 2. Execution Testing

1. **Manual Trigger Test**:

   - Trigger each DAG manually
   - Watch execution in Graph view
   - Verify parallel tasks start simultaneously
   - Check that dependencies are respected

2. **Log Verification**:

   - Check logs for each task
   - Verify your functions are executing correctly
   - Ensure data is being passed between tasks appropriately

3. **Performance Testing**:
   - Monitor execution times
   - Verify parallel tasks improve overall runtime
   - Check resource utilization during parallel execution

### 3. Dependency Edge Cases

Test these scenarios:

1. **Partial Failure**: Stop one parallel task and verify others continue
2. **Retry Behavior**: Force a task to fail and verify retry logic
3. **Downstream Impact**: Verify that upstream failures prevent downstream execution

## Success Criteria

### Global Sales Analytics DAG:

- [ ] 5 distinct processing phases with proper dependencies
- [ ] 4 regional processing tasks execute in parallel
- [ ] 4 output generation tasks execute in parallel
- [ ] Sequential phases execute in correct order
- [ ] TaskGroups organize related tasks (optional but recommended)
- [ ] All tasks complete successfully when triggered
- [ ] Graph view clearly shows the workflow pattern

### Data Quality Pipeline DAG:

- [ ] Multi-stage validation with parallel and sequential phases
- [ ] 4 data collection tasks execute in parallel
- [ ] 4 validation tasks execute in parallel after collection
- [ ] Conditional processing based on validation results
- [ ] Final processing phase completes the pipeline
- [ ] Appropriate error handling and timeouts configured
- [ ] Clear separation between validation stages

## Advanced Challenges

Once your basic implementation works, try these enhancements:

### 1. Dynamic Task Generation

Create tasks dynamically based on configuration:

```python
# Generate regional processing tasks from config
REGIONS = ['North America', 'Europe', 'Asia Pacific', 'Latin America', 'Africa']

regional_tasks = []
for region in REGIONS:
    task = PythonOperator(
        task_id=f'process_{region.lower().replace(" ", "_")}',
        python_callable=process_region_data(region),
        dag=dag
    )
    regional_tasks.append(task)

# Set up dependencies
prepare_data >> regional_tasks >> consolidate_data
```

### 2. Conditional Dependencies

Implement branching logic based on data quality:

```python
from airflow.operators.python import BranchPythonOperator

def choose_processing_path(**context):
    # Get quality score from previous task
    quality_score = context['task_instance'].xcom_pull(task_ids='assess_overall_quality')

    if quality_score > 0.9:
        return 'process_high_quality_data'
    elif quality_score > 0.7:
        return 'process_medium_quality_data'
    else:
        return 'quarantine_poor_quality_data'

branch_task = BranchPythonOperator(
    task_id='choose_processing_path',
    python_callable=choose_processing_path,
    dag=dag
)
```

### 3. Cross-DAG Dependencies

Make one DAG depend on another DAG's completion:

```python
from airflow.sensors.external_task import ExternalTaskSensor

wait_for_upstream = ExternalTaskSensor(
    task_id='wait_for_data_prep',
    external_dag_id='data_preparation_dag',
    external_task_id='final_data_prep_task',
    timeout=1800,  # 30 minutes
    poke_interval=60,  # Check every minute
    dag=dag
)
```

## Common Pitfalls and Solutions

### 1. Dependency Confusion

**Problem**: Tasks don't execute in expected order

**Solution**:

- Draw your dependency graph before coding
- Use clear, descriptive task IDs
- Test with manual triggers to verify flow

### 2. Parallel Task Bottlenecks

**Problem**: Parallel tasks don't improve performance

**Solution**:

- Check Airflow executor configuration
- Verify `max_active_tasks` setting
- Monitor resource utilization

### 3. Complex Dependency Debugging

**Problem**: Hard to understand why tasks aren't running

**Solution**:

- Use TaskGroups to organize related tasks
- Add clear task descriptions
- Use the Graph view to visualize dependencies

### 4. Memory and Resource Issues

**Problem**: Too many parallel tasks overwhelm the system

**Solution**:

- Use resource pools to limit concurrent tasks
- Implement proper task timeouts
- Monitor system resources during execution

## Verification Questions

Test your understanding:

1. **What's the difference between fan-out and fan-in patterns, and when would you use each?**

2. **How do TaskGroups affect dependency definition and execution?**

3. **What happens if one task in a parallel group fails? How does it affect downstream tasks?**

4. **Why might you choose to use multiple smaller DAGs instead of one large DAG with complex dependencies?**

5. **How can you ensure that parallel tasks don't compete for the same resources?**

## Next Steps

After completing this exercise, you'll have mastered:

- Complex dependency pattern design
- Parallel processing optimization
- TaskGroup organization
- Error handling in complex workflows
- Performance considerations for large DAGs

This prepares you for Module 3, where you'll explore different types of operators and how to choose the right operator for each task type!
