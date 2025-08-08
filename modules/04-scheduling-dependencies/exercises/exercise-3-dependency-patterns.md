# Exercise 3: Dependency Patterns

## Objective

Master different dependency patterns in Airflow including fan-out, fan-in, diamond patterns, and conditional dependencies to build robust and efficient workflows.

## Background

Understanding dependency patterns is crucial for designing scalable and maintainable data pipelines. Different patterns serve different purposes and understanding when to use each pattern will help you build better workflows.

## Tasks

### Task 1: E-commerce Data Pipeline (Fan-Out Pattern)

Create a DAG that processes e-commerce data using a fan-out pattern where one data preparation task feeds multiple parallel processing tasks.

**Requirements:**

- DAG name: `ecommerce_fanout_pipeline`
- Schedule: Daily at 3 AM
- One preparation task that feeds 4 parallel processing tasks
- Each parallel task should process different aspects of the data

**Data Processing Tasks:**

1. **Customer Analysis**: Process customer behavior data
2. **Product Analysis**: Process product performance data
3. **Order Analysis**: Process order and transaction data
4. **Inventory Analysis**: Process inventory and stock data

**Implementation Structure:**

```
    prepare_ecommerce_data
    /        |        |        \
customer  product   order   inventory
analysis  analysis analysis  analysis
```

**Hints:**

- Use PythonOperator for data preparation
- Each analysis task should receive data from the preparation task
- Include logging to show what data each task is processing
- Use XCom to pass data between tasks

### Task 2: Multi-Source Data Integration (Fan-In Pattern)

Create a DAG that collects data from multiple sources and consolidates them into a single dataset.

**Requirements:**

- DAG name: `multi_source_integration`
- Schedule: Every 6 hours
- 3 parallel data extraction tasks feeding into 1 consolidation task
- Include data validation and quality checks

**Data Sources:**

1. **CRM System**: Extract customer data
2. **Sales Database**: Extract sales transactions
3. **Marketing Platform**: Extract campaign data

**Implementation Structure:**

```
extract_crm    extract_sales    extract_marketing
    \              |              /
         consolidate_all_data
```

**Hints:**

- Each extraction task should simulate different data volumes
- Consolidation task should combine all data and provide summary statistics
- Include error handling for missing or invalid data
- Use appropriate trigger rules if needed

### Task 3: Data Quality Pipeline (Diamond Pattern)

Create a DAG that implements a diamond pattern for data quality processing with parallel quality checks.

**Requirements:**

- DAG name: `data_quality_diamond`
- Schedule: Daily at 1 AM
- Diamond pattern: initialize → parallel checks → final validation
- Include different types of quality checks

**Quality Checks:**

1. **Schema Validation**: Check data structure and types
2. **Business Rules**: Validate business logic constraints
3. **Data Completeness**: Check for missing or null values

**Implementation Structure:**

```
    initialize_quality_check
    /           |           \
schema      business    completeness
validation    rules       check
    \           |           /
    final_quality_validation
```

**Hints:**

- Initialize task should prepare data for quality checks
- Each quality check should focus on different aspects
- Final validation should aggregate results from all checks
- Include quality scores and pass/fail status

### Task 4: Conditional Processing Pipeline

Create a DAG with conditional dependencies based on data characteristics or external conditions.

**Requirements:**

- DAG name: `conditional_processing_pipeline`
- Schedule: Every 2 hours
- Use BranchPythonOperator for conditional logic
- Include different processing paths based on conditions

**Processing Paths:**

1. **High Volume Path**: For large datasets (>10,000 records)
2. **Standard Path**: For medium datasets (1,000-10,000 records)
3. **Light Path**: For small datasets (<1,000 records)

**Conditions to Check:**

- Data volume
- Data freshness
- System load (simulate)

**Implementation Structure:**

```
    check_processing_conditions
    /           |           \
high_volume  standard    light
processing  processing processing
    \           |           /
    final_processing_summary
```

**Hints:**

- Use random number generation to simulate different conditions
- Each processing path should have different logic
- Include a join task that runs regardless of the path taken
- Use appropriate trigger rules for the join task

## Advanced Challenges

### Challenge 1: Dynamic Fan-Out

Create a DAG where the number of parallel tasks is determined dynamically based on input data.

**Requirements:**

- Read configuration to determine number of parallel tasks
- Generate tasks dynamically
- Handle variable workloads

### Challenge 2: Conditional Fan-In

Create a DAG where the consolidation task only runs when specific upstream tasks complete successfully.

**Requirements:**

- Some upstream tasks are optional
- Consolidation logic adapts to available data
- Include fallback mechanisms

### Challenge 3: Multi-Level Dependencies

Create a complex DAG with multiple levels of fan-out and fan-in patterns.

**Requirements:**

- At least 3 levels of dependencies
- Mix different dependency patterns
- Include error handling and recovery

## Validation and Testing

### Testing Dependency Patterns

1. **Verify Task Dependencies:**

   ```python
   # Check that dependencies are set correctly
   dag = dag_bag.get_dag('your_dag_name')
   task = dag.get_task('your_task_name')
   upstream_tasks = task.upstream_task_ids
   downstream_tasks = task.downstream_task_ids
   ```

2. **Test Parallel Execution:**

   - Verify that parallel tasks run simultaneously
   - Check resource usage during parallel execution
   - Ensure no unnecessary blocking

3. **Validate Conditional Logic:**
   - Test different conditions and verify correct paths
   - Ensure join tasks handle all scenarios
   - Check that skipped tasks don't cause failures

## Files to Create

Create your solutions in the following files:

- `dags/ecommerce_fanout_pipeline.py`
- `dags/multi_source_integration.py`
- `dags/data_quality_diamond.py`
- `dags/conditional_processing_pipeline.py`

## Success Criteria

- [ ] All DAGs parse without errors
- [ ] Dependency patterns are implemented correctly
- [ ] Parallel tasks execute simultaneously where expected
- [ ] Conditional logic works for different scenarios
- [ ] XCom data passing works between tasks
- [ ] Error handling is appropriate for each pattern
- [ ] DAG graphs clearly show the intended patterns
- [ ] Logging provides visibility into task execution

## Common Pitfalls to Avoid

1. **Circular Dependencies**: Ensure no task depends on itself through a chain
2. **Resource Contention**: Don't create too many parallel tasks without considering resources
3. **Data Race Conditions**: Ensure data consistency when multiple tasks access shared resources
4. **Improper Trigger Rules**: Use correct trigger rules for join tasks in conditional flows
5. **XCom Size Limits**: Don't pass large datasets through XCom

## Best Practices

1. **Keep Dependencies Simple**: Complex dependency graphs are hard to debug
2. **Use Meaningful Task Names**: Make dependencies self-documenting
3. **Group Related Tasks**: Consider using TaskGroups for logical organization
4. **Handle Failures Gracefully**: Design dependencies to handle task failures
5. **Monitor Performance**: Track execution times for parallel tasks

## Next Steps

After completing this exercise, you'll have a solid understanding of dependency patterns and be ready to tackle more advanced Airflow concepts like sensors and triggers.
