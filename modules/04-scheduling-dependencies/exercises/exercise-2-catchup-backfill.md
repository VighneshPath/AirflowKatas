# Exercise 2: Catchup and Backfill Scenarios

## Objective

Understand and implement catchup behavior and backfill scenarios for different data processing requirements.

## Background

Your data engineering team needs to handle various scenarios where historical data processing is required. Understanding when to use catchup and how to design for backfill scenarios is crucial for reliable data pipelines.

## Tasks

### Task 1: Historical Data Migration

Create a DAG that processes historical sales data with proper catchup behavior.

**Requirements:**

- DAG name: `historical_sales_migration`
- Schedule: Daily at 2 AM
- Start date: 30 days ago from today
- Enable catchup to process all historical days
- Limit concurrent runs to avoid overwhelming the system
- Include data validation for each day's processing

**Implementation Details:**

```python
# Use this structure as a starting point
from datetime import datetime, timedelta

start_date = datetime.now() - timedelta(days=30)

dag = DAG(
    'historical_sales_migration',
    start_date=start_date,
    schedule_interval='0 2 * * *',
    catchup=True,
    max_active_runs=3,  # Process max 3 days concurrently
    # Add your configuration
)
```

**Tasks to include:**

1. Validate data availability for the processing date
2. Extract sales data for the specific date
3. Transform and clean the data
4. Load data into the target system
5. Generate processing summary

### Task 2: System Recovery After Outage

Create a DAG that demonstrates recovery from a system outage scenario.

**Requirements:**

- DAG name: `system_recovery_processing`
- Schedule: Every 6 hours
- Simulate a scenario where the system was down for 3 days
- Design the DAG to handle the backlog efficiently
- Include monitoring and alerting for the recovery process

**Scenario Setup:**

- The DAG should start processing from 3 days ago
- Use appropriate retry logic for failed tasks
- Implement task prioritization (critical vs. non-critical data)
- Add logging to track recovery progress

**Tasks to include:**

1. Check system health and availability
2. Prioritize critical data processing
3. Process non-critical data
4. Validate data integrity
5. Send recovery status notifications

### Task 3: Incremental Data Processing

Create a DAG that handles incremental data processing with smart catchup logic.

**Requirements:**

- DAG name: `incremental_data_processing`
- Schedule: Hourly
- Implement logic to detect and process only changed data
- Handle both full and incremental processing modes
- Include data freshness validation

**Implementation Approach:**

- Use execution date to determine the data window
- Implement checkpointing to track processed data
- Add logic to switch between full and incremental modes
- Include data quality checks

**Tasks to include:**

1. Determine processing mode (full vs. incremental)
2. Identify data changes since last run
3. Process changed data only
4. Update processing checkpoints
5. Validate data consistency

### Task 4: Multi-Source Data Synchronization

Create a DAG that synchronizes data from multiple sources with different catchup requirements.

**Requirements:**

- DAG name: `multi_source_sync`
- Schedule: Daily at midnight
- Handle different data sources with varying availability
- Implement conditional catchup based on source reliability
- Include cross-source data validation

**Data Sources to Simulate:**

1. **Reliable Source**: Always available, enable catchup
2. **Unreliable Source**: Sometimes unavailable, disable catchup
3. **Batch Source**: Available weekly, custom catchup logic

**Tasks to include:**

1. Check availability of each data source
2. Process reliable source data (with catchup)
3. Process unreliable source data (without catchup)
4. Handle batch source data (custom logic)
5. Synchronize and validate cross-source consistency

## Advanced Challenges

### Challenge 1: Dynamic Catchup Control

Implement a DAG that can dynamically enable/disable catchup based on external conditions.

**Requirements:**

- Read catchup configuration from a file or database
- Modify DAG behavior without code changes
- Include monitoring for configuration changes

### Challenge 2: Selective Backfill

Create a mechanism to backfill only specific date ranges or conditions.

**Requirements:**

- Allow backfill for specific date ranges
- Skip certain dates based on business rules
- Provide backfill progress tracking

## Validation and Testing

### Testing Catchup Behavior

1. **Create test scenarios:**

   ```bash
   # Test with different start dates
   # Observe DAG run creation in the UI
   # Monitor task execution order
   ```

2. **Verify backfill functionality:**

   ```bash
   # Use airflow dags backfill command
   airflow dags backfill historical_sales_migration \
     --start-date 2024-01-01 \
     --end-date 2024-01-07
   ```

3. **Monitor resource usage:**
   - Check concurrent task execution
   - Monitor system resource consumption
   - Validate data processing accuracy

## Files to Create

Create your solutions in the following files:

- `dags/historical_sales_migration.py`
- `dags/system_recovery_processing.py`
- `dags/incremental_data_processing.py`
- `dags/multi_source_sync.py`

## Success Criteria

- [ ] DAGs handle catchup behavior correctly
- [ ] Backfill scenarios work as expected
- [ ] Resource usage is controlled with max_active_runs
- [ ] Data validation is implemented for each scenario
- [ ] Error handling and retry logic is appropriate
- [ ] Logging provides clear visibility into processing status
- [ ] DAGs can recover gracefully from failures

## Troubleshooting Common Issues

### Issue 1: Too Many Concurrent Runs

**Solution:** Adjust `max_active_runs` and `max_active_tasks_per_dag`

### Issue 2: Memory Issues During Backfill

**Solution:** Process smaller date ranges, optimize task memory usage

### Issue 3: Data Inconsistency

**Solution:** Implement proper data validation and rollback mechanisms

## Next Steps

After completing this exercise, you'll understand how to design robust data pipelines that can handle historical processing requirements and system recovery scenarios.
