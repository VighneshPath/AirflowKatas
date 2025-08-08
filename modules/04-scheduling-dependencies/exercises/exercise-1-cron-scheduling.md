# Exercise 1: Advanced Cron Scheduling

## Objective

Practice creating DAGs with complex scheduling patterns using cron expressions to meet various business requirements.

## Background

Your company has different data processing needs that require specific scheduling patterns. You need to create DAGs that handle these various scheduling requirements efficiently.

## Tasks

### Task 1: Peak Hours Processing

Create a DAG that processes high-priority data during peak business hours.

**Requirements:**

- DAG name: `peak_hours_processing`
- Schedule: Every 30 minutes between 8 AM and 6 PM on weekdays
- Should not catch up on missed runs
- Include a task that logs the current processing time

**Hints:**

- Use cron expression: `*/30 8-18 * * 1-5`
- Set `catchup=False` to avoid processing historical data
- Use BashOperator or PythonOperator to log timestamps

### Task 2: Weekend Maintenance

Create a DAG for weekend maintenance tasks that should run less frequently.

**Requirements:**

- DAG name: `weekend_maintenance`
- Schedule: Every 4 hours on weekends only
- Should process historical runs if the DAG was down
- Include tasks for system cleanup and health checks

**Hints:**

- Weekend days: Saturday (6) and Sunday (0 or 7)
- Cron expression: `0 */4 * * 0,6`
- Set `catchup=True` for maintenance consistency

### Task 3: Month-End Financial Processing

Create a DAG that handles month-end financial calculations.

**Requirements:**

- DAG name: `month_end_financial`
- Schedule: Last 3 days of every month at 10 PM
- Must handle timezone (use your local timezone)
- Should limit to one active run at a time
- Include validation to check if it's actually month-end

**Hints:**

- Use days 29-31: `0 22 29-31 * *`
- Import timezone from pendulum
- Set `max_active_runs=1`
- Create a Python function to validate the last day of month

### Task 4: Quarterly Report Generation

Create a DAG for quarterly business reports.

**Requirements:**

- DAG name: `quarterly_reports`
- Schedule: First day of each quarter (Jan, Apr, Jul, Oct) at 6 AM
- Should process all historical quarters since start date
- Include tasks for data extraction, processing, and report generation
- Add appropriate retry logic

**Hints:**

- Quarterly months: 1,4,7,10
- Cron expression: `0 6 1 1,4,7,10 *`
- Use default_args for retry configuration
- Chain multiple tasks for the report pipeline

## Validation

Test your DAGs by:

1. Checking that they parse correctly in the Airflow UI
2. Verifying the schedule intervals show the expected next run times
3. Testing with different start dates to see catchup behavior
4. Examining the DAG graph to ensure task dependencies are correct

## Files to Create

Create your solutions in the following files:

- `dags/peak_hours_processing.py`
- `dags/weekend_maintenance.py`
- `dags/month_end_financial.py`
- `dags/quarterly_reports.py`

## Success Criteria

- [ ] All DAGs parse without errors
- [ ] Cron expressions match the specified requirements
- [ ] Catchup behavior is configured correctly
- [ ] Timezone handling is implemented where required
- [ ] Tasks include appropriate logging and validation
- [ ] DAGs show expected scheduling in the Airflow UI

## Next Steps

After completing this exercise, you'll be ready to tackle dependency patterns in Exercise 2.
