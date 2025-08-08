# Airflow Coding Kata - Troubleshooting Guide

This guide provides solutions to common issues you might encounter while working through the Airflow Coding Kata.

## Table of Contents

- [Environment Setup Issues](#environment-setup-issues)
- [Docker-Related Problems](#docker-related-problems)
- [DAG Development Issues](#dag-development-issues)
- [Airflow UI Problems](#airflow-ui-problems)
- [Task Execution Issues](#task-execution-issues)
- [Common Python Errors](#common-python-errors)
- [Performance Issues](#performance-issues)
- [FAQ](#frequently-asked-questions)

## Environment Setup Issues

### Docker Installation Problems

**Problem**: Docker is not installed or not running

```bash
docker: command not found
```

**Solution**:

1. Install Docker Desktop from [docker.com](https://www.docker.com/products/docker-desktop)
2. Start Docker Desktop application
3. Verify installation: `docker --version`

**Problem**: Docker Compose not found

```bash
docker-compose: command not found
```

**Solution**:

- For newer Docker versions, use `docker compose` (without hyphen)
- Or install docker-compose separately: `pip install docker-compose`

### Port Conflicts

**Problem**: Port 8080 already in use

```bash
Error: Port 8080 is already allocated
```

**Solution**:

1. Find process using port: `lsof -i :8080` (macOS/Linux) or `netstat -ano | findstr :8080` (Windows)
2. Kill the process or change Airflow port in `docker-compose.yml`:

```yaml
ports:
  - "8081:8080" # Change external port to 8081
```

### Permission Issues

**Problem**: Permission denied when running Docker commands

**Solution**:

- macOS/Linux: Add user to docker group: `sudo usermod -aG docker $USER`
- Windows: Run Docker Desktop as administrator
- Alternative: Use `sudo` with docker commands (not recommended for production)

## Docker-Related Problems

### Container Startup Issues

**Problem**: Airflow containers fail to start

```bash
airflow-webserver exited with code 1
```

**Solution**:

1. Check logs: `docker-compose logs airflow-webserver`
2. Ensure all required environment variables are set
3. Verify database initialization: `docker-compose up airflow-init`
4. Clean restart: `docker-compose down -v && docker-compose up`

**Problem**: Database connection errors

```bash
sqlalchemy.exc.OperationalError: (psycopg2.OperationalError)
```

**Solution**:

1. Ensure PostgreSQL container is running: `docker-compose ps`
2. Wait for database to be ready (may take 30-60 seconds on first start)
3. Check database credentials in `docker-compose.yml`
4. Reset database: `docker-compose down -v && docker-compose up airflow-init`

### Volume Mount Issues

**Problem**: DAGs not appearing in Airflow UI

**Solution**:

1. Verify volume mounts in `docker-compose.yml`:

```yaml
volumes:
  - ./dags:/opt/airflow/dags
```

2. Check file permissions: `chmod -R 755 ./dags`
3. Restart containers: `docker-compose restart`

## DAG Development Issues

### DAG Import Errors

**Problem**: DAG fails to import

```python
ImportError: No module named 'your_module'
```

**Solution**:

1. Ensure all required packages are in `requirements.txt`
2. Rebuild containers: `docker-compose build`
3. Check Python path in DAG file
4. Verify file is in correct directory structure

**Problem**: DAG not showing in UI

```bash
DAG seems to be missing
```

**Solution**:

1. Check DAG file syntax: `python your_dag.py`
2. Ensure DAG has unique `dag_id`
3. Verify file is in `dags/` directory
4. Check Airflow logs: `docker-compose logs airflow-scheduler`
5. Refresh DAGs: Click "Refresh" in Airflow UI

### Syntax and Configuration Errors

**Problem**: DAG parsing errors

```python
SyntaxError: invalid syntax
```

**Solution**:

1. Use Python syntax checker: `python -m py_compile your_dag.py`
2. Check indentation (use spaces, not tabs)
3. Verify all imports are correct
4. Ensure proper DAG context manager usage

**Problem**: Task dependency errors

```python
AirflowException: Task dependencies are not valid
```

**Solution**:

1. Check task IDs are unique within DAG
2. Verify dependency syntax:

```python
# Correct
task1 >> task2 >> task3

# Or
task1.set_downstream(task2)
task2.set_downstream(task3)
```

3. Avoid circular dependencies

## Airflow UI Problems

### Login Issues

**Problem**: Cannot access Airflow UI at localhost:8080

**Solution**:

1. Verify containers are running: `docker-compose ps`
2. Check if webserver is healthy: `docker-compose logs airflow-webserver`
3. Try different browser or incognito mode
4. Clear browser cache and cookies

**Problem**: Authentication errors

**Solution**:

1. Use default credentials: username `airflow`, password `airflow`
2. Reset admin user:

```bash
docker-compose exec airflow-webserver airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin
```

### UI Performance Issues

**Problem**: Airflow UI is slow or unresponsive

**Solution**:

1. Increase Docker memory allocation (4GB+ recommended)
2. Reduce DAG parsing frequency in `airflow.cfg`:

```ini
[scheduler]
dag_dir_list_interval = 300
```

3. Limit number of DAGs in development environment

## Task Execution Issues

### Task Failure Debugging

**Problem**: Tasks fail without clear error messages

**Solution**:

1. Check task logs in Airflow UI (click on task → View Log)
2. Increase log verbosity in `airflow.cfg`:

```ini
[logging]
logging_level = DEBUG
```

3. Add debug prints to your task functions
4. Check container logs: `docker-compose logs airflow-worker`

**Problem**: PythonOperator import errors

```python
ModuleNotFoundError: No module named 'your_module'
```

**Solution**:

1. Ensure module is in Python path
2. Use absolute imports in task functions
3. Add modules to `plugins/` directory
4. Install packages in container: add to `requirements.txt`

### XCom Issues

**Problem**: XCom data not passing between tasks

```python
KeyError: 'task_instance'
```

**Solution**:

1. Verify task IDs are correct in `xcom_pull`
2. Check task execution order (upstream task must complete first)
3. Ensure data is JSON serializable
4. Use proper XCom syntax:

```python
# Push
return value  # or ti.xcom_push(key='my_key', value=value)

# Pull
value = ti.xcom_pull(task_ids='upstream_task_id')
```

## Common Python Errors

### Import and Path Issues

**Problem**: Module import errors in DAG files

**Solution**:

1. Use absolute imports from project root
2. Add project root to Python path:

```python
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
```

3. Structure imports properly in DAG files

### Date and Time Issues

**Problem**: Timezone and datetime errors

```python
TypeError: can't compare offset-naive and offset-aware datetimes
```

**Solution**:

1. Always use timezone-aware datetimes:

```python
from datetime import datetime
from airflow.utils.dates import days_ago

start_date = days_ago(1)  # Recommended
# Or
start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
```

2. Set timezone in `airflow.cfg`:

```ini
[core]
default_timezone = UTC
```

## Performance Issues

### Slow DAG Processing

**Problem**: DAGs take long time to appear or update

**Solution**:

1. Reduce DAG complexity and file size
2. Optimize imports (avoid heavy imports at module level)
3. Use DAG serialization:

```ini
[core]
store_serialized_dags = True
```

4. Increase scheduler resources in `docker-compose.yml`

### Memory Issues

**Problem**: Out of memory errors

**Solution**:

1. Increase Docker memory limits
2. Reduce concurrent task execution:

```ini
[celery]
worker_concurrency = 2
```

3. Optimize task memory usage
4. Use task pools to limit concurrent execution

## Frequently Asked Questions

### General Questions

**Q: How do I reset the entire Airflow environment?**

A: Run the following commands:

```bash
docker-compose down -v  # Remove containers and volumes
docker-compose up airflow-init  # Reinitialize database
docker-compose up  # Start services
```

**Q: How do I add new Python packages?**

A:

1. Add package to `requirements.txt`
2. Rebuild containers: `docker-compose build`
3. Restart services: `docker-compose up

**Q: Why are my DAG changes not reflected in the UI?**

A:

1. DAG files are parsed periodically (default: every 30 seconds)
2. Force refresh by clicking "Refresh" in UI
3. Check for syntax errors in DAG file
4. Restart scheduler if needed: `docker-compose restart airflow-scheduler`

### Development Questions

**Q: How do I debug a failing task?**

A:

1. Check task logs in Airflow UI
2. Add print statements or logging to your task function
3. Test task function independently outside Airflow
4. Use Airflow's test command:

```bash
docker-compose exec airflow-webserver airflow tasks test dag_id task_id 2024-01-01
```

**Q: How do I handle secrets and sensitive data?**

A:

1. Use Airflow Variables: Admin → Variables in UI
2. Use Connections: Admin → Connections in UI
3. Environment variables in `docker-compose.yml`
4. Never hardcode secrets in DAG files

**Q: How do I schedule a DAG to run only once?**

A:

```python
dag = DAG(
    'one_time_dag',
    schedule_interval=None,  # Manual trigger only
    # or
    schedule_interval='@once',  # Run once when turned on
)
```

### Troubleshooting Commands

**Check Airflow version:**

```bash
docker-compose exec airflow-webserver airflow version
```

**List all DAGs:**

```bash
docker-compose exec airflow-webserver airflow dags list
```

**Test DAG parsing:**

```bash
docker-compose exec airflow-webserver python /opt/airflow/dags/your_dag.py
```

**Check scheduler health:**

```bash
docker-compose exec airflow-scheduler airflow jobs check --job-type SchedulerJob --hostname $(hostname)
```

**View configuration:**

```bash
docker-compose exec airflow-webserver airflow config list
```

## Getting Help

If you're still experiencing issues after trying these solutions:

1. Check the [Apache Airflow Documentation](https://airflow.apache.org/docs/)
2. Search [Airflow GitHub Issues](https://github.com/apache/airflow/issues)
3. Ask questions on [Stack Overflow](https://stackoverflow.com/questions/tagged/airflow) with the `airflow` tag
4. Join the [Apache Airflow Slack Community](https://apache-airflow-slack.herokuapp.com/)

## Contributing to This Guide

Found a solution to a problem not covered here? Please contribute by:

1. Adding the issue and solution to this guide
2. Testing the solution thoroughly
3. Following the existing format and style
4. Submitting a pull request with your changes

Remember: The best troubleshooting approach is often to start with the simplest solution and work your way up to more complex fixes.
