# Airflow Coding Kata - Frequently Asked Questions

This FAQ addresses common questions that arise while working through the Airflow Coding Kata.

## Table of Contents

- [Getting Started](#getting-started)
- [Environment Setup](#environment-setup)
- [DAG Development](#dag-development)
- [Task Operations](#task-operations)
- [Scheduling and Execution](#scheduling-and-execution)
- [Data Management](#data-management)
- [Troubleshooting](#troubleshooting)
- [Best Practices](#best-practices)
- [Advanced Topics](#advanced-topics)

## Getting Started

### Q: What is Apache Airflow and why should I learn it?

**A:** Apache Airflow is an open-source platform for developing, scheduling, and monitoring workflows. It's essential for:

- **Data Engineering**: Building ETL/ELT pipelines
- **MLOps**: Orchestrating machine learning workflows
- **DevOps**: Automating deployment and maintenance tasks
- **Business Process Automation**: Scheduling recurring business operations

Learning Airflow is valuable because it's widely adopted in the industry and provides a robust solution for workflow orchestration.

### Q: What prerequisites do I need for this kata?

**A:** You should have:

- **Basic Python knowledge**: Understanding of functions, classes, and modules
- **Command line familiarity**: Basic terminal/command prompt usage
- **Docker basics**: Understanding of containers (helpful but not required)
- **SQL knowledge**: Basic queries (helpful for later modules)

### Q: How long does it take to complete the kata?

**A:** The time varies by experience level:

- **Beginners**: 20-30 hours over 2-4 weeks
- **Intermediate**: 15-20 hours over 1-2 weeks
- **Experienced developers**: 10-15 hours over 1 week

Each module is designed to take 1-3 hours depending on complexity.

### Q: Can I skip modules if I already know some concepts?

**A:** Yes, but we recommend:

1. **Review the concepts**: Even familiar topics may have Airflow-specific nuances
2. **Check prerequisites**: Later modules build on earlier ones
3. **Do the exercises**: Hands-on practice reinforces learning
4. **Use as reference**: Skip ahead but return when needed

## Environment Setup

### Q: Do I need to install Airflow locally?

**A:** No! The kata uses Docker Compose to provide a complete Airflow environment. This approach:

- **Eliminates installation issues**: No need to manage Python environments
- **Ensures consistency**: Everyone uses the same Airflow version
- **Simplifies cleanup**: Easy to reset or remove
- **Matches production**: Similar to how Airflow is deployed

### Q: What if I can't use Docker?

**A:** While Docker is recommended, you can:

1. **Install Airflow locally**: Follow [official installation guide](https://airflow.apache.org/docs/apache-airflow/stable/installation/)
2. **Use cloud services**: Try Airflow on Google Cloud Composer or AWS MWAA
3. **Use virtual machines**: Set up Airflow in a VM

Note: Instructions assume Docker setup, so you'll need to adapt commands.

### Q: Why is the initial setup taking so long?

**A:** First-time setup involves:

- **Downloading Docker images**: Can be 1-2 GB depending on your connection
- **Database initialization**: PostgreSQL setup and Airflow metadata creation
- **Service startup**: Multiple containers need to start and become healthy

Subsequent starts are much faster (30-60 seconds).

### Q: How much disk space and memory do I need?

**A:** Recommended requirements:

- **Disk space**: 5-10 GB free space
- **Memory**: 8 GB RAM (4 GB minimum)
- **CPU**: 2+ cores recommended

Docker Desktop settings should allocate at least 4 GB RAM to containers.

## DAG Development

### Q: What's the difference between a DAG and a workflow?

**A:**

- **Workflow**: General term for a sequence of tasks
- **DAG**: Directed Acyclic Graph - Airflow's specific implementation of workflows
- **Key characteristics**:
  - Directed: Tasks have clear order/dependencies
  - Acyclic: No circular dependencies allowed
  - Graph: Visual representation of task relationships

### Q: How do I choose between different operators?

**A:** Choose based on what your task needs to do:

- **PythonOperator**: Run Python functions (most flexible)
- **BashOperator**: Execute shell commands or scripts
- **SQLOperator**: Run SQL queries against databases
- **EmailOperator**: Send email notifications
- **Custom Operators**: For specialized or reusable logic

**Rule of thumb**: Start with PythonOperator for most tasks, use specialized operators for specific needs.

### Q: Should I put all my logic in the DAG file?

**A:** No! Best practices:

- **Keep DAGs lightweight**: Only workflow definition
- **Separate business logic**: Put in modules or packages
- **Use functions**: Import and call from DAG tasks
- **Avoid heavy imports**: Don't import large libraries at DAG level

```python
# Good: Lightweight DAG
from my_modules.data_processing import process_data

def my_task():
    return process_data()

# Bad: Heavy logic in DAG
def my_task():
    # 100 lines of processing logic here...
```

### Q: How do I handle different environments (dev, staging, prod)?

**A:** Use Airflow Variables and Connections:

```python
from airflow.models import Variable

# Environment-specific configuration
env = Variable.get("environment", default_var="dev")
database_url = Variable.get(f"database_url_{env}")

# Or use Connections for external services
from airflow.hooks.base import BaseHook
conn = BaseHook.get_connection("my_database")
```

## Task Operations

### Q: How do I pass data between tasks?

**A:** Airflow provides several methods:

1. **XComs** (recommended for small data):

```python
# Push data
def push_task(**context):
    return {"key": "value"}

# Pull data
def pull_task(**context):
    data = context['ti'].xcom_pull(task_ids='push_task')
```

2. **External storage** (for large data):

```python
# Save to file/database in one task, read in another
def save_data(**context):
    data.to_csv('/shared/data.csv')

def load_data(**context):
    data = pd.read_csv('/shared/data.csv')
```

3. **Database tables**: Store intermediate results in database

### Q: What's the maximum size for XCom data?

**A:** XCom limitations:

- **Default backend**: ~48KB (varies by database)
- **Practical limit**: Keep under 1KB for performance
- **Large data**: Use external storage (S3, database, shared filesystem)

### Q: How do I handle task failures?

**A:** Multiple strategies:

1. **Retries**: Automatic retry with backoff

```python
task = PythonOperator(
    task_id='my_task',
    retries=3,
    retry_delay=timedelta(minutes=5)
)
```

2. **Callbacks**: Custom logic on failure

```python
def failure_callback(context):
    # Send alert, log error, etc.
    pass

task = PythonOperator(
    task_id='my_task',
    on_failure_callback=failure_callback
)
```

3. **Trigger rules**: Control when tasks run

```python
cleanup_task = PythonOperator(
    task_id='cleanup',
    trigger_rule='all_done'  # Run even if upstream fails
)
```

## Scheduling and Execution

### Q: How does Airflow scheduling work?

**A:** Key concepts:

- **Schedule interval**: How often DAG runs (e.g., daily, hourly)
- **Start date**: When scheduling begins
- **Execution date**: The data interval being processed (not when task runs)
- **Catchup**: Whether to run missed intervals

**Example**: DAG with daily schedule starting Jan 1st:

- First run: Jan 2nd at midnight (processes Jan 1st data)
- Second run: Jan 3rd at midnight (processes Jan 2nd data)

### Q: Why isn't my DAG running when expected?

**A:** Common issues:

1. **Start date in future**: DAG won't run until start_date passes
2. **Catchup disabled**: Won't run historical intervals
3. **DAG paused**: Check if DAG is turned on in UI
4. **Schedule interval**: Verify cron expression or preset
5. **Dependencies**: Upstream tasks may be failing

### Q: What's the difference between `@daily` and `timedelta(days=1)`?

**A:**

- **`@daily`**: Runs at midnight (00:00) each day
- **`timedelta(days=1)`**: Runs 24 hours after the previous run
- **`@hourly`**: Runs at the top of each hour (XX:00)
- **`timedelta(hours=1)`**: Runs 1 hour after the previous run

Use presets (`@daily`, `@hourly`) for regular schedules, timedeltas for relative timing.

### Q: How do I run a DAG only once?

**A:** Several options:

```python
# Option 1: No schedule (manual trigger only)
schedule_interval=None

# Option 2: Run once when turned on
schedule_interval='@once'

# Option 3: Specific date range
start_date=datetime(2024, 1, 1),
end_date=datetime(2024, 1, 1),
schedule_interval='@daily'
```

## Data Management

### Q: How do I work with databases in Airflow?

**A:** Use Connections and Hooks:

1. **Set up Connection**: Admin → Connections in UI
2. **Use appropriate Hook**:

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook

def database_task(**context):
    hook = PostgresHook(postgres_conn_id='my_postgres')
    results = hook.get_records("SELECT * FROM my_table")
    return results
```

### Q: How do I handle file operations?

**A:** Best practices:

- **Use shared storage**: Mount volumes in Docker
- **Check file existence**: Before processing
- **Handle permissions**: Ensure Airflow can read/write
- **Clean up**: Remove temporary files

```python
import os
from pathlib import Path

def file_task(**context):
    file_path = Path('/shared/data/input.csv')

    if not file_path.exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")

    # Process file
    # ...

    # Clean up if needed
    temp_file.unlink()
```

### Q: How do I work with APIs in Airflow?

**A:** Use HTTP operators or custom functions:

```python
from airflow.providers.http.operators.http import SimpleHttpOperator

# Using HTTP operator
api_task = SimpleHttpOperator(
    task_id='call_api',
    http_conn_id='my_api',
    endpoint='data',
    method='GET'
)

# Or custom function
import requests

def api_task(**context):
    response = requests.get('https://api.example.com/data')
    response.raise_for_status()
    return response.json()
```

## Troubleshooting

### Q: My DAG isn't showing up in the UI. What should I check?

**A:** Debug checklist:

1. **File syntax**: `python dags/your_dag.py`
2. **File location**: Must be in `dags/` directory
3. **DAG object**: Must have a DAG instance in global scope
4. **Unique DAG ID**: No duplicates allowed
5. **Scheduler logs**: Check for import errors
6. **Refresh**: Click refresh button in UI

### Q: Tasks are stuck in "running" state. What do I do?

**A:** Investigation steps:

1. **Check logs**: Task logs in UI or container logs
2. **Resource usage**: CPU/memory constraints
3. **Deadlocks**: Database or external service issues
4. **Infinite loops**: Code that never completes
5. **Kill task**: Mark as failed and investigate

### Q: How do I debug XCom issues?

**A:** Debugging approach:

1. **Check XCom UI**: Admin → XComs to see stored data
2. **Verify task completion**: Upstream task must succeed
3. **Check task IDs**: Must match exactly in xcom_pull
4. **Data serialization**: Must be JSON serializable
5. **Size limits**: Keep data small

### Q: Performance is slow. How do I optimize?

**A:** Optimization strategies:

1. **Reduce DAG complexity**: Fewer tasks, simpler logic
2. **Optimize imports**: Avoid heavy imports at module level
3. **Increase resources**: More CPU/memory for containers
4. **Parallel execution**: Use task pools and parallelism
5. **Database tuning**: Optimize Airflow metadata database

## Best Practices

### Q: What are the most important Airflow best practices?

**A:** Top recommendations:

1. **Keep DAGs simple**: One responsibility per DAG
2. **Idempotent tasks**: Tasks should produce same result when rerun
3. **Atomic operations**: Tasks should be all-or-nothing
4. **Proper error handling**: Retries, callbacks, monitoring
5. **Resource management**: Clean up temporary resources
6. **Documentation**: Clear task and DAG descriptions
7. **Testing**: Unit test DAG structure and task logic

### Q: How should I organize my Airflow project?

**A:** Recommended structure:

```
airflow-project/
├── dags/                    # DAG definitions
│   ├── data_pipeline.py
│   └── ml_training.py
├── plugins/                 # Custom operators, hooks
│   ├── operators/
│   └── hooks/
├── modules/                 # Business logic
│   ├── data_processing/
│   └── ml_models/
├── tests/                   # Unit tests
├── config/                  # Configuration files
└── requirements.txt
```

### Q: How do I handle secrets and credentials?

**A:** Security best practices:

1. **Use Connections**: Store in Airflow UI, not code
2. **Environment variables**: For container-level secrets
3. **External secret managers**: AWS Secrets Manager, HashiCorp Vault
4. **Never hardcode**: No passwords or API keys in DAG files
5. **Encrypt connections**: Use Fernet key for encryption

### Q: Should I use SubDAGs or TaskGroups?

**A:** **TaskGroups** are recommended over SubDAGs:

- **TaskGroups**: Lightweight, better UI, easier debugging
- **SubDAGs**: Legacy, complex, performance issues

```python
from airflow.utils.task_group import TaskGroup

with TaskGroup("data_processing") as processing_group:
    extract = PythonOperator(...)
    transform = PythonOperator(...)
    load = PythonOperator(...)

    extract >> transform >> load
```

## Advanced Topics

### Q: How do I create custom operators?

**A:** Extend BaseOperator:

```python
from airflow.models.baseoperator import BaseOperator

class MyCustomOperator(BaseOperator):
    def __init__(self, my_param, **kwargs):
        super().__init__(**kwargs)
        self.my_param = my_param

    def execute(self, context):
        # Your custom logic here
        self.log.info(f"Executing with param: {self.my_param}")
        return "success"
```

### Q: How do I implement branching logic?

**A:** Use BranchPythonOperator:

```python
from airflow.operators.python import BranchPythonOperator

def choose_branch(**context):
    # Your decision logic
    if condition:
        return 'task_a'
    else:
        return 'task_b'

branch = BranchPythonOperator(
    task_id='branching',
    python_callable=choose_branch
)

branch >> [task_a, task_b]
```

### Q: How do I handle dynamic task generation?

**A:** Create tasks programmatically:

```python
# Generate tasks based on data
for i in range(5):
    task = PythonOperator(
        task_id=f'dynamic_task_{i}',
        python_callable=my_function,
        op_args=[i],
        dag=dag
    )
```

### Q: How do I monitor DAG performance?

**A:** Monitoring strategies:

1. **Airflow UI**: Built-in metrics and graphs
2. **SLA monitoring**: Set task and DAG SLAs
3. **Custom metrics**: Push to external monitoring systems
4. **Alerting**: Email, Slack, or custom notifications
5. **Logging**: Comprehensive logging for debugging

### Q: How do I scale Airflow for production?

**A:** Production considerations:

1. **Executor choice**: CeleryExecutor or KubernetesExecutor for scaling
2. **Database**: Use PostgreSQL or MySQL, not SQLite
3. **Resource allocation**: Proper CPU/memory sizing
4. **High availability**: Multiple scheduler instances (Airflow 2.0+)
5. **Monitoring**: Comprehensive observability setup
6. **Security**: Authentication, authorization, network security

## Getting More Help

### Q: Where can I find more resources?

**A:** Additional learning resources:

- **Official Documentation**: [airflow.apache.org](https://airflow.apache.org/)
- **Community**: [Apache Airflow Slack](https://apache-airflow-slack.herokuapp.com/)
- **Stack Overflow**: Tag questions with `apache-airflow`
- **GitHub**: [apache/airflow](https://github.com/apache/airflow) for issues and source
- **Conferences**: Airflow Summit, data engineering conferences
- **Courses**: Online platforms with Airflow courses

### Q: How do I contribute to the Airflow community?

**A:** Ways to contribute:

1. **Report bugs**: Submit issues on GitHub
2. **Documentation**: Improve docs and examples
3. **Code contributions**: Fix bugs or add features
4. **Community support**: Help others on Slack/Stack Overflow
5. **Content creation**: Write blogs, tutorials, or talks
6. **Testing**: Test new releases and provide feedback

### Q: What's next after completing this kata?

**A:** Continue your learning journey:

1. **Build real projects**: Apply skills to actual use cases
2. **Explore providers**: Learn specific integrations (AWS, GCP, etc.)
3. **Advanced patterns**: Study complex workflow patterns
4. **Production deployment**: Learn about scaling and operations
5. **Related tools**: Explore dbt, Kubernetes, cloud platforms
6. **Contribute**: Give back to the community

Remember: The best way to learn Airflow is by building real workflows. Start with simple use cases and gradually increase complexity as you gain experience.
