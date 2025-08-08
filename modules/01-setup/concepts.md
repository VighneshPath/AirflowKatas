# Airflow Concepts & Workflow Orchestration

## What is Workflow Orchestration?

Workflow orchestration is the automated coordination and management of complex data workflows. Think of it as a conductor directing an orchestra - each musician (task) plays their part at the right time, in the right order, to create a harmonious performance (successful data pipeline).

### Why Do We Need Workflow Orchestration?

In modern data engineering, we often need to:

- **Process data in sequence**: Extract → Transform → Load (ETL)
- **Handle dependencies**: Task B can only run after Task A completes successfully
- **Manage failures**: Retry failed tasks, send alerts, or trigger alternative workflows
- **Schedule work**: Run daily reports, weekly aggregations, or real-time processing
- **Monitor progress**: Track which tasks are running, failed, or completed

### Traditional Approaches vs. Orchestration

**Without Orchestration (Cron Jobs):**

```bash
# Fragile, hard to manage dependencies
0 2 * * * /scripts/extract_data.sh
30 2 * * * /scripts/transform_data.sh
0 3 * * * /scripts/load_data.sh
```

**With Orchestration (Airflow):**

- Visual workflow representation
- Automatic dependency management
- Built-in retry logic and error handling
- Rich monitoring and logging
- Dynamic workflow generation

## What is Apache Airflow?

Apache Airflow is an open-source platform for developing, scheduling, and monitoring workflows. It allows you to define workflows as code using Python, making them maintainable, versionable, and testable.

### Key Benefits

1. **Workflows as Code**: Define workflows in Python with version control
2. **Rich UI**: Visual representation of workflows and their status
3. **Extensible**: Hundreds of pre-built operators and hooks
4. **Scalable**: Distributed execution across multiple workers
5. **Active Community**: Large ecosystem and continuous development

## Core Airflow Concepts

### 1. DAG (Directed Acyclic Graph)

A DAG represents a workflow - a collection of tasks with dependencies between them.

**Key Properties:**

- **Directed**: Tasks have a specific order/direction
- **Acyclic**: No circular dependencies (Task A → Task B → Task A is not allowed)
- **Graph**: Visual representation of task relationships

```python
# Simple DAG structure
Task A → Task B → Task C
         ↓
       Task D
```

### 2. Tasks

Tasks are the individual units of work within a DAG. Each task represents a single operation like:

- Running a SQL query
- Calling an API
- Processing a file
- Sending an email

### 3. Operators

Operators define what a task actually does. They are templates for creating tasks.

**Common Operators:**

- `BashOperator`: Execute bash commands
- `PythonOperator`: Run Python functions
- `SQLOperator`: Execute SQL queries
- `EmailOperator`: Send emails
- `FileSensor`: Wait for files to appear

### 4. Task Instances

A task instance is a specific run of a task for a particular execution date. If a DAG runs daily, each day creates new task instances.

### 5. DAG Runs

A DAG run is a specific execution of a DAG for a particular execution date.

## Airflow Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Web Server    │    │    Scheduler    │    │    Executor    │
│                 │    │                 │    │                 │
│ - UI Dashboard  │    │ - Monitors DAGs │    │ - Runs Tasks   │
│ - REST API      │    │ - Creates Runs  │    │ - Manages Queue│
│ - User Auth     │    │ - Schedules     │    │ - Workers      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │    Metadata     │
                    │    Database     │
                    │                 │
                    │ - DAG Info      │
                    │ - Task Status   │
                    │ - Connections   │
                    │ - Variables     │
                    └─────────────────┘
```

### Components Explained

**Web Server:**

- Provides the user interface at http://localhost:8080
- REST API for programmatic access
- User authentication and authorization

**Scheduler:**

- Heart of Airflow - monitors DAG files
- Creates DAG runs based on schedules
- Submits tasks to the executor when dependencies are met

**Executor:**

- Runs the actual tasks
- Different types: Sequential, Local, Celery, Kubernetes
- Manages task queues and worker processes

**Metadata Database:**

- Stores all Airflow state information
- DAG definitions, task instances, connections, variables
- PostgreSQL in our Docker setup

## Workflow Lifecycle

1. **DAG Definition**: Write Python code defining your workflow
2. **DAG Parsing**: Scheduler reads and parses DAG files
3. **DAG Scheduling**: Scheduler creates DAG runs based on schedule
4. **Task Queuing**: Tasks with satisfied dependencies are queued
5. **Task Execution**: Executor runs tasks on available workers
6. **State Updates**: Task results update the metadata database
7. **Monitoring**: View progress and results in the web UI

## Best Practices Preview

While we'll cover these in detail later, here are key principles:

1. **Idempotent Tasks**: Tasks should produce the same result when run multiple times
2. **Atomic Operations**: Each task should be a single, focused operation
3. **Proper Dependencies**: Clearly define what must happen before each task
4. **Error Handling**: Plan for failures with retries and alerts
5. **Resource Management**: Consider memory, CPU, and I/O requirements

## Real-World Example

Consider a daily sales report workflow:

```
Extract Sales Data → Clean Data → Calculate Metrics → Generate Report → Send Email
                                      ↓
                                 Update Dashboard
```

This becomes an Airflow DAG with:

- 5 tasks with clear dependencies
- Daily schedule (runs every morning)
- Error handling (retry failed extractions)
- Monitoring (alerts if report generation fails)

## What's Next

Now that you understand the fundamental concepts, let's get hands-on! In the exercises, you'll:

1. Explore the Airflow UI
2. Examine example DAGs
3. Create your first simple workflow
4. Run and monitor your DAG

The beauty of Airflow is that these concepts become intuitive once you start building workflows. Let's dive in!
