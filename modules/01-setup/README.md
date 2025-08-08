# Module 1: Setup & Introduction

Welcome to the Airflow Coding Kata! This module will get you up and running with Apache Airflow and introduce you to the fundamental concepts of workflow orchestration.

## Learning Objectives

By the end of this module, you will be able to:

- Set up a local Airflow development environment using Docker
- Understand the basic concepts of workflow orchestration and why it matters
- Navigate the Airflow web interface confidently
- Identify key components of the Airflow architecture
- Create and run your first DAG
- Understand the relationship between DAGs, tasks, and operators

## Prerequisites

- Basic Python knowledge (functions, classes, imports)
- Docker installed on your system ([Installation Guide](https://docs.docker.com/get-docker/))
- Text editor or IDE (VS Code, PyCharm, etc.)
- Command line familiarity

## Setup Instructions

### 1. Verify Docker Installation

First, ensure Docker is running on your system:

```bash
docker --version
docker-compose --version
```

You should see version information for both commands.

### 2. Start Airflow Environment

From the root directory of this kata, run:

```bash
docker-compose up -d
```

This will start:

- Airflow webserver (http://localhost:8080)
- Airflow scheduler
- PostgreSQL database
- Redis (for task queuing)

### 3. Access Airflow UI

1. Open your browser and navigate to http://localhost:8080
2. Login with:
   - Username: `airflow`
   - Password: `airflow`

### 4. Verify Setup

You should see the Airflow dashboard with example DAGs. If you encounter issues, check the troubleshooting section below.

## Module Structure

- `concepts.md` - Core Airflow concepts and workflow orchestration fundamentals
- `exercises/` - Hands-on exercises for this module
- `examples/` - Working code examples
- `solutions/` - Exercise solutions
- `resources.md` - Additional learning resources

## Troubleshooting

### Common Issues

**Port 8080 already in use:**

```bash
# Stop any existing services on port 8080
sudo lsof -ti:8080 | xargs kill -9
# Or modify docker-compose.yml to use a different port
```

**Docker daemon not running:**

```bash
# On macOS/Linux
sudo systemctl start docker
# On Windows, start Docker Desktop
```

**Permission denied errors:**

```bash
# On Linux, you may need to add your user to the docker group
sudo usermod -aG docker $USER
# Then log out and back in
```

**Containers won't start:**

```bash
# Check logs for specific errors
docker-compose logs
# Clean up and restart
docker-compose down -v
docker-compose up -d
```

## Estimated Time

45-60 minutes

## What's Next

After completing this module, you'll have:

- A working Airflow environment
- Understanding of workflow orchestration concepts
- Familiarity with the Airflow UI
- Your first DAG running successfully

Proceed to Module 2: DAG Fundamentals to dive deeper into DAG creation and configuration.
