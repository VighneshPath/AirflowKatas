"""
Pytest configuration and fixtures for Airflow Coding Kata tests.

This module provides common fixtures and configuration for testing DAGs,
modules, and the overall kata structure.
"""

import os
import sys
import pytest
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import Mock

# Add project root to Python path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "dags"))

# Mock Airflow imports for testing without full Airflow installation
try:
    from airflow import DAG
    from airflow.models import TaskInstance
    from airflow.utils.state import State
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False


@pytest.fixture
def mock_airflow():
    """Mock Airflow components for testing without full installation."""
    if AIRFLOW_AVAILABLE:
        return None

    # Create mock DAG class
    mock_dag = Mock()
    mock_dag.dag_id = "test_dag"
    mock_dag.start_date = datetime(2024, 1, 1)

    # Create mock TaskInstance
    mock_ti = Mock()
    mock_ti.task_id = "test_task"
    mock_ti.dag_id = "test_dag"
    mock_ti.execution_date = datetime(2024, 1, 1)
    mock_ti.state = State.SUCCESS

    return {
        'dag': mock_dag,
        'task_instance': mock_ti
    }


@pytest.fixture
def sample_dag_content():
    """Sample DAG content for testing."""
    return '''
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'test',
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'test_dag',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)

task = BashOperator(
    task_id='test_task',
    bash_command='echo "test"',
    dag=dag,
)
'''


@pytest.fixture
def temp_dag_file(tmp_path, sample_dag_content):
    """Create a temporary DAG file for testing."""
    dag_file = tmp_path / "test_dag.py"
    dag_file.write_text(sample_dag_content)
    return str(dag_file)


@pytest.fixture
def kata_structure():
    """Expected kata directory structure."""
    return {
        'required_dirs': [
            'modules',
            'dags',
            'scripts',
            'data',
            'tests'
        ],
        'required_files': [
            'README.md',
            'docker-compose.yml',
            'requirements.txt',
            'pyproject.toml'
        ],
        'module_structure': [
            'README.md',
            'concepts.md',
            'resources.md',
            'examples',
            'exercises',
            'solutions'
        ]
    }


@pytest.fixture
def module_list():
    """List of expected learning modules."""
    return [
        '01-setup',
        '02-dag-fundamentals',
        '03-tasks-operators',
        '04-scheduling-dependencies',
        '05-sensors-triggers',
        '06-data-passing-xcoms',
        '07-branching-conditionals',
        '08-error-handling',
        '09-advanced-patterns',
        '10-real-world-projects'
    ]


@pytest.fixture
def exercise_metadata():
    """Sample exercise metadata for testing."""
    return {
        'id': 'exercise-1',
        'title': 'Create Your First DAG',
        'difficulty': 'beginner',
        'estimated_time': 30,
        'learning_objectives': [
            'Understand DAG structure',
            'Create basic tasks',
            'Define dependencies'
        ],
        'requirements': ['1.1', '1.2']
    }


@pytest.fixture
def docker_compose_content():
    """Sample docker-compose.yml content for testing."""
    return '''
version: '3.8'
services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres_db_volume:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 5s
      retries: 5
    restart: always

  redis:
    image: redis:latest
    expose:
      - 6379
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 30s
      retries: 50
    restart: always

  airflow-webserver:
    build: .
    command: webserver
    ports:
      - 8080:8080
    depends_on:
      - postgres
      - redis
    environment:
      - AIRFLOW__CORE__EXECUTOR=CeleryExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
      - AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:airflow@postgres/airflow
      - AIRFLOW__CELERY__BROKER_URL=redis://:@redis:6379/0
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
      - ./plugins:/opt/airflow/plugins
    restart: always

volumes:
  postgres_db_volume:
'''
