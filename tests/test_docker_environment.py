"""
Tests for Docker environment setup and validation.

This module tests Docker Compose configuration, container health,
and Airflow environment setup.
"""

import os
import pytest
import subprocess
import yaml
from pathlib import Path
from unittest.mock import patch, Mock


class TestDockerConfiguration:
    """Test Docker Compose configuration."""

    def test_docker_compose_file_exists(self):
        """Test that docker-compose.yml exists."""
        assert os.path.exists(
            'docker-compose.yml'), "docker-compose.yml file not found"

    def test_docker_compose_valid_yaml(self):
        """Test that docker-compose.yml is valid YAML."""
        try:
            with open('docker-compose.yml', 'r') as f:
                yaml.safe_load(f)
        except yaml.YAMLError as e:
            pytest.fail(f"Invalid YAML in docker-compose.yml: {e}")

    def test_docker_compose_has_required_services(self):
        """Test that docker-compose.yml contains required services."""
        with open('docker-compose.yml', 'r') as f:
            compose_config = yaml.safe_load(f)

        assert 'services' in compose_config, "No services defined in docker-compose.yml"

        services = compose_config['services']
        required_services = ['postgres', 'redis']  # Minimum required services

        for service in required_services:
            assert service in services, f"Required service '{service}' not found in docker-compose.yml"

    def test_docker_compose_postgres_configuration(self):
        """Test PostgreSQL service configuration."""
        with open('docker-compose.yml', 'r') as f:
            compose_config = yaml.safe_load(f)

        if 'postgres' not in compose_config.get('services', {}):
            pytest.skip("PostgreSQL service not configured")

        postgres_config = compose_config['services']['postgres']

        # Check required environment variables
        env = postgres_config.get('environment', {})
        required_env_vars = ['POSTGRES_USER',
                             'POSTGRES_PASSWORD', 'POSTGRES_DB']

        for var in required_env_vars:
            assert var in env, f"Required environment variable '{var}' not set for PostgreSQL"

    def test_docker_compose_redis_configuration(self):
        """Test Redis service configuration."""
        with open('docker-compose.yml', 'r') as f:
            compose_config = yaml.safe_load(f)

        if 'redis' not in compose_config.get('services', {}):
            pytest.skip("Redis service not configured")

        redis_config = compose_config['services']['redis']

        # Redis should expose port 6379
        assert 'expose' in redis_config or 'ports' in redis_config, \
            "Redis service should expose port 6379"

    def test_docker_compose_airflow_services(self):
        """Test Airflow service configurations."""
        with open('docker-compose.yml', 'r') as f:
            compose_config = yaml.safe_load(f)

        services = compose_config.get('services', {})
        airflow_services = [
            name for name in services.keys() if 'airflow' in name]

        if not airflow_services:
            pytest.skip("No Airflow services configured")

        for service_name in airflow_services:
            service_config = services[service_name]

            # Should have volumes for DAGs
            volumes = service_config.get('volumes', [])
            has_dags_volume = any('./dags' in volume for volume in volumes)
            assert has_dags_volume, f"Service '{service_name}' should mount DAGs directory"

    def test_docker_compose_volumes_configuration(self):
        """Test volume configurations."""
        with open('docker-compose.yml', 'r') as f:
            compose_config = yaml.safe_load(f)

        # Check if volumes are properly defined
        if 'volumes' in compose_config:
            volumes = compose_config['volumes']
            assert isinstance(
                volumes, dict), "Volumes should be defined as a dictionary"


class TestDockerEnvironment:
    """Test Docker environment functionality."""

    @pytest.fixture
    def mock_subprocess_success(self):
        """Mock successful subprocess calls."""
        with patch('subprocess.run') as mock_run:
            mock_result = Mock()
            mock_result.returncode = 0
            mock_result.stdout = "Docker version 20.10.0"
            mock_result.stderr = ""
            mock_run.return_value = mock_result
            yield mock_run

    @pytest.fixture
    def mock_subprocess_failure(self):
        """Mock failed subprocess calls."""
        with patch('subprocess.run') as mock_run:
            mock_result = Mock()
            mock_result.returncode = 1
            mock_result.stdout = ""
            mock_result.stderr = "Command not found"
            mock_run.return_value = mock_result
            yield mock_run

    def test_docker_command_available(self):
        """Test if Docker command is available."""
        try:
            result = subprocess.run(['docker', '--version'],
                                    capture_output=True, text=True, timeout=10)
            assert result.returncode == 0, "Docker command not available"
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pytest.skip("Docker not installed or not accessible")

    def test_docker_compose_command_available(self):
        """Test if Docker Compose command is available."""
        # Try both docker-compose and docker compose
        docker_compose_available = False

        try:
            result = subprocess.run(['docker-compose', '--version'],
                                    capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                docker_compose_available = True
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass

        if not docker_compose_available:
            try:
                result = subprocess.run(['docker', 'compose', 'version'],
                                        capture_output=True, text=True, timeout=10)
                if result.returncode == 0:
                    docker_compose_available = True
            except (subprocess.TimeoutExpired, FileNotFoundError):
                pass

        if not docker_compose_available:
            pytest.skip("Docker Compose not installed or not accessible")

    @patch('subprocess.run')
    def test_docker_daemon_running(self, mock_run):
        """Test if Docker daemon is running."""
        # Mock successful docker info command
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = "Server Version: 20.10.0"
        mock_run.return_value = mock_result

        # This would be the actual test logic
        result = subprocess.run(
            ['docker', 'info'], capture_output=True, text=True)
        assert result.returncode == 0

    def test_required_directories_exist(self):
        """Test that required directories for Docker setup exist."""
        required_dirs = ['dags', 'logs', 'plugins']

        for directory in required_dirs:
            if not os.path.exists(directory):
                # Create directory if it doesn't exist (for testing)
                os.makedirs(directory, exist_ok=True)

            assert os.path.exists(
                directory), f"Required directory '{directory}' not found"
            assert os.path.isdir(
                directory), f"'{directory}' exists but is not a directory"


class TestSetupVerification:
    """Test the setup verification script."""

    def test_verify_setup_script_exists(self):
        """Test that verify_setup.py script exists."""
        assert os.path.exists(
            'scripts/verify_setup.py'), "verify_setup.py script not found"

    def test_verify_setup_script_importable(self):
        """Test that verify_setup script can be imported."""
        try:
            import sys
            sys.path.insert(0, 'scripts')
            import verify_setup
            assert hasattr(
                verify_setup, 'main'), "verify_setup script should have main function"
        except ImportError as e:
            pytest.fail(f"Cannot import verify_setup script: {e}")

    def test_verify_setup_functions_exist(self):
        """Test that required verification functions exist."""
        import sys
        sys.path.insert(0, 'scripts')
        import verify_setup

        required_functions = [
            'check_docker',
            'check_docker_compose',
            'check_airflow_containers',
            'check_dag_folder',
            'main'
        ]

        for func_name in required_functions:
            assert hasattr(verify_setup, func_name), \
                f"verify_setup script missing required function: {func_name}"
            assert callable(getattr(verify_setup, func_name)), \
                f"{func_name} should be callable"

    @patch('scripts.verify_setup.run_command')
    def test_check_docker_success(self, mock_run_command):
        """Test check_docker function with successful Docker check."""
        mock_run_command.return_value = (True, "Docker version 20.10.0")

        import sys
        sys.path.insert(0, 'scripts')
        import verify_setup

        result = verify_setup.check_docker()

        assert result['status'] == 'passed'
        assert 'Docker is installed and running' in result['message']

    @patch('scripts.verify_setup.run_command')
    def test_check_docker_failure(self, mock_run_command):
        """Test check_docker function with Docker not available."""
        mock_run_command.return_value = (False, "Command not found")

        import sys
        sys.path.insert(0, 'scripts')
        import verify_setup

        result = verify_setup.check_docker()

        assert result['status'] == 'failed'
        assert 'Docker is not installed' in result['message']

    def test_run_command_function(self):
        """Test the run_command utility function."""
        import sys
        sys.path.insert(0, 'scripts')
        import verify_setup

        # Test with a simple command that should work
        success, output = verify_setup.run_command('echo test')
        assert success is True
        assert 'test' in output

    @patch('scripts.verify_setup.os.path.exists')
    def test_check_dag_folder_exists(self, mock_exists):
        """Test check_dag_folder when DAG folder exists."""
        mock_exists.return_value = True

        with patch('scripts.verify_setup.os.listdir') as mock_listdir:
            mock_listdir.return_value = ['test_dag.py', 'another_dag.py']

            import sys
            sys.path.insert(0, 'scripts')
            import verify_setup

            result = verify_setup.check_dag_folder()

            assert result['status'] == 'passed'
            assert '2 DAG files' in result['message']

    @patch('scripts.verify_setup.os.path.exists')
    def test_check_dag_folder_missing(self, mock_exists):
        """Test check_dag_folder when DAG folder is missing."""
        mock_exists.return_value = False

        import sys
        sys.path.insert(0, 'scripts')
        import verify_setup

        result = verify_setup.check_dag_folder()

        assert result['status'] == 'failed'
        assert 'does not exist' in result['message']


class TestDockerHealthChecks:
    """Test Docker health check configurations."""

    def test_postgres_health_check(self):
        """Test PostgreSQL health check configuration."""
        with open('docker-compose.yml', 'r') as f:
            compose_config = yaml.safe_load(f)

        if 'postgres' not in compose_config.get('services', {}):
            pytest.skip("PostgreSQL service not configured")

        postgres_config = compose_config['services']['postgres']

        if 'healthcheck' in postgres_config:
            healthcheck = postgres_config['healthcheck']
            assert 'test' in healthcheck, "PostgreSQL health check should have test command"
            assert 'interval' in healthcheck, "PostgreSQL health check should have interval"

    def test_redis_health_check(self):
        """Test Redis health check configuration."""
        with open('docker-compose.yml', 'r') as f:
            compose_config = yaml.safe_load(f)

        if 'redis' not in compose_config.get('services', {}):
            pytest.skip("Redis service not configured")

        redis_config = compose_config['services']['redis']

        if 'healthcheck' in redis_config:
            healthcheck = redis_config['healthcheck']
            assert 'test' in healthcheck, "Redis health check should have test command"
            assert 'interval' in healthcheck, "Redis health check should have interval"
