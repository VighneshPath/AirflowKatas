"""
End-to-End Integration Tests

This module contains comprehensive integration tests that verify all modules
of the Airflow Coding Kata work together properly.
"""

from scripts.end_to_end_validation import EndToEndValidator, ValidationResult, WalkthroughResult
import os
import sys
import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, Mock

# Add project root to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))


class TestEndToEndIntegration:
    """Test end-to-end integration functionality."""

    def test_validator_initialization(self):
        """Test that the end-to-end validator initializes correctly."""
        validator = EndToEndValidator()

        assert validator.verbose is False
        assert validator.dag_validator is not None
        assert validator.exercise_validator is not None
        assert validator.progress_tracker is not None
        assert isinstance(validator.test_results, list)

    def test_kata_structure_validation_success(self):
        """Test kata structure validation with complete structure."""
        validator = EndToEndValidator()

        # Mock the required directories and files to exist
        with patch('os.path.exists') as mock_exists:
            mock_exists.return_value = True

            with patch.object(validator, '_validate_module_structure') as mock_module_validation:
                mock_module_validation.return_value = []  # No issues

                result = validator.validate_kata_structure()

                assert isinstance(result, ValidationResult)
                assert result.test_name == "kata_structure"
                assert result.passed is True
                assert result.score == 1.0
                assert len(result.errors) == 0

    def test_kata_structure_validation_missing_dirs(self):
        """Test kata structure validation with missing directories."""
        validator = EndToEndValidator()

        # Mock some directories as missing
        def mock_exists(path):
            missing_dirs = ['plugins', 'logs']
            return path not in missing_dirs

        with patch('os.path.exists', side_effect=mock_exists):
            with patch.object(validator, '_validate_module_structure') as mock_module_validation:
                mock_module_validation.return_value = []

                result = validator.validate_kata_structure()

                assert result.passed is False
                assert result.score < 1.0
                assert len(result.errors) > 0
                assert "Missing required directories" in result.errors[0]

    def test_docker_environment_validation_success(self):
        """Test Docker environment validation with valid setup."""
        validator = EndToEndValidator()

        with patch('os.path.exists') as mock_exists:
            mock_exists.return_value = True

            with patch('subprocess.run') as mock_run:
                # Mock successful docker-compose config
                mock_run.return_value = Mock(returncode=0, stderr="")

                result = validator.validate_docker_environment()

                assert isinstance(result, ValidationResult)
                assert result.test_name == "docker_environment"
                assert result.passed is True
                assert result.score == 1.0

    def test_docker_environment_validation_missing_compose(self):
        """Test Docker environment validation with missing docker-compose.yml."""
        validator = EndToEndValidator()

        with patch('os.path.exists') as mock_exists:
            mock_exists.return_value = False

            result = validator.validate_docker_environment()

            assert result.passed is False
            assert len(result.errors) > 0
            assert "docker-compose.yml not found" in result.errors[0]

    def test_dag_integration_validation_success(self):
        """Test DAG integration validation with valid DAGs."""
        validator = EndToEndValidator()

        # Mock DAG validator results
        mock_dag_results = {
            'summary': {'total': 5, 'valid': 5, 'invalid': 0},
            'errors': []
        }

        with patch.object(validator.dag_validator, 'validate_all_dags') as mock_validate:
            mock_validate.return_value = mock_dag_results

            with patch('os.walk') as mock_walk:
                # Mock finding some DAG files
                mock_walk.return_value = [
                    ('.', [], ['test_dag.py', 'another_dag.py'])
                ]

                result = validator.validate_all_dags_integration()

                assert result.passed is True
                assert result.score == 1.0
                assert len(result.errors) == 0

    def test_dag_integration_validation_with_conflicts(self):
        """Test DAG integration validation with DAG ID conflicts."""
        validator = EndToEndValidator()

        # Mock DAG validator results
        mock_dag_results = {
            'summary': {'total': 2, 'valid': 2, 'invalid': 0},
            'errors': []
        }

        # Mock DAG files with duplicate IDs
        dag_content = '''
        from airflow import DAG
        dag = DAG('duplicate_id', start_date=datetime(2024, 1, 1))
        '''

        with patch.object(validator.dag_validator, 'validate_all_dags') as mock_validate:
            mock_validate.return_value = mock_dag_results

            with patch('os.walk') as mock_walk:
                mock_walk.return_value = [
                    ('.', [], ['dag1.py', 'dag2.py'])
                ]

                with patch('builtins.open', mock_open_multiple_files({
                    'dag1.py': dag_content,
                    'dag2.py': dag_content
                })):
                    result = validator.validate_all_dags_integration()

                    assert result.passed is False
                    assert len(result.errors) > 0
                    assert "Duplicate DAG IDs" in result.errors[0]

    def test_final_assessment_creation(self):
        """Test final assessment creation."""
        validator = EndToEndValidator()

        with tempfile.TemporaryDirectory() as temp_dir:
            # Change to temp directory
            original_cwd = os.getcwd()
            os.chdir(temp_dir)

            try:
                result = validator.create_final_assessment()

                assert isinstance(result, ValidationResult)
                assert result.test_name == "final_assessment"

                # Check if assessment files were created
                assessment_dir = Path('assessments')
                assert assessment_dir.exists()
                assert (assessment_dir / 'final_assessment.md').exists()
                assert (assessment_dir / 'final_assessment_solution.py').exists()

            finally:
                os.chdir(original_cwd)

    def test_module_walkthrough_validation(self):
        """Test validation of a single module walkthrough."""
        validator = EndToEndValidator()

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create mock module structure
            module_dir = Path(temp_dir) / 'modules' / '01-setup'
            exercises_dir = module_dir / 'exercises'
            solutions_dir = module_dir / 'solutions'

            exercises_dir.mkdir(parents=True)
            solutions_dir.mkdir(parents=True)

            # Create mock exercise and solution files
            (exercises_dir / 'exercise-1.md').write_text("# Exercise 1")
            (exercises_dir / 'exercise-2.md').write_text("# Exercise 2")

            solution_content = '''
from airflow import DAG
from datetime import datetime

dag = DAG('test', start_date=datetime(2024, 1, 1))
'''
            (solutions_dir / 'exercise-1-solution.py').write_text(solution_content)
            (solutions_dir / 'exercise-2-solution.py').write_text(solution_content)

            # Change to temp directory
            original_cwd = os.getcwd()
            os.chdir(temp_dir)

            try:
                result = validator._validate_module_walkthrough('01-setup')

                assert isinstance(result, dict)
                assert 'total_exercises' in result
                assert 'completed_exercises' in result
                assert 'completion_rate' in result
                assert 'average_score' in result

                assert result['total_exercises'] == 2

            finally:
                os.chdir(original_cwd)

    def test_complete_walkthrough_execution(self):
        """Test complete walkthrough execution."""
        validator = EndToEndValidator()

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create minimal module structure
            modules_dir = Path(temp_dir) / 'modules'
            modules_dir.mkdir()

            # Create a single test module
            test_module = modules_dir / '01-setup'
            test_module.mkdir()
            (test_module / 'exercises').mkdir()
            (test_module / 'solutions').mkdir()

            # Change to temp directory
            original_cwd = os.getcwd()
            os.chdir(temp_dir)

            try:
                result = validator.run_complete_walkthrough()

                assert isinstance(result, WalkthroughResult)
                assert result.total_modules >= 0
                assert result.completed_modules >= 0
                assert result.total_exercises >= 0
                assert result.completed_exercises >= 0
                assert isinstance(result.overall_score, float)
                assert isinstance(result.time_taken, float)
                assert isinstance(result.module_results, dict)
                assert isinstance(result.critical_errors, list)

            finally:
                os.chdir(original_cwd)

    def test_integration_tests_execution(self):
        """Test integration tests execution."""
        validator = EndToEndValidator()

        results = validator.run_integration_tests()

        assert isinstance(results, list)
        assert len(results) > 0

        for result in results:
            assert isinstance(result, ValidationResult)
            assert hasattr(result, 'test_name')
            assert hasattr(result, 'passed')
            assert hasattr(result, 'score')
            assert hasattr(result, 'errors')
            assert hasattr(result, 'warnings')

    def test_comprehensive_report_generation(self):
        """Test comprehensive report generation."""
        validator = EndToEndValidator()

        # Create mock walkthrough result
        walkthrough_result = WalkthroughResult(
            total_modules=10,
            completed_modules=8,
            total_exercises=50,
            completed_exercises=40,
            overall_score=0.85,
            time_taken=120.0,
            module_results={},
            critical_errors=[]
        )

        # Create mock integration results
        integration_results = [
            ValidationResult(
                test_name="test1",
                passed=True,
                score=1.0,
                details={},
                errors=[],
                warnings=[],
                execution_time=1.0
            ),
            ValidationResult(
                test_name="test2",
                passed=False,
                score=0.5,
                details={},
                errors=["Test error"],
                warnings=[],
                execution_time=2.0
            )
        ]

        report = validator.generate_comprehensive_report(
            walkthrough_result, integration_results)

        assert isinstance(report, dict)
        assert 'validation_timestamp' in report
        assert 'overall_status' in report
        assert 'overall_score' in report
        assert 'walkthrough_summary' in report
        assert 'integration_summary' in report
        assert 'detailed_results' in report
        assert 'recommendations' in report

        # Check walkthrough summary
        ws = report['walkthrough_summary']
        assert ws['total_modules'] == 10
        assert ws['completed_modules'] == 8
        assert ws['module_completion_rate'] == 0.8

        # Check integration summary
        its = report['integration_summary']
        assert its['total_tests'] == 2
        assert its['passed_tests'] == 1
        assert its['failed_tests'] == 1

    def test_recommendations_generation(self):
        """Test recommendations generation based on results."""
        validator = EndToEndValidator()

        # Test with poor walkthrough results
        poor_walkthrough = WalkthroughResult(
            total_modules=10,
            completed_modules=3,
            total_exercises=50,
            completed_exercises=15,
            overall_score=0.3,
            time_taken=60.0,
            module_results={},
            critical_errors=["Critical error 1"]
        )

        failed_integration = [
            ValidationResult(
                test_name="kata_structure",
                passed=False,
                score=0.0,
                details={},
                errors=["Structure error"],
                warnings=[],
                execution_time=1.0
            )
        ]

        recommendations = validator._generate_recommendations(
            poor_walkthrough, failed_integration)

        assert isinstance(recommendations, list)
        assert len(recommendations) > 0

        # Should recommend completing modules
        assert any("Complete remaining" in rec for rec in recommendations)

        # Should recommend improving scores
        assert any("improve exercise solutions" in rec for rec in recommendations)

        # Should recommend addressing critical errors
        assert any("critical errors" in rec for rec in recommendations)

        # Should recommend fixing structure issues
        assert any("required directories" in rec for rec in recommendations)


class TestFinalAssessment:
    """Test final assessment functionality."""

    def test_final_assessment_content_generation(self):
        """Test final assessment content generation."""
        validator = EndToEndValidator()

        content = validator._generate_final_assessment_content()

        assert isinstance(content, str)
        assert len(content) > 1000  # Should be substantial content
        assert "Final Assessment" in content
        assert "ecommerce_data_pipeline" in content
        assert "Requirements" in content
        assert "Success Criteria" in content

    def test_final_assessment_solution_generation(self):
        """Test final assessment solution generation."""
        validator = EndToEndValidator()

        solution = validator._generate_final_assessment_solution()

        assert isinstance(solution, str)
        assert len(solution) > 500  # Should be substantial code
        assert "from airflow import DAG" in solution
        assert "ecommerce_data_pipeline" in solution

    def test_final_assessment_file_creation(self, tmp_path):
        """Test final assessment file creation."""
        validator = EndToEndValidator()

        # Change to temp directory
        original_cwd = os.getcwd()
        os.chdir(tmp_path)

        try:
            result = validator.create_final_assessment()

            # Check files were created
            assessment_dir = Path('assessments')
            assert assessment_dir.exists()

            assessment_file = assessment_dir / 'final_assessment.md'
            solution_file = assessment_dir / 'final_assessment_solution.py'

            assert assessment_file.exists()
            assert solution_file.exists()

            # Check content
            assert assessment_file.read_text().startswith("# Final Assessment")
            assert "from airflow import DAG" in solution_file.read_text()

        finally:
            os.chdir(original_cwd)


def mock_open_multiple_files(files_dict):
    """Helper function to mock opening multiple files with different content."""
    def mock_open_func(filename, mode='r', **kwargs):
        from unittest.mock import mock_open
        if filename in files_dict:
            return mock_open(read_data=files_dict[filename]).return_value
        else:
            return mock_open().return_value

    return mock_open_func


class TestIntegrationScenarios:
    """Test various integration scenarios."""

    def test_empty_project_validation(self):
        """Test validation of completely empty project."""
        validator = EndToEndValidator()

        with tempfile.TemporaryDirectory() as temp_dir:
            original_cwd = os.getcwd()
            os.chdir(temp_dir)

            try:
                # Run structure validation on empty directory
                result = validator.validate_kata_structure()

                assert result.passed is False
                assert result.score < 0.5
                assert len(result.errors) > 0

            finally:
                os.chdir(original_cwd)

    def test_partial_project_validation(self):
        """Test validation of partially complete project."""
        validator = EndToEndValidator()

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create some but not all required directories
            (Path(temp_dir) / 'modules').mkdir()
            (Path(temp_dir) / 'dags').mkdir()
            (Path(temp_dir) / 'README.md').write_text("# Test Project")

            original_cwd = os.getcwd()
            os.chdir(temp_dir)

            try:
                result = validator.validate_kata_structure()

                # Should have some errors but not complete failure
                assert result.score > 0.0
                assert result.score < 1.0

            finally:
                os.chdir(original_cwd)

    def test_complete_project_validation(self):
        """Test validation of complete project structure."""
        validator = EndToEndValidator()

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create all required directories and files
            required_dirs = ['modules', 'dags', 'scripts', 'data', 'tests',
                             'plugins', 'logs', 'config', 'resources', 'solutions']
            required_files = ['README.md', 'docker-compose.yml',
                              'requirements.txt', 'pyproject.toml', '.gitignore']

            for dir_name in required_dirs:
                (Path(temp_dir) / dir_name).mkdir()

            for file_name in required_files:
                (Path(temp_dir) / file_name).write_text(f"# {file_name}")

            # Create modules structure
            modules_dir = Path(temp_dir) / 'modules'
            for i in range(1, 11):
                module_name = f"{i:02d}-module"
                module_path = modules_dir / module_name
                module_path.mkdir()

                (module_path / 'README.md').write_text("# Module")
                (module_path / 'concepts.md').write_text("# Concepts")
                (module_path / 'resources.md').write_text("# Resources")
                (module_path / 'examples').mkdir()
                (module_path / 'exercises').mkdir()
                (module_path / 'solutions').mkdir()

            original_cwd = os.getcwd()
            os.chdir(temp_dir)

            try:
                result = validator.validate_kata_structure()

                # Should pass with complete structure
                assert result.passed is True
                assert result.score == 1.0
                assert len(result.errors) == 0

            finally:
                os.chdir(original_cwd)
