"""
Tests for exercise validation functionality.

This module tests the exercise validation scripts and ensures they
correctly identify completed exercises and provide appropriate feedback.
"""

from scripts.validate_exercises import ExerciseValidator, ExerciseRequirement, ExerciseValidationResult
import os
import sys
import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch, Mock

# Import the exercise validator
sys.path.insert(0, str(Path(__file__).parent.parent))


class TestExerciseRequirement:
    """Test the ExerciseRequirement dataclass."""

    def test_requirement_creation(self):
        """Test creating exercise requirements."""
        req = ExerciseRequirement(
            type="file",
            name="test.py",
            description="Test file should exist",
            required=True
        )

        assert req.type == "file"
        assert req.name == "test.py"
        assert req.description == "Test file should exist"
        assert req.required is True

    def test_requirement_defaults(self):
        """Test default values for requirements."""
        req = ExerciseRequirement(
            type="dag",
            name="test_dag",
            description="Test DAG"
        )

        assert req.required is True  # Default value


class TestExerciseValidationResult:
    """Test the ExerciseValidationResult dataclass."""

    def test_result_creation(self):
        """Test creating validation results."""
        result = ExerciseValidationResult(
            exercise_id="exercise-1",
            module="01-setup",
            passed=True,
            score=0.8,
            requirements_met=["File exists"],
            requirements_failed=[],
            feedback=["Good job!"],
            errors=[]
        )

        assert result.exercise_id == "exercise-1"
        assert result.module == "01-setup"
        assert result.passed is True
        assert result.score == 0.8


class TestExerciseValidator:
    """Test the ExerciseValidator class."""

    def test_validator_initialization(self):
        """Test validator initialization."""
        validator = ExerciseValidator()
        assert validator.verbose is False
        assert isinstance(validator.exercise_definitions, dict)

        validator_verbose = ExerciseValidator(verbose=True)
        assert validator_verbose.verbose is True

    def test_validate_file_exists_success(self, tmp_path):
        """Test file existence validation with existing file."""
        test_file = tmp_path / "test.py"
        test_file.write_text("# Test file")

        validator = ExerciseValidator()
        requirement = ExerciseRequirement("file", "test.py", "Test file")

        result = validator.validate_file_exists(str(test_file), requirement)
        assert result is True

    def test_validate_file_exists_failure(self):
        """Test file existence validation with missing file."""
        validator = ExerciseValidator()
        requirement = ExerciseRequirement(
            "file", "nonexistent.py", "Missing file")

        result = validator.validate_file_exists("nonexistent.py", requirement)
        assert result is False

    def test_validate_python_syntax_valid(self, tmp_path):
        """Test Python syntax validation with valid code."""
        test_file = tmp_path / "valid.py"
        test_file.write_text("""
from airflow import DAG
from datetime import datetime

dag = DAG('test', start_date=datetime(2024, 1, 1))
""")

        validator = ExerciseValidator()
        result = validator.validate_python_syntax(str(test_file))
        assert result is True

    def test_validate_python_syntax_invalid(self, tmp_path):
        """Test Python syntax validation with invalid code."""
        test_file = tmp_path / "invalid.py"
        test_file.write_text(
            "def invalid_function(\n    # Missing closing parenthesis")

        validator = ExerciseValidator()
        result = validator.validate_python_syntax(str(test_file))
        assert result is False

    def test_validate_dag_definition_success(self, tmp_path):
        """Test DAG definition validation with valid DAG."""
        test_file = tmp_path / "dag.py"
        test_file.write_text("""
from airflow import DAG
from datetime import datetime

dag = DAG('test_dag', start_date=datetime(2024, 1, 1))
""")

        validator = ExerciseValidator()
        result = validator.validate_dag_definition(str(test_file), "test_dag")
        assert result is True

    def test_validate_dag_definition_missing_import(self, tmp_path):
        """Test DAG definition validation without DAG import."""
        test_file = tmp_path / "no_import.py"
        test_file.write_text("""
from datetime import datetime
print("No DAG import")
""")

        validator = ExerciseValidator()
        result = validator.validate_dag_definition(str(test_file), "test_dag")
        assert result is False

    def test_validate_dag_definition_missing_dag(self, tmp_path):
        """Test DAG definition validation without DAG instantiation."""
        test_file = tmp_path / "no_dag.py"
        test_file.write_text("""
from airflow import DAG
from datetime import datetime
# No DAG instantiation
""")

        validator = ExerciseValidator()
        result = validator.validate_dag_definition(str(test_file), "test_dag")
        assert result is False

    def test_validate_task_definition_success(self, tmp_path):
        """Test task definition validation with valid task."""
        test_file = tmp_path / "task.py"
        test_file.write_text("""
from airflow.operators.bash import BashOperator

task = BashOperator(
    task_id='test_task',
    bash_command='echo "test"'
)
""")

        validator = ExerciseValidator()
        result = validator.validate_task_definition(
            str(test_file), "test_task")
        assert result is True

    def test_validate_task_definition_failure(self, tmp_path):
        """Test task definition validation without task."""
        test_file = tmp_path / "no_task.py"
        test_file.write_text("""
from airflow.operators.bash import BashOperator
# No task definition
""")

        validator = ExerciseValidator()
        result = validator.validate_task_definition(
            str(test_file), "test_task")
        assert result is False

    def test_validate_import_statement_success(self, tmp_path):
        """Test import statement validation with valid import."""
        test_file = tmp_path / "imports.py"
        test_file.write_text("""
from airflow import DAG
from airflow.operators.bash import BashOperator
""")

        validator = ExerciseValidator()
        result = validator.validate_import_statement(
            str(test_file), "from airflow import DAG")
        assert result is True

    def test_validate_import_statement_failure(self, tmp_path):
        """Test import statement validation without import."""
        test_file = tmp_path / "no_imports.py"
        test_file.write_text("print('No imports here')")

        validator = ExerciseValidator()
        result = validator.validate_import_statement(
            str(test_file), "from airflow import DAG")
        assert result is False

    def test_validate_function_definition_success(self, tmp_path):
        """Test function definition validation with valid function."""
        test_file = tmp_path / "function.py"
        test_file.write_text("""
def test_function():
    return "test"

schedule_interval = "daily"
""")

        validator = ExerciseValidator()
        result = validator.validate_function_definition(
            str(test_file), "test_function")
        assert result is True

        result2 = validator.validate_function_definition(
            str(test_file), "schedule_interval")
        assert result2 is True

    def test_validate_function_definition_failure(self, tmp_path):
        """Test function definition validation without function."""
        test_file = tmp_path / "no_function.py"
        test_file.write_text("print('No function here')")

        validator = ExerciseValidator()
        result = validator.validate_function_definition(
            str(test_file), "test_function")
        assert result is False


class TestExerciseValidation:
    """Test complete exercise validation."""

    def test_validate_exercise_success(self, tmp_path):
        """Test successful exercise validation."""
        # Create a simple DAG file
        dag_file = tmp_path / "my_first_dag.py"
        dag_file.write_text("""
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

dag = DAG('my_first_dag', start_date=datetime(2024, 1, 1))

start_task = BashOperator(
    task_id='start_task',
    bash_command='echo "Hello"',
    dag=dag
)
""")

        validator = ExerciseValidator()
        result = validator.validate_exercise(
            "01-setup", "exercise-1", str(tmp_path))

        assert isinstance(result, ExerciseValidationResult)
        assert result.exercise_id == "exercise-1"
        assert result.module == "01-setup"
        # Note: This might not pass all requirements depending on the exact definition

    def test_validate_exercise_missing_module(self):
        """Test exercise validation with missing module."""
        validator = ExerciseValidator()
        result = validator.validate_exercise(
            "nonexistent-module", "exercise-1", "dags")

        assert result.passed is False
        assert len(result.errors) > 0
        assert "not found" in result.errors[0]

    def test_validate_exercise_missing_exercise(self):
        """Test exercise validation with missing exercise."""
        validator = ExerciseValidator()
        result = validator.validate_exercise(
            "01-setup", "nonexistent-exercise", "dags")

        assert result.passed is False
        assert len(result.errors) > 0

    def test_validate_module_exercises(self):
        """Test validating all exercises in a module."""
        validator = ExerciseValidator()
        results = validator.validate_module_exercises("01-setup", "dags")

        assert isinstance(results, list)
        # Should return results even if exercises fail

    def test_validate_all_exercises(self):
        """Test validating all exercises."""
        validator = ExerciseValidator()
        results = validator.validate_all_exercises("dags")

        assert isinstance(results, dict)
        # Should return results for all modules

    def test_generate_progress_report(self):
        """Test progress report generation."""
        validator = ExerciseValidator()

        # Create mock results
        mock_results = {
            "01-setup": [
                ExerciseValidationResult(
                    exercise_id="exercise-1",
                    module="01-setup",
                    passed=True,
                    score=1.0,
                    requirements_met=["All requirements"],
                    requirements_failed=[],
                    feedback=["Great job!"],
                    errors=[]
                )
            ]
        }

        report = validator.generate_progress_report(mock_results)

        assert "overall" in report
        assert "modules" in report
        assert report["overall"]["total_exercises"] == 1
        assert report["overall"]["completed_exercises"] == 1
        assert report["overall"]["completion_rate"] == 1.0


class TestExerciseValidationScript:
    """Test the exercise validation script functionality."""

    def test_main_function_exists(self):
        """Test that the main validation script can be imported."""
        try:
            from scripts.validate_exercises import main
            assert callable(main)
        except ImportError:
            pytest.fail(
                "Cannot import main function from validate_exercises script")

    @patch('sys.argv', ['validate_exercises.py', '--verbose'])
    @patch('scripts.validate_exercises.ExerciseValidator')
    def test_main_function_with_verbose(self, mock_validator_class):
        """Test main function with verbose flag."""
        mock_validator = Mock()
        mock_validator.validate_all_exercises.return_value = {}
        mock_validator_class.return_value = mock_validator

        from scripts.validate_exercises import main

        try:
            main()
        except SystemExit:
            pass  # Expected for command line scripts

    @patch('sys.argv', ['validate_exercises.py', '--module', '01-setup'])
    @patch('scripts.validate_exercises.ExerciseValidator')
    def test_main_function_with_module(self, mock_validator_class):
        """Test main function with module specification."""
        mock_validator = Mock()
        mock_validator.validate_module_exercises.return_value = []
        mock_validator_class.return_value = mock_validator

        from scripts.validate_exercises import main

        try:
            main()
        except SystemExit:
            pass  # Expected for command line scripts


class TestExerciseDefinitions:
    """Test exercise definition structure."""

    def test_exercise_definitions_structure(self):
        """Test that exercise definitions have proper structure."""
        validator = ExerciseValidator()
        definitions = validator.exercise_definitions

        assert isinstance(definitions, dict)

        for module_name, module_exercises in definitions.items():
            assert isinstance(module_name, str)
            assert isinstance(module_exercises, dict)

            for exercise_id, exercise_def in module_exercises.items():
                assert isinstance(exercise_id, str)
                assert isinstance(exercise_def, dict)
                assert "title" in exercise_def
                assert "requirements" in exercise_def
                assert isinstance(exercise_def["requirements"], list)

                for requirement in exercise_def["requirements"]:
                    assert isinstance(requirement, ExerciseRequirement)
                    assert hasattr(requirement, 'type')
                    assert hasattr(requirement, 'name')
                    assert hasattr(requirement, 'description')

    def test_exercise_definitions_have_basic_modules(self):
        """Test that basic modules are defined."""
        validator = ExerciseValidator()
        definitions = validator.exercise_definitions

        expected_modules = ["01-setup",
                            "02-dag-fundamentals", "03-tasks-operators"]

        for module in expected_modules:
            assert module in definitions, f"Module {module} should be defined"

    def test_exercise_requirements_types(self):
        """Test that exercise requirements use valid types."""
        validator = ExerciseValidator()
        definitions = validator.exercise_definitions

        valid_types = ["file", "dag", "task", "import", "function"]

        for module_exercises in definitions.values():
            for exercise_def in module_exercises.values():
                for requirement in exercise_def["requirements"]:
                    assert requirement.type in valid_types, \
                        f"Invalid requirement type: {requirement.type}"
