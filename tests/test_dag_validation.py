"""
Tests for DAG validation functionality.

This module tests the DAG validation scripts and ensures all example
and solution DAGs are syntactically correct and properly structured.
"""

from scripts.validate_dags import DAGValidator
import os
import sys
import ast
import pytest
import importlib.util
from pathlib import Path
from unittest.mock import patch, Mock

# Import the DAG validator
sys.path.insert(0, str(Path(__file__).parent.parent))


class TestDAGValidator:
    """Test the DAG validation functionality."""

    def test_validator_initialization(self):
        """Test DAGValidator can be initialized."""
        validator = DAGValidator()
        assert validator.verbose is False
        assert validator.errors == []
        assert validator.warnings == []

        validator_verbose = DAGValidator(verbose=True)
        assert validator_verbose.verbose is True

    def test_validate_syntax_valid_file(self, temp_dag_file):
        """Test syntax validation with a valid DAG file."""
        validator = DAGValidator()
        result = validator.validate_syntax(temp_dag_file)

        assert result is True
        assert len(validator.errors) == 0

    def test_validate_syntax_invalid_file(self, tmp_path):
        """Test syntax validation with invalid Python syntax."""
        invalid_file = tmp_path / "invalid.py"
        invalid_file.write_text(
            "def invalid_function(\n    # Missing closing parenthesis")

        validator = DAGValidator()
        result = validator.validate_syntax(str(invalid_file))

        assert result is False
        assert len(validator.errors) > 0
        assert "Syntax error" in validator.errors[0]

    def test_validate_syntax_nonexistent_file(self):
        """Test syntax validation with non-existent file."""
        validator = DAGValidator()
        result = validator.validate_syntax("nonexistent_file.py")

        assert result is False
        assert len(validator.errors) > 0

    def test_validate_dag_structure_valid(self, temp_dag_file):
        """Test DAG structure validation with valid DAG."""
        validator = DAGValidator()
        result = validator.validate_dag_structure(temp_dag_file)

        assert result is True

    def test_validate_dag_structure_missing_dag_import(self, tmp_path):
        """Test structure validation with missing DAG import."""
        dag_file = tmp_path / "no_import.py"
        dag_file.write_text('''
from datetime import datetime
print("No DAG import here")
''')

        validator = DAGValidator()
        result = validator.validate_dag_structure(str(dag_file))

        assert result is True  # Structure validation passes but adds warnings
        assert len(validator.warnings) > 0
        assert "No DAG import found" in validator.warnings[0]

    def test_validate_dag_structure_no_tasks(self, tmp_path):
        """Test structure validation with no tasks defined."""
        dag_file = tmp_path / "no_tasks.py"
        dag_file.write_text('''
from airflow import DAG
from datetime import datetime

dag = DAG('test', start_date=datetime(2024, 1, 1))
''')

        validator = DAGValidator()
        result = validator.validate_dag_structure(str(dag_file))

        assert result is True
        assert len(validator.warnings) > 0
        assert "No tasks found" in validator.warnings[0]

    def test_find_dag_files_existing_directory(self):
        """Test finding DAG files in existing directory."""
        validator = DAGValidator()

        # Test with actual dags directory if it exists
        if os.path.exists("dags"):
            dag_files = validator.find_dag_files("dags")
            assert isinstance(dag_files, list)
            # Should find Python files, excluding __pycache__ files
            for file in dag_files:
                assert file.endswith('.py')
                assert '__pycache__' not in file

    def test_find_dag_files_nonexistent_directory(self):
        """Test finding DAG files in non-existent directory."""
        validator = DAGValidator()
        dag_files = validator.find_dag_files("nonexistent_directory")

        assert dag_files == []
        # find_dag_files logs but doesn't add to warnings
        assert len(validator.warnings) == 0

    def test_validate_file_complete(self, temp_dag_file):
        """Test complete file validation."""
        validator = DAGValidator()
        result = validator.validate_file(temp_dag_file)

        assert isinstance(result, dict)
        assert 'file' in result
        assert 'syntax_valid' in result
        assert 'imports_valid' in result
        assert 'structure_valid' in result
        assert 'overall_valid' in result

        assert result['file'] == temp_dag_file
        assert result['syntax_valid'] is True
        assert result['structure_valid'] is True

    @patch('importlib.util.spec_from_file_location')
    def test_validate_imports_import_error(self, mock_spec, temp_dag_file):
        """Test import validation with import error."""
        mock_spec.return_value = None

        validator = DAGValidator()
        result = validator.validate_imports(temp_dag_file)

        assert result is False
        assert len(validator.errors) > 0

    def test_validate_all_dags_empty_directory(self, tmp_path):
        """Test validating all DAGs in empty directory."""
        validator = DAGValidator()
        results = validator.validate_all_dags(str(tmp_path))

        assert results['files'] == []
        assert results['summary']['total'] == 0
        assert results['summary']['valid'] == 0
        assert results['summary']['invalid'] == 0


class TestDAGFiles:
    """Test actual DAG files in the project."""

    def test_main_dag_files_exist(self):
        """Test that main DAG files exist and are valid."""
        expected_dags = [
            'dags/hello_world_dag.py',
            'dags/exercise-1-solution.py'
        ]

        for dag_path in expected_dags:
            if os.path.exists(dag_path):
                # Test syntax
                with open(dag_path, 'r') as f:
                    content = f.read()

                # Should parse without syntax errors
                try:
                    ast.parse(content)
                except SyntaxError:
                    pytest.fail(f"Syntax error in {dag_path}")

    def test_example_dags_syntax(self):
        """Test all example DAGs have valid syntax."""
        examples_dir = Path("modules")

        if not examples_dir.exists():
            pytest.skip("Modules directory not found")

        dag_files = []
        for module_dir in examples_dir.iterdir():
            if module_dir.is_dir():
                examples_subdir = module_dir / "examples"
                if examples_subdir.exists():
                    dag_files.extend(examples_subdir.glob("*.py"))

        for dag_file in dag_files:
            with open(dag_file, 'r') as f:
                content = f.read()

            try:
                ast.parse(content)
            except SyntaxError as e:
                pytest.fail(f"Syntax error in {dag_file}: {e}")

    def test_solution_dags_syntax(self):
        """Test all solution DAGs have valid syntax."""
        modules_dir = Path("modules")

        if not modules_dir.exists():
            pytest.skip("Modules directory not found")

        dag_files = []
        for module_dir in modules_dir.iterdir():
            if module_dir.is_dir():
                solutions_subdir = module_dir / "solutions"
                if solutions_subdir.exists():
                    dag_files.extend(solutions_subdir.glob("*.py"))

        for dag_file in dag_files:
            with open(dag_file, 'r') as f:
                content = f.read()

            try:
                ast.parse(content)
            except SyntaxError as e:
                pytest.fail(f"Syntax error in {dag_file}: {e}")

    def test_dag_files_have_required_elements(self):
        """Test that DAG files contain required Airflow elements."""
        dag_files = []

        # Collect all DAG files
        for dag_dir in ['dags', 'modules']:
            if os.path.exists(dag_dir):
                for root, dirs, files in os.walk(dag_dir):
                    for file in files:
                        if file.endswith('.py') and not file.startswith('__'):
                            dag_files.append(os.path.join(root, file))

        for dag_file in dag_files:
            with open(dag_file, 'r') as f:
                content = f.read()

            # Skip files that are clearly not DAG files
            if 'from airflow import DAG' not in content and 'import DAG' not in content:
                continue

            # Should have DAG instantiation
            assert ('DAG(' in content or 'dag =' in content.lower()), \
                f"No DAG instantiation found in {dag_file}"

            # Should have some form of task definition
            task_indicators = ['Operator(', 'task_id=', '@task', 'TaskGroup']
            has_tasks = any(
                indicator in content for indicator in task_indicators)

            # Allow some files to be examples without tasks
            if not any(skip in dag_file for skip in ['__init__', 'conftest', 'test_']):
                assert has_tasks, f"No tasks found in {dag_file}"


class TestDAGValidationScript:
    """Test the DAG validation script functionality."""

    def test_dag_validator_main_function_exists(self):
        """Test that the main validation script can be imported."""
        try:
            from scripts.validate_dags import main
            assert callable(main)
        except ImportError:
            pytest.fail(
                "Cannot import main function from validate_dags script")

    @patch('sys.argv', ['validate_dags.py', '--verbose'])
    @patch('scripts.validate_dags.DAGValidator')
    def test_main_function_with_verbose(self, mock_validator_class):
        """Test main function with verbose flag."""
        mock_validator = Mock()
        mock_validator.validate_all_dags.return_value = {
            'files': [],
            'summary': {'total': 0, 'valid': 0, 'invalid': 0, 'errors': 0, 'warnings': 0},
            'errors': [],
            'warnings': []
        }
        mock_validator_class.return_value = mock_validator

        from scripts.validate_dags import main

        try:
            main()
        except SystemExit as e:
            # Script should exit with 0 for success
            assert e.code == 0 or e.code is None

    @patch('sys.argv', ['validate_dags.py', '--dag-file', 'nonexistent.py'])
    def test_main_function_with_nonexistent_file(self):
        """Test main function with non-existent file."""
        from scripts.validate_dags import main

        with pytest.raises(SystemExit) as exc_info:
            main()

        # Should exit with error code
        assert exc_info.value.code == 1
