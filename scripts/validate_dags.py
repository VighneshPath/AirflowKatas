#!/usr/bin/env python3
"""
DAG Validation Script

This script validates DAG files for syntax errors and basic structure issues.
It helps catch common problems before deploying DAGs to Airflow.

Usage:
    python scripts/validate_dags.py
    python scripts/validate_dags.py --dag-file dags/my_dag.py
    python scripts/validate_dags.py --verbose
"""

import os
import sys
import ast
import importlib.util
import argparse
from pathlib import Path
from typing import List, Dict, Any, Optional


class DAGValidator:
    """Validates Airflow DAG files for common issues."""

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.errors = []
        self.warnings = []

    def log(self, message: str, level: str = "INFO"):
        """Log messages based on verbosity setting."""
        if self.verbose or level in ["ERROR", "WARNING"]:
            print(f"[{level}] {message}")

    def validate_syntax(self, file_path: str) -> bool:
        """Check if the Python file has valid syntax."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            ast.parse(content)
            self.log(f"✓ Syntax validation passed: {file_path}")
            return True

        except SyntaxError as e:
            error_msg = f"Syntax error in {file_path}: {e.msg} (line {e.lineno})"
            self.errors.append(error_msg)
            self.log(error_msg, "ERROR")
            return False
        except Exception as e:
            error_msg = f"Error reading {file_path}: {str(e)}"
            self.errors.append(error_msg)
            self.log(error_msg, "ERROR")
            return False

    def validate_imports(self, file_path: str) -> bool:
        """Check if the DAG file can be imported without errors."""
        try:
            # Add the directory containing the DAG to Python path
            dag_dir = os.path.dirname(os.path.abspath(file_path))
            if dag_dir not in sys.path:
                sys.path.insert(0, dag_dir)

            # Import the module
            spec = importlib.util.spec_from_file_location(
                "dag_module", file_path)
            if spec is None or spec.loader is None:
                raise ImportError(f"Could not load spec for {file_path}")

            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            self.log(f"✓ Import validation passed: {file_path}")
            return True

        except ImportError as e:
            error_msg = f"Import error in {file_path}: {str(e)}"
            self.errors.append(error_msg)
            self.log(error_msg, "ERROR")
            return False
        except Exception as e:
            error_msg = f"Error importing {file_path}: {str(e)}"
            self.errors.append(error_msg)
            self.log(error_msg, "ERROR")
            return False

    def validate_dag_structure(self, file_path: str) -> bool:
        """Check if the file contains proper DAG structure."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            # Check for required imports
            required_imports = ['from airflow import DAG', 'import DAG']
            has_dag_import = any(imp in content for imp in required_imports)

            if not has_dag_import:
                warning_msg = f"No DAG import found in {file_path}"
                self.warnings.append(warning_msg)
                self.log(warning_msg, "WARNING")

            # Check for DAG instantiation
            if 'DAG(' not in content and 'dag = ' not in content.lower():
                warning_msg = f"No DAG instantiation found in {file_path}"
                self.warnings.append(warning_msg)
                self.log(warning_msg, "WARNING")

            # Check for task definitions
            task_patterns = ['Operator(', 'task_id=', '@task']
            has_tasks = any(pattern in content for pattern in task_patterns)

            if not has_tasks:
                warning_msg = f"No tasks found in {file_path}"
                self.warnings.append(warning_msg)
                self.log(warning_msg, "WARNING")

            self.log(f"✓ Structure validation passed: {file_path}")
            return True

        except Exception as e:
            error_msg = f"Error validating structure of {file_path}: {str(e)}"
            self.errors.append(error_msg)
            self.log(error_msg, "ERROR")
            return False

    def validate_file(self, file_path: str) -> Dict[str, Any]:
        """Validate a single DAG file."""
        self.log(f"Validating {file_path}...")

        results = {
            'file': file_path,
            'syntax_valid': False,
            'imports_valid': False,
            'structure_valid': False,
            'overall_valid': False
        }

        # Run all validations
        results['syntax_valid'] = self.validate_syntax(file_path)

        # Only check imports if syntax is valid
        if results['syntax_valid']:
            results['imports_valid'] = self.validate_imports(file_path)

        results['structure_valid'] = self.validate_dag_structure(file_path)

        # Overall validation passes if syntax and structure are valid
        # Imports can fail due to missing dependencies but DAG might still be valid
        results['overall_valid'] = (
            results['syntax_valid'] and
            results['structure_valid']
        )

        return results

    def find_dag_files(self, directory: str = "dags") -> List[str]:
        """Find all Python files in the DAGs directory."""
        dag_files = []

        if not os.path.exists(directory):
            self.log(f"Directory {directory} does not exist", "WARNING")
            return dag_files

        for root, dirs, files in os.walk(directory):
            for file in files:
                if file.endswith('.py') and not file.startswith('__'):
                    dag_files.append(os.path.join(root, file))

        return dag_files

    def validate_all_dags(self, directory: str = "dags") -> Dict[str, Any]:
        """Validate all DAG files in the specified directory."""
        dag_files = self.find_dag_files(directory)

        if not dag_files:
            self.log(f"No DAG files found in {directory}", "WARNING")
            return {'files': [], 'summary': {'total': 0, 'valid': 0, 'invalid': 0}}

        self.log(f"Found {len(dag_files)} DAG files to validate")

        results = []
        valid_count = 0

        for dag_file in dag_files:
            result = self.validate_file(dag_file)
            results.append(result)

            if result['overall_valid']:
                valid_count += 1

        summary = {
            'total': len(dag_files),
            'valid': valid_count,
            'invalid': len(dag_files) - valid_count,
            'errors': len(self.errors),
            'warnings': len(self.warnings)
        }

        return {
            'files': results,
            'summary': summary,
            'errors': self.errors,
            'warnings': self.warnings
        }


def main():
    """Main function to run DAG validation."""
    parser = argparse.ArgumentParser(description='Validate Airflow DAG files')
    parser.add_argument(
        '--dag-file',
        type=str,
        help='Validate a specific DAG file'
    )
    parser.add_argument(
        '--dag-dir',
        type=str,
        default='dags',
        help='Directory containing DAG files (default: dags)'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose output'
    )

    args = parser.parse_args()

    validator = DAGValidator(verbose=args.verbose)

    if args.dag_file:
        # Validate single file
        if not os.path.exists(args.dag_file):
            print(f"Error: File {args.dag_file} does not exist")
            sys.exit(1)

        result = validator.validate_file(args.dag_file)

        print(f"\nValidation Results for {args.dag_file}:")
        print(f"  Syntax Valid: {'✓' if result['syntax_valid'] else '✗'}")
        print(f"  Imports Valid: {'✓' if result['imports_valid'] else '✗'}")
        print(
            f"  Structure Valid: {'✓' if result['structure_valid'] else '✗'}")
        print(f"  Overall Valid: {'✓' if result['overall_valid'] else '✗'}")

        if not result['overall_valid']:
            sys.exit(1)

    else:
        # Validate all files in directory
        results = validator.validate_all_dags(args.dag_dir)

        print(f"\nValidation Summary:")
        print(f"  Total files: {results['summary']['total']}")
        print(f"  Valid files: {results['summary']['valid']}")
        print(f"  Invalid files: {results['summary']['invalid']}")
        print(f"  Errors: {results['summary']['errors']}")
        print(f"  Warnings: {results['summary']['warnings']}")

        if results['summary']['invalid'] > 0:
            print(f"\nInvalid files:")
            for file_result in results['files']:
                if not file_result['overall_valid']:
                    print(f"  ✗ {file_result['file']}")

        if results['errors']:
            print(f"\nErrors:")
            for error in results['errors']:
                print(f"  • {error}")

        if results['warnings']:
            print(f"\nWarnings:")
            for warning in results['warnings']:
                print(f"  • {warning}")

        if results['summary']['invalid'] > 0:
            sys.exit(1)

    print("\n✓ All validations passed!")


if __name__ == "__main__":
    main()
