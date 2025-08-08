#!/usr/bin/env python3
"""
Exercise Completion Validation Script

This script validates exercise completion by checking if students have
created the required files and implemented the expected functionality.

Usage:
    python scripts/validate_exercises.py
    python scripts/validate_exercises.py --module 01-setup
    python scripts/validate_exercises.py --exercise exercise-1
"""

import os
import sys
import ast
import argparse
import importlib.util
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import re


@dataclass
class ExerciseRequirement:
    """Represents a requirement for an exercise."""
    type: str  # 'file', 'function', 'dag', 'task', 'import'
    name: str
    description: str
    required: bool = True


@dataclass
class ExerciseValidationResult:
    """Results of exercise validation."""
    exercise_id: str
    module: str
    passed: bool
    score: float  # 0.0 to 1.0
    requirements_met: List[str]
    requirements_failed: List[str]
    feedback: List[str]
    errors: List[str]


class ExerciseValidator:
    """Validates exercise completion."""

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.exercise_definitions = self._load_exercise_definitions()

    def log(self, message: str, level: str = "INFO"):
        """Log messages based on verbosity setting."""
        if self.verbose or level in ["ERROR", "WARNING"]:
            print(f"[{level}] {message}")

    def _load_exercise_definitions(self) -> Dict[str, Dict[str, Any]]:
        """Load exercise definitions and requirements."""
        # This would typically load from configuration files
        # For now, we'll define some common patterns
        return {
            "01-setup": {
                "exercise-1": {
                    "title": "Create Your First DAG",
                    "requirements": [
                        ExerciseRequirement(
                            "file", "my_first_dag.py", "DAG file should exist"),
                        ExerciseRequirement(
                            "dag", "my_first_dag", "DAG should be defined"),
                        ExerciseRequirement(
                            "task", "start_task", "Should have a start task"),
                        ExerciseRequirement(
                            "import", "from airflow import DAG", "Should import DAG"),
                    ]
                }
            },
            "02-dag-fundamentals": {
                "exercise-1": {
                    "title": "DAG Configuration",
                    "requirements": [
                        ExerciseRequirement(
                            "dag", "daily_sales_report", "Should create daily sales DAG"),
                        ExerciseRequirement(
                            "function", "schedule_interval", "Should set schedule interval"),
                        ExerciseRequirement(
                            "function", "catchup", "Should configure catchup"),
                    ]
                }
            },
            "03-tasks-operators": {
                "exercise-1": {
                    "title": "Bash Operators",
                    "requirements": [
                        ExerciseRequirement(
                            "import", "BashOperator", "Should import BashOperator"),
                        ExerciseRequirement(
                            "task", "bash_task", "Should create bash task"),
                    ]
                },
                "exercise-2": {
                    "title": "Python Operators",
                    "requirements": [
                        ExerciseRequirement(
                            "import", "PythonOperator", "Should import PythonOperator"),
                        ExerciseRequirement(
                            "function", "python_function", "Should define Python function"),
                        ExerciseRequirement(
                            "task", "python_task", "Should create Python task"),
                    ]
                }
            }
        }

    def validate_file_exists(self, file_path: str, requirement: ExerciseRequirement) -> bool:
        """Validate that a required file exists."""
        if os.path.exists(file_path):
            self.log(f"✓ File exists: {file_path}")
            return True
        else:
            self.log(f"✗ File missing: {file_path}", "ERROR")
            return False

    def validate_python_syntax(self, file_path: str) -> bool:
        """Validate Python file syntax."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            ast.parse(content)
            self.log(f"✓ Valid Python syntax: {file_path}")
            return True
        except SyntaxError as e:
            self.log(f"✗ Syntax error in {file_path}: {e}", "ERROR")
            return False
        except Exception as e:
            self.log(f"✗ Error reading {file_path}: {e}", "ERROR")
            return False

    def validate_dag_definition(self, file_path: str, dag_name: str) -> bool:
        """Validate that a DAG is properly defined."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            # Check for DAG import
            if 'from airflow import DAG' not in content and 'import DAG' not in content:
                self.log(f"✗ No DAG import found in {file_path}", "ERROR")
                return False

            # Check for DAG instantiation
            dag_patterns = [
                f"dag = DAG('{dag_name}'",
                f'dag = DAG("{dag_name}"',
                f"DAG('{dag_name}'",
                f'DAG("{dag_name}"',
                f"dag_id='{dag_name}'",
                f'dag_id="{dag_name}"'
            ]

            has_dag = any(pattern in content for pattern in dag_patterns)
            if not has_dag:
                self.log(
                    f"✗ DAG '{dag_name}' not found in {file_path}", "ERROR")
                return False

            self.log(f"✓ DAG '{dag_name}' found in {file_path}")
            return True

        except Exception as e:
            self.log(f"✗ Error validating DAG in {file_path}: {e}", "ERROR")
            return False

    def validate_task_definition(self, file_path: str, task_id: str) -> bool:
        """Validate that a task is properly defined."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            # Look for task definition patterns
            task_patterns = [
                f"task_id='{task_id}'",
                f'task_id="{task_id}"',
                f"{task_id} = ",
                f"'{task_id}':",
                f'"{task_id}":',
            ]

            has_task = any(pattern in content for pattern in task_patterns)
            if not has_task:
                self.log(
                    f"✗ Task '{task_id}' not found in {file_path}", "ERROR")
                return False

            self.log(f"✓ Task '{task_id}' found in {file_path}")
            return True

        except Exception as e:
            self.log(f"✗ Error validating task in {file_path}: {e}", "ERROR")
            return False

    def validate_import_statement(self, file_path: str, import_statement: str) -> bool:
        """Validate that required imports are present."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            if import_statement in content:
                self.log(f"✓ Import found: {import_statement}")
                return True
            else:
                self.log(f"✗ Import missing: {import_statement}", "ERROR")
                return False

        except Exception as e:
            self.log(f"✗ Error checking imports in {file_path}: {e}", "ERROR")
            return False

    def validate_function_definition(self, file_path: str, function_name: str) -> bool:
        """Validate that a function is defined."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            # Look for function definition
            function_patterns = [
                f"def {function_name}(",
                f"{function_name} = ",
                f"'{function_name}':",
                f'"{function_name}":',
            ]

            has_function = any(
                pattern in content for pattern in function_patterns)
            if not has_function:
                self.log(
                    f"✗ Function/attribute '{function_name}' not found in {file_path}", "ERROR")
                return False

            self.log(
                f"✓ Function/attribute '{function_name}' found in {file_path}")
            return True

        except Exception as e:
            self.log(
                f"✗ Error validating function in {file_path}: {e}", "ERROR")
            return False

    def validate_exercise(self, module: str, exercise_id: str,
                          student_files_dir: str = "dags") -> ExerciseValidationResult:
        """Validate a specific exercise."""
        self.log(f"Validating exercise {exercise_id} in module {module}")

        result = ExerciseValidationResult(
            exercise_id=exercise_id,
            module=module,
            passed=False,
            score=0.0,
            requirements_met=[],
            requirements_failed=[],
            feedback=[],
            errors=[]
        )

        # Get exercise definition
        if module not in self.exercise_definitions:
            result.errors.append(
                f"Module '{module}' not found in exercise definitions")
            return result

        if exercise_id not in self.exercise_definitions[module]:
            result.errors.append(
                f"Exercise '{exercise_id}' not found in module '{module}'")
            return result

        exercise_def = self.exercise_definitions[module][exercise_id]
        requirements = exercise_def["requirements"]

        total_requirements = len(requirements)
        met_requirements = 0

        for requirement in requirements:
            requirement_met = False

            if requirement.type == "file":
                file_path = os.path.join(student_files_dir, requirement.name)
                requirement_met = self.validate_file_exists(
                    file_path, requirement)

                # Also validate syntax if it's a Python file
                if requirement_met and requirement.name.endswith('.py'):
                    syntax_valid = self.validate_python_syntax(file_path)
                    if not syntax_valid:
                        requirement_met = False

            elif requirement.type == "dag":
                # Look for DAG in any Python file in the directory
                dag_found = False
                for file in os.listdir(student_files_dir):
                    if file.endswith('.py'):
                        file_path = os.path.join(student_files_dir, file)
                        if self.validate_dag_definition(file_path, requirement.name):
                            dag_found = True
                            break
                requirement_met = dag_found

            elif requirement.type == "task":
                # Look for task in any Python file in the directory
                task_found = False
                for file in os.listdir(student_files_dir):
                    if file.endswith('.py'):
                        file_path = os.path.join(student_files_dir, file)
                        if self.validate_task_definition(file_path, requirement.name):
                            task_found = True
                            break
                requirement_met = task_found

            elif requirement.type == "import":
                # Look for import in any Python file in the directory
                import_found = False
                for file in os.listdir(student_files_dir):
                    if file.endswith('.py'):
                        file_path = os.path.join(student_files_dir, file)
                        if self.validate_import_statement(file_path, requirement.name):
                            import_found = True
                            break
                requirement_met = import_found

            elif requirement.type == "function":
                # Look for function in any Python file in the directory
                function_found = False
                for file in os.listdir(student_files_dir):
                    if file.endswith('.py'):
                        file_path = os.path.join(student_files_dir, file)
                        if self.validate_function_definition(file_path, requirement.name):
                            function_found = True
                            break
                requirement_met = function_found

            # Record result
            if requirement_met:
                result.requirements_met.append(requirement.description)
                met_requirements += 1
            else:
                result.requirements_failed.append(requirement.description)
                if requirement.required:
                    result.errors.append(
                        f"Required: {requirement.description}")

        # Calculate score and pass/fail
        result.score = met_requirements / \
            total_requirements if total_requirements > 0 else 0.0
        result.passed = result.score >= 0.8  # 80% threshold

        # Generate feedback
        if result.passed:
            result.feedback.append("✅ Exercise completed successfully!")
            result.feedback.append(f"Score: {result.score:.1%}")
        else:
            result.feedback.append("❌ Exercise not yet complete.")
            result.feedback.append(f"Score: {result.score:.1%}")
            result.feedback.append("Missing requirements:")
            for failed_req in result.requirements_failed:
                result.feedback.append(f"  • {failed_req}")

        return result

    def validate_module_exercises(self, module: str,
                                  student_files_dir: str = "dags") -> List[ExerciseValidationResult]:
        """Validate all exercises in a module."""
        results = []

        if module not in self.exercise_definitions:
            self.log(
                f"Module '{module}' not found in exercise definitions", "ERROR")
            return results

        for exercise_id in self.exercise_definitions[module]:
            result = self.validate_exercise(
                module, exercise_id, student_files_dir)
            results.append(result)

        return results

    def validate_all_exercises(self, student_files_dir: str = "dags") -> Dict[str, List[ExerciseValidationResult]]:
        """Validate all exercises across all modules."""
        all_results = {}

        for module in self.exercise_definitions:
            results = self.validate_module_exercises(module, student_files_dir)
            all_results[module] = results

        return all_results

    def generate_progress_report(self, results: Dict[str, List[ExerciseValidationResult]]) -> Dict[str, Any]:
        """Generate a progress report from validation results."""
        total_exercises = 0
        completed_exercises = 0
        total_score = 0.0

        module_progress = {}

        for module, exercise_results in results.items():
            module_total = len(exercise_results)
            module_completed = sum(1 for r in exercise_results if r.passed)
            module_score = sum(r.score for r in exercise_results) / \
                module_total if module_total > 0 else 0.0

            module_progress[module] = {
                'total_exercises': module_total,
                'completed_exercises': module_completed,
                'completion_rate': module_completed / module_total if module_total > 0 else 0.0,
                'average_score': module_score,
                'exercises': {r.exercise_id: {'passed': r.passed, 'score': r.score} for r in exercise_results}
            }

            total_exercises += module_total
            completed_exercises += module_completed
            total_score += module_score * module_total

        overall_completion_rate = completed_exercises / \
            total_exercises if total_exercises > 0 else 0.0
        overall_average_score = total_score / \
            total_exercises if total_exercises > 0 else 0.0

        return {
            'overall': {
                'total_exercises': total_exercises,
                'completed_exercises': completed_exercises,
                'completion_rate': overall_completion_rate,
                'average_score': overall_average_score
            },
            'modules': module_progress
        }


def main():
    """Main function to run exercise validation."""
    parser = argparse.ArgumentParser(
        description='Validate exercise completion')
    parser.add_argument(
        '--module',
        type=str,
        help='Validate exercises for a specific module'
    )
    parser.add_argument(
        '--exercise',
        type=str,
        help='Validate a specific exercise (requires --module)'
    )
    parser.add_argument(
        '--student-dir',
        type=str,
        default='dags',
        help='Directory containing student files (default: dags)'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose output'
    )
    parser.add_argument(
        '--report',
        action='store_true',
        help='Generate progress report'
    )

    args = parser.parse_args()

    validator = ExerciseValidator(verbose=args.verbose)

    if args.exercise and not args.module:
        print("Error: --exercise requires --module")
        sys.exit(1)

    if args.module and args.exercise:
        # Validate specific exercise
        result = validator.validate_exercise(
            args.module, args.exercise, args.student_dir)

        print(f"\nExercise Validation: {args.module}/{args.exercise}")
        print("=" * 50)
        print(f"Status: {'PASSED' if result.passed else 'FAILED'}")
        print(f"Score: {result.score:.1%}")

        if result.feedback:
            print("\nFeedback:")
            for feedback in result.feedback:
                print(f"  {feedback}")

        if result.errors:
            print("\nErrors:")
            for error in result.errors:
                print(f"  {error}")

        sys.exit(0 if result.passed else 1)

    elif args.module:
        # Validate all exercises in module
        results = validator.validate_module_exercises(
            args.module, args.student_dir)

        print(f"\nModule Validation: {args.module}")
        print("=" * 50)

        for result in results:
            status = "PASSED" if result.passed else "FAILED"
            print(f"{result.exercise_id}: {status} ({result.score:.1%})")

        total_passed = sum(1 for r in results if r.passed)
        print(f"\nSummary: {total_passed}/{len(results)} exercises passed")

    else:
        # Validate all exercises
        all_results = validator.validate_all_exercises(args.student_dir)

        if args.report:
            # Generate detailed progress report
            progress = validator.generate_progress_report(all_results)

            print("\n📊 PROGRESS REPORT")
            print("=" * 50)
            print(
                f"Overall Completion: {progress['overall']['completion_rate']:.1%}")
            print(f"Overall Score: {progress['overall']['average_score']:.1%}")
            print(
                f"Exercises: {progress['overall']['completed_exercises']}/{progress['overall']['total_exercises']}")

            print(f"\n📚 MODULE BREAKDOWN")
            print("-" * 30)
            for module, module_data in progress['modules'].items():
                print(f"{module}:")
                print(f"  Completion: {module_data['completion_rate']:.1%}")
                print(f"  Score: {module_data['average_score']:.1%}")
                print(
                    f"  Exercises: {module_data['completed_exercises']}/{module_data['total_exercises']}")
        else:
            # Simple summary
            print("\n📋 EXERCISE VALIDATION SUMMARY")
            print("=" * 40)

            total_exercises = 0
            total_passed = 0

            for module, results in all_results.items():
                module_passed = sum(1 for r in results if r.passed)
                total_exercises += len(results)
                total_passed += module_passed

                print(f"{module}: {module_passed}/{len(results)} passed")

            print(
                f"\nOverall: {total_passed}/{total_exercises} exercises passed")
            completion_rate = total_passed / total_exercises if total_exercises > 0 else 0.0
            print(f"Completion Rate: {completion_rate:.1%}")


if __name__ == "__main__":
    main()
