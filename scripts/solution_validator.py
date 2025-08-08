#!/usr/bin/env python3
"""
Solution Validation Script

This script validates student solutions against expected exercise outcomes
and provides detailed feedback on correctness and completeness.

Usage:
    python scripts/solution_validator.py --exercise 01-setup exercise-1
    python scripts/solution_validator.py --validate-all
    python scripts/solution_validator.py --compare-solution dags/student_dag.py modules/01-setup/solutions/exercise-1-solution.py
"""

import os
import sys
import ast
import re
import argparse
import difflib
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
import importlib.util


@dataclass
class ValidationResult:
    """Results of solution validation."""
    exercise_id: str
    module: str
    student_file: str
    reference_file: Optional[str]
    passed: bool
    score: float  # 0.0 to 1.0
    criteria_met: List[str]
    criteria_failed: List[str]
    feedback: List[str]
    suggestions: List[str]
    errors: List[str]


@dataclass
class ValidationCriterion:
    """Represents a validation criterion."""
    id: str
    description: str
    weight: float  # Relative importance (0.0 to 1.0)
    validator_func: str  # Name of validation function
    required: bool = True


class SolutionValidator:
    """Validates student solutions against exercise criteria."""

    def __init__(self):
        self.validation_criteria = self._load_validation_criteria()
        self.reference_solutions = self._load_reference_solutions()

    def _load_validation_criteria(self) -> Dict[str, Dict[str, List[ValidationCriterion]]]:
        """Load validation criteria for each exercise."""
        return {
            "01-setup": {
                "exercise-1": [
                    ValidationCriterion(
                        id="has_dag_import",
                        description="Imports DAG from airflow",
                        weight=0.2,
                        validator_func="validate_dag_import"
                    ),
                    ValidationCriterion(
                        id="has_dag_definition",
                        description="Defines a DAG instance",
                        weight=0.3,
                        validator_func="validate_dag_definition"
                    ),
                    ValidationCriterion(
                        id="has_tasks",
                        description="Contains at least one task",
                        weight=0.3,
                        validator_func="validate_has_tasks"
                    ),
                    ValidationCriterion(
                        id="has_dependencies",
                        description="Defines task dependencies",
                        weight=0.2,
                        validator_func="validate_has_dependencies"
                    )
                ]
            },
            "02-dag-fundamentals": {
                "exercise-1": [
                    ValidationCriterion(
                        id="correct_dag_id",
                        description="Uses correct DAG ID",
                        weight=0.15,
                        validator_func="validate_dag_id"
                    ),
                    ValidationCriterion(
                        id="has_schedule_interval",
                        description="Sets schedule_interval",
                        weight=0.2,
                        validator_func="validate_schedule_interval"
                    ),
                    ValidationCriterion(
                        id="has_start_date",
                        description="Sets start_date",
                        weight=0.2,
                        validator_func="validate_start_date"
                    ),
                    ValidationCriterion(
                        id="has_catchup_config",
                        description="Configures catchup parameter",
                        weight=0.15,
                        validator_func="validate_catchup_config"
                    ),
                    ValidationCriterion(
                        id="has_default_args",
                        description="Uses default_args",
                        weight=0.15,
                        validator_func="validate_default_args"
                    ),
                    ValidationCriterion(
                        id="proper_task_structure",
                        description="Tasks are properly structured",
                        weight=0.15,
                        validator_func="validate_task_structure"
                    )
                ]
            },
            "03-tasks-operators": {
                "exercise-1": [
                    ValidationCriterion(
                        id="uses_bash_operator",
                        description="Uses BashOperator",
                        weight=0.4,
                        validator_func="validate_bash_operator_usage"
                    ),
                    ValidationCriterion(
                        id="proper_bash_commands",
                        description="Bash commands are appropriate",
                        weight=0.3,
                        validator_func="validate_bash_commands"
                    ),
                    ValidationCriterion(
                        id="task_naming",
                        description="Tasks have descriptive names",
                        weight=0.3,
                        validator_func="validate_task_naming"
                    )
                ],
                "exercise-2": [
                    ValidationCriterion(
                        id="uses_python_operator",
                        description="Uses PythonOperator",
                        weight=0.3,
                        validator_func="validate_python_operator_usage"
                    ),
                    ValidationCriterion(
                        id="defines_python_functions",
                        description="Defines Python functions for tasks",
                        weight=0.4,
                        validator_func="validate_python_functions"
                    ),
                    ValidationCriterion(
                        id="proper_function_calls",
                        description="Functions are properly called in operators",
                        weight=0.3,
                        validator_func="validate_function_calls"
                    )
                ]
            }
        }

    def _load_reference_solutions(self) -> Dict[str, str]:
        """Load reference solution file paths."""
        reference_solutions = {}
        modules_dir = Path("modules")

        if modules_dir.exists():
            for module_dir in modules_dir.iterdir():
                if module_dir.is_dir():
                    solutions_dir = module_dir / "solutions"
                    if solutions_dir.exists():
                        for solution_file in solutions_dir.glob("*.py"):
                            # Extract exercise ID from filename
                            # e.g., "exercise-1-solution.py" -> "exercise-1"
                            match = re.match(
                                r'(exercise-\d+)', solution_file.name)
                            if match:
                                exercise_id = match.group(1)
                                key = f"{module_dir.name}/{exercise_id}"
                                reference_solutions[key] = str(solution_file)

        return reference_solutions

    def validate_dag_import(self, content: str, lines: List[str]) -> Tuple[bool, str]:
        """Validate DAG import statement."""
        dag_imports = [
            "from airflow import DAG",
            "from airflow.models import DAG",
            "import airflow"
        ]

        has_import = any(imp in content for imp in dag_imports)

        if has_import:
            return True, "DAG import found"
        else:
            return False, "Missing DAG import. Add 'from airflow import DAG'"

    def validate_dag_definition(self, content: str, lines: List[str]) -> Tuple[bool, str]:
        """Validate DAG definition."""
        dag_patterns = [
            r'dag\s*=\s*DAG\s*\(',
            r'\w+\s*=\s*DAG\s*\(',
            r'DAG\s*\('
        ]

        has_dag = any(re.search(pattern, content, re.IGNORECASE)
                      for pattern in dag_patterns)

        if has_dag:
            return True, "DAG definition found"
        else:
            return False, "No DAG definition found. Create a DAG instance."

    def validate_has_tasks(self, content: str, lines: List[str]) -> Tuple[bool, str]:
        """Validate presence of tasks."""
        operator_patterns = [
            r'\w*Operator\s*\(',
            r'@task',
            r'TaskGroup'
        ]

        task_count = sum(len(re.findall(pattern, content))
                         for pattern in operator_patterns)

        if task_count > 0:
            return True, f"Found {task_count} task(s)"
        else:
            return False, "No tasks found. Add operators like BashOperator or PythonOperator."

    def validate_has_dependencies(self, content: str, lines: List[str]) -> Tuple[bool, str]:
        """Validate task dependencies."""
        dependency_patterns = [
            r'>>',
            r'set_downstream',
            r'set_upstream',
            r'<<'
        ]

        has_dependencies = any(re.search(pattern, content)
                               for pattern in dependency_patterns)

        # Count tasks to see if dependencies are needed
        task_count = len(re.findall(r'\w*Operator\s*\(', content))

        if task_count <= 1:
            return True, "Single task - no dependencies needed"
        elif has_dependencies:
            return True, "Task dependencies defined"
        else:
            return False, "Multiple tasks but no dependencies defined. Use >> to set task order."

    def validate_dag_id(self, content: str, lines: List[str]) -> Tuple[bool, str]:
        """Validate DAG ID."""
        dag_id_match = re.search(r"DAG\s*\(\s*['\"]([^'\"]+)['\"]", content)

        if not dag_id_match:
            return False, "DAG ID not found or not properly formatted"

        dag_id = dag_id_match.group(1)

        # Check naming conventions
        if not re.match(r'^[a-z][a-z0-9_]*$', dag_id):
            return False, f"DAG ID '{dag_id}' should use snake_case"

        if len(dag_id) < 3:
            return False, f"DAG ID '{dag_id}' should be more descriptive"

        return True, f"DAG ID '{dag_id}' is properly formatted"

    def validate_schedule_interval(self, content: str, lines: List[str]) -> Tuple[bool, str]:
        """Validate schedule_interval configuration."""
        if 'schedule_interval' in content or 'schedule=' in content:
            return True, "Schedule interval configured"
        else:
            return False, "Missing schedule_interval. Add schedule_interval parameter to DAG."

    def validate_start_date(self, content: str, lines: List[str]) -> Tuple[bool, str]:
        """Validate start_date configuration."""
        if 'start_date' in content:
            return True, "Start date configured"
        else:
            return False, "Missing start_date. Add start_date parameter to DAG or default_args."

    def validate_catchup_config(self, content: str, lines: List[str]) -> Tuple[bool, str]:
        """Validate catchup configuration."""
        if 'catchup' in content:
            return True, "Catchup parameter configured"
        else:
            return False, "Consider adding catchup=False to prevent backfilling."

    def validate_default_args(self, content: str, lines: List[str]) -> Tuple[bool, str]:
        """Validate default_args usage."""
        if 'default_args' in content:
            return True, "Default args configured"
        else:
            return False, "Consider using default_args for common task parameters."

    def validate_task_structure(self, content: str, lines: List[str]) -> Tuple[bool, str]:
        """Validate task structure."""
        # Find all operator instantiations
        operators = re.findall(r'(\w+)\s*=\s*(\w*Operator)\s*\(', content)

        issues = []
        for var_name, operator_type in operators:
            # Check for task_id
            operator_def_pattern = rf'{re.escape(var_name)}\s*=\s*{re.escape(operator_type)}\s*\([^)]*\)'
            operator_match = re.search(
                operator_def_pattern, content, re.DOTALL)

            if operator_match:
                operator_def = operator_match.group(0)

                if 'task_id=' not in operator_def:
                    issues.append(f"Missing task_id in {var_name}")

                if 'dag=' not in operator_def:
                    issues.append(f"Missing dag parameter in {var_name}")

        if issues:
            return False, "; ".join(issues)
        else:
            return True, "All tasks properly structured"

    def validate_bash_operator_usage(self, content: str, lines: List[str]) -> Tuple[bool, str]:
        """Validate BashOperator usage."""
        if 'BashOperator' not in content:
            return False, "BashOperator not used. Import and use BashOperator for bash commands."

        bash_operators = re.findall(
            r'BashOperator\s*\([^)]*\)', content, re.DOTALL)

        if not bash_operators:
            return False, "BashOperator imported but not instantiated."

        return True, f"Found {len(bash_operators)} BashOperator(s)"

    def validate_bash_commands(self, content: str, lines: List[str]) -> Tuple[bool, str]:
        """Validate bash commands in BashOperators."""
        bash_command_matches = re.findall(
            r"bash_command\s*=\s*['\"]([^'\"]+)['\"]", content)

        if not bash_command_matches:
            return False, "No bash_command found in BashOperator"

        # Check for meaningful commands (not just echo)
        meaningful_commands = [
            cmd for cmd in bash_command_matches if not cmd.strip().startswith('echo')]

        if len(meaningful_commands) == 0 and len(bash_command_matches) > 0:
            return True, "Basic bash commands found (consider more complex commands)"

        return True, f"Found {len(bash_command_matches)} bash command(s)"

    def validate_python_operator_usage(self, content: str, lines: List[str]) -> Tuple[bool, str]:
        """Validate PythonOperator usage."""
        if 'PythonOperator' not in content:
            return False, "PythonOperator not used. Import and use PythonOperator for Python functions."

        python_operators = re.findall(
            r'PythonOperator\s*\([^)]*\)', content, re.DOTALL)

        if not python_operators:
            return False, "PythonOperator imported but not instantiated."

        return True, f"Found {len(python_operators)} PythonOperator(s)"

    def validate_python_functions(self, content: str, lines: List[str]) -> Tuple[bool, str]:
        """Validate Python function definitions."""
        function_defs = re.findall(r'def\s+(\w+)\s*\([^)]*\):', content)

        if not function_defs:
            return False, "No Python functions defined. Define functions for PythonOperator tasks."

        return True, f"Found {len(function_defs)} function(s): {', '.join(function_defs)}"

    def validate_function_calls(self, content: str, lines: List[str]) -> Tuple[bool, str]:
        """Validate function calls in PythonOperators."""
        python_callable_matches = re.findall(
            r"python_callable\s*=\s*(\w+)", content)

        if not python_callable_matches:
            return False, "No python_callable found in PythonOperator"

        # Check if referenced functions are defined
        function_defs = re.findall(r'def\s+(\w+)\s*\([^)]*\):', content)

        undefined_functions = [
            func for func in python_callable_matches if func not in function_defs]

        if undefined_functions:
            return False, f"Undefined functions referenced: {', '.join(undefined_functions)}"

        return True, f"All {len(python_callable_matches)} function call(s) properly defined"

    def validate_task_naming(self, content: str, lines: List[str]) -> Tuple[bool, str]:
        """Validate task naming conventions."""
        task_ids = re.findall(r"task_id\s*=\s*['\"]([^'\"]+)['\"]", content)

        if not task_ids:
            return False, "No task IDs found"

        issues = []
        for task_id in task_ids:
            if not re.match(r'^[a-z][a-z0-9_]*$', task_id):
                issues.append(f"'{task_id}' should use snake_case")

            if len(task_id) < 3:
                issues.append(f"'{task_id}' should be more descriptive")

        if issues:
            return False, "; ".join(issues)

        return True, f"All {len(task_ids)} task ID(s) properly named"

    def validate_solution(self, student_file: str, module: str, exercise_id: str) -> ValidationResult:
        """Validate a student solution against criteria."""
        result = ValidationResult(
            exercise_id=exercise_id,
            module=module,
            student_file=student_file,
            reference_file=None,
            passed=False,
            score=0.0,
            criteria_met=[],
            criteria_failed=[],
            feedback=[],
            suggestions=[],
            errors=[]
        )

        # Check if file exists
        if not os.path.exists(student_file):
            result.errors.append(f"File not found: {student_file}")
            return result

        # Load file content
        try:
            with open(student_file, 'r', encoding='utf-8') as f:
                content = f.read()
                lines = content.split('\n')
        except Exception as e:
            result.errors.append(f"Error reading file: {e}")
            return result

        # Check syntax first
        try:
            ast.parse(content)
        except SyntaxError as e:
            result.errors.append(f"Syntax error: {e.msg} (line {e.lineno})")
            return result

        # Get validation criteria
        if module not in self.validation_criteria:
            result.errors.append(
                f"No validation criteria for module: {module}")
            return result

        if exercise_id not in self.validation_criteria[module]:
            result.errors.append(
                f"No validation criteria for exercise: {exercise_id}")
            return result

        criteria = self.validation_criteria[module][exercise_id]
        total_weight = sum(criterion.weight for criterion in criteria)
        achieved_weight = 0.0

        # Validate each criterion
        for criterion in criteria:
            validator_func = getattr(self, criterion.validator_func, None)

            if not validator_func:
                result.errors.append(
                    f"Validator function not found: {criterion.validator_func}")
                continue

            try:
                passed, message = validator_func(content, lines)

                if passed:
                    result.criteria_met.append(criterion.description)
                    achieved_weight += criterion.weight
                    result.feedback.append(
                        f"✅ {criterion.description}: {message}")
                else:
                    result.criteria_failed.append(criterion.description)
                    result.feedback.append(
                        f"❌ {criterion.description}: {message}")

                    if criterion.required:
                        result.errors.append(
                            f"Required criterion failed: {criterion.description}")

            except Exception as e:
                result.errors.append(
                    f"Error validating {criterion.description}: {e}")

        # Calculate score
        result.score = achieved_weight / total_weight if total_weight > 0 else 0.0
        result.passed = result.score >= 0.8 and len([c for c in criteria if c.required]) == len(
            [c for c in criteria if c.required and c.description in result.criteria_met])

        # Add reference solution comparison if available
        reference_key = f"{module}/{exercise_id}"
        if reference_key in self.reference_solutions:
            result.reference_file = self.reference_solutions[reference_key]
            comparison_feedback = self._compare_with_reference(
                student_file, result.reference_file)
            result.suggestions.extend(comparison_feedback)

        return result

    def _compare_with_reference(self, student_file: str, reference_file: str) -> List[str]:
        """Compare student solution with reference solution."""
        suggestions = []

        try:
            with open(student_file, 'r') as f:
                student_content = f.read()

            with open(reference_file, 'r') as f:
                reference_content = f.read()

            # Basic structural comparison
            student_lines = [line.strip()
                             for line in student_content.split('\n') if line.strip()]
            reference_lines = [
                line.strip() for line in reference_content.split('\n') if line.strip()]

            # Check for missing imports in student solution
            reference_imports = [
                line for line in reference_lines if line.startswith(('import ', 'from '))]
            student_imports = [
                line for line in student_lines if line.startswith(('import ', 'from '))]

            missing_imports = []
            for ref_import in reference_imports:
                if not any(ref_import.split()[1] in student_import for student_import in student_imports):
                    missing_imports.append(ref_import)

            if missing_imports:
                suggestions.append(
                    f"Consider adding imports: {', '.join(missing_imports)}")

            # Check for similar structure
            if len(student_lines) < len(reference_lines) * 0.5:
                suggestions.append(
                    "Your solution seems shorter than expected. Check if you've implemented all requirements.")

        except Exception as e:
            suggestions.append(
                f"Could not compare with reference solution: {e}")

        return suggestions

    def generate_validation_report(self, result: ValidationResult) -> str:
        """Generate a formatted validation report."""
        report_lines = []
        report_lines.append(f"📋 SOLUTION VALIDATION REPORT")
        report_lines.append("=" * 50)
        report_lines.append(f"Exercise: {result.module}/{result.exercise_id}")
        report_lines.append(f"Student File: {result.student_file}")
        if result.reference_file:
            report_lines.append(f"Reference: {result.reference_file}")
        report_lines.append("")

        # Overall result
        status_icon = "✅" if result.passed else "❌"
        report_lines.append(
            f"Status: {status_icon} {'PASSED' if result.passed else 'FAILED'}")
        report_lines.append(f"Score: {result.score:.1%}")
        report_lines.append("")

        # Criteria results
        if result.criteria_met or result.criteria_failed:
            report_lines.append("📊 CRITERIA RESULTS")
            report_lines.append("-" * 25)

            for feedback in result.feedback:
                report_lines.append(f"  {feedback}")

            report_lines.append("")

        # Errors
        if result.errors:
            report_lines.append("❌ ERRORS")
            report_lines.append("-" * 15)
            for error in result.errors:
                report_lines.append(f"  • {error}")
            report_lines.append("")

        # Suggestions
        if result.suggestions:
            report_lines.append("💡 SUGGESTIONS")
            report_lines.append("-" * 20)
            for suggestion in result.suggestions:
                report_lines.append(f"  • {suggestion}")
            report_lines.append("")

        # Next steps
        if not result.passed:
            report_lines.append("🎯 NEXT STEPS")
            report_lines.append("-" * 15)
            if result.errors:
                report_lines.append("  1. Fix the errors listed above")
            if result.criteria_failed:
                report_lines.append("  2. Address the failed criteria:")
                for criterion in result.criteria_failed:
                    report_lines.append(f"     • {criterion}")
            report_lines.append("  3. Test your solution and resubmit")
        else:
            report_lines.append("🎉 CONGRATULATIONS!")
            report_lines.append("-" * 20)
            report_lines.append("Your solution meets all requirements!")
            if result.suggestions:
                report_lines.append(
                    "Consider the suggestions above for even better code.")

        return "\n".join(report_lines)


def main():
    """Main function for solution validation."""
    parser = argparse.ArgumentParser(description='Validate student solutions')
    parser.add_argument(
        '--exercise',
        nargs=2,
        metavar=('MODULE', 'EXERCISE_ID'),
        help='Validate specific exercise (e.g., 01-setup exercise-1)'
    )
    parser.add_argument(
        '--student-file',
        type=str,
        help='Student solution file to validate'
    )
    parser.add_argument(
        '--validate-all',
        action='store_true',
        help='Validate all exercises in dags directory'
    )
    parser.add_argument(
        '--compare-solution',
        nargs=2,
        metavar=('STUDENT_FILE', 'REFERENCE_FILE'),
        help='Compare student solution with reference'
    )

    args = parser.parse_args()

    validator = SolutionValidator()

    if args.exercise:
        module, exercise_id = args.exercise

        # Determine student file
        if args.student_file:
            student_file = args.student_file
        else:
            # Try to find student file in dags directory
            possible_files = [
                f"dags/{exercise_id}.py",
                f"dags/{exercise_id}_solution.py",
                f"dags/my_{exercise_id}.py"
            ]

            student_file = None
            for possible_file in possible_files:
                if os.path.exists(possible_file):
                    student_file = possible_file
                    break

            if not student_file:
                print(
                    f"Error: Could not find student file for {module}/{exercise_id}")
                print(f"Tried: {', '.join(possible_files)}")
                print("Use --student-file to specify the file path")
                sys.exit(1)

        # Validate the solution
        result = validator.validate_solution(student_file, module, exercise_id)
        report = validator.generate_validation_report(result)
        print(report)

        sys.exit(0 if result.passed else 1)

    elif args.validate_all:
        # Validate all DAG files against known exercises
        dags_dir = "dags"
        if not os.path.exists(dags_dir):
            print(f"Error: {dags_dir} directory not found")
            sys.exit(1)

        dag_files = [f for f in os.listdir(dags_dir) if f.endswith(
            '.py') and not f.startswith('__')]

        if not dag_files:
            print(f"No Python files found in {dags_dir}")
            sys.exit(0)

        print(f"🔍 Validating {len(dag_files)} files in {dags_dir}")
        print("=" * 50)

        total_passed = 0
        for dag_file in dag_files:
            file_path = os.path.join(dags_dir, dag_file)

            # Try to match file to exercise
            # This is a simple heuristic - in practice you'd have better mapping
            if "exercise-1" in dag_file or "first" in dag_file:
                result = validator.validate_solution(
                    file_path, "01-setup", "exercise-1")
            elif "daily" in dag_file or "sales" in dag_file:
                result = validator.validate_solution(
                    file_path, "02-dag-fundamentals", "exercise-1")
            else:
                print(f"⚠️  {dag_file}: Could not determine exercise type")
                continue

            print(f"\n{validator.generate_validation_report(result)}")

            if result.passed:
                total_passed += 1

        print(
            f"\n📊 SUMMARY: {total_passed}/{len(dag_files)} solutions passed validation")

    elif args.compare_solution:
        student_file, reference_file = args.compare_solution

        if not os.path.exists(student_file):
            print(f"Error: Student file not found: {student_file}")
            sys.exit(1)

        if not os.path.exists(reference_file):
            print(f"Error: Reference file not found: {reference_file}")
            sys.exit(1)

        suggestions = validator._compare_with_reference(
            student_file, reference_file)

        print(f"📊 COMPARISON: {student_file} vs {reference_file}")
        print("=" * 60)

        if suggestions:
            print("💡 SUGGESTIONS:")
            for suggestion in suggestions:
                print(f"  • {suggestion}")
        else:
            print("✅ No specific suggestions - your solution looks good!")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
