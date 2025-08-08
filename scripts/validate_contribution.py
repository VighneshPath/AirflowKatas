#!/usr/bin/env python3
"""
Contribution Validation Script

This script validates contributions to ensure they meet the project standards
before being merged. It checks code quality, structure, and completeness.

Usage:
    python scripts/validate_contribution.py --module 01-setup
    python scripts/validate_contribution.py --pr 123
    python scripts/validate_contribution.py --all
"""

from validate_exercises import ExerciseValidator
from validate_dags import DAGValidator
import os
import sys
import argparse
import subprocess
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import json

# Import validation modules
sys.path.insert(0, str(Path(__file__).parent))


@dataclass
class ValidationIssue:
    """Represents a validation issue."""
    severity: str  # 'error', 'warning', 'info'
    category: str  # 'structure', 'code', 'documentation', 'tests'
    message: str
    file_path: Optional[str] = None
    line_number: Optional[int] = None


@dataclass
class ContributionValidationResult:
    """Results of contribution validation."""
    passed: bool
    score: float  # 0.0 to 1.0
    issues: List[ValidationIssue]
    summary: Dict[str, Any]


class ContributionValidator:
    """Validates contributions for quality and completeness."""

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.dag_validator = DAGValidator(verbose=verbose)
        self.exercise_validator = ExerciseValidator(verbose=verbose)
        self.issues = []

    def log(self, message: str, level: str = "INFO"):
        """Log messages based on verbosity setting."""
        if self.verbose or level in ["ERROR", "WARNING"]:
            print(f"[{level}] {message}")

    def add_issue(self, severity: str, category: str, message: str,
                  file_path: str = None, line_number: int = None):
        """Add a validation issue."""
        issue = ValidationIssue(
            severity=severity,
            category=category,
            message=message,
            file_path=file_path,
            line_number=line_number
        )
        self.issues.append(issue)
        self.log(f"{severity.upper()}: {message}", severity.upper())

    def validate_module_structure(self, module_path: Path) -> bool:
        """Validate module directory structure."""
        self.log(f"Validating module structure: {module_path}")

        if not module_path.exists():
            self.add_issue("error", "structure",
                           f"Module directory does not exist: {module_path}")
            return False

        # Required files
        required_files = ['README.md', 'concepts.md', 'resources.md']
        for file_name in required_files:
            file_path = module_path / file_name
            if not file_path.exists():
                self.add_issue("error", "structure",
                               f"Missing required file: {file_path}")
            elif file_path.stat().st_size == 0:
                self.add_issue("warning", "structure",
                               f"Empty file: {file_path}")

        # Required directories
        required_dirs = ['examples', 'exercises', 'solutions']
        for dir_name in required_dirs:
            dir_path = module_path / dir_name
            if not dir_path.exists():
                self.add_issue("error", "structure",
                               f"Missing required directory: {dir_path}")
            elif not any(dir_path.iterdir()):
                self.add_issue("warning", "structure",
                               f"Empty directory: {dir_path}")

        return len([issue for issue in self.issues if issue.severity == "error"]) == 0

    def validate_module_content(self, module_path: Path) -> bool:
        """Validate module content quality."""
        self.log(f"Validating module content: {module_path}")

        # Validate README.md
        readme_path = module_path / 'README.md'
        if readme_path.exists():
            self._validate_readme_content(readme_path)

        # Validate concepts.md
        concepts_path = module_path / 'concepts.md'
        if concepts_path.exists():
            self._validate_concepts_content(concepts_path)

        # Validate resources.md
        resources_path = module_path / 'resources.md'
        if resources_path.exists():
            self._validate_resources_content(resources_path)

        return True

    def _validate_readme_content(self, readme_path: Path):
        """Validate README.md content."""
        try:
            content = readme_path.read_text()

            # Check for required sections
            required_sections = [
                'Learning Objectives',
                'Prerequisites',
                'Module Overview',
                'Getting Started'
            ]

            for section in required_sections:
                if section not in content:
                    self.add_issue("warning", "documentation",
                                   f"Missing section '{section}' in README.md", str(readme_path))

            # Check for learning objectives checkboxes
            if '- [ ]' not in content:
                self.add_issue("warning", "documentation",
                               "No checkboxes found in learning objectives", str(readme_path))

            # Check minimum content length
            if len(content) < 500:
                self.add_issue("warning", "documentation",
                               "README.md content seems too brief", str(readme_path))

        except Exception as e:
            self.add_issue("error", "documentation",
                           f"Error reading README.md: {e}", str(readme_path))

    def _validate_concepts_content(self, concepts_path: Path):
        """Validate concepts.md content."""
        try:
            content = concepts_path.read_text()

            # Check for code examples
            if '```python' not in content:
                self.add_issue("warning", "documentation",
                               "No Python code examples found in concepts.md", str(concepts_path))

            # Check for practical applications
            if 'practical' not in content.lower() and 'example' not in content.lower():
                self.add_issue("warning", "documentation",
                               "No practical applications or examples mentioned", str(concepts_path))

            # Check minimum content length
            if len(content) < 1000:
                self.add_issue("warning", "documentation",
                               "concepts.md content seems too brief", str(concepts_path))

        except Exception as e:
            self.add_issue("error", "documentation",
                           f"Error reading concepts.md: {e}", str(concepts_path))

    def _validate_resources_content(self, resources_path: Path):
        """Validate resources.md content."""
        try:
            content = resources_path.read_text()

            # Check for external links
            if 'http' not in content:
                self.add_issue("warning", "documentation",
                               "No external links found in resources.md", str(resources_path))

            # Check for official Airflow documentation links
            if 'airflow.apache.org' not in content:
                self.add_issue("warning", "documentation",
                               "No official Airflow documentation links found", str(resources_path))

        except Exception as e:
            self.add_issue("error", "documentation",
                           f"Error reading resources.md: {e}", str(resources_path))

    def validate_examples(self, module_path: Path) -> bool:
        """Validate example DAGs."""
        self.log(f"Validating examples: {module_path}")

        examples_dir = module_path / 'examples'
        if not examples_dir.exists():
            return True  # Already reported as structure issue

        example_files = list(examples_dir.glob('*.py'))
        if not example_files:
            self.add_issue("warning", "code",
                           "No example files found", str(examples_dir))
            return True

        for example_file in example_files:
            # Validate DAG syntax
            dag_result = self.dag_validator.validate_file(str(example_file))
            if not dag_result['overall_valid']:
                self.add_issue("error", "code",
                               f"Example DAG validation failed: {example_file.name}",
                               str(example_file))

            # Check for documentation
            self._validate_dag_documentation(example_file)

        return True

    def validate_exercises(self, module_path: Path) -> bool:
        """Validate exercises and solutions."""
        self.log(f"Validating exercises: {module_path}")

        exercises_dir = module_path / 'exercises'
        solutions_dir = module_path / 'solutions'

        if not exercises_dir.exists() or not solutions_dir.exists():
            return True  # Already reported as structure issue

        # Get exercise and solution files
        exercise_files = list(exercises_dir.glob('exercise-*.md'))
        solution_files = list(solutions_dir.glob('exercise-*-solution.py'))

        if not exercise_files:
            self.add_issue("warning", "structure",
                           "No exercise files found", str(exercises_dir))
            return True

        # Validate each exercise has a corresponding solution
        for exercise_file in exercise_files:
            exercise_name = exercise_file.stem
            expected_solution = f"{exercise_name}-solution.py"
            solution_path = solutions_dir / expected_solution

            if not solution_path.exists():
                self.add_issue("error", "structure",
                               f"Missing solution for {exercise_file.name}", str(solution_path))
            else:
                # Validate solution DAG
                dag_result = self.dag_validator.validate_file(
                    str(solution_path))
                if not dag_result['overall_valid']:
                    self.add_issue("error", "code",
                                   f"Solution DAG validation failed: {solution_path.name}",
                                   str(solution_path))

        # Validate exercise content
        for exercise_file in exercise_files:
            self._validate_exercise_content(exercise_file)

        return True

    def _validate_exercise_content(self, exercise_path: Path):
        """Validate exercise markdown content."""
        try:
            content = exercise_path.read_text()

            # Check for required sections
            required_sections = [
                'Objective',
                'Prerequisites',
                'Requirements',
                'Instructions',
                'Validation'
            ]

            for section in required_sections:
                if f"## {section}" not in content:
                    self.add_issue("warning", "documentation",
                                   f"Missing section '## {section}' in exercise", str(exercise_path))

            # Check for step-by-step instructions
            if 'Step 1' not in content and '### Step' not in content:
                self.add_issue("warning", "documentation",
                               "No step-by-step instructions found", str(exercise_path))

            # Check minimum content length
            if len(content) < 800:
                self.add_issue("warning", "documentation",
                               "Exercise content seems too brief", str(exercise_path))

        except Exception as e:
            self.add_issue("error", "documentation",
                           f"Error reading exercise file: {e}", str(exercise_path))

    def _validate_dag_documentation(self, dag_path: Path):
        """Validate DAG documentation and comments."""
        try:
            content = dag_path.read_text()

            # Check for module docstring
            if '"""' not in content[:500]:
                self.add_issue("warning", "code",
                               "Missing module docstring", str(dag_path))

            # Check for function docstrings
            if 'def ' in content and '"""' not in content[content.find('def '):]:
                self.add_issue("warning", "code",
                               "Functions missing docstrings", str(dag_path))

            # Check for DAG documentation
            if 'dag.doc_md' not in content and 'description=' not in content:
                self.add_issue("warning", "code",
                               "DAG missing documentation", str(dag_path))

        except Exception as e:
            self.add_issue("error", "code",
                           f"Error reading DAG file: {e}", str(dag_path))

    def validate_tests(self, module_path: Path) -> bool:
        """Validate that appropriate tests exist."""
        self.log(f"Validating tests for: {module_path}")

        module_name = module_path.name
        test_file_patterns = [
            f"tests/test_{module_name}.py",
            f"tests/test_{module_name.replace('-', '_')}.py",
            f"tests/modules/test_{module_name}.py"
        ]

        test_exists = False
        for pattern in test_file_patterns:
            if Path(pattern).exists():
                test_exists = True
                break

        if not test_exists:
            self.add_issue("warning", "tests",
                           f"No tests found for module {module_name}")

        return True

    def validate_code_quality(self, module_path: Path) -> bool:
        """Validate code quality using static analysis."""
        self.log(f"Validating code quality: {module_path}")

        python_files = []
        for subdir in ['examples', 'solutions']:
            subdir_path = module_path / subdir
            if subdir_path.exists():
                python_files.extend(subdir_path.glob('*.py'))

        for python_file in python_files:
            self._validate_python_file_quality(python_file)

        return True

    def _validate_python_file_quality(self, file_path: Path):
        """Validate individual Python file quality."""
        try:
            content = file_path.read_text()

            # Check for basic quality indicators
            lines = content.split('\n')

            # Check line length (PEP 8)
            for i, line in enumerate(lines, 1):
                if len(line) > 88:  # Slightly more lenient than PEP 8's 79
                    self.add_issue("warning", "code",
                                   f"Line too long ({len(line)} chars)", str(file_path), i)

            # Check for TODO comments in solutions
            if 'solution' in file_path.name.lower() and 'TODO' in content:
                self.add_issue("warning", "code",
                               "TODO comments found in solution file", str(file_path))

            # Check for proper imports
            if 'from airflow import DAG' not in content and 'import DAG' not in content:
                if 'dag' in file_path.name.lower():
                    self.add_issue("warning", "code",
                                   "DAG file missing DAG import", str(file_path))

            # Check for hardcoded dates
            if '2023' in content or '2022' in content:
                self.add_issue("info", "code",
                               "Hardcoded year found, consider using relative dates", str(file_path))

        except Exception as e:
            self.add_issue("error", "code",
                           f"Error analyzing Python file: {e}", str(file_path))

    def run_external_validations(self, module_path: Path) -> bool:
        """Run external validation tools."""
        self.log(f"Running external validations: {module_path}")

        # Run DAG validation
        try:
            result = subprocess.run(
                ['python', 'scripts/validate_dags.py',
                    '--dag-dir', str(module_path)],
                capture_output=True,
                text=True,
                timeout=60
            )
            if result.returncode != 0:
                self.add_issue("error", "code",
                               f"DAG validation failed: {result.stderr}")
        except subprocess.TimeoutExpired:
            self.add_issue("warning", "code", "DAG validation timed out")
        except Exception as e:
            self.add_issue("warning", "code",
                           f"Could not run DAG validation: {e}")

        return True

    def validate_module(self, module_path: Path) -> ContributionValidationResult:
        """Validate a complete module."""
        self.log(f"Starting module validation: {module_path}")
        self.issues = []  # Reset issues for this validation

        # Run all validation checks
        validations = [
            self.validate_module_structure,
            self.validate_module_content,
            self.validate_examples,
            self.validate_exercises,
            self.validate_tests,
            self.validate_code_quality,
            self.run_external_validations
        ]

        for validation_func in validations:
            try:
                validation_func(module_path)
            except Exception as e:
                self.add_issue("error", "validation",
                               f"Validation function {validation_func.__name__} failed: {e}")

        # Calculate score and determine pass/fail
        error_count = len(
            [issue for issue in self.issues if issue.severity == "error"])
        warning_count = len(
            [issue for issue in self.issues if issue.severity == "warning"])
        info_count = len(
            [issue for issue in self.issues if issue.severity == "info"])

        # Scoring: errors are heavily penalized, warnings moderately, info minimally
        total_penalty = error_count * 0.2 + warning_count * 0.05 + info_count * 0.01
        score = max(0.0, 1.0 - total_penalty)

        # Pass if no errors and score > 0.7
        passed = error_count == 0 and score > 0.7

        summary = {
            'total_issues': len(self.issues),
            'errors': error_count,
            'warnings': warning_count,
            'info': info_count,
            'score': score,
            'passed': passed
        }

        return ContributionValidationResult(
            passed=passed,
            score=score,
            issues=self.issues.copy(),
            summary=summary
        )

    def validate_all_modules(self) -> Dict[str, ContributionValidationResult]:
        """Validate all modules in the project."""
        self.log("Validating all modules")

        results = {}
        modules_dir = Path('modules')

        if not modules_dir.exists():
            self.log("Modules directory not found", "ERROR")
            return results

        for module_dir in sorted(modules_dir.iterdir()):
            if module_dir.is_dir() and module_dir.name.startswith(('01-', '02-', '03-', '04-', '05-', '06-', '07-', '08-', '09-', '10-')):
                result = self.validate_module(module_dir)
                results[module_dir.name] = result

        return results

    def generate_report(self, results: Dict[str, ContributionValidationResult]) -> str:
        """Generate a comprehensive validation report."""
        report_lines = []
        report_lines.append("# Contribution Validation Report")
        report_lines.append(
            f"Generated: {__import__('datetime').datetime.now().isoformat()}")
        report_lines.append("")

        # Summary
        total_modules = len(results)
        passed_modules = sum(1 for result in results.values() if result.passed)
        avg_score = sum(result.score for result in results.values()
                        ) / total_modules if total_modules > 0 else 0.0

        report_lines.append("## Summary")
        report_lines.append(f"- **Total Modules**: {total_modules}")
        report_lines.append(f"- **Passed Modules**: {passed_modules}")
        report_lines.append(
            f"- **Failed Modules**: {total_modules - passed_modules}")
        report_lines.append(f"- **Average Score**: {avg_score:.2f}")
        report_lines.append("")

        # Module Results
        report_lines.append("## Module Results")
        for module_name, result in results.items():
            status = "✅ PASSED" if result.passed else "❌ FAILED"
            report_lines.append(
                f"### {module_name}: {status} (Score: {result.score:.2f})")

            if result.issues:
                report_lines.append("#### Issues:")
                for issue in result.issues:
                    icon = {"error": "🔴", "warning": "🟡",
                            "info": "🔵"}.get(issue.severity, "⚪")
                    location = f" ({issue.file_path}:{issue.line_number})" if issue.file_path and issue.line_number else f" ({issue.file_path})" if issue.file_path else ""
                    report_lines.append(
                        f"- {icon} **{issue.severity.upper()}**: {issue.message}{location}")
            else:
                report_lines.append("No issues found.")

            report_lines.append("")

        return "\n".join(report_lines)


def main():
    """Main function for contribution validation."""
    parser = argparse.ArgumentParser(
        description='Validate contributions to Airflow Kata')
    parser.add_argument(
        '--module',
        type=str,
        help='Validate specific module (e.g., "01-setup")'
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help='Validate all modules'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose output'
    )
    parser.add_argument(
        '--report-file',
        type=str,
        help='Save report to file'
    )
    parser.add_argument(
        '--json-output',
        action='store_true',
        help='Output results in JSON format'
    )

    args = parser.parse_args()

    validator = ContributionValidator(verbose=args.verbose)

    if args.module:
        # Validate specific module
        module_path = Path('modules') / args.module
        if not module_path.exists():
            print(f"❌ Module not found: {module_path}")
            sys.exit(1)

        result = validator.validate_module(module_path)

        if args.json_output:
            # Output JSON
            output = {
                'module': args.module,
                'passed': result.passed,
                'score': result.score,
                'summary': result.summary,
                'issues': [
                    {
                        'severity': issue.severity,
                        'category': issue.category,
                        'message': issue.message,
                        'file_path': issue.file_path,
                        'line_number': issue.line_number
                    }
                    for issue in result.issues
                ]
            }
            print(json.dumps(output, indent=2))
        else:
            # Human-readable output
            status = "✅ PASSED" if result.passed else "❌ FAILED"
            print(f"\n📋 Validation Result for {args.module}: {status}")
            print(f"Score: {result.score:.2f}")
            print(
                f"Issues: {result.summary['errors']} errors, {result.summary['warnings']} warnings, {result.summary['info']} info")

            if result.issues:
                print(f"\n📝 Issues Found:")
                for issue in result.issues:
                    icon = {"error": "🔴", "warning": "🟡",
                            "info": "🔵"}.get(issue.severity, "⚪")
                    location = f" ({issue.file_path})" if issue.file_path else ""
                    print(
                        f"  {icon} {issue.severity.upper()}: {issue.message}{location}")

        sys.exit(0 if result.passed else 1)

    elif args.all:
        # Validate all modules
        results = validator.validate_all_modules()

        if args.json_output:
            # Output JSON
            output = {
                'timestamp': __import__('datetime').datetime.now().isoformat(),
                'summary': {
                    'total_modules': len(results),
                    'passed_modules': sum(1 for r in results.values() if r.passed),
                    'average_score': sum(r.score for r in results.values()) / len(results) if results else 0.0
                },
                'modules': {
                    name: {
                        'passed': result.passed,
                        'score': result.score,
                        'summary': result.summary,
                        'issues': [
                            {
                                'severity': issue.severity,
                                'category': issue.category,
                                'message': issue.message,
                                'file_path': issue.file_path,
                                'line_number': issue.line_number
                            }
                            for issue in result.issues
                        ]
                    }
                    for name, result in results.items()
                }
            }
            print(json.dumps(output, indent=2))
        else:
            # Generate and display report
            report = validator.generate_report(results)
            print(report)

            # Save report if requested
            if args.report_file:
                with open(args.report_file, 'w') as f:
                    f.write(report)
                print(f"\n📄 Report saved to: {args.report_file}")

        # Exit with error code if any module failed
        failed_modules = sum(
            1 for result in results.values() if not result.passed)
        sys.exit(0 if failed_modules == 0 else 1)

    else:
        print("❌ Please specify --module <name> or --all")
        sys.exit(1)


if __name__ == "__main__":
    main()
