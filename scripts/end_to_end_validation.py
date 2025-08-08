#!/usr/bin/env python3
"""
End-to-End Validation Script

This script performs comprehensive validation of the entire Airflow Coding Kata,
ensuring all modules work together and students can complete the full learning path.

Usage:
    python scripts/end_to_end_validation.py
    python scripts/end_to_end_validation.py --full-walkthrough
    python scripts/end_to_end_validation.py --integration-tests
    python scripts/end_to_end_validation.py --final-assessment
"""

from progress_tracker import ProgressTracker
from validate_exercises import ExerciseValidator, ExerciseValidationResult
from validate_dags import DAGValidator
import os
import sys
import json
import argparse
import subprocess
import tempfile
import shutil
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
import importlib.util

# Import our validation modules
sys.path.insert(0, str(Path(__file__).parent))


@dataclass
class ValidationResult:
    """Results of end-to-end validation."""
    test_name: str
    passed: bool
    score: float
    details: Dict[str, Any]
    errors: List[str]
    warnings: List[str]
    execution_time: float


@dataclass
class WalkthroughResult:
    """Results of complete kata walkthrough."""
    total_modules: int
    completed_modules: int
    total_exercises: int
    completed_exercises: int
    overall_score: float
    time_taken: float
    module_results: Dict[str, Any]
    critical_errors: List[str]


class EndToEndValidator:
    """Comprehensive validation of the entire kata."""

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.dag_validator = DAGValidator(verbose=verbose)
        self.exercise_validator = ExerciseValidator(verbose=verbose)
        self.progress_tracker = ProgressTracker()
        self.test_results = []

    def log(self, message: str, level: str = "INFO"):
        """Log messages based on verbosity setting."""
        if self.verbose or level in ["ERROR", "WARNING"]:
            timestamp = datetime.now().strftime("%H:%M:%S")
            print(f"[{timestamp}] [{level}] {message}")

    def validate_kata_structure(self) -> ValidationResult:
        """Validate the overall kata directory structure."""
        start_time = datetime.now()
        errors = []
        warnings = []
        details = {}

        self.log("Validating kata directory structure...")

        # Required directories
        required_dirs = [
            'modules', 'dags', 'scripts', 'data', 'tests',
            'plugins', 'logs', 'config', 'resources', 'solutions'
        ]

        # Required files
        required_files = [
            'README.md', 'docker-compose.yml', 'requirements.txt',
            'pyproject.toml', '.gitignore'
        ]

        # Check directories
        missing_dirs = []
        for dir_name in required_dirs:
            if not os.path.exists(dir_name):
                missing_dirs.append(dir_name)

        if missing_dirs:
            errors.append(f"Missing required directories: {missing_dirs}")

        details['directories'] = {
            'required': required_dirs,
            'missing': missing_dirs,
            'found': [d for d in required_dirs if d not in missing_dirs]
        }

        # Check files
        missing_files = []
        for file_name in required_files:
            if not os.path.exists(file_name):
                missing_files.append(file_name)

        if missing_files:
            errors.append(f"Missing required files: {missing_files}")

        details['files'] = {
            'required': required_files,
            'missing': missing_files,
            'found': [f for f in required_files if f not in missing_files]
        }

        # Check module structure
        modules_dir = Path('modules')
        if modules_dir.exists():
            module_structure_issues = self._validate_module_structure(
                modules_dir)
            if module_structure_issues:
                warnings.extend(module_structure_issues)
            details['module_structure'] = module_structure_issues

        execution_time = (datetime.now() - start_time).total_seconds()
        passed = len(errors) == 0

        return ValidationResult(
            test_name="kata_structure",
            passed=passed,
            score=1.0 if passed else 0.5 if len(warnings) == 0 else 0.0,
            details=details,
            errors=errors,
            warnings=warnings,
            execution_time=execution_time
        )

    def _validate_module_structure(self, modules_dir: Path) -> List[str]:
        """Validate individual module structure."""
        issues = []
        expected_modules = [
            '01-setup', '02-dag-fundamentals', '03-tasks-operators',
            '04-scheduling-dependencies', '05-sensors-triggers',
            '06-data-passing-xcoms', '07-branching-conditionals',
            '08-error-handling', '09-advanced-patterns', '10-real-world-projects'
        ]

        for module_name in expected_modules:
            module_path = modules_dir / module_name
            if not module_path.exists():
                issues.append(f"Missing module: {module_name}")
                continue

            # Check required files in each module
            required_module_files = [
                'README.md', 'concepts.md', 'resources.md']
            required_module_dirs = ['examples', 'exercises', 'solutions']

            for file_name in required_module_files:
                if not (module_path / file_name).exists():
                    issues.append(f"Missing {file_name} in {module_name}")

            for dir_name in required_module_dirs:
                if not (module_path / dir_name).exists():
                    issues.append(
                        f"Missing {dir_name} directory in {module_name}")

        return issues

    def validate_docker_environment(self) -> ValidationResult:
        """Validate Docker environment setup."""
        start_time = datetime.now()
        errors = []
        warnings = []
        details = {}

        self.log("Validating Docker environment...")

        # Check if docker-compose.yml exists and is valid
        if not os.path.exists('docker-compose.yml'):
            errors.append("docker-compose.yml not found")
        else:
            try:
                # Try to validate docker-compose file
                result = subprocess.run(
                    ['docker-compose', 'config'],
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                if result.returncode != 0:
                    errors.append(
                        f"Invalid docker-compose.yml: {result.stderr}")
                else:
                    details['docker_compose'] = "Valid configuration"
            except subprocess.TimeoutExpired:
                warnings.append("Docker compose validation timed out")
            except FileNotFoundError:
                warnings.append("Docker Compose not installed or not in PATH")

        # Check if Docker is running (optional)
        try:
            result = subprocess.run(
                ['docker', 'ps'],
                capture_output=True,
                text=True,
                timeout=10
            )
            if result.returncode == 0:
                details['docker_status'] = "Docker is running"
            else:
                warnings.append("Docker is not running")
        except (subprocess.TimeoutExpired, FileNotFoundError):
            warnings.append("Could not check Docker status")

        execution_time = (datetime.now() - start_time).total_seconds()
        passed = len(errors) == 0

        return ValidationResult(
            test_name="docker_environment",
            passed=passed,
            score=1.0 if passed else 0.7 if len(warnings) < 2 else 0.3,
            details=details,
            errors=errors,
            warnings=warnings,
            execution_time=execution_time
        )

    def validate_all_dags_integration(self) -> ValidationResult:
        """Validate all DAGs work together without conflicts."""
        start_time = datetime.now()
        errors = []
        warnings = []
        details = {}

        self.log("Validating DAG integration...")

        # Collect all DAG files
        dag_files = []
        for root, dirs, files in os.walk('.'):
            for file in files:
                if file.endswith('.py') and 'dag' in file.lower():
                    # Skip test files and __pycache__
                    if not any(skip in root for skip in ['__pycache__', '.git', 'test']):
                        dag_files.append(os.path.join(root, file))

        details['total_dag_files'] = len(dag_files)

        # Validate each DAG file
        dag_results = self.dag_validator.validate_all_dags('.')
        details['dag_validation'] = dag_results['summary']

        if dag_results['summary']['invalid'] > 0:
            errors.extend(dag_results['errors'])

        # Check for DAG ID conflicts
        dag_ids = set()
        duplicate_dag_ids = []

        for dag_file in dag_files:
            try:
                with open(dag_file, 'r') as f:
                    content = f.read()

                # Extract DAG IDs (simplified regex approach)
                import re
                dag_id_patterns = [
                    r"dag_id\s*=\s*['\"]([^'\"]+)['\"]",
                    r"DAG\s*\(\s*['\"]([^'\"]+)['\"]"
                ]

                for pattern in dag_id_patterns:
                    matches = re.findall(pattern, content)
                    for dag_id in matches:
                        if dag_id in dag_ids:
                            duplicate_dag_ids.append(dag_id)
                        else:
                            dag_ids.add(dag_id)

            except Exception as e:
                warnings.append(
                    f"Could not parse DAG IDs from {dag_file}: {e}")

        if duplicate_dag_ids:
            errors.append(f"Duplicate DAG IDs found: {duplicate_dag_ids}")

        details['unique_dag_ids'] = len(dag_ids)
        details['duplicate_dag_ids'] = duplicate_dag_ids

        execution_time = (datetime.now() - start_time).total_seconds()
        passed = len(errors) == 0

        return ValidationResult(
            test_name="dag_integration",
            passed=passed,
            score=1.0 if passed else max(0.0, 1.0 - len(errors) * 0.2),
            details=details,
            errors=errors,
            warnings=warnings,
            execution_time=execution_time
        )

    def create_final_assessment(self) -> ValidationResult:
        """Create and validate final assessment exercises."""
        start_time = datetime.now()
        errors = []
        warnings = []
        details = {}

        self.log("Creating final assessment exercises...")

        assessment_dir = Path('assessments')
        assessment_dir.mkdir(exist_ok=True)

        # Create comprehensive final assessment
        final_assessment_content = self._generate_final_assessment_content()

        assessment_file = assessment_dir / 'final_assessment.md'
        try:
            with open(assessment_file, 'w') as f:
                f.write(final_assessment_content)
            details['assessment_file'] = str(assessment_file)
        except Exception as e:
            errors.append(f"Failed to create assessment file: {e}")

        # Create assessment solution
        solution_content = self._generate_final_assessment_solution()
        solution_file = assessment_dir / 'final_assessment_solution.py'

        try:
            with open(solution_file, 'w') as f:
                f.write(solution_content)

            # Validate the solution
            dag_result = self.dag_validator.validate_file(str(solution_file))
            if not dag_result['overall_valid']:
                errors.append("Final assessment solution is not valid")
            else:
                details['solution_valid'] = True

        except Exception as e:
            errors.append(f"Failed to create assessment solution: {e}")

        execution_time = (datetime.now() - start_time).total_seconds()
        passed = len(errors) == 0

        return ValidationResult(
            test_name="final_assessment",
            passed=passed,
            score=1.0 if passed else 0.0,
            details=details,
            errors=errors,
            warnings=warnings,
            execution_time=execution_time
        )

    def _generate_final_assessment_content(self) -> str:
        """Generate comprehensive final assessment content."""
        return '''# Final Assessment: Complete Data Pipeline

## Overview

This final assessment combines all concepts learned throughout the Airflow Coding Kata. You will build a complete data pipeline that demonstrates mastery of DAGs, tasks, operators, sensors, XComs, branching, error handling, and monitoring.

## Scenario

You are building a data pipeline for an e-commerce company that processes daily sales data, validates it, transforms it, and generates reports. The pipeline should handle various data quality scenarios and provide comprehensive monitoring.

## Requirements

### 1. DAG Configuration (Module 1-2 Concepts)
- Create a DAG named `ecommerce_data_pipeline`
- Schedule to run daily at 2 AM UTC
- Set appropriate start_date, catchup, and retry settings
- Include comprehensive documentation

### 2. Data Extraction Tasks (Module 3 Concepts)
- **extract_sales_data**: Use BashOperator to download sales data
- **extract_customer_data**: Use PythonOperator to fetch customer information
- **check_data_availability**: Use FileSensor to wait for required files

### 3. Data Validation and Quality Checks (Module 4-5 Concepts)
- **validate_data_format**: Check file formats and schemas
- **data_quality_checks**: Validate data completeness and accuracy
- **handle_data_issues**: Branch based on data quality results

### 4. Data Transformation (Module 6-7 Concepts)
- **clean_sales_data**: Clean and standardize sales data
- **enrich_customer_data**: Add customer segmentation
- **calculate_metrics**: Compute daily KPIs
- Use XComs to pass data between transformation tasks

### 5. Conditional Processing (Module 7 Concepts)
- **check_business_rules**: Validate business logic
- **route_processing**: Branch to different processing paths based on data volume
- **weekend_special_processing**: Additional processing for weekend data

### 6. Error Handling and Monitoring (Module 8 Concepts)
- Implement retry strategies for all tasks
- Add failure callbacks for critical tasks
- Set up SLA monitoring
- Create custom alerting for data quality issues

### 7. Advanced Patterns (Module 9 Concepts)
- Use TaskGroups to organize related tasks
- Implement dynamic task generation for multiple data sources
- Add performance optimization techniques

### 8. Real-world Integration (Module 10 Concepts)
- **api_integration**: Call external APIs for data enrichment
- **database_operations**: Write results to database
- **generate_reports**: Create business reports
- **send_notifications**: Notify stakeholders of completion

## Success Criteria

Your solution should demonstrate:

1. **Proper DAG Structure**: Well-organized, documented, and configured
2. **Task Dependencies**: Logical flow with appropriate dependencies
3. **Error Resilience**: Proper error handling and recovery
4. **Data Flow**: Effective use of XComs and data passing
5. **Conditional Logic**: Smart branching based on data conditions
6. **Monitoring**: Comprehensive logging and alerting
7. **Best Practices**: Following Airflow and Python best practices
8. **Real-world Readiness**: Production-ready code quality

## Bonus Challenges

- Implement custom operators for specific business logic
- Add data lineage tracking
- Create a custom XCom backend for large data
- Implement advanced scheduling with external triggers
- Add comprehensive unit tests

## Submission

Create your solution as `final_assessment_solution.py` in the assessments directory.
Include detailed comments explaining your design decisions and how each requirement is addressed.

## Time Estimate

This assessment should take 2-3 hours to complete thoroughly.
Focus on demonstrating understanding of concepts rather than complex business logic.
'''

    def _generate_final_assessment_solution(self) -> str:
        """Generate a comprehensive solution for the final assessment."""
        # Read the solution file we just created
        try:
            with open('assessments/final_assessment_solution.py', 'r') as f:
                return f.read()
        except FileNotFoundError:
            # Fallback minimal solution
            return '''"""
Final Assessment Solution: Complete Data Pipeline
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data-team',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    'ecommerce_data_pipeline',
    default_args=default_args,
    schedule_interval='0 2 * * *',
    catchup=False,
)

def extract_data():
    return {"status": "extracted"}

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)
'''

    def run_integration_tests(self) -> List[ValidationResult]:
        """Run comprehensive integration tests."""
        self.log("Running integration tests...")

        integration_tests = [
            self.validate_kata_structure,
            self.validate_docker_environment,
            self.validate_all_dags_integration,
            self.create_final_assessment,
        ]

        results = []
        for test_func in integration_tests:
            try:
                result = test_func()
                results.append(result)

                status = "✅ PASSED" if result.passed else "❌ FAILED"
                self.log(
                    f"{test_func.__name__}: {status} (Score: {result.score:.2f})")

            except Exception as e:
                error_result = ValidationResult(
                    test_name=test_func.__name__,
                    passed=False,
                    score=0.0,
                    details={'error': str(e)},
                    errors=[str(e)],
                    warnings=[],
                    execution_time=0.0
                )
                results.append(error_result)
                self.log(f"{test_func.__name__}: ❌ ERROR - {e}", "ERROR")

        return results

    def run_complete_walkthrough(self) -> WalkthroughResult:
        """Run a complete walkthrough of the entire kata."""
        start_time = datetime.now()
        self.log("Starting complete kata walkthrough...")

        # Run through each module
        module_results = {}
        total_modules = 0
        completed_modules = 0
        total_exercises = 0
        completed_exercises = 0
        total_score = 0.0
        critical_errors = []

        modules_dir = Path('modules')
        if modules_dir.exists():
            for module_dir in sorted(modules_dir.iterdir()):
                if module_dir.is_dir() and module_dir.name.startswith(('01-', '02-', '03-', '04-', '05-', '06-', '07-', '08-', '09-', '10-')):
                    total_modules += 1
                    module_name = module_dir.name

                    self.log(f"Processing module: {module_name}")

                    try:
                        module_result = self._validate_module_walkthrough(
                            module_name)
                        module_results[module_name] = module_result

                        total_exercises += module_result['total_exercises']
                        completed_exercises += module_result['completed_exercises']
                        total_score += module_result['average_score'] * \
                            module_result['total_exercises']

                        if module_result['completion_rate'] >= 0.8:
                            completed_modules += 1

                    except Exception as e:
                        critical_errors.append(
                            f"Failed to process module {module_name}: {e}")
                        module_results[module_name] = {
                            'error': str(e),
                            'total_exercises': 0,
                            'completed_exercises': 0,
                            'completion_rate': 0.0,
                            'average_score': 0.0
                        }

        overall_score = total_score / total_exercises if total_exercises > 0 else 0.0
        time_taken = (datetime.now() - start_time).total_seconds()

        return WalkthroughResult(
            total_modules=total_modules,
            completed_modules=completed_modules,
            total_exercises=total_exercises,
            completed_exercises=completed_exercises,
            overall_score=overall_score,
            time_taken=time_taken,
            module_results=module_results,
            critical_errors=critical_errors
        )

    def _validate_module_walkthrough(self, module_name: str) -> Dict[str, Any]:
        """Validate a single module in the walkthrough."""
        module_path = Path('modules') / module_name
        exercises_dir = module_path / 'exercises'
        solutions_dir = module_path / 'solutions'

        total_exercises = 0
        completed_exercises = 0
        exercise_scores = []

        if exercises_dir.exists():
            # Count exercises
            exercise_files = list(exercises_dir.glob('exercise-*.md'))
            total_exercises = len(exercise_files)

            # Validate solutions exist and work
            if solutions_dir.exists():
                solution_files = list(
                    solutions_dir.glob('exercise-*-solution.py'))

                for solution_file in solution_files:
                    try:
                        # Validate the solution
                        dag_result = self.dag_validator.validate_file(
                            str(solution_file))
                        if dag_result['overall_valid']:
                            completed_exercises += 1
                            exercise_scores.append(1.0)
                        else:
                            exercise_scores.append(0.5)

                    except Exception as e:
                        self.log(
                            f"Error validating solution {solution_file}: {e}", "WARNING")
                        exercise_scores.append(0.0)

        completion_rate = completed_exercises / \
            total_exercises if total_exercises > 0 else 0.0
        average_score = sum(exercise_scores) / \
            len(exercise_scores) if exercise_scores else 0.0

        return {
            'total_exercises': total_exercises,
            'completed_exercises': completed_exercises,
            'completion_rate': completion_rate,
            'average_score': average_score,
            'exercise_scores': exercise_scores
        }

    def generate_comprehensive_report(self, walkthrough_result: WalkthroughResult,
                                      integration_results: List[ValidationResult]) -> Dict[str, Any]:
        """Generate a comprehensive validation report."""

        # Calculate overall scores
        integration_score = sum(r.score for r in integration_results) / \
            len(integration_results) if integration_results else 0.0

        # Combine results
        report = {
            'validation_timestamp': datetime.now().isoformat(),
            'overall_status': 'PASSED' if integration_score >= 0.8 and walkthrough_result.overall_score >= 0.8 else 'FAILED',
            'overall_score': (integration_score + walkthrough_result.overall_score) / 2,

            'walkthrough_summary': {
                'total_modules': walkthrough_result.total_modules,
                'completed_modules': walkthrough_result.completed_modules,
                'module_completion_rate': walkthrough_result.completed_modules / walkthrough_result.total_modules if walkthrough_result.total_modules > 0 else 0.0,
                'total_exercises': walkthrough_result.total_exercises,
                'completed_exercises': walkthrough_result.completed_exercises,
                'exercise_completion_rate': walkthrough_result.completed_exercises / walkthrough_result.total_exercises if walkthrough_result.total_exercises > 0 else 0.0,
                'overall_score': walkthrough_result.overall_score,
                'time_taken': walkthrough_result.time_taken,
                'critical_errors': walkthrough_result.critical_errors
            },

            'integration_summary': {
                'total_tests': len(integration_results),
                'passed_tests': sum(1 for r in integration_results if r.passed),
                'failed_tests': sum(1 for r in integration_results if not r.passed),
                'average_score': integration_score,
                'total_errors': sum(len(r.errors) for r in integration_results),
                'total_warnings': sum(len(r.warnings) for r in integration_results)
            },

            'detailed_results': {
                'walkthrough_modules': walkthrough_result.module_results,
                'integration_tests': {r.test_name: {
                    'passed': r.passed,
                    'score': r.score,
                    'execution_time': r.execution_time,
                    'errors': r.errors,
                    'warnings': r.warnings,
                    'details': r.details
                } for r in integration_results}
            },

            'recommendations': self._generate_recommendations(walkthrough_result, integration_results)
        }

        return report

    def _generate_recommendations(self, walkthrough_result: WalkthroughResult,
                                  integration_results: List[ValidationResult]) -> List[str]:
        """Generate recommendations based on validation results."""
        recommendations = []

        # Walkthrough recommendations
        if walkthrough_result.completed_modules < walkthrough_result.total_modules:
            recommendations.append(
                f"Complete remaining {walkthrough_result.total_modules - walkthrough_result.completed_modules} modules")

        if walkthrough_result.overall_score < 0.8:
            recommendations.append(
                "Review and improve exercise solutions to achieve higher scores")

        if walkthrough_result.critical_errors:
            recommendations.append(
                "Address critical errors in module processing")

        # Integration test recommendations
        failed_tests = [r for r in integration_results if not r.passed]
        if failed_tests:
            recommendations.append(
                f"Fix issues in failed integration tests: {[r.test_name for r in failed_tests]}")

        # Specific recommendations based on test results
        for result in integration_results:
            if result.test_name == 'kata_structure' and not result.passed:
                recommendations.append(
                    "Ensure all required directories and files are present")
            elif result.test_name == 'docker_environment' and not result.passed:
                recommendations.append("Fix Docker environment setup issues")
            elif result.test_name == 'dag_integration' and not result.passed:
                recommendations.append(
                    "Resolve DAG conflicts and validation errors")

        if not recommendations:
            recommendations.append(
                "🎉 Excellent work! All validations passed successfully.")

        return recommendations


def main():
    """Main function for end-to-end validation."""
    parser = argparse.ArgumentParser(
        description='End-to-end Airflow Kata validation')
    parser.add_argument(
        '--full-walkthrough',
        action='store_true',
        help='Run complete kata walkthrough'
    )
    parser.add_argument(
        '--integration-tests',
        action='store_true',
        help='Run integration tests only'
    )
    parser.add_argument(
        '--final-assessment',
        action='store_true',
        help='Create final assessment only'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose output'
    )
    parser.add_argument(
        '--report-file',
        type=str,
        help='Save detailed report to file'
    )

    args = parser.parse_args()

    validator = EndToEndValidator(verbose=args.verbose)

    if args.final_assessment:
        # Create final assessment only
        result = validator.create_final_assessment()
        print(
            f"\n📝 Final Assessment Creation: {'✅ SUCCESS' if result.passed else '❌ FAILED'}")
        if result.errors:
            print("Errors:")
            for error in result.errors:
                print(f"  • {error}")
        return

    if args.integration_tests:
        # Run integration tests only
        results = validator.run_integration_tests()

        print(f"\n🔧 INTEGRATION TESTS SUMMARY")
        print("=" * 40)

        passed_tests = sum(1 for r in results if r.passed)
        total_tests = len(results)
        avg_score = sum(r.score for r in results) / \
            total_tests if total_tests > 0 else 0.0

        print(f"Tests Passed: {passed_tests}/{total_tests}")
        print(f"Average Score: {avg_score:.1%}")

        for result in results:
            status = "✅" if result.passed else "❌"
            print(f"{status} {result.test_name}: {result.score:.1%}")

        return

    if args.full_walkthrough:
        # Run complete walkthrough
        print("🚀 Starting complete kata walkthrough...")
        walkthrough_result = validator.run_complete_walkthrough()

        print(f"\n📊 WALKTHROUGH RESULTS")
        print("=" * 40)
        print(
            f"Modules: {walkthrough_result.completed_modules}/{walkthrough_result.total_modules}")
        print(
            f"Exercises: {walkthrough_result.completed_exercises}/{walkthrough_result.total_exercises}")
        print(f"Overall Score: {walkthrough_result.overall_score:.1%}")
        print(f"Time Taken: {walkthrough_result.time_taken:.1f} seconds")

        if walkthrough_result.critical_errors:
            print(f"\n❌ Critical Errors:")
            for error in walkthrough_result.critical_errors:
                print(f"  • {error}")

        return

    # Default: Run everything
    print("🎯 Running comprehensive end-to-end validation...")

    # Run walkthrough
    walkthrough_result = validator.run_complete_walkthrough()

    # Run integration tests
    integration_results = validator.run_integration_tests()

    # Generate comprehensive report
    report = validator.generate_comprehensive_report(
        walkthrough_result, integration_results)

    # Display summary
    print(f"\n🏆 COMPREHENSIVE VALIDATION RESULTS")
    print("=" * 50)
    print(f"Overall Status: {report['overall_status']}")
    print(f"Overall Score: {report['overall_score']:.1%}")
    print()

    print(f"📚 Walkthrough Summary:")
    ws = report['walkthrough_summary']
    print(
        f"  Modules: {ws['completed_modules']}/{ws['total_modules']} ({ws['module_completion_rate']:.1%})")
    print(
        f"  Exercises: {ws['completed_exercises']}/{ws['total_exercises']} ({ws['exercise_completion_rate']:.1%})")
    print(f"  Score: {ws['overall_score']:.1%}")
    print()

    print(f"🔧 Integration Summary:")
    its = report['integration_summary']
    print(f"  Tests: {its['passed_tests']}/{its['total_tests']}")
    print(f"  Score: {its['average_score']:.1%}")
    print(f"  Errors: {its['total_errors']}")
    print(f"  Warnings: {its['total_warnings']}")
    print()

    if report['recommendations']:
        print(f"💡 Recommendations:")
        for rec in report['recommendations']:
            print(f"  • {rec}")

    # Save detailed report if requested
    if args.report_file:
        try:
            with open(args.report_file, 'w') as f:
                json.dump(report, f, indent=2)
            print(f"\n📄 Detailed report saved to: {args.report_file}")
        except Exception as e:
            print(f"\n❌ Failed to save report: {e}")

    # Exit with appropriate code
    sys.exit(0 if report['overall_status'] == 'PASSED' else 1)


if __name__ == "__main__":
    main()
