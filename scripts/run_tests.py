#!/usr/bin/env python3
"""
Test Runner for Airflow Coding Kata

This script runs all tests for the Airflow Coding Kata project,
including DAG validation, module structure, Docker environment,
and exercise validation tests.

Usage:
    python scripts/run_tests.py
    python scripts/run_tests.py --category dag-validation
    python scripts/run_tests.py --verbose
    python scripts/run_tests.py --coverage
"""

import os
import sys
import subprocess
import argparse
from pathlib import Path
from typing import List, Dict, Any
import time


class TestRunner:
    """Runs and manages tests for the Airflow Coding Kata."""

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.project_root = Path(__file__).parent.parent
        self.test_categories = {
            'dag-validation': {
                'description': 'DAG syntax and structure validation',
                'test_files': ['tests/test_dag_validation.py'],
                'dependencies': ['pytest', 'ast']
            },
            'docker-environment': {
                'description': 'Docker setup and environment validation',
                'test_files': ['tests/test_docker_environment.py'],
                'dependencies': ['pytest', 'yaml', 'subprocess']
            },
            'module-structure': {
                'description': 'Learning module structure and content',
                'test_files': ['tests/test_module_structure.py'],
                'dependencies': ['pytest', 'markdown']
            },
            'exercise-validation': {
                'description': 'Exercise completion validation',
                'test_files': ['tests/test_exercise_validation.py'],
                'dependencies': ['pytest']
            },
            'progress-tracking': {
                'description': 'Progress tracking system',
                'test_files': ['tests/test_progress_tracking.py'],
                'dependencies': ['pytest', 'json']
            }
        }

    def log(self, message: str, level: str = "INFO"):
        """Log messages based on verbosity setting."""
        if self.verbose or level in ["ERROR", "WARNING"]:
            timestamp = time.strftime("%H:%M:%S")
            print(f"[{timestamp}] [{level}] {message}")

    def check_dependencies(self) -> Dict[str, bool]:
        """Check if required dependencies are available."""
        self.log("Checking test dependencies...")

        dependencies_status = {}
        all_deps = set()

        # Collect all dependencies
        for category in self.test_categories.values():
            all_deps.update(category['dependencies'])

        # Check each dependency
        for dep in all_deps:
            try:
                __import__(dep)
                dependencies_status[dep] = True
                self.log(f"✓ {dep} available")
            except ImportError:
                dependencies_status[dep] = False
                self.log(f"✗ {dep} missing", "WARNING")

        return dependencies_status

    def run_pytest(self, test_files: List[str], extra_args: List[str] = None) -> Dict[str, Any]:
        """Run pytest on specified test files."""
        if extra_args is None:
            extra_args = []

        # Build pytest command
        cmd = ['python', '-m', 'pytest'] + test_files + extra_args

        if self.verbose:
            cmd.append('-v')

        self.log(f"Running: {' '.join(cmd)}")

        try:
            # Change to project root directory
            original_cwd = os.getcwd()
            os.chdir(self.project_root)

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )

            os.chdir(original_cwd)

            return {
                'success': result.returncode == 0,
                'returncode': result.returncode,
                'stdout': result.stdout,
                'stderr': result.stderr,
                'command': ' '.join(cmd)
            }

        except subprocess.TimeoutExpired:
            return {
                'success': False,
                'returncode': -1,
                'stdout': '',
                'stderr': 'Test execution timed out',
                'command': ' '.join(cmd)
            }
        except Exception as e:
            return {
                'success': False,
                'returncode': -1,
                'stdout': '',
                'stderr': str(e),
                'command': ' '.join(cmd)
            }

    def run_category_tests(self, category: str, extra_args: List[str] = None) -> Dict[str, Any]:
        """Run tests for a specific category."""
        if category not in self.test_categories:
            return {
                'success': False,
                'error': f"Unknown test category: {category}"
            }

        category_info = self.test_categories[category]
        self.log(f"Running {category} tests: {category_info['description']}")

        # Check if test files exist
        missing_files = []
        for test_file in category_info['test_files']:
            if not (self.project_root / test_file).exists():
                missing_files.append(test_file)

        if missing_files:
            return {
                'success': False,
                'error': f"Missing test files: {', '.join(missing_files)}"
            }

        # Run the tests
        return self.run_pytest(category_info['test_files'], extra_args)

    def run_all_tests(self, extra_args: List[str] = None) -> Dict[str, Dict[str, Any]]:
        """Run all test categories."""
        self.log("Running all test categories...")

        results = {}
        overall_success = True

        for category in self.test_categories:
            self.log(f"\n{'='*50}")
            self.log(f"CATEGORY: {category.upper()}")
            self.log(f"{'='*50}")

            result = self.run_category_tests(category, extra_args)
            results[category] = result

            if not result['success']:
                overall_success = False
                self.log(f"✗ {category} tests failed", "ERROR")
            else:
                self.log(f"✓ {category} tests passed")

        results['overall_success'] = overall_success
        return results

    def generate_test_report(self, results: Dict[str, Dict[str, Any]]) -> str:
        """Generate a formatted test report."""
        report_lines = []
        report_lines.append("🧪 AIRFLOW CODING KATA TEST REPORT")
        report_lines.append("=" * 50)
        report_lines.append("")

        overall_success = results.get('overall_success', False)
        status_icon = "✅" if overall_success else "❌"
        report_lines.append(
            f"Overall Status: {status_icon} {'PASSED' if overall_success else 'FAILED'}")
        report_lines.append("")

        # Category results
        report_lines.append("📊 CATEGORY RESULTS")
        report_lines.append("-" * 30)

        for category, result in results.items():
            if category == 'overall_success':
                continue

            category_info = self.test_categories.get(category, {})
            description = category_info.get('description', 'Unknown')

            if 'error' in result:
                report_lines.append(f"❌ {category}: ERROR")
                report_lines.append(f"   {result['error']}")
            elif result['success']:
                report_lines.append(f"✅ {category}: PASSED")
                report_lines.append(f"   {description}")
            else:
                report_lines.append(f"❌ {category}: FAILED")
                report_lines.append(f"   {description}")
                if result.get('stderr'):
                    # Show first few lines of error
                    error_lines = result['stderr'].split('\n')[:3]
                    for line in error_lines:
                        if line.strip():
                            report_lines.append(f"   {line.strip()}")

        report_lines.append("")

        # Recommendations
        failed_categories = [cat for cat, result in results.items()
                             if cat != 'overall_success' and not result.get('success', False)]

        if failed_categories:
            report_lines.append("🔧 RECOMMENDATIONS")
            report_lines.append("-" * 20)

            for category in failed_categories:
                if category == 'dag-validation':
                    report_lines.append("• Check DAG syntax and structure")
                    report_lines.append(
                        "• Run: python scripts/validate_dags.py")
                elif category == 'docker-environment':
                    report_lines.append(
                        "• Verify Docker is installed and running")
                    report_lines.append(
                        "• Run: python scripts/verify_setup.py")
                elif category == 'module-structure':
                    report_lines.append("• Check module directories and files")
                    report_lines.append(
                        "• Ensure all modules have required structure")
                elif category == 'exercise-validation':
                    report_lines.append("• Verify exercise validation logic")
                    report_lines.append("• Check exercise definitions")
                elif category == 'progress-tracking':
                    report_lines.append(
                        "• Check progress tracking functionality")
                    report_lines.append(
                        "• Verify JSON serialization/deserialization")
        else:
            report_lines.append(
                "🎉 All tests passed! The kata is ready for students.")

        return "\n".join(report_lines)

    def run_with_coverage(self, category: str = None) -> Dict[str, Any]:
        """Run tests with coverage reporting."""
        self.log("Running tests with coverage analysis...")

        try:
            # Check if coverage is available
            import coverage
        except ImportError:
            return {
                'success': False,
                'error': 'Coverage package not installed. Run: pip install coverage'
            }

        # Coverage arguments
        coverage_args = [
            '--cov=scripts',
            '--cov=tests',
            '--cov-report=term-missing',
            '--cov-report=html:htmlcov'
        ]

        if category:
            return self.run_category_tests(category, coverage_args)
        else:
            return self.run_all_tests(coverage_args)

    def list_categories(self):
        """List available test categories."""
        print("\n📋 AVAILABLE TEST CATEGORIES")
        print("=" * 40)

        for category, info in self.test_categories.items():
            print(f"\n🔹 {category}")
            print(f"   Description: {info['description']}")
            print(f"   Test files: {', '.join(info['test_files'])}")
            print(f"   Dependencies: {', '.join(info['dependencies'])}")


def main():
    """Main function for test runner."""
    parser = argparse.ArgumentParser(
        description='Run Airflow Coding Kata tests')
    parser.add_argument(
        '--category',
        type=str,
        help='Run tests for specific category'
    )
    parser.add_argument(
        '--list-categories',
        action='store_true',
        help='List available test categories'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose output'
    )
    parser.add_argument(
        '--coverage',
        action='store_true',
        help='Run tests with coverage analysis'
    )
    parser.add_argument(
        '--report-only',
        action='store_true',
        help='Generate report from last test run'
    )
    parser.add_argument(
        '--quick',
        action='store_true',
        help='Run quick tests only (skip slow integration tests)'
    )

    args = parser.parse_args()

    runner = TestRunner(verbose=args.verbose)

    if args.list_categories:
        runner.list_categories()
        return

    # Check dependencies
    deps_status = runner.check_dependencies()
    missing_deps = [dep for dep, available in deps_status.items()
                    if not available]

    if missing_deps:
        print(f"\n⚠️  Missing dependencies: {', '.join(missing_deps)}")
        print("Install with: pip install " + " ".join(missing_deps))
        if not args.verbose:
            print("Use --verbose to see detailed dependency check")

    # Prepare extra arguments
    extra_args = []
    if args.quick:
        extra_args.extend(['-m', 'not slow'])

    # Run tests
    if args.coverage:
        if args.category:
            results = {args.category: runner.run_with_coverage(args.category)}
            results['overall_success'] = results[args.category]['success']
        else:
            results = runner.run_with_coverage()
    elif args.category:
        result = runner.run_category_tests(args.category, extra_args)
        results = {args.category: result, 'overall_success': result['success']}
    else:
        results = runner.run_all_tests(extra_args)

    # Generate and display report
    report = runner.generate_test_report(results)
    print(f"\n{report}")

    # Exit with appropriate code
    sys.exit(0 if results.get('overall_success', False) else 1)


if __name__ == "__main__":
    main()
