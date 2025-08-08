#!/usr/bin/env python3
"""
Automated Feedback System

This script provides automated feedback for common mistakes in Airflow DAGs
and exercises, helping students learn from their errors.

Usage:
    python scripts/feedback_system.py --analyze-dag dags/my_dag.py
    python scripts/feedback_system.py --check-exercise 01-setup exercise-1
"""

import os
import sys
import ast
import re
import argparse
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
import importlib.util


@dataclass
class FeedbackItem:
    """Represents a feedback item for a student."""
    type: str  # 'error', 'warning', 'suggestion', 'tip'
    category: str  # 'syntax', 'structure', 'best_practice', 'performance'
    message: str
    explanation: str
    fix_suggestion: str
    line_number: Optional[int] = None
    severity: int = 1  # 1-5, where 5 is most severe


class FeedbackAnalyzer:
    """Analyzes code and provides automated feedback."""

    def __init__(self):
        self.common_patterns = self._load_common_patterns()
        self.best_practices = self._load_best_practices()

    def _load_common_patterns(self) -> Dict[str, Any]:
        """Load patterns for common mistakes."""
        return {
            'missing_imports': {
                'patterns': [
                    r'DAG\(',
                    r'BashOperator\(',
                    r'PythonOperator\(',
                    r'datetime\(',
                    r'timedelta\('
                ],
                'required_imports': {
                    r'DAG\(': 'from airflow import DAG',
                    r'BashOperator\(': 'from airflow.operators.bash import BashOperator',
                    r'PythonOperator\(': 'from airflow.operators.python import PythonOperator',
                    r'datetime\(': 'from datetime import datetime',
                    r'timedelta\(': 'from datetime import timedelta'
                }
            },
            'dag_issues': {
                'missing_dag_id': r'DAG\([^)]*\)',
                'missing_start_date': r'start_date',
                'missing_schedule': r'schedule_interval',
                'hardcoded_dates': r'datetime\(\d{4},\s*\d{1,2},\s*\d{1,2}\)'
            },
            'task_issues': {
                'missing_task_id': r'(BashOperator|PythonOperator|.*Operator)\([^)]*\)',
                'missing_dag_reference': r'dag\s*=',
                'duplicate_task_ids': r'task_id\s*=\s*[\'"]([^\'"]+)[\'"]'
            },
            'dependency_issues': {
                'no_dependencies': r'>>|set_downstream|set_upstream',
                'circular_dependencies': r'(\w+)\s*>>\s*.*>>\s*\1'
            },
            'python_issues': {
                'unused_imports': r'^import\s+(\w+)',
                'long_lines': r'.{120,}',  # Lines longer than 120 characters
                'missing_docstrings': r'^def\s+\w+\([^)]*\):',
                'bare_except': r'except\s*:'
            }
        }

    def _load_best_practices(self) -> Dict[str, Any]:
        """Load best practice recommendations."""
        return {
            'dag_naming': {
                'pattern': r"dag_id\s*=\s*['\"]([^'\"]+)['\"]",
                'rules': [
                    ('snake_case', r'^[a-z][a-z0-9_]*$',
                     'Use snake_case for DAG IDs'),
                    ('descriptive',
                     r'.{5,}', 'DAG IDs should be descriptive (5+ characters)'),
                    ('no_spaces', r'^[^\s]*$',
                     'DAG IDs should not contain spaces')
                ]
            },
            'task_naming': {
                'pattern': r"task_id\s*=\s*['\"]([^'\"]+)['\"]",
                'rules': [
                    ('snake_case', r'^[a-z][a-z0-9_]*$',
                     'Use snake_case for task IDs'),
                    ('descriptive',
                     r'.{3,}', 'Task IDs should be descriptive (3+ characters)'),
                    ('verb_noun', r'^(get|set|create|update|delete|process|send|receive)_',
                     'Consider using verb_noun pattern for task IDs')
                ]
            },
            'code_organization': {
                'imports_at_top': r'^(from|import)\s+',
                'functions_before_dag': r'^def\s+\w+',
                'dag_at_end': r'dag\s*=\s*DAG\('
            },
            'documentation': {
                'dag_docstring': r'""".*?"""',
                'function_docstring': r'def\s+\w+[^:]*:\s*"""',
                'inline_comments': r'#\s+\w+'
            }
        }

    def analyze_file(self, file_path: str) -> List[FeedbackItem]:
        """Analyze a Python file and return feedback."""
        feedback_items = []

        if not os.path.exists(file_path):
            feedback_items.append(FeedbackItem(
                type='error',
                category='file',
                message=f"File not found: {file_path}",
                explanation="The specified file does not exist.",
                fix_suggestion="Check the file path and ensure the file exists."
            ))
            return feedback_items

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                lines = content.split('\n')
        except Exception as e:
            feedback_items.append(FeedbackItem(
                type='error',
                category='file',
                message=f"Cannot read file: {e}",
                explanation="There was an error reading the file.",
                fix_suggestion="Check file permissions and encoding."
            ))
            return feedback_items

        # Check syntax first
        syntax_feedback = self._check_syntax(content, file_path)
        feedback_items.extend(syntax_feedback)

        # If syntax is valid, do deeper analysis
        if not any(item.type == 'error' and item.category == 'syntax' for item in syntax_feedback):
            feedback_items.extend(self._check_imports(content, lines))
            feedback_items.extend(self._check_dag_structure(content, lines))
            feedback_items.extend(self._check_task_structure(content, lines))
            feedback_items.extend(self._check_dependencies(content, lines))
            feedback_items.extend(self._check_best_practices(content, lines))

        return feedback_items

    def _check_syntax(self, content: str, file_path: str) -> List[FeedbackItem]:
        """Check Python syntax."""
        feedback_items = []

        try:
            ast.parse(content)
        except SyntaxError as e:
            feedback_items.append(FeedbackItem(
                type='error',
                category='syntax',
                message=f"Syntax error: {e.msg}",
                explanation=f"Python syntax error on line {e.lineno}.",
                fix_suggestion="Check for missing parentheses, quotes, or proper indentation.",
                line_number=e.lineno,
                severity=5
            ))
        except Exception as e:
            feedback_items.append(FeedbackItem(
                type='error',
                category='syntax',
                message=f"Parse error: {str(e)}",
                explanation="Could not parse the Python file.",
                fix_suggestion="Check the file for syntax errors.",
                severity=5
            ))

        return feedback_items

    def _check_imports(self, content: str, lines: List[str]) -> List[FeedbackItem]:
        """Check import statements."""
        feedback_items = []
        patterns = self.common_patterns['missing_imports']

        # Check for missing imports
        for pattern, required_import in patterns['required_imports'].items():
            if re.search(pattern, content) and required_import not in content:
                feedback_items.append(FeedbackItem(
                    type='error',
                    category='imports',
                    message=f"Missing import: {required_import}",
                    explanation=f"You're using functionality that requires this import.",
                    fix_suggestion=f"Add '{required_import}' at the top of your file.",
                    severity=4
                ))

        # Check import organization
        import_lines = []
        for i, line in enumerate(lines):
            if re.match(r'^(from|import)\s+', line.strip()):
                import_lines.append((i + 1, line.strip()))

        if import_lines:
            # Check if imports are at the top
            first_import_line = import_lines[0][0]
            non_empty_lines_before = [
                i + 1 for i, line in enumerate(lines[:first_import_line - 1])
                if line.strip() and not line.strip().startswith('#') and not line.strip().startswith('"""')
            ]

            if non_empty_lines_before:
                feedback_items.append(FeedbackItem(
                    type='suggestion',
                    category='structure',
                    message="Imports should be at the top of the file",
                    explanation="Python convention is to place all imports at the beginning.",
                    fix_suggestion="Move import statements to the top of the file, after docstrings.",
                    line_number=first_import_line,
                    severity=2
                ))

        return feedback_items

    def _check_dag_structure(self, content: str, lines: List[str]) -> List[FeedbackItem]:
        """Check DAG structure and configuration."""
        feedback_items = []

        # Check if DAG is defined
        if 'DAG(' not in content and 'dag =' not in content.lower():
            feedback_items.append(FeedbackItem(
                type='error',
                category='structure',
                message="No DAG definition found",
                explanation="Every Airflow file should define a DAG.",
                fix_suggestion="Create a DAG instance: dag = DAG('your_dag_id', ...)",
                severity=5
            ))
            return feedback_items

        # Check DAG configuration
        dag_patterns = self.common_patterns['dag_issues']

        # Check for DAG ID
        dag_match = re.search(r'DAG\s*\(\s*[\'"]([^\'"]+)[\'"]', content)
        if not dag_match:
            feedback_items.append(FeedbackItem(
                type='error',
                category='structure',
                message="DAG ID not found or not properly formatted",
                explanation="DAG constructor should have a string ID as first parameter.",
                fix_suggestion="Use: DAG('your_dag_id', ...)",
                severity=4
            ))

        # Check for start_date
        if 'start_date' not in content:
            feedback_items.append(FeedbackItem(
                type='error',
                category='structure',
                message="Missing start_date parameter",
                explanation="DAGs must have a start_date to determine when to begin execution.",
                fix_suggestion="Add start_date=datetime(2024, 1, 1) to your DAG or default_args.",
                severity=4
            ))

        # Check for schedule_interval
        if 'schedule_interval' not in content and 'schedule=' not in content:
            feedback_items.append(FeedbackItem(
                type='warning',
                category='structure',
                message="No schedule_interval specified",
                explanation="Without a schedule, your DAG won't run automatically.",
                fix_suggestion="Add schedule_interval=timedelta(days=1) or a cron expression.",
                severity=3
            ))

        return feedback_items

    def _check_task_structure(self, content: str, lines: List[str]) -> List[FeedbackItem]:
        """Check task definitions."""
        feedback_items = []

        # Find all operator instantiations
        operator_pattern = r'(\w+)\s*=\s*(\w*Operator)\s*\('
        operators = re.findall(operator_pattern, content)

        if not operators:
            feedback_items.append(FeedbackItem(
                type='warning',
                category='structure',
                message="No tasks found",
                explanation="DAGs should contain at least one task.",
                fix_suggestion="Add tasks using operators like BashOperator or PythonOperator.",
                severity=3
            ))
            return feedback_items

        # Check each operator
        task_ids = []
        for var_name, operator_type in operators:
            # Find the full operator definition
            operator_def_pattern = rf'{re.escape(var_name)}\s*=\s*{re.escape(operator_type)}\s*\([^)]*\)'
            operator_match = re.search(
                operator_def_pattern, content, re.DOTALL)

            if operator_match:
                operator_def = operator_match.group(0)

                # Check for task_id
                task_id_match = re.search(
                    r"task_id\s*=\s*['\"]([^'\"]+)['\"]", operator_def)
                if not task_id_match:
                    feedback_items.append(FeedbackItem(
                        type='error',
                        category='structure',
                        message=f"Missing task_id in {var_name}",
                        explanation="Every task must have a unique task_id.",
                        fix_suggestion=f"Add task_id='{var_name}' to the {operator_type}.",
                        severity=4
                    ))
                else:
                    task_id = task_id_match.group(1)
                    task_ids.append(task_id)

                # Check for DAG reference
                if 'dag=' not in operator_def:
                    feedback_items.append(FeedbackItem(
                        type='error',
                        category='structure',
                        message=f"Missing dag parameter in {var_name}",
                        explanation="Tasks must be associated with a DAG.",
                        fix_suggestion=f"Add dag=dag to the {operator_type}.",
                        severity=4
                    ))

        # Check for duplicate task IDs
        seen_task_ids = set()
        for task_id in task_ids:
            if task_id in seen_task_ids:
                feedback_items.append(FeedbackItem(
                    type='error',
                    category='structure',
                    message=f"Duplicate task_id: {task_id}",
                    explanation="Task IDs must be unique within a DAG.",
                    fix_suggestion=f"Change one of the tasks with task_id='{task_id}' to use a different ID.",
                    severity=4
                ))
            seen_task_ids.add(task_id)

        return feedback_items

    def _check_dependencies(self, content: str, lines: List[str]) -> List[FeedbackItem]:
        """Check task dependencies."""
        feedback_items = []

        # Check if dependencies are defined
        dependency_patterns = [r'>>', r'set_downstream', r'set_upstream']
        has_dependencies = any(re.search(pattern, content)
                               for pattern in dependency_patterns)

        # Count operators
        operator_count = len(re.findall(r'\w*Operator\s*\(', content))

        if operator_count > 1 and not has_dependencies:
            feedback_items.append(FeedbackItem(
                type='warning',
                category='structure',
                message="No task dependencies defined",
                explanation="Multiple tasks should have dependencies to control execution order.",
                fix_suggestion="Use >> operator or set_downstream() to define task order.",
                severity=3
            ))

        return feedback_items

    def _check_best_practices(self, content: str, lines: List[str]) -> List[FeedbackItem]:
        """Check best practices."""
        feedback_items = []
        best_practices = self.best_practices

        # Check DAG naming
        dag_id_match = re.search(
            best_practices['dag_naming']['pattern'], content)
        if dag_id_match:
            dag_id = dag_id_match.group(1)
            for rule_name, pattern, message in best_practices['dag_naming']['rules']:
                if not re.match(pattern, dag_id):
                    feedback_items.append(FeedbackItem(
                        type='suggestion',
                        category='best_practice',
                        message=f"DAG ID naming: {message}",
                        explanation=f"DAG ID '{dag_id}' doesn't follow the {rule_name} convention.",
                        fix_suggestion=f"Consider renaming to follow {rule_name} convention.",
                        severity=1
                    ))

        # Check task naming
        task_id_matches = re.findall(
            best_practices['task_naming']['pattern'], content)
        for task_id in task_id_matches:
            for rule_name, pattern, message in best_practices['task_naming']['rules']:
                if rule_name != 'verb_noun' and not re.match(pattern, task_id):
                    feedback_items.append(FeedbackItem(
                        type='suggestion',
                        category='best_practice',
                        message=f"Task ID naming: {message}",
                        explanation=f"Task ID '{task_id}' doesn't follow the {rule_name} convention.",
                        fix_suggestion=f"Consider renaming to follow {rule_name} convention.",
                        severity=1
                    ))

        # Check for documentation
        if '"""' not in content:
            feedback_items.append(FeedbackItem(
                type='suggestion',
                category='best_practice',
                message="Consider adding docstrings",
                explanation="Docstrings help document your DAG's purpose and functionality.",
                fix_suggestion="Add a docstring at the top of your file describing the DAG.",
                severity=1
            ))

        # Check line length
        for i, line in enumerate(lines):
            if len(line) > 120:
                feedback_items.append(FeedbackItem(
                    type='suggestion',
                    category='best_practice',
                    message="Long line detected",
                    explanation="Lines longer than 120 characters can be hard to read.",
                    fix_suggestion="Consider breaking long lines into multiple lines.",
                    line_number=i + 1,
                    severity=1
                ))

        return feedback_items

    def generate_feedback_report(self, feedback_items: List[FeedbackItem], file_path: str) -> str:
        """Generate a formatted feedback report."""
        if not feedback_items:
            return f"✅ Great job! No issues found in {file_path}"

        report_lines = []
        report_lines.append(f"📝 FEEDBACK REPORT FOR {file_path}")
        report_lines.append("=" * 60)
        report_lines.append("")

        # Group by type
        errors = [item for item in feedback_items if item.type == 'error']
        warnings = [item for item in feedback_items if item.type == 'warning']
        suggestions = [
            item for item in feedback_items if item.type == 'suggestion']

        # Summary
        report_lines.append("📊 SUMMARY")
        report_lines.append("-" * 20)
        report_lines.append(f"❌ Errors: {len(errors)}")
        report_lines.append(f"⚠️  Warnings: {len(warnings)}")
        report_lines.append(f"💡 Suggestions: {len(suggestions)}")
        report_lines.append("")

        # Detailed feedback
        if errors:
            report_lines.append("❌ ERRORS (Must Fix)")
            report_lines.append("-" * 30)
            for item in sorted(errors, key=lambda x: x.severity, reverse=True):
                report_lines.append(f"• {item.message}")
                if item.line_number:
                    report_lines.append(f"  Line {item.line_number}")
                report_lines.append(f"  {item.explanation}")
                report_lines.append(f"  💡 Fix: {item.fix_suggestion}")
                report_lines.append("")

        if warnings:
            report_lines.append("⚠️  WARNINGS (Should Fix)")
            report_lines.append("-" * 30)
            for item in warnings:
                report_lines.append(f"• {item.message}")
                if item.line_number:
                    report_lines.append(f"  Line {item.line_number}")
                report_lines.append(f"  {item.explanation}")
                report_lines.append(f"  💡 Fix: {item.fix_suggestion}")
                report_lines.append("")

        if suggestions:
            report_lines.append("💡 SUGGESTIONS (Nice to Have)")
            report_lines.append("-" * 30)
            for item in suggestions:
                report_lines.append(f"• {item.message}")
                if item.line_number:
                    report_lines.append(f"  Line {item.line_number}")
                report_lines.append(f"  {item.explanation}")
                report_lines.append(f"  💡 Tip: {item.fix_suggestion}")
                report_lines.append("")

        # Overall assessment
        if errors:
            report_lines.append("🎯 NEXT STEPS")
            report_lines.append("-" * 15)
            report_lines.append(
                "Focus on fixing the errors first, then address warnings.")
            report_lines.append(
                "Suggestions can help improve code quality but aren't required.")
        elif warnings:
            report_lines.append("🎯 NEXT STEPS")
            report_lines.append("-" * 15)
            report_lines.append(
                "Good progress! Address the warnings to improve your DAG.")
            report_lines.append(
                "Consider the suggestions for even better code quality.")
        else:
            report_lines.append("🎉 EXCELLENT WORK!")
            report_lines.append("-" * 20)
            report_lines.append(
                "Your DAG looks great! Consider the suggestions for polish.")

        return "\n".join(report_lines)


def main():
    """Main function for feedback system."""
    parser = argparse.ArgumentParser(
        description='Provide automated feedback for Airflow DAGs')
    parser.add_argument(
        '--analyze-dag',
        type=str,
        help='Analyze a specific DAG file'
    )
    parser.add_argument(
        '--analyze-directory',
        type=str,
        default='dags',
        help='Analyze all DAG files in directory (default: dags)'
    )
    parser.add_argument(
        '--output-format',
        choices=['text', 'json'],
        default='text',
        help='Output format (default: text)'
    )
    parser.add_argument(
        '--severity-threshold',
        type=int,
        choices=[1, 2, 3, 4, 5],
        default=1,
        help='Minimum severity level to report (1-5, default: 1)'
    )

    args = parser.parse_args()

    analyzer = FeedbackAnalyzer()

    if args.analyze_dag:
        # Analyze single file
        feedback_items = analyzer.analyze_file(args.analyze_dag)

        # Filter by severity
        filtered_items = [
            item for item in feedback_items
            if item.severity >= args.severity_threshold
        ]

        if args.output_format == 'json':
            import json
            output = {
                'file': args.analyze_dag,
                'feedback_items': [
                    {
                        'type': item.type,
                        'category': item.category,
                        'message': item.message,
                        'explanation': item.explanation,
                        'fix_suggestion': item.fix_suggestion,
                        'line_number': item.line_number,
                        'severity': item.severity
                    }
                    for item in filtered_items
                ]
            }
            print(json.dumps(output, indent=2))
        else:
            report = analyzer.generate_feedback_report(
                filtered_items, args.analyze_dag)
            print(report)

    else:
        # Analyze directory
        if not os.path.exists(args.analyze_directory):
            print(f"Error: Directory {args.analyze_directory} does not exist")
            sys.exit(1)

        dag_files = [
            os.path.join(args.analyze_directory, f)
            for f in os.listdir(args.analyze_directory)
            if f.endswith('.py') and not f.startswith('__')
        ]

        if not dag_files:
            print(f"No Python files found in {args.analyze_directory}")
            sys.exit(0)

        print(
            f"🔍 Analyzing {len(dag_files)} files in {args.analyze_directory}")
        print("=" * 60)

        total_issues = 0
        for dag_file in dag_files:
            feedback_items = analyzer.analyze_file(dag_file)

            # Filter by severity
            filtered_items = [
                item for item in feedback_items
                if item.severity >= args.severity_threshold
            ]

            if filtered_items:
                total_issues += len(filtered_items)
                print(
                    f"\n{analyzer.generate_feedback_report(filtered_items, dag_file)}")
            else:
                print(f"\n✅ {dag_file}: No issues found")

        print(
            f"\n📊 SUMMARY: Found {total_issues} issues across {len(dag_files)} files")


if __name__ == "__main__":
    main()
