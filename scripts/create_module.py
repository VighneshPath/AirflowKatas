#!/usr/bin/env python3
"""
Module Creation Script

This script helps contributors create new modules for the Airflow Coding Kata
by generating the proper directory structure and template files.

Usage:
    python scripts/create_module.py --name "advanced-sensors" --number 11
    python scripts/create_module.py --name "custom-operators" --number 12 --description "Building Custom Operators"
"""

import os
import sys
import argparse
import shutil
from pathlib import Path
from datetime import datetime


def create_module_structure(module_number: int, module_name: str, description: str = None):
    """
    Create a new module with the standard directory structure.

    Args:
        module_number: The module number (e.g., 11, 12)
        module_name: The module name in kebab-case (e.g., "advanced-sensors")
        description: Optional description of the module
    """
    # Format module directory name
    module_dir_name = f"{module_number:02d}-{module_name}"
    module_path = Path("modules") / module_dir_name

    # Check if module already exists
    if module_path.exists():
        print(f"❌ Module {module_dir_name} already exists!")
        return False

    # Create module directory structure
    print(f"📁 Creating module structure: {module_dir_name}")

    # Create main directories
    directories = [
        module_path,
        module_path / "examples",
        module_path / "exercises",
        module_path / "solutions"
    ]

    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
        print(f"   Created: {directory}")

    # Copy template files
    template_path = Path("templates/module-template")

    if not template_path.exists():
        print(f"❌ Template directory not found: {template_path}")
        return False

    # Copy and customize template files
    template_files = [
        "README.md",
        "concepts.md",
        "resources.md"
    ]

    for template_file in template_files:
        source_file = template_path / template_file
        dest_file = module_path / template_file

        if source_file.exists():
            # Read template content
            with open(source_file, 'r') as f:
                content = f.read()

            # Customize content
            content = customize_template_content(
                content, module_number, module_name, description
            )

            # Write customized content
            with open(dest_file, 'w') as f:
                f.write(content)

            print(f"   Created: {dest_file}")
        else:
            print(f"   ⚠️  Template file not found: {source_file}")

    # Create example files
    create_example_files(module_path, module_name)

    # Create exercise placeholders
    create_exercise_placeholders(module_path, module_name)

    print(f"✅ Module {module_dir_name} created successfully!")
    print(f"\n📝 Next steps:")
    print(f"   1. Edit {module_path}/README.md to define learning objectives")
    print(f"   2. Write concepts in {module_path}/concepts.md")
    print(f"   3. Add relevant resources to {module_path}/resources.md")
    print(f"   4. Create example DAGs in {module_path}/examples/")
    print(f"   5. Design exercises in {module_path}/exercises/")
    print(f"   6. Implement solutions in {module_path}/solutions/")

    return True


def customize_template_content(content: str, module_number: int, module_name: str, description: str = None) -> str:
    """
    Customize template content with module-specific information.

    Args:
        content: Template content to customize
        module_number: Module number
        module_name: Module name
        description: Optional module description

    Returns:
        Customized content
    """
    # Convert kebab-case to title case
    module_title = module_name.replace('-', ' ').title()

    # Use description if provided, otherwise use title
    module_description = description or module_title

    # Replace placeholders
    replacements = {
        '[Module Name]': module_description,
        '[module-name]': module_name,
        'Module XX': f'Module {module_number:02d}',
        'XX-': f'{module_number:02d}-'
    }

    for placeholder, replacement in replacements.items():
        content = content.replace(placeholder, replacement)

    return content


def create_example_files(module_path: Path, module_name: str):
    """
    Create example DAG files for the module.

    Args:
        module_path: Path to the module directory
        module_name: Module name
    """
    examples_dir = module_path / "examples"

    # Create basic example
    basic_example = examples_dir / \
        f"basic_{module_name.replace('-', '_')}_example.py"
    with open(basic_example, 'w') as f:
        f.write(f'''"""
Basic {module_name.replace('-', ' ').title()} Example

This example demonstrates the fundamental concepts of {module_name.replace('-', ' ')}.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# DAG Configuration
default_args = {{
    'owner': 'kata-example',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}}

dag = DAG(
    'basic_{module_name.replace('-', '_')}_example',
    default_args=default_args,
    description='Basic example demonstrating {module_name.replace('-', ' ')} concepts',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['kata', 'example', '{module_name}'],
)

def example_function():
    """Example function demonstrating basic concepts."""
    print("This is a basic example of {module_name.replace('-', ' ')}")
    return "example_complete"

# Example task
example_task = PythonOperator(
    task_id='example_task',
    python_callable=example_function,
    dag=dag,
)
''')

    print(f"   Created: {basic_example}")

    # Create advanced example placeholder
    advanced_example = examples_dir / \
        f"advanced_{module_name.replace('-', '_')}_example.py"
    with open(advanced_example, 'w') as f:
        f.write(f'''"""
Advanced {module_name.replace('-', ' ').title()} Example

This example demonstrates advanced concepts and patterns for {module_name.replace('-', ' ')}.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# TODO: Implement advanced example
# This file is a placeholder for advanced concepts

default_args = {{
    'owner': 'kata-example',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}}

dag = DAG(
    'advanced_{module_name.replace('-', '_')}_example',
    default_args=default_args,
    description='Advanced example for {module_name.replace('-', ' ')}',
    schedule_interval=None,  # Manual trigger
    catchup=False,
    tags=['kata', 'example', 'advanced', '{module_name}'],
)

# TODO: Add advanced tasks and patterns
''')

    print(f"   Created: {advanced_example}")


def create_exercise_placeholders(module_path: Path, module_name: str):
    """
    Create exercise placeholder files.

    Args:
        module_path: Path to the module directory
        module_name: Module name
    """
    exercises_dir = module_path / "exercises"
    solutions_dir = module_path / "solutions"

    # Create exercise placeholders
    for i in range(1, 4):  # Create 3 exercise placeholders
        exercise_file = exercises_dir / \
            f"exercise-{i}-{module_name.replace('-', '_')}.md"
        solution_file = solutions_dir / f"exercise-{i}-solution.py"

        # Create exercise file
        with open(exercise_file, 'w') as f:
            f.write(f'''# Exercise {i}: {module_name.replace('-', ' ').title()} Practice

## Objective

[Define clear learning objectives for this exercise]

## Prerequisites

- [ ] Completed previous exercises in this module
- [ ] Understanding of basic {module_name.replace('-', ' ')} concepts

## Scenario

[Provide a realistic scenario that requires using {module_name.replace('-', ' ')} concepts]

## Requirements

1. **Requirement 1**: [Specific, testable requirement]
2. **Requirement 2**: [Another requirement]
3. **Requirement 3**: [Additional requirement]

## Instructions

### Step 1: Setup
[Provide setup instructions]

### Step 2: Implementation
[Provide implementation guidance]

### Step 3: Testing
[Provide testing instructions]

## Validation

- [ ] DAG parses without errors
- [ ] All requirements are met
- [ ] Code follows best practices

## Extension Challenges

[Optional additional challenges for advanced students]

---

**Need help?** Review the concepts and examples in this module.
''')

        # Create solution file
        with open(solution_file, 'w') as f:
            f.write(f'''"""
Exercise {i} Solution: {module_name.replace('-', ' ').title()}

This solution demonstrates the implementation for exercise {i}.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# TODO: Implement solution for exercise {i}

default_args = {{
    'owner': 'kata-solution',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}}

dag = DAG(
    'exercise_{i}_{module_name.replace('-', '_')}_solution',
    default_args=default_args,
    description='Solution for exercise {i}',
    schedule_interval=None,
    catchup=False,
    tags=['kata', 'solution', 'exercise-{i}'],
)

# TODO: Implement solution tasks
''')

        print(f"   Created: {exercise_file}")
        print(f"   Created: {solution_file}")


def validate_module_name(name: str) -> bool:
    """
    Validate that the module name follows conventions.

    Args:
        name: Module name to validate

    Returns:
        True if valid, False otherwise
    """
    if not name:
        print("❌ Module name cannot be empty")
        return False

    if not name.replace('-', '').replace('_', '').isalnum():
        print("❌ Module name can only contain letters, numbers, hyphens, and underscores")
        return False

    if name.startswith('-') or name.endswith('-'):
        print("❌ Module name cannot start or end with a hyphen")
        return False

    if '--' in name:
        print("❌ Module name cannot contain consecutive hyphens")
        return False

    return True


def check_prerequisites():
    """
    Check that all prerequisites are met for creating a module.

    Returns:
        True if all prerequisites are met, False otherwise
    """
    # Check if we're in the right directory
    if not Path("modules").exists():
        print("❌ modules/ directory not found. Are you in the kata root directory?")
        return False

    # Check if templates exist
    if not Path("templates/module-template").exists():
        print("❌ Module template not found. Please ensure templates/ directory exists.")
        return False

    return True


def main():
    """Main function for module creation."""
    parser = argparse.ArgumentParser(
        description='Create a new Airflow Kata module')
    parser.add_argument(
        '--name',
        type=str,
        required=True,
        help='Module name in kebab-case (e.g., "advanced-sensors")'
    )
    parser.add_argument(
        '--number',
        type=int,
        required=True,
        help='Module number (e.g., 11, 12)'
    )
    parser.add_argument(
        '--description',
        type=str,
        help='Optional description of the module'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force creation even if module exists (will overwrite)'
    )

    args = parser.parse_args()

    # Validate inputs
    if not validate_module_name(args.name):
        sys.exit(1)

    if args.number < 1:
        print("❌ Module number must be positive")
        sys.exit(1)

    # Check prerequisites
    if not check_prerequisites():
        sys.exit(1)

    # Check if module already exists
    module_dir_name = f"{args.number:02d}-{args.name}"
    module_path = Path("modules") / module_dir_name

    if module_path.exists() and not args.force:
        print(f"❌ Module {module_dir_name} already exists!")
        print("   Use --force to overwrite, or choose a different name/number")
        sys.exit(1)

    if args.force and module_path.exists():
        print(f"⚠️  Removing existing module: {module_dir_name}")
        shutil.rmtree(module_path)

    # Create the module
    print(f"🚀 Creating new module: {module_dir_name}")
    if args.description:
        print(f"   Description: {args.description}")

    success = create_module_structure(args.number, args.name, args.description)

    if success:
        print(f"\n🎉 Module creation completed!")
        print(f"\n📋 Contribution checklist:")
        print(
            f"   - [ ] Update module README.md with specific learning objectives")
        print(f"   - [ ] Write comprehensive concepts.md")
        print(f"   - [ ] Add relevant resources to resources.md")
        print(f"   - [ ] Create working example DAGs")
        print(f"   - [ ] Design progressive exercises")
        print(f"   - [ ] Implement and test all solutions")
        print(f"   - [ ] Add tests for the new module")
        print(f"   - [ ] Update main README.md to include the new module")
        print(f"   - [ ] Run validation scripts to ensure everything works")

        print(f"\n🔧 Validation commands:")
        print(
            f"   python scripts/validate_dags.py --dag-dir modules/{module_dir_name}")
        print(
            f"   python scripts/validate_exercises.py --module {module_dir_name}")
        print(f"   python scripts/end_to_end_validation.py --integration-tests")

    else:
        print(f"❌ Module creation failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
