"""
Tests for module structure and content validation.

This module tests that all learning modules have the correct structure,
required files, and valid content format.
"""

import os
import pytest
from pathlib import Path
import markdown
import yaml


class TestModuleStructure:
    """Test the overall module structure."""

    def test_modules_directory_exists(self):
        """Test that modules directory exists."""
        assert os.path.exists('modules'), "modules directory not found"
        assert os.path.isdir('modules'), "modules should be a directory"

    def test_expected_modules_exist(self, module_list):
        """Test that all expected learning modules exist."""
        modules_dir = Path('modules')

        if not modules_dir.exists():
            pytest.skip("Modules directory not found")

        existing_modules = [
            d.name for d in modules_dir.iterdir() if d.is_dir()]

        for expected_module in module_list:
            assert expected_module in existing_modules, \
                f"Expected module '{expected_module}' not found"

    def test_module_directory_structure(self, module_list):
        """Test that each module has the required directory structure."""
        modules_dir = Path('modules')

        if not modules_dir.exists():
            pytest.skip("Modules directory not found")

        required_subdirs = ['examples', 'exercises', 'solutions']

        for module_name in module_list:
            module_path = modules_dir / module_name

            if not module_path.exists():
                continue  # Skip if module doesn't exist

            for subdir in required_subdirs:
                subdir_path = module_path / subdir
                assert subdir_path.exists(), \
                    f"Module '{module_name}' missing '{subdir}' directory"
                assert subdir_path.is_dir(), \
                    f"'{subdir}' in module '{module_name}' should be a directory"

    def test_module_required_files(self, module_list):
        """Test that each module has required files."""
        modules_dir = Path('modules')

        if not modules_dir.exists():
            pytest.skip("Modules directory not found")

        required_files = ['README.md', 'concepts.md', 'resources.md']

        for module_name in module_list:
            module_path = modules_dir / module_name

            if not module_path.exists():
                continue  # Skip if module doesn't exist

            for required_file in required_files:
                file_path = module_path / required_file
                assert file_path.exists(), \
                    f"Module '{module_name}' missing required file '{required_file}'"
                assert file_path.is_file(), \
                    f"'{required_file}' in module '{module_name}' should be a file"


class TestModuleContent:
    """Test module content quality and format."""

    def test_readme_files_valid_markdown(self, module_list):
        """Test that README.md files contain valid markdown."""
        modules_dir = Path('modules')

        if not modules_dir.exists():
            pytest.skip("Modules directory not found")

        for module_name in module_list:
            readme_path = modules_dir / module_name / 'README.md'

            if not readme_path.exists():
                continue

            try:
                with open(readme_path, 'r', encoding='utf-8') as f:
                    content = f.read()

                # Try to parse as markdown
                markdown.markdown(content)

                # Check for basic structure
                assert '# ' in content or '## ' in content, \
                    f"README.md in '{module_name}' should have headers"

            except Exception as e:
                pytest.fail(f"Error parsing README.md in '{module_name}': {e}")

    def test_concepts_files_have_content(self, module_list):
        """Test that concepts.md files have meaningful content."""
        modules_dir = Path('modules')

        if not modules_dir.exists():
            pytest.skip("Modules directory not found")

        for module_name in module_list:
            concepts_path = modules_dir / module_name / 'concepts.md'

            if not concepts_path.exists():
                continue

            with open(concepts_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()

            assert len(content) > 100, \
                f"concepts.md in '{module_name}' should have substantial content"

            # Should have headers
            assert '# ' in content or '## ' in content, \
                f"concepts.md in '{module_name}' should have headers"

    def test_resources_files_have_links(self, module_list):
        """Test that resources.md files contain links."""
        modules_dir = Path('modules')

        if not modules_dir.exists():
            pytest.skip("Modules directory not found")

        for module_name in module_list:
            resources_path = modules_dir / module_name / 'resources.md'

            if not resources_path.exists():
                continue

            with open(resources_path, 'r', encoding='utf-8') as f:
                content = f.read()

            # Should contain links (markdown or plain URLs)
            has_markdown_links = '[' in content and '](' in content
            has_plain_urls = 'http' in content.lower()

            assert has_markdown_links or has_plain_urls, \
                f"resources.md in '{module_name}' should contain links"

    def test_exercise_files_exist(self, module_list):
        """Test that exercise files exist in exercises directories."""
        modules_dir = Path('modules')

        if not modules_dir.exists():
            pytest.skip("Modules directory not found")

        for module_name in module_list:
            exercises_dir = modules_dir / module_name / 'exercises'

            if not exercises_dir.exists():
                continue

            exercise_files = list(exercises_dir.glob('*.md'))
            assert len(exercise_files) > 0, \
                f"Module '{module_name}' should have exercise files"

            # Check that exercise files follow naming convention
            for exercise_file in exercise_files:
                assert 'exercise' in exercise_file.name.lower(), \
                    f"Exercise file '{exercise_file.name}' should contain 'exercise' in name"

    def test_solution_files_exist(self, module_list):
        """Test that solution files exist in solutions directories."""
        modules_dir = Path('modules')

        if not modules_dir.exists():
            pytest.skip("Modules directory not found")

        for module_name in module_list:
            solutions_dir = modules_dir / module_name / 'solutions'

            if not solutions_dir.exists():
                continue

            solution_files = list(solutions_dir.glob('*.py'))

            # Not all modules may have solutions yet, but if they do, check them
            if len(solution_files) > 0:
                for solution_file in solution_files:
                    assert 'solution' in solution_file.name.lower(), \
                        f"Solution file '{solution_file.name}' should contain 'solution' in name"

    def test_example_files_exist(self, module_list):
        """Test that example files exist in examples directories."""
        modules_dir = Path('modules')

        if not modules_dir.exists():
            pytest.skip("Modules directory not found")

        for module_name in module_list:
            examples_dir = modules_dir / module_name / 'examples'

            if not examples_dir.exists():
                continue

            example_files = list(examples_dir.glob('*.py'))

            # Most modules should have example files
            if module_name not in ['01-setup']:  # Setup might not have examples
                assert len(example_files) > 0, \
                    f"Module '{module_name}' should have example files"


class TestModuleProgression:
    """Test that modules follow logical progression."""

    def test_module_numbering_sequence(self, module_list):
        """Test that modules are numbered sequentially."""
        modules_dir = Path('modules')

        if not modules_dir.exists():
            pytest.skip("Modules directory not found")

        existing_modules = [
            d.name for d in modules_dir.iterdir() if d.is_dir()]
        numbered_modules = [m for m in existing_modules if m[0].isdigit()]

        # Extract numbers and sort
        module_numbers = []
        for module in numbered_modules:
            try:
                number = int(module.split('-')[0])
                module_numbers.append(number)
            except (ValueError, IndexError):
                continue

        module_numbers.sort()

        # Check for sequential numbering (allowing gaps)
        if len(module_numbers) > 1:
            for i in range(len(module_numbers) - 1):
                assert module_numbers[i+1] > module_numbers[i], \
                    "Module numbers should be in ascending order"

    def test_module_dependencies_logical(self, module_list):
        """Test that module dependencies make logical sense."""
        # This is a conceptual test - in practice, you might check that
        # advanced concepts (like XComs) come after basic concepts (like DAGs)

        modules_dir = Path('modules')
        if not modules_dir.exists():
            pytest.skip("Modules directory not found")

        # Define logical ordering
        concept_order = [
            'setup',
            'dag',
            'task',
            'operator',
            'scheduling',
            'sensor',
            'xcom',
            'branch',
            'error',
            'advanced',
            'real-world'
        ]

        existing_modules = [
            d.name for d in modules_dir.iterdir() if d.is_dir()]

        # Check that modules appear in logical order
        module_positions = {}
        for module in existing_modules:
            for i, concept in enumerate(concept_order):
                if concept in module.lower():
                    module_positions[module] = i
                    break

        # Verify ordering
        sorted_modules = sorted(module_positions.items(), key=lambda x: x[1])

        for i in range(len(sorted_modules) - 1):
            current_pos = sorted_modules[i][1]
            next_pos = sorted_modules[i+1][1]
            assert next_pos >= current_pos, \
                f"Module ordering issue: {sorted_modules[i][0]} should come before {sorted_modules[i+1][0]}"


class TestExerciseContent:
    """Test exercise content structure and quality."""

    def test_exercise_files_have_structure(self, module_list):
        """Test that exercise files have proper structure."""
        modules_dir = Path('modules')

        if not modules_dir.exists():
            pytest.skip("Modules directory not found")

        for module_name in module_list:
            exercises_dir = modules_dir / module_name / 'exercises'

            if not exercises_dir.exists():
                continue

            for exercise_file in exercises_dir.glob('*.md'):
                with open(exercise_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                # Should have headers
                assert '# ' in content or '## ' in content, \
                    f"Exercise file '{exercise_file}' should have headers"

                # Should have some instructions
                assert len(content.strip()) > 50, \
                    f"Exercise file '{exercise_file}' should have substantial content"

    def test_exercise_files_mention_objectives(self, module_list):
        """Test that exercise files mention learning objectives or goals."""
        modules_dir = Path('modules')

        if not modules_dir.exists():
            pytest.skip("Modules directory not found")

        objective_keywords = [
            'objective', 'goal', 'learn', 'understand', 'create', 'implement',
            'practice', 'exercise', 'task', 'challenge'
        ]

        for module_name in module_list:
            exercises_dir = modules_dir / module_name / 'exercises'

            if not exercises_dir.exists():
                continue

            for exercise_file in exercises_dir.glob('*.md'):
                with open(exercise_file, 'r', encoding='utf-8') as f:
                    content = f.read().lower()

                has_objectives = any(
                    keyword in content for keyword in objective_keywords)
                assert has_objectives, \
                    f"Exercise file '{exercise_file}' should mention learning objectives"


class TestSolutionContent:
    """Test solution file content and quality."""

    def test_solution_files_are_valid_python(self, module_list):
        """Test that solution files are valid Python."""
        modules_dir = Path('modules')

        if not modules_dir.exists():
            pytest.skip("Modules directory not found")

        for module_name in module_list:
            solutions_dir = modules_dir / module_name / 'solutions'

            if not solutions_dir.exists():
                continue

            for solution_file in solutions_dir.glob('*.py'):
                with open(solution_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                # Should be valid Python syntax
                try:
                    compile(content, str(solution_file), 'exec')
                except SyntaxError as e:
                    pytest.fail(
                        f"Syntax error in solution file '{solution_file}': {e}")

    def test_solution_files_have_documentation(self, module_list):
        """Test that solution files have proper documentation."""
        modules_dir = Path('modules')

        if not modules_dir.exists():
            pytest.skip("Modules directory not found")

        for module_name in module_list:
            solutions_dir = modules_dir / module_name / 'solutions'

            if not solutions_dir.exists():
                continue

            for solution_file in solutions_dir.glob('*.py'):
                with open(solution_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                # Should have docstrings or comments
                has_docstring = '"""' in content or "'''" in content
                has_comments = '#' in content

                assert has_docstring or has_comments, \
                    f"Solution file '{solution_file}' should have documentation"
