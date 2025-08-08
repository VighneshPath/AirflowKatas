# Contributing to Airflow Coding Kata

Thank you for your interest in contributing to the Airflow Coding Kata! This document provides guidelines for contributing new modules, exercises, improvements, and bug fixes.

## Table of Contents

- [Getting Started](#getting-started)
- [Types of Contributions](#types-of-contributions)
- [Module Contribution Guidelines](#module-contribution-guidelines)
- [Exercise Creation Guidelines](#exercise-creation-guidelines)
- [Code Standards](#code-standards)
- [Testing Requirements](#testing-requirements)
- [Documentation Standards](#documentation-standards)
- [Submission Process](#submission-process)
- [Community Guidelines](#community-guidelines)

## Getting Started

### Prerequisites

Before contributing, ensure you have:

- Python 3.11+ installed
- Docker and Docker Compose installed
- Git installed and configured
- Basic understanding of Apache Airflow
- Familiarity with the kata structure

### Development Setup

1. Fork the repository
2. Clone your fork locally
3. Set up the development environment:

```bash
# Install dependencies
pip install -r requirements.txt

# Set up pre-commit hooks
pre-commit install

# Validate your setup
python scripts/verify_setup.py
```

4. Run the existing tests to ensure everything works:

```bash
pytest tests/
python scripts/validate_dags.py
python scripts/validate_exercises.py
```

## Types of Contributions

We welcome several types of contributions:

### 🆕 New Modules

- Advanced Airflow topics not currently covered
- Integration with specific tools or services
- Industry-specific use cases

### 📚 New Exercises

- Additional practice exercises for existing modules
- Real-world scenarios and case studies
- Different difficulty levels

### 🐛 Bug Fixes

- Corrections to existing code
- Documentation fixes
- Test improvements

### 🔧 Improvements

- Better explanations or examples
- Performance optimizations
- User experience enhancements

### 📖 Documentation

- Clearer instructions
- Additional resources
- Translation to other languages

## Module Contribution Guidelines

### Module Structure

Each module must follow this exact structure:

```
modules/XX-module-name/
├── README.md                    # Module overview and learning objectives
├── concepts.md                  # Theoretical concepts and explanations
├── resources.md                 # Links and additional reading
├── examples/                    # Working code examples
│   ├── basic_example.py
│   ├── advanced_example.py
│   └── ...
├── exercises/                   # Hands-on exercises
│   ├── exercise-1-name.md
│   ├── exercise-2-name.md
│   └── ...
└── solutions/                   # Exercise solutions
    ├── exercise-1-solution.py
    ├── exercise-2-solution.py
    └── ...
```

### Module Naming Convention

- Use two-digit numbering: `01-`, `02-`, etc.
- Use kebab-case for module names
- Choose descriptive, clear names
- Ensure logical progression from existing modules

### Module Content Requirements

#### README.md

Must include:

- Clear learning objectives
- Prerequisites from other modules
- Estimated completion time
- Overview of concepts covered

#### concepts.md

Must include:

- Theoretical background
- Key concepts with examples
- Best practices
- Common pitfalls to avoid

#### resources.md

Must include:

- Links to official Airflow documentation
- Additional tutorials and guides
- Relevant blog posts or articles
- Community resources

### Module Template

Use the module template provided in `templates/module-template/` as your starting point:

```bash
cp -r templates/module-template/ modules/XX-your-module-name/
```

## Exercise Creation Guidelines

### Exercise Structure

Each exercise should:

1. **Have clear objectives**: What will students learn?
2. **Build incrementally**: Each step should build on the previous
3. **Include validation**: How can students verify their solution?
4. **Provide context**: Real-world scenarios when possible

### Exercise Format

```markdown
# Exercise X: Title

## Objective

Clear statement of what students will accomplish.

## Prerequisites

- List of required knowledge
- Previous exercises that must be completed

## Scenario

Real-world context for the exercise.

## Requirements

1. Specific, testable requirements
2. Each requirement should be verifiable
3. Include acceptance criteria

## Instructions

Step-by-step guidance without giving away the solution.

## Validation

How students can verify their solution works.

## Extension Challenges

Optional additional challenges for advanced students.
```

### Solution Requirements

Every exercise must have a corresponding solution that:

- Passes all DAG validation tests
- Includes comprehensive comments
- Demonstrates best practices
- Handles edge cases appropriately
- Is production-ready code quality

## Code Standards

### Python Code Style

- Follow PEP 8 style guidelines
- Use type hints where appropriate
- Include docstrings for all functions and classes
- Use meaningful variable and function names
- Keep functions focused and small

### DAG Standards

```python
# Required imports
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Standard default_args
default_args = {
    'owner': 'kata-contributor',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Clear DAG definition
dag = DAG(
    'descriptive_dag_id',
    default_args=default_args,
    description='Clear description of what this DAG does',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['kata', 'module-name'],
)
```

### Documentation Standards

- Use clear, concise language
- Avoid jargon without explanation
- Include code examples for complex concepts
- Use consistent formatting
- Provide context for why, not just how

## Testing Requirements

### Required Tests

All contributions must include:

1. **DAG Validation Tests**: Ensure DAGs parse correctly
2. **Exercise Validation Tests**: Verify solutions work
3. **Integration Tests**: Ensure compatibility with existing modules

### Test Structure

```python
def test_your_feature():
    """Test description following pytest conventions."""
    # Arrange
    setup_test_data()

    # Act
    result = execute_test()

    # Assert
    assert result.is_valid()
    assert result.meets_requirements()
```

### Running Tests

Before submitting:

```bash
# Run all tests
pytest tests/

# Validate DAGs
python scripts/validate_dags.py

# Validate exercises
python scripts/validate_exercises.py

# Run end-to-end validation
python scripts/end_to_end_validation.py --integration-tests
```

## Submission Process

### Before Submitting

1. **Test thoroughly**: Run all validation scripts
2. **Review checklist**: Use the contribution checklist below
3. **Update documentation**: Ensure all docs are current
4. **Check formatting**: Run pre-commit hooks

### Pull Request Process

1. **Create feature branch**: `git checkout -b feature/your-feature-name`
2. **Make changes**: Follow all guidelines above
3. **Commit changes**: Use clear, descriptive commit messages
4. **Push branch**: `git push origin feature/your-feature-name`
5. **Create PR**: Use the provided PR template
6. **Address feedback**: Respond to review comments promptly

### PR Template

```markdown
## Description

Brief description of changes and motivation.

## Type of Change

- [ ] New module
- [ ] New exercise
- [ ] Bug fix
- [ ] Documentation update
- [ ] Other (please describe)

## Testing

- [ ] All existing tests pass
- [ ] New tests added for new functionality
- [ ] DAG validation passes
- [ ] Exercise validation passes

## Checklist

- [ ] Code follows style guidelines
- [ ] Self-review completed
- [ ] Documentation updated
- [ ] Tests added/updated
- [ ] No breaking changes
```

## Contribution Checklist

### For New Modules

- [ ] Module follows required directory structure
- [ ] README.md includes all required sections
- [ ] concepts.md provides clear explanations
- [ ] resources.md includes relevant links
- [ ] At least 3 exercises with solutions
- [ ] All DAGs pass validation
- [ ] Tests added for new functionality
- [ ] Integration with existing modules verified

### For New Exercises

- [ ] Clear learning objectives defined
- [ ] Prerequisites clearly stated
- [ ] Step-by-step instructions provided
- [ ] Solution provided and tested
- [ ] Validation criteria specified
- [ ] Fits logically in module progression

### For Bug Fixes

- [ ] Issue clearly identified
- [ ] Root cause understood
- [ ] Fix tested thoroughly
- [ ] No regression introduced
- [ ] Documentation updated if needed

## Community Guidelines

### Code of Conduct

We are committed to providing a welcoming and inclusive environment. Please:

- Be respectful and constructive in all interactions
- Focus on what is best for the community
- Show empathy towards other community members
- Accept constructive criticism gracefully

### Communication

- **Issues**: Use GitHub issues for bug reports and feature requests
- **Discussions**: Use GitHub discussions for questions and ideas
- **Reviews**: Provide constructive, helpful feedback on PRs

### Recognition

Contributors will be recognized in:

- CONTRIBUTORS.md file
- Release notes for significant contributions
- Module attribution for new modules

## Maintenance Guidelines

### For Maintainers

#### Regular Maintenance Tasks

1. **Update Dependencies**: Keep requirements.txt current
2. **Airflow Compatibility**: Test with new Airflow versions
3. **Link Validation**: Ensure external links remain active
4. **Content Review**: Periodically review content for accuracy

#### Release Process

1. **Version Tagging**: Use semantic versioning
2. **Release Notes**: Document all changes
3. **Testing**: Full end-to-end validation before release
4. **Communication**: Announce releases to community

#### Content Standards

- Maintain consistency across all modules
- Ensure progressive difficulty
- Keep examples current with Airflow best practices
- Regular review of community feedback

### Content Lifecycle

#### New Content

1. Community proposal or maintainer identification
2. Design and planning phase
3. Development and testing
4. Community review and feedback
5. Integration and release

#### Updates

1. Identify need for updates (new Airflow features, deprecated APIs)
2. Plan update scope
3. Implement changes
4. Test thoroughly
5. Release with clear migration notes

#### Deprecation

1. Announce deprecation with timeline
2. Provide migration path
3. Maintain for at least one major version
4. Remove with clear communication

## Getting Help

### For Contributors

- **Documentation**: Check existing docs first
- **Issues**: Search existing issues before creating new ones
- **Discussions**: Use GitHub discussions for questions
- **Contact**: Reach out to maintainers for guidance

### For Maintainers

- **Contributor Guide**: This document
- **Maintainer Docs**: See `docs/maintainer-guide.md`
- **Templates**: Use provided templates in `templates/`
- **Scripts**: Leverage automation scripts in `scripts/`

## Templates and Tools

### Available Templates

- `templates/module-template/`: Complete module structure
- `templates/exercise-template.md`: Exercise format template
- `templates/dag-template.py`: Standard DAG template
- `templates/test-template.py`: Test structure template

### Automation Scripts

- `scripts/create_module.py`: Generate new module structure
- `scripts/validate_contribution.py`: Validate contributions
- `scripts/update_docs.py`: Update documentation
- `scripts/check_links.py`: Validate external links

### Usage Examples

```bash
# Create new module
python scripts/create_module.py --name "advanced-sensors" --number 11

# Validate contribution
python scripts/validate_contribution.py --module 11-advanced-sensors

# Check all external links
python scripts/check_links.py
```

## Thank You!

Your contributions help make Airflow more accessible to developers worldwide. Every contribution, no matter how small, makes a difference in the learning experience of others.

For questions about contributing, please open a GitHub discussion or contact the maintainers.

Happy contributing! 🚀
