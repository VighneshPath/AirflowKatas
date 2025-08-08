# Module 6: Data Passing & XComs

## Learning Objectives

By the end of this module, you will be able to:

- Understand XCom concepts and inter-task communication in Airflow
- Implement basic XCom push and pull operations between tasks
- Work with different data types in XComs (strings, numbers, JSON, lists)
- Configure XCom serialization and deserialization
- Build complex data passing scenarios with multiple task dependencies
- Apply XCom best practices and understand performance limitations
- Implement custom XCom backends for advanced use cases

## Prerequisites

- Completed Module 5: Sensors & Triggers
- Understanding of task dependencies and DAG execution
- Basic knowledge of Python data types and JSON serialization
- Familiarity with Airflow task context and templating

## Module Structure

```
06-data-passing-xcoms/
├── README.md                    # This overview
├── concepts.md                  # XCom theory and concepts
├── examples/                    # Working code examples
│   ├── basic_xcom_examples.py
│   ├── advanced_xcom_patterns.py
│   └── custom_xcom_backend.py
├── exercises/                   # Hands-on practice
│   ├── exercise-1-basic-xcoms.md
│   ├── exercise-2-data-types.md
│   ├── exercise-3-complex-patterns.md
│   └── exercise-4-custom-backends.md
├── solutions/                   # Exercise solutions
│   ├── exercise-1-solution.py
│   ├── exercise-2-solution.py
│   ├── exercise-3-solution.py
│   └── exercise-4-solution.py
└── resources.md                 # Additional learning materials
```

## What You'll Build

In this module, you'll create several data-passing workflows:

1. **ETL Pipeline with Data Flow**: Extract, transform, and load data with XCom communication
2. **Multi-Stage Processing**: Pass processed data through multiple transformation tasks
3. **Dynamic Configuration**: Use XComs to pass runtime configuration between tasks
4. **Data Validation Pipeline**: Implement quality checks with result passing
5. **Custom XCom Backend**: Build a file-based XCom storage system

## Key Concepts Covered

### XCom Fundamentals

- What are XComs and when to use them
- XCom push and pull operations
- Task context and XCom access patterns
- XCom key naming and organization

### Data Types and Serialization

- **Primitive Types**: Strings, numbers, booleans
- **Complex Types**: Lists, dictionaries, JSON objects
- **Custom Objects**: Serialization strategies
- **Large Data**: Limitations and alternatives

### Advanced Patterns

- Multiple XCom values from single tasks
- XCom templating in task parameters
- Conditional data passing with branching
- XCom cleanup and lifecycle management

### Performance Considerations

- XCom size limitations and best practices
- Memory usage and database storage
- Alternative approaches for large datasets
- Custom backends for specialized storage

## Estimated Time

- **Reading concepts**: 20-25 minutes
- **Reviewing examples**: 25-30 minutes
- **Exercise 1 (Basic XComs)**: 30-35 minutes
- **Exercise 2 (Data Types)**: 35-40 minutes
- **Exercise 3 (Complex Patterns)**: 40-45 minutes
- **Exercise 4 (Custom Backends)**: 45-50 minutes
- **Total**: 195-225 minutes

## Getting Started

1. **Read the concepts**: Start with `concepts.md` to understand XCom theory
2. **Explore examples**: Review the example DAGs to see XComs in action
3. **Complete exercises**: Work through exercises in order, building complexity
4. **Check solutions**: Compare your implementations with provided solutions
5. **Explore resources**: Use `resources.md` for deeper learning

## Quick Start

To jump right in, create a simple XCom push/pull:

```python
from airflow.operators.python import PythonOperator

def push_data(**context):
    # Push data to XCom
    return {"processed_records": 150, "status": "success"}

def pull_data(**context):
    # Pull data from XCom
    data = context['task_instance'].xcom_pull(task_ids='push_task')
    print(f"Received: {data}")

push_task = PythonOperator(
    task_id='push_task',
    python_callable=push_data
)

pull_task = PythonOperator(
    task_id='pull_task',
    python_callable=pull_data
)

push_task >> pull_task
```

## Common Use Cases

XComs are essential for:

- **ETL Pipelines**: Pass extracted data through transformation stages
- **Configuration Management**: Share runtime parameters between tasks
- **Result Aggregation**: Collect results from parallel processing tasks
- **Quality Control**: Pass validation results and error information
- **Dynamic Workflows**: Use data to influence downstream task behavior

## Best Practices Preview

- Keep XCom payloads small (< 1MB) for optimal performance
- Use meaningful XCom keys for complex workflows
- Implement proper error handling for XCom operations
- Consider alternatives like shared storage for large datasets
- Clean up XComs when no longer needed to manage database size

## Troubleshooting Tips

- **Serialization errors**: Ensure data types are JSON-serializable
- **Missing XComs**: Check task execution order and dependencies
- **Performance issues**: Monitor XCom size and frequency
- **Database growth**: Implement XCom cleanup strategies

## Next Steps

After completing this module:

- **Module 7**: Learn about branching and conditional workflows using XCom data
- **Module 8**: Implement error handling patterns with XCom result passing
- **Module 9**: Explore advanced patterns combining XComs with dynamic DAGs

## Support

If you encounter issues:

1. Check the troubleshooting section in each exercise
2. Review the example implementations
3. Consult the resources for additional documentation
4. Practice with the provided test scenarios

---

Ready to master data passing in Airflow? Let's start with the concepts!
