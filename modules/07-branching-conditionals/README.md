# Module 7: Branching & Conditionals

## Learning Objectives

By the end of this module, you will be able to:

- Master BranchPythonOperator for conditional workflows
- Implement data-driven branching logic using XCom data
- Create complex multi-path branching scenarios
- Build intelligent error recovery patterns with branching
- Combine branching with advanced Airflow features
- Design dynamic workflows that adapt to runtime conditions

## Prerequisites

- Completed Module 6: Data Passing & XComs
- Understanding of task dependencies and trigger rules
- Knowledge of Python conditional logic and data structures
- Familiarity with Airflow task context and templating

## Module Structure

```
07-branching-conditionals/
├── README.md                    # This overview
├── concepts.md                  # Branching theory and patterns
├── examples/                    # Working code examples
│   ├── basic_branching_examples.py
│   └── advanced_branching_patterns.py
├── exercises/                   # Hands-on practice
│   ├── exercise-1-basic-branching.md
│   ├── exercise-2-data-driven-branching.md
│   └── exercise-3-complex-conditional-workflows.md
├── solutions/                   # Exercise solutions
│   ├── exercise-1-solution.py
│   ├── exercise-2-solution.py
│   └── exercise-3-solution.py
└── resources.md                 # Additional learning materials
```

## What You'll Build

In this module, you'll create sophisticated branching workflows:

1. **Time-Based Processing**: Different workflows for business hours vs off-hours
2. **Data-Driven Pipelines**: Processing paths determined by data characteristics
3. **Quality-Based Routing**: Automatic routing based on data quality metrics
4. **Error Recovery Systems**: Intelligent fallback mechanisms using branching
5. **Multi-Region Processing**: Geographic routing with nested decision points

## Key Concepts Covered

### Basic Branching

- **BranchPythonOperator**: Core branching functionality
- **Task Skipping**: Understanding which tasks execute vs skip
- **Trigger Rules**: Joining execution paths after branching
- **Simple Conditions**: Time, date, and basic data-driven decisions

### Advanced Branching Patterns

- **Multi-Path Branching**: Selecting multiple tasks for parallel execution
- **Nested Branching**: Multiple levels of decision points
- **XCom-Driven Decisions**: Using data from previous tasks for branching
- **Dynamic Task Selection**: Runtime determination of workflow paths

### Error Handling with Branching

- **Error Classification**: Different recovery strategies for different errors
- **Fallback Mechanisms**: Automatic switching to alternative processing
- **Circuit Breaker Patterns**: Preventing cascading failures
- **Recovery Workflows**: Sophisticated error recovery using branching

### Performance Considerations

- **Branching Logic Optimization**: Keeping decision functions lightweight
- **XCom Payload Management**: Handling large data in branching decisions
- **Resource Allocation**: Intelligent resource usage based on conditions
- **Scalability Patterns**: Branching that scales with system load

## Estimated Time

- **Reading concepts**: 25-30 minutes
- **Reviewing examples**: 30-35 minutes
- **Exercise 1 (Basic Branching)**: 45-60 minutes
- **Exercise 2 (Data-Driven)**: 90-120 minutes
- **Exercise 3 (Complex Workflows)**: 180-240 minutes
- **Total**: 370-485 minutes (6-8 hours)

## Getting Started

1. **Read the concepts**: Start with `concepts.md` to understand branching theory
2. **Explore examples**: Review the example DAGs to see branching in action
3. **Complete exercises**: Work through exercises in order, building complexity
4. **Check solutions**: Compare your implementations with provided solutions
5. **Explore resources**: Use `resources.md` for deeper learning

## Quick Start

To jump right in, create a simple branch:

```python
from airflow.operators.python import BranchPythonOperator

def simple_branch(**context):
    # Simple condition check
    if datetime.now().hour < 12:
        return 'morning_task'
    else:
        return 'afternoon_task'

branch_task = BranchPythonOperator(
    task_id='time_branch',
    python_callable=simple_branch
)

# Tasks will be created and connected
branch_task >> [morning_task, afternoon_task]
```

## Common Use Cases

Branching is essential for:

- **Environment-Specific Processing**: Different logic for dev/staging/production
- **Data Quality Routing**: Route data based on quality assessments
- **Resource-Based Decisions**: Adapt processing based on available resources
- **Business Logic Implementation**: Complex business rules in data pipelines
- **Error Recovery**: Intelligent handling of different failure scenarios
- **Performance Optimization**: Skip unnecessary processing based on conditions

## Best Practices Preview

- Keep branching functions simple and fast-executing
- Use meaningful task IDs that clearly indicate the processing path
- Implement proper error handling in branching logic
- Log branching decisions for debugging and analysis
- Use trigger rules appropriately for joining after branches
- Test all possible branching paths thoroughly

## Troubleshooting Tips

- **Tasks not skipping**: Verify branching function returns exact task_id strings
- **Join tasks not executing**: Check trigger rules on downstream tasks
- **XCom data missing**: Ensure upstream tasks completed successfully
- **Performance issues**: Profile branching logic for bottlenecks

## Next Steps

After completing this module:

- **Module 8**: Learn error handling and monitoring patterns
- **Module 9**: Explore advanced workflow patterns and optimization
- **Module 10**: Apply branching in real-world project scenarios

## Support

If you encounter issues:

1. Check the troubleshooting section in each exercise
2. Review the example implementations for patterns
3. Test your branching functions independently before adding to DAGs
4. Use the Airflow UI to visualize and debug task execution flow

---

Ready to master conditional workflows in Airflow? Let's start with the concepts!
