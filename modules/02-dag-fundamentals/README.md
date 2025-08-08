# Module 2: DAG Fundamentals

Welcome to Module 2! Now that you have Airflow running and understand the basic concepts, it's time to dive deep into DAG creation, configuration, and management. This module will transform you from someone who can run a DAG to someone who can design robust, well-configured workflows.

## Learning Objectives

By the end of this module, you will be able to:

- Understand DAG configuration parameters and their impact on execution
- Configure scheduling intervals using cron expressions and timedelta objects
- Control DAG behavior with catchup, max_active_runs, and other parameters
- Define complex task dependencies using various methods
- Implement fan-out and fan-in patterns in your workflows
- Debug common DAG configuration issues
- Apply best practices for DAG design and organization

## Prerequisites

- Completed Module 1: Setup & Introduction
- Working Airflow environment
- Basic understanding of Python datetime objects
- Familiarity with cron expressions (we'll review this)

## What You'll Learn

### DAG Configuration Mastery

- **Scheduling**: From simple daily runs to complex cron expressions
- **Catchup Behavior**: Understanding how Airflow handles historical runs
- **Concurrency Control**: Managing parallel DAG and task execution
- **Timeouts and SLAs**: Setting expectations for task completion
- **Tags and Documentation**: Organizing and documenting your workflows

### Dependency Patterns

- **Linear Dependencies**: Simple A → B → C patterns
- **Fan-out**: One task triggering multiple parallel tasks
- **Fan-in**: Multiple tasks converging to a single task
- **Complex Patterns**: Combining multiple dependency types
- **Best Practices**: When to use each pattern

## Module Structure

- `concepts.md` - Deep dive into DAG configuration and dependency concepts
- `exercises/` - Hands-on exercises for different DAG patterns
- `examples/` - Working code examples demonstrating various configurations
- `solutions/` - Complete solutions with explanations
- `resources.md` - Links to official documentation and advanced topics

## Quick Start

If you're ready to jump in:

1. Read through `concepts.md` to understand the theory
2. Examine the examples in `examples/` directory
3. Work through exercises starting with `exercise-1-dag-configuration.md`
4. Check your solutions against the provided answers
5. Explore additional resources for deeper learning

## Estimated Time

90-120 minutes

## Key Concepts Preview

### DAG Parameters You'll Master

```python
dag = DAG(
    dag_id='advanced_dag',
    description='A well-configured DAG',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'email_on_failure': True,
        'email_on_retry': False,
    },
    tags=['production', 'daily', 'etl']
)
```

### Dependency Patterns You'll Implement

```python
# Linear: A → B → C
task_a >> task_b >> task_c

# Fan-out: A → [B, C, D]
task_a >> [task_b, task_c, task_d]

# Fan-in: [A, B] → C
[task_a, task_b] >> task_c

# Complex: A → [B, C] → D → [E, F] → G
task_a >> [task_b, task_c] >> task_d >> [task_e, task_f] >> task_g
```

## What's Different from Module 1?

Module 1 focused on getting Airflow running and understanding basic concepts. Module 2 is about mastering the craft:

- **Depth over Breadth**: Fewer concepts, but much deeper understanding
- **Real-world Scenarios**: Exercises based on actual production challenges
- **Configuration Focus**: Understanding why each parameter matters
- **Pattern Recognition**: Learning to identify and implement common workflow patterns

## Success Indicators

You'll know you've mastered this module when you can:

- [ ] Configure a DAG to run on complex schedules (e.g., "every weekday at 9 AM, but not on holidays")
- [ ] Explain the difference between `schedule_interval` and `start_date`
- [ ] Design a workflow with parallel processing that converges to a final task
- [ ] Debug why a DAG isn't running when expected
- [ ] Choose appropriate concurrency settings for different workflow types

## Common Challenges

**Scheduling Confusion**: Understanding when DAGs actually run vs. when you think they should run

**Catchup Behavior**: Dealing with unexpected historical runs when enabling a DAG

**Dependency Complexity**: Creating dependencies that are logical and maintainable

**Performance Issues**: DAGs that work in development but struggle in production

Don't worry - we'll address all of these with clear explanations and practical exercises!

## What's Next

After this module, you'll have a solid foundation in DAG design. Module 3 will build on this knowledge by exploring different types of operators and how to choose the right one for each task.

Ready to become a DAG configuration expert? Let's start with the concepts!
