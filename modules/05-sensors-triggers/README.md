# Module 5: Sensors & Triggers

## Learning Objectives

By the end of this module, you will be able to:

- Understand sensor concepts and event-driven workflows in Airflow
- Implement FileSensor for file system monitoring
- Create S3-like sensors using mock cloud storage
- Configure sensor timeouts, retries, and performance optimization
- Build custom sensors with proper polling logic
- Apply sensor best practices for production workflows

## Prerequisites

- Completed Module 4: Scheduling & Dependencies
- Understanding of DAG fundamentals and task dependencies
- Basic knowledge of file systems and cloud storage concepts

## Module Structure

```
05-sensors-triggers/
├── README.md                    # This overview
├── concepts.md                  # Sensor theory and concepts
├── examples/                    # Working code examples
│   ├── file_sensor_examples.py
│   └── s3_sensor_examples.py
├── exercises/                   # Hands-on practice
│   ├── exercise-1-file-sensors.md
│   ├── exercise-2-s3-sensors.md
│   └── exercise-3-sensor-configuration.md
├── solutions/                   # Exercise solutions
│   ├── exercise-1-solution.py
│   └── exercise-2-solution.py
└── resources.md                 # Additional learning materials
```

## What You'll Build

In this module, you'll create several sensor-based workflows:

1. **File Processing Pipeline**: Wait for daily sales reports and process them automatically
2. **Multi-Source Data Pipeline**: Coordinate multiple file dependencies with different reliability levels
3. **Cloud Storage Monitor**: Simulate S3-like sensors with mock cloud storage
4. **Advanced Sensor Configuration**: Implement enterprise-grade sensors with performance optimization

## Key Concepts Covered

### Sensor Fundamentals

- What are sensors and when to use them
- Polling vs event-driven approaches
- Sensor modes: poke vs reschedule

### Built-in Sensors

- **FileSensor**: Monitor file system changes
- **S3KeySensor**: Wait for cloud storage objects (simulated)
- **HttpSensor**: Monitor API endpoints
- **DateTimeSensor**: Time-based triggers

### Advanced Configuration

- Timeout strategies for different reliability levels
- Retry mechanisms with exponential backoff
- Performance optimization and resource management
- Custom sensors with specialized logic

### Production Patterns

- Circuit breaker patterns for failing sensors
- Performance monitoring and metrics collection
- SLA monitoring and alerting
- Adaptive polling intervals

## Estimated Time

- **Reading concepts**: 15-20 minutes
- **Reviewing examples**: 20-25 minutes
- **Exercise 1 (File Sensors)**: 25-30 minutes
- **Exercise 2 (S3 Sensors)**: 30-35 minutes
- **Exercise 3 (Advanced Config)**: 35-40 minutes
- **Total**: 125-150 minutes

## Getting Started

1. **Read the concepts**: Start with `concepts.md` to understand sensor theory
2. **Explore examples**: Review the example DAGs to see sensors in action
3. **Complete exercises**: Work through exercises in order, building complexity
4. **Check solutions**: Compare your implementations with provided solutions
5. **Explore resources**: Use `resources.md` for deeper learning

## Quick Start

To jump right in, create a simple FileSensor:

```python
from airflow.sensors.filesystem import FileSensor

wait_for_file = FileSensor(
    task_id='wait_for_data',
    filepath='/tmp/data/input.csv',
    poke_interval=30,
    timeout=300
)
```

## Common Use Cases

Sensors are essential for:

- **Data Pipeline Triggers**: Wait for upstream data before processing
- **External System Integration**: Monitor APIs, databases, or file systems
- **Batch Processing**: Coordinate with external batch jobs
- **Event-Driven Workflows**: React to external events and changes
- **Quality Gates**: Wait for validation or approval processes

## Best Practices Preview

- Use reschedule mode for long-running sensors to free up worker slots
- Set appropriate timeouts based on expected data arrival patterns
- Implement proper error handling and alerting for sensor failures
- Monitor sensor performance and adjust configurations based on metrics
- Consider alternatives like event-driven triggers for high-frequency scenarios

## Troubleshooting Tips

- **Sensor timeouts**: Check if timeout values are realistic for your data sources
- **Worker slot exhaustion**: Use reschedule mode for sensors with long wait times
- **Performance issues**: Monitor sensor frequency and adjust poke intervals
- **File permission errors**: Ensure Airflow has proper access to monitored directories

## Next Steps

After completing this module:

- **Module 6**: Learn about XComs and data passing between sensor and processing tasks
- **Module 7**: Explore branching based on sensor results
- **Module 8**: Implement comprehensive error handling for sensor-based workflows

## Support

If you encounter issues:

1. Check the troubleshooting section in each exercise
2. Review the example implementations
3. Consult the resources for additional documentation
4. Practice with the provided test data setups

---

Ready to build event-driven workflows? Let's start with the concepts!
