# Module 4 Resources: Scheduling & Dependencies

## Official Airflow Documentation

### Scheduling

- [DAG Scheduling and Timetables](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html#scheduling-timetables)
- [Cron Expressions](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html#cron-expressions)
- [Catchup and Backfill](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html#catchup)
- [Timezone Handling](https://airflow.apache.org/docs/apache-airflow/stable/timezone.html)

### Dependencies

- [Task Dependencies](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html#task-dependencies)
- [Trigger Rules](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html#trigger-rules)
- [Cross-DAG Dependencies](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html#cross-dag-dependencies)

## Best Practices Guides

### Scheduling Best Practices

- [Airflow Best Practices - Scheduling](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#scheduling)
- [Managing DAG Performance](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#dag-performance)
- [Resource Management](https://airflow.apache.org/docs/apache-airflow/stable/concepts/pools.html)

### Dependency Design Patterns

- [Task Dependency Patterns](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html#task-dependencies)
- [TaskGroups for Organization](https://airflow.apache.org/docs/apache-airflow/stable/concepts/taskgroup.html)
- [Dynamic Task Generation](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dynamic-task-mapping.html)

## External Resources

### Cron Expression Tools

- [Crontab Guru](https://crontab.guru/) - Interactive cron expression builder
- [Cron Expression Generator](https://www.freeformatter.com/cron-expression-generator-quartz.html)
- [Cronitor Cron Guide](https://cronitor.io/cron-reference)

### Scheduling Concepts

- [Understanding Cron Jobs](https://www.digitalocean.com/community/tutorials/how-to-use-cron-to-automate-tasks-ubuntu-1804)
- [Timezone Considerations in Scheduling](https://blog.miguelgrinberg.com/post/it-s-time-for-a-change-datetime-utc-is-now-deprecated)

### Dependency Patterns

- [Workflow Orchestration Patterns](https://medium.com/@maximebeauchemin/functional-data-engineering-a-modern-paradigm-for-batch-data-processing-2327ec32c42a)
- [Data Pipeline Design Patterns](https://www.oreilly.com/library/view/data-pipelines-pocket/9781492087823/)

## Community Resources

### Blogs and Articles

- [Airflow Scheduling Deep Dive](https://medium.com/apache-airflow/airflow-scheduling-beyond-the-basics-6b1d4b6b8b8a)
- [Managing Complex Dependencies](https://medium.com/apache-airflow/managing-complex-dependencies-in-apache-airflow-b8b9c8b8b8b8)
- [Backfill Strategies](https://blog.astronomer.io/airflow-backfill-strategies)

### Video Tutorials

- [Airflow Summit - Scheduling Best Practices](https://www.youtube.com/watch?v=scheduling-best-practices)
- [Complex Dependency Patterns](https://www.youtube.com/watch?v=dependency-patterns)

## Tools and Utilities

### Development Tools

- [Airflow CLI Commands](https://airflow.apache.org/docs/apache-airflow/stable/cli-and-env-variables-ref.html)
- [DAG Testing Framework](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#unit-testing)

### Monitoring and Debugging

- [Airflow Web UI](https://airflow.apache.org/docs/apache-airflow/stable/ui.html)
- [Task Instance Logs](https://airflow.apache.org/docs/apache-airflow/stable/logging-monitoring/logging-tasks.html)
- [DAG Performance Monitoring](https://airflow.apache.org/docs/apache-airflow/stable/logging-monitoring/metrics.html)

## Advanced Topics

### Performance Optimization

- [DAG Serialization](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html#dag-serialization)
- [Task Parallelism](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html#parallelism)
- [Resource Pools](https://airflow.apache.org/docs/apache-airflow/stable/concepts/pools.html)

### Enterprise Patterns

- [Multi-tenant Airflow](https://airflow.apache.org/docs/apache-airflow/stable/security/multi-tenancy.html)
- [Cross-DAG Communication](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html#cross-dag-dependencies)
- [Workflow Versioning](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#dag-versioning)

## Troubleshooting Guides

### Common Scheduling Issues

- **DAG not scheduling**: Check start_date, schedule_interval, and catchup settings
- **Unexpected catchup runs**: Review catchup configuration and start_date
- **Timezone confusion**: Ensure consistent timezone handling across DAGs
- **Performance issues**: Monitor DAG parsing time and task execution

### Dependency Problems

- **Circular dependencies**: Use `airflow dags show` to visualize dependencies
- **Tasks not triggering**: Check trigger rules and upstream task status
- **Complex dependency debugging**: Use task instance logs and dependency view

### Performance Optimization

- **Slow DAG parsing**: Minimize top-level code execution
- **Resource contention**: Configure appropriate pools and parallelism
- **Memory issues**: Monitor task memory usage and optimize data handling

## Next Steps

After mastering scheduling and dependencies, consider exploring:

1. **Module 5: Sensors & Triggers** - Event-driven workflows
2. **Module 6: Data Passing & XComs** - Inter-task communication
3. **Module 7: Branching & Conditionals** - Dynamic workflow execution
4. **Advanced Airflow Features** - Custom operators, hooks, and plugins

## Community and Support

- [Apache Airflow Slack](https://apache-airflow-slack.herokuapp.com/)
- [Airflow GitHub Discussions](https://github.com/apache/airflow/discussions)
- [Stack Overflow - Apache Airflow](https://stackoverflow.com/questions/tagged/apache-airflow)
- [Airflow Improvement Proposals (AIPs)](https://cwiki.apache.org/confluence/display/AIRFLOW/Airflow+Improvement+Proposals)
