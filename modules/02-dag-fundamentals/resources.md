# DAG Fundamentals Resources

## Official Airflow Documentation

### Core Concepts

- [DAG Concepts](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html) - Official DAG documentation
- [Scheduling & Timetables](https://airflow.apache.org/docs/apache-airflow/stable/concepts/timetables.html) - Advanced scheduling concepts
- [Task Dependencies](https://airflow.apache.org/docs/apache-airflow/stable/concepts/tasks.html#task-dependencies) - Dependency management
- [DAG Runs](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dag-run.html) - Understanding DAG execution

### Configuration Reference

- [DAG Configuration](https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#dag) - Complete configuration options
- [Default Arguments](https://airflow.apache.org/docs/apache-airflow/stable/concepts/operators.html#default-arguments) - Setting task defaults
- [Catchup and Backfill](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dag-run.html#catchup) - Historical run behavior

## Scheduling Deep Dive

### Cron Expression Resources

- [Crontab Guru](https://crontab.guru/) - Interactive cron expression builder and validator
- [Cron Expression Generator](https://www.freeformatter.com/cron-expression-generator-quartz.html) - Visual cron builder
- [Airflow Scheduling Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#scheduling) - Official scheduling guidelines

### Time Zone Handling

- [Time Zones in Airflow](https://airflow.apache.org/docs/apache-airflow/stable/timezone.html) - Working with time zones
- [Pendulum Documentation](https://pendulum.eustace.io/) - Python datetime library used by Airflow

## Dependency Patterns

### Task Dependencies

- [Task Groups](https://airflow.apache.org/docs/apache-airflow/stable/concepts/task-groups.html) - Organizing related tasks
- [Cross-DAG Dependencies](https://airflow.apache.org/docs/apache-airflow/stable/concepts/cross-dag-dependencies.html) - Dependencies between DAGs
- [Branching](https://airflow.apache.org/docs/apache-airflow/stable/concepts/operators.html#branching) - Conditional task execution

### Advanced Patterns

- [SubDAGs](https://airflow.apache.org/docs/apache-airflow/stable/concepts/subdags.html) - Nested workflow patterns
- [Dynamic Task Mapping](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dynamic-task-mapping.html) - Creating tasks dynamically
- [TaskFlow API](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html) - Modern Python-centric approach

## Performance and Best Practices

### DAG Design

- [Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html) - Official best practices guide
- [DAG Writing Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#writing-a-dag) - Code organization tips
- [Performance Tuning](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#performance-tuning) - Optimization strategies

### Common Pitfalls

- [Common Pitfalls](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#common-pitfalls) - What to avoid
- [DAG Loading Performance](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#dag-loading-performance) - Parsing optimization
- [Resource Management](https://airflow.apache.org/docs/apache-airflow/stable/concepts/pools.html) - Managing resource usage

## Debugging and Troubleshooting

### Debugging Tools

- [Airflow CLI](https://airflow.apache.org/docs/apache-airflow/stable/cli-and-env-variables-ref.html) - Command-line debugging tools
- [Testing DAGs](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#testing-a-dag) - Unit testing strategies
- [Logging](https://airflow.apache.org/docs/apache-airflow/stable/logging-monitoring/logging-tasks.html) - Understanding task logs

### Common Issues

- [FAQ](https://airflow.apache.org/docs/apache-airflow/stable/faq.html) - Frequently asked questions
- [Troubleshooting](https://airflow.apache.org/docs/apache-airflow/stable/troubleshooting.html) - Common problems and solutions

## Community Resources

### Tutorials and Guides

- [Astronomer Guides](https://docs.astronomer.io/learn/) - Comprehensive Airflow tutorials
- [Apache Airflow Tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html) - Official tutorial
- [Real Python Airflow Guide](https://realpython.com/airflow-tutorial/) - Python-focused tutorial

### Video Resources

- [Apache Airflow YouTube Channel](https://www.youtube.com/c/ApacheAirflow) - Official videos and webinars
- [Astronomer YouTube Channel](https://www.youtube.com/c/AstronomerIO) - Airflow tutorials and best practices
- [Airflow Summit Talks](https://airflowsummit.org/) - Conference presentations

### Community Forums

- [Apache Airflow Slack](https://apache-airflow-slack.herokuapp.com/) - Active community chat
- [Stack Overflow](https://stackoverflow.com/questions/tagged/airflow) - Q&A for specific problems
- [GitHub Discussions](https://github.com/apache/airflow/discussions) - Feature discussions and help

## Advanced Topics

### Custom Components

- [Custom Operators](https://airflow.apache.org/docs/apache-airflow/stable/howto/custom-operator.html) - Building custom operators
- [Custom Hooks](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html#custom-connection-types) - Creating custom connections
- [Plugins](https://airflow.apache.org/docs/apache-airflow/stable/plugins.html) - Extending Airflow functionality

### Integration Patterns

- [Provider Packages](https://airflow.apache.org/docs/apache-airflow-providers/) - Pre-built integrations
- [REST API](https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html) - Programmatic DAG management
- [Kubernetes Integration](https://airflow.apache.org/docs/apache-airflow/stable/kubernetes.html) - Running on Kubernetes

## Tools and Utilities

### Development Tools

- [Airflow IDE Plugins](https://marketplace.visualstudio.com/search?term=airflow&target=VSCode) - IDE extensions
- [DAG Visualization Tools](https://airflow.apache.org/docs/apache-airflow/stable/ui.html#graph-view) - Understanding workflow visualization
- [Local Development Setup](https://airflow.apache.org/docs/apache-airflow/stable/start/local.html) - Development environment

### Monitoring and Observability

- [Metrics and Monitoring](https://airflow.apache.org/docs/apache-airflow/stable/logging-monitoring/metrics.html) - Performance monitoring
- [Health Checks](https://airflow.apache.org/docs/apache-airflow/stable/logging-monitoring/check-health.html) - System health monitoring
- [Alerting](https://airflow.apache.org/docs/apache-airflow/stable/howto/email-config.html) - Setting up notifications

## Books and In-Depth Resources

### Recommended Reading

- [Data Pipelines with Apache Airflow](https://www.manning.com/books/data-pipelines-with-apache-airflow) - Comprehensive book on Airflow
- [Apache Airflow: The Definitive Guide](https://www.oreilly.com/library/view/apache-airflow-the/9781492032955/) - O'Reilly guide
- [Building Data Pipelines with Python](https://www.packtpub.com/product/building-data-pipelines-with-python/9781801079211) - Practical pipeline development

### Research Papers

- [Apache Airflow: A Platform to Program and Monitor Workflows](https://cwiki.apache.org/confluence/display/AIRFLOW/Research+Papers) - Academic research on Airflow
- [Workflow Management Systems](https://en.wikipedia.org/wiki/Workflow_management_system) - General workflow concepts

## Practice Datasets

### Sample Data Sources

- [NYC Taxi Data](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) - Real-world transportation data
- [COVID-19 Data](https://github.com/CSSEGISandData/COVID-19) - Time-series health data
- [Stock Market Data](https://www.alphavantage.co/) - Financial time-series data
- [Weather Data](https://openweathermap.org/api) - Meteorological data

### Practice Scenarios

- ETL Pipeline Development
- Data Quality Monitoring
- Report Generation Workflows
- API Integration Patterns
- Batch Processing Pipelines

## Next Steps

After mastering DAG fundamentals, consider exploring:

1. **Module 3: Tasks & Operators** - Different operator types and when to use them
2. **Advanced Scheduling** - Custom timetables and complex scheduling logic
3. **Production Deployment** - Scaling Airflow for production workloads
4. **Data Engineering Patterns** - Real-world data pipeline architectures
5. **MLOps with Airflow** - Machine learning workflow orchestration

## Contributing to This Resource

Found a helpful resource not listed here? Consider contributing:

1. Check if the resource is still active and relevant
2. Verify the quality and accuracy of the content
3. Add it to the appropriate section with a brief description
4. Include the difficulty level (beginner/intermediate/advanced)

Remember: The best way to learn DAG fundamentals is through hands-on practice. Use these resources to supplement your practical exercises!
