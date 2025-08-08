# Sensors & Triggers Resources

## Official Airflow Documentation

### Core Sensor Documentation

- [Sensors Overview](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/sensors.html) - Official sensor concepts and usage
- [BaseSensorOperator](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/sensors/base/index.html) - Base class for all sensors
- [Sensor Modes](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/sensors.html#sensor-modes) - Poke vs Reschedule modes

### Built-in Sensors

- [FileSensor](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/sensors/filesystem/index.html) - File system sensors
- [DateTimeSensor](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/sensors/date_time/index.html) - Time-based sensors
- [TimeSensor](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/sensors/time_sensor/index.html) - Simple time sensors

### Provider Sensors

- [AWS S3 Sensors](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/sensors/s3.html) - S3KeySensor and related sensors
- [HTTP Sensors](https://airflow.apache.org/docs/apache-airflow-providers-http/stable/sensors.html) - HTTP endpoint monitoring
- [Database Sensors](https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/sensors.html) - Database table/record sensors

## Best Practices Guides

### Sensor Configuration

- [Sensor Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#sensors) - Official best practices
- [Timeout and Retry Strategies](https://medium.com/apache-airflow/airflow-sensor-timeout-and-retry-strategies-b8c9c0b8c5f4) - Comprehensive timeout guide
- [Reschedule vs Poke Mode](https://marclamberti.com/blog/airflow-sensors-reschedule-vs-poke/) - When to use each mode

### Performance Optimization

- [Sensor Performance Tuning](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#sensor-performance) - Optimizing sensor performance
- [Worker Slot Management](https://medium.com/apache-airflow/airflow-worker-slot-management-for-sensors-8c8c8c8c8c8c) - Managing resources effectively
- [Sensor Monitoring](https://airflow.apache.org/docs/apache-airflow/stable/logging-monitoring/metrics.html) - Monitoring sensor health

## Advanced Topics

### Custom Sensors

- [Creating Custom Sensors](https://airflow.apache.org/docs/apache-airflow/stable/howto/custom-operator.html#sensors) - Building your own sensors
- [Sensor Inheritance Patterns](https://medium.com/apache-airflow/custom-airflow-sensors-inheritance-patterns-12345) - Advanced sensor design
- [Async Sensors](https://airflow.apache.org/docs/apache-airflow/stable/concepts/deferrable-operators.html) - Deferrable/async sensor patterns

### Error Handling

- [Sensor Error Handling](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html#task-failure-handling) - Handling sensor failures
- [Circuit Breaker Pattern](https://martinfowler.com/bliki/CircuitBreaker.html) - Preventing cascading failures
- [Exponential Backoff](https://cloud.google.com/iot/docs/how-tos/exponential-backoff) - Retry strategies

## Cloud Provider Specific

### AWS

- [S3 Event Notifications](https://docs.aws.amazon.com/AmazonS3/latest/userguide/NotificationHowTo.html) - Alternative to polling
- [CloudWatch Events](https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/WhatIsCloudWatchEvents.html) - Event-driven triggers
- [SQS Sensors](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/sensors/sqs.html) - Message queue sensors

### Google Cloud

- [GCS Sensors](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/sensors/gcs.html) - Google Cloud Storage sensors
- [Pub/Sub Sensors](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/sensors/pubsub.html) - Message queue sensors
- [BigQuery Sensors](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/sensors/bigquery.html) - Data warehouse sensors

### Azure

- [Blob Storage Sensors](https://airflow.apache.org/docs/apache-airflow-providers-microsoft-azure/stable/sensors/wasb.html) - Azure blob sensors
- [Data Factory Sensors](https://airflow.apache.org/docs/apache-airflow-providers-microsoft-azure/stable/sensors/data_factory.html) - Pipeline monitoring

## Monitoring and Observability

### Metrics and Monitoring

- [Airflow Metrics](https://airflow.apache.org/docs/apache-airflow/stable/logging-monitoring/metrics.html) - Built-in metrics
- [Prometheus Integration](https://airflow.apache.org/docs/apache-airflow/stable/logging-monitoring/metrics.html#prometheus) - Metrics collection
- [Grafana Dashboards](https://github.com/apache/airflow/tree/main/chart/files/grafana) - Visualization templates

### Alerting

- [Email Notifications](https://airflow.apache.org/docs/apache-airflow/stable/howto/email-config.html) - Email alerts setup
- [Slack Integration](https://airflow.apache.org/docs/apache-airflow-providers-slack/stable/operators/slack_webhook.html) - Slack notifications
- [PagerDuty Integration](https://airflow.apache.org/docs/apache-airflow-providers-pagerduty/stable/) - Incident management

## Tutorials and Examples

### Beginner Tutorials

- [Sensor Tutorial Series](https://marclamberti.com/blog/airflow-sensors-tutorial/) - Step-by-step sensor guide
- [File Sensor Examples](https://github.com/apache/airflow/tree/main/airflow/example_dags) - Official examples
- [Sensor Patterns](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/fundamentals.html#sensors) - Common patterns

### Advanced Examples

- [Event-Driven Workflows](https://medium.com/apache-airflow/event-driven-airflow-workflows-12345) - Real-world patterns
- [Multi-Cloud Sensors](https://github.com/astronomer/airflow-guides/tree/main/guides) - Cross-cloud examples
- [Sensor Orchestration](https://www.astronomer.io/guides/airflow-sensors/) - Complex coordination patterns

## Tools and Libraries

### Testing

- [pytest-airflow](https://pypi.org/project/pytest-airflow/) - Testing framework for Airflow
- [Sensor Testing Patterns](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#testing) - Testing strategies
- [Mock Frameworks](https://docs.python.org/3/library/unittest.mock.html) - Mocking external dependencies

### Development Tools

- [Airflow CLI](https://airflow.apache.org/docs/apache-airflow/stable/cli-and-env-variables-ref.html) - Command line tools
- [VS Code Extensions](https://marketplace.visualstudio.com/items?itemName=ms-python.python) - Development environment
- [Docker Compose](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html) - Local development setup

## Community Resources

### Forums and Discussion

- [Apache Airflow Slack](https://apache-airflow-slack.herokuapp.com/) - Community chat
- [Stack Overflow](https://stackoverflow.com/questions/tagged/airflow) - Q&A platform
- [Reddit r/apacheairflow](https://www.reddit.com/r/apacheairflow/) - Community discussions

### Blogs and Articles

- [Astronomer Blog](https://www.astronomer.io/blog/) - Airflow best practices and tutorials
- [Marc Lamberti's Blog](https://marclamberti.com/blog/) - Comprehensive Airflow guides
- [Apache Airflow Blog](https://airflow.apache.org/blog/) - Official announcements and guides

### Video Content

- [Airflow Summit Talks](https://www.youtube.com/c/ApacheAirflow) - Conference presentations
- [Astronomer Webinars](https://www.astronomer.io/events/) - Regular training sessions
- [YouTube Tutorials](https://www.youtube.com/results?search_query=apache+airflow+sensors) - Community tutorials

## Books and Courses

### Books

- [Data Pipelines with Apache Airflow](https://www.manning.com/books/data-pipelines-with-apache-airflow) - Comprehensive guide
- [Apache Airflow: The Definitive Guide](https://www.oreilly.com/library/view/apache-airflow-the/9781492032946/) - In-depth coverage

### Online Courses

- [Astronomer Academy](https://academy.astronomer.io/) - Official training
- [Udemy Airflow Courses](https://www.udemy.com/topic/apache-airflow/) - Various skill levels
- [Coursera Data Engineering](https://www.coursera.org/specializations/data-engineering-gcp) - Including Airflow modules

## Troubleshooting Resources

### Common Issues

- [Sensor Troubleshooting Guide](https://airflow.apache.org/docs/apache-airflow/stable/troubleshooting.html) - Official troubleshooting
- [Performance Issues](https://medium.com/apache-airflow/airflow-performance-troubleshooting-12345) - Performance debugging
- [Memory Management](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#memory-usage) - Resource optimization

### Debug Tools

- [Airflow Logs](https://airflow.apache.org/docs/apache-airflow/stable/logging-monitoring/logging-tasks.html) - Log analysis
- [Task Instance Details](https://airflow.apache.org/docs/apache-airflow/stable/ui.html#task-instance-details) - UI debugging
- [Database Queries](https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html) - Direct database inspection

## Next Steps

After completing this module, consider exploring:

1. **Module 6: Data Passing & XComs** - Learn how sensors can pass data to downstream tasks
2. **Module 7: Branching & Conditionals** - Use sensor results to control workflow paths
3. **Module 8: Error Handling** - Advanced error handling patterns for sensors
4. **Real-world Projects** - Apply sensor knowledge to complete data pipeline projects

## Contributing

Found a useful resource not listed here? Consider contributing to this kata by:

1. Opening an issue with the resource details
2. Submitting a pull request with the addition
3. Sharing your experience in the community forums

---

_Last updated: January 2024_
_Module maintainer: Airflow Kata Team_
