# Exercise 5: Workflow Health Monitoring

## Objective

Learn to build comprehensive workflow health monitoring systems that proactively identify issues, track performance trends, and provide actionable insights for maintaining healthy data pipelines.

## Background

Proactive monitoring is essential for maintaining reliable data pipelines. Rather than just reacting to failures, effective monitoring systems:

- Track performance trends to identify degradation before failures occur
- Monitor system health metrics to predict capacity issues
- Analyze failure patterns to identify systemic problems
- Provide actionable insights for optimization and maintenance
- Generate comprehensive health reports for stakeholders

## Tasks

### Task 5.1: Performance Metrics Collection

Create a DAG called `health_monitoring_dag` with comprehensive metrics collection:

1. **Task-Level Metrics**: Collect detailed task performance data

   - Execution duration and resource usage
   - Success/failure rates over time
   - Queue wait times and scheduling delays
   - Memory and CPU utilization patterns

2. **DAG-Level Metrics**: Track overall workflow health

   - End-to-end execution times
   - Dependency chain performance
   - Critical path analysis
   - Overall success rates and reliability metrics

3. **System-Level Metrics**: Monitor Airflow infrastructure health
   - Scheduler performance and lag
   - Worker capacity and utilization
   - Database connection health
   - Queue depths and processing rates

### Task 5.2: Trend Analysis and Anomaly Detection

Implement intelligent trend analysis:

1. **Performance Baselines**: Establish performance baselines

   - Calculate rolling averages for key metrics
   - Identify seasonal patterns and normal variations
   - Set dynamic thresholds based on historical data
   - Track performance against SLA requirements

2. **Anomaly Detection**: Detect performance anomalies
   - Identify tasks running significantly slower than normal
   - Detect unusual failure patterns or spikes
   - Flag capacity issues before they cause problems
   - Alert on trending degradation before SLA violations

### Task 5.3: Health Scoring and Reporting

Create comprehensive health scoring system:

1. **Health Score Calculation**: Develop composite health scores

   - Weight different metrics based on business impact
   - Calculate DAG-level and system-level health scores
   - Track health score trends over time
   - Set thresholds for different health levels

2. **Automated Health Reports**: Generate regular health reports
   - Daily/weekly/monthly health summaries
   - Trend analysis and performance insights
   - Recommendations for optimization
   - Executive dashboards with key metrics

### Task 5.4: Proactive Alerting and Recommendations

Implement proactive alerting based on health trends:

1. **Predictive Alerts**: Alert before problems occur

   - Warn when performance trends indicate future SLA violations
   - Alert on capacity trends that may cause resource exhaustion
   - Notify when failure patterns suggest systemic issues
   - Escalate based on business impact and urgency

2. **Actionable Recommendations**: Provide specific improvement suggestions
   - Identify optimization opportunities
   - Suggest resource scaling recommendations
   - Highlight tasks that need attention
   - Provide links to relevant documentation and runbooks

## Implementation Guidelines

### Health Monitor Class Structure

```python
class WorkflowHealthMonitor:
    def __init__(self):
        self.metrics_history = defaultdict(deque)
        self.performance_baselines = {}
        self.health_thresholds = {}

    def collect_metrics(self, dag_id):
        # Collect comprehensive metrics
        pass

    def analyze_trends(self, dag_id):
        # Analyze performance trends
        pass

    def calculate_health_score(self, metrics):
        # Calculate composite health score
        pass

    def generate_recommendations(self, analysis):
        # Generate actionable recommendations
        pass
```

### Metrics Collection

```python
def collect_task_metrics(context):
    """
    Collect detailed task metrics.
    """
    task_instance = context['task_instance']

    metrics = {
        'duration': task_instance.duration.total_seconds(),
        'memory_usage': get_memory_usage(),
        'cpu_usage': get_cpu_usage(),
        'queue_time': calculate_queue_time(task_instance),
        'retry_count': task_instance.try_number - 1
    }

    return metrics
```

### Health Score Calculation

```python
def calculate_health_score(metrics):
    """
    Calculate composite health score (0-100).
    """
    # Success rate component (40%)
    success_component = metrics['success_rate'] * 40

    # Performance component (35%)
    performance_component = calculate_performance_score(metrics) * 35

    # Reliability component (25%)
    reliability_component = calculate_reliability_score(metrics) * 25

    return success_component + performance_component + reliability_component
```

## Expected Outcomes

After completing this exercise, you should have:

1. Comprehensive metrics collection system
2. Trend analysis and anomaly detection
3. Health scoring methodology
4. Automated health reporting
5. Proactive alerting system
6. Actionable recommendations engine

## Testing Your Implementation

1. **Run monitoring tasks** and verify metrics collection
2. **Generate test data** with various performance patterns
3. **Validate trend analysis** with known performance changes
4. **Test health scoring** with different scenarios
5. **Verify alert generation** for various conditions

## Health Metrics Categories

### Performance Metrics

- Task execution duration
- Queue wait times
- Resource utilization
- Throughput rates

### Reliability Metrics

- Success/failure rates
- Retry patterns
- SLA compliance
- Error frequencies

### Capacity Metrics

- Worker utilization
- Queue depths
- Memory usage
- Storage consumption

### Business Metrics

- Data freshness
- Processing volumes
- Business SLA compliance
- Cost efficiency

## Health Score Components

| Component    | Weight | Description                    |
| ------------ | ------ | ------------------------------ |
| Success Rate | 40%    | Task/DAG success percentage    |
| Performance  | 35%    | Speed vs baseline/SLA          |
| Reliability  | 25%    | Consistency and predictability |

## Alerting Thresholds

| Health Score | Status    | Action              |
| ------------ | --------- | ------------------- |
| 90-100       | Excellent | Monitor             |
| 80-89        | Good      | Track trends        |
| 70-79        | Fair      | Investigate         |
| 60-69        | Poor      | Take action         |
| <60          | Critical  | Immediate attention |

## Best Practices

1. **Establish baselines** before implementing alerting
2. **Use rolling windows** for trend analysis
3. **Weight metrics** by business impact
4. **Provide context** in all alerts and reports
5. **Make recommendations** specific and actionable

## Bonus Challenges

1. **Machine Learning**: Use ML for anomaly detection
2. **Predictive Analytics**: Forecast future performance issues
3. **Cost Optimization**: Include cost metrics in health scoring
4. **Cross-DAG Analysis**: Analyze dependencies between DAGs
5. **Real-time Dashboards**: Build live monitoring dashboards

## Common Pitfalls

- Collecting too many metrics without clear purpose
- Setting static thresholds that don't adapt to patterns
- Not considering business context in health scoring
- Generating alerts without actionable recommendations
- Focusing on symptoms rather than root causes

## Integration with External Systems

### Metrics Storage

```python
# Example: Send metrics to time-series database
def send_metrics_to_influxdb(metrics):
    from influxdb import InfluxDBClient

    client = InfluxDBClient(host='localhost', port=8086)
    points = [
        {
            "measurement": "airflow_metrics",
            "tags": {
                "dag_id": metrics['dag_id'],
                "task_id": metrics['task_id']
            },
            "fields": {
                "duration": metrics['duration'],
                "success_rate": metrics['success_rate']
            }
        }
    ]
    client.write_points(points)
```

### Dashboard Integration

```python
# Example: Update Grafana dashboard
def update_grafana_dashboard(health_data):
    # Update dashboard with latest health metrics
    pass
```

## Monitoring Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Airflow       │    │   Health         │    │   Alerting      │
│   Tasks         │───▶│   Monitor        │───▶│   System        │
│                 │    │                  │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │   Metrics        │
                       │   Storage        │
                       │   (InfluxDB)     │
                       └──────────────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │   Dashboards     │
                       │   (Grafana)      │
                       └──────────────────┘
```

## Next Steps

Once you complete this exercise, you'll have built a comprehensive monitoring system that can proactively identify and prevent issues in your Airflow workflows. This completes the error handling and monitoring module, giving you the tools needed to build production-ready, reliable data pipelines.
