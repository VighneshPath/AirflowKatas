# Exercise 4: Custom Alerting Systems

## Objective

Learn to build custom alerting systems that intelligently route notifications based on error severity, implement alert throttling, and integrate with multiple notification channels.

## Background

Production Airflow deployments require sophisticated alerting systems that can:

- Route alerts to appropriate teams based on severity and context
- Prevent alert fatigue through intelligent throttling
- Integrate with multiple notification channels (email, Slack, PagerDuty)
- Provide actionable information for quick resolution
- Track alert patterns for system improvement

## Tasks

### Task 4.1: Multi-Channel Alert Router

Create a DAG called `custom_alerting_dag` with an intelligent alert routing system:

1. **Alert Manager Class**: Implement a centralized alert management system

   - Support multiple notification channels (email, Slack, PagerDuty)
   - Route alerts based on severity levels (critical, high, medium, low)
   - Implement alert throttling to prevent spam
   - Track alert history and patterns

2. **Severity Classification**: Create intelligent error severity classification
   - Analyze error types and messages to determine severity
   - Consider task context (retry attempts, dependencies, business impact)
   - Implement custom severity rules for different task types
   - Log classification decisions for analysis

### Task 4.2: Alert Throttling and Deduplication

Implement smart alert throttling mechanisms:

1. **Time-Based Throttling**: Prevent duplicate alerts within time windows

   - Different throttling periods for different severity levels
   - Critical: No throttling (always alert)
   - High: 5-minute throttling window
   - Medium: 15-minute throttling window
   - Low: 1-hour throttling window

2. **Content-Based Deduplication**: Group similar alerts together
   - Generate alert fingerprints based on error type and context
   - Combine multiple similar alerts into summary notifications
   - Track alert frequency and patterns
   - Implement escalation for repeated issues

### Task 4.3: Rich Notification Formatting

Create rich, actionable notifications:

1. **Slack Integration**: Implement formatted Slack notifications

   - Use Slack blocks and attachments for rich formatting
   - Include action buttons for common responses
   - Add contextual information (logs, graphs, runbook links)
   - Support different message formats for different alert types

2. **Email Templates**: Create HTML email templates
   - Professional formatting with company branding
   - Include relevant context and debugging information
   - Add direct links to Airflow UI, logs, and documentation
   - Support both HTML and plain text formats

### Task 4.4: Integration with External Systems

Integrate with external monitoring and incident management systems:

1. **PagerDuty Integration**: Implement PagerDuty incident creation

   - Create incidents for critical alerts
   - Include relevant context and debugging information
   - Support incident escalation and acknowledgment
   - Track incident resolution times

2. **Metrics Integration**: Send alert metrics to monitoring systems
   - Track alert frequency and patterns
   - Monitor alert resolution times
   - Identify trending issues before they become critical
   - Generate dashboards for alert analytics

## Implementation Guidelines

### Alert Manager Structure

```python
class AlertManager:
    def __init__(self):
        self.alert_history = {}
        self.alert_thresholds = {
            'critical': 0,      # Always alert
            'high': 300,        # 5 minutes
            'medium': 900,      # 15 minutes
            'low': 3600         # 1 hour
        }

    def send_alert(self, alert_data):
        # Implement alert routing logic
        pass

    def should_throttle(self, alert_key, severity):
        # Implement throttling logic
        pass
```

### Severity Classification

```python
def classify_error_severity(exception, context):
    """
    Classify error severity based on multiple factors.
    """
    # Analyze error message and type
    # Consider task context and business impact
    # Return severity level
    pass
```

### Notification Templates

```python
def format_slack_alert(alert_data):
    """
    Format alert for Slack with rich formatting.
    """
    return {
        "text": "Alert notification",
        "attachments": [
            {
                "color": "danger",
                "fields": [
                    # Alert details
                ]
            }
        ]
    }
```

## Expected Outcomes

After completing this exercise, you should have:

1. A centralized alert management system
2. Intelligent severity classification
3. Multi-channel notification routing
4. Alert throttling and deduplication
5. Rich notification formatting
6. External system integrations

## Testing Your Implementation

1. **Trigger different alert types** and verify routing
2. **Test throttling behavior** with repeated alerts
3. **Verify notification formatting** across channels
4. **Check external integrations** (PagerDuty, metrics)
5. **Validate alert deduplication** logic

## Alert Routing Matrix

| Severity | Email | Slack | PagerDuty | Throttling |
| -------- | ----- | ----- | --------- | ---------- |
| Critical | ✓     | ✓     | ✓         | None       |
| High     | ✓     | ✓     | ✗         | 5 min      |
| Medium   | ✗     | ✓     | ✗         | 15 min     |
| Low      | ✗     | ✗     | ✗         | 1 hour     |

## Best Practices

1. **Keep alerts actionable** - include specific steps to resolve
2. **Avoid alert fatigue** - use appropriate throttling
3. **Test notification channels** - ensure delivery works
4. **Monitor alert patterns** - identify systemic issues
5. **Document escalation procedures** - clear ownership

## Bonus Challenges

1. **Dynamic Severity**: Adjust severity based on time of day/business hours
2. **Alert Correlation**: Group related alerts across multiple DAGs
3. **Auto-Resolution**: Automatically resolve alerts when issues are fixed
4. **Alert Analytics**: Build dashboards showing alert trends
5. **Chatbot Integration**: Create Slack bot for alert management

## Common Pitfalls

- Over-alerting on transient issues
- Not providing enough context in alerts
- Failing to test notification delivery
- Not considering alert fatigue
- Missing escalation procedures for critical issues

## Integration Examples

### Slack Webhook

```python
import requests

def send_slack_alert(webhook_url, message):
    response = requests.post(webhook_url, json=message)
    response.raise_for_status()
```

### PagerDuty API

```python
def create_pagerduty_incident(routing_key, alert_data):
    payload = {
        "routing_key": routing_key,
        "event_action": "trigger",
        "payload": {
            "summary": alert_data['title'],
            "source": alert_data['source'],
            "severity": "critical"
        }
    }
    # Send to PagerDuty Events API
```

## Next Steps

Once you complete this exercise, you'll be ready to move on to Exercise 5: Workflow Health Monitoring, where you'll learn to build comprehensive monitoring systems that proactively identify issues before they cause failures.
