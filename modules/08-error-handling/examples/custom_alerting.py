"""
Error Handling Examples: Custom Alerting Mechanisms

This module demonstrates how to implement custom alerting mechanisms
for Airflow workflows, including multi-channel notifications and
intelligent alert routing.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.models import Variable
import logging
import json
import requests
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, Any, List
import hashlib
import time

# Configure logging
logger = logging.getLogger(__name__)


class AlertManager:
    """
    Centralized alert management system for Airflow workflows.
    Handles multiple notification channels and alert routing.
    """

    def __init__(self):
        self.alert_history = {}
        self.alert_thresholds = {
            'critical': 0,      # Always alert
            'high': 300,        # Alert if not sent in last 5 minutes
            'medium': 900,      # Alert if not sent in last 15 minutes
            'low': 3600         # Alert if not sent in last hour
        }

    def should_send_alert(self, alert_key: str, severity: str) -> bool:
        """
        Determine if an alert should be sent based on throttling rules.
        """
        current_time = time.time()
        last_sent = self.alert_history.get(alert_key, 0)
        threshold = self.alert_thresholds.get(severity, 0)

        if current_time - last_sent > threshold:
            self.alert_history[alert_key] = current_time
            return True
        return False

    def send_multi_channel_alert(self, alert_data: Dict[str, Any]) -> None:
        """
        Send alerts through multiple channels based on severity.
        """
        severity = alert_data.get('severity', 'medium')
        alert_key = self.generate_alert_key(alert_data)

        if not self.should_send_alert(alert_key, severity):
            logger.info(f"Alert throttled: {alert_key}")
            return

        # Route alerts based on severity
        if severity == 'critical':
            self.send_pagerduty_alert(alert_data)
            self.send_slack_alert(alert_data)
            self.send_email_alert(alert_data)
        elif severity == 'high':
            self.send_slack_alert(alert_data)
            self.send_email_alert(alert_data)
        elif severity == 'medium':
            self.send_slack_alert(alert_data)
        else:  # low
            self.log_alert(alert_data)

    def generate_alert_key(self, alert_data: Dict[str, Any]) -> str:
        """Generate unique key for alert throttling."""
        key_components = [
            alert_data.get('dag_id', ''),
            alert_data.get('task_id', ''),
            alert_data.get('alert_type', ''),
            alert_data.get('severity', '')
        ]
        key_string = '|'.join(key_components)
        return hashlib.md5(key_string.encode()).hexdigest()

    def send_pagerduty_alert(self, alert_data: Dict[str, Any]) -> None:
        """Send alert to PagerDuty for critical issues."""
        pagerduty_payload = {
            "routing_key": "YOUR_PAGERDUTY_INTEGRATION_KEY",
            "event_action": "trigger",
            "payload": {
                "summary": f"Critical Airflow Alert: {alert_data.get('title', 'Unknown Issue')}",
                "source": f"{alert_data.get('dag_id')}.{alert_data.get('task_id')}",
                "severity": "critical",
                "component": "airflow",
                "group": "data-engineering",
                "class": alert_data.get('alert_type', 'task_failure'),
                "custom_details": alert_data
            }
        }

        logger.error(
            f"PAGERDUTY ALERT: {json.dumps(pagerduty_payload, indent=2)}")

        # In production, send to actual PagerDuty:
        # response = requests.post(
        #     "https://events.pagerduty.com/v2/enqueue",
        #     json=pagerduty_payload,
        #     timeout=10
        # )

    def send_slack_alert(self, alert_data: Dict[str, Any]) -> None:
        """Send formatted alert to Slack."""
        severity = alert_data.get('severity', 'medium')
        color_map = {
            'critical': 'danger',
            'high': 'warning',
            'medium': '#ffcc00',
            'low': 'good'
        }

        emoji_map = {
            'critical': '🚨',
            'high': '⚠️',
            'medium': '⚡',
            'low': 'ℹ️'
        }

        slack_payload = {
            "text": f"{emoji_map.get(severity, '📢')} Airflow Alert",
            "attachments": [
                {
                    "color": color_map.get(severity, 'good'),
                    "title": alert_data.get('title', 'Airflow Notification'),
                    "fields": [
                        {"title": "Severity", "value": severity.upper(),
                         "short": True},
                        {"title": "DAG", "value": alert_data.get(
                            'dag_id', 'N/A'), "short": True},
                        {"title": "Task", "value": alert_data.get(
                            'task_id', 'N/A'), "short": True},
                        {"title": "Alert Type", "value": alert_data.get(
                            'alert_type', 'N/A'), "short": True},
                        {"title": "Message", "value": alert_data.get(
                            'message', 'No details available'), "short": False}
                    ],
                    "footer": "Airflow Monitoring",
                    "ts": int(time.time())
                }
            ]
        }

        logger.error(f"SLACK ALERT: {json.dumps(slack_payload, indent=2)}")

    def send_email_alert(self, alert_data: Dict[str, Any]) -> None:
        """Send detailed email alert."""
        severity = alert_data.get('severity', 'medium')

        # Determine recipient based on severity
        recipients = {
            'critical': ['oncall@company.com', 'engineering-leads@company.com'],
            'high': ['data-team@company.com', 'oncall@company.com'],
            'medium': ['data-team@company.com'],
            'low': ['data-team@company.com']
        }

        email_content = f"""
        <html>
        <body>
        <h2>Airflow Alert - {severity.upper()}</h2>
        
        <h3>Alert Details</h3>
        <ul>
            <li><strong>DAG:</strong> {alert_data.get('dag_id', 'N/A')}</li>
            <li><strong>Task:</strong> {alert_data.get('task_id', 'N/A')}</li>
            <li><strong>Alert Type:</strong> {alert_data.get('alert_type', 'N/A')}</li>
            <li><strong>Timestamp:</strong> {alert_data.get('timestamp', 'N/A')}</li>
        </ul>
        
        <h3>Message</h3>
        <p>{alert_data.get('message', 'No details available')}</p>
        
        <h3>Additional Context</h3>
        <pre>{json.dumps(alert_data.get('context', {}), indent=2)}</pre>
        
        <h3>Actions</h3>
        <ul>
            <li><a href="http://localhost:8080/admin/airflow/graph?dag_id={alert_data.get('dag_id', '')}">View DAG</a></li>
            <li><a href="http://localhost:8080/admin/airflow/log?dag_id={alert_data.get('dag_id', '')}&task_id={alert_data.get('task_id', '')}">View Logs</a></li>
        </ul>
        
        <p><em>This is an automated alert from Airflow Monitoring System</em></p>
        </body>
        </html>
        """

        logger.error(f"EMAIL ALERT SENT TO: {recipients.get(severity, [])}")
        logger.error(f"EMAIL CONTENT: {email_content}")

    def log_alert(self, alert_data: Dict[str, Any]) -> None:
        """Log alert for low-priority notifications."""
        logger.info(f"LOW_PRIORITY_ALERT: {json.dumps(alert_data, indent=2)}")


# Global alert manager instance
alert_manager = AlertManager()


def custom_failure_callback(context: Dict[str, Any]) -> None:
    """
    Enhanced failure callback with intelligent alerting.
    """
    task_instance = context['task_instance']
    exception = context.get('exception')

    # Analyze error to determine severity
    severity = determine_error_severity(exception, task_instance)

    alert_data = {
        'title': f'Task Failure: {context["task"].task_id}',
        'dag_id': context['dag'].dag_id,
        'task_id': context['task'].task_id,
        'alert_type': 'task_failure',
        'severity': severity,
        'timestamp': datetime.now().isoformat(),
        'message': f'Task failed with error: {str(exception)}',
        'context': {
            'try_number': task_instance.try_number,
            'max_tries': task_instance.max_tries,
            'duration': str(task_instance.duration) if task_instance.duration else None,
            'error_type': type(exception).__name__ if exception else 'Unknown'
        }
    }

    alert_manager.send_multi_channel_alert(alert_data)


def determine_error_severity(exception, task_instance) -> str:
    """
    Determine alert severity based on error type and context.
    """
    if not exception:
        return 'low'

    error_message = str(exception).lower()
    error_type = type(exception).__name__.lower()

    # Critical errors
    if any(keyword in error_message for keyword in ['critical', 'fatal', 'corruption']):
        return 'critical'

    # High priority errors
    if any(keyword in error_message for keyword in ['permission', 'authentication', 'authorization']):
        return 'high'

    # Check if this is a final failure (no more retries)
    if task_instance.try_number >= task_instance.max_tries:
        return 'high'

    # Medium priority for transient errors
    if any(keyword in error_message for keyword in ['timeout', 'connection', 'network']):
        return 'medium'

    return 'medium'  # Default severity


def performance_monitoring_callback(context: Dict[str, Any]) -> None:
    """
    Monitor task performance and send alerts for anomalies.
    """
    task_instance = context['task_instance']

    if task_instance.duration:
        duration_seconds = task_instance.duration.total_seconds()

        # Define performance thresholds (in production, these would be dynamic)
        performance_thresholds = {
            'critical': 1800,  # 30 minutes
            'high': 900,       # 15 minutes
            'medium': 300      # 5 minutes
        }

        severity = None
        for level, threshold in performance_thresholds.items():
            if duration_seconds > threshold:
                severity = level
                break

        if severity:
            alert_data = {
                'title': f'Performance Alert: {context["task"].task_id}',
                'dag_id': context['dag'].dag_id,
                'task_id': context['task'].task_id,
                'alert_type': 'performance_degradation',
                'severity': severity,
                'timestamp': datetime.now().isoformat(),
                'message': f'Task took {duration_seconds:.2f} seconds to complete',
                'context': {
                    'duration_seconds': duration_seconds,
                    'threshold_exceeded': performance_thresholds[severity],
                    'performance_impact': 'high' if duration_seconds > 1800 else 'medium'
                }
            }

            alert_manager.send_multi_channel_alert(alert_data)


def data_quality_alert_callback(context: Dict[str, Any]) -> None:
    """
    Send alerts for data quality issues.
    """
    # This would typically be called from within a task that detects data quality issues
    alert_data = {
        'title': f'Data Quality Alert: {context["task"].task_id}',
        'dag_id': context['dag'].dag_id,
        'task_id': context['task'].task_id,
        'alert_type': 'data_quality',
        'severity': 'high',
        'timestamp': datetime.now().isoformat(),
        'message': 'Data quality check failed - manual review required',
        'context': {
            'quality_check': 'failed',
            'impact': 'downstream_tasks_affected'
        }
    }

    alert_manager.send_multi_channel_alert(alert_data)


# DAG configuration
default_args = {
    'owner': 'airflow-kata',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'custom_alerting_examples',
    default_args=default_args,
    description='Examples of custom alerting mechanisms',
    schedule_interval=None,
    catchup=False,
    tags=['error-handling', 'alerting', 'monitoring', 'examples']
)

# Task functions for demonstration


def critical_failure_task(**context):
    """Task that simulates a critical failure."""
    logger.info("Simulating critical system failure")
    raise AirflowException(
        "CRITICAL: Database corruption detected - immediate attention required")


def high_priority_failure_task(**context):
    """Task that simulates a high priority failure."""
    logger.info("Simulating high priority failure")
    raise AirflowException(
        "Permission denied: Unable to access required data source")


def medium_priority_failure_task(**context):
    """Task that simulates a medium priority failure."""
    logger.info("Simulating medium priority failure")
    raise AirflowException("Connection timeout: External API not responding")


def slow_performance_task(**context):
    """Task that simulates slow performance."""
    logger.info("Simulating slow task performance")
    time.sleep(10)  # Simulate slow processing
    return "Task completed slowly"


def data_quality_check_task(**context):
    """Task that simulates data quality issues."""
    logger.info("Running data quality checks")

    # Simulate data quality check failure
    quality_score = 0.65  # Below acceptable threshold

    if quality_score < 0.8:
        # Trigger data quality alert
        data_quality_alert_callback(context)
        raise AirflowException(
            f"Data quality check failed: score {quality_score} below threshold 0.8")

    return f"Data quality check passed: score {quality_score}"


def monitoring_health_check(**context):
    """Task that monitors overall system health."""
    logger.info("Performing system health check")

    # Simulate various health metrics
    health_metrics = {
        'cpu_usage': 85,
        'memory_usage': 78,
        'disk_usage': 92,
        'active_connections': 150
    }

    alerts_triggered = []

    # Check thresholds and trigger alerts
    if health_metrics['cpu_usage'] > 80:
        alerts_triggered.append('high_cpu_usage')

    if health_metrics['disk_usage'] > 90:
        alerts_triggered.append('high_disk_usage')

    if alerts_triggered:
        alert_data = {
            'title': 'System Health Alert',
            'dag_id': context['dag'].dag_id,
            'task_id': context['task'].task_id,
            'alert_type': 'system_health',
            'severity': 'high' if 'high_disk_usage' in alerts_triggered else 'medium',
            'timestamp': datetime.now().isoformat(),
            'message': f'System health issues detected: {", ".join(alerts_triggered)}',
            'context': {
                'health_metrics': health_metrics,
                'alerts_triggered': alerts_triggered
            }
        }

        alert_manager.send_multi_channel_alert(alert_data)

    return health_metrics

# Configure tasks with custom alerting


critical_task = PythonOperator(
    task_id='critical_failure_example',
    python_callable=critical_failure_task,
    on_failure_callback=custom_failure_callback,
    retries=0,  # No retries for critical failures
    dag=dag
)

high_priority_task = PythonOperator(
    task_id='high_priority_failure_example',
    python_callable=high_priority_failure_task,
    on_failure_callback=custom_failure_callback,
    retries=1,
    dag=dag
)

medium_priority_task = PythonOperator(
    task_id='medium_priority_failure_example',
    python_callable=medium_priority_failure_task,
    on_failure_callback=custom_failure_callback,
    dag=dag
)

performance_task = PythonOperator(
    task_id='slow_performance_example',
    python_callable=slow_performance_task,
    on_success_callback=performance_monitoring_callback,
    dag=dag
)

data_quality_task = PythonOperator(
    task_id='data_quality_check_example',
    python_callable=data_quality_check_task,
    on_failure_callback=custom_failure_callback,
    dag=dag
)

health_check_task = PythonOperator(
    task_id='system_health_check',
    python_callable=monitoring_health_check,
    dag=dag
)

# Set up task dependencies
critical_task >> high_priority_task >> medium_priority_task
performance_task >> data_quality_task >> health_check_task
