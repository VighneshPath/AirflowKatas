"""
Solution: Exercise 4 - Custom Alerting Systems

This solution demonstrates a comprehensive custom alerting system with
intelligent routing, throttling, and multi-channel notifications.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
import logging
import json
import hashlib
import time
import requests
from typing import Dict, Any, List, Optional
from collections import defaultdict
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib

# Configure logging
logger = logging.getLogger(__name__)


class AlertManager:
    """
    Comprehensive alert management system with intelligent routing,
    throttling, and multi-channel notifications.
    """

    def __init__(self):
        self.alert_history = {}
        self.alert_patterns = defaultdict(list)
        self.alert_thresholds = {
            'critical': 0,      # Always alert
            'high': 300,        # 5 minutes
            'medium': 900,      # 15 minutes
            'low': 3600         # 1 hour
        }
        self.notification_channels = {
            'critical': ['email', 'slack', 'pagerduty'],
            'high': ['email', 'slack'],
            'medium': ['slack'],
            'low': ['log']
        }

    def send_alert(self, alert_data: Dict[str, Any]) -> None:
        """
        Main alert routing method with intelligent processing.
        """
        # Generate alert fingerprint for deduplication
        alert_key = self._generate_alert_key(alert_data)
        severity = alert_data.get('severity', 'medium')

        # Check if alert should be throttled
        if self._should_throttle_alert(alert_key, severity):
            logger.info(f"Alert throttled: {alert_key}")
            return

        # Enhance alert with additional context
        enhanced_alert = self._enhance_alert_data(alert_data)

        # Route to appropriate channels based on severity
        channels = self.notification_channels.get(severity, ['log'])

        for channel in channels:
            try:
                self._send_to_channel(channel, enhanced_alert)
            except Exception as e:
                logger.error(f"Failed to send alert to {channel}: {e}")

        # Track alert for pattern analysis
        self._track_alert_pattern(alert_key, enhanced_alert)

    def _generate_alert_key(self, alert_data: Dict[str, Any]) -> str:
        """Generate unique key for alert deduplication."""
        key_components = [
            alert_data.get('dag_id', ''),
            alert_data.get('task_id', ''),
            alert_data.get('alert_type', ''),
            alert_data.get('error_type', ''),
            str(alert_data.get('severity', ''))
        ]
        key_string = '|'.join(key_components)
        return hashlib.md5(key_string.encode()).hexdigest()[:12]

    def _should_throttle_alert(self, alert_key: str, severity: str) -> bool:
        """Determine if alert should be throttled based on history."""
        current_time = time.time()
        threshold = self.alert_thresholds.get(severity, 0)

        if alert_key in self.alert_history:
            last_sent = self.alert_history[alert_key]['last_sent']
            if current_time - last_sent < threshold:
                # Update throttle count
                self.alert_history[alert_key]['throttle_count'] += 1
                return True

        # Record alert sending
        self.alert_history[alert_key] = {
            'last_sent': current_time,
            'throttle_count': 0,
            'total_count': self.alert_history.get(alert_key, {}).get('total_count', 0) + 1
        }

        return False

    def _enhance_alert_data(self, alert_data: Dict[str, Any]) -> Dict[str, Any]:
        """Enhance alert with additional context and metadata."""
        enhanced = alert_data.copy()

        # Add timestamp if not present
        if 'timestamp' not in enhanced:
            enhanced['timestamp'] = datetime.now().isoformat()

        # Add alert ID for tracking
        enhanced['alert_id'] = self._generate_alert_key(alert_data)

        # Add runbook links based on alert type
        enhanced['runbook_url'] = self._get_runbook_url(alert_data)

        # Add related alerts if any
        enhanced['related_alerts'] = self._find_related_alerts(alert_data)

        # Add escalation information
        enhanced['escalation_info'] = self._get_escalation_info(alert_data)

        return enhanced

    def _get_runbook_url(self, alert_data: Dict[str, Any]) -> str:
        """Get relevant runbook URL based on alert type."""
        alert_type = alert_data.get('alert_type', 'general')
        runbook_map = {
            'task_failure': 'https://wiki.company.com/runbooks/airflow-task-failures',
            'performance_degradation': 'https://wiki.company.com/runbooks/performance-issues',
            'sla_miss': 'https://wiki.company.com/runbooks/sla-violations',
            'system_health': 'https://wiki.company.com/runbooks/system-health',
            'data_quality': 'https://wiki.company.com/runbooks/data-quality'
        }
        return runbook_map.get(alert_type, 'https://wiki.company.com/runbooks/general')

    def _find_related_alerts(self, alert_data: Dict[str, Any]) -> List[str]:
        """Find related alerts that might be connected to this issue."""
        # In a real implementation, this would query alert history
        # For demo purposes, return empty list
        return []

    def _get_escalation_info(self, alert_data: Dict[str, Any]) -> Dict[str, Any]:
        """Get escalation information based on alert severity and type."""
        severity = alert_data.get('severity', 'medium')

        escalation_map = {
            'critical': {
                'primary_contact': 'oncall-engineer@company.com',
                'escalation_time': 15,  # minutes
                'escalation_contact': 'engineering-manager@company.com'
            },
            'high': {
                'primary_contact': 'data-team@company.com',
                'escalation_time': 60,  # minutes
                'escalation_contact': 'data-team-lead@company.com'
            },
            'medium': {
                'primary_contact': 'data-team@company.com',
                'escalation_time': 240,  # minutes
                'escalation_contact': None
            },
            'low': {
                'primary_contact': 'data-team@company.com',
                'escalation_time': None,
                'escalation_contact': None
            }
        }

        return escalation_map.get(severity, escalation_map['medium'])

    def _send_to_channel(self, channel: str, alert_data: Dict[str, Any]) -> None:
        """Send alert to specific notification channel."""
        if channel == 'email':
            self._send_email_alert(alert_data)
        elif channel == 'slack':
            self._send_slack_alert(alert_data)
        elif channel == 'pagerduty':
            self._send_pagerduty_alert(alert_data)
        elif channel == 'log':
            self._log_alert(alert_data)
        else:
            logger.warning(f"Unknown notification channel: {channel}")

    def _send_email_alert(self, alert_data: Dict[str, Any]) -> None:
        """Send formatted email alert."""
        severity = alert_data.get('severity', 'medium')

        # Determine recipients based on escalation info
        escalation_info = alert_data.get('escalation_info', {})
        recipients = [escalation_info.get(
            'primary_contact', 'data-team@company.com')]

        subject = f"🚨 Airflow Alert [{severity.upper()}]: {alert_data.get('title', 'System Alert')}"

        # Create HTML email content
        html_content = self._create_email_template(alert_data)

        # Log email (in production, send actual email)
        logger.error(f"EMAIL ALERT SENT:")
        logger.error(f"To: {recipients}")
        logger.error(f"Subject: {subject}")
        logger.error(f"Content: {html_content}")

        # In production:
        # self._send_smtp_email(recipients, subject, html_content)

    def _create_email_template(self, alert_data: Dict[str, Any]) -> str:
        """Create HTML email template."""
        severity = alert_data.get('severity', 'medium')
        severity_colors = {
            'critical': '#dc3545',
            'high': '#fd7e14',
            'medium': '#ffc107',
            'low': '#28a745'
        }

        html_template = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; }}
                .header {{ background-color: {severity_colors.get(severity, '#ffc107')}; color: white; padding: 15px; border-radius: 5px; }}
                .content {{ padding: 20px; border: 1px solid #ddd; border-radius: 5px; margin-top: 10px; }}
                .details {{ background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin: 10px 0; }}
                .actions {{ background-color: #e9ecef; padding: 15px; border-radius: 5px; margin: 10px 0; }}
                .button {{ background-color: #007bff; color: white; padding: 10px 15px; text-decoration: none; border-radius: 3px; margin: 5px; }}
                .footer {{ font-size: 12px; color: #666; margin-top: 20px; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>Airflow Alert - {severity.upper()}</h2>
                <p>{alert_data.get('title', 'System Alert')}</p>
            </div>
            
            <div class="content">
                <div class="details">
                    <h3>Alert Details</h3>
                    <ul>
                        <li><strong>DAG:</strong> {alert_data.get('dag_id', 'N/A')}</li>
                        <li><strong>Task:</strong> {alert_data.get('task_id', 'N/A')}</li>
                        <li><strong>Alert Type:</strong> {alert_data.get('alert_type', 'N/A')}</li>
                        <li><strong>Timestamp:</strong> {alert_data.get('timestamp', 'N/A')}</li>
                        <li><strong>Alert ID:</strong> {alert_data.get('alert_id', 'N/A')}</li>
                    </ul>
                </div>
                
                <div class="details">
                    <h3>Message</h3>
                    <p>{alert_data.get('message', 'No details available')}</p>
                </div>
                
                <div class="details">
                    <h3>Context</h3>
                    <pre>{json.dumps(alert_data.get('context', {}), indent=2)}</pre>
                </div>
                
                <div class="actions">
                    <h3>Quick Actions</h3>
                    <a href="http://localhost:8080/admin/airflow/graph?dag_id={alert_data.get('dag_id', '')}" class="button">View DAG</a>
                    <a href="http://localhost:8080/admin/airflow/log?dag_id={alert_data.get('dag_id', '')}&task_id={alert_data.get('task_id', '')}" class="button">View Logs</a>
                    <a href="{alert_data.get('runbook_url', '#')}" class="button">View Runbook</a>
                </div>
                
                <div class="details">
                    <h3>Escalation Information</h3>
                    <p><strong>Primary Contact:</strong> {alert_data.get('escalation_info', {}).get('primary_contact', 'N/A')}</p>
                    <p><strong>Escalation Time:</strong> {alert_data.get('escalation_info', {}).get('escalation_time', 'N/A')} minutes</p>
                </div>
            </div>
            
            <div class="footer">
                <p>This is an automated alert from the Airflow Monitoring System.</p>
                <p>Alert ID: {alert_data.get('alert_id', 'N/A')} | Generated at: {alert_data.get('timestamp', 'N/A')}</p>
            </div>
        </body>
        </html>
        """

        return html_template

    def _send_slack_alert(self, alert_data: Dict[str, Any]) -> None:
        """Send formatted Slack alert with rich formatting."""
        severity = alert_data.get('severity', 'medium')

        # Severity-based formatting
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

        # Create rich Slack message
        slack_payload = {
            "text": f"{emoji_map.get(severity, '📢')} Airflow Alert - {severity.upper()}",
            "attachments": [
                {
                    "color": color_map.get(severity, 'good'),
                    "title": alert_data.get('title', 'Airflow Alert'),
                    "title_link": f"http://localhost:8080/admin/airflow/graph?dag_id={alert_data.get('dag_id', '')}",
                    "fields": [
                        {
                            "title": "Severity",
                            "value": severity.upper(),
                            "short": True
                        },
                        {
                            "title": "DAG",
                            "value": alert_data.get('dag_id', 'N/A'),
                            "short": True
                        },
                        {
                            "title": "Task",
                            "value": alert_data.get('task_id', 'N/A'),
                            "short": True
                        },
                        {
                            "title": "Alert Type",
                            "value": alert_data.get('alert_type', 'N/A'),
                            "short": True
                        },
                        {
                            "title": "Alert ID",
                            "value": alert_data.get('alert_id', 'N/A'),
                            "short": True
                        },
                        {
                            "title": "Timestamp",
                            "value": alert_data.get('timestamp', 'N/A'),
                            "short": True
                        },
                        {
                            "title": "Message",
                            "value": alert_data.get('message', 'No details available')[:500],
                            "short": False
                        }
                    ],
                    "actions": [
                        {
                            "type": "button",
                            "text": "View DAG",
                            "url": f"http://localhost:8080/admin/airflow/graph?dag_id={alert_data.get('dag_id', '')}"
                        },
                        {
                            "type": "button",
                            "text": "View Logs",
                            "url": f"http://localhost:8080/admin/airflow/log?dag_id={alert_data.get('dag_id', '')}&task_id={alert_data.get('task_id', '')}"
                        },
                        {
                            "type": "button",
                            "text": "Runbook",
                            "url": alert_data.get('runbook_url', '#')
                        }
                    ],
                    "footer": "Airflow Monitoring System",
                    "footer_icon": "https://airflow.apache.org/docs/apache-airflow/stable/_images/pin_large.png",
                    "ts": int(time.time())
                }
            ]
        }

        # Log Slack message (in production, send to actual Slack)
        logger.error(
            f"SLACK ALERT SENT: {json.dumps(slack_payload, indent=2)}")

        # In production:
        # webhook_url = "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK"
        # requests.post(webhook_url, json=slack_payload, timeout=10)

    def _send_pagerduty_alert(self, alert_data: Dict[str, Any]) -> None:
        """Send alert to PagerDuty for critical issues."""
        pagerduty_payload = {
            "routing_key": "YOUR_PAGERDUTY_INTEGRATION_KEY",
            "event_action": "trigger",
            "dedup_key": alert_data.get('alert_id'),
            "payload": {
                "summary": f"Critical Airflow Alert: {alert_data.get('title', 'System Issue')}",
                "source": f"{alert_data.get('dag_id', 'unknown')}.{alert_data.get('task_id', 'unknown')}",
                "severity": "critical",
                "component": "airflow",
                "group": "data-engineering",
                "class": alert_data.get('alert_type', 'system_alert'),
                "custom_details": {
                    "dag_id": alert_data.get('dag_id'),
                    "task_id": alert_data.get('task_id'),
                    "alert_type": alert_data.get('alert_type'),
                    "message": alert_data.get('message'),
                    "context": alert_data.get('context', {}),
                    "runbook_url": alert_data.get('runbook_url'),
                    "airflow_url": f"http://localhost:8080/admin/airflow/graph?dag_id={alert_data.get('dag_id', '')}"
                }
            }
        }

        # Log PagerDuty alert (in production, send to actual PagerDuty)
        logger.error(
            f"PAGERDUTY ALERT SENT: {json.dumps(pagerduty_payload, indent=2)}")

        # In production:
        # response = requests.post(
        #     "https://events.pagerduty.com/v2/enqueue",
        #     json=pagerduty_payload,
        #     timeout=10
        # )

    def _log_alert(self, alert_data: Dict[str, Any]) -> None:
        """Log alert for low-priority notifications."""
        logger.info(f"LOW_PRIORITY_ALERT: {json.dumps(alert_data, indent=2)}")

    def _track_alert_pattern(self, alert_key: str, alert_data: Dict[str, Any]) -> None:
        """Track alert patterns for analysis."""
        pattern_data = {
            'timestamp': time.time(),
            'alert_key': alert_key,
            'severity': alert_data.get('severity'),
            'alert_type': alert_data.get('alert_type'),
            'dag_id': alert_data.get('dag_id'),
            'task_id': alert_data.get('task_id')
        }

        self.alert_patterns[alert_key].append(pattern_data)

        # Keep only recent patterns (last 100)
        if len(self.alert_patterns[alert_key]) > 100:
            self.alert_patterns[alert_key] = self.alert_patterns[alert_key][-100:]


def classify_error_severity(exception, context: Dict[str, Any]) -> str:
    """
    Intelligent error severity classification based on multiple factors.
    """
    if not exception:
        return 'low'

    error_message = str(exception).lower()
    error_type = type(exception).__name__.lower()
    task_instance = context.get('task_instance')

    # Critical severity indicators
    critical_keywords = [
        'critical', 'fatal', 'corruption', 'security', 'breach',
        'unauthorized', 'data loss', 'system failure'
    ]

    if any(keyword in error_message for keyword in critical_keywords):
        return 'critical'

    # High severity indicators
    high_keywords = [
        'permission', 'authentication', 'authorization', 'access denied',
        'database', 'connection pool', 'deadlock'
    ]

    if any(keyword in error_message for keyword in high_keywords):
        return 'high'

    # Check if this is a final failure (no more retries)
    if task_instance and task_instance.try_number >= task_instance.max_tries:
        return 'high'

    # Medium severity for transient errors
    medium_keywords = [
        'timeout', 'connection', 'network', 'temporary', 'retry',
        'unavailable', 'busy'
    ]

    if any(keyword in error_message for keyword in medium_keywords):
        return 'medium'

    # Check task importance (could be configured per task)
    task_id = context.get('task', {}).task_id if context.get('task') else ''
    critical_tasks = ['data_validation',
                      'financial_processing', 'security_check']

    if any(critical_task in task_id for critical_task in critical_tasks):
        return 'high'

    return 'medium'  # Default severity


# Global alert manager instance
alert_manager = AlertManager()


def enhanced_failure_callback(context: Dict[str, Any]) -> None:
    """
    Enhanced failure callback with intelligent alerting.
    """
    task_instance = context['task_instance']
    exception = context.get('exception')

    # Classify error severity
    severity = classify_error_severity(exception, context)

    # Determine alert type based on context
    alert_type = 'task_failure'
    if 'sla' in str(exception).lower():
        alert_type = 'sla_miss'
    elif 'data' in str(exception).lower() and 'quality' in str(exception).lower():
        alert_type = 'data_quality'
    elif 'performance' in str(exception).lower() or 'slow' in str(exception).lower():
        alert_type = 'performance_degradation'

    # Create comprehensive alert data
    alert_data = {
        'title': f'Task Failure: {context["task"].task_id}',
        'dag_id': context['dag'].dag_id,
        'task_id': context['task'].task_id,
        'alert_type': alert_type,
        'severity': severity,
        'message': f'Task failed with error: {str(exception)}',
        'error_type': type(exception).__name__ if exception else 'Unknown',
        'context': {
            'try_number': task_instance.try_number,
            'max_tries': task_instance.max_tries,
            'duration': str(task_instance.duration) if task_instance.duration else None,
            'start_date': task_instance.start_date.isoformat() if task_instance.start_date else None,
            'end_date': task_instance.end_date.isoformat() if task_instance.end_date else None,
            'operator': context['task'].__class__.__name__,
            'pool': getattr(context['task'], 'pool', None),
            'queue': getattr(context['task'], 'queue', None)
        }
    }

    # Send alert through alert manager
    alert_manager.send_alert(alert_data)


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
    'custom_alerting_dag',
    default_args=default_args,
    description='Solution for custom alerting systems exercise',
    schedule_interval=None,
    catchup=False,
    tags=['error-handling', 'alerting', 'exercise', 'solution']
)

# Task functions for testing different alert scenarios


def critical_system_failure(**context):
    """Simulate critical system failure."""
    logger.info("Simulating critical system failure")
    raise AirflowException(
        "CRITICAL: Database corruption detected - immediate attention required")


def authentication_failure(**context):
    """Simulate authentication failure."""
    logger.info("Simulating authentication failure")
    raise AirflowException(
        "Authentication failed: Access denied to data warehouse")


def network_timeout_failure(**context):
    """Simulate network timeout."""
    logger.info("Simulating network timeout")
    raise AirflowException(
        "Network timeout: Unable to connect to external API")


def data_quality_failure(**context):
    """Simulate data quality issue."""
    logger.info("Simulating data quality failure")
    raise AirflowException(
        "Data quality check failed: 15% of records contain invalid data")


def performance_degradation(**context):
    """Simulate performance issue."""
    logger.info("Simulating performance degradation")
    time.sleep(5)  # Simulate slow processing
    raise AirflowException(
        "Performance degradation: Task taking 300% longer than baseline")


def low_priority_warning(**context):
    """Simulate low priority warning."""
    logger.info("Simulating low priority warning")
    raise AirflowException(
        "Warning: Disk usage at 75% - monitoring recommended")

# Configure tasks with enhanced alerting


critical_task = PythonOperator(
    task_id='critical_system_failure',
    python_callable=critical_system_failure,
    on_failure_callback=enhanced_failure_callback,
    retries=0,  # No retries for critical failures
    dag=dag
)

auth_task = PythonOperator(
    task_id='authentication_failure',
    python_callable=authentication_failure,
    on_failure_callback=enhanced_failure_callback,
    retries=1,
    dag=dag
)

network_task = PythonOperator(
    task_id='network_timeout_failure',
    python_callable=network_timeout_failure,
    on_failure_callback=enhanced_failure_callback,
    dag=dag
)

data_quality_task = PythonOperator(
    task_id='data_quality_failure',
    python_callable=data_quality_failure,
    on_failure_callback=enhanced_failure_callback,
    dag=dag
)

performance_task = PythonOperator(
    task_id='performance_degradation',
    python_callable=performance_degradation,
    on_failure_callback=enhanced_failure_callback,
    dag=dag
)

warning_task = PythonOperator(
    task_id='low_priority_warning',
    python_callable=low_priority_warning,
    on_failure_callback=enhanced_failure_callback,
    dag=dag
)

# Set up task dependencies to test different scenarios
critical_task >> auth_task >> network_task
data_quality_task >> performance_task >> warning_task
