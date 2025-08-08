"""
Error Handling Examples: Workflow Health Monitoring

This module demonstrates comprehensive workflow health monitoring,
including metrics collection, trend analysis, and proactive alerting.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException
from airflow.models import Variable, DagRun, TaskInstance
from airflow.utils.state import State
import logging
import json
import time
import statistics
from typing import Dict, Any, List, Optional
from collections import defaultdict, deque

# Configure logging
logger = logging.getLogger(__name__)


class WorkflowHealthMonitor:
    """
    Comprehensive workflow health monitoring system.
    Tracks performance metrics, failure patterns, and system health.
    """

    def __init__(self):
        self.metrics_history = defaultdict(lambda: deque(maxlen=100))
        self.failure_patterns = defaultdict(list)
        self.performance_baselines = {}
        self.health_thresholds = {
            'success_rate': 0.95,      # 95% success rate
            'avg_duration': 300,       # 5 minutes average
            'failure_spike': 3,        # 3 failures in short period
            'performance_degradation': 1.5  # 50% slower than baseline
        }

    def collect_dag_metrics(self, dag_id: str) -> Dict[str, Any]:
        """
        Collect comprehensive metrics for a DAG.
        """
        try:
            # In a real implementation, this would query the Airflow metadata database
            # For this example, we'll simulate metrics collection

            current_time = datetime.now()

            # Simulate metrics collection
            metrics = {
                'timestamp': current_time.isoformat(),
                'dag_id': dag_id,
                'total_runs': self._get_total_runs(dag_id),
                'successful_runs': self._get_successful_runs(dag_id),
                'failed_runs': self._get_failed_runs(dag_id),
                'avg_duration': self._get_avg_duration(dag_id),
                'last_success': self._get_last_success(dag_id),
                'last_failure': self._get_last_failure(dag_id),
                'active_tasks': self._get_active_tasks(dag_id),
                'queued_tasks': self._get_queued_tasks(dag_id)
            }

            # Calculate derived metrics
            total_runs = metrics['total_runs']
            if total_runs > 0:
                metrics['success_rate'] = metrics['successful_runs'] / total_runs
                metrics['failure_rate'] = metrics['failed_runs'] / total_runs
            else:
                metrics['success_rate'] = 1.0
                metrics['failure_rate'] = 0.0

            # Store metrics in history
            self.metrics_history[dag_id].append(metrics)

            return metrics

        except Exception as e:
            logger.error(f"Error collecting metrics for DAG {dag_id}: {e}")
            return {}

    def _get_total_runs(self, dag_id: str) -> int:
        """Simulate getting total DAG runs."""
        import random
        return random.randint(50, 200)

    def _get_successful_runs(self, dag_id: str) -> int:
        """Simulate getting successful DAG runs."""
        total = self._get_total_runs(dag_id)
        return int(total * random.uniform(0.85, 0.98))

    def _get_failed_runs(self, dag_id: str) -> int:
        """Simulate getting failed DAG runs."""
        total = self._get_total_runs(dag_id)
        successful = self._get_successful_runs(dag_id)
        return total - successful

    def _get_avg_duration(self, dag_id: str) -> float:
        """Simulate getting average DAG duration."""
        import random
        return random.uniform(120, 600)  # 2-10 minutes

    def _get_last_success(self, dag_id: str) -> str:
        """Simulate getting last successful run."""
        return (datetime.now() - timedelta(hours=random.randint(1, 24))).isoformat()

    def _get_last_failure(self, dag_id: str) -> Optional[str]:
        """Simulate getting last failure."""
        if random.random() < 0.3:  # 30% chance of recent failure
            return (datetime.now() - timedelta(hours=random.randint(1, 72))).isoformat()
        return None

    def _get_active_tasks(self, dag_id: str) -> int:
        """Simulate getting active task count."""
        return random.randint(0, 5)

    def _get_queued_tasks(self, dag_id: str) -> int:
        """Simulate getting queued task count."""
        return random.randint(0, 10)

    def analyze_health_trends(self, dag_id: str) -> Dict[str, Any]:
        """
        Analyze health trends for a DAG.
        """
        if dag_id not in self.metrics_history or len(self.metrics_history[dag_id]) < 2:
            return {'status': 'insufficient_data'}

        # Last 10 data points
        recent_metrics = list(self.metrics_history[dag_id])[-10:]

        # Calculate trends
        success_rates = [m['success_rate'] for m in recent_metrics]
        durations = [m['avg_duration'] for m in recent_metrics]
        failure_counts = [m['failed_runs'] for m in recent_metrics]

        analysis = {
            'dag_id': dag_id,
            'analysis_timestamp': datetime.now().isoformat(),
            'data_points': len(recent_metrics),
            'success_rate_trend': self._calculate_trend(success_rates),
            'duration_trend': self._calculate_trend(durations),
            'failure_trend': self._calculate_trend(failure_counts),
            'current_health_score': self._calculate_health_score(recent_metrics[-1]),
            'alerts': []
        }

        # Generate alerts based on trends
        if analysis['success_rate_trend']['direction'] == 'declining':
            analysis['alerts'].append({
                'type': 'success_rate_decline',
                'severity': 'high',
                'message': f"Success rate declining: {analysis['success_rate_trend']['change']:.2%}"
            })

        if analysis['duration_trend']['direction'] == 'increasing':
            analysis['alerts'].append({
                'type': 'performance_degradation',
                'severity': 'medium',
                'message': f"Average duration increasing: {analysis['duration_trend']['change']:.1f}s"
            })

        if recent_metrics[-1]['success_rate'] < self.health_thresholds['success_rate']:
            analysis['alerts'].append({
                'type': 'low_success_rate',
                'severity': 'critical',
                'message': f"Success rate below threshold: {recent_metrics[-1]['success_rate']:.2%}"
            })

        return analysis

    def _calculate_trend(self, values: List[float]) -> Dict[str, Any]:
        """Calculate trend direction and magnitude."""
        if len(values) < 2:
            return {'direction': 'stable', 'change': 0}

        # Simple linear trend calculation
        recent_avg = statistics.mean(
            values[-3:]) if len(values) >= 3 else values[-1]
        older_avg = statistics.mean(values[:3]) if len(
            values) >= 3 else values[0]

        change = recent_avg - older_avg
        change_percent = (change / older_avg) if older_avg != 0 else 0

        if abs(change_percent) < 0.05:  # Less than 5% change
            direction = 'stable'
        elif change > 0:
            direction = 'increasing'
        else:
            direction = 'declining'

        return {
            'direction': direction,
            'change': change,
            'change_percent': change_percent
        }

    def _calculate_health_score(self, metrics: Dict[str, Any]) -> float:
        """Calculate overall health score (0-100)."""
        score = 100

        # Success rate component (40% of score)
        success_rate = metrics.get('success_rate', 1.0)
        score *= 0.6 + (0.4 * success_rate)

        # Duration component (30% of score)
        avg_duration = metrics.get('avg_duration', 0)
        duration_score = max(0, 1 - (avg_duration / 1800)
                             )  # Penalize if > 30 minutes
        score *= 0.7 + (0.3 * duration_score)

        # Queue health component (30% of score)
        queued_tasks = metrics.get('queued_tasks', 0)
        # Penalize if > 50 queued
        queue_score = max(0, 1 - (queued_tasks / 50))
        score *= 0.7 + (0.3 * queue_score)

        return min(100, max(0, score))

    def generate_health_report(self, dag_ids: List[str]) -> Dict[str, Any]:
        """
        Generate comprehensive health report for multiple DAGs.
        """
        report = {
            'report_timestamp': datetime.now().isoformat(),
            'dags_analyzed': len(dag_ids),
            'overall_health': 'healthy',
            'dag_health': {},
            'system_alerts': [],
            'recommendations': []
        }

        health_scores = []

        for dag_id in dag_ids:
            metrics = self.collect_dag_metrics(dag_id)
            analysis = self.analyze_health_trends(dag_id)

            dag_health = {
                'current_metrics': metrics,
                'trend_analysis': analysis,
                'health_score': analysis.get('current_health_score', 0)
            }

            report['dag_health'][dag_id] = dag_health
            health_scores.append(dag_health['health_score'])

            # Collect system-level alerts
            for alert in analysis.get('alerts', []):
                alert['dag_id'] = dag_id
                report['system_alerts'].append(alert)

        # Calculate overall system health
        if health_scores:
            avg_health = statistics.mean(health_scores)
            if avg_health >= 80:
                report['overall_health'] = 'healthy'
            elif avg_health >= 60:
                report['overall_health'] = 'degraded'
            else:
                report['overall_health'] = 'unhealthy'

        # Generate recommendations
        report['recommendations'] = self._generate_recommendations(report)

        return report

    def _generate_recommendations(self, report: Dict[str, Any]) -> List[str]:
        """Generate actionable recommendations based on health report."""
        recommendations = []

        # Check for common issues
        high_severity_alerts = [
            a for a in report['system_alerts'] if a['severity'] == 'critical']
        if high_severity_alerts:
            recommendations.append(
                "Address critical alerts immediately - system stability at risk")

        degraded_dags = [dag_id for dag_id, health in report['dag_health'].items()
                         if health['health_score'] < 70]
        if degraded_dags:
            recommendations.append(
                f"Review and optimize DAGs with low health scores: {', '.join(degraded_dags)}")

        # Check for performance issues
        slow_dags = [dag_id for dag_id, health in report['dag_health'].items()
                     if health['current_metrics'].get('avg_duration', 0) > 600]
        if slow_dags:
            recommendations.append(
                f"Investigate performance issues in slow DAGs: {', '.join(slow_dags)}")

        # Check for queue buildup
        queued_issues = [dag_id for dag_id, health in report['dag_health'].items()
                         if health['current_metrics'].get('queued_tasks', 0) > 20]
        if queued_issues:
            recommendations.append(
                f"Address task queue buildup in: {', '.join(queued_issues)}")

        if not recommendations:
            recommendations.append(
                "System health is good - continue monitoring")

        return recommendations


# Global health monitor instance
health_monitor = WorkflowHealthMonitor()


def health_monitoring_callback(context: Dict[str, Any]) -> None:
    """
    Callback to collect health metrics on task completion.
    """
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    task_instance = context['task_instance']

    # Collect task-level metrics
    task_metrics = {
        'dag_id': dag_id,
        'task_id': task_id,
        'duration': task_instance.duration.total_seconds() if task_instance.duration else 0,
        'state': task_instance.state,
        'try_number': task_instance.try_number,
        'timestamp': datetime.now().isoformat()
    }

    logger.info(f"TASK_METRICS: {json.dumps(task_metrics)}")

    # Trigger DAG-level health analysis
    health_analysis = health_monitor.analyze_health_trends(dag_id)

    # Log health analysis
    logger.info(f"HEALTH_ANALYSIS: {json.dumps(health_analysis, indent=2)}")

    # Send alerts if health issues detected
    for alert in health_analysis.get('alerts', []):
        logger.warning(f"HEALTH_ALERT: {json.dumps(alert)}")


# DAG configuration
default_args = {
    'owner': 'airflow-kata',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'workflow_health_monitoring',
    default_args=default_args,
    description='Workflow health monitoring examples',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['error-handling', 'monitoring', 'health', 'examples']
)

# Task functions for health monitoring demonstration


def data_processing_task(**context):
    """Simulate data processing with variable performance."""
    import random

    # Simulate variable processing time
    processing_time = random.uniform(30, 180)  # 30 seconds to 3 minutes
    logger.info(f"Processing data for {processing_time:.1f} seconds")

    time.sleep(min(processing_time, 10))  # Cap at 10 seconds for demo

    # Simulate occasional failures
    if random.random() < 0.1:  # 10% failure rate
        raise AirflowException(
            "Data processing failed due to resource constraints")

    return {
        'status': 'success',
        'processing_time': processing_time,
        'records_processed': random.randint(1000, 10000)
    }


def api_integration_task(**context):
    """Simulate API integration with health monitoring."""
    import random

    # Simulate API call duration
    api_duration = random.uniform(5, 60)  # 5 seconds to 1 minute
    logger.info(f"Making API call (estimated duration: {api_duration:.1f}s)")

    time.sleep(min(api_duration, 5))  # Cap at 5 seconds for demo

    # Simulate API failures
    if random.random() < 0.05:  # 5% failure rate
        raise AirflowException("API integration failed: Service unavailable")

    return {
        'status': 'success',
        'api_duration': api_duration,
        'response_size': random.randint(100, 5000)
    }


def database_maintenance_task(**context):
    """Simulate database maintenance with performance monitoring."""
    import random

    # Simulate maintenance duration
    maintenance_time = random.uniform(60, 300)  # 1-5 minutes
    logger.info(
        f"Running database maintenance (estimated: {maintenance_time:.1f}s)")

    time.sleep(min(maintenance_time, 8))  # Cap at 8 seconds for demo

    return {
        'status': 'success',
        'maintenance_time': maintenance_time,
        'tables_optimized': random.randint(5, 50)
    }


def system_health_check(**context):
    """Comprehensive system health check."""
    logger.info("Running comprehensive system health check")

    # Generate health report for current DAG and simulated others
    dag_ids = [context['dag'].dag_id, 'example_dag_1',
               'example_dag_2', 'example_dag_3']
    health_report = health_monitor.generate_health_report(dag_ids)

    # Log comprehensive health report
    logger.info(f"SYSTEM_HEALTH_REPORT: {json.dumps(health_report, indent=2)}")

    # Check if system health is concerning
    if health_report['overall_health'] in ['degraded', 'unhealthy']:
        logger.warning(
            f"System health is {health_report['overall_health']} - review required")

        # In production, this would trigger alerts
        for alert in health_report['system_alerts']:
            if alert['severity'] in ['critical', 'high']:
                logger.error(f"CRITICAL_HEALTH_ISSUE: {json.dumps(alert)}")

    return health_report


def performance_benchmark_task(**context):
    """Task to establish performance benchmarks."""
    import random

    logger.info("Running performance benchmark")

    # Simulate benchmark operations
    benchmark_results = {
        'cpu_benchmark': random.uniform(80, 120),  # Relative performance score
        'memory_benchmark': random.uniform(85, 115),
        'disk_io_benchmark': random.uniform(75, 125),
        'network_benchmark': random.uniform(90, 110)
    }

    # Calculate overall performance score
    overall_score = statistics.mean(benchmark_results.values())
    benchmark_results['overall_score'] = overall_score

    logger.info(f"PERFORMANCE_BENCHMARK: {json.dumps(benchmark_results)}")

    # Alert if performance is significantly degraded
    if overall_score < 85:
        logger.warning(
            f"Performance degradation detected: {overall_score:.1f}/100")

    return benchmark_results

# Configure tasks with health monitoring


data_task = PythonOperator(
    task_id='data_processing_with_monitoring',
    python_callable=data_processing_task,
    on_success_callback=health_monitoring_callback,
    on_failure_callback=health_monitoring_callback,
    dag=dag
)

api_task = PythonOperator(
    task_id='api_integration_with_monitoring',
    python_callable=api_integration_task,
    on_success_callback=health_monitoring_callback,
    on_failure_callback=health_monitoring_callback,
    dag=dag
)

db_task = PythonOperator(
    task_id='database_maintenance_with_monitoring',
    python_callable=database_maintenance_task,
    on_success_callback=health_monitoring_callback,
    on_failure_callback=health_monitoring_callback,
    dag=dag
)

health_check = PythonOperator(
    task_id='comprehensive_health_check',
    python_callable=system_health_check,
    dag=dag
)

benchmark_task = PythonOperator(
    task_id='performance_benchmark',
    python_callable=performance_benchmark_task,
    dag=dag
)

# Bash task for system resource monitoring
resource_monitor = BashOperator(
    task_id='system_resource_monitor',
    bash_command="""
    echo "=== System Resource Monitoring ==="
    echo "Timestamp: $(date)"
    echo "CPU Usage: $(top -bn1 | grep "Cpu(s)" | awk '{print $2}' | cut -d'%' -f1)%"
    echo "Memory Usage: $(free | grep Mem | awk '{printf("%.1f%%", $3/$2 * 100.0)}')"
    echo "Disk Usage: $(df -h / | tail -1 | awk '{print $5}')"
    echo "Load Average: $(uptime | awk -F'load average:' '{print $2}')"
    echo "Active Processes: $(ps aux | wc -l)"
    echo "=== End Resource Monitoring ==="
    """,
    dag=dag
)

# Set up task dependencies
data_task >> api_task >> db_task >> health_check
benchmark_task >> resource_monitor >> health_check
