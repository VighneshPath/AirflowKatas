"""
Solution: Exercise 5 - Workflow Health Monitoring

This solution demonstrates comprehensive workflow health monitoring with
metrics collection, trend analysis, and proactive alerting.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException
import logging
import json
import time
import statistics
import random
from typing import Dict, Any, List, Optional, Tuple
from collections import defaultdict, deque
import psutil
import os

# Configure logging
logger = logging.getLogger(__name__)


class WorkflowHealthMonitor:
    """
    Comprehensive workflow health monitoring system with metrics collection,
    trend analysis, anomaly detection, and proactive alerting.
    """

    def __init__(self):
        self.metrics_history = defaultdict(lambda: deque(maxlen=1000))
        self.performance_baselines = {}
        self.health_thresholds = {
            'success_rate_critical': 0.90,
            'success_rate_warning': 0.95,
            'duration_multiplier_critical': 3.0,
            'duration_multiplier_warning': 2.0,
            'queue_depth_critical': 50,
            'queue_depth_warning': 20,
            'failure_spike_threshold': 5
        }
        self.alert_history = defaultdict(list)

    def collect_task_metrics(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Collect comprehensive task-level metrics.
        """
        task_instance = context['task_instance']
        task_id = context['task'].task_id
        dag_id = context['dag'].dag_id

        # Basic task metrics
        metrics = {
            'timestamp': datetime.now().isoformat(),
            'dag_id': dag_id,
            'task_id': task_id,
            'duration_seconds': task_instance.duration.total_seconds() if task_instance.duration else 0,
            'state': task_instance.state,
            'try_number': task_instance.try_number,
            'max_tries': task_instance.max_tries,
            'start_date': task_instance.start_date.isoformat() if task_instance.start_date else None,
            'end_date': task_instance.end_date.isoformat() if task_instance.end_date else None,
            'operator_type': context['task'].__class__.__name__,
            'pool': getattr(context['task'], 'pool', 'default_pool'),
            'queue': getattr(context['task'], 'queue', 'default')
        }

        # System resource metrics
        try:
            process = psutil.Process()
            metrics.update({
                'cpu_percent': process.cpu_percent(),
                'memory_mb': process.memory_info().rss / 1024 / 1024,
                'memory_percent': process.memory_percent(),
                'num_threads': process.num_threads(),
                'system_cpu_percent': psutil.cpu_percent(),
                'system_memory_percent': psutil.virtual_memory().percent,
                'system_disk_percent': psutil.disk_usage('/').percent
            })
        except Exception as e:
            logger.warning(f"Could not collect system metrics: {e}")
            metrics.update({
                'cpu_percent': 0,
                'memory_mb': 0,
                'memory_percent': 0,
                'num_threads': 1,
                'system_cpu_percent': 0,
                'system_memory_percent': 0,
                'system_disk_percent': 0
            })

        # Queue metrics (simulated)
        metrics.update({
            'queue_depth': random.randint(0, 30),
            'queue_wait_time': random.uniform(0, 60)
        })

        # Store metrics in history
        metric_key = f"{dag_id}.{task_id}"
        self.metrics_history[metric_key].append(metrics)

        return metrics

    def collect_dag_metrics(self, dag_id: str) -> Dict[str, Any]:
        """
        Collect DAG-level metrics by aggregating task metrics.
        """
        current_time = datetime.now()

        # Get recent task metrics for this DAG
        dag_task_metrics = []
        for key, metrics_list in self.metrics_history.items():
            if key.startswith(f"{dag_id}."):
                # Get metrics from last 24 hours
                recent_metrics = [
                    m for m in metrics_list
                    if (current_time - datetime.fromisoformat(m['timestamp'])).total_seconds() < 86400
                ]
                dag_task_metrics.extend(recent_metrics)

        if not dag_task_metrics:
            return self._get_empty_dag_metrics(dag_id)

        # Calculate aggregated metrics
        total_tasks = len(dag_task_metrics)
        successful_tasks = len(
            [m for m in dag_task_metrics if m['state'] == 'success'])
        failed_tasks = len(
            [m for m in dag_task_metrics if m['state'] == 'failed'])

        durations = [m['duration_seconds']
                     for m in dag_task_metrics if m['duration_seconds'] > 0]

        dag_metrics = {
            'timestamp': current_time.isoformat(),
            'dag_id': dag_id,
            'total_tasks': total_tasks,
            'successful_tasks': successful_tasks,
            'failed_tasks': failed_tasks,
            'success_rate': successful_tasks / total_tasks if total_tasks > 0 else 1.0,
            'failure_rate': failed_tasks / total_tasks if total_tasks > 0 else 0.0,
            'avg_duration': statistics.mean(durations) if durations else 0,
            'median_duration': statistics.median(durations) if durations else 0,
            'max_duration': max(durations) if durations else 0,
            'min_duration': min(durations) if durations else 0,
            'total_retries': sum(m['try_number'] - 1 for m in dag_task_metrics),
            'avg_cpu_percent': statistics.mean([m['cpu_percent'] for m in dag_task_metrics]),
            'avg_memory_mb': statistics.mean([m['memory_mb'] for m in dag_task_metrics]),
            'avg_queue_depth': statistics.mean([m['queue_depth'] for m in dag_task_metrics]),
            'avg_queue_wait_time': statistics.mean([m['queue_wait_time'] for m in dag_task_metrics])
        }

        # Store DAG metrics
        self.metrics_history[dag_id].append(dag_metrics)

        return dag_metrics

    def _get_empty_dag_metrics(self, dag_id: str) -> Dict[str, Any]:
        """Return empty metrics structure when no data is available."""
        return {
            'timestamp': datetime.now().isoformat(),
            'dag_id': dag_id,
            'total_tasks': 0,
            'successful_tasks': 0,
            'failed_tasks': 0,
            'success_rate': 1.0,
            'failure_rate': 0.0,
            'avg_duration': 0,
            'median_duration': 0,
            'max_duration': 0,
            'min_duration': 0,
            'total_retries': 0,
            'avg_cpu_percent': 0,
            'avg_memory_mb': 0,
            'avg_queue_depth': 0,
            'avg_queue_wait_time': 0
        }

    def analyze_performance_trends(self, dag_id: str, lookback_hours: int = 24) -> Dict[str, Any]:
        """
        Analyze performance trends for a DAG over specified time period.
        """
        if dag_id not in self.metrics_history:
            return {'status': 'no_data', 'dag_id': dag_id}

        # Get recent metrics
        current_time = datetime.now()
        cutoff_time = current_time - timedelta(hours=lookback_hours)

        recent_metrics = [
            m for m in self.metrics_history[dag_id]
            if datetime.fromisoformat(m['timestamp']) >= cutoff_time
        ]

        if len(recent_metrics) < 2:
            return {'status': 'insufficient_data', 'dag_id': dag_id}

        # Calculate trends
        analysis = {
            'dag_id': dag_id,
            'analysis_timestamp': current_time.isoformat(),
            'lookback_hours': lookback_hours,
            'data_points': len(recent_metrics),
            'current_metrics': recent_metrics[-1] if recent_metrics else {},
            'trends': {},
            'anomalies': [],
            'health_score': 0,
            'recommendations': []
        }

        # Analyze key metrics trends
        metrics_to_analyze = [
            'success_rate', 'avg_duration', 'failure_rate',
            'avg_cpu_percent', 'avg_memory_mb', 'avg_queue_depth'
        ]

        for metric in metrics_to_analyze:
            values = [m.get(metric, 0) for m in recent_metrics]
            trend_analysis = self._calculate_trend(values, metric)
            analysis['trends'][metric] = trend_analysis

        # Detect anomalies
        analysis['anomalies'] = self._detect_anomalies(recent_metrics, dag_id)

        # Calculate health score
        analysis['health_score'] = self._calculate_health_score(
            recent_metrics[-1])

        # Generate recommendations
        analysis['recommendations'] = self._generate_recommendations(analysis)

        return analysis

    def _calculate_trend(self, values: List[float], metric_name: str) -> Dict[str, Any]:
        """
        Calculate trend direction and magnitude for a metric.
        """
        if len(values) < 2:
            return {'direction': 'stable', 'change': 0, 'change_percent': 0}

        # Use first and last quartile for trend calculation
        quartile_size = max(1, len(values) // 4)
        early_values = values[:quartile_size]
        recent_values = values[-quartile_size:]

        early_avg = statistics.mean(early_values)
        recent_avg = statistics.mean(recent_values)

        change = recent_avg - early_avg
        change_percent = (change / early_avg) if early_avg != 0 else 0

        # Determine significance threshold based on metric type
        significance_thresholds = {
            'success_rate': 0.02,      # 2% change is significant
            'failure_rate': 0.01,      # 1% change is significant
            'avg_duration': 0.10,      # 10% change is significant
            'avg_cpu_percent': 0.15,   # 15% change is significant
            'avg_memory_mb': 0.20,     # 20% change is significant
            'avg_queue_depth': 0.25    # 25% change is significant
        }

        threshold = significance_thresholds.get(metric_name, 0.10)

        if abs(change_percent) < threshold:
            direction = 'stable'
        elif change > 0:
            direction = 'increasing'
        else:
            direction = 'decreasing'

        return {
            'direction': direction,
            'change': change,
            'change_percent': change_percent,
            'significance': 'significant' if abs(change_percent) >= threshold else 'minor',
            'early_avg': early_avg,
            'recent_avg': recent_avg
        }

    def _detect_anomalies(self, metrics: List[Dict[str, Any]], dag_id: str) -> List[Dict[str, Any]]:
        """
        Detect anomalies in metrics using statistical methods.
        """
        anomalies = []

        if len(metrics) < 10:  # Need sufficient data for anomaly detection
            return anomalies

        current_metrics = metrics[-1]

        # Check success rate anomalies
        success_rates = [m['success_rate'] for m in metrics]
        success_rate_mean = statistics.mean(success_rates)
        success_rate_stdev = statistics.stdev(
            success_rates) if len(success_rates) > 1 else 0

        if current_metrics['success_rate'] < (success_rate_mean - 2 * success_rate_stdev):
            anomalies.append({
                'type': 'success_rate_anomaly',
                'severity': 'high',
                'current_value': current_metrics['success_rate'],
                'expected_range': [success_rate_mean - success_rate_stdev, success_rate_mean + success_rate_stdev],
                'description': f"Success rate ({current_metrics['success_rate']:.2%}) is significantly below normal range"
            })

        # Check duration anomalies
        durations = [m['avg_duration']
                     for m in metrics if m['avg_duration'] > 0]
        if durations:
            duration_mean = statistics.mean(durations)
            duration_stdev = statistics.stdev(
                durations) if len(durations) > 1 else 0

            if current_metrics['avg_duration'] > (duration_mean + 2 * duration_stdev):
                anomalies.append({
                    'type': 'duration_anomaly',
                    'severity': 'medium',
                    'current_value': current_metrics['avg_duration'],
                    'expected_range': [duration_mean - duration_stdev, duration_mean + duration_stdev],
                    'description': f"Average duration ({current_metrics['avg_duration']:.1f}s) is significantly above normal range"
                })

        # Check failure spike
        recent_failures = [m['failed_tasks']
                           for m in metrics[-5:]]  # Last 5 data points
        if sum(recent_failures) >= self.health_thresholds['failure_spike_threshold']:
            anomalies.append({
                'type': 'failure_spike',
                'severity': 'high',
                'current_value': sum(recent_failures),
                'threshold': self.health_thresholds['failure_spike_threshold'],
                'description': f"Failure spike detected: {sum(recent_failures)} failures in recent period"
            })

        return anomalies

    def _calculate_health_score(self, metrics: Dict[str, Any]) -> float:
        """
        Calculate composite health score (0-100) based on multiple factors.
        """
        if not metrics:
            return 0

        score = 100

        # Success rate component (40% weight)
        success_rate = metrics.get('success_rate', 1.0)
        if success_rate < self.health_thresholds['success_rate_critical']:
            score *= 0.3  # Severe penalty
        elif success_rate < self.health_thresholds['success_rate_warning']:
            score *= 0.7  # Moderate penalty
        else:
            score *= (0.6 + 0.4 * success_rate)  # Scaled bonus

        # Performance component (35% weight)
        avg_duration = metrics.get('avg_duration', 0)
        baseline_duration = self.performance_baselines.get(
            metrics.get('dag_id'), {}).get('avg_duration', 300)

        if avg_duration > 0 and baseline_duration > 0:
            duration_ratio = avg_duration / baseline_duration
            if duration_ratio > self.health_thresholds['duration_multiplier_critical']:
                score *= 0.4  # Severe penalty
            elif duration_ratio > self.health_thresholds['duration_multiplier_warning']:
                score *= 0.7  # Moderate penalty
            else:
                score *= (0.65 + 0.35 / max(1, duration_ratio))  # Scaled bonus

        # Resource utilization component (25% weight)
        queue_depth = metrics.get('avg_queue_depth', 0)
        if queue_depth > self.health_thresholds['queue_depth_critical']:
            score *= 0.5  # Severe penalty
        elif queue_depth > self.health_thresholds['queue_depth_warning']:
            score *= 0.8  # Moderate penalty
        else:
            score *= (0.75 + 0.25 * (1 - queue_depth /
                      self.health_thresholds['queue_depth_warning']))

        return max(0, min(100, score))

    def _generate_recommendations(self, analysis: Dict[str, Any]) -> List[str]:
        """
        Generate actionable recommendations based on analysis.
        """
        recommendations = []
        trends = analysis.get('trends', {})
        anomalies = analysis.get('anomalies', [])
        health_score = analysis.get('health_score', 100)

        # Health score based recommendations
        if health_score < 50:
            recommendations.append(
                "URGENT: Health score is critically low - immediate investigation required")
        elif health_score < 70:
            recommendations.append(
                "Health score indicates issues - review and optimize workflow")

        # Trend based recommendations
        if trends.get('success_rate', {}).get('direction') == 'decreasing':
            recommendations.append(
                "Success rate is declining - investigate recent failures and error patterns")

        if trends.get('avg_duration', {}).get('direction') == 'increasing':
            change_percent = trends['avg_duration'].get('change_percent', 0)
            if change_percent > 0.2:  # 20% increase
                recommendations.append(
                    "Performance degradation detected - review task efficiency and resource allocation")

        if trends.get('avg_queue_depth', {}).get('direction') == 'increasing':
            recommendations.append(
                "Queue depth increasing - consider scaling workers or optimizing task scheduling")

        # Anomaly based recommendations
        for anomaly in anomalies:
            if anomaly['type'] == 'success_rate_anomaly':
                recommendations.append(
                    "Success rate anomaly detected - check for recent code changes or infrastructure issues")
            elif anomaly['type'] == 'duration_anomaly':
                recommendations.append(
                    "Performance anomaly detected - investigate resource constraints or data volume changes")
            elif anomaly['type'] == 'failure_spike':
                recommendations.append(
                    "Failure spike detected - immediate investigation of error patterns required")

        # Resource based recommendations
        current_metrics = analysis.get('current_metrics', {})
        if current_metrics.get('avg_cpu_percent', 0) > 80:
            recommendations.append(
                "High CPU utilization - consider optimizing tasks or scaling resources")

        if current_metrics.get('avg_memory_mb', 0) > 1000:  # 1GB
            recommendations.append(
                "High memory usage - review memory-intensive tasks and consider optimization")

        # Default recommendation if no issues found
        if not recommendations:
            recommendations.append(
                "System health is good - continue monitoring and maintain current practices")

        return recommendations

    def generate_health_report(self, dag_ids: List[str]) -> Dict[str, Any]:
        """
        Generate comprehensive health report for multiple DAGs.
        """
        report = {
            'report_timestamp': datetime.now().isoformat(),
            'report_period_hours': 24,
            'dags_analyzed': len(dag_ids),
            'overall_health_score': 0,
            'overall_status': 'unknown',
            'dag_health': {},
            'system_alerts': [],
            'top_recommendations': [],
            'summary_statistics': {}
        }

        health_scores = []
        all_recommendations = []
        all_alerts = []

        # Analyze each DAG
        for dag_id in dag_ids:
            try:
                # Collect current metrics
                dag_metrics = self.collect_dag_metrics(dag_id)

                # Analyze trends
                trend_analysis = self.analyze_performance_trends(dag_id)

                # Store DAG health information
                dag_health = {
                    'current_metrics': dag_metrics,
                    'trend_analysis': trend_analysis,
                    'health_score': trend_analysis.get('health_score', 0),
                    'status': self._determine_dag_status(trend_analysis.get('health_score', 0))
                }

                report['dag_health'][dag_id] = dag_health
                health_scores.append(dag_health['health_score'])

                # Collect recommendations and alerts
                all_recommendations.extend(
                    trend_analysis.get('recommendations', []))

                # Generate alerts for significant issues
                for anomaly in trend_analysis.get('anomalies', []):
                    alert = {
                        'dag_id': dag_id,
                        'type': anomaly['type'],
                        'severity': anomaly['severity'],
                        'description': anomaly['description'],
                        'timestamp': datetime.now().isoformat()
                    }
                    all_alerts.append(alert)

            except Exception as e:
                logger.error(f"Error analyzing DAG {dag_id}: {e}")
                report['dag_health'][dag_id] = {
                    'error': str(e),
                    'health_score': 0,
                    'status': 'error'
                }

        # Calculate overall health
        if health_scores:
            report['overall_health_score'] = statistics.mean(health_scores)
            report['overall_status'] = self._determine_system_status(
                health_scores)

        # Prioritize and deduplicate recommendations
        report['top_recommendations'] = self._prioritize_recommendations(
            all_recommendations)[:10]

        # Store system alerts
        report['system_alerts'] = sorted(
            all_alerts, key=lambda x: x['severity'], reverse=True)

        # Generate summary statistics
        report['summary_statistics'] = {
            'healthy_dags': len([s for s in health_scores if s >= 80]),
            'degraded_dags': len([s for s in health_scores if 60 <= s < 80]),
            'unhealthy_dags': len([s for s in health_scores if s < 60]),
            'critical_alerts': len([a for a in all_alerts if a['severity'] == 'high']),
            'warning_alerts': len([a for a in all_alerts if a['severity'] == 'medium']),
            'avg_health_score': statistics.mean(health_scores) if health_scores else 0,
            'min_health_score': min(health_scores) if health_scores else 0,
            'max_health_score': max(health_scores) if health_scores else 0
        }

        return report

    def _determine_dag_status(self, health_score: float) -> str:
        """Determine DAG status based on health score."""
        if health_score >= 80:
            return 'healthy'
        elif health_score >= 60:
            return 'degraded'
        else:
            return 'unhealthy'

    def _determine_system_status(self, health_scores: List[float]) -> str:
        """Determine overall system status."""
        if not health_scores:
            return 'unknown'

        avg_score = statistics.mean(health_scores)
        unhealthy_count = len([s for s in health_scores if s < 60])

        if avg_score >= 80 and unhealthy_count == 0:
            return 'healthy'
        elif avg_score >= 70 and unhealthy_count <= 1:
            return 'degraded'
        else:
            return 'unhealthy'

    def _prioritize_recommendations(self, recommendations: List[str]) -> List[str]:
        """Prioritize and deduplicate recommendations."""
        # Remove duplicates while preserving order
        seen = set()
        unique_recommendations = []
        for rec in recommendations:
            if rec not in seen:
                seen.add(rec)
                unique_recommendations.append(rec)

        # Prioritize by keywords
        priority_keywords = ['URGENT', 'critical',
                             'immediate', 'spike', 'anomaly']

        def get_priority(rec):
            for i, keyword in enumerate(priority_keywords):
                if keyword.lower() in rec.lower():
                    return i
            return len(priority_keywords)

        return sorted(unique_recommendations, key=get_priority)


# Global health monitor instance
health_monitor = WorkflowHealthMonitor()


def health_monitoring_callback(context: Dict[str, Any]) -> None:
    """
    Comprehensive health monitoring callback for task completion.
    """
    try:
        # Collect task metrics
        task_metrics = health_monitor.collect_task_metrics(context)

        # Log task metrics
        logger.info(
            f"TASK_HEALTH_METRICS: {json.dumps(task_metrics, indent=2)}")

        # Analyze DAG health if enough data
        dag_id = context['dag'].dag_id
        trend_analysis = health_monitor.analyze_performance_trends(dag_id)

        if trend_analysis.get('status') not in ['no_data', 'insufficient_data']:
            logger.info(
                f"DAG_HEALTH_ANALYSIS: {json.dumps(trend_analysis, indent=2)}")

            # Generate alerts for significant issues
            health_score = trend_analysis.get('health_score', 100)
            if health_score < 70:
                logger.warning(
                    f"DAG_HEALTH_ALERT: {dag_id} health score is {health_score:.1f}")

            # Log anomalies
            for anomaly in trend_analysis.get('anomalies', []):
                logger.warning(f"HEALTH_ANOMALY: {json.dumps(anomaly)}")

    except Exception as e:
        logger.error(f"Error in health monitoring callback: {e}")


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
    'health_monitoring_dag',
    default_args=default_args,
    description='Solution for workflow health monitoring exercise',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['error-handling', 'monitoring', 'health', 'exercise', 'solution']
)

# Task functions for health monitoring demonstration


def data_processing_with_metrics(**context):
    """Data processing task with variable performance for metrics collection."""
    import random

    # Simulate variable processing characteristics
    processing_scenarios = ['fast', 'normal', 'slow', 'very_slow']
    scenario = random.choice(processing_scenarios)

    scenario_configs = {
        'fast': {'duration': (5, 15), 'failure_rate': 0.02},
        'normal': {'duration': (15, 45), 'failure_rate': 0.05},
        'slow': {'duration': (45, 90), 'failure_rate': 0.08},
        'very_slow': {'duration': (90, 180), 'failure_rate': 0.12}
    }

    config = scenario_configs[scenario]
    duration = random.uniform(*config['duration'])

    logger.info(
        f"Processing scenario: {scenario}, estimated duration: {duration:.1f}s")

    # Simulate processing time (capped for demo)
    time.sleep(min(duration, 8))

    # Simulate occasional failures based on scenario
    if random.random() < config['failure_rate']:
        raise AirflowException(f"Processing failed in {scenario} scenario")

    return {
        'scenario': scenario,
        'duration': duration,
        'records_processed': random.randint(1000, 10000),
        'data_quality_score': random.uniform(0.85, 0.99)
    }


def api_integration_with_monitoring(**context):
    """API integration task with health monitoring."""
    import random

    # Simulate API performance variations
    api_health = random.choice(['excellent', 'good', 'degraded', 'poor'])

    health_configs = {
        'excellent': {'duration': (2, 8), 'failure_rate': 0.01},
        'good': {'duration': (5, 15), 'failure_rate': 0.03},
        'degraded': {'duration': (15, 30), 'failure_rate': 0.08},
        'poor': {'duration': (30, 60), 'failure_rate': 0.15}
    }

    config = health_configs[api_health]
    duration = random.uniform(*config['duration'])

    logger.info(
        f"API health: {api_health}, estimated duration: {duration:.1f}s")

    # Simulate API call time
    time.sleep(min(duration, 5))

    # Simulate failures based on API health
    if random.random() < config['failure_rate']:
        raise AirflowException(
            f"API integration failed - API health: {api_health}")

    return {
        'api_health': api_health,
        'duration': duration,
        'response_size_kb': random.randint(10, 500),
        'api_version': '2.1'
    }


def database_operations_with_metrics(**context):
    """Database operations with performance monitoring."""
    import random

    # Simulate database load scenarios
    db_load = random.choice(['light', 'moderate', 'heavy', 'overloaded'])

    load_configs = {
        'light': {'duration': (10, 25), 'failure_rate': 0.01},
        'moderate': {'duration': (20, 45), 'failure_rate': 0.03},
        'heavy': {'duration': (40, 80), 'failure_rate': 0.07},
        'overloaded': {'duration': (80, 150), 'failure_rate': 0.12}
    }

    config = load_configs[db_load]
    duration = random.uniform(*config['duration'])

    logger.info(
        f"Database load: {db_load}, estimated duration: {duration:.1f}s")

    # Simulate database operation time
    time.sleep(min(duration, 6))

    # Simulate failures based on database load
    if random.random() < config['failure_rate']:
        raise AirflowException(f"Database operation failed - load: {db_load}")

    return {
        'db_load': db_load,
        'duration': duration,
        'rows_processed': random.randint(100, 5000),
        'connection_pool_usage': random.uniform(0.3, 0.9)
    }


def system_health_report_task(**context):
    """Generate comprehensive system health report."""
    logger.info("Generating comprehensive system health report")

    # Simulate multiple DAGs for health reporting
    dag_ids = [
        context['dag'].dag_id,
        'example_etl_dag',
        'data_validation_dag',
        'reporting_dag',
        'maintenance_dag'
    ]

    # Generate health report
    health_report = health_monitor.generate_health_report(dag_ids)

    # Log comprehensive health report
    logger.info(f"SYSTEM_HEALTH_REPORT: {json.dumps(health_report, indent=2)}")

    # Check for critical issues
    critical_alerts = [
        a for a in health_report['system_alerts'] if a['severity'] == 'high']
    if critical_alerts:
        logger.error(
            f"CRITICAL_HEALTH_ISSUES: {len(critical_alerts)} critical issues detected")
        for alert in critical_alerts:
            logger.error(f"CRITICAL_ALERT: {json.dumps(alert)}")

    # Log top recommendations
    for i, recommendation in enumerate(health_report['top_recommendations'][:5], 1):
        logger.info(f"RECOMMENDATION_{i}: {recommendation}")

    return {
        'overall_health_score': health_report['overall_health_score'],
        'overall_status': health_report['overall_status'],
        'dags_analyzed': health_report['dags_analyzed'],
        'critical_alerts': len(critical_alerts),
        'recommendations_count': len(health_report['top_recommendations'])
    }


def performance_benchmark_task(**context):
    """Establish performance benchmarks for health monitoring."""
    import random

    logger.info("Running performance benchmarks")

    # Simulate benchmark operations
    benchmarks = {
        'cpu_benchmark_score': random.uniform(80, 120),
        'memory_benchmark_score': random.uniform(85, 115),
        'disk_io_benchmark_score': random.uniform(75, 125),
        'network_benchmark_score': random.uniform(90, 110),
        'database_benchmark_score': random.uniform(70, 130)
    }

    # Calculate overall performance score
    overall_score = statistics.mean(benchmarks.values())
    benchmarks['overall_performance_score'] = overall_score

    # Update performance baselines
    dag_id = context['dag'].dag_id
    health_monitor.performance_baselines[dag_id] = {
        'avg_duration': 60,  # 1 minute baseline
        'cpu_baseline': benchmarks['cpu_benchmark_score'],
        'memory_baseline': benchmarks['memory_benchmark_score'],
        'updated_at': datetime.now().isoformat()
    }

    logger.info(f"PERFORMANCE_BENCHMARKS: {json.dumps(benchmarks, indent=2)}")

    # Alert if performance is significantly degraded
    if overall_score < 85:
        logger.warning(
            f"Performance degradation detected: {overall_score:.1f}/100")

    return benchmarks

# Configure tasks with health monitoring callbacks


data_processing = PythonOperator(
    task_id='data_processing_with_metrics',
    python_callable=data_processing_with_metrics,
    on_success_callback=health_monitoring_callback,
    on_failure_callback=health_monitoring_callback,
    dag=dag
)

api_integration = PythonOperator(
    task_id='api_integration_with_monitoring',
    python_callable=api_integration_with_monitoring,
    on_success_callback=health_monitoring_callback,
    on_failure_callback=health_monitoring_callback,
    dag=dag
)

database_ops = PythonOperator(
    task_id='database_operations_with_metrics',
    python_callable=database_operations_with_metrics,
    on_success_callback=health_monitoring_callback,
    on_failure_callback=health_monitoring_callback,
    dag=dag
)

health_report = PythonOperator(
    task_id='system_health_report',
    python_callable=system_health_report_task,
    dag=dag
)

benchmark_task = PythonOperator(
    task_id='performance_benchmark',
    python_callable=performance_benchmark_task,
    dag=dag
)

# System monitoring bash task
system_monitor = BashOperator(
    task_id='system_resource_monitoring',
    bash_command="""
    echo "=== Comprehensive System Monitoring ==="
    echo "Timestamp: $(date)"
    echo "Hostname: $(hostname)"
    echo "Uptime: $(uptime)"
    echo ""
    echo "=== CPU Information ==="
    echo "CPU Usage: $(top -bn1 | grep "Cpu(s)" | awk '{print $2}' | cut -d'%' -f1)%"
    echo "Load Average: $(uptime | awk -F'load average:' '{print $2}')"
    echo ""
    echo "=== Memory Information ==="
    echo "Memory Usage: $(free | grep Mem | awk '{printf("%.1f%%", $3/$2 * 100.0)}')"
    echo "Available Memory: $(free -h | grep Mem | awk '{print $7}')"
    echo ""
    echo "=== Disk Information ==="
    echo "Disk Usage: $(df -h / | tail -1 | awk '{print $5}')"
    echo "Available Space: $(df -h / | tail -1 | awk '{print $4}')"
    echo ""
    echo "=== Process Information ==="
    echo "Active Processes: $(ps aux | wc -l)"
    echo "Python Processes: $(ps aux | grep python | wc -l)"
    echo ""
    echo "=== Network Information ==="
    echo "Network Connections: $(netstat -an | wc -l)"
    echo "=== End System Monitoring ==="
    """,
    dag=dag
)

# Set up task dependencies for comprehensive monitoring
benchmark_task >> data_processing >> api_integration >> database_ops
system_monitor >> health_report
[data_processing, api_integration, database_ops] >> health_report
