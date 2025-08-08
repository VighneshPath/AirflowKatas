"""
Exercise 3 Solution: Complex XCom Patterns

This solution demonstrates:
- Large data handling with file references
- Dynamic XCom key generation
- Conditional XCom processing based on data quality
- Performance optimization and resource cleanup
- Error handling and recovery patterns
"""

from datetime import datetime, timedelta
import json
import tempfile
import os
import random
import time
import hashlib
from typing import Dict, List, Any, Tuple, Optional
from airflow import DAG
from airflow.operators.python import PythonOperator

# DAG configuration
default_args = {
    'owner': 'airflow-kata',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'financial_analytics_pipeline',
    default_args=default_args,
    description='Complex XCom patterns for financial analytics',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['exercise', 'xcom', 'complex-patterns', 'solution']
)

# =============================================================================
# Utility Functions
# =============================================================================


def handle_large_dataset(data: Any, threshold_mb: float = 1.0) -> Dict[str, Any]:
    """Handle large datasets by storing in files if they exceed threshold"""
    json_str = json.dumps(data, default=str)
    size_mb = len(json_str.encode('utf-8')) / (1024 * 1024)

    if size_mb > threshold_mb:
        # Store in file
        temp_file = tempfile.NamedTemporaryFile(
            mode='w',
            delete=False,
            suffix='.json',
            prefix='financial_data_'
        )

        with temp_file as f:
            f.write(json_str)
            file_path = temp_file.name

        return {
            "data_type": "file_reference",
            "file_path": file_path,
            "size_mb": round(size_mb, 2),
            "record_count": len(data) if isinstance(data, list) else 1,
            "created_at": datetime.now().isoformat(),
            "cleanup_required": True
        }
    else:
        # Store directly
        return {
            "data_type": "direct",
            "data": data,
            "size_mb": round(size_mb, 2),
            "created_at": datetime.now().isoformat(),
            "cleanup_required": False
        }


def load_from_reference(data_ref: Dict[str, Any]) -> Any:
    """Load data from file reference or return direct data"""
    if data_ref.get("data_type") == "file_reference":
        file_path = data_ref["file_path"]

        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                return json.load(f)
        else:
            raise FileNotFoundError(f"Referenced file not found: {file_path}")
    else:
        # Direct data
        return data_ref.get("data")


def calculate_quality_score(data: Any, data_type: str) -> Tuple[float, List[str]]:
    """Calculate quality score and identify issues"""
    if not data:
        return 0.0, ["No data available"]

    score = 1.0
    issues = []

    if data_type == "market_data":
        required_fields = ["symbol", "price", "volume", "timestamp"]
        if isinstance(data, list) and data:
            sample = data[0]
            for field in required_fields:
                if field not in sample:
                    score -= 0.2
                    issues.append(f"Missing required field: {field}")

            # Check for null values
            null_count = sum(1 for record in data[:100] if any(
                record.get(f) is None for f in required_fields))
            if null_count > 10:  # More than 10% null values in sample
                score -= 0.3
                issues.append("High null value rate")

    elif data_type == "trading_data":
        if isinstance(data, list):
            # Check for timestamp consistency
            timestamps = [record.get("timestamp")
                          for record in data[:50] if record.get("timestamp")]
            if len(timestamps) < len(data[:50]) * 0.8:
                score -= 0.25
                issues.append("Missing timestamps")

    elif data_type == "risk_parameters":
        required_params = ["var_threshold",
                           "stress_scenarios", "correlation_matrix"]
        if isinstance(data, dict):
            for param in required_params:
                if param not in data:
                    score -= 0.3
                    issues.append(f"Missing risk parameter: {param}")

    # Simulate random quality issues for demonstration
    if random.random() < 0.3:  # 30% chance of additional issues
        additional_issues = ["data corruption",
                             "incomplete records", "format inconsistency"]
        issue = random.choice(additional_issues)
        score -= random.uniform(0.1, 0.4)
        issues.append(issue)

    return max(0.0, score), issues

# =============================================================================
# Task Functions
# =============================================================================


def ingest_data(**context):
    """Ingest data from multiple sources with dynamic XCom key generation"""
    ti = context['task_instance']

    # Generate different types of data sources
    data_sources = {}

    # 1. Market Data (Large dataset - will use file reference)
    market_data = [
        {
            "symbol": f"STOCK_{i:04d}",
            "price": round(random.uniform(10, 1000), 2),
            "volume": random.randint(1000, 100000),
            "timestamp": datetime.now().isoformat(),
            "bid": round(random.uniform(10, 1000), 2),
            "ask": round(random.uniform(10, 1000), 2),
            "metadata": {
                "exchange": random.choice(["NYSE", "NASDAQ", "LSE"]),
                "sector": random.choice(["tech", "finance", "healthcare", "energy"]),
                "market_cap": random.randint(1000000, 100000000000)
            }
        }
        for i in range(5000)  # Large dataset
    ]

    # 2. Trading Data (Medium dataset)
    trading_data = [
        {
            "trade_id": f"TRADE_{i:06d}",
            "symbol": f"STOCK_{random.randint(0, 999):04d}",
            "quantity": random.randint(100, 10000),
            "price": round(random.uniform(10, 1000), 2),
            "timestamp": datetime.now().isoformat() if random.random() > 0.1 else None,  # 10% missing
            "trade_type": random.choice(["BUY", "SELL"]),
            "trader_id": f"TRADER_{random.randint(1, 100):03d}"
        }
        for i in range(1000)
    ]

    # 3. Risk Parameters (Small configuration data)
    risk_parameters = {
        "var_threshold": 0.05,
        "stress_scenarios": [
            {"name": "market_crash", "probability": 0.01, "impact": -0.3},
            {"name": "interest_rate_spike", "probability": 0.05, "impact": -0.15},
            {"name": "currency_devaluation", "probability": 0.03, "impact": -0.2}
        ],
        "correlation_matrix": [[1.0, 0.7, 0.3], [0.7, 1.0, 0.5], [0.3, 0.5, 1.0]],
        "confidence_level": 0.95,
        "holding_period": 1,
        "last_updated": datetime.now().isoformat()
    }

    # 4. External Feeds (Variable size based on random condition)
    feed_size = random.choice([50, 500, 2000])  # Variable size
    external_feeds = [
        {
            "feed_id": f"FEED_{i:04d}",
            "source": random.choice(["Reuters", "Bloomberg", "Yahoo", "Alpha"]),
            "data": f"External data content {i}" * (10 if feed_size > 1000 else 1),
            "timestamp": datetime.now().isoformat(),
            "reliability": random.uniform(0.7, 1.0),
            "corrupted": random.random() < 0.1  # 10% chance of corruption
        }
        for i in range(feed_size)
    ]

    # Handle each data source appropriately
    sources_info = {
        "market_data": market_data,
        "trading_data": trading_data,
        "risk_parameters": risk_parameters,
        "external_feeds": external_feeds
    }

    ingestion_results = {}
    pushed_keys = []

    for source_name, source_data in sources_info.items():
        # Handle large datasets with file references
        processed_data = handle_large_dataset(
            source_data, threshold_mb=0.5)  # Lower threshold for demo

        # Push with dynamic key
        key = f"data_source_{source_name}"
        ti.xcom_push(key=key, value=processed_data)
        pushed_keys.append(key)

        ingestion_results[source_name] = {
            "record_count": processed_data["record_count"],
            "size_mb": processed_data["size_mb"],
            "storage_type": processed_data["data_type"],
            "ingested_at": processed_data["created_at"]
        }

        print(f"Ingested {source_name}: {processed_data['record_count']} records, "
              f"{processed_data['size_mb']}MB, stored as {processed_data['data_type']}")

    # Push ingestion metadata
    metadata = {
        "sources_ingested": list(sources_info.keys()),
        "pushed_keys": pushed_keys,
        "total_sources": len(sources_info),
        "ingestion_completed_at": datetime.now().isoformat(),
        "ingestion_results": ingestion_results
    }
    ti.xcom_push(key="ingestion_metadata", value=metadata)

    print(f"Data ingestion completed: {len(sources_info)} sources processed")

    return {
        "status": "completed",
        "sources_processed": len(sources_info),
        "keys_created": pushed_keys,
        "total_records": sum(result["record_count"] for result in ingestion_results.values()),
        "total_size_mb": sum(result["size_mb"] for result in ingestion_results.values())
    }


def assess_quality(**context):
    """Assess data quality and create conditional XCom outputs"""
    ti = context['task_instance']

    # Get ingestion metadata to know what keys to pull
    metadata = ti.xcom_pull(task_ids='ingest_data', key='ingestion_metadata')
    if not metadata:
        raise ValueError("No ingestion metadata found")

    quality_assessment = {}
    high_quality_data = {}
    medium_quality_data = {}
    low_quality_data = {}
    quality_issues = {}

    # Assess quality for each data source
    for key in metadata['pushed_keys']:
        source_name = key.replace('data_source_', '')
        data_ref = ti.xcom_pull(task_ids='ingest_data', key=key)

        if not data_ref:
            quality_assessment[source_name] = {
                "score": 0.0,
                "issues": ["Data not available"],
                "record_count": 0
            }
            continue

        try:
            # Load data (from file if necessary)
            actual_data = load_from_reference(data_ref)

            # Calculate quality score
            score, issues = calculate_quality_score(actual_data, source_name)

            quality_assessment[source_name] = {
                "score": round(score, 2),
                "issues": issues,
                "record_count": data_ref["record_count"],
                "size_mb": data_ref["size_mb"],
                "storage_type": data_ref["data_type"]
            }

            # Categorize based on quality score
            if score >= 0.8:
                high_quality_data[source_name] = {
                    "data_reference": data_ref,
                    "quality_score": score,
                    "record_count": data_ref["record_count"]
                }
            elif score >= 0.5:
                medium_quality_data[source_name] = {
                    "data_reference": data_ref,
                    "quality_score": score,
                    "record_count": data_ref["record_count"],
                    "issues": issues
                }
            else:
                low_quality_data[source_name] = {
                    "data_reference": data_ref,
                    "quality_score": score,
                    "record_count": data_ref["record_count"],
                    "issues": issues
                }
                quality_issues[source_name] = {
                    "critical_issues": issues,
                    "recommended_action": "manual_review"
                }

            print(
                f"Quality assessment for {source_name}: score={score:.2f}, issues={len(issues)}")

        except Exception as e:
            print(f"Error assessing {source_name}: {str(e)}")
            quality_assessment[source_name] = {
                "score": 0.0,
                "issues": [f"Assessment error: {str(e)}"],
                "record_count": 0
            }

    # Push conditional outputs
    if high_quality_data:
        ti.xcom_push(key='high_quality_data', value=high_quality_data)

    if medium_quality_data:
        ti.xcom_push(key='medium_quality_data', value=medium_quality_data)

    if low_quality_data:
        ti.xcom_push(key='low_quality_data', value=low_quality_data)

    if quality_issues:
        ti.xcom_push(key='quality_issues', value=quality_issues)

    # Calculate overall quality score
    scores = [assessment["score"]
              for assessment in quality_assessment.values()]
    overall_score = sum(scores) / len(scores) if scores else 0.0

    result = {
        "quality_assessment": quality_assessment,
        "overall_quality_score": round(overall_score, 2),
        "high_quality_sources": len(high_quality_data),
        "medium_quality_sources": len(medium_quality_data),
        "low_quality_sources": len(low_quality_data),
        "assessment_completed_at": datetime.now().isoformat()
    }

    print(f"Quality assessment completed: overall score={overall_score:.2f}")
    print(
        f"High quality: {len(high_quality_data)}, Medium: {len(medium_quality_data)}, Low: {len(low_quality_data)}")

    return result


def analyze_risk(**context):
    """Perform risk analysis based on data quality with error handling"""
    ti = context['task_instance']

    # Pull quality assessment results
    quality_results = ti.xcom_pull(task_ids='assess_quality')

    # Pull conditional data based on what's available
    high_quality_data = ti.xcom_pull(
        task_ids='assess_quality', key='high_quality_data') or {}
    medium_quality_data = ti.xcom_pull(
        task_ids='assess_quality', key='medium_quality_data') or {}
    low_quality_data = ti.xcom_pull(
        task_ids='assess_quality', key='low_quality_data') or {}
    quality_issues = ti.xcom_pull(
        task_ids='assess_quality', key='quality_issues') or {}

    risk_analysis_results = {}
    risk_warnings = []
    failed_analyses = {}
    analysis_metadata = {
        "start_time": datetime.now().isoformat(),
        "analyses_attempted": 0,
        "analyses_completed": 0,
        "analyses_failed": 0
    }

    # Analyze high quality data (full analysis)
    for source_name, source_info in high_quality_data.items():
        analysis_metadata["analyses_attempted"] += 1

        try:
            # Load actual data
            actual_data = load_from_reference(source_info["data_reference"])

            # Perform full risk analysis
            if source_name == "market_data":
                # Market risk analysis
                prices = [record["price"]
                          for record in actual_data[:1000]]  # Sample for performance
                volatility = calculate_volatility(prices)
                var_95 = calculate_var(prices, 0.95)

                risk_analysis_results[source_name] = {
                    "analysis_type": "full_market_risk",
                    "volatility": volatility,
                    "var_95": var_95,
                    "risk_level": "high" if volatility > 0.3 else "medium" if volatility > 0.15 else "low",
                    "confidence": 0.95
                }

            elif source_name == "trading_data":
                # Trading risk analysis
                trade_volumes = [record["quantity"]
                                 for record in actual_data if record.get("quantity")]
                avg_volume = sum(trade_volumes) / \
                    len(trade_volumes) if trade_volumes else 0

                risk_analysis_results[source_name] = {
                    "analysis_type": "trading_risk",
                    "average_volume": avg_volume,
                    "volume_risk": "high" if avg_volume > 5000 else "medium" if avg_volume > 2000 else "low",
                    "liquidity_score": min(1.0, avg_volume / 10000),
                    "confidence": 0.9
                }

            analysis_metadata["analyses_completed"] += 1
            print(f"Completed full risk analysis for {source_name}")

        except Exception as e:
            analysis_metadata["analyses_failed"] += 1
            failed_analyses[source_name] = {
                "error": str(e),
                "analysis_type": "full_analysis",
                "failure_time": datetime.now().isoformat()
            }
            print(f"Failed to analyze {source_name}: {str(e)}")

    # Analyze medium quality data (basic analysis with warnings)
    for source_name, source_info in medium_quality_data.items():
        analysis_metadata["analyses_attempted"] += 1

        try:
            # Load actual data
            actual_data = load_from_reference(source_info["data_reference"])

            # Perform basic analysis
            basic_stats = {
                "record_count": len(actual_data) if isinstance(actual_data, list) else 1,
                "data_completeness": source_info["quality_score"],
                "issues_identified": len(source_info.get("issues", []))
            }

            risk_analysis_results[source_name] = {
                "analysis_type": "basic_analysis",
                "basic_statistics": basic_stats,
                "risk_level": "medium",
                "confidence": 0.7,
                "limitations": "Analysis limited due to data quality issues"
            }

            # Add warnings
            risk_warnings.append({
                "source": source_name,
                "warning": f"Medium quality data used for {source_name} analysis",
                "impact": "Reduced confidence in results",
                "issues": source_info.get("issues", [])
            })

            analysis_metadata["analyses_completed"] += 1
            print(
                f"Completed basic risk analysis for {source_name} with warnings")

        except Exception as e:
            analysis_metadata["analyses_failed"] += 1
            failed_analyses[source_name] = {
                "error": str(e),
                "analysis_type": "basic_analysis",
                "failure_time": datetime.now().isoformat()
            }

    # Handle low quality data (skip analysis but log)
    for source_name, source_info in low_quality_data.items():
        risk_warnings.append({
            "source": source_name,
            "warning": f"Skipped analysis for {source_name} due to low data quality",
            "quality_score": source_info["quality_score"],
            "issues": source_info.get("issues", []),
            "recommendation": "Improve data quality before analysis"
        })
        print(
            f"Skipped analysis for {source_name} due to low quality (score: {source_info['quality_score']})")

    # Finalize metadata
    analysis_metadata["end_time"] = datetime.now().isoformat()
    analysis_metadata["success_rate"] = (
        analysis_metadata["analyses_completed"] /
        analysis_metadata["analyses_attempted"]
        if analysis_metadata["analyses_attempted"] > 0 else 0
    )

    # Push results with different keys
    ti.xcom_push(key='risk_analysis_results', value=risk_analysis_results)
    ti.xcom_push(key='risk_warnings', value=risk_warnings)
    ti.xcom_push(key='analysis_metadata', value=analysis_metadata)

    if failed_analyses:
        ti.xcom_push(key='failed_analyses', value=failed_analyses)

    print(
        f"Risk analysis completed: {analysis_metadata['analyses_completed']}/{analysis_metadata['analyses_attempted']} successful")

    return {
        "status": "completed",
        "analyses_completed": analysis_metadata["analyses_completed"],
        "analyses_failed": analysis_metadata["analyses_failed"],
        "warnings_generated": len(risk_warnings),
        "overall_success_rate": analysis_metadata["success_rate"]
    }


def generate_reports(**context):
    """Generate adaptive reports based on available data"""
    ti = context['task_instance']

    # Pull all available data
    quality_results = ti.xcom_pull(task_ids='assess_quality')
    risk_results = ti.xcom_pull(
        task_ids='analyze_risk', key='risk_analysis_results') or {}
    risk_warnings = ti.xcom_pull(
        task_ids='analyze_risk', key='risk_warnings') or []
    analysis_metadata = ti.xcom_pull(
        task_ids='analyze_risk', key='analysis_metadata') or {}
    failed_analyses = ti.xcom_pull(
        task_ids='analyze_risk', key='failed_analyses') or {}

    reports = {}

    # 1. Executive Summary (always generated)
    executive_summary = generate_executive_summary(
        quality_results, risk_results, analysis_metadata
    )
    reports['executive_summary'] = executive_summary

    # 2. Detailed Analysis (only if sufficient high-quality data)
    high_quality_count = quality_results.get(
        'high_quality_sources', 0) if quality_results else 0
    if high_quality_count >= 2:  # Need at least 2 high-quality sources
        detailed_analysis = generate_detailed_analysis(
            risk_results, quality_results)
        reports['detailed_analysis'] = detailed_analysis
    else:
        reports['detailed_analysis'] = {
            "status": "insufficient_data",
            "message": "Detailed analysis requires at least 2 high-quality data sources",
            "available_sources": high_quality_count
        }

    # 3. Quality Report (always generated)
    quality_report = generate_quality_report(quality_results, risk_warnings)
    reports['quality_report'] = quality_report

    # 4. Recommendations (adaptive based on available data)
    recommendations = generate_recommendations(
        quality_results, risk_results, risk_warnings, failed_analyses
    )
    reports['recommendations'] = recommendations

    # Push each report with its own key
    for report_type, report_content in reports.items():
        ti.xcom_push(key=report_type, value=report_content)

    print(f"Generated {len(reports)} reports based on available data")

    return {
        "reports_generated": list(reports.keys()),
        "total_reports": len(reports),
        "generation_completed_at": datetime.now().isoformat(),
        "data_sources_used": {
            "quality_data": bool(quality_results),
            "risk_data": bool(risk_results),
            "warnings": len(risk_warnings),
            "failures": len(failed_analyses)
        }
    }


def cleanup_resources(**context):
    """Clean up temporary files and optimize XCom storage"""
    ti = context['task_instance']

    cleanup_summary = {
        "files_cleaned": [],
        "files_failed": [],
        "xcoms_cleared": [],
        "storage_saved_mb": 0,
        "cleanup_started_at": datetime.now().isoformat()
    }

    # Get ingestion metadata to find files that need cleanup
    metadata = ti.xcom_pull(task_ids='ingest_data', key='ingestion_metadata')

    if metadata:
        # Find and clean up temporary files
        for key in metadata['pushed_keys']:
            data_ref = ti.xcom_pull(task_ids='ingest_data', key=key)

            if data_ref and data_ref.get('cleanup_required') and data_ref.get('data_type') == 'file_reference':
                file_path = data_ref.get('file_path')

                if file_path and os.path.exists(file_path):
                    try:
                        file_size_mb = os.path.getsize(
                            file_path) / (1024 * 1024)
                        os.unlink(file_path)
                        cleanup_summary['files_cleaned'].append({
                            "file_path": file_path,
                            "size_mb": round(file_size_mb, 2),
                            "source": key
                        })
                        cleanup_summary['storage_saved_mb'] += file_size_mb
                        print(
                            f"Cleaned up file: {file_path} ({file_size_mb:.2f}MB)")

                    except Exception as e:
                        cleanup_summary['files_failed'].append({
                            "file_path": file_path,
                            "error": str(e),
                            "source": key
                        })
                        print(f"Failed to clean up {file_path}: {str(e)}")

    # Clean up temporary XCom keys (optional - for demonstration)
    temp_keys_to_clear = ['high_quality_data',
                          'medium_quality_data', 'low_quality_data']

    for key in temp_keys_to_clear:
        data = ti.xcom_pull(task_ids='assess_quality', key=key)
        if data:
            cleanup_summary['xcoms_cleared'].append(key)
            # Note: In practice, XComs are automatically cleaned up by Airflow
            # This is just for demonstration of the cleanup pattern

    cleanup_summary['cleanup_completed_at'] = datetime.now().isoformat()
    cleanup_summary['storage_saved_mb'] = round(
        cleanup_summary['storage_saved_mb'], 2)

    print(f"Cleanup completed: {len(cleanup_summary['files_cleaned'])} files cleaned, "
          f"{cleanup_summary['storage_saved_mb']}MB saved")

    return cleanup_summary

# =============================================================================
# Helper Functions for Risk Analysis and Report Generation
# =============================================================================


def calculate_volatility(prices: List[float]) -> float:
    """Calculate price volatility"""
    if len(prices) < 2:
        return 0.0

    returns = [(prices[i] - prices[i-1]) / prices[i-1]
               for i in range(1, len(prices))]
    mean_return = sum(returns) / len(returns)
    variance = sum((r - mean_return) ** 2 for r in returns) / len(returns)

    return variance ** 0.5


def calculate_var(prices: List[float], confidence: float) -> float:
    """Calculate Value at Risk"""
    if not prices:
        return 0.0

    returns = [(prices[i] - prices[i-1]) / prices[i-1]
               for i in range(1, len(prices))]
    returns.sort()

    index = int((1 - confidence) * len(returns))
    return abs(returns[index]) if index < len(returns) else 0.0


def generate_executive_summary(quality_results, risk_results, analysis_metadata):
    """Generate executive summary report"""
    return {
        "report_type": "executive_summary",
        "overall_quality_score": quality_results.get('overall_quality_score', 0) if quality_results else 0,
        "risk_analyses_completed": analysis_metadata.get('analyses_completed', 0),
        "success_rate": analysis_metadata.get('success_rate', 0),
        "key_findings": [
            f"Data quality score: {quality_results.get('overall_quality_score', 0):.2f}" if quality_results else "No quality data",
            f"Risk analyses completed: {analysis_metadata.get('analyses_completed', 0)}",
            f"Analysis success rate: {analysis_metadata.get('success_rate', 0):.1%}"
        ],
        "generated_at": datetime.now().isoformat()
    }


def generate_detailed_analysis(risk_results, quality_results):
    """Generate detailed analysis report"""
    return {
        "report_type": "detailed_analysis",
        "risk_analysis_details": risk_results,
        "data_quality_breakdown": quality_results.get('quality_assessment', {}) if quality_results else {},
        "analysis_depth": "comprehensive",
        "confidence_level": "high",
        "generated_at": datetime.now().isoformat()
    }


def generate_quality_report(quality_results, risk_warnings):
    """Generate data quality report"""
    return {
        "report_type": "quality_report",
        "overall_score": quality_results.get('overall_quality_score', 0) if quality_results else 0,
        "source_breakdown": quality_results.get('quality_assessment', {}) if quality_results else {},
        "warnings_count": len(risk_warnings),
        "quality_issues": [warning for warning in risk_warnings if 'quality' in warning.get('warning', '').lower()],
        "generated_at": datetime.now().isoformat()
    }


def generate_recommendations(quality_results, risk_results, risk_warnings, failed_analyses):
    """Generate adaptive recommendations"""
    recommendations = []

    if quality_results:
        if quality_results.get('overall_quality_score', 0) < 0.7:
            recommendations.append(
                "Improve data quality processes to achieve better analysis results")

        if quality_results.get('low_quality_sources', 0) > 0:
            recommendations.append(
                "Review and remediate low-quality data sources")

    if risk_warnings:
        recommendations.append(
            "Address data quality warnings to improve risk analysis confidence")

    if failed_analyses:
        recommendations.append(
            "Investigate and resolve failed analysis components")

    if not recommendations:
        recommendations.append(
            "Continue monitoring data quality and risk metrics")

    return {
        "report_type": "recommendations",
        "recommendations": recommendations,
        "priority": "high" if len(recommendations) > 2 else "medium",
        "generated_at": datetime.now().isoformat()
    }

# =============================================================================
# Task Definitions
# =============================================================================


ingest_task = PythonOperator(
    task_id='ingest_data',
    python_callable=ingest_data,
    dag=dag
)

quality_task = PythonOperator(
    task_id='assess_quality',
    python_callable=assess_quality,
    dag=dag
)

risk_task = PythonOperator(
    task_id='analyze_risk',
    python_callable=analyze_risk,
    dag=dag
)

reports_task = PythonOperator(
    task_id='generate_reports',
    python_callable=generate_reports,
    dag=dag
)

cleanup_task = PythonOperator(
    task_id='cleanup_resources',
    python_callable=cleanup_resources,
    dag=dag
)

# Set up task dependencies
ingest_task >> quality_task >> risk_task >> reports_task >> cleanup_task
