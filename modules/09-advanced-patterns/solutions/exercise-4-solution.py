"""
Solution for Exercise 4: Performance Tuning

This solution demonstrates comprehensive performance tuning techniques
for optimizing Airflow pipeline performance and resource utilization.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
import time
import psutil
import logging
import gc
import threading
from typing import Dict, List, Any, Generator
import json

# Performance-optimized default arguments
default_args = {
    'owner': 'airflow-kata',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'performance_tuning_pipeline',
    default_args=default_args,
    description='Performance tuning and optimization techniques',
    schedule_interval=timedelta(hours=4),
    catchup=False,
    max_active_runs=2,  # Limit concurrent runs to prevent resource exhaustion
    tags=['exercise', 'performance', 'optimization']
)

# Global cache for expensive computations
COMPUTATION_CACHE = {}
CACHE_LOCK = threading.Lock()

# Performance monitoring utilities


class PerformanceMonitor:
    """Utility class for monitoring task performance"""

    def __init__(self, task_name: str):
        self.task_name = task_name
        self.start_time = None
        self.start_memory = None
        self.metrics = {}

    def __enter__(self):
        self.start_time = time.time()
        self.start_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        print(f"🔍 Starting performance monitoring for {self.task_name}")
        print(f"   Initial memory usage: {self.start_memory:.2f} MB")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        end_time = time.time()
        end_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB

        self.metrics = {
            'task_name': self.task_name,
            'execution_time': end_time - self.start_time,
            'start_memory_mb': self.start_memory,
            'end_memory_mb': end_memory,
            'memory_delta_mb': end_memory - self.start_memory,
            'peak_memory_mb': end_memory,  # Simplified for demo
            'cpu_percent': psutil.cpu_percent(),
            'timestamp': datetime.now().isoformat()
        }

        print(f"📊 Performance metrics for {self.task_name}:")
        print(
            f"   Execution time: {self.metrics['execution_time']:.2f} seconds")
        print(
            f"   Memory usage: {self.start_memory:.2f} MB → {end_memory:.2f} MB")
        print(f"   Memory delta: {self.metrics['memory_delta_mb']:.2f} MB")
        print(f"   CPU usage: {self.metrics['cpu_percent']:.1f}%")


def memory_optimized_processing(dataset_size: int = 100000, chunk_size: int = 10000, **context):
    """
    Process large dataset in memory-efficient chunks

    Args:
        dataset_size (int): Total number of records to process
        chunk_size (int): Size of each processing chunk
    """

    with PerformanceMonitor("memory_optimized_processing") as monitor:
        print(f"🧠 Starting memory-optimized processing")
        print(f"   Dataset size: {dataset_size:,} records")
        print(f"   Chunk size: {chunk_size:,} records")

        def data_generator(size: int) -> Generator[Dict[str, Any], None, None]:
            """Generator function for memory-efficient data iteration"""
            for i in range(size):
                yield {
                    'id': i,
                    'data': f'record_{i}',
                    'value': i * 2,
                    'processed': False
                }

        def process_chunk(chunk_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
            """Process a chunk of data"""
            processed_chunk = []
            for record in chunk_data:
                # Simulate processing
                processed_record = {
                    'id': record['id'],
                    'data': record['data'].upper(),
                    'value': record['value'] * 1.5,
                    'processed': True,
                    'processing_timestamp': time.time()
                }
                processed_chunk.append(processed_record)
            return processed_chunk

        total_processed = 0
        chunk_count = 0

        # Process data in chunks using generator pattern
        data_gen = data_generator(dataset_size)

        while True:
            # Collect chunk data
            chunk_data = []
            try:
                for _ in range(chunk_size):
                    chunk_data.append(next(data_gen))
            except StopIteration:
                if not chunk_data:
                    break

            chunk_count += 1
            chunk_start_time = time.time()

            print(
                f"   Processing chunk {chunk_count}: {len(chunk_data):,} records")

            # Process the chunk
            processed_chunk = process_chunk(chunk_data)
            total_processed += len(processed_chunk)

            # Simulate writing processed data (in real scenario, would write to storage)
            chunk_processing_time = time.time() - chunk_start_time
            print(
                f"   ✅ Chunk {chunk_count} completed in {chunk_processing_time:.2f}s")

            # Explicit cleanup to free memory
            del chunk_data
            del processed_chunk
            gc.collect()  # Force garbage collection

            # Memory usage check
            current_memory = psutil.Process().memory_info().rss / 1024 / 1024
            print(
                f"   Memory after chunk {chunk_count}: {current_memory:.2f} MB")

        print(f"✅ Memory-optimized processing completed:")
        print(f"   Total records processed: {total_processed:,}")
        print(f"   Total chunks: {chunk_count}")
        print(
            f"   Average chunk size: {total_processed / chunk_count:.0f} records")

        return {
            'total_processed': total_processed,
            'chunk_count': chunk_count,
            'processing_mode': 'memory_optimized',
            'performance_metrics': monitor.metrics
        }


def optimized_database_operations(**context):
    """
    Demonstrate optimized database operations with connection pooling
    """

    with PerformanceMonitor("database_operations") as monitor:
        print("🔗 Starting optimized database operations")

        # Simulate connection pool configuration
        connection_pool_config = {
            'pool_size': 10,
            'max_overflow': 20,
            'pool_timeout': 30,
            'pool_recycle': 3600,
            'pool_pre_ping': True
        }

        print(f"   Connection pool config: {connection_pool_config}")

        # Simulate database operations with connection reuse
        operations = [
            'SELECT * FROM customers WHERE active = true',
            'SELECT * FROM orders WHERE date >= CURRENT_DATE - INTERVAL 7 DAY',
            'SELECT * FROM products WHERE category = "electronics"',
            'UPDATE inventory SET last_updated = NOW()',
            'INSERT INTO audit_log (operation, timestamp) VALUES ("batch_process", NOW())'
        ]

        # Simulate connection pool statistics
        connection_stats = {
            'connections_created': 1,  # Reuse existing connections
            'connections_reused': len(operations) - 1,
            'total_operations': len(operations),
            'pool_utilization': 0.3,  # 30% of pool used
            'avg_operation_time': 0.05,
            'connection_wait_time': 0.0  # No waiting due to good pool sizing
        }

        print("   Executing database operations with connection pooling:")

        for i, operation in enumerate(operations, 1):
            operation_start = time.time()

            # Simulate getting connection from pool (fast)
            print(f"   Operation {i}: Getting connection from pool...")
            time.sleep(0.001)  # Minimal time to get pooled connection

            # Simulate query execution
            print(f"   Operation {i}: {operation[:50]}...")
            time.sleep(0.05)  # Simulate query execution time

            # Simulate returning connection to pool
            print(f"   Operation {i}: Returning connection to pool")

            operation_time = time.time() - operation_start
            print(f"   ✅ Operation {i} completed in {operation_time:.3f}s")

        # Simulate batch operations for efficiency
        print("   Executing batch operations:")
        batch_operations = [
            "INSERT INTO processed_records (id, status) VALUES",
            "UPDATE customer_metrics SET last_calculated = NOW() WHERE",
            "DELETE FROM temp_processing WHERE created_at <"
        ]

        for batch_op in batch_operations:
            print(
                f"   Batch operation: {batch_op}... (processing 1000 records)")
            time.sleep(0.1)  # Simulate batch operation
            print(f"   ✅ Batch operation completed")

        # Connection pool health check
        pool_health = {
            'active_connections': 3,
            'idle_connections': 7,
            'total_connections': 10,
            'failed_connections': 0,
            'pool_overflow': 0
        }

        print(f"🔗 Database optimization completed:")
        print(
            f"   Connection reuse ratio: {connection_stats['connections_reused'] / connection_stats['total_operations']:.1%}")
        print(
            f"   Average operation time: {connection_stats['avg_operation_time']:.3f}s")
        print(
            f"   Pool utilization: {connection_stats['pool_utilization']:.1%}")
        print(f"   Pool health: {pool_health}")

        return {
            'connection_stats': connection_stats,
            'pool_health': pool_health,
            'optimization_type': 'connection_pooling'
        }


def cpu_intensive_function(task_name: str, **context):
    """Simulate CPU-intensive task for resource pool demonstration"""

    with PerformanceMonitor(f"cpu_intensive_{task_name}") as monitor:
        print(f"🔥 Starting CPU-intensive task: {task_name}")
        print(f"   Allocated to CPU resource pool")

        # Simulate CPU-intensive computation
        computation_steps = [
            "Mathematical calculations",
            "Data transformation algorithms",
            "Statistical analysis",
            "Machine learning model training",
            "Complex aggregations"
        ]

        for i, step in enumerate(computation_steps, 1):
            print(f"   CPU Step {i}/5: {step}")

            # Simulate CPU-intensive work
            start_time = time.time()
            while time.time() - start_time < 0.3:  # 300ms of CPU work
                # Simulate computation
                result = sum(x**2 for x in range(1000))

            cpu_usage = psutil.cpu_percent(interval=0.1)
            print(f"   ✅ Step {i} completed (CPU usage: {cpu_usage:.1f}%)")

        print(f"✅ CPU-intensive task {task_name} completed")
        return {
            'task_name': task_name,
            'task_type': 'cpu_intensive',
            'resource_pool': 'cpu_pool',
            'completion_status': 'success'
        }


def memory_intensive_function(task_name: str, **context):
    """Simulate memory-intensive task for resource pool demonstration"""

    with PerformanceMonitor(f"memory_intensive_{task_name}") as monitor:
        print(f"🧠 Starting memory-intensive task: {task_name}")
        print(f"   Allocated to memory resource pool")

        # Simulate memory-intensive operations
        memory_operations = [
            "Loading large datasets into memory",
            "Creating in-memory data structures",
            "Caching frequently accessed data",
            "Building lookup tables"
        ]

        large_data_structures = []

        for i, operation in enumerate(memory_operations, 1):
            print(f"   Memory Step {i}/4: {operation}")

            # Simulate memory allocation
            data_structure = [f"data_item_{j}" for j in range(10000)]
            large_data_structures.append(data_structure)

            current_memory = psutil.Process().memory_info().rss / 1024 / 1024
            print(
                f"   ✅ Step {i} completed (Memory usage: {current_memory:.2f} MB)")

            time.sleep(0.2)  # Simulate processing time

        # Cleanup memory
        del large_data_structures
        gc.collect()

        final_memory = psutil.Process().memory_info().rss / 1024 / 1024
        print(f"✅ Memory-intensive task {task_name} completed")
        print(f"   Final memory usage: {final_memory:.2f} MB")

        return {
            'task_name': task_name,
            'task_type': 'memory_intensive',
            'resource_pool': 'memory_pool',
            'completion_status': 'success'
        }


def io_intensive_function(task_name: str, **context):
    """Simulate I/O-intensive task for resource pool demonstration"""

    with PerformanceMonitor(f"io_intensive_{task_name}") as monitor:
        print(f"💾 Starting I/O-intensive task: {task_name}")
        print(f"   Allocated to I/O resource pool")

        # Simulate I/O-intensive operations
        io_operations = [
            "Reading large files from disk",
            "Writing processed data to storage",
            "Database bulk operations",
            "Network data transfers",
            "File system operations"
        ]

        for i, operation in enumerate(io_operations, 1):
            print(f"   I/O Step {i}/5: {operation}")

            # Simulate I/O wait time
            time.sleep(0.2)  # Simulate I/O latency

            # Simulate data transfer
            data_size = 1024 * 1024 * 10  # 10MB
            transfer_rate = data_size / 0.2  # MB/s

            print(
                f"   ✅ Step {i} completed (Transfer rate: {transfer_rate / 1024 / 1024:.1f} MB/s)")

        print(f"✅ I/O-intensive task {task_name} completed")
        return {
            'task_name': task_name,
            'task_type': 'io_intensive',
            'resource_pool': 'io_pool',
            'completion_status': 'success'
        }


def cached_computation(computation_key: str, expensive_function_name: str, **context):
    """
    Implement caching for expensive computations

    Args:
        computation_key (str): Unique key for the computation
        expensive_function_name (str): Name of the expensive function to cache
    """

    with PerformanceMonitor(f"cached_computation_{computation_key}") as monitor:
        print(f"💾 Starting cached computation: {computation_key}")

        # Thread-safe cache access
        with CACHE_LOCK:
            cache_hit = computation_key in COMPUTATION_CACHE

            if cache_hit:
                print(f"   ✅ Cache HIT for {computation_key}")
                cached_result = COMPUTATION_CACHE[computation_key]

                # Simulate fast cache retrieval
                time.sleep(0.01)

                print(f"   Retrieved from cache in 0.01s")
                print(f"   Cache entry created: {cached_result['created_at']}")

                return {
                    'computation_key': computation_key,
                    'cache_status': 'hit',
                    'result': cached_result['result'],
                    'retrieval_time': 0.01,
                    'time_saved': cached_result['original_computation_time'] - 0.01
                }
            else:
                print(f"   ❌ Cache MISS for {computation_key}")
                print(
                    f"   Executing expensive computation: {expensive_function_name}")

        # Simulate expensive computation
        computation_start = time.time()

        # Different expensive computations based on function name
        if expensive_function_name == "complex_analytics":
            print("   Performing complex analytics calculations...")
            time.sleep(2.0)  # Simulate 2 seconds of computation
            result = {"analytics_score": 0.85, "segments": ["A", "B", "C"]}
        elif expensive_function_name == "ml_model_inference":
            print("   Running machine learning model inference...")
            time.sleep(1.5)  # Simulate 1.5 seconds of computation
            result = {"prediction": 0.92, "confidence": 0.88}
        elif expensive_function_name == "data_aggregation":
            print("   Performing large-scale data aggregation...")
            time.sleep(1.0)  # Simulate 1 second of computation
            result = {"total_records": 1000000, "aggregated_value": 12345.67}
        else:
            print("   Performing generic expensive computation...")
            time.sleep(1.2)  # Default computation time
            result = {"computation_result": "completed", "value": 42}

        computation_time = time.time() - computation_start

        # Store result in cache
        with CACHE_LOCK:
            COMPUTATION_CACHE[computation_key] = {
                'result': result,
                'created_at': datetime.now().isoformat(),
                'original_computation_time': computation_time,
                'access_count': 1
            }

        print(f"   ✅ Computation completed in {computation_time:.2f}s")
        print(f"   💾 Result cached for future use")

        return {
            'computation_key': computation_key,
            'cache_status': 'miss',
            'result': result,
            'computation_time': computation_time,
            'cached': True
        }


def incremental_data_processing(**context):
    """
    Process only new/changed data since last run
    """

    with PerformanceMonitor("incremental_processing") as monitor:
        print("📈 Starting incremental data processing")

        # Simulate getting last processing timestamp
        last_processed_timestamp = context.get('prev_execution_date')
        current_timestamp = context['execution_date']

        if last_processed_timestamp:
            print(f"   Last processed: {last_processed_timestamp}")
            print(f"   Current run: {current_timestamp}")

            # Simulate identifying incremental data
            time_diff = (current_timestamp -
                         last_processed_timestamp).total_seconds()

            # Simulate incremental data volume based on time difference
            # 1000 records per hour, max 50k
            incremental_records = min(int(time_diff / 3600 * 1000), 50000)
            total_records = 1000000  # Total records in dataset

            processing_ratio = incremental_records / total_records
            time_saved_ratio = 1 - processing_ratio

            print(f"   Time since last run: {time_diff / 3600:.1f} hours")
            print(f"   Incremental records: {incremental_records:,}")
            print(f"   Total records: {total_records:,}")
            print(f"   Processing only {processing_ratio:.1%} of data")
            print(f"   Time saved: {time_saved_ratio:.1%}")

            # Simulate incremental processing
            processing_steps = [
                "Identifying changed records",
                "Loading incremental data",
                "Processing new/changed records",
                "Updating derived tables",
                "Refreshing materialized views"
            ]

            total_processing_time = 0
            for i, step in enumerate(processing_steps, 1):
                step_start = time.time()
                print(f"   Step {i}/5: {step}")

                # Processing time proportional to incremental data
                step_time = 0.1 * processing_ratio + 0.05  # Base time + proportional time
                time.sleep(step_time)

                step_duration = time.time() - step_start
                total_processing_time += step_duration
                print(f"   ✅ Step {i} completed in {step_duration:.2f}s")

            # Compare with full processing time
            estimated_full_processing_time = 5.0  # Estimated time for full processing
            actual_time_saved = estimated_full_processing_time - total_processing_time

            result = {
                'incremental_records': incremental_records,
                'total_records': total_records,
                'processing_ratio': processing_ratio,
                'time_saved_ratio': time_saved_ratio,
                'processing_time': total_processing_time,
                'estimated_full_time': estimated_full_processing_time,
                'actual_time_saved': actual_time_saved,
                'efficiency_gain': actual_time_saved / estimated_full_processing_time
            }

        else:
            print("   No previous run found - performing full processing")

            # Simulate full processing for first run
            time.sleep(2.0)

            result = {
                'incremental_records': 1000000,
                'total_records': 1000000,
                'processing_ratio': 1.0,
                'time_saved_ratio': 0.0,
                'processing_time': 2.0,
                'first_run': True
            }

        print(f"📈 Incremental processing completed:")
        print(f"   Processing time: {result['processing_time']:.2f}s")
        if 'actual_time_saved' in result:
            print(f"   Time saved: {result['actual_time_saved']:.2f}s")
            print(f"   Efficiency gain: {result['efficiency_gain']:.1%}")

        return result


def performance_monitor_task(task_name: str, **context):
    """
    Monitor and log performance metrics for tasks

    Args:
        task_name (str): Name of the task being monitored
    """

    print(f"📊 Starting performance monitoring for: {task_name}")

    # Collect system metrics
    system_metrics = {
        'cpu_percent': psutil.cpu_percent(interval=1),
        'memory_percent': psutil.virtual_memory().percent,
        'disk_usage_percent': psutil.disk_usage('/').percent,
        'load_average': psutil.getloadavg()[0] if hasattr(psutil, 'getloadavg') else 0,
        'process_count': len(psutil.pids()),
        'timestamp': datetime.now().isoformat()
    }

    # Simulate task-specific metrics
    task_metrics = {
        'task_name': task_name,
        'execution_time': 45.2,
        'records_processed': 50000,
        'throughput': 1111,  # records per second
        'error_count': 0,
        'retry_count': 0,
        'memory_peak_mb': 256,
        'cache_hit_ratio': 0.85
    }

    # Performance analysis
    performance_analysis = {
        'status': 'optimal',
        'bottlenecks': [],
        'recommendations': []
    }

    # Analyze metrics and generate insights
    if system_metrics['cpu_percent'] > 80:
        performance_analysis['bottlenecks'].append('high_cpu_usage')
        performance_analysis['recommendations'].append(
            'Consider CPU optimization or scaling')
        performance_analysis['status'] = 'needs_attention'

    if system_metrics['memory_percent'] > 85:
        performance_analysis['bottlenecks'].append('high_memory_usage')
        performance_analysis['recommendations'].append(
            'Implement memory optimization techniques')
        performance_analysis['status'] = 'needs_attention'

    if task_metrics['throughput'] < 500:
        performance_analysis['bottlenecks'].append('low_throughput')
        performance_analysis['recommendations'].append(
            'Optimize data processing algorithms')
        performance_analysis['status'] = 'needs_attention'

    if task_metrics['cache_hit_ratio'] < 0.7:
        performance_analysis['bottlenecks'].append('low_cache_efficiency')
        performance_analysis['recommendations'].append(
            'Review caching strategy and cache size')
        performance_analysis['status'] = 'needs_attention'

    # Generate performance report
    performance_report = {
        'system_metrics': system_metrics,
        'task_metrics': task_metrics,
        'performance_analysis': performance_analysis,
        'monitoring_timestamp': datetime.now().isoformat()
    }

    print(f"📊 Performance monitoring completed for {task_name}:")
    print(f"   System CPU: {system_metrics['cpu_percent']:.1f}%")
    print(f"   System Memory: {system_metrics['memory_percent']:.1f}%")
    print(f"   Task throughput: {task_metrics['throughput']:,} records/sec")
    print(f"   Cache hit ratio: {task_metrics['cache_hit_ratio']:.1%}")
    print(f"   Performance status: {performance_analysis['status'].upper()}")

    if performance_analysis['recommendations']:
        print(f"   Recommendations:")
        for rec in performance_analysis['recommendations']:
            print(f"   - {rec}")

    return performance_report


# Build the DAG
with dag:
    start = DummyOperator(task_id='start')

    # Memory Optimization Group
    with TaskGroup("memory_optimization") as memory_group:
        memory_processing = PythonOperator(
            task_id='memory_optimized_processing',
            python_callable=memory_optimized_processing,
            op_kwargs={'dataset_size': 100000, 'chunk_size': 10000},
            pool='memory_intensive_pool'  # Use memory resource pool
        )

        memory_monitoring = PythonOperator(
            task_id='monitor_memory_performance',
            python_callable=performance_monitor_task,
            op_kwargs={'task_name': 'memory_processing'}
        )

        memory_processing >> memory_monitoring

    # Database Optimization Group
    with TaskGroup("database_optimization") as db_group:
        db_operations = PythonOperator(
            task_id='optimized_database_operations',
            python_callable=optimized_database_operations,
            pool='database_pool'  # Use database connection pool
        )

        db_monitoring = PythonOperator(
            task_id='monitor_database_performance',
            python_callable=performance_monitor_task,
            op_kwargs={'task_name': 'database_operations'}
        )

        db_operations >> db_monitoring

    # Resource Pool Management Group
    with TaskGroup("resource_pool_management") as resource_group:
        # CPU-intensive tasks
        cpu_task_1 = PythonOperator(
            task_id='cpu_intensive_analytics',
            python_callable=cpu_intensive_function,
            op_kwargs={'task_name': 'analytics'},
            pool='cpu_intensive_pool',
            pool_slots=2
        )

        cpu_task_2 = PythonOperator(
            task_id='cpu_intensive_modeling',
            python_callable=cpu_intensive_function,
            op_kwargs={'task_name': 'modeling'},
            pool='cpu_intensive_pool',
            pool_slots=2
        )

        # Memory-intensive tasks
        memory_task_1 = PythonOperator(
            task_id='memory_intensive_caching',
            python_callable=memory_intensive_function,
            op_kwargs={'task_name': 'caching'},
            pool='memory_intensive_pool',
            pool_slots=1
        )

        memory_task_2 = PythonOperator(
            task_id='memory_intensive_aggregation',
            python_callable=memory_intensive_function,
            op_kwargs={'task_name': 'aggregation'},
            pool='memory_intensive_pool',
            pool_slots=1
        )

        # I/O-intensive tasks
        io_task_1 = PythonOperator(
            task_id='io_intensive_file_processing',
            python_callable=io_intensive_function,
            op_kwargs={'task_name': 'file_processing'},
            pool='io_intensive_pool',
            pool_slots=3
        )

        io_task_2 = PythonOperator(
            task_id='io_intensive_data_transfer',
            python_callable=io_intensive_function,
            op_kwargs={'task_name': 'data_transfer'},
            pool='io_intensive_pool',
            pool_slots=3
        )

        # Resource monitoring
        resource_monitoring = PythonOperator(
            task_id='monitor_resource_utilization',
            python_callable=performance_monitor_task,
            op_kwargs={'task_name': 'resource_pools'}
        )

        # Parallel execution by resource type
        [cpu_task_1, cpu_task_2, memory_task_1, memory_task_2,
            io_task_1, io_task_2] >> resource_monitoring

    # Caching Optimization Group
    with TaskGroup("caching_optimization") as caching_group:
        # Cached computations
        cached_analytics = PythonOperator(
            task_id='cached_complex_analytics',
            python_callable=cached_computation,
            op_kwargs={
                'computation_key': 'analytics_{{ ds }}',
                'expensive_function_name': 'complex_analytics'
            }
        )

        cached_ml_inference = PythonOperator(
            task_id='cached_ml_inference',
            python_callable=cached_computation,
            op_kwargs={
                'computation_key': 'ml_model_{{ ds }}',
                'expensive_function_name': 'ml_model_inference'
            }
        )

        cached_aggregation = PythonOperator(
            task_id='cached_data_aggregation',
            python_callable=cached_computation,
            op_kwargs={
                'computation_key': 'aggregation_{{ ds }}',
                'expensive_function_name': 'data_aggregation'
            }
        )

        # Cache performance monitoring
        cache_monitoring = PythonOperator(
            task_id='monitor_cache_performance',
            python_callable=performance_monitor_task,
            op_kwargs={'task_name': 'caching_system'}
        )

        [cached_analytics, cached_ml_inference,
            cached_aggregation] >> cache_monitoring

    # Incremental Processing Group
    with TaskGroup("incremental_processing") as incremental_group:
        incremental_processing_task = PythonOperator(
            task_id='incremental_data_processing',
            python_callable=incremental_data_processing
        )

        incremental_monitoring = PythonOperator(
            task_id='monitor_incremental_performance',
            python_callable=performance_monitor_task,
            op_kwargs={'task_name': 'incremental_processing'}
        )

        incremental_processing_task >> incremental_monitoring

    # Overall Performance Analysis
    overall_performance_analysis = PythonOperator(
        task_id='overall_performance_analysis',
        python_callable=lambda: print(
            "📊 Overall performance analysis completed - all optimizations working effectively")
    )

    # Performance Report Generation
    generate_performance_report = BashOperator(
        task_id='generate_performance_report',
        bash_command='echo "📋 Comprehensive performance report generated and sent to stakeholders"'
    )

    end = DummyOperator(task_id='end')

    # Define workflow dependencies
    start >> [memory_group, db_group, resource_group,
              caching_group, incremental_group]
    [memory_group, db_group, resource_group, caching_group,
        incremental_group] >> overall_performance_analysis
    overall_performance_analysis >> generate_performance_report >> end
