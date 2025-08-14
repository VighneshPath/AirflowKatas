"""
Trigger Examples for Airflow Coding Kata

This module demonstrates various trigger patterns and deferrable operators
in Apache Airflow. Triggers provide an efficient, event-driven alternative
to traditional sensors.

Key concepts demonstrated:
- Deferrable sensors using triggers
- Custom trigger implementation
- Trigger-based file monitoring
- Async event handling
- Resource-efficient waiting patterns
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from airflow.exceptions import AirflowSensorTimeout, AirflowException
import asyncio
import os
from typing import Tuple, Dict, Any, AsyncIterator

# DAG Configuration
default_args = {
    'owner': 'kata-triggers',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'trigger_examples',
    default_args=default_args,
    description='Examples of triggers and deferrable operators',
    schedule_interval=None,  # Manual trigger
    catchup=False,
    tags=['kata', 'triggers', 'sensors', 'async'],
)

# Example 1: Basic Deferrable File Sensor
# This demonstrates the simplest way to use triggers - just add deferrable=True

deferrable_file_sensor = FileSensor(
    task_id='wait_for_file_deferrable',
    filepath='/tmp/trigger_test_file.txt',
    deferrable=True,  # This enables trigger-based execution
    poke_interval=10,  # Check every 10 seconds
    timeout=300,       # 5 minute timeout
    dag=dag,
)

# Example 2: Deferrable Time Sensor
# Wait for a specific time duration using triggers

time_sensor_deferrable = TimeDeltaSensor(
    task_id='wait_30_seconds_deferrable',
    delta=timedelta(seconds=30),
    deferrable=True,  # Use trigger instead of blocking worker
    dag=dag,
)

# Example 3: Custom Trigger Implementation
# This shows how to create your own trigger for custom conditions


class CustomFileTrigger(BaseTrigger):
    """
    Custom trigger that monitors multiple files and succeeds when any file appears.

    This demonstrates:
    - Custom trigger logic
    - Async/await patterns
    - Event emission
    - Proper serialization
    """

    def __init__(self, filepaths: list, check_interval: int = 10):
        super().__init__()
        self.filepaths = filepaths
        self.check_interval = check_interval

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serialize trigger for storage and reconstruction."""
        return (
            "modules.05-sensors-triggers.examples.trigger_examples.CustomFileTrigger",
            {
                "filepaths": self.filepaths,
                "check_interval": self.check_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Main trigger logic - runs asynchronously in the triggerer process.

        This method should:
        1. Check conditions asynchronously
        2. Yield TriggerEvent when condition is met
        3. Handle timeouts and errors gracefully
        """
        start_time = asyncio.get_event_loop().time()

        while True:
            # Check if any of the files exist
            for filepath in self.filepaths:
                if os.path.exists(filepath):
                    # File found! Emit success event
                    yield TriggerEvent({
                        "status": "success",
                        "filepath": filepath,
                        "message": f"File found: {filepath}",
                        "wait_time": asyncio.get_event_loop().time() - start_time
                    })
                    return

            # No files found, wait before checking again
            await asyncio.sleep(self.check_interval)


class MultiFileSensorAsync(BaseSensorOperator):
    """
    Deferrable sensor that waits for any of multiple files to appear.

    This demonstrates:
    - Custom deferrable sensor implementation
    - Using custom triggers
    - Proper event handling
    - Fallback to regular sensor mode
    """

    def __init__(self, filepaths: list, check_interval: int = 10, **kwargs):
        super().__init__(**kwargs)
        self.filepaths = filepaths
        self.check_interval = check_interval

    def execute(self, context: Context):
        """Main execution method - decides between sync and async modes."""
        if not self.deferrable:
            # Fallback to synchronous polling
            return self.poke(context)

        # Use trigger for asynchronous execution
        self.defer(
            trigger=CustomFileTrigger(
                filepaths=self.filepaths,
                check_interval=self.check_interval
            ),
            method_name="execute_complete"
        )

    def poke(self, context: Context) -> bool:
        """Synchronous polling logic (fallback)."""
        for filepath in self.filepaths:
            if os.path.exists(filepath):
                self.log.info(f"File found: {filepath}")
                return True
        return False

    def execute_complete(self, context: Context, event: Dict[str, Any]):
        """
        Handle trigger completion.

        This method is called when the trigger emits an event.
        It should process the event and return the result.
        """
        if event.get("status") == "success":
            filepath = event.get("filepath")
            wait_time = event.get("wait_time", 0)

            self.log.info(f"Trigger completed successfully!")
            self.log.info(f"Found file: {filepath}")
            self.log.info(f"Wait time: {wait_time:.2f} seconds")

            return {
                "found_file": filepath,
                "wait_time": wait_time
            }
        else:
            raise AirflowException(f"Trigger failed: {event}")


# Example 4: Using the Custom Deferrable Sensor

multi_file_sensor = MultiFileSensorAsync(
    task_id='wait_for_any_file',
    filepaths=[
        '/tmp/file1.txt',
        '/tmp/file2.txt',
        '/tmp/file3.txt'
    ],
    deferrable=True,
    check_interval=5,
    timeout=300,
    dag=dag,
)

# Example 5: Advanced Trigger with External API Monitoring


class ApiHealthTrigger(BaseTrigger):
    """
    Trigger that monitors API health endpoint.

    Demonstrates:
    - HTTP requests in triggers
    - Error handling
    - Configurable retry logic
    """

    def __init__(self, api_url: str, expected_status: int = 200,
                 check_interval: int = 30, max_retries: int = 3):
        super().__init__()
        self.api_url = api_url
        self.expected_status = expected_status
        self.check_interval = check_interval
        self.max_retries = max_retries

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        return (
            "modules.05-sensors-triggers.examples.trigger_examples.ApiHealthTrigger",
            {
                "api_url": self.api_url,
                "expected_status": self.expected_status,
                "check_interval": self.check_interval,
                "max_retries": self.max_retries,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Monitor API health asynchronously."""
        import aiohttp

        retry_count = 0

        async with aiohttp.ClientSession() as session:
            while retry_count < self.max_retries:
                try:
                    async with session.get(self.api_url, timeout=10) as response:
                        if response.status == self.expected_status:
                            yield TriggerEvent({
                                "status": "success",
                                "api_status": response.status,
                                "message": f"API healthy: {self.api_url}",
                                "retry_count": retry_count
                            })
                            return
                        else:
                            self.log.warning(
                                f"API returned status {response.status}, expected {self.expected_status}")

                except asyncio.TimeoutError:
                    self.log.warning(f"API request timed out: {self.api_url}")
                except Exception as e:
                    self.log.warning(f"API request failed: {e}")

                retry_count += 1
                if retry_count < self.max_retries:
                    await asyncio.sleep(self.check_interval)

            # Max retries exceeded
            yield TriggerEvent({
                "status": "failed",
                "message": f"API health check failed after {self.max_retries} retries",
                "retry_count": retry_count
            })


# Example 6: Processing Tasks that Run After Triggers

def process_file_data(**context):
    """
    Process file data after sensor completes.

    This demonstrates how to access trigger results in downstream tasks.
    """
    # Get data from the sensor task
    sensor_result = context['task_instance'].xcom_pull(
        task_ids='wait_for_any_file')

    if sensor_result:
        found_file = sensor_result.get('found_file')
        wait_time = sensor_result.get('wait_time', 0)

        print(f"Processing file: {found_file}")
        print(f"File was detected after {wait_time:.2f} seconds")

        # Simulate file processing
        if found_file:
            with open(found_file, 'r') as f:
                content = f.read()
                print(f"File content length: {len(content)} characters")

        return {
            "processed_file": found_file,
            "processing_time": wait_time,
            "status": "completed"
        }
    else:
        print("No file data received from sensor")
        return {"status": "no_data"}


process_file_task = PythonOperator(
    task_id='process_detected_file',
    python_callable=process_file_data,
    dag=dag,
)

# Example 7: Setup and Cleanup Tasks

setup_test_environment = BashOperator(
    task_id='setup_test_files',
    bash_command='''
    echo "Setting up test environment for triggers..."
    mkdir -p /tmp/trigger_test
    
    # Create test file after a delay (simulates external process)
    (sleep 15 && echo "Test data" > /tmp/trigger_test_file.txt) &
    
    # Create one of the multi-file test files after different delays
    (sleep 25 && echo "Multi-file test 1" > /tmp/file1.txt) &
    
    echo "Test environment setup initiated"
    ''',
    dag=dag,
)

cleanup_test_environment = BashOperator(
    task_id='cleanup_test_files',
    bash_command='''
    echo "Cleaning up test files..."
    rm -f /tmp/trigger_test_file.txt
    rm -f /tmp/file1.txt /tmp/file2.txt /tmp/file3.txt
    rm -rf /tmp/trigger_test
    echo "Cleanup completed"
    ''',
    trigger_rule='all_done',  # Run regardless of upstream success/failure
    dag=dag,
)

# Example 8: Comparison Task - Traditional Sensor vs Trigger

traditional_file_sensor = FileSensor(
    task_id='wait_for_file_traditional',
    filepath='/tmp/trigger_test_file.txt',
    deferrable=False,  # Traditional polling mode
    poke_interval=10,
    timeout=300,
    mode='reschedule',  # At least use reschedule to free worker slots
    dag=dag,
)


def compare_approaches(**context):
    """Compare traditional sensor vs trigger approach."""
    print("=== Sensor vs Trigger Comparison ===")
    print()
    print("Traditional Sensor:")
    print("- Uses worker slots even when waiting")
    print("- Polls at regular intervals")
    print("- Simple to understand and debug")
    print("- Works with all Airflow versions")
    print()
    print("Trigger-based Sensor:")
    print("- Frees up worker slots while waiting")
    print("- Event-driven, more efficient")
    print("- Can handle thousands of concurrent waits")
    print("- Requires Airflow 2.2+ and triggerer process")
    print()
    print("Use triggers for:")
    print("- High concurrency scenarios")
    print("- Long wait times")
    print("- Resource-constrained environments")
    print("- Modern Airflow deployments")


comparison_task = PythonOperator(
    task_id='compare_sensor_approaches',
    python_callable=compare_approaches,
    dag=dag,
)

# Define task dependencies
setup_test_environment >> [deferrable_file_sensor, traditional_file_sensor]
setup_test_environment >> time_sensor_deferrable >> multi_file_sensor

# Both sensors feed into processing
[deferrable_file_sensor, traditional_file_sensor] >> comparison_task
multi_file_sensor >> process_file_task

# Cleanup runs after everything
[comparison_task, process_file_task] >> cleanup_test_environment

# Documentation for the DAG
dag.doc_md = """
## Trigger Examples DAG

This DAG demonstrates various trigger patterns and deferrable operators in Airflow.

### Key Concepts Demonstrated

1. **Basic Deferrable Sensors**: Simple conversion from traditional sensors
2. **Custom Triggers**: Building your own trigger logic
3. **Event Handling**: Processing trigger events properly
4. **Resource Efficiency**: Comparing traditional vs trigger approaches

### Tasks Overview

- `setup_test_files`: Creates test files for sensor demonstrations
- `wait_for_file_deferrable`: Basic deferrable file sensor
- `wait_for_file_traditional`: Traditional file sensor for comparison
- `wait_30_seconds_deferrable`: Time-based deferrable sensor
- `wait_for_any_file`: Custom multi-file sensor using triggers
- `process_detected_file`: Processes results from trigger-based sensors
- `compare_sensor_approaches`: Explains differences between approaches
- `cleanup_test_files`: Cleans up test environment

### Running This DAG

1. Ensure you have Airflow 2.2+ with triggerer process running
2. Trigger the DAG manually
3. Watch how deferrable sensors free up worker slots
4. Compare execution patterns between traditional and trigger-based sensors

### Monitoring Triggers

- Check triggerer logs: `airflow triggerer`
- Monitor active triggers in Airflow UI
- Watch for trigger events in task logs
"""
