# Exercise 2: S3 Sensors and Mock Cloud Storage

## Objective

Learn to implement S3-like sensors using mock cloud storage and understand timeout/retry configurations for cloud-based sensors.

## Background

Your company processes data files that arrive in cloud storage (S3-like buckets). You need to build a pipeline that waits for these files and processes them once they're available. Since we're in a learning environment, we'll simulate S3 behavior using local filesystem.

## Tasks

### Task 1: Mock S3 Sensor Implementation

Create a DAG named `cloud_data_pipeline` that simulates S3KeySensor behavior:

1. Create a mock S3 bucket structure using local directories
2. Implement a custom sensor that waits for files in the mock bucket
3. Configure appropriate timeout and retry settings
4. Process files once they're detected

**Mock S3 Structure:**

```
/tmp/mock-s3/
├── data-bucket/
│   ├── raw/
│   ├── processed/
│   └── archive/
└── logs-bucket/
    ├── application/
    └── access/
```

**Requirements:**

- Wait for file: `data-bucket/raw/daily_export_YYYYMMDD.json`
- Use 45-second poke intervals
- Set 10-minute timeout
- Implement 2 retries with 2-minute delay

### Task 2: Multi-Bucket Sensor Coordination

Create a more complex scenario with multiple buckets and file dependencies:

1. **Primary data**: `data-bucket/raw/transactions_YYYYMMDD.csv`
2. **Reference data**: `data-bucket/raw/customers_YYYYMMDD.json`
3. **Configuration**: `config-bucket/daily/processing_config_YYYYMMDD.yaml`
4. **Validation**: `logs-bucket/validation/data_quality_YYYYMMDD.log`

**Requirements:**

- All files must be present before processing starts
- Use different timeout strategies:
  - Transactions: 15 minutes (critical path)
  - Customers: 20 minutes (can be delayed)
  - Config: 5 minutes (should arrive early)
  - Validation: 25 minutes (generated after data validation)
- Implement reschedule mode for long-running sensors

### Task 3: Advanced Cloud Sensor Patterns

Implement enterprise-grade sensor patterns:

1. **Sensor with exponential backoff**
2. **Custom failure handling and alerting**
3. **SLA monitoring with escalation**
4. **Sensor performance metrics collection**

**Requirements:**

- Exponential backoff starting at 30 seconds, max 5 minutes
- Custom failure callbacks with detailed logging
- 45-minute SLA with escalation callbacks
- Collect and log sensor performance metrics

## Starter Code

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
import os
import glob
import time

class MockS3Sensor(BaseSensorOperator):
    """
    Custom sensor that simulates S3KeySensor behavior using local filesystem
    """

    def __init__(self, bucket_path: str, key_pattern: str, **kwargs):
        super().__init__(**kwargs)
        self.bucket_path = bucket_path
        self.key_pattern = key_pattern

    def poke(self, context: Context) -> bool:
        """Check if the file exists in mock S3 bucket"""
        full_path = os.path.join(self.bucket_path, self.key_pattern)

        # Handle templated patterns (like {{ ds_nodash }})
        full_path = self.render_template(full_path, context)

        # Use glob for pattern matching
        matches = glob.glob(full_path)

        if matches:
            self.log.info(f"Found file(s): {matches}")
            return True
        else:
            self.log.info(f"File not found: {full_path}")
            return False

    def render_template(self, template: str, context: Context) -> str:
        """Simple template rendering for date patterns"""
        ds_nodash = context['ds_nodash']
        return template.replace('{{ ds_nodash }}', ds_nodash)

# Your DAG implementation here
default_args = {
    'owner': 'your-name',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
```

## Test Data Setup

```python
def setup_mock_s3_environment():
    """Create mock S3 bucket structure and test files"""
    import os
    from datetime import datetime

    # Create bucket structure
    buckets = [
        '/tmp/mock-s3/data-bucket/raw',
        '/tmp/mock-s3/data-bucket/processed',
        '/tmp/mock-s3/data-bucket/archive',
        '/tmp/mock-s3/config-bucket/daily',
        '/tmp/mock-s3/logs-bucket/application',
        '/tmp/mock-s3/logs-bucket/access',
        '/tmp/mock-s3/logs-bucket/validation'
    ]

    for bucket in buckets:
        os.makedirs(bucket, exist_ok=True)

    # Create test files
    today = datetime.now().strftime('%Y%m%d')

    test_files = {
        f'/tmp/mock-s3/data-bucket/raw/daily_export_{today}.json':
            '{"records": [{"id": 1, "value": "test"}]}',
        f'/tmp/mock-s3/data-bucket/raw/transactions_{today}.csv':
            'id,amount,customer\n1,100.00,customer1\n2,250.50,customer2',
        f'/tmp/mock-s3/data-bucket/raw/customers_{today}.json':
            '{"customers": [{"id": "customer1", "name": "John"}, {"id": "customer2", "name": "Jane"}]}',
        f'/tmp/mock-s3/config-bucket/daily/processing_config_{today}.yaml':
            'processing:\n  batch_size: 1000\n  timeout: 300',
        f'/tmp/mock-s3/logs-bucket/validation/data_quality_{today}.log':
            'INFO: Data validation completed successfully\nINFO: 2 records processed'
    }

    for file_path, content in test_files.items():
        with open(file_path, 'w') as f:
            f.write(content)
        print(f"Created: {file_path}")

def create_delayed_files():
    """Create files with delays to simulate real-world scenarios"""
    import time
    from datetime import datetime

    today = datetime.now().strftime('%Y%m%d')

    # Simulate files arriving at different times
    delayed_files = [
        ('/tmp/mock-s3/config-bucket/daily/processing_config_{}.yaml'.format(today), 10),  # 10 seconds
        ('/tmp/mock-s3/data-bucket/raw/transactions_{}.csv'.format(today), 30),  # 30 seconds
        ('/tmp/mock-s3/data-bucket/raw/customers_{}.json'.format(today), 60),  # 1 minute
        ('/tmp/mock-s3/logs-bucket/validation/data_quality_{}.log'.format(today), 120)  # 2 minutes
    ]

    for file_path, delay in delayed_files:
        time.sleep(delay)
        with open(file_path, 'w') as f:
            f.write(f'Test content for {os.path.basename(file_path)}')
        print(f"Created delayed file: {file_path} (after {delay}s)")
```

## Expected Behavior

1. **Task 1**: Sensor should wait for the daily export file and process it once available
2. **Task 2**: Multiple sensors should coordinate to ensure all required files are present
3. **Task 3**: Advanced sensors should implement proper retry logic, callbacks, and monitoring

## Validation Criteria

- [ ] Mock S3 structure is created correctly
- [ ] Custom sensors detect files properly
- [ ] Timeout and retry configurations work as expected
- [ ] Task dependencies ensure proper execution order
- [ ] Callbacks are triggered appropriately
- [ ] SLA monitoring is functional

## Real S3 Configuration Reference

For production use with real AWS S3, your configuration would look like:

```python
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

# Real S3 sensor configuration
s3_sensor = S3KeySensor(
    task_id='wait_for_s3_data',
    bucket_name='my-data-bucket',
    bucket_key='raw/daily_export_{{ ds_nodash }}.json',
    aws_conn_id='aws_default',
    mode='reschedule',
    poke_interval=300,  # 5 minutes
    timeout=3600,       # 1 hour
    retries=3,
    retry_delay=timedelta(minutes=10)
)
```

## Troubleshooting Tips

1. **Mock files not found**: Ensure test data setup runs before sensors
2. **Template rendering issues**: Check date format in file patterns
3. **Sensor timeout**: Adjust timeout values for testing
4. **Permission errors**: Verify write permissions to `/tmp/mock-s3/`

## Extension Challenges

1. **Prefix-based sensing**: Modify sensors to wait for any file with a specific prefix
2. **File size validation**: Add checks to ensure files meet minimum size requirements
3. **Multi-region simulation**: Create multiple mock regions with different latencies
4. **Sensor metrics dashboard**: Build a simple dashboard to monitor sensor performance

## Solution Location

Check your implementation against the solution in `solutions/exercise-2-solution.py`
