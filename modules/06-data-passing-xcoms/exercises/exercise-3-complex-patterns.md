# Exercise 3: Complex XCom Patterns

## Learning Objectives

By completing this exercise, you will:

- Implement advanced XCom patterns for complex data flows
- Handle large datasets using file references instead of direct XCom storage
- Create dynamic XCom key generation based on runtime conditions
- Implement XCom cleanup and lifecycle management
- Build performance-optimized data passing patterns
- Handle conditional XCom usage with error recovery

## Scenario

You're building a multi-stage data processing system for a financial analytics company. The system needs to:

1. **Data Ingestion**: Process multiple data sources with varying sizes and formats
2. **Quality Assessment**: Validate data quality and generate detailed reports
3. **Risk Analysis**: Perform complex calculations that may succeed, partially succeed, or fail
4. **Report Generation**: Create multiple output formats based on available data
5. **Cleanup**: Manage temporary data and optimize storage usage

The system must handle large datasets efficiently, adapt to runtime conditions, and provide robust error handling.

## Your Task

Create a DAG called `financial_analytics_pipeline` that implements advanced XCom patterns:

```
ingest_data → assess_quality → analyze_risk → generate_reports → cleanup_resources
```

### Step 1: Data Ingestion with Large Data Handling

Create a task called `ingest_data` that:

- Simulates ingesting data from multiple sources (some large, some small)
- Uses file references for large datasets instead of storing in XComs
- Generates dynamic XCom keys based on data source characteristics
- Handles different data formats and sizes

**Implementation Requirements:**

1. **Generate 4 different data sources:**

   - `market_data`: Large dataset (> 1MB when serialized) - use file reference
   - `trading_data`: Medium dataset - store in XCom with custom key
   - `risk_parameters`: Small configuration data - store normally
   - `external_feeds`: Variable size based on random condition

2. **For large datasets, create file references:**

```python
{
    "data_type": "file_reference",
    "file_path": "/tmp/market_data_20240115.json",
    "record_count": 10000,
    "file_size_bytes": 1500000,
    "created_at": "2024-01-15T10:00:00",
    "cleanup_required": True
}
```

3. **Use dynamic XCom keys:**
   - Push each data source with key pattern: `data_source_{source_name}`
   - Push metadata with key: `ingestion_metadata`
   - Return summary of what was ingested

### Step 2: Quality Assessment with Conditional Processing

Create a task called `assess_quality` that:

- Pulls data from all dynamic keys created in step 1
- Performs different quality checks based on data type
- Creates conditional XCom outputs based on quality results
- Handles missing or corrupted data gracefully

**Quality Assessment Logic:**

1. **For file references:** Load file, check record count and data integrity
2. **For direct XCom data:** Validate structure and completeness
3. **Generate quality scores:** 0.0 (failed) to 1.0 (perfect)
4. **Create conditional outputs:**
   - If quality score ≥ 0.8: Push to `high_quality_data` key
   - If 0.5 ≤ quality score < 0.8: Push to `medium_quality_data` key
   - If quality score < 0.5: Push to `low_quality_data` key and `quality_issues` key

**Expected output structure:**

```python
{
    "quality_assessment": {
        "market_data": {"score": 0.95, "issues": [], "record_count": 10000},
        "trading_data": {"score": 0.75, "issues": ["missing timestamps"], "record_count": 500},
        "risk_parameters": {"score": 1.0, "issues": [], "record_count": 50},
        "external_feeds": {"score": 0.45, "issues": ["data corruption", "incomplete"], "record_count": 200}
    },
    "overall_quality_score": 0.79,
    "assessment_completed_at": "2024-01-15T10:01:00"
}
```

### Step 3: Risk Analysis with Error Handling

Create a task called `analyze_risk` that:

- Processes data based on quality assessment results
- Implements different analysis strategies based on available data
- Handles partial failures and continues processing
- Uses multiple XCom keys for different types of results

**Risk Analysis Logic:**

1. **High Quality Data:** Perform full risk analysis
2. **Medium Quality Data:** Perform basic risk analysis with warnings
3. **Low Quality Data:** Skip analysis but log issues
4. **Handle Analysis Failures:** Continue with available data

**Push results with different keys:**

- `risk_analysis_results`: Main analysis results
- `risk_warnings`: Any warnings or concerns
- `analysis_metadata`: Processing statistics and timing
- `failed_analyses`: Information about any failed calculations

### Step 4: Report Generation with Adaptive Output

Create a task called `generate_reports` that:

- Adapts report generation based on available XCom data
- Creates different report formats based on data quality
- Handles missing data gracefully
- Generates multiple output types

**Report Generation Logic:**

1. **Check what data is available** from previous tasks
2. **Generate appropriate reports:**
   - Full report if high-quality data available
   - Summary report if only medium-quality data available
   - Error report if mostly low-quality data
3. **Create multiple output formats:**
   - `executive_summary`: High-level overview
   - `detailed_analysis`: Technical details (if data supports it)
   - `quality_report`: Data quality assessment
   - `recommendations`: Action items based on analysis

### Step 5: Resource Cleanup and Optimization

Create a task called `cleanup_resources` that:

- Identifies temporary files and XComs that need cleanup
- Removes large temporary files
- Optimizes XCom storage by removing unnecessary data
- Generates cleanup summary

**Cleanup Tasks:**

1. **File Cleanup:** Remove temporary files created for large datasets
2. **XCom Optimization:** Clear temporary XCom keys
3. **Resource Summary:** Report on storage saved and resources cleaned
4. **Audit Trail:** Log what was cleaned up for debugging

## Implementation Guidelines

### Large Data Handling Pattern

```python
def handle_large_dataset(data, threshold_mb=1):
    """Handle large datasets by storing in files"""
    import json
    import tempfile
    import os

    # Serialize to check size
    json_str = json.dumps(data, default=str)
    size_mb = len(json_str.encode('utf-8')) / (1024 * 1024)

    if size_mb > threshold_mb:
        # Store in file
        temp_file = tempfile.NamedTemporaryFile(
            mode='w',
            delete=False,
            suffix='.json',
            prefix='airflow_data_'
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
            "created_at": datetime.now().isoformat()
        }
```

### Dynamic XCom Key Pattern

```python
def push_with_dynamic_keys(ti, data_dict, key_prefix="data"):
    """Push multiple data items with dynamic keys"""
    pushed_keys = []

    for source_name, source_data in data_dict.items():
        key = f"{key_prefix}_{source_name}"
        ti.xcom_push(key=key, value=source_data)
        pushed_keys.append(key)

    # Return metadata about pushed keys
    return {
        "keys_pushed": pushed_keys,
        "total_items": len(data_dict),
        "push_timestamp": datetime.now().isoformat()
    }
```

### Conditional XCom Processing Pattern

```python
def process_conditional_xcoms(ti, task_id, possible_keys):
    """Process XComs that may or may not exist"""
    available_data = {}
    missing_keys = []

    for key in possible_keys:
        data = ti.xcom_pull(task_ids=task_id, key=key)
        if data is not None:
            available_data[key] = data
        else:
            missing_keys.append(key)

    return available_data, missing_keys
```

### File Reference Loading Pattern

```python
def load_from_file_reference(file_ref):
    """Load data from file reference"""
    import json
    import os

    if file_ref.get("data_type") == "file_reference":
        file_path = file_ref["file_path"]

        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                return json.load(f)
        else:
            raise FileNotFoundError(f"Referenced file not found: {file_path}")
    else:
        # Direct data
        return file_ref.get("data")
```

## Testing Your Implementation

### Test Data Generation

Create realistic test data for each source:

```python
def generate_market_data(size="large"):
    """Generate market data of specified size"""
    if size == "large":
        # Generate 10,000+ records
        return [
            {
                "symbol": f"STOCK_{i:04d}",
                "price": round(random.uniform(10, 1000), 2),
                "volume": random.randint(1000, 100000),
                "timestamp": datetime.now().isoformat(),
                "metadata": {"exchange": "NYSE", "sector": "tech"}
            }
            for i in range(10000)
        ]
    # ... other sizes
```

### Quality Score Calculation

```python
def calculate_quality_score(data, data_type):
    """Calculate quality score based on data characteristics"""
    score = 1.0
    issues = []

    if not data:
        return 0.0, ["No data available"]

    # Check for missing required fields
    if data_type == "market_data":
        required_fields = ["symbol", "price", "volume", "timestamp"]
        # ... validation logic

    return score, issues
```

### Performance Monitoring

Add timing and memory usage tracking:

```python
import time
import psutil
import os

def monitor_performance(func):
    """Decorator to monitor task performance"""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        process = psutil.Process(os.getpid())
        start_memory = process.memory_info().rss / 1024 / 1024  # MB

        result = func(*args, **kwargs)

        end_time = time.time()
        end_memory = process.memory_info().rss / 1024 / 1024  # MB

        performance_stats = {
            "execution_time_seconds": round(end_time - start_time, 2),
            "memory_usage_mb": round(end_memory - start_memory, 2),
            "peak_memory_mb": round(end_memory, 2)
        }

        print(f"Performance: {performance_stats}")
        return result

    return wrapper
```

## Success Criteria

Your implementation is successful when:

1. ✅ Large datasets are handled via file references, not direct XCom storage
2. ✅ Dynamic XCom keys are generated and used correctly
3. ✅ Quality assessment creates conditional XCom outputs
4. ✅ Risk analysis adapts to available data quality
5. ✅ Report generation handles missing data gracefully
6. ✅ Cleanup task removes temporary files and optimizes storage
7. ✅ All tasks complete successfully even with simulated failures
8. ✅ Performance is acceptable for large datasets

## Extension Challenges

### Challenge 1: Implement XCom Compression

Add compression for medium-sized datasets:

```python
def compress_xcom_data(data):
    """Compress data before storing in XCom"""
    import gzip
    import base64
    import json

    json_str = json.dumps(data, default=str)
    compressed = gzip.compress(json_str.encode('utf-8'))

    return {
        "compressed": True,
        "data": base64.b64encode(compressed).decode('utf-8'),
        "original_size": len(json_str),
        "compressed_size": len(compressed),
        "compression_ratio": len(compressed) / len(json_str)
    }
```

### Challenge 2: Add Data Lineage Tracking

Track data flow through the pipeline:

```python
def track_data_lineage(ti, data_source, transformation):
    """Track data lineage through XComs"""
    lineage_info = {
        "source_task": ti.task_id,
        "data_source": data_source,
        "transformation": transformation,
        "timestamp": datetime.now().isoformat(),
        "dag_run_id": ti.dag_run.run_id
    }

    # Append to lineage tracking
    existing_lineage = ti.xcom_pull(key='data_lineage') or []
    existing_lineage.append(lineage_info)
    ti.xcom_push(key='data_lineage', value=existing_lineage)
```

### Challenge 3: Implement Circuit Breaker Pattern

Add circuit breaker for failing data sources:

```python
class DataSourceCircuitBreaker:
    """Circuit breaker for unreliable data sources"""

    def __init__(self, failure_threshold=3, timeout_seconds=300):
        self.failure_threshold = failure_threshold
        self.timeout_seconds = timeout_seconds
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN

    def call(self, func, *args, **kwargs):
        """Execute function with circuit breaker protection"""
        if self.state == "OPEN":
            if self._should_attempt_reset():
                self.state = "HALF_OPEN"
            else:
                raise Exception("Circuit breaker is OPEN")

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise e
```

## Common Pitfalls and Solutions

### Pitfall 1: Memory Issues with Large Data

**Problem**: Loading large datasets into memory causes OOM errors

**Solution**: Stream processing and file-based storage

```python
def stream_process_large_file(file_path, chunk_size=1000):
    """Process large files in chunks"""
    with open(file_path, 'r') as f:
        chunk = []
        for line in f:
            chunk.append(json.loads(line))
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk
```

### Pitfall 2: File Cleanup Failures

**Problem**: Temporary files not cleaned up properly

**Solution**: Robust cleanup with error handling

```python
def safe_file_cleanup(file_paths):
    """Safely clean up files with error handling"""
    cleaned = []
    failed = []

    for file_path in file_paths:
        try:
            if os.path.exists(file_path):
                os.unlink(file_path)
                cleaned.append(file_path)
        except Exception as e:
            failed.append({"file": file_path, "error": str(e)})

    return {"cleaned": cleaned, "failed": failed}
```

### Pitfall 3: XCom Key Conflicts

**Problem**: Dynamic keys overwrite each other

**Solution**: Unique key generation

```python
def generate_unique_key(base_key, task_id, dag_run_id):
    """Generate unique XCom key"""
    import hashlib

    unique_suffix = hashlib.md5(
        f"{task_id}_{dag_run_id}_{datetime.now().isoformat()}".encode()
    ).hexdigest()[:8]

    return f"{base_key}_{unique_suffix}"
```

## File Location

Save your implementation as: `dags/exercise_3_complex_patterns.py`

---

**Estimated Time**: 40-45 minutes

**Difficulty**: Advanced

**Key Concepts**: Large data handling, dynamic XCom keys, conditional processing, file references, resource cleanup, performance optimization
