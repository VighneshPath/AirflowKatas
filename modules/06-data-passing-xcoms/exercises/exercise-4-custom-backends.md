# Exercise 4: Custom XCom Backends

## Learning Objectives

By completing this exercise, you will:

- Understand the architecture of XCom backends in Airflow
- Implement custom XCom backends for specialized storage needs
- Create file-based XCom backend for large data handling
- Build compressed XCom backend for space efficiency
- Implement encrypted XCom backend for sensitive data
- Configure and test custom backends in Airflow
- Compare performance characteristics of different backends

## Scenario

Your organization has diverse data processing needs that require specialized XCom storage solutions:

1. **Large Dataset Processing**: Some workflows handle datasets that are too large for database storage
2. **Space Optimization**: Repetitive data needs compression to reduce storage costs
3. **Security Requirements**: Sensitive data must be encrypted at rest
4. **Performance Monitoring**: Need to track XCom usage patterns and performance metrics

You need to implement custom XCom backends that address these specific requirements while maintaining compatibility with existing workflows.

## Your Task

Create three custom XCom backends and demonstrate their usage:

1. **FileXComBackend**: Store large data in files with database references
2. **CompressedXComBackend**: Compress data to save database space
3. **EncryptedXComBackend**: Encrypt sensitive data for security

### Part 1: File-Based XCom Backend

Implement a custom XCom backend that automatically stores large data in files and keeps only references in the database.

**Requirements:**

1. **Automatic Size Detection**: If serialized data > 1KB, store in file
2. **File Management**: Create organized file structure with cleanup support
3. **Metadata Tracking**: Store file path, size, and creation time in database
4. **Error Handling**: Handle missing files and corruption gracefully

**Implementation Structure:**

```python
class FileXComBackend(BaseXCom):
    """Custom XCom backend for large data storage in files"""

    XCOM_FILE_DIR = "/tmp/airflow_xcom_files"
    SIZE_THRESHOLD_BYTES = 1024  # 1KB threshold

    @staticmethod
    def serialize_value(value: Any) -> Any:
        """Serialize value and store in file if large"""
        # Your implementation here
        pass

    @staticmethod
    def deserialize_value(result) -> Any:
        """Deserialize value, loading from file if necessary"""
        # Your implementation here
        pass

    @classmethod
    def clear_db(cls, **kwargs):
        """Custom cleanup method for files"""
        # Your implementation here
        pass
```

**File Reference Format:**

```python
{
    "_xcom_file_backend": True,
    "filepath": "/tmp/airflow_xcom_files/xcom_20240115_143022_123456.json",
    "size_bytes": 2048,
    "record_count": 100,
    "created_at": "2024-01-15T14:30:22.123456",
    "checksum": "md5_hash_here"
}
```

### Part 2: Compressed XCom Backend

Implement a backend that compresses data to reduce database storage requirements.

**Requirements:**

1. **Compression Logic**: Compress data > 100 bytes using gzip
2. **Compression Metrics**: Track original size, compressed size, and ratio
3. **Base64 Encoding**: Encode compressed data for database storage
4. **Performance Monitoring**: Log compression statistics

**Implementation Structure:**

```python
class CompressedXComBackend(BaseXCom):
    """Custom XCom backend with data compression"""

    COMPRESSION_THRESHOLD_BYTES = 100

    @staticmethod
    def serialize_value(value: Any) -> Any:
        """Serialize and compress value if beneficial"""
        # Your implementation here
        pass

    @staticmethod
    def deserialize_value(result) -> Any:
        """Decompress and deserialize value"""
        # Your implementation here
        pass
```

**Compressed Data Format:**

```python
{
    "_xcom_compressed": True,
    "data": "base64_encoded_compressed_data",
    "original_size": 1500,
    "compressed_size": 450,
    "compression_ratio": 0.3,
    "algorithm": "gzip"
}
```

### Part 3: Encrypted XCom Backend

Implement a backend that encrypts sensitive data before storage.

**Requirements:**

1. **Sensitive Data Detection**: Identify data containing sensitive keywords
2. **Encryption**: Use proper encryption (AES-256 recommended)
3. **Key Management**: Secure key storage and rotation support
4. **Integrity Checking**: Verify data integrity after decryption

**Implementation Structure:**

```python
class EncryptedXComBackend(BaseXCom):
    """Custom XCom backend with encryption for sensitive data"""

    SENSITIVE_KEYWORDS = ['password', 'secret', 'token', 'key', 'credential', 'ssn']

    @staticmethod
    def serialize_value(value: Any) -> Any:
        """Serialize and encrypt sensitive data"""
        # Your implementation here
        pass

    @staticmethod
    def deserialize_value(result) -> Any:
        """Decrypt and deserialize sensitive data"""
        # Your implementation here
        pass

    @staticmethod
    def _is_sensitive(data_str: str) -> bool:
        """Check if data contains sensitive information"""
        # Your implementation here
        pass
```

**Encrypted Data Format:**

```python
{
    "_xcom_encrypted": True,
    "encrypted_data": "base64_encoded_encrypted_data",
    "encryption_algorithm": "AES-256-GCM",
    "iv": "base64_encoded_iv",
    "tag": "base64_encoded_auth_tag",
    "encrypted_at": "2024-01-15T14:30:22.123456",
    "checksum": "sha256_hash_of_original_data"
}
```

### Part 4: Backend Comparison and Testing

Create a comprehensive test DAG that demonstrates all three backends:

**Test DAG Requirements:**

1. **Data Generation**: Create test data of various sizes and types
2. **Backend Testing**: Test each backend with appropriate data
3. **Performance Measurement**: Compare serialization/deserialization times
4. **Storage Efficiency**: Measure storage space usage
5. **Error Handling**: Test error scenarios and recovery

**Test Data Types:**

- Small configuration data (< 100 bytes)
- Medium structured data (1KB - 100KB)
- Large datasets (> 1MB)
- Repetitive data (good for compression)
- Sensitive data (contains passwords, tokens, etc.)
- Binary-like data (random bytes)

### Part 5: Configuration and Deployment

Create configuration examples and deployment instructions:

**Configuration Files:**

1. **airflow.cfg modifications**
2. **Environment variable setup**
3. **Docker configuration (if applicable)**
4. **Security considerations**

## Implementation Guidelines

### Base XCom Backend Structure

```python
from airflow.models.xcom import BaseXCom
from airflow.utils.context import Context
import json
import gzip
import base64
import hashlib
import os
from datetime import datetime
from typing import Any, Optional

class CustomXComBackend(BaseXCom):
    """Base structure for custom XCom backends"""

    @staticmethod
    def serialize_value(value: Any) -> Any:
        """
        Serialize value for storage

        Args:
            value: The value to serialize

        Returns:
            Serialized value (must be JSON-serializable)
        """
        raise NotImplementedError

    @staticmethod
    def deserialize_value(result) -> Any:
        """
        Deserialize value from storage

        Args:
            result: XCom result object with .value attribute

        Returns:
            Deserialized original value
        """
        raise NotImplementedError

    @classmethod
    def clear_db(cls, **kwargs):
        """
        Custom cleanup method (optional)
        Called during XCom cleanup operations
        """
        super().clear_db(**kwargs)
        # Add custom cleanup logic here
```

### Error Handling Patterns

```python
def safe_file_operation(operation, *args, **kwargs):
    """Safely perform file operations with error handling"""
    try:
        return operation(*args, **kwargs)
    except FileNotFoundError as e:
        raise ValueError(f"XCom file not found: {e}")
    except PermissionError as e:
        raise ValueError(f"XCom file permission error: {e}")
    except Exception as e:
        raise ValueError(f"XCom file operation failed: {e}")

def validate_data_integrity(original_checksum, data):
    """Validate data integrity using checksums"""
    current_checksum = hashlib.sha256(
        json.dumps(data, sort_keys=True).encode()
    ).hexdigest()

    if original_checksum != current_checksum:
        raise ValueError("Data integrity check failed")
```

### Performance Monitoring

```python
import time
from functools import wraps

def monitor_performance(operation_name):
    """Decorator to monitor backend performance"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                print(f"{operation_name} completed in {duration:.3f}s")
                return result
            except Exception as e:
                duration = time.time() - start_time
                print(f"{operation_name} failed after {duration:.3f}s: {e}")
                raise
        return wrapper
    return decorator
```

## Testing Your Implementation

### Unit Tests

Create unit tests for each backend:

```python
import unittest
import tempfile
import os
from unittest.mock import Mock

class TestFileXComBackend(unittest.TestCase):

    def setUp(self):
        self.backend = FileXComBackend()
        self.temp_dir = tempfile.mkdtemp()
        FileXComBackend.XCOM_FILE_DIR = self.temp_dir

    def tearDown(self):
        # Clean up test files
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_small_data_direct_storage(self):
        """Test that small data is stored directly"""
        small_data = {"key": "value"}
        result = self.backend.serialize_value(small_data)

        # Should be stored as JSON string, not file reference
        self.assertIsInstance(result, str)
        self.assertNotIn("_xcom_file_backend", result)

    def test_large_data_file_storage(self):
        """Test that large data is stored in files"""
        large_data = {"data": "x" * 2000}  # > 1KB
        result = self.backend.serialize_value(large_data)

        # Should be stored as file reference
        self.assertIsInstance(result, dict)
        self.assertTrue(result.get("_xcom_file_backend"))
        self.assertIn("filepath", result)

        # File should exist
        self.assertTrue(os.path.exists(result["filepath"]))
```

### Integration Tests

Create integration tests with actual DAGs:

```python
def test_backend_integration():
    """Test backend with actual DAG execution"""

    def generate_test_data():
        return {"large_dataset": ["item"] * 1000}

    def process_test_data(**context):
        data = context['task_instance'].xcom_pull(task_ids='generate_data')
        assert data is not None
        assert len(data["large_dataset"]) == 1000
        return {"processed": True}

    # Create test DAG and run it
    # Verify data is correctly passed through custom backend
```

### Performance Benchmarks

```python
def benchmark_backends():
    """Compare performance of different backends"""
    import time

    test_data_sizes = [100, 1000, 10000, 100000]  # bytes
    backends = [FileXComBackend, CompressedXComBackend, EncryptedXComBackend]

    results = {}

    for backend_class in backends:
        backend = backend_class()
        backend_results = {}

        for size in test_data_sizes:
            test_data = {"data": "x" * size}

            # Measure serialization time
            start = time.time()
            serialized = backend.serialize_value(test_data)
            serialize_time = time.time() - start

            # Measure deserialization time
            mock_result = Mock()
            mock_result.value = serialized

            start = time.time()
            deserialized = backend.deserialize_value(mock_result)
            deserialize_time = time.time() - start

            backend_results[size] = {
                "serialize_time": serialize_time,
                "deserialize_time": deserialize_time,
                "total_time": serialize_time + deserialize_time
            }

        results[backend_class.__name__] = backend_results

    return results
```

## Configuration Instructions

### Airflow Configuration

To use your custom backends, you need to configure Airflow:

**Method 1: airflow.cfg**

```ini
[core]
xcom_backend = path.to.your.module.FileXComBackend
```

**Method 2: Environment Variable**

```bash
export AIRFLOW__CORE__XCOM_BACKEND=path.to.your.module.CompressedXComBackend
```

**Method 3: Docker Configuration**

```dockerfile
ENV AIRFLOW__CORE__XCOM_BACKEND=path.to.your.module.EncryptedXComBackend
```

### Security Configuration

For encrypted backend:

```bash
# Set encryption key (use proper key management in production)
export AIRFLOW_XCOM_ENCRYPTION_KEY="your-32-character-encryption-key"

# Set key rotation schedule
export AIRFLOW_XCOM_KEY_ROTATION_DAYS=90
```

## Success Criteria

Your implementation is successful when:

1. ✅ All three backends are implemented and functional
2. ✅ File backend automatically handles large data with file storage
3. ✅ Compressed backend reduces storage space for repetitive data
4. ✅ Encrypted backend secures sensitive data
5. ✅ All backends pass unit and integration tests
6. ✅ Performance benchmarks show expected characteristics
7. ✅ Configuration works correctly in Airflow
8. ✅ Error handling is robust and informative

## Extension Challenges

### Challenge 1: Hybrid Backend

Create a backend that automatically chooses the best storage method:

```python
class HybridXComBackend(BaseXCom):
    """Automatically choose optimal storage method"""

    def serialize_value(self, value):
        # Analyze data characteristics
        # Choose file, compression, encryption, or direct storage
        # Based on size, sensitivity, and compressibility
        pass
```

### Challenge 2: Distributed Backend

Implement a backend that works with distributed storage:

```python
class S3XComBackend(BaseXCom):
    """Store XComs in AWS S3 for distributed access"""

    def serialize_value(self, value):
        # Upload to S3 and return reference
        pass

    def deserialize_value(self, result):
        # Download from S3 and return data
        pass
```

### Challenge 3: Monitoring and Metrics

Add comprehensive monitoring to your backends:

```python
class MonitoredXComBackend(BaseXCom):
    """Backend with comprehensive monitoring"""

    def serialize_value(self, value):
        # Track metrics: size, time, compression ratio, etc.
        # Send metrics to monitoring system
        pass
```

## File Locations

Save your implementations as:

- `plugins/xcom_backends.py` - Custom backend implementations
- `dags/test_custom_backends.py` - Test DAG
- `tests/test_xcom_backends.py` - Unit tests
- `config/backend_config.py` - Configuration examples

---

**Estimated Time**: 45-50 minutes

**Difficulty**: Advanced

**Key Concepts**: Custom XCom backends, file storage, compression, encryption, performance optimization, configuration management
