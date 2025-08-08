"""
Exercise 4 Solution: Custom XCom Backends

This solution demonstrates:
- File-based XCom backend for large data
- Compressed XCom backend for space efficiency
- Encrypted XCom backend for sensitive data
- Performance monitoring and comparison
- Comprehensive testing and error handling

Note: This is for educational purposes. In production, you would:
1. Use proper encryption libraries (not the simplified example here)
2. Implement proper key management
3. Add comprehensive logging and monitoring
4. Handle edge cases and error scenarios more robustly
"""

import json
import gzip
import base64
import hashlib
import os
import tempfile
import time
from datetime import datetime, timedelta
from typing import Any, Optional, Dict, List
from functools import wraps
from airflow.models.xcom import BaseXCom
from airflow import DAG
from airflow.operators.python import PythonOperator

# =============================================================================
# Performance Monitoring Decorator
# =============================================================================


def monitor_performance(operation_name: str):
    """Decorator to monitor backend performance"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                print(f"[{operation_name}] completed in {duration:.3f}s")
                return result
            except Exception as e:
                duration = time.time() - start_time
                print(f"[{operation_name}] failed after {duration:.3f}s: {e}")
                raise
        return wrapper
    return decorator

# =============================================================================
# Custom XCom Backend Implementations
# =============================================================================


class FileXComBackend(BaseXCom):
    """
    Custom XCom backend that stores large data in files
    and only keeps file references in the database
    """

    XCOM_FILE_DIR = "/tmp/airflow_xcom_files"
    SIZE_THRESHOLD_BYTES = 1024  # 1KB threshold

    @staticmethod
    @monitor_performance("FileXCom Serialize")
    def serialize_value(value: Any) -> Any:
        """Serialize value and store in file if large"""
        # Convert value to JSON string to check size
        json_str = json.dumps(value, default=str)
        size_bytes = len(json_str.encode('utf-8'))

        if size_bytes > FileXComBackend.SIZE_THRESHOLD_BYTES:
            # Create directory if it doesn't exist
            os.makedirs(FileXComBackend.XCOM_FILE_DIR, exist_ok=True)

            # Generate unique filename with timestamp and hash
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            content_hash = hashlib.md5(json_str.encode()).hexdigest()[:8]
            filename = f"xcom_{timestamp}_{content_hash}.json"
            filepath = os.path.join(FileXComBackend.XCOM_FILE_DIR, filename)

            # Write data to file
            try:
                with open(filepath, 'w') as f:
                    f.write(json_str)

                # Calculate checksum for integrity verification
                checksum = hashlib.sha256(json_str.encode()).hexdigest()

                # Return file reference instead of actual data
                return {
                    "_xcom_file_backend": True,
                    "filepath": filepath,
                    "size_bytes": size_bytes,
                    "record_count": len(value) if isinstance(value, (list, dict)) else 1,
                    "created_at": datetime.now().isoformat(),
                    "checksum": checksum,
                    "filename": filename
                }

            except Exception as e:
                # If file creation fails, fall back to direct storage
                print(f"File creation failed, using direct storage: {e}")
                return json_str
        else:
            # Small data, store normally
            return json_str

    @staticmethod
    @monitor_performance("FileXCom Deserialize")
    def deserialize_value(result) -> Any:
        """Deserialize value, loading from file if necessary"""
        if isinstance(result.value, dict) and result.value.get("_xcom_file_backend"):
            # This is a file reference, load from file
            file_ref = result.value
            filepath = file_ref["filepath"]

            if not os.path.exists(filepath):
                raise FileNotFoundError(f"XCom file not found: {filepath}")

            try:
                with open(filepath, 'r') as f:
                    json_str = f.read()

                # Verify data integrity
                expected_checksum = file_ref.get("checksum")
                if expected_checksum:
                    actual_checksum = hashlib.sha256(
                        json_str.encode()).hexdigest()
                    if actual_checksum != expected_checksum:
                        raise ValueError(
                            f"Data integrity check failed for {filepath}")

                return json.loads(json_str)

            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON in XCom file {filepath}: {e}")
            except Exception as e:
                raise ValueError(f"Error reading XCom file {filepath}: {e}")
        else:
            # Regular data, deserialize normally
            return json.loads(result.value)

    @classmethod
    def clear_db(cls, **kwargs):
        """Custom cleanup method for files"""
        # Call parent cleanup first
        super().clear_db(**kwargs)

        # Clean up orphaned files
        if os.path.exists(cls.XCOM_FILE_DIR):
            try:
                files_cleaned = 0
                for filename in os.listdir(cls.XCOM_FILE_DIR):
                    if filename.startswith("xcom_") and filename.endswith(".json"):
                        filepath = os.path.join(cls.XCOM_FILE_DIR, filename)
                        try:
                            os.unlink(filepath)
                            files_cleaned += 1
                        except Exception as e:
                            print(f"Failed to clean up file {filepath}: {e}")

                print(f"Cleaned up {files_cleaned} XCom files")

            except Exception as e:
                print(f"Error during XCom file cleanup: {e}")


class CompressedXComBackend(BaseXCom):
    """
    Custom XCom backend that compresses data to save database space
    """

    COMPRESSION_THRESHOLD_BYTES = 100

    @staticmethod
    @monitor_performance("CompressedXCom Serialize")
    def serialize_value(value: Any) -> Any:
        """Serialize and compress value if beneficial"""
        # Convert to JSON string
        json_str = json.dumps(value, default=str)
        json_bytes = json_str.encode('utf-8')
        original_size = len(json_bytes)

        # Compress if data is larger than threshold
        if original_size > CompressedXComBackend.COMPRESSION_THRESHOLD_BYTES:
            try:
                # Compress the data
                compressed_data = gzip.compress(json_bytes)
                compressed_size = len(compressed_data)

                # Only use compression if it actually saves space
                if compressed_size < original_size * 0.9:  # At least 10% savings
                    # Encode as base64 for database storage
                    compressed_b64 = base64.b64encode(
                        compressed_data).decode('utf-8')

                    return {
                        "_xcom_compressed": True,
                        "data": compressed_b64,
                        "original_size": original_size,
                        "compressed_size": compressed_size,
                        "compression_ratio": round(compressed_size / original_size, 3),
                        "algorithm": "gzip",
                        "compressed_at": datetime.now().isoformat()
                    }
                else:
                    # Compression not beneficial, store normally
                    print(
                        f"Compression not beneficial: {compressed_size}/{original_size}")
                    return json_str

            except Exception as e:
                print(f"Compression failed, using direct storage: {e}")
                return json_str
        else:
            # Small data, don't compress
            return json_str

    @staticmethod
    @monitor_performance("CompressedXCom Deserialize")
    def deserialize_value(result) -> Any:
        """Decompress and deserialize value"""
        if isinstance(result.value, dict) and result.value.get("_xcom_compressed"):
            # This is compressed data
            compressed_info = result.value
            compressed_b64 = compressed_info["data"]

            try:
                # Decode from base64
                compressed_data = base64.b64decode(
                    compressed_b64.encode('utf-8'))

                # Decompress
                json_bytes = gzip.decompress(compressed_data)
                json_str = json_bytes.decode('utf-8')

                # Verify size matches expected
                expected_size = compressed_info.get("original_size")
                if expected_size and len(json_bytes) != expected_size:
                    print(
                        f"Warning: Decompressed size mismatch: {len(json_bytes)} vs {expected_size}")

                return json.loads(json_str)

            except Exception as e:
                raise ValueError(f"Decompression failed: {e}")
        else:
            # Regular data
            return json.loads(result.value)


class EncryptedXComBackend(BaseXCom):
    """
    Custom XCom backend that encrypts sensitive data
    Note: This is a simplified example. In production, use proper encryption libraries.
    """

    SENSITIVE_KEYWORDS = ['password', 'secret',
                          'token', 'key', 'credential', 'ssn', 'api_key']
    # Simple encryption key (in production, use proper key management)
    ENCRYPTION_KEY = "my_secret_key_32_characters_long"

    @staticmethod
    def _is_sensitive(data_str: str) -> bool:
        """Check if data contains sensitive information"""
        data_lower = data_str.lower()
        return any(keyword in data_lower for keyword in EncryptedXComBackend.SENSITIVE_KEYWORDS)

    @staticmethod
    def _simple_encrypt(data: str, key: str) -> tuple:
        """
        Simple encryption (NOT for production use)
        Returns (encrypted_data, iv) tuple
        """
        # This is just for demonstration - use proper encryption in production
        import random

        # Generate a simple IV
        iv = ''.join(random.choices(
            'abcdefghijklmnopqrstuvwxyz0123456789', k=16))

        encrypted = ""
        for i, char in enumerate(data):
            key_char = key[i % len(key)]
            iv_char = iv[i % len(iv)]
            encrypted_ord = (ord(char) + ord(key_char) + ord(iv_char)) % 256
            encrypted += chr(encrypted_ord)

        encrypted_b64 = base64.b64encode(
            encrypted.encode('latin-1')).decode('utf-8')
        return encrypted_b64, iv

    @staticmethod
    def _simple_decrypt(encrypted_data: str, key: str, iv: str) -> str:
        """
        Simple decryption (NOT for production use)
        """
        # This is just for demonstration - use proper decryption in production
        try:
            encrypted = base64.b64decode(
                encrypted_data.encode('utf-8')).decode('latin-1')
            decrypted = ""

            for i, char in enumerate(encrypted):
                key_char = key[i % len(key)]
                iv_char = iv[i % len(iv)]
                decrypted_ord = (ord(char) - ord(key_char) -
                                 ord(iv_char)) % 256
                decrypted += chr(decrypted_ord)

            return decrypted

        except Exception as e:
            raise ValueError(f"Decryption failed: {e}")

    @staticmethod
    @monitor_performance("EncryptedXCom Serialize")
    def serialize_value(value: Any) -> Any:
        """Serialize and encrypt value if it contains sensitive data"""
        json_str = json.dumps(value, default=str)

        # Check if data contains sensitive information
        if EncryptedXComBackend._is_sensitive(json_str):
            try:
                # Encrypt the data
                encrypted_data, iv = EncryptedXComBackend._simple_encrypt(
                    json_str,
                    EncryptedXComBackend.ENCRYPTION_KEY
                )

                # Calculate checksum of original data for integrity verification
                checksum = hashlib.sha256(json_str.encode()).hexdigest()

                return {
                    "_xcom_encrypted": True,
                    "encrypted_data": encrypted_data,
                    "iv": iv,
                    "encryption_algorithm": "simple_demo",  # In production: "AES-256-GCM"
                    "encrypted_at": datetime.now().isoformat(),
                    "checksum": checksum,
                    "data_size": len(json_str)
                }

            except Exception as e:
                print(f"Encryption failed, using direct storage: {e}")
                return json_str
        else:
            # Not sensitive, store normally
            return json_str

    @staticmethod
    @monitor_performance("EncryptedXCom Deserialize")
    def deserialize_value(result) -> Any:
        """Decrypt and deserialize value if necessary"""
        if isinstance(result.value, dict) and result.value.get("_xcom_encrypted"):
            # This is encrypted data
            encrypted_info = result.value
            encrypted_data = encrypted_info["encrypted_data"]
            iv = encrypted_info["iv"]

            try:
                # Decrypt
                json_str = EncryptedXComBackend._simple_decrypt(
                    encrypted_data,
                    EncryptedXComBackend.ENCRYPTION_KEY,
                    iv
                )

                # Verify checksum for data integrity
                expected_checksum = encrypted_info.get("checksum")
                if expected_checksum:
                    actual_checksum = hashlib.sha256(
                        json_str.encode()).hexdigest()
                    if actual_checksum != expected_checksum:
                        raise ValueError(
                            "Data integrity check failed after decryption")

                return json.loads(json_str)

            except Exception as e:
                raise ValueError(f"Decryption failed: {e}")
        else:
            # Regular data
            return json.loads(result.value)

# =============================================================================
# Test DAG for Custom Backends
# =============================================================================


# Default arguments
default_args = {
    'owner': 'airflow-kata',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create test DAG
test_dag = DAG(
    'test_custom_xcom_backends',
    default_args=default_args,
    description='Test custom XCom backends with various data types',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['xcom', 'custom-backends', 'test']
)

# =============================================================================
# Test Data Generation Functions
# =============================================================================


def generate_small_data():
    """Generate small data (< 100 bytes) - should use direct storage"""
    return {
        "type": "small_data",
        "size": "tiny",
        "data": "Small configuration data",
        "timestamp": datetime.now().isoformat()
    }


def generate_medium_data():
    """Generate medium data (1KB - 100KB) - good for compression testing"""
    # Create repetitive data that compresses well
    base_pattern = {
        "id": 1,
        "status": "active",
        "category": "standard",
        "description": "This is a repeated pattern that appears many times in the dataset",
        "metadata": {
            "created_by": "system",
            "version": "1.0",
            "environment": "production",
            "tags": ["important", "processed", "validated"]
        }
    }

    # Repeat pattern to create medium-sized data
    return {
        "type": "medium_data",
        "size": "medium",
        "patterns": [
            {**base_pattern, "id": i, "sequence": i}
            for i in range(50)  # Creates ~10KB of repetitive data
        ],
        "generated_at": datetime.now().isoformat()
    }


def generate_large_data():
    """Generate large data (> 1MB) - should use file storage"""
    # Create large dataset
    return {
        "type": "large_data",
        "size": "large",
        "records": [
            {
                "record_id": i,
                "name": f"Record_{i:06d}",
                # Make it verbose
                "description": f"Detailed description for record {i} " * 20,
                "data_payload": f"Large data content for record {i} " * 50,
                "metadata": {
                    "created_at": datetime.now().isoformat(),
                    "tags": [f"tag_{j}" for j in range(10)],
                    "properties": {f"prop_{k}": f"value_{k}_{i}" for k in range(15)},
                    "nested_data": {
                        "level1": {
                            "level2": {
                                "level3": f"Deep nested data for record {i}"
                            }
                        }
                    }
                }
            }
            for i in range(500)  # Creates ~2MB of data
        ],
        "summary": {
            "total_records": 500,
            "estimated_size_mb": 2.0,
            "generated_at": datetime.now().isoformat()
        }
    }


def generate_sensitive_data():
    """Generate data with sensitive information - should be encrypted"""
    return {
        "type": "sensitive_data",
        "user_info": {
            "username": "john_doe",
            "email": "john@example.com",
            "api_token": "secret_token_abc123xyz789",  # This triggers encryption
            "password_hash": "hashed_password_value",  # This triggers encryption
            "access_key": "access_key_sensitive_data",  # This triggers encryption
        },
        "system_credentials": {
            "database_password": "db_secret_password",  # This triggers encryption
            "api_key": "system_api_key_12345",  # This triggers encryption
            "encryption_key": "system_encryption_key"  # This triggers encryption
        },
        "public_info": {
            "name": "John Doe",
            "department": "Engineering",
            "location": "Seattle"
        },
        "generated_at": datetime.now().isoformat()
    }


def generate_binary_like_data():
    """Generate binary-like data - tests compression effectiveness"""
    import random

    # Generate random data that doesn't compress well
    random_data = [random.randint(0, 255) for _ in range(1000)]

    return {
        "type": "binary_like_data",
        "random_bytes": random_data,
        "entropy": "high",
        "compressibility": "low",
        "size_bytes": len(random_data),
        "generated_at": datetime.now().isoformat()
    }

# =============================================================================
# Test Processing Functions
# =============================================================================


def process_test_data(**context):
    """Process data from all test generators and analyze results"""
    ti = context['task_instance']

    # Pull data from all generators
    test_results = {}

    data_sources = [
        'generate_small_data',
        'generate_medium_data',
        'generate_large_data',
        'generate_sensitive_data',
        'generate_binary_like_data'
    ]

    for source in data_sources:
        try:
            start_time = time.time()
            data = ti.xcom_pull(task_ids=source)
            pull_time = time.time() - start_time

            if data:
                test_results[source] = {
                    "status": "success",
                    "data_type": data.get("type", "unknown"),
                    "data_size": data.get("size", "unknown"),
                    "pull_time_seconds": round(pull_time, 3),
                    "record_count": len(data.get("records", [])) if "records" in data else 1,
                    "has_sensitive_data": any(
                        keyword in str(data).lower()
                        for keyword in EncryptedXComBackend.SENSITIVE_KEYWORDS
                    )
                }

                # Verify data integrity
                if data.get("type") == "large_data":
                    expected_records = 500
                    actual_records = len(data.get("records", []))
                    test_results[source]["data_integrity"] = actual_records == expected_records

            else:
                test_results[source] = {
                    "status": "failed",
                    "error": "No data received"
                }

        except Exception as e:
            test_results[source] = {
                "status": "error",
                "error": str(e),
                "pull_time_seconds": 0
            }

    # Generate summary
    successful_tests = sum(
        1 for result in test_results.values() if result["status"] == "success")
    total_tests = len(test_results)

    summary = {
        "test_results": test_results,
        "summary": {
            "total_tests": total_tests,
            "successful_tests": successful_tests,
            "success_rate": successful_tests / total_tests if total_tests > 0 else 0,
            "total_pull_time": sum(
                result.get("pull_time_seconds", 0)
                for result in test_results.values()
            )
        },
        "backend_analysis": {
            "file_backend_used": any(
                result.get("data_type") == "large_data"
                for result in test_results.values()
            ),
            "compression_candidates": sum(
                1 for result in test_results.values()
                if result.get("data_type") in ["medium_data", "binary_like_data"]
            ),
            "encryption_candidates": sum(
                1 for result in test_results.values()
                if result.get("has_sensitive_data", False)
            )
        },
        "processed_at": datetime.now().isoformat()
    }

    print(
        f"Backend test completed: {successful_tests}/{total_tests} successful")
    print(
        f"Total processing time: {summary['summary']['total_pull_time']:.3f}s")

    return summary

# =============================================================================
# Task Definitions
# =============================================================================


# Data generation tasks
small_data_task = PythonOperator(
    task_id='generate_small_data',
    python_callable=generate_small_data,
    dag=test_dag
)

medium_data_task = PythonOperator(
    task_id='generate_medium_data',
    python_callable=generate_medium_data,
    dag=test_dag
)

large_data_task = PythonOperator(
    task_id='generate_large_data',
    python_callable=generate_large_data,
    dag=test_dag
)

sensitive_data_task = PythonOperator(
    task_id='generate_sensitive_data',
    python_callable=generate_sensitive_data,
    dag=test_dag
)

binary_data_task = PythonOperator(
    task_id='generate_binary_like_data',
    python_callable=generate_binary_like_data,
    dag=test_dag
)

# Processing task
process_task = PythonOperator(
    task_id='process_test_data',
    python_callable=process_test_data,
    dag=test_dag
)

# Set up dependencies
data_generation_tasks = [
    small_data_task,
    medium_data_task,
    large_data_task,
    sensitive_data_task,
    binary_data_task
]

# All data generation tasks must complete before processing
for task in data_generation_tasks:
    task >> process_task

# =============================================================================
# Configuration Examples and Instructions
# =============================================================================

"""
To use these custom XCom backends in your Airflow installation:

1. File Backend Configuration:
   Add to airflow.cfg:
   [core]
   xcom_backend = path.to.this.module.FileXComBackend
   
   Or set environment variable:
   export AIRFLOW__CORE__XCOM_BACKEND=path.to.this.module.FileXComBackend

2. Compressed Backend Configuration:
   [core]
   xcom_backend = path.to.this.module.CompressedXComBackend

3. Encrypted Backend Configuration:
   [core]
   xcom_backend = path.to.this.module.EncryptedXComBackend
   
   Also set encryption key:
   export AIRFLOW_XCOM_ENCRYPTION_KEY="your-32-character-encryption-key"

4. Directory Permissions:
   Ensure Airflow has write permissions to /tmp/airflow_xcom_files/
   or change FileXComBackend.XCOM_FILE_DIR to a suitable location.

5. Production Considerations:
   - Use proper encryption libraries (cryptography, PyCrypto, etc.)
   - Implement proper key management and rotation
   - Add comprehensive logging and monitoring
   - Consider backup and recovery strategies for file-based storage
   - Implement proper cleanup mechanisms
   - Add performance monitoring and alerting

6. Testing:
   Run this DAG to test all backends with different data types.
   Monitor the logs for performance metrics and any issues.

Note: Only one XCom backend can be active at a time per Airflow instance.
Choose the backend that best fits your primary use case, or implement
a hybrid backend that automatically selects the appropriate storage method.
"""
