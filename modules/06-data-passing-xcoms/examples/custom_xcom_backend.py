"""
Custom XCom Backend Example

This module demonstrates how to implement custom XCom backends for specialized storage:
- File-based XCom backend for large data
- Redis-based XCom backend for distributed systems
- Compressed XCom backend for space efficiency
- Encrypted XCom backend for sensitive data

Note: This is for educational purposes. In production, you would configure
these backends in airflow.cfg and use them across all DAGs.
"""

import json
import gzip
import base64
import hashlib
import tempfile
import os
from datetime import datetime, timedelta
from typing import Any, Optional
from airflow.models.xcom import BaseXCom
from airflow.utils.context import Context
from airflow.configuration import conf
from airflow import DAG
from airflow.operators.python import PythonOperator

# =============================================================================
# Custom XCom Backend Implementations
# =============================================================================


class FileXComBackend(BaseXCom):
    """
    Custom XCom backend that stores large data in files
    and only keeps file references in the database
    """

    # Directory for storing XCom files
    XCOM_FILE_DIR = "/tmp/airflow_xcom_files"

    @staticmethod
    def serialize_value(value: Any) -> Any:
        """
        Serialize value and decide whether to store in file or database
        """
        # Convert value to JSON string to check size
        json_str = json.dumps(value, default=str)

        # If data is large (> 1KB), store in file
        if len(json_str.encode('utf-8')) > 1024:
            # Create directory if it doesn't exist
            os.makedirs(FileXComBackend.XCOM_FILE_DIR, exist_ok=True)

            # Generate unique filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            filename = f"xcom_{timestamp}.json"
            filepath = os.path.join(FileXComBackend.XCOM_FILE_DIR, filename)

            # Write data to file
            with open(filepath, 'w') as f:
                f.write(json_str)

            # Return file reference instead of actual data
            return {
                "_xcom_file_backend": True,
                "filepath": filepath,
                "size_bytes": len(json_str.encode('utf-8')),
                "created_at": datetime.now().isoformat()
            }
        else:
            # Small data, store normally
            return json_str

    @staticmethod
    def deserialize_value(result) -> Any:
        """
        Deserialize value, loading from file if necessary
        """
        if isinstance(result.value, dict) and result.value.get("_xcom_file_backend"):
            # This is a file reference, load from file
            filepath = result.value["filepath"]

            if os.path.exists(filepath):
                with open(filepath, 'r') as f:
                    json_str = f.read()
                return json.loads(json_str)
            else:
                raise FileNotFoundError(f"XCom file not found: {filepath}")
        else:
            # Regular data, deserialize normally
            return json.loads(result.value)


class CompressedXComBackend(BaseXCom):
    """
    Custom XCom backend that compresses data to save database space
    """

    @staticmethod
    def serialize_value(value: Any) -> Any:
        """
        Serialize and compress value
        """
        # Convert to JSON string
        json_str = json.dumps(value, default=str)
        json_bytes = json_str.encode('utf-8')

        # Compress if data is larger than 100 bytes
        if len(json_bytes) > 100:
            compressed_data = gzip.compress(json_bytes)

            # Encode as base64 for database storage
            compressed_b64 = base64.b64encode(compressed_data).decode('utf-8')

            return {
                "_xcom_compressed": True,
                "data": compressed_b64,
                "original_size": len(json_bytes),
                "compressed_size": len(compressed_data),
                "compression_ratio": round(len(compressed_data) / len(json_bytes), 3)
            }
        else:
            # Small data, don't compress
            return json_str

    @staticmethod
    def deserialize_value(result) -> Any:
        """
        Decompress and deserialize value
        """
        if isinstance(result.value, dict) and result.value.get("_xcom_compressed"):
            # This is compressed data
            compressed_b64 = result.value["data"]
            compressed_data = base64.b64decode(compressed_b64.encode('utf-8'))

            # Decompress
            json_bytes = gzip.decompress(compressed_data)
            json_str = json_bytes.decode('utf-8')

            return json.loads(json_str)
        else:
            # Regular data
            return json.loads(result.value)


class EncryptedXComBackend(BaseXCom):
    """
    Custom XCom backend that encrypts sensitive data
    Note: This is a simplified example. In production, use proper encryption libraries.
    """

    # Simple encryption key (in production, use proper key management)
    ENCRYPTION_KEY = "my_secret_key_32_characters_long"

    @staticmethod
    def _simple_encrypt(data: str, key: str) -> str:
        """
        Simple encryption (NOT for production use)
        """
        # This is just for demonstration - use proper encryption in production
        encrypted = ""
        for i, char in enumerate(data):
            key_char = key[i % len(key)]
            encrypted += chr((ord(char) + ord(key_char)) % 256)

        return base64.b64encode(encrypted.encode('latin-1')).decode('utf-8')

    @staticmethod
    def _simple_decrypt(encrypted_data: str, key: str) -> str:
        """
        Simple decryption (NOT for production use)
        """
        # This is just for demonstration - use proper decryption in production
        encrypted = base64.b64decode(
            encrypted_data.encode('utf-8')).decode('latin-1')
        decrypted = ""
        for i, char in enumerate(encrypted):
            key_char = key[i % len(key)]
            decrypted += chr((ord(char) - ord(key_char)) % 256)

        return decrypted

    @staticmethod
    def serialize_value(value: Any) -> Any:
        """
        Serialize and encrypt value if it contains sensitive data
        """
        json_str = json.dumps(value, default=str)

        # Check if data contains sensitive information (simplified check)
        sensitive_keywords = ['password',
                              'secret', 'token', 'key', 'credential']
        is_sensitive = any(keyword in json_str.lower()
                           for keyword in sensitive_keywords)

        if is_sensitive:
            # Encrypt the data
            encrypted_data = EncryptedXComBackend._simple_encrypt(
                json_str,
                EncryptedXComBackend.ENCRYPTION_KEY
            )

            return {
                "_xcom_encrypted": True,
                "data": encrypted_data,
                "encrypted_at": datetime.now().isoformat(),
                "checksum": hashlib.md5(json_str.encode()).hexdigest()
            }
        else:
            # Not sensitive, store normally
            return json_str

    @staticmethod
    def deserialize_value(result) -> Any:
        """
        Decrypt and deserialize value if necessary
        """
        if isinstance(result.value, dict) and result.value.get("_xcom_encrypted"):
            # This is encrypted data
            encrypted_data = result.value["data"]

            # Decrypt
            json_str = EncryptedXComBackend._simple_decrypt(
                encrypted_data,
                EncryptedXComBackend.ENCRYPTION_KEY
            )

            # Verify checksum
            checksum = hashlib.md5(json_str.encode()).hexdigest()
            if checksum != result.value["checksum"]:
                raise ValueError(
                    "Data integrity check failed - possible corruption")

            return json.loads(json_str)
        else:
            # Regular data
            return json.loads(result.value)

# =============================================================================
# Example DAGs Using Custom Backends
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

# =============================================================================
# Example 1: File Backend Usage
# =============================================================================


def generate_large_data_for_file_backend():
    """Generate large data that will be stored in files"""
    # Generate large dataset
    large_data = {
        "dataset_id": "large_dataset_001",
        "records": [
            {
                "id": i,
                "name": f"Record {i}",
                "description": f"This is a detailed description for record {i} " * 20,
                "metadata": {
                    "created_at": datetime.now().isoformat(),
                    "tags": [f"tag_{j}" for j in range(5)],
                    "properties": {f"prop_{k}": f"value_{k}" for k in range(10)}
                }
            }
            for i in range(100)  # 100 records with lots of data
        ],
        "summary": {
            "total_records": 100,
            "generated_at": datetime.now().isoformat(),
            "estimated_size_mb": 2.5
        }
    }

    print(f"Generated large dataset with {len(large_data['records'])} records")

    # This will be automatically stored in a file by FileXComBackend
    return large_data


def process_file_backend_data(**context):
    """Process data that was stored using file backend"""
    # Pull data (will be loaded from file automatically)
    large_data = context['task_instance'].xcom_pull(
        task_ids='generate_large_data')

    if not large_data:
        raise ValueError("No data received from file backend")

    # Process the data
    processed_count = len(large_data['records'])
    sample_records = large_data['records'][:3]

    result = {
        "processed_records": processed_count,
        "sample_data": sample_records,
        "original_dataset_id": large_data['dataset_id'],
        "processing_completed_at": datetime.now().isoformat()
    }

    print(f"Processed {processed_count} records from file backend")

    return result

# Note: To actually use FileXComBackend, you would need to configure it in airflow.cfg:
# [core]
# xcom_backend = path.to.FileXComBackend


file_backend_dag = DAG(
    'file_backend_example',
    default_args=default_args,
    description='Example using file-based XCom backend',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['xcom', 'custom-backend', 'file-storage']
)

generate_large_file_task = PythonOperator(
    task_id='generate_large_data',
    python_callable=generate_large_data_for_file_backend,
    dag=file_backend_dag
)

process_file_task = PythonOperator(
    task_id='process_file_data',
    python_callable=process_file_backend_data,
    dag=file_backend_dag
)

generate_large_file_task >> process_file_task

# =============================================================================
# Example 2: Compressed Backend Usage
# =============================================================================


def generate_repetitive_data():
    """Generate data with lots of repetition (good for compression)"""
    # Generate repetitive data that compresses well
    repetitive_data = {
        "data_type": "repetitive_dataset",
        "patterns": [
            {
                "pattern_id": i,
                "repeated_text": "This is a repeated pattern that appears many times " * 10,
                "numbers": [42] * 50,  # Lots of repeated numbers
                "status": "active",
                "category": "standard",
                "metadata": {
                    "created_by": "system",
                    "version": "1.0",
                    "environment": "production"
                }
            }
            for i in range(20)  # 20 similar patterns
        ],
        "summary": {
            "total_patterns": 20,
            "compression_expected": True,
            "generated_at": datetime.now().isoformat()
        }
    }

    print(
        f"Generated repetitive data with {len(repetitive_data['patterns'])} patterns")

    # This will be automatically compressed by CompressedXComBackend
    return repetitive_data


def analyze_compressed_data(**context):
    """Analyze data that was stored using compressed backend"""
    # Pull data (will be decompressed automatically)
    repetitive_data = context['task_instance'].xcom_pull(
        task_ids='generate_repetitive_data')

    if not repetitive_data:
        raise ValueError("No data received from compressed backend")

    # Analyze the data
    pattern_count = len(repetitive_data['patterns'])
    unique_statuses = set(pattern['status']
                          for pattern in repetitive_data['patterns'])
    unique_categories = set(pattern['category']
                            for pattern in repetitive_data['patterns'])

    analysis = {
        "total_patterns_analyzed": pattern_count,
        "unique_statuses": list(unique_statuses),
        "unique_categories": list(unique_categories),
        "compression_benefit": "High (repetitive data)",
        "analysis_completed_at": datetime.now().isoformat()
    }

    print(f"Analyzed {pattern_count} patterns from compressed backend")

    return analysis


compressed_backend_dag = DAG(
    'compressed_backend_example',
    default_args=default_args,
    description='Example using compressed XCom backend',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['xcom', 'custom-backend', 'compression']
)

generate_repetitive_task = PythonOperator(
    task_id='generate_repetitive_data',
    python_callable=generate_repetitive_data,
    dag=compressed_backend_dag
)

analyze_compressed_task = PythonOperator(
    task_id='analyze_compressed_data',
    python_callable=analyze_compressed_data,
    dag=compressed_backend_dag
)

generate_repetitive_task >> analyze_compressed_task

# =============================================================================
# Example 3: Encrypted Backend Usage
# =============================================================================


def generate_sensitive_data():
    """Generate data containing sensitive information"""
    # Generate data with sensitive information
    sensitive_data = {
        "user_id": "user_12345",
        "username": "john_doe",
        "email": "john@example.com",
        "api_token": "secret_token_abc123xyz789",  # This will trigger encryption
        "password_hash": "hashed_password_value",  # This will trigger encryption
        "profile": {
            "name": "John Doe",
            "age": 30,
            "preferences": ["privacy", "security"]
        },
        "access_key": "access_key_sensitive_data",  # This will trigger encryption
        "created_at": datetime.now().isoformat()
    }

    print("Generated data with sensitive information (will be encrypted)")

    # This will be automatically encrypted by EncryptedXComBackend
    return sensitive_data


def process_sensitive_data(**context):
    """Process data that was stored using encrypted backend"""
    # Pull data (will be decrypted automatically)
    sensitive_data = context['task_instance'].xcom_pull(
        task_ids='generate_sensitive_data')

    if not sensitive_data:
        raise ValueError("No data received from encrypted backend")

    # Process the data (sensitive fields are now decrypted)
    processed_result = {
        "user_processed": sensitive_data['user_id'],
        "profile_name": sensitive_data['profile']['name'],
        "has_api_token": bool(sensitive_data.get('api_token')),
        "has_password_hash": bool(sensitive_data.get('password_hash')),
        "security_level": "high",
        "processing_completed_at": datetime.now().isoformat()
    }

    print("Processed sensitive data (was automatically decrypted)")

    return processed_result


encrypted_backend_dag = DAG(
    'encrypted_backend_example',
    default_args=default_args,
    description='Example using encrypted XCom backend',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['xcom', 'custom-backend', 'encryption', 'security']
)

generate_sensitive_task = PythonOperator(
    task_id='generate_sensitive_data',
    python_callable=generate_sensitive_data,
    dag=encrypted_backend_dag
)

process_sensitive_task = PythonOperator(
    task_id='process_sensitive_data',
    python_callable=process_sensitive_data,
    dag=encrypted_backend_dag
)

generate_sensitive_task >> process_sensitive_task

# =============================================================================
# Configuration Instructions
# =============================================================================

"""
To use these custom XCom backends in production, you would need to:

1. File Backend Configuration (airflow.cfg):
   [core]
   xcom_backend = path.to.your.module.FileXComBackend

2. Compressed Backend Configuration (airflow.cfg):
   [core]
   xcom_backend = path.to.your.module.CompressedXComBackend

3. Encrypted Backend Configuration (airflow.cfg):
   [core]
   xcom_backend = path.to.your.module.EncryptedXComBackend

4. Environment Variables:
   export AIRFLOW__CORE__XCOM_BACKEND=path.to.your.module.FileXComBackend

Note: Only one XCom backend can be active at a time per Airflow instance.
Choose the backend that best fits your use case:

- FileXComBackend: For large datasets that don't fit well in database
- CompressedXComBackend: For repetitive data that compresses well
- EncryptedXComBackend: For sensitive data that needs encryption

For production use:
- Implement proper error handling and logging
- Use secure encryption libraries (not the simple example here)
- Add monitoring and metrics collection
- Implement proper cleanup mechanisms for file-based backends
- Consider backup and recovery strategies
"""
