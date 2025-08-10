from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator

def identify_archive_data(**context):
    """Find data older than 30 days"""
    execution_date = context['execution_date']
    archive_cutoff = execution_date - timedelta(days=30)

    print(
        f"Identifying data to archive for month ending: {execution_date.strftime('%Y-%m-%d')}")
    print(f"Archive cutoff date: {archive_cutoff.strftime('%Y-%m-%d')}")

    # Simulate data identification process
    print("  - Scanning transaction tables...")
    print("  - Scanning log files...")
    print("  - Scanning user activity data...")
    print("  - Scanning temporary files...")

    # Simulate identified data
    import random
    data_categories = {
        'transactions': random.randint(50000, 100000),
        'logs': random.randint(1000000, 2000000),
        'user_activity': random.randint(200000, 500000),
        'temp_files': random.randint(10000, 50000)
    }

    total_records = sum(data_categories.values())
    estimated_size_gb = total_records * 0.001  # Rough estimate

    print(f"  - Data identified for archiving:")
    for category, count in data_categories.items():
        print(f"    {category}: {count:,} records")

    print(f"  - Total records: {total_records:,}")
    print(f"  - Estimated size: {estimated_size_gb:.2f} GB")

    return {
        'archive_month': execution_date.strftime('%Y-%m'),
        'cutoff_date': archive_cutoff.strftime('%Y-%m-%d'),
        'data_categories': data_categories,
        'total_records': total_records,
        'estimated_size_gb': estimated_size_gb
    }


def backup_to_storage(**context):
    """Create backup of data to be archived"""
    archive_data = context['task_instance'].xcom_pull(
        task_ids='identify_archive_data')

    print(f"Creating backup for {archive_data['total_records']:,} records...")
    print(f"Estimated backup size: {archive_data['estimated_size_gb']:.2f} GB")

    # Simulate backup process
    print("  - Initializing backup storage connection...")
    print("  - Creating backup directory structure...")

    backup_files = []
    for category, count in archive_data['data_categories'].items():
        print(f"  - Backing up {category} data ({count:,} records)...")

        # Simulate backup file creation
        backup_file = f"backup_{archive_data['archive_month']}_{category}.tar.gz"
        backup_files.append(backup_file)

        # Simulate backup time based on data size
        import time
        time.sleep(0.5)  # Simulate backup time

        print(f"    Created: {backup_file}")

    print("  - Verifying backup integrity...")
    print("  - Calculating checksums...")
    print("  - Backup completed successfully!")

    return {
        'backup_files': backup_files,
        'backup_location': f"s3://company-archives/{archive_data['archive_month']}/",
        'backup_size_gb': archive_data['estimated_size_gb'],
        'backup_timestamp': datetime.now().isoformat()
    }


def compress_data(**context):
    """Compress archived data"""
    backup_data = context['task_instance'].xcom_pull(
        task_ids='backup_to_storage')

    print("Compressing archived data...")
    print(f"Processing {len(backup_data['backup_files'])} backup files...")

    compressed_files = []
    total_original_size = backup_data['backup_size_gb']

    for backup_file in backup_data['backup_files']:
        print(f"  - Compressing {backup_file}...")

        # Simulate compression
        import random
        compression_ratio = random.uniform(0.3, 0.7)  # 30-70% compression

        compressed_file = backup_file.replace('.tar.gz', '.tar.bz2')
        compressed_files.append(compressed_file)

        print(f"    Original: {backup_file}")
        print(f"    Compressed: {compressed_file}")
        print(f"    Compression ratio: {compression_ratio:.1%}")

    # Calculate total compressed size
    total_compressed_size = total_original_size * 0.5  # Average 50% compression
    space_saved = total_original_size - total_compressed_size

    print(f"  - Compression completed!")
    print(f"  - Original size: {total_original_size:.2f} GB")
    print(f"  - Compressed size: {total_compressed_size:.2f} GB")
    print(
        f"  - Space saved: {space_saved:.2f} GB ({space_saved/total_original_size:.1%})")

    return {
        'compressed_files': compressed_files,
        'original_size_gb': total_original_size,
        'compressed_size_gb': total_compressed_size,
        'space_saved_gb': space_saved,
        'compression_ratio': space_saved / total_original_size
    }


def update_metadata(**context):
    """Update data catalog with archive information"""
    archive_data = context['task_instance'].xcom_pull(
        task_ids='identify_archive_data')
    backup_data = context['task_instance'].xcom_pull(
        task_ids='backup_to_storage')
    compression_data = context['task_instance'].xcom_pull(
        task_ids='compress_data')

    print("Updating data catalog with archive information...")

    # Create metadata record
    metadata_record = {
        'archive_id': f"archive_{archive_data['archive_month']}",
        'archive_date': datetime.now().isoformat(),
        'data_period': archive_data['archive_month'],
        'cutoff_date': archive_data['cutoff_date'],
        'total_records': archive_data['total_records'],
        'data_categories': archive_data['data_categories'],
        'backup_location': backup_data['backup_location'],
        'compressed_files': compression_data['compressed_files'],
        'original_size_gb': compression_data['original_size_gb'],
        'compressed_size_gb': compression_data['compressed_size_gb'],
        'space_saved_gb': compression_data['space_saved_gb'],
        'status': 'archived'
    }

    print("  - Creating metadata record...")
    print(f"    Archive ID: {metadata_record['archive_id']}")
    print(f"    Data period: {metadata_record['data_period']}")
    print(f"    Records archived: {metadata_record['total_records']:,}")
    print(f"    Storage location: {metadata_record['backup_location']}")

    print("  - Updating data catalog database...")
    print("  - Creating archive index entries...")
    print("  - Updating data lineage information...")
    print("  - Generating archive manifest...")

    print("  - Metadata update completed!")

    return metadata_record


def cleanup_old_data(**context):
    """Remove archived data from active storage"""
    archive_data = context['task_instance'].xcom_pull(
        task_ids='identify_archive_data')
    metadata_record = context['task_instance'].xcom_pull(
        task_ids='update_metadata')

    print("Cleaning up archived data from active storage...")
    print(f"Removing {archive_data['total_records']:,} archived records...")

    # Simulate cleanup process
    cleanup_summary = {
        'tables_cleaned': [],
        'records_removed': 0,
        'space_freed_gb': 0
    }

    for category, count in archive_data['data_categories'].items():
        print(f"  - Cleaning {category} data...")
        print(f"    Removing {count:,} records...")

        # Simulate cleanup
        table_name = f"{category}_table"
        cleanup_summary['tables_cleaned'].append(table_name)
        cleanup_summary['records_removed'] += count

        # Estimate space freed (rough calculation)
        space_freed = count * 0.001  # GB per record
        cleanup_summary['space_freed_gb'] += space_freed

        print(f"    Cleaned table: {table_name}")
        print(f"    Space freed: {space_freed:.2f} GB")

    print("  - Running database optimization...")
    print("  - Updating table statistics...")
    print("  - Rebuilding indexes...")

    print(f"  - Cleanup completed!")
    print(f"  - Total records removed: {cleanup_summary['records_removed']:,}")
    print(f"  - Total space freed: {cleanup_summary['space_freed_gb']:.2f} GB")
    print(f"  - Tables cleaned: {len(cleanup_summary['tables_cleaned'])}")

    # Final verification
    print("  - Verifying archive integrity...")
    print("  - Confirming data accessibility in archive...")
    print("  - Archive process completed successfully!")

    return cleanup_summary


default_args = {
    'owner': 'data_team',
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
    'depends_on_past': False,
    'execution_timeout': timedelta(hours=4),
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = DAG(
    dag_id="monthly_data_archive",
    description="Archive previous month's data on first day of the month",
    schedule_interval="0 3 1 * *",
    start_date=datetime(2024, 2, 1),
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=['archive', 'monthly', 'maintenance']
)

identify_data_task = PythonOperator(
    task_id='identify_archive_data',
    python_callable=identify_archive_data,
    dag=dag
)

backup_data_task = PythonOperator(
    task_id='backup_to_storage',
    python_callable=backup_to_storage,
    dag=dag
)

compress_data_task = PythonOperator(
    task_id='compress_data',
    python_callable=compress_data,
    dag=dag
)

update_metadata_task = PythonOperator(
    task_id='update_metadata',
    python_callable=update_metadata,
    dag=dag
)

cleanup_data_task = PythonOperator(
    task_id='cleanup_old_data',
    python_callable=cleanup_old_data,
    dag=dag
)

# Set up linear dependencies - each task depends on the previous one
identify_data_task >> backup_data_task >> compress_data_task >> update_metadata_task >> cleanup_data_task
