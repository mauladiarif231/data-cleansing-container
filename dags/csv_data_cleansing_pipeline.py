from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.exceptions import AirflowSkipException
import os
import hashlib
from docker.types import Mount

# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 14, 7, 0),  # 07:00 AM WIB today
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

# DAG definition
dag = DAG(
    'csv_data_cleansing_pipeline',
    default_args=default_args,
    description='Hourly CSV data cleansing pipeline',
    schedule_interval='0 * * * *',  # Hourly at 00 minutes
    max_active_runs=1,
    tags=['data-engineering', 'etl', 'csv'],
)

# Task 1: Check source file
check_source_file = FileSensor(
    task_id='check_source_file',
    filepath='/opt/airflow/source/scrap.csv',
    fs_conn_id='fs_default',
    poke_interval=30,
    timeout=600,
    mode='poke',
    dag=dag,
)

# Task 2: Validate database
validate_db = PostgresOperator(
    task_id='validate_database',
    postgres_conn_id='postgres_default',
    sql='SELECT version();',
    dag=dag,
)

# Task 3: Backup source file
backup_source = BashOperator(
    task_id='backup_source_file',
    bash_command='''
    set -e
    timestamp=$(date +%Y%m%d_%H%M%S)
    mkdir -p /opt/airflow/backup
    if [ ! -f /opt/airflow/source/scrap.csv ]; then
        echo "ERROR: Source file not found!"
        exit 1
    fi
    cp /opt/airflow/source/scrap.csv /opt/airflow/backup/scrap_${timestamp}.csv
    if [ -f /opt/airflow/backup/scrap_${timestamp}.csv ]; then
        echo "Backup created: /opt/airflow/backup/scrap_${timestamp}.csv"
    else
        echo "ERROR: Backup failed!"
        exit 1
    fi
    ''',
    dag=dag,
)

# Task 4: Check file changes
def check_file_changes(**context):
    filepath = '/opt/airflow/source/scrap.csv'
    # Compute MD5 hash of the file
    try:
        with open(filepath, 'rb') as f:
            content = f.read()
            current_hash = hashlib.md5(content).hexdigest()
            print(f"Computed hash for {filepath}: {current_hash}")
    except FileNotFoundError:
        raise FileNotFoundError(f"Source file not found: {filepath}")

    postgres_hook = PostgresHook(postgres_conn_id='postgres_data')
    
    # Create table if it doesn't exist
    postgres_hook.run("""
        CREATE TABLE IF NOT EXISTS file_processing_log (
            id SERIAL PRIMARY KEY,
            file_hash VARCHAR(32) UNIQUE,
            file_path VARCHAR(500),
            processing_status VARCHAR(50) DEFAULT 'processed',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    # Check if file was already processed
    result = postgres_hook.get_first("SELECT file_hash FROM file_processing_log WHERE file_hash = %s", (current_hash,))
    if result:
        print(f"File with hash {current_hash} already processed, skipping...")
        raise AirflowSkipException(f"File already processed with hash {current_hash}")
    
    # Insert new hash with explicit parameters
    insert_query = "INSERT INTO file_processing_log (file_hash) VALUES (%s)"
    params = (current_hash,)  # Explicit tuple for parameters
    print(f"Executing INSERT with query: {insert_query} and params: {params}")
    postgres_hook.run(insert_query, parameters=params)
    
    print(f"New file detected with hash {current_hash}, proceeding with processing")
    return "proceed_processing"

check_file_hash = PythonOperator(
    task_id='check_file_changes',
    python_callable=check_file_changes,
    dag=dag,
)

# Task 5: Run data cleansing
run_data_cleansing = DockerOperator(
    task_id='run_data_cleansing',
    image='data-cleaner:latest',
    container_name='data_cleaner_{{ ts_nodash }}',
    api_version='auto',
    auto_remove=True,
    command=["python", "/app/main.py"],
    environment={
        'DB_HOST': 'postgres',
        'DB_PORT': '5432',
        'DB_NAME': 'data_cleansing',
        'DB_USER': 'postgres',
        'DB_PASSWORD': 'password',
        'EXECUTION_DATE': '{{ ds }}',
        'EXECUTION_DATE_NODASH': '{{ ts_nodash }}',
    },
    mounts=[
        Mount(source='/d/Document work/EDTS/edts-de-technical-test-v4-data-cleansing-container-senior-level-question/source', target='/source', type='bind', read_only=True),
        Mount(source='/d/Document work/EDTS/edts-de-technical-test-v4-data-cleansing-container-senior-level-question/target', target='/target', type='bind'),
        Mount(source='/d/Document work/EDTS/edts-de-technical-test-v4-data-cleansing-container-senior-level-question/logs', target='/app/logs', type='bind'),
    ],
    network_mode='data-network',
    mount_tmp_dir=False,
    dag=dag,
)

# Error handling callback
def handle_processing_errors(**context):
    task_instance = context['task_instance']
    dag_run = context['dag_run']
    exception = context.get('exception', 'Unknown error')
    postgres_hook = PostgresHook(postgres_conn_id='postgres_data')
    postgres_hook.run("""
        CREATE TABLE IF NOT EXISTS error_log (
            id SERIAL PRIMARY KEY,
            dag_id VARCHAR(255),
            execution_date TIMESTAMP,
            task_id VARCHAR(255),
            error_message TEXT,
            log_url TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    postgres_hook.run("""
        INSERT INTO error_log (dag_id, execution_date, task_id, error_message)
        VALUES (%s, %s, %s, %s)
    """, (dag_run.dag_id, dag_run.execution_date, task_instance.task_id, str(exception)))

run_data_cleansing.on_failure_callback = handle_processing_errors

# Task 6: Validate output files
def validate_output_files(**context):
    timestamp = context['ts_nodash']
    json_file = f'/opt/airflow/target/data_{timestamp}.json'
    csv_file = f'/opt/airflow/target/data_reject_{timestamp}.csv'
    if not (os.path.exists(json_file) and os.path.exists(csv_file)):
        raise FileNotFoundError(f"Missing output files: {json_file} or {csv_file}")
    return "Output files validated"

validate_outputs = PythonOperator(
    task_id='validate_output_files',
    python_callable=validate_output_files,
    dag=dag,
)

# Task 7: Data quality check
def perform_data_quality_checks(**context):
    postgres_hook = PostgresHook(postgres_conn_id='postgres_data')
    postgres_hook.run("""
        CREATE TABLE IF NOT EXISTS data (
            dates DATE,
            ids VARCHAR(255) PRIMARY KEY,
            names VARCHAR(255),
            monthly_listeners BIGINT,
            popularity INTEGER,
            followers BIGINT,
            genres TEXT,
            first_release VARCHAR(4),
            last_release VARCHAR(4),
            num_releases INTEGER,
            num_tracks INTEGER,
            playlists_found VARCHAR(255),
            feat_track_ids TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS data_reject (
            id SERIAL PRIMARY KEY,
            dates DATE,
            ids VARCHAR(255),
            names VARCHAR(255),
            monthly_listeners BIGINT,
            popularity INTEGER,
            followers BIGINT,
            genres TEXT,
            first_release VARCHAR(4),
            last_release VARCHAR(4),
            num_releases INTEGER,
            num_tracks INTEGER,
            playlists_found VARCHAR(255),
            feat_track_ids TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    clean_count = postgres_hook.get_first("SELECT COUNT(*) FROM data")[0]
    reject_count = postgres_hook.get_first("SELECT COUNT(*) FROM data_reject")[0]
    if clean_count + reject_count == 0:
        raise ValueError("No data processed")
    return f"Clean: {clean_count}, Reject: {reject_count}"

data_quality_check = PythonOperator(
    task_id='data_quality_check',
    python_callable=perform_data_quality_checks,
    dag=dag,
)

# Task 8: Collect metrics
def collect_pipeline_metrics(**context):
    postgres_hook = PostgresHook(postgres_conn_id='postgres_data')
    postgres_hook.run("""
        CREATE TABLE IF NOT EXISTS pipeline_metrics (
            id SERIAL PRIMARY KEY,
            execution_date DATE,
            total_records INTEGER,
            clean_records INTEGER,
            rejected_records INTEGER,
            processing_time FLOAT,
            file_size BIGINT,
            memory_usage FLOAT,
            success_rate FLOAT,
            dag_run_id VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    quality_results = context['task_instance'].xcom_pull(task_ids='data_quality_check')
    if not quality_results or "No data processed" in quality_results:
        print("No valid data quality results available, inserting default values")
        clean = 0
        reject = 0
    else:
        try:
            parts = quality_results.split(', ')
            clean_part = parts[0].split(': ')[1]
            reject_part = parts[1].split(': ')[1]
            clean = int(clean_part)
            reject = int(reject_part)
        except (ValueError, AttributeError, IndexError) as e:
            print(f"Failed to parse quality results '{quality_results}': {str(e)}")
            clean = 0
            reject = 0
    
    params = (context['ds'], clean + reject, clean, reject, context['dag_run'].run_id)
    print(f"Executing INSERT with parameters: {params}")
    postgres_hook.run("""
        INSERT INTO pipeline_metrics (execution_date, total_records, clean_records, rejected_records, dag_run_id)
        VALUES (%s, %s, %s, %s, %s)
    """, parameters=params)

metrics_collection = PythonOperator(
    task_id='collect_metrics',
    python_callable=collect_pipeline_metrics,
    dag=dag,
)

# Task 9: Archive files
archive_files = BashOperator(
    task_id='archive_processed_files',
    bash_command='''
    timestamp={{ ts_nodash }}
    mkdir -p /opt/airflow/archive/$timestamp
    cp /opt/airflow/target/data_$timestamp.json /opt/airflow/archive/$timestamp/ 2>/dev/null || true
    cp /opt/airflow/target/data_reject_$timestamp.csv /opt/airflow/archive/$timestamp/ 2>/dev/null || true
    echo "Files archived for $timestamp"
    ''',
    dag=dag,
)

# Task 10: Cleanup old files
cleanup_old_files = BashOperator(
    task_id='cleanup_old_files',
    bash_command='''
    find /opt/airflow/archive -type d -mtime +7 -exec rm -rf {} + 2>/dev/null || true
    find /opt/airflow/target -name "*.json" -mtime +7 -delete 2>/dev/null || true
    find /opt/airflow/target -name "*.csv" -mtime +7 -delete 2>/dev/null || true
    ''',
    dag=dag,
)

# Task Dependencies
check_source_file >> validate_db >> backup_source >> check_file_hash >> run_data_cleansing
run_data_cleansing >> [validate_outputs, data_quality_check] >> metrics_collection >> archive_files >> cleanup_old_files