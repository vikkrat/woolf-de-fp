from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import os

# Default arguments for the DAG
default_args = {
    'owner': 'dataeng',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1
}

# DAG definition
dag = DAG(
    'olympic_data_pipeline',
    default_args=default_args,
    description='End-to-End Olympic Data Pipeline - Batch Processing',
    schedule_interval='0 6 * * *',  # Daily at 6 AM
    catchup=False,
    tags=['olympic', 'data-engineering', 'etl']
)

# Configuration
PROJECT_BASE_PATH = "/mnt/d/Projects/Data Engineering/woolf-de-fp"
SPARK_HOME = "/opt/spark"  # Adjust based on your Spark installation
PYTHON_PATH = "/usr/bin/python3"  # Adjust based on your Python installation

def check_prerequisites():
    """
    Check if required directories and files exist
    """
    required_paths = [
        f"{PROJECT_BASE_PATH}/dags",
        f"{PROJECT_BASE_PATH}/data",
        f"{PROJECT_BASE_PATH}/logs"
    ]
    
    for path in required_paths:
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
            print(f"Created directory: {path}")
        else:
            print(f"Directory exists: {path}")
    
    # Check if processing scripts exist
    scripts = [
        f"{PROJECT_BASE_PATH}/dags/landing_to_bronze.py",
        f"{PROJECT_BASE_PATH}/dags/bronze_to_silver.py", 
        f"{PROJECT_BASE_PATH}/dags/silver_to_gold.py"
    ]
    
    for script in scripts:
        if os.path.exists(script):
            print(f"Script found: {script}")
        else:
            raise FileNotFoundError(f"Required script not found: {script}")

def validate_landing_data():
    """
    Validate that landing data exists and is accessible
    """
    landing_path = f"{PROJECT_BASE_PATH}/data/landing"
    expected_files = ["athlete_bio.csv", "athlete_event_results.csv"]
    
    os.makedirs(landing_path, exist_ok=True)
    
    for file_name in expected_files:
        file_path = os.path.join(landing_path, file_name)
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            print(f"Landing file validated: {file_name}")
        else:
            print(f"Warning: Landing file missing or empty: {file_name}")

def cleanup_checkpoints():
    """
    Clean up any existing checkpoints from previous runs
    """
    checkpoint_paths = [
        "/tmp/checkpoint",
        "/tmp/streaming_checkpoint"
    ]
    
    for path in checkpoint_paths:
        if os.path.exists(path):
            import shutil
            shutil.rmtree(path)
            print(f"Cleaned up checkpoint: {path}")

def log_pipeline_start():
    """
    Log the start of pipeline execution
    """
    print(f"Starting Olympic Data Pipeline at {datetime.now()}")
    print(f"Project base path: {PROJECT_BASE_PATH}")

def log_pipeline_completion():
    """
    Log the completion of pipeline execution
    """
    print(f"Olympic Data Pipeline completed at {datetime.now()}")
    
    # Log data layer statistics
    layers = ["bronze", "silver", "gold"]
    for layer in layers:
        layer_path = f"{PROJECT_BASE_PATH}/data/{layer}"
        if os.path.exists(layer_path):
            subdirs = [d for d in os.listdir(layer_path) 
                      if os.path.isdir(os.path.join(layer_path, d))]
            print(f"{layer.capitalize()} layer contains: {len(subdirs)} datasets")

# Task definitions

# Pre-processing tasks
check_prerequisites_task = PythonOperator(
    task_id='check_prerequisites',
    python_callable=check_prerequisites,
    dag=dag
)

validate_landing_task = PythonOperator(
    task_id='validate_landing_data',
    python_callable=validate_landing_data,
    dag=dag
)

cleanup_task = PythonOperator(
    task_id='cleanup_checkpoints',
    python_callable=cleanup_checkpoints,
    dag=dag
)

log_start_task = PythonOperator(
    task_id='log_pipeline_start',
    python_callable=log_pipeline_start,
    dag=dag
)

# Main processing tasks using Python operators (since SparkSubmitOperator may need additional configuration)
landing_to_bronze_task = BashOperator(
    task_id='landing_to_bronze',
    bash_command=f'cd {PROJECT_BASE_PATH} && python3 dags/landing_to_bronze.py',
    env={
        'PYTHONPATH': PROJECT_BASE_PATH,
        'PYSPARK_PYTHON': 'python3'
    },
    dag=dag
)

bronze_to_silver_task = BashOperator(
    task_id='bronze_to_silver',
    bash_command=f'cd {PROJECT_BASE_PATH} && python3 dags/bronze_to_silver.py',
    env={
        'PYTHONPATH': PROJECT_BASE_PATH,
        'PYSPARK_PYTHON': 'python3'
    },
    dag=dag
)

silver_to_gold_task = BashOperator(
    task_id='silver_to_gold',
    bash_command=f'cd {PROJECT_BASE_PATH} && python3 dags/silver_to_gold.py',
    env={
        'PYTHONPATH': PROJECT_BASE_PATH,
        'PYSPARK_PYTHON': 'python3'
    },
    dag=dag
)

# Data validation tasks
validate_bronze_task = BashOperator(
    task_id='validate_bronze_layer',
    bash_command=f'''
    echo "Validating Bronze layer..."
    if [ -d "{PROJECT_BASE_PATH}/data/bronze" ]; then
        echo "Bronze layer directory exists"
        find {PROJECT_BASE_PATH}/data/bronze -name "*.parquet" -type f | head -5
    else
        echo "ERROR: Bronze layer directory not found"
        exit 1
    fi
    ''',
    dag=dag
)

validate_silver_task = BashOperator(
    task_id='validate_silver_layer',
    bash_command=f'''
    echo "Validating Silver layer..."
    if [ -d "{PROJECT_BASE_PATH}/data/silver" ]; then
        echo "Silver layer directory exists"
        find {PROJECT_BASE_PATH}/data/silver -name "*.parquet" -type f | head -5
    else
        echo "ERROR: Silver layer directory not found"
        exit 1
    fi
    ''',
    dag=dag
)

validate_gold_task = BashOperator(
    task_id='validate_gold_layer',
    bash_command=f'''
    echo "Validating Gold layer..."
    if [ -d "{PROJECT_BASE_PATH}/data/gold" ]; then
        echo "Gold layer directory exists"
        find {PROJECT_BASE_PATH}/data/gold -name "*.parquet" -type f | head -10
    else
        echo "ERROR: Gold layer directory not found"
        exit 1
    fi
    ''',
    dag=dag
)

# Post-processing tasks
log_completion_task = PythonOperator(
    task_id='log_pipeline_completion',
    python_callable=log_pipeline_completion,
    dag=dag
)

# Optional: Create data quality report
create_quality_report_task = BashOperator(
    task_id='create_data_quality_report',
    bash_command=f'''
    echo "=== Olympic Data Pipeline Quality Report ===" > {PROJECT_BASE_PATH}/logs/quality_report_$(date +%Y%m%d).txt
    echo "Execution Date: $(date)" >> {PROJECT_BASE_PATH}/logs/quality_report_$(date +%Y%m%d).txt
    echo "" >> {PROJECT_BASE_PATH}/logs/quality_report_$(date +%Y%m%d).txt
    
    for layer in bronze silver gold; do
        echo "=== ${{layer^^}} LAYER ===" >> {PROJECT_BASE_PATH}/logs/quality_report_$(date +%Y%m%d).txt
        if [ -d "{PROJECT_BASE_PATH}/data/$layer" ]; then
            echo "Datasets: $(ls {PROJECT_BASE_PATH}/data/$layer | wc -l)" >> {PROJECT_BASE_PATH}/logs/quality_report_$(date +%Y%m%d).txt
            echo "Files: $(find {PROJECT_BASE_PATH}/data/$layer -type f | wc -l)" >> {PROJECT_BASE_PATH}/logs/quality_report_$(date +%Y%m%d).txt
            echo "Size: $(du -sh {PROJECT_BASE_PATH}/data/$layer | cut -f1)" >> {PROJECT_BASE_PATH}/logs/quality_report_$(date +%Y%m%d).txt
        else
            echo "Directory not found" >> {PROJECT_BASE_PATH}/logs/quality_report_$(date +%Y%m%d).txt
        fi
        echo "" >> {PROJECT_BASE_PATH}/logs/quality_report_$(date +%Y%m%d).txt
    done
    
    echo "Quality report created: {PROJECT_BASE_PATH}/logs/quality_report_$(date +%Y%m%d).txt"
    ''',
    dag=dag
)

# Define task dependencies
check_prerequisites_task >> validate_landing_task >> cleanup_task >> log_start_task

log_start_task >> landing_to_bronze_task >> validate_bronze_task
validate_bronze_task >> bronze_to_silver_task >> validate_silver_task
validate_silver_task >> silver_to_gold_task >> validate_gold_task

validate_gold_task >> [log_completion_task, create_quality_report_task]

# Optional: Add alerts/notifications task
# send_completion_notification_task = BashOperator(
#     task_id='send_completion_notification',
#     bash_command='echo "Pipeline completed successfully" | mail -s "Olympic Data Pipeline Success" admin@example.com',
#     dag=dag
# )
# 
# [log_completion_task, create_quality_report_task] >> send_completion_notification_task

if __name__ == "__main__":
    dag.cli()