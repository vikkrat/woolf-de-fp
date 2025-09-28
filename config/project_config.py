"""
Project Configuration Settings for Olympic Data Engineering Project
"""
import os

# Project Paths
PROJECT_BASE_PATH = "/mnt/d/Projects/Data Engineering/woolf-de-fp"
DATA_PATH = os.path.join(PROJECT_BASE_PATH, "data")
LOGS_PATH = os.path.join(PROJECT_BASE_PATH, "logs")

# Database Configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'dataeng', 
    'password': 'password123',
    'database': 'olympic_dataset'
}

# Kafka Configuration
KAFKA_CONFIG = {
    'bootstrap_servers': ['localhost:9092'],
    'topic': 'athlete_event_results',
    'auto_offset_reset': 'latest',
    'group_id': 'olympic_data_consumers'
}

# Spark Configuration
SPARK_CONFIG = {
    'app_name': 'Olympic Data Pipeline',
    'master': 'local[*]',
    'mysql_jar_path': os.path.join(PROJECT_BASE_PATH, "data", "mysql-connector-j-8.0.32.jar"),
    'jar_packages': [
        'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0'
    ],
    'config': {
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true',
        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer'
    }
}

# Airflow Configuration
AIRFLOW_CONFIG = {
    'dag_id': 'olympic_data_pipeline',
    'schedule_interval': '0 6 * * *',  # Daily at 6 AM
    'default_args': {
        'owner': 'dataeng',
        'retries': 1,
        'retry_delay_minutes': 5
    }
}

# Data Lake Layers
LAYERS = {
    'landing': os.path.join(DATA_PATH, "landing"),
    'bronze': os.path.join(DATA_PATH, "bronze"), 
    'silver': os.path.join(DATA_PATH, "silver"),
    'gold': os.path.join(DATA_PATH, "gold")
}

# Data Quality Thresholds
DATA_QUALITY = {
    'height_range': (100, 250),  # cm
    'weight_range': (30, 200),   # kg
    'age_range': (12, 50),       # years
    'min_completeness_score': 0.8
}

# Streaming Configuration
STREAMING_CONFIG = {
    'checkpoint_location': '/tmp/streaming_checkpoint',
    'processing_time_trigger': '30 seconds',
    'watermark_threshold': '10 minutes'
}

# Analytics Configuration
ANALYTICS_CONFIG = {
    'top_countries_limit': 20,
    'top_athletes_limit': 50,
    'performance_categories': {
        'Elite': {'gold_medals': 3, 'total_medals': 5},
        'High Performer': {'gold_medals': 1, 'total_medals': 3},
        'Medalist': {'gold_medals': 0, 'total_medals': 1}
    }
}