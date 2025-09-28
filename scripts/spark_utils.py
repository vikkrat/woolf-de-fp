"""
Utility functions for Spark session creation and jar management
"""
import os
import sys
from pyspark.sql import SparkSession

# Add config to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))

try:
    from project_config import SPARK_CONFIG, PROJECT_BASE_PATH
except ImportError:
    # Fallback configuration if config file not available
    SPARK_CONFIG = {
        'mysql_jar_path': '/mnt/d/Projects/Data Engineering/woolf-de-fp/data/mysql-connector-j-8.0.32.jar'
    }
    PROJECT_BASE_PATH = '/mnt/d/Projects/Data Engineering/woolf-de-fp'

def get_mysql_jar_path():
    """
    Get the MySQL JDBC jar file path
    """
    jar_path = SPARK_CONFIG.get('mysql_jar_path')
    if jar_path and os.path.exists(jar_path):
        return jar_path
    
    # Try alternative locations
    alternative_paths = [
        os.path.join(PROJECT_BASE_PATH, "data", "mysql-connector-j-8.0.32.jar"),
        "/opt/spark/jars/mysql-connector-j-8.0.32.jar",
        "./mysql-connector-j-8.0.32.jar"
    ]
    
    for path in alternative_paths:
        if os.path.exists(path):
            print(f"Found MySQL jar at: {path}")
            return path
    
    print("Warning: MySQL JDBC jar not found. MySQL connectivity may not work.")
    return None

def create_spark_session_with_mysql(app_name, additional_configs=None):
    """
    Create a Spark session with MySQL JDBC support
    
    Args:
        app_name (str): Name of the Spark application
        additional_configs (dict): Additional Spark configurations
    
    Returns:
        SparkSession: Configured Spark session
    """
    mysql_jar = get_mysql_jar_path()
    
    builder = SparkSession.builder.appName(app_name)
    
    # Add MySQL jar if available
    if mysql_jar:
        builder = builder.config("spark.jars", mysql_jar)
    
    # Add Kafka package for streaming applications
    if "streaming" in app_name.lower() or "kafka" in app_name.lower():
        builder = builder.config("spark.jars.packages", 
                                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0")
    
    # Default configurations
    default_configs = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
    }
    
    # Apply default configurations
    for key, value in default_configs.items():
        builder = builder.config(key, value)
    
    # Apply additional configurations if provided
    if additional_configs:
        for key, value in additional_configs.items():
            builder = builder.config(key, value)
    
    return builder.getOrCreate()

def create_mysql_connection_properties():
    """
    Create MySQL connection properties for Spark JDBC
    
    Returns:
        dict: Connection properties
    """
    return {
        "url": "jdbc:mysql://localhost:3306/olympic_dataset",
        "driver": "com.mysql.cj.jdbc.Driver",
        "user": "dataeng",
        "password": "password123"
    }

def read_from_mysql(spark, table_name):
    """
    Read data from MySQL table using Spark
    
    Args:
        spark: Spark session
        table_name (str): Name of the MySQL table
    
    Returns:
        DataFrame: Spark DataFrame containing the table data
    """
    properties = create_mysql_connection_properties()
    
    return spark.read \
        .format("jdbc") \
        .option("url", properties["url"]) \
        .option("driver", properties["driver"]) \
        .option("dbtable", table_name) \
        .option("user", properties["user"]) \
        .option("password", properties["password"]) \
        .load()

def write_to_mysql(df, table_name, mode="append"):
    """
    Write DataFrame to MySQL table
    
    Args:
        df: Spark DataFrame to write
        table_name (str): Target MySQL table name
        mode (str): Write mode (append, overwrite, etc.)
    """
    properties = create_mysql_connection_properties()
    
    df.write \
        .format("jdbc") \
        .option("url", properties["url"]) \
        .option("driver", properties["driver"]) \
        .option("dbtable", table_name) \
        .option("user", properties["user"]) \
        .option("password", properties["password"]) \
        .mode(mode) \
        .save()

def validate_jar_availability():
    """
    Validate that required JAR files are available
    
    Returns:
        dict: Status of jar file availability
    """
    mysql_jar = get_mysql_jar_path()
    
    status = {
        "mysql_jdbc": {
            "available": mysql_jar is not None,
            "path": mysql_jar,
            "required_for": ["MySQL connectivity", "JDBC operations"]
        }
    }
    
    return status

if __name__ == "__main__":
    # Test jar availability
    print("=== JAR File Status Check ===")
    status = validate_jar_availability()
    
    for jar_type, info in status.items():
        print(f"\n{jar_type.upper()} JAR:")
        print(f"  Available: {info['available']}")
        print(f"  Path: {info['path']}")
        print(f"  Required for: {', '.join(info['required_for'])}")
    
    # Test Spark session creation
    print("\n=== Testing Spark Session Creation ===")
    try:
        spark = create_spark_session_with_mysql("TestApplication")
        print("✅ Spark session created successfully")
        print(f"Spark version: {spark.version}")
        spark.stop()
    except Exception as e:
        print(f"❌ Error creating Spark session: {e}")