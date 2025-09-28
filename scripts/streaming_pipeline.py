from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

def create_spark_session():
    """
    Create Spark session with necessary configurations
    """
    # Path to the MySQL JDBC jar file
    mysql_jar_path = "/mnt/d/Projects/Data Engineering/woolf-de-fp/data/mysql-connector-j-8.0.32.jar"
    
    return SparkSession.builder \
        .appName("OlympicStreamingPipeline") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
        .config("spark.jars", mysql_jar_path) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

def read_mysql_table(spark, table_name):
    """
    Read data from MySQL table
    """
    return spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/olympic_dataset") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", table_name) \
        .option("user", "dataeng") \
        .option("password", "password123") \
        .load()

def write_to_kafka(df, topic_name):
    """
    Write DataFrame to Kafka topic
    """
    df.selectExpr("CAST(athlete_id AS STRING) AS key", "to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", topic_name) \
        .mode("append") \
        .save()

def read_from_kafka(spark, topic_name):
    """
    Read streaming data from Kafka topic
    """
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

def process_stream_data(spark):
    """
    Main streaming data processing logic
    """
    print("Reading athlete bio data from MySQL...")
    # Read athlete bio data (static)
    athlete_bio = read_mysql_table(spark, "athlete_bio")
    
    print("Setting up Kafka stream...")
    # Read streaming data from Kafka
    kafka_stream = read_from_kafka(spark, "athlete_event_results")
    
    # Parse JSON from Kafka
    schema = StructType([
        StructField("athlete_id", IntegerType()),
        StructField("sport", StringType()),
        StructField("medal", StringType()),
        StructField("timestamp", StringType())
    ])
    
    print("Parsing Kafka messages...")
    parsed_stream = kafka_stream.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Filter out rows with null medals and convert timestamp
    print("Filtering and transforming data...")
    filtered_stream = parsed_stream.filter(
        (col("medal") != "nan") & 
        col("medal").isNotNull()
    ).withColumn("timestamp", to_timestamp(col("timestamp")))
    
    # Join with athlete bio data
    print("Joining with athlete bio data...")
    enriched_stream = filtered_stream.join(
        athlete_bio,
        "athlete_id",
        "inner"
    )
    
    # Calculate aggregations with windowing
    print("Calculating aggregations...")
    aggregated_stream = enriched_stream \
        .withWatermark("timestamp", "10 minutes") \
        .groupBy(
            window(col("timestamp"), "5 minutes"),
            "sport", "medal", "sex", "country_noc"
        ) \
        .agg(
            avg("height").alias("avg_height"),
            avg("weight").alias("avg_weight"),
            count("*").alias("athlete_count"),
            current_timestamp().alias("processed_time")
        ) \
        .select(
            col("sport"),
            col("medal"), 
            col("sex"),
            col("country_noc"),
            col("avg_height"),
            col("avg_weight"),
            col("athlete_count"),
            col("processed_time")
        )
    
    return aggregated_stream

def write_to_mysql(batch_df, batch_id):
    """
    Write batch DataFrame to MySQL
    """
    try:
        print(f"Writing batch {batch_id} to MySQL...")
        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:mysql://localhost:3306/olympic_dataset") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", "athlete_enriched_agg") \
            .option("user", "dataeng") \
            .option("password", "password123") \
            .mode("append") \
            .save()
        print(f"Successfully wrote batch {batch_id}")
    except Exception as e:
        print(f"Error writing batch {batch_id}: {e}")

def main():
    """
    Main function to run the streaming pipeline
    """
    print("Starting Olympic Streaming Pipeline...")
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Process streaming data
        print("Processing streaming data...")
        result_stream = process_stream_data(spark)
        
        # Write to MySQL using foreachBatch
        print("Starting streaming query...")
        query = result_stream.writeStream \
            .foreachBatch(write_to_mysql) \
            .outputMode("update") \
            .option("checkpointLocation", "/tmp/streaming_checkpoint") \
            .trigger(processingTime='30 seconds') \
            .start()
        
        print("Streaming query started. Waiting for termination...")
        query.awaitTermination()
        
    except Exception as e:
        print(f"Error in streaming pipeline: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()