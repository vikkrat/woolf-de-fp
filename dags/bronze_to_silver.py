from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

def create_spark_session():
    """
    Create Spark session for Bronze to Silver processing
    """
    # Path to the MySQL JDBC jar file
    mysql_jar_path = "/mnt/d/Projects/Data Engineering/woolf-de-fp/data/mysql-connector-j-8.0.32.jar"
    
    return SparkSession.builder \
        .appName("BronzeToSilver") \
        .config("spark.jars", mysql_jar_path) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

def clean_text_columns(df, columns):
    """
    Clean text columns by trimming whitespace and handling empty strings
    """
    for col_name in columns:
        if col_name in df.columns:
            df = df.withColumn(col_name, trim(col(col_name)))
            df = df.withColumn(col_name, when(col(col_name) == "", None).otherwise(col(col_name)))
            df = df.withColumn(col_name, when(col(col_name) == "nan", None).otherwise(col(col_name)))
    return df

def remove_duplicates(df, key_columns):
    """
    Remove duplicates based on key columns, keeping the latest record
    """
    if "ingestion_timestamp" in df.columns:
        # Keep the most recent record for each key
        window_spec = Window.partitionBy(*key_columns).orderBy(desc("ingestion_timestamp"))
        df = df.withColumn("row_number", row_number().over(window_spec)) \
              .filter(col("row_number") == 1) \
              .drop("row_number")
    else:
        df = df.dropDuplicates(key_columns)
    
    return df

def validate_numeric_ranges(df, column_configs):
    """
    Validate numeric columns are within expected ranges
    column_configs: dict with column_name: (min_val, max_val) 
    """
    for col_name, (min_val, max_val) in column_configs.items():
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                when(
                    (col(col_name) >= min_val) & (col(col_name) <= max_val),
                    col(col_name)
                ).otherwise(None)
            )
    return df

def standardize_categories(df, column_mappings):
    """
    Standardize categorical values
    column_mappings: dict with column_name: {old_value: new_value}
    """
    for col_name, mappings in column_mappings.items():
        if col_name in df.columns:
            # Create when-otherwise chain for mappings
            mapping_expr = col(col_name)
            for old_val, new_val in mappings.items():
                mapping_expr = when(col(col_name) == old_val, new_val).otherwise(mapping_expr)
            df = df.withColumn(col_name, mapping_expr)
    return df

def process_athlete_bio(spark, bronze_path):
    """
    Process athlete bio data from bronze to silver
    """
    print("Processing athlete bio data...")
    
    # Read bronze data
    df = spark.read.parquet(f"{bronze_path}/athlete_bio")
    
    print(f"Bronze athlete bio records: {df.count()}")
    
    # Clean text columns
    text_columns = ["name", "sex", "country_noc", "sport"] 
    df = clean_text_columns(df, text_columns)
    
    # Standardize sex values
    sex_mappings = {
        "sex": {
            "M": "Male",
            "F": "Female", 
            "male": "Male",
            "female": "Female"
        }
    }
    df = standardize_categories(df, sex_mappings)
    
    # Validate height and weight ranges
    numeric_ranges = {
        "height": (100, 250),  # cm
        "weight": (30, 200),   # kg
        "age": (12, 50) if "age" in df.columns else None
    }
    # Remove None values from numeric_ranges
    numeric_ranges = {k: v for k, v in numeric_ranges.items() if v is not None}
    df = validate_numeric_ranges(df, numeric_ranges)
    
    # Remove duplicates based on athlete_id
    df = remove_duplicates(df, ["athlete_id"])
    
    # Filter out records with missing critical data
    df = df.filter(
        col("athlete_id").isNotNull() &
        col("name").isNotNull() &
        col("sex").isNotNull()
    )
    
    # Add data quality flags
    df = df.withColumn(
        "data_quality_score",
        when(col("height").isNull() | col("weight").isNull(), 0.7)
        .otherwise(1.0)
    )
    
    # Add processing metadata
    df = df.withColumn("processed_timestamp", current_timestamp()) \
          .withColumn("data_layer", lit("silver"))
    
    print(f"Cleaned athlete bio records: {df.count()}")
    
    return df

def process_athlete_events(spark, bronze_path):
    """
    Process athlete event results from bronze to silver
    """
    print("Processing athlete event results...")
    
    # Read bronze data
    df = spark.read.parquet(f"{bronze_path}/athlete_event_results")
    
    print(f"Bronze event records: {df.count()}")
    
    # Clean text columns
    text_columns = ["sport", "medal", "season"] if "season" in df.columns else ["sport", "medal"]
    df = clean_text_columns(df, text_columns)
    
    # Standardize medal values
    medal_mappings = {
        "medal": {
            "gold": "Gold",
            "silver": "Silver", 
            "bronze": "Bronze",
            "Gold Medal": "Gold",
            "Silver Medal": "Silver",
            "Bronze Medal": "Bronze"
        }
    }
    df = standardize_categories(df, medal_mappings)
    
    # Filter out invalid records
    df = df.filter(
        col("athlete_id").isNotNull() &
        col("sport").isNotNull()
    )
    
    # Handle timestamp column if it exists
    if "timestamp" in df.columns:
        df = df.withColumn(
            "timestamp",
            when(col("timestamp").isNull(), current_timestamp())
            .otherwise(to_timestamp(col("timestamp")))
        )
    
    # Add event year if not present
    if "event_year" not in df.columns:
        df = df.withColumn("event_year", year(current_timestamp()))
    
    # Validate event years (reasonable range)
    df = df.withColumn(
        "event_year",
        when(
            (col("event_year") >= 1896) & (col("event_year") <= 2030),
            col("event_year")
        ).otherwise(None)
    )
    
    # Create medal indicator
    df = df.withColumn(
        "is_medalist",
        when(col("medal").isin(["Gold", "Silver", "Bronze"]), True)
        .otherwise(False)
    )
    
    # Add processing metadata
    df = df.withColumn("processed_timestamp", current_timestamp()) \
          .withColumn("data_layer", lit("silver"))
    
    print(f"Cleaned event records: {df.count()}")
    
    return df

def validate_silver_data(athlete_bio_df, athlete_events_df):
    """
    Validate silver layer data quality
    """
    print("Validating silver layer data...")
    
    # Check athlete bio validation
    bio_count = athlete_bio_df.count()
    bio_complete = athlete_bio_df.filter(
        col("athlete_id").isNotNull() & 
        col("name").isNotNull() & 
        col("sex").isNotNull()
    ).count()
    
    print(f"Athlete bio - Total: {bio_count}, Complete records: {bio_complete}")
    
    # Check event results validation
    events_count = athlete_events_df.count()
    events_with_athletes = athlete_events_df.filter(
        col("athlete_id").isNotNull() & 
        col("sport").isNotNull()
    ).count()
    
    print(f"Event results - Total: {events_count}, Valid records: {events_with_athletes}")
    
    # Check medal distribution
    medal_dist = athlete_events_df.groupBy("medal").count().collect()
    print("Medal distribution in silver layer:")
    for row in medal_dist:
        medal = row['medal'] if row['medal'] else 'No Medal'
        print(f"  {medal}: {row['count']}")
    
    # Check data quality scores
    avg_quality = athlete_bio_df.agg(avg("data_quality_score").alias("avg_quality")).collect()[0]["avg_quality"]
    print(f"Average data quality score: {avg_quality:.2f}")
    
    return bio_complete > 0 and events_with_athletes > 0

def process_bronze_to_silver(spark, bronze_path, silver_path):
    """
    Main function to process all bronze data to silver layer
    """
    print("Starting Bronze to Silver processing...")
    
    # Process athlete bio data
    athlete_bio_clean = process_athlete_bio(spark, bronze_path)
    
    # Process athlete events data  
    athlete_events_clean = process_athlete_events(spark, bronze_path)
    
    # Validate processed data
    if not validate_silver_data(athlete_bio_clean, athlete_events_clean):
        raise Exception("Silver layer validation failed")
    
    # Write to silver layer with partitioning
    print("Writing athlete bio to silver layer...")
    athlete_bio_clean.coalesce(2) \
        .write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(f"{silver_path}/athlete_bio")
    
    print("Writing athlete events to silver layer...")
    athlete_events_clean.coalesce(4) \
        .write \
        .mode("overwrite") \
        .partitionBy("event_year") \
        .option("compression", "snappy") \
        .parquet(f"{silver_path}/athlete_event_results")
    
    print("Silver layer processing completed successfully!")

def main():
    """
    Main function for Bronze to Silver processing
    """
    print("Starting Bronze to Silver data processing...")
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Define paths
        base_path = "/mnt/d/Projects/Data Engineering/woolf-de-fp"
        bronze_path = f"{base_path}/data/bronze"
        silver_path = f"{base_path}/data/silver"
        
        # Create silver directory
        os.makedirs(silver_path, exist_ok=True)
        
        # Process bronze to silver
        process_bronze_to_silver(spark, bronze_path, silver_path)
        
        print("Bronze to Silver processing completed!")
        
    except Exception as e:
        print(f"Error in Bronze to Silver processing: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()