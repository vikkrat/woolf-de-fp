import requests
import os
import pandas as pd
import random
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

def create_spark_session():
    """
    Create Spark session for batch processing
    """
    # Path to the MySQL JDBC jar file
    mysql_jar_path = "/mnt/d/Projects/Data Engineering/woolf-de-fp/data/mysql-connector-j-8.0.32.jar"
    
    return SparkSession.builder \
        .appName("LandingToBronze") \
        .config("spark.jars", mysql_jar_path) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def download_data(local_file_path):
    """
    Download Olympic data from external source or create sample data
    """
    print(f"Downloading data to {local_file_path}")
    
    try:
        # Try to download real Olympic data
        base_url = "https://raw.githubusercontent.com/rfordatascience/tidytuesday/master/data/2021/2021-07-27/"
        
        # Download athlete bio data
        bio_response = requests.get(f"{base_url}olympics.csv", timeout=30)
        if bio_response.status_code == 200:
            with open(f"{local_file_path}/athlete_bio.csv", 'wb') as file:
                file.write(bio_response.content)
            print("Downloaded real Olympic data")
        else:
            raise Exception("Failed to download real data")
        
        # Create sample event results (since original might not be available)
        create_sample_event_results(f"{local_file_path}/athlete_event_results.csv")
        
    except Exception as e:
        print(f"Download failed ({e}), creating sample data instead...")
        create_sample_data(local_file_path)

def create_sample_data(local_file_path):
    """
    Create sample Olympic dataset
    """
    print("Creating sample athlete bio data...")
    
    # Sample data for athlete bio
    athletes = []
    countries = ['USA', 'GER', 'CHN', 'RUS', 'GBR', 'FRA', 'ITA', 'AUS', 'CAN', 'JPN']
    sports = ['Athletics', 'Swimming', 'Cycling', 'Hockey', 'Boxing', 'Wrestling', 'Shooting', 'Rowing']
    
    for i in range(1, 2001):  # Create 2000 athletes
        sex = random.choice(['Male', 'Female'])
        height = random.uniform(150, 200) if sex == 'Male' else random.uniform(140, 190)
        weight = random.uniform(50, 100) if sex == 'Female' else random.uniform(60, 120)
        
        athletes.append({
            'athlete_id': i,
            'name': f'Athlete_{i}',
            'sex': sex,
            'country_noc': random.choice(countries),
            'height': round(height, 1),
            'weight': round(weight, 1),
            'sport': random.choice(sports),
            'age': random.randint(18, 35)
        })
    
    # Save athlete bio data
    pd.DataFrame(athletes).to_csv(f"{local_file_path}/athlete_bio.csv", index=False)
    print(f"Created {len(athletes)} athlete records")
    
    # Create event results
    create_sample_event_results(f"{local_file_path}/athlete_event_results.csv")

def create_sample_event_results(file_path):
    """
    Create sample event results data
    """
    print("Creating sample event results...")
    
    events = []
    sports = ['Athletics', 'Swimming', 'Cycling', 'Hockey', 'Boxing', 'Wrestling', 'Shooting', 'Rowing']
    medals = ['Gold', 'Silver', 'Bronze', None, None, None]  # More non-medalists
    
    for i in range(10000):  # Create 10000 event records
        events.append({
            'result_id': i + 1,
            'athlete_id': random.randint(1, 2000),
            'sport': random.choice(sports),
            'medal': random.choice(medals),
            'timestamp': (datetime.now() - timedelta(days=random.randint(0, 1095))).strftime('%Y-%m-%d %H:%M:%S'),
            'event_year': random.choice([2016, 2020, 2021, 2024]),
            'season': random.choice(['Summer', 'Winter'])
        })
    
    # Save event results
    pd.DataFrame(events).to_csv(file_path, index=False)
    print(f"Created {len(events)} event records")

def process_to_bronze(spark, input_path, output_path, table_name):
    """
    Process CSV files to Bronze layer (Parquet format)
    """
    print(f"Processing {table_name} to bronze layer...")
    
    try:
        # Read CSV file with error handling
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("multiline", "true") \
            .option("escape", '"') \
            .csv(f"{input_path}/{table_name}.csv")
        
        # Add metadata columns
        df = df.withColumn("ingestion_timestamp", 
                          spark.sql("SELECT current_timestamp()").collect()[0][0]) \
              .withColumn("source_file", spark.lit(f"{table_name}.csv"))
        
        # Write to Bronze layer as Parquet with partitioning
        df.coalesce(4) \
          .write \
          .mode("overwrite") \
          .option("compression", "snappy") \
          .parquet(f"{output_path}/{table_name}")
        
        record_count = df.count()
        print(f"Successfully processed {record_count} records for {table_name}")
        
        # Log summary statistics
        print(f"Schema for {table_name}:")
        df.printSchema()
        
    except Exception as e:
        print(f"Error processing {table_name}: {e}")
        raise

def validate_bronze_data(spark, bronze_path):
    """
    Validate data quality in bronze layer
    """
    print("Validating bronze layer data...")
    
    try:
        # Check athlete_bio
        bio_df = spark.read.parquet(f"{bronze_path}/athlete_bio")
        bio_count = bio_df.count()
        print(f"Athlete bio records: {bio_count}")
        
        # Check for null athlete_ids
        null_ids = bio_df.filter(bio_df.athlete_id.isNull()).count()
        if null_ids > 0:
            print(f"Warning: {null_ids} records with null athlete_id")
        
        # Check athlete_event_results
        events_df = spark.read.parquet(f"{bronze_path}/athlete_event_results")
        events_count = events_df.count()
        print(f"Event results records: {events_count}")
        
        # Check data quality metrics
        medal_dist = events_df.groupBy("medal").count().collect()
        print("Medal distribution:")
        for row in medal_dist:
            print(f"  {row['medal']}: {row['count']}")
            
        return True
        
    except Exception as e:
        print(f"Validation error: {e}")
        return False

def main():
    """
    Main function for Landing to Bronze processing
    """
    print("Starting Landing to Bronze processing...")
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Define paths (adjust these for your environment)
        base_path = "/mnt/d/Projects/Data Engineering/woolf-de-fp"
        landing_path = f"{base_path}/data/landing"
        bronze_path = f"{base_path}/data/bronze"
        
        # Create directories
        os.makedirs(landing_path, exist_ok=True)
        os.makedirs(bronze_path, exist_ok=True)
        
        # Download/create data
        download_data(landing_path)
        
        # Process to bronze layer
        process_to_bronze(spark, landing_path, bronze_path, "athlete_bio")
        process_to_bronze(spark, landing_path, bronze_path, "athlete_event_results")
        
        # Validate the results
        if validate_bronze_data(spark, bronze_path):
            print("Bronze layer processing completed successfully!")
        else:
            print("Bronze layer validation failed!")
        
    except Exception as e:
        print(f"Error in main processing: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()