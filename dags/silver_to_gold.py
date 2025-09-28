from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

def create_spark_session():
    """
    Create Spark session for Silver to Gold processing
    """
    # Path to the MySQL JDBC jar file
    mysql_jar_path = "/mnt/d/Projects/Data Engineering/woolf-de-fp/data/mysql-connector-j-8.0.32.jar"
    
    return SparkSession.builder \
        .appName("SilverToGold") \
        .config("spark.jars", mysql_jar_path) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

def create_athlete_performance_summary(joined_data):
    """
    Create comprehensive athlete performance analytics
    """
    print("Creating athlete performance summary...")
    
    athlete_summary = joined_data.groupBy("athlete_id", "name", "sex", "country_noc").agg(
        count("*").alias("total_events"),
        countDistinct("sport").alias("sports_participated"),
        sum(when(col("medal") == "Gold", 1).otherwise(0)).alias("gold_medals"),
        sum(when(col("medal") == "Silver", 1).otherwise(0)).alias("silver_medals"),
        sum(when(col("medal") == "Bronze", 1).otherwise(0)).alias("bronze_medals"),
        sum(when(col("is_medalist") == True, 1).otherwise(0)).alias("total_medals"),
        avg("height").alias("height"),
        avg("weight").alias("weight"),
        first("data_quality_score").alias("data_quality_score"),
        current_timestamp().alias("created_at")
    )
    
    # Calculate medal efficiency (medals per event)
    athlete_summary = athlete_summary.withColumn(
        "medal_efficiency",
        round(col("total_medals") / col("total_events"), 3)
    )
    
    # Add performance category
    athlete_summary = athlete_summary.withColumn(
        "performance_category",
        when(col("gold_medals") >= 3, "Elite")
        .when(col("total_medals") >= 3, "High Performer") 
        .when(col("total_medals") >= 1, "Medalist")
        .otherwise("Participant")
    )
    
    return athlete_summary

def create_country_analytics(joined_data):
    """
    Create country-level analytics
    """
    print("Creating country analytics...")
    
    country_stats = joined_data.groupBy("country_noc").agg(
        countDistinct("athlete_id").alias("total_athletes"),
        countDistinct("sport").alias("sports_represented"),
        count("*").alias("total_events"),
        sum(when(col("medal") == "Gold", 1).otherwise(0)).alias("gold_medals"),
        sum(when(col("medal") == "Silver", 1).otherwise(0)).alias("silver_medals"),
        sum(when(col("medal") == "Bronze", 1).otherwise(0)).alias("bronze_medals"),
        sum(when(col("is_medalist") == True, 1).otherwise(0)).alias("total_medals"),
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight"),
        current_timestamp().alias("created_at")
    )
    
    # Calculate medal ratios
    country_stats = country_stats.withColumn(
        "medals_per_athlete",
        round(col("total_medals") / col("total_athletes"), 2)
    ).withColumn(
        "gold_medal_ratio", 
        round(col("gold_medals") / (col("total_medals") + 1), 3)  # +1 to avoid division by zero
    )
    
    # Add country ranking based on total medals
    window_spec = Window.orderBy(desc("total_medals"))
    country_stats = country_stats.withColumn(
        "medal_rank",
        row_number().over(window_spec)
    )
    
    return country_stats

def create_sport_analytics(joined_data):
    """
    Create sport-level analytics including gender participation
    """
    print("Creating sport analytics...")
    
    sport_stats = joined_data.groupBy("sport").agg(
        countDistinct("athlete_id").alias("total_athletes"),
        countDistinct("country_noc").alias("countries_represented"),
        count("*").alias("total_events"),
        sum(when(col("sex") == "Male", 1).otherwise(0)).alias("male_participants"),
        sum(when(col("sex") == "Female", 1).otherwise(0)).alias("female_participants"),
        sum(when(col("is_medalist") == True, 1).otherwise(0)).alias("total_medals"),
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight"),
        current_timestamp().alias("created_at")
    )
    
    # Calculate gender ratios and competitiveness
    sport_stats = sport_stats.withColumn(
        "gender_ratio_male",
        round(col("male_participants") / col("total_athletes"), 2)
    ).withColumn(
        "gender_ratio_female", 
        round(col("female_participants") / col("total_athletes"), 2)
    ).withColumn(
        "competitiveness_score",
        round(col("countries_represented") / col("total_athletes") * 100, 1)
    )
    
    return sport_stats

def create_medal_trends(joined_data):
    """
    Create medal trends over time (if event_year is available)
    """
    print("Creating medal trends...")
    
    if "event_year" in joined_data.columns:
        medal_trends = joined_data.filter(col("event_year").isNotNull()) \
            .groupBy("event_year", "country_noc").agg(
                sum(when(col("medal") == "Gold", 1).otherwise(0)).alias("gold_medals"),
                sum(when(col("medal") == "Silver", 1).otherwise(0)).alias("silver_medals"), 
                sum(when(col("medal") == "Bronze", 1).otherwise(0)).alias("bronze_medals"),
                sum(when(col("is_medalist") == True, 1).otherwise(0)).alias("total_medals"),
                countDistinct("athlete_id").alias("athletes_count"),
                current_timestamp().alias("created_at")
            )
        
        # Add year-over-year growth calculation
        window_spec = Window.partitionBy("country_noc").orderBy("event_year")
        medal_trends = medal_trends.withColumn(
            "prev_year_medals",
            lag("total_medals", 1).over(window_spec)
        ).withColumn(
            "medal_growth",
            when(col("prev_year_medals").isNotNull(),
                 round((col("total_medals") - col("prev_year_medals")) / col("prev_year_medals") * 100, 1))
            .otherwise(None)
        )
        
        return medal_trends
    else:
        # Return empty DataFrame with expected schema if no event_year
        schema = StructType([
            StructField("event_year", IntegerType()),
            StructField("country_noc", StringType()),
            StructField("gold_medals", LongType()),
            StructField("silver_medals", LongType()),
            StructField("bronze_medals", LongType()),
            StructField("total_medals", LongType()),
            StructField("athletes_count", LongType()),
            StructField("medal_growth", DoubleType()),
            StructField("created_at", TimestampType())
        ])
        return joined_data.sparkSession.createDataFrame([], schema)

def create_physical_attributes_analysis(joined_data):
    """
    Analyze physical attributes by sport and medal performance
    """
    print("Creating physical attributes analysis...")
    
    physical_analysis = joined_data.filter(
        col("height").isNotNull() & col("weight").isNotNull()
    ).groupBy("sport", "medal", "sex").agg(
        count("*").alias("athlete_count"),
        avg("height").alias("avg_height"),
        stddev("height").alias("stddev_height"),
        avg("weight").alias("avg_weight"), 
        stddev("weight").alias("stddev_weight"),
        min("height").alias("min_height"),
        max("height").alias("max_height"),
        min("weight").alias("min_weight"),
        max("weight").alias("max_weight"),
        current_timestamp().alias("created_at")
    )
    
    # Calculate BMI statistics
    physical_analysis = physical_analysis.withColumn(
        "avg_bmi",
        round(col("avg_weight") / pow(col("avg_height") / 100, 2), 1)
    )
    
    # Add height/weight categories
    physical_analysis = physical_analysis.withColumn(
        "height_category",
        when(col("avg_height") >= 180, "Tall")
        .when(col("avg_height") >= 170, "Medium")
        .otherwise("Short")
    ).withColumn(
        "weight_category",
        when(col("avg_weight") >= 80, "Heavy")
        .when(col("avg_weight") >= 65, "Medium") 
        .otherwise("Light")
    )
    
    return physical_analysis

def create_gold_aggregations(spark, silver_path, gold_path):
    """
    Create all gold layer aggregations and analytics
    """
    print("Creating gold layer analytics...")
    
    # Read silver layer data
    athlete_bio = spark.read.parquet(f"{silver_path}/athlete_bio")
    athlete_events = spark.read.parquet(f"{silver_path}/athlete_event_results")
    
    print(f"Silver layer - Athletes: {athlete_bio.count()}, Events: {athlete_events.count()}")
    
    # Join the datasets
    joined_data = athlete_events.join(athlete_bio, "athlete_id", "inner")
    
    print(f"Joined data records: {joined_data.count()}")
    
    # Create various analytics
    athlete_performance = create_athlete_performance_summary(joined_data)
    country_analytics = create_country_analytics(joined_data)
    sport_analytics = create_sport_analytics(joined_data)
    medal_trends = create_medal_trends(joined_data)
    physical_analysis = create_physical_attributes_analysis(joined_data)
    
    # Create overall summary statistics
    overall_stats = joined_data.agg(
        countDistinct("athlete_id").alias("total_unique_athletes"),
        countDistinct("country_noc").alias("total_countries"),
        countDistinct("sport").alias("total_sports"),
        count("*").alias("total_events"),
        sum(when(col("is_medalist") == True, 1).otherwise(0)).alias("total_medal_events"),
        current_timestamp().alias("created_at")
    ).withColumn("dataset_name", lit("Olympic Analytics Summary"))
    
    # Write all analytics to gold layer
    analytics = {
        "athlete_performance_summary": athlete_performance,
        "country_analytics": country_analytics,
        "sport_analytics": sport_analytics,
        "medal_trends": medal_trends,
        "physical_attributes_analysis": physical_analysis,
        "overall_summary": overall_stats
    }
    
    for name, df in analytics.items():
        if df.count() > 0:  # Only write non-empty DataFrames
            print(f"Writing {name} to gold layer ({df.count()} records)...")
            df.coalesce(1) \
              .write \
              .mode("overwrite") \
              .option("compression", "snappy") \
              .parquet(f"{gold_path}/{name}")
        else:
            print(f"Skipping {name} - no data to write")
    
    print("Gold layer analytics created successfully!")

def validate_gold_layer(spark, gold_path):
    """
    Validate gold layer data
    """
    print("Validating gold layer data...")
    
    try:
        # Check each analytics table
        analytics_files = [
            "athlete_performance_summary",
            "country_analytics", 
            "sport_analytics",
            "physical_attributes_analysis",
            "overall_summary"
        ]
        
        for file_name in analytics_files:
            try:
                df = spark.read.parquet(f"{gold_path}/{file_name}")
                count = df.count()
                print(f"{file_name}: {count} records")
            except Exception as e:
                print(f"Warning: Could not read {file_name}: {e}")
        
        return True
        
    except Exception as e:
        print(f"Gold layer validation error: {e}")
        return False

def main():
    """
    Main function for Silver to Gold processing
    """
    print("Starting Silver to Gold data processing...")
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Define paths
        base_path = "/mnt/d/Projects/Data Engineering/woolf-de-fp"
        silver_path = f"{base_path}/data/silver"
        gold_path = f"{base_path}/data/gold"
        
        # Create gold directory
        os.makedirs(gold_path, exist_ok=True)
        
        # Create gold layer aggregations
        create_gold_aggregations(spark, silver_path, gold_path)
        
        # Validate results
        if validate_gold_layer(spark, gold_path):
            print("Silver to Gold processing completed successfully!")
        else:
            print("Warning: Gold layer validation had issues")
        
    except Exception as e:
        print(f"Error in Silver to Gold processing: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()