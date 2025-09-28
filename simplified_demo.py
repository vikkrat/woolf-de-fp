#!/usr/bin/env python3
"""
Olympic Data Engineering Project - Simplified Demo
Demonstrates complete ETL pipeline with results for screenshots
"""

import pandas as pd
import numpy as np
import json
import os
from datetime import datetime, timedelta
import random
import mysql.connector
from mysql.connector import Error
import warnings
warnings.filterwarnings('ignore')

def create_sample_olympic_data():
    """Create comprehensive Olympic sample data"""
    print("🎯 CREATING OLYMPIC SAMPLE DATA")
    print("=" * 50)
    
    # Ensure data directories exist
    os.makedirs("data/landing", exist_ok=True)
    os.makedirs("data/bronze", exist_ok=True)
    os.makedirs("data/silver", exist_ok=True)
    os.makedirs("data/gold", exist_ok=True)
    
    # Athletes data
    countries = ['USA', 'CHN', 'GBR', 'RUS', 'GER', 'JPN', 'FRA', 'ITA', 'AUS', 'CAN', 'NED', 'BRA']
    sports = ['Swimming', 'Athletics', 'Gymnastics', 'Rowing', 'Cycling', 'Boxing', 'Wrestling', 'Weightlifting']
    
    athletes_data = []
    for i in range(1000):
        athlete = {
            'athlete_id': f'ATH{i+1:04d}',
            'name': f'Athlete {i+1}',
            'country': random.choice(countries),
            'sport': random.choice(sports),
            'age': random.randint(18, 35),
            'gender': random.choice(['M', 'F']),
            'height': random.randint(150, 210),
            'weight': random.randint(50, 120)
        }
        athletes_data.append(athlete)
    
    # Events data with results
    events_data = []
    for i in range(2000):
        athlete = random.choice(athletes_data)
        event = {
            'event_id': f'EVT{i+1:04d}',
            'athlete_id': athlete['athlete_id'],
            'event_name': f"{athlete['sport']} - Event {(i % 20) + 1}",
            'date': (datetime.now() - timedelta(days=random.randint(1, 30))).strftime('%Y-%m-%d'),
            'result': round(random.uniform(10.0, 30.0), 2),
            'medal': random.choices(['Gold', 'Silver', 'Bronze', None], weights=[5, 8, 12, 75])[0],
            'country': athlete['country']
        }
        events_data.append(event)
    
    # Save to CSV files
    athletes_df = pd.DataFrame(athletes_data)
    events_df = pd.DataFrame(events_data)
    
    athletes_df.to_csv('data/landing/athletes.csv', index=False)
    events_df.to_csv('data/landing/events.csv', index=False)
    
    print(f"✅ Created {len(athletes_data)} athletes records")
    print(f"✅ Created {len(events_data)} events records")
    print("📁 Data saved to data/landing/")
    
    return athletes_df, events_df

def process_etl_pipeline(athletes_df, events_df):
    """Process complete ETL pipeline"""
    print("\n🔄 RUNNING ETL PIPELINE")
    print("=" * 50)
    
    # BRONZE LAYER - Raw data with basic validation
    print("📊 Processing Landing → Bronze...")
    
    # Clean and validate data
    athletes_bronze = athletes_df.copy()
    athletes_bronze = athletes_bronze.dropna()
    athletes_bronze['created_at'] = datetime.now()
    
    events_bronze = events_df.copy()
    events_bronze = events_bronze.dropna()
    events_bronze['created_at'] = datetime.now()
    
    # Save bronze data
    athletes_bronze.to_csv('data/bronze/athletes_bronze.csv', index=False)
    events_bronze.to_csv('data/bronze/events_bronze.csv', index=False)
    print(f"✅ Bronze layer: {len(athletes_bronze)} athletes, {len(events_bronze)} events")
    
    # SILVER LAYER - Cleaned and standardized data
    print("📊 Processing Bronze → Silver...")
    
    # Data cleaning and standardization
    athletes_silver = athletes_bronze.copy()
    athletes_silver['name'] = athletes_silver['name'].str.upper()
    athletes_silver['country'] = athletes_silver['country'].str.upper()
    athletes_silver['bmi'] = round(athletes_silver['weight'] / (athletes_silver['height']/100)**2, 1)
    
    events_silver = events_bronze.copy()
    events_silver['event_name'] = events_silver['event_name'].str.upper()
    events_silver['country'] = events_silver['country'].str.upper()
    events_silver['year'] = pd.to_datetime(events_silver['date']).dt.year
    events_silver['month'] = pd.to_datetime(events_silver['date']).dt.month
    
    # Save silver data
    athletes_silver.to_csv('data/silver/athletes_silver.csv', index=False)
    events_silver.to_csv('data/silver/events_silver.csv', index=False)
    print(f"✅ Silver layer: {len(athletes_silver)} athletes, {len(events_silver)} events")
    
    # GOLD LAYER - Business aggregations
    print("📊 Processing Silver → Gold...")
    
    # Medal counts by country
    medal_counts = events_silver[events_silver['medal'].notna()].groupby(['country', 'medal']).size().reset_index(name='count')
    
    # Country performance summary
    country_summary = events_silver.groupby('country').agg({
        'athlete_id': 'nunique',
        'event_id': 'count',
        'result': 'mean',
        'medal': lambda x: x.notna().sum()
    }).reset_index()
    country_summary.columns = ['country', 'total_athletes', 'total_events', 'avg_performance', 'total_medals']
    country_summary = country_summary.round(2)
    
    # Sport performance analysis
    sport_analysis = events_silver.merge(athletes_silver[['athlete_id', 'sport']], on='athlete_id')
    sport_summary = sport_analysis.groupby('sport').agg({
        'athlete_id': 'nunique',
        'result': ['mean', 'min', 'max'],
        'medal': lambda x: x.notna().sum()
    }).reset_index()
    sport_summary.columns = ['sport', 'total_athletes', 'avg_result', 'best_result', 'worst_result', 'total_medals']
    sport_summary = sport_summary.round(2)
    
    # Top performers
    top_performers = events_silver[events_silver['medal'].notna()].merge(
        athletes_silver[['athlete_id', 'name', 'sport']], on='athlete_id'
    ).groupby(['athlete_id', 'name', 'sport', 'country']).size().reset_index(name='medal_count')
    top_performers = top_performers.sort_values('medal_count', ascending=False).head(20)
    
    # Save gold data
    medal_counts.to_csv('data/gold/medal_counts_by_country.csv', index=False)
    country_summary.to_csv('data/gold/country_performance_summary.csv', index=False)
    sport_summary.to_csv('data/gold/sport_analysis.csv', index=False)
    top_performers.to_csv('data/gold/top_performers.csv', index=False)
    
    print(f"✅ Gold layer: Created 4 analytical datasets")
    print("   📊 Medal counts by country")
    print("   📊 Country performance summary") 
    print("   📊 Sport analysis")
    print("   📊 Top performers")
    
    return {
        'medal_counts': medal_counts,
        'country_summary': country_summary,
        'sport_summary': sport_summary,
        'top_performers': top_performers
    }

def setup_mysql_database():
    """Setup MySQL database and tables"""
    try:
        print("\n💾 SETTING UP MYSQL DATABASE")
        print("=" * 50)
        
        # Connect to MySQL
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='root123',
            charset='utf8mb4',
            collation='utf8mb4_unicode_ci'
        )
        cursor = connection.cursor()
        
        # Create database
        cursor.execute("CREATE DATABASE IF NOT EXISTS olympic_db")
        cursor.execute("USE olympic_db")
        
        # Create tables
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS athlete_bio (
                athlete_id VARCHAR(20) PRIMARY KEY,
                name VARCHAR(100),
                country VARCHAR(10),
                sport VARCHAR(50),
                age INT,
                gender CHAR(1),
                height INT,
                weight INT,
                bmi DECIMAL(4,1),
                created_at DATETIME
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS athlete_events (
                event_id VARCHAR(20) PRIMARY KEY,
                athlete_id VARCHAR(20),
                event_name VARCHAR(100),
                event_date DATE,
                result DECIMAL(8,2),
                medal VARCHAR(10),
                country VARCHAR(10),
                created_at DATETIME,
                FOREIGN KEY (athlete_id) REFERENCES athlete_bio(athlete_id)
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS medal_summary (
                id INT AUTO_INCREMENT PRIMARY KEY,
                country VARCHAR(10),
                medal_type VARCHAR(10),
                count INT,
                created_at DATETIME
            )
        """)
        
        print("✅ Database and tables created successfully")
        return connection
        
    except Error as e:
        print(f"❌ MySQL Error: {e}")
        return None

def populate_mysql_with_results(connection, gold_data):
    """Populate MySQL with processed results"""
    if not connection:
        print("❌ No database connection available")
        return
        
    try:
        print("\n📊 POPULATING MYSQL WITH RESULTS")
        print("=" * 50)
        
        cursor = connection.cursor()
        
        # Load silver data for MySQL population
        athletes_silver = pd.read_csv('data/silver/athletes_silver.csv')
        events_silver = pd.read_csv('data/silver/events_silver.csv')
        
        # Clear existing data
        cursor.execute("DELETE FROM medal_summary")
        cursor.execute("DELETE FROM athlete_events")
        cursor.execute("DELETE FROM athlete_bio")
        
        # Insert athlete bio data
        for _, row in athletes_silver.head(100).iterrows():  # Limit for demo
            cursor.execute("""
                INSERT INTO athlete_bio 
                (athlete_id, name, country, sport, age, gender, height, weight, bmi, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['athlete_id'], row['name'], row['country'], row['sport'],
                int(row['age']), row['gender'], int(row['height']), int(row['weight']),
                float(row['bmi']), datetime.now()
            ))
        
        # Insert events data
        events_sample = events_silver.head(200)  # Limit for demo
        for _, row in events_sample.iterrows():
            cursor.execute("""
                INSERT INTO athlete_events 
                (event_id, athlete_id, event_name, event_date, result, medal, country, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['event_id'], row['athlete_id'], row['event_name'],
                row['date'], float(row['result']), row['medal'],
                row['country'], datetime.now()
            ))
        
        # Insert medal summary
        for _, row in gold_data['medal_counts'].iterrows():
            cursor.execute("""
                INSERT INTO medal_summary (country, medal_type, count, created_at)
                VALUES (%s, %s, %s, %s)
            """, (row['country'], row['medal'], int(row['count']), datetime.now()))
        
        connection.commit()
        print("✅ Successfully populated MySQL database")
        print(f"   📊 {len(athletes_silver.head(100))} athletes inserted")
        print(f"   📊 {len(events_sample)} events inserted")
        print(f"   📊 {len(gold_data['medal_counts'])} medal records inserted")
        
    except Error as e:
        print(f"❌ Error populating database: {e}")

def display_final_results(gold_data, connection=None):
    """Display comprehensive results for screenshots"""
    print("\n📈 FINAL PROJECT RESULTS")
    print("=" * 60)
    
    print("\n🏆 TOP 10 COUNTRIES BY MEDAL COUNT")
    print("-" * 40)
    country_medals = gold_data['medal_counts'].groupby('country')['count'].sum().sort_values(ascending=False).head(10)
    for i, (country, count) in enumerate(country_medals.items(), 1):
        print(f"{i:2d}. {country:<4} - {count:3d} medals")
    
    print("\n🏅 MEDAL DISTRIBUTION BY TYPE")
    print("-" * 40)
    medal_dist = gold_data['medal_counts'].groupby('medal')['count'].sum()
    for medal, count in medal_dist.items():
        print(f"{medal:7} - {count:3d} medals")
    
    print("\n⭐ TOP 10 PERFORMING ATHLETES")
    print("-" * 40)
    top_10 = gold_data['top_performers'].head(10)
    for i, row in top_10.iterrows():
        print(f"{i+1:2d}. {row['name']:<15} ({row['country']}) - {row['medal_count']} medals")
    
    print("\n🏃 SPORT PERFORMANCE ANALYSIS")
    print("-" * 40)
    sport_stats = gold_data['sport_summary'].sort_values('total_medals', ascending=False)
    for _, row in sport_stats.iterrows():
        print(f"{row['sport']:<12} - {row['total_athletes']:3d} athletes, {row['total_medals']:3d} medals")
    
    # MySQL Results if available
    if connection:
        print("\n💾 MYSQL DATABASE SAMPLE QUERIES")
        print("-" * 40)
        try:
            cursor = connection.cursor()
            
            # Top countries query
            cursor.execute("""
                SELECT country, COUNT(*) as medal_count 
                FROM athlete_events 
                WHERE medal IS NOT NULL 
                GROUP BY country 
                ORDER BY medal_count DESC 
                LIMIT 5
            """)
            results = cursor.fetchall()
            print("Top 5 countries with medals:")
            for country, count in results:
                print(f"  {country}: {count} medals")
            
            # Athletes by sport
            cursor.execute("""
                SELECT sport, COUNT(*) as athlete_count 
                FROM athlete_bio 
                GROUP BY sport 
                ORDER BY athlete_count DESC
            """)
            results = cursor.fetchall()
            print("\nAthletes by sport:")
            for sport, count in results:
                print(f"  {sport}: {count} athletes")
                
        except Error as e:
            print(f"❌ MySQL query error: {e}")

def simulate_kafka_streaming():
    """Simulate Kafka streaming results"""
    print("\n📡 KAFKA STREAMING SIMULATION")
    print("-" * 40)
    print("🔄 Simulating real-time Olympic data streaming...")
    
    # Simulate streaming data messages
    streaming_messages = [
        {"athlete_id": "ATH0001", "event": "Swimming 100m", "time": "47.23", "timestamp": "2024-09-28T15:30:00Z"},
        {"athlete_id": "ATH0045", "event": "Athletics 200m", "time": "19.85", "timestamp": "2024-09-28T15:31:00Z"},
        {"athlete_id": "ATH0123", "event": "Gymnastics Floor", "score": "14.75", "timestamp": "2024-09-28T15:32:00Z"},
        {"athlete_id": "ATH0089", "event": "Swimming 50m", "time": "21.34", "timestamp": "2024-09-28T15:33:00Z"},
        {"athlete_id": "ATH0234", "event": "Boxing 75kg", "result": "WIN", "timestamp": "2024-09-28T15:34:00Z"}
    ]
    
    print("📊 Sample streaming messages (Topic: olympic-events-stream):")
    for i, msg in enumerate(streaming_messages, 1):
        print(f"  Message {i}: {json.dumps(msg, indent=2)}")
    
    print(f"\n✅ Simulated {len(streaming_messages)} streaming messages")
    print("📈 In production: Kafka would process 1000+ messages/second")

def main():
    """Main demo execution"""
    print("🎯 OLYMPIC DATA ENGINEERING PROJECT - FINAL DEMO")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    try:
        # Step 1: Create sample data
        athletes_df, events_df = create_sample_olympic_data()
        
        # Step 2: Run ETL pipeline
        gold_data = process_etl_pipeline(athletes_df, events_df)
        
        # Step 3: Setup MySQL (optional)
        connection = setup_mysql_database()
        if connection:
            populate_mysql_with_results(connection, gold_data)
        
        # Step 4: Display results
        display_final_results(gold_data, connection)
        
        # Step 5: Simulate Kafka streaming
        simulate_kafka_streaming()
        
        print(f"\n🎉 DEMO COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print("📸 Ready for screenshots:")
        print("   1. File system data lake structure (data/gold/ folder)")
        print("   2. CSV analysis results (country_performance_summary.csv)")
        print("   3. MySQL database tables (if available)")
        print("   4. Kafka streaming simulation output")
        print("=" * 60)
        
        if connection:
            connection.close()
            
    except Exception as e:
        print(f"\n❌ Demo failed with error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()