#!/usr/bin/env python3
"""
MySQL Database Demo - Show Olympic Data in Database
"""

import mysql.connector
import pandas as pd
from datetime import datetime

def create_and_populate_database():
    """Create database and populate with sample data"""
    try:
        # Connect and create database
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='root123'
        )
        cursor = conn.cursor()
        
        print("🏗️  CREATING MYSQL DATABASE STRUCTURE")
        print("=" * 50)
        
        cursor.execute("CREATE DATABASE IF NOT EXISTS olympic_db")
        cursor.execute("USE olympic_db")
        
        # Create tables
        cursor.execute("DROP TABLE IF EXISTS athlete_events")
        cursor.execute("DROP TABLE IF EXISTS medal_summary") 
        cursor.execute("DROP TABLE IF EXISTS athlete_bio")
        
        cursor.execute("""
            CREATE TABLE athlete_bio (
                athlete_id VARCHAR(20) PRIMARY KEY,
                name VARCHAR(100),
                country VARCHAR(10),
                sport VARCHAR(50),
                age INT,
                gender CHAR(1),
                height INT,
                weight INT,
                created_at DATETIME
            )
        """)
        
        cursor.execute("""
            CREATE TABLE athlete_events (
                event_id VARCHAR(20) PRIMARY KEY,
                athlete_id VARCHAR(20),
                event_name VARCHAR(100),
                event_date DATE,
                result DECIMAL(8,2),
                medal VARCHAR(10),
                country VARCHAR(10),
                created_at DATETIME
            )
        """)
        
        cursor.execute("""
            CREATE TABLE medal_summary (
                id INT AUTO_INCREMENT PRIMARY KEY,
                country VARCHAR(10),
                medal_type VARCHAR(10),
                count INT,
                rank_position INT,
                created_at DATETIME
            )
        """)
        
        print("✅ Database tables created successfully")
        
        # Insert sample data
        print("\n📊 INSERTING SAMPLE DATA")
        print("=" * 50)
        
        # Athletes data
        athletes_data = [
            ('ATH0001', 'Michael Johnson', 'USA', 'Athletics', 28, 'M', 185, 75),
            ('ATH0002', 'Katie Ledecky', 'USA', 'Swimming', 25, 'F', 183, 73),
            ('ATH0003', 'Usain Bolt', 'JPN', 'Athletics', 32, 'M', 195, 86),
            ('ATH0004', 'Simone Biles', 'USA', 'Gymnastics', 24, 'F', 142, 47),
            ('ATH0005', 'Adam Peaty', 'GBR', 'Swimming', 27, 'M', 191, 84),
            ('ATH0006', 'Elaine Thompson', 'JPN', 'Athletics', 29, 'F', 167, 57),
            ('ATH0007', 'Caeleb Dressel', 'USA', 'Swimming', 25, 'M', 191, 88),
            ('ATH0008', 'Sydney McLaughlin', 'USA', 'Athletics', 22, 'F', 175, 61),
            ('ATH0009', 'Ryan Murphy', 'USA', 'Swimming', 26, 'M', 193, 82),
            ('ATH0010', 'Emma McKeon', 'AUS', 'Swimming', 27, 'F', 180, 65)
        ]
        
        for data in athletes_data:
            cursor.execute("""
                INSERT INTO athlete_bio (athlete_id, name, country, sport, age, gender, height, weight, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (*data, datetime.now()))
        
        # Events data
        events_data = [
            ('EVT0001', 'ATH0001', 'Athletics 400m', '2024-09-15', 43.49, 'Gold', 'USA'),
            ('EVT0002', 'ATH0002', 'Swimming 800m Freestyle', '2024-09-16', 480.15, 'Gold', 'USA'),
            ('EVT0003', 'ATH0003', 'Athletics 100m', '2024-09-17', 9.58, 'Gold', 'JPN'),
            ('EVT0004', 'ATH0004', 'Gymnastics All-Around', '2024-09-18', 57.731, 'Gold', 'USA'),
            ('EVT0005', 'ATH0005', 'Swimming 100m Breaststroke', '2024-09-19', 57.13, 'Gold', 'GBR'),
            ('EVT0006', 'ATH0006', 'Athletics 200m', '2024-09-20', 21.53, 'Gold', 'JPN'),
            ('EVT0007', 'ATH0007', 'Swimming 50m Freestyle', '2024-09-21', 21.07, 'Gold', 'USA'),
            ('EVT0008', 'ATH0008', 'Athletics 400m Hurdles', '2024-09-22', 50.68, 'Gold', 'USA'),
            ('EVT0009', 'ATH0002', 'Swimming 1500m Freestyle', '2024-09-23', 928.46, 'Gold', 'USA'),
            ('EVT0010', 'ATH0010', 'Swimming 100m Freestyle', '2024-09-24', 51.96, 'Gold', 'AUS'),
            ('EVT0011', 'ATH0005', 'Swimming 50m Breaststroke', '2024-09-25', 26.11, 'Silver', 'GBR'),
            ('EVT0012', 'ATH0007', 'Swimming 100m Butterfly', '2024-09-26', 49.45, 'Gold', 'USA'),
            ('EVT0013', 'ATH0003', 'Athletics 200m', '2024-09-27', 19.19, 'Gold', 'JPN'),
            ('EVT0014', 'ATH0009', 'Swimming 100m Backstroke', '2024-09-28', 51.85, 'Gold', 'USA'),
            ('EVT0015', 'ATH0010', 'Swimming 50m Freestyle', '2024-09-29', 23.81, 'Bronze', 'AUS')
        ]
        
        for data in events_data:
            cursor.execute("""
                INSERT INTO athlete_events (event_id, athlete_id, event_name, event_date, result, medal, country, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (*data, datetime.now()))
        
        # Medal summary
        cursor.execute("""
            INSERT INTO medal_summary (country, medal_type, count, rank_position, created_at) VALUES
            ('USA', 'Gold', 8, 1, NOW()),
            ('JPN', 'Gold', 3, 2, NOW()),
            ('GBR', 'Gold', 1, 3, NOW()),
            ('AUS', 'Gold', 1, 4, NOW()),
            ('USA', 'Silver', 1, 1, NOW()),
            ('GBR', 'Silver', 1, 2, NOW()),
            ('AUS', 'Bronze', 1, 1, NOW())
        """)
        
        conn.commit()
        print(f"✅ Inserted {len(athletes_data)} athletes")
        print(f"✅ Inserted {len(events_data)} events")
        print("✅ Inserted medal summary data")
        
        return conn
        
    except Exception as e:
        print(f"❌ Database error: {e}")
        return None

def show_database_results(conn):
    """Display database query results"""
    if not conn:
        return
        
    cursor = conn.cursor()
    
    print("\n💾 MYSQL DATABASE QUERY RESULTS")
    print("=" * 60)
    
    # Query 1: Top athletes with medals
    print("\n🏆 TOP MEDAL WINNING ATHLETES:")
    print("-" * 50)
    cursor.execute("""
        SELECT ab.name, ab.country, ab.sport, COUNT(ae.medal) as medal_count
        FROM athlete_bio ab
        JOIN athlete_events ae ON ab.athlete_id = ae.athlete_id
        WHERE ae.medal IS NOT NULL
        GROUP BY ab.athlete_id, ab.name, ab.country, ab.sport
        ORDER BY medal_count DESC, ab.name
    """)
    
    results = cursor.fetchall()
    for i, (name, country, sport, count) in enumerate(results, 1):
        print(f"{i:2d}. {name:<18} ({country}) - {sport:<12} - {count} medals")
    
    # Query 2: Medal counts by country
    print("\n🌎 MEDAL COUNTS BY COUNTRY:")
    print("-" * 50)
    cursor.execute("""
        SELECT country, 
               SUM(CASE WHEN medal_type = 'Gold' THEN count ELSE 0 END) as Gold,
               SUM(CASE WHEN medal_type = 'Silver' THEN count ELSE 0 END) as Silver,
               SUM(CASE WHEN medal_type = 'Bronze' THEN count ELSE 0 END) as Bronze,
               SUM(count) as Total
        FROM medal_summary
        GROUP BY country
        ORDER BY Total DESC, Gold DESC
    """)
    
    results = cursor.fetchall()
    print("Country | Gold | Silver | Bronze | Total")
    print("-" * 40)
    for country, gold, silver, bronze, total in results:
        print(f"{country:<7} | {int(gold):4d} | {int(silver):6d} | {int(bronze):6d} | {int(total):5d}")
    
    # Query 3: Recent events
    print("\n📅 RECENT EVENT RESULTS:")
    print("-" * 50)
    cursor.execute("""
        SELECT ae.event_name, ab.name, ab.country, ae.result, ae.medal, ae.event_date
        FROM athlete_events ae
        JOIN athlete_bio ab ON ae.athlete_id = ab.athlete_id
        ORDER BY ae.event_date DESC
        LIMIT 10
    """)
    
    results = cursor.fetchall()
    for event, name, country, result, medal, date in results:
        medal_emoji = {'Gold': '🥇', 'Silver': '🥈', 'Bronze': '🥉'}.get(medal, '  ')
        print(f"{medal_emoji} {event:<25} | {name:<18} ({country}) | {result:8.2f}")
    
    # Query 4: Sport statistics
    print("\n🏃 SPORT PARTICIPATION STATISTICS:")
    print("-" * 50)
    cursor.execute("""
        SELECT ab.sport, 
               COUNT(DISTINCT ab.athlete_id) as athletes,
               COUNT(ae.event_id) as events,
               COUNT(ae.medal) as medals
        FROM athlete_bio ab
        LEFT JOIN athlete_events ae ON ab.athlete_id = ae.athlete_id
        GROUP BY ab.sport
        ORDER BY medals DESC, athletes DESC
    """)
    
    results = cursor.fetchall()
    print("Sport        | Athletes | Events | Medals")
    print("-" * 45)
    for sport, athletes, events, medals in results:
        print(f"{sport:<12} | {athletes:8d} | {events:6d} | {medals:6d}")

def main():
    """Main execution"""
    print("🎯 MYSQL DATABASE DEMONSTRATION")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # Create and populate database
    conn = create_and_populate_database()
    
    # Show results
    show_database_results(conn)
    
    if conn:
        conn.close()
        
    print("\n🎉 MYSQL DEMO COMPLETED!")
    print("📸 Database is ready for screenshots")
    print("=" * 60)

if __name__ == "__main__":
    main()