import mysql.connector
import random
from datetime import datetime, timedelta

def populate_sample_data():
    """
    Populate sample Olympic dataset in MySQL database
    """
    try:
        # Database connection
        conn = mysql.connector.connect(
            host='localhost',
            user='dataeng',
            password='password123',
            database='olympic_dataset'
        )
        cursor = conn.cursor()
        
        # Sample data
        sports = ['Athletics', 'Swimming', 'Cycling', 'Hockey', 'Boxing', 'Wrestling', 'Shooting']
        countries = ['USA', 'GER', 'CHN', 'RUS', 'GBR', 'FRA', 'ITA', 'AUS', 'CAN', 'JPN']
        sexes = ['Male', 'Female']
        medals = ['Gold', 'Silver', 'Bronze', 'nan']
        
        print("Inserting athlete bio data...")
        # Insert athlete bio data
        for i in range(1, 1001):
            sex = random.choice(sexes)
            height = random.uniform(150, 200) if sex == 'Male' else random.uniform(140, 190)
            weight = random.uniform(50, 100) if sex == 'Female' else random.uniform(60, 120)
            
            cursor.execute("""
                INSERT INTO athlete_bio (athlete_id, name, sex, country_noc, height, weight)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                sex = VALUES(sex),
                country_noc = VALUES(country_noc),
                height = VALUES(height),
                weight = VALUES(weight)
            """, (i, f"Athlete_{i}", sex, random.choice(countries), height, weight))
        
        print("Inserting athlete event results...")
        # Insert event results
        for i in range(5000):
            athlete_id = random.randint(1, 1000)
            sport = random.choice(sports)
            medal = random.choice(medals)
            timestamp = datetime.now() - timedelta(days=random.randint(0, 365))
            
            cursor.execute("""
                INSERT INTO athlete_event_results (athlete_id, sport, medal, timestamp)
                VALUES (%s, %s, %s, %s)
            """, (athlete_id, sport, medal, timestamp))
        
        conn.commit()
        conn.close()
        print("Sample data populated successfully!")
        
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        if conn:
            conn.close()
    except Exception as e:
        print(f"Unexpected error: {e}")
        if conn:
            conn.close()

def create_database_and_tables():
    """
    Create database and tables if they don't exist
    """
    try:
        # Connect to MySQL server (without database)
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='your_root_password'  # Update this
        )
        cursor = conn.cursor()
        
        # Create database
        cursor.execute("CREATE DATABASE IF NOT EXISTS olympic_dataset")
        cursor.execute("USE olympic_dataset")
        
        # Create user if not exists
        cursor.execute("""
            CREATE USER IF NOT EXISTS 'dataeng'@'localhost' 
            IDENTIFIED BY 'password123'
        """)
        cursor.execute("""
            GRANT ALL PRIVILEGES ON olympic_dataset.* 
            TO 'dataeng'@'localhost'
        """)
        cursor.execute("FLUSH PRIVILEGES")
        
        # Create tables
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS athlete_bio (
                athlete_id INT PRIMARY KEY,
                name VARCHAR(255),
                sex VARCHAR(10),
                country_noc VARCHAR(10),
                height FLOAT,
                weight FLOAT
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS athlete_event_results (
                result_id INT AUTO_INCREMENT PRIMARY KEY,
                athlete_id INT,
                sport VARCHAR(100),
                medal VARCHAR(20),
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_athlete_id (athlete_id)
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS athlete_enriched_agg (
                agg_id INT AUTO_INCREMENT PRIMARY KEY,
                sport VARCHAR(100),
                medal VARCHAR(20),
                sex VARCHAR(10),
                country_noc VARCHAR(10),
                avg_height FLOAT,
                avg_weight FLOAT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY unique_agg (sport, medal, sex, country_noc)
            )
        """)
        
        conn.commit()
        conn.close()
        print("Database and tables created successfully!")
        
    except mysql.connector.Error as err:
        print(f"Database setup error: {err}")
        if conn:
            conn.close()

if __name__ == "__main__":
    print("Setting up database and tables...")
    create_database_and_tables()
    
    print("Populating sample data...")
    populate_sample_data()
    
    print("Setup complete!")