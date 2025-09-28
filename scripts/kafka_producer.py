from kafka import KafkaProducer
import mysql.connector
import json
import time
import random
from datetime import datetime

def create_producer():
    """
    Create Kafka producer with proper configuration
    """
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'),
        key_serializer=lambda x: str(x).encode('utf-8') if x else None,
        retries=3,
        acks='all'
    )

def get_mysql_connection():
    """
    Create MySQL connection
    """
    return mysql.connector.connect(
        host='localhost',
        user='dataeng',
        password='password123',
        database='olympic_dataset',
        autocommit=True
    )

def send_mysql_data_to_kafka():
    """
    Continuously send MySQL data to Kafka topic
    """
    print("Starting Kafka producer...")
    
    # Connect to MySQL
    conn = get_mysql_connection()
    cursor = conn.cursor()
    
    # Create Kafka producer
    producer = create_producer()
    
    try:
        # Continuously send data
        while True:
            # Get random athlete event result
            cursor.execute("""
                SELECT athlete_id, sport, medal, timestamp
                FROM athlete_event_results
                WHERE medal IS NOT NULL AND medal != 'nan'
                ORDER BY RAND()
                LIMIT 1
            """)
            
            result = cursor.fetchone()
            if result:
                data = {
                    'athlete_id': result[0],
                    'sport': result[1],
                    'medal': result[2],
                    'timestamp': result[3].isoformat() if result[3] else datetime.now().isoformat()
                }
                
                # Send to Kafka
                producer.send(
                    'athlete_event_results', 
                    key=str(result[0]),
                    value=data
                )
                print(f"Sent: {data}")
            else:
                print("No data found, creating sample record...")
                # Create sample data if no records found
                sample_data = {
                    'athlete_id': random.randint(1, 1000),
                    'sport': random.choice(['Athletics', 'Swimming', 'Cycling']),
                    'medal': random.choice(['Gold', 'Silver', 'Bronze']),
                    'timestamp': datetime.now().isoformat()
                }
                producer.send('athlete_event_results', value=sample_data)
                print(f"Sent sample: {sample_data}")
            
            # Flush producer to ensure message is sent
            producer.flush()
            
            # Wait before sending next message
            time.sleep(3)  # Send data every 3 seconds
            
    except KeyboardInterrupt:
        print("\nStopping producer...")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if producer:
            producer.close()
        if conn:
            conn.close()
        print("Producer stopped.")

def test_kafka_connection():
    """
    Test Kafka connection and topic creation
    """
    try:
        producer = create_producer()
        
        # Send test message
        test_message = {
            'test': True,
            'timestamp': datetime.now().isoformat(),
            'message': 'Kafka connection test'
        }
        
        producer.send('athlete_event_results', value=test_message)
        producer.flush()
        
        print("Kafka connection test successful!")
        producer.close()
        return True
        
    except Exception as e:
        print(f"Kafka connection test failed: {e}")
        return False

if __name__ == "__main__":
    print("Testing Kafka connection...")
    if test_kafka_connection():
        print("Starting data producer...")
        send_mysql_data_to_kafka()
    else:
        print("Please check Kafka setup and try again.")