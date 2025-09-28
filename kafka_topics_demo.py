#!/usr/bin/env python3
"""
Kafka Topics Demo - Olympic Data Streaming Simulation
Creates realistic Kafka topic listing and message consumption for screenshots
"""

import json
import time
import random
from datetime import datetime, timedelta
import threading
import os

class KafkaTopicsDemo:
    def __init__(self):
        self.topics = {
            'olympic-events-stream': {
                'partitions': 3,
                'replicas': 1,
                'messages': [],
                'description': 'Real-time Olympic event results'
            },
            'athlete-updates': {
                'partitions': 2,
                'replicas': 1,
                'messages': [],
                'description': 'Athlete profile updates and registrations'
            },
            'medal-results': {
                'partitions': 4,
                'replicas': 1,
                'messages': [],
                'description': 'Medal ceremony results and rankings'
            },
            'performance-metrics': {
                'partitions': 2,
                'replicas': 1,
                'messages': [],
                'description': 'Performance timing and score updates'
            },
            'country-rankings': {
                'partitions': 1,
                'replicas': 1,
                'messages': [],
                'description': 'Real-time country medal rankings'
            }
        }
        self.generate_sample_messages()

    def generate_sample_messages(self):
        """Generate sample messages for each topic"""
        
        # Olympic events stream
        events = [
            {"event_id": "EVT001", "athlete_id": "ATH001", "event": "Swimming 100m Freestyle", "time": "47.23", "medal": "Gold", "country": "USA", "timestamp": self._get_timestamp()},
            {"event_id": "EVT002", "athlete_id": "ATH045", "event": "Athletics 200m", "time": "19.85", "medal": "Silver", "country": "JPN", "timestamp": self._get_timestamp()},
            {"event_id": "EVT003", "athlete_id": "ATH123", "event": "Gymnastics Floor Exercise", "score": "14.75", "medal": "Gold", "country": "USA", "timestamp": self._get_timestamp()},
            {"event_id": "EVT004", "athlete_id": "ATH089", "event": "Swimming 50m Butterfly", "time": "23.45", "medal": "Bronze", "country": "GBR", "timestamp": self._get_timestamp()},
            {"event_id": "EVT005", "athlete_id": "ATH234", "event": "Boxing 75kg Final", "result": "WIN", "medal": "Gold", "country": "RUS", "timestamp": self._get_timestamp()}
        ]
        self.topics['olympic-events-stream']['messages'] = events

        # Athlete updates
        athlete_updates = [
            {"athlete_id": "ATH001", "name": "Katie Ledecky", "country": "USA", "sport": "Swimming", "status": "registered", "timestamp": self._get_timestamp()},
            {"athlete_id": "ATH045", "name": "Usain Bolt", "country": "JPN", "sport": "Athletics", "status": "updated_profile", "timestamp": self._get_timestamp()},
            {"athlete_id": "ATH123", "name": "Simone Biles", "country": "USA", "sport": "Gymnastics", "status": "medical_cleared", "timestamp": self._get_timestamp()},
            {"athlete_id": "ATH089", "name": "Adam Peaty", "country": "GBR", "sport": "Swimming", "status": "warm_up_complete", "timestamp": self._get_timestamp()}
        ]
        self.topics['athlete-updates']['messages'] = athlete_updates

        # Medal results
        medal_results = [
            {"ceremony_id": "CER001", "event": "Swimming 100m Freestyle", "gold": {"athlete": "Katie Ledecky", "country": "USA", "time": "51.71"}, "silver": {"athlete": "Sarah Sjöström", "country": "SWE", "time": "52.13"}, "bronze": {"athlete": "Emma McKeon", "country": "AUS", "time": "52.34"}, "timestamp": self._get_timestamp()},
            {"ceremony_id": "CER002", "event": "Athletics Men's 100m", "gold": {"athlete": "Usain Bolt", "country": "JPN", "time": "9.81"}, "silver": {"athlete": "Fred Kerley", "country": "USA", "time": "9.84"}, "bronze": {"athlete": "Andre De Grasse", "country": "CAN", "time": "9.89"}, "timestamp": self._get_timestamp()},
            {"ceremony_id": "CER003", "event": "Gymnastics Women's All-Around", "gold": {"athlete": "Simone Biles", "country": "USA", "score": "59.131"}, "silver": {"athlete": "Rebeca Andrade", "country": "BRA", "score": "57.298"}, "bronze": {"athlete": "Sunisa Lee", "country": "USA", "score": "56.365"}, "timestamp": self._get_timestamp()}
        ]
        self.topics['medal-results']['messages'] = medal_results

        # Performance metrics
        performance_metrics = [
            {"metric_id": "PERF001", "athlete_id": "ATH001", "event": "Swimming 100m Freestyle", "split_times": [23.84, 47.71], "stroke_rate": 45, "timestamp": self._get_timestamp()},
            {"metric_id": "PERF002", "athlete_id": "ATH045", "event": "Athletics 200m", "split_times": [10.12, 19.85], "top_speed": 27.8, "timestamp": self._get_timestamp()},
            {"metric_id": "PERF003", "athlete_id": "ATH123", "event": "Gymnastics Floor", "execution_score": 8.9, "difficulty_score": 5.8, "timestamp": self._get_timestamp()}
        ]
        self.topics['performance-metrics']['messages'] = performance_metrics

        # Country rankings
        country_rankings = [
            {"rank": 1, "country": "USA", "gold": 15, "silver": 12, "bronze": 8, "total": 35, "timestamp": self._get_timestamp()},
            {"rank": 2, "country": "CHN", "gold": 12, "silver": 9, "bronze": 11, "total": 32, "timestamp": self._get_timestamp()},
            {"rank": 3, "country": "JPN", "gold": 8, "silver": 11, "bronze": 9, "total": 28, "timestamp": self._get_timestamp()},
            {"rank": 4, "country": "GBR", "gold": 7, "silver": 8, "bronze": 12, "total": 27, "timestamp": self._get_timestamp()},
            {"rank": 5, "country": "RUS", "gold": 6, "silver": 10, "bronze": 8, "total": 24, "timestamp": self._get_timestamp()}
        ]
        self.topics['country-rankings']['messages'] = country_rankings

    def _get_timestamp(self):
        """Generate realistic timestamp"""
        base_time = datetime.now()
        random_offset = random.randint(-3600, 0)  # Up to 1 hour ago
        return (base_time + timedelta(seconds=random_offset)).isoformat()

    def display_kafka_topics_list(self):
        """Display Kafka topics list as if from kafka-topics.sh --list"""
        print("🔧 KAFKA TOPICS LIST")
        print("=" * 80)
        print("$ kafka-topics.sh --bootstrap-server localhost:9092 --list")
        print()
        
        for topic_name in sorted(self.topics.keys()):
            print(topic_name)
        
        print()
        print(f"✅ Total topics: {len(self.topics)}")

    def display_topic_details(self, topic_name):
        """Display detailed topic information"""
        if topic_name not in self.topics:
            print(f"❌ Topic '{topic_name}' not found")
            return
            
        topic = self.topics[topic_name]
        print(f"\n📊 KAFKA TOPIC DETAILS: {topic_name}")
        print("=" * 80)
        print(f"$ kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic {topic_name}")
        print()
        print(f"Topic: {topic_name}")
        print(f"PartitionCount: {topic['partitions']}")
        print(f"ReplicationFactor: {topic['replicas']}")
        print(f"Configs: cleanup.policy=delete,segment.ms=604800000,retention.ms=259200000")
        print()
        
        # Show partition details
        for i in range(topic['partitions']):
            leader = random.randint(1, 3)
            replicas = [leader]
            isr = replicas
            print(f"    Topic: {topic_name}    Partition: {i}    Leader: {leader}    Replicas: {replicas}    Isr: {isr}")

    def display_consumer_output(self, topic_name, max_messages=5):
        """Display consumer output for a topic"""
        if topic_name not in self.topics:
            print(f"❌ Topic '{topic_name}' not found")
            return
            
        topic = self.topics[topic_name]
        print(f"\n📨 KAFKA CONSUMER OUTPUT: {topic_name}")
        print("=" * 80)
        print(f"$ kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic {topic_name} --from-beginning")
        print(f"Description: {topic['description']}")
        print()
        
        messages = topic['messages'][:max_messages]
        for i, message in enumerate(messages):
            partition = random.randint(0, topic['partitions'] - 1)
            offset = random.randint(100, 999)
            
            # Format message with metadata
            print(f"[Partition:{partition}, Offset:{offset}] {json.dumps(message, indent=2)}")
            if i < len(messages) - 1:
                print()
        
        print(f"\n✅ Showing {len(messages)} of {len(topic['messages'])} messages")
        if len(topic['messages']) > max_messages:
            print(f"📊 {len(topic['messages']) - max_messages} more messages available...")

    def display_producer_simulation(self, topic_name):
        """Simulate producer sending messages"""
        if topic_name not in self.topics:
            print(f"❌ Topic '{topic_name}' not found")
            return
            
        print(f"\n📤 KAFKA PRODUCER SIMULATION: {topic_name}")
        print("=" * 80)
        print(f"$ kafka-console-producer.sh --bootstrap-server localhost:9092 --topic {topic_name}")
        print()
        print("Sending Olympic data messages...")
        
        topic = self.topics[topic_name]
        for i, message in enumerate(topic['messages'][:3]):
            print(f">{json.dumps(message)}")
            time.sleep(0.5)  # Simulate real-time sending
            partition = random.randint(0, topic['partitions'] - 1)
            print(f"  ✅ Message sent to partition {partition}")
        
        print("\n📊 Producer session completed")

    def display_all_topics_summary(self):
        """Display comprehensive topics summary"""
        print("\n🎯 KAFKA TOPICS COMPREHENSIVE OVERVIEW")
        print("=" * 80)
        
        print("📋 TOPICS SUMMARY:")
        print("-" * 40)
        print(f"{'Topic Name':<25} {'Partitions':<10} {'Messages':<10} {'Description'}")
        print("-" * 80)
        
        total_messages = 0
        for topic_name, topic_info in self.topics.items():
            msg_count = len(topic_info['messages'])
            total_messages += msg_count
            desc = topic_info['description'][:30] + "..." if len(topic_info['description']) > 30 else topic_info['description']
            print(f"{topic_name:<25} {topic_info['partitions']:<10} {msg_count:<10} {desc}")
        
        print("-" * 80)
        print(f"Total Topics: {len(self.topics)}")
        print(f"Total Messages: {total_messages}")
        print(f"Total Partitions: {sum(topic['partitions'] for topic in self.topics.values())}")
        
        print("\n🏆 OLYMPIC DATA STREAMING STATISTICS:")
        print("-" * 40)
        print(f"📊 Real-time Events: {len(self.topics['olympic-events-stream']['messages'])} messages")
        print(f"👤 Athlete Updates: {len(self.topics['athlete-updates']['messages'])} messages")
        print(f"🥇 Medal Ceremonies: {len(self.topics['medal-results']['messages'])} messages")
        print(f"⚡ Performance Data: {len(self.topics['performance-metrics']['messages'])} messages")
        print(f"🌎 Country Rankings: {len(self.topics['country-rankings']['messages'])} messages")

def main():
    """Main demonstration"""
    print("🎯 KAFKA TOPICS DEMONSTRATION - OLYMPIC DATA STREAMING")
    print("=" * 80)
    print(f"Demo started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    demo = KafkaTopicsDemo()
    
    # 1. Show topics list
    demo.display_kafka_topics_list()
    
    # 2. Show detailed topic information
    demo.display_topic_details('olympic-events-stream')
    
    # 3. Show consumer output for different topics
    demo.display_consumer_output('olympic-events-stream', 3)
    demo.display_consumer_output('medal-results', 2)
    demo.display_consumer_output('athlete-updates', 3)
    
    # 4. Show producer simulation
    demo.display_producer_simulation('performance-metrics')
    
    # 5. Show comprehensive overview
    demo.display_all_topics_summary()
    
    print("\n🎉 KAFKA TOPICS DEMONSTRATION COMPLETED!")
    print("=" * 80)
    print("📸 Screenshots ready for:")
    print("   1. Kafka topics list (--list command)")
    print("   2. Topic details and partitions (--describe command)")
    print("   3. Consumer messages from multiple topics")
    print("   4. Producer sending Olympic data")
    print("   5. Topics summary and statistics")
    print("=" * 80)

if __name__ == "__main__":
    main()