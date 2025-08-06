#!/usr/bin/env python3
# kafka_producer.py

from confluent_kafka import Producer
import json
import time

# Kafka Producer configuration
producer = Producer({
    'bootstrap.servers': 'localhost:9092',
})

def produce_emoji_data(data):
    """Function to send emoji data to Kafka topic."""
    producer.produce('emoji_topic', value=json.dumps(data).encode('utf-8'))
    producer.flush()  # Flush every time the data is produced (adjustable for performance)
    time.sleep(0.5)  # Ensure there's a delay to meet the 500ms flush requirement


