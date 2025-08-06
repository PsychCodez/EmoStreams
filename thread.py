#!/usr/bin/env python3
# multi_client_simulation.py

import threading
import random
import time
from datetime import datetime
from confluent_kafka import Producer

# List of emojis to simulate user interaction
EMOJI_LIST = ["😊", "😂", "👍", "💖", "🔥"]

# Kafka Producer configuration
producer = Producer({
    'bootstrap.servers': 'localhost:9092',
})

def send_emoji_data():
    while True:
        data = {
            "user_id": f"user_{random.randint(1, 100)}",
            "emoji_type": random.choice(EMOJI_LIST),
            "timestamp": datetime.now().isoformat()
        }
        producer.produce('emoji_topic', value=str(data))  # Send data to Kafka topic
        producer.flush()  # Ensure data is sent to Kafka
        print(f"Sent: {data}")
        time.sleep(0.005)  # Adjust to achieve ~200 messages per second

# Spawn multiple threads for simulating multiple clients
threads = []
for i in range(10):  # Adjust the number of threads as needed
    thread = threading.Thread(target=send_emoji_data)
    threads.append(thread)
    thread.start()

# Wait for threads to finish
for thread in threads:
    thread.join()
