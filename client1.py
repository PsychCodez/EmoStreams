import requests
import random
import datetime
import time
from flask import Flask, request, jsonify
app = Flask(__name__)
# Function to generate a random emoji
def get_random_emoji():
    emojis = ['😀', '😁', '😂', '🤣', '😄', '😅', '😉', '😊', '😋', '😎', '😍', '🥰', '😘', '🤩', '🥳', '😛', '😜', '😝', '😎']
    return random.choice(emojis)

# Function to send 1000 emojis to the server
def send_emoji_data(user_id):
    for i in range(1, 1001):
        random_emoji = get_random_emoji()
        generated_time = datetime.datetime.now().isoformat()
        
        message = {
            'user_id': user_id,
            'emoji_type': random_emoji,
            'timestamp': generated_time
        }

        response = requests.post('http://localhost:3000/send-emoji', json=message)
        print(f"Client: Sent emoji #{i}, Status: {response.status_code}")

# Function to retrieve representative emojis from the server, with polling
def retrieve_representative_emojis():
    while True:
        # Requesting the server for representative emojis
        response = requests.get('http://localhost:3000/send-representative-emojis')  # Use the new endpoint
        if response.status_code == 200:
            emojis = response.json().get('representative_emojis', [])
            if emojis:
                print("Received representative emojis:", emojis)
                break  # Exit loop if emojis are received
            else:
                print("Received empty data, retrying...")
        else:
            print("Failed to retrieve representative emojis:", response.status_code)

        time.sleep(10)  # Wait a bit before polling again

# Run the client functions
user_id = 'user1'
send_emoji_data(user_id)
retrieve_representative_emojis()
