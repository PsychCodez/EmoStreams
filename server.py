from flask import Flask, request, jsonify
from kafka import KafkaProducer, KafkaConsumer
import json
import threading

app = Flask(__name__)
mydata = []
# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Initialize Kafka consumer for receiving processed emojis
consumer = KafkaConsumer(
    'processed_emoji_topic',  # Topic to receive processed data
    bootstrap_servers='localhost:9092',
    group_id='emoji_group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Function to send message to Kafka
def send_to_kafka(message):
    try:
        producer.send('emoji_topic', value=message)
        print(f"Sent message to Kafka: {message}")
    except Exception as e:
        print(f"Error sending to Kafka: {e}")

# Function to process and send back representative emojis
def process_and_send_representative_emojis():
    for message in consumer:
        # Extract processed emojis from the message
        processed_emojis = message.value.get("representative_emojis", [])
        print(f"Processed emojis received from Kafka: {processed_emojis}")

        # Send the processed emojis to the client (or another route in your app)
        # Here we will call the method to send this data back
        send_processed_emojis_to_client(processed_emojis)

# This method will trigger the actual sending of processed emojis
def send_processed_emojis_to_client(processed_emojis):
    # This should ideally be a separate HTTP request to the client or
    # an open connection to send the message back
    print(f"Sending representative emojis back to the client: {processed_emojis}")
    # Send HTTP response back to the client here if needed or use WebSocket

# Route to receive emoji data from the client and forward to Kafka
@app.route('/send-emoji', methods=['POST'])
def receive_emoji():
    data = request.json
    if not all(k in data for k in ("user_id", "emoji_type", "timestamp")):
        return jsonify({'error': 'User ID, emoji, and timestamp are required'}), 400

    send_to_kafka(data)  # Send to Kafka for processing
    return jsonify({'message': 'Emoji data sent to Kafka successfully!'}), 200

# Route to receive representative emojis from Kafka-Spark processing and send back to client
@app.route('/receive-representative-emojis', methods=['POST'])
def receive_representative_emojis():

    data = request.json
    actual = data.get('message', [])
    mydata.append(actual)
    # This is where you would connect to your Spark streaming output
    # Here we simulate receiving processed emojis
    processed_emojis = ["😀", "😊", "😂"]  # Mock response

    return jsonify({'representative_emojis': processed_emojis}), 200

@app.route('/send-representative-emojis', methods=['GET'])
def send_representative_emojis():
    # Simulate sending the processed emojis (you can replace this with actual data)
    processed_emojis = ['😀', '😂', '😎']  # This should be data from Kafka-Spark processing

    # Send representative emojis back to the client
    return jsonify({'representative_emojis': mydata}), 200

# Start the Flask server
if __name__ == '__main__':
    # Run the consumer in a separate thread to handle incoming messages
    consumer_thread = threading.Thread(target=process_and_send_representative_emojis)
    consumer_thread.start()

    # Run the Flask app
    app.run(port=3000)
