const express = require('express');
const { Kafka } = require('kafkajs');

const app = express();
const port = 3000;

app.use(express.json()); // Middleware to parse JSON bodies

// Initialize Kafka Client
const kafka = new Kafka({
  clientId: 'emoji-server',
  brokers: ['localhost:9092'],
});

// Initialize Kafka Producer
const producer = kafka.producer();

// Send Emoji Data to Kafka
const sendToKafka = async (message) => {
  try {
    await producer.connect();
    await producer.send({
      topic: 'emoji_topic',
      messages: [{ value: JSON.stringify(message) }], // Send the full message as a JSON string
    });
    console.log('Sent message to Kafka:', message);
  } catch (error) {
    console.error('Error sending to Kafka:', error);
  }
};

// POST route to receive emoji data from clients
app.post('/send-emoji', async (req, res) => {
  const { user_id, emoji_type, timestamp } = req.body;

  // Validate that all required fields are present
  if (!user_id || !emoji_type || !timestamp) {
    return res.status(400).json({ error: 'User ID, emoji, and timestamp are required' });
  }

  // Create the message object to send to Kafka
  const message = {
    user_id: user_id,
    emoji_type: emoji_type,
    timestamp: timestamp,
  };

  // Forward emoji data to Kafka
  await sendToKafka(message);
  return res.status(200).json({ message: 'Emoji data sent to Kafka successfully!' });
});

// Start Express server
app.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`)
});
