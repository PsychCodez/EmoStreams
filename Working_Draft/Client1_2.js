const express = require('express');
const { Kafka } = require('kafkajs');
const app = express();
const port = 7001;  // Each client has a unique port

// Kafka setup for sending emoji data to Kafka Producer
const kafka = new Kafka({
  clientId: 'client',
  brokers: ['localhost:9092'],  // Kafka broker address
});

const producer = kafka.producer();

app.use(express.json());

// URL of the subscriber that will send feedback
const subscriberUrl = 'http://localhost:6001';  // Modify if necessary

// Register the client with the subscriber
async function registerClient() {
  try {
    console.log(`Registering client with subscriber at ${subscriberUrl}/registerClient`);

    // Dynamically import node-fetch
    const { default: fetch } = await import('node-fetch');

    const response = await fetch(`${subscriberUrl}/registerClient`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ clientUrl: `http://localhost:${port}` }),
    });

    if (response.ok) {
      console.log('Client registered successfully');
    } else {
      console.log('Failed to register client, Status Code:', response.status);
    }
  } catch (error) {
    console.error('Error during client registration:', error);
  }
}

// Sample list of emojis to randomly pick from
const emojiList = ['😊', '😂', '😢', '😎', '😍', '😜', '🤔', '😡', '🤩', '😱'];

// Function to send random emoji data to Kafka Producer
async function sendRandomEmojiDataToKafka() {
  try {
    // Randomly select an emoji from the emojiList
    const randomEmoji = emojiList[Math.floor(Math.random() * emojiList.length)];

    // Create emoji data object with unique user ID and timestamp
    const emojiData = {
      user_id: `client_${port}`,  // Unique user ID based on client port
      emoji_type: randomEmoji,    // Randomly selected emoji
      timestamp: new Date().toISOString(),  // Current timestamp
    };

    // Initialize Kafka producer
    await producer.connect();

    // Send emoji data to Kafka topic
    await producer.send({
      topic: 'emoji_topic',  // The topic where emojis are sent
      messages: [
        {
          value: JSON.stringify(emojiData),  // Send the emoji data as a string
        },
      ],
    });

  } catch (error) {
    console.error('Error sending emoji data to Kafka:', error);
  }
}

// Call the function to send random emoji data every 5 seconds
setInterval(() => {
  sendRandomEmojiDataToKafka();
}, 10);  // Send every 5 seconds

// Call the registerClient function on startup
registerClient();

// Handle incoming feedback from the subscriber
app.post('/data', (req, res) => {
  const feedbackData = req.body;
  console.log('Received feedback from subscriber:', feedbackData);  // Check if data is being received
  res.status(200).send('Feedback received');
});

app.listen(port, () => {
  console.log(`Client listening on port ${port}`);
});

