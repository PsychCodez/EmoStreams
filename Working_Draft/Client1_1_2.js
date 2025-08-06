const express = require('express');
const { Kafka } = require('kafkajs');
const app = express();
const port = 7001; // Each client has a unique port

// Kafka setup for sending emoji data to Kafka Producer
const kafka = new Kafka({
  clientId: 'client',
  brokers: ['localhost:9092'], // Kafka broker address
});

const producer = kafka.producer();
app.use(express.json());

// URL of the subscriber allocator
const allocatorUrl = 'http://localhost:8000'; // Adjust if necessary

// Fetch a subscriber's port from the allocator
async function getSubscriberPort() {
  try {
    console.log(Requesting subscriber port from allocator at ${allocatorUrl}/getSubscriber);

    const { default: fetch } = await import('node-fetch');

    const response = await fetch(${allocatorUrl}/getSubscriber, {
      method: 'GET',
    });

    if (response.ok) {
      const { subscriberPort } = await response.json();
      console.log(Received subscriber port: ${subscriberPort});
      return subscriberPort;
    } else {
      console.error('Failed to fetch subscriber port, Status Code:', response.status);
      process.exit(1); // Exit if allocator fails
    }
  } catch (error) {
    console.error('Error fetching subscriber port:', error);
    process.exit(1); // Exit if allocator fails
  }
}

// Register the client with the allocated subscriber
async function registerClient(subscriberPort) {
  try {
    const subscriberUrl = http://localhost:${subscriberPort};
    console.log(Registering client with subscriber at ${subscriberUrl}/registerClient);

    const { default: fetch } = await import('node-fetch');

    const response = await fetch(${subscriberUrl}/registerClient, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ clientUrl: http://localhost:${port} }),
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
    const randomEmoji = emojiList[Math.floor(Math.random() * emojiList.length)];
    const emojiData = {
      user_id: client_${port}, // Unique user ID based on client port
      emoji_type: randomEmoji,
      timestamp: new Date().toISOString(),
    };

    await producer.connect();
    await producer.send({
      topic: 'emoji_topic',
      messages: [
        {
          value: JSON.stringify(emojiData),
        },
      ],
    });
  } catch (error) {
    console.error('Error sending emoji data to Kafka:', error);
  }
}

// Handle incoming feedback from the subscriber
app.post('/data', (req, res) => {
  const feedbackData = req.body;
  console.log('Received feedback from subscriber:', feedbackData);
  res.status(200).send('Feedback received');
});

// Main execution flow
(async () => {
  const subscriberPort = await getSubscriberPort(); // Step 1: Get subscriber port
  await registerClient(subscriberPort); // Step 2: Register client with the allocated subscriber
  setInterval(() => sendRandomEmojiDataToKafka(), 10); // Step 3: Start sending emoji data to Kafka
})();

app.listen(port, () => {
  console.log(Client listening on port ${port});
});

