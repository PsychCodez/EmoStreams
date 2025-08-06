const express = require('express');
const { Kafka } = require('kafkajs');
const app = express();
const port = 5001;

const kafka = new Kafka({
  clientId: 'main-server',
  brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'emoji_feedback_group' });

let registeredClients = [];
let latestEmojiData = [];  // Store the latest emoji feedback

app.use(express.json());

// Register a new client (we'll log all requests for debugging)
app.post('/register', (req, res) => {
  try {
    const clientUrl = req.body.clientUrl;
    console.log(`Received registration request for: ${clientUrl}`); // Debugging log

    if (clientUrl && !registeredClients.includes(clientUrl)) {
      registeredClients.push(clientUrl);
      console.log(`Client registered: ${clientUrl}`);
      res.status(200).send('Client registered successfully');
    } else {
      console.log('Invalid or duplicate client URL');
      res.status(400).send('Invalid or duplicate client URL');
    }
  } catch (error) {
    console.error('Error in /register route:', error);
    res.status(500).send('Server error');
  }
});

// Kafka Consumer logic
async function startConsumer() {
  await consumer.connect();
  console.log("Consumer connected to Kafka");

  await consumer.subscribe({ topic: 'emoji_feedback_topic', fromBeginning: true });
  console.log("Subscribed to emoji_feedback_topic");

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const feedbackData = JSON.parse(message.value.toString());
      console.log(`Received feedback from Kafka: ${JSON.stringify(feedbackData)}`);

      // Store the latest emoji data
      latestEmojiData.push(feedbackData);

      // After every 2 seconds, send the representative emoji to clients
      setTimeout(() => sendRepresentativeEmoji(), 2000);
    },
  });
}

// Function to select and send the representative emoji
async function sendRepresentativeEmoji() {
  if (latestEmojiData.length === 0) {
    return;
  }

  // Find the emoji with the highest count (representative emoji)
  const emojiCounts = {};

  latestEmojiData.forEach((data) => {
    const emojiType = data.emoji_type;
    if (emojiCounts[emojiType]) {
      emojiCounts[emojiType]++;
    } else {
      emojiCounts[emojiType] = 1;
    }
  });

  // Find the emoji with the maximum count
  let representativeEmoji = null;
  let maxCount = 0;
  for (const emojiType in emojiCounts) {
    if (emojiCounts[emojiType] > maxCount) {
      maxCount = emojiCounts[emojiType];
      representativeEmoji = emojiType;
    }
  }

  const representativeEmojiData = {
    emoji_type: representativeEmoji,
    count: maxCount,
    timestamp: new Date().toISOString(),
  };

  console.log(`Representative Emoji: ${representativeEmoji} with count: ${maxCount}`);

  // Dynamically import fetch (since it's not available natively in CommonJS)
  const { default: fetch } = await import('node-fetch');

  // Send the representative emoji data to all registered clients
  registeredClients.forEach(async (clientUrl) => {
    try {
      const response = await fetch(`${clientUrl}/data`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(representativeEmojiData),
      });

      console.log(`Sent representative emoji to ${clientUrl}: ${response.status}`);
    } catch (error) {
      console.error(`Error sending representative emoji to ${clientUrl}: ${error.message}`);
    }
  });

  // Clear the stored data after sending feedback
  latestEmojiData = [];
}

// Start the consumer
startConsumer().catch(console.error);

// Start the Express server
app.listen(port, () => {
  console.log(`Server listening on port ${port}`);
});

