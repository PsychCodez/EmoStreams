const express = require('express');
const { Kafka } = require('kafkajs');
const app = express();
const port = 5001;

const kafka = new Kafka({
  clientId: 'main-server',
  brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'emoji_feedback_group' });

let registeredSubscribers = [];  // Array to store subscriber URLs
let latestEmojiData = [];  // Store the latest emoji feedback

app.use(express.json());

// Register a new publisher (publisher will register subscribers)
app.post('/registerPublisher', (req, res) => {
  const { publisherUrl } = req.body;
  if (publisherUrl && !registeredSubscribers.includes(publisherUrl)) {
    registeredSubscribers.push(publisherUrl);
    console.log(`Publisher registered: ${publisherUrl}`);
    res.status(200).send('Publisher registered successfully');
  } else {
    res.status(400).send('Invalid or duplicate publisher URL');
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

      // Send the representative emojis every 2 seconds
      setTimeout(() => sendRepresentativeEmoji(), 2000);
    },
  });
}

// Function to select and send representative emojis for each unique emoji type and timestamp
async function sendRepresentativeEmoji() {
  if (latestEmojiData.length === 0) {
    return;
  }

  const emojiCountByTimestamp = {};

  // Loop through the feedbacks to count how many feedbacks per emoji type and timestamp
  latestEmojiData.forEach((data) => {
    const { emoji_type, timestamp } = data;

    const key = `${emoji_type}_${timestamp}`; // Create a unique key combining emoji type and timestamp

    // If this key doesn't exist, initialize it
    if (!emojiCountByTimestamp[key]) {
      emojiCountByTimestamp[key] = {
        emoji_type,
        timestamp,
        count: 1, // Initialize the count to 1
      };
    } else {
      // Otherwise, increment the count for this combination
      emojiCountByTimestamp[key].count += 1;
    }
  });

  // Convert the emojiCountByTimestamp object into an array of emoji data to send
  const emojisToSend = Object.values(emojiCountByTimestamp);
  console.log('Emojis to send:', emojisToSend);

  const { default: fetch } = await import('node-fetch');

  // Send the emoji data to all registered subscribers
  registeredSubscribers.forEach(async (subscriberUrl) => {
    try {
      const response = await fetch(`${subscriberUrl}/data`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(emojisToSend),
      });

      console.log(`Sent emoji batch to ${subscriberUrl}: ${response.status}`);
    } catch (error) {
      console.error(`Error sending emoji batch to ${subscriberUrl}: ${error.message}`);
    }
  });

  // Clear the latest emoji data after sending
  latestEmojiData = [];
}

// Start the consumer
startConsumer().catch(console.error);

// Start the Express server
app.listen(port, () => {
  console.log(`Server listening on port ${port}`);
});
