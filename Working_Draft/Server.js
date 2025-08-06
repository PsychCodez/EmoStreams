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

// Register a new subscriber (subscriber will register clients)
app.post('/registerSubscriber', (req, res) => {
  const { subscriberUrl } = req.body;
  if (subscriberUrl && !registeredSubscribers.includes(subscriberUrl)) {
    registeredSubscribers.push(subscriberUrl);
    console.log(`Subscriber registered: ${subscriberUrl}`);
    res.status(200).send('Subscriber registered successfully');
  } else {
    res.status(400).send('Invalid or duplicate subscriber URL');
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

      // After every 2 seconds, send the representative emojis to all subscribers
      setTimeout(() => sendRepresentativeEmoji(), 2000);
    },
  });
}

// Function to select and send representative emojis for each unique emoji type
async function sendRepresentativeEmoji() {
  if (latestEmojiData.length === 0) {
    return;
  }

  // Find the unique emoji types in the latest batch
  const uniqueEmojis = {};

  latestEmojiData.forEach((data) => {
    const emojiType = data.emoji_type;
    if (!uniqueEmojis[emojiType]) {
      uniqueEmojis[emojiType] = {
        emoji_type: emojiType,
        count: 1,  // Initialize count for this emoji type
        timestamp: new Date().toISOString(),  // Use the current timestamp for this emoji
      };
    } else {
      uniqueEmojis[emojiType].count += 1;  // Increment the count if the emoji type already exists
    }
  });

  // Now we have the unique emoji types in the `uniqueEmojis` object, each with a count
  const emojisToSend = Object.values(uniqueEmojis);
  console.log('Emojis to send:', emojisToSend);

  // Dynamically import fetch (since it's not available natively in CommonJS)
  const { default: fetch } = await import('node-fetch');

  // Send the `emojisToSend` array as a single payload to all registered subscribers
  registeredSubscribers.forEach(async (subscriberUrl) => {
    try {
      const response = await fetch(`${subscriberUrl}/data`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(emojisToSend),  // Send the array of emojis
      });

      console.log(`Sent emoji batch to ${subscriberUrl}: ${response.status}`);
    } catch (error) {
      console.error(`Error sending emoji batch to ${subscriberUrl}: ${error.message}`);
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

