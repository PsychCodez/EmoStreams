const express = require('express');
const app = express();
const port = 5500;

let registeredSubscribers = []; // Array to store registered subscriber URLs

app.use(express.json());

// Register the publisher with the main server at port 5001
async function registerWithMainServer() {
  const serverUrl = 'http://localhost:5001/registerPublisher';
  try {
    console.log(`Registering publisher with main server at ${serverUrl}`);
    
    const { default: fetch } = await import('node-fetch');
    const response = await fetch(serverUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ publisherUrl: `http://localhost:${port}` }), // Sending publisherUrl
    });

    if (response.ok) {
      console.log('Publisher registered successfully with main server');
    } else {
      console.log('Failed to register publisher, Status Code:', response.status);
    }
  } catch (error) {
    console.error('Error during publisher registration:', error.message);
  }
}

// Endpoint for subscribers to register with the publisher
app.post('/registerSubscriber', (req, res) => {
  const { subscriberUrl } = req.body; // Expecting subscriberUrl in the payload
  if (subscriberUrl && !registeredSubscribers.includes(subscriberUrl)) {
    registeredSubscribers.push(subscriberUrl);
    console.log(`Subscriber registered: ${subscriberUrl}`);
    res.status(200).send('Subscriber registered successfully');
  } else {
    res.status(400).send('Invalid or duplicate subscriber URL');
  }
});

// Endpoint for receiving data from the main server and forwarding it to subscribers
app.post('/data', async (req, res) => {
  const feedbackData = req.body;
  console.log('Received data from main server:', feedbackData);

  const { default: fetch } = await import('node-fetch');

  // Forward the received data to all registered subscribers
  registeredSubscribers.forEach(async (subscriberUrl) => {
    try {
      const response = await fetch(`${subscriberUrl}/data`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(feedbackData),
      });
      console.log(`Sent data to subscriber at ${subscriberUrl}, Status: ${response.status}`);
    } catch (error) {
      console.error(`Error sending data to subscriber at ${subscriberUrl}: ${error.message}`);
    }
  });

  res.status(200).send('Data forwarded to subscribers');
});

// Register with the main server on startup
registerWithMainServer();

// Start the Express server
app.listen(port, () => {
  console.log(`Publisher listening on port ${port}`);
});

