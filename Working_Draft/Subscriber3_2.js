const express = require('express');
const app = express();
const port = 6005;

let registeredClients = [];  // List of clients registered with this subscriber

app.use(express.json());

// URL of the server that sends processed feedback
const serverUrl = 'http://localhost:5502';
async function registerSubscriber() {
  try {
    console.log(`Registering subscriber with server at ${serverUrl}/registerSubscriber`);

    const response = await fetch(`${serverUrl}/registerSubscriber`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ subscriberUrl: `http://localhost:${port}` }), // Subscriber URL to register
    });

    if (response.ok) {
      console.log('Subscriber registered successfully with the server');
    } else {
      console.log('Failed to register subscriber, Status Code:', response.status);
    }
  } catch (error) {
    console.error('Error during subscriber registration:', error);
  }
}

// Register the subscriber when it starts
registerSubscriber();// Modify if necessary

// Register the client with the subscriber
app.post('/registerClient', (req, res) => {
  const { clientUrl } = req.body;
  if (clientUrl && !registeredClients.includes(clientUrl)) {
    registeredClients.push(clientUrl);
    console.log(`Client registered: ${clientUrl}`);
    res.status(200).send('Client registered successfully');
  } else {
    res.status(400).send('Invalid or duplicate client URL');
  }
});

// Receive processed emoji data from the server and forward to clients
app.post('/data', (req, res) => {
  const feedbackData = req.body;
  console.log('Received feedback from server:', feedbackData);

  // Forward the data to each registered client
  registeredClients.forEach(async (clientUrl) => {
    try {
      const { default: fetch } = await import('node-fetch');
      const response = await fetch(`${clientUrl}/data`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(feedbackData),  // Forward the processed data to client
      });
      console.log(`Sent feedback to client at ${clientUrl}, Status: ${response.status}`);
    } catch (error) {
      console.error(`Error sending feedback to client at ${clientUrl}: ${error.message}`);
    }
  });

  res.status(200).send('Data forwarded to clients');
});

app.listen(port, () => {
  console.log(`Subscriber listening on port ${port}`);
});
