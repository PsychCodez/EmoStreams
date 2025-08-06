const express = require('express');
const app = express();
const port = 6000;

app.use(express.json());

// URL of the server that sends feedback
const serverUrl = 'http://localhost:5001'; // Modify if necessary

// Register the client with the server
async function registerClient() {
  try {
    console.log(`Registering client with server at ${serverUrl}/register`); // Debugging log

    // Dynamically import node-fetch
    const { default: fetch } = await import('node-fetch');

    const response = await fetch(`${serverUrl}/register`, {
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

// Call the registerClient function on startup
registerClient();

// Handle incoming feedback from the server
app.post('/data', (req, res) => {
  const feedbackData = req.body;
  console.log('Received feedback from server:', feedbackData);  // Check if data is being received
  res.status(200).send('Feedback received');
});

app.listen(port, () => {
  console.log(`Client listening on port ${port}`);
});
