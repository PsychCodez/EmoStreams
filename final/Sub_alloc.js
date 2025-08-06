const express = require('express');
const app = express();
const port = 8000;

// List of available subscriber ports
const subscriberPorts = [6000, 6001, 6002, 6003, 6004, 6005];

// Endpoint to allocate a random subscriber port
app.get('/getSubscriber', (req, res) => {
  const randomPort = subscriberPorts[Math.floor(Math.random() * subscriberPorts.length)];
  console.log(`Allocating subscriber port: ${randomPort}`);
  res.status(200).json({ subscriberPort: randomPort });
});

app.listen(port, () => {
  console.log(`Subscriber Allocator listening on port ${port}`);
});
