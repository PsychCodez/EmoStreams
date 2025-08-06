// subscriber.js (Example Cluster Subscriber with dynamic import)
const express = require("express");
const app = express();
app.use(express.json());

let fetch;
let clients = [];

(async () => {
  // Dynamically import `node-fetch`
  fetch = (await import('node-fetch')).default;

  app.post("/subscribe", (req, res) => {
    clients.push(req.body.clientUrl);
    res.send("Subscribed successfully");
  });

  app.post("/data", (req, res) => {
    const data = req.body;
    clients.forEach(url => {
      fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(data)
      }).catch(error => console.error('Error sending data to client:', error));
    });
    res.send("Data distributed to clients");
  });

  app.listen(5002, () => console.log("Cluster Subscriber on port 5002"));
})();
    });
    res.send("Data distributed to clients");
});

app.listen(5002, () => console.log("Cluster Subscriber on port 5002"));

