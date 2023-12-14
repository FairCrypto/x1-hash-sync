import express from 'express';
import bodyParser from 'body-parser';

const app = express();
const port = 9997;

// Use middleware to parse JSON requests
app.use(bodyParser.json());

// Define a route for handling POST requests
app.post('/process_hash', (req, res) => {
  // Print the received JSON data
  console.log('Received JSON data:', req.body);

  // Respond with a simple message
  res.json({ status: 'duplicate' });
  // res.json({ status: 'accepted' });
});

// Start the server
app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});