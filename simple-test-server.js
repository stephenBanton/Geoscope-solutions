const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');

const app = express();

// Middleware
app.use(cors());
app.use(bodyParser.json());

console.log('Middleware set up');

// Test routes
app.post('/test/route1', (req, res) => {
  console.log('Route 1 called');
  res.json({ success: true, message: 'Route 1 works' });
});

app.get('/test/route2', (req, res) => {
  console.log('Route 2 called');
  res.json({ success: true, message: 'Route 2 works' });
});

// Auth routes
app.post('/auth/login', (req, res) => {
  console.log('Auth login called');
  res.json({ success: true, token: 'test-token' });
});

console.log('Routes registered');

app.listen(5555, () => {
  console.log('Simple test server running on port 5555');
});
