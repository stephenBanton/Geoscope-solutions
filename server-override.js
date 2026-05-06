const PORT = 6000;
require('dotenv').config();
process.env.PORT = PORT;
const app = require('./server.js');
console.log(`Override server running on port ${PORT}`);
