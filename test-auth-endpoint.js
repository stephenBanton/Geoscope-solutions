const http = require('http');

function testEndpoint(path, method, data) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: 'localhost',
      port: 5000,
      path: path,
      method: method,
      headers: {
        'Content-Type': 'application/json'
      }
    };

    const req = http.request(options, (res) => {
      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => {
        console.log(`\n[${method} ${path}]`);
        console.log(`Status: ${res.statusCode}`);
        console.log(`Response: ${body}`);
        resolve({ status: res.statusCode, body });
      });
    });

    req.on('error', (err) => {
      console.log(`Error: ${err.message}`);
      reject(err);
    });

    if (data) {
      req.write(JSON.stringify(data));
    }
    req.end();
  });
}

async function runTests() {
  console.log('Testing Auth Endpoints...\n');
  
  try {
    // Test login
    await testEndpoint('/auth/login', 'POST', {
      email: 'admin@geoscope.com',
      password: 'admin123'
    });

    // Test register
    await testEndpoint('/auth/register', 'POST', {
      name: 'Test User',
      email: 'test@example.com',
      password: 'testpass123',
      role: 'client'
    });

    console.log('\n✓ Tests completed');
  } catch (err) {
    console.error('Test failed:', err);
  }
  
  process.exit(0);
}

runTests();
