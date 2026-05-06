const http = require('http');

function testEndpoint(port, path, method, data) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: 'localhost',
      port: port,
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
        console.log(`\n[${method} http://localhost:${port}${path}]`);
        console.log(`Status: ${res.statusCode}`);
        try {
          const parsed = JSON.parse(body);
          console.log(`Response: ${JSON.stringify(parsed, null, 2)}`);
        } catch {
          console.log(`Response (not JSON): ${body.substring(0, 200)}`);
        }
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
  console.log('Testing Endpoints...\n');
  
  try {
    // Test simple server
    await testEndpoint(5555, '/test/route1', 'POST', { test: 'data' });
    await testEndpoint(5555, '/auth/login', 'POST', { email: 'test@test.com', password: 'test' });

    // Test main server
    console.log('\n\n--- Testing main server on port 5000 ---');
    await testEndpoint(5000, '/auth/login', 'POST', { email: 'admin@geoscope.com', password: 'admin123' });

    console.log('\n✓ Tests completed');
  } catch (err) {
    console.error('Test failed:', err);
  }
  
  process.exit(0);
}

runTests();
