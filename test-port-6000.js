const http = require('http');

function testEndpoint(port, path, method, data) {
  return new Promise((resolve) => {
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
      resolve(null);
    });

    if (data) {
      req.write(JSON.stringify(data));
    }
    req.end();
  });
}

async function runTests() {
  console.log('Testing Server on port 6000...\n');
  
  await testEndpoint(6000, '/auth/login', 'POST', { email: 'admin@geoscope.com', password: 'admin123' });
  await testEndpoint(6000, '/auth/register', 'POST', { name: 'Test', email: 'test@test.com', password: 'test123', role: 'client' });
  
  console.log('\n✓ Tests completed');
  process.exit(0);
}

runTests();
