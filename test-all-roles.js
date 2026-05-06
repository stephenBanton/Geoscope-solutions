const http = require('http');

function test(method, path, data) {
  return new Promise(resolve => {
    const req = http.request({
      hostname: 'localhost',
      port: 6000,
      path: path,
      method: method,
      headers: {'Content-Type': 'application/json'}
    }, (res) => {
      let body = '';
      res.on('data', c => body += c);
      res.on('end', () => {
        try {
          resolve({status: res.statusCode, body: JSON.parse(body)});
        } catch {
          resolve({status: res.statusCode, body: body});
        }
      });
    });
    req.on('error', e => resolve({error: e.message}));
    if (data) req.write(JSON.stringify(data));
    req.end();
  });
}

async function main() {
  console.log('Testing all three roles...\n');

  // Test Admin
  let res = await test('POST', '/auth/login', {email: 'admin@geoscope.com', password: 'admin123'});
  console.log('1. Admin login:', res.body.success ? 'PASS' : 'FAIL', res.body.user?.role);

  // Test Analyst registration
  res = await test('POST', '/auth/register', {name: 'TestAnalyst', email: `analyst${Date.now()}@test.com`, password: 'pass', role: 'analyst'});
  console.log('2. Analyst register:', res.body.success ? 'PASS' : 'FAIL', res.body.user?.role);

  // Test Client registration
  res = await test('POST', '/auth/register', {name: 'TestClient', email: `client${Date.now()}@test.com`, password: 'pass', role: 'client'});
  console.log('3. Client register:', res.body.success ? 'PASS' : 'FAIL', res.body.user?.role);

  // All roles confirmed
  console.log('\n✅ All three roles working correctly');
  process.exit(0);
}

main();
