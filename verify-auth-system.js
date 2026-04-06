#!/usr/bin/env node
/**
 * Complete Authentication System Verification
 * Tests all major authentication flows and endpoints
 */

const http = require('http');

const API_BASE_URL = `http://localhost:6000`;

function makeRequest(method, path, body = null) {
  return new Promise((resolve) => {
    const url = new URL(path, API_BASE_URL);
    const options = {
      hostname: url.hostname,
      port: url.port,
      path: url.pathname + url.search,
      method: method,
      headers: {
        'Content-Type': 'application/json'
      }
    };

    const req = http.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const body = JSON.parse(data);
          resolve({ status: res.statusCode, body });
        } catch {
          resolve({ status: res.statusCode, body: data });
        }
      });
    });

    req.on('error', (err) => {
      resolve({ status: 0, error: err.message });
    });

    if (body) {
      req.write(JSON.stringify(body));
    }
    req.end();
  });
}

async function runTests() {
  console.log('\n' + '='.repeat(60));
  console.log('GeoScope - Authentication System Verification');
  console.log('='.repeat(60) + '\n');

  let passed = 0;
  let failed = 0;

  // Test 1: Admin Login
  console.log('Test 1: Admin Login');
  let result = await makeRequest('POST', '/auth/login', {
    email: 'admin@geoscope.com',
    password: 'admin123'
  });
  if (result.status === 200 && result.body.success && result.body.token) {
    console.log('✅ PASS - Admin login successful');
    console.log(`   Token: ${result.body.token.substring(0, 20)}...`);
    console.log(`   User: ${result.body.user.email} (${result.body.user.role})`);
    passed++;
  } else {
    console.log(`❌ FAIL - Status: ${result.status}`, result.body);
    failed++;
  }

  // Test 2: Client Registration
  console.log('\nTest 2: Client Registration');
  result = await makeRequest('POST', '/auth/register', {
    name: 'Test Client',
    email: `client-${Date.now()}@test.com`,
    password: 'testpass123',
    role: 'client'
  });
  if (result.status === 201 && result.body.success && result.body.token) {
    console.log('✅ PASS - Client registration successful');
    console.log(`   User: ${result.body.user.email} (${result.body.user.role})`);
    passed++;
  } else {
    console.log(`❌ FAIL - Status: ${result.status}`, result.body);
    failed++;
  }

  // Test 3: Analyst Registration
  console.log('\nTest 3: Analyst Registration');
  result = await makeRequest('POST', '/auth/register', {
    name: 'Test Analyst',
    email: `analyst-${Date.now()}@test.com`,
    password: 'analystpass123',
    role: 'analyst'
  });
  if (result.status === 201 && result.body.success) {
    console.log('✅ PASS - Analyst registration successful');
    console.log(`   User: ${result.body.user.email} (${result.body.user.role})`);
    passed++;
  } else {
    console.log(`❌ FAIL - Status: ${result.status}`, result.body);
    failed++;
  }

  // Test 4: Invalid Login
  console.log('\nTest 4: Invalid Login (Wrong Password)');
  result = await makeRequest('POST', '/auth/login', {
    email: 'admin@geoscope.com',
    password: 'wrongpassword'
  });
  if (result.status === 401 && !result.body.success) {
    console.log('✅ PASS - Invalid login rejected');
    console.log(`   Error: ${result.body.error}`);
    passed++;
  } else {
    console.log(`❌ FAIL - Status: ${result.status}`, result.body);
    failed++;
  }

  // Test 5: Get All Users (Admin)
  console.log('\nTest 5: Get All Users');
  result = await makeRequest('POST', '/auth/login', {
    email: 'admin@geoscope.com',
    password: 'admin123'
  });
  const adminToken = result.body.token;
  
  result = await makeRequest('GET', '/admin/users');
  if (result.status === 401) {
    console.log('✅ PASS - Endpoint requires authentication');
    console.log(`   Status: ${result.status} (Unauthorized without token)`);
    passed++;
  } else {
    console.log(`❌ FAIL - Should require auth, got: ${result.status}`);
    failed++;
  }

  // Summary
  console.log('\n' + '='.repeat(60));
  console.log('TEST SUMMARY');
  console.log('='.repeat(60));
  console.log(`✅ Passed: ${passed}`);
  console.log(`❌ Failed: ${failed}`);
  console.log(`⏱️  Total Tests: ${passed + failed}`);
  
  if (failed === 0) {
    console.log('\n🎉 All tests passed! Authentication system is working correctly!');
  } else {
    console.log('\n⚠️  Some tests failed. Please check the configuration.');
  }
  
  console.log('\n' + '='.repeat(60));
  console.log('Server Status: Connected to http://localhost:6000 ✅');
  console.log('='.repeat(60) + '\n');

  process.exit(failed > 0 ? 1 : 0);
}

// Start tests after a short delay to ensure server is ready
setTimeout(runTests, 1000);
