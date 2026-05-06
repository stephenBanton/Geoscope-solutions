// Quick end-to-end flow test: generate report → (optionally) send to client
// Usage: node test-sample-order.js

const http = require('http');

function post(path, payload) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify(payload);
    const options = {
      hostname: 'localhost',
      port: 5000,
      path,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body)
      }
    };
    const req = http.request(options, (res) => {
      let data = '';
      res.on('data', (chunk) => { data += chunk; });
      res.on('end', () => {
        try { resolve({ status: res.statusCode, body: JSON.parse(data) }); }
        catch { resolve({ status: res.statusCode, body: data }); }
      });
    });
    req.on('error', reject);
    req.setTimeout(90000, () => { req.destroy(new Error('Request timeout')); });
    req.write(body);
    req.end();
  });
}

(async () => {
  console.log('\n=== GeoScope Sample Order Flow Test ===');
  console.log('Recipient: nyangelos4@gmail.com\n');

  // ── Step 1: Generate report ─────────────────────────────────────
  console.log('[1] Generating report...');
  const genPayload = {
    project_name: 'Sample Environmental Screening Report',
    client_name: 'Nick Yangelos',
    client_company: 'Yangelos Properties LLC',
    address: '100 Biscayne Blvd, Miami, FL 33132',
    latitude: 25.7617,
    longitude: -80.1918,
    paid: true,
    summary: 'Sample environmental screening deliverable generated for flow verification.',
    environmentalData: {
      environmentalSites: [
        { id: 'S1', name: 'Fuel Terminal', database: 'EPA FUELS', database_name: 'EPA FUELS', address: '111 Harbor Rd', distance: 480, status: 'Active' },
        { id: 'S2', name: 'Historic Dry Cleaner', database: 'SCRD DRYCLEANERS', database_name: 'SCRD DRYCLEANERS', address: '222 Market St', distance: 960, status: 'Closed' },
        { id: 'S3', name: 'Downtown Public School', database: 'SCHOOLS PUBLIC', database_name: 'SCHOOLS PUBLIC', address: '333 School Ave', distance: 1450, status: 'Active' }
      ],
      floodZones: [{ attributes: { FLD_ZONE: 'AE', SFHA_TF: 'T' } }],
      schools: [{ attributes: { NAME: 'Downtown Public School', CITY: 'Miami' } }],
      wetlands: [],
      rainfall: [{ date: '2024-01-01', precipitation: '18 mm' }],
      governmentRecords: [{ FacilityName: 'Municipal Storage Site', Status: 'Active' }]
    },
    radius: 1500
  };

  const genResult = await post('/generate-report', genPayload);
  console.log(`   HTTP ${genResult.status}`);

  if (genResult.status !== 200 || !genResult.body.success) {
    console.error('   ✗ Report generation FAILED');
    console.error('   Response:', JSON.stringify(genResult.body, null, 2));
    process.exit(1);
  }

  const { reportPath, fileName, orderId, downloadUrl } = genResult.body;
  console.log(`   ✓ Report generated`);
  console.log(`   File     : ${reportPath}`);
  console.log(`   Order ID : ${orderId}`);
  console.log(`   Download : http://localhost:5000${downloadUrl}`);

  // ── Step 2: Verify download endpoint ────────────────────────────
  console.log('\n[2] Testing download endpoint...');
  const dlTest = await new Promise((resolve) => {
    http.get(`http://localhost:5000${downloadUrl}`, (res) => {
      res.resume();
      resolve({ status: res.statusCode, contentType: res.headers['content-type'] });
    }).on('error', (e) => resolve({ status: 0, error: e.message }));
  });

  if (dlTest.status === 200) {
    console.log(`   ✓ Download endpoint OK (${dlTest.contentType})`);
  } else {
    console.warn(`   ⚠ Download endpoint returned HTTP ${dlTest.status}`);
  }

  // ── Step 3: Test /order record submission ──────────────────────
  console.log('\n[3] Submitting order record...');
  const orderPayload = {
    project_name: 'Sample Environmental Screening Report',
    client_name: 'Nick Yangelos',
    email: 'nyangelos4@gmail.com',
    address: '100 Biscayne Blvd, Miami, FL 33132',
    latitude: 25.7617,
    longitude: -80.1918,
    dataset_date: new Date().toISOString().split('T')[0]
  };

  const orderResult = await post('/order', orderPayload);
  console.log(`   HTTP ${orderResult.status}`);

  if (orderResult.status === 200 && orderResult.body.success) {
    console.log('   ✓ Order record created');
    if (orderResult.body.emailNotified) {
      console.log('   ✓ Notification email sent');
    } else {
      console.log(`   ⚠ Email notification skipped: ${orderResult.body.emailError || 'no mailer'}`);
    }
  } else {
    console.warn('   ⚠ Order submission response:', JSON.stringify(orderResult.body));
  }

  // ── Summary ──────────────────────────────────────────────────
  console.log('\n=== FLOW COMPLETE ===');
  console.log('To email the report once Gmail SMTP is configured:');
  console.log(`  POST /send-to-client  { "email": "nyangelos4@gmail.com", "filePath": "${reportPath}" }`);
  console.log('\nTo configure email, add to geoscope/.env:');
  console.log('  GMAIL_USER=your-account@gmail.com');
  console.log('  GMAIL_PASS=your-app-password');
  console.log('\nDone.\n');
})();
