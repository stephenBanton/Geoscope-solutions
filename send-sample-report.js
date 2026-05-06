const fs = require('fs');

(async () => {
  const recipientEmail = process.argv[2] || process.env.INFO_MAIL || process.env.COMPANY_INFO_EMAIL || 'info@geoscopesolutions.com';
  const apiBaseUrl = (process.env.API_BASE_URL || 'http://localhost:5000').replace(/\/$/, '');

  const payload = {
    project_name: 'Sample ESG Site Report',
    client_name: 'GeoScope Demo Client',
    address: '100 Biscayne Blvd, Miami, FL',
    latitude: 25.7617,
    longitude: -80.1918,
    paid: true,
    summary: 'Sample deliverable with imagery, graph, and statistics blocks included.',
    environmentalData: {
      environmentalSites: [
        { id: 'S1', name: 'Fuel Terminal', database: 'EPA FUELS', address: '111 Harbor Rd', distance: '0.3 mi', status: 'Active' },
        { id: 'S2', name: 'Historic Dry Cleaner', database: 'SCRD DRYCLEANERS', address: '222 Market St', distance: '0.6 mi', status: 'Closed' },
        { id: 'S3', name: 'School Campus', database: 'SCHOOLS PUBLIC', address: '333 School Ave', distance: '0.9 mi', status: 'Active' }
      ],
      floodZones: [{ attributes: { FLD_ZONE: 'AE' } }],
      schools: [{ attributes: { NAME: 'Downtown Public School' } }],
      governmentRecords: [{ FacilityName: 'Municipal Storage Site' }],
      rainfall: [{ date: '2023-01-01', precipitation: '18 mm' }]
    },
    radius: 1609.344
  };

  const generateRes = await fetch(`${apiBaseUrl}/generate-report`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });

  const generated = await generateRes.json();
  const generatedPath = generated.filePath || generated.reportPath || null;
  console.log('GENERATE_STATUS', generateRes.status);
  console.log('GENERATE_BODY', JSON.stringify(generated));
  console.log('GENERATE_FILE', generatedPath || 'none');

  if (!generateRes.ok || !generatedPath || !fs.existsSync(generatedPath)) {
    throw new Error('Report generation failed or file missing');
  }

  const sendRes = await fetch(`${apiBaseUrl}/send-to-client`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      email: recipientEmail,
      filePath: generatedPath
    })
  });

  const sendText = await sendRes.text();
  console.log('SEND_STATUS', sendRes.status);
  console.log('SEND_BODY', sendText);

  if (!sendRes.ok) {
    throw new Error('Send endpoint failed');
  }
})();
