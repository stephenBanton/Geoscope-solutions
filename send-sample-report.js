const fs = require('fs');

(async () => {
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
    radius: 1500
  };

  const generateRes = await fetch('http://localhost:5000/generate-report', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });

  const generated = await generateRes.json();
  console.log('GENERATE_STATUS', generateRes.status);
  console.log('GENERATE_FILE', generated.filePath || 'none');

  if (!generateRes.ok || !generated.filePath || !fs.existsSync(generated.filePath)) {
    throw new Error('Report generation failed or file missing');
  }

  const sendRes = await fetch('http://localhost:5000/send-to-client', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      email: 'steveochibo@gmail.com',
      filePath: generated.filePath
    })
  });

  const sendText = await sendRes.text();
  console.log('SEND_STATUS', sendRes.status);
  console.log('SEND_BODY', sendText);

  if (!sendRes.ok) {
    throw new Error('Send endpoint failed');
  }
})();
