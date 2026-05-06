#!/usr/bin/env node
// Test HIFLD schools service - probe field names and test different query formats
const https = require('https');

function fetchJson(url) {
  return new Promise((resolve) => {
    const req = https.get(url, { headers: { 'User-Agent': 'GeoScope/1.0' } }, (res) => {
      let data = '';
      res.on('data', (c) => data += c);
      res.on('end', () => {
        console.log('HTTP', res.statusCode);
        try { resolve(JSON.parse(data)); } catch (e) { console.log('parse err:', data.slice(0, 400)); resolve(null); }
      });
    });
    req.setTimeout(20000, () => { req.destroy(); resolve(null); });
    req.on('error', (e) => { console.log('net err:', e.message); resolve(null); });
  });
}

async function main() {
  const base = 'https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Public_Schools/FeatureServer/0';

  // Test 1: simple unfiltered query with small limit
  console.log('\n--- Test 1: no filter ---');
  const t1 = await fetchJson(`${base}/query?where=1%3D1&outFields=NCESSCH,NAME,STREET,CITY,STATE,ZIP,LAT,LON&f=json&resultRecordCount=3`);
  if (t1 && t1.features && t1.features[0]) {
    console.log('Fields:', Object.keys(t1.features[0].attributes).join(', '));
    console.log('Sample:', JSON.stringify(t1.features[0].attributes));
    console.log('exceededTransferLimit:', t1.exceededTransferLimit);
  } else if (t1 && t1.error) {
    console.log('Error:', JSON.stringify(t1.error));
  }

  // Test 2: filter by STATE field
  console.log('\n--- Test 2: STATE=AK filter ---');
  const t2 = await fetchJson(`${base}/query?where=STATE+%3D+%27AK%27&outFields=NCESSCH,NAME,STATE,LAT,LON&f=json&resultRecordCount=3`);
  if (t2 && t2.features) {
    console.log('Count:', t2.features.length);
    if (t2.features[0]) console.log('Sample:', JSON.stringify(t2.features[0].attributes));
  } else if (t2 && t2.error) {
    console.log('Error:', JSON.stringify(t2.error));
  }

  // Test 3: check max record count
  console.log('\n--- Test 3: service info ---');
  const t3 = await fetchJson(`${base}?f=json`);
  if (t3) {
    console.log('Max record count:', t3.maxRecordCount);
    console.log('Supports pagination:', t3.advancedQueryCapabilities && t3.advancedQueryCapabilities.supportsPagination);
    if (t3.fields) {
      console.log('Fields:', t3.fields.map(f => f.name).join(', '));
    }
  }
}

main().catch(console.error);
