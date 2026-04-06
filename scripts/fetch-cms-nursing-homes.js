#!/usr/bin/env node
// Fetches CMS Nursing Home Compare (Provider Info) JSON API and writes a CSV
// with standard columns that import-csv.js can handle.
// Usage: node scripts/fetch-cms-nursing-homes.js <output.csv>

const https = require('https');
const fs = require('fs');
const path = require('path');

const outFile = process.argv[2] || path.join(__dirname, '../downloads/missing/NURSING_HOMES/cms_nh_facilities.csv');
const BASE_URL = 'https://data.cms.gov/provider-data/api/1/datastore/query/4pq5-n9py/0';
const PAGE_SIZE = 500;

function fetchPage(offset) {
  return new Promise((resolve, reject) => {
    const url = `${BASE_URL}?limit=${PAGE_SIZE}&offset=${offset}&results_format=json&count=false&schema=false`;
    https.get(url, { headers: { 'Accept': 'application/json' } }, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); }
        catch (e) { reject(new Error('JSON parse error at offset ' + offset + ': ' + e.message)); }
      });
    }).on('error', reject);
  });
}

const CSV_FIELDS = [
  'cms_certification_number_ccn',
  'provider_name',
  'provider_address',
  'citytown',
  'state',
  'zip_code',
  'latitude',
  'longitude',
];

const CSV_HEADERS = [
  'source_id',
  'name',
  'address',
  'city',
  'state',
  'zip',
  'latitude',
  'longitude',
];

function escapeCSV(v) {
  if (v === null || v === undefined) return '';
  const s = String(v);
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

async function main() {
  const dir = path.dirname(outFile);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

  const ws = fs.createWriteStream(outFile, { encoding: 'utf8' });
  ws.write(CSV_HEADERS.join(',') + '\n');

  let offset = 0;
  let total = 0;

  while (true) {
    process.stdout.write(`  Fetching offset=${offset}...`);
    let result;
    try {
      result = await fetchPage(offset);
    } catch (e) {
      console.error('\nFetch error:', e.message);
      break;
    }

    const rows = result.results || result.data || [];
    if (!rows.length) {
      console.log(' done (empty page)');
      break;
    }

    for (const row of rows) {
      const lat = row.latitude;
      const lon = row.longitude;
      if (!lat || !lon || lat === '' || lon === '') continue;

      const line = [
        escapeCSV('NH_' + row.cms_certification_number_ccn),
        escapeCSV(row.provider_name),
        escapeCSV(row.provider_address),
        escapeCSV(row.citytown),
        escapeCSV(row.state),
        escapeCSV(row.zip_code),
        escapeCSV(lat),
        escapeCSV(lon),
      ].join(',') + '\n';
      ws.write(line);
      total++;
    }

    console.log(` wrote ${rows.length} rows (total so far: ${total})`);

    if (rows.length < PAGE_SIZE) break;
    offset += PAGE_SIZE;
    await new Promise(r => setTimeout(r, 200)); // polite delay
  }

  ws.end();
  console.log(`✅ Done. Total rows written: ${total} → ${outFile}`);
}

main().catch(e => { console.error(e); process.exit(1); });
