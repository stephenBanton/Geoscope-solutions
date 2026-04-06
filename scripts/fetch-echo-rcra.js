#!/usr/bin/env node
// =============================================================================
// fetch-echo-rcra.js  — Download RCRA handler data from EPA ECHO REST API
// Fetches SQG, TSDF, VSQG, LQG (and others) and writes separate CSVs.
// Usage: node scripts/fetch-echo-rcra.js [category]
//   category: SQG | TSDF | VSQG | LQG | ALL  (default: ALL)
// =============================================================================

const https = require('https');
const fs = require('fs');
const path = require('path');

const BASE = 'https://ofmpub.epa.gov/echo/rcra_rest_services.get_facility_info';

// RCRA generator categories (pgm_sys_cnst values)
// LQG=Large Quantity Generator, SQG=Small Qty, VSQG=Very Small Qty
// TSDF=Treatment/Storage/Disposal
const CATEGORIES = {
  'RCRA SQG':  { qparams: 'p_hreport=SQG', dbName: 'RCRA SQG',  category: 'contamination' },
  'RCRA TSDF': { qparams: 'p_hreport=TSD', dbName: 'RCRA TSDF', category: 'contamination' },
  'RCRA VSQG': { qparams: 'p_hreport=VSQG', dbName: 'RCRA VSQG', category: 'contamination' },
};

const targetArg = (process.argv[2] || 'ALL').toUpperCase();
const selectedCats = targetArg === 'ALL'
  ? Object.keys(CATEGORIES)
  : Object.keys(CATEGORIES).filter(k => k.includes(targetArg));

const OUT_DIR = path.join(__dirname, '../downloads/missing');

function httpGet(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { 'Accept': 'application/json' } }, res => {
      let d = '';
      res.on('data', c => d += c);
      res.on('end', () => {
        try { resolve(JSON.parse(d)); }
        catch (e) { reject(new Error('JSON parse error: ' + e.message + '\nURL: ' + url)); }
      });
    }).on('error', reject);
  });
}

function esc(v) {
  if (v === null || v === undefined) return '';
  const s = String(v).trim();
  return (s.includes(',') || s.includes('"') || s.includes('\n'))
    ? '"' + s.replace(/"/g, '""') + '"'
    : s;
}

async function fetchCategory(catName, config) {
  console.log(`\n=== Fetching ${catName} ===`);
  const dir = path.join(OUT_DIR, catName.replace(/\s+/g, '_'));
  fs.mkdirSync(dir, { recursive: true });
  const outFile = path.join(dir, catName.toLowerCase().replace(/\s+/g, '_') + '.csv');
  const ws = fs.createWriteStream(outFile, { encoding: 'utf8' });
  ws.write('source_id,name,address,city,state,zip,latitude,longitude\n');

  let page = 1;
  let total = 0;

  while (true) {
    const url = `https://ofmpub.epa.gov/echo/rcra_rest_services.get_facility_info?output=JSON&p_fn=&${config.qparams}&p_pn=${page}&p_rpp=1000`;
    process.stdout.write(`  Page ${page}...`);
    let result;
    try {
      result = await httpGet(url);
    } catch (e) {
      console.error('\n  ERROR:', e.message);
      break;
    }

    const facilities = result?.Results?.Facilities || [];
    if (!facilities.length) {
      console.log(' no more results');
      break;
    }

    for (const f of facilities) {
      const lat = f.FacLat || f.LatWgs84;
      const lon = f.FacLong || f.LongWgs84;
      if (!lat || !lon) continue;
      ws.write([
        esc('RCRA_' + (f.RegistryID || f.FacilityID || f.HandlerID || '')),
        esc(f.FacName || f.FacilityName || ''),
        esc(f.FacStreet || f.FacAddr1 || ''),
        esc(f.FacCity || ''),
        esc(f.FacState || ''),
        esc(f.FacZip || ''),
        esc(lat),
        esc(lon),
      ].join(',') + '\n');
      total++;
    }

    console.log(` ${facilities.length} records (total: ${total})`);
    if (facilities.length < 1000) break;
    page++;
    await new Promise(r => setTimeout(r, 300));
  }

  ws.end();
  console.log(`  ✅ ${catName}: ${total} rows → ${outFile}`);
  return { catName, total, outFile };
}

async function main() {
  console.log('Fetching RCRA data from EPA ECHO API...');
  console.log('Categories:', selectedCats.join(', '));

  for (const cat of selectedCats) {
    await fetchCategory(cat, CATEGORIES[cat]);
  }

  console.log('\n=== All done ===');
}

main().catch(e => { console.error(e); process.exit(1); });
