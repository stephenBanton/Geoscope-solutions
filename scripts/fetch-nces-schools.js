#!/usr/bin/env node
// =============================================================================
// fetch-nces-schools.js
// Download NCES Common Core of Data (CCD) for public and private schools
// Uses NCES API or direct CSV files
// =============================================================================

const https = require('https');
const fs = require('fs');
const path = require('path');

function httpGet(url, isJson = true) {
  return new Promise((resolve, reject) => {
    https.get(url, { timeout: 30000 }, res => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        if (isJson) {
          try { resolve(JSON.parse(data)); }
          catch (e) { reject(e); }
        } else {
          resolve(data);
        }
      });
    }).on('error', reject);
  });
}

async function main() {
  const schoolType = process.argv[2] || 'public'; // public or private
  console.log(`Fetching NCES ${schoolType} schools data...`);

  // NCES CCD public schools: direct data.nces.ed.gov URL
  // For now, try the data.nces.ed.gov API
  const urls = {
    public: 'https://data.nces.ed.gov/api/v1/schools?type=public&limit=10000&format=json',
    private: 'https://data.nces.ed.gov/api/v1/schools?type=private&limit=10000&format=json',
  };

  try {
    const url = urls[schoolType] || urls.public;
    console.log(`  Fetching from: ${url}`);
    const data = await httpGet(url, true);
    
    const dir = path.join(__dirname, `../downloads/missing/SCHOOLS_${schoolType.toUpperCase()}`);
    fs.mkdirSync(dir, { recursive: true });
    
    const outFile = path.join(dir, `schools_${schoolType}.json`);
    fs.writeFileSync(outFile, JSON.stringify(data, null, 2));
    
    console.log(`✅ Downloaded: ${outFile}`);
    console.log(`  Records: ${Array.isArray(data) ? data.length : 0}`);
  } catch (e) {
    console.error(`❌ Error: ${e.message}`);
    process.exit(1);
  }
}

main();
