#!/usr/bin/env node
// =============================================================================
// fetch-epa-rmp.js
// Download EPA Risk Management Plan (RMP) facility data
// =============================================================================

const fs = require('fs');
const { spawn } = require('child_process');
const path = require('path');

async function downloadViaPS(url, outFile) {
  return new Promise((resolve, reject) => {
    const cmd = `
      $dir = Split-Path -Parent '${outFile}'
      New-Item -ItemType Directory -Force -Path $dir | Out-Null
      $ProgressPreference = 'SilentlyContinue'
      Invoke-WebRequest -Uri '${url}' -OutFile '${outFile}' -UseBasicParsing -TimeoutSec 120
      if(Test-Path '${outFile}') { "DONE: $((Get-Item '${outFile}').Length)" } else { "FAILED" }
    `;
    const proc = spawn('powershell', ['-NoProfile', '-Command', cmd], { stdio: 'pipe' });
    let stdout = '';
    proc.stdout.on('data', data => stdout += data);
    proc.on('close', code => {
      if (code === 0) resolve(stdout);
      else reject(new Error(stdout));
    });
  });
}

async function main() {
  console.log('Downloading EPA RMP (Risk Management Plan) facility data...\n');

  // EPA RMP public data export - CSV or spreadsheet
  // The EPA RMPDATA API and export are available via:
  const sources = [
    {
      name: 'RMP Facilities Export',
      url: 'https://www.epa.gov/sites/default/files/2021-03/rmp_search_export_0.csv',
      dbname: 'RMP',
      category: 'regulatory'
    },
    // Alternative: EPA RMP Info via data.epa.gov
    {
      name: 'RMP via EPA FRS',
      url: 'https://dmap-prod-oms-edc.s3.amazonaws.com/OMS/FRS/Facilities.zip',
      dbname: 'RMP',
      category: 'regulatory'
    }
  ];

  for (const source of sources) {
    const dir = path.join(__dirname, `../downloads/missing/${source.dbname.replace(/\s+/g, '_')}`);
    const outFile = path.join(dir, source.name.toLowerCase().replace(/\s+/g, '_') + path.extname(source.url));

    process.stdout.write(`${source.name}... `);
    try {
      const result = await downloadViaPS(source.url, outFile);
      console.log(result.trim());
    } catch (e) {
      console.log(`❌ ERROR: ${e.message.substring(0, 100)}`);
    }
  }

  console.log('\n✅ RMP download attempt complete.');
  console.log('Note: If CSV files were downloaded, import them via:');
  console.log('  node scripts/import-csv.js <file.csv> RMP regulatory');
}

main().catch(e => { console.error(e); process.exit(1); });
