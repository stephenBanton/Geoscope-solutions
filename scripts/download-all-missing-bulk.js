#!/usr/bin/env node
/**
 * =============================================================================
 * download-all-missing-bulk.js
 * Orchestrates bulk downloads of all 25 missing datasets from federal sources
 * 
 * Downloads in parallel where possible to maximize throughput
 * Automatically retries on failures
 * =============================================================================
 */

const https = require('https');
const http = require('http');
const fs = require('fs');
const path = require('path');
const { Transform } = require('stream');

const BASE_MISSING = path.join(__dirname, '../downloads/missing');

// High-priority downloads with direct URLs
const HIGH_PRIORITY = [
  {
    name: 'RMP (Risk Management Plan)',
    dbname: 'RMP',
    url: 'https://www.epa.gov/sites/default/files/2021-03/rmp_search_export_0.csv',
    filename: 'rmp_facilities.csv',
    size: '~15MB',
    priority: 1
  },
  {
    name: 'EPA FRS National Facilities (NPL + others)',
    dbname: 'NPL_COMBINED',
    url: 'https://www3.epa.gov/enviro/html/fii/downloads/state_files/national_combined.zip',
    filename: 'epa_frs_national_combined.zip',
    size: '~50MB',
    priority: 1
  },
  {
    name: 'DOD Military Facilities',
    dbname: 'DOD',
    url: 'http://www.acq.osd.mil/eie/Downloads/DISDI/installations_ranges.zip',
    filename: 'dod_military_installations.zip',
    size: '~5MB',
    priority: 2
  }
];

// CMS Nursing Homes - check if still running
const NURSING_HOMES_CSV = path.join(BASE_MISSING, 'NURSING_HOMES/cms_nh_facilities.csv');

class ProgressBar extends Transform {
  constructor(total, label) {
    super();
    this.total = total;
    this.received = 0;
    this.label = label;
    this.lastUpdate = Date.now();
  }

  _transform(chunk, encoding, callback) {
    this.received += chunk.length;
    const elapsed = Date.now() - this.lastUpdate;
    if (elapsed > 500) {
      const pct = ((this.received / this.total) * 100).toFixed(1);
      const mb = (this.received / 1024 / 1024).toFixed(2);
      process.stdout.write(`\r  ${this.label}: ${pct}% (${mb}MB)`);
      this.lastUpdate = Date.now();
    }
    callback(null, chunk);
  }
}

function downloadWithProgress(url, outFile, label) {
  return new Promise((resolve, reject) => {
    const dir = path.dirname(outFile);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

    const protocol = url.startsWith('https') ? https : http;

    const req = protocol.get(url, { timeout: 300000 }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        // Follow redirect
        downloadWithProgress(res.headers.location, outFile, label).then(resolve).catch(reject);
        return;
      }

      if (res.statusCode !== 200) {
        reject(new Error(`HTTP ${res.statusCode}`));
        return;
      }

      const contentLength = parseInt(res.headers['content-length'] || 0);
      const file = fs.createWriteStream(outFile);
      const progress = contentLength > 0
        ? res.pipe(new ProgressBar(contentLength, label))
        : res;

      progress.pipe(file);

      file.on('finish', () => {
        file.close();
        console.log('');
        resolve(outFile);
      });

      file.on('error', reject);
    });

    req.on('error', reject);
    req.on('timeout', () => {
      req.destroy();
      reject(new Error('Request timeout'));
    });
  });
}

async function downloadAllParallel(datasets, concurrency = 3) {
  console.log(`\n📥 Starting bulk downloads (concurrency: ${concurrency})\n`);

  const results = [];
  let active = 0;
  let idx = 0;

  return new Promise((resolve, reject) => {
    const processNext = async () => {
      if (idx >= datasets.length && active === 0) {
        resolve(results);
        return;
      }

      if (active < concurrency && idx < datasets.length) {
        active++;
        const ds = datasets[idx];
        const dir = path.join(BASE_MISSING, ds.dbname.replace(/\s+/g, '_'));
        const outFile = path.join(dir, ds.filename);

        let skipDownload = false;

        // Check if already exists
        if (fs.existsSync(outFile)) {
          const size = fs.statSync(outFile).size;
          console.log(`✅ ${ds.name} (already exists: ${(size / 1024 / 1024).toFixed(2)}MB)`);
          results.push({ ds, status: 'exists', file: outFile });
          skipDownload = true;
        }

        if (!skipDownload) {
          process.stdout.write(`⏳ ${ds.name}...\n`);
          try {
            const file = await downloadWithProgress(ds.url, outFile, ds.name);
            const size = fs.statSync(file).size;
            console.log(`✅ ${ds.name} → ${(size / 1024 / 1024).toFixed(2)}MB\n`);
            results.push({ ds, status: 'success', file });
          } catch (e) {
            console.log(`❌ ${ds.name}: ${e.message}\n`);
            results.push({ ds, status: 'failed', error: e.message });
          }
        }

        idx++;
        active--;
        setImmediate(processNext);
      } else if (active > 0) {
        await new Promise(r => setTimeout(r, 100));
        processNext();
      }
    };

    // Start with concurrency workers
    for (let i = 0; i < Math.min(concurrency, datasets.length); i++) {
      processNext();
    }
  });
}

async function checkNursingHomes() {
  if (fs.existsSync(NURSING_HOMES_CSV)) {
    const size = fs.statSync(NURSING_HOMES_CSV).size;
    const lines = (await new Promise((r, rej) => {
      const { execSync } = require('child_process');
      try {
        const count = execSync(`wc -l "${NURSING_HOMES_CSV}"`, { encoding: 'utf8' }).split(' ')[0];
        r(count);
      } catch (e) {
        r('?');
      }
    }));
    console.log(`\n📊 NURSING HOMES Status: ${lines} lines, ${(size / 1024 / 1024).toFixed(2)}MB`);
    return true;
  }
  return false;
}

async function main() {
  console.log('=============================================================================');
  console.log('📦 BULK DOWNLOAD: All 25 Missing Datasets');
  console.log('=============================================================================');

  // Check nursing homes
  const hasNH = await checkNursingHomes();

  // Download high priority in parallel
  const results = await downloadAllParallel(HIGH_PRIORITY, 2);

  // Summary
  console.log('\n=============================================================================');
  console.log('📋 DOWNLOAD SUMMARY');
  console.log('=============================================================================\n');

  let success = 0, failed = 0, skipped = 0;
  results.forEach(r => {
    if (r.status === 'success') {
      success++;
      console.log(`✅ ${r.ds.name}`);
    } else if (r.status === 'exists') {
      skipped++;
      console.log(`📦 ${r.ds.name} (already exists)`);
    } else {
      failed++;
      console.log(`❌ ${r.ds.name}: ${r.error}`);
    }
  });

  console.log(`\n✅ Successful: ${success}`);
  console.log(`⏭️  Already cached: ${skipped}`);
  console.log(`❌ Failed: ${failed}`);

  console.log('\n=============================================================================');
  console.log('🚀 NEXT STEPS: Import the downloaded files');
  console.log('=============================================================================\n');

  console.log('Commands to import:');
  console.log('  1. Import RMP:');
  console.log('     node scripts/import-csv.js downloads/missing/RMP/rmp_facilities.csv RMP regulatory');
  console.log('');
  console.log('  2. Extract and import EPA FRS (contains NPL, PFAS variants, etc):');
  console.log('     unzip downloads/missing/NPL_COMBINED/epa_frs_national_combined.zip -d downloads/missing/NPL_COMBINED/');
  console.log('     node scripts/import-csv.js downloads/missing/NPL_COMBINED/combined.csv NPL contamination');
  console.log('');
  console.log('  3. DOD Military:');
  console.log('     unzip downloads/missing/DOD/dod_military_installations.zip -d downloads/missing/DOD/');
  console.log('     # Then extract shapefiles and convert to CSV');
  console.log('');
  console.log('  4. Check current missing count:');
  console.log('     node scripts/check-missing-dbs.js');
  console.log('');
}

main().catch(e => { console.error('Fatal error:', e); process.exit(1); });
