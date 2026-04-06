#!/usr/bin/env node
/**
 * =============================================================================
 * download-epa-frs-bulk.js
 * Downloads EPA FRS bulk data from AWS S3 - contains NPL, RMP, PFAS, etc.
 * =============================================================================
 */

const https = require('https');
const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

const BASE_MISSING = path.join(__dirname, '../downloads/missing');

// EPA FRS datasets available on AWS S3 (public)
const EPA_S3_FILES = [
  {
    name: 'EPA FRS Interests',
    s3url: 'https://dmap-prod-oms-edc.s3.amazonaws.com/OMS/FRS/FRS_Interests_Download.zip',
    dbnames: ['NPL', 'RMP', 'PFAS', 'ALL_FACILITIES'],
    description: 'Complete EPA FRS facility interests and environmental dataset names'
  }
];

function downloadFile(url, dest) {
  return new Promise((resolve, reject) => {
    const dir = path.dirname(dest);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

    const file = fs.createWriteStream(dest);
    const startTime = Date.now();
    let lastUpdate = startTime;
    let downloaded = 0;

    https.get(url, { timeout: 600000 }, res => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        downloadFile(res.headers.location, dest).then(resolve).catch(reject);
        return;
      }

      if (res.statusCode !== 200) {
        reject(new Error(`HTTP ${res.statusCode}`));
        return;
      }

      const contentLength = parseInt(res.headers['content-length'] || 0);
      res.on('data', chunk => {
        downloaded += chunk.length;
        const now = Date.now();
        if (now - lastUpdate > 2000) {
          const mb = (downloaded / 1024 / 1024).toFixed(2);
          const pct = contentLength > 0 ? ((downloaded / contentLength) * 100).toFixed(0) : '?';
          const elapsed = ((now - startTime) / 1000).toFixed(1);
          process.stdout.write(`\r  Progress: ${mb}MB (${pct}%) [${elapsed}s]`);
          lastUpdate = now;
        }
      });

      res.pipe(file);
      file.on('finish', () => {
        file.close();
        const totalMB = (downloaded / 1024 / 1024).toFixed(2);
        process.stdout.write(`\r  ✅ Downloaded: ${totalMB}MB\n`);
        resolve(dest);
      });
      file.on('error', reject);
    }).on('error', reject).on('timeout', () => reject(new Error('Timeout')));
  });
}

function extractZip(zipPath, destDir) {
  return new Promise((resolve, reject) => {
    console.log(`  📦 Extracting...`);
    try {
      const cmd = `Expand-Archive -LiteralPath '${zipPath}' -DestinationPath '${destDir}' -Force`;
      execSync(`powershell -NoProfile -Command "${cmd}"`, { stdio: 'inherit' });
      console.log(`  ✅ Extracted to ${destDir}`);
      resolve(destDir);
    } catch (e) {
      reject(e);
    }
  });
}

async function processFile(source) {
  console.log(`\n📥 ${source.name}`);
  console.log(`   ${source.description}`);

  const dir = path.join(BASE_MISSING, 'EPA_FRS_BULK');
  fs.mkdirSync(dir, { recursive: true });

  const zipFile = path.join(dir, `${source.name.replace(/\s+/g, '_')}.zip`);
  const extractDir = path.join(dir, source.name.replace(/\s+/g, '_'));

  // Check if already exists
  if (fs.existsSync(zipFile)) {
    console.log(`  📦 Already downloaded: ${(fs.statSync(zipFile).size / 1024 / 1024).toFixed(2)}MB`);
    if (!fs.existsSync(extractDir)) {
      await extractZip(zipFile, extractDir);
    } else {
      console.log(`  📂 Already extracted`);
    }
    return extractDir;
  }

  // Download
  console.log(`  ⏳ Downloading from AWS S3...`);
  try {
    await downloadFile(source.s3url, zipFile);
    const size = fs.statSync(zipFile).size;
    
    // Extract
    await extractZip(zipFile, extractDir);
    
    // List contents
    console.log(`\n  📂 Contents:`);
    const files = execSync(`dir /b "${extractDir}"`, { encoding: 'utf8' }).split('\n').filter(f => f.trim());
    files.forEach(f => console.log(`     - ${f}`));

    return extractDir;
  } catch (e) {
    console.error(`  ❌ Error: ${e.message}`);
    throw e;
  }
}

async function main() {
  console.log('=============================================================================');
  console.log('📥 EPA FRS BULK DOWNLOAD - NPL, RMP, PFAS & More');
  console.log('=============================================================================\n');

  const results = [];
  for (const source of EPA_S3_FILES) {
    try {
      const extractDir = await processFile(source);
      results.push({ source, status: 'success', dir: extractDir });
    } catch (e) {
      results.push({ source, status: 'failed', error: e.message });
    }
  }

  // Summary
  console.log('\n\n=============================================================================');
  console.log('📋 BULK DOWNLOAD SUMMARY');
  console.log('=============================================================================\n');

  for (const result of results) {
    if (result.status === 'success') {
      console.log(`✅ ${result.source.name}`);
      console.log(`   Location: ${result.dir}`);
      console.log(`   Datasets: ${result.source.dbnames.join(', ')}`);
    } else {
      console.log(`❌ ${result.source.name}: ${result.error}`);
    }
  }

  console.log('\n=============================================================================');
  console.log('🚀 NEXT STEPS');
  console.log('=============================================================================\n');

  console.log(`List downloaded CSV files:`);
  console.log(`  ls -la downloads/missing/EPA_FRS_BULK/*/*.csv\n`);

  console.log(`Then import via:`);
  console.log(`  node scripts/import-csv.js <csv-file> <DATABASE_NAME> <category>\n`);

  console.log(`Example for NPL:`);
  console.log(`  node scripts/import-csv.js downloads/missing/EPA_FRS_BULK/FRS_Interests/Combined.csv NPL contamination\n`);
}

main().catch(e => { console.error('Fatal:', e); process.exit(1); });
