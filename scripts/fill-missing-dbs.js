#!/usr/bin/env node
require('dotenv').config({ path: require('path').join(__dirname, '../.env') });

const fs = require('fs');
const path = require('path');
const axios = require('axios');
const { spawnSync } = require('child_process');
const { Pool } = require('pg');
const sourceMap = require('./missing-db-sources');

const args = process.argv.slice(2);
const dryRun = args.includes('--dry-run');
const recursive = !args.includes('--no-recursive');
const sourceRootArg = getArgValue('--source-root');
const downloadRootArg = getArgValue('--download-root');
const sourceRoot = sourceRootArg || 'H:\\databae';
const downloadRoot = downloadRootArg || path.join(__dirname, '..', 'downloads', 'missing');

const pool = new Pool({
  host: process.env.PG_HOST,
  port: Number(process.env.PG_PORT || 5432),
  database: process.env.PG_DATABASE,
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
});

function getArgValue(flag) {
  const i = args.indexOf(flag);
  if (i === -1) return null;
  return args[i + 1] || null;
}

function safeName(name) {
  return name.replace(/[^a-zA-Z0-9._-]+/g, '_');
}

function walkFiles(dir, out = []) {
  if (!fs.existsSync(dir)) return out;
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  for (const e of entries) {
    const p = path.join(dir, e.name);
    if (e.isDirectory()) {
      if (recursive) walkFiles(p, out);
      continue;
    }
    out.push(p);
  }
  return out;
}

function toRegexList(localRegex) {
  return (localRegex || []).map((r) => new RegExp(r, 'i'));
}

function isZipFile(filePath) {
  return /\.zip$/i.test(String(filePath || ''));
}

function extractZip(zipPath, outDir) {
  fs.mkdirSync(outDir, { recursive: true });
  const command = [
    '-NoProfile',
    '-ExecutionPolicy', 'Bypass',
    '-Command',
    `Expand-Archive -LiteralPath '${zipPath.replace(/'/g, "''")}' -DestinationPath '${outDir.replace(/'/g, "''")}' -Force`
  ];
  const result = spawnSync('powershell', command, {
    cwd: path.join(__dirname, '..'),
    stdio: 'pipe',
    encoding: 'utf8',
    shell: false,
  });

  if (result.status !== 0) {
    throw new Error((result.stderr || result.stdout || 'zip extraction failed').trim());
  }
}

function listCsvFiles(dir) {
  return walkFiles(dir).filter((f) => /\.csv$/i.test(f));
}

function findExtractedMatches(allCsvFiles, cfg = {}, dbName = '') {
  const patterns = [];
  if (Array.isArray(cfg.extractedCsvRegex)) patterns.push(...cfg.extractedCsvRegex);
  if (!patterns.length && dbName) patterns.push(safeName(dbName));
  const regexes = patterns.map((r) => new RegExp(r, 'i'));
  return allCsvFiles.filter((filePath) => regexes.some((re) => re.test(filePath)));
}

function findLocalMatches(allCsvFiles, dbName) {
  const cfg = sourceMap[dbName] || {};
  const regexes = toRegexList(cfg.localRegex);
  if (!regexes.length) return [];
  return allCsvFiles.filter((f) => regexes.some((re) => re.test(f)));
}

async function getMissing() {
  const q = `
    SELECT UPPER(c.name) AS name, c.category
    FROM database_catalog c
    LEFT JOIN (
      SELECT DISTINCT UPPER(database_name) AS name
      FROM environmental_sites
      WHERE database_name IS NOT NULL AND TRIM(database_name) <> ''
    ) l ON l.name = UPPER(c.name)
    WHERE c.category IN ('contamination','regulatory','hydrology','geology','receptors')
      AND l.name IS NULL
    ORDER BY 1
  `;
  const r = await pool.query(q);
  return r.rows.map((x) => ({ name: x.name, category: x.category }));
}

async function downloadFile(url, outPath) {
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  const res = await axios({ method: 'get', url, responseType: 'stream', timeout: 120000 });
  await new Promise((resolve, reject) => {
    const w = fs.createWriteStream(outPath);
    res.data.pipe(w);
    w.on('finish', resolve);
    w.on('error', reject);
  });
}

function runImport(filePath, dbName, category) {
  const cmdArgs = [path.join('scripts', 'import-csv.js'), filePath, dbName, category];
  const r = spawnSync('node', cmdArgs, {
    cwd: path.join(__dirname, '..'),
    stdio: 'inherit',
    shell: false,
  });
  return r.status === 0;
}

async function main() {
  console.log('=== Fill Missing Databases ===');
  console.log('Source root: ' + sourceRoot);
  console.log('Download root: ' + downloadRoot);
  console.log('Mode: ' + (dryRun ? 'dry-run' : 'apply'));

  const missing = await getMissing();
  console.log('Missing count: ' + missing.length);
  if (!missing.length) return;

  const allSourceFiles = walkFiles(sourceRoot).filter((f) => /\.csv$/i.test(f));
  console.log('Local CSV files discovered: ' + allSourceFiles.length);

  const summary = {
    localImported: 0,
    downloadedImported: 0,
    noSourceConfigured: [],
    noLocalMatch: [],
    failedImports: [],
    downloadedFiles: 0,
  };

  for (const item of missing) {
    const dbName = item.name;
    const category = item.category;
    const cfg = sourceMap[dbName] || {};

    console.log('\n--- ' + dbName + ' (' + category + ') ---');

    const localMatches = findLocalMatches(allSourceFiles, dbName);
    if (localMatches.length) {
      console.log('Local matches: ' + localMatches.length);
      for (const filePath of localMatches) {
        console.log('  local: ' + filePath);
        if (!dryRun) {
          const ok = runImport(filePath, dbName, category);
          if (ok) summary.localImported++;
          else summary.failedImports.push(filePath + ' => ' + dbName);
        }
      }
    } else {
      summary.noLocalMatch.push(dbName);
      console.log('Local matches: 0');
    }

    const urls = Array.isArray(cfg.urls) ? cfg.urls : [];
    if (!urls.length) {
      summary.noSourceConfigured.push(dbName);
      console.log('Download URLs: not configured');
      continue;
    }

    for (const url of urls) {
      const ext = path.extname(new URL(url).pathname) || '.csv';
      const outFile = path.join(downloadRoot, safeName(dbName), safeName(path.basename(new URL(url).pathname || (dbName + '.csv'))));
      console.log('  download: ' + url);
      console.log('  to      : ' + outFile);
      if (dryRun) continue;
      try {
        await downloadFile(url, outFile);
        summary.downloadedFiles++;
      } catch (err) {
        summary.failedImports.push('download failed: ' + url + ' => ' + err.message);
        continue;
      }

      if (isZipFile(outFile)) {
        const extractDir = path.join(downloadRoot, safeName(dbName), safeName(path.basename(outFile, path.extname(outFile))) + '_extracted');
        console.log('  extract : ' + extractDir);
        if (dryRun) continue;

        try {
          extractZip(outFile, extractDir);
        } catch (err) {
          summary.failedImports.push('extract failed: ' + outFile + ' => ' + err.message);
          continue;
        }

        const extractedCsvs = findExtractedMatches(listCsvFiles(extractDir), cfg, dbName);
        if (!extractedCsvs.length) {
          summary.failedImports.push('no extracted csv match: ' + outFile + ' => ' + dbName);
          continue;
        }

        for (const extractedCsv of extractedCsvs) {
          console.log('  extracted csv: ' + extractedCsv);
          const ok = runImport(extractedCsv, dbName, category);
          if (ok) summary.downloadedImported++;
          else summary.failedImports.push(extractedCsv + ' => ' + dbName);
        }
        continue;
      }

      if (!/\.csv$/i.test(ext) && !/\.csv$/i.test(outFile)) {
        console.log('  skip import (non-csv): ' + outFile);
        continue;
      }

      const ok = runImport(outFile, dbName, category);
      if (ok) summary.downloadedImported++;
      else summary.failedImports.push(outFile + ' => ' + dbName);
    }
  }

  console.log('\n=== Summary ===');
  console.log('Local imports: ' + summary.localImported);
  console.log('Downloaded files: ' + summary.downloadedFiles);
  console.log('Downloaded imports: ' + summary.downloadedImported);
  console.log('Failed actions: ' + summary.failedImports.length);
  console.log('Missing local match count: ' + summary.noLocalMatch.length);
  console.log('No URL configured count: ' + summary.noSourceConfigured.length);

  if (summary.noSourceConfigured.length) {
    console.log('\nNo URL configured for:');
    for (const n of summary.noSourceConfigured) console.log(' - ' + n);
  }

  if (summary.failedImports.length) {
    console.log('\nFailures:');
    for (const f of summary.failedImports.slice(0, 100)) console.log(' - ' + f);
  }
}

main()
  .catch((err) => {
    console.error(err.message);
    process.exit(1);
  })
  .finally(async () => {
    await pool.end();
  });
