#!/usr/bin/env node
/**
 * =============================================================================
 * download-50state-coverage.js
 * Downloads and imports the 15 most critical missing environmental datasets
 * to achieve full 50-state USA coverage in GeoScope.
 *
 * Each dataset uses a reliable public federal API or bulk download URL.
 * No API keys required — all sources are publicly accessible.
 *
 * Usage:
 *   node scripts/download-50state-coverage.js            # download ALL
 *   node scripts/download-50state-coverage.js MSHA       # single dataset
 *   node scripts/download-50state-coverage.js --list     # show all available
 *   node scripts/download-50state-coverage.js --import-only MSHA  # skip download
 *
 * Datasets covered (all 50 US states):
 *   1.  MSHA_MINES      - Mine Safety & Health Administration (~85K mines)
 *   2.  NID_DAMS        - National Inventory of Dams (~90K dams)
 *   3.  NPL_SUPERFUND   - EPA Superfund National Priorities List (~1,300 active + archived)
 *   4.  RMP             - EPA Risk Management Plans (~15K chemical facilities)
 *   5.  UST             - EPA Underground Storage Tanks (~600K)
 *   6.  BROWNFIELDS     - EPA Brownfields / ACRES cleanup sites
 *   7.  HOSPITALS       - CMS Hospital General Information (~8K)
 *   8.  NURSING_HOMES   - CMS Nursing Home Care (~15K)
 *   9.  POWER_PLANTS    - HIFLD / EIA Power Plants (~10K)
 *  10.  AIRPORTS        - FAA Airport Data (~25K)
 *  11.  NUCLEAR         - NRC Licensed Nuclear Facilities (~5K)
 *  12.  PIPELINES       - PHMSA Gas Distribution Facilities (~1.5K)
 *  13.  PFAS_SITES      - EPA PFAS Analytics Tool sites
 *  14.  LANDFILLS       - EPA RCRA TSDF / LMOP Landfills (~2K methane)
 *  15.  COLLEGES        - NCES Colleges & Universities (~7K)
 * =============================================================================
 */

require('dotenv').config({ path: require('path').join(__dirname, '../.env') });

const https = require('https');
const http  = require('http');
const fs    = require('fs');
const path  = require('path');
const { parse } = require('csv-parse');
const { Pool } = require('pg');

const DOWNLOADS_DIR = path.join(__dirname, '../downloads/50state');
const BATCH_SIZE    = 500;

// ── Column aliases for normalizing diverse federal CSV formats ─────────────
const LAT_ALIASES   = ['latitude','lat','y_lat','latitude_measure','decimal_latitude','y','lat_dd','dec_lat','lat_wgs84','b_lat_c','y_coord','county_lat'];
const LON_ALIASES   = ['longitude','lon','long','x_long','longitude_measure','decimal_longitude','x','lon_dd','dec_long','lon_wgs84','b_long_c','x_coord','county_long'];
const NAME_ALIASES  = ['facility_name','site_name','name','mine_name','dam_name','hospital_name','provider_name','plant_name','fac_name','primary_name','title','institution_name'];
const ADDR_ALIASES  = ['address','street_address','location_address','fac_street','addr1','addr','street','physical_address'];
const CITY_ALIASES  = ['city','city_name','fac_city','county_name','provider_city','county'];
const STATE_ALIASES = ['state','state_code','fac_state','st','state_abbr','provider_state','state_name'];
const ZIP_ALIASES   = ['zip_code','zip','fac_zip','postal_code','provider_zip_code'];
const STATUS_ALIASES= ['status','activity_status','site_status','active_flag','generator_status','mine_status'];
const ID_ALIASES    = ['id','registry_id','facility_id','handler_id','site_id','mine_id','nid_id','rmp_id','source_id'];

// Extend aliases for MSHA and NID-specific column names
LAT_ALIASES.push('latitude', 'lat_dd83', 'dec_lat_va', 'latitude_degrees');
LON_ALIASES.push('longitude', 'lon_dd83', 'dec_long_va', 'longitude_degrees');
NAME_ALIASES.push('current_mine_name', 'dam_name', 'facilityname');
CITY_ALIASES.push('fips_cnty_nm', 'nearest_town', 'county_name', 'nld_id');
STATE_ALIASES.push('state', 'state_abbr');
ID_ALIASES.push('mine_id', 'nid_id', 'recordid');

function findCol(headers, aliases) {
  const lower = headers.map(h => (h || '').toLowerCase().trim().replace(/[^a-z0-9_]/g, '_'));
  for (const a of aliases) {
    const i = lower.indexOf(a);
    if (i !== -1) return headers[i];
  }
  return null;
}
function getVal(row, col) {
  if (!col) return null;
  const v = row[col];
  if (v === undefined || v === null || String(v).trim() === '' || String(v).trim() === 'NULL') return null;
  let s = String(v).trim();
  // Strip surrounding double-quotes that remain when quote:false is used for pipe-delimited files
  if (s.length >= 2 && s.startsWith('"') && s.endsWith('"')) s = s.slice(1, -1).trim();
  return s || null;
}

// ── Database pool (local geoscope DB) ─────────────────────────────────────
function createPool() {
  return new Pool({
    host: process.env.DATA_PG_HOST || process.env.PG_HOST || 'localhost',
    port: parseInt(process.env.DATA_PG_PORT || process.env.PG_PORT || '5432'),
    database: process.env.DATA_PG_DATABASE || process.env.PG_DATABASE || 'geoscope',
    user: process.env.DATA_PG_USER || process.env.PG_USER || 'postgres',
    password: process.env.DATA_PG_PASSWORD || process.env.PG_PASSWORD,
    max: 5,
    idleTimeoutMillis: 30000,
  });
}

// ── HTTP/HTTPS downloader with redirect follow + progress ─────────────────
function download(url, outFile, label = '') {
  return new Promise((resolve, reject) => {
    const dir = path.dirname(outFile);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

    const protocol = url.startsWith('https') ? https : http;
    const req = protocol.get(url, { headers: { 'User-Agent': 'GeoScope/1.0 Federal Data Importer' } }, (res) => {
      if (res.statusCode === 301 || res.statusCode === 302 || res.statusCode === 307 || res.statusCode === 308) {
        return download(res.headers.location, outFile, label).then(resolve).catch(reject);
      }
      if (res.statusCode !== 200) {
        return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
      }
      const total = parseInt(res.headers['content-length'] || '0');
      let received = 0;
      let lastLog = Date.now();

      const file = fs.createWriteStream(outFile);
      res.on('data', chunk => {
        received += chunk.length;
        if (total && Date.now() - lastLog > 3000) {
          const pct = ((received / total) * 100).toFixed(0);
          process.stdout.write(`\r  ${label}: ${pct}% (${(received/1e6).toFixed(1)}MB/${(total/1e6).toFixed(1)}MB)`);
          lastLog = Date.now();
        }
      });
      res.pipe(file);
      file.on('finish', () => { file.close(); if (total) console.log(); resolve(outFile); });
      file.on('error', reject);
    });
    req.on('error', reject);
    req.setTimeout(120000, () => { req.abort(); reject(new Error('Timeout')); });
  });
}

// ── Paginated JSON API downloader (for ECHO/ArcGIS/CMS) ───────────────────
async function downloadPaginatedJSON(baseUrl, params, outFile, label, pageSize = 5000) {
  const dir = path.dirname(outFile);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

  const allRows = [];
  let offset = 0;
  let hasMore = true;
  let page = 0;

  while (hasMore) {
    const url = `${baseUrl}?${new URLSearchParams({ ...params, offset, limit: pageSize })}`;
    const data = await fetchJSON(url);
    const rows = data.results || data.data || data.features || data.facilities || data.hits?.hits || data || [];
    if (!Array.isArray(rows) || rows.length === 0) break;
    allRows.push(...rows);
    offset += rows.length;
    page++;
    process.stdout.write(`\r  ${label}: ${allRows.length.toLocaleString()} records fetched (page ${page})`);
    if (rows.length < pageSize) { hasMore = false; }
    await new Promise(r => setTimeout(r, 300)); // polite rate limiting
  }
  console.log(`\n  ${label}: Total ${allRows.length.toLocaleString()} records`);

  fs.writeFileSync(outFile, JSON.stringify(allRows, null, 0));
  return allRows.length;
}

function fetchJSON(url) {
  return new Promise((resolve, reject) => {
    const protocol = url.startsWith('https') ? https : http;
    let raw = '';
    protocol.get(url, { headers: { 'User-Agent': 'GeoScope/1.0' } }, (res) => {
      if (res.statusCode === 301 || res.statusCode === 302) {
        return fetchJSON(res.headers.location).then(resolve).catch(reject);
      }
      res.on('data', d => raw += d);
      res.on('end', () => {
        try { resolve(JSON.parse(raw)); }
        catch(e) { reject(new Error(`JSON parse error: ${e.message} from ${url}`)); }
      });
    }).on('error', reject);
  });
}

// ── CSV Importer into environmental_sites ─────────────────────────────────
async function importCSV(csvFile, dbName, category, classCode = null, priorityTier = 'HIGH') {
  const pool = createPool();
  const client = await pool.connect();

  console.log(`  Importing: ${path.basename(csvFile)} → "${dbName}"`);

  // Check for existing records
  const { rows: existing } = await client.query(
    'SELECT COUNT(*) as c FROM environmental_sites WHERE database_name = $1', [dbName]
  );
  const existingCount = parseInt(existing[0].c);
  if (existingCount > 0) {
    console.log(`  ⚠️  ${dbName} already has ${existingCount.toLocaleString()} records — skipping import (use --force to overwrite)`);
    client.release();
    await pool.end();
    return existingCount;
  }

  // Auto-detect delimiter from first line
  const firstLine = fs.readFileSync(csvFile, { encoding: 'utf8' }).split('\n')[0] || '';
  const delimiter = firstLine.includes('|') ? '|' : ',';

  // For pipe-delimited files, disable quote processing entirely (some MSHA/federal files
  // embed literal "Y" inside already-quoted fields which breaks standard CSV parsers).
  const parseOpts = delimiter === '|'
    ? { columns: true, skip_empty_lines: true, bom: true, trim: true, relax_column_count: true, quote: false, delimiter }
    : { columns: true, skip_empty_lines: true, bom: true, trim: true, relax_column_count: true, relax_quotes: true, delimiter };

  return new Promise((resolve, reject) => {
    const stream = fs.createReadStream(csvFile);
    const parser = parse(parseOpts);
    let headers = null;
    let colMap = null;
    let batch = [];
    let totalImported = 0;
    let totalSkipped = 0;

    async function flushBatch() {
      if (batch.length === 0) return;
      const toInsert = batch.splice(0);

      const vals = [];
      const params = [];
      let p = 1;
      const seenIds = new Set();

      for (const row of toInsert) {
        const latRaw = getVal(row, colMap.latCol);
        const lonRaw = getVal(row, colMap.lonCol);
        const lat = parseFloat(latRaw);
        const lon = parseFloat(lonRaw);
        if (!latRaw || !lonRaw || isNaN(lat) || isNaN(lon) ||
            lat < 17 || lat > 72 || lon < -180 || lon > -60) {
          totalSkipped++; continue;
        }
        const sourceId = getVal(row, colMap.idCol);
        if (sourceId && seenIds.has(sourceId)) { totalSkipped++; continue; }
        if (sourceId) seenIds.add(sourceId);

        const siteName  = getVal(row, colMap.nameCol);
        const address   = getVal(row, colMap.addrCol);
        const city      = getVal(row, colMap.cityCol);
        const stateRaw  = getVal(row, colMap.stateCol);
        const state     = stateRaw ? stateRaw.substring(0, 2).toUpperCase() : null;
        const zip       = getVal(row, colMap.zipCol);
        const status    = getVal(row, colMap.statusCol);

        const knownCols = Object.values(colMap).filter(Boolean);
        const extra = {};
        for (const [k, v] of Object.entries(row)) {
          if (!knownCols.includes(k) && v !== null && v !== '') extra[k] = v;
        }
        const attrs = Object.keys(extra).length > 0 ? JSON.stringify(extra) : null;

        vals.push(`(ST_SetSRID(ST_MakePoint($${p},$${p+1}),4326),$${p+2},$${p+3},$${p+4},$${p+5},$${p+6},$${p+7},$${p+8},$${p+9},$${p+10},$${p+11},$${p+12},$${p+13})`);
        params.push(lon, lat, dbName, category, siteName, address, city, state, zip, status, sourceId, attrs, classCode, priorityTier);
        p += 14;
      }

      if (vals.length === 0) return;

      const sql = `INSERT INTO environmental_sites
        (location, database_name, category, site_name, address, city, state, zip, status, source_id, attributes, class_code, priority_tier)
        VALUES ${vals.join(',')}
        ON CONFLICT (source_id) WHERE source_id IS NOT NULL DO NOTHING`;
      try {
        const res = await client.query(sql, params);
        totalImported += res.rowCount;
        if (totalImported % 10000 < BATCH_SIZE) {
          process.stdout.write(`\r    ${totalImported.toLocaleString()} imported, ${totalSkipped.toLocaleString()} skipped`);
        }
      } catch (e) {
        console.error('\n  DB error:', e.message.substring(0, 200));
      }
    }

    parser.on('readable', async () => {
      let record;
      while ((record = parser.read()) !== null) {
        if (!headers) {
          headers = Object.keys(record);
          colMap = {
            latCol:    findCol(headers, LAT_ALIASES),
            lonCol:    findCol(headers, LON_ALIASES),
            nameCol:   findCol(headers, NAME_ALIASES),
            addrCol:   findCol(headers, ADDR_ALIASES),
            cityCol:   findCol(headers, CITY_ALIASES),
            stateCol:  findCol(headers, STATE_ALIASES),
            zipCol:    findCol(headers, ZIP_ALIASES),
            statusCol: findCol(headers, STATUS_ALIASES),
            idCol:     findCol(headers, ID_ALIASES),
          };
          if (!colMap.latCol || !colMap.lonCol) {
            parser.destroy(new Error(`No lat/lon columns found in ${path.basename(csvFile)}. Headers: ${headers.slice(0,10).join(',')}`));
            return;
          }
        }
        batch.push(record);
        if (batch.length >= BATCH_SIZE) {
          parser.pause();
          await flushBatch();
          parser.resume();
        }
      }
    });

    parser.on('end', async () => {
      await flushBatch();
      console.log(`\n  ✅ ${dbName}: ${totalImported.toLocaleString()} imported, ${totalSkipped.toLocaleString()} skipped`);
      client.release();
      await pool.end();
      resolve(totalImported);
    });

    parser.on('error', async (e) => {
      console.error(`\n  ❌ CSV parse error: ${e.message}`);
      client.release();
      await pool.end();
      reject(e);
    });

    stream.pipe(parser);
  });
}

// ── JSON importer (for ArcGIS GeoJSON/FeatureServer results) ──────────────
async function importJSON(jsonFile, dbName, category, fieldMap, classCode = null, priorityTier = 'HIGH') {
  const pool = createPool();
  const client = await pool.connect();

  console.log(`  Importing: ${path.basename(jsonFile)} → "${dbName}"`);

  const { rows: existing } = await client.query(
    'SELECT COUNT(*) as c FROM environmental_sites WHERE database_name = $1', [dbName]
  );
  if (parseInt(existing[0].c) > 0) {
    console.log(`  ⚠️  ${dbName} already has ${existing[0].c} records — skipping`);
    client.release();
    await pool.end();
    return;
  }

  const raw = JSON.parse(fs.readFileSync(jsonFile, 'utf8'));
  const records = Array.isArray(raw) ? raw : (raw.features || raw.results || raw.data || []);

  let totalImported = 0, totalSkipped = 0;

  for (let i = 0; i < records.length; i += BATCH_SIZE) {
    const batch = records.slice(i, i + BATCH_SIZE);
    const vals = [];
    const params = [];
    let p = 1;
    const seenIds = new Set();

    for (const rec of batch) {
      const props = rec.properties || rec.attributes || rec;
      const lat = parseFloat(props[fieldMap.lat] || rec.geometry?.y || rec.geometry?.coordinates?.[1]);
      const lon = parseFloat(props[fieldMap.lon] || rec.geometry?.x || rec.geometry?.coordinates?.[0]);
      if (isNaN(lat) || isNaN(lon) || lat < 17 || lat > 72 || lon < -180 || lon > -60) {
        totalSkipped++; continue;
      }
      const sourceId = props[fieldMap.id] ? String(props[fieldMap.id]) : null;
      if (sourceId && seenIds.has(sourceId)) { totalSkipped++; continue; }
      if (sourceId) seenIds.add(sourceId);

      const siteName = props[fieldMap.name] ? String(props[fieldMap.name]).trim() : null;
      const address  = props[fieldMap.address] ? String(props[fieldMap.address]).trim() : null;
      const city     = props[fieldMap.city] ? String(props[fieldMap.city]).trim() : null;
      const stateRaw = props[fieldMap.state] ? String(props[fieldMap.state]).trim() : null;
      const state    = stateRaw ? stateRaw.substring(0, 2).toUpperCase() : null;
      const zip      = props[fieldMap.zip] ? String(props[fieldMap.zip]).trim() : null;
      const status   = props[fieldMap.status] ? String(props[fieldMap.status]).trim() : null;
      const attrs    = JSON.stringify(props);

      vals.push(`(ST_SetSRID(ST_MakePoint($${p},$${p+1}),4326),$${p+2},$${p+3},$${p+4},$${p+5},$${p+6},$${p+7},$${p+8},$${p+9},$${p+10},$${p+11},$${p+12},$${p+13})`);
      params.push(lon, lat, dbName, category, siteName, address, city, state, zip, status, sourceId, attrs, classCode, priorityTier);
      p += 14;
    }

    if (vals.length === 0) continue;
    const sql = `INSERT INTO environmental_sites
      (location, database_name, category, site_name, address, city, state, zip, status, source_id, attributes, class_code, priority_tier)
      VALUES ${vals.join(',')}
      ON CONFLICT (source_id) WHERE source_id IS NOT NULL DO NOTHING`;
    try {
      const res = await client.query(sql, params);
      totalImported += res.rowCount;
      process.stdout.write(`\r    ${totalImported.toLocaleString()} imported`);
    } catch (e) {
      console.error('\n  DB error:', e.message.substring(0, 200));
    }
  }

  console.log(`\n  ✅ ${dbName}: ${totalImported.toLocaleString()} imported, ${totalSkipped.toLocaleString()} skipped`);
  client.release();
  await pool.end();
}

// ── ZIP extractor ──────────────────────────────────────────────────────────
async function extractZip(zipFile, outDir) {
  const { exec } = require('child_process');
  const { promisify } = require('util');
  const execAsync = promisify(exec);
  if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });
  // Use PowerShell Expand-Archive (available on Windows without extra deps)
  await execAsync(`powershell -NoProfile -Command "Expand-Archive -Path '${zipFile}' -DestinationPath '${outDir}' -Force"`);
  return outDir;
}

function findFileInDir(dir, ...exts) {
  const files = fs.readdirSync(dir);
  for (const ext of exts) {
    const found = files.find(f => f.toLowerCase().endsWith(ext));
    if (found) return path.join(dir, found);
  }
  for (const f of files) {
    const sub = path.join(dir, f);
    if (fs.statSync(sub).isDirectory()) {
      const r = findFileInDir(sub, ...exts);
      if (r) return r;
    }
  }
  return null;
}

// ── ArcGIS FeatureServer paginated downloader ──────────────────────────────
async function downloadArcGIS(serviceUrl, outFile, label, pageSize = 1000) {
  const dir = path.dirname(outFile);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

  const allFeatures = [];
  let offset = 0;
  let hasMore = true;

  while (hasMore) {
    const url = `${serviceUrl}/query?f=json&where=1%3D1&outFields=*&resultOffset=${offset}&resultRecordCount=${pageSize}&geometryType=esriGeometryPoint&outSR=4326`;
    const data = await fetchJSON(url);

    if (data.error) throw new Error(`ArcGIS error: ${data.error.message}`);
    const features = data.features || [];
    if (features.length === 0) { hasMore = false; break; }

    allFeatures.push(...features);
    offset += features.length;
    process.stdout.write(`\r  ${label}: ${allFeatures.length.toLocaleString()} features`);

    if (features.length < pageSize) hasMore = false;
    await new Promise(r => setTimeout(r, 200));
  }

  console.log(`\n  ${label}: Total ${allFeatures.length.toLocaleString()} features`);
  fs.writeFileSync(outFile, JSON.stringify(allFeatures));
  return allFeatures.length;
}

// ═════════════════════════════════════════════════════════════════════════
// DATASET DEFINITIONS
// ═════════════════════════════════════════════════════════════════════════
const DATASETS = {

  MSHA_MINES: {
    name: 'MSHA MINES',
    label: 'Mine Safety & Health Administration — All US Mines',
    category: 'mines',
    classCode: 'MINES',
    priority: 'HIGH',
    estimated: '~85,000 mines (all 50 states)',
    async download(dir) {
      const zipFile = path.join(dir, 'msha_mines.zip');
      const csvFile = path.join(dir, 'Mines.csv');
      if (fs.existsSync(csvFile)) { console.log('  (already downloaded)'); return csvFile; }
      console.log('  Downloading MSHA Mines from arlweb.msha.gov...');
      await download('https://arlweb.msha.gov/OpenGovernmentData/DataSets/Mines.zip', zipFile, 'MSHA Mines ZIP');
      await extractZip(zipFile, dir);
      return findFileInDir(dir, '.csv', '.txt') || csvFile;
    },
    async import(csvFile) {
      return importCSV(csvFile, 'MSHA MINES', 'mines', 'MINES', 'HIGH');
    }
  },

  NID_DAMS: {
    name: 'NID DAMS',
    label: 'National Inventory of Dams — US Army Corps of Engineers',
    category: 'infrastructure',
    classCode: 'DAMS',
    priority: 'HIGH',
    estimated: '~90,000 dams (all 50 states)',
    async download(dir) {
      const csvFile = path.join(dir, 'nid_dams.csv');
      if (fs.existsSync(csvFile)) { console.log('  (already downloaded)'); return csvFile; }
      console.log('  Downloading NID Dams from USACE API...');
      await download('https://nid.sec.usace.army.mil/api/nation/csv', csvFile, 'NID Dams CSV');
      return csvFile;
    },
    async import(csvFile) {
      return importCSV(csvFile, 'NID DAMS', 'infrastructure', 'DAMS', 'HIGH');
    }
  },

  NPL_SUPERFUND: {
    name: 'NPL SUPERFUND',
    label: 'EPA Superfund National Priorities List',
    category: 'contamination',
    classCode: 'NPL',
    priority: 'CRITICAL',
    estimated: '~1,300 active + ~40,000 archived sites',
    async download(dir) {
      const csvFile = path.join(dir, 'npl_sites.csv');
      if (fs.existsSync(csvFile)) { console.log('  (already downloaded)'); return csvFile; }
      console.log('  Downloading NPL Superfund sites from EPA SEMS...');
      // EPA SEMS public download
      await download(
        'https://semspub.epa.gov/work/HQ/405006.csv',
        csvFile, 'NPL Sites CSV'
      );
      if (!fs.existsSync(csvFile) || fs.statSync(csvFile).size < 1000) {
        // Fallback: EPA ECHO via FRS for Superfund program
        console.log('  Primary source failed, trying ECHO fallback...');
        const jsonFile = path.join(dir, 'npl_echo.json');
        await downloadPaginatedJSON(
          'https://echo.epa.gov/rest/services/rpt/rptfrs/query',
          { p_ms: 'SU', output: 'JSON', qcolumns: '1,2,3,4,5,8,9,10,11,12,13,14,15' },
          jsonFile, 'NPL ECHO', 1000
        );
        return jsonFile;
      }
      return csvFile;
    },
    async import(file) {
      if (file.endsWith('.json')) {
        return importJSON(file, 'NPL SUPERFUND', 'contamination', {
          lat: 'FacLat', lon: 'FacLong', name: 'FacName', address: 'FacStreet',
          city: 'FacCity', state: 'FacState', zip: 'FacZip', id: 'RegistryID', status: 'FacActiveFlag'
        }, 'NPL', 'CRITICAL');
      }
      return importCSV(file, 'NPL SUPERFUND', 'contamination', 'NPL', 'CRITICAL');
    }
  },

  RMP: {
    name: 'RMP',
    label: 'EPA Risk Management Plans — Chemical Accident Prevention',
    category: 'regulatory',
    classCode: 'RMP',
    priority: 'HIGH',
    estimated: '~12,000 chemical facilities (all 50 states)',
    async download(dir) {
      const jsonFile = path.join(dir, 'rmp_facilities.json');
      if (fs.existsSync(jsonFile)) { console.log('  (already downloaded)'); return jsonFile; }
      console.log('  Downloading RMP facilities via EPA ECHO API...');
      await downloadPaginatedJSON(
        'https://echo.epa.gov/rest/services/rpt/rptfrs/query',
        { p_ms: 'RM', output: 'JSON', qcolumns: '1,2,3,4,5,8,9,10,11,12,13,14,15' },
        jsonFile, 'RMP ECHO', 1000
      );
      return jsonFile;
    },
    async import(file) {
      return importJSON(file, 'RMP', 'regulatory', {
        lat: 'FacLat', lon: 'FacLong', name: 'FacName', address: 'FacStreet',
        city: 'FacCity', state: 'FacState', zip: 'FacZip', id: 'RegistryID', status: 'FacActiveFlag'
      }, 'RMP', 'HIGH');
    }
  },

  UST: {
    name: 'UST',
    label: 'EPA Underground Storage Tanks — OUST Program',
    category: 'contamination',
    classCode: 'UST',
    priority: 'CRITICAL',
    estimated: '~600,000 registered tanks (all 50 states)',
    async download(dir) {
      const jsonFile = path.join(dir, 'ust_facilities.json');
      if (fs.existsSync(jsonFile)) { console.log('  (already downloaded)'); return jsonFile; }
      console.log('  Downloading UST facilities via EPA ECHO API...');
      await downloadPaginatedJSON(
        'https://echo.epa.gov/rest/services/rpt/rptfrs/query',
        { p_ms: 'US', output: 'JSON', qcolumns: '1,2,3,4,5,8,9,10,11,12,13,14,15' },
        jsonFile, 'UST ECHO', 2000
      );
      return jsonFile;
    },
    async import(file) {
      return importJSON(file, 'UST', 'contamination', {
        lat: 'FacLat', lon: 'FacLong', name: 'FacName', address: 'FacStreet',
        city: 'FacCity', state: 'FacState', zip: 'FacZip', id: 'RegistryID', status: 'FacActiveFlag'
      }, 'UST', 'CRITICAL');
    }
  },

  BROWNFIELDS: {
    name: 'BROWNFIELDS',
    label: 'EPA Brownfields / ACRES Cleanup Sites',
    category: 'contamination',
    classCode: 'BRWN',
    priority: 'HIGH',
    estimated: '~450,000 brownfield/cleanup sites (all 50 states)',
    async download(dir) {
      const jsonFile = path.join(dir, 'brownfields.json');
      if (fs.existsSync(jsonFile)) { console.log('  (already downloaded)'); return jsonFile; }
      console.log('  Downloading Brownfields via EPA ECHO API...');
      await downloadPaginatedJSON(
        'https://echo.epa.gov/rest/services/rpt/rptfrs/query',
        { p_ms: 'BF', output: 'JSON', qcolumns: '1,2,3,4,5,8,9,10,11,12,13,14,15' },
        jsonFile, 'Brownfields ECHO', 2000
      );
      return jsonFile;
    },
    async import(file) {
      return importJSON(file, 'BROWNFIELDS', 'contamination', {
        lat: 'FacLat', lon: 'FacLong', name: 'FacName', address: 'FacStreet',
        city: 'FacCity', state: 'FacState', zip: 'FacZip', id: 'RegistryID', status: 'FacActiveFlag'
      }, 'BRWN', 'HIGH');
    }
  },

  HOSPITALS: {
    name: 'HOSPITALS',
    label: 'CMS Hospital General Information — All US Hospitals',
    category: 'receptors',
    classCode: 'HOSP',
    priority: 'HIGH',
    estimated: '~8,000 hospitals (all 50 states)',
    async download(dir) {
      const csvFile = path.join(dir, 'cms_hospitals.csv');
      if (fs.existsSync(csvFile)) { console.log('  (already downloaded)'); return csvFile; }
      console.log('  Downloading CMS Hospitals from data.cms.gov...');
      await download(
        'https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0?results_format=csv&limit=20000',
        csvFile, 'CMS Hospitals CSV'
      );
      return csvFile;
    },
    async import(csvFile) {
      return importCSV(csvFile, 'HOSPITALS', 'receptors', 'HOSP', 'HIGH');
    }
  },

  NURSING_HOMES: {
    name: 'NURSING HOMES',
    label: 'CMS Nursing Home Care Facilities',
    category: 'receptors',
    classCode: 'NRSH',
    priority: 'HIGH',
    estimated: '~15,000 nursing homes (all 50 states)',
    async download(dir) {
      const csvFile = path.join(dir, 'cms_nursing_homes.csv');
      if (fs.existsSync(csvFile)) { console.log('  (already downloaded)'); return csvFile; }
      console.log('  Downloading CMS Nursing Homes from data.cms.gov...');
      await download(
        'https://data.cms.gov/provider-data/api/1/datastore/query/4pq5-n9py/0?results_format=csv&limit=30000',
        csvFile, 'CMS Nursing Homes CSV'
      );
      return csvFile;
    },
    async import(csvFile) {
      return importCSV(csvFile, 'NURSING HOMES', 'receptors', 'NRSH', 'HIGH');
    }
  },

  POWER_PLANTS: {
    name: 'POWER PLANTS',
    label: 'HIFLD Power Plants — Electric Generation Facilities',
    category: 'infrastructure',
    classCode: 'PWRP',
    priority: 'HIGH',
    estimated: '~10,000 power plants (all 50 states)',
    async download(dir) {
      const jsonFile = path.join(dir, 'power_plants.json');
      if (fs.existsSync(jsonFile)) { console.log('  (already downloaded)'); return jsonFile; }
      console.log('  Downloading Power Plants from HIFLD ArcGIS...');
      await downloadArcGIS(
        'https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Power_Plants/FeatureServer/0',
        jsonFile, 'Power Plants', 1000
      );
      return jsonFile;
    },
    async import(file) {
      return importJSON(file, 'POWER PLANTS', 'infrastructure', {
        lat: 'Latitude', lon: 'Longitude', name: 'Plant_Name', address: 'Street_Add',
        city: 'City', state: 'State', zip: 'Zip', id: 'EIA_PtID', status: 'Status'
      }, 'PWRP', 'HIGH');
    }
  },

  AIRPORTS: {
    name: 'AIRPORTS',
    label: 'HIFLD Airports — FAA Public Use Airports',
    category: 'infrastructure',
    classCode: 'ARPT',
    priority: 'MEDIUM',
    estimated: '~20,000 airports (all 50 states)',
    async download(dir) {
      const jsonFile = path.join(dir, 'airports.json');
      if (fs.existsSync(jsonFile)) { console.log('  (already downloaded)'); return jsonFile; }
      console.log('  Downloading Airports from HIFLD ArcGIS...');
      await downloadArcGIS(
        'https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Airports_-_All/FeatureServer/0',
        jsonFile, 'Airports', 1000
      );
      return jsonFile;
    },
    async import(file) {
      return importJSON(file, 'AIRPORTS', 'infrastructure', {
        lat: 'Y', lon: 'X', name: 'NAME', address: 'ADDRESS',
        city: 'CITY', state: 'STATE', zip: 'ZIP', id: 'IDENT', status: 'STATUS'
      }, 'ARPT', 'MEDIUM');
    }
  },

  NUCLEAR: {
    name: 'NUCLEAR',
    label: 'NRC Nuclear Reactor & Materials Licensees',
    category: 'regulatory',
    classCode: 'NUC',
    priority: 'HIGH',
    estimated: '~5,000 NRC licensed facilities',
    async download(dir) {
      const jsonFile = path.join(dir, 'nuclear.json');
      if (fs.existsSync(jsonFile)) { console.log('  (already downloaded)'); return jsonFile; }
      console.log('  Downloading Nuclear facilities via EPA ECHO API...');
      await downloadPaginatedJSON(
        'https://echo.epa.gov/rest/services/rpt/rptfrs/query',
        { p_ms: 'NR', output: 'JSON', qcolumns: '1,2,3,4,5,8,9,10,11,12,13,14,15' },
        jsonFile, 'Nuclear ECHO', 1000
      );
      return jsonFile;
    },
    async import(file) {
      return importJSON(file, 'NUCLEAR', 'regulatory', {
        lat: 'FacLat', lon: 'FacLong', name: 'FacName', address: 'FacStreet',
        city: 'FacCity', state: 'FacState', zip: 'FacZip', id: 'RegistryID', status: 'FacActiveFlag'
      }, 'NUC', 'HIGH');
    }
  },

  LANDFILLS: {
    name: 'LANDFILLS',
    label: 'EPA LMOP Landfill Methane Outreach Program',
    category: 'landfills',
    classCode: 'LNDF',
    priority: 'HIGH',
    estimated: '~2,600 landfill gas sites (all 50 states)',
    async download(dir) {
      const csvFile = path.join(dir, 'lmop_landfills.csv');
      if (fs.existsSync(csvFile)) { console.log('  (already downloaded)'); return csvFile; }
      console.log('  Downloading LMOP Landfills from EPA...');
      await download(
        'https://www.epa.gov/sites/default/files/2021-06/lmopdata.csv',
        csvFile, 'LMOP CSV'
      );
      return csvFile;
    },
    async import(csvFile) {
      return importCSV(csvFile, 'LANDFILLS', 'landfills', 'LNDF', 'HIGH');
    }
  },

  PFAS_SITES: {
    name: 'PFAS SITES',
    label: 'EPA PFAS Contamination — Known & Suspected Sites',
    category: 'pfas',
    classCode: 'PFAS',
    priority: 'CRITICAL',
    estimated: '~5,000+ known PFAS-impacted sites',
    async download(dir) {
      const jsonFile = path.join(dir, 'pfas_sites.json');
      if (fs.existsSync(jsonFile)) { console.log('  (already downloaded)'); return jsonFile; }
      console.log('  Downloading PFAS sites via EPA ECHO API (PFAS program)...');
      await downloadPaginatedJSON(
        'https://echo.epa.gov/rest/services/rpt/rptfrs/query',
        { p_ms: 'PF', output: 'JSON', qcolumns: '1,2,3,4,5,8,9,10,11,12,13,14,15' },
        jsonFile, 'PFAS ECHO', 1000
      );
      return jsonFile;
    },
    async import(file) {
      return importJSON(file, 'PFAS SITES', 'pfas', {
        lat: 'FacLat', lon: 'FacLong', name: 'FacName', address: 'FacStreet',
        city: 'FacCity', state: 'FacState', zip: 'FacZip', id: 'RegistryID', status: 'FacActiveFlag'
      }, 'PFAS', 'CRITICAL');
    }
  },

  COLLEGES: {
    name: 'COLLEGES',
    label: 'NCES Colleges & Universities (IPEDS)',
    category: 'receptors',
    classCode: 'COLL',
    priority: 'MEDIUM',
    estimated: '~7,000 degree-granting institutions',
    async download(dir) {
      const csvFile = path.join(dir, 'colleges.csv');
      if (fs.existsSync(csvFile)) { console.log('  (already downloaded)'); return csvFile; }
      console.log('  Downloading Colleges from HIFLD ArcGIS...');
      // HIFLD Colleges & Universities layer
      const jsonFile = path.join(dir, 'colleges.json');
      await downloadArcGIS(
        'https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Colleges_and_Universities/FeatureServer/0',
        jsonFile, 'Colleges', 2000
      );
      return jsonFile;
    },
    async import(file) {
      return importJSON(file, 'COLLEGES', 'receptors', {
        lat: 'LATITUDE', lon: 'LONGITUDE', name: 'NAME', address: 'ADDRESS',
        city: 'CITY', state: 'STATE', zip: 'ZIP', id: 'IPEDSID', status: 'STATUS'
      }, 'COLL', 'MEDIUM');
    }
  },

  PIPELINES: {
    name: 'PIPELINES',
    label: 'PHMSA Gas & Liquid Pipeline Operators',
    category: 'infrastructure',
    classCode: 'PIPE',
    priority: 'HIGH',
    estimated: '~1,500+ regulated pipeline operators',
    async download(dir) {
      const jsonFile = path.join(dir, 'pipelines.json');
      if (fs.existsSync(jsonFile)) { console.log('  (already downloaded)'); return jsonFile; }
      console.log('  Downloading Pipelines via EPA ECHO API...');
      await downloadPaginatedJSON(
        'https://echo.epa.gov/rest/services/rpt/rptfrs/query',
        { p_ms: 'PL', output: 'JSON', qcolumns: '1,2,3,4,5,8,9,10,11,12,13,14,15' },
        jsonFile, 'Pipelines ECHO', 1000
      );
      return jsonFile;
    },
    async import(file) {
      return importJSON(file, 'PIPELINES', 'infrastructure', {
        lat: 'FacLat', lon: 'FacLong', name: 'FacName', address: 'FacStreet',
        city: 'FacCity', state: 'FacState', zip: 'FacZip', id: 'RegistryID', status: 'FacActiveFlag'
      }, 'PIPE', 'HIGH');
    }
  },

};

// ═════════════════════════════════════════════════════════════════════════
// MAIN
// ═════════════════════════════════════════════════════════════════════════
async function main() {
  const args = process.argv.slice(2);

  if (args.includes('--list')) {
    console.log('\n📋 Available datasets:\n');
    for (const [key, ds] of Object.entries(DATASETS)) {
      console.log(`  ${key.padEnd(20)} ${ds.label}`);
      console.log(`  ${''.padEnd(20)} Estimated: ${ds.estimated} | Priority: ${ds.priority}\n`);
    }
    return;
  }

  const importOnly = args.includes('--import-only');
  const targetArgs = args.filter(a => !a.startsWith('--'));
  const targets = targetArgs.length > 0
    ? targetArgs.map(t => t.toUpperCase()).filter(t => t in DATASETS)
    : Object.keys(DATASETS);

  if (targets.length === 0) {
    console.error('❌ No matching datasets found. Use --list to see available datasets.');
    process.exit(1);
  }

  console.log(`\n🌎 GeoScope 50-State Coverage Downloader`);
  console.log(`   Datasets to process: ${targets.join(', ')}\n`);

  const results = { success: [], failed: [], skipped: [] };

  for (const key of targets) {
    const ds = DATASETS[key];
    const dsDir = path.join(DOWNLOADS_DIR, key);
    if (!fs.existsSync(dsDir)) fs.mkdirSync(dsDir, { recursive: true });

    console.log(`\n${'='.repeat(60)}`);
    console.log(`📦 [${key}] ${ds.label}`);
    console.log(`   Category: ${ds.category} | Priority: ${ds.priority}`);
    console.log(`   Expected: ${ds.estimated}`);

    try {
      let dataFile;
      if (!importOnly) {
        dataFile = await ds.download(dsDir);
        console.log(`  ✅ Downloaded: ${dataFile}`);
      } else {
        // Find existing file in dir
        const files = fs.readdirSync(dsDir);
        const f = files.find(f => f.endsWith('.csv') || f.endsWith('.json') || f.endsWith('.txt'));
        if (!f) { console.log(`  ⚠️  No file found in ${dsDir} — skipping`); results.skipped.push(key); continue; }
        dataFile = path.join(dsDir, f);
      }

      await ds.import(dataFile);
      results.success.push(key);
    } catch (e) {
      console.error(`  ❌ ${key} FAILED: ${e.message}`);
      results.failed.push({ key, error: e.message });
    }
  }

  console.log(`\n${'='.repeat(60)}`);
  console.log(`\n🎯 SUMMARY:`);
  console.log(`   ✅ Success  (${results.success.length}): ${results.success.join(', ') || 'none'}`);
  console.log(`   ❌ Failed   (${results.failed.length}): ${results.failed.map(f => `${f.key}(${f.error.substring(0,40)})`).join(', ') || 'none'}`);
  console.log(`   ⏭️  Skipped  (${results.skipped.length}): ${results.skipped.join(', ') || 'none'}`);
  console.log(`\n   Run 'node scripts/verify-data-store.js' to see updated totals.\n`);
}

main().catch(e => { console.error('Fatal error:', e); process.exit(1); });
