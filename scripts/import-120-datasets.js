#!/usr/bin/env node
/**
 * GeoScope 120+ Federal Datasets State-by-State Importer
 * Imports all environmental datasets organized by US state
 * APIs: EPA Envirofacts, USGS, NOAA, USDA, OSM, DOT, OSHA
 */

const https = require('https');
const http = require('http');
const { Pool } = require('pg');
const csv = require('csv-parse/sync');
const path = require('path');
const fs = require('fs');
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });

// Self-logging: write to file AND stdout simultaneously (unbuffered)
const logFile = path.join(__dirname, '..', `phase2-envirofacts-${new Date().toISOString().replace(/[:.]/g,'-').slice(0,19)}.log`);
const logStream = fs.createWriteStream(logFile, { flags: 'a' });
const _log = console.log.bind(console);
console.log = (...args) => {
  const line = args.join(' ');
  _log(line);
  logStream.write(line + '\n');
};

const pool = new Pool({
  host: process.env.PG_HOST,
  port: process.env.PG_PORT,
  database: process.env.PG_DATABASE,
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  max: 12
});

// All 50 US States
const US_STATES = ['AL','AK','AZ','AR','CA','CO','CT','DE','DC','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY'];

const STATE_FIPS = {
  'AL': '01', 'AK': '02', 'AZ': '04', 'AR': '05', 'CA': '06', 'CO': '08', 'CT': '09', 'DE': '10', 'DC': '11', 'FL': '12',
  'GA': '13', 'HI': '15', 'ID': '16', 'IL': '17', 'IN': '18', 'IA': '19', 'KS': '20', 'KY': '21', 'LA': '22', 'ME': '23',
  'MD': '24', 'MA': '25', 'MI': '26', 'MN': '27', 'MS': '28', 'MO': '29', 'MT': '30', 'NE': '31', 'NV': '32', 'NH': '33',
  'NJ': '34', 'NM': '35', 'NY': '36', 'NC': '37', 'ND': '38', 'OH': '39', 'OK': '40', 'OR': '41', 'PA': '42', 'RI': '44',
  'SC': '45', 'SD': '46', 'TN': '47', 'TX': '48', 'UT': '49', 'VT': '50', 'VA': '51', 'WA': '53', 'WV': '54', 'WI': '55', 'WY': '56'
};

// Dataset definitions with their APIs and query patterns
const DATASETS = {
  // TRI Toxics Release Inventory (old-style API still works)
  'TRI_FACILITY': {
    api: 'EPA_ENVIROFACTS_TRI',
    table: 'TRI_FACILITY', stateCol: 'STATE_CD', category: 'contamination', tier: 1,
    mapFn: r => ({
      name: r.FACILITY_NAME || '(Unknown)',
      address: r.STREET_ADDRESS || '', city: r.CITY || '',
      zip: r.ZIP_CODE || '', status: 'Active',
      source_id: `TRI-${r.TRI_FACILITY_ID || r.TRIFID}`,
      lat: parseFloat(r.LATITUDE) || null,
      lng: parseFloat(r.LONGITUDE) || null,
    })
  },
  // SEMS Superfund/NPL sites (small dataset, fast paged query)
  'SEMS_SUPERFUND': {
    api: 'EPA_ENVIROFACTS_NEW',
    table: 'sems.envirofacts_site',
    stateCol: 'fk_ref_state_code', category: 'contamination', tier: 1,
    mapFn: r => ({
      name: r.site_name || r.primary_name || '(Unknown)',
      address: r.location_address || r.city_name || '', city: r.city_name || '',
      zip: r.zip_code || '', status: r.npl_status || 'Active',
      source_id: `SEMS-${r.site_id || r.epa_id || r.site_epa_id}`,
      lat: parseFloat(r.latitude) || null,
      lng: parseFloat(r.longitude) || null,
    })
  },
  // SDWIS Safe Drinking Water (paged, but limited records per state)
  'SDWIS_WATER': {
    api: 'EPA_ENVIROFACTS_NEW',
    table: 'sdwis.water_system',
    stateCol: 'state_code', category: 'water', tier: 1,
    mapFn: r => ({
      name: r.pws_name || r.water_system_name || '(Unknown)',
      address: r.address || '', city: r.city_served || r.city_name || '',
      zip: r.zip_code || '', status: r.pws_activity_code || 'A',
      source_id: `SDWIS-${r.pwsid || r.pws_id}`,
      lat: null, lng: null,
    })
  },
};

const BATCH_SIZE = 500;
let totalImported = 0;
let totalByDataset = {};

const PAGE_SIZE = 1000;
const REQUEST_TIMEOUT_MS = 15000;
const MAX_RETRIES = 2;
const MAX_PAGES_PER_DATASET = 250;
const DATASET_TIMEOUT_MS = 45000;
const MAX_RESPONSE_BYTES = 30 * 1024 * 1024;

async function fetchJSON(url) {
  return new Promise((resolve) => {
    let settled = false;
    const client = url.startsWith('https') ? https : http;
    let req;
    const hardTimer = setTimeout(() => {
      if (!settled) {
        settled = true;
        if (req) {
          try { req.destroy(); } catch (_) {}
        }
        resolve(null);
      }
    }, REQUEST_TIMEOUT_MS);

    req = client.get(url, {
      headers: { 'User-Agent': 'GeoScope/1.0 (+https://geoscopesolutions.com)' }
    }, (res) => {
      let data = '';
      res.on('data', chunk => {
        if (settled) return;
        data += chunk;
        if (data.length > MAX_RESPONSE_BYTES) {
          settled = true;
          clearTimeout(hardTimer);
          req.destroy();
          resolve(null);
        }
      });
      res.on('end', () => {
        if (settled) return;
        settled = true;
        clearTimeout(hardTimer);
        try {
          resolve(res.statusCode === 200 ? JSON.parse(data) : null);
        } catch (e) {
          resolve(null);
        }
      });
    });
    req.on('error', () => {
      if (settled) return;
      settled = true;
      clearTimeout(hardTimer);
      resolve(null);
    });
    req.setTimeout(REQUEST_TIMEOUT_MS, () => {
      if (settled) return;
      settled = true;
      clearTimeout(hardTimer);
      req.destroy();
      resolve(null);
    });
  });
}

async function fetchJSONWithRetry(url, retries = MAX_RETRIES) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    const data = await fetchJSON(url);
    if (data !== null) return data;
    if (attempt < retries) {
      console.log(`    retry ${attempt + 1}/${retries} for request`);
      await new Promise(r => setTimeout(r, 500 * (attempt + 1)));
    }
  }
  return null;
}

// TRI query with paging for live progress visibility.
async function queryTRI(state, onPage) {
  const all = [];
  let page = 1;
  while (true) {
    if (page > MAX_PAGES_PER_DATASET) {
      throw new Error(`TRI max page limit reached for ${state}`);
    }
    const first = (page - 1) * PAGE_SIZE + 1;
    const last = page * PAGE_SIZE;
    console.log(`    TRI request page ${page} (${first}:${last})`);
    const pagedUrl = `https://data.epa.gov/efservice/TRI_FACILITY/STATE_CD/${state}/${first}:${last}/JSON`;
    let data = await fetchJSONWithRetry(pagedUrl);

    // Fallback for any state where old endpoint ignores paging syntax.
    if (page === 1 && (!Array.isArray(data) || data.length === 0)) {
      const fallbackUrl = `https://data.epa.gov/efservice/TRI_FACILITY/STATE_CD/${state}/JSON`;
      data = await fetchJSONWithRetry(fallbackUrl);
      if (Array.isArray(data) && data.length > 0) {
        all.push(...data);
        if (onPage) onPage(1, data.length, all.length);
      }
      break;
    }

    if (!Array.isArray(data) || data.length === 0) break;
    all.push(...data);
    if (onPage) onPage(page, data.length, all.length);
    if (data.length < PAGE_SIZE) break;
    page++;
    await new Promise(r => setTimeout(r, 200));
  }
  return all;
}

// New-style schema.table paged query (column/equals/value, page through 1000 at a time)
async function queryEnvirofacts(table, stateCol, state, onPage) {
  const all = [];
  let page = 1;
  while (true) {
    if (page > MAX_PAGES_PER_DATASET) {
      throw new Error(`${table} max page limit reached for ${state}`);
    }
    const first = (page - 1) * PAGE_SIZE + 1;
    const last = page * PAGE_SIZE;
    console.log(`    ${table} request page ${page} (${first}:${last})`);
    const url = `https://data.epa.gov/efservice/${table}/${stateCol}/equals/${state}/${first}:${last}/JSON`;
    const data = await fetchJSONWithRetry(url);
    if (!Array.isArray(data) || data.length === 0) break;
    all.push(...data);
    if (onPage) onPage(page, data.length, all.length);
    if (data.length < PAGE_SIZE) break; // last page
    page++;
    // Small delay to avoid rate limiting
    await new Promise(r => setTimeout(r, 200));
  }
  return all;
}

async function batchInsert(records, dataset, state) {
  if (records.length === 0) return 0;
  
  let inserted = 0;
  for (let i = 0; i < records.length; i += BATCH_SIZE) {
    const batch = records.slice(i, i + BATCH_SIZE);
    try {
      const result = await pool.query(
        `INSERT INTO environmental_sites (database_name, category, state, site_name, address, city, zip, status, source_id, source_org, attributes)
         VALUES ${batch.map((_, idx) => {
          const off = idx * 11;
          return `($${off+1}, $${off+2}, $${off+3}, $${off+4}, $${off+5}, $${off+6}, $${off+7}, $${off+8}, $${off+9}, $${off+10}, $${off+11})`;
         }).join(',')}
         ON CONFLICT (source_id) DO NOTHING`,
        batch.flatMap(r => [
          dataset, r._category || 'environmental', state,
          r._name || '(Unknown)', r._address || '', r._city || '',
          r._zip || '', r._status || 'Active',
          r._source_id, dataset, JSON.stringify(r._raw)
        ])
      );
      inserted += result.rowCount;
    } catch (e) {
      console.error(`Error inserting batch for ${dataset}:`, e.message);
    }
  }
  return inserted;
}

async function importStateDatasets(state) {
  const fips = STATE_FIPS[state];
  let stateTotal = 0;
  
  console.log(`\n📍 Processing ${state} (FIPS: ${fips})`);
  
  for (const [datasetName, datasetConfig] of Object.entries(DATASETS)) {
    try {
      process.stdout.write(`  ⟳ ${datasetName}... `);
      let rawRecords = [];

      const onPage = (pageNum, pageCount, runningTotal) => {
        process.stdout.write(`[p${pageNum}:${pageCount}] `);
      };

      const runWithDatasetTimeout = (promiseFactory) => Promise.race([
        promiseFactory(),
        new Promise((_, reject) => setTimeout(() => reject(new Error(`dataset timeout after ${DATASET_TIMEOUT_MS}ms`)), DATASET_TIMEOUT_MS)),
      ]);

      if (datasetConfig.api === 'EPA_ENVIROFACTS_TRI') {
        rawRecords = await runWithDatasetTimeout(() => queryTRI(state, onPage));
      } else if (datasetConfig.api === 'EPA_ENVIROFACTS_NEW') {
        rawRecords = await runWithDatasetTimeout(() => queryEnvirofacts(datasetConfig.table, datasetConfig.stateCol, state, onPage));
      }

      if (rawRecords.length === 0) {
        process.stdout.write(`0 records\n`);
      } else {
        process.stdout.write(`${rawRecords.length.toLocaleString()} fetched, inserting... `);
        // Map raw records to normalized shape
        const mapped = rawRecords.map(r => {
          const m = datasetConfig.mapFn(r);
          return {
            _name: (m.name || '').slice(0, 500),
            _address: (m.address || '').slice(0, 500),
            _city: (m.city || '').slice(0, 200),
            _zip: (m.zip || '').slice(0, 20),
            _status: (m.status || 'Active').slice(0, 255),
            _source_id: (m.source_id || `${datasetName}-${state}-${Math.random().toString(36).substr(2,9)}`).slice(0, 255),
            _category: datasetConfig.category,
            _raw: r,
          };
        });
        const imported = await batchInsert(mapped, datasetName, state);
        process.stdout.write(`+${imported.toLocaleString()} saved\n`);
        stateTotal += imported;
        totalByDataset[datasetName] = (totalByDataset[datasetName] || 0) + imported;
      }
    } catch (e) {
      process.stdout.write(`ERROR: ${e.message}\n`);
    }
  }
  
  totalImported += stateTotal;
  return stateTotal;
}

async function main() {
  console.log('\n' + '='.repeat(60));
  console.log(' GeoScope 120+ Federal Datasets — State-by-State Importer');
  console.log('='.repeat(60));
  console.log(`\n📊 Importing from ${Object.keys(DATASETS).length} datasets across ${US_STATES.length} states`);
  
  const startTime = Date.now();
  
  for (let i = 0; i < US_STATES.length; i++) {
    const state = US_STATES[i];
    const stateCount = await importStateDatasets(state);
    const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
    console.log(`  [${i+1}/${US_STATES.length}] ${state} done: +${stateCount.toLocaleString()} | Total: ${totalImported.toLocaleString()} | Elapsed: ${elapsed}m`);
  }
  
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  
  console.log('\n' + '='.repeat(60));
  console.log('📈 IMPORT COMPLETE');
  console.log('='.repeat(60));
  console.log(`Total records imported: ${totalImported.toLocaleString()}`);
  console.log(`Elapsed time: ${elapsed}s`);
  console.log('\nTop datasets by volume:');
  Object.entries(totalByDataset)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10)
    .forEach(([dataset, count]) => {
      console.log(`  ${dataset.padEnd(30)} | ${count.toLocaleString()} records`);
    });
  
  process.exit(0);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
