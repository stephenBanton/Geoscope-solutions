#!/usr/bin/env node
/**
 * =============================================================================
 * GeoScope Mega-Install — Parallel Dataset Downloader & Importer
 * =============================================================================
 * Runs all federal dataset downloads in parallel (default: 4 concurrent) to
 * maximise throughput.  Covers ECHO/FRS/RCRA/NPDES/Air/SDWA/TRI/SEMS + new
 * sources: PFAS, GHG, MSHA Mines, CMS Nursing-Homes & Hospitals, RMP, HMIRS,
 * Brownfields, EJScreen, OpenFEMA.
 *
 * Usage:
 *   node scripts/mega-install.js                         # all sources, 4 workers
 *   node scripts/mega-install.js --workers 6             # 6 parallel workers
 *   node scripts/mega-install.js --only echo,frs,rcra    # specific sources
 *   node scripts/mega-install.js --skip-existing         # skip DBs with >1000 rows
 *   node scripts/mega-install.js --dry-run               # count only, no inserts
 *   node scripts/mega-install.js --normalize-ust         # normalise UST/LUST state+city
 *   node scripts/mega-install.js --list                  # list all source keys and exit
 * =============================================================================
 */

require('dotenv').config();

const { spawn }      = require('child_process');
const path           = require('path');
const fs             = require('fs');
const https          = require('https');
const http           = require('http');
const { Pool }       = require('pg');
const { parse: csvParse } = require('csv-parse');

let unzipper;
try { unzipper = require('unzipper'); } catch (e) { /* optional */ }

// ─── DB Pool ─────────────────────────────────────────────────────────────────
const pool = new Pool({
  host:     process.env.PG_HOST     || 'localhost',
  port:     parseInt(process.env.PG_PORT || '5432'),
  database: process.env.PG_DATABASE || 'geoscope',
  user:     process.env.PG_USER     || 'postgres',
  password: process.env.PG_PASSWORD,
  max: 12
});

// ─── Directories ─────────────────────────────────────────────────────────────
const DOWNLOAD_DIR = path.join(__dirname, '..', 'downloads', 'mega');
fs.mkdirSync(DOWNLOAD_DIR, { recursive: true });

// ─── CLI args ─────────────────────────────────────────────────────────────────
const argv = process.argv.slice(2);
const flag  = (f) => argv.includes(f);
const argVal = (f) => { const a = argv.find(x => x.startsWith(f + '=')); return a ? a.split('=').slice(1).join('=') : null; };

const WORKERS     = parseInt(argVal('--workers') || '4');
const DRY_RUN     = flag('--dry-run');
const SKIP_LOADED = flag('--skip-existing');
const ONLY_KEYS   = argVal('--only') ? argVal('--only').split(',').map(s => s.trim()) : null;
const NORMALIZE   = flag('--normalize-ust');
const LIST_ONLY   = flag('--list');
const BATCH_SIZE  = 500;

const normalizeFromOnly = Array.isArray(ONLY_KEYS) && ONLY_KEYS.includes('normalize-ust');
const SHOULD_NORMALIZE = NORMALIZE || normalizeFromOnly;

// ─── Helpers ─────────────────────────────────────────────────────────────────
const trim = (v) => (v || '').toString().trim();
const toFloat = (v) => { const f = parseFloat(v); return Number.isFinite(f) ? f : null; };

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function downloadFile(url, dest, label) {
  return new Promise((resolve, reject) => {
    if (fs.existsSync(dest) && fs.statSync(dest).size > 1024) {
      console.log(`  ↩  ${label}: already downloaded (${(fs.statSync(dest).size / 1024 / 1024).toFixed(1)} MB)`);
      return resolve(dest);
    }
    const dir = path.dirname(dest);
    fs.mkdirSync(dir, { recursive: true });
    const tmp = dest + '.tmp';
    const protocol = url.startsWith('https') ? https : http;
    const startMs = Date.now();
    const req = protocol.get(url, { timeout: 600000, headers: { 'User-Agent': 'GeoScope/1.0' } }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return downloadFile(res.headers.location, dest, label).then(resolve).catch(reject);
      }
      if (res.statusCode !== 200) {
        res.resume();
        return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
      }
      const total = parseInt(res.headers['content-length'] || 0);
      let received = 0, lastLog = 0;
      const file = fs.createWriteStream(tmp);
      res.on('data', (chunk) => {
        received += chunk.length;
        const pct = total > 0 ? Math.floor(received / total * 100) : '?';
        if (Date.now() - lastLog > 3000) {
          process.stdout.write(`\r  ⬇  ${label}: ${pct}% (${(received / 1024 / 1024).toFixed(1)} MB)`);
          lastLog = Date.now();
        }
      });
      res.pipe(file);
      file.on('finish', () => {
        const elapsed = ((Date.now() - startMs) / 1000).toFixed(1);
        console.log(`\r  ✔  ${label}: ${(received / 1024 / 1024).toFixed(1)} MB in ${elapsed}s`);
        fs.renameSync(tmp, dest);
        resolve(dest);
      });
      file.on('error', reject);
      res.on('error', reject);
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error(`Timeout downloading ${url}`)); });
  });
}

async function batchInsert(rows) {
  if (DRY_RUN || rows.length === 0) return rows.length;
  const COLS = ['database_name','category','class_code','priority_tier','priority_score',
                'site_name','address','city','state','zip','status',
                'registry_id','source_id','source_org','location','attributes'];
  const values = [];
  const params = [];
  let pi = 1;
  for (const r of rows) {
    const loc = (r.lat && r.lng && Number.isFinite(r.lat) && Number.isFinite(r.lng))
      ? `ST_SetSRID(ST_MakePoint($${pi + 13}, $${pi + 12}), 4326)` : 'NULL';
    values.push(`($${pi},$${pi+1},$${pi+2},$${pi+3},$${pi+4},$${pi+5},$${pi+6},$${pi+7},$${pi+8},$${pi+9},$${pi+10},$${pi+11},$${pi+12 + (r.lat ? 2 : 0)},$${pi+13 + (r.lat ? 0 : -2) /* fix below */},${ r.lat ? `ST_SetSRID(ST_MakePoint($${pi+14},$${pi+13}),4326)` : 'NULL'},$${pi + (r.lat ? 15 : 13)})`);
    // simplified — rebuild properly below
    params.push(r.database_name, r.category || 'contamination', r.class_code || 'UNKNOWN',
                r.priority_tier || 'standard', r.priority_score || 50,
                r.site_name || '', r.address || '', r.city || '', r.state || '', r.zip || '',
                r.status || 'Unknown', r.registry_id || null, r.source_id,
                r.source_org || 'EPA',
                r.lng !== null ? r.lng : null,
                r.lat !== null ? r.lat : null,
                r.attributes ? JSON.stringify(r.attributes) : null);
    pi += 17;
  }
  // ─── Rebuild with correct parameterisation ───────────────────────────────
  const safeRows = rows.map(r => ({
    database_name:  r.database_name,
    category:       r.category       || 'contamination',
    class_code:     r.class_code      || 'UNKNOWN',
    priority_tier:  r.priority_tier   || 'standard',
    priority_score: r.priority_score  || 50,
    site_name:      r.site_name        || '',
    address:        r.address          || '',
    city:           r.city             || '',
    state:          r.state            || '',
    zip:            r.zip              || '',
    status:         r.status           || 'Unknown',
    registry_id:    r.registry_id      || null,
    source_id:      r.source_id,
    source_org:     r.source_org       || 'EPA',
    lat:            r.lat,
    lng:            r.lng,
    attributes:     r.attributes       || {}
  }));

  const chunks = [];
  for (let i = 0; i < safeRows.length; i += BATCH_SIZE) chunks.push(safeRows.slice(i, i + BATCH_SIZE));

  let inserted = 0;
  for (const chunk of chunks) {
    const vals = [];
    const args = [];
    let idx = 1;
    for (const r of chunk) {
      if (r.lat && r.lng) {
        vals.push(`($${idx},$${idx+1},$${idx+2},$${idx+3},$${idx+4},$${idx+5},$${idx+6},$${idx+7},$${idx+8},$${idx+9},$${idx+10},$${idx+11},$${idx+12},$${idx+13},ST_SetSRID(ST_MakePoint($${idx+14},$${idx+15}),4326),$${idx+16})`);
        args.push(r.database_name, r.category, r.class_code, r.priority_tier, r.priority_score,
                  r.site_name, r.address, r.city, r.state, r.zip, r.status,
                  r.registry_id, r.source_id, r.source_org, r.lng, r.lat, JSON.stringify(r.attributes));
        idx += 17;
      } else {
        vals.push(`($${idx},$${idx+1},$${idx+2},$${idx+3},$${idx+4},$${idx+5},$${idx+6},$${idx+7},$${idx+8},$${idx+9},$${idx+10},$${idx+11},$${idx+12},$${idx+13},NULL,$${idx+14})`);
        args.push(r.database_name, r.category, r.class_code, r.priority_tier, r.priority_score,
                  r.site_name, r.address, r.city, r.state, r.zip, r.status,
                  r.registry_id, r.source_id, r.source_org, JSON.stringify(r.attributes));
        idx += 15;
      }
    }
    const sql = `INSERT INTO environmental_sites
      (database_name,category,class_code,priority_tier,priority_score,
       site_name,address,city,state,zip,status,registry_id,source_id,source_org,location,attributes)
      VALUES ${vals.join(',')}
      ON CONFLICT (source_id) DO NOTHING`;
    try {
      const res = await pool.query(sql, args);
      inserted += res.rowCount || 0;
    } catch (e) {
      // retry row-by-row on error
      for (const r of chunk) {
        try {
          const locSql = r.lat && r.lng
            ? 'ST_SetSRID(ST_MakePoint($15,$16),4326)' : 'NULL';
          const sArgs = r.lat && r.lng
            ? [r.database_name, r.category, r.class_code, r.priority_tier, r.priority_score,
               r.site_name, r.address, r.city, r.state, r.zip, r.status,
               r.registry_id, r.source_id, r.source_org, r.lng, r.lat, JSON.stringify(r.attributes)]
            : [r.database_name, r.category, r.class_code, r.priority_tier, r.priority_score,
               r.site_name, r.address, r.city, r.state, r.zip, r.status,
               r.registry_id, r.source_id, r.source_org, JSON.stringify(r.attributes)];
          const paramCount = r.lat && r.lng ? 17 : 15;
          const sVals = r.lat && r.lng
            ? `($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,ST_SetSRID(ST_MakePoint($15,$16),4326),$17)`
            : `($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,NULL,$15)`;
          const rSql = `INSERT INTO environmental_sites
            (database_name,category,class_code,priority_tier,priority_score,
             site_name,address,city,state,zip,status,registry_id,source_id,source_org,location,attributes)
            VALUES ${sVals} ON CONFLICT (source_id) DO NOTHING`;
          const rr = await pool.query(rSql, sArgs);
          inserted += rr.rowCount || 0;
        } catch (_) { /* skip bad row */ }
      }
    }
  }
  return inserted;
}

// ─── Check existing datasets ──────────────────────────────────────────────────
async function getLoadedDatasets() {
  const res = await pool.query(
    `SELECT database_name, COUNT(*) as cnt
     FROM environmental_sites GROUP BY database_name`
  );
  const map = {};
  for (const r of res.rows) map[r.database_name.toUpperCase()] = parseInt(r.cnt);
  return map;
}

// ─── SOURCE DEFINITIONS ───────────────────────────────────────────────────────
//
// type: 'spawn'       → fork federal-bulk-download.js with --source=<key>
//       'zip_csv'     → download ZIP, extract CSV matching targetFile, stream-import
//       'direct_csv'  → download CSV directly, stream-import
//       'fema_api'    → OpenFEMA paginated JSON API
//       'cms_api'     → CMS Socrata-style CSV
//       'msha_zip'    → MSHA zip with CSV inside
//

const SOURCES = {
  // ─── Spawn existing federal-bulk-download workers ──────────────────────────
  echo:         { type: 'spawn', key: 'echo',         label: 'EPA ECHO Exporter (1.5M+ facilities)',  dbs: ['EPA ECHO','RCRA','NPDES','ICIS-Air','SDWA','TRI','GHG Reporting','ICIS FE&C','Superfund SEMS'] },
  frs:          { type: 'spawn', key: 'frs',           label: 'EPA FRS Facility Registry (4.7M+)',     dbs: ['EPA FRS'] },
  rcra:         { type: 'spawn', key: 'rcra',          label: 'EPA RCRAInfo (hazardous waste)',         dbs: ['RCRA LQG','RCRA SQG','RCRA Handler'] },
  air:          { type: 'spawn', key: 'air',           label: 'EPA ICIS-Air (CAA sources)',             dbs: ['ICIS-Air'] },
  air_emissions:{ type: 'spawn', key: 'air_emissions', label: 'EPA Air Emissions (NEI/GHGRP/TRI)',     dbs: ['Air Emissions'] },
  npdes:        { type: 'spawn', key: 'npdes',         label: 'EPA ICIS-NPDES (CWA permits)',          dbs: ['NPDES'] },
  sdwa:         { type: 'spawn', key: 'sdwa',          label: 'EPA SDWA (public water systems)',       dbs: ['SDWA Public Water System'] },
  enforcement:  { type: 'spawn', key: 'enforcement',   label: 'EPA ICIS FE&C (enforcement actions)',   dbs: ['ICIS FE&C'] },
  tri:          { type: 'spawn', key: 'tri',           label: 'EPA TRI (toxic releases)',              dbs: ['TRI'] },
  superfund:    { type: 'spawn', key: 'superfund',     label: 'EPA Superfund SEMS (NPL/CERCLIS)',      dbs: ['Superfund SEMS','CERCLIS'] },
  brownfields:  { type: 'spawn', key: 'brownfields',   label: 'EPA Brownfields ACRES',                 dbs: ['Brownfields ACRES','EPA Brownfields'] },

  // ─── PFAS (from ECHO PFAS ZIP) ─────────────────────────────────────────────
  pfas: {
    type: 'zip_csv',
    label: 'EPA PFAS Dataset (8 PFAS sub-datasets)',
    url: 'https://echo.epa.gov/files/echodownloads/PFAS_downloads.zip',
    sizeMb: 80,
    // fallback: https://echo.epa.gov/files/echodownloads/attains_pfas_v3.json (43MB JSON)
    files: [
      { match: /PFAS_SITES/i,       db: 'PFAS FED SITES',  cat: 'contamination', tier: 'high',   score: 92, mapper: mapPfasSites },
      { match: /PFAS_INDUSTRY/i,    db: 'PFAS INDUSTRY',   cat: 'contamination', tier: 'high',   score: 90, mapper: mapPfasGeneric('PFAS INDUSTRY', 'PFAS_INDUSTRY') },
      { match: /PFAS_NPL/i,         db: 'PFAS NPL',        cat: 'contamination', tier: 'high',   score: 95, mapper: mapPfasGeneric('PFAS NPL', 'PFAS_NPL') },
      { match: /PFAS_PROD/i,        db: 'PFAS PROD',       cat: 'contamination', tier: 'high',   score: 88, mapper: mapPfasGeneric('PFAS PROD', 'PFAS_PRODUCTION') },
      { match: /PFAS_UCMR/i,        db: 'PFAS UCMR3',      cat: 'hydrology',     tier: 'medium', score: 75, mapper: mapPfasGeneric('PFAS UCMR3', 'PFAS_UCMR') },
      { match: /PFAS_WQP/i,         db: 'PFAS WQP',        cat: 'hydrology',     tier: 'medium', score: 72, mapper: mapPfasGeneric('PFAS WQP', 'PFAS_WQP') },
      { match: /PFAS_TRI/i,         db: 'PFAS TRIS',       cat: 'contamination', tier: 'high',   score: 88, mapper: mapPfasGeneric('PFAS TRIS', 'PFAS_TRI') },
    ],
    dbs: ['PFAS FED SITES','PFAS INDUSTRY','PFAS NPL','PFAS PROD','PFAS UCMR3','PFAS WQP','PFAS TRIS']
  },

  // ─── GHG Greenhouse Gas Reporters ─────────────────────────────────────────
  ghg: {
    type: 'direct_csv',
    label: 'EPA Greenhouse Gas Reporters',
    url: 'https://data.epa.gov/efservice/GHG_EMITTER_FACILITY/ROWS/0:99999/CSV',
    sizeMb: 20,
    mapper: mapGhgFacility,
    dbs: ['GHG Emitter']
  },

  // ─── MSHA Mine Data ────────────────────────────────────────────────────────
  msha_mines: {
    type: 'zip_csv',
    label: 'MSHA Active Mines (nationwide)',
    url: 'https://arlweb.msha.gov/OpenGovernmentData/DataSets/Mines.zip',
    sizeMb: 35,
    files: [
      { match: /mines\.csv$/i, db: 'MINES', cat: 'geology', tier: 'medium', score: 65, mapper: mapMsha }
    ],
    dbs: ['MINES']
  },

  // ─── CMS Nursing Homes ─────────────────────────────────────────────────────
  cms_nh: {
    type: 'cms_api',
    label: 'CMS Nursing Home Facilities (nationwide)',
    url: 'https://data.cms.gov/resource/4pq5-n9py.csv',
    sizeMb: 5,
    limit: 20000,
    db: 'NURSING HOMES',
    cat: 'demographics',
    tier: 'low',
    score: 30,
    mapper: mapCmsNursingHome,
    dbs: ['NURSING HOMES']
  },

  // ─── CMS Hospitals ─────────────────────────────────────────────────────────
  cms_hosp: {
    type: 'cms_api',
    label: 'CMS Hospitals (nationwide)',
    url: 'https://data.cms.gov/resource/xubh-q36u.csv',
    sizeMb: 1,
    limit: 10000,
    db: 'HOSPITALS',
    cat: 'demographics',
    tier: 'low',
    score: 25,
    mapper: mapCmsHospital,
    dbs: ['HOSPITALS']
  },

  // ─── RMP Risk Management Plans ─────────────────────────────────────────────
  rmp: {
    type: 'direct_csv',
    label: 'EPA RMP Active Facilities (accidental release risk)',
    url: 'https://www.epa.gov/sites/default/files/2021-03/rmp_search_export_0.csv',
    sizeMb: 15,
    mapper: mapRmp,
    dbs: ['RMP']
  },

  // ─── HMIRS DOT Hazmat Incidents ────────────────────────────────────────────
  hmirs: {
    type: 'direct_csv',
    label: 'DOT HMIRS Hazmat Incidents',
    url: 'https://hazmatonline.phmsa.dot.gov/IncidentReportsSearch/results.aspx?ExportToCSV=1&hmirsStartDate=01/01/2010&hmirsEndDate=12/31/2024&mode=hm',
    sizeMb: 30,
    mapper: mapHmirs,
    dbs: ['HMIRS (DOT)']
  },

  // ─── OpenFEMA Claims (direct CSV endpoint) ─────────────────────────────────
  fema_claims: {
    type: 'fema_api',
    label: 'FEMA NFIP Flood Claims (2.4M+)',
    url: 'https://www.fema.gov/api/open/v2/FimaNfipClaims.csv',
    pageUrl: 'https://www.fema.gov/api/open/v2/FimaNfipClaims.json',
    sizeMb: 500,
    db: 'FEMA NFIP Flood Claims',
    mapper: mapFemaClaims,
    dbs: ['FEMA NFIP Flood Claims']
  },

  // ─── OpenFEMA Policies ─────────────────────────────────────────────────────
  fema_policies: {
    type: 'fema_api',
    label: 'FEMA NFIP Policies (5M+)',
    url: 'https://www.fema.gov/api/open/v2/FimaNfipPolicies.csv',
    pageUrl: 'https://www.fema.gov/api/open/v2/FimaNfipPolicies.json',
    sizeMb: 1000,
    db: 'FEMA NFIP Policies',
    mapper: mapFemaPolicies,
    dbs: ['FEMA NFIP Policies']
  },

  // ─── NPL Superfund CSV (alternative direct source) ────────────────────────
  npl: {
    type: 'direct_csv',
    label: 'EPA NPL Final/Proposed Sites',
    url: 'https://data.epa.gov/efservice/SEMS_SITES/ROWS/0:50000/CSV',
    sizeMb: 15,
    mapper: mapSemsSite,
    dbs: ['NPL','SEMS Active','PROPOSED NPL','DELISTED NPL']
  },

  // ─── EPA EJScreen (Environmental Justice) ─────────────────────────────────
  ejscreen: {
    type: 'zip_csv',
    label: 'EPA EJScreen (Env. Justice census data)',
    url: 'https://gaftp.epa.gov/EJSCREEN/2023/2.22_September_UseMe/EJSCREEN_2023_BG.csv.zip',
    sizeMb: 120,
    files: [
      { match: /EJSCREEN.*\.csv$/i, db: 'EJ SCREEN', cat: 'demographics', tier: 'standard', score: 55, mapper: mapEjscreen }
    ],
    dbs: ['EJ SCREEN']
  },

  // ─── USGS Water Quality Portal ─────────────────────────────────────────────
  usgs_wq: {
    type: 'direct_csv',
    label: 'USGS NWIS Water Quality Monitoring Stations',
    url: 'https://www.waterqualitydata.us/portal/station/search?countrycode=US&mimeType=csv&providers=NWIS&siteType=Well&siteType=Stream&siteType=Lake',
    sizeMb: 80,
    mapper: mapUsgsWq,
    dbs: ['NWIS ODI','USGS Water Quality']
  },

  // ─── ERNS Emergency Response Notification System ───────────────────────────
  erns: {
    type: 'direct_csv',
    label: 'EPA ERNS Emergency Response Notifications',
    url: 'https://data.epa.gov/efservice/ERNS_SPILL_REPORTS/ROWS/0:50000/CSV',
    sizeMb: 20,
    mapper: mapErns,
    dbs: ['ERNS']
  },

  // ─── Tribal Brownfields ────────────────────────────────────────────────────
  tribal_bf: {
    type: 'direct_csv',
    label: 'EPA Tribal Brownfields',
    url: 'https://data.epa.gov/efservice/ACRES_TRIBAL_SITES/ROWS/0:99999/CSV',
    sizeMb: 5,
    mapper: mapTribalBf,
    dbs: ['TRIBAL BROWNFIELDS']
  },
};

// ─── MAPPERS ──────────────────────────────────────────────────────────────────

function mapPfasSites(row) {
  const lat = toFloat(row['LATITUDE83'] || row['LATITUDE'] || row['FAC_LAT']);
  const lng = toFloat(row['LONGITUDE83'] || row['LONGITUDE'] || row['FAC_LONG']);
  return [{
    database_name:  'PFAS FED SITES',
    category:       'contamination',
    site_name:      trim(row['FACILITY_NAME'] || row['FAC_NAME'] || row['SITE_NAME']),
    address:        trim(row['ADDRESS'] || row['LOCATION_ADDRESS'] || row['FAC_STREET']),
    city:           trim(row['CITY'] || row['CITY_NAME'] || row['FAC_CITY']),
    state:          trim(row['STATE'] || row['STATE_CODE'] || row['FAC_STATE']),
    zip:            trim(row['ZIP'] || row['POSTAL_CODE'] || row['FAC_ZIP']),
    status:         'Active',
    registry_id:    trim(row['REGISTRY_ID']),
    source_id:      `PFAS-${trim(row['REGISTRY_ID'] || row['FACILITY_ID'] || row['SITE_ID'] || Math.random().toString(36).slice(2))}`,
    source_org:     'EPA',
    class_code:     'PFAS_FEDERAL_SITE',
    priority_tier:  'high',
    priority_score: 92,
    lat, lng,
    attributes: { pfas_chemical: row['PFAS_CHEMICAL'], contamination_type: row['CONTAM_TYPE'] }
  }];
}

function mapPfasGeneric(dbName, prefix) {
  return (row) => {
    const lat = toFloat(row['LATITUDE83'] || row['LATITUDE'] || row['FAC_LAT']);
    const lng = toFloat(row['LONGITUDE83'] || row['LONGITUDE'] || row['FAC_LONG']);
    const id = trim(row['REGISTRY_ID'] || row['FACILITY_ID'] || row['PWSID'] || row['TRI_FACILITY_ID']);
    if (!id && !row['FACILITY_NAME'] && !row['FAC_NAME']) return [];
    return [{
      database_name:  dbName,
      category:       dbName.includes('WQP') || dbName.includes('UCMR') ? 'hydrology' : 'contamination',
      site_name:      trim(row['FACILITY_NAME'] || row['FAC_NAME'] || row['PWS_NAME'] || dbName + ' Site'),
      address:        trim(row['ADDRESS'] || row['LOCATION_ADDRESS'] || row['FAC_STREET'] || ''),
      city:           trim(row['CITY'] || row['CITY_NAME'] || row['FAC_CITY'] || ''),
      state:          trim(row['STATE'] || row['STATE_CODE'] || row['FAC_STATE'] || ''),
      zip:            trim(row['ZIP'] || row['POSTAL_CODE'] || row['FAC_ZIP'] || ''),
      status:         'Active',
      registry_id:    id || null,
      source_id:      `${prefix}-${id || Math.random().toString(36).slice(2)}`,
      source_org:     'EPA',
      class_code:     prefix,
      priority_tier:  'high',
      priority_score: 88,
      lat, lng,
      attributes: {}
    }];
  };
}

function mapGhgFacility(row) {
  const lat = toFloat(row['LATITUDE'] || row['FAC_LAT']);
  const lng = toFloat(row['LONGITUDE'] || row['FAC_LONG']);
  return [{
    database_name:  'GHG Emitter',
    category:       'contamination',
    site_name:      trim(row['FACILITY_NAME'] || row['FAC_NAME']),
    address:        trim(row['FACILITY_ADDRESS'] || row['FAC_STREET']),
    city:           trim(row['CITY_NAME'] || row['FAC_CITY']),
    state:          trim(row['STATE_CODE'] || row['FAC_STATE']),
    zip:            trim(row['ZIP_CODE'] || row['FAC_ZIP']),
    status:         'Active',
    registry_id:    trim(row['REGISTRY_ID']),
    source_id:      `GHG-${trim(row['FACILITY_ID'] || row['REGISTRY_ID'])}`,
    source_org:     'EPA',
    class_code:     'GHG_REPORTING_FACILITY',
    priority_tier:  'medium',
    priority_score: 65,
    lat, lng,
    attributes: { ghg_id: row['FACILITY_ID'], total_emissions: row['TOTAL_REPORTED_EMISSIONS_MT_CO2E'] }
  }];
}

function mapMsha(row) {
  const lat = toFloat(row['LATITUDE'] || row['MINE_LATITUDE']);
  const lng = toFloat(row['LONGITUDE'] || row['MINE_LONGITUDE']);
  const status = trim(row['CURRENT_MINE_STATUS'] || row['MINE_STATUS'] || 'Unknown');
  return [{
    database_name:  'MINES',
    category:       'geology',
    site_name:      trim(row['MINE_NAME'] || row['MINE_ID']),
    address:        '',
    city:           trim(row['COUNTY'] || ''),
    state:          trim(row['STATE'] || row['STATE_ABBR']),
    zip:            '',
    status,
    registry_id:    null,
    source_id:      `MSHA-${trim(row['MINE_ID'])}`,
    source_org:     'MSHA',
    class_code:     'MINE_' + trim(row['PRIMARY_SIC_CD'] || 'UNKNOWN').replace(/\s+/g, '_'),
    priority_tier:  'medium',
    priority_score: 65,
    lat, lng,
    attributes: {
      mine_id:   row['MINE_ID'],
      mine_type: row['PRIMARY_SIC_CD'],
      coal_metal_indicator: row['COAL_METAL_IND'],
      status
    }
  }];
}

function mapCmsNursingHome(row) {
  const lat = toFloat(row['geocoded_coordinate']?.coordinates?.[1] || row['latitude']);
  const lng = toFloat(row['geocoded_coordinate']?.coordinates?.[0] || row['longitude']);
  const id = trim(row['federal_provider_number'] || row['provider_name']);
  return [{
    database_name:  'NURSING HOMES',
    category:       'demographics',
    site_name:      trim(row['provider_name'] || 'Nursing Home'),
    address:        trim(row['address_line_1'] || row['address']),
    city:           trim(row['city_town'] || row['city']),
    state:          trim(row['state']),
    zip:            trim(row['zip_code'] || row['zip']),
    status:         row['provider_sub_type'] || 'Active',
    registry_id:    trim(row['federal_provider_number']),
    source_id:      `NH-${id}`,
    source_org:     'CMS',
    class_code:     'NURSING_HOME',
    priority_tier:  'low',
    priority_score: 30,
    lat, lng,
    attributes: { beds: row['number_of_certified_beds'], ownership: row['ownership_type'] }
  }];
}

function mapCmsHospital(row) {
  const lat = toFloat(row['lat'] || row['latitude']);
  const lng = toFloat(row['lng'] || row['longitude']);
  const id = trim(row['provider_id'] || row['facility_id'] || row['hospital_name']);
  return [{
    database_name:  'HOSPITALS',
    category:       'demographics',
    site_name:      trim(row['hospital_name'] || row['facility_name']),
    address:        trim(row['address'] || row['address_line_1']),
    city:           trim(row['city'] || row['city_town']),
    state:          trim(row['state']),
    zip:            trim(row['zip_code'] || row['zip']),
    status:         trim(row['hospital_ownership'] || 'Active'),
    registry_id:    trim(row['provider_id']),
    source_id:      `HOSP-${id}`,
    source_org:     'CMS',
    class_code:     'HOSPITAL_' + trim(row['hospital_type'] || 'GENERAL').replace(/\s+/g, '_').toUpperCase(),
    priority_tier:  'low',
    priority_score: 25,
    lat, lng,
    attributes: { hospital_type: row['hospital_type'], beds: row['number_of_beds'] }
  }];
}

function mapRmp(row) {
  const lat = toFloat(row['Lat'] || row['LATITUDE'] || row['lat']);
  const lng = toFloat(row['Long'] || row['LONGITUDE'] || row['lng'] || row['lon']);
  const id = trim(row['EPAFacilityID'] || row['EPA_FACILITY_ID'] || row['FacilityID']);
  return [{
    database_name:  'RMP',
    category:       'regulatory',
    site_name:      trim(row['FacilityName'] || row['FACILITY_NAME']),
    address:        trim(row['FacilityStr1'] || row['ADDRESS']),
    city:           trim(row['FacilityCity'] || row['CITY']),
    state:          trim(row['FacilityState'] || row['STATE']),
    zip:            trim(row['FacilityZip'] || row['ZIP']),
    status:         'Active',
    registry_id:    id || null,
    source_id:      `RMP-${id || Math.random().toString(36).slice(2)}`,
    source_org:     'EPA',
    class_code:     'RMP_ACCIDENTAL_RELEASE',
    priority_tier:  'high',
    priority_score: 90,
    lat, lng,
    attributes: {
      facility_id: id,
      naics:        row['NAICS'] || row['NAICSCode'],
      num_processes: row['NumProcs'] || row['NumberProcesses']
    }
  }];
}

function mapHmirs(row) {
  const lat = toFloat(row['LATITUDE'] || row['Latitude']);
  const lng = toFloat(row['LONGITUDE'] || row['Longitude']);
  const id = trim(row['REPORT_NUMBER'] || row['IncidentID'] || row['ID']);
  const dateStr = trim(row['INCIDENT_DATE'] || row['Date'] || '');
  return [{
    database_name:  'HMIRS (DOT)',
    category:       'regulatory',
    site_name:      `Hazmat Incident ${dateStr.substring(0, 10)}`,
    address:        trim(row['STREET'] || row['Street'] || ''),
    city:           trim(row['CITY'] || row['City'] || ''),
    state:          trim(row['STATE'] || row['State'] || ''),
    zip:            trim(row['ZIP'] || row['Zip'] || ''),
    status:         'Closed',
    registry_id:    null,
    source_id:      `HMIRS-${id || Math.random().toString(36).slice(2)}`,
    source_org:     'DOT',
    class_code:     'HAZMAT_INCIDENT',
    priority_tier:  'medium',
    priority_score: 70,
    lat, lng,
    attributes: {
      incident_date: dateStr,
      hazmat_class:  row['HAZMAT_CLASS'] || row['HazmatClass'],
      material_name: row['MATERIAL_NAME'] || row['MaterialName'],
      quantity:      row['QUANTITY_RELEASED']
    }
  }];
}

function mapFemaClaims(row) {
  const lat = toFloat(row['latitude']);
  const lng = toFloat(row['longitude']);
  const id = trim(row['id'] || row['claimId']);
  return [{
    database_name:  'FEMA NFIP Flood Claims',
    category:       'hydrology',
    site_name:      `Flood Claim - ${row['occupancyType'] || 'Residential'}`,
    address:        '',
    city:           trim(row['countyCode'] || ''),
    state:          trim(row['state'] || ''),
    zip:            trim(row['reportedZipcode'] || ''),
    status:         'Closed',
    source_id:      `FEMA-CLAIM-${id || Math.random().toString(36).slice(2)}`,
    source_org:     'FEMA',
    class_code:     'FEMA_FLOOD_CLAIM',
    priority_tier:  'high',
    priority_score: 85,
    lat, lng,
    attributes: {
      date_of_loss: row['dateOfLoss'],
      amount_paid:  row['amountPaidOnBuildingClaim'],
      flood_zone:   row['floodZone'],
      occupancy:    row['occupancyType']
    }
  }];
}

function mapFemaPolicies(row) {
  const lat = toFloat(row['latitude']);
  const lng = toFloat(row['longitude']);
  const id = trim(row['id'] || row['policyId']);
  return [{
    database_name:  'FEMA NFIP Policies',
    category:       'hydrology',
    site_name:      `NFIP Policy - ${row['occupancyType'] || 'Residential'}`,
    address:        '',
    city:           '',
    state:          trim(row['propertyState'] || row['state'] || ''),
    zip:            trim(row['reportedZipcode'] || ''),
    status:         row['policyTerminationDate'] ? 'Expired' : 'Active',
    source_id:      `FEMA-POLICY-${id || Math.random().toString(36).slice(2)}`,
    source_org:     'FEMA',
    class_code:     'FEMA_FLOOD_POLICY',
    priority_tier:  'medium',
    priority_score: 60,
    lat, lng,
    attributes: { flood_zone: row['floodZone'], coverage: row['totalBuildingInsuranceCoverage'] }
  }];
}

function mapSemsSite(row) {
  const lat = toFloat(row['LATITUDE83'] || row['LATITUDE'] || row['FAC_LAT']);
  const lng = toFloat(row['LONGITUDE83'] || row['LONGITUDE'] || row['FAC_LONG']);
  const nplStatus = trim(row['NPL_STATUS_CODE'] || row['SITE_STATUS'] || '');
  let dbName = 'SEMS Active';
  if (nplStatus.includes('DELIST') || nplStatus.includes('D')) dbName = 'DELISTED NPL';
  else if (nplStatus.includes('PROP') || nplStatus.includes('P')) dbName = 'PROPOSED NPL';
  else if (nplStatus.includes('FINAL') || nplStatus.includes('F') || nplStatus.includes('NPL')) dbName = 'NPL';
  const id = trim(row['SITE_ID'] || row['EPA_SITE_ID'] || row['REGISTRY_ID']);
  return [{
    database_name:  dbName,
    category:       'contamination',
    site_name:      trim(row['SITE_NAME'] || row['FAC_NAME']),
    address:        trim(row['ADDRESS'] || row['LOCATION_ADDRESS'] || ''),
    city:           trim(row['CITY'] || row['CITY_NAME'] || ''),
    state:          trim(row['STATE'] || row['STATE_CODE'] || ''),
    zip:            trim(row['ZIP'] || row['POSTAL_CODE'] || ''),
    status:         nplStatus || 'Active',
    registry_id:    trim(row['REGISTRY_ID']),
    source_id:      `SEMS-${id || Math.random().toString(36).slice(2)}`,
    source_org:     'EPA',
    class_code:     'SUPERFUND_' + dbName.replace(/\s+/g, '_').toUpperCase(),
    priority_tier:  'high',
    priority_score: 95,
    lat, lng,
    attributes: { site_id: id, npl_status: nplStatus, operable_units: row['OPERABLE_UNITS_COUNT'] }
  }];
}

function mapEjscreen(row) {
  const lat = toFloat(row['PCTSGLE_I'] ? row['LAT'] : row['LATITUDE'] || row['LAT']);
  const lng = toFloat(row['LON'] || row['LONGITUDE']);
  const id = trim(row['ID'] || row['GEOID'] || row['OBJECTID']);
  if (!id) return [];
  return [{
    database_name:  'EJ SCREEN',
    category:       'demographics',
    site_name:      `Census Block Group ${trim(row['ACSTOTPOP'] ? row['ID'] : id)}`,
    address:        '',
    city:           trim(row['CNTY_NAME'] || ''),
    state:          trim(row['ST_ABBREV'] || ''),
    zip:            '',
    status:         'Active',
    registry_id:    null,
    source_id:      `EJSCREEN-${id}`,
    source_org:     'EPA',
    class_code:     'EJ_CENSUS_BLOCK_GROUP',
    priority_tier:  'standard',
    priority_score: 50,
    lat, lng,
    attributes: {
      geoid:        id,
      population:   row['ACSTOTPOP'],
      ej_index:     row['EJSCREEN_SCORE_10'],
      minority_pct: row['MINORPCT'],
      low_income:   row['LOWINCPCT'],
      pm25:         row['PM25']
    }
  }];
}

function mapUsgsWq(row) {
  const lat = toFloat(row['LatitudeMeasure'] || row['LATITUDE']);
  const lng = toFloat(row['LongitudeMeasure'] || row['LONGITUDE']);
  const id = trim(row['MonitoringLocationIdentifier'] || row['SITE_ID']);
  if (!id) return [];
  return [{
    database_name:  'NWIS ODI',
    category:       'hydrology',
    site_name:      trim(row['MonitoringLocationName'] || row['STATION_NAME'] || id),
    address:        '',
    city:           trim(row['CountyCode'] || ''),
    state:          trim(row['StateCode'] || ''),
    zip:            '',
    status:         'Active',
    registry_id:    null,
    source_id:      `NWIS-${id}`,
    source_org:     'USGS',
    class_code:     'WATER_QUALITY_MONITORING_STATION',
    priority_tier:  'standard',
    priority_score: 45,
    lat, lng,
    attributes: {
      site_type: row['MonitoringLocationTypeName'],
      huc_8:     row['HUCEightDigitCode'],
      provider:  row['ProviderName']
    }
  }];
}

function mapErns(row) {
  const lat = toFloat(row['LATITUDE'] || row['LAT']);
  const lng = toFloat(row['LONGITUDE'] || row['LON']);
  const id = trim(row['SEQNOS'] || row['INCIDENT_ID'] || row['REPORT_ID']);
  return [{
    database_name:  'ERNS',
    category:       'contamination',
    site_name:      `ERNS Spill - ${trim(row['MATERIAL_INVOLVED'] || row['CHEMICAL_NAME'] || 'Unknown')}`,
    address:        trim(row['STREET_ADDRESS'] || ''),
    city:           trim(row['CITY'] || ''),
    state:          trim(row['STATE'] || ''),
    zip:            '',
    status:         'Closed',
    registry_id:    null,
    source_id:      `ERNS-${id || Math.random().toString(36).slice(2)}`,
    source_org:     'EPA',
    class_code:     'EMERGENCY_RESPONSE_NOTIFICATION',
    priority_tier:  'medium',
    priority_score: 72,
    lat, lng,
    attributes: { material: row['MATERIAL_INVOLVED'], quantity: row['QTY_RELEASED'], date: row['DATE_OF_INCIDENT'] }
  }];
}

function mapTribalBf(row) {
  const lat = toFloat(row['LATITUDE83'] || row['LATITUDE'] || row['FAC_LAT']);
  const lng = toFloat(row['LONGITUDE83'] || row['LONGITUDE'] || row['FAC_LONG']);
  const id = trim(row['SITE_ID'] || row['REGISTRY_ID'] || row['ACRES_ID']);
  return [{
    database_name:  'TRIBAL BROWNFIELDS',
    category:       'contamination',
    site_name:      trim(row['SITE_NAME'] || row['FAC_NAME'] || 'Tribal Brownfield'),
    address:        trim(row['ADDRESS'] || row['LOCATION_ADDRESS'] || ''),
    city:           trim(row['CITY'] || row['CITY_NAME'] || ''),
    state:          trim(row['STATE'] || row['STATE_CODE'] || ''),
    zip:            '',
    status:         trim(row['SITE_STATUS'] || 'Active'),
    registry_id:    trim(row['REGISTRY_ID']),
    source_id:      `TRIBAL-BF-${id || Math.random().toString(36).slice(2)}`,
    source_org:     'EPA',
    class_code:     'TRIBAL_BROWNFIELD',
    priority_tier:  'medium',
    priority_score: 70,
    lat, lng,
    attributes: { acres_id: row['ACRES_ID'], tribe: row['TRIBE_NAME'] || row['TRIBAL_ENTITY_NAME'] }
  }];
}

// ─── Import: stream CSV file into DB ─────────────────────────────────────────
async function importCsvStream(readStream, mapper, label) {
  let rows = 0, inserted = 0, buf = [];
  await new Promise((resolve, reject) => {
    readStream
      .pipe(csvParse({ columns: true, skip_empty_lines: true, relax_quotes: true, trim: true }))
      .on('data', async (row) => {
        try {
          const mapped = mapper(row);
          if (mapped && mapped.length) buf.push(...mapped);
          if (buf.length >= BATCH_SIZE) {
            const toFlush = buf.splice(0, BATCH_SIZE);
            rows += toFlush.length;
            inserted += await batchInsert(toFlush);
            if (rows % 10000 === 0) process.stdout.write(`\r  ↑  ${label}: ${rows.toLocaleString()} rows processed, ${inserted.toLocaleString()} inserted`);
          }
        } catch (_) { /* skip bad row */ }
      })
      .on('end', async () => {
        if (buf.length) {
          rows += buf.length;
          inserted += await batchInsert(buf);
          buf = [];
        }
        console.log(`\r  ✔  ${label}: ${rows.toLocaleString()} rows → ${inserted.toLocaleString()} inserted`);
        resolve();
      })
      .on('error', reject);
  });
  return { rows, inserted };
}

// ─── Import: ZIP file with multiple CSVs ─────────────────────────────────────
async function importZipCsv(zipPath, fileConfigs, label) {
  if (!unzipper) throw new Error('unzipper not installed — run: npm install unzipper');
  let totalRows = 0, totalInserted = 0;
  const dir = await unzipper.Open.file(zipPath);
  for (const fileConf of fileConfigs) {
    const entry = dir.files.find(f => fileConf.match.test(f.path));
    if (!entry) { console.log(`  ⚠  ${label}: no file matching ${fileConf.match} in ZIP`); continue; }
    console.log(`  📄 ${label}: importing ${entry.path} → ${fileConf.db}`);
    const stream = entry.stream();
    const { rows, inserted } = await importCsvStream(stream, fileConf.mapper, `${label}/${fileConf.db}`);
    totalRows += rows; totalInserted += inserted;
  }
  return { rows: totalRows, inserted: totalInserted };
}

// ─── Import: direct CSV URL (stream to DB) ────────────────────────────────────
async function importDirectCsv(url, mapper, label) {
  const dest = path.join(DOWNLOAD_DIR, label.replace(/[^a-z0-9]/gi, '_').toLowerCase() + '.csv');
  await downloadFile(url, dest, label);
  const { rows, inserted } = await importCsvStream(fs.createReadStream(dest), mapper, label);
  return { rows, inserted };
}

// ─── Import: OpenFEMA CSV bulk download ───────────────────────────────────────
async function importFemaCSV(src, label) {
  const dest = path.join(DOWNLOAD_DIR, src.db.replace(/[^a-z0-9]/gi, '_').toLowerCase() + '.csv');
  // Try direct CSV first
  try {
    await downloadFile(src.url, dest, label);
    return await importCsvStream(fs.createReadStream(dest), src.mapper, label);
  } catch (e) {
    console.log(`  ⚠  ${label}: direct CSV failed (${e.message}), trying paginated JSON…`);
    // Paginated JSON fallback
    const pageUrl = src.pageUrl;
    const PAGE_SIZE = 10000;
    let skip = 0, totalInserted = 0, totalRows = 0;
    while (true) {
      const url = `${pageUrl}?$top=${PAGE_SIZE}&$skip=${skip}&$inlinecount=allpages&$format=json`;
      const data = await fetchJson(url);
      const records = data[Object.keys(data).find(k => Array.isArray(data[k]))] || [];
      if (records.length === 0) break;
      const mapped = records.flatMap(r => src.mapper(r)).filter(r => r.source_id);
      totalRows += mapped.length;
      totalInserted += await batchInsert(mapped);
      skip += PAGE_SIZE;
      process.stdout.write(`\r  ↑  ${label}: ${totalRows.toLocaleString()} fetched, ${totalInserted.toLocaleString()} inserted`);
      if (records.length < PAGE_SIZE) break;
      await sleep(200);
    }
    console.log(`\r  ✔  ${label}: ${totalRows.toLocaleString()} → ${totalInserted.toLocaleString()} inserted`);
    return { rows: totalRows, inserted: totalInserted };
  }
}

// ─── Import: CMS Socrata CSV (paginated) ─────────────────────────────────────
async function importCmsAPI(src, label) {
  const dest = path.join(DOWNLOAD_DIR, src.db.replace(/[^a-z0-9]/gi, '_').toLowerCase() + '.csv');
  const fullUrl = `${src.url}?$limit=${src.limit || 50000}&$$app_token=`;
  try {
    await downloadFile(fullUrl, dest, label);
    return await importCsvStream(fs.createReadStream(dest), src.mapper, label);
  } catch (e) {
    console.log(`  ⚠  ${label}: CMS API failed — ${e.message}`);
    return { rows: 0, inserted: 0 };
  }
}

// ─── fetchJson helper ─────────────────────────────────────────────────────────
function fetchJson(url) {
  return new Promise((resolve, reject) => {
    const protocol = url.startsWith('https') ? https : http;
    const req = protocol.get(url, { headers: { 'User-Agent': 'GeoScope/1.0' }, timeout: 60000 }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location)
        return fetchJson(res.headers.location).then(resolve).catch(reject);
      let body = '';
      res.on('data', d => body += d);
      res.on('end', () => {
        try { resolve(JSON.parse(body)); } catch (e) { reject(e); }
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
  });
}

// ─── Spawn federal-bulk-download.js worker ───────────────────────────────────
function spawnFederal(key, label) {
  return new Promise((resolve) => {
    const script = path.join(__dirname, 'federal-bulk-download.js');
    const args = [`--source=${key}`, '--skip-download', '--no-geocode'];
    // first try with skip-download, then without
    const tryWithDownload = () => new Promise((res) => {
      const child = spawn(process.execPath, [script, `--source=${key}`, '--no-geocode'], {
        stdio: ['ignore', 'pipe', 'pipe'],
        env: process.env,
        cwd: path.join(__dirname, '..')
      });
      let out = '';
      child.stdout.on('data', d => { out += d; process.stdout.write(d); });
      child.stderr.on('data', d => { out += d; });
      child.on('close', code => res({ code, out }));
    });

    const child = spawn(process.execPath, [script, `--source=${key}`, '--no-geocode'], {
      stdio: ['ignore', 'pipe', 'pipe'],
      env: process.env,
      cwd: path.join(__dirname, '..')
    });

    let out = '';
    child.stdout.on('data', d => { out += d.toString(); process.stdout.write(`[${key}] ${d}`); });
    child.stderr.on('data', d => { process.stderr.write(`[${key}] ${d}`); });
    child.on('close', (code) => {
      if (code !== 0) console.log(`  ⚠  ${label}: exited ${code}`);
      else console.log(`  ✔  ${label}: completed`);
      resolve({ source: key, label, code });
    });
  });
}

// ─── UST/LUST normalisation SQL ───────────────────────────────────────────────
async function normalizeUstLust() {
  console.log('\n🔧 Normalising UST/LUST state+city from attributes JSONB…');

  // DC shapefile records: populate address+zip from JSONB, set state=DC, city=Washington
  let r = await pool.query(
    `UPDATE environmental_sites
     SET state='DC', city='Washington',
         zip=COALESCE(NULLIF(TRIM(attributes->>'ZIPCODE'),''), zip),
         address=COALESCE(NULLIF(TRIM(attributes->>'SITE_ADDRESS'),''), address)
     WHERE database_name IN ('EPA LUST','EPA UST')
       AND (state IS NULL OR state='')
       AND (attributes->>'ZIPCODE' IS NOT NULL OR attributes->>'SITE_ADDRESS' IS NOT NULL)`
  );
  if (r.rowCount > 0) console.log(`  ✔  DC LUST/UST records fixed: ${r.rowCount} rows`);

  // Plain UST records (attributes=null, DC-style addresses)
  r = await pool.query(
    `UPDATE environmental_sites SET state='DC', city='Washington'
     WHERE database_name='UST' AND (state IS NULL OR state='')`
  );
  if (r.rowCount > 0) console.log(`  ✔  Plain UST DC records fixed: ${r.rowCount} rows`);

  // Generic JSONB state/city keys for any UST-type data added later
  const dbs = "'UST','EPA UST','EPA LUST','LUST','UST FINDER'";
  const stateKeys = ['STATE','State','state','FAC_STATE','STATE_CODE'];
  const cityKeys  = ['CITY','City','city','FAC_CITY','CITY_NAME'];
  for (const k of stateKeys) {
    r = await pool.query(
      `UPDATE environmental_sites SET state=TRIM(attributes->>'${k}')
       WHERE database_name IN (${dbs})
         AND (state IS NULL OR state='')
         AND attributes->>'${k}' IS NOT NULL AND TRIM(attributes->>'${k}')<>''`
    );
    if (r.rowCount > 0) console.log(`  ✔  state from ['${k}']: ${r.rowCount} rows`);
  }
  for (const k of cityKeys) {
    r = await pool.query(
      `UPDATE environmental_sites SET city=TRIM(attributes->>'${k}')
       WHERE database_name IN (${dbs})
         AND (city IS NULL OR city='')
         AND attributes->>'${k}' IS NOT NULL AND TRIM(attributes->>'${k}')<>''`
    );
    if (r.rowCount > 0) console.log(`  ✔  city from ['${k}']: ${r.rowCount} rows`);
  }
  console.log('  ✔  UST/LUST normalisation complete.');
}

// ─── QUEUE RUNNER ─────────────────────────────────────────────────────────────
async function runQueue(tasks, workers) {
  const queue = [...tasks];
  const inFlight = [];
  const results = [];

  async function runTask(task) {
    const { key, src } = task;
    console.log(`\n▶  Starting: ${src.label}`);
    try {
      let result;
      if (src.type === 'spawn') {
        result = await spawnFederal(src.key, src.label);
      } else if (src.type === 'zip_csv') {
        const dest = path.join(DOWNLOAD_DIR, key + '.zip');
        await downloadFile(src.url, dest, src.label);
        result = await importZipCsv(dest, src.files, src.label);
      } else if (src.type === 'direct_csv') {
        result = await importDirectCsv(src.url, src.mapper, src.label);
      } else if (src.type === 'fema_api') {
        result = await importFemaCSV(src, src.label);
      } else if (src.type === 'cms_api') {
        result = await importCmsAPI(src, src.label);
      } else if (src.type === 'msha_zip') {
        const dest = path.join(DOWNLOAD_DIR, key + '.zip');
        await downloadFile(src.url, dest, src.label);
        result = await importZipCsv(dest, src.files, src.label);
      }
      results.push({ key, success: true, ...result });
    } catch (e) {
      console.error(`\n  ✗  ${src.label}: ${e.message}`);
      results.push({ key, success: false, error: e.message });
    }
  }

  while (queue.length > 0 || inFlight.length > 0) {
    while (inFlight.length < workers && queue.length > 0) {
      const task = queue.shift();
      const p = runTask(task).then(() => {
        const idx = inFlight.indexOf(p);
        if (idx >= 0) inFlight.splice(idx, 1);
      });
      inFlight.push(p);
    }
    if (inFlight.length > 0) await Promise.race(inFlight);
  }

  return results;
}

// ─── MAIN ─────────────────────────────────────────────────────────────────────
async function main() {
  if (LIST_ONLY) {
    console.log('\nAvailable source keys:\n');
    for (const [k, v] of Object.entries(SOURCES)) {
      console.log(`  ${k.padEnd(20)} — ${v.label}`);
    }
    await pool.end();
    return;
  }

  console.log('\n╔══════════════════════════════════════════════════════════════╗');
  console.log('║          GeoScope Mega-Install  (Parallel Dataset Loader)    ║');
  console.log('╚══════════════════════════════════════════════════════════════╝\n');

  // Check current DB state
  console.log('🔍 Checking existing datasets…');
  const loaded = await getLoadedDatasets();
  const totalBefore = Object.values(loaded).reduce((a, b) => a + b, 0);
  console.log(`   Current: ${totalBefore.toLocaleString()} records across ${Object.keys(loaded).length} datasets\n`);

  // Run UST/LUST normalisation first if requested
  if (SHOULD_NORMALIZE) {
    await normalizeUstLust();
    if (normalizeFromOnly && ONLY_KEYS.length === 1) {
      await pool.end();
      return;
    }
  }

  // Build task list
  let allKeys = ONLY_KEYS ? ONLY_KEYS.filter(k => k !== 'normalize-ust') : Object.keys(SOURCES);
  const tasks = [];

  for (const key of allKeys) {
    const src = SOURCES[key];
    if (!src) { console.warn(`⚠  Unknown source key: ${key}`); continue; }

    // Skip already-loaded check
    if (SKIP_LOADED && src.dbs) {
      const alreadyLoaded = src.dbs.every(db => (loaded[db.toUpperCase()] || 0) > 1000);
      if (alreadyLoaded) {
        console.log(`  ↩  Skipping ${src.label} (already loaded)`);
        continue;
      }
    }

    tasks.push({ key, src });
  }

  if (tasks.length === 0) {
    console.log('✅ All requested datasets already loaded. Nothing to do.');
    await pool.end();
    return;
  }

  console.log(`📋 ${tasks.length} sources to process, ${WORKERS} parallel workers\n`);
  const startTime = Date.now();

  const results = await runQueue(tasks, WORKERS);

  // Always run UST normalisation at the end unless explicitly skipped
  if (!NORMALIZE && !flag('--no-normalize')) {
    await normalizeUstLust();
  }

  // Final stats
  console.log('\n\n╔══════════════════════════════════════════════════════════════╗');
  console.log('║                      IMPORT SUMMARY                         ║');
  console.log('╚══════════════════════════════════════════════════════════════╝');
  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  let succeed = 0, failed = 0;
  for (const r of results) {
    if (r.success) {
      succeed++;
      const ins = r.inserted !== undefined ? ` (+${r.inserted?.toLocaleString() || '?'} rows)` : '';
      console.log(`  ✔  ${r.key}${ins}`);
    } else {
      failed++;
      console.log(`  ✗  ${r.key}: ${r.error}`);
    }
  }

  const loaded2 = await getLoadedDatasets();
  const totalAfter = Object.values(loaded2).reduce((a, b) => a + b, 0);
  console.log(`\n  Before: ${totalBefore.toLocaleString()} records  |  After: ${totalAfter.toLocaleString()} records`);
  console.log(`  New:    +${(totalAfter - totalBefore).toLocaleString()} records`);
  console.log(`  Time:   ${elapsed} minutes`);
  console.log(`  Done:   ${succeed} succeeded, ${failed} failed\n`);

  await pool.end();
}

main().catch(e => { console.error('Fatal:', e.message); process.exit(1); });
