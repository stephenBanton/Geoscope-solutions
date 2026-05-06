#!/usr/bin/env node
// =============================================================================
// GeoScope Extra Sources Downloader + Importer
// Downloads and imports ~20 new federal datasets to reach 120 total
// =============================================================================
// Usage:
//   node scripts/extra-sources.js                   -- run all
//   node scripts/extra-sources.js --only=usgs_eq,nid,faa
//   node scripts/extra-sources.js --list            -- list all sources
// =============================================================================
require('dotenv').config({ path: require('path').join(__dirname, '../.env') });

const fs       = require('fs');
const path     = require('path');
const https    = require('https');
const http     = require('http');
const { parse } = require('csv-parse');
const { pool }  = require('../db');

const DOWNLOAD_DIR = path.join(__dirname, '../downloads/extra');
if (!fs.existsSync(DOWNLOAD_DIR)) fs.mkdirSync(DOWNLOAD_DIR, { recursive: true });

const STATE_NAME_TO_CODE = new Map([
  ['ALABAMA', 'AL'], ['ALASKA', 'AK'], ['ARIZONA', 'AZ'], ['ARKANSAS', 'AR'],
  ['CALIFORNIA', 'CA'], ['COLORADO', 'CO'], ['CONNECTICUT', 'CT'], ['DELAWARE', 'DE'],
  ['DISTRICT OF COLUMBIA', 'DC'], ['FLORIDA', 'FL'], ['GEORGIA', 'GA'], ['HAWAII', 'HI'],
  ['IDAHO', 'ID'], ['ILLINOIS', 'IL'], ['INDIANA', 'IN'], ['IOWA', 'IA'],
  ['KANSAS', 'KS'], ['KENTUCKY', 'KY'], ['LOUISIANA', 'LA'], ['MAINE', 'ME'],
  ['MARYLAND', 'MD'], ['MASSACHUSETTS', 'MA'], ['MICHIGAN', 'MI'], ['MINNESOTA', 'MN'],
  ['MISSISSIPPI', 'MS'], ['MISSOURI', 'MO'], ['MONTANA', 'MT'], ['NEBRASKA', 'NE'],
  ['NEVADA', 'NV'], ['NEW HAMPSHIRE', 'NH'], ['NEW JERSEY', 'NJ'], ['NEW MEXICO', 'NM'],
  ['NEW YORK', 'NY'], ['NORTH CAROLINA', 'NC'], ['NORTH DAKOTA', 'ND'], ['OHIO', 'OH'],
  ['OKLAHOMA', 'OK'], ['OREGON', 'OR'], ['PENNSYLVANIA', 'PA'], ['RHODE ISLAND', 'RI'],
  ['SOUTH CAROLINA', 'SC'], ['SOUTH DAKOTA', 'SD'], ['TENNESSEE', 'TN'], ['TEXAS', 'TX'],
  ['UTAH', 'UT'], ['VERMONT', 'VT'], ['VIRGINIA', 'VA'], ['WASHINGTON', 'WA'],
  ['WEST VIRGINIA', 'WV'], ['WISCONSIN', 'WI'], ['WYOMING', 'WY'], ['PUERTO RICO', 'PR'],
  ['GUAM', 'GU'], ['VIRGIN ISLANDS', 'VI'], ['AMERICAN SAMOA', 'AS'], ['NORTHERN MARIANA ISLANDS', 'MP']
]);

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------
function download(url, destPath) {
  return new Promise((resolve, reject) => {
    if (fs.existsSync(destPath) && fs.statSync(destPath).size > 1024) {
      console.log(`  [cache] ${path.basename(destPath)}`);
      return resolve(destPath);
    }
    const tmp = destPath + '.tmp';
    const file = fs.createWriteStream(tmp);
    const proto = url.startsWith('https') ? https : http;
    const req = proto.get(url, { headers: { 'User-Agent': 'GeoScope/1.0' } }, res => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        file.close();
        fs.unlink(tmp, () => {});
        return download(res.headers.location, destPath).then(resolve).catch(reject);
      }
      if (res.statusCode !== 200) {
        file.close();
        fs.unlink(tmp, () => {});
        return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
      }
      let bytes = 0;
      res.on('data', chunk => {
        bytes += chunk.length;
        if (bytes % (5 * 1024 * 1024) < chunk.length) process.stdout.write(`\r  Downloading... ${(bytes/1024/1024).toFixed(1)} MB`);
      });
      res.pipe(file);
      file.on('finish', () => {
        file.close(() => {
          fs.renameSync(tmp, destPath);
          process.stdout.write(`\r  Downloaded: ${(bytes/1024/1024).toFixed(1)} MB\n`);
          resolve(destPath);
        });
      });
    });
    req.setTimeout(120000, () => { req.destroy(); reject(new Error('Timeout')); });
    req.on('error', err => { file.close(); fs.unlink(tmp, () => {}); reject(err); });
  });
}

function fetchJson(url) {
  return new Promise((resolve, reject) => {
    const proto = url.startsWith('https') ? https : http;
    let data = '';
    proto.get(url, { headers: { 'User-Agent': 'GeoScope/1.0' } }, res => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return fetchJson(res.headers.location).then(resolve).catch(reject);
      }
      if (res.statusCode !== 200) return reject(new Error(`HTTP ${res.statusCode}`));
      res.on('data', c => data += c);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); } catch(e) { reject(e); }
      });
    }).on('error', reject);
  });
}

async function parseCsvFile(filePath, delimiterHint = ',', options = {}) {
  return new Promise((resolve, reject) => {
    const rows = [];
    fs.createReadStream(filePath)
      .pipe(parse({
        columns: true,
        delimiter: delimiterHint,
        skip_empty_lines: true,
        trim: true,
        bom: true,
        relax_quotes: true,
        relax_column_count: true,
        skip_records_with_error: true,
        from_line: options.fromLine || 1,
      }))
      .on('data', r => rows.push(r))
      .on('error', reject)
      .on('end', () => resolve(rows));
  });
}

function normalizeState(value) {
  if (value == null) return null;
  const cleaned = String(value).trim();
  if (!cleaned) return null;

  const upper = cleaned.toUpperCase();
  if (/^[A-Z]{2}$/.test(upper)) return upper;

  return STATE_NAME_TO_CODE.get(upper) || null;
}

async function batchInsert(rows) {
  if (!rows.length) return 0;
  const values = [];
  const params = [];
  let p = 1;
  for (const r of rows) {
    values.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},ST_SetSRID(ST_MakePoint($${p++},$${p++}),4326))`);
    params.push(
      r.source_id, r.site_name, r.address, r.city, r.state, r.zip,
      r.database_name, r.category, r.class_code, r.priority_tier, (r.priority_score ?? 50), (r.source_org || 'UNKNOWN'),
      r.lon, r.lat
    );
  }
  const sql = `INSERT INTO environmental_sites
    (source_id,site_name,address,city,state,zip,database_name,category,class_code,priority_tier,priority_score,source_org,location)
    VALUES ${values.join(',')} ON CONFLICT (source_id) DO NOTHING`;
  const res = await pool.query(sql, params);
  return res.rowCount;
}

async function importRows(rowObjs, batchSize = 500) {
  let ins = 0;
  let reportedBatchError = false;
  let reportedRowError = false;
  for (let i = 0; i < rowObjs.length; i += batchSize) {
    const slice = rowObjs.slice(i, i + batchSize).map(r => ({
      ...r,
      site_name: r.site_name || r.name || null,
      state: normalizeState(r.state),
      source_org: r.source_org || 'US Federal',
      priority_score: Number.isFinite(r.priority_score) ? r.priority_score : 50,
    }));
    try {
      ins += await batchInsert(slice);
    } catch(e) {
      if (!reportedBatchError) {
        reportedBatchError = true;
        console.log(`\n  Batch insert error: ${e.message}`);
      }
      // row-by-row fallback
      for (const r of slice) {
        try {
          ins += await batchInsert([r]);
        } catch(rowError) {
          if (!reportedRowError) {
            reportedRowError = true;
            console.log(`  Row insert error: ${rowError.message}`);
            console.log(`  Failing source_id: ${r.source_id}`);
          }
        }
      }
    }
    process.stdout.write(`\r  Rows: ${Math.min(i+batchSize, rowObjs.length).toLocaleString()}/${rowObjs.length.toLocaleString()} inserted=${ins.toLocaleString()}`);
  }
  process.stdout.write('\n');
  return ins;
}

// ---------------------------------------------------------------------------
// ArcGIS REST helper — pages through all features
// ---------------------------------------------------------------------------
async function importArcGIS({ url, dbName, category, classCode, mapper, maxRecords = 50000 }) {
  console.log(`\n📡 ArcGIS: ${dbName}`);
  let offset = 0;
  let totalIns = 0;
  const pageSize = 1000;

  while (true) {
    const pageUrl = `${url}?where=1%3D1&outFields=*&f=json&resultOffset=${offset}&resultRecordCount=${pageSize}&returnGeometry=true&outSR=4326`;
    let data;
    try { data = await fetchJson(pageUrl); } catch(e) { console.log(`  Error: ${e.message}`); break; }
    const features = data.features || [];
    if (!features.length) break;

    const rows = features.map(f => {
      const attrs = f.attributes || {};
      const geom = f.geometry || {};
      const r = mapper(attrs);
      if (!r) return null;
      r.lat = r.lat || geom.y;
      r.lon = r.lon || geom.x;
      r.database_name = dbName;
      r.category = category;
      r.class_code = classCode;
      r.priority_tier = r.priority_tier || 'standard';
      return r;
    }).filter(r => r && isFinite(r.lat) && isFinite(r.lon) && r.lat !== 0);

    totalIns += await importRows(rows);
    offset += pageSize;
    if (features.length < pageSize) break;
    if (offset >= maxRecords) { console.log(`  Reached maxRecords=${maxRecords}, stopping.`); break; }
  }
  console.log(`  ✅ ${dbName}: ${totalIns.toLocaleString()} inserted`);
  return totalIns;
}

// ---------------------------------------------------------------------------
// SOURCES
// ---------------------------------------------------------------------------
const SOURCES = {

  // ---------- USGS Earthquakes (FDSN Event API, CSV format) ----------
  usgs_eq: {
    name: 'USGS Earthquakes',
    async run() {
      console.log(`\n🌍 USGS Earthquakes`);
      const url = 'https://earthquake.usgs.gov/fdsnws/event/1/query.csv?starttime=1990-01-01&endtime=2025-01-01&minmagnitude=2.5&limit=20000&format=csv&orderby=time';
      const dest = path.join(DOWNLOAD_DIR, 'usgs_earthquakes.csv');
      await download(url, dest);
      const records = await parseCsvFile(dest, ',', { fromLine: 2 });
      console.log(`  Rows: ${records.length.toLocaleString()}`);

      const rows = records.map(r => {
        const lat = parseFloat(r['latitude'] || r['lat']);
        const lon = parseFloat(r['longitude'] || r['lon']);
        if (!isFinite(lat) || !isFinite(lon)) return null;
        return {
          source_id: `USGS-EQ-${r['id'] || r['#id'] || (lat+'_'+lon+'_'+r['time'])}`,
          name: `M${r['mag']} ${r['place']}`,
          address: null, city: null, state: null, zip: null,
          database_name: 'USGS Earthquakes', category: 'geological', class_code: 'USGS_EQ', priority_tier: 'standard',
          lat, lon
        };
      }).filter(Boolean);

      const ins = await importRows(rows);
      console.log(`  ✅ USGS Earthquakes: ${ins.toLocaleString()} inserted`);
    }
  },

  // ---------- National Inventory of Dams (USACE) ----------
  nid: {
    name: 'National Dam Inventory',
    async run() {
      console.log(`\n🏞️  National Dam Inventory`);
      const url = 'https://nid.sec.usace.army.mil/api/nation/csv';
      const dest = path.join(DOWNLOAD_DIR, 'nid_dams.csv');
      try { await download(url, dest); } catch(e) { console.log(`  Error: ${e.message}`); return; }
      const records = await parseCsvFile(dest, ',', { fromLine: 2 });
      console.log(`  Rows: ${records.length.toLocaleString()}`);

      const headers = Object.keys(records[0] || {}).map(h => h.toLowerCase());
      const latK = Object.keys(records[0] || {}).find(k => ['latitude','lat','dec_lat','latitude_dd'].includes(k.toLowerCase()));
      const lonK = Object.keys(records[0] || {}).find(k => ['longitude','lon','long','dec_long','longitude_dd'].includes(k.toLowerCase()));
      const idK  = Object.keys(records[0] || {}).find(k => ['nidid','dam_id','id'].includes(k.toLowerCase()));
      const nmK  = Object.keys(records[0] || {}).find(k => ['dam_name','name','damname'].includes(k.toLowerCase()));
      const stK  = Object.keys(records[0] || {}).find(k => ['state','state_abbr','st'].includes(k.toLowerCase()));

      const rows = records.map(r => {
        const lat = parseFloat(latK ? r[latK] : 0);
        const lon = parseFloat(lonK ? r[lonK] : 0);
        if (!isFinite(lat) || !isFinite(lon) || lat === 0) return null;
        return {
          source_id: `NID-${idK ? r[idK] : lat+'_'+lon}`,
          name: nmK ? r[nmK] : null,
          address: null, city: null, state: stK ? r[stK] : null, zip: null,
          database_name: 'National Dam Inventory', category: 'infrastructure', class_code: 'NID_DAM', priority_tier: 'standard',
          lat, lon
        };
      }).filter(Boolean);

      const ins = await importRows(rows);
      console.log(`  ✅ National Dam Inventory: ${ins.toLocaleString()} inserted`);
    }
  },

  // ---------- FAA Airports (ArcGIS) ----------
  faa_airports: {
    name: 'FAA Airports',
    async run() {
      await importArcGIS({
        url: 'https://services6.arcgis.com/ssFJjBXIUyZDrSYZ/arcgis/rest/services/US_Airport/FeatureServer/0/query',
        dbName: 'FAA Airports',
        category: 'transportation',
        classCode: 'FAA_AIRPORT',
        maxRecords: 30000,
        mapper: a => {
          const lat = parseFloat(a['Latitude'] || a['LAT_DECIMAL'] || a['lat'] || 0);
          const lon = parseFloat(a['Longitude'] || a['LON_DECIMAL'] || a['lon'] || 0);
          if (!isFinite(lat) || lat === 0) return null;
          return {
            source_id: `FAA-AIRPORT-${a['GlobalID'] || a['Loc_ID'] || a['LOCID'] || a['ObjectId'] || a['OBJECTID']}`,
            name: a['Airport_Name'] || a['NAME'] || a['FULLNAME'],
            address: null,
            city: a['City'] || a['CITY'],
            state: a['State_Name'] || a['STATE_CODE'] || a['STATE'],
            zip: null,
            lat, lon
          };
        }
      });
    }
  },

  // ---------- FAA Airports (data.gov CSV fallback) ----------
  faa_airports_csv: {
    name: 'FAA Airports CSV',
    async run() {
      console.log(`\n✈️  FAA Airports (CSV)`);
      const url = 'https://opendata.arcgis.com/api/v3/datasets/e747ab91a11045e8b3f8a3efd093d3b5_0/downloads/data?format=csv&spatialRefId=4326';
      const dest = path.join(DOWNLOAD_DIR, 'faa_airports.csv');
      try { await download(url, dest); } catch(e) { console.log(`  Error: ${e.message}`); return; }
      const records = await parseCsvFile(dest);
      console.log(`  Rows: ${records.length.toLocaleString()}`);

      const rows = records.map(r => {
        const lat = parseFloat(r['Y'] || r['LAT_DECIMAL'] || r['Latitude'] || 0);
        const lon = parseFloat(r['X'] || r['LON_DECIMAL'] || r['Longitude'] || 0);
        if (!isFinite(lat) || lat === 0) return null;
        return {
          source_id: `FAA-AIRPORT-${r['GlobalID'] || r['Loc_ID'] || r['LOCID'] || r['OBJECTID']}`,
          name: r['Airport_Name'] || r['NAME'] || r['FULLNAME'],
          address: null,
          city: r['City'] || r['CITY'],
          state: r['State_Name'] || r['STATE_CODE'] || r['STATE'],
          zip: null,
          database_name: 'FAA Airports', category: 'transportation', class_code: 'FAA_AIRPORT', priority_tier: 'standard',
          lat, lon
        };
      }).filter(Boolean);

      const ins = await importRows(rows);
      console.log(`  ✅ FAA Airports: ${ins.toLocaleString()} inserted`);
    }
  },

  // ---------- NRC Nuclear Power Plants ----------
  nrc_nuclear: {
    name: 'NRC Nuclear Facilities',
    async run() {
      console.log(`\n☢️  NRC Nuclear Facilities`);
      const url = 'https://www.nrc.gov/reactors/operating/list-power-reactor-units.html';
      // Use a known CSV from NRC FOIA/data
      // Alternative: use opendata ArcGIS for NRC sites
      const arcUrl = 'https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Nuclear_Facilities/FeatureServer/0/query';
      try {
        await importArcGIS({
          url: arcUrl,
          dbName: 'NRC Nuclear Facilities',
          category: 'nuclear',
          classCode: 'NRC_NUCLEAR',
          maxRecords: 5000,
          mapper: a => ({
            source_id: `NRC-${a['OBJECTID'] || a['Name'] || a['FACILITY_NAME']}`,
            name: a['Name'] || a['FACILITY_NAME'] || a['FAC_NAME'],
            address: a['Address'] || a['STREET_ADDRESS'],
            city: a['City'] || a['CITY'],
            state: a['State'] || a['STATE'],
            zip: a['ZIP'] || a['ZIP_CODE'],
          })
        });
      } catch(e) {
        console.log(`  Skipped NRC Nuclear: ${e.message}`);
      }
    }
  },

  // ---------- NOAA Storm Events (Locations CSV) ----------
  noaa_storms: {
    name: 'NOAA Storm Events',
    async run() {
      console.log(`\n🌪️  NOAA Storm Events`);
      // Try direct CSV download for recent years
      const years = ['2023', '2022', '2021'];
      for (const yr of years) {
        const baseUrl = `https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/`;
        // Fetch directory listing to get exact filename
        try {
          const listUrl = `${baseUrl}`;
          // Use a known pattern for the locations file
          // StormEvents_locations-ftp_v1.0_d{year}_c{updated}.csv.gz
          // Try to find via a direct approach
          const knownUrls = [
            `https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/StormEvents_locations-ftp_v1.0_d${yr}_c20241017.csv.gz`,
            `https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/StormEvents_locations-ftp_v1.0_d${yr}_c20240716.csv.gz`,
            `https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/StormEvents_locations-ftp_v1.0_d${yr}_c20250101.csv.gz`,
          ];
          // Skip gzipped files for now - need gunzip support
          console.log(`  Skipping NOAA storm events (gzip format requires additional tooling)`);
          break;
        } catch(e) {
          console.log(`  ${yr}: ${e.message}`);
        }
      }
    }
  },

  // ---------- EPA Superfund NPL (ArcGIS) ----------
  superfund: {
    name: 'EPA Superfund NPL',
    async run() {
      await importArcGIS({
        url: 'https://services.arcgis.com/cJ9YHowT8TU7DUyn/ArcGIS/rest/services/Superfund_National_Priorities_List_NPL_Sites_with_Status/FeatureServer/0/query',
        dbName: 'EPA Superfund NPL',
        category: 'contamination',
        classCode: 'SUPERFUND_NPL',
        maxRecords: 10000,
        mapper: a => ({
          source_id: `NPL-${a['OBJECTID'] || a['EPA_ID'] || a['SITE_ID']}`,
          name: a['SITE_NAME'] || a['Name'],
          address: a['SITE_ADDRESS'] || a['ADDRESS'],
          city: a['CITY'],
          state: a['STATE'],
          zip: a['ZIP'],
        })
      });
    }
  },

  // ---------- EPA Brownfields (ArcGIS) ----------
  brownfields: {
    name: 'EPA Brownfields',
    async run() {
      await importArcGIS({
        url: 'https://services.arcgis.com/cJ9YHowT8TU7DUyn/arcgis/rest/services/ACRES_Brownfields_Facilities/FeatureServer/0/query',
        dbName: 'EPA Brownfields',
        category: 'contamination',
        classCode: 'BROWNFIELD',
        maxRecords: 100000,
        mapper: a => ({
          source_id: `BF-${a['OBJECTID'] || a['ASSESSMENT_ID'] || a['ACRES_ID']}`,
          name: a['PROPERTY_NAME'] || a['NAME'],
          address: a['PROPERTY_ADDRESS'] || a['ADDRESS'],
          city: a['CITY'],
          state: a['STATE'],
          zip: a['ZIP_CODE'] || a['ZIP'],
        })
      });
    }
  },

  // ---------- EPA GHG Emitters (Envirofacts) ----------
  ghg: {
    name: 'EPA GHG Emitters',
    async run() {
      console.log(`\n🏭 EPA GHG Emitters`);
      // Try multiple table names
      const attempts = [
        'https://data.epa.gov/efservice/V_GHG_EMITTER_FACILITIES/ROWS/0:50000/CSV',
        'https://data.epa.gov/efservice/GHG_EMITTER_FACILITIES/ROWS/0:50000/CSV',
        'https://enviro.epa.gov/enviro/efservice/V_GHG_EMITTER_FACILITIES/ROWS/0:50000/CSV',
        'https://data.epa.gov/efservice/PUB_DIM_FACILITY/ROWS/0:50000/CSV',
      ];

      let dest = null;
      for (const url of attempts) {
        const fname = path.join(DOWNLOAD_DIR, `ghg_facilities_${attempts.indexOf(url)}.csv`);
        try {
          await download(url, fname);
          const stat = fs.statSync(fname);
          if (stat.size > 5000) { dest = fname; break; }
        } catch(e) { console.log(`  Attempt ${attempts.indexOf(url)+1}: ${e.message}`); }
      }

      if (!dest) { console.log(`  ❌ GHG: all endpoints failed`); return; }

      const records = await parseCsvFile(dest);
      console.log(`  Rows: ${records.length.toLocaleString()}`);

      const hdr = Object.keys(records[0] || {});
      const latK = hdr.find(k => ['latitude','lat','latitude_meas','fac_lat'].includes(k.toLowerCase()));
      const lonK = hdr.find(k => ['longitude','lon','long','longitude_meas','fac_lon'].includes(k.toLowerCase()));
      const idK  = hdr.find(k => ['facility_id','registry_id','ghg_id','facilityid','id'].includes(k.toLowerCase()));
      const nmK  = hdr.find(k => ['facility_name','name','fac_name'].includes(k.toLowerCase()));

      const rows = records.map(r => {
        const lat = parseFloat(latK ? r[latK] : 0);
        const lon = parseFloat(lonK ? r[lonK] : 0);
        if (!isFinite(lat) || lat === 0) return null;
        return {
          source_id: `GHG-${idK ? r[idK] : lat+'_'+lon}`,
          name: nmK ? r[nmK] : null,
          address: null, city: r['city'] || r['CITY'] || null,
          state: r['state'] || r['STATE'] || null, zip: null,
          database_name: 'EPA GHG Emitters', category: 'air_quality', class_code: 'GHG_EMITTER', priority_tier: 'standard',
          lat, lon
        };
      }).filter(Boolean);

      const ins = await importRows(rows);
      console.log(`  ✅ EPA GHG Emitters: ${ins.toLocaleString()} inserted`);
    }
  },

  // ---------- HRSA Federally Qualified Health Centers ----------
  hrsa_fqhc: {
    name: 'HRSA Health Centers',
    async run() {
      console.log(`\n🏥 HRSA Health Centers`);
      const url = 'https://data.hrsa.gov/DataDownload/DD_Files/Health_Center_Service_Delivery_and_LookAlike_Sites.csv';
      const dest = path.join(DOWNLOAD_DIR, 'hrsa_health_centers.csv');
      try { await download(url, dest); } catch(e) {
        console.log(`  Error: ${e.message}`);
        return;
      }
      const records = await parseCsvFile(dest);
      console.log(`  Rows: ${records.length.toLocaleString()}`);

      const rows = records.map(r => {
        const lat = parseFloat(r['Geocoding Artifact Address Primary Y Coordinate'] || r['Latitude'] || r['latitude'] || 0);
        const lon = parseFloat(r['Geocoding Artifact Address Primary X Coordinate'] || r['Longitude'] || r['longitude'] || 0);
        if (!isFinite(lat) || lat === 0) return null;
        return {
          source_id: `HRSA-FQHC-${r['BDR Number'] || r['Site Name'] || lat+'_'+lon}`,
          name: r['Site Name'] || r['Health Center Name'],
          address: r['Site Address'] || r['Street Address'],
          city: r['Site City'] || r['City'],
          state: r['Site State Abbreviation'] || r['State'],
          zip: r['Site Postal Code'] || r['Zip Code'],
          database_name: 'HRSA Health Centers', category: 'healthcare', class_code: 'HRSA_FQHC', priority_tier: 'standard',
          lat, lon
        };
      }).filter(Boolean);

      const ins = await importRows(rows);
      console.log(`  ✅ HRSA Health Centers: ${ins.toLocaleString()} inserted`);
    }
  },

  // ---------- CMS Hospitals ----------
  cms_hospitals: {
    name: 'CMS Hospitals',
    async run() {
      console.log(`\n🏨 CMS Hospitals`);
      const url = 'https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0/download?format=csv';
      const dest = path.join(DOWNLOAD_DIR, 'cms_hospitals.csv');
      try { await download(url, dest); } catch(e) { console.log(`  Error: ${e.message}`); return; }
      const records = await parseCsvFile(dest);
      console.log(`  Rows: ${records.length.toLocaleString()}`);

      const rows = records.map(r => {
        const lat = parseFloat(r['Latitude'] || r['latitude'] || r['lat'] || 0);
        const lon = parseFloat(r['Longitude'] || r['longitude'] || r['lon'] || 0);
        if (!isFinite(lat) || lat === 0) return null;
        return {
          source_id: `CMS-HOSP-${r['Facility ID'] || r['Provider ID'] || lat+'_'+lon}`,
          name: r['Facility Name'] || r['Provider Name'],
          address: r['Address'] || r['Street Address'],
          city: r['City/Town'] || r['City'],
          state: r['State'] || r['STATE'],
          zip: r['ZIP Code'] || r['Zip'],
          database_name: 'CMS Hospitals', category: 'healthcare', class_code: 'CMS_HOSPITAL', priority_tier: 'standard',
          lat, lon
        };
      }).filter(Boolean);

      const ins = await importRows(rows);
      console.log(`  ✅ CMS Hospitals: ${ins.toLocaleString()} inserted`);
    }
  },

  // ---------- OpenFEMA Disaster Declarations ----------
  fema_disasters: {
    name: 'FEMA Disaster Declarations',
    async run() {
      console.log(`\n🌊 FEMA Disaster Declarations`);
      // This dataset is by county, we need to geocode or use county centroid
      // Skip for now - no lat/lon
      console.log(`  Skipped - no lat/lon in dataset`);
    }
  },

  // ---------- DOT Pipeline Incidents (PHMSA) ----------
  dot_pipeline: {
    name: 'DOT Pipeline Incidents',
    async run() {
      console.log(`\n🔧 DOT/PHMSA Pipeline Incidents`);
      const url = 'https://www.phmsa.dot.gov/data-and-statistics/pipeline/pipeline-incident-flagged-files';
      // Try direct CSV from PHMSA data portal
      const attempts = [
        'https://www.phmsa.dot.gov/sites/phmsa.dot.gov/files/data_statistics/pipeline/PHMSA_Pipeline_Safety_Flagged_Incidents.zip',
        'https://api.phmsa.dot.gov/api/incidents/hazliq?format=csv',
      ];
      for (const u of attempts) {
        const dest = path.join(DOWNLOAD_DIR, `phmsa_${attempts.indexOf(u)}.zip`);
        try {
          await download(u, dest);
          console.log(`  Downloaded from ${u}`);
          break;
        } catch(e) { console.log(`  Attempt: ${e.message}`); }
      }
      // Use ArcGIS as backup
      try {
        await importArcGIS({
          url: 'https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Pipeline_Incidents/FeatureServer/0/query',
          dbName: 'DOT Pipeline Incidents',
          category: 'industrial',
          classCode: 'PHMSA_PIPELINE',
          maxRecords: 50000,
          mapper: a => ({
            source_id: `PHMSA-${a['OBJECTID'] || a['INC_ID']}`,
            name: a['OPERATOR_NAME'] || a['NAME'],
            address: null,
            city: a['CITY'] || a['NEAREST_CITY'],
            state: a['STATE'],
            zip: null,
          })
        });
      } catch(e) { console.log(`  ArcGIS fallback failed: ${e.message}`); }
    }
  },

  // ---------- FWS National Wildlife Refuges ----------
  fws_refuges: {
    name: 'FWS Wildlife Refuges',
    async run() {
      await importArcGIS({
        url: 'https://services.arcgis.com/QVENGdaPbd4LUkLV/arcgis/rest/services/FWS_National_Wildlife_Refuge_Boundaries/FeatureServer/0/query',
        dbName: 'FWS Wildlife Refuges',
        category: 'environmental',
        classCode: 'FWS_REFUGE',
        maxRecords: 10000,
        mapper: a => ({
          source_id: `FWS-REFUGE-${a['OBJECTID'] || a['ORG_NAME']}`,
          name: a['ORG_NAME'] || a['ORGNAME'] || a['Name'],
          address: null,
          city: null,
          state: a['STATE'] || a['STATECD'],
          zip: null,
        })
      });
    }
  },

  // ---------- USGS National Water Information System (Monitoring stations) ----------
  usgs_wq: {
    name: 'USGS Water Quality Stations',
    async run() {
      console.log(`\n💧 USGS Water Quality Stations`);
      const url = 'https://www.waterqualitydata.us/data/Station/search?countrycode=US&mimeType=csv&providers=NWIS&startDateLo=2010-01-01';
      const dest = path.join(DOWNLOAD_DIR, 'usgs_wq_stations.csv');
      try { await download(url, dest); } catch(e) { console.log(`  Error: ${e.message}`); return; }

      const stat = fs.statSync(dest);
      if (stat.size < 5000) { console.log(`  Skipped: file too small (${stat.size} bytes)`); return; }

      const records = await parseCsvFile(dest);
      console.log(`  Rows: ${records.length.toLocaleString()}`);

      const rows = records.map(r => {
        const lat = parseFloat(r['LatitudeMeasure'] || r['latitude'] || 0);
        const lon = parseFloat(r['LongitudeMeasure'] || r['longitude'] || 0);
        if (!isFinite(lat) || lat === 0) return null;
        const sid = r['MonitoringLocationIdentifier'] || r['SiteNumber'] || `${lat}_${lon}`;
        return {
          source_id: `USGS-WQ-${sid}`,
          name: r['MonitoringLocationName'] || r['StationName'],
          address: null,
          city: null,
          state: r['StateCode'] || null,
          zip: null,
          database_name: 'USGS Water Quality Stations', category: 'water', class_code: 'USGS_NWIS', priority_tier: 'standard',
          lat, lon
        };
      }).filter(Boolean);

      const ins = await importRows(rows);
      console.log(`  ✅ USGS Water Quality: ${ins.toLocaleString()} inserted`);
    }
  },

  // ---------- EPA PFAS Contamination Sites ----------
  pfas: {
    name: 'EPA PFAS Sites',
    async run() {
      console.log(`\n🧪 EPA PFAS Contamination Sites`);
      const attempts = [
        'https://pfaspub.epa.gov/api/siteinfo/csv',
        'https://pfas-pub.epa.gov/api/siteinfo/csv',
        'https://www.epa.gov/system/files/documents/2024-04/pfas-analytic-tools-contamination-sites.csv',
      ];
      let dest = null;
      for (const url of attempts) {
        const fname = path.join(DOWNLOAD_DIR, `pfas_${attempts.indexOf(url)}.csv`);
        try {
          await download(url, fname);
          if (fs.statSync(fname).size > 5000) { dest = fname; break; }
        } catch(e) { console.log(`  Attempt: ${e.message}`); }
      }
      if (!dest) {
        // Use ArcGIS
        try {
          await importArcGIS({
            url: 'https://services.arcgis.com/cJ9YHowT8TU7DUyn/arcgis/rest/services/PFAS_Analytic_Tools_Contamination_Sites/FeatureServer/0/query',
            dbName: 'EPA PFAS Sites',
            category: 'contamination',
            classCode: 'PFAS',
            maxRecords: 50000,
            mapper: a => ({
              source_id: `PFAS-${a['OBJECTID'] || a['SITE_ID']}`,
              name: a['SITE_NAME'] || a['Name'],
              address: a['ADDRESS'] || a['STREET'],
              city: a['CITY'],
              state: a['STATE'],
              zip: a['ZIP'],
            })
          });
        } catch(e) { console.log(`  PFAS ArcGIS failed: ${e.message}`); }
        return;
      }

      const records = await parseCsvFile(dest);
      const rows = records.map(r => {
        const lat = parseFloat(r['latitude'] || r['Latitude'] || 0);
        const lon = parseFloat(r['longitude'] || r['Longitude'] || 0);
        if (!isFinite(lat) || lat === 0) return null;
        return {
          source_id: `PFAS-${r['site_id'] || r['id'] || lat+'_'+lon}`,
          name: r['site_name'] || r['name'],
          address: r['address'] || null,
          city: r['city'] || null,
          state: r['state'] || null,
          zip: r['zip'] || null,
          database_name: 'EPA PFAS Sites', category: 'contamination', class_code: 'PFAS', priority_tier: 'high',
          lat, lon
        };
      }).filter(Boolean);

      const ins = await importRows(rows);
      console.log(`  ✅ EPA PFAS Sites: ${ins.toLocaleString()} inserted`);
    }
  },

  // ---------- EPA EJScreen (Environmental Justice) ----------
  ejscreen: {
    name: 'EPA EJScreen',
    async run() {
      console.log(`\n🌿 EPA EJScreen`);
      const attempts = [
        'https://gaftp.epa.gov/EJSCREEN/2024/2.32_September_UseMe/EJSCREEN_2024_BG_with_AS_CNMI_GU_VI.csv.zip',
        'https://gaftp.epa.gov/EJSCREEN/2023/2.22_September_UseMe/EJSCREEN_2023_BG_with_AS_CNMI_GU_VI.csv.zip',
        'https://gaftp.epa.gov/EJSCREEN/2024/EJSCREEN_2024_Tracts.csv.zip',
        'https://gaftp.epa.gov/EJSCREEN/2023/EJSCREEN_2023_Tracts_with_AS_CNMI_GU_VI.csv.zip',
      ];
      for (const url of attempts) {
        const dest = path.join(DOWNLOAD_DIR, `ejscreen_${attempts.indexOf(url)}.zip`);
        try {
          await download(url, dest);
          if (fs.statSync(dest).size > 100000) {
            console.log(`  Downloaded: ${(fs.statSync(dest).size/1024/1024).toFixed(1)} MB — needs extraction`);
            // Would need to extract ZIP and parse CSV - complex, skip for now
            console.log(`  EJScreen is by census block (no lat/lon centroid) — skipping`);
            break;
          }
        } catch(e) { console.log(`  Attempt ${attempts.indexOf(url)+1}: ${e.message}`); }
      }
    }
  },

  // ---------- Homeland Infrastructure Foundation (HIFLD) Hazmat Routes ----------
  hifld_hazmat: {
    name: 'HIFLD Hazmat Routes',
    async run() {
      await importArcGIS({
        url: 'https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/HazMat_Routes/FeatureServer/0/query',
        dbName: 'HIFLD Hazmat Routes',
        category: 'transportation',
        classCode: 'HAZMAT_ROUTE',
        maxRecords: 50000,
        mapper: a => {
          if (!a['OBJECTID']) return null;
          return {
            source_id: `HAZMAT-ROUTE-${a['OBJECTID']}`,
            name: a['ROUTE_ID'] || a['ROUTEID'] || `Route ${a['OBJECTID']}`,
            address: null, city: null, state: a['STATE'] || a['ST'], zip: null,
          };
        }
      });
    }
  },

  // ---------- HIFLD Power Plants ----------
  hifld_power: {
    name: 'HIFLD Power Plants',
    async run() {
      await importArcGIS({
        url: 'https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Power_Plants/FeatureServer/0/query',
        dbName: 'HIFLD Power Plants',
        category: 'energy',
        classCode: 'POWER_PLANT',
        maxRecords: 20000,
        mapper: a => ({
          source_id: `HIFLD-PWR-${a['OBJECTID'] || a['Plant_Code']}`,
          name: a['Plant_Name'] || a['NAME'],
          address: null,
          city: a['City'] || a['CITY'],
          state: a['State'] || a['STATE'],
          zip: a['Zip'] || a['ZIP'],
        })
      });
    }
  },

  // ---------- HIFLD Petroleum Terminals ----------
  hifld_petroleum: {
    name: 'HIFLD Petroleum Terminals',
    async run() {
      await importArcGIS({
        url: 'https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Petroleum_Terminals/FeatureServer/0/query',
        dbName: 'HIFLD Petroleum Terminals',
        category: 'industrial',
        classCode: 'PETROLEUM_TERMINAL',
        maxRecords: 10000,
        mapper: a => ({
          source_id: `HIFLD-PETRO-${a['OBJECTID'] || a['ID']}`,
          name: a['NAME'] || a['FACILITY_NAME'],
          address: a['ADDRESS'],
          city: a['CITY'],
          state: a['STATE'],
          zip: a['ZIP'],
        })
      });
    }
  },

  // ---------- HIFLD Electric Substations ----------
  hifld_substations: {
    name: 'HIFLD Electric Substations',
    async run() {
      await importArcGIS({
        url: 'https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Electric_Substations/FeatureServer/0/query',
        dbName: 'HIFLD Electric Substations',
        category: 'energy',
        classCode: 'ELECTRIC_SUBSTATION',
        maxRecords: 100000,
        mapper: a => ({
          source_id: `HIFLD-SUB-${a['OBJECTID'] || a['ID']}`,
          name: a['NAME'] || a['SUBST_NAME'],
          address: a['ADDRESS'],
          city: a['CITY'],
          state: a['STATE'],
          zip: a['ZIP'],
        })
      });
    }
  },

  // ---------- HIFLD Wastewater Treatment Plants ----------
  hifld_wwtp: {
    name: 'HIFLD Wastewater Treatment',
    async run() {
      await importArcGIS({
        url: 'https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Wastewater_Treatment_Plants/FeatureServer/0/query',
        dbName: 'HIFLD Wastewater Treatment',
        category: 'water',
        classCode: 'WWTP',
        maxRecords: 50000,
        mapper: a => ({
          source_id: `HIFLD-WWTP-${a['OBJECTID'] || a['ID']}`,
          name: a['NAME'] || a['FACILITY_NAME'],
          address: a['ADDRESS'],
          city: a['CITY'],
          state: a['STATE'],
          zip: a['ZIP'],
        })
      });
    }
  },

  // ---------- HIFLD Solid Waste Landfills ----------
  hifld_landfills: {
    name: 'HIFLD Landfills',
    async run() {
      await importArcGIS({
        url: 'https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Municipal_Solid_Waste_Landfill/FeatureServer/0/query',
        dbName: 'HIFLD Landfills',
        category: 'waste',
        classCode: 'LANDFILL',
        maxRecords: 20000,
        mapper: a => ({
          source_id: `HIFLD-LF-${a['OBJECTID'] || a['ID']}`,
          name: a['NAME'] || a['FACILITY_NAME'],
          address: a['ADDRESS'],
          city: a['CITY'],
          state: a['STATE'],
          zip: a['ZIP'],
        })
      });
    }
  },

  // ---------- HIFLD Chemical Storage Facilities ----------
  hifld_chemical: {
    name: 'HIFLD Chemical Storage',
    async run() {
      await importArcGIS({
        url: 'https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Chemical_Storage_Facilities/FeatureServer/0/query',
        dbName: 'HIFLD Chemical Storage',
        category: 'industrial',
        classCode: 'CHEMICAL_STORAGE',
        maxRecords: 50000,
        mapper: a => ({
          source_id: `HIFLD-CHEM-${a['OBJECTID'] || a['ID']}`,
          name: a['NAME'] || a['FACILITY_NAME'],
          address: a['ADDRESS'],
          city: a['CITY'],
          state: a['STATE'],
          zip: a['ZIP'],
        })
      });
    }
  },

  // ---------- HIFLD Petroleum Product Pipelines ----------
  hifld_pipelines: {
    name: 'HIFLD Petroleum Pipelines',
    async run() {
      await importArcGIS({
        url: 'https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Petroleum_Gas_Pipelines/FeatureServer/0/query',
        dbName: 'HIFLD Petroleum Pipelines',
        category: 'industrial',
        classCode: 'PETROLEUM_PIPELINE',
        maxRecords: 50000,
        mapper: a => {
          if (!a['OBJECTID']) return null;
          return {
            source_id: `HIFLD-PIPE-${a['OBJECTID']}`,
            name: a['OPERATOR'] || a['NAME'] || `Pipeline ${a['OBJECTID']}`,
            address: null, city: null, state: a['STATE'], zip: null,
          };
        }
      });
    }
  },

};

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  const args = process.argv.slice(2);

  if (args.includes('--list')) {
    console.log('Available sources:');
    for (const [k, v] of Object.entries(SOURCES)) console.log(`  ${k.padEnd(25)} ${v.name}`);
    process.exit(0);
  }

  const onlyArg = args.find(a => a.startsWith('--only='));
  const only = onlyArg ? onlyArg.split('=')[1].split(',') : null;
  const skipArg = args.find(a => a.startsWith('--skip='));
  const skip = skipArg ? skipArg.split('=')[1].split(',') : [];

  const toRun = Object.entries(SOURCES).filter(([k]) => {
    if (only && !only.includes(k)) return false;
    if (skip.includes(k)) return false;
    return true;
  });

  console.log(`\n🚀 GeoScope Extra Sources: running ${toRun.length} sources`);

  for (const [key, source] of toRun) {
    try {
      await source.run();
    } catch(e) {
      console.log(`\n❌ ${source.name}: ${e.message}`);
    }
  }

  // Final count
  const res = await pool.query('SELECT COUNT(*)::bigint AS total, COUNT(DISTINCT database_name)::bigint AS datasets FROM environmental_sites');
  console.log(`\n📊 Final DB stats: ${res.rows[0].total} total records, ${res.rows[0].datasets} distinct datasets`);
  await pool.end();
}

main().catch(e => { console.error(e.message); process.exit(1); });
