#!/usr/bin/env node
// =============================================================================
// import-new-sources.js
// Import new high-volume federal datasets not covered by extra-sources.js
// Sources: CMS Nursing Homes, NCES Schools, CMS Home Health, BLM Oil/Gas Wells,
//          OpenFEMA NFIP policies, EPA RMP via ECHO, USGS GNIS places
// =============================================================================
require('dotenv').config({ path: require('path').join(__dirname, '../.env') });

const https = require('https');
const http  = require('http');
const fs    = require('fs');
const path  = require('path');
const { parse } = require('csv-parse');
const { Pool } = require('pg');

const DOWNLOAD_DIR = path.join(__dirname, '../downloads/new-sources');
if (!fs.existsSync(DOWNLOAD_DIR)) fs.mkdirSync(DOWNLOAD_DIR, { recursive: true });

const BATCH_SIZE = 500;

// ── DB pool ────────────────────────────────────────────────────────────────
const pool = new Pool({
  host:     process.env.PG_HOST,
  port:     parseInt(process.env.PG_PORT || '5432'),
  database: process.env.PG_DATABASE,
  user:     process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  max: 5,
});

// ── HTTP helpers ───────────────────────────────────────────────────────────
function download(url, destPath, label = '') {
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
        return download(res.headers.location, destPath, label).then(resolve).catch(reject);
      }
      if (res.statusCode !== 200) {
        file.close();
        fs.unlink(tmp, () => {});
        return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
      }
      let bytes = 0;
      res.on('data', chunk => {
        bytes += chunk.length;
        if (bytes % (5 * 1024 * 1024) < chunk.length)
          process.stdout.write(`\r  Downloading ${label}... ${(bytes/1024/1024).toFixed(1)} MB`);
      });
      res.pipe(file);
      file.on('finish', () => {
        file.close(() => {
          fs.renameSync(tmp, destPath);
          process.stdout.write(`\r  Downloaded ${label}: ${(bytes/1024/1024).toFixed(1)} MB\n`);
          resolve(destPath);
        });
      });
    });
    req.setTimeout(180000, () => { req.destroy(); reject(new Error('Timeout')); });
    req.on('error', err => { file.close(); fs.unlink(tmp, () => {}); reject(err); });
  });
}

function fetchJSON(url) {
  return new Promise((resolve, reject) => {
    const proto = url.startsWith('https') ? https : http;
    let data = '';
    proto.get(url, { headers: { 'User-Agent': 'GeoScope/1.0', 'Accept': 'application/json' } }, res => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location)
        return fetchJSON(res.headers.location).then(resolve).catch(reject);
      if (res.statusCode !== 200) return reject(new Error(`HTTP ${res.statusCode}`));
      res.on('data', c => data += c);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); }
        catch(e) { reject(new Error(`JSON parse: ${e.message}`)); }
      });
    }).on('error', reject).setTimeout(60000, function() { this.destroy(); reject(new Error('Timeout')); });
  });
}

// ── CSV parser ─────────────────────────────────────────────────────────────
function parseCSV(filePath, delimiter = ',') {
  return new Promise((resolve, reject) => {
    const rows = [];
    fs.createReadStream(filePath)
      .pipe(parse({ columns: true, skip_empty_lines: true, delimiter, relax_quotes: true, trim: true }))
      .on('data', r => rows.push(r))
      .on('end', () => resolve(rows))
      .on('error', reject);
  });
}

// ── Batch insert ───────────────────────────────────────────────────────────
async function batchInsert(rows) {
  if (!rows.length) return 0;
  let inserted = 0;
  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const chunk = rows.slice(i, i + BATCH_SIZE);
    const vals = [], params = [];
    let p = 1;
    for (const r of chunk) {
      vals.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},ST_SetSRID(ST_MakePoint($${p++},$${p++}),4326))`);
      params.push(r.source_id, r.name, r.address, r.city, r.state, r.zip,
                  r.database_name, r.category, r.class_code, r.lon, r.lat);
    }
    const sql = `INSERT INTO environmental_sites
      (source_id,site_name,address,city,state,zip,database_name,category,class_code,location)
      VALUES ${vals.join(',')} ON CONFLICT (source_id) DO NOTHING`;
    try {
      const res = await pool.query(sql, params);
      inserted += res.rowCount;
    } catch(e) {
      // row-by-row fallback
      for (const r of chunk) {
        try {
          const res = await pool.query(
            `INSERT INTO environmental_sites (source_id,site_name,address,city,state,zip,database_name,category,class_code,location)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,ST_SetSRID(ST_MakePoint($10,$11),4326))
             ON CONFLICT (source_id) DO NOTHING`,
            [r.source_id, r.name, r.address, r.city, r.state, r.zip,
             r.database_name, r.category, r.class_code, r.lon, r.lat]
          );
          inserted += res.rowCount;
        } catch(e2) { /* skip */ }
      }
    }
    process.stdout.write(`\r  Rows: ${Math.min(i + BATCH_SIZE, rows.length).toLocaleString()}/${rows.length.toLocaleString()} inserted=${inserted.toLocaleString()}`);
  }
  process.stdout.write('\n');
  return inserted;
}

// ── Paginated CMS API ───────────────────────────────────────────────────────
async function fetchCMSPages(endpoint, dbName, category, classCode, idField, prefix) {
  const PAGE_SIZE = 500;
  let offset = 0;
  const rows = [];
  while (true) {
    const url = `https://data.cms.gov/provider-data/api/1/datastore/query/${endpoint}/0?limit=${PAGE_SIZE}&offset=${offset}&results_format=json&count=false&schema=false`;
    let result;
    try {
      result = await fetchJSON(url);
    } catch(e) {
      console.log(`\n  Fetch error at offset ${offset}: ${e.message}`);
      break;
    }
    const pageRows = result.results || result.data || [];
    if (!pageRows.length) break;
    rows.push(...pageRows);
    offset += pageRows.length;
    process.stdout.write(`\r  Fetched ${rows.length.toLocaleString()} records...`);
    if (pageRows.length < PAGE_SIZE) break;
    await new Promise(r => setTimeout(r, 200));
  }
  process.stdout.write('\n');
  console.log(`  Total fetched: ${rows.length.toLocaleString()}`);
  return rows;
}

// ══════════════════════════════════════════════════════════════════════════════
// SOURCES
// ══════════════════════════════════════════════════════════════════════════════

const SOURCES = {

  // ── CMS Nursing Homes (already downloaded CSV) ─────────────────────────
  nursing_homes: {
    name: 'CMS Nursing Homes',
    async run() {
      console.log(`\n🏥 CMS Nursing Homes`);
      const csvFile = path.join(__dirname, '../downloads/missing/NURSING_HOMES/cms_nh_facilities.csv');
      if (!fs.existsSync(csvFile)) {
        console.log('  ❌ CSV not found — run fetch-cms-nursing-homes.js first');
        return;
      }
      const records = await parseCSV(csvFile);
      console.log(`  Rows: ${records.length.toLocaleString()}`);
      const rows = records.map(r => ({
        source_id:     r.source_id || `NH-${r.name}-${r.state}`,
        name:          r.name || null,
        address:       r.address || null,
        city:          r.city || null,
        state:         r.state || null,
        zip:           r.zip || null,
        database_name: 'CMS Nursing Homes',
        category:      'healthcare',
        class_code:    'NURSING_HOME',
        lat:           parseFloat(r.latitude),
        lon:           parseFloat(r.longitude),
      })).filter(r => isFinite(r.lat) && r.lat !== 0 && isFinite(r.lon));
      const ins = await batchInsert(rows);
      console.log(`  ✅ CMS Nursing Homes: ${ins.toLocaleString()} inserted`);
    }
  },

  // ── CMS Home Health Agencies ────────────────────────────────────────────
  home_health: {
    name: 'CMS Home Health Agencies',
    async run() {
      console.log(`\n🏠 CMS Home Health Agencies`);
      // CMS dataset ID for Home Health Agencies
      const rows = await fetchCMSPages('6jpm-sxkc', 'CMS Home Health', 'healthcare', 'HOME_HEALTH', 'cms_certification_number_ccn', 'HH');
      const mapped = rows.map(r => {
        const lat = parseFloat(r.latitude || r.lat);
        const lon = parseFloat(r.longitude || r.lon);
        if (!isFinite(lat) || lat === 0) return null;
        return {
          source_id:     `HH-${r.cms_certification_number_ccn || r.ccn || lat+'_'+lon}`,
          name:          r.agency_name || r.provider_name || r.name || null,
          address:       r.address || r.provider_address || null,
          city:          r.city || r.citytown || null,
          state:         r.state || null,
          zip:           r.zip_code || r.zip || null,
          database_name: 'CMS Home Health',
          category:      'healthcare',
          class_code:    'HOME_HEALTH',
          lat, lon,
        };
      }).filter(Boolean);
      const ins = await batchInsert(mapped);
      console.log(`  ✅ CMS Home Health Agencies: ${ins.toLocaleString()} inserted`);
    }
  },

  // ── CMS Hospice Providers ───────────────────────────────────────────────
  hospice: {
    name: 'CMS Hospice Providers',
    async run() {
      console.log(`\n🕊️  CMS Hospice Providers`);
      const rows = await fetchCMSPages('252m-zfp9', 'CMS Hospice', 'healthcare', 'HOSPICE', 'cms_certification_number_ccn', 'HSP');
      const mapped = rows.map(r => {
        const lat = parseFloat(r.latitude || r.lat);
        const lon = parseFloat(r.longitude || r.lon);
        if (!isFinite(lat) || lat === 0) return null;
        return {
          source_id:     `HSP-${r.cms_certification_number_ccn || r.ccn || lat+'_'+lon}`,
          name:          r.facility_name || r.provider_name || r.name || null,
          address:       r.address || r.provider_address || null,
          city:          r.city || r.citytown || null,
          state:         r.state || null,
          zip:           r.zip_code || r.zip || null,
          database_name: 'CMS Hospice',
          category:      'healthcare',
          class_code:    'HOSPICE',
          lat, lon,
        };
      }).filter(Boolean);
      const ins = await batchInsert(mapped);
      console.log(`  ✅ CMS Hospice Providers: ${ins.toLocaleString()} inserted`);
    }
  },

  // ── CMS Dialysis Facilities ─────────────────────────────────────────────
  dialysis: {
    name: 'CMS Dialysis Facilities',
    async run() {
      console.log(`\n💉 CMS Dialysis Facilities`);
      const rows = await fetchCMSPages('23ew-n7w9', 'CMS Dialysis', 'healthcare', 'DIALYSIS', 'provider_number', 'DLY');
      const mapped = rows.map(r => {
        const lat = parseFloat(r.latitude || r.lat);
        const lon = parseFloat(r.longitude || r.lon);
        if (!isFinite(lat) || lat === 0) return null;
        return {
          source_id:     `DLY-${r.provider_number || r.cms_certification_number_ccn || lat+'_'+lon}`,
          name:          r.facility_name || r.provider_name || r.name || null,
          address:       r.address || r.provider_address || null,
          city:          r.city || r.citytown || null,
          state:         r.state || null,
          zip:           r.zip_code || r.zip || null,
          database_name: 'CMS Dialysis',
          category:      'healthcare',
          class_code:    'DIALYSIS',
          lat, lon,
        };
      }).filter(Boolean);
      const ins = await batchInsert(mapped);
      console.log(`  ✅ CMS Dialysis Facilities: ${ins.toLocaleString()} inserted`);
    }
  },

  // ── NCES Public Schools (EDGE geocoded) ─────────────────────────────────
  nces_schools: {
    name: 'NCES Public Schools',
    async run() {
      console.log(`\n🏫 NCES Public Schools`);
      const csvFile = path.join(DOWNLOAD_DIR, 'nces_public_schools.csv');
      // NCES EDGE Geocoded Schools CSV (FY2022-23)
      const urls = [
        'https://nces.ed.gov/programs/edge/data/EDGE_GEOCODE_PUBLICSCH_2223.zip',
        'https://nces.ed.gov/programs/edge/data/EDGE_GEOCODE_PUBLICSCH_2122.zip',
        // Fallback: NCES CCD public school universe survey CSV
        'https://nces.ed.gov/ccd/data/csv/ccd_sch_029_2122_w_1a_050123.csv',
        'https://nces.ed.gov/ccd/data/csv/ccd_sch_029_2021_l_1a_080421.csv',
      ];

      let downloaded = null;
      for (const url of urls) {
        const fname = path.join(DOWNLOAD_DIR, 'nces_schools_raw' + path.extname(url));
        try {
          await download(url, fname, 'NCES Schools');
          if (fs.statSync(fname).size > 50000) { downloaded = fname; break; }
        } catch(e) { console.log(`  Attempt: ${e.message}`); }
      }

      if (!downloaded) {
        // Try ArcGIS REST API for schools
        console.log('  Trying ArcGIS NCES schools...');
        const arcUrl = 'https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Public_Schools/FeatureServer/0/query?where=1%3D1&outFields=NCESSCH,NAME,STREET,CITY,STATE,ZIP,LAT,LON&f=json&resultOffset=0&resultRecordCount=2000';
        try {
          const data = await fetchJSON(arcUrl);
          const features = data.features || [];
          console.log(`  ArcGIS fetched ${features.length} schools`);
          const rows = features.map(f => {
            const a = f.attributes || {};
            const lat = a.LAT || (f.geometry && f.geometry.y);
            const lon = a.LON || (f.geometry && f.geometry.x);
            if (!lat || !lon) return null;
            return {
              source_id: `NCES-${a.NCESSCH || lat+'_'+lon}`,
              name: a.NAME || null,
              address: a.STREET || null,
              city: a.CITY || null,
              state: a.STATE || null,
              zip: a.ZIP || null,
              database_name: 'NCES Public Schools',
              category: 'education',
              class_code: 'PUBLIC_SCHOOL',
              lat: parseFloat(lat),
              lon: parseFloat(lon),
            };
          }).filter(Boolean);
          const ins = await batchInsert(rows);
          console.log(`  ✅ NCES Schools (ArcGIS partial): ${ins.toLocaleString()} inserted`);
          return;
        } catch(e) { console.log(`  ArcGIS error: ${e.message}`); }
        console.log('  ❌ NCES Schools: all sources failed');
        return;
      }

      // If it's a ZIP, extract first CSV
      if (downloaded.endsWith('.zip')) {
        const { execSync } = require('child_process');
        try {
          execSync(`powershell -Command "Expand-Archive -Path '${downloaded}' -DestinationPath '${DOWNLOAD_DIR}\\nces_extract' -Force"`, { stdio: 'pipe' });
          const files = fs.readdirSync(path.join(DOWNLOAD_DIR, 'nces_extract')).filter(f => f.endsWith('.csv'));
          if (files.length > 0) downloaded = path.join(DOWNLOAD_DIR, 'nces_extract', files[0]);
        } catch(e) { console.log(`  Zip extract error: ${e.message}`); return; }
      }

      const records = await parseCSV(downloaded);
      console.log(`  Rows: ${records.length.toLocaleString()}`);
      const hdr = Object.keys(records[0] || {});
      const latK = hdr.find(k => ['LAT', 'LATITUDE', 'lat', 'latitude'].includes(k));
      const lonK = hdr.find(k => ['LON', 'LONGITUDE', 'lon', 'longitude'].includes(k));
      const idK  = hdr.find(k => ['NCESSCH', 'SCHOOL_ID', 'ncessch'].includes(k));
      const nmK  = hdr.find(k => ['SCHOOL', 'NAME', 'school_name', 'SCHNAM'].includes(k));

      const rows = records.map(r => {
        const lat = parseFloat(latK ? r[latK] : 0);
        const lon = parseFloat(lonK ? r[lonK] : 0);
        if (!isFinite(lat) || lat === 0) return null;
        return {
          source_id: `NCES-${idK ? r[idK] : lat+'_'+lon}`,
          name:      nmK ? r[nmK] : null,
          address:   r['LSTREE'] || r['address'] || r['STREET'] || null,
          city:      r['LCITY'] || r['city'] || r['CITY'] || null,
          state:     r['LSTATE'] || r['state'] || r['STATE'] || null,
          zip:       r['LZIP'] || r['zip'] || r['ZIP'] || null,
          database_name: 'NCES Public Schools',
          category:  'education',
          class_code:'PUBLIC_SCHOOL',
          lat, lon,
        };
      }).filter(Boolean);
      const ins = await batchInsert(rows);
      console.log(`  ✅ NCES Public Schools: ${ins.toLocaleString()} inserted`);
    }
  },

  // ── NCES Colleges (IPEDS) ───────────────────────────────────────────────
  nces_colleges: {
    name: 'NCES Colleges',
    async run() {
      console.log(`\n🎓 NCES Colleges & Universities (IPEDS)`);
      const urls = [
        'https://nces.ed.gov/programs/edge/data/EDGE_GEOCODE_POSTSECSCH_2223.zip',
        'https://nces.ed.gov/programs/edge/data/EDGE_GEOCODE_POSTSECSCH_2122.zip',
      ];
      let downloaded = null;
      for (const url of urls) {
        const fname = path.join(DOWNLOAD_DIR, 'nces_colleges_raw.zip');
        try {
          await download(url, fname, 'NCES Colleges');
          if (fs.statSync(fname).size > 10000) { downloaded = fname; break; }
        } catch(e) { console.log(`  Attempt: ${e.message}`); }
      }
      if (!downloaded) {
        // Fallback: HIFLD Colleges ArcGIS
        console.log('  Trying HIFLD Colleges ArcGIS...');
        try {
          const url = 'https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Colleges_and_Universities/FeatureServer/0/query?where=1%3D1&outFields=UNITID,NAME,ADDRESS,CITY,STATE,ZIP,LATITUDE,LONGITUDE&f=json&resultOffset=0&resultRecordCount=2000';
          const data = await fetchJSON(url);
          const features = data.features || [];
          console.log(`  HIFLD fetched ${features.length} colleges`);
          const rows = features.map(f => {
            const a = f.attributes || {};
            const lat = a.LATITUDE || (f.geometry && f.geometry.y);
            const lon = a.LONGITUDE || (f.geometry && f.geometry.x);
            if (!lat || !lon) return null;
            return {
              source_id: `COL-${a.UNITID || lat+'_'+lon}`,
              name: a.NAME || null,
              address: a.ADDRESS || null,
              city: a.CITY || null,
              state: a.STATE || null,
              zip: a.ZIP || null,
              database_name: 'NCES Colleges',
              category: 'education',
              class_code: 'COLLEGE',
              lat: parseFloat(lat),
              lon: parseFloat(lon),
            };
          }).filter(Boolean);
          const ins = await batchInsert(rows);
          console.log(`  ✅ NCES Colleges (HIFLD): ${ins.toLocaleString()} inserted`);
          return;
        } catch(e) { console.log(`  HIFLD error: ${e.message}`); }
        console.log('  ❌ NCES Colleges: all sources failed');
        return;
      }
      // Extract and parse
      const { execSync } = require('child_process');
      try {
        execSync(`powershell -Command "Expand-Archive -Path '${downloaded}' -DestinationPath '${DOWNLOAD_DIR}\\nces_college_extract' -Force"`, { stdio: 'pipe' });
        const files = fs.readdirSync(path.join(DOWNLOAD_DIR, 'nces_college_extract')).filter(f => f.endsWith('.csv'));
        if (!files.length) { console.log('  ❌ No CSV in ZIP'); return; }
        const csvFile = path.join(DOWNLOAD_DIR, 'nces_college_extract', files[0]);
        const records = await parseCSV(csvFile);
        console.log(`  Rows: ${records.length.toLocaleString()}`);
        const hdr = Object.keys(records[0] || {});
        const latK = hdr.find(k => ['LAT', 'LATITUDE'].includes(k));
        const lonK = hdr.find(k => ['LON', 'LONGITUDE'].includes(k));
        const rows = records.map(r => {
          const lat = parseFloat(latK ? r[latK] : 0);
          const lon = parseFloat(lonK ? r[lonK] : 0);
          if (!isFinite(lat) || lat === 0) return null;
          return {
            source_id: `COL-${r['UNITID'] || r['unitid'] || lat+'_'+lon}`,
            name:      r['INSTNM'] || r['NAME'] || null,
            address:   r['ADDR'] || r['address'] || null,
            city:      r['CITY'] || null,
            state:     r['STABBR'] || r['STATE'] || null,
            zip:       r['ZIP'] || null,
            database_name: 'NCES Colleges',
            category:  'education',
            class_code:'COLLEGE',
            lat, lon,
          };
        }).filter(Boolean);
        const ins = await batchInsert(rows);
        console.log(`  ✅ NCES Colleges: ${ins.toLocaleString()} inserted`);
      } catch(e) { console.log(`  Error: ${e.message}`); }
    }
  },

  // ── EPA RMP via ECHO (corrected endpoint) ──────────────────────────────
  epa_rmp: {
    name: 'EPA RMP Facilities',
    async run() {
      console.log(`\n☣️  EPA Risk Management Plan Facilities`);
      // Try EPA ECHO FRS query with p_ptype=RM
      const urls = [
        'https://echodata.epa.gov/echo/rest_lookups.get_facilities?output=JSON&p_ptype=RM&per_page=1000&pageNo=1',
        'https://echo.epa.gov/rest/services/rcra/rest_lookups/facilities?output=JSON&p_ptype=RM&per_page=1000',
        // Direct RMP CSV from EPA
        'https://www.epa.gov/sites/default/files/2021-03/rmp_facilities.csv',
      ];
      for (const url of urls) {
        const fname = path.join(DOWNLOAD_DIR, 'rmp_facilities' + (url.endsWith('.csv') ? '.csv' : '_test.json'));
        try {
          await download(url, fname, 'RMP');
          const stat = fs.statSync(fname);
          if (stat.size < 5000) { fs.unlinkSync(fname); continue; }
          if (url.endsWith('.csv')) {
            const records = await parseCSV(fname);
            console.log(`  Rows: ${records.length.toLocaleString()}`);
            const hdr = Object.keys(records[0] || {});
            const latK = hdr.find(k => /lat/i.test(k));
            const lonK = hdr.find(k => /lon|lng/i.test(k));
            const idK  = hdr.find(k => /rmp_id|facility_id|id/i.test(k));
            const rows = records.map(r => {
              const lat = parseFloat(latK ? r[latK] : 0);
              const lon = parseFloat(lonK ? r[lonK] : 0);
              if (!isFinite(lat) || lat === 0) return null;
              return {
                source_id: `RMP-${idK ? r[idK] : lat+'_'+lon}`,
                name: r['FACILITY_NAME'] || r['facility_name'] || r['name'] || null,
                address: r['STREET_1'] || r['address'] || null,
                city: r['CITY'] || r['city'] || null,
                state: r['STATE'] || r['state'] || null,
                zip: r['ZIP'] || r['zip'] || null,
                database_name: 'EPA RMP',
                category: 'hazardous',
                class_code: 'RMP_FACILITY',
                lat, lon,
              };
            }).filter(Boolean);
            const ins = await batchInsert(rows);
            console.log(`  ✅ EPA RMP: ${ins.toLocaleString()} inserted`);
            return;
          }
        } catch(e) { console.log(`  Attempt: ${e.message}`); }
      }
      // Fallback: HIFLD EPA RMP facilities
      console.log('  Trying HIFLD RMP ArcGIS...');
      try {
        const baseUrl = 'https://services2.arcgis.com/FiaFA4Ngl3cptdEA/arcgis/rest/services/RMPs/FeatureServer/0/query';
        let offset = 0;
        const allRows = [];
        while (true) {
          const url = `${baseUrl}?where=1%3D1&outFields=FacilityID,FacilityName,StreetAddress,City,State,Zip,Latitude,Longitude&f=json&resultOffset=${offset}&resultRecordCount=1000`;
          const data = await fetchJSON(url);
          const features = data.features || [];
          if (!features.length) break;
          allRows.push(...features);
          offset += features.length;
          process.stdout.write(`\r  ArcGIS RMP: ${allRows.length} features`);
          if (!data.exceededTransferLimit) break;
          await new Promise(r => setTimeout(r, 300));
        }
        process.stdout.write('\n');
        const rows = allRows.map(f => {
          const a = f.attributes || {};
          const lat = a.Latitude || (f.geometry && f.geometry.y);
          const lon = a.Longitude || (f.geometry && f.geometry.x);
          if (!lat || !lon) return null;
          return {
            source_id: `RMP-${a.FacilityID || lat+'_'+lon}`,
            name: a.FacilityName || null,
            address: a.StreetAddress || null,
            city: a.City || null,
            state: a.State || null,
            zip: a.Zip || null,
            database_name: 'EPA RMP',
            category: 'hazardous',
            class_code: 'RMP_FACILITY',
            lat: parseFloat(lat),
            lon: parseFloat(lon),
          };
        }).filter(Boolean);
        const ins = await batchInsert(rows);
        console.log(`  ✅ EPA RMP (HIFLD): ${ins.toLocaleString()} inserted`);
      } catch(e) { console.log(`  HIFLD RMP error: ${e.message}`); }
    }
  },

  // ── UST via OpenData (ArcGIS alternative) ──────────────────────────────
  ust: {
    name: 'EPA Underground Storage Tanks',
    async run() {
      console.log(`\n⛽ EPA Underground Storage Tanks`);
      // Try HIFLD UST ArcGIS endpoint
      const arcgisUrls = [
        'https://services2.arcgis.com/FiaFA4Ngl3cptdEA/arcgis/rest/services/UST/FeatureServer/0/query',
        'https://geo.epa.gov/arcgis/rest/services/OUST/UST_Facilities/MapServer/0/query',
        'https://services.arcgis.com/cJ9YHowT8TU7DUyn/arcgis/rest/services/UST_Facility/FeatureServer/0/query',
      ];
      for (const baseUrl of arcgisUrls) {
        try {
          let offset = 0, allRows = [];
          while (true) {
            const url = `${baseUrl}?where=1%3D1&outFields=*&f=json&resultOffset=${offset}&resultRecordCount=1000`;
            const data = await fetchJSON(url);
            const features = data.features || [];
            if (!features.length) break;
            allRows.push(...features);
            offset += features.length;
            process.stdout.write(`\r  ArcGIS UST: ${allRows.length} features`);
            if (!data.exceededTransferLimit) break;
            await new Promise(r => setTimeout(r, 300));
          }
          process.stdout.write('\n');
          if (!allRows.length) continue;
          const rows = allRows.map(f => {
            const a = f.attributes || {};
            const lat = a.LATITUDE || a.Latitude || a.lat || (f.geometry && f.geometry.y);
            const lon = a.LONGITUDE || a.Longitude || a.lon || (f.geometry && f.geometry.x);
            if (!lat || !lon) return null;
            return {
              source_id: `UST-${a.REGISTRY_ID || a.FacilityID || a.OBJECTID || lat+'_'+lon}`,
              name: a.PRIMARY_NAME || a.FacilityName || a.NAME || null,
              address: a.LOCATION_ADDRESS || a.StreetAddress || a.ADDRESS || null,
              city: a.CITY_NAME || a.City || a.CITY || null,
              state: a.STATE_CODE || a.State || a.STATE || null,
              zip: a.POSTAL_CODE || a.Zip || a.ZIP || null,
              database_name: 'EPA UST',
              category: 'contamination',
              class_code: 'UST',
              lat: parseFloat(lat),
              lon: parseFloat(lon),
            };
          }).filter(Boolean);
          const ins = await batchInsert(rows);
          console.log(`  ✅ EPA UST (ArcGIS): ${ins.toLocaleString()} inserted`);
          return;
        } catch(e) { console.log(`  ArcGIS attempt: ${e.message}`); }
      }

      // Fallback: EPA ECHO FRS corrected endpoint state-by-state
      console.log('  Trying EPA ECHO FRS state-by-state for UST...');
      const states = ['AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA',
                      'KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
                      'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT',
                      'VA','WA','WV','WI','WY'];
      let total = 0;
      for (const st of states) {
        try {
          const url = `https://echodata.epa.gov/echo/rest_lookups.get_facilities?output=JSON&p_st=${st}&p_ptype=UST&per_page=1000&pageNo=1`;
          const data = await fetchJSON(url);
          const facs = data.Results?.facilities || data.facilities || data.results || [];
          if (!facs.length) continue;
          const rows = facs.map(f => {
            const lat = parseFloat(f.FacLat || f.lat || 0);
            const lon = parseFloat(f.FacLong || f.lon || 0);
            if (!isFinite(lat) || lat === 0) return null;
            return {
              source_id: `UST-${f.RegistryID || f.FacilityID || lat+'_'+lon}`,
              name: f.FacName || f.name || null,
              address: f.FacStreet || f.address || null,
              city: f.FacCity || f.city || null,
              state: f.FacState || st,
              zip: f.FacZip || f.zip || null,
              database_name: 'EPA UST',
              category: 'contamination',
              class_code: 'UST',
              lat, lon,
            };
          }).filter(Boolean);
          const ins = await batchInsert(rows);
          total += ins;
          process.stdout.write(`\r  ECHO UST ${st}: +${ins} (total=${total})`);
          await new Promise(r => setTimeout(r, 300));
        } catch(e) { process.stdout.write(`\r  ${st} error: ${e.message.substring(0,40)}`); }
      }
      process.stdout.write('\n');
      console.log(`  ✅ EPA UST (ECHO): ${total.toLocaleString()} inserted`);
    }
  },

  // ── USGS GNIS Geographic Names (populated places with coords) ──────────
  usgs_gnis: {
    name: 'USGS GNIS Places',
    async run() {
      console.log(`\n📍 USGS GNIS Geographic Names — Populated Places`);
      // USGS Geographic Names Information System national file
      const urls = [
        'https://prd-tnm.s3.amazonaws.com/StagedProducts/GeographicNames/DomesticNames/NationalFile.zip',
        'https://geonames.usgs.gov/docs/stategaz/NationalFile.zip',
      ];
      let downloaded = null;
      for (const url of urls) {
        const fname = path.join(DOWNLOAD_DIR, 'gnis_national.zip');
        try {
          await download(url, fname, 'GNIS National');
          if (fs.statSync(fname).size > 100000) { downloaded = fname; break; }
        } catch(e) { console.log(`  Attempt: ${e.message}`); }
      }
      if (!downloaded) {
        console.log('  ❌ GNIS: download failed');
        return;
      }
      const { execSync } = require('child_process');
      try {
        execSync(`powershell -Command "Expand-Archive -Path '${downloaded}' -DestinationPath '${DOWNLOAD_DIR}\\gnis_extract' -Force"`, { stdio: 'pipe' });
        const files = fs.readdirSync(path.join(DOWNLOAD_DIR, 'gnis_extract')).filter(f => f.endsWith('.txt') || f.endsWith('.csv'));
        if (!files.length) { console.log('  ❌ No text file in GNIS ZIP'); return; }
        const txtFile = path.join(DOWNLOAD_DIR, 'gnis_extract', files[0]);
        // GNIS is pipe-delimited; filter to populated places only
        const records = await parseCSV(txtFile, '|');
        const places = records.filter(r => (r['FEATURE_CLASS'] || '').toLowerCase().includes('populated'));
        console.log(`  Total rows: ${records.length.toLocaleString()}, populated places: ${places.length.toLocaleString()}`);
        const rows = places.map(r => {
          const lat = parseFloat(r['PRIM_LAT_DEC'] || r['lat'] || 0);
          const lon = parseFloat(r['PRIM_LONG_DEC'] || r['lon'] || 0);
          if (!isFinite(lat) || lat === 0) return null;
          return {
            source_id: `GNIS-${r['FEATURE_ID'] || lat+'_'+lon}`,
            name:      r['FEATURE_NAME'] || null,
            address:   null,
            city:      r['COUNTY_NAME'] || null,
            state:     r['STATE_ALPHA'] || null,
            zip:       null,
            database_name: 'USGS GNIS',
            category:  'geographic',
            class_code:'POPULATED_PLACE',
            lat, lon,
          };
        }).filter(Boolean);
        const ins = await batchInsert(rows);
        console.log(`  ✅ USGS GNIS Populated Places: ${ins.toLocaleString()} inserted`);
      } catch(e) { console.log(`  Error: ${e.message}`); }
    }
  },

  // ── BLM Active Oil & Gas Wells ──────────────────────────────────────────
  blm_oil_gas: {
    name: 'BLM Oil & Gas Wells',
    async run() {
      console.log(`\n🛢️  BLM Active Oil & Gas Wells`);
      const urls = [
        'https://services.arcgis.com/P3ePLMYs2RVChkJx/arcgis/rest/services/BLM_Natl_Oil_Gas_Well_Points/FeatureServer/0/query',
        'https://services5.arcgis.com/FCN3uyRHHFHiMnLH/arcgis/rest/services/BLM_Oil_Gas_Wells/FeatureServer/0/query',
      ];
      for (const baseUrl of urls) {
        try {
          let offset = 0, allRows = [], pages = 0;
          while (pages < 100) {
            const url = `${baseUrl}?where=1%3D1&outFields=CASENO,WELLNAME,OPERATOR,STATECODE,COUNTY,WELLTYPE&f=json&resultOffset=${offset}&resultRecordCount=1000&geometryType=esriGeometryPoint&returnGeometry=true`;
            const data = await fetchJSON(url);
            const features = data.features || [];
            if (!features.length) break;
            allRows.push(...features);
            offset += features.length;
            pages++;
            process.stdout.write(`\r  BLM Wells: ${allRows.length.toLocaleString()} features`);
            if (!data.exceededTransferLimit) break;
            await new Promise(r => setTimeout(r, 300));
          }
          process.stdout.write('\n');
          if (!allRows.length) continue;
          const rows = allRows.map(f => {
            const a = f.attributes || {};
            const lat = f.geometry && f.geometry.y;
            const lon = f.geometry && f.geometry.x;
            if (!lat || !lon) return null;
            return {
              source_id: `BLM-WELL-${a.CASENO || a.OBJECTID || lat+'_'+lon}`,
              name: a.WELLNAME || a.OPERATOR || null,
              address: null,
              city: a.COUNTY || null,
              state: a.STATECODE || null,
              zip: null,
              database_name: 'BLM Oil Gas Wells',
              category: 'industrial',
              class_code: 'OIL_GAS_WELL',
              lat: parseFloat(lat),
              lon: parseFloat(lon),
            };
          }).filter(Boolean);
          const ins = await batchInsert(rows);
          console.log(`  ✅ BLM Oil & Gas Wells: ${ins.toLocaleString()} inserted`);
          return;
        } catch(e) { console.log(`  Attempt: ${e.message}`); }
      }
      console.log('  ❌ BLM Wells: all sources failed');
    }
  },

  // ── EPA TRI (via ECHO corrected endpoint) ──────────────────────────────
  tri_echo: {
    name: 'EPA TRI via ECHO',
    async run() {
      console.log(`\n🏭 EPA TRI Facilities (via ECHO FRS)`);
      const states = ['AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA',
                      'KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
                      'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT',
                      'VA','WA','WV','WI','WY'];
      let total = 0;
      for (const st of states) {
        try {
          const url = `https://echodata.epa.gov/echo/rest_lookups.get_facilities?output=JSON&p_st=${st}&p_ptype=TR&per_page=1000&pageNo=1`;
          const data = await fetchJSON(url);
          const facs = data.Results?.facilities || data.facilities || data.results || [];
          if (!facs.length) continue;
          const rows = facs.map(f => {
            const lat = parseFloat(f.FacLat || f.lat || 0);
            const lon = parseFloat(f.FacLong || f.lon || 0);
            if (!isFinite(lat) || lat === 0) return null;
            return {
              source_id: `TRI-ECHO-${f.RegistryID || f.FacilityID || lat+'_'+lon}`,
              name: f.FacName || f.name || null,
              address: f.FacStreet || f.address || null,
              city: f.FacCity || f.city || null,
              state: f.FacState || st,
              zip: f.FacZip || f.zip || null,
              database_name: 'EPA TRI',
              category: 'hazardous',
              class_code: 'TRI_FACILITY',
              lat, lon,
            };
          }).filter(Boolean);
          const ins = await batchInsert(rows);
          total += ins;
          process.stdout.write(`\r  ECHO TRI ${st}: +${ins} (total=${total})`);
          await new Promise(r => setTimeout(r, 300));
        } catch(e) { process.stdout.write(`\r  ${st} error: ${e.message.substring(0,40)}\n`); }
      }
      process.stdout.write('\n');
      console.log(`  ✅ EPA TRI (ECHO): ${total.toLocaleString()} inserted`);
    }
  },

};

// ══════════════════════════════════════════════════════════════════════════════
// MAIN
// ══════════════════════════════════════════════════════════════════════════════
async function main() {
  const args = process.argv.slice(2);
  const onlyArg = args.find(a => a.startsWith('--only='));
  const listArg = args.includes('--list');

  if (listArg) {
    console.log('\nAvailable sources:');
    for (const [k, v] of Object.entries(SOURCES))
      console.log(`  ${k.padEnd(25)} ${v.name}`);
    process.exit(0);
  }

  const toRun = onlyArg
    ? onlyArg.replace('--only=', '').split(',').filter(k => SOURCES[k])
    : Object.keys(SOURCES);

  console.log(`\n🌐 GeoScope New Sources: running ${toRun.length} sources\n`);

  for (const key of toRun) {
    try {
      await SOURCES[key].run();
    } catch(e) {
      console.log(`  ❌ ${key} ERROR: ${e.message}`);
    }
  }

  const { rows } = await pool.query('SELECT COUNT(*)::bigint AS c FROM environmental_sites');
  console.log(`\n📊 Final DB: ${Number(rows[0].c).toLocaleString()} total records`);
  await pool.end();
}

main().catch(e => { console.error(e); process.exit(1); });
