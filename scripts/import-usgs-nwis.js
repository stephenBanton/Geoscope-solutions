#!/usr/bin/env node
/**
 * Import USGS NWIS water sites (wells, springs, streams, lakes, etc.)
 * API: https://waterservices.usgs.gov/nwis/site/?format=rdb&stateCd=XX&siteStatus=all
 * Fields: agency_cd, site_no, station_nm, site_tp_cd, dec_lat_va, dec_long_va
 * Estimated: ~2M+ sites nationally across all types
 */
require('dotenv').config({ path: require('path').join(__dirname, '../.env') });

const https = require('https');
const { Pool } = require('pg');

const pool = new Pool({
  host: process.env.PG_HOST || 'localhost',
  port: parseInt(process.env.PG_PORT || '5432'),
  database: process.env.PG_DATABASE || 'geoscope',
  user: process.env.PG_USER || 'postgres',
  password: process.env.PG_PASSWORD,
  max: 5,
});

const STATES = [
  'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA',
  'HI','ID','IL','IN','IA','KS','KY','LA','ME','MD',
  'MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
  'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC',
  'SD','TN','TX','UT','VT','VA','WA','WV','WI','WY',
  'DC','PR','VI','GU','AS','MP'
];

// Site type codes → category/class
const SITE_TYPE_MAP = {
  GW: { category: 'hydrology', class: 'WELL', label: 'Groundwater Well' },
  SW: { category: 'hydrology', class: 'STRM', label: 'Stream' },
  SP: { category: 'hydrology', class: 'SPRG', label: 'Spring' },
  LK: { category: 'hydrology', class: 'LAKE', label: 'Lake' },
  WE: { category: 'hydrology', class: 'WETL', label: 'Wetland' },
  ES: { category: 'hydrology', class: 'ESTU', label: 'Estuary' },
  OC: { category: 'hydrology', class: 'OCEN', label: 'Ocean' },
  LA: { category: 'hydrology', class: 'LAND', label: 'Land' },
  AT: { category: 'hydrology', class: 'ATMO', label: 'Atmospheric' },
  GL: { category: 'hydrology', class: 'GLAC', label: 'Glacier' },
};

function fetchNWIS(stateCd) {
  return new Promise((resolve, reject) => {
    const url = `https://waterservices.usgs.gov/nwis/site/?format=rdb&stateCd=${stateCd}&siteStatus=all`;
    const req = https.get(url, { headers: { 'Accept': 'text/plain' } }, res => {
      if (res.statusCode === 302 || res.statusCode === 301) {
        const loc = res.headers.location;
        if (loc) { req.destroy(); return fetchNWIS_url(loc).then(resolve).catch(reject); }
      }
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        if (res.statusCode !== 200) return reject(new Error(`HTTP ${res.statusCode}`));
        resolve(data);
      });
    });
    req.on('error', reject);
    req.setTimeout(60000, () => { req.destroy(); reject(new Error('timeout')); });
  });
}

function fetchNWIS_url(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, { headers: { 'Accept': 'text/plain' } }, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        if (res.statusCode !== 200) return reject(new Error(`HTTP ${res.statusCode}`));
        resolve(data);
      });
    });
    req.on('error', reject);
    req.setTimeout(60000, () => { req.destroy(); reject(new Error('timeout')); });
  });
}

function parseRDB(text) {
  const lines = text.split('\n');
  // Skip comment lines starting with #
  const dataLines = lines.filter(l => !l.startsWith('#') && l.trim());
  if (dataLines.length < 2) return [];

  // First non-comment line is header, second is type spec
  const headers = dataLines[0].split('\t').map(h => h.trim().toLowerCase());
  // dataLines[1] is the type row - skip it
  const rows = [];

  for (let i = 2; i < dataLines.length; i++) {
    const cols = dataLines[i].split('\t');
    if (cols.length < 3) continue;
    const row = {};
    for (let j = 0; j < headers.length; j++) {
      row[headers[j]] = (cols[j] || '').trim();
    }
    rows.push(row);
  }
  return rows;
}

async function batchInsert(rows) {
  if (!rows.length) return 0;
  const CHUNK = 500;
  let inserted = 0;
  for (let i = 0; i < rows.length; i += CHUNK) {
    const chunk = rows.slice(i, i + CHUNK);
    const valParts = [];
    const params = [];
    let p = 1;
    for (const r of chunk) {
      if (r.lat != null && r.lon != null) {
        valParts.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},ST_SetSRID(ST_MakePoint($${p++},$${p++}),4326))`);
        params.push(r.source_id,r.site_name,r.address,r.city,r.state,r.zip,r.database_name,r.category,r.class_code,r.lon,r.lat);
      } else {
        valParts.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},NULL)`);
        params.push(r.source_id,r.site_name,r.address,r.city,r.state,r.zip,r.database_name,r.category,r.class_code);
      }
    }
    try {
      const res = await pool.query(
        `INSERT INTO environmental_sites (source_id,site_name,address,city,state,zip,database_name,category,class_code,location)
         VALUES ${valParts.join(',')} ON CONFLICT (source_id) DO NOTHING`,
        params
      );
      inserted += res.rowCount || 0;
    } catch (e) {
      // fallback row by row
      for (const r of chunk) {
        try {
          const res2 = await pool.query(
            `INSERT INTO environmental_sites (source_id,site_name,address,city,state,zip,database_name,category,class_code,location)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,${r.lat!=null&&r.lon!=null?'ST_SetSRID(ST_MakePoint($11,$10),4326)':'NULL'})
             ON CONFLICT (source_id) DO NOTHING`,
            r.lat!=null&&r.lon!=null
              ?[r.source_id,r.site_name,r.address,r.city,r.state,r.zip,r.database_name,r.category,r.class_code,r.lat,r.lon]
              :[r.source_id,r.site_name,r.address,r.city,r.state,r.zip,r.database_name,r.category,r.class_code]
          );
          inserted += res2.rowCount||0;
        } catch(_) {}
      }
    }
  }
  return inserted;
}

async function importState(stateCd) {
  let text;
  try {
    text = await fetchNWIS(stateCd);
  } catch (e) {
    console.log(`  ${stateCd} fetch error: ${e.message}`);
    return 0;
  }

  const rawRows = parseRDB(text);
  if (!rawRows.length) {
    console.log(`  ${stateCd}: 0 sites`);
    return 0;
  }

  const rows = [];
  for (const r of rawRows) {
    const siteNo = r.site_no || r['site_no'];
    const name = r.station_nm || '';
    const lat = parseFloat(r.dec_lat_va);
    const lon = parseFloat(r.dec_long_va);
    const siteType = (r.site_tp_cd || 'GW').trim();
    const typeInfo = SITE_TYPE_MAP[siteType] || SITE_TYPE_MAP['GW'];

    if (!siteNo) continue;

    rows.push({
      source_id: `NWIS-${siteNo}`,
      site_name: name || `USGS Site ${siteNo}`,
      address: null,
      city: null,
      state: stateCd,
      zip: null,
      database_name: `USGS NWIS ${typeInfo.label}`,
      category: typeInfo.category,
      class_code: typeInfo.class,
      lat: isFinite(lat) ? lat : null,
      lon: isFinite(lon) ? lon : null,
    });
  }

  const inserted = await batchInsert(rows);
  console.log(`  ${stateCd}: ${rawRows.length} sites fetched, ${inserted} inserted`);
  return inserted;
}

async function main() {
  const args = process.argv.slice(2);
  let states = STATES;

  if (args.includes('--only')) {
    const idx = args.indexOf('--only');
    states = args[idx+1].split(',').map(s => s.trim().toUpperCase());
  }

  console.log(`\n🌊 USGS NWIS Water Sites Import`);
  console.log(`   States to process: ${states.length}\n`);

  const startCount = await pool.query('SELECT COUNT(*) FROM environmental_sites');
  console.log(`   DB start: ${parseInt(startCount.rows[0].count).toLocaleString()}\n`);

  let totalInserted = 0;

  for (const state of states) {
    const n = await importState(state);
    totalInserted += n;
  }

  const endCount = await pool.query('SELECT COUNT(*) FROM environmental_sites');
  console.log(`\n✅ USGS NWIS complete`);
  console.log(`   Total inserted: ${totalInserted.toLocaleString()}`);
  console.log(`   DB final: ${parseInt(endCount.rows[0].count).toLocaleString()}`);

  await pool.end();
}

main().catch(e => { console.error(e); process.exit(1); });
