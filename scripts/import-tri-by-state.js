#!/usr/bin/env node
/**
 * TRI_FACILITY importer - state-by-state via EPA Envirofacts
 * Uses correct lowercase field names: state_abbr, fac_latitude, fac_longitude
 * Supports: --states=TX,CA,FL or --all (default all 50)
 *           --skip-al (skip Alabama if already imported)
 */
require('dotenv').config({ path: require('path').join(__dirname, '../.env') });

const https = require('https');
const { Pool } = require('pg');

const pool = new Pool({
  host: process.env.PG_HOST,
  port: process.env.PG_PORT,
  database: process.env.PG_DATABASE,
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  max: 4,
});

const ALL_STATES = [
  'AK','AL','AR','AZ','CA','CO','CT','DE','FL','GA',
  'HI','IA','ID','IL','IN','KS','KY','LA','MA','MD',
  'ME','MI','MN','MO','MS','MT','NC','ND','NE','NH',
  'NJ','NM','NV','NY','OH','OK','OR','PA','RI','SC',
  'SD','TN','TX','UT','VA','VT','WA','WI','WV','WY',
];

const PAGE_SIZE = 1000;

function fetchJson(url, timeoutMs = 20000) {
  return new Promise((resolve) => {
    let done = false;
    const req = https.get(url, { headers: { 'User-Agent': 'GeoScope/1.0' } }, (res) => {
      let data = '';
      res.on('data', (c) => { if (!done) data += c; });
      res.on('end', () => {
        if (done) return;
        done = true;
        if (res.statusCode !== 200) return resolve(null);
        try { resolve(JSON.parse(data)); } catch (_) { resolve(null); }
      });
    });
    req.setTimeout(timeoutMs, () => { if (!done) { done = true; req.destroy(); resolve(null); } });
    req.on('error', () => { if (!done) { done = true; resolve(null); } });
  });
}

function mapRow(r) {
  const sourceId = `TRI-${r.tri_facility_id || ''}`;
  if (!sourceId || sourceId === 'TRI-') return null;
  const lat = parseFloat(r.fac_latitude || r.pref_latitude || '');
  const lon = parseFloat(r.fac_longitude || r.pref_longitude || '');
  return {
    source_id: sourceId,
    site_name: (r.facility_name || '').trim() || '(Unknown TRI Facility)',
    address: (r.street_address || '').trim() || null,
    city: (r.city_name || '').trim() || null,
    state: (r.state_abbr || '').trim() || null,
    zip: (r.zip_code || '').trim() || null,
    database_name: 'TRI_FACILITY',
    category: 'contamination',
    class_code: 'TRI',
    lat: Number.isFinite(lat) ? lat : null,
    lon: Number.isFinite(lon) ? lon : null,
  };
}

async function insertBatch(rows) {
  if (!rows.length) return 0;
  const vals = [];
  const params = [];
  let p = 1;
  for (const r of rows) {
    if (r.lat != null && r.lon != null) {
      vals.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},ST_SetSRID(ST_MakePoint($${p++},$${p++}),4326))`);
      params.push(r.source_id, r.site_name, r.address, r.city, r.state, r.zip, r.database_name, r.category, r.class_code, r.lon, r.lat);
    } else {
      vals.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},NULL)`);
      params.push(r.source_id, r.site_name, r.address, r.city, r.state, r.zip, r.database_name, r.category, r.class_code);
    }
  }
  const sql = `INSERT INTO environmental_sites (source_id,site_name,address,city,state,zip,database_name,category,class_code,location)
               VALUES ${vals.join(',')}
               ON CONFLICT (source_id) DO UPDATE SET
                 state = EXCLUDED.state,
                 location = COALESCE(EXCLUDED.location, environmental_sites.location)
               WHERE environmental_sites.state IS NULL OR EXCLUDED.state IS NOT NULL`;
  const res = await pool.query(sql, params);
  return res.rowCount || 0;
}

async function importState(stateCode) {
  let page = 0;
  let totalInserted = 0;
  while (true) {
    const start = page * PAGE_SIZE;
    const end = start + PAGE_SIZE - 1;
    const url = `https://data.epa.gov/efservice/TRI_FACILITY/STATE_ABBR/=/${stateCode}/ROWS/${start}:${end}/JSON`;
    let data = null;
    for (let attempt = 0; attempt < 3 && !data; attempt++) {
      data = await fetchJson(url, 25000);
      if (!data) await new Promise((r) => setTimeout(r, 500 * (attempt + 1)));
    }
    if (!Array.isArray(data) || data.length === 0) break;

    const mapped = data.map(mapRow).filter(Boolean);
    const CHUNK = 500;
    let ins = 0;
    for (let i = 0; i < mapped.length; i += CHUNK) {
      try { ins += await insertBatch(mapped.slice(i, i + CHUNK)); } catch (_) {}
    }
    totalInserted += ins;
    if (data.length < PAGE_SIZE) break;
    page++;
    await new Promise((r) => setTimeout(r, 200));
  }
  return totalInserted;
}

async function main() {
  const args = process.argv.slice(2);
  const statesArg = args.find((a) => a.startsWith('--states='));
  const skipAL = args.includes('--skip-al');

  let states = statesArg
    ? statesArg.split('=')[1].split(',').map((s) => s.trim().toUpperCase())
    : ALL_STATES;

  if (skipAL) states = states.filter((s) => s !== 'AL');

  console.log(`\nTRI_FACILITY state-by-state import`);
  console.log(`States: ${states.join(', ')}`);

  const startCount = await pool.query('SELECT COUNT(*) FROM environmental_sites');
  console.log(`DB start: ${Number(startCount.rows[0].count).toLocaleString()}`);

  let grandTotal = 0;
  for (const state of states) {
    process.stdout.write(`  ${state}... `);
    const ins = await importState(state);
    console.log(`inserted/updated ${ins.toLocaleString()}`);
    grandTotal += ins;
    await new Promise((r) => setTimeout(r, 100));
  }

  const endCount = await pool.query('SELECT COUNT(*) FROM environmental_sites');
  console.log(`\nTRI state-by-state complete`);
  console.log(`  Total inserted/updated: ${grandTotal.toLocaleString()}`);
  console.log(`  DB final: ${Number(endCount.rows[0].count).toLocaleString()}`);
  await pool.end();
}

main().catch((e) => { console.error(e.message); process.exit(1); });
