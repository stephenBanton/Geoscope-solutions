#!/usr/bin/env node
require('dotenv').config({ path: require('path').join(__dirname, '../.env') });

const path = require('path');
const XLSX = require('xlsx');
const { Pool } = require('pg');

const INPUT = path.join(__dirname, '../downloads/missing/SCHOOLS_NATIONAL/edge_private_2122/EDGE_GEOCODE_PRIVATESCH_2122/EDGE_GEOCODE_PRIVATESCH_2122.xlsx');
const VALID_STATES = new Set(['AK','AL','AR','AZ','CA','CO','CT','DE','FL','GA','HI','IA','ID','IL','IN','KS','KY','LA','MA','MD','ME','MI','MN','MO','MS','MT','NC','ND','NE','NH','NJ','NM','NV','NY','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VA','VT','WA','WI','WV','WY','DC']);

const pool = new Pool({
  host: process.env.PG_HOST,
  port: process.env.PG_PORT,
  database: process.env.PG_DATABASE,
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  max: 6,
});

function pick(row, keys) {
  for (const k of keys) {
    if (row[k] != null && String(row[k]).trim() !== '') return String(row[k]).trim();
  }
  return '';
}

async function insertBatch(rows) {
  if (!rows.length) return 0;
  const vals = [];
  const params = [];
  let p = 1;
  for (const r of rows) {
    vals.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},ST_SetSRID(ST_MakePoint($${p++},$${p++}),4326))`);
    params.push(r.source_id, r.site_name, r.address, r.city, r.state, r.zip, r.database_name, r.category, r.class_code, r.lon, r.lat);
  }
  const sql = `INSERT INTO environmental_sites (source_id,site_name,address,city,state,zip,database_name,category,class_code,location)
               VALUES ${vals.join(',')}
               ON CONFLICT (source_id) DO UPDATE SET
                 state = EXCLUDED.state,
                 location = COALESCE(EXCLUDED.location, environmental_sites.location),
                 database_name = EXCLUDED.database_name`;
  const res = await pool.query(sql, params);
  return res.rowCount || 0;
}

async function main() {
  console.log('\nNCES EDGE Private Schools Import');
  console.log('Input:', INPUT);

  const wb = XLSX.readFile(INPUT, { cellDates: false });
  const ws = wb.Sheets[wb.SheetNames[0]];
  const rows = XLSX.utils.sheet_to_json(ws, { defval: '' });
  console.log('Rows in sheet:', rows.length.toLocaleString());

  const startCount = await pool.query('SELECT COUNT(*) FROM environmental_sites');
  console.log('DB start:', Number(startCount.rows[0].count).toLocaleString());

  const BATCH = 500;
  let totalRead = 0;
  let totalIns = 0;
  let batch = [];

  for (const row of rows) {
    const id = pick(row, ['PPIN', 'PIN', 'NCESSCH', 'SCHOOL_ID', 'ID']);
    const state = pick(row, ['STATE', 'LSTATE', 'STATEAB', 'STATE_ABBR']).toUpperCase();
    const lat = parseFloat(pick(row, ['LAT', 'LATITUDE', 'X', 'YCOORD']));
    const lon = parseFloat(pick(row, ['LON', 'LONGITUDE', 'Y', 'XCOORD']));

    if (!id || !VALID_STATES.has(state) || !Number.isFinite(lat) || !Number.isFinite(lon)) continue;

    totalRead++;
    batch.push({
      source_id: `NCES-PRV-${id}`,
      site_name: pick(row, ['SCHOOL_NAME', 'NAME', 'SCHNAM']) || '(NCES Private School)',
      address: pick(row, ['LSTREE', 'STREET', 'ADDRESS']) || null,
      city: pick(row, ['LCITY', 'CITY']) || null,
      state,
      zip: pick(row, ['LZIP', 'ZIP']) || null,
      database_name: 'SCHOOLS PRIVATE',
      category: 'education',
      class_code: 'PRIVATE_SCHOOL',
      lat,
      lon,
    });

    if (batch.length >= BATCH) {
      totalIns += await insertBatch(batch);
      batch = [];
      process.stdout.write(`\r  Read ${totalRead.toLocaleString()} inserted/updated ${totalIns.toLocaleString()}`);
    }
  }

  if (batch.length) totalIns += await insertBatch(batch);

  const endCount = await pool.query('SELECT COUNT(*) FROM environmental_sites');
  console.log(`\nDone. Read ${totalRead.toLocaleString()}, inserted/updated ${totalIns.toLocaleString()}`);
  console.log('DB final:', Number(endCount.rows[0].count).toLocaleString());
  await pool.end();
}

main().catch((e) => { console.error(e.message); process.exit(1); });
