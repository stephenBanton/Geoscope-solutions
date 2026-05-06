#!/usr/bin/env node
require('dotenv').config({ path: require('path').join(__dirname, '../.env') });

const fs = require('fs');
const path = require('path');
const readline = require('readline');
const { Pool } = require('pg');

const pool = new Pool({
  host: process.env.PG_HOST,
  port: process.env.PG_PORT,
  database: process.env.PG_DATABASE,
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  max: 6,
});

const INPUT = path.join(__dirname, '../downloads/missing/SCHOOLS_NATIONAL/edge_public_2223/EDGE_GEOCODE_PUBLICSCH_2223.TXT');
const VALID_STATES = new Set(['AK','AL','AR','AZ','CA','CO','CT','DE','FL','GA','HI','IA','ID','IL','IN','KS','KY','LA','MA','MD','ME','MI','MN','MO','MS','MT','NC','ND','NE','NH','NJ','NM','NV','NY','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VA','VT','WA','WI','WV','WY','DC']);

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
  if (!fs.existsSync(INPUT)) {
    console.error('Missing input:', INPUT);
    process.exit(1);
  }

  console.log('\nNCES EDGE Public Schools Import');
  console.log('Input:', INPUT);

  const startCount = await pool.query('SELECT COUNT(*) FROM environmental_sites');
  console.log(`DB start: ${Number(startCount.rows[0].count).toLocaleString()}`);

  const rl = readline.createInterface({ input: fs.createReadStream(INPUT), crlfDelay: Infinity });
  let batch = [];
  let totalRead = 0;
  let totalIns = 0;
  const BATCH = 500;

  for await (const line of rl) {
    if (!line || !line.trim()) continue;
    const c = line.split('|');
    // Expected columns: 0=NCESSCH, 2=school name, 4=address, 5=city, 6=state, 7=zip, 12=lat, 13=lon
    if (c.length < 14) continue;

    const state = String(c[6] || '').trim().toUpperCase();
    if (!VALID_STATES.has(state)) continue;

    const lat = parseFloat(c[12]);
    const lon = parseFloat(c[13]);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;

    const id = String(c[0] || '').trim();
    if (!id) continue;

    totalRead++;
    batch.push({
      source_id: `NCES-${id}`,
      site_name: String(c[2] || '').trim() || '(NCES Public School)',
      address: String(c[4] || '').trim() || null,
      city: String(c[5] || '').trim() || null,
      state,
      zip: String(c[7] || '').trim() || null,
      database_name: 'SCHOOLS PUBLIC',
      category: 'education',
      class_code: 'PUBLIC_SCHOOL',
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
  console.log(`DB final: ${Number(endCount.rows[0].count).toLocaleString()}`);
  await pool.end();
}

main().catch((e) => { console.error(e.message); process.exit(1); });
