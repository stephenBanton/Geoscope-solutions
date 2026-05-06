#!/usr/bin/env node
require('dotenv').config({ path: require('path').join(__dirname, '../.env') });

const https = require('https');
const { Pool } = require('pg');

const pool = new Pool({
  host: process.env.PG_HOST,
  port: process.env.PG_PORT,
  database: process.env.PG_DATABASE,
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  max: 8,
});

const PAGE_SIZE = 1000;

function fetchJson(url, timeoutMs = 15000) {
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

async function fetchTriPage(start, end) {
  const url = `https://data.epa.gov/efservice/TRI_FACILITY/ROWS/${start}:${end}/JSON`;
  let data = null;
  for (let attempt = 0; attempt < 3 && !data; attempt++) {
    data = await fetchJson(url, 20000);
    if (!data) await new Promise((r) => setTimeout(r, 350 * (attempt + 1)));
  }
  if (!Array.isArray(data)) return [];
  return data;
}

async function fetchAndInsertAll(maxPages = 0, startOffset = 0) {
  let totalFetched = 0;
  let totalInserted = 0;
  let page = 0;
  while (true) {
    const start = startOffset + page * PAGE_SIZE;
    const end = start + PAGE_SIZE - 1;
    process.stdout.write(`\r  TRI page ${page + 1} rows ${start}:${end}...`);

    const data = await fetchTriPage(start, end);

    if (!Array.isArray(data) || data.length === 0) break;

    totalFetched += data.length;
    const mapped = data.map(mapTri).filter(Boolean);
    const ins = await insertRows(mapped, 'TRI');
    totalInserted += ins;

    console.log(`  Page ${page + 1}: fetched=${data.length.toLocaleString()} inserted=${ins.toLocaleString()} totalInserted=${totalInserted.toLocaleString()}`);

    if (data.length < PAGE_SIZE) break;
    page++;
    if (maxPages > 0 && page >= maxPages) break;
    await new Promise((r) => setTimeout(r, 150));
  }
  return { totalFetched, totalInserted };
}

function mapTri(r) {
  const sourceId = `TRI-${r.tri_facility_id || r.TRIFID || r.tri_id || ''}`;
  if (!sourceId || sourceId === 'TRI-') return null;
  const lat = parseFloat(r.latitude || r.LATITUDE || '');
  const lon = parseFloat(r.longitude || r.LONGITUDE || '');
  return {
    source_id: sourceId,
    site_name: (r.facility_name || r.FACILITY_NAME || '').trim() || '(Unknown TRI Facility)',
    address: (r.street_address || r.STREET_ADDRESS || '').trim() || null,
    city: (r.city_name || r.CITY || '').trim() || null,
    state: (r.state_cd || r.STATE_CD || '').trim() || null,
    zip: (r.zip_code || r.ZIP || '').trim() || null,
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
               ON CONFLICT (source_id) DO NOTHING`;
  const res = await pool.query(sql, params);
  return res.rowCount || 0;
}

async function insertRows(rows, state) {
  let inserted = 0;
  const CHUNK = 500;
  for (let i = 0; i < rows.length; i += CHUNK) {
    const chunk = rows.slice(i, i + CHUNK);
    try {
      inserted += await insertBatch(chunk);
    } catch (_) {
      for (const r of chunk) {
        try {
          inserted += await insertBatch([r]);
        } catch (_) {}
      }
    }
    process.stdout.write(`\r  ${state} inserted ${Math.min(i + CHUNK, rows.length).toLocaleString()}/${rows.length.toLocaleString()} -> ${inserted.toLocaleString()}`);
  }
  process.stdout.write('\n');
  return inserted;
}

async function main() {
  const args = process.argv.slice(2);
  const maxPagesArg = args.find((a) => a.startsWith('--max-pages='));
  const startArg = args.find((a) => a.startsWith('--start='));
  const maxPages = maxPagesArg ? parseInt(maxPagesArg.split('=')[1], 10) : 0;
  const startOffset = startArg ? parseInt(startArg.split('=')[1], 10) : 0;

  console.log('\nTRI Envirofacts Import');
  console.log('  Mode: full TRI_FACILITY table scan');
  if (startOffset > 0) console.log(`  Start offset: ${startOffset}`);
  if (maxPages > 0) console.log(`  Max pages: ${maxPages}`);

  const start = await pool.query('select count(*) from environmental_sites');
  console.log(`  DB start: ${Number(start.rows[0].count).toLocaleString()}`);

  console.log('\nFetching + inserting TRI rows...');
  const result = await fetchAndInsertAll(maxPages, startOffset);
  const totalInserted = result.totalInserted;
  console.log(`  TRI fetched: ${result.totalFetched.toLocaleString()}`);
  console.log(`  TRI inserted: ${totalInserted.toLocaleString()}`);

  const end = await pool.query('select count(*) from environmental_sites');
  console.log('\nTRI import complete');
  console.log(`  Total inserted: ${totalInserted.toLocaleString()}`);
  console.log(`  DB final: ${Number(end.rows[0].count).toLocaleString()}`);
  await pool.end();
}

main().catch(async (e) => {
  console.error(e.message || e);
  try { await pool.end(); } catch (_) {}
  process.exit(1);
});
