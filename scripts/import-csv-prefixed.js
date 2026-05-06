#!/usr/bin/env node
require('dotenv').config({ path: require('path').join(__dirname, '../.env') });

const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse');
const { Pool } = require('pg');

const LAT_ALIASES = ['latitude','lat','y','y_coord','lat_dd','dec_lat','publication_lat'];
const LON_ALIASES = ['longitude','lon','long','x','x_coord','lon_dd','dec_long','publication_lon','publication_long'];
const ID_ALIASES = ['source_id','id','registry_id','facility_id','site_id','handler_id'];
const NAME_ALIASES = ['site_name','name','facility_name','fac_name','handler_name','title'];
const ADDR_ALIASES = ['address','street_address','addr','location_address'];
const CITY_ALIASES = ['city','city_name','fac_city'];
const STATE_ALIASES = ['state','state_code','st','fac_state'];
const ZIP_ALIASES = ['zip','zip_code','postal_code','fac_zip'];

function findCol(headers, aliases) {
  const lower = headers.map(h => h.toLowerCase().trim());
  for (const a of aliases) {
    const idx = lower.indexOf(a);
    if (idx !== -1) return headers[idx];
  }
  return null;
}

function clean(v) {
  if (v === undefined || v === null) return null;
  const s = String(v).trim().replace(/^"+|"+$/g, '');
  return s.length ? s : null;
}

function toFloat(v) {
  const normalized = String(v || '').trim().replace(/^"+|"+$/g, '');
  const f = parseFloat(normalized);
  return Number.isFinite(f) ? f : null;
}

async function batchInsert(pool, rows) {
  if (!rows.length) return 0;
  const vals = [];
  const args = [];
  let i = 1;
  for (const r of rows) {
    vals.push(`($${i},$${i+1},$${i+2},$${i+3},$${i+4},$${i+5},$${i+6},$${i+7},$${i+8},$${i+9},$${i+10},$${i+11},ST_SetSRID(ST_MakePoint($${i+12},$${i+13}),4326))`);
    args.push(
      r.source_id,
      r.site_name,
      r.address,
      r.city,
      r.state,
      r.zip,
      r.database_name,
      r.category,
      r.class_code,
      r.priority_tier,
      r.priority_score,
      r.source_org,
      r.lon,
      r.lat
    );
    i += 14;
  }

  const sql = `
    INSERT INTO environmental_sites
      (source_id, site_name, address, city, state, zip, database_name, category, class_code, priority_tier, priority_score, source_org, location)
    VALUES ${vals.join(',')}
    ON CONFLICT (source_id) DO NOTHING
  `;
  const res = await pool.query(sql, args);
  return res.rowCount || 0;
}

async function main() {
  const csvPath = process.argv[2];
  const databaseName = process.argv[3];
  const category = process.argv[4] || 'other';
  const classCode = process.argv[5] || 'LOCAL_CACHE';
  const priorityTier = process.argv[6] || 'standard';
  const delimiter = process.argv[7] || ',';

  if (!csvPath || !databaseName) {
    console.error('Usage: node scripts/import-csv-prefixed.js <file> <database_name> [category] [class_code] [priority_tier] [delimiter]');
    process.exit(1);
  }
  if (!fs.existsSync(csvPath)) {
    console.error('File not found:', csvPath);
    process.exit(1);
  }

  console.log(`\nImporting with prefixed IDs: ${csvPath}`);
  console.log(`  database_name=${databaseName} class_code=${classCode}`);

  const pool = new Pool({
    host: process.env.PG_HOST,
    port: Number(process.env.PG_PORT),
    database: process.env.PG_DATABASE,
    user: process.env.PG_USER,
    password: process.env.PG_PASSWORD,
    max: 5,
  });

  const parser = fs.createReadStream(csvPath).pipe(parse({
    columns: true,
    delimiter,
    skip_empty_lines: true,
    trim: true,
    bom: true,
    relax_quotes: true,
    relax_column_count: true,
    skip_records_with_error: true,
  }));

  let headers = null;
  let latCol = null, lonCol = null, idCol = null, nameCol = null, addrCol = null, cityCol = null, stateCol = null, zipCol = null;

  const batch = [];
  let processed = 0;
  let skipped = 0;
  let inserted = 0;

  for await (const row of parser) {
    if (!headers) {
      headers = Object.keys(row);
      latCol = findCol(headers, LAT_ALIASES);
      lonCol = findCol(headers, LON_ALIASES);
      idCol = findCol(headers, ID_ALIASES);
      nameCol = findCol(headers, NAME_ALIASES);
      addrCol = findCol(headers, ADDR_ALIASES);
      cityCol = findCol(headers, CITY_ALIASES);
      stateCol = findCol(headers, STATE_ALIASES);
      zipCol = findCol(headers, ZIP_ALIASES);
      console.log(`  Columns detected: lat=${latCol} lon=${lonCol} id=${idCol}`);
    }

    processed++;
    const lat = toFloat(latCol ? row[latCol] : null);
    const lon = toFloat(lonCol ? row[lonCol] : null);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
      skipped++;
      continue;
    }

    const rawId = clean(idCol ? row[idCol] : null) || `${processed}`;
    const safeId = rawId.replace(/[^A-Za-z0-9_.-]/g, '_').slice(0, 80);
    const sourceId = `${classCode}-${safeId}`.slice(0, 100);

    batch.push({
      source_id: sourceId,
      site_name: (clean(nameCol ? row[nameCol] : null) || databaseName).slice(0, 500),
      address: (clean(addrCol ? row[addrCol] : null) || '').slice(0, 500) || null,
      city: (clean(cityCol ? row[cityCol] : null) || '').slice(0, 100) || null,
      state: (clean(stateCol ? row[stateCol] : null) || '').slice(0, 2) || null,
      zip: (clean(zipCol ? row[zipCol] : null) || '').slice(0, 20) || null,
      database_name: databaseName,
      category,
      class_code: classCode,
      priority_tier: priorityTier,
      priority_score: 50,
      source_org: 'LOCAL_CACHE',
      lat,
      lon,
    });

    if (batch.length >= 1000) {
      inserted += await batchInsert(pool, batch);
      batch.length = 0;
      if (processed % 50000 < 1000) {
        console.log(`  Progress: processed=${processed.toLocaleString()} inserted=${inserted.toLocaleString()} skipped=${skipped.toLocaleString()}`);
      }
    }
  }

  if (batch.length) {
    inserted += await batchInsert(pool, batch);
  }

  console.log(`\nDone: processed=${processed.toLocaleString()} inserted=${inserted.toLocaleString()} skipped=${skipped.toLocaleString()}`);
  await pool.end();
}

main().catch(err => {
  console.error(err.message);
  process.exit(1);
});
