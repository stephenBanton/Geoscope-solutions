#!/usr/bin/env node
// Import MSHA Mines pipe-delimited TXT file
require('dotenv').config({ path: require('path').join(__dirname, '../.env') });

const fs   = require('fs');
const path = require('path');
const { parse } = require('csv-parse');
const { pool } = require('../db');

const FILE = path.join(__dirname, '../downloads/mega/msha_extract/Mines.txt');
const DB_NAME = 'MSHA Mines';
const CATEGORY = 'mining';
const CLASS_CODE = 'MSHA_MINE';
const BATCH_SIZE = 1000;

async function batchInsert(rows) {
  if (!rows.length) return { inserted: 0, updated: 0 };
  const values = [];
  const params = [];
  let p = 1;
  for (const r of rows) {
    values.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},ST_SetSRID(ST_MakePoint($${p++},$${p++}),4326))`);
    params.push(
      r.source_id, r.site_name, r.address, r.city, r.state, r.zip,
      r.database_name, r.category, r.class_code, r.priority_tier, r.priority_score, r.source_org,
      r.lon, r.lat
    );
  }
  const sql = `
    INSERT INTO environmental_sites
      (source_id,site_name,address,city,state,zip,database_name,category,class_code,priority_tier,priority_score,source_org,location)
    VALUES ${values.join(',')}
    ON CONFLICT (source_id) DO NOTHING
  `;
  const res = await pool.query(sql, params);
  return { inserted: res.rowCount, updated: rows.length - res.rowCount };
}

async function main() {
  console.log(`\n🏗️  Importing MSHA Mines`);
  console.log(`   File: ${FILE}`);

  if (!fs.existsSync(FILE)) {
    console.error('File not found:', FILE);
    process.exit(1);
  }

  const records = await new Promise((resolve, reject) => {
    const rows = [];
    fs.createReadStream(FILE)
      .pipe(parse({
        columns: true,
        delimiter: '|',
        skip_empty_lines: true,
        trim: true,
        bom: true,
        relax_quotes: true,
        relax_column_count: true,
        skip_records_with_error: true,
      }))
        .on('data', r => rows.push(r))
        .on('error', reject)
      .on('end', () => resolve(rows));
  });


  console.log(`   Rows: ${records.length.toLocaleString()}`);

  let inserted = 0, updated = 0, skipped = 0;
  const batch = [];

  for (const r of records) {
    const lat = parseFloat(r['LATITUDE']);
    const lon = parseFloat(r['LONGITUDE']);
    if (!isFinite(lat) || !isFinite(lon) || lat === 0 || lon === 0) { skipped++; continue; }

    const mineId = r['MINE_ID'] ? `MSHA-MINE-${r['MINE_ID'].replace(/['"]/g, '')}` : null;
    if (!mineId) { skipped++; continue; }

    batch.push({
      source_id: mineId,
      site_name: r['CURRENT_MINE_NAME'] || null,
      address: null,
      city: r['NEAREST_TOWN'] || null,
      state: r['STATE'] || null,
      zip: null,
      database_name: DB_NAME,
      category: CATEGORY,
      class_code: CLASS_CODE,
      priority_tier: 'standard',
      priority_score: 50,
      source_org: 'MSHA',
      lat, lon
    });

    if (batch.length >= BATCH_SIZE) {
      const res = await batchInsert(batch);
      inserted += res.inserted;
      updated += res.updated;
      batch.length = 0;
      process.stdout.write(`\r   Progress: ${(inserted+updated).toLocaleString()} processed...`);
    }
  }

  if (batch.length) {
    const res = await batchInsert(batch);
    inserted += res.inserted;
    updated += res.updated;
  }

  console.log(`\n\n✅  MSHA Mines import complete`);
  console.log(`   Inserted : ${inserted.toLocaleString()}`);
  console.log(`   Skipped  : ${skipped.toLocaleString()}`);

  await pool.end();
}

main().catch(e => { console.error(e.message); process.exit(1); });
