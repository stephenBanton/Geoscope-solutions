#!/usr/bin/env node
/**
 * Bulk CSV importer for local data files
 * Handles: TRI facilities, child care, UST, schools
 * Usage: node scripts/import-csv-bulk.js [--files=tri,childcare,schools]
 */
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
  max: 8,
});

const BASE = path.join(__dirname, '../downloads/missing');

// CSV files to import, with metadata
const IMPORTS = {
  echo_tri: {
    file: path.join(BASE, 'ECHO_TRI/echo_tri.csv'),
    database_name: 'TRI_FACILITY',
    category: 'contamination',
    class_code: 'TRI',
    // source_id already in CSV with EPA_ prefix
    id_prefix: null,
  },
  tri_hdrive: {
    file: path.join(BASE, 'TRI/tri_from_hdrive.csv'),
    database_name: 'TRI_FACILITY',
    category: 'contamination',
    class_code: 'TRI',
    id_prefix: 'TRI-',
  },
  child_care: {
    file: path.join(BASE, 'HIFLD_CHILD_CARE/child_care.csv'),
    database_name: 'HIFLD Child Care',
    category: 'education',
    class_code: 'CHILD_CARE',
    id_prefix: 'CC-',
  },
  schools_public_derived: {
    file: path.join(BASE, 'DERIVED/schools_public_derived.csv'),
    database_name: 'SCHOOLS PUBLIC',
    category: 'education',
    class_code: 'PUBLIC_SCHOOL',
    id_prefix: null,
  },
  schools_private_derived: {
    file: path.join(BASE, 'DERIVED/schools_private_derived.csv'),
    database_name: 'SCHOOLS PRIVATE',
    category: 'education',
    class_code: 'PRIVATE_SCHOOL',
    id_prefix: null,
  },
};

function parseCSVLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') { current += '"'; i++; }
      else { inQuotes = !inQuotes; }
    } else if (ch === ',' && !inQuotes) {
      result.push(current);
      current = '';
    } else {
      current += ch;
    }
  }
  result.push(current);
  return result;
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
                 database_name = EXCLUDED.database_name,
                 location = COALESCE(EXCLUDED.location, environmental_sites.location)
               WHERE environmental_sites.state IS NULL OR EXCLUDED.state IS NOT NULL`;
  const res = await pool.query(sql, params);
  return res.rowCount || 0;
}

function dedupBatch(rows) {
  const seen = new Map();
  for (const r of rows) { seen.set(r.source_id, r); }
  return Array.from(seen.values());
}

async function importFile(key, cfg) {
  if (!fs.existsSync(cfg.file)) {
    console.log(`  ❌ File not found: ${cfg.file}`);
    return 0;
  }

  console.log(`\n📥 ${key}: ${path.basename(cfg.file)} -> ${cfg.database_name}`);

  const rl = readline.createInterface({ input: fs.createReadStream(cfg.file), crlfDelay: Infinity });
  let headers = null;
  let batch = [];
  let totalInserted = 0;
  let totalRows = 0;
  const BATCH_SIZE = 500;
  const VALID_STATES = new Set(['AK','AL','AR','AZ','CA','CO','CT','DE','FL','GA','HI','IA','ID','IL','IN','KS','KY','LA','MA','MD','ME','MI','MN','MO','MS','MT','NC','ND','NE','NH','NJ','NM','NV','NY','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VA','VT','WA','WI','WV','WY','DC']);

  for await (const line of rl) {
    if (!line.trim()) continue;
    const fields = parseCSVLine(line);
    if (!headers) {
      headers = fields.map((h) => h.trim().toLowerCase().replace(/^"|"$/g, ''));
      continue;
    }

    const row = {};
    headers.forEach((h, i) => { row[h] = (fields[i] || '').trim().replace(/^"|"$/g, ''); });

    // Normalize state - skip non-US territories and junk values
    const rawState = (row.state || '').trim().toUpperCase();
    const state = VALID_STATES.has(rawState) ? rawState : null;

    const lat = parseFloat(row.latitude || row.lat || '');
    const lon = parseFloat(row.longitude || row.lon || '');

    const rawId = row.source_id || row.id || '';
    const source_id = cfg.id_prefix && rawId && !rawId.startsWith(cfg.id_prefix)
      ? `${cfg.id_prefix}${rawId}`
      : rawId || null;

    if (!source_id) continue;

    totalRows++;
    batch.push({
      source_id,
      site_name: (row.name || row.site_name || '').trim() || `(${cfg.database_name})`,
      address: row.address || null,
      city: row.city || null,
      state,
      zip: row.zip || null,
      database_name: cfg.database_name,
      category: cfg.category,
      class_code: cfg.class_code,
      lat: Number.isFinite(lat) ? lat : null,
      lon: Number.isFinite(lon) ? lon : null,
    });

    if (batch.length >= BATCH_SIZE) {
      const deduped = dedupBatch(batch);
      totalInserted += await insertBatch(deduped);
      batch = [];
      process.stdout.write(`\r  Rows: ${totalRows.toLocaleString()} Inserted: ${totalInserted.toLocaleString()}`);
    }
  }

  if (batch.length > 0) {
    totalInserted += await insertBatch(dedupBatch(batch));
  }

  console.log(`\n  ✅ Done: ${totalRows.toLocaleString()} rows, ${totalInserted.toLocaleString()} inserted/updated`);
  return totalInserted;
}

async function main() {
  const args = process.argv.slice(2);
  const filesArg = args.find((a) => a.startsWith('--files='));
  const keys = filesArg
    ? filesArg.split('=')[1].split(',').map((k) => k.trim())
    : Object.keys(IMPORTS);

  console.log('\nBulk CSV Importer');
  console.log(`Importing: ${keys.join(', ')}`);

  const startCount = await pool.query('SELECT COUNT(*) FROM environmental_sites');
  console.log(`DB start: ${Number(startCount.rows[0].count).toLocaleString()}`);

  let grandTotal = 0;
  for (const key of keys) {
    const cfg = IMPORTS[key];
    if (!cfg) { console.log(`  ⚠️  Unknown key: ${key}`); continue; }
    grandTotal += await importFile(key, cfg);
  }

  const endCount = await pool.query('SELECT COUNT(*) FROM environmental_sites');
  console.log(`\nBulk import complete`);
  console.log(`  Total inserted/updated: ${grandTotal.toLocaleString()}`);
  console.log(`  DB final: ${Number(endCount.rows[0].count).toLocaleString()}`);
  await pool.end();
}

main().catch((e) => { console.error(e.message); process.exit(1); });
