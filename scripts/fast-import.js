#!/usr/bin/env node
// =============================================================================
// GeoScope FAST Batch CSV Importer — batched multi-row INSERTs, 20-50x faster
// =============================================================================
// Usage:
//   DATABASE_URL="..." node scripts/fast-import.js <csv> "<DB Name>" <category> [class_code] [priority_tier] [batchSize]
//
// Examples:
//   node scripts/fast-import.js downloads/missing/LUST/lust_from_hdrive.csv "LUST" contamination LUST HIGH
//   node scripts/fast-import.js downloads/missing/ECHO_RCRA/echo_rcra_all.csv "ECHO RCRA" contamination RCRA HIGH
//   node scripts/fast-import.js downloads/missing/ECHO_NPDES/echo_npdes_all.csv "ECHO NPDES" regulatory NPDES MEDIUM
// =============================================================================

require('dotenv').config({ path: require('path').join(__dirname, '../.env') });

const fs   = require('fs');
const path = require('path');
const { parse } = require('csv-parse');
const { Pool } = require('pg');

// Column aliases
const LAT_ALIASES  = ['latitude','lat','latitude83','y_coord','y','lat_dd','dec_lat','lat_wgs84','latitude_wgs84','b_lat_c'];
const LON_ALIASES  = ['longitude','lon','long','longitude83','x_coord','x','lon_dd','dec_long','lon_wgs84','long_wgs84','b_long_c'];
const NAME_ALIASES = ['site_name','facility_name','name','fac_name','handler_name','primary_name','facility','title'];
const ADDR_ALIASES = ['address','location_address','street_address','addr','fac_street','addr1','street'];
const CITY_ALIASES = ['city','city_name','fac_city'];
const STATE_ALIASES= ['state','state_code','fac_state','st'];
const ZIP_ALIASES  = ['zip','zip_code','fac_zip','postal_code'];
const STATUS_ALIASES=['status','activity_status','fac_active_flag','site_status'];
const ID_ALIASES   = ['source_id','registry_id','id','handler_id','site_id','facility_id'];

function findCol(headers, aliases) {
  const lower = headers.map(h => h.toLowerCase().trim());
  for (const a of aliases) { const i = lower.indexOf(a); if (i !== -1) return headers[i]; }
  return null;
}
function getVal(row, col) {
  if (!col) return null;
  const v = row[col];
  if (v === undefined || v === null || v === '' || v === 'NULL') return null;
  const s = String(v).trim().replace(/^"+|"+$/g, '');
  return s.length ? s : null;
}

function toFloat(v) {
  const s = String(v || '').trim().replace(/^"+|"+$/g, '');
  const n = parseFloat(s);
  return Number.isFinite(n) ? n : null;
}

const STATE_NAME_TO_CODE = {
  ALABAMA: 'AL', ALASKA: 'AK', ARIZONA: 'AZ', ARKANSAS: 'AR', CALIFORNIA: 'CA', COLORADO: 'CO',
  CONNECTICUT: 'CT', DELAWARE: 'DE', FLORIDA: 'FL', GEORGIA: 'GA', HAWAII: 'HI', IDAHO: 'ID',
  ILLINOIS: 'IL', INDIANA: 'IN', IOWA: 'IA', KANSAS: 'KS', KENTUCKY: 'KY', LOUISIANA: 'LA',
  MAINE: 'ME', MARYLAND: 'MD', MASSACHUSETTS: 'MA', MICHIGAN: 'MI', MINNESOTA: 'MN', MISSISSIPPI: 'MS',
  MISSOURI: 'MO', MONTANA: 'MT', NEBRASKA: 'NE', NEVADA: 'NV', 'NEW HAMPSHIRE': 'NH', 'NEW JERSEY': 'NJ',
  'NEW MEXICO': 'NM', 'NEW YORK': 'NY', 'NORTH CAROLINA': 'NC', 'NORTH DAKOTA': 'ND', OHIO: 'OH',
  OKLAHOMA: 'OK', OREGON: 'OR', PENNSYLVANIA: 'PA', 'RHODE ISLAND': 'RI', 'SOUTH CAROLINA': 'SC',
  'SOUTH DAKOTA': 'SD', TENNESSEE: 'TN', TEXAS: 'TX', UTAH: 'UT', VERMONT: 'VT', VIRGINIA: 'VA',
  WASHINGTON: 'WA', 'WEST VIRGINIA': 'WV', WISCONSIN: 'WI', WYOMING: 'WY',
};

function normalizeState(stateRaw) {
  if (!stateRaw) return null;
  const upper = String(stateRaw).trim().toUpperCase();
  if (/^[A-Z]{2}$/.test(upper)) return upper;
  if (STATE_NAME_TO_CODE[upper]) return STATE_NAME_TO_CODE[upper];
  return null;
}

async function processBatch(client, rows, colMap, dbName, category, classCode, priorityTier) {
  const { latCol, lonCol, nameCol, addrCol, cityCol, stateCol, zipCol, statusCol, idCol } = colMap;
  const knownSet = new Set(Object.values(colMap).filter(Boolean));

  const vals = [];
  const params = [];
  let p = 1;
  const seenIds = new Set();

  for (const row of rows) {
    const latRaw = getVal(row, latCol);
    const lonRaw = getVal(row, lonCol);
    const lat = toFloat(latRaw);
    const lon = toFloat(lonRaw);
    if (!latRaw || !lonRaw || lat === null || lon === null ||
        lat < -90 || lat > 90 || lon < -180 || lon > 180) continue;

    const sourceId = getVal(row, idCol);
    // Skip duplicate source_ids within same batch (would cause ON CONFLICT double-update error)
    if (sourceId && seenIds.has(sourceId)) continue;
    if (sourceId) seenIds.add(sourceId);
    const siteName = getVal(row, nameCol);
    const address  = getVal(row, addrCol);
    const city     = getVal(row, cityCol);
    const stateRaw = getVal(row, stateCol);
    const state    = normalizeState(stateRaw);
    const zip      = getVal(row, zipCol);
    const status   = getVal(row, statusCol);

    const extra = {};
    for (const [k, v] of Object.entries(row)) {
      if (!knownSet.has(k) && v !== null && v !== '' && v !== 'NULL') extra[k] = v;
    }
    const attrs = Object.keys(extra).length > 0 ? JSON.stringify(extra) : null;

    vals.push(`(ST_SetSRID(ST_MakePoint($${p},$${p+1}),4326),$${p+2},$${p+3},$${p+4},$${p+5},$${p+6},$${p+7},$${p+8},$${p+9},$${p+10},$${p+11},$${p+12},$${p+13},TRUE)`);
    params.push(lon, lat, dbName, category, siteName, address, city, state, zip, status, sourceId, attrs, classCode, priorityTier);
    p += 14;
  }

  if (!vals.length) return 0;

  await client.query(`
    INSERT INTO environmental_sites
      (location, database_name, category, site_name, address, city, state, zip, status, source_id, attributes, class_code, priority_tier, geocoded)
    VALUES ${vals.join(',')}
    ON CONFLICT (source_id) WHERE source_id IS NOT NULL DO NOTHING
  `, params);

  return vals.length;
}

async function main() {
  const csvPath     = process.argv[2];
  const dbNameArg   = process.argv[3];
  const categoryArg = process.argv[4] || 'regulatory';
  const classCode   = process.argv[5] || null;
  const priority    = process.argv[6] || 'standard';
  const BATCH       = parseInt(process.argv[7] || '300', 10);

  if (!csvPath || !dbNameArg) {
    console.error('Usage: node fast-import.js <csv> "<DB Name>" <category> [class_code] [priority_tier] [batchSize]');
    process.exit(1);
  }
  if (!fs.existsSync(csvPath)) { console.error('File not found:', csvPath); process.exit(1); }

  const connStr = process.env.DATABASE_URL ||
    'postgresql://postgres.imvcveoynxkceupggnnw:Mombasad3780%2A@aws-1-eu-west-1.pooler.supabase.com:5432/postgres';

  const pool = new Pool({ connectionString: connStr, ssl: { rejectUnauthorized: false }, max: 3, connectionTimeoutMillis: 20000 });
  console.log(`\n📂  Fast-importing: ${path.basename(csvPath)}`);
  console.log(`    DB: "${dbNameArg}" | category: ${categoryArg} | batchSize: ${BATCH}`);

  const client = await pool.connect();
  console.log('    Connected to DB ✓');

  // Ensure needed columns exist (gracefully ignore if already present)
  for (const sql of [
    `ALTER TABLE environmental_sites ADD COLUMN IF NOT EXISTS geocoded BOOLEAN DEFAULT FALSE`,
    `ALTER TABLE environmental_sites ADD COLUMN IF NOT EXISTS geocode_attempted BOOLEAN DEFAULT FALSE`,
  ]) {
    await client.query(sql).catch(() => {});
  }

  // Create unique index on source_id
  await client.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS env_sites_source_id_uidx
    ON environmental_sites (source_id) WHERE source_id IS NOT NULL
  `).catch(() => {});

  // Read first few rows to detect columns
  const sample = await new Promise((res, rej) => {
    const rows = [];
    fs.createReadStream(csvPath)
      .pipe(parse({ columns: true, skip_empty_lines: true, trim: true, bom: true, to: 3 }))
      .on('data', r => rows.push(r)).on('end', () => res(rows)).on('error', rej);
  });
  if (!sample.length) { console.error('Empty CSV'); process.exit(1); }

  const headers = Object.keys(sample[0]);
  const colMap = {
    latCol:   findCol(headers, LAT_ALIASES),
    lonCol:   findCol(headers, LON_ALIASES),
    nameCol:  findCol(headers, NAME_ALIASES),
    addrCol:  findCol(headers, ADDR_ALIASES),
    cityCol:  findCol(headers, CITY_ALIASES),
    stateCol: findCol(headers, STATE_ALIASES),
    zipCol:   findCol(headers, ZIP_ALIASES),
    statusCol:findCol(headers, STATUS_ALIASES),
    idCol:    findCol(headers, ID_ALIASES),
  };

  console.log(`    lat=${colMap.latCol}  lon=${colMap.lonCol}  name=${colMap.nameCol}  state=${colMap.stateCol}  id=${colMap.idCol}`);
  if (!colMap.latCol || !colMap.lonCol) {
    console.error('ERROR: Cannot find lat/lon columns. Headers found:', headers.join(', '));
    process.exit(1);
  }

  let inserted = 0, skipped = 0, batchErrors = 0, totalRows = 0;
  let batch = [];
  const start = Date.now();

  const flushBatch = async () => {
    if (!batch.length) return;
    const cur = batch; batch = [];
    try {
      const n = await processBatch(client, cur, colMap, dbNameArg, categoryArg, classCode, priority);
      inserted += n;
      skipped += cur.length - n;
    } catch (e) {
      batchErrors++;
      if (batchErrors <= 3) console.error(`\n  Batch error: ${e.message.substring(0, 200)}`);
      skipped += cur.length;
    }
  };

  await new Promise((resolve, reject) => {
    const parser = parse({ columns: true, skip_empty_lines: true, trim: true, bom: true });

    parser.on('data', (row) => {
      batch.push(row);
      totalRows++;
      if (batch.length >= BATCH) {
        parser.pause();
        flushBatch().then(() => {
          const elapsed = ((Date.now() - start) / 1000).toFixed(1);
          process.stdout.write(`\r  Rows: ${totalRows.toLocaleString()} | Inserted: ${inserted.toLocaleString()} | Skipped: ${skipped} | ${elapsed}s   `);
          parser.resume();
        }).catch(reject);
      }
    });

    parser.on('end', () => flushBatch().then(resolve).catch(reject));
    parser.on('error', reject);
    fs.createReadStream(csvPath).pipe(parser);
  });

  const elapsed = ((Date.now() - start) / 1000).toFixed(1);
  console.log(`\n\n✅  Done in ${elapsed}s`);
  console.log(`    Total rows read:       ${totalRows.toLocaleString()}`);
  console.log(`    Inserted/updated:      ${inserted.toLocaleString()}`);
  console.log(`    Skipped (no coords):   ${skipped}`);
  console.log(`    Batch errors:          ${batchErrors}`);

  const cnt = await client.query(`SELECT COUNT(*) FROM environmental_sites`);
  console.log(`\n    Total in environmental_sites: ${cnt.rows[0].count}`);

  client.release();
  await pool.end();
}

main().catch(e => { console.error('Fatal:', e.message); process.exit(1); });
