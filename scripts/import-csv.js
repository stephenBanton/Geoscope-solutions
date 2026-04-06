#!/usr/bin/env node
// =============================================================================
// GeoScope CSV Importer
// =============================================================================
// Usage:
//   node scripts/import-csv.js <path-to-csv-file> [database_name] [category]
//
// Examples:
//   node scripts/import-csv.js C:\Downloads\rcra_lqg_florida.csv "RCRA LQG" contamination
//   node scripts/import-csv.js C:\Downloads\echo_facilities.csv "ECHO" regulatory
//   node scripts/import-csv.js C:\Downloads\geoscope_export.csv
//
// The script auto-detects the CSV format (GeoScope export, EPA ECHO, or generic).
// If latitude/longitude columns are found the row is inserted into PostgreSQL.
// Rows without coordinates are skipped and counted separately.
// =============================================================================

require('dotenv').config({
  path: require('path').join(__dirname, '../.env'),
  quiet: true,
});

const fs   = require('fs');
const path = require('path');
const { parse } = require('csv-parse');
const { pool } = require('../db');

// ---------------------------------------------------------------------------
// Column name maps — add more aliases here as needed
// ---------------------------------------------------------------------------
const LAT_ALIASES  = ['latitude', 'lat', 'latitude83', 'y_coord', 'y', 'lat_dd', 'dec_lat',
  'lat_wgs84', 'latitude_wgs84', 'latitude_wgs_84', 'b_lat_c', 'publication_lat'];
const LON_ALIASES  = ['longitude', 'lon', 'long', 'longitude83', 'x_coord', 'x', 'lon_dd', 'dec_long',
  'lon_wgs84', 'long_wgs84', 'lon_wgs_84', 'long_wgs_84', 'longitude_wgs84', 'longitude_wgs_84',
  'b_long_c', 'publication_long', 'publication_lon'];
const NAME_ALIASES = ['site_name', 'facility_name', 'name', 'fac_name', 'site', 'handler_name',
  'primary_name', 'wwtp_name', 'site_name_short', 'facility', 'title'];
const ADDR_ALIASES = ['address', 'location_address', 'street_address', 'addr', 'fac_street',
  'location_address_line_1', 'address_line1', 'admin_addy', 'addr1', 'street', 'location_desc'];
const CITY_ALIASES = ['city', 'city_name', 'fac_city', 'city_code', 'city_nam'];
const STATE_ALIASES= ['state', 'state_code', 'fac_state', 'st', 'state_name', 'stateprovince'];
const ZIP_ALIASES  = ['zip', 'zip_code', 'fac_zip', 'postal_code', 'zip_code_name'];
const STATUS_ALIASES=['status', 'activity_status', 'fac_active_flag', 'site_status', 'wastestatus'];
const ID_ALIASES   = ['source_id', 'registry_id', 'id', 'handler_id', 'site_id', 'eis_facility_id',
  'ris_facility_id', 'facility_id', 'fpds_id', 'waste_id', 'sample_id'];
const DBNAME_ALIASES=['database_name', 'database', 'db_name', 'source_db', 'pgm_sys_acrnm', 'source'];
const CAT_ALIASES  = ['category', 'cat', 'type', 'category_description'];
const ORG_ALIASES  = ['source_org', 'agency', 'org', 'source', 'program_acronym'];

function findCol(headers, aliases) {
  const lower = headers.map(h => h.toLowerCase().trim());
  for (const a of aliases) {
    const idx = lower.indexOf(a);
    if (idx !== -1) return headers[idx];
  }
  return null;
}

function getVal(row, col) {
  if (!col) return null;
  const v = row[col];
  return (v === undefined || v === null || v === '' || v === 'NULL') ? null : String(v).trim();
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  const csvPath    = process.argv[2];
  const dbNameArg  = process.argv[3] || null;   // override database_name
  const categoryArg= process.argv[4] || null;   // override category

  if (!csvPath) {
    console.error('Usage: node scripts/import-csv.js <file.csv> [database_name] [category]');
    process.exit(1);
  }

  if (!fs.existsSync(csvPath)) {
    console.error('File not found:', csvPath);
    process.exit(1);
  }

  const filename = path.basename(csvPath);
  const guessedFromFilename = guessDBNameAndCategoryFromFilename(filename);

  console.log(`\n📂  Importing: ${csvPath}`);
  if (guessedFromFilename) {
    console.log(`    📋  Filename pattern detected: ${guessedFromFilename[0]} / ${guessedFromFilename[1]}`);
  }
  if (dbNameArg)  console.log(`    database_name override: "${dbNameArg}"`);
  if (categoryArg)console.log(`    category override:      "${categoryArg}"`);

  // -------------------------------------------------------------------------
  // Parse CSV
  // -------------------------------------------------------------------------
  const records = await new Promise((resolve, reject) => {
    const rows = [];
    fs.createReadStream(csvPath)
      .pipe(parse({ columns: true, skip_empty_lines: true, trim: true, bom: true }))
      .on('data', (row) => rows.push(row))
      .on('error', reject)
      .on('end', () => resolve(rows));
  });

  if (records.length === 0) {
    console.error('CSV file is empty or has no data rows.');
    process.exit(1);
  }

  const headers = Object.keys(records[0]);
  console.log(`\n📋  Detected ${records.length} rows, ${headers.length} columns`);
  console.log(`    Columns: ${headers.join(', ')}`);

  // Map column names
  const latCol    = findCol(headers, LAT_ALIASES);
  const lonCol    = findCol(headers, LON_ALIASES);
  const nameCol   = findCol(headers, NAME_ALIASES);
  const addrCol   = findCol(headers, ADDR_ALIASES);
  const cityCol   = findCol(headers, CITY_ALIASES);
  const stateCol  = findCol(headers, STATE_ALIASES);
  const zipCol    = findCol(headers, ZIP_ALIASES);
  const statusCol = findCol(headers, STATUS_ALIASES);
  const idCol     = findCol(headers, ID_ALIASES);
  const dbnameCol = findCol(headers, DBNAME_ALIASES);
  const catCol    = findCol(headers, CAT_ALIASES);
  const orgCol    = findCol(headers, ORG_ALIASES);

  console.log(`\n🔍  Column mapping:`);
  console.log(`    lat=${latCol || '⚠ NOT FOUND'}  lon=${lonCol || '⚠ NOT FOUND'}`);
  console.log(`    name=${nameCol}  address=${addrCol}  city=${cityCol}  state=${stateCol}`);
  console.log(`    id=${idCol}  database_name=${dbnameCol}  category=${catCol}`);

  const hasCoordsInFile = !!(latCol && lonCol);
  if (!hasCoordsInFile) {
    console.log('\n⚠️   No lat/lon columns found — will import address-only rows (geocoding later).');
    console.log(`    address col: ${addrCol || 'NOT FOUND'}  city: ${cityCol || 'NOT FOUND'}  state: ${stateCol || 'NOT FOUND'}`);
    if (!addrCol && !cityCol) {
      console.error('\n❌  No coordinates AND no address columns found — nothing to import.');
      process.exit(1);
    }
  }

  // -------------------------------------------------------------------------
  // Connect & ensure schema
  // -------------------------------------------------------------------------
  console.log('\n🔌  Connecting to PostgreSQL...');
  const client = await pool.connect();

  try {
    // Enable PostGIS if not already
    await client.query('CREATE EXTENSION IF NOT EXISTS postgis');

    // Ensure table exists (nullable location so address-only rows are allowed)
    await client.query(`
      CREATE TABLE IF NOT EXISTS environmental_sites (
        id            BIGSERIAL PRIMARY KEY,
        location      GEOMETRY(POINT, 4326),        -- nullable; filled by geocoder later
        database_name VARCHAR(200) NOT NULL,
        category      VARCHAR(50)  NOT NULL,
        source_org    VARCHAR(100),
        site_name     VARCHAR(500),
        address       TEXT,
        city          VARCHAR(200),
        state         CHAR(2),
        zip           VARCHAR(20),
        status        VARCHAR(100),
        registry_id   VARCHAR(100),
        source_id     VARCHAR(100),
        attributes    JSONB,
        geocoded      BOOLEAN DEFAULT FALSE,
        geocode_attempted BOOLEAN DEFAULT FALSE,
        imported_at   TIMESTAMPTZ DEFAULT NOW(),
        updated_at    TIMESTAMPTZ DEFAULT NOW()
      )
    `);

    await client.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS env_sites_source_id_uidx
        ON environmental_sites (source_id)
        WHERE source_id IS NOT NULL
    `);

    await client.query(`
      CREATE INDEX IF NOT EXISTS env_sites_location_gidx
        ON environmental_sites USING GIST (location)
        WHERE location IS NOT NULL
    `);

    // -----------------------------------------------------------------------
    // Import rows
    // -----------------------------------------------------------------------
    let inserted = 0, updated = 0, skipped = 0, addressOnly = 0, errors = 0;

    console.log('\n⬆️   Importing rows...\n');

    for (const row of records) {
      const latRaw = hasCoordsInFile ? getVal(row, latCol) : null;
      const lonRaw = hasCoordsInFile ? getVal(row, lonCol) : null;

      const lat = parseFloat(latRaw);
      const lon = parseFloat(lonRaw);

      const hasCoords = latRaw && lonRaw && !isNaN(lat) && !isNaN(lon) && lat !== 0 && lon !== 0
        && lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180;

      // If file has coord columns but this row is missing them, check for address fallback
      if (!hasCoords && hasCoordsInFile) {
        // Only keep as address-only if row has a usable address
        const addr = getVal(row, addrCol);
        const city = getVal(row, cityCol);
        if (!addr && !city) { skipped++; continue; }
        // fall through to address-only insert
      }

      const dbName   = dbNameArg  || (guessedFromFilename ? guessedFromFilename[0] : null) || getVal(row, dbnameCol) || 'UNKNOWN';
      const category = categoryArg|| (guessedFromFilename ? guessedFromFilename[1] : null) || getVal(row, catCol)    || normalizeCategoryFromDB(dbName);
      const sourceId = getVal(row, idCol);
      const siteName = getVal(row, nameCol);
      const address  = getVal(row, addrCol);
      const city     = getVal(row, cityCol);
      const state    = getVal(row, stateCol);
      const zip      = getVal(row, zipCol);
      const status   = getVal(row, statusCol);
      const org      = getVal(row, orgCol);

      // Collect any extra columns as attributes JSON
      const knownCols = new Set([latCol, lonCol, nameCol, addrCol, cityCol, stateCol,
        zipCol, statusCol, idCol, dbnameCol, catCol, orgCol].filter(Boolean));
      const extra = {};
      for (const [k, v] of Object.entries(row)) {
        if (!knownCols.has(k) && v !== null && v !== '' && v !== 'NULL') {
          extra[k] = v;
        }
      }

      try {
        const locationVal = hasCoords
          ? `ST_SetSRID(ST_MakePoint(${lon}, ${lat}), 4326)`
          : null;

        let res;
        if (hasCoords) {
          res = await client.query(
            `INSERT INTO environmental_sites
               (location, database_name, category, source_org, site_name,
                address, city, state, zip, status, source_id, attributes, geocoded)
             VALUES
               (ST_SetSRID(ST_MakePoint($1, $2), 4326),
                $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, TRUE)
             ON CONFLICT (source_id)
               WHERE source_id IS NOT NULL
             DO UPDATE SET
               location      = EXCLUDED.location,
               database_name = EXCLUDED.database_name,
               category      = EXCLUDED.category,
               source_org    = EXCLUDED.source_org,
               site_name     = EXCLUDED.site_name,
               address       = EXCLUDED.address,
               city          = EXCLUDED.city,
               state         = EXCLUDED.state,
               zip           = EXCLUDED.zip,
               status        = EXCLUDED.status,
               attributes    = EXCLUDED.attributes,
               geocoded      = TRUE,
               updated_at    = NOW()
             RETURNING (xmax = 0) AS was_inserted`,
            [lon, lat,
             dbName, category, org,
             siteName, address, city,
             state ? state.substring(0, 2) : null,
             zip, status, sourceId,
             Object.keys(extra).length > 0 ? JSON.stringify(extra) : null]
          );
        } else {
          // Address-only insert — location stays NULL until geocoded later
          res = await client.query(
            `INSERT INTO environmental_sites
               (location, database_name, category, source_org, site_name,
                address, city, state, zip, status, source_id, attributes, geocoded)
             VALUES
               (NULL, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, FALSE)
             ON CONFLICT (source_id)
               WHERE source_id IS NOT NULL
             DO UPDATE SET
               database_name = EXCLUDED.database_name,
               category      = EXCLUDED.category,
               source_org    = EXCLUDED.source_org,
               site_name     = EXCLUDED.site_name,
               address       = EXCLUDED.address,
               city          = EXCLUDED.city,
               state         = EXCLUDED.state,
               zip           = EXCLUDED.zip,
               status        = EXCLUDED.status,
               attributes    = EXCLUDED.attributes,
               updated_at    = NOW()
             RETURNING (xmax = 0) AS was_inserted`,
            [dbName, category, org,
             siteName, address, city,
             state ? state.substring(0, 2) : null,
             zip, status, sourceId,
             Object.keys(extra).length > 0 ? JSON.stringify(extra) : null]
          );
          addressOnly++;
        }

        if (res.rows[0]?.was_inserted) inserted++;
        else updated++;

      } catch (err) {
        errors++;
        if (errors <= 5) console.error(`  Row error: ${err.message} | row sample: ${siteName}`);
      }
    }

    // -----------------------------------------------------------------------
    // Summary
    // -----------------------------------------------------------------------
    console.log('\n✅  Import complete!');
    console.log('    ─────────────────────────────────');
    console.log(`    Inserted      : ${inserted}`);
    console.log(`    Updated       : ${updated}`);
    console.log(`    Address-only  : ${addressOnly}  (geocode pending)`);
    console.log(`    Skipped       : ${skipped}  (no coords or address)`);
    console.log(`    Errors        : ${errors}`);
    console.log('    ─────────────────────────────────');

    // Count totals
    const countRes  = await client.query('SELECT COUNT(*) FROM environmental_sites');
    const geocodePending = await client.query('SELECT COUNT(*) FROM environmental_sites WHERE location IS NULL');
    console.log(`    Total in DB   : ${countRes.rows[0].count} sites`);
    console.log(`    Geocode queue : ${geocodePending.rows[0].count} sites (no coordinates yet)\n`);

  } finally {
    client.release();
    await pool.end();
  }
}

// ---------------------------------------------------------------------------
// Guess category and database name from filename + column patterns
// ---------------------------------------------------------------------------
function guessDBNameAndCategoryFromFilename(filename) {
  const f = filename.toLowerCase();

  // EPA Facility Exchange patterns
  if (f.includes('ef_acres'))      return ['RCRA LQG', 'contamination'];
  if (f.includes('ef_icis_air'))   return ['AIR FACILITY', 'regulatory'];
  if (f.includes('ef_npdes'))      return ['NPDES', 'regulatory'];
  if (f.includes('ef_npl'))        return ['NPL', 'contamination'];
  if (f.includes('ef_rcra'))       return ['RCRA LQG', 'contamination'];
  if (f.includes('ef_tri'))        return ['TRIS', 'contamination'];

  // USGS data release patterns
  if (f.includes('ar40ar39'))      return ['USGS GEOLOGIC AGE', 'geology'];
  if (f.includes('be10al26'))      return ['USGS GEOLOGIC AGE', 'geology'];
  if (f.includes('c14_usgs'))      return ['USGS GEOLOGIC AGE', 'geology'];
  if (f.includes('fissiontrack'))  return ['USGS GEOLOGIC AGE', 'geology'];
  if (f.includes('kar_usgs') || f.includes('kar_usg'))    return ['USGS GEOLOGIC AGE', 'geology'];
  if (f.includes('luhf_usgs'))     return ['USGS GEOLOGIC AGE', 'geology'];
  if (f.includes('luminescence'))  return ['USGS GEOLOGIC AGE', 'geology'];
  if (f.includes('rbsr_usgs'))     return ['USGS GEOLOGIC AGE', 'geology'];
  if (f.includes('reos_usgs'))     return ['USGS GEOLOGIC AGE', 'geology'];
  if (f.includes('uthhe_usgs'))    return ['USGS GEOLOGIC AGE', 'geology'];
  if (f.includes('uthpbinsitu'))   return ['USGS GEOLOGIC AGE', 'geology'];
  if (f.includes('useriessolution'))  return ['USGS GEOLOGIC AGE', 'geology'];

  // Mine/geology databases
  if (f.includes('mineplant') || f.includes('mine'))  return ['MINES', 'geology'];
  if (f.includes('minerals'))      return ['MINES', 'geology'];
  if (f.includes('geochem'))       return ['USGS GEOLOGIC AGE', 'geology'];

  // Geochemistry/sediment samples
  if (f.includes('nuresed'))       return ['USGS GEOLOGIC AGE', 'geology'];
  if (f.includes('nurewtr'))       return ['USGS GEOLOGIC AGE', 'geology'];

  // Environmental/water
  if (f.includes('hydrowaste'))    return ['STORMWATER', 'hydrology'];
  if (f.includes('emri'))          return ['USGS GEOLOGIC AGE', 'geology'];

  return null;
}

function normalizeCategoryFromDB(dbName) {
  const d = dbName.toLowerCase();
  if (/rcra|cercl|npl|lust|ust|brownfield|corracts|tris|pcb|mgp|pfas|spill/.test(d)) return 'contamination';
  if (/echo|npdes|air|rmp|icis|dod|fuds|federal/.test(d))                             return 'regulatory';
  if (/flood|wetland|storm|hydro|water|nwi/.test(d))                                  return 'hydrology';
  if (/mine|geolog|radon|coal|ssurgo|asbestos/.test(d))                               return 'geology';
  if (/school|hospital|daycare|nursing|college|prison|receptor/.test(d))              return 'receptors';
  return 'contamination'; // safe default
}

main().catch(err => {
  console.error('\n❌  Fatal error:', err.message);
  if (err.message.includes('password') || err.message.includes('authentication')) {
    console.error('\n💡  Fix: open .env and set the correct PG_PASSWORD.');
  }
  if (err.message.includes('database') && err.message.includes('does not exist')) {
    console.error('\n💡  Fix: create the database first in DBeaver or psql:');
    console.error('    CREATE DATABASE geoscope;');
  }
  process.exit(1);
});
