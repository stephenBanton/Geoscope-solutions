#!/usr/bin/env node
// =============================================================================
// GeoScope Targeted Import  — BATCH version (1000 rows per INSERT = 100x faster)
// Maps specific H-drive files → correct GeoScope database_name values
// Handles EPA ECHO multi-type files (RCRA, NPL) by splitting on pgm_sys_acrnm
// =============================================================================
require('dotenv').config({ path: require('path').join(__dirname, '../.env') });

const fs   = require('fs');
const path = require('path');
const { parse } = require('csv-parse');
const { pool } = require('../db');

const BATCH_SIZE = 1000;

// pgm_sys_acrnm → { db, cat }
const ACRNM_MAP = {
  // Actual EPA ECHO pgm_sys_acrnm values seen in EF_* files
  'RCRAINFO':        { db: 'RCRA LQG',           cat: 'contamination' },
  'RCRA':            { db: 'RCRA LQG',           cat: 'contamination' },
  'RCRA_LQG':        { db: 'RCRA LQG',           cat: 'contamination' },
  'RCRA_SQG':        { db: 'RCRA SQG',           cat: 'contamination' },
  'RCRA_VSQG':       { db: 'RCRA VSQG',          cat: 'contamination' },
  'RCRA_CESQG':      { db: 'RCRA VSQG',          cat: 'contamination' },
  'RCRA_TSDF':       { db: 'RCRA TSDF',          cat: 'contamination' },
  // NPL / CERCLIS — EPA ECHO uses SEMS program acronym
  'SEMS':            { db: 'NPL',                cat: 'contamination' },
  'NPL':             { db: 'NPL',                cat: 'contamination' },
  'PROPOSED_NPL':    { db: 'PROPOSED NPL',       cat: 'contamination' },
  'DELISTED_NPL':    { db: 'DELISTED NPL',       cat: 'contamination' },
  'CERCLIS':         { db: 'CERCLIS',            cat: 'contamination' },
  'CERCLIS_NFRAP':   { db: 'DELISTED NPL',       cat: 'contamination' },
  'CORRACTS':        { db: 'CORRACTS',           cat: 'contamination' },
  // Air
  'ICIS_AIR':        { db: 'AIR FACILITY',       cat: 'regulatory'   },
  'ICIS-AIR':        { db: 'AIR FACILITY',       cat: 'regulatory'   },
  'AIR':             { db: 'AIR FACILITY',       cat: 'regulatory'   },
  'EIS':             { db: 'AIR FACILITY',       cat: 'regulatory'   },
  // Water / regulatory
  'ICIS_NPDES':      { db: 'NPDES',             cat: 'regulatory'   },
  'NPDES':           { db: 'NPDES',             cat: 'regulatory'   },
  'NPDES_STORM':     { db: 'STORMWATER',         cat: 'hydrology'    },
  'RMP':             { db: 'RMP',               cat: 'regulatory'   },
  'ICIS':            { db: 'ICIS',              cat: 'regulatory'   },
  // TRI
  'TRIS':            { db: 'TRIS',              cat: 'contamination' },
  'TRI':             { db: 'TRIS',              cat: 'contamination' },
  // PFAS / FRS
  'PFAS':            { db: 'PFAS FEDERAL SITES', cat: 'contamination' },
  'FRS':             { db: 'FEDERAL FACILITY',   cat: 'regulatory'   },
  'ACRES':           { db: 'BROWNFIELDS',        cat: 'contamination' },
};

// Filename pattern → { db, cat }
const FILE_OVERRIDES = [
  { re: /mineplant/i,           db: 'MINE OPERATIONS',   cat: 'geology'       },
  { re: /HydroWASTE/i,          db: 'STORMWATER',        cat: 'hydrology'     },
  { re: /EF_ACRES/i,            db: 'BROWNFIELDS',       cat: 'contamination' },
  { re: /ussoils/i,             db: 'SSURGO',            cat: 'geology'       },
  { re: /ngdbsoil[/\\]main/i,   db: 'SSURGO',            cat: 'geology'       },
  { re: /Air_Quality/i,         db: 'AIR FACILITY',      cat: 'regulatory'    },
  { re: /EF_ICIS_AIR/i,         db: 'AIR FACILITY',      cat: 'regulatory'    },
  { re: /USGS_.*_US_CSV.*Site/i,db: 'MINE OPERATIONS',   cat: 'geology'       },
];

function findCol(h, aliases) {
  const lower = h.map(x => x.toLowerCase().trim());
  for (const a of aliases) { const i = lower.indexOf(a); if (i !== -1) return h[i]; }
  return null;
}

function getV(row, col) {
  if (!col) return null;
  const x = row[col];
  if (x === undefined || x === null) return null;
  const s = String(x).trim();
  return (s === '' || s === 'NULL' || s === 'N/A' || s === 'n/a') ? null : s;
}

function resolveDbCat(acrnmVal, filePath, dbOverride, catOverride) {
  if (dbOverride) return { db: dbOverride, cat: catOverride || 'contamination' };
  if (acrnmVal) {
    const key = acrnmVal.toUpperCase().replace(/[-\s]/g, '_');
    if (ACRNM_MAP[key]) return ACRNM_MAP[key];
    for (const [k, val] of Object.entries(ACRNM_MAP)) {
      if (key.startsWith(k) || k.startsWith(key)) return val;
    }
  }
  for (const fo of FILE_OVERRIDES) {
    if (fo.re.test(filePath)) return { db: fo.db, cat: fo.cat };
  }
  return null;
}

// ---------------------------------------------------------------------------
// Flush one batch using multi-row INSERT
// 14 params per row: lon, lat, db, cat, org, name, addr, city, state, zip,
//                    status, source_id, attributes, geocoded
// ---------------------------------------------------------------------------
async function flushBatch(client, batch) {
  if (!batch.length) return;
  // Deduplicate by source_id to prevent "ON CONFLICT DO UPDATE command cannot affect row a second time"
  const seen = new Map();
  for (const r of batch) {
    const key = r.sourceId || `__nosrc_${Math.random()}`;
    seen.set(key, r);
  }
  const deduped = Array.from(seen.values());

  const vp = [];
  const params = [];
  for (let i = 0; i < deduped.length; i++) {
    const r  = deduped[i];
    const b  = i * 14 + 1;
    const locExpr = r.hasCoords
      ? `ST_SetSRID(ST_MakePoint($${b},$${b+1}),4326)`
      : `NULL`;
    vp.push(`(${locExpr},$${b+2},$${b+3},$${b+4},$${b+5},$${b+6},$${b+7},$${b+8},$${b+9},$${b+10},$${b+11},$${b+12},$${b+13})`);
    params.push(
      r.hasCoords ? r.lon : null,
      r.hasCoords ? r.lat : null,
      r.db, r.cat, null,
      r.name, r.address, r.city, r.state, r.zip, r.status,
      r.sourceId, r.extra, r.geocoded
    );
  }
  const sql = `
    INSERT INTO environmental_sites
      (location,database_name,category,source_org,
       site_name,address,city,state,zip,status,
       source_id,attributes,geocoded)
    VALUES ${vp.join(',')}
    ON CONFLICT (source_id) WHERE source_id IS NOT NULL
    DO UPDATE SET
      location      = COALESCE(EXCLUDED.location, environmental_sites.location),
      database_name = EXCLUDED.database_name,
      category      = EXCLUDED.category,
      site_name     = COALESCE(EXCLUDED.site_name, environmental_sites.site_name),
      address       = COALESCE(EXCLUDED.address,   environmental_sites.address),
      city          = COALESCE(EXCLUDED.city,      environmental_sites.city),
      state         = COALESCE(EXCLUDED.state,     environmental_sites.state),
      zip           = COALESCE(EXCLUDED.zip,       environmental_sites.zip),
      status        = EXCLUDED.status,
      attributes    = EXCLUDED.attributes,
      geocoded      = EXCLUDED.geocoded OR environmental_sites.geocoded,
      updated_at    = NOW()`;
  await client.query(sql, params);
}

// ---------------------------------------------------------------------------
// Import one CSV file
// ---------------------------------------------------------------------------
async function importFile(filePath, dbOverride, catOverride, client) {
  if (!fs.existsSync(filePath)) {
    process.stdout.write(`  SKIP (not found): ${path.basename(filePath)}\n`);
    return { inserted: 0, skipped: 0 };
  }
  process.stdout.write(`\n>>> ${path.basename(filePath)}\n`);

  let headers = null;
  let latCol, lonCol, nameCol, addrCol, cityCol, stateCol, zipCol, statusCol, idCol, acrnmCol;
  const allRows = [];
  const buckets = {};
  let skippedRows = 0;

  // Collect all rows first (streaming)
  await new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(parse({ columns: true, skip_empty_lines: true, trim: true, bom: true, relax_column_count: true }))
      .on('error', reject)
      .on('end', resolve)
      .on('data', (row) => {
        if (!headers) {
          headers = Object.keys(row);
          latCol    = findCol(headers, ['latitude','lat','lat_wwtp','lat_out','lat_dd','latitude83','y_coord','y','latitude_wgs84','lat_wgs84']);
          lonCol    = findCol(headers, ['longitude','lon','long','lon_wwtp','lon_out','lon_dd','longitude83','x_coord','x','longitude_wgs84','lon_wgs84']);
          nameCol   = findCol(headers, ['primary_name','site_name','facility_name','name','wwtp_name','plant_min']);
          addrCol   = findCol(headers, ['location_address','address','street_address','addr','fac_street']);
          cityCol   = findCol(headers, ['city_name','city','fac_city']);
          stateCol  = findCol(headers, ['state_code','state','fac_state','state_loca']);
          zipCol    = findCol(headers, ['postal_code','zip','fac_zip']);
          statusCol = findCol(headers, ['status','activity_status']);
          idCol     = findCol(headers, ['registry_id','source_id','site_id','waste_id','id','pgm_sys_id']);
          acrnmCol  = findCol(headers, ['pgm_sys_acrnm','pgm_acrnm']);
          process.stdout.write(`    lat=${latCol||'?'} lon=${lonCol||'?'} id=${idCol||'?'} acrnm=${acrnmCol||'none'}\n`);
        }

        const acrnmVal = getV(row, acrnmCol);
        const resolved = resolveDbCat(acrnmVal, filePath, dbOverride, catOverride);
        if (!resolved) { skippedRows++; return; }

        const { db: db_, cat: cat_ } = resolved;
        const latRaw = getV(row, latCol);
        const lonRaw = getV(row, lonCol);
        const lat = parseFloat(latRaw);
        const lon = parseFloat(lonRaw);
        const hasCoords = latRaw && lonRaw && !isNaN(lat) && !isNaN(lon) &&
                          lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180 && !(lat === 0 && lon === 0);

        const nameVal = getV(row, nameCol);
        const addrVal = getV(row, addrCol);
        if (!hasCoords && !addrVal && !nameVal) { skippedRows++; return; }

        const stateVal = getV(row, stateCol);
        const knownCols = new Set([latCol,lonCol,nameCol,addrCol,cityCol,stateCol,zipCol,statusCol,idCol,acrnmCol].filter(Boolean));
        const extra = {};
        for (const [k, x] of Object.entries(row)) {
          if (!knownCols.has(k) && x && x !== 'NULL' && x !== 'N/A') extra[k] = x;
        }

        allRows.push({
          hasCoords,
          lon: hasCoords ? lon : null, lat: hasCoords ? lat : null,
          db: db_, cat: cat_,
          const buckets = {};
          let skippedRows = 0;
          let inserted = 0, batchErrors = 0;
          let rowCount = 0;
          let streamBatch = [];

          const flushStream = async () => {
            if (!streamBatch.length) return;
            try {
              await flushBatch(client, streamBatch);
              inserted += streamBatch.length;
            } catch (err) {
              batchErrors++;
              if (batchErrors <= 3) process.stdout.write(`    Batch err: ${err.message}\n`);
            }
            streamBatch = [];
          };

          // Streaming: process rows as they arrive, flush every BATCH_SIZE rows
          await new Promise((resolve, reject) => {
            const parser = parse({ columns: true, skip_empty_lines: true, trim: true, bom: true, relax_column_count: true });
            let pending = Promise.resolve();

            parser.on('error', reject);
            parser.on('end', () => pending.then(resolve).catch(reject));
            parser.on('data', (row) => {
              if (!headers) {
                headers = Object.keys(row);
                latCol    = findCol(headers, ['latitude','lat','lat_wwtp','lat_out','lat_dd','latitude83','y_coord','y','latitude_wgs84','lat_wgs84']);
                lonCol    = findCol(headers, ['longitude','lon','long','lon_wwtp','lon_out','lon_dd','longitude83','x_coord','x','longitude_wgs84','lon_wgs84']);
                nameCol   = findCol(headers, ['primary_name','site_name','facility_name','name','wwtp_name','plant_min']);
                addrCol   = findCol(headers, ['location_address','address','street_address','addr','fac_street']);
                cityCol   = findCol(headers, ['city_name','city','fac_city']);
                stateCol  = findCol(headers, ['state_code','state','fac_state','state_loca']);
                zipCol    = findCol(headers, ['postal_code','zip','fac_zip']);
                statusCol = findCol(headers, ['status','activity_status']);
                idCol     = findCol(headers, ['registry_id','source_id','site_id','waste_id','id','pgm_sys_id']);
                acrnmCol  = findCol(headers, ['pgm_sys_acrnm','pgm_acrnm']);
                process.stdout.write(`    lat=${latCol||'?'} lon=${lonCol||'?'} id=${idCol||'?'} acrnm=${acrnmCol||'none'}\n`);
              }

              const acrnmVal = getV(row, acrnmCol);
              const resolved = resolveDbCat(acrnmVal, filePath, dbOverride, catOverride);
              if (!resolved) { skippedRows++; return; }

              const { db: db_, cat: cat_ } = resolved;
              const latRaw = getV(row, latCol);
              const lonRaw = getV(row, lonCol);
              const lat = parseFloat(latRaw);
              const lon = parseFloat(lonRaw);
              const hasCoords = latRaw && lonRaw && !isNaN(lat) && !isNaN(lon) &&
                                lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180 && !(lat === 0 && lon === 0);

              const nameVal = getV(row, nameCol);
              const addrVal = getV(row, addrCol);
              if (!hasCoords && !addrVal && !nameVal) { skippedRows++; return; }

              const stateVal = getV(row, stateCol);
              const knownCols = new Set([latCol,lonCol,nameCol,addrCol,cityCol,stateCol,zipCol,statusCol,idCol,acrnmCol].filter(Boolean));
              const extra = {};
              for (const [k, x] of Object.entries(row)) {
                if (!knownCols.has(k) && x && x !== 'NULL' && x !== 'N/A') extra[k] = x;
              }

              streamBatch.push({
                hasCoords,
                lon: hasCoords ? lon : null, lat: hasCoords ? lat : null,
                db: db_, cat: cat_,
                name: nameVal, address: addrVal,
                city: getV(row, cityCol),
                state: stateVal ? stateVal.substring(0, 2) : null,
                zip: getV(row, zipCol), status: getV(row, statusCol),
                sourceId: getV(row, idCol),
                extra: Object.keys(extra).length ? JSON.stringify(extra) : null,
                geocoded: hasCoords,
              });
              buckets[db_] = (buckets[db_] || 0) + 1;
              rowCount++;

              if (streamBatch.length >= BATCH_SIZE) {
                parser.pause();
                const chunk = streamBatch;
                streamBatch = [];
                pending = pending.then(() => flushBatch(client, chunk).then(() => {
                  inserted += chunk.length;
                  if (inserted % 50000 === 0) process.stdout.write(`\r    Inserted: ${inserted.toLocaleString()} rows...`);
                  parser.resume();
                }).catch(err => {
                  batchErrors++;
                  if (batchErrors <= 3) process.stdout.write(`\n    Batch err: ${err.message}\n`);
                  parser.resume();
                }));
              }
            });

            fs.createReadStream(filePath).pipe(parser);
          });

          // Flush any remaining rows
          if (streamBatch.length) {
            try { await flushBatch(client, streamBatch); inserted += streamBatch.length; }
            catch (err) { batchErrors++; process.stdout.write(`\n    Final batch err: ${err.message}\n`); }
          }
  process.stdout.write(`Inserted this run : ${totalIns.toLocaleString()}\n`);
  process.stdout.write(`Skipped this run  : ${totalSkip.toLocaleString()}\n`);
  process.stdout.write(`Total in DB       : ${parseInt(tot.rows[0].count).toLocaleString()}\n`);
  process.stdout.write('\nAll loaded database buckets:\n');
  byDb.rows.forEach(r => process.stdout.write(`  ${r.database_name}: ${parseInt(r.cnt).toLocaleString()}\n`));
}

main().catch(async err => {
  process.stdout.write(`\nFATAL: ${err.message}\n`);
  try { await pool.end(); } catch {}
  process.exit(1);
});
