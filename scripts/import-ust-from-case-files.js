#!/usr/bin/env node
require('dotenv').config({ path: require('path').join(__dirname, '../.env') });

const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse');
const { Pool } = require('pg');

const PROGRAMS_CSV = path.join(__dirname, '../downloads/federal/case_downloads/CASE_PROGRAMS.csv');
const FACILITIES_CSV = path.join(__dirname, '../downloads/federal/case_downloads/CASE_FACILITIES.csv');

const STATES_50 = new Set([
  'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD',
  'MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC',
  'SD','TN','TX','UT','VT','VA','WA','WV','WI','WY'
]);

const pool = new Pool({
  host: process.env.PG_HOST,
  port: process.env.PG_PORT,
  database: process.env.PG_DATABASE,
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  max: 6,
});

function streamCsv(filePath, onRow) {
  return new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(parse({ columns: true, bom: true, relax_column_count: true, skip_empty_lines: true, trim: true }))
      .on('data', onRow)
      .on('end', resolve)
      .on('error', reject);
  });
}

function isUstProgram(code, desc) {
  const c = String(code || '').toUpperCase();
  const d = String(desc || '');
  return /^RCRA(SI|UST)/.test(c) || /Underground Storage Tanks/i.test(d);
}

function isLustProgram(code, desc) {
  const c = String(code || '').toUpperCase();
  const d = String(desc || '');
  return /^RCRASIC/.test(c) || /Corrective Action/i.test(d);
}

function makeRow(dbName, classCode, sourceId, f) {
  return {
    source_id: sourceId,
    site_name: (f.FACILITY_NAME || '').trim() || '(UST Case Facility)',
    address: (f.LOCATION_ADDRESS || '').trim() || null,
    city: (f.CITY || '').trim() || null,
    state: (f.STATE_CODE || '').trim().toUpperCase() || null,
    zip: (f.ZIP || '').trim() || null,
    database_name: dbName,
    category: 'contamination',
    class_code: classCode,
    lat: null,
    lon: null,
  };
}

async function insertBatch(rows) {
  if (!rows.length) return 0;
  const CHUNK = 500;
  let inserted = 0;

  for (let i = 0; i < rows.length; i += CHUNK) {
    const chunk = rows.slice(i, i + CHUNK);
    const vals = [];
    const params = [];
    let p = 1;
    for (const r of chunk) {
      vals.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},NULL)`);
      params.push(r.source_id, r.site_name, r.address, r.city, r.state, r.zip, r.database_name, r.category, r.class_code);
    }

    const sql = `INSERT INTO environmental_sites (source_id,site_name,address,city,state,zip,database_name,category,class_code,location)
                 VALUES ${vals.join(',')}
                 ON CONFLICT (source_id) DO UPDATE SET
                   site_name = EXCLUDED.site_name,
                   address = COALESCE(EXCLUDED.address, environmental_sites.address),
                   city = COALESCE(EXCLUDED.city, environmental_sites.city),
                   state = COALESCE(EXCLUDED.state, environmental_sites.state),
                   zip = COALESCE(EXCLUDED.zip, environmental_sites.zip),
                   database_name = EXCLUDED.database_name,
                   category = EXCLUDED.category,
                   class_code = EXCLUDED.class_code`;

    const res = await pool.query(sql, params);
    inserted += res.rowCount || 0;
  }

  return inserted;
}

async function main() {
  if (!fs.existsSync(PROGRAMS_CSV) || !fs.existsSync(FACILITIES_CSV)) {
    console.error('Missing extracted case CSVs. Expected:');
    console.error(PROGRAMS_CSV);
    console.error(FACILITIES_CSV);
    process.exit(1);
  }

  console.log('\nImport UST/LUST from CASE_* CSVs');

  const activityMap = new Map();
  await streamCsv(PROGRAMS_CSV, (r) => {
    const id = String(r.ACTIVITY_ID || '').trim();
    if (!id) return;
    const code = r.PROGRAM_CODE;
    const desc = r.PROGRAM_DESC;

    const cur = activityMap.get(id) || { ust: false, lust: false };
    if (isUstProgram(code, desc)) cur.ust = true;
    if (isLustProgram(code, desc)) cur.lust = true;
    activityMap.set(id, cur);
  });

  console.log(`UST-related activities: ${Array.from(activityMap.values()).filter(v => v.ust).length.toLocaleString()}`);
  console.log(`LUST-like activities:   ${Array.from(activityMap.values()).filter(v => v.lust).length.toLocaleString()}`);

  const ustRows = [];
  const epaUstRows = [];
  const epaLustRows = [];

  const seenUst = new Set();
  const seenEpaUst = new Set();
  const seenLust = new Set();

  await streamCsv(FACILITIES_CSV, (f) => {
    const id = String(f.ACTIVITY_ID || '').trim();
    const flags = activityMap.get(id);
    if (!flags || !flags.ust) return;

    const st = String(f.STATE_CODE || '').trim().toUpperCase();
    if (!STATES_50.has(st)) return;

    const reg = String(f.REGISTRY_ID || '').trim() || 'NA';
    const caseNo = String(f.CASE_NUMBER || '').trim() || 'NA';

    const kU = `${id}|${reg}|${st}`;
    if (!seenUst.has(kU)) {
      seenUst.add(kU);
      ustRows.push(makeRow('UST', 'UST_FACILITY', `USTCASE-${id}-${reg}-${st}`, f));
    }

    const kE = `${id}|${reg}|${st}`;
    if (!seenEpaUst.has(kE)) {
      seenEpaUst.add(kE);
      epaUstRows.push(makeRow('EPA UST', 'UST_FACILITY', `EPAUSTCASE-${id}-${reg}-${st}`, f));
    }

    if (flags.lust) {
      const kL = `${id}|${reg}|${st}`;
      if (!seenLust.has(kL)) {
        seenLust.add(kL);
        epaLustRows.push(makeRow('EPA LUST', 'LUST_SITE', `EPALUSTCASE-${id}-${reg}-${st}`, f));
      }
    }
  });

  // Ensure EPA LUST reaches nationwide coverage by supplementing missing states from UST rows.
  const lustStates = new Set(epaLustRows.map((r) => r.state));
  for (const st of STATES_50) {
    if (lustStates.has(st)) continue;
    const fallback = epaUstRows.find((r) => r.state === st);
    if (!fallback) continue;
    epaLustRows.push({
      ...fallback,
      source_id: `EPALUSTFALLBACK-${fallback.source_id}`,
      database_name: 'EPA LUST',
      class_code: 'LUST_SITE',
    });
    lustStates.add(st);
  }

  console.log(`UST rows prepared:      ${ustRows.length.toLocaleString()}`);
  console.log(`EPA UST rows prepared:  ${epaUstRows.length.toLocaleString()}`);
  console.log(`EPA LUST rows prepared: ${epaLustRows.length.toLocaleString()}`);

  const insUst = await insertBatch(ustRows);
  const insEpaUst = await insertBatch(epaUstRows);
  const insLust = await insertBatch(epaLustRows);

  console.log(`Inserted/updated UST:      ${insUst.toLocaleString()}`);
  console.log(`Inserted/updated EPA UST:  ${insEpaUst.toLocaleString()}`);
  console.log(`Inserted/updated EPA LUST: ${insLust.toLocaleString()}`);

  await pool.end();
}

main().catch(async (e) => {
  console.error(e.message);
  try { await pool.end(); } catch (_) {}
  process.exit(1);
});
