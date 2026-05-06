#!/usr/bin/env node
require('dotenv').config({ path: require('path').join(__dirname, '../.env') });

const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');

const INPUT = path.join(__dirname, '../downloads/missing/NURSING_HOMES/cms_nh_provider_info.csv');
const VALID_STATES = new Set(['AK','AL','AR','AZ','CA','CO','CT','DE','FL','GA','HI','IA','ID','IL','IN','KS','KY','LA','MA','MD','ME','MI','MN','MO','MS','MT','NC','ND','NE','NH','NJ','NM','NV','NY','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VA','VT','WA','WI','WV','WY','DC']);

const pool = new Pool({
  host: process.env.PG_HOST,
  port: process.env.PG_PORT,
  database: process.env.PG_DATABASE,
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  max: 4,
});

function normalizeObjects(raw) {
  const t = raw.trim();
  if (!t) return [];

  // Case 1: proper JSON array or object stream
  try {
    const v = JSON.parse(t);
    if (Array.isArray(v)) return v;
    if (v && typeof v === 'object') {
      if (Array.isArray(v.results)) return v.results;
      if (Array.isArray(v.data)) return v.data;
      return [v];
    }
  } catch (_) {}

  // Case 2: line starts after BOM / weird prefix but contains { ... }
  const firstBrace = t.indexOf('{');
  const lastBrace = t.lastIndexOf('}');
  if (firstBrace >= 0 && lastBrace > firstBrace) {
    const slice = t.slice(firstBrace, lastBrace + 1);

    // 2a: concatenated objects with '},{'
    const wrapped = '[' + slice.replace(/}\s*,\s*{/g, '},{') + ']';
    try {
      const arr = JSON.parse(wrapped);
      if (Array.isArray(arr)) return arr;
    } catch (_) {}

    // 2b: NDJSON-ish: split by lines and parse objects
    const objs = [];
    for (const ln of slice.split(/\r?\n/)) {
      const s = ln.trim().replace(/,$/, '');
      if (!s.startsWith('{')) continue;
      try { objs.push(JSON.parse(s)); } catch (_) {}
    }
    if (objs.length) return objs;
  }

  // Case 3: truncated JSON object that still contains a complete results array stream
  const resultsMarker = t.indexOf('"results":[');
  if (resultsMarker >= 0) {
    const startObj = t.indexOf('{', resultsMarker + '"results":['.length);
    if (startObj >= 0) {
      const objs = [];
      let depth = 0;
      let inString = false;
      let escaped = false;
      let objStart = -1;

      for (let i = startObj; i < t.length; i += 1) {
        const ch = t[i];

        if (inString) {
          if (escaped) {
            escaped = false;
          } else if (ch === '\\') {
            escaped = true;
          } else if (ch === '"') {
            inString = false;
          }
          continue;
        }

        if (ch === '"') {
          inString = true;
          continue;
        }

        if (ch === '{') {
          if (depth === 0) objStart = i;
          depth += 1;
          continue;
        }

        if (ch === '}') {
          if (depth > 0) depth -= 1;
          if (depth === 0 && objStart >= 0) {
            const candidate = t.slice(objStart, i + 1);
            try {
              const parsed = JSON.parse(candidate);
              if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
                objs.push(parsed);
              }
            } catch (_) {}
            objStart = -1;
          }
        }
      }

      if (objs.length) return objs;
    }
  }

  return [];
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
  if (!fs.existsSync(INPUT)) {
    console.error('Missing file:', INPUT);
    process.exit(1);
  }
  const raw = fs.readFileSync(INPUT, 'utf8');
  const objs = normalizeObjects(raw);
  console.log('Parsed objects:', objs.length);
  if (!objs.length) {
    console.error('No parseable objects found in provider_info source.');
    process.exit(2);
  }

  const start = await pool.query("select count(*) c from environmental_sites where database_name='CMS Nursing Homes'");
  console.log('CMS Nursing Homes start rows:', start.rows[0].c);

  const rows = [];
  for (const o of objs) {
    const state = String(o.state || o.STATE || '').trim().toUpperCase();
    const lat = parseFloat(o.latitude || o.LATITUDE || '');
    const lon = parseFloat(o.longitude || o.LONGITUDE || '');
    const ccn = String(o.cms_certification_number_ccn || o.ccn || o.provider_id || '').trim();
    if (!ccn || !VALID_STATES.has(state) || !Number.isFinite(lat) || !Number.isFinite(lon)) continue;

    rows.push({
      source_id: `CMSNH-${ccn}`,
      site_name: String(o.provider_name || o.name || '').trim() || '(CMS Nursing Home)',
      address: String(o.provider_address || o.address || '').trim() || null,
      city: String(o.citytown || o.city || '').trim() || null,
      state,
      zip: String(o.zip_code || o.zip || '').trim() || null,
      database_name: 'CMS Nursing Homes',
      category: 'healthcare',
      class_code: 'NURSING_HOME',
      lat,
      lon,
    });
  }

  console.log('Eligible rows:', rows.length);
  const CHUNK = 500;
  let ins = 0;
  for (let i = 0; i < rows.length; i += CHUNK) {
    ins += await insertBatch(rows.slice(i, i + CHUNK));
  }
  console.log('Inserted/updated:', ins);

  const spread = await pool.query("select state,count(*) c from environmental_sites where database_name='CMS Nursing Homes' and state is not null group by state order by state");
  console.log('Distinct states now:', spread.rowCount);

  await pool.end();
}

main().catch((e) => {
  console.error(e.message);
  process.exit(1);
});
