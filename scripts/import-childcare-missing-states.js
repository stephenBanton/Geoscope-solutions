#!/usr/bin/env node
require('dotenv').config({ path: require('path').join(__dirname, '../.env') });

const { Pool } = require('pg');

const pool = new Pool({
  host: process.env.PG_HOST,
  port: process.env.PG_PORT,
  database: process.env.PG_DATABASE,
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  max: 4,
});

const SERVICE_URL = 'https://services2.arcgis.com/FiaPA4ga0iQKduv3/arcgis/rest/services/Child_Care_Centers_%28Archive%29/FeatureServer/0/query';
const TARGET_STATES = ['KS', 'ND', 'RI', 'SD', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV'];
const PAGE_SIZE = 2000;

function normalizeStr(v) {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  return s.length ? s : null;
}

function parseNum(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function toRow(attrs) {
  const state = normalizeStr(attrs.STATE);
  if (!state || !TARGET_STATES.includes(state)) return null;

  const idPart = normalizeStr(attrs.ID) || String(attrs.OBJECTID || '').trim();
  if (!idPart) return null;

  return {
    source_id: `HIFLD_ARC_CC_${idPart}`,
    site_name: normalizeStr(attrs.NAME) || 'Child Care Site',
    address: normalizeStr(attrs.ADDRESS),
    city: normalizeStr(attrs.CITY),
    state,
    zip: normalizeStr(attrs.ZIP),
    database_name: 'HIFLD Child Care',
    category: 'education',
    class_code: 'CHILD_CARE',
    lat: parseNum(attrs.LATITUDE),
    lon: parseNum(attrs.LONGITUDE),
  };
}

async function fetchStateFeatures(state) {
  const all = [];
  let offset = 0;

  while (true) {
    const params = new URLSearchParams({
      where: `STATE='${state}'`,
      outFields: 'OBJECTID,ID,NAME,ADDRESS,CITY,STATE,ZIP,LATITUDE,LONGITUDE',
      f: 'json',
      returnGeometry: 'false',
      resultOffset: String(offset),
      resultRecordCount: String(PAGE_SIZE),
      orderByFields: 'OBJECTID ASC',
    });

    const res = await fetch(`${SERVICE_URL}?${params.toString()}`);
    if (!res.ok) {
      throw new Error(`ArcGIS fetch failed for ${state}: HTTP ${res.status}`);
    }

    const data = await res.json();
    const feats = Array.isArray(data.features) ? data.features : [];
    if (!feats.length) break;

    for (const f of feats) {
      const row = toRow(f.attributes || {});
      if (row) all.push(row);
    }

    if (feats.length < PAGE_SIZE) break;
    offset += feats.length;
  }

  return all;
}

async function upsertBatch(rows) {
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

  const sql = `
    INSERT INTO environmental_sites
      (source_id, site_name, address, city, state, zip, database_name, category, class_code, location)
    VALUES ${vals.join(',')}
    ON CONFLICT (source_id) DO UPDATE SET
      site_name = EXCLUDED.site_name,
      address = COALESCE(EXCLUDED.address, environmental_sites.address),
      city = COALESCE(EXCLUDED.city, environmental_sites.city),
      state = COALESCE(EXCLUDED.state, environmental_sites.state),
      zip = COALESCE(EXCLUDED.zip, environmental_sites.zip),
      database_name = EXCLUDED.database_name,
      category = EXCLUDED.category,
      class_code = EXCLUDED.class_code,
      location = COALESCE(EXCLUDED.location, environmental_sites.location)
  `;

  const r = await pool.query(sql, params);
  return r.rowCount || 0;
}

function dedupRows(rows) {
  const map = new Map();
  for (const r of rows) map.set(r.source_id, r);
  return Array.from(map.values());
}

async function main() {
  console.log('Import missing-state Child Care from ArcGIS archive');

  let allRows = [];
  for (const st of TARGET_STATES) {
    const rows = await fetchStateFeatures(st);
    allRows = allRows.concat(rows);
    console.log(`${st}: fetched ${rows.length.toLocaleString()}`);
  }

  const deduped = dedupRows(allRows);
  console.log(`Total rows fetched: ${allRows.length.toLocaleString()}`);
  console.log(`Rows after dedup:   ${deduped.length.toLocaleString()}`);

  const BATCH = 1000;
  let total = 0;
  for (let i = 0; i < deduped.length; i += BATCH) {
    const chunk = deduped.slice(i, i + BATCH);
    total += await upsertBatch(chunk);
  }

  console.log(`Inserted/updated:   ${total.toLocaleString()}`);
  await pool.end();
}

main().catch(async (e) => {
  console.error(e);
  try { await pool.end(); } catch {}
  process.exit(1);
});
