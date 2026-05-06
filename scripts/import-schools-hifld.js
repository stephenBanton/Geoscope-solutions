#!/usr/bin/env node
/**
 * Schools importer - HIFLD ArcGIS Public Schools + Private Schools
 * Fetches all 50 states via ArcGIS REST with pagination
 * Stores as database_name='SCHOOLS PUBLIC' and 'SCHOOLS PRIVATE'
 */
require('dotenv').config({ path: require('path').join(__dirname, '../.env') });

const https = require('https');
const { Pool } = require('pg');

const pool = new Pool({
  host: process.env.PG_HOST,
  port: process.env.PG_PORT,
  database: process.env.PG_DATABASE,
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  max: 4,
});

const ALL_STATES = [
  'AK','AL','AR','AZ','CA','CO','CT','DE','FL','GA',
  'HI','IA','ID','IL','IN','KS','KY','LA','MA','MD',
  'ME','MI','MN','MO','MS','MT','NC','ND','NE','NH',
  'NJ','NM','NV','NY','OH','OK','OR','PA','RI','SC',
  'SD','TN','TX','UT','VA','VT','WA','WI','WV','WY',
];

// HIFLD ArcGIS services
const SERVICES = {
  public:  'https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Public_Schools/FeatureServer/0',
  private: 'https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Private_Schools/FeatureServer/0',
};

function fetchJson(url, timeoutMs = 25000) {
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

function buildUrl(serviceUrl, stateCode, offset) {
  const where = encodeURIComponent(`STATE='${stateCode}'`);
  return `${serviceUrl}/query?where=${where}&outFields=NCESSCH,OBJECTID,NAME,STREET,CITY,STATE,ZIP,LATITUDE,LONGITUDE,NAICS_DESC,ENROLLMENT&outSR=4326&f=json&resultOffset=${offset}&resultRecordCount=2000`;
}

function mapFeature(f, dbName, classCode, prefix) {
  const a = f.attributes || {};
  const lat = parseFloat(a.LATITUDE || (f.geometry && f.geometry.y) || '');
  const lon = parseFloat(a.LONGITUDE || (f.geometry && f.geometry.x) || '');
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
  const id = a.NCESSCH || a.OBJECTID || `${lat}_${lon}`;
  return {
    source_id: `${prefix}-${id}`,
    site_name: (a.NAME || '').trim() || '(Unknown School)',
    address: (a.STREET || '').trim() || null,
    city: (a.CITY || '').trim() || null,
    state: (a.STATE || '').trim() || null,
    zip: (a.ZIP || '').trim() || null,
    database_name: dbName,
    category: 'education',
    class_code: classCode,
    lat,
    lon,
  };
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
                 location = COALESCE(EXCLUDED.location, environmental_sites.location)
               WHERE environmental_sites.state IS NULL OR EXCLUDED.state IS NOT NULL`;
  const res = await pool.query(sql, params);
  return res.rowCount || 0;
}

async function importSchoolsForState(serviceUrl, stateCode, dbName, classCode, prefix) {
  let offset = 0;
  let totalInserted = 0;
  while (true) {
    const url = buildUrl(serviceUrl, stateCode, offset);
    let data = null;
    for (let attempt = 0; attempt < 3 && !data; attempt++) {
      data = await fetchJson(url);
      if (!data) await new Promise((r) => setTimeout(r, 500 * (attempt + 1)));
    }
    if (!data || !Array.isArray(data.features) || data.features.length === 0) break;

    const rows = data.features.map((f) => mapFeature(f, dbName, classCode, prefix)).filter(Boolean);
    const CHUNK = 500;
    let ins = 0;
    for (let i = 0; i < rows.length; i += CHUNK) {
      try { ins += await insertBatch(rows.slice(i, i + CHUNK)); } catch (_) {}
    }
    totalInserted += ins;

    if (data.features.length < 2000 || data.exceededTransferLimit === false) break;
    offset += 2000;
    await new Promise((r) => setTimeout(r, 150));
  }
  return totalInserted;
}

async function main() {
  const args = process.argv.slice(2);
  const statesArg = args.find((a) => a.startsWith('--states='));
  const onlyPublic = args.includes('--public-only');
  const onlyPrivate = args.includes('--private-only');

  const states = statesArg
    ? statesArg.split('=')[1].split(',').map((s) => s.trim().toUpperCase())
    : ALL_STATES;

  console.log('\nSchools importer (HIFLD ArcGIS)');
  console.log(`States: ${states.join(', ')}`);

  const startCount = await pool.query('SELECT COUNT(*) FROM environmental_sites');
  console.log(`DB start: ${Number(startCount.rows[0].count).toLocaleString()}`);

  let totalPublic = 0;
  let totalPrivate = 0;

  if (!onlyPrivate) {
    console.log('\n📚 Public Schools (HIFLD)...');
    for (const state of states) {
      process.stdout.write(`  ${state}... `);
      const ins = await importSchoolsForState(SERVICES.public, state, 'SCHOOLS PUBLIC', 'PUBLIC_SCHOOL', 'PUBSCH');
      console.log(`${ins.toLocaleString()}`);
      totalPublic += ins;
      await new Promise((r) => setTimeout(r, 100));
    }
    console.log(`  Total public: ${totalPublic.toLocaleString()}`);
  }

  if (!onlyPublic) {
    console.log('\n🏫 Private Schools (HIFLD)...');
    for (const state of states) {
      process.stdout.write(`  ${state}... `);
      const ins = await importSchoolsForState(SERVICES.private, state, 'SCHOOLS PRIVATE', 'PRIVATE_SCHOOL', 'PRVSCH');
      console.log(`${ins.toLocaleString()}`);
      totalPrivate += ins;
      await new Promise((r) => setTimeout(r, 100));
    }
    console.log(`  Total private: ${totalPrivate.toLocaleString()}`);
  }

  const endCount = await pool.query('SELECT COUNT(*) FROM environmental_sites');
  console.log(`\nSchools import complete`);
  console.log(`  Public: ${totalPublic.toLocaleString()}`);
  console.log(`  Private: ${totalPrivate.toLocaleString()}`);
  console.log(`  DB final: ${Number(endCount.rows[0].count).toLocaleString()}`);
  await pool.end();
}

main().catch((e) => { console.error(e.message); process.exit(1); });
