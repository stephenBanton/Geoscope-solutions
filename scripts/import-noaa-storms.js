#!/usr/bin/env node
require('dotenv').config({ path: require('path').join(__dirname, '../.env') });

const fs = require('fs');
const path = require('path');
const https = require('https');
const { createGunzip } = require('zlib');
const { parse } = require('csv-parse');
const { Pool } = require('pg');

const BASE_URL = 'https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/';
const OUT_DIR = path.join(__dirname, '../downloads/extra/noaa_storms');
if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

const pool = new Pool({
  host: process.env.PG_HOST || 'localhost',
  port: parseInt(process.env.PG_PORT || '5432', 10),
  database: process.env.PG_DATABASE || 'geoscope',
  user: process.env.PG_USER || 'postgres',
  password: process.env.PG_PASSWORD,
  max: 5,
});

function get(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { 'User-Agent': 'GeoScope/1.0' } }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return resolve(get(res.headers.location));
      }
      if (res.statusCode !== 200) {
        res.resume();
        return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
      }
      resolve(res);
    }).on('error', reject);
  });
}

async function fetchText(url) {
  const res = await get(url);
  return new Promise((resolve, reject) => {
    let d = '';
    res.on('data', (c) => (d += c));
    res.on('end', () => resolve(d));
    res.on('error', reject);
  });
}

async function downloadFile(url, destPath) {
  if (fs.existsSync(destPath) && fs.statSync(destPath).size > 1024) {
    console.log(`  [cache] ${path.basename(destPath)}`);
    return destPath;
  }
  const res = await get(url);
  const tmp = `${destPath}.tmp`;
  const ws = fs.createWriteStream(tmp);
  return new Promise((resolve, reject) => {
    let bytes = 0;
    res.on('data', (c) => {
      bytes += c.length;
      if (bytes % (10 * 1024 * 1024) < c.length) {
        process.stdout.write(`\r  Downloading ${path.basename(destPath)} ${(bytes / 1024 / 1024).toFixed(1)} MB`);
      }
    });
    res.pipe(ws);
    ws.on('finish', () => {
      ws.close(() => {
        fs.renameSync(tmp, destPath);
        process.stdout.write(`\r  Downloaded ${path.basename(destPath)} ${(bytes / 1024 / 1024).toFixed(1)} MB\n`);
        resolve(destPath);
      });
    });
    ws.on('error', (e) => {
      fs.unlink(tmp, () => {});
      reject(e);
    });
  });
}

function chooseLatestFile(indexHtml, year) {
  const re = new RegExp(`StormEvents_locations-ftp_v1\\.0_d${year}_c(\\d{8})\\.csv\\.gz`, 'g');
  let m;
  let best = null;
  while ((m = re.exec(indexHtml)) !== null) {
    const fn = m[0];
    if (!best || fn > best) best = fn;
  }
  return best;
}

function normalizeRow(r, year) {
  const lat = parseFloat(r.LATITUDE || r.BEGIN_LAT || r.BEGIN_Y || r.END_LAT || r.END_Y || '');
  const lon = parseFloat(r.LONGITUDE || r.BEGIN_LON || r.BEGIN_X || r.END_LON || r.END_X || '');
  if (!Number.isFinite(lat) || !Number.isFinite(lon) || lat === 0 || lon === 0) return null;

  const eventId = r.EVENT_ID || r.EVENTID || r.EVENT_ID_NUMBER || '';
  const episodeId = r.EPISODE_ID || r.EPISODEID || '';
  const locId = r.LOCATION_INDEX || r.LOCATION || r.LOCATION_ID || '';
  const sidPart = `${year}-${episodeId}-${eventId}-${locId}`.replace(/[^0-9A-Za-z\-]/g, '');

  return {
    source_id: `NOAA-STORM-${sidPart}`,
    site_name: r.LOCATION || r.CZ_NAME || r.COUNTY || `NOAA Storm Event ${eventId || ''}`.trim(),
    address: null,
    city: r.CZ_NAME || r.COUNTY || null,
    state: (r.STATE_ABBR || r.STATE || '').toString().slice(0, 2).toUpperCase() || null,
    zip: null,
    database_name: 'NOAA Storm Events',
    category: 'climate',
    class_code: 'NOAA_STORM',
    lat,
    lon,
  };
}

async function batchInsert(rows) {
  if (!rows.length) return 0;
  const CHUNK = 500;
  let inserted = 0;
  for (let i = 0; i < rows.length; i += CHUNK) {
    const slice = rows.slice(i, i + CHUNK);
    const vals = [];
    const params = [];
    let p = 1;
    for (const r of slice) {
      vals.push(`($${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},$${p++},ST_SetSRID(ST_MakePoint($${p++},$${p++}),4326))`);
      params.push(
        r.source_id,
        r.site_name,
        r.address,
        r.city,
        r.state,
        r.zip,
        r.database_name,
        r.category,
        r.class_code,
        r.lon,
        r.lat
      );
    }

    const sql = `INSERT INTO environmental_sites (source_id,site_name,address,city,state,zip,database_name,category,class_code,location)
                 VALUES ${vals.join(',')}
                 ON CONFLICT (source_id) DO NOTHING`;
    try {
      const res = await pool.query(sql, params);
      inserted += res.rowCount || 0;
    } catch (_) {
      for (const r of slice) {
        try {
          const res2 = await pool.query(
            `INSERT INTO environmental_sites (source_id,site_name,address,city,state,zip,database_name,category,class_code,location)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,ST_SetSRID(ST_MakePoint($10,$11),4326))
             ON CONFLICT (source_id) DO NOTHING`,
            [r.source_id, r.site_name, r.address, r.city, r.state, r.zip, r.database_name, r.category, r.class_code, r.lon, r.lat]
          );
          inserted += res2.rowCount || 0;
        } catch (_) {}
      }
    }
  }
  return inserted;
}

async function importGzCsv(filePath, year) {
  const rs = fs.createReadStream(filePath);
  const gunzip = createGunzip();
  const parser = parse({ columns: true, bom: true, relax_column_count: true, skip_empty_lines: true, trim: true });

  let seen = 0;
  let inserted = 0;
  let buf = [];
  const FLUSH_SIZE = 2000;

  async function flush() {
    if (!buf.length) return;
    const chunk = buf;
    buf = [];
    const ins = await batchInsert(chunk);
    inserted += ins;
    process.stdout.write(`\r  ${year}: parsed=${seen.toLocaleString()} inserted=${inserted.toLocaleString()}`);
  }

  rs.pipe(gunzip).pipe(parser);

  for await (const row of parser) {
    seen += 1;
    const n = normalizeRow(row, year);
    if (n) buf.push(n);
    if (buf.length >= FLUSH_SIZE) {
      await flush();
    }
  }

  await flush();
  process.stdout.write('\n');
  return { seen, inserted };
}

async function main() {
  const args = process.argv.slice(2);
  let years = ['2024', '2023', '2022'];
  const yArg = args.find((a) => a.startsWith('--years='));
  if (yArg) years = yArg.split('=')[1].split(',').map((s) => s.trim());

  console.log('\nNOAA Storm Events Import');
  console.log(`  Years: ${years.join(', ')}`);

  const start = await pool.query('SELECT COUNT(*) FROM environmental_sites');
  console.log(`  DB start: ${Number(start.rows[0].count).toLocaleString()}`);

  const indexHtml = await fetchText(BASE_URL);
  let totalInserted = 0;

  for (const year of years) {
    const fn = chooseLatestFile(indexHtml, year);
    if (!fn) {
      console.log(`  ${year}: no locations file found`);
      continue;
    }

    const url = `${BASE_URL}${fn}`;
    const dest = path.join(OUT_DIR, fn);
    console.log(`\n  ${year}: ${fn}`);

    try {
      await downloadFile(url, dest);
      const stats = await importGzCsv(dest, year);
      totalInserted += stats.inserted;
      console.log(`  ${year}: done parsed=${stats.seen.toLocaleString()} inserted=${stats.inserted.toLocaleString()}`);
    } catch (e) {
      console.log(`  ${year}: error ${e.message}`);
    }
  }

  const end = await pool.query('SELECT COUNT(*) FROM environmental_sites');
  console.log('\nNOAA complete');
  console.log(`  Total inserted: ${totalInserted.toLocaleString()}`);
  console.log(`  DB final: ${Number(end.rows[0].count).toLocaleString()}`);

  await pool.end();
}

main().catch(async (e) => {
  console.error(e);
  try { await pool.end(); } catch (_) {}
  process.exit(1);
});
