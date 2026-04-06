#!/usr/bin/env node
// =============================================================================
// GeoScope Batch Geocoder
// =============================================================================
// Finds all environmental_sites rows where location IS NULL (address-only
// rows imported without coordinates) and geocodes them using the free
// Nominatim OSM API (no API key needed).
//
// Usage:
//   node scripts/geocode-pending.js              -- run all pending
//   node scripts/geocode-pending.js --limit 500  -- batch of 500
//   node scripts/geocode-pending.js --dry-run    -- show count only
//
// Rate limit: Nominatim allows 1 req/sec. This script respects that.
// For large batches, run overnight or in multiple sessions.
// =============================================================================

require('dotenv').config({ path: require('path').join(__dirname, '../.env') });

const https  = require('https');
const { pool } = require('../db');

const LIMIT   = parseInt(process.argv.find(a => a.startsWith('--limit='))?.split('=')[1] || '5000', 10);
const DRY_RUN = process.argv.includes('--dry-run');
const DELAY_MS = 1100; // Nominatim: max 1 req/sec

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function nominatimGeocode(address, city, state, zip) {
  const q = [address, city, state, zip, 'USA'].filter(Boolean).join(', ');
  const url = `https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(q)}&format=json&limit=1`;

  return new Promise((resolve) => {
    const req = https.get(url, {
      headers: { 'User-Agent': 'GeoScope/1.0 (environmental screening platform)' }
    }, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const results = JSON.parse(data);
          if (results.length > 0) {
            resolve({ lat: parseFloat(results[0].lat), lon: parseFloat(results[0].lon) });
          } else {
            resolve(null);
          }
        } catch { resolve(null); }
      });
    });
    req.on('error', () => resolve(null));
    req.setTimeout(8000, () => { req.destroy(); resolve(null); });
  });
}

async function main() {
  const client = await pool.connect();

  try {
    const countRes = await client.query(
      `SELECT COUNT(*) FROM environmental_sites WHERE location IS NULL`
    );
    const total = parseInt(countRes.rows[0].count, 10);
    console.log(`\nGeocoding queue: ${total} sites have no coordinates`);

    if (DRY_RUN) {
      console.log('Dry run — exiting without geocoding.');
      return;
    }

    if (total === 0) {
      console.log('Nothing to geocode!');
      return;
    }

    const rows = await client.query(
      `SELECT id, site_name, address, city, state, zip
       FROM environmental_sites
       WHERE location IS NULL
         AND geocode_attempted = FALSE
       ORDER BY id
       LIMIT $1`,
      [LIMIT]
    );

    console.log(`Processing ${rows.rows.length} rows (limit: ${LIMIT})...\n`);

    let success = 0, failed = 0;

    for (const row of rows.rows) {
      await sleep(DELAY_MS);

      const coords = await nominatimGeocode(row.address, row.city, row.state, row.zip);

      if (coords) {
        await client.query(
          `UPDATE environmental_sites
           SET location = ST_SetSRID(ST_MakePoint($1, $2), 4326),
               geocoded = TRUE,
               geocode_attempted = TRUE,
               updated_at = NOW()
           WHERE id = $3`,
          [coords.lon, coords.lat, row.id]
        );
        success++;
        if (success % 50 === 0) console.log(`  Geocoded: ${success} | Failed: ${failed}`);
      } else {
        await client.query(
          `UPDATE environmental_sites
           SET geocode_attempted = TRUE, updated_at = NOW()
           WHERE id = $1`,
          [row.id]
        );
        failed++;
      }
    }

    console.log(`\nDone!`);
    console.log(`  Geocoded successfully : ${success}`);
    console.log(`  Could not geocode     : ${failed}`);

    const remaining = await client.query(
      `SELECT COUNT(*) FROM environmental_sites WHERE location IS NULL AND geocode_attempted = FALSE`
    );
    console.log(`  Still pending         : ${remaining.rows[0].count}\n`);

  } finally {
    client.release();
    await pool.end();
  }
}

main().catch(err => {
  console.error('Fatal:', err.message);
  process.exit(1);
});
