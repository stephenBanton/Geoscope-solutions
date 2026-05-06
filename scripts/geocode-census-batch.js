#!/usr/bin/env node
// =============================================================================
// GeoScope – US Census Bureau Batch Geocoder
// =============================================================================
// Uses the FREE US Census Geocoding API (no API key required).
//   https://geocoding.geo.census.gov/geocoder/locations/addressbatch
//
// Speed   : ~10,000 rows per 30–90 seconds  ≈ 100K–200K rows / hour
// Queue   : all environmental_sites WHERE location IS NULL
//            AND geocode_attempted = FALSE
//
// Usage:
//   node scripts/geocode-census-batch.js              -- run until queue empty
//   node scripts/geocode-census-batch.js --limit 500  -- stop after N rows total
//   node scripts/geocode-census-batch.js --dry-run    -- show count only
//   node scripts/geocode-census-batch.js --batch 5000 -- rows per API call (max 9500)
//
// Graceful shutdown: press Ctrl+C — the current batch completes before exit.
// Resume anytime: already-attempted rows are skipped automatically.
// =============================================================================

'use strict';

require('dotenv').config({ path: require('path').join(__dirname, '../.env') });

const https  = require('https');
const http   = require('http');
const { pool } = require('../db');

// ---------------------------------------------------------------------------
// CLI flags
// ---------------------------------------------------------------------------
const args     = process.argv.slice(2);
const DRY_RUN  = args.includes('--dry-run');
const LIMIT    = parseInt(args.find(a => a.startsWith('--limit='))?.split('=')[1]  || '0', 10);   // 0 = unlimited
const BATCH_SZ = Math.min(
  parseInt(args.find(a => a.startsWith('--batch='))?.split('=')[1] || '9500', 10),
  9500  // Census hard limit is 10,000; stay just under
);

const CENSUS_URL = 'https://geocoding.geo.census.gov/geocoder/locations/addressbatch';
const BENCHMARK  = 'Public_AR_Current';
const TIMEOUT_MS = 180_000;   // 3 min per batch (large batches can be slow)

// ---------------------------------------------------------------------------
// Graceful shutdown
// ---------------------------------------------------------------------------
let shuttingDown = false;
process.on('SIGINT', () => {
  if (!shuttingDown) {
    shuttingDown = true;
    console.log('\n\n⚠️  Ctrl+C detected — finishing current batch then exiting...');
  }
});

// ---------------------------------------------------------------------------
// CSV helpers
// ---------------------------------------------------------------------------

/** Escape a value for inclusion in a plain CSV row (no quotes unless needed). */
function csvEscape(val) {
  if (val === null || val === undefined) return '';
  const s = String(val).replace(/\r?\n/g, ' ').trim();
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

/**
 * Minimal CSV line parser that handles quoted fields.
 * Handles the Census response format: some fields may be double-quoted.
 */
function parseCSVRow(line) {
  const fields = [];
  let cur = '';
  let inQuote = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuote) {
      if (ch === '"') {
        if (line[i + 1] === '"') { cur += '"'; i++; }   // escaped quote
        else inQuote = false;
      } else {
        cur += ch;
      }
    } else {
      if (ch === '"') {
        inQuote = true;
      } else if (ch === ',') {
        fields.push(cur);
        cur = '';
      } else {
        cur += ch;
      }
    }
  }
  fields.push(cur);
  return fields;
}

// ---------------------------------------------------------------------------
// Census API call (native https — no extra dependencies)
// ---------------------------------------------------------------------------

/**
 * Sends up to 9,500 rows to the Census Batch Geocoder.
 * Returns a Map<id, {lon, lat}> for successfully matched records.
 */
function censusBatchGeocode(rows) {
  return new Promise((resolve, reject) => {
    // Build the input CSV (no header row)
    // Format: Unique_ID,Street,City,State,ZIP
    const csvLines = rows.map(r =>
      [
        r.id,
        csvEscape(r.address),
        csvEscape(r.city),
        csvEscape(r.state),
        csvEscape(r.zip),
      ].join(',')
    );
    const csvBody = csvLines.join('\n');

    // Build multipart/form-data manually
    const boundary = '----CensusBatchBoundary' + Date.now();
    const CRLF = '\r\n';

    const filepart =
      `--${boundary}${CRLF}` +
      `Content-Disposition: form-data; name="addressFile"; filename="addresses.csv"${CRLF}` +
      `Content-Type: text/plain${CRLF}${CRLF}` +
      csvBody + CRLF;

    const benchmarkPart =
      `--${boundary}${CRLF}` +
      `Content-Disposition: form-data; name="benchmark"${CRLF}${CRLF}` +
      `${BENCHMARK}${CRLF}`;

    const closing = `--${boundary}--${CRLF}`;
    const body = filepart + benchmarkPart + closing;
    const bodyBuf = Buffer.from(body, 'utf8');

    const options = {
      hostname: 'geocoding.geo.census.gov',
      path: '/geocoder/locations/addressbatch',
      method: 'POST',
      headers: {
        'Content-Type': `multipart/form-data; boundary=${boundary}`,
        'Content-Length': bodyBuf.length,
        'User-Agent': 'GeoScope/2.0 (environmental screening platform)',
      },
      timeout: TIMEOUT_MS,
    };

    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => { data += chunk; });
      res.on('end', () => {
        try {
          const matched = new Map();

          for (const line of data.split('\n')) {
            if (!line.trim()) continue;
            const parts = parseCSVRow(line);
            // parts[0]=ID  parts[2]=Match/No_Match/Tie  parts[5]=coordinates "lon,lat"
            if (parts.length < 6) continue;
            const id = parseInt(parts[0], 10);
            if (isNaN(id)) continue;
            const matchResult = (parts[2] || '').trim();
            const coordStr   = (parts[5] || '').trim();

            if (matchResult === 'Match' && coordStr) {
              const commaIdx = coordStr.indexOf(',');
              if (commaIdx !== -1) {
                const lon = parseFloat(coordStr.substring(0, commaIdx));
                const lat = parseFloat(coordStr.substring(commaIdx + 1));
                if (!isNaN(lon) && !isNaN(lat) &&
                    lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180) {
                  matched.set(id, { lon, lat });
                }
              }
            }
          }
          resolve(matched);
        } catch (err) {
          reject(new Error('Census response parse error: ' + err.message));
        }
      });
    });

    req.on('timeout', () => {
      req.destroy();
      reject(new Error(`Census API timed out after ${TIMEOUT_MS / 1000}s`));
    });
    req.on('error', reject);
    req.write(bodyBuf);
    req.end();
  });
}

// ---------------------------------------------------------------------------
// Database helpers
// ---------------------------------------------------------------------------

/** Fetch next batch of un-geocoded rows. */
async function fetchPending(client, batchSize) {
  const res = await client.query(
    `SELECT id, address, city, state, zip
     FROM environmental_sites
     WHERE location IS NULL
       AND geocode_attempted = FALSE
     ORDER BY id
     LIMIT $1`,
    [batchSize]
  );
  return res.rows;
}

/** Bulk-update matched records and mark all as attempted. */
async function applyResults(client, rows, matched) {
  const allIds = rows.map(r => BigInt(r.id));

  // Update rows that got coordinates
  if (matched.size > 0) {
    const mIds   = [];
    const mLons  = [];
    const mLats  = [];
    for (const [id, { lon, lat }] of matched) {
      mIds.push(BigInt(id));
      mLons.push(lon);
      mLats.push(lat);
    }

    await client.query(
      `UPDATE environmental_sites AS e
       SET location        = ST_SetSRID(ST_MakePoint(v.lon, v.lat), 4326),
           geocoded         = TRUE,
           geocode_attempted = TRUE,
           updated_at       = NOW()
       FROM UNNEST($1::bigint[], $2::float8[], $3::float8[]) AS v(id, lon, lat)
       WHERE e.id = v.id`,
      [mIds, mLons, mLats]
    );
  }

  // Mark the rest as attempted (no match)
  await client.query(
    `UPDATE environmental_sites
     SET geocode_attempted = TRUE, updated_at = NOW()
     WHERE id = ANY($1::bigint[])
       AND geocode_attempted = FALSE`,
    [allIds]
  );
}

// ---------------------------------------------------------------------------
// Progress formatting
// ---------------------------------------------------------------------------
function fmtDuration(ms) {
  const s = Math.round(ms / 1000);
  if (s < 60)   return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`;
  return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
}

function fmtRate(count, ms) {
  const perSec = count / (ms / 1000);
  if (perSec >= 1000) return `${(perSec / 1000).toFixed(1)}K rows/sec`;
  return `${Math.round(perSec)} rows/sec`;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  const client = await pool.connect();

  try {
    // Count queue
    const countRes = await client.query(
      `SELECT COUNT(*) FROM environmental_sites WHERE location IS NULL`
    );
    const queueTotal = parseInt(countRes.rows[0].count, 10);

    const pendingRes = await client.query(
      `SELECT COUNT(*) FROM environmental_sites WHERE location IS NULL AND geocode_attempted = FALSE`
    );
    const queuePending = parseInt(pendingRes.rows[0].count, 10);

    console.log('\n============================================================');
    console.log('  GeoScope Census Batch Geocoder');
    console.log('============================================================');
    console.log(`  Sites without coordinates : ${queueTotal.toLocaleString()}`);
    console.log(`  Not yet attempted          : ${queuePending.toLocaleString()}`);
    console.log(`  Batch size                 : ${BATCH_SZ.toLocaleString()} rows/call`);
    console.log(`  Estimated batches          : ~${Math.ceil(queuePending / BATCH_SZ)}`);
    console.log(`  Census API                 : ${CENSUS_URL}`);
    console.log('============================================================\n');

    if (DRY_RUN) {
      console.log('Dry run — exiting without geocoding.\n');
      return;
    }

    if (queuePending === 0) {
      console.log('Nothing to geocode — queue is empty!\n');
      return;
    }

    let totalProcessed = 0;
    let totalMatched   = 0;
    let batchNum       = 0;
    const sessionStart = Date.now();

    // Enforce --limit
    const maxRows = LIMIT > 0 ? LIMIT : Infinity;

    while (!shuttingDown && totalProcessed < maxRows) {
      const thisBatch = Math.min(BATCH_SZ, maxRows - totalProcessed);
      const rows = await fetchPending(client, thisBatch);

      if (rows.length === 0) {
        console.log('\n✅  Queue exhausted — all rows have been attempted.');
        break;
      }

      batchNum++;
      const batchStart = Date.now();
      process.stdout.write(
        `Batch ${batchNum.toString().padStart(4)} | ${rows.length.toLocaleString()} rows | sending to Census...`
      );

      let matched;
      try {
        matched = await censusBatchGeocode(rows);
      } catch (err) {
        console.log(`\n  ⚠️  Census API error: ${err.message}`);
        console.log('  Marking batch as attempted and continuing...');
        matched = new Map();
      }

      await applyResults(client, rows, matched);

      const elapsed = Date.now() - batchStart;
      totalProcessed += rows.length;
      totalMatched   += matched.size;

      const matchPct = ((matched.size / rows.length) * 100).toFixed(1);
      const sessionElapsed = Date.now() - sessionStart;
      const overallRate = fmtRate(totalProcessed, sessionElapsed);

      console.log(
        ` matched ${matched.size.toLocaleString()}/${rows.length.toLocaleString()}` +
        ` (${matchPct}%) in ${fmtDuration(elapsed)} | total: ${totalProcessed.toLocaleString()} | ${overallRate}`
      );
    }

    // Final summary
    const sessionElapsed = Date.now() - sessionStart;
    console.log('\n============================================================');
    console.log('  Session complete');
    console.log('============================================================');
    console.log(`  Rows processed  : ${totalProcessed.toLocaleString()}`);
    console.log(`  Geocoded        : ${totalMatched.toLocaleString()}`);
    console.log(`  No match        : ${(totalProcessed - totalMatched).toLocaleString()}`);
    console.log(`  Match rate      : ${((totalMatched / totalProcessed) * 100).toFixed(1)}%`);
    console.log(`  Time elapsed    : ${fmtDuration(sessionElapsed)}`);

    // Remaining queue
    const remainRes = await client.query(
      `SELECT COUNT(*) FROM environmental_sites WHERE location IS NULL AND geocode_attempted = FALSE`
    );
    console.log(`  Still pending   : ${parseInt(remainRes.rows[0].count, 10).toLocaleString()}`);
    console.log('============================================================\n');

  } finally {
    client.release();
    await pool.end();
  }
}

main().catch(err => {
  console.error('\nFatal:', err.message);
  process.exit(1);
});
