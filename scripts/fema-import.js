#!/usr/bin/env node
// =============================================================================
// FEMA NFIP Claims & Policies Importer
// Uses async-iterator streaming with proper backpressure (no pool exhaustion)
// =============================================================================
// Usage:
//   node scripts/fema-import.js claims    → imports downloads/mega/fema_nfip_flood_claims.csv
//   node scripts/fema-import.js policies  → imports downloads/mega/fema_nfip_policies.csv
// =============================================================================

require('dotenv').config({ path: require('path').join(__dirname, '../.env') });

const fs   = require('fs');
const path = require('path');
const { parse } = require('csv-parse');
const { Pool } = require('pg');

const pool = new Pool({
  host:     process.env.PG_HOST,
  port:     Number(process.env.PG_PORT),
  database: process.env.PG_DATABASE,
  user:     process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  max: 5,
});

const BATCH_SIZE = 2000;
const trim    = (v) => (v || '').toString().trim();
const toFloat = (v) => { const f = parseFloat(v); return Number.isFinite(f) ? f : null; };

const TYPE = (process.argv[2] || 'claims').toLowerCase();

const SOURCES = {
  claims: {
    file: path.join(__dirname, '../downloads/mega/fema_nfip_flood_claims.csv'),
    db:   'FEMA NFIP Flood Claims',
    mapper(row) {
      const lat = toFloat(row['latitude']);
      const lng = toFloat(row['longitude']);
      const id  = trim(row['id']);
      return {
        database_name:  'FEMA NFIP Flood Claims',
        category:       'hydrology',
        class_code:     'FEMA_FLOOD_CLAIM',
        priority_tier:  'high',
        priority_score: 85,
        site_name:      `Flood Claim ${row['occupancyType'] || ''}`.trim(),
        address:        '',
        city:           trim(row['countyCode'] || row['reportedCity'] || ''),
        state:          trim(row['state'] || ''),
        zip:            trim(row['reportedZipCode'] || ''),
        status:         'Closed',
        registry_id:    null,
        source_id:      id ? `FEMA-CLAIM-${id}` : null,
        source_org:     'FEMA',
        lat, lng,
        attributes: {
          date_of_loss:   row['dateOfLoss'],
          amount_paid:    row['amountPaidOnBuildingClaim'],
          flood_zone:     row['floodZoneCurrent'],
          occupancy:      row['occupancyType'],
          year_of_loss:   row['yearOfLoss'],
        },
      };
    },
  },
  policies: {
    file: path.join(__dirname, '../downloads/mega/fema_nfip_policies.csv'),
    db:   'FEMA NFIP Policies',
    mapper(row) {
      const lat = toFloat(row['latitude']);
      const lng = toFloat(row['longitude']);
      const id  = trim(row['id'] || row['policyID'] || row['policyId']);
      return {
        database_name:  'FEMA NFIP Policies',
        category:       'hydrology',
        class_code:     'FEMA_FLOOD_POLICY',
        priority_tier:  'medium',
        priority_score: 60,
        site_name:      `NFIP Policy ${row['occupancyType'] || ''}`.trim(),
        address:        '',
        city:           '',
        state:          trim(row['propertyState'] || row['state'] || ''),
        zip:            trim(row['reportedZipCode'] || ''),
        status:         row['policyTerminationDate'] ? 'Expired' : 'Active',
        registry_id:    null,
        source_id:      id ? `FEMA-POLICY-${id}` : null,
        source_org:     'FEMA',
        lat, lng,
        attributes: {
          coverage_amount:   row['totalInsurancePremiumOfThePolicy'],
          occupancy:         row['occupancyType'],
          flood_zone:        row['floodZone'],
          termination_date:  row['policyTerminationDate'],
        },
      };
    },
  },
};

async function batchInsert(rows) {
  if (!rows.length) return 0;
  const vals = [], args = [];
  let idx = 1, inserted = 0;

  for (const r of rows) {
    if (r.lat && r.lng) {
      vals.push(`($${idx},$${idx+1},$${idx+2},$${idx+3},$${idx+4},$${idx+5},$${idx+6},$${idx+7},$${idx+8},$${idx+9},$${idx+10},$${idx+11},$${idx+12},$${idx+13},ST_SetSRID(ST_MakePoint($${idx+14},$${idx+15}),4326),$${idx+16})`);
      args.push(r.database_name, r.category, r.class_code, r.priority_tier, r.priority_score,
                r.site_name, r.address, r.city, r.state, r.zip, r.status,
                r.registry_id, r.source_id, r.source_org, r.lng, r.lat,
                JSON.stringify(r.attributes));
      idx += 17;
    } else {
      vals.push(`($${idx},$${idx+1},$${idx+2},$${idx+3},$${idx+4},$${idx+5},$${idx+6},$${idx+7},$${idx+8},$${idx+9},$${idx+10},$${idx+11},$${idx+12},$${idx+13},NULL,$${idx+14})`);
      args.push(r.database_name, r.category, r.class_code, r.priority_tier, r.priority_score,
                r.site_name, r.address, r.city, r.state, r.zip, r.status,
                r.registry_id, r.source_id, r.source_org,
                JSON.stringify(r.attributes));
      idx += 15;
    }
  }

  try {
    const res = await pool.query(
      `INSERT INTO environmental_sites
         (database_name,category,class_code,priority_tier,priority_score,
          site_name,address,city,state,zip,status,registry_id,source_id,source_org,location,attributes)
       VALUES ${vals.join(',')}
       ON CONFLICT (source_id) DO NOTHING`,
      args
    );
    inserted = res.rowCount || 0;
  } catch (e) {
    // Row-by-row fallback
    for (const r of rows) {
      if (!r.source_id) continue;
      try {
        const locExpr = (r.lat && r.lng)
          ? 'ST_SetSRID(ST_MakePoint($15,$16),4326)'
          : 'NULL';
        const sArgs = (r.lat && r.lng)
          ? [r.database_name, r.category, r.class_code, r.priority_tier, r.priority_score,
             r.site_name, r.address, r.city, r.state, r.zip, r.status,
             r.registry_id, r.source_id, r.source_org, r.lng, r.lat,
             JSON.stringify(r.attributes)]
          : [r.database_name, r.category, r.class_code, r.priority_tier, r.priority_score,
             r.site_name, r.address, r.city, r.state, r.zip, r.status,
             r.registry_id, r.source_id, r.source_org,
             JSON.stringify(r.attributes)];
        const nParams = (r.lat && r.lng) ? 17 : 15;
        await pool.query(
          `INSERT INTO environmental_sites
             (database_name,category,class_code,priority_tier,priority_score,
              site_name,address,city,state,zip,status,registry_id,source_id,source_org,location,attributes)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,${locExpr},$${nParams})
           ON CONFLICT (source_id) DO NOTHING`,
          sArgs
        );
        inserted++;
      } catch (_) {}
    }
  }
  return inserted;
}

async function main() {
  const src = SOURCES[TYPE];
  if (!src) {
    console.error(`Unknown type "${TYPE}". Use: claims | policies`);
    process.exit(1);
  }
  if (!fs.existsSync(src.file)) {
    console.error(`File not found: ${src.file}`);
    process.exit(1);
  }

  const stat = fs.statSync(src.file);
  console.log(`\n📂  Importing ${src.db}`);
  console.log(`    File: ${src.file} (${(stat.size / 1024 / 1024).toFixed(1)} MB)`);
  console.log(`    Batch size: ${BATCH_SIZE}`);
  console.log(`    Started: ${new Date().toISOString()}\n`);

  const start = Date.now();
  let totalRows = 0, totalInserted = 0, skipped = 0;
  let batch = [];

  const parser = fs.createReadStream(src.file)
    .pipe(parse({ columns: true, skip_empty_lines: true, trim: true, bom: true }));

  for await (const row of parser) {
    const mapped = src.mapper(row);
    if (!mapped.source_id) { skipped++; continue; }
    batch.push(mapped);
    totalRows++;

    if (batch.length >= BATCH_SIZE) {
      const ins = await batchInsert(batch);
      totalInserted += ins;
      batch = [];
      if (totalRows % 50000 === 0) {
        const elapsed = ((Date.now() - start) / 1000).toFixed(0);
        process.stdout.write(`\r  ↑  ${totalRows.toLocaleString()} rows, ${totalInserted.toLocaleString()} inserted, ${skipped} skipped  [${elapsed}s]`);
      }
    }
  }

  if (batch.length) {
    totalInserted += await batchInsert(batch);
  }

  const elapsed = ((Date.now() - start) / 1000).toFixed(1);
  console.log(`\n\n✅  Done!`);
  console.log(`    Rows processed:  ${totalRows.toLocaleString()}`);
  console.log(`    Rows inserted:   ${totalInserted.toLocaleString()}`);
  console.log(`    Skipped (no ID): ${skipped}`);
  console.log(`    Time elapsed:    ${elapsed}s`);

  // Show final DB count for this dataset
  const r = await pool.query(
    `SELECT COUNT(*)::bigint n FROM environmental_sites WHERE database_name = $1`,
    [src.db]
  );
  console.log(`    DB total for "${src.db}": ${r.rows[0].n}`);

  await pool.end();
}

main().catch(e => { console.error(e.message); process.exit(1); });
