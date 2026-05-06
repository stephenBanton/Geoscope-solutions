#!/usr/bin/env node
'use strict';

require('dotenv').config();
const { Pool } = require('pg');

function env(name, fallback = '') {
  return String(process.env[name] || fallback).trim();
}

function intEnv(name, fallback) {
  const n = Number.parseInt(env(name, ''), 10);
  return Number.isFinite(n) ? n : fallback;
}

function buildSourceConfig() {
  const connectionString = env('SOURCE_DATABASE_URL') || env('SUPABASE_DATA_DATABASE_URL');
  if (connectionString) {
    return {
      connectionString,
      ssl: { rejectUnauthorized: false },
      max: 4,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 10000,
    };
  }

  throw new Error('Missing source DB config. Set SOURCE_DATABASE_URL (or SUPABASE_DATA_DATABASE_URL).');
}

function buildTargetConfig() {
  const connectionString = env('DATA_DATABASE_URL') || env('TARGET_DATABASE_URL');
  if (connectionString) {
    const isLocalHost = /localhost|127\.0\.0\.1/i.test(connectionString);
    return {
      connectionString,
      ssl: isLocalHost ? false : { rejectUnauthorized: false },
      max: 6,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 10000,
    };
  }

  return {
    host: env('DATA_PG_HOST') || env('PG_HOST') || 'localhost',
    port: intEnv('DATA_PG_PORT', intEnv('PG_PORT', 5432)),
    database: env('DATA_PG_DATABASE') || env('PG_DATABASE') || 'geoscope',
    user: env('DATA_PG_USER') || env('PG_USER') || 'postgres',
    password: env('DATA_PG_PASSWORD') || env('PG_PASSWORD'),
    max: 6,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 10000,
  };
}

async function ensureTargetSchema(client) {
  await client.query('CREATE EXTENSION IF NOT EXISTS postgis');
  await client.query(`
    CREATE TABLE IF NOT EXISTS environmental_sites (
      id BIGSERIAL PRIMARY KEY,
      location geometry(POINT, 4326) NOT NULL,
      database_name VARCHAR(200) NOT NULL,
      category VARCHAR(50) NOT NULL,
      class_code VARCHAR(80),
      class_description VARCHAR(255),
      priority_tier VARCHAR(20) DEFAULT 'standard',
      priority_score INTEGER DEFAULT 0,
      source_org VARCHAR(100),
      site_name VARCHAR(500),
      address TEXT,
      city VARCHAR(200),
      state CHAR(2),
      zip VARCHAR(20),
      status VARCHAR(100),
      registry_id VARCHAR(100),
      source_id VARCHAR(100),
      attributes JSONB,
      imported_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await client.query('CREATE INDEX IF NOT EXISTS env_sites_location_gist ON environmental_sites USING GIST (location)');
  await client.query('CREATE INDEX IF NOT EXISTS env_sites_database_idx ON environmental_sites (database_name)');
  await client.query('CREATE INDEX IF NOT EXISTS env_sites_category_idx ON environmental_sites (category)');
  await client.query('CREATE UNIQUE INDEX IF NOT EXISTS env_sites_source_id_uidx ON environmental_sites (source_id) WHERE source_id IS NOT NULL');
}

async function main() {
  const sourcePool = new Pool(buildSourceConfig());
  const targetPool = new Pool(buildTargetConfig());

  const batchSize = intEnv('MIGRATION_BATCH_SIZE', 5000);
  const maxRows = intEnv('MIGRATION_MAX_ROWS', 0);

  let copied = 0;
  let lastId = 0;
  const started = Date.now();

  try {
    const sourceClient = await sourcePool.connect();
    const targetClient = await targetPool.connect();

    try {
      await ensureTargetSchema(targetClient);

      const totalQ = await sourceClient.query('SELECT COUNT(*)::bigint AS total FROM environmental_sites');
      const sourceTotal = Number(totalQ.rows[0]?.total || 0);
      console.log(`[migrate] source rows: ${sourceTotal}`);
      console.log(`[migrate] batch size: ${batchSize}`);
      if (maxRows > 0) {
        console.log(`[migrate] max rows cap: ${maxRows}`);
      }

      while (true) {
        if (maxRows > 0 && copied >= maxRows) break;

        const rowsQ = await sourceClient.query(
          `
          SELECT
            id,
            database_name,
            category,
            class_code,
            class_description,
            priority_tier,
            priority_score,
            source_org,
            site_name,
            address,
            city,
            state,
            zip,
            status,
            registry_id,
            source_id,
            attributes,
            ST_Y(location::geometry) AS lat,
            ST_X(location::geometry) AS lng,
            imported_at,
            updated_at
          FROM environmental_sites
          WHERE id > $1
          ORDER BY id ASC
          LIMIT $2
          `,
          [lastId, batchSize]
        );

        const rows = rowsQ.rows || [];
        if (rows.length === 0) break;

        await targetClient.query('BEGIN');
        try {
          for (const r of rows) {
            if (!Number.isFinite(Number(r.lat)) || !Number.isFinite(Number(r.lng))) continue;

            const resolvedSourceId = String(r.source_id || '').trim() || `supabase-${r.id}`;

            await targetClient.query(
              `
              INSERT INTO environmental_sites (
                location, database_name, category, class_code, class_description,
                priority_tier, priority_score, source_org, site_name, address,
                city, state, zip, status, registry_id, source_id, attributes,
                imported_at, updated_at
              )
              VALUES (
                ST_SetSRID(ST_MakePoint($1, $2), 4326),
                $3, $4, $5, $6,
                $7, $8, $9, $10, $11,
                $12, $13, $14, $15, $16, $17, $18,
                COALESCE($19, NOW()), COALESCE($20, NOW())
              )
              ON CONFLICT (source_id) DO UPDATE SET
                location = EXCLUDED.location,
                database_name = EXCLUDED.database_name,
                category = EXCLUDED.category,
                class_code = EXCLUDED.class_code,
                class_description = EXCLUDED.class_description,
                priority_tier = EXCLUDED.priority_tier,
                priority_score = EXCLUDED.priority_score,
                source_org = EXCLUDED.source_org,
                site_name = EXCLUDED.site_name,
                address = EXCLUDED.address,
                city = EXCLUDED.city,
                state = EXCLUDED.state,
                zip = EXCLUDED.zip,
                status = EXCLUDED.status,
                registry_id = EXCLUDED.registry_id,
                attributes = EXCLUDED.attributes,
                updated_at = EXCLUDED.updated_at
              `,
              [
                Number(r.lng),
                Number(r.lat),
                String(r.database_name || 'UNKNOWN').trim(),
                String(r.category || 'regulatory').trim(),
                r.class_code || null,
                r.class_description || null,
                String(r.priority_tier || 'standard').trim(),
                Number.isFinite(Number(r.priority_score)) ? Number(r.priority_score) : 0,
                r.source_org || 'Supabase',
                r.site_name || null,
                r.address || null,
                r.city || null,
                r.state || null,
                r.zip || null,
                r.status || null,
                r.registry_id || null,
                resolvedSourceId,
                r.attributes || null,
                r.imported_at || null,
                r.updated_at || null,
              ]
            );
          }

          await targetClient.query('COMMIT');
        } catch (err) {
          await targetClient.query('ROLLBACK');
          throw err;
        }

        copied += rows.length;
        lastId = Number(rows[rows.length - 1].id);

        const elapsedSec = Math.max(1, Math.floor((Date.now() - started) / 1000));
        const rate = Math.floor(copied / elapsedSec);
        console.log(`[migrate] copied=${copied} lastId=${lastId} rate=${rate}/s`);
      }

      const verifyQ = await targetClient.query('SELECT COUNT(*)::bigint AS total FROM environmental_sites');
      const targetTotal = Number(verifyQ.rows[0]?.total || 0);
      console.log(`[migrate] target rows now: ${targetTotal}`);
      console.log('[migrate] done');
    } finally {
      sourceClient.release();
      targetClient.release();
    }
  } finally {
    await sourcePool.end();
    await targetPool.end();
  }
}

main().catch((err) => {
  console.error('[migrate] failed:', err.message);
  process.exit(1);
});
