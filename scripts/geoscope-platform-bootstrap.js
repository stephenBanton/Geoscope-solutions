#!/usr/bin/env node
'use strict';

require('dotenv').config();
const { dataPool, pingDataDB } = require('../db');

const PLATFORM_NAME = process.env.GEOSCOPE_PLATFORM_NAME || 'GeoScope Data Platform';
const PLATFORM_OWNER = process.env.GEOSCOPE_PLATFORM_OWNER || 'GeoScope Solutions';

async function ensurePlatformTables() {
  await dataPool.query('CREATE EXTENSION IF NOT EXISTS postgis');

  await dataPool.query(`
    CREATE TABLE IF NOT EXISTS ingestion_jobs (
      id BIGSERIAL PRIMARY KEY,
      source_name VARCHAR(255) NOT NULL,
      source_path TEXT,
      status VARCHAR(50) NOT NULL DEFAULT 'queued',
      rows_seen BIGINT NOT NULL DEFAULT 0,
      rows_inserted BIGINT NOT NULL DEFAULT 0,
      rows_updated BIGINT NOT NULL DEFAULT 0,
      rows_failed BIGINT NOT NULL DEFAULT 0,
      error_message TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      started_at TIMESTAMPTZ,
      finished_at TIMESTAMPTZ
    )
  `);

  await dataPool.query('CREATE INDEX IF NOT EXISTS ingestion_jobs_status_idx ON ingestion_jobs (status)');
  await dataPool.query('CREATE INDEX IF NOT EXISTS ingestion_jobs_created_at_idx ON ingestion_jobs (created_at DESC)');

  await dataPool.query(`
    CREATE TABLE IF NOT EXISTS geoscope_platform_registry (
      id SMALLINT PRIMARY KEY DEFAULT 1,
      platform_name VARCHAR(255) NOT NULL,
      platform_owner VARCHAR(255) NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await dataPool.query(
    `
    INSERT INTO geoscope_platform_registry (id, platform_name, platform_owner)
    VALUES (1, $1, $2)
    ON CONFLICT (id) DO UPDATE
      SET platform_name = EXCLUDED.platform_name,
          platform_owner = EXCLUDED.platform_owner,
          updated_at = NOW()
    `,
    [PLATFORM_NAME, PLATFORM_OWNER]
  );
}

async function main() {
  console.log(`[bootstrap] ${PLATFORM_NAME} (${PLATFORM_OWNER})`);

  const connected = await pingDataDB();
  if (!connected) {
    console.error('[bootstrap] Dataset database is not reachable.');
    process.exit(1);
  }

  await ensurePlatformTables();

  const checks = await dataPool.query(`
    SELECT
      (SELECT COUNT(*)::bigint FROM environmental_sites) AS env_rows,
      (SELECT COUNT(*)::bigint FROM ingestion_jobs) AS jobs_rows
  `);

  const envRows = Number(checks.rows[0]?.env_rows || 0);
  const jobsRows = Number(checks.rows[0]?.jobs_rows || 0);

  console.log('[bootstrap] GeoScope platform tables ready.');
  console.log(`[bootstrap] environmental_sites rows: ${envRows}`);
  console.log(`[bootstrap] ingestion_jobs rows: ${jobsRows}`);
  console.log('[bootstrap] COMPLETE');
}

main()
  .catch((err) => {
    console.error('[bootstrap] failed:', err.message);
    process.exit(1);
  })
  .finally(async () => {
    await dataPool.end();
  });
