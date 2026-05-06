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

function buildTargetConfig() {
  const connectionString = env('DATA_DATABASE_URL') || env('TARGET_DATABASE_URL');
  if (connectionString) {
    const isLocalHost = /localhost|127\.0\.0\.1/i.test(connectionString);
    return {
      connectionString,
      ssl: isLocalHost ? false : { rejectUnauthorized: false },
      max: 3,
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
    max: 3,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 10000,
  };
}

async function main() {
  const pool = new Pool(buildTargetConfig());
  const client = await pool.connect();

  try {
    const checks = {};

    const tableExistsQ = await client.query(`
      SELECT EXISTS (
        SELECT FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'environmental_sites'
      ) AS exists
    `);
    checks.environmental_sites_table = Boolean(tableExistsQ.rows[0]?.exists);

    if (!checks.environmental_sites_table) {
      console.log(JSON.stringify({ ok: false, checks, error: 'environmental_sites table missing' }, null, 2));
      process.exit(1);
    }

    const totalsQ = await client.query('SELECT COUNT(*)::bigint AS total FROM environmental_sites');
    checks.total_rows = Number(totalsQ.rows[0]?.total || 0);

    const dbCountQ = await client.query('SELECT COUNT(DISTINCT database_name)::int AS total FROM environmental_sites');
    checks.distinct_databases = Number(dbCountQ.rows[0]?.total || 0);

    const geoQ = await client.query(`
      SELECT
        COUNT(*) FILTER (WHERE location IS NOT NULL)::bigint AS with_location,
        COUNT(*) FILTER (WHERE source_id IS NOT NULL AND source_id <> '')::bigint AS with_source_id
      FROM environmental_sites
    `);

    checks.with_location = Number(geoQ.rows[0]?.with_location || 0);
    checks.with_source_id = Number(geoQ.rows[0]?.with_source_id || 0);

    const sampleQ = await client.query(`
      SELECT database_name, site_name, city, state, source_id
      FROM environmental_sites
      ORDER BY updated_at DESC NULLS LAST
      LIMIT 5
    `);

    checks.sample = sampleQ.rows;

    console.log(JSON.stringify({ ok: true, checks }, null, 2));
  } finally {
    client.release();
    await pool.end();
  }
}

main().catch((err) => {
  console.error(JSON.stringify({ ok: false, error: err.message }, null, 2));
  process.exit(1);
});
