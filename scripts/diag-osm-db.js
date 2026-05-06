#!/usr/bin/env node
require('dotenv').config({ path: require('path').join(__dirname, '../.env') });
const { Pool } = require('pg');

(async function main() {
  const pool = new Pool({
    host: process.env.PG_HOST,
    port: Number(process.env.PG_PORT),
    database: process.env.PG_DATABASE,
    user: process.env.PG_USER,
    password: process.env.PG_PASSWORD,
  });

  try {
    const tables = await pool.query(
      "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name IN ('area_features','planet_osm_line','osm_lines_raw') ORDER BY table_name"
    );
    console.log('tables:', tables.rows);

    const cols = await pool.query(
      "SELECT column_name, data_type, udt_name FROM information_schema.columns WHERE table_schema='public' AND table_name='area_features' ORDER BY ordinal_position"
    );
    console.log('area_features columns:', cols.rows);

    const counts = await pool.query(
      "SELECT (SELECT COUNT(*) FROM area_features) AS area_features_count"
    );
    console.log('counts:', counts.rows[0]);
  } catch (err) {
    console.error('diag failed:', err.message);
    process.exitCode = 1;
  } finally {
    await pool.end();
  }
})();
