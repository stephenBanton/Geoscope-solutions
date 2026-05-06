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
    const counts = await pool.query(`
      SELECT
        (SELECT COUNT(*) FROM area_features) AS area_features_total,
        (SELECT COUNT(*) FROM area_features WHERE type='road') AS area_features_roads,
        (SELECT COUNT(*) FROM osm_lines_raw) AS osm_lines_raw_total
    `);
    console.log('counts:', counts.rows[0]);

    const activity = await pool.query(`
      SELECT pid, state, wait_event_type, wait_event,
             LEFT(query, 120) AS query_snippet
      FROM pg_stat_activity
      WHERE datname = current_database()
        AND query ILIKE '%osm_lines_raw%'
      ORDER BY state, pid
    `);
    console.log('activity:', activity.rows);
  } catch (err) {
    console.error(err.message);
    process.exitCode = 1;
  } finally {
    await pool.end();
  }
})();
