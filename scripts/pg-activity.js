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
    const res = await pool.query(`
      SELECT pid, usename, application_name, state, wait_event_type, wait_event,
             backend_start, xact_start,
             LEFT(query, 200) AS query
      FROM pg_stat_activity
      WHERE datname = current_database()
      ORDER BY state, pid
    `);
    console.log(JSON.stringify(res.rows, null, 2));
  } catch (err) {
    console.error(err.message);
    process.exitCode = 1;
  } finally {
    await pool.end();
  }
})();
