require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({
  host: process.env.PG_HOST,
  port: +process.env.PG_PORT,
  database: process.env.PG_DATABASE,
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
});

(async () => {
  const q = `
    SELECT
      COUNT(1)::bigint AS total,
      SUM(CASE WHEN COALESCE(TRIM(database_name), '') IN ('', 'UNKNOWN') THEN 1 ELSE 0 END)::bigint AS unknown_rows
    FROM environmental_sites
  `;
  const r = await pool.query(q);
  console.log('TOTAL=' + r.rows[0].total);
  console.log('UNKNOWN=' + r.rows[0].unknown_rows);
  await pool.end();
})().catch(async (e) => {
  console.error('COUNT_UNKNOWN_ERROR=' + e.message);
  try { await pool.end(); } catch {}
  process.exit(1);
});
