require('dotenv').config();
const { Pool } = require('pg');
(async () => {
  const pool = new Pool({
    host: process.env.PG_HOST || 'localhost',
    port: Number(process.env.PG_PORT || 5432),
    database: process.env.PG_DATABASE || 'geoscope',
    user: process.env.PG_USER || 'postgres',
    password: process.env.PG_PASSWORD || ''
  });
  try {
    const total = await pool.query("SELECT COUNT(DISTINCT database_name) AS n FROM environmental_sites WHERE database_name IS NOT NULL AND TRIM(database_name) <> ''");
    const q = await pool.query(`WITH d AS (SELECT database_name, COUNT(DISTINCT state) FILTER (WHERE state IS NOT NULL AND TRIM(state) <> '') AS states, COUNT(DISTINCT city) FILTER (WHERE city IS NOT NULL AND TRIM(city) <> '') AS cities FROM environmental_sites GROUP BY database_name) SELECT COUNT(*) FILTER (WHERE states>=50) AS d50, COUNT(*) FILTER (WHERE states>=45) AS d45, COUNT(*) FILTER (WHERE states>=50 AND cities>=1000) AS d50c1000, COUNT(*) FILTER (WHERE states>=45 AND cities>=500) AS d45c500 FROM d`);
    const top = await pool.query(`SELECT database_name, COUNT(*) AS records, COUNT(DISTINCT state) FILTER (WHERE state IS NOT NULL AND TRIM(state) <> '') AS states, COUNT(DISTINCT city) FILTER (WHERE city IS NOT NULL AND TRIM(city) <> '') AS cities FROM environmental_sites GROUP BY database_name HAVING COUNT(DISTINCT state) FILTER (WHERE state IS NOT NULL AND TRIM(state) <> '') >= 50 ORDER BY cities DESC, records DESC LIMIT 10`);
    console.log(JSON.stringify({ total_datasets: Number(total.rows[0].n), coverage: q.rows[0], top_50_state: top.rows }, null, 2));
  } catch(e) {
    console.error('ERR:', e.message);
  } finally {
    await pool.end();
  }
})();
