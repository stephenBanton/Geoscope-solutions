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

  const total = await pool.query(`
    SELECT COUNT(DISTINCT database_name) AS datasets
    FROM environmental_sites
    WHERE database_name IS NOT NULL AND TRIM(database_name) <> ''
  `);

  const nationwide = await pool.query(`
    SELECT
      COUNT(*) FILTER (WHERE state_count >= 50) AS datasets_50_states,
      COUNT(*) FILTER (WHERE state_count >= 45) AS datasets_45_states,
      COUNT(*) FILTER (WHERE state_count >= 50 AND city_count >= 1000) AS datasets_50_states_1000_cities,
      COUNT(*) FILTER (WHERE state_count >= 45 AND city_count >= 500) AS datasets_45_states_500_cities
    FROM (
      SELECT
        database_name,
        COUNT(DISTINCT state) FILTER (WHERE state IS NOT NULL AND TRIM(state) <> '') AS state_count,
        COUNT(DISTINCT city) FILTER (WHERE city IS NOT NULL AND TRIM(city) <> '') AS city_count
      FROM environmental_sites
      GROUP BY database_name
    ) x
  `);

  const top = await pool.query(`
    SELECT
      database_name,
      COUNT(*) AS records,
      COUNT(DISTINCT state) FILTER (WHERE state IS NOT NULL AND TRIM(state) <> '') AS states,
      COUNT(DISTINCT city) FILTER (WHERE city IS NOT NULL AND TRIM(city) <> '') AS cities
    FROM environmental_sites
    GROUP BY database_name
    HAVING COUNT(DISTINCT state) FILTER (WHERE state IS NOT NULL AND TRIM(state) <> '') >= 45
    ORDER BY states DESC, cities DESC, records DESC
    LIMIT 25
  `);

  console.log(JSON.stringify({
    total_datasets: Number(total.rows[0].datasets),
    coverage_counts: nationwide.rows[0],
    top_nationwide: top.rows
  }, null, 2));

  await pool.end();
})();
