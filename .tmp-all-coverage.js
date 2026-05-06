require('dotenv').config();
const { Pool } = require('pg');
(async () => {
  const p = new Pool({host:process.env.PG_HOST||'localhost',port:Number(process.env.PG_PORT||5432),database:process.env.PG_DATABASE||'geoscope',user:process.env.PG_USER||'postgres',password:process.env.PG_PASSWORD||''});
  const r = await p.query(`
    WITH d AS (
      SELECT database_name,
             COUNT(DISTINCT state) FILTER (WHERE state IS NOT NULL AND TRIM(state) <> '') AS states,
             COUNT(DISTINCT city) FILTER (WHERE city IS NOT NULL AND TRIM(city) <> '') AS cities,
             COUNT(*) AS records
      FROM environmental_sites
      WHERE database_name IS NOT NULL AND TRIM(database_name) <> ''
      GROUP BY database_name
    )
    SELECT
      COUNT(*) FILTER (WHERE states >= 50) AS datasets_50_states,
      COUNT(*) FILTER (WHERE states >= 50 AND cities >= 1000) AS datasets_50_states_1000_cities,
      COUNT(*) FILTER (WHERE states >= 45 AND cities >= 500) AS datasets_45_states_500_cities,
      COUNT(*) AS total_distinct
    FROM d
  `);
  console.log(JSON.stringify(r.rows[0], null, 2));
  await p.end();
})();
