require('dotenv').config();
const { Pool } = require('pg');
(async () => {
  const p = new Pool({
    host: process.env.PG_HOST || 'localhost',
    port: Number(process.env.PG_PORT || 5432),
    database: process.env.PG_DATABASE || 'geoscope',
    user: process.env.PG_USER || 'postgres',
    password: process.env.PG_PASSWORD || ''
  });

  const sample = await p.query(`
    SELECT database_name, source_org, site_name, status, left(attributes::text, 300) AS attrs
    FROM environmental_sites
    WHERE attributes::text ILIKE '%ust%' OR attributes::text ILIKE '%lust%'
    ORDER BY id DESC
    LIMIT 20
  `);

  console.log(JSON.stringify(sample.rows, null, 2));
  await p.end();
})();
