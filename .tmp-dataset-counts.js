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

  const top = await p.query(`
    SELECT database_name, COUNT(*)::bigint AS c
    FROM environmental_sites
    GROUP BY database_name
    ORDER BY c DESC
    LIMIT 30
  `);

  const ust = await p.query(`
    SELECT database_name, COUNT(*)::bigint AS c
    FROM environmental_sites
    WHERE database_name ILIKE '%ust%' OR database_name ILIKE '%lust%'
    GROUP BY database_name
    ORDER BY c DESC
  `);

  console.log('TOP DATABASES');
  console.log(JSON.stringify(top.rows, null, 2));
  console.log('UST/LUST DATABASES');
  console.log(JSON.stringify(ust.rows, null, 2));

  await p.end();
})();
