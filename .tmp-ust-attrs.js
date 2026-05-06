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

  const q1 = await p.query(`
    SELECT database_name, COUNT(*)::bigint AS c
    FROM environmental_sites
    WHERE attributes::text ~* '(^|[^A-Z])UST([^A-Z]|$)' OR attributes::text ~* 'LUST'
    GROUP BY database_name
    ORDER BY c DESC
    LIMIT 30
  `);

  const q2 = await p.query(`
    SELECT database_name, source_org, site_name, left(attributes::text, 500) AS attrs
    FROM environmental_sites
    WHERE (attributes::text ~* '(^|[^A-Z])UST([^A-Z]|$)' OR attributes::text ~* 'LUST')
    ORDER BY id DESC
    LIMIT 20
  `);

  console.log('MATCH COUNTS');
  console.log(JSON.stringify(q1.rows, null, 2));
  console.log('SAMPLES');
  console.log(JSON.stringify(q2.rows, null, 2));
  await p.end();
})();
