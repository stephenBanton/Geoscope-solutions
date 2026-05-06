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

  const cols = await p.query(`
    SELECT column_name FROM information_schema.columns
    WHERE table_name='database_catalog'
    ORDER BY ordinal_position
  `);
  console.log('database_catalog columns:', cols.rows.map(r => r.column_name).join(', '));

  const r = await p.query(`
    SELECT *
    FROM database_catalog
    WHERE name ILIKE '%ust%' OR name ILIKE '%lust%' OR name ILIKE '%rcra%' OR name ILIKE '%tri%' OR name ILIKE '%superfund%'
    ORDER BY name
    LIMIT 50
  `);

  console.log(JSON.stringify(r.rows, null, 2));
  await p.end();
})();
