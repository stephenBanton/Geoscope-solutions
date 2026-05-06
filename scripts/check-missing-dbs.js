require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({
  host: process.env.PG_HOST,
  port: Number(process.env.PG_PORT || 5432),
  database: process.env.PG_DATABASE,
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
});

async function main() {
  const expectedRes = await pool.query(`
    SELECT DISTINCT UPPER(name) AS name
    FROM database_catalog
    WHERE category IN ('contamination', 'regulatory', 'hydrology', 'geology', 'receptors')
    ORDER BY 1
  `);

  const loadedRes = await pool.query(`
    SELECT DISTINCT UPPER(database_name) AS name
    FROM environmental_sites
    WHERE database_name IS NOT NULL AND TRIM(database_name) <> ''
    ORDER BY 1
  `);

  const expected = new Set(expectedRes.rows.map((r) => r.name));
  const loaded = new Set(loadedRes.rows.map((r) => r.name));

  const missing = [...expected].filter((x) => !loaded.has(x)).sort();
  const extra = [...loaded].filter((x) => !expected.has(x)).sort();

  const byDb = await pool.query(`
    SELECT database_name, category, COUNT(*)::bigint AS cnt
    FROM environmental_sites
    GROUP BY database_name, category
    ORDER BY cnt DESC
    LIMIT 40
  `);

  console.log('EXPECTED_COUNT=' + expected.size);
  console.log('LOADED_DISTINCT_COUNT=' + loaded.size);
  console.log('MISSING_COUNT=' + missing.length);
  console.log('EXTRA_COUNT=' + extra.length);

  console.log('\nMISSING_LIST_START');
  missing.forEach((x) => console.log(x));
  console.log('MISSING_LIST_END');

  console.log('\nTOP_LOADED_START');
  byDb.rows.forEach((r) => console.log(`${r.database_name}\t${r.category}\t${r.cnt}`));
  console.log('TOP_LOADED_END');

  console.log('\nEXTRA_LIST_START');
  extra.slice(0, 100).forEach((x) => console.log(x));
  console.log('EXTRA_LIST_END');
}

main()
  .catch((err) => {
    console.error(err.message);
    process.exit(1);
  })
  .finally(async () => {
    await pool.end();
  });
