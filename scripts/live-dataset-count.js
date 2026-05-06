const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

async function main() {
  const distinct = await pool.query(`
    SELECT COUNT(DISTINCT NULLIF(TRIM(database_name), ''))::bigint AS distinct_datasets
    FROM environmental_sites
    WHERE COALESCE(TRIM(database_name), '') <> ''
  `);

  const top = await pool.query(`
    SELECT database_name, COUNT(*)::bigint AS cnt
    FROM environmental_sites
    WHERE COALESCE(TRIM(database_name), '') <> ''
    GROUP BY database_name
    ORDER BY cnt DESC
    LIMIT 15
  `);

  const key = await pool.query(`
    SELECT database_name, COUNT(*)::bigint AS cnt
    FROM environmental_sites
    WHERE UPPER(COALESCE(database_name, '')) IN (
      'LUST', 'ECHO RCRA', 'ECHO NPDES', 'HAZWASTE', 'RCRAINFO HANDLERS', 'RCRAINFO CORRACTS'
    )
    GROUP BY database_name
    ORDER BY database_name
  `);

  const byCategory = await pool.query(`
    SELECT COALESCE(category, 'uncategorized') AS category,
           COUNT(DISTINCT database_name)::bigint AS datasets,
           COUNT(*)::bigint AS rows
    FROM environmental_sites
    WHERE COALESCE(TRIM(database_name), '') <> ''
    GROUP BY COALESCE(category, 'uncategorized')
    ORDER BY rows DESC
  `);

  console.log(`DISTINCT=${distinct.rows[0].distinct_datasets}`);
  console.log(`TOP=${JSON.stringify(top.rows)}`);
  console.log(`KEY=${JSON.stringify(key.rows)}`);
  console.log(`CATEGORY=${JSON.stringify(byCategory.rows)}`);
}

main()
  .catch((err) => {
    console.error(`QUERY_ERR=${err.message}`);
    process.exitCode = 1;
  })
  .finally(async () => {
    await pool.end();
  });