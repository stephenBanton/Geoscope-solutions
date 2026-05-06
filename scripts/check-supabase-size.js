const { Pool } = require('pg');

const connectionString =
  process.env.DATABASE_URL ||
  'postgresql://postgres.imvcveoynxkceupggnnw:Mombasad3780%2A@aws-1-eu-west-1.pooler.supabase.com:5432/postgres';

async function main() {
  const pool = new Pool({
    connectionString,
    ssl: { rejectUnauthorized: false }
  });

  try {
    const sql = `
      SELECT
        pg_database_size(current_database()) AS db_bytes,
        pg_size_pretty(pg_database_size(current_database())) AS db_pretty,
        pg_total_relation_size('environmental_sites') AS env_total_bytes,
        pg_size_pretty(pg_total_relation_size('environmental_sites')) AS env_total_pretty,
        pg_relation_size('environmental_sites') AS env_table_bytes,
        pg_indexes_size('environmental_sites') AS env_index_bytes,
        (SELECT COUNT(*) FROM environmental_sites) AS env_rows
    `;

    const { rows } = await pool.query(sql);
    console.log(JSON.stringify(rows[0], null, 2));
  } finally {
    await pool.end();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
