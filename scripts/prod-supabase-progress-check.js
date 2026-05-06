require('dotenv').config({ path: '.env.production.vercel' });
const { Pool } = require('pg');

(async () => {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    console.log(JSON.stringify({ ok: false, error: 'DATABASE_URL missing' }, null, 2));
    process.exit(1);
  }

  const isLocal = /localhost|127\.0\.0\.1/i.test(connectionString);
  const pool = new Pool({
    connectionString,
    ssl: isLocal ? false : { rejectUnauthorized: false },
    connectionTimeoutMillis: 15000,
    idleTimeoutMillis: 10000,
    max: 2,
  });

  try {
    const query = `
      WITH totals AS (
        SELECT
          COUNT(*)::bigint AS total_rows,
          COUNT(*) FILTER (WHERE attributes ? 'analyst_corrected_at')::bigint AS corrected_rows
        FROM environmental_sites
      ),
      million AS (
        SELECT
          COUNT(*) FILTER (
            WHERE attributes ? 'analyst_corrected_at'
              AND COALESCE((attributes->>'analyst_correction_note'),'') ILIKE '%million%'
          )::bigint AS correction_note_mentions_million
        FROM environmental_sites
      )
      SELECT
        totals.total_rows,
        totals.corrected_rows,
        ROUND((totals.corrected_rows::numeric / NULLIF(totals.total_rows,0)::numeric) * 100, 4) AS corrected_pct,
        ROUND((totals.total_rows::numeric / 1000000), 6) AS total_rows_in_millions,
        ROUND((totals.corrected_rows::numeric / 1000000), 6) AS corrected_rows_in_millions,
        million.correction_note_mentions_million
      FROM totals
      CROSS JOIN million;
    `;

    const result = await pool.query(query);
    console.log(JSON.stringify({ ok: true, progress: result.rows[0] }, null, 2));
  } catch (error) {
    console.log(JSON.stringify({ ok: false, error: error.message }, null, 2));
    process.exitCode = 1;
  } finally {
    await pool.end().catch(() => {});
  }
})();
