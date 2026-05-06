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
    connectionTimeoutMillis: 10000,
    idleTimeoutMillis: 10000,
    max: 1,
  });

  try {
    await pool.query("SET statement_timeout TO '15000'");

    const stats = await pool.query(`
      SELECT
        c.reltuples::bigint AS estimated_total_rows,
        pg_total_relation_size(c.oid) AS bytes
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = 'public' AND c.relname = 'environmental_sites'
      LIMIT 1
    `);

    const sampled = await pool.query(`
      SELECT
        COUNT(*)::bigint AS sample_rows,
        COUNT(*) FILTER (WHERE attributes ? 'analyst_corrected_at')::bigint AS sample_corrected_rows
      FROM environmental_sites TABLESAMPLE SYSTEM (0.5)
    `);

    const totalEst = Number(stats.rows[0]?.estimated_total_rows || 0);
    const sampleRows = Number(sampled.rows[0]?.sample_rows || 0);
    const sampleCorrected = Number(sampled.rows[0]?.sample_corrected_rows || 0);
    const correctedRate = sampleRows > 0 ? sampleCorrected / sampleRows : 0;
    const correctedEst = Math.round(totalEst * correctedRate);

    console.log(JSON.stringify({
      ok: true,
      estimate: {
        estimated_total_rows: totalEst,
        estimated_total_rows_in_millions: Number((totalEst / 1000000).toFixed(3)),
        sampled_rows: sampleRows,
        sampled_corrected_rows: sampleCorrected,
        estimated_corrected_rows: correctedEst,
        estimated_corrected_rows_in_millions: Number((correctedEst / 1000000).toFixed(6)),
        estimated_corrected_pct: Number((correctedRate * 100).toFixed(4))
      }
    }, null, 2));
  } catch (error) {
    console.log(JSON.stringify({ ok: false, error: error.message }, null, 2));
    process.exitCode = 1;
  } finally {
    await pool.end().catch(() => {});
  }
})();
