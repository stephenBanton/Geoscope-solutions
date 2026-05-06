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
    await pool.query("SET statement_timeout TO '0'");

    const bounds = await pool.query('SELECT MIN(id)::bigint AS min_id, MAX(id)::bigint AS max_id FROM environmental_sites');
    const minId = Number(bounds.rows[0]?.min_id || 0);
    const maxId = Number(bounds.rows[0]?.max_id || 0);

    if (!Number.isFinite(minId) || !Number.isFinite(maxId) || maxId <= 0) {
      console.log(JSON.stringify({ ok: true, progress: { total_rows: 0, corrected_rows: 0, corrected_pct: 0, total_rows_in_millions: 0, corrected_rows_in_millions: 0 } }, null, 2));
      return;
    }

    const chunkSize = 100000;
    let totalRows = 0;
    let correctedRows = 0;

    for (let start = minId; start <= maxId; start += chunkSize) {
      const end = Math.min(start + chunkSize - 1, maxId);
      const q = await pool.query(
        `SELECT
           COUNT(*)::bigint AS total_rows,
           COUNT(*) FILTER (WHERE attributes ? 'analyst_corrected_at')::bigint AS corrected_rows
         FROM environmental_sites
         WHERE id BETWEEN $1 AND $2`,
        [start, end]
      );

      totalRows += Number(q.rows[0]?.total_rows || 0);
      correctedRows += Number(q.rows[0]?.corrected_rows || 0);
    }

    const correctedPct = totalRows > 0 ? Number(((correctedRows / totalRows) * 100).toFixed(4)) : 0;
    const payload = {
      ok: true,
      progress: {
        total_rows: totalRows,
        corrected_rows: correctedRows,
        corrected_pct: correctedPct,
        total_rows_in_millions: Number((totalRows / 1000000).toFixed(6)),
        corrected_rows_in_millions: Number((correctedRows / 1000000).toFixed(6)),
      }
    };

    console.log(JSON.stringify(payload, null, 2));
  } catch (error) {
    console.log(JSON.stringify({ ok: false, error: error.message }, null, 2));
    process.exitCode = 1;
  } finally {
    await pool.end().catch(() => {});
  }
})();
