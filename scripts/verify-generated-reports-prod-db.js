require('dotenv').config({ path: '.env.production.vercel' });

const { Pool } = require('pg');

function cleanEnvValue(value) {
  return String(value ?? '').replace(/\r|\n/g, '').trim();
}

function buildPoolConfig() {
  const databaseUrl = cleanEnvValue(process.env.DATABASE_URL);
  if (!databaseUrl) {
    throw new Error('DATABASE_URL missing in .env.production.vercel');
  }

  const isLocalHost = /localhost|127\.0\.0\.1/i.test(databaseUrl);
  return {
    connectionString: databaseUrl,
    ssl: isLocalHost ? false : { rejectUnauthorized: false },
    max: 2,
    idleTimeoutMillis: 10000,
    connectionTimeoutMillis: 10000,
  };
}

async function main() {
  const pool = new Pool(buildPoolConfig());
  const startedAt = Date.now();
  const probeOrderId = 999999999;

  try {
    await pool.query('SELECT 1');

    const migrationSql = `
      CREATE TABLE IF NOT EXISTS generated_reports (
          order_id  BIGINT PRIMARY KEY REFERENCES orders(id) ON DELETE CASCADE,
          file_name TEXT NOT NULL,
          mime_type TEXT NOT NULL DEFAULT 'application/pdf',
          pdf_data  BYTEA NOT NULL,
          storage_path TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS generated_reports_created_at_idx ON generated_reports (created_at DESC);
    `;

    await pool.query(migrationSql);

    const existsResult = await pool.query(
      `SELECT EXISTS (
         SELECT 1
         FROM information_schema.tables
         WHERE table_schema = 'public' AND table_name = 'generated_reports'
       ) AS exists`
    );

    const columnResult = await pool.query(
      `SELECT column_name
       FROM information_schema.columns
       WHERE table_schema = 'public' AND table_name = 'generated_reports'
       ORDER BY ordinal_position`
    );

    // Permission and upsert verification in a transaction and rollback.
    await pool.query('BEGIN');

    const deleteProbeOrderSql = 'DELETE FROM orders WHERE id = $1';
    const insertProbeOrderSql = `
      INSERT INTO orders (id, project_name, address, status, priority, source, created_at, updated_at)
      VALUES ($1, 'Generated Reports Probe', 'Probe Address', 'received', 'normal', 'diagnostic-script', NOW(), NOW())
      ON CONFLICT (id) DO UPDATE SET updated_at = NOW()
    `;

    await pool.query(deleteProbeOrderSql, [probeOrderId]);
    await pool.query(insertProbeOrderSql, [probeOrderId]);

    const firstUpsert = await pool.query(
      `INSERT INTO generated_reports (order_id, file_name, mime_type, pdf_data, storage_path, updated_at)
       VALUES ($1, $2, 'application/pdf', $3, $4, NOW())
       ON CONFLICT (order_id) DO UPDATE
       SET file_name = EXCLUDED.file_name,
           mime_type = EXCLUDED.mime_type,
           pdf_data = EXCLUDED.pdf_data,
           storage_path = EXCLUDED.storage_path,
           updated_at = NOW()
       RETURNING order_id, file_name, storage_path`,
      [probeOrderId, 'probe-report-v1.pdf', Buffer.from('%PDF-probe-v1%'), 'orders/probe/probe-report-v1.pdf']
    );

    const secondUpsert = await pool.query(
      `INSERT INTO generated_reports (order_id, file_name, mime_type, pdf_data, storage_path, updated_at)
       VALUES ($1, $2, 'application/pdf', $3, $4, NOW())
       ON CONFLICT (order_id) DO UPDATE
       SET file_name = EXCLUDED.file_name,
           mime_type = EXCLUDED.mime_type,
           pdf_data = EXCLUDED.pdf_data,
           storage_path = EXCLUDED.storage_path,
           updated_at = NOW()
       RETURNING order_id, file_name, storage_path`,
      [probeOrderId, 'probe-report-v2.pdf', Buffer.from('%PDF-probe-v2%'), 'orders/probe/probe-report-v2.pdf']
    );

    const canInsertResult = await pool.query(
      `SELECT has_table_privilege(current_user, 'public.generated_reports', 'INSERT') AS can_insert,
              has_table_privilege(current_user, 'public.generated_reports', 'UPDATE') AS can_update,
              has_table_privilege(current_user, 'public.generated_reports', 'SELECT') AS can_select`
    );

    await pool.query('ROLLBACK');

    const payload = {
      ok: true,
      elapsedMs: Date.now() - startedAt,
      tableExists: Boolean(existsResult.rows[0]?.exists),
      columns: columnResult.rows.map((r) => r.column_name),
      permissions: canInsertResult.rows[0] || null,
      upsertProbe: {
        first: firstUpsert.rows[0] || null,
        second: secondUpsert.rows[0] || null,
      },
    };

    console.log(JSON.stringify(payload, null, 2));
  } catch (error) {
    try {
      await pool.query('ROLLBACK');
    } catch {
      // ignore rollback errors
    }
    console.error(JSON.stringify({ ok: false, error: error.message }, null, 2));
    process.exitCode = 1;
  } finally {
    await pool.end().catch(() => {});
  }
}

main();
