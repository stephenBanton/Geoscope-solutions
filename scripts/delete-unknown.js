require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({
  host: process.env.PG_HOST,
  port: +process.env.PG_PORT,
  database: process.env.PG_DATABASE,
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
});

const BATCH_SIZE = 50000;

async function countUnknown(client) {
  const r = await client.query("SELECT COUNT(*)::bigint AS c FROM environmental_sites WHERE COALESCE(TRIM(database_name),'') IN ('','UNKNOWN')");
  return Number(r.rows[0].c || 0);
}

(async () => {
  const client = await pool.connect();
  try {
    const before = await countUnknown(client);
    console.log('UNKNOWN_ROWS_BEFORE=' + before);

    let totalDeleted = 0;
    let loops = 0;

    while (true) {
      const del = await client.query(
        `WITH doomed AS (
           SELECT ctid
           FROM environmental_sites
           WHERE COALESCE(TRIM(database_name),'') IN ('','UNKNOWN')
           LIMIT $1
         )
         DELETE FROM environmental_sites e
         USING doomed d
         WHERE e.ctid = d.ctid`,
        [BATCH_SIZE]
      );

      const deleted = del.rowCount || 0;
      totalDeleted += deleted;
      loops += 1;

      console.log(`BATCH_${loops}_DELETED=${deleted}`);

      if (deleted === 0) break;
    }

    const after = await countUnknown(client);
    const total = await client.query('SELECT COUNT(*)::bigint AS c FROM environmental_sites');

    console.log('UNKNOWN_ROWS_DELETED=' + totalDeleted);
    console.log('UNKNOWN_ROWS_AFTER=' + after);
    console.log('TOTAL_ROWS_AFTER=' + total.rows[0].c);
  } finally {
    client.release();
    await pool.end();
  }
})().catch(async (e) => {
  console.error('DELETE_UNKNOWN_ERROR=' + e.message);
  try { await pool.end(); } catch {}
  process.exit(1);
});
