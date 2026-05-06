require('dotenv').config({ path: '.env.prod.local' });
const { Pool } = require('pg');

function clean(v) {
  return v ? String(v).replace(/[\r\n\s]+$/, '').replace(/^["']+|["']+$/g, '') : v;
}

const pool = new Pool({
  connectionString: clean(process.env.DATABASE_URL),
  ssl: { rejectUnauthorized: false }
});

async function tableCount(tableName) {
  const r = await pool.query(`SELECT COUNT(*)::bigint AS c FROM ${tableName}`);
  return Number(r.rows[0].c);
}

async function run() {
  try {
    const beforeEnv = await tableCount('environmental_sites');
    console.log('Before environmental_sites:', beforeEnv.toLocaleString());

    // Free space by clearing the largest dataset table.
    await pool.query('TRUNCATE TABLE environmental_sites RESTART IDENTITY');

    const afterEnv = await tableCount('environmental_sites');
    console.log('After environmental_sites:', afterEnv.toLocaleString());

    try {
      await pool.query('VACUUM (ANALYZE) environmental_sites');
      console.log('Vacuum completed for environmental_sites.');
    } catch (vacErr) {
      console.log('Vacuum skipped:', vacErr.message);
    }

    const users = await tableCount('users');
    const orders = await tableCount('orders');
    const wbOrders = await tableCount('workbench_orders');
    console.log('users:', users.toLocaleString());
    console.log('orders:', orders.toLocaleString());
    console.log('workbench_orders:', wbOrders.toLocaleString());
  } catch (e) {
    console.error('FAILED:', e.message);
    process.exitCode = 1;
  } finally {
    await pool.end();
  }
}

run();
