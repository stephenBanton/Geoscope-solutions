require('dotenv').config({ path: '.env.prod.local' });
const { Pool } = require('pg');

function clean(v) {
  return v ? String(v).replace(/[\r\n\s]+$/, '').replace(/^["']+|["']+$/g, '') : v;
}

const pool = new Pool({
  connectionString: clean(process.env.DATABASE_URL),
  ssl: { rejectUnauthorized: false }
});

async function main() {
  try {
    const r1 = await pool.query('SELECT COUNT(*) as total FROM environmental_sites');
    console.log('environmental_sites EXACT total:', Number(r1.rows[0].total).toLocaleString());

    const r2 = await pool.query('SELECT database_name, COUNT(*) as cnt FROM environmental_sites GROUP BY database_name ORDER BY cnt DESC');
    console.log('\nBreakdown by database_name (' + r2.rows.length + ' datasets):');
    r2.rows.forEach(row => console.log('  ' + (row.database_name || 'NULL') + ': ' + Number(row.cnt).toLocaleString()));

    const r3 = await pool.query('SELECT COUNT(DISTINCT database_name) as datasets FROM environmental_sites');
    console.log('\nTotal distinct datasets:', r3.rows[0].datasets);
  } catch (e) {
    console.error('ERROR:', e.message);
  } finally {
    await pool.end();
  }
}

main();
