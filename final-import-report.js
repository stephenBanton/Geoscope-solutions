require('dotenv').config();
const { pool } = require('./db');

(async () => {
  try {
    const r1 = await pool.query('SELECT COUNT(*) FROM environmental_sites');
    const r2 = await pool.query('SELECT database_name, category, COUNT(*) as cnt FROM environmental_sites GROUP BY database_name, category ORDER BY database_name, category');
    
    console.log('\n✅ FINAL IMPORT SUMMARY\n');
    console.log('Total environmental sites:', r1.rows[0].count);
    console.log('\nDatabase distribution:\n');
    r2.rows.forEach(row => {
      console.log(`  ${row.database_name.padEnd(25)} [${row.category.padEnd(15)}]: ${row.cnt.toLocaleString()}`);
    });
    
    await pool.end();
  } catch(e) {
    console.error('Error:', e.message);
    process.exit(1);
  }
})();
