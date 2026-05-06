require('dotenv').config({ path: '.env.prod.local' });
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL.replace(/\r|\n/g, '').trim(),
  ssl: { rejectUnauthorized: false }
});

pool.query("SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name")
  .then(r => {
    console.log('Tables in Supabase:');
    r.rows.forEach(row => console.log(' -', row.table_name));
    pool.end();
  })
  .catch(e => {
    console.error('Error:', e.message);
    pool.end();
  });
