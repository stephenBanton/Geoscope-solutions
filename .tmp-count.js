require('dotenv').config();
const { Pool } = require('pg');
const p = new Pool({
  host: process.env.PG_HOST,
  port: +process.env.PG_PORT,
  database: process.env.PG_DATABASE,
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD
});

async function run() {
  const r = await p.query("SELECT database_name, COUNT(*)::bigint AS n FROM environmental_sites WHERE database_name IN ('EPA ICIS-Air','RCRA Handler','RCRA LQG','RCRA SQG','RCRA TSDF','RCRA US') GROUP BY database_name ORDER BY n DESC");
  console.log(r.rows);
  const t = await p.query('SELECT COUNT(*)::bigint AS total FROM environmental_sites');
  console.log('Total:', t.rows[0].total);
  await p.end();
}
run().catch(async e => { console.error(e.message); await p.end(); });
