require('dotenv').config();
const { Pool } = require('pg');
const p = new Pool({
  host: process.env.PG_HOST,
  port: +process.env.PG_PORT,
  database: process.env.PG_DATABASE,
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD
});

async function run(){
  const q = `
    SELECT pid, state, wait_event_type, wait_event, left(query,120) AS q
    FROM pg_stat_activity
    WHERE datname = current_database() AND usename = current_user
    ORDER BY query_start DESC
    LIMIT 10`;
  const r = await p.query(q);
  console.table(r.rows);
  await p.end();
}
run().catch(async e => { console.error(e.message); await p.end(); });
