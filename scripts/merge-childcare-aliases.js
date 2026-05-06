#!/usr/bin/env node
require('dotenv').config({ path: require('path').join(__dirname, '../.env') });

const { Pool } = require('pg');

const pool = new Pool({
  host: process.env.PG_HOST,
  port: process.env.PG_PORT,
  database: process.env.PG_DATABASE,
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  max: 1,
});

async function main() {
  const before = await pool.query(
    "select database_name, count(*) as c from environmental_sites where database_name in ('DAYCARE','CHILD CARE','HIFLD Child Care') group by database_name order by database_name"
  );
  console.log('Before:', before.rows);

  const upd = await pool.query(
    "update environmental_sites set database_name='HIFLD Child Care' where database_name in ('DAYCARE','CHILD CARE')"
  );
  console.log('Updated rows:', upd.rowCount || 0);

  const after = await pool.query(
    "select database_name, count(*) as c from environmental_sites where database_name='HIFLD Child Care' group by database_name"
  );
  console.log('After:', after.rows);

  await pool.end();
}

main().catch((e) => {
  console.error(e.message);
  process.exit(1);
});
