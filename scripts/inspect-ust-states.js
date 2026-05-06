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

async function run() {
  const dbs = ['UST', 'EPA UST', 'EPA LUST', 'HIFLD Child Care'];
  for (const db of dbs) {
    const dist = await pool.query(
      `select coalesce(state,'<NULL>') as state, count(*) as c
       from environmental_sites
       where database_name = $1
       group by 1
       order by c desc
       limit 20`,
      [db]
    );
    console.log(`\n=== ${db} state distribution (top 20) ===`);
    console.table(dist.rows);

    const samples = await pool.query(
      `select source_id, site_name, city, state, address
       from environmental_sites
       where database_name = $1
       order by random()
       limit 5`,
      [db]
    );
    console.log(`=== ${db} samples ===`);
    console.table(samples.rows);
  }

  await pool.end();
}

run().catch(async (e) => {
  console.error(e.message);
  try { await pool.end(); } catch (_) {}
  process.exit(1);
});
