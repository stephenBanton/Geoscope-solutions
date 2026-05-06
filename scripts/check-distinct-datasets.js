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
  const total = await pool.query('SELECT COUNT(DISTINCT database_name)::int AS c FROM environmental_sites');
  console.log('distinct_datasets=' + total.rows[0].c);

  const targets = [
    'MSHA Metal Mines',
    'MSHA Coal Mines',
    'MSHA Nonmetal Mines',
    'FEMA Flood Q3',
    'RCRA SQG',
    'RCRA LQG',
    'RCRA VSQG',
    'RCRA GenSV',
    'NPDES Minor Dischargers',
    'CAFO Facilities',
  ];

  const rows = await pool.query(
    'SELECT database_name, COUNT(*)::int AS c FROM environmental_sites WHERE database_name = ANY($1) GROUP BY database_name ORDER BY database_name',
    [targets]
  );

  rows.rows.forEach((r) => {
    console.log(r.database_name + '|' + r.c);
  });

  await pool.end();
}

main().catch(async (e) => {
  console.error(e);
  try {
    await pool.end();
  } catch {}
  process.exit(1);
});
