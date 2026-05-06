require('dotenv').config({ path: require('path').join(__dirname, '../.env') });
const { Pool } = require('pg');
const pool = new Pool({
  host: process.env.PG_HOST,
  port: process.env.PG_PORT,
  database: process.env.PG_DATABASE,
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  max: 1
});

const mappings = [
  ['EPA FRS', 'FRS_FACILITY', 'FRS_FACILITY'],
  ['FEMA NFIP Flood Claims', 'FEMA_FLOOD_CLAIM', 'FEMA_FLOOD_CLAIM'],
  ['OPENSTREETMAP ROAD NETWORK', 'OSM_ROAD', 'OSM_ROAD'],
  ['USGS NWIS Groundwater Well', 'WELL', 'WELL'],
  ['FEMA NFIP Policies', 'FEMA_FLOOD_POLICY', 'FEMA_FLOOD_POLICY']
];

(async () => {
  const existing = await pool.query('select distinct database_name from environmental_sites');
  const existingSet = new Set(existing.rows.map(r => r.database_name));

  let totalUpdated = 0;
  for (const [fromDb, cls, newDb] of mappings) {
    if (existingSet.has(newDb)) {
      console.log(`SKIP ${newDb} (already exists as bucket)`);
      continue;
    }
    const r = await pool.query(
      'update environmental_sites set database_name=$1 where database_name=$2 and class_code=$3',
      [newDb, fromDb, cls]
    );
    console.log(`${fromDb} [${cls}] -> ${newDb}: ${r.rowCount} rows`);
    totalUpdated += r.rowCount;
    existingSet.add(newDb);
  }

  const final = await pool.query('select count(*)::bigint as total, count(distinct database_name)::int as buckets from environmental_sites');
  console.log('RESULT', final.rows[0], 'total_rows_relabelled', totalUpdated);

  await pool.end();
})().catch(async (e) => {
  console.error(e);
  try { await pool.end(); } catch {}
  process.exit(1);
});
