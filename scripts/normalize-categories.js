require('dotenv').config({ path: require('path').join(__dirname, '../.env') });
const { pool } = require('../db');

async function main() {
  const sql = `
    UPDATE environmental_sites
    SET category = CASE
      WHEN lower(coalesce(database_name,'')) ~ '(rcra|cercl|npl|lust|ust|brown|corracts|tri|tris|pcb|mgp|pfas|spill|acres)'
        OR lower(coalesce(category,'')) ~ '(contam|toxic|hazard|waste|npl|rcra|tri|ust|lust|pfas)'
        THEN 'contamination'
      WHEN lower(coalesce(database_name,'')) ~ '(echo|npdes|air|rmp|icis|dod|fuds|federal)'
        OR lower(coalesce(category,'')) ~ '(regulat|permit|compliance|enforce)'
        THEN 'regulatory'
      WHEN lower(coalesce(database_name,'')) ~ '(flood|wetland|storm|hydro|water|nwi)'
        OR lower(coalesce(category,'')) ~ '(hydro|flood|wetland|water)'
        THEN 'hydrology'
      WHEN lower(coalesce(database_name,'')) ~ '(mine|geolog|radon|coal|ssurgo|asbestos|usgs|geochem|nure)'
        OR lower(coalesce(category,'')) ~ '(geolog|mine|radon|soil|rock|nure|state|rass|pluto)'
        THEN 'geology'
      WHEN lower(coalesce(database_name,'')) ~ '(school|hospital|daycare|nursing|college|prison|receptor)'
        OR lower(coalesce(category,'')) ~ '(receptor|school|hospital|daycare|nursing|college|prison)'
        THEN 'receptors'
      ELSE 'contamination'
    END
    WHERE category IS DISTINCT FROM CASE
      WHEN lower(coalesce(database_name,'')) ~ '(rcra|cercl|npl|lust|ust|brown|corracts|tri|tris|pcb|mgp|pfas|spill|acres)'
        OR lower(coalesce(category,'')) ~ '(contam|toxic|hazard|waste|npl|rcra|tri|ust|lust|pfas)'
        THEN 'contamination'
      WHEN lower(coalesce(database_name,'')) ~ '(echo|npdes|air|rmp|icis|dod|fuds|federal)'
        OR lower(coalesce(category,'')) ~ '(regulat|permit|compliance|enforce)'
        THEN 'regulatory'
      WHEN lower(coalesce(database_name,'')) ~ '(flood|wetland|storm|hydro|water|nwi)'
        OR lower(coalesce(category,'')) ~ '(hydro|flood|wetland|water)'
        THEN 'hydrology'
      WHEN lower(coalesce(database_name,'')) ~ '(mine|geolog|radon|coal|ssurgo|asbestos|usgs|geochem|nure)'
        OR lower(coalesce(category,'')) ~ '(geolog|mine|radon|soil|rock|nure|state|rass|pluto)'
        THEN 'geology'
      WHEN lower(coalesce(database_name,'')) ~ '(school|hospital|daycare|nursing|college|prison|receptor)'
        OR lower(coalesce(category,'')) ~ '(receptor|school|hospital|daycare|nursing|college|prison)'
        THEN 'receptors'
      ELSE 'contamination'
    END
  `;

  const updated = await pool.query(sql);
  console.log('UPDATED_ROWS', updated.rowCount);

  const counts = await pool.query(`
    SELECT category, COUNT(*)::bigint AS c
    FROM environmental_sites
    GROUP BY category
    ORDER BY c DESC
  `);

  console.log('CATEGORY_COUNTS');
  for (const row of counts.rows) {
    console.log(`${row.category}: ${row.c}`);
  }

  await pool.end();
}

main().catch(async (err) => {
  console.error('ERROR', err.message);
  try { await pool.end(); } catch {}
  process.exit(1);
});
