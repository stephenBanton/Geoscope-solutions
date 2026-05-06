require('dotenv').config();
const {Pool} = require('pg');
const p = new Pool({ host:process.env.PG_HOST, port:+process.env.PG_PORT, database:process.env.PG_DATABASE, user:process.env.PG_USER, password:process.env.PG_PASSWORD });
p.query("SELECT attributes FROM environmental_sites WHERE database_name IN ('UST','EPA UST','EPA LUST') AND attributes IS NOT NULL AND (state IS NULL OR state='') LIMIT 3")
  .then(r => {
    if (r.rows.length === 0) { console.log('No UST rows with NULL state - checking all UST rows'); }
    r.rows.forEach(x => console.log(JSON.stringify(x.attributes)));
    return p.query("SELECT attributes FROM environmental_sites WHERE database_name IN ('UST','EPA UST','EPA LUST') AND attributes IS NOT NULL LIMIT 3");
  })
  .then(r => { r.rows.forEach(x => console.log('SAMPLE:', JSON.stringify(x.attributes))); p.end(); })
  .catch(e => { console.error(e.message); p.end(); });
