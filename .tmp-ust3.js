require('dotenv').config();
const {Pool} = require('pg');
const p = new Pool({ host:process.env.PG_HOST, port:+process.env.PG_PORT, database:process.env.PG_DATABASE, user:process.env.PG_USER, password:process.env.PG_PASSWORD });
p.query("SELECT attributes, state, city, address, zip FROM environmental_sites WHERE database_name='UST' LIMIT 3")
  .then(r => { r.rows.forEach(x => console.log(JSON.stringify(x))); p.end(); })
  .catch(e => { console.error(e.message); p.end(); });
