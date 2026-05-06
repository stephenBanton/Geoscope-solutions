require('dotenv').config();
const { Pool } = require('pg');
(async () => {
  const p = new Pool({host:process.env.PG_HOST||'localhost',port:Number(process.env.PG_PORT||5432),database:process.env.PG_DATABASE||'geoscope',user:process.env.PG_USER||'postgres',password:process.env.PG_PASSWORD||''});
  try {
    const a = await p.query("SELECT COUNT(DISTINCT database_name) AS datasets FROM environmental_sites WHERE database_name IS NOT NULL AND TRIM(database_name)<>''");
    console.log('datasets=' + a.rows[0].datasets);
  } catch(e){ console.log('ERR ' + e.message); }
  await p.end();
})();
