require('dotenv').config();
const { Pool } = require('pg');
(async () => {
  const p = new Pool({host:process.env.PG_HOST||'localhost',port:Number(process.env.PG_PORT||5432),database:process.env.PG_DATABASE||'geoscope',user:process.env.PG_USER||'postgres',password:process.env.PG_PASSWORD||''});
  const names = [
    'FEDERAL FACILITY','HAZWASTE','RCRA','VIOLATIONS NPDES','NPDES','SDWA','ICIS','AIR','TRI','STORMWATER','GHG','FEDERAL','TRIS','ECHO','UST','EPA UST'
  ];
  const r = await p.query(`
    SELECT database_name,
           COUNT(*) AS records,
           COUNT(DISTINCT state) FILTER (WHERE state IS NOT NULL AND TRIM(state) <> '') AS states,
           COUNT(DISTINCT city) FILTER (WHERE city IS NOT NULL AND TRIM(city) <> '') AS cities
    FROM environmental_sites
    WHERE database_name = ANY($1)
    GROUP BY database_name
    ORDER BY states DESC, cities DESC, records DESC
  `, [names]);
  console.log(JSON.stringify(r.rows, null, 2));
  await p.end();
})();
