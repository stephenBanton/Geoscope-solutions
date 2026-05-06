require('dotenv').config();
const {Pool} = require('pg');
const p = new Pool({ host:process.env.PG_HOST, port:+process.env.PG_PORT, database:process.env.PG_DATABASE, user:process.env.PG_USER, password:process.env.PG_PASSWORD });

async function run() {
  // DC LUST shapefile - set state=DC, set city=Washington, fix address from SITE_ADDRESS, zip from ZIPCODE
  let r = await p.query(UPDATE environmental_sites SET state='DC', city='Washington', zip=COALESCE(NULLIF(TRIM(attributes->>'ZIPCODE'),''), zip), address=COALESCE(NULLIF(TRIM(attributes->>'SITE_ADDRESS'),''), address) WHERE database_name IN ('UST','EPA UST','EPA LUST') AND (state IS NULL OR state='') AND attributes->>'ZIPCODE' IS NOT NULL);
  console.log('DC LUST rows fixed:', r.rowCount);
  
  // Also fix zip for all UST rows missing it
  r = await p.query(UPDATE environmental_sites SET zip=TRIM(attributes->>'ZIPCODE') WHERE database_name IN ('UST','EPA UST','EPA LUST') AND (zip IS NULL OR zip='') AND attributes->>'ZIPCODE' IS NOT NULL AND TRIM(attributes->>'ZIPCODE') <> '');
  console.log('UST zip fixed:', r.rowCount);
  
  // Update address from SITE_ADDRESS where missing
  r = await p.query(UPDATE environmental_sites SET address=TRIM(attributes->>'SITE_ADDRESS') WHERE database_name IN ('UST','EPA UST','EPA LUST') AND (address IS NULL OR address='') AND attributes->>'SITE_ADDRESS' IS NOT NULL AND TRIM(attributes->>'SITE_ADDRESS') <> '');
  console.log('UST address fixed:', r.rowCount);
  
  // Verify
  const v = await p.query(SELECT database_name, COUNT(*) as total, COUNT(state) FILTER(WHERE state<>'') as with_state, COUNT(city) FILTER(WHERE city<>'') as with_city FROM environmental_sites WHERE database_name IN ('UST','EPA UST','EPA LUST') GROUP BY database_name);
  console.log('\nUST/LUST coverage after fix:');
  v.rows.forEach(x => console.log(  :  total,  with state,  with city));
  
  p.end();
}
run().catch(e => { console.error(e.message); p.end(); });
