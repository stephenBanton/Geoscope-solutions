require('dotenv').config();
const {Pool} = require('pg');
const p = new Pool({ host:process.env.PG_HOST, port:+process.env.PG_PORT, database:process.env.PG_DATABASE, user:process.env.PG_USER, password:process.env.PG_PASSWORD });

async function run() {
  const dbs = "database_name IN ('UST','EPA UST','EPA LUST')";

  // UST records (attributes=null, DC addresses) - set state+city directly
  let r2 = await p.query(
    "UPDATE environmental_sites SET state='DC', city='Washington' WHERE database_name='UST' AND (state IS NULL OR state='')"
  );
  console.log('Plain UST state+city fixed:', r2.rowCount);

  // DC LUST records — set state=DC, city=Washington, populate address+zip from JSONB
  let r = await p.query(
    `UPDATE environmental_sites
     SET state='DC', city='Washington',
         zip=COALESCE(NULLIF(TRIM(attributes->>'ZIPCODE'),''), zip),
         address=COALESCE(NULLIF(TRIM(attributes->>'SITE_ADDRESS'),''), address)
     WHERE ${dbs}
       AND (state IS NULL OR state='')
       AND (attributes->>'ZIPCODE' IS NOT NULL OR attributes->>'SITE_ADDRESS' IS NOT NULL)`
  );
  console.log('DC LUST state+city fixed:', r.rowCount);

  // Also fix zip for rows missing it
  r = await p.query(
    `UPDATE environmental_sites
     SET zip=TRIM(attributes->>'ZIPCODE')
     WHERE ${dbs}
       AND (zip IS NULL OR zip='')
       AND attributes->>'ZIPCODE' IS NOT NULL
       AND TRIM(attributes->>'ZIPCODE') <> ''`
  );
  console.log('UST zip fixed:', r.rowCount);

  // Fix address from SITE_ADDRESS where missing
  r = await p.query(
    `UPDATE environmental_sites
     SET address=TRIM(attributes->>'SITE_ADDRESS')
     WHERE ${dbs}
       AND (address IS NULL OR address='')
       AND attributes->>'SITE_ADDRESS' IS NOT NULL
       AND TRIM(attributes->>'SITE_ADDRESS') <> ''`
  );
  console.log('UST address fixed:', r.rowCount);

  // Verify
  const v = await p.query(
    `SELECT database_name, COUNT(*) as total,
       COUNT(state) FILTER(WHERE state<>'') as with_state,
       COUNT(city) FILTER(WHERE city<>'') as with_city,
       COUNT(zip) FILTER(WHERE zip<>'') as with_zip
     FROM environmental_sites
     WHERE database_name IN ('UST','EPA UST','EPA LUST')
     GROUP BY database_name`
  );
  console.log('\nUST/LUST coverage after fix:');
  v.rows.forEach(x => console.log(`  ${x.database_name}: ${x.total} total | state: ${x.with_state} | city: ${x.with_city} | zip: ${x.with_zip}`));
  p.end();
}
run().catch(e => { console.error(e.message); p.end(); });
