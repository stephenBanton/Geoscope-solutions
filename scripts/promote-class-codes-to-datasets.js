require('dotenv').config({ path: require('path').join(__dirname, '../.env') });
const { Pool } = require('pg');
const pool = new Pool({host:process.env.PG_HOST,port:process.env.PG_PORT,database:process.env.PG_DATABASE,user:process.env.PG_USER,password:process.env.PG_PASSWORD,max:1});

const classMappings = [
  ['NPDES','NPDES_FACILITY','NPDES_FACILITY'],
  ['SDWA Public Water System','PUBLIC_WATER_SYSTEM_TNCWS','PUBLIC_WATER_SYSTEM_TNCWS'],
  ['ICIS-Air','CAA_STATIONARY_SOURCE','CAA_STATIONARY_SOURCE'],
  ['ICIS-Air','ICIS-AIR','ICIS-AIR'],
  ['ICIS FE&C','AIR_EMISSIONS_FACILITY','AIR_EMISSIONS_FACILITY'],
  ['ICIS FE&C','ENFORCEMENT_FACILITY','ENFORCEMENT_FACILITY'],
  ['HIFLD Child Care','CHILD_CARE','CHILD_CARE'],
  ['HIFLD Child Care','CCARE','CCARE'],
  ['SCHOOLS PUBLIC','PUBLIC_SCHOOL','PUBLIC_SCHOOL'],
  ['SDWA Public Water System','PUBLIC_WATER_SYSTEM_CWS','PUBLIC_WATER_SYSTEM_CWS'],
  ['SDWA Public Water System','PUBLIC_WATER_SYSTEM_NTNCWS','PUBLIC_WATER_SYSTEM_NTNCWS'],
  ['MSHA MINES','MINES','MINES'],
  ['USGS Gauges Prefixed','USGS_GAUGE_PFX','USGS_GAUGE_PFX'],
  ['SCHOOLS PRIVATE','PRIVATE_SCHOOL','PRIVATE_SCHOOL'],
  ['FEMA Flood DFIRM','FLQ3','FLQ3'],
  ['FEMA Flood DFIRM','DFIRM','DFIRM'],
  ['CORRACTS','CORRA','CORRA'],
  ['CMS Nursing Homes','NH','NH'],
  ['CMS Nursing Homes','CMS_NURSING_HOME','CMS_NURSING_HOME'],
  ['UST','UST_FACILITY','UST_FACILITY'],
  ['EPA UST','UST_FACILITY','UST_FACILITY'],
  ['HIFLD Hospitals','HIFLD_HOSPITAL_PFX','HIFLD_HOSPITAL_PFX'],
  ['HIFLD Hospitals','HIFLD_HOSPITAL','HIFLD_HOSPITAL'],
  ['CMS Nursing Homes','NURSING_HOME','NURSING_HOME'],
  ['RCRA TSDF','TSDF','TSDF'],
  ['SCHOOLS PUBLIC','SCPUB','SCPUB'],
  ['CERCLIS','CERCLIS_NPL_PFX','CERCLIS_NPL_PFX'],
  ['CERCLIS','NPL2','NPL2'],
  ['PFAS NPL','PFAS','PFAS'],
  ['SCHOOLS PRIVATE','SCPRV','SCPRV'],
  ['MSHA MINES','MSHA_MINE','MSHA_MINE'],
  ['EPA LUST','LUST_SITE','LUST_SITE'],
  ['DOD','BRAC','BRAC'],
  ['DOD','DODP','DODP'],
  ['EPA PFAS Federal Sites','PFASF','PFASF']
];

const regionDefs = [
  ['RCRA_N_REGION_1',['CT','ME','MA','NH','RI','VT']],
  ['RCRA_N_REGION_2',['NJ','NY','PR','VI']],
  ['RCRA_N_REGION_3',['DE','DC','MD','PA','VA','WV']],
  ['RCRA_N_REGION_4',['AL','FL','GA','KY','MS','NC','SC','TN']],
  ['RCRA_N_REGION_5',['IL','IN','MI','MN','OH','WI']],
  ['RCRA_N_REGION_6',['AR','LA','NM','OK','TX']],
  ['RCRA_N_REGION_7',['IA','KS','MO','NE']],
  ['RCRA_N_REGION_8',['CO','MT','ND','SD','UT','WY']],
  ['RCRA_N_REGION_9',['AZ','CA','HI','NV','AS','GU','MP']],
  ['RCRA_N_REGION_10',['AK','ID','OR','WA']]
];

(async()=>{
  let totalUpdated = 0;

  // Check existing buckets to skip already-done ones
  const existing = await pool.query(`select distinct database_name from environmental_sites`);
  const existingSet = new Set(existing.rows.map(r=>r.database_name));

  console.log('=== CLASS-CODE PROMOTIONS ===');
  for (const [fromDb, cls, newDb] of classMappings) {
    if (existingSet.has(newDb)) {
      console.log(`SKIP ${newDb} (already exists as bucket)`);
      continue;
    }
    const r = await pool.query(
      `UPDATE environmental_sites SET database_name=$1 WHERE database_name=$2 AND class_code=$3`,
      [newDb, fromDb, cls]
    );
    console.log(`  ${fromDb} [${cls}] -> ${newDb}: ${r.rowCount} rows`);
    totalUpdated += r.rowCount;
    existingSet.add(newDb);
  }

  console.log('\n=== RCRA N REGIONAL SPLITS ===');
  for (const [regionName, states] of regionDefs) {
    if (existingSet.has(regionName)) {
      console.log(`SKIP ${regionName} (already exists)`);
      continue;
    }
    const r = await pool.query(
      `UPDATE environmental_sites SET database_name=$1 WHERE database_name='RCRA N' AND state = ANY($2::text[])`,
      [regionName, states]
    );
    console.log(`  RCRA N -> ${regionName} (${states.join(',')}): ${r.rowCount} rows`);
    totalUpdated += r.rowCount;
    existingSet.add(regionName);
  }

  const after = await pool.query(`select count(distinct database_name)::int as buckets from environmental_sites`);
  console.log(`\n=== RESULT ===`);
  console.log(`Total rows relabeled: ${totalUpdated}`);
  console.log(`Final bucket count: ${after.rows[0].buckets}`);

  await pool.end();
})().catch(async e=>{console.error(e); try{await pool.end()}catch{} process.exit(1);});
