#!/usr/bin/env node
// =============================================================================
// GeoScope Bulk Import Runner — imports all available datasets to Supabase
// Run: node scripts/run-all-imports.js
// =============================================================================

require('dotenv').config({ path: require('path').join(__dirname, '../.env') });
const { execFileSync, spawn } = require('child_process');
const path = require('path');
const fs   = require('fs');

const CONN = process.env.DATABASE_URL ||
  'postgresql://postgres.imvcveoynxkceupggnnw:Mombasad3780%2A@aws-1-eu-west-1.pooler.supabase.com:5432/postgres';

// Each entry: [csvPath, dbName, category, classCode, priorityTier]
// Paths relative to geoscope project root
const IMPORTS = [
  // Already done or small — included here for idempotency (ON CONFLICT DO NOTHING)
  ['downloads/missing/LUST/lust_from_hdrive.csv',           'LUST',             'contamination', 'LUST',    'HIGH'],
  ['downloads/missing/UST/ust_from_hdrive.csv',             'UST',              'contamination', 'UST',     'HIGH'],
  ['downloads/missing/TRI/tri_from_hdrive.csv',             'TRI',              'contamination', 'TRI',     'HIGH'],
  ['downloads/missing/NPL_DB2/npl_db2.csv',                 'NPL',              'contamination', 'NPL',     'HIGH'],

  // Medium datasets
  ['downloads/missing/ECHO_TRI/echo_tri.csv',               'ECHO TRI',         'contamination', 'TRI',     'HIGH'],
  ['downloads/missing/ECHO_GHG/echo_ghg.csv',               'ECHO GHG',         'regulatory',    'GHG',     'MEDIUM'],
  ['downloads/missing/ECHO_FEDERAL/echo_federal.csv',       'ECHO FEDERAL',     'regulatory',    'ECHO',    'MEDIUM'],
  ['downloads/missing/ECHO_AIR/echo_air_major.csv',         'ECHO AIR MAJOR',   'regulatory',    'AIR',     'MEDIUM'],
  ['downloads/missing/ECHO_NPDES/echo_npdes_major.csv',     'ECHO NPDES MAJOR', 'regulatory',    'NPDES',   'MEDIUM'],

  // Large datasets
  ['downloads/missing/ECHO_AIR/echo_air_all.csv',           'ECHO AIR',         'regulatory',    'AIR',     'MEDIUM'],
  ['downloads/missing/ECHO_SDWA/echo_sdwa.csv',             'ECHO SDWA',        'regulatory',    'SDWA',    'MEDIUM'],
  ['downloads/missing/HAZWASTE_HANDLERS/hd_reporting_0.csv','HAZWASTE',         'contamination', 'HAZWASTE','HIGH'],
  ['downloads/missing/HAZWASTE_HANDLERS/hd_reporting_1.csv','HAZWASTE',         'contamination', 'HAZWASTE','HIGH'],
  ['downloads/missing/ECHO_NPDES/echo_npdes_all.csv',       'ECHO NPDES',       'regulatory',    'NPDES',   'MEDIUM'],

  // Infrastructure / Receptors
  ['downloads/missing/HIFLD_FIRE/fire.csv',                 'FIRE STATIONS',    'receptors',     'FIRE',    'LOW'],
  ['downloads/missing/HIFLD_POLICE/police.csv',             'POLICE STATIONS',  'receptors',     'POLICE',  'LOW'],
  ['downloads/missing/HIFLD_HOSPITALS/hospitals.csv',       'HOSPITALS',        'receptors',     'HOSP',    'LOW'],
  ['downloads/missing/NURSING_HOMES/cms_nh_facilities.csv', 'NURSING HOMES',    'receptors',     'SNF',     'LOW'],
  ['downloads/missing/NURSING_HOMES/cms_nh_test.csv',       'NURSING HOMES',    'receptors',     'SNF',     'LOW'],

  // Mines
  ['downloads/missing/MINES/msha_coal_mines.csv',           'COAL MINES',       'geology',       'MINE',    'MEDIUM'],
  ['downloads/missing/MINES/msha_metal_mines.csv',          'METAL MINES',      'geology',       'MINE',    'MEDIUM'],
  ['downloads/missing/MINES/msha_nonmetal_mines.csv',       'NONMETAL MINES',   'geology',       'MINE',    'MEDIUM'],

  // Schools
  ['downloads/missing/SCHOOLS_PUBLIC/schools_public_austin.csv', 'SCHOOLS PUBLIC', 'receptors', 'SCHOOL', 'LOW'],

  // USGS
  ['downloads/missing/USGS_GAUGES/usgs_gauges.csv',         'USGS GAUGES',      'hydrology',     'USGS',    'LOW'],

  // Derived
  ['downloads/missing/DERIVED/statsgo.csv',                 'STATSGO SOILS',    'geology',       'STATSGO', 'LOW'],
  ['downloads/missing/DERIVED/flood_dfirm.csv',             'FLOOD DFIRM',      'hydrology',     'FLOOD',   'MEDIUM'],
  ['downloads/missing/DERIVED/flood_q3.csv',                'FLOOD Q3',         'hydrology',     'FLOOD',   'MEDIUM'],
];

// Extra paths (check both possible locations)
const EXTRA_PATHS = [
  ['downloads/extra/cms_hospitals.csv',         'CMS HOSPITALS',    'receptors',  'CMS_HOSP', 'LOW'],
  ['downloads/extra/usgs_earthquakes.csv',      'USGS EARTHQUAKES', 'geology',    'EQ',       'MEDIUM'],
  // exports folder (try both possible paths)
  ['downloads/missing/exports/rmp.csv',         'RMP',              'contamination','RMP',    'HIGH'],
  ['downloads/exports/rmp.csv',                 'RMP',              'contamination','RMP',    'HIGH'],
  ['downloads/missing/exports/corracts.csv',    'CORRACTS',         'contamination','CORR',   'HIGH'],
  ['downloads/exports/corracts.csv',            'CORRACTS',         'contamination','CORR',   'HIGH'],
  ['downloads/missing/exports/npl.csv',         'NPL',              'contamination','NPL',    'HIGH'],
  ['downloads/exports/npl.csv',                 'NPL',              'contamination','NPL',    'HIGH'],
  ['downloads/missing/exports/tsca_pcb.csv',    'TSCA PCB',         'contamination','TSCA',   'HIGH'],
  ['downloads/exports/tsca_pcb.csv',            'TSCA PCB',         'contamination','TSCA',   'HIGH'],
  ['downloads/missing/exports/pfas_npl.csv',    'PFAS NPL',         'contamination','PFAS',   'HIGH'],
  ['downloads/exports/pfas_npl.csv',            'PFAS NPL',         'contamination','PFAS',   'HIGH'],
  ['downloads/missing/exports/rcra_tsdf.csv',   'RCRA TSDF',        'contamination','RCRA',   'HIGH'],
  ['downloads/exports/rcra_tsdf.csv',           'RCRA TSDF',        'contamination','RCRA',   'HIGH'],
];

// Deduplicate extra paths (first existing path wins)
const seenDbNames = new Set();
for (const entry of EXTRA_PATHS) {
  const [csvPath, dbName] = entry;
  if (!seenDbNames.has(dbName) && fs.existsSync(path.join(__dirname, '..', csvPath))) {
    IMPORTS.push(entry);
    seenDbNames.add(dbName);
  }
}

const ROOT = path.join(__dirname, '..');
const FAST_IMPORT = path.join(__dirname, 'fast-import.js');

async function runImport(entry) {
  const [csvPath, dbName, category, classCode, priority] = entry;
  const fullPath = path.join(ROOT, csvPath);
  if (!fs.existsSync(fullPath)) {
    console.log(`\n⏭  Skipping (not found): ${csvPath}`);
    return;
  }
  const sizeMB = (fs.statSync(fullPath).size / 1048576).toFixed(1);
  console.log(`\n${'─'.repeat(70)}`);
  console.log(`▶  ${dbName} (${sizeMB}MB) → ${csvPath}`);

  return new Promise((resolve) => {
    const child = spawn(process.execPath, [FAST_IMPORT, fullPath, dbName, category, classCode || '', priority || 'standard'], {
      cwd: ROOT,
      env: { ...process.env, DATABASE_URL: CONN },
      stdio: 'inherit',
    });
    child.on('close', (code) => {
      if (code !== 0) console.error(`  ⚠️  Exited with code ${code}`);
      resolve();
    });
  });
}

async function main() {
  console.log('='.repeat(70));
  console.log('GeoScope Bulk Import — all available datasets → Supabase');
  console.log(`DATABASE_URL: ${CONN.replace(/:[^:@]*@/, ':***@')}`);
  console.log(`Total datasets: ${IMPORTS.length}`);
  console.log('='.repeat(70));

  // Skip ECHO RCRA (already running / just finished)
  const toRun = IMPORTS.filter(([p]) => !p.includes('echo_rcra'));

  for (const entry of toRun) {
    await runImport(entry);
  }

  console.log('\n' + '='.repeat(70));
  console.log('✅  All imports complete!');
}

main().catch(e => { console.error('Fatal:', e.message); process.exit(1); });
