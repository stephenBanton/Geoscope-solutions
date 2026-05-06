#!/usr/bin/env node
/**
 * batch-import.js
 * Runs import-csv-prefixed.js for each configured source in sequence.
 * Usage: node scripts/batch-import.js
 */
const { execSync } = require('child_process');
const path = require('path');

const ROOT = path.join(__dirname, '..');
const SCRIPT = path.join(__dirname, 'import-csv-prefixed.js');

// [filePath, databaseName, category, classCode, priorityTier]
const SOURCES = [
  // NPL DB2 (re-run after state fix)
  ['downloads/missing/NPL_DB2/npl_db2.csv', 'NPL DB2 Prefixed', 'Contamination', 'NPL2', 'T1'],

  // EPA FRS Bulk exports
  ['downloads/missing/EPA_FRS_BULK/exports/tsca_pcb.csv', 'EPA TSCA PCB', 'Contamination', 'TSCA', 'T1'],
  ['downloads/missing/EPA_FRS_BULK/exports/rmp.csv', 'EPA RMP Sites', 'Industrial', 'RMP', 'T1'],
  ['downloads/missing/EPA_FRS_BULK/exports/corracts.csv', 'EPA CORRACTS', 'Contamination', 'CORRA', 'T2'],
  ['downloads/missing/EPA_FRS_BULK/exports/rcra_tsdf.csv', 'EPA RCRA TSDF', 'Waste', 'TSDF', 'T1'],
  ['downloads/missing/EPA_FRS_BULK/exports/pfas_npl.csv', 'EPA PFAS NPL', 'Contamination', 'PFAS', 'T1'],
  ['downloads/missing/EPA_FRS_BULK/exports/proposed_npl.csv', 'EPA Proposed NPL', 'Contamination', 'PNPL', 'T1'],
  ['downloads/missing/EPA_FRS_BULK/exports/delisted_npl.csv', 'EPA Delisted NPL', 'Contamination', 'DNPL', 'T2'],
  ['downloads/missing/EPA_FRS_BULK/exports/mgp.csv', 'EPA Manufactured Gas Plants', 'Contamination', 'MGP', 'T1'],
  ['downloads/missing/EPA_FRS_BULK/exports/radon_epa.csv', 'EPA Radon', 'Contamination', 'RADON', 'T2'],
  ['downloads/missing/EPA_FRS_BULK/exports/coal_ash_epa.csv', 'EPA Coal Ash', 'Waste', 'COAL', 'T1'],
  ['downloads/missing/EPA_FRS_BULK/exports/dod_brac.csv', 'DoD BRAC Sites', 'Military', 'BRAC', 'T1'],
  ['downloads/missing/EPA_FRS_BULK/exports/fuds.csv', 'DoD FUDS', 'Military', 'FUDS', 'T1'],
  ['downloads/missing/EPA_FRS_BULK/exports/pfas_federal_sites.csv', 'EPA PFAS Federal Sites', 'Contamination', 'PFASF', 'T1'],
  ['downloads/missing/EPA_FRS_BULK/exports/dod_prefixed.csv', 'DoD Prefixed Sites', 'Military', 'DODP', 'T2'],

  // Mines
  ['downloads/missing/MINES/msha_metal_mines.csv', 'MSHA Metal Mines', 'Mining', 'MSHAM', 'T2'],
  ['downloads/missing/MINES/msha_nonmetal_mines.csv', 'MSHA Nonmetal Mines', 'Mining', 'MSHANM', 'T2'],
  ['downloads/missing/MINES/msha_coal_mines.csv', 'MSHA Coal Mines', 'Mining', 'MSHAC', 'T2'],

  // Health / Care
  ['downloads/missing/NURSING_HOMES/cms_nh_facilities.csv', 'CMS Nursing Homes', 'Healthcare', 'NH', 'T3'],
  ['downloads/missing/HIFLD_CHILD_CARE/child_care.csv', 'HIFLD Child Care', 'Social', 'CCARE', 'T3'],
  ['downloads/missing/PRISONS/correctional_institutions.csv', 'Correctional Institutions', 'Government', 'PRISON', 'T3'],

  // Environment
  ['downloads/missing/WETLANDS/wetland_inventory.csv', 'Wetland Inventory', 'Environment', 'WETL', 'T3'],
  ['downloads/missing/CAFO/cafo_facilities.csv', 'CAFO Facilities', 'Agriculture', 'CAFO', 'T2'],

  // Underground tanks
  ['downloads/missing/UST/ust_from_hdrive.csv', 'UST HDrive', 'Contamination', 'UST', 'T2'],
  ['downloads/missing/LUST/lust_from_hdrive.csv', 'LUST HDrive', 'Contamination', 'LUST', 'T1'],

  // TRI
  ['downloads/missing/TRI/tri_facilities.csv', 'TRI Facilities', 'Industrial', 'TRI', 'T1'],

  // RCRA subtypes
  ['downloads/missing/RCRA_GENSV/rcra_gensv.csv', 'RCRA GenSV', 'Waste', 'GENSV', 'T2'],
  ['downloads/missing/RCRA_VSQG/rcra_vsqg.csv', 'RCRA VSQG', 'Waste', 'VSQG', 'T3'],
  ['downloads/missing/RCRA_SQG/rcra_sqg.csv', 'RCRA SQG', 'Waste', 'SQG', 'T2'],
  ['downloads/missing/RCRA_LQG/rcra_lqg.csv', 'RCRA LQG', 'Waste', 'LQG', 'T1'],

  // NPDES
  ['downloads/missing/NPDES/npdes_major.csv', 'NPDES Major Dischargers', 'Water', 'NPDES', 'T1'],
  ['downloads/missing/NPDES/npdes_minor.csv', 'NPDES Minor Dischargers', 'Water', 'NPDESM', 'T2'],

  // Schools / Education
  ['downloads/missing/DERIVED/schools_public_derived.csv', 'Public Schools', 'Education', 'SCPUB', 'T3'],
  ['downloads/missing/DERIVED/schools_private_derived.csv', 'Private Schools', 'Education', 'SCPRV', 'T3'],
  ['downloads/missing/DERIVED/colleges_derived.csv', 'Colleges', 'Education', 'COLL', 'T3'],

  // Flood / FEMA
  ['downloads/missing/DERIVED/flood_dfirm.csv', 'FEMA Flood DFIRM', 'Flood', 'DFIRM', 'T2'],
  ['downloads/missing/DERIVED/flood_q3.csv', 'FEMA Flood Q3', 'Flood', 'FLQ3', 'T2'],
];

let passed = 0;
let failed = 0;

for (const [filePath, dbName, category, classCode, priorityTier] of SOURCES) {
  const absFile = path.join(ROOT, filePath);
  const label = `[${dbName}]`;
  try {
    console.log(`\n${'='.repeat(70)}`);
    console.log(`${label} Importing: ${filePath}`);
    execSync(
      `node "${SCRIPT}" "${absFile}" "${dbName}" "${category}" "${classCode}" "${priorityTier}"`,
      { cwd: ROOT, stdio: 'inherit' }
    );
    passed++;
  } catch (err) {
    console.error(`${label} FAILED: ${err.message}`);
    failed++;
  }
}

console.log(`\n${'='.repeat(70)}`);
console.log(`Batch complete. Passed: ${passed}  Failed: ${failed}`);
