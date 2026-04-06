#!/usr/bin/env node
/**
 * =============================================================================
 * import-epa-frs-bulk.js
 * Automatically extracts EPA FRS ZIP and imports multiple datasets
 * Mapping: identifies CSV files and matches to database categories
 * =============================================================================
 */

const fs = require('fs');
const path = require('path');
const { execSync, spawnSync } = require('child_process');

const BASE_MISSING = path.join(__dirname, '../downloads/missing');
const FRS_DIR = path.join(BASE_MISSING, 'EPA_FRS_BULK');

// Map CSV patterns to database names and categories
const FRS_MAPPINGS = [
  {
    pattern: /npl|superfund/i,
    dbname: 'NPL',
    category: 'contamination',
    priority: 'HIGH'
  },
  {
    pattern: /delisted/i,
    dbname: 'DELISTED NPL',
    category: 'contamination',
    priority: 'HIGH'
  },
  {
    pattern: /proposed|candidate/i,
    dbname: 'PROPOSED NPL',
    category: 'contamination',
    priority: 'HIGH'
  },
  {
    pattern: /rmp|risk.*management/i,
    dbname: 'RMP',
    category: 'regulatory',
    priority: 'HIGH'
  },
  {
    pattern: /pfas/i,
    dbname: 'PFAS',
    category: 'contamination',
    priority: 'MEDIUM'
  },
  {
    pattern: /corrects|corrective/i,
    dbname: 'CORRACTS',
    category: 'contamination',
    priority: 'MEDIUM'
  },
  {
    pattern: /fuds|formerly.*used.*defense/i,
    dbname: 'FUDS',
    category: 'regulatory',
    priority: 'MEDIUM'
  }
];

async function findCsvFiles() {
  console.log(`🔍 Scanning for CSV files in ${FRS_DIR}...\n`);

  const csvFiles = [];
  const searchDir = (dir, relativePath = '') => {
    if (!fs.existsSync(dir)) return;
    
    const entries = fs.readdirSync(dir);
    for (const entry of entries) {
      const fullPath = path.join(dir, entry);
      const rel = path.join(relativePath, entry);
      
      if (fs.statSync(fullPath).isDirectory()) {
        searchDir(fullPath, rel);
      } else if (entry.toLowerCase().endsWith('.csv')) {
        csvFiles.push({
          path: fullPath,
          name: entry,
          relative: rel,
          size: fs.statSync(fullPath).size
        });
      }
    }
  };

  searchDir(FRS_DIR);
  return csvFiles;
}

async function matchCsvToDatabase(csvFile) {
  // Try to match filename to a known database
  let bestMatch = null;
  let bestScore = 0;

  for (const mapping of FRS_MAPPINGS) {
    if (mapping.pattern.test(csvFile.name)) {
      if (mapping.priority === 'HIGH' && bestScore < 2) {
        bestMatch = mapping;
        bestScore = 2;
      } else if (mapping.priority === 'MEDIUM' && bestScore < 1) {
        bestMatch = mapping;
        bestScore = 1;
      }
    }
  }

  // Check file contents for better matching
  if (!bestMatch) {
    console.log(`    ⚠️  Checking file contents for ${csvFile.name}...`);
    try {
      const head = execSync(`powershell -NoProfile -Command "Get-Content '${csvFile.path}' -TotalCount 2"`, 
        { encoding: 'utf8', timeout: 5000 }).toLowerCase();
      
      for (const mapping of FRS_MAPPINGS) {
        if (mapping.pattern.test(head)) {
          bestMatch = mapping;
          break;
        }
      }
    } catch (e) {
      // Ignore errors reading file
    }
  }

  return bestMatch;
}

async function importCsv(csvFile, mapping) {
  console.log(`\n📥 Importing: ${path.basename(csvFile)}`);
  console.log(`   Database: ${mapping.dbname}`);
  console.log(`   Category: ${mapping.category}`);
  console.log(`   Size: ${(csvFile.size / 1024 / 1024).toFixed(2)}MB`);

  try {
    const cmd = `node scripts/import-csv.js "${csvFile.path}" "${mapping.dbname}" "${mapping.category}"`;
    process.stdout.write(`   Running import...`);
    
    const result = spawnSync('cmd', ['/c', cmd], {
      cwd: path.join(__dirname, '..'),
      encoding: 'utf8',
      timeout: 600000   // 10 min timeout per file
    });

    if (result.status === 0) {
      console.log(` ✅\n`);
      return true;
    } else {
      console.log(` ❌\n   Error: ${result.stderr || result.stdout}`);
      return false;
    }
  } catch (e) {
    console.log(` ❌\n   ${e.message}`);
    return false;
  }
}

async function main() {
  console.log('=============================================================================');
  console.log('📦 EPA FRS BULK IMPORT');
  console.log('=============================================================================\n');

  // Find CSV files
  const csvFiles = await findCsvFiles();
  
  if (csvFiles.length === 0) {
    console.log(`❌ No CSV files found in ${FRS_DIR}`);
    console.log(`   Make sure EPA FRS ZIP has been extracted`);
    process.exit(1);
  }

  console.log(`Found ${csvFiles.length} CSV file(s):\n`);
  csvFiles.forEach(f => {
    console.log(`  - ${f.relative} (${(f.size / 1024 / 1024).toFixed(2)}MB)`);
  });

  // Sort by size (smallest first to get quick wins)
  csvFiles.sort((a, b) => a.size - b.size);

  console.log('\n=============================================================================');
  console.log('🔍 MATCHING & IMPORTING');
  console.log('=============================================================================\n');

  let imported = 0;
  let skipped = 0;

  for (const csv of csvFiles) {
    // Skip if too large (probably won't fit in memory for quick processing)
    if (csv.size > 500000000) {
      console.log(`⚠️  SKIPPING (too large): ${csv.name}`);
      console.log(`   Size: ${(csv.size / 1024 / 1024).toFixed(2)}MB`);
      skipped++;
      continue;
    }

    const mapping = await matchCsvToDatabase(csv);
    if (mapping) {
      const success = await importCsv(csv.path, mapping);
      if (success) imported++;
    } else {
      console.log(`⚠️  SKIPPING (unknown): ${csv.name}`);
      skipped++;
    }
  }

  console.log('\n=============================================================================');
  console.log('📋 SUMMARY');
  console.log('=============================================================================\n');
  console.log(`✅ Imported: ${imported}`);
  console.log(`⏭️  Skipped: ${skipped}`);
  console.log(`\nCheck progress: node scripts/check-missing-dbs.js`);
}

main().catch(e => { console.error(e); process.exit(1); });
