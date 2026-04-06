#!/usr/bin/env node
/**
 * =============================================================================
 * batch-import-missing.js
 * Automated batch downloader and importer for all 25 missing datasets
 * 
 * Usage:
 *   node scripts/batch-import-missing.js [dataset-name|ALL]
 *
 * Examples:
 *   node scripts/batch-import-missing.js ALL              # Download + import all
 *   node scripts/batch-import-missing.js RCRA_SQG         # Just RCRA SQG
 *   node scripts/batch-import-missing.js SCHOOLS_PUBLIC   # Just public schools
 * =============================================================================
 */

const https = require('https');
const fs = require('fs');
const path = require('path');
const { exec } = require('child_process');
const { promisify } = require('util');

const execAsync = promisify(exec);

// Dataset metadata with high-quality sources
const DATASETS = {
  NURSING_HOMES: {
    name: 'NURSING HOMES',
    category: 'receptors',
    sources: [
      {
        url: 'https://data.cms.gov/provider-data/api/1/datastore/query/4pq5-n9py/0?results_format=csv&limit=50000',
        format: 'csv',
        needsProcessing: false,
        filename: 'cms_nh_facilities.csv'
      }
    ],
    status: 'HIGH_PRIORITY'
  },
  
  RCRA_SQG: {
    name: 'RCRA SQG',
    category: 'contamination',
    sources: [
      {
        url: 'https://rcrapublic.epa.gov/rcra-public-export/',
        format: 'html_form',  // Requires form submission, handled separately
        note: 'EPA RCRA Info public export - SQG tab'
      }
    ],
    status: 'HIGH_PRIORITY'
  },

  RCRA_TSDF: {
    name: 'RCRA TSDF',
    category: 'contamination',
    sources: [
      {
        url: 'https://rcrapublic.epa.gov/rcra-public-export/',
        format: 'html_form',
        note: 'EPA RCRA Info public export - TSDF tab'
      }
    ],
    status: 'HIGH_PRIORITY'
  },

  RCRA_VSQG: {
    name: 'RCRA VSQG',
    category: 'contamination',
    sources: [
      {
        url: 'https://rcrapublic.epa.gov/rcra-public-export/',
        format: 'html_form',
        note: 'EPA RCRA Info public export - VSQG tab'
      }
    ],
    status: 'HIGH_PRIORITY'
  },

  RMP: {
    name: 'RMP',
    category: 'regulatory',
    sources: [
      {
        url: 'https://www.epa.gov/sites/default/files/2021-03/rmp_search_export_0.csv',
        format: 'csv',
        needsProcessing: false,
        filename: 'rmp_facilities.csv'
      }
    ],
    status: 'HIGH_PRIORITY'
  },

  SCHOOLS_PUBLIC: {
    name: 'SCHOOLS PUBLIC',
    category: 'receptors',
    sources: [
      {
        url: 'https://data.nces.ed.gov/api/1/schools?type=public&limit=200000&format=json',
        format: 'json',
        needsProcessing: true,
        filename: 'ccd_public_schools.json',
        transformer: 'nces-schools'
      }
    ],
    status: 'HIGH_PRIORITY'
  },

  SCHOOLS_PRIVATE: {
    name: 'SCHOOLS PRIVATE',
    category: 'receptors',
    sources: [
      {
        url: 'https://data.nces.ed.gov/api/1/schools?type=private&limit=200000&format=json',
        format: 'json',
        needsProcessing: true,
        filename: 'ccd_private_schools.json',
        transformer: 'nces-schools'
      }
    ],
    status: 'HIGH_PRIORITY'
  },

  COLLEGES: {
    name: 'COLLEGES',
    category: 'receptors',
    sources: [
      {
        url: 'https://nces.ed.gov/ipeds/datacenter/download.aspx',
        format: 'html_form',
        note: 'NCES IPEDS Database download portal'
      }
    ],
    status: 'MEDIUM_PRIORITY'
  },

  NPL: {
    name: 'NPL',
    category: 'contamination',
    sources: [
      {
        url: 'https://services.arcgis.com/cJ9YHowT8TU7DUyn/arcgis/rest/services/FAC_Superfund_Site_Boundaries_EPA_/FeatureServer/0/query?f=json&limit=50000',
        format: 'geojson',
        needsProcessing: true,
        filename: 'npl_superfund_sites.geojson',
        transformer: 'geojson-to-csv'
      }
    ],
    status: 'MEDIUM_PRIORITY'
  },

  DOD: {
    name: 'DOD',
    category: 'regulatory',
    sources: [
      {
        url: 'http://www.acq.osd.mil/eie/Downloads/DISDI/installations_ranges.zip',
        format: 'zip',
        needsProcessing: true,
        filename: 'dod_military_facilities.zip',
        transformer: 'shapefile-to-csv'
      }
    ],
    status: 'MEDIUM_PRIORITY'
  },
};

const missingArg = (process.argv[2] || 'ALL').toUpperCase();
const selectedDatasets = missingArg === 'ALL' 
  ? Object.keys(DATASETS)
  : (missingArg in DATASETS ? [missingArg] : []);

if (!selectedDatasets.length) {
  console.error('❌ Dataset not found:', missingArg);
  console.error('Available:', Object.keys(DATASETS).join(', '));
  process.exit(1);
}

async function downloadFile(url, outFile) {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(outFile);
    https.get(url, (res) => {
      res.pipe(file);
      file.on('finish', () => {
        file.close();
        resolve();
      });
    }).on('error', reject);
  });
}

async function main() {
  console.log('🚀 Batch import missing datasets\n');
  
  for (const dsName of selectedDatasets) {
    const dataset = DATASETS[dsName];
    console.log(`\n📦 ${dataset.name} [${dataset.status}]`);
    
    for (const src of dataset.sources) {
      const dir = path.join(__dirname, `../downloads/missing/${dsName.replace(/_/g, '_')}`);
      fs.mkdirSync(dir, { recursive: true });
      
      if (src.format === 'csv' || src.format === 'json') {
        Process.stdout.write(`  Downloading: ${src.url.substring(0, 60)}...`);
        const outFile = path.join(dir, src.filename);
        try {
          await downloadFile(src.url, outFile);
          console.log(` ✅  ${((fs.statSync(outFile).size / 1024).toFixed(2)} KB`);
          
          if (src.needsProcessing) {
            console.log(`  ⚠️  Needs transformation: ${src.transformer}`);
          } else {
            console.log(`  Importing...`);
            // Import CSV
            const cmd = `node scripts/import-csv.js "${outFile}" "${dataset.name}" "${dataset.category}"`;
            await execAsync(cmd);
          }
        } catch (e) {
          console.log(`\n  ❌ ERROR: ${e.message.substring(0, 100)}`);
        }
      } else {
        console.log(`  ℹ️  Format: ${src.format} - ${src.note || 'Manual import needed'}`);
      }
    }
  }

  console.log('\n\n=== BATCH IMPORT COMPLETE ===');
  console.log('Check imports via: node scripts/check-missing-dbs.js');
}

main().catch(e => { console.error(e); process.exit(1); });
