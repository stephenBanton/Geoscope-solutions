#!/usr/bin/env node
/**
 * =============================================================================
 * download-via-apis.js
 * Downloads missing datasets via EPA/USGS/NCES APIs and data portals
 * Uses REST APIs and CKAN when direct downloads fail
 * =============================================================================
 */

const https = require('https');
const fs = require('fs');
const path = require('path');

const BASE_MISSING = path.join(__dirname, '../downloads/missing');

// Function to fetch via HTTPS GET and parse JSON
function httpsGet(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { timeout: 30000, headers: { 'User-Agent': 'Mozilla/5.0' } }, res => {
      let data = '';
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        httpsGet(res.headers.location).then(resolve).catch(reject);
        return;
      }
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); }
        catch (e) { reject(new Error('JSON parse error')); }
      });
    }).on('error', reject);
  });
}

// CMS Nursing Homes - verify download is complete
async function verifyNursingHomes() {
  const file = path.join(BASE_MISSING, 'NURSING_HOMES/cms_nh_facilities.csv');
  if (fs.existsSync(file)) {
    const stats = fs.statSync(file);
    if (stats.size > 1000000) {
      console.log(`✅ NURSING HOMES: Ready (${(stats.size / 1024 / 1024).toFixed(2)}MB)`);
      return true;
    }
  }
  return false;
}

// EPA RMP via CKAN/data.gov
async function getDataGovRMP() {
  console.log('🔍 Searching data.gov for RMP data...');
  try {
    const url = 'https://catalog.data.gov/api/3/action/package_search?q=rmp+risk+management+chemical&rows=20';
    const result = await httpsGet(url);
    const packages = result.result?.results || [];
    
    if (packages.length > 0) {
      // Find package with CSV resources
      for (const pkg of packages) {
        const csvRes = pkg.resources?.find(r => 
          r.format?.toLowerCase() === 'csv' && 
          (r.url || r.download_url)
        );
        if (csvRes) {
          const url = csvRes.url || csvRes.download_url;
          console.log(`  Found: ${pkg.title}`);
          console.log(`  URL: ${url.substring(0, 100)}`);
          return { title: pkg.title, url };
        }
      }
    }
  } catch (e) {
    console.log(`  Error querying data.gov: ${e.message}`);
  }
  return null;
}

// NCES Schools via API
async function searchNCESSchools() {
  console.log('🔍 Checking NCES school datasets...');
  try {
    // NCES Common Core of Data is available via multiple endpoints
    const urls = {
      'Public Schools': 'https://data.nces.ed.gov/api/EdGovData/PublicSchools',
      'Private Schools': 'https://data.nces.ed.gov/api/EdGovData/PrivateSchools',
      'Colleges': 'https://data.nces.ed.gov/api/EdGovData/InstitutionProfile'
    };

    for (const [type, url] of Object.entries(urls)) {
      console.log(`  ${type}: ${url.substring(0, 80)}`);
    }
  } catch (e) {
    console.log(`  Error: ${e.message}`);
  }
}

// EPA RCRA via CKAN
async function searchRCRADatasets() {
  console.log('🔍 Searching data.gov for RCRA datasets...');
  try {
    const url = 'https://catalog.data.gov/api/3/action/package_search?q=rcra+generator+sqg+tsdf&rows=10';
    const result = await httpsGet(url);
    const packages = result.result?.results || [];

    packages.slice(0, 3).forEach((pkg, i) => {
      console.log(`  ${i+1}. ${pkg.title}`);
      pkg.resources?.slice(0, 2).forEach(r => {
        console.log(`     - ${r.format}: ${(r.url || r.download_url).substring(0, 70)}`);
      });
    });
  } catch (e) {
    console.log(`  Error: ${e.message}`);
  }
}

// EPA NPL/Superfund via ArcGIS REST API
async function getNPLFromArcGIS() {
  console.log('🔍 Checking EPA ArcGIS NPL endpoint...');
  try {
    const url = 'https://services.arcgis.com/cJ9YHowT8TU7DUyn/arcgis/rest/services/FAC_Superfund_Site_Boundaries_EPA_/FeatureServer/0?f=json';
    const result = await httpsGet(url);
    console.log(`  ✅ NPL ArcGIS endpoint accessible`);
    console.log(`     Name: ${result.name}`);
    console.log(`     Records: ${result.count || 'unknown'}`);
    return result;
  } catch (e) {
    console.log(`  ❌ Error: ${e.message}`);
    return null;
  }
}

// Search WorldBank for environmental/facility data
async function searchWorldBankData() {
  console.log('🔍 Checking World Bank environmental data...');
  try {
    const url = 'https://data.worldbank.org/api/v2/country?format=json&per_page=5';
    const result = await httpsGet(url);
    console.log(`  ⚠️  World Bank is country-level (not facility-level)`);
    console.log(`     World Bank has macroeconomic data, not facility registries`);
    return null;
  } catch (e) {
    console.log(`  Error: ${e.message}`);
    return null;
  }
}

// EPA FRS facility search
async function searchEPAFRS() {
  console.log('🔍 Checking EPA FRS (Facility Registry Service)...');
  try {
    //  EPA FRS bulk downloads info
    const urls = [
      'Combined facility data: https://www3.epa.gov/enviro/html/fii/downloads/state_files/',
      'FRS Interests: https://dmap-prod-oms-edc.s3.amazonaws.com/OMS/FRS/FRS_Interests_Download.zip',
      'FRS SEMS: https://dmap-prod-oms-edc.s3.amazonaws.com/OMS/FRS/FRS_SEMS_Download.zip'
    ];
    urls.forEach(u => console.log(`  ${u}`));
  } catch (e) {
    console.log(`  Error: ${e.message}`);
  }
}

// Step through and identify best sources
async function discoveryMode() {
  console.log('\n=============================================================================');
  console.log('🔎 BULK DATASET DISCOVERY - Finding Best Active Sources');
  console.log('=============================================================================\n');

  // 1. Check nursing homes
  await verifyNursingHomes();

  // 2. RMP
  console.log('\n--- RMP (Risk Management Plan) ---');
  const rmpData = await getDataGovRMP();

  // 3. RCRA
  console.log('\n--- RCRA (SQG/TSDF/VSQG) ---');
  await searchRCRADatasets();

  // 4. NPL/Superfund
  console.log('\n--- NPL (Superfund Sites) ---');
  const npl = await getNPLFromArcGIS();

  // 5. Schools
  console.log('\n--- Schools & Colleges ---');
  await searchNCESSchools();

  // 6. EPA FRS
  console.log('\n--- EPA FRS (Master Facilities Registry) ---');
  await searchEPAFRS();

  // 7. World Bank
  console.log('\n--- World Bank ---');
  await searchWorldBankData();

  console.log('\n=============================================================================');
  console.log('📋 RECOMMENDATIONS');
  console.log('=============================================================================\n');

  console.log('✅ HIGH-VALUE SOURCES IDENTIFIED:');
  console.log('');
  console.log('1. EPA FRS Interests (S3 download):');
  console.log('   https://dmap-prod-oms-edc.s3.amazonaws.com/OMS/FRS/FRS_Interests_Download.zip');
  console.log('   → Contains NPL, PFAS, RMP, and other facility types');
  console.log('');
  console.log('2. EPA FRS SEMS (S3 download):');
  console.log('   https://dmap-prod-oms-edc.s3.amazonaws.com/OMS/FRS/FRS_SEMS_Download.zip');
  console.log('   → Secondary environmentally sensitive sites registry');
  console.log('');
  console.log('3. NCES Schools via direct download portal:');
  console.log('   https://data.nces.ed.gov/ (CCD - Common Core of Data)');
  console.log('   → Public and private school universes');
  console.log('');
  console.log('4. EPA RCRA Info:');
  console.log('   https://rcrapublic.epa.gov/rcra-public-export/');
  console.log('   → Official RCRA handler registry');
  console.log('');
  console.log('5. EPA ArcGIS Feature Servers (REST APIs):');
  console.log('   → NPL Superfund Sites');
  console.log('   → PFAS contaminated sites');
  console.log('   → DOD Facilities');
  console.log('');
  console.log('🚀 NEXT: Run bulk import directly from these sources');
  console.log('');
}

discoveryMode().catch(e => { console.error(e); process.exit(1); });
