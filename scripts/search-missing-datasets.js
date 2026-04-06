#!/usr/bin/env node
// =============================================================================
// search-missing-datasets.js
// Search catalog.data.gov and data.worldbank.org for missing datasets
// =============================================================================

const https = require('https');

const MISSING_DATASETS = [
  { name: 'COAL ASH EPA', terms: ['coal', 'ash', 'epa', 'power', 'generation'] },
  { name: 'COLLEGES', terms: ['colleges', 'universities', 'higher', 'education', 'ipeds'] },
  { name: 'CORRACTS', terms: ['corracts', 'corrective', 'action', 'rcra'] },
  { name: 'DELISTED NPL', terms: ['delisted', 'npl', 'superfund', 'remediation'] },
  { name: 'DOD', terms: ['dod', 'military', 'facilities', 'defense', 'bases'] },
  { name: 'FLOOD DFIRM', terms: ['flood', 'dfirm', 'fema', 'hazard'] },
  { name: 'FLOOD Q3', terms: ['flood', 'q3', '100-year', 'floodplain'] },
  { name: 'FUDS', terms: ['fuds', 'formerly', 'used', 'defense', 'sites'] },
  { name: 'HYDROLOGIC UNIT', terms: ['hydrologic', 'unit', 'watershed', 'basin', 'huc'] },
  { name: 'MGP', terms: ['mgp', 'gas', 'plant', 'manufactured'] },
  { name: 'NURSING HOMES', terms: ['nursing', 'homes', 'snf', 'long-term', 'care', 'cms'] },
  { name: 'PCB TRANSFORMER', terms: ['pcb', 'transformer', 'electrical', 'hazard'] },
  { name: 'PFAS FEDERAL SITES', terms: ['pfas', 'federal', 'sites', 'contamination'] },
  { name: 'PFAS NPL', terms: ['pfas', 'npl', 'superfund', 'forever'] },
  { name: 'PFAS SPILLS', terms: ['pfas', 'spills', 'releases', 'incidents'] },
  { name: 'PFAS TRIS', terms: ['pfas', 'tris', 'toxics', 'release', 'inventory'] },
  { name: 'PROPOSED NPL', terms: ['proposed', 'npl', 'superfund', 'candidates'] },
  { name: 'RADON EPA', terms: ['radon', 'epa', 'indoor', 'air', 'quality'] },
  { name: 'RCRA SQG', terms: ['rcra', 'sqg', 'small', 'quantity', 'generator'] },
  { name: 'RCRA TSDF', terms: ['rcra', 'tsdf', 'treatment', 'storage', 'disposal'] },
  { name: 'RCRA VSQG', terms: ['rcra', 'vsqg', 'very', 'small', 'quantity'] },
  { name: 'RMP', terms: ['rmp', 'risk', 'management', 'plan', 'chemical'] },
  { name: 'SCHOOLS PRIVATE', terms: ['schools', 'private', 'education', 'nces'] },
  { name: 'SCHOOLS PUBLIC', terms: ['schools', 'public', 'public', 'education'] },
  { name: 'STATSGO', terms: ['statsgo', 'soil', 'survey', 'usda', 'geographic'] },
];

function httpsGet(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { timeout: 15000 }, res => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); }
        catch (e) { reject(e); }
      });
    }).on('error', reject).on('timeout', () => reject(new Error('timeout')));
  });
}

async function searchDataGov(terms) {
  const q = terms.slice(0, 3).join(' OR ');
  const url = `https://catalog.data.gov/api/3/action/package_search?q=${encodeURIComponent(q)}&rows=20`;
  try {
    const result = await httpsGet(url);
    return result.result?.results || [];
  } catch (e) {
    return [];
  }
}

async function searchWorldBank(terms) {
  // World Bank API is limited, try a basic search
  const q = terms[0];
  const url = `https://data.worldbank.org/api/v2/country?format=json`;
  try {
    const result = await httpsGet(url);
    return result || [];
  } catch (e) {
    return [];
  }
}

async function main() {
  console.log('=============================================================================');
  console.log('Searching for missing datasets on catalog.data.gov and data.worldbank.org');
  console.log('=============================================================================\n');

  for (const dataset of MISSING_DATASETS) {
    process.stdout.write(`[${dataset.name.padEnd(20)}] Searching...`);
    const govResults = await searchDataGov(dataset.terms);
    process.stdout.write(` [data.gov: ${govResults.length} results]`);
    
    if (govResults.length > 0) {
      console.log('\n  ✅ Found on data.gov:');
      govResults.slice(0, 3).forEach((pkg, i) => {
        const url = pkg.resources?.[0]?.url || pkg.resources?.[0]?.download_url || '';
        const fmt = pkg.resources?.[0]?.format || 'unknown';
        console.log(`    ${i+1}. ${pkg.title}`);
        if (url) console.log(`       URL: ${url.substring(0, 100)}${url.length > 100 ? '...' : ''}`);
        if (fmt) console.log(`       Format: ${fmt}`);
      });
    } else {
      console.log(' ❌ No results');
    }
    console.log();
  }

  console.log('\n=== Search Summary ===');
  console.log('Check the URLs above for downloadable datasets.');
  console.log('Most EPA data is available as CSV or shapefile via data.gov.');
}

main().catch(e => { console.error(e); process.exit(1); });
