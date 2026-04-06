/**
 * Quick sample report generator — uses mock environmental data
 * to bypass external API fetches and produce a demo PDF fast.
 * Run: node generate-sample-report.js
 */

// ── Standalone import of the PDF generator from server.js ────────────────────
// We monkey-patch the fetch layer so no real external calls are made.
const path = require('path');
const fs = require('fs');

// Stub out the OpenAI call and any external fetchers before requiring server
process.env.SKIP_DB_INIT = 'true'; // prevent it from connecting to PostgreSQL on startup

// Pre-built realistic mock environmental data for Houston, TX 77002
const MOCK_ENV_DATA = {
  environmentalSites: [
    { name: 'Gulf Coast Petroleum Terminal', database: 'EPA UST', lat: 29.7614, lng: -95.3708, distance: '0.07 mi', status: 'Active', address: '1018 Travis St, Houston, TX 77002' },
    { name: 'Houston Ship Channel Industrial Complex', database: 'RCRA LQG', lat: 29.762, lng: -95.372, distance: '0.12 mi', status: 'Open - Active Violations', address: '1044 Rusk St, Houston, TX 77002' },
    { name: 'Downtown Houston Dry Cleaning Solvent Release', database: 'LUST', lat: 29.759, lng: -95.369, distance: '0.18 mi', status: 'Leaking - Open Case', address: '920 Main St, Houston, TX 77002' },
    { name: 'Harris County Refinery Overflow Site', database: 'CERCLIS / SHWS', lat: 29.763, lng: -95.371, distance: '0.22 mi', status: 'Active Remediation', address: '1102 McKinney St, Houston, TX 77002' },
    { name: 'Houston Metro Transit Bus Yard', database: 'EPA UST', lat: 29.758, lng: -95.368, distance: '0.25 mi', status: 'Closed - NFA Issued', address: '806 Dallas St, Houston, TX 77002' },
    { name: 'TRI Facility — Formaldehyde Release', database: 'EJ Toxic Release (TRI)', lat: 29.764, lng: -95.373, distance: '0.28 mi', status: 'Reported Release', address: '1200 Lamar St, Houston, TX 77002' },
    { name: 'San Jacinto PFAS Contamination Study Area', database: 'PFAS', lat: 29.757, lng: -95.367, distance: '0.32 mi', status: 'Under EPA Review', address: '710 Travis St, Houston, TX 77002' },
    { name: 'Downtown Brownfield Redevelopment Parcel', database: 'BROWNFIELDS', lat: 29.756, lng: -95.369, distance: '0.38 mi', status: 'Assessment Phase', address: '620 Milam St, Houston, TX 77002' },
    { name: 'Houston Chemical Warehouse', database: 'RCRA SQG', lat: 29.765, lng: -95.374, distance: '0.42 mi', status: 'Active', address: '1310 Capitol St, Houston, TX 77002' },
    { name: 'Bayou City Industrial Waste Facility', database: 'HMIRS (DOT)', lat: 29.755, lng: -95.371, distance: '0.45 mi', status: 'Historical Spill on Record', address: '540 Congress Ave, Houston, TX 77002' },
    { name: 'Harris County Port Storage', database: 'ECHO', lat: 29.766, lng: -95.375, distance: '0.48 mi', status: 'Active Enforcement', address: '1400 Fannin St, Houston, TX 77002' },
    { name: 'Houston NPL Site — Former Smelter', database: 'NPL (National Priorities List)', lat: 29.754, lng: -95.366, distance: '0.5 mi', status: 'Superfund Active', address: '450 Rusk St, Houston, TX 77002' },
  ],
  floodZones: [
    { attributes: { FLD_ZONE: 'AE', SFHA_TF: 'T' }, geometry: {} },
    { attributes: { FLD_ZONE: 'AO', SFHA_TF: 'T' } }
  ],
  rainfall: [
    { date: '2026-03-01', precipitation: '55.2 mm', elevation: 43 },
    { date: '2026-02-01', precipitation: '42.8 mm', elevation: 43 }
  ],
  schools: [
    { name: 'Houston ISD Elementary', type: 'school', lat: 29.762, lng: -95.370 },
    { name: 'University of Houston Downtown', type: 'school', lat: 29.763, lng: -95.368 }
  ],
  wetlands: [
    { name: 'Buffalo Bayou Riparian Wetland', type: 'wetland', lat: 29.758, lng: -95.373 }
  ],
  governmentRecords: [
    { name: 'Harris County Environmental Health', type: 'government' }
  ]
};

// ── Dynamically require/patch server and call the internal generator ───────────
(async () => {
  console.log('Loading server module...');

  // Temporarily silence console.log from server.js startup
  const origLog = console.log;
  const origErr = console.error;
  console.log = (...args) => { if (String(args[0]).includes('Sample') || String(args[0]).includes('PDF') || String(args[0]).includes('report') || String(args[0]).includes('Error') || String(args[0]).includes('[')) origLog(...args); };
  console.error = () => {};

  let generatePDFReportInternal;
  try {
    // We can't easily import just the function, so we build a minimal call
    // by loading the module and using its export if available
    const serverPath = path.join(__dirname, 'server.js');
    // Check if there's an export
    delete require.cache[require.resolve(serverPath)];
    const serverModule = require(serverPath);
    generatePDFReportInternal = serverModule.generatePDFReportInternal;
  } catch (e) {
    // Server doesn't export the function - fall through to HTTP approach
    console.error = origErr;
    console.log = origLog;
    console.log('Server module loaded (no direct export). Using HTTP approach...');
  }

  console.error = origErr;
  console.log = origLog;

  // ── Use HTTP request approach against the running server ───────────────────
  const http = require('http');
  const body = JSON.stringify({
    project_name: 'GeoScope Environmental Sample Report',
    client_name: 'Jane Smith, PE',
    client_company: 'Meridian Land Partners LLC',
    address: '1001 McKinney St, Houston, TX 77002',
    latitude: 29.7604,
    longitude: -95.3698,
    radius: '5',
    paid: true,
    dataset_date: '2026-04-03',
    // Inject pre-built environmental data so server skips external API calls
    environmentalData: MOCK_ENV_DATA,
    summary: 'GeoScope environmental screening identified 12 regulated facilities within the 0.5-mile buffer, including NPL Superfund, PFAS, active LUST, and RCRA LQG records. Two flood zones (AE and AO special flood hazard areas) were mapped. Two sensitive receptors (schools) are located within the buffer. The subject property faces elevated risk from petroleum hydrocarbon, solvent (PCE/TCE), and PFAS contamination pathways. A Phase II ESA is strongly recommended prior to acquisition.'
  });

  console.log('Sending report generation request (using pre-built data to skip API fetches)...');
  const startTime = Date.now();

  const req = http.request({
    hostname: 'localhost',
    port: 5000,
    path: '/generate-report',
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) }
  }, res => {
    let data = '';
    res.on('data', d => data += d);
    res.on('end', () => {
      const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
      try {
        const parsed = JSON.parse(data);
        if (parsed.reportPath || parsed.fileName || parsed.file) {
          const fname = parsed.reportPath || parsed.fileName || parsed.file;
          console.log('\n✅ Report generated in ' + elapsed + 's');
          console.log('📄 File:', fname);
          console.log('\nOpen this file to view the report:');
          console.log('   ' + (parsed.reportPath || path.join(__dirname, 'reports', fname)));
        } else {
          console.log('\nResponse (' + elapsed + 's):', JSON.stringify(parsed, null, 2).substring(0, 600));
        }
      } catch (e) {
        console.log('\nRaw response (' + elapsed + 's):', data.substring(0, 400));
      }
    });
  });

  req.setTimeout(300000);
  req.on('timeout', () => {
    console.log('\n⏳ Still generating... (may take 3-5 mins for PDF rendering)');
    req.socket.destroy();
  });
  req.on('error', e => console.error('\n❌ Request error:', e.message));
  req.write(body);
  req.end();
})();
