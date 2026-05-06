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
    { name: 'Gulf Coast Petroleum Terminal', database: 'EPA UST', lat: 29.7614, lng: -95.3708, distance: '0.07 mi', status: 'Active', address: '1018 Travis St, Houston, TX 77002', epa_id: 'TX0002345678', frs_id: '110006789001', risk_level: 'High', regulatory_program: 'Underground Storage Tanks', last_updated: '2025-11-14', tank_count: 4, substance: 'Petroleum / Gasoline', handler_id: null },
    { name: 'Houston Ship Channel Industrial Complex', database: 'RCRA LQG', lat: 29.762, lng: -95.372, distance: '0.12 mi', status: 'Open - Active Violations', address: '1044 Rusk St, Houston, TX 77002', epa_id: 'TXD000123456', frs_id: '110007654321', risk_level: 'High', handler_id: 'TXD000123456', regulatory_program: 'Hazardous Waste Generation & Management', last_updated: '2025-09-30', violation_count: 3 },
    { name: 'Downtown Houston Dry Cleaning Solvent Release', database: 'LUST', lat: 29.759, lng: -95.369, distance: '0.18 mi', status: 'Leaking - Open Case', address: '920 Main St, Houston, TX 77002', epa_id: null, frs_id: '110009988776', lust_id: 'TX-LUST-20190445', risk_level: 'High', regulatory_program: 'Leaking Underground Storage Tank', last_updated: '2026-01-10', contaminants: 'PCE, TCE, cis-1,2-DCE' },
    { name: 'Harris County Refinery Overflow Site', database: 'CERCLIS / SHWS', lat: 29.763, lng: -95.371, distance: '0.22 mi', status: 'Active Remediation', address: '1102 McKinney St, Houston, TX 77002', epa_id: 'TXN000556612', frs_id: '110003344556', cerclis_id: 'TXN000556612', risk_level: 'High', regulatory_program: 'CERCLA Removal Action', last_updated: '2025-07-22', remedy_type: 'Soil Excavation & Groundwater Pump-and-Treat' },
    { name: 'Houston Metro Transit Bus Yard', database: 'EPA UST', lat: 29.758, lng: -95.368, distance: '0.25 mi', status: 'Closed - NFA Issued', address: '806 Dallas St, Houston, TX 77002', epa_id: 'TX0009876543', frs_id: '110005566001', risk_level: 'Low', regulatory_program: 'Underground Storage Tanks', last_updated: '2023-05-01', tank_count: 2, substance: 'Diesel Fuel', closure_date: '2023-04-28' },
    { name: 'TRI Facility — Formaldehyde Release', database: 'EJ Toxic Release (TRI)', lat: 29.764, lng: -95.373, distance: '0.28 mi', status: 'Reported Release', address: '1200 Lamar St, Houston, TX 77002', tri_id: '77002FRMLD1200L', frs_id: '110002233445', risk_level: 'Moderate', regulatory_program: 'Toxic Release Inventory (EPCRA §313)', last_updated: '2025-01-15', chemical: 'Formaldehyde (HCHO)', release_lbs_air: 1840, release_lbs_water: 0 },
    { name: 'San Jacinto PFAS Contamination Study Area', database: 'PFAS', lat: 29.757, lng: -95.367, distance: '0.32 mi', status: 'Under EPA Review', address: '710 Travis St, Houston, TX 77002', epa_id: null, frs_id: '110008877660', risk_level: 'High', regulatory_program: 'PFAS Contamination Investigation', last_updated: '2026-02-28', pfas_compounds: 'PFOA, PFOS, PFHxS', media_affected: 'Groundwater, Surface Water' },
    { name: 'Downtown Brownfield Redevelopment Parcel', database: 'BROWNFIELDS', lat: 29.756, lng: -95.369, distance: '0.38 mi', status: 'Assessment Phase', address: '620 Milam St, Houston, TX 77002', epa_id: 'BF-TX-2021-0034', frs_id: null, risk_level: 'Moderate', regulatory_program: 'EPA Brownfields Assessment Grant', last_updated: '2025-06-15', grant_amount: '$500,000', phase: 'Phase II ESA Underway' },
    { name: 'Houston Chemical Warehouse', database: 'RCRA SQG', lat: 29.765, lng: -95.374, distance: '0.42 mi', status: 'Active', address: '1310 Capitol St, Houston, TX 77002', epa_id: 'TXD000987654', frs_id: '110001122334', handler_id: 'TXD000987654', risk_level: 'Moderate', regulatory_program: 'Small Quantity Hazardous Waste Generator', last_updated: '2025-10-20', waste_codes: 'F001, F003, D018' },
    { name: 'Bayou City Industrial Waste Facility', database: 'HMIRS (DOT)', lat: 29.755, lng: -95.371, distance: '0.45 mi', status: 'Historical Spill on Record', address: '540 Congress Ave, Houston, TX 77002', epa_id: null, frs_id: '110007788900', hmirs_id: 'HM-TX-199908-0221', risk_level: 'Moderate', regulatory_program: 'Hazardous Materials Incident Reporting (DOT)', last_updated: '1999-08-14', spill_material: 'Xylene', spill_qty_gallons: 400 },
    { name: 'Harris County Port Storage', database: 'ECHO', lat: 29.766, lng: -95.375, distance: '0.48 mi', status: 'Active Enforcement', address: '1400 Fannin St, Houston, TX 77002', epa_id: 'TX0000654321', frs_id: '110009900112', npdes_id: 'TX0065072', risk_level: 'High', regulatory_program: 'NPDES Industrial Stormwater Permit', last_updated: '2025-12-01', penalty_amount: '$45,000', inspection_date: '2025-11-08' },
    { name: 'Houston NPL Site — Former Smelter', database: 'NPL (National Priorities List)', lat: 29.754, lng: -95.366, distance: '0.5 mi', status: 'Superfund Active', address: '450 Rusk St, Houston, TX 77002', epa_id: 'TXD980628684', frs_id: '110004455667', sems_id: 'TXD980628684', risk_level: 'High', regulatory_program: 'Superfund (CERCLA) National Priorities List', last_updated: '2025-08-19', contaminants: 'Lead, Arsenic, Cadmium', remedy_status: 'Remedial Design Phase', npl_listing_date: '1983-09-08' },
    { name: 'Midtown Houston Auto Body Shop Release', database: 'LUST', lat: 29.752, lng: -95.374, distance: '0.52 mi', status: 'Closed - Residual Contamination', address: '320 Gray St, Houston, TX 77002', lust_id: 'TX-LUST-20120188', frs_id: null, risk_level: 'Low', regulatory_program: 'Leaking Underground Storage Tank', last_updated: '2020-03-11', contaminants: 'Benzene, MTBE', closure_type: 'Risk-Based Closure' },
    { name: 'Houston Industrial Canal TSCA PCB Disposal', database: 'TSCA', lat: 29.770, lng: -95.380, distance: '0.57 mi', status: 'Restricted Use', address: '1800 Clinton Dr, Houston, TX 77003', epa_id: 'TSCA-TX-0099-PCB', frs_id: '110006677889', risk_level: 'High', regulatory_program: 'Toxic Substances Control Act — PCB Disposal Site', last_updated: '2024-04-05', contaminants: 'PCBs (Aroclor 1254, 1260)', institutional_control: 'Deed Restriction — No Residential Use' },
    { name: 'East Downtown Houston Air Emission Source', database: 'AIRS / AFS (Air Quality)', lat: 29.748, lng: -95.362, distance: '0.61 mi', status: 'Active Permit', address: '225 Chartres St, Houston, TX 77002', epa_id: 'TX-AIR-20040128', frs_id: '110008899001', air_facility_id: 'TX-AFS-0044821', risk_level: 'Moderate', regulatory_program: 'Clean Air Act Title V Operating Permit', last_updated: '2025-03-17', pollutants: 'NOx, SO2, PM2.5, VOCs' },
    { name: 'Allen Parkway Drycleaner Solvent Plume', database: 'LUST', lat: 29.775, lng: -95.385, distance: '0.72 mi', status: 'Active Monitoring', address: '3901 Allen Pkwy, Houston, TX 77019', lust_id: 'TX-LUST-20070311', frs_id: null, risk_level: 'Moderate', regulatory_program: 'Leaking Underground Storage Tank', last_updated: '2025-09-14', contaminants: 'PCE, TCE', monitoring_wells: 6 },
    { name: 'Port of Houston SPCC Regulated Tank Farm', database: 'SPCC (Oil Spill)', lat: 29.767, lng: -95.376, distance: '0.82 mi', status: 'Active SPCC Plan', address: '1510 Jacintoport Blvd, Houston, TX 77029', epa_id: 'SPCC-TX-2018-0073', frs_id: '110005500221', risk_level: 'Moderate', regulatory_program: 'Spill Prevention, Control, Countermeasure (SPCC)', last_updated: '2024-10-22', capacity_gallons: 2800000, product: 'Crude Oil / Refined Products' },
    { name: 'Midtown Dry Cleaner — Groundwater Plume', database: 'RCRA Corrective Action', lat: 29.751, lng: -95.375, distance: '0.9 mi', status: 'Under Corrective Action', address: '400 Gray St, Houston, TX 77002', epa_id: 'TXD000441827', frs_id: '110002211330', handler_id: 'TXD000441827', risk_level: 'High', regulatory_program: 'RCRA Corrective Action (SWMU)', last_updated: '2025-11-02', corrective_action_units: 3, contaminants: 'Tetrachloroethylene (PCE), Trichloroethylene (TCE)' },
    { name: 'Harris County Landfill — Former Open Dump', database: 'SOLID WASTE / SWIS', lat: 29.780, lng: -95.390, distance: '0.95 mi', status: 'Closed — Post-Closure Monitoring', address: '5200 Navigation Blvd, Houston, TX 77011', epa_id: null, frs_id: '110001199002', swis_id: 'TX-SW-1988-0031', risk_level: 'Moderate', regulatory_program: 'Solid Waste Information System', last_updated: '2022-07-30', landfill_type: 'Former Municipal Solid Waste', gas_monitoring: 'Active Methane Collection System' },
    { name: 'Houston Waterway Industrial Effluent Source', database: 'NPDES / TPDES', lat: 29.745, lng: -95.358, distance: '1.02 mi', status: 'Active Permit', address: '100 Navigation Blvd, Houston, TX 77011', npdes_id: 'TX0072834', frs_id: '110003344009', risk_level: 'Moderate', regulatory_program: 'Texas Pollutant Discharge Elimination System (TPDES)', last_updated: '2025-08-01', receiving_body: 'Buffalo Bayou', discharge_type: 'Process Wastewater + Stormwater' },
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
    summary: 'GeoScope environmental screening identified 20 regulated facilities within the 1-mile buffer, including NPL Superfund, PFAS, RCRA LQG, active LUST, TSCA PCB disposal, HMIRS spill records, NPDES, SPCC, and TRI toxic release records. Two flood zones (AE and AO special flood hazard areas) were mapped. Two sensitive receptors (schools) are located within the buffer. The subject property faces elevated cumulative risk from petroleum hydrocarbon, solvent (PCE/TCE/MTBE), PFAS, heavy metal (Lead, Arsenic, Cadmium), and PCB contamination pathways. A Phase II ESA with targeted soil/groundwater sampling is strongly recommended prior to acquisition. Dominant concerns: High-risk Superfund NPL site 0.5 mi NE, active RCRA LQG violations 0.12 mi N, open LUST solvent plume 0.18 mi E, and PFAS groundwater investigation 0.32 mi SE.',
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
