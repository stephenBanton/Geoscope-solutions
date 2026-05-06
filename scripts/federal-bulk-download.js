#!/usr/bin/env node
/**
 * ============================================================================
 * GeoScope Federal Bulk Downloader & Importer
 * ============================================================================
 * Downloads authoritative federal environmental datasets and imports them
 * into the PostgreSQL `environmental_sites` table for nationwide GIS coverage.
 *
 * TARGET: 50M+ records across all 50 US states
 *
 * DATA SOURCES:
 *   1. EPA ECHO Exporter          - 1.5M+ regulated facilities (RCRA, NPDES, Air, SDWA, TRI)
 *   2. EPA FRS Download           - Facility Registry Service master list
 *   3. EPA RCRAInfo               - Hazardous waste handlers & TSDFs
 *   4. EPA ICIS-Air               - Clean Air Act stationary sources
 *   5. EPA ICIS-NPDES             - Clean Water Act permitted dischargers
 *   6. EPA Air Emissions          - NEI, GHGRP, TRI, Clean Air Markets combined
 *   7. EPA SDWA                   - Safe Drinking Water Act public water systems
 *   8. EPA ICIS FE&C              - Federal enforcement (UST, TSCA, FIFRA, CAA 112r)
 *   9. FEMA NFIP Claims           - 2.4M+ flood insurance claims (OpenFEMA API)
 *  10. FEMA NFIP Policies         - 5M+ flood insurance policies (OpenFEMA API)
 *  11. EPA TRI                    - Toxic Release Inventory (multi-year)
 *  12. EPA Superfund SEMS         - NPL/CERCLIS hazardous waste sites
 *  13. EPA PFAS                   - PFAS occurrence & testing sites
 *  14. USGS National Water Quality- Water quality monitoring stations
 *  15. EPA EJScreen               - Environmental Justice census-block data
 *
 * USAGE:
 *   node scripts/federal-bulk-download.js                    # Download & import all
 *   node scripts/federal-bulk-download.js --source echo      # Only ECHO Exporter
 *   node scripts/federal-bulk-download.js --source fema      # Only FEMA NFIP
 *   node scripts/federal-bulk-download.js --source rcra      # Only RCRA/hazardous waste
 *   node scripts/federal-bulk-download.js --source tri       # Only TRI releases
 *   node scripts/federal-bulk-download.js --source all       # All sources
 *   node scripts/federal-bulk-download.js --download-only    # Download ZIPs only
 *   node scripts/federal-bulk-download.js --import-only      # Import pre-downloaded ZIPs
 *   node scripts/federal-bulk-download.js --skip-download    # Skip existing ZIPs
 *   node scripts/federal-bulk-download.js --dry-run          # Count records, no DB insert
 *
 * ENVIRONMENT:
 *   PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD
 *   DOWNLOAD_DIR  (default: ./downloads/federal)
 *
 * ============================================================================
 */

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');
const { Pool } = require('pg');
const { parse: csvParse } = require('csv-parse');

// Try to load optional dependency gracefully
let unzipper;
try { unzipper = require('unzipper'); } catch (e) {
  console.warn('⚠ unzipper not installed. Run: npm install unzipper');
}

// ============================================================================
// CONFIGURATION
// ============================================================================

const DOWNLOAD_DIR = process.env.DOWNLOAD_DIR || path.join(__dirname, '..', 'downloads', 'federal');
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || '500');
const FEMA_PAGE_SIZE = 10000;
const MAX_FEMA_RECORDS = parseInt(process.env.MAX_FEMA_RECORDS || '5000000'); // 5M per dataset
const DEFAULT_GEOCODE_LIMIT = parseInt(process.env.GEOSCOPE_GEOCODE_LIMIT || '2000');
const GEOCODE_DELAY_MS = parseInt(process.env.GEOSCOPE_GEOCODE_DELAY_MS || '1200');

const SOURCES = {
  // ─── EPA ECHO ─────────────────────────────────────────────────────────────
  echo: {
    name: 'EPA ECHO Exporter',
    url: 'https://echo.epa.gov/files/echodownloads/echo_exporter.zip',
    sizeMb: 392,
    type: 'zip_csv',
    targetFile: /ECHO_EXPORTER/i,
    mapper: mapEchoExporter,
    category: 'contamination',
    source_org: 'EPA',
    priority_tier: 'high',
    priority_score: 85,
    description: '1.5M+ regulated facilities covering all EPA programs'
  },
  frs: {
    name: 'EPA FRS Facilities',
    url: 'https://echo.epa.gov/files/echodownloads/frs_downloads.zip',
    sizeMb: 318,
    type: 'zip_csv',
    targetFile: /FRS_FACILITIES/i,
    mapper: mapFrsFacilities,
    category: 'regulatory',
    source_org: 'EPA',
    priority_tier: 'standard',
    priority_score: 50,
    description: 'EPA Facility Registry Service - authoritative facility master list'
  },
  rcra: {
    name: 'EPA RCRAInfo',
    url: 'https://echo.epa.gov/files/echodownloads/rcra_downloads.zip',
    sizeMb: 103,
    type: 'zip_csv',
    targetFile: /RCRA_FACILITIES/i,
    mapper: mapRcraSites,
    category: 'contamination',
    source_org: 'EPA',
    priority_tier: 'high',
    priority_score: 90,
    description: 'Hazardous waste handlers, generators, TSDFs'
  },
  air: {
    name: 'EPA ICIS-Air',
    url: 'https://echo.epa.gov/files/echodownloads/ICIS-AIR_downloads.zip',
    sizeMb: 66,
    type: 'zip_csv',
    targetFile: /ICIS.AIR.FACILITIES/i,
    mapper: mapIcisAir,
    category: 'contamination',
    source_org: 'EPA',
    priority_tier: 'medium',
    priority_score: 70,
    description: 'Clean Air Act stationary sources'
  },
  air_emissions: {
    name: 'EPA Air Emissions',
    url: 'https://echo.epa.gov/files/echodownloads/POLL_RPT_COMBINED_EMISSIONS.zip',
    sizeMb: 150,
    type: 'zip_csv',
    targetFile: /POLL_RPT_COMBINED_EMISSIONS/i,
    mapper: mapAirEmissions,
    category: 'contamination',
    source_org: 'EPA',
    priority_tier: 'high',
    priority_score: 80,
    description: 'NEI, GHGRP, TRI, and Clean Air Markets combined emissions data'
  },
  npdes: {
    name: 'EPA ICIS-NPDES',
    url: 'https://echo.epa.gov/files/echodownloads/npdes_downloads.zip',
    sizeMb: 323,
    type: 'zip_csv',
    targetFile: /ICIS_NPDES_FACILITIES/i,
    mapper: mapNpdesFacilities,
    category: 'hydrology',
    source_org: 'EPA',
    priority_tier: 'high',
    priority_score: 80,
    description: 'Clean Water Act NPDES permitted dischargers'
  },
  sdwa: {
    name: 'EPA SDWA',
    url: 'https://echo.epa.gov/files/echodownloads/SDWA_latest_downloads.zip',
    sizeMb: 499,
    type: 'zip_csv',
    targetFile: /SDWA_PUB_WATER_SYSTEMS/i,
    mapper: mapSdwa,
    category: 'hydrology',
    source_org: 'EPA',
    priority_tier: 'medium',
    priority_score: 65,
    description: 'Public water systems under Safe Drinking Water Act'
  },
  enforcement: {
    name: 'EPA ICIS FE&C',
    url: 'https://echo.epa.gov/files/echodownloads/case_downloads.zip',
    sizeMb: 75,
    type: 'zip_csv',
    targetFile: /ICIS_FEC_FACILITIES/i,
    mapper: mapEnforcement,
    category: 'regulatory',
    source_org: 'EPA',
    priority_tier: 'high',
    priority_score: 90,
    description: 'Federal enforcement actions (UST, TSCA, FIFRA, CAA 112r)'
  },
  // ─── FEMA ─────────────────────────────────────────────────────────────────
  fema_claims: {
    name: 'FEMA NFIP Claims',
    url: 'https://www.fema.gov/api/open/v2/FimaNfipClaims.json',
    type: 'fema_api',
    mapper: mapFemaClaims,
    category: 'hydrology',
    source_org: 'FEMA',
    priority_tier: 'high',
    priority_score: 85,
    description: '2.4M+ flood insurance claims — flood risk indicator'
  },
  fema_policies: {
    name: 'FEMA NFIP Policies',
    url: 'https://www.fema.gov/api/open/v2/FimaNfipPolicies.json',
    type: 'fema_api',
    mapper: mapFemaPolicies,
    category: 'hydrology',
    source_org: 'FEMA',
    priority_tier: 'medium',
    priority_score: 60,
    description: '5M+ active NFIP flood insurance policies'
  },
  // ─── EPA TRI ──────────────────────────────────────────────────────────────
  tri: {
    name: 'EPA TRI Facilities',
    url: 'https://data.epa.gov/efservice/TRI_FACILITY/ROWS/0:99999/CSV',
    sizeMb: 25,
    type: 'direct_csv',
    mapper: mapTriFacility,
    category: 'contamination',
    source_org: 'EPA',
    priority_tier: 'high',
    priority_score: 88,
    description: 'Toxic Release Inventory — facilities with toxic chemical releases'
  },
  // ─── EPA Superfund ────────────────────────────────────────────────────────
  superfund: {
    name: 'EPA Superfund Sites',
    url: 'https://data.epa.gov/efservice/SEMS_SITES/ROWS/0:99999/CSV',
    sizeMb: 15,
    type: 'direct_csv',
    mapper: mapSuperfund,
    category: 'contamination',
    source_org: 'EPA',
    priority_tier: 'high',
    priority_score: 95,
    description: 'NPL and CERCLIS Superfund hazardous waste sites'
  },
  // ─── EPA Brownfields ──────────────────────────────────────────────────────
  brownfields: {
    name: 'EPA Brownfields',
    url: 'https://data.epa.gov/efservice/ACRES_CLEANUPS/ROWS/0:99999/CSV',
    sizeMb: 10,
    type: 'direct_csv',
    mapper: mapBrownfields,
    category: 'contamination',
    source_org: 'EPA',
    priority_tier: 'medium',
    priority_score: 75,
    description: 'EPA Brownfields cleanup sites and assessments'
  },
  // ─── EPA Underground Storage Tanks ────────────────────────────────────────
  ust: {
    name: 'EPA UST/LUST',
    url: 'https://data.epa.gov/efservice/RCRA_SITES/ROWS/0:999999/CSV',
    sizeMb: 30,
    type: 'direct_csv',
    mapper: mapUst,
    category: 'contamination',
    source_org: 'EPA',
    priority_tier: 'high',
    priority_score: 88,
    description: 'Underground storage tanks and leaking USTs'
  }
};

// ─── Source Groups ─────────────────────────────────────────────────────────
const SOURCE_GROUPS = {
  all:     Object.keys(SOURCES),
  echo:    ['echo', 'frs'],
  epa:     ['echo', 'frs', 'rcra', 'air', 'air_emissions', 'npdes', 'sdwa', 'enforcement', 'tri', 'superfund', 'brownfields', 'ust'],
  fema:    ['fema_claims', 'fema_policies'],
  water:   ['npdes', 'sdwa', 'fema_claims', 'fema_policies'],
  hazmat:  ['rcra', 'enforcement', 'superfund', 'ust', 'tri'],
  fast:    ['echo', 'fema_claims', 'superfund', 'tri']
};

// ============================================================================
// CLI PARSING
// ============================================================================

const argv = process.argv.slice(2);
const flags = {
  downloadOnly: argv.includes('--download-only'),
  importOnly:   argv.includes('--import-only'),
  skipDownload: argv.includes('--skip-download') || argv.includes('--import-only'),
  dryRun:       argv.includes('--dry-run'),
  autoGeocode:  !argv.includes('--no-geocode'),
  verbose:      argv.includes('--verbose') || argv.includes('-v'),
  help:         argv.includes('--help') || argv.includes('-h')
};

const geocodeLimitArg = argv.find(a => a.startsWith('--geocode-limit='));
const geocodeLimit = geocodeLimitArg ? parseInt(geocodeLimitArg.split('=')[1]) : DEFAULT_GEOCODE_LIMIT;

const sourceArg = argv.find(a => a.startsWith('--source=') || (!a.startsWith('--') && !a.startsWith('-')));
const sourceName = sourceArg ? sourceArg.replace('--source=', '') : 'all';
const selectedSources = SOURCES[sourceName] ? [sourceName] : (SOURCE_GROUPS[sourceName] || SOURCE_GROUPS.all);

if (flags.help) {
  console.log(`
GeoScope Federal Bulk Downloader

Usage: node scripts/federal-bulk-download.js [options]

Options:
  --source=<name>    Source to download (all, epa, fema, water, hazmat, fast, or specific key)
  --download-only    Only download ZIP files, don't import
  --import-only      Only import pre-downloaded files
  --skip-download    Skip downloading if file already exists
  --dry-run          Parse records but don't insert to database
  --no-geocode       Disable automatic geocoding for records with missing lat/lng
  --geocode-limit=N  Maximum number of geocode requests in one run (default: ${DEFAULT_GEOCODE_LIMIT})
  --verbose          Verbose logging
  --help             Show this help

Available sources: ${Object.keys(SOURCES).join(', ')}
Source groups:     ${Object.keys(SOURCE_GROUPS).join(', ')}

Examples:
  node scripts/federal-bulk-download.js --source echo
  node scripts/federal-bulk-download.js --source fema --import-only
  node scripts/federal-bulk-download.js --source all --dry-run
  node scripts/federal-bulk-download.js --source ust --geocode-limit=10000
  `);
  process.exit(0);
}

// ============================================================================
// DATABASE
// ============================================================================

const pgPool = new Pool({
  host:     process.env.PG_HOST     || 'localhost',
  port:     parseInt(process.env.PG_PORT || '5432'),
  database: process.env.PG_DATABASE || 'geoscope',
  user:     process.env.PG_USER     || 'postgres',
  password: process.env.PG_PASSWORD || '2030',
  max: 10
});

const geocodeState = {
  requests: 0,
  hits: 0,
  misses: 0,
  cacheHits: 0,
  cache: new Map(),
  lastRequestAt: 0
};

// ============================================================================
// COLUMN MAPPERS — normalize each source to environmental_sites schema
// ============================================================================

/**
 * Map ECHO Exporter row to multiple site records (one per active program).
 * ECHO Exporter has one row per facility with flags for each program.
 */
function mapEchoExporter(row) {
  const sites = [];
  const lat  = parseFloat(row['FAC_LAT']  || row['LATITUDE']  || '');
  const lng  = parseFloat(row['FAC_LONG'] || row['LONGITUDE'] || '');
  const hasCoords = Number.isFinite(lat) && Number.isFinite(lng);

  const base = {
    site_name:  trim(row['FAC_NAME']),
    address:    trim(row['FAC_STREET']),
    city:       trim(row['FAC_CITY']),
    state:      trim(row['FAC_STATE']),
    zip:        trim(row['FAC_ZIP']),
    status:     row['FAC_ACTIVE_FLAG'] === 'Y' ? 'Active' : 'Inactive',
    registry_id:trim(row['REGISTRY_ID']),
    source_org: 'EPA',
    lat: hasCoords ? lat : null,
    lng: hasCoords ? lng : null,
    attributes: {
      naics: row['FAC_NAICS'],
      sic:   row['FAC_SIC'],
      fips_county: row['FAC_COUNTY_NAME'],
      region: row['FAC_EPA_REGION'],
      indian_country: row['FAC_INDIAN_CNTRY_FLG']
    }
  };

  // Map each program flag to a separate site record
  const programs = [
    { flag: 'RCRA_FLAG',  db: 'RCRA',          cat: 'contamination', tier: 'high',   score: 90 },
    { flag: 'NPDES_FLAG', db: 'NPDES',          cat: 'hydrology',     tier: 'high',   score: 80 },
    { flag: 'AIR_FLAG',   db: 'ICIS-Air',       cat: 'contamination', tier: 'medium', score: 70 },
    { flag: 'SDWIS_FLAG', db: 'SDWA',           cat: 'hydrology',     tier: 'medium', score: 65 },
    { flag: 'TRI_FLAG',   db: 'TRI',            cat: 'contamination', tier: 'high',   score: 88 },
    { flag: 'SEMS_FLAG',  db: 'Superfund SEMS', cat: 'contamination', tier: 'high',   score: 95 },
    { flag: 'GHG_FLAG',   db: 'GHG Reporting',  cat: 'contamination', tier: 'medium', score: 60 },
    { flag: 'FCES_FLAG',  db: 'ICIS FE&C',      cat: 'regulatory',    tier: 'high',   score: 90 }
  ];

  let added = false;
  for (const p of programs) {
    if (row[p.flag] === 'Y') {
      sites.push({
        ...base,
        database_name:  p.db,
        category:       p.cat,
        priority_tier:  p.tier,
        priority_score: p.score,
        source_id:      `ECHO-${trim(row['REGISTRY_ID'])}-${p.db}`,
        class_code:     p.db.replace(/\s+/g, '_').toUpperCase()
      });
      added = true;
    }
  }

  // Always add at least one record per facility (as generic ECHO record)
  if (!added) {
    sites.push({
      ...base,
      database_name:  'EPA ECHO',
      category:       'regulatory',
      priority_tier:  'standard',
      priority_score: 40,
      source_id:      `ECHO-${trim(row['REGISTRY_ID'])}-GENERIC`,
      class_code:     'ECHO_FACILITY'
    });
  }

  return sites;
}

function mapFrsFacilities(row) {
  const lat = parseFloat(row['LATITUDE83']  || row['LATITUDE']  || '');
  const lng = parseFloat(row['LONGITUDE83'] || row['LONGITUDE'] || '');
  const hasCoords = Number.isFinite(lat) && Number.isFinite(lng);
  return [{
    database_name:  'EPA FRS',
    category:       'regulatory',
    site_name:      trim(row['PRIMARY_NAME'] || row['FAC_NAME']),
    address:        trim(row['LOCATION_ADDRESS'] || row['ADDRESS']),
    city:           trim(row['CITY_NAME']  || row['CITY']),
    state:          trim(row['STATE_CODE'] || row['STATE']),
    zip:            trim(row['POSTAL_CODE'] || row['ZIP']),
    status:         'Active',
    registry_id:    trim(row['REGISTRY_ID']),
    source_id:      `FRS-${trim(row['REGISTRY_ID'])}`,
    source_org:     'EPA',
    class_code:     'FRS_FACILITY',
    priority_tier:  'standard',
    priority_score: 50,
    lat: hasCoords ? lat : null,
    lng: hasCoords ? lng : null,
    attributes: {
      interest_types: row['INTEREST_TYPES'],
      env_interest:   row['PRIMARY_INTEREST_TYPE']
    }
  }];
}

function mapRcraSites(row) {
  const lat = parseFloat(row['LATITUDE83']  || row['FAC_LAT']  || '');
  const lng = parseFloat(row['LONGITUDE83'] || row['FAC_LONG'] || '');
  const hasCoords = Number.isFinite(lat) && Number.isFinite(lng);
  const id = trim(row['ID_NUMBER'] || row['HANDLER_ID'] || row['REGISTRY_ID']);
  const name = trim(row['FACILITY_NAME'] || row['FAC_NAME']);
  if (!id && !name) return [];
  const isActive = (row['ACTIVE_SITE'] || '').toUpperCase() === 'Y' || (row['ACTIVE_SITE'] || '').toUpperCase() === 'TRUE';
  const isTsdf = (row['OPERATING_TSDF'] || '').toUpperCase() === 'Y';
  const genStatus = trim(row['FED_WASTE_GENERATOR'] || row['GENERATOR_STATUS'] || '');
  const tier = genStatus === 'LQG' ? 'high' : genStatus === 'SQG' ? 'medium' : 'standard';
  const classCode = isTsdf ? 'RCRA_TSDF' : genStatus ? `RCRA_${genStatus}` : 'RCRA_HANDLER';
  return [{
    database_name:  isTsdf ? 'RCRA TSDF' : (genStatus ? `RCRA ${genStatus}` : 'RCRA Handler'),
    category:       'contamination',
    site_name:      name,
    address:        trim(row['STREET_ADDRESS'] || row['LOCATION_STREET1']),
    city:           trim(row['CITY_NAME'] || row['LOCATION_CITY']),
    state:          trim(row['STATE_CODE'] || row['LOCATION_STATE']),
    zip:            trim(row['ZIP_CODE'] || row['LOCATION_ZIP']),
    status:         isActive ? 'Active' : 'Inactive',
    registry_id:    trim(row['REGISTRY_ID']),
    source_id:      `RCRA-${id}`,
    source_org:     'EPA',
    class_code:     classCode,
    priority_score: tier === 'high' ? 90 : tier === 'medium' ? 70 : 40,
    lat: hasCoords ? lat : null,
    lng: hasCoords ? lng : null,
    attributes: {
      id_number: row['ID_NUMBER'],
      generator_status: genStatus,
      tsdf: row['OPERATING_TSDF'],
      transporter: row['TRANSPORTER'],
      full_enforcement: row['FULL_ENFORCEMENT']
    }
  }];
}

function mapIcisAir(row) {
  // ICIS-AIR_FACILITIES.csv columns: PGM_SYS_ID, REGISTRY_ID, FACILITY_NAME,
  // STREET_ADDRESS, CITY, COUNTY_NAME, STATE, ZIP_CODE — no lat/lng in this file
  const lat = parseFloat(row['FAC_LAT'] || row['LATITUDE'] || '');
  const lng = parseFloat(row['FAC_LONG'] || row['LONGITUDE'] || '');
  const hasCoords = Number.isFinite(lat) && Number.isFinite(lng);
  const id = trim(row['PGM_SYS_ID'] || row['REGISTRY_ID']);
  if (!id) return [];
  return [{
    database_name:  'ICIS-Air',
    category:       'contamination',
    site_name:      trim(row['FACILITY_NAME'] || row['FAC_NAME']),
    address:        trim(row['STREET_ADDRESS'] || row['FAC_STREET']),
    city:           trim(row['CITY'] || row['FAC_CITY']),
    state:          trim(row['STATE'] || row['FAC_STATE']),
    zip:            trim(row['ZIP_CODE'] || row['FAC_ZIP']),
    status:         row['AIR_OPERATING_STATUS_DESC'] || (row['FAC_ACTIVE_FLAG'] === 'Y' ? 'Active' : 'Inactive'),
    registry_id:    trim(row['REGISTRY_ID']),
    source_id:      `AIR-${id}`,
    source_org:     'EPA',
    class_code:     'CAA_STATIONARY_SOURCE',
    priority_tier:  'medium',
    priority_score: 70,
    lat: hasCoords ? lat : null,
    lng: hasCoords ? lng : null,
    attributes: {
      pgm_sys_id:    row['PGM_SYS_ID'],
      air_programs:  row['AIR_PROGRAMS'],
      pollutant_class: row['AIR_POLLUTANT_CLASS_DESC'],
      naics:         row['NAICS_CODES'],
      current_hpv:   row['CURRENT_HPV']
    }
  }];
}

function mapAirEmissions(row) {
  const lat = parseFloat(row['FAC_LAT'] || row['LATITUDE'] || '');
  const lng = parseFloat(row['FAC_LONG'] || row['LONGITUDE'] || '');
  const hasCoords = Number.isFinite(lat) && Number.isFinite(lng);
  const id = trim(row['REGISTRY_ID']);
  if (!id && !row['FAC_NAME']) return [];
  return [{
    database_name:  'Air Emissions',
    category:       'contamination',
    site_name:      trim(row['FAC_NAME'] || row['FACILITY_NAME']),
    address:        trim(row['FAC_STREET'] || row['STREET_ADDRESS']),
    city:           trim(row['FAC_CITY'] || row['CITY']),
    state:          trim(row['FAC_STATE'] || row['STATE']),
    zip:            trim(row['FAC_ZIP'] || row['ZIP_CODE']),
    status:         'Active',
    registry_id:    id || null,
    source_id:      `AIREMIS-${id || Math.random().toString(36).slice(2)}`,
    source_org:     'EPA',
    class_code:     'AIR_EMISSIONS_FACILITY',
    priority_tier:  'high',
    priority_score: 80,
    lat: hasCoords ? lat : null,
    lng: hasCoords ? lng : null,
    attributes: {
      pollutant: row['POLLUTANT_NAME'],
      total_emissions: row['TOTAL_EMISSIONS'],
      unit: row['UNIT_OF_MEASURE'],
      program: row['PROGRAM_CODE']
    }
  }];
}

function mapNpdesFacilities(row) {
  const lat = parseFloat(row['FAC_LAT'] || row['LATITUDE'] || '');
  const lng = parseFloat(row['FAC_LONG'] || row['LONGITUDE'] || '');
  const hasCoords = Number.isFinite(lat) && Number.isFinite(lng);
  const id = trim(row['NPDES_ID'] || row['REGISTRY_ID'] || row['EXTERNAL_PERMIT_NMBR']);
  if (!id && !row['FAC_NAME']) return [];
  return [{
    database_name:  'NPDES',
    category:       'hydrology',
    site_name:      trim(row['FAC_NAME'] || row['FACILITY_NAME']),
    address:        trim(row['FAC_STREET'] || row['LOCATION_ADDRESS']),
    city:           trim(row['FAC_CITY'] || row['CITY_NAME']),
    state:          trim(row['FAC_STATE'] || row['STATE_CODE']),
    zip:            trim(row['FAC_ZIP'] || row['ZIP_CODE']),
    status:         row['FAC_ACTIVE_FLAG'] === 'Y' || row['PERMIT_STATUS_CODE'] === 'EFF' ? 'Active' : 'Inactive',
    registry_id:    trim(row['REGISTRY_ID']),
    source_id:      `NPDES-${id || Math.random().toString(36).slice(2)}`,
    source_org:     'EPA',
    class_code:     'NPDES_' + (row['FACILITY_TYPE_INDICATOR'] || row['PERMIT_TYPE_CODE'] || 'FACILITY'),
    priority_tier:  'high',
    priority_score: 80,
    lat: hasCoords ? lat : null,
    lng: hasCoords ? lng : null,
    attributes: {
      npdes_id: row['NPDES_ID'] || row['EXTERNAL_PERMIT_NMBR'],
      permit_type: row['PERMIT_TYPE'] || row['PERMIT_TYPE_CODE'],
      major_minor: row['MAJOR_MINOR_FLAG'],
      receiving_water: row['RECEIVING_WATER_NAME']
    }
  }];
}

function mapSdwa(row) {
  const lat = parseFloat(row['LATITUDE'] || row['FAC_LAT'] || '');
  const lng = parseFloat(row['LONGITUDE'] || row['FAC_LONG'] || '');
  const hasCoords = Number.isFinite(lat) && Number.isFinite(lng);
  const id = trim(row['PWSID'] || row['REGISTRY_ID']);
  if (!id) return [];
  return [{
    database_name:  'SDWA Public Water System',
    category:       'hydrology',
    site_name:      trim(row['PWS_NAME'] || row['FAC_NAME']),
    address:        trim(row['ADDRESS_LINE1'] || row['LOCATION_ADDRESS']),
    city:           trim(row['CITY_NAME'] || row['FAC_CITY']),
    state:          trim(row['PRIMACY_AGENCY_CODE'] || row['STATE_CODE'] || row['FAC_STATE']),
    zip:            trim(row['ZIP_CODE'] || row['FAC_ZIP']),
    status:         row['PWS_ACTIVITY_CODE'] === 'A' ? 'Active' : 'Inactive',
    registry_id:    trim(row['REGISTRY_ID']),
    source_id:      `SDWA-${id}`,
    source_org:     'EPA',
    class_code:     'PUBLIC_WATER_SYSTEM_' + (row['PWS_TYPE_CODE'] || 'CWS'),
    priority_tier:  'medium',
    priority_score: 65,
    lat: hasCoords ? lat : null,
    lng: hasCoords ? lng : null,
    attributes: {
      pwsid: row['PWSID'],
      pws_type: row['PWS_TYPE_CODE'],
      population_served: row['POPULATION_SERVED_COUNT'],
      source_water: row['PRIMARY_SOURCE_CODE']
    }
  }];
}

function mapEnforcement(row) {
  const lat = parseFloat(row['FAC_LAT'] || row['LATITUDE'] || '');
  const lng = parseFloat(row['FAC_LONG'] || row['LONGITUDE'] || '');
  const hasCoords = Number.isFinite(lat) && Number.isFinite(lng);
  const id = trim(row['REGISTRY_ID'] || row['ACTIVITY_ID']);
  if (!id && !row['FAC_NAME'] && !row['FACILITY_NAME']) return [];
  return [{
    database_name:  'ICIS FE&C',
    category:       'regulatory',
    site_name:      trim(row['FAC_NAME'] || row['FACILITY_NAME']),
    address:        trim(row['FAC_STREET'] || row['LOCATION_ADDRESS']),
    city:           trim(row['FAC_CITY'] || row['CITY_NAME']),
    state:          trim(row['FAC_STATE'] || row['STATE_CODE']),
    zip:            trim(row['FAC_ZIP'] || row['ZIP_CODE']),
    status:         'Active',
    registry_id:    id || null,
    source_id:      `FEC-${id || Math.random().toString(36).slice(2)}`,
    source_org:     'EPA',
    class_code:     'ENFORCEMENT_FACILITY',
    priority_tier:  'high',
    priority_score: 90,
    lat: hasCoords ? lat : null,
    lng: hasCoords ? lng : null,
    attributes: {
      program: row['ACT_TYPE'] || row['ACTIVITY_TYPE_CODE'],
      last_inspection: row['DATE_LAST_INSPECTION'],
      violations: row['TOTAL_VIOLATIONS']
    }
  }];
}

function mapFemaClaims(row) {
  const lat = parseFloat(row['latitude'] || row['geocodedLongLat']?.split(',')[0] || '');
  const lng = parseFloat(row['longitude'] || row['geocodedLongLat']?.split(',')[1] || '');
  const hasCoords = Number.isFinite(lat) && Number.isFinite(lng);
  const claimYear = (row['dateOfLoss'] || '').substring(0, 4);
  const id = trim(row['id'] || row['claimId']);
  if (!id && !row['dateOfLoss']) return [];
  return [{
    database_name:  'FEMA NFIP Flood Claims',
    category:       'hydrology',
    site_name:      `Flood Claim - ${row['occupancyType'] || 'Residential'} (${claimYear})`,
    address:        trim(row['reportedZipcode'] ? `ZIP ${row['reportedZipcode']}` : ''),
    city:           trim(row['countyCode'] || ''),
    state:          trim(row['state'] || ''),
    zip:            trim(row['reportedZipcode'] || ''),
    status:         'Closed',
    source_id:      `FEMA-CLAIM-${id || Math.random().toString(36).slice(2)}`,
    source_org:     'FEMA',
    class_code:     'FEMA_FLOOD_CLAIM',
    priority_tier:  'high',
    priority_score: 85,
    lat: hasCoords ? lat : null,
    lng: hasCoords ? lng : null,
    attributes: {
      claim_id:         row['id'],
      date_of_loss:     row['dateOfLoss'],
      amount_paid:      row['amountPaidOnContentsPayment'],
      building_damage:  row['amountPaidOnBuildingClaim'],
      flood_zone:       row['floodZone'],
      occupancy_type:   row['occupancyType'],
      census_tract:     row['censusTract']
    }
  }];
}

function mapFemaPolicies(row) {
  const lat = parseFloat(row['latitude'] || '');
  const lng = parseFloat(row['longitude'] || '');
  const hasCoords = Number.isFinite(lat) && Number.isFinite(lng);
  const id = trim(row['id']);
  if (!id && !row['state'] && !row['reportedZipcode']) return [];
  return [{
    database_name:  'FEMA NFIP Policies',
    category:       'hydrology',
    site_name:      `NFIP Policy - ${row['occupancyType'] || 'Residential'}`,
    address:        trim(row['reportedZipcode'] ? `ZIP ${row['reportedZipcode']}` : ''),
    city:           '',
    state:          trim(row['state'] || row['propertyState'] || ''),
    zip:            trim(row['reportedZipcode'] || ''),
    status:         row['policyTerminationDate'] ? 'Expired' : 'Active',
    source_id:      `FEMA-POLICY-${id || Math.random().toString(36).slice(2)}`,
    source_org:     'FEMA',
    class_code:     'FEMA_FLOOD_POLICY',
    priority_tier:  'medium',
    priority_score: 60,
    lat: hasCoords ? lat : null,
    lng: hasCoords ? lng : null,
    attributes: {
      flood_zone:      row['floodZone'],
      occupancy_type:  row['occupancyType'],
      coverage_building: row['totalBuildingInsuranceCoverage'],
      original_nbdate:   row['originalConstructionDate'],
      census_tract:      row['censusTract']
    }
  }];
}

function mapTriFacility(row) {
  const lat = parseFloat(row['LATITUDE']  || row['FAC_LAT']  || '');
  const lng = parseFloat(row['LONGITUDE'] || row['FAC_LONG'] || '');
  const hasCoords = Number.isFinite(lat) && Number.isFinite(lng);
  const id = trim(row['TRI_FACILITY_ID'] || row['REGISTRY_ID']);
  if (!id && !row['FAC_NAME'] && !row['FACILITY_NAME']) return [];
  return [{
    database_name:  'TRI',
    category:       'contamination',
    site_name:      trim(row['FAC_NAME'] || row['FACILITY_NAME']),
    address:        trim(row['STREET_ADDRESS']),
    city:           trim(row['CITY']),
    state:          trim(row['STATE']),
    zip:            trim(row['ZIP']),
    status:         'Active',
    registry_id:    trim(row['REGISTRY_ID']),
    source_id:      `TRI-${id || Math.random().toString(36).slice(2)}`,
    source_org:     'EPA',
    class_code:     'TRI_FACILITY',
    priority_tier:  'high',
    priority_score: 88,
    lat: hasCoords ? lat : null,
    lng: hasCoords ? lng : null,
    attributes: {
      tri_id:   row['TRI_FACILITY_ID'],
      industry: row['INDUSTRY_SECTION'],
      naics:    row['NAICS'],
      chemicals_reported: row['CHEMICALS_REPORTED']
    }
  }];
}

function mapSuperfund(row) {
  const lat = parseFloat(row['LATITUDE']  || row['SITE_LATITUDE']  || '');
  const lng = parseFloat(row['LONGITUDE'] || row['SITE_LONGITUDE'] || '');
  const hasCoords = Number.isFinite(lat) && Number.isFinite(lng);
  const onNpl = (row['NPL_STATUS'] || '').includes('NPL') || row['ON_NPL_FLAG'] === 'Y';
  const id = trim(row['SITE_ID'] || row['CERCLIS_ID']);
  if (!id && !row['SITE_NAME']) return [];
  return [{
    database_name:  onNpl ? 'Superfund NPL' : 'Superfund SEMS',
    category:       'contamination',
    site_name:      trim(row['SITE_NAME']),
    address:        trim(row['ADDRESS']),
    city:           trim(row['CITY']),
    state:          trim(row['STATE']),
    zip:            trim(row['ZIP']),
    status:         trim(row['SITE_STATUS'] || row['STATUS'] || 'Active'),
    source_id:      `SEMS-${id || Math.random().toString(36).slice(2)}`,
    source_org:     'EPA',
    class_code:     onNpl ? 'SUPERFUND_NPL' : 'SUPERFUND_SEMS',
    priority_tier:  'high',
    priority_score: 95,
    lat: hasCoords ? lat : null,
    lng: hasCoords ? lng : null,
    attributes: {
      site_id:    row['SITE_ID'],
      cerclis_id: row['CERCLIS_ID'],
      npl_status: row['NPL_STATUS'],
      operable_units: row['OPERABLE_UNITS'],
      lead_agency: row['LEAD_AGENCY']
    }
  }];
}

function mapBrownfields(row) {
  const lat = parseFloat(row['LATITUDE'] || '');
  const lng = parseFloat(row['LONGITUDE'] || '');
  const hasCoords = Number.isFinite(lat) && Number.isFinite(lng);
  const id = trim(row['ACRES_ID'] || row['SITE_ID'] || row['REGISTRY_ID']);
  if (!id && !row['SITE_NAME'] && !row['PROPERTY_NAME']) return [];
  return [{
    database_name:  'EPA Brownfields',
    category:       'contamination',
    site_name:      trim(row['SITE_NAME'] || row['PROPERTY_NAME']),
    address:        trim(row['STREET_ADDRESS'] || row['ADDRESS']),
    city:           trim(row['CITY']),
    state:          trim(row['STATE']),
    zip:            trim(row['ZIP']),
    status:         trim(row['CLEANUP_STATUS'] || 'Active'),
    source_id:      `BF-${id || Math.random().toString(36).slice(2)}`,
    source_org:     'EPA',
    class_code:     'BROWNFIELD',
    priority_tier:  'medium',
    priority_score: 75,
    lat: hasCoords ? lat : null,
    lng: hasCoords ? lng : null,
    attributes: {
      acres_id:   row['ACRES_ID'],
      grant_type: row['GRANT_TYPE'],
      contaminants: row['CONTAMINANTS'],
      cleanup_status: row['CLEANUP_STATUS']
    }
  }];
}

function mapUst(row) {
  const lat = parseFloat(row['LATITUDE83'] || row['FAC_LAT'] || '');
  const lng = parseFloat(row['LONGITUDE83'] || row['FAC_LONG'] || '');
  const hasCoords = Number.isFinite(lat) && Number.isFinite(lng);
  const hasLeak = row['LUST_FLAG'] === 'Y' || (row['GENERATOR_STATUS'] || '').includes('LUST');
  return [{
    database_name:  hasLeak ? 'LUST' : 'UST',
    category:       'contamination',
    site_name:      trim(row['FAC_NAME']),
    address:        trim(row['LOCATION_STREET1']),
    city:           trim(row['LOCATION_CITY']),
    state:          trim(row['STATE_CODE']),
    zip:            trim(row['LOCATION_ZIP']),
    status:         row['ACTIVE_SITE'] === 'Y' ? 'Active' : 'Inactive',
    registry_id:    trim(row['REGISTRY_ID']),
    source_id:      `UST-${trim(row['HANDLER_ID'] || row['REGISTRY_ID'])}`,
    source_org:     'EPA',
    class_code:     hasLeak ? 'LUST_SITE' : 'UST_FACILITY',
    priority_tier:  hasLeak ? 'high' : 'medium',
    priority_score: hasLeak ? 92 : 70,
    lat: hasCoords ? lat : null,
    lng: hasCoords ? lng : null,
    attributes: {
      handler_id: row['HANDLER_ID'],
      lust_flag:  row['LUST_FLAG'],
      tank_count: row['TANK_COUNT']
    }
  }];
}

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function trim(v) {
  return (v || '').toString().trim();
}

function log(msg, level = 'info') {
  const prefix = { info: '  ', warn: '⚠ ', error: '✗ ', ok: '✓ ', data: '→ ' };
  if (level === 'verbose' && !flags.verbose) return;
  console.log(`${prefix[level] || '  '}${msg}`);
}

function formatBytes(bytes) {
  if (bytes > 1e9) return (bytes / 1e9).toFixed(1) + ' GB';
  if (bytes > 1e6) return (bytes / 1e6).toFixed(1) + ' MB';
  return (bytes / 1e3).toFixed(1) + ' KB';
}

function ensureDir(dir) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

function buildGeocodeQuery(site) {
  const parts = [
    trim(site.address),
    trim(site.city),
    trim(site.state),
    trim(site.zip),
    'USA'
  ].filter(Boolean);
  if (parts.length < 2) return null;
  return parts.join(', ');
}

async function delay(ms) {
  if (ms <= 0) return;
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function geocodeAddress(query) {
  if (!query || !flags.autoGeocode) return null;
  if (geocodeState.cache.has(query)) {
    geocodeState.cacheHits++;
    return geocodeState.cache.get(query);
  }
  if (geocodeState.requests >= geocodeLimit) return null;

  const sinceLast = Date.now() - geocodeState.lastRequestAt;
  if (sinceLast < GEOCODE_DELAY_MS) {
    await delay(GEOCODE_DELAY_MS - sinceLast);
  }

  const url = `https://nominatim.openstreetmap.org/search?format=json&limit=1&q=${encodeURIComponent(query)}`;
  geocodeState.requests++;
  geocodeState.lastRequestAt = Date.now();

  return new Promise((resolve) => {
    https.get(url, {
      headers: {
        'User-Agent': 'GeoScope-FederalImporter/1.0 (support@geoscopesolutions.com)',
        'Accept': 'application/json'
      }
    }, (res) => {
      let body = '';
      res.on('data', (chunk) => { body += chunk; });
      res.on('end', () => {
        try {
          const rows = JSON.parse(body);
          const first = Array.isArray(rows) ? rows[0] : null;
          const lat = Number(first?.lat);
          const lng = Number(first?.lon);
          if (Number.isFinite(lat) && Number.isFinite(lng)) {
            const coords = { lat, lng };
            geocodeState.cache.set(query, coords);
            geocodeState.hits++;
            resolve(coords);
            return;
          }
        } catch (_) {
          // ignore parse errors
        }
        geocodeState.cache.set(query, null);
        geocodeState.misses++;
        resolve(null);
      });
    }).on('error', () => {
      geocodeState.misses++;
      resolve(null);
    });
  });
}

async function enrichMissingCoordinates(rows, sourceLabel) {
  if (!flags.autoGeocode || rows.length === 0) return rows;

  for (const row of rows) {
    if (Number.isFinite(Number(row.lat)) && Number.isFinite(Number(row.lng))) continue;
    const query = buildGeocodeQuery(row);
    if (!query) continue;
    const coords = await geocodeAddress(query);
    if (coords) {
      row.lat = coords.lat;
      row.lng = coords.lng;
      row.status = row.status || 'Geocoded';
      row.attributes = {
        ...(row.attributes || {}),
        geocoded_by: 'nominatim',
        geocode_query: query
      };
    }
  }

  if (flags.verbose) {
    log(`[${sourceLabel}] geocode stats requests=${geocodeState.requests} hits=${geocodeState.hits} cache_hits=${geocodeState.cacheHits} misses=${geocodeState.misses}`, 'verbose');
  }
  return rows;
}

// ============================================================================
// DOWNLOAD FUNCTIONS
// ============================================================================

function downloadFile(url, destPath) {
  return new Promise((resolve, reject) => {
    if (flags.skipDownload && fs.existsSync(destPath)) {
      log(`Skipping download (exists): ${path.basename(destPath)}`, 'verbose');
      return resolve(destPath);
    }
    log(`Downloading ${path.basename(destPath)} from ${url.substring(0, 60)}...`);
    const maxAttempts = 5;
    const retryableCodes = new Set(['ECONNRESET', 'ETIMEDOUT', 'ECONNABORTED', 'EAI_AGAIN', 'ENOTFOUND']);

    const doAttempt = (reqUrl, attempt, redirectCount = 0) => {
      const proto = reqUrl.startsWith('https') ? https : http;
      // Use process/attempt-scoped temp file to avoid collisions across concurrent runs.
      const tmpPath = `${destPath}.${process.pid}.${attempt}.tmp`;
      let downloaded = 0;
      let total = 0;
      let lastLog = Date.now();
      const file = fs.createWriteStream(tmpPath);

      const req = proto.get(reqUrl, {
        headers: { 'User-Agent': 'GeoScope/2.0 (geoscopesolutions.com)' },
        timeout: 300000
      }, (res) => {
        if ((res.statusCode === 301 || res.statusCode === 302) && res.headers.location) {
          file.destroy();
          try { if (fs.existsSync(tmpPath)) fs.unlinkSync(tmpPath); } catch (_) {}
          if (redirectCount > 5) {
            return reject(new Error(`Too many redirects for ${reqUrl}`));
          }
          return doAttempt(res.headers.location, attempt, redirectCount + 1);
        }

        if (res.statusCode !== 200) {
          file.destroy();
          try { if (fs.existsSync(tmpPath)) fs.unlinkSync(tmpPath); } catch (_) {}
          const isRetryableStatus = res.statusCode >= 500 || res.statusCode === 429;
          if (isRetryableStatus && attempt < maxAttempts) {
            const delayMs = Math.min(30000, attempt * 3000);
            log(`Download retry ${attempt}/${maxAttempts} after HTTP ${res.statusCode} (${path.basename(destPath)})`, 'warn');
            return setTimeout(() => doAttempt(url, attempt + 1), delayMs);
          }
          return reject(new Error(`HTTP ${res.statusCode} for ${reqUrl}`));
        }

        total = parseInt(res.headers['content-length'] || '0');
        res.on('data', (chunk) => {
          downloaded += chunk.length;
          if (Date.now() - lastLog > 5000) {
            const pct = total ? Math.round(downloaded / total * 100) : '?';
            log(`  ${formatBytes(downloaded)} / ${total ? formatBytes(total) : '?'} (${pct}%)`, 'verbose');
            lastLog = Date.now();
          }
        });

        res.pipe(file);

        file.on('finish', () => {
          file.close(() => {
            try {
              fs.renameSync(tmpPath, destPath);
            } catch (err) {
              if (err?.code === 'ENOENT' && fs.existsSync(destPath)) {
                log(`Download finalized by another process: ${path.basename(destPath)}`, 'warn');
              } else {
                return reject(err);
              }
            }
            log(`Downloaded: ${path.basename(destPath)} (${formatBytes(downloaded)})`, 'ok');
            resolve(destPath);
          });
        });
      });

      req.on('timeout', () => req.destroy(new Error('ETIMEDOUT')));
      req.on('error', (err) => {
        try { file.destroy(); } catch (_) {}
        try { if (fs.existsSync(tmpPath)) fs.unlinkSync(tmpPath); } catch (_) {}
        if (retryableCodes.has(err?.code) && attempt < maxAttempts) {
          const delayMs = Math.min(30000, attempt * 3000);
          log(`Download retry ${attempt}/${maxAttempts} after ${err.code} (${path.basename(destPath)})`, 'warn');
          return setTimeout(() => doAttempt(url, attempt + 1), delayMs);
        }
        reject(err);
      });
    };

    doAttempt(url, 1);
  });
}

async function fetchFemaPage(url, skip, top) {
  const pageUrl = `${url}?$top=${top}&$skip=${skip}&$format=json`;
  return new Promise((resolve, reject) => {
    const proto = pageUrl.startsWith('https') ? https : http;
    let data = '';
    proto.get(pageUrl, { headers: { 'User-Agent': 'GeoScope/2.0' } }, (res) => {
      if (res.statusCode !== 200) return reject(new Error(`HTTP ${res.statusCode}`));
      res.on('data', chunk => { data += chunk; });
      res.on('end', () => {
        try { resolve(JSON.parse(data)); }
        catch (e) { reject(new Error('JSON parse failed: ' + e.message)); }
      });
    }).on('error', reject);
  });
}

// ============================================================================
// DATABASE BATCH INSERT
// ============================================================================

async function batchInsert(rows, sourceLabel) {
  if (flags.dryRun || rows.length === 0) return rows.length;

  await enrichMissingCoordinates(rows, sourceLabel);

  // Deduplicate within batch to avoid intra-batch constraint violations
  const seen = new Set();
  const deduped = [];
  for (const r of rows) {
    const key = r.source_id;
    if (!key || !seen.has(key)) {
      if (key) seen.add(key);
      deduped.push(r);
    }
  }

  // Build parameterized query
  const placeholders = [];
  const values = [];
  let idx = 1;

  for (const r of deduped) {
    const hasCoords = Number.isFinite(Number(r.lat)) && Number.isFinite(Number(r.lng));
    let locExpr;
    if (hasCoords) {
      locExpr = `ST_SetSRID(ST_MakePoint($${idx++}, $${idx++}), 4326)`;
      values.push(r.lng, r.lat);
    } else {
      locExpr = 'NULL';
    }
    placeholders.push(
      `(${locExpr}, $${idx++}, $${idx++}, $${idx++}, $${idx++}, $${idx++}, $${idx++}, $${idx++}, $${idx++}, $${idx++}, $${idx++}, $${idx++}, $${idx++}::jsonb, $${idx++}, $${idx++}, $${idx++}, $${idx++})`
    );
    values.push(
      r.database_name || 'Unknown',
      r.category      || 'other',
      r.site_name     || null,
      r.address       || null,
      r.city          || null,
      r.state         || null,
      r.zip           || null,
      r.status        || null,
      r.registry_id   || null,
      r.source_id     || null,
      r.source_org    || null,
      JSON.stringify(r.attributes || {}),
      Number.isFinite(Number(r.priority_score)) ? Number(r.priority_score) : 0,
      r.class_code || null,
      sourceLabel,
      r.class_description || null
    );
  }

  if (placeholders.length === 0) return 0;

  const sql = `
    INSERT INTO environmental_sites
      (location, database_name, category, site_name, address, city, state, zip,
       status, registry_id, source_id, source_org, attributes,
       priority_score, class_code, source_name, class_description)
    VALUES ${placeholders.join(',\n')}
    ON CONFLICT (source_id) DO UPDATE SET updated_at = NOW()
  `;

  try {
    const result = await pgPool.query(sql, values);
    return result.rowCount;
  } catch (err) {
    log(`Batch insert error [${sourceLabel}]: ${err.message}`, 'error');
    if (flags.verbose) log(err.stack, 'error');
    return 0;
  }
}

// ============================================================================
// PROCESS A CSV STREAM
// ============================================================================

async function processCSVStream(stream, source, stats) {
  const { mapper, name, category, source_org, priority_tier, priority_score } = source;
  let batch = [];
  let totalParsed = 0;
  let totalImported = 0;

  const parser = csvParse({ columns: true, skip_empty_lines: true, trim: true, relax_column_count: true });
  stream.pipe(parser);

  for await (const row of parser) {
    try {
      const sites = mapper(row, { category, source_org, priority_tier, priority_score });
      for (const site of sites) {
        batch.push(site);
        totalParsed++;
      }
      if (batch.length >= BATCH_SIZE) {
        const count = await batchInsert(batch, name);
        totalImported += count;
        batch = [];
        if (totalParsed % 50000 === 0) {
          log(`  [${name}] Processed ${totalParsed.toLocaleString()} records, imported ${totalImported.toLocaleString()}...`);
        }
      }
    } catch (e) {
      log(`Row processing error: ${e.message}`, 'verbose');
    }
  }

  if (batch.length > 0) {
    const count = await batchInsert(batch, name);
    totalImported += count;
  }

  stats.parsed   += totalParsed;
  stats.imported += totalImported;
  log(`  [${name}] Done: ${totalParsed.toLocaleString()} parsed, ${totalImported.toLocaleString()} imported`, 'ok');
}

// ============================================================================
// PROCESS A ZIP FILE
// ============================================================================

async function processZip(zipPath, source, stats) {
  if (!unzipper) throw new Error('unzipper package required. Run: npm install unzipper');
  log(`Processing ZIP: ${path.basename(zipPath)}`);
  const directory = await unzipper.Open.file(zipPath);
  let processed = 0;

  for (const entry of directory.files) {
    if (entry.type !== 'File') continue;
    if (!entry.path.match(/\.csv$/i)) continue;
    if (source.targetFile && !source.targetFile.test(entry.path)) {
      log(`  Skipping ${entry.path} (not target file)`, 'verbose');
      continue;
    }
    log(`  Processing: ${entry.path}`);
    const stream = entry.stream();
    await processCSVStream(stream, source, stats);
    processed++;
    if (!source.targetFile) break; // only process first CSV if no filter
  }

  if (processed === 0) {
    // Try processing all CSV files
    log(`  No matching CSV found in ZIP, trying all CSVs...`, 'warn');
    for (const entry of directory.files) {
      if (entry.type !== 'File' || !entry.path.match(/\.csv$/i)) continue;
      log(`  Processing: ${entry.path}`);
      const stream = entry.stream();
      await processCSVStream(stream, source, stats);
      break; // just first one
    }
  }
}

// ============================================================================
// PROCESS FEMA API
// ============================================================================

async function processFemaApi(source, stats) {
  const { name, url, mapper } = source;
  log(`Fetching ${name} from OpenFEMA API...`);
  let skip = 0;
  let totalParsed = 0;
  let totalImported = 0;
  let keepGoing = true;

  while (keepGoing && skip < MAX_FEMA_RECORDS) {
    try {
      log(`  Fetching page skip=${skip}...`, 'verbose');
      const result = await fetchFemaPage(url, skip, FEMA_PAGE_SIZE);
      const records = result?.FimaNfipClaims || result?.FimaNfipPolicies || result?.data || [];

      if (!records || records.length === 0) {
        keepGoing = false;
        break;
      }

      const batch = [];
      for (const row of records) {
        const sites = mapper(row);
        for (const site of sites) batch.push(site);
        totalParsed++;
      }

      const count = await batchInsert(batch, name);
      totalImported += count;
      skip += FEMA_PAGE_SIZE;

      if (totalParsed % 100000 === 0) {
        log(`  [${name}] Fetched ${totalParsed.toLocaleString()} so far...`);
      }

      if (records.length < FEMA_PAGE_SIZE) keepGoing = false;

      // Small delay to be respectful to the API
      await new Promise(r => setTimeout(r, 200));
    } catch (err) {
      log(`FEMA API error at skip=${skip}: ${err.message}`, 'error');
      keepGoing = false;
    }
  }

  stats.parsed   += totalParsed;
  stats.imported += totalImported;
  log(`  [${name}] Done: ${totalParsed.toLocaleString()} fetched, ${totalImported.toLocaleString()} imported`, 'ok');
}

// ============================================================================
// PROCESS A DIRECT CSV URL (streamed download → CSV parse)
// ============================================================================

async function processDirectCsv(source, stats) {
  const { name, url } = source;
  log(`Streaming CSV from: ${url.substring(0, 70)}...`);

  return new Promise((resolve, reject) => {
    const proto = url.startsWith('https') ? https : http;
    proto.get(url, { headers: { 'User-Agent': 'GeoScope/2.0' } }, async (res) => {
      if (res.statusCode === 301 || res.statusCode === 302) {
        log(`Following redirect...`, 'verbose');
        // Simple redirect follow
        const newSource = { ...source, url: res.headers.location };
        processDirectCsv(newSource, stats).then(resolve).catch(reject);
        return;
      }
      if (res.statusCode !== 200) {
        reject(new Error(`HTTP ${res.statusCode} for ${url}`));
        return;
      }
      try {
        await processCSVStream(res, source, stats);
        resolve();
      } catch (e) {
        reject(e);
      }
    }).on('error', reject);
  });
}

// ============================================================================
// ENSURE SCHEMA (source_id unique constraint)
// ============================================================================

async function ensureSchema() {
  if (flags.dryRun) return;
  try {
    // Add source_id unique constraint if not exists
    await pgPool.query(`
      DO $$ BEGIN
        IF NOT EXISTS (
          SELECT 1 FROM pg_constraint
          WHERE conname = 'environmental_sites_source_id_key'
          AND conrelid = 'environmental_sites'::regclass
        ) THEN
          ALTER TABLE environmental_sites ADD CONSTRAINT environmental_sites_source_id_key UNIQUE (source_id);
        END IF;
      EXCEPTION WHEN OTHERS THEN NULL;
      END $$;
    `);
    // Add source_name column for tracking which import run added records
    await pgPool.query(`
      ALTER TABLE environmental_sites ADD COLUMN IF NOT EXISTS source_name TEXT;
      ALTER TABLE environmental_sites ADD COLUMN IF NOT EXISTS class_code VARCHAR(80);
      ALTER TABLE environmental_sites ADD COLUMN IF NOT EXISTS class_description VARCHAR(255);
      ALTER TABLE environmental_sites ADD COLUMN IF NOT EXISTS priority_score INTEGER DEFAULT 0;
    `).catch(() => {});
    log('Schema ready', 'ok');
  } catch (e) {
    log(`Schema check: ${e.message}`, 'verbose');
  }
}

// ============================================================================
// MAIN ORCHESTRATOR
// ============================================================================

async function main() {
  console.log('\n============================================================');
  console.log(' GeoScope Federal Bulk Downloader');
  console.log('============================================================');
  console.log(` Sources: ${selectedSources.join(', ')}`);
  console.log(` Mode:    ${flags.dryRun ? 'DRY RUN' : 'LIVE IMPORT'}`);
  console.log(` Geocode: ${flags.autoGeocode ? `ON (limit ${geocodeLimit})` : 'OFF'}`);
  console.log(` DB:      ${process.env.PG_HOST || 'localhost'}/${process.env.PG_DATABASE || 'geoscope'}`);
  console.log('');

  ensureDir(DOWNLOAD_DIR);
  await ensureSchema();

  const globalStats = { parsed: 0, imported: 0, errors: 0, skipped: 0 };
  const startTime = Date.now();

  for (const key of selectedSources) {
    const source = SOURCES[key];
    if (!source) {
      log(`Unknown source: ${key}`, 'warn');
      continue;
    }

    console.log(`\n─── ${source.name} ────────────────────────────────────`);
    log(source.description);

    const stats = { parsed: 0, imported: 0 };

    try {
      if (source.type === 'fema_api') {
        if (!flags.downloadOnly) await processFemaApi(source, stats);
      } else if (source.type === 'direct_csv') {
        if (!flags.downloadOnly) await processDirectCsv(source, stats);
      } else if (source.type === 'zip_csv') {
        const filename  = path.basename(new URL(source.url).pathname);
        const destPath  = path.join(DOWNLOAD_DIR, filename);

        if (!flags.importOnly) {
          await downloadFile(source.url, destPath);
        }
        if (!flags.downloadOnly) {
          if (!fs.existsSync(destPath)) {
            log(`ZIP not found: ${destPath}`, 'error');
            stats.errors = (stats.errors || 0) + 1;
            continue;
          }
          await processZip(destPath, source, stats);
        }
      }
    } catch (err) {
      log(`Failed [${source.name}]: ${err.message}`, 'error');
      globalStats.errors++;
    }

    globalStats.parsed   += stats.parsed;
    globalStats.imported += stats.imported;

    log(`Subtotal — parsed: ${stats.parsed.toLocaleString()}, imported: ${stats.imported.toLocaleString()}`);
  }

  const elapsed = Math.round((Date.now() - startTime) / 1000);

  console.log('\n============================================================');
  console.log(' SUMMARY');
  console.log('============================================================');
  console.log(` Total records parsed:   ${globalStats.parsed.toLocaleString()}`);
  console.log(` Total records imported: ${globalStats.imported.toLocaleString()}`);
  console.log(` Errors:                 ${globalStats.errors}`);
  console.log(` Time elapsed:           ${Math.floor(elapsed / 60)}m ${elapsed % 60}s`);
  if (flags.autoGeocode) {
    console.log(` Geocode requests:       ${geocodeState.requests}`);
    console.log(` Geocode hits:           ${geocodeState.hits}`);
    console.log(` Geocode cache hits:     ${geocodeState.cacheHits}`);
  }
  if (flags.dryRun) console.log(' *** DRY RUN — no records were inserted ***');
  console.log('');

  // Print final DB count
  if (!flags.dryRun && !flags.downloadOnly) {
    try {
      const r = await pgPool.query('SELECT COUNT(*) as total FROM environmental_sites');
      console.log(` Database total:         ${parseInt(r.rows[0].total).toLocaleString()} sites`);
    } catch (e) { /* ignore */ }
  }

  await pgPool.end();
}

main().catch(err => {
  console.error('\n✗ Fatal error:', err.message);
  if (flags.verbose) console.error(err.stack);
  process.exit(1);
});
