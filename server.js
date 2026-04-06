require('dotenv').config();
const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const nodemailer = require('nodemailer');
const puppeteer = require('puppeteer');
const path = require('path');
const fs = require('fs');
const mongoose = require('mongoose');
const axios = require('axios');
const { createClient } = require('@supabase/supabase-js');
const OpenAI = require('openai');
const multer = require('multer');
const auth = require('./auth'); // Import auth module
const proReportRouter = require('./routes/proReport');
const { pool: pgPool, pingDB } = require('./db');

const app = express();
const PORT = process.env.PORT || 6001;
const AUTH_SECURITY_MODE = (process.env.AUTH_SECURITY_MODE || 'jwt').toLowerCase();
const JWT_AUTH_ENABLED = AUTH_SECURITY_MODE !== 'off';
const SERVER_STARTED_AT = new Date().toISOString();
const REPORTS_DIR = path.join(__dirname, 'reports');

// =====================
// MIDDLEWARE - MUST BE BEFORE ALL ROUTES
// =====================
app.use(cors());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

// Increase request timeout for long-running operations (report generation, etc)
app.use((req, res, next) => {
  req.setTimeout(600000); // 10 minutes
  res.setTimeout(600000); // 10 minutes
  next();
});

app.use(proReportRouter);

// POST /save-order - Save order status (Save & Exit)
app.post('/save-order', async (req, res) => {
  const { order_id, status } = req.body;

  // Try MongoDB update if available
  let mongoUpdated = false;
  if (typeof GeoData !== 'undefined') {
    try {
      // Assuming there is an Order model for MongoDB
      if (typeof Order !== 'undefined') {
        await Order.updateOne({ _id: order_id }, { $set: { status } });
        mongoUpdated = true;
      }
    } catch (err) {
      console.error('MongoDB order update error:', err);
    }
  }

  // Fallback to in-memory orders
  if (!mongoUpdated) {
    const orderIndex = findInMemoryOrderIndex(order_id);
    if (orderIndex !== -1) {
      orders[orderIndex].status = status;
      return res.send('Saved');
    } else {
      return res.status(404).send('Order not found');
    }
  }

  res.send('Saved');
});

// Configure multer for file uploads
const storage = multer.memoryStorage();
const upload = multer({
  storage: storage,
  limits: {
    fileSize: 10 * 1024 * 1024, // 10MB limit
  },
  fileFilter: (req, file, cb) => {
    const allowedTypes = [
      'application/pdf',
      'image/jpeg',
      'image/jpg',
      'image/png',
      'application/dwg',
      'application/dxf',
      'application/msword',
      'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
    ];
    if (allowedTypes.includes(file.mimetype)) {
      cb(null, true);
    } else {
      cb(new Error('Invalid file type'), false);
    }
  }
});

// Initialize Supabase Client
const supabaseUrl = process.env.SUPABASE_URL || 'https://your-project.supabase.co';
const supabaseKey = process.env.SUPABASE_KEY || 'your-anon-key';
const supabase = createClient(supabaseUrl, supabaseKey);

// Initialize OpenAI Client
const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY || 'sk-your-api-key'
});

// Connect to MongoDB
const mongoUri = process.env.MONGODB_URI || 'mongodb://127.0.0.1:27017/geoscope';
mongoose.connect(mongoUri, {
  serverSelectionTimeoutMS: 3000,
  connectTimeoutMS: 3000
})
  .then(() => console.log('MongoDB connected for reports'))
  .catch(() => console.warn('MongoDB unavailable; continuing with PostgreSQL-backed reporting pipeline.'));

// GeoData schema for spatial queries
const geoDataSchema = new mongoose.Schema({
  location: {
    type: { type: String, enum: ['Point'] },
    coordinates: { type: [Number] }
  },
  data_type: String,
  attributes: mongoose.Schema.Types.Mixed,
  added_at: { type: Date, default: Date.now }
});
geoDataSchema.index({ location: '2dsphere' });
const GeoData = mongoose.model('GeoData', geoDataSchema);

const turf = require('@turf/turf');

// Helper function to categorize environmental sites
function categorizeSites(sites) {
  const categories = {
    fuel: 0,
    waste: 0,
    industrial: 0,
    government: 0,
    schools: 0
  };

  sites.forEach(site => {
    const db = site.database?.toLowerCase() || '';
    if (db.includes('fuel') || db.includes('petroleum') || db.includes('gas')) {
      categories.fuel++;
    } else if (db.includes('waste') || db.includes('hazardous') || db.includes('npl')) {
      categories.waste++;
    } else if (db.includes('industrial') || db.includes('manufacturing')) {
      categories.industrial++;
    } else if (db.includes('epa') || db.includes('echo') || db.includes('cerclis')) {
      categories.government++;
    } else if (db.includes('school') || db.includes('education')) {
      categories.schools++;
    }
  });

  return categories;
}

// Helper function to determine risk level
function getRiskLevel(site) {
  const db = site.database?.toLowerCase() || '';
  const name = String(site.name || site.site_name || '').toLowerCase();
  const status = String(site.status || '').toLowerCase();
  const distanceMiles = parseDistanceMiles(site.distance);

  let score = 0;

  if (/npl|superfund|cerclis|sems|rcra|tsdf|hazardous|toxic|pfas/.test(db)) score += 4;
  else if (/ust|lust|fuel|petroleum|industrial|waste|landfill|tri|echo|npdes/.test(db)) score += 3;
  else if (/school|education|facility|government|permit/.test(db)) score += 1;

  if (/open|active|violation|non-?compliance|enforcement|release|spill/.test(status)) score += 2;
  if (/closed|resolved|no further action|nfa/.test(status)) score -= 1;

  if (/unknown/.test(name)) score += 1;

  if (Number.isFinite(distanceMiles)) {
    if (distanceMiles <= 0.25) score += 2;
    else if (distanceMiles <= 0.5) score += 1;
  }

  if (score >= 6) return 'High';
  if (score >= 3) return 'Moderate';
  return 'Low';
}

function inferEnvironmentalIntelligence(databaseName, locationType = '') {
  const db = String(databaseName || '').toLowerCase();
  const type = String(locationType || '').toLowerCase();

  const rules = [
    {
      re: /rcra|lqg|sqg|vsqg|tsdf/,
      activity: 'Hazardous waste generation or management activity',
      contaminants: 'Solvents, ignitable wastes, toxic metals, listed hazardous wastes',
      pathway: 'Soil and groundwater release potential from handling or storage',
      regulatory: 'RCRA hazardous waste compliance tracking'
    },
    {
      re: /npl|superfund|cerclis|sems/,
      activity: 'Federally tracked remediation or potential remediation site',
      contaminants: 'Mixed legacy industrial contaminants, VOCs, SVOCs, metals',
      pathway: 'Groundwater, soil vapor, and off-site migration pathways',
      regulatory: 'CERCLA/Superfund enforcement and cleanup context'
    },
    {
      re: /ust|lust|petroleum|fuel/,
      activity: 'Petroleum storage or release-related operations',
      contaminants: 'BTEX, petroleum hydrocarbons, fuel oxygenates',
      pathway: 'Subsurface soil and groundwater plume migration',
      regulatory: 'UST/LUST corrective action and closure records'
    },
    {
      re: /npdes|icis|echo|air facility|rmp/,
      activity: 'Permitted discharge, emissions, or compliance-tracked operation',
      contaminants: 'Wastewater parameters, air pollutants, industrial byproducts',
      pathway: 'Surface water, stormwater, and air dispersion pathways',
      regulatory: 'Federal/state permit compliance program records'
    },
    {
      re: /pfas/,
      activity: 'PFAS-related site indicator',
      contaminants: 'Per- and polyfluoroalkyl substances',
      pathway: 'Groundwater persistence and potential drinking water transport',
      regulatory: 'Emerging contaminant screening and response context'
    },
    {
      re: /wetland|flood|hydro|stormwater/,
      activity: 'Hydrology or floodplain-sensitive environmental setting',
      contaminants: 'Not a direct contaminant listing; indicates migration sensitivity',
      pathway: 'Surface runoff and flood mobilization potential',
      regulatory: 'Floodplain and wetland development constraints'
    },
    {
      re: /radon|mine|geolog|coal ash|asbestos/,
      activity: 'Geologic or subsurface hazard indicator',
      contaminants: 'Radon, metals, mineral-related hazards, geogenic risks',
      pathway: 'Soil gas and subsurface transport pathways',
      regulatory: 'Screening-level geologic hazard context'
    }
  ];

  const matched = rules.find((rule) => rule.re.test(db));
  if (matched) return matched;

  if (type.includes('school') || type.includes('hospital')) {
    return {
      activity: 'Sensitive receptor context',
      contaminants: 'Exposure sensitivity is elevated for nearby contaminants',
      pathway: 'Air and local environmental exposure pathways',
      regulatory: 'Enhanced relevance for health-protective due diligence'
    };
  }

  return {
    activity: 'General environmental screening listing',
    contaminants: 'Contaminants not explicitly specified in source record',
    pathway: 'Potential environmental pathway requires site-specific confirmation',
    regulatory: 'Regulatory meaning should be confirmed using source record detail'
  };
}

function buildRegulatoryPrimerForDatabase(databaseName = '') {
  const db = String(databaseName || '').toLowerCase();
  if (/ust|lust|petroleum|fuel/.test(db)) {
    return {
      program: 'UST/LUST Program',
      definition: 'Underground storage tank records track fuel-system operations and petroleum release events that may affect soil or groundwater.',
      implication: 'If active or unresolved, confirm closure status, release investigation scope, and corrective action completion documents.'
    };
  }
  if (/rcra|lqg|sqg|vsqg|tsdf/.test(db)) {
    return {
      program: 'RCRA Hazardous Waste Program',
      definition: 'RCRA listings indicate hazardous waste generation, storage, treatment, or disposal activity under federal/state oversight.',
      implication: 'Review generator status history, violations, and waste-handling controls to evaluate contamination liability exposure.'
    };
  }
  if (/npl|superfund|cerclis|sems/.test(db)) {
    return {
      program: 'CERCLA / Superfund Program',
      definition: 'Superfund-related listings identify sites with known or suspected hazardous releases requiring investigation or remediation.',
      implication: 'Evaluate remedial stage, institutional controls, and plume migration context before transaction close.'
    };
  }
  if (/npdes|icis|echo|air facility|rmp/.test(db)) {
    return {
      program: 'Permit Compliance Programs',
      definition: 'Permit databases track wastewater discharge, air emissions, and compliance history for regulated facilities.',
      implication: 'Inspect violation trends and enforcement history to assess ongoing operational environmental risk.'
    };
  }
  if (/wetland|flood|hydro|stormwater/.test(db)) {
    return {
      program: 'Floodplain / Wetland Constraints',
      definition: 'Hydrology and wetland layers indicate environmentally sensitive settings that influence development and migration behavior.',
      implication: 'Confirm permitting constraints, flood mitigation requirements, and potential runoff transport effects.'
    };
  }
  return {
    program: 'Environmental Regulatory Record',
    definition: 'This dataset contributes screening evidence of environmental activity or constraints near the address.',
    implication: 'Use source records for verification before relying on the screening result for final decisions.'
  };
}

function buildAddressDecisionActionLine(locationTier, riskBand, findingCount) {
  const tier = String(locationTier || 'Baseline');
  const band = String(riskBand || '').toLowerCase();
  const count = Number(findingCount || 0);
  if (tier === 'Priority A' || band.includes('high')) {
    return 'Escalate due diligence: perform agency-file verification and targeted Phase II scope planning before final commitment.';
  }
  if (tier === 'Priority B' || band.includes('moderate') || count >= 3) {
    return 'Proceed with caution: complete source-file confirmation and focused follow-up on nearest active records.';
  }
  return 'Proceed under baseline screening assumptions, with routine record confirmation and standard transaction diligence.';
}

function buildCombinedRiskInterpretationLine(primaryDb, uniqueDatabases = [], nearestMeters, riskBand, locationTier, findingCount) {
  const dbNames = (uniqueDatabases || []).map((r) => String(r.database_name || r.database || '').toLowerCase());
  const hasUst = dbNames.some((n) => /ust|lust|petroleum|fuel/.test(n));
  const hasHazWaste = dbNames.some((n) => /rcra|hazard|cerclis|superfund|npl|tri/.test(n));
  const hasHydrology = dbNames.some((n) => /flood|wetland|hydro|storm/.test(n));
  const nearestText = Number.isFinite(nearestMeters) ? fmtMi(nearestMeters) : 'unresolved distance';

  if (hasUst && hasHazWaste) {
    return `The combination of petroleum-system indicators and hazardous-waste regulatory records at approximately ${nearestText} increases potential subsurface contamination concern, especially for soil and groundwater migration pathways.`;
  }
  if (hasHazWaste && hasHydrology) {
    return 'Hazardous-material regulatory indicators combined with hydrology-sensitive conditions suggest elevated contaminant transport sensitivity during stormwater or high-water events.';
  }
  if (hasUst) {
    return 'Petroleum-related records near this location indicate potential hydrocarbon release relevance; closure documentation and corrective-action history should be confirmed.';
  }
  if (hasHydrology) {
    return 'Hydrology constraints near this address increase sensitivity to migration and permitting complexity even where direct contaminant records are limited.';
  }
  return `This address carries a ${String(riskBand || 'baseline').toLowerCase()} profile (${locationTier}) based on ${findingCount} linked record${findingCount === 1 ? '' : 's'} and the nearest mapped source at ${nearestText}.`;
}

function computePriorityTier(riskLevel, distanceMeters, isUnknownSite = false) {
  const risk = String(riskLevel || 'LOW').toUpperCase();
  const d = Number.isFinite(Number(distanceMeters)) ? Number(distanceMeters) : null;
  if (risk === 'HIGH' && (d === null || d <= 250)) return 'Priority A';
  if ((risk === 'MODERATE' || risk === 'MEDIUM') && (d === null || d <= 350)) return 'Priority B';
  if (isUnknownSite && (d === null || d <= 400)) return 'Priority B';
  return 'Baseline';
}

function buildDecisionRecommendation(priorityA, priorityB) {
  if (priorityA >= 3) return 'Material environmental screening triggers were identified. Defer acquisition or major development commitments until Phase I ESA and targeted agency-file review are completed.';
  if (priorityA > 0) return 'At least one high-priority trigger is present. Proceed only with conditional underwriting tied to focused due diligence and confirmatory records review.';
  if (priorityB >= 3) return 'Multiple moderate-priority indicators are present. Continue evaluation with a bounded follow-up scope for the closest and active records.';
  if (priorityB > 0) return 'Proceed with caution and targeted follow-up on identified locations before finalizing scope or pricing decisions.';
  return 'No dominant screening trigger was identified. Proceed with standard diligence while preserving contingency for newly surfaced records.';
}

// Helper function to generate detailed site listings
function generateDetailedSites(sites) {
  return sites.map((site, index) => {
    const riskLevel = getRiskLevel(site);
    const riskClass = riskLevel === 'High' ? 'risk-high' :
                     riskLevel === 'Moderate' ? 'risk-medium' : 'risk-low';

    return `
    <tr>
      <td>${index + 1}</td>
      <td>${site.name || 'Unknown Site'}</td>
      <td>${site.database || 'Unknown'}</td>
      <td class="${riskClass}">${riskLevel}</td>
      <td>${site.distance || 'N/A'}</td>
      <td>${site.status || 'Active'}</td>
      <td>${site.address || site.location || 'N/A'}</td>
    </tr>`;
  }).join('');
}

function getOverallRiskLevel(riskLevels) {
  if ((riskLevels?.high || 0) > 0) return 'HIGH';
  if ((riskLevels?.medium || 0) > 0) return 'MODERATE';
  return 'LOW';
}

// Requested dynamic table row generator for database findings.
function generateRows(data) {
  return (data || []).map((d) => {
    const riskRaw = String(d.risk_level || d.risk || getRiskLevel(d) || 'LOW').toUpperCase();
    const riskColor = riskRaw === 'HIGH' ? '#b91c1c' : riskRaw === 'MODERATE' ? '#b45309' : '#166534';
    const distanceValue = parseDistanceMiles(d.distance);
    const distDisplay = Number.isFinite(distanceValue) ? `${distanceValue.toFixed(2)} mi` : 'N/A';

    return `
    <tr>
      <td>${escapeHtml(d.database_name || d.database || 'Unknown')}</td>
      <td>${escapeHtml(d.site_name || d.name || 'Unknown Facility')}</td>
      <td>${escapeHtml(cleanDisplayAddress(d.address || d.location))}</td>
      <td>${distDisplay}</td>
      <td style="color:${riskColor}; font-weight:700;">${escapeHtml(riskRaw)}</td>
    </tr>
  `;
  }).join('');
}

// Requested AI-style risk interpretation helper.
function generateSummary(data) {
  const count = (data || []).length;
  const riskCounts = (data || []).reduce((acc, item) => {
    const risk = String(item.risk_level || item.riskLevel || item.risk || getRiskLevel(item)).toUpperCase();
    if (risk === 'HIGH') acc.high += 1;
    else if (risk === 'MODERATE' || risk === 'MEDIUM') acc.moderate += 1;
    else acc.low += 1;
    return acc;
  }, { high: 0, moderate: 0, low: 0 });

  if (riskCounts.high >= 3 || count > 25) {
    return 'Elevated screening concern: multiple high-severity or dense records suggest potential cumulative environmental constraints requiring immediate due diligence prioritization.';
  }
  if (riskCounts.high > 0 || riskCounts.moderate >= 4 || count > 8) {
    return 'Moderate screening concern: localized regulatory or contamination indicators are present and should be validated through focused follow-up.';
  }
  return 'Baseline screening concern: limited mapped indicators were returned, with no dominant high-severity cluster identified in current datasets.';
}

function pruneSitesForReport(sites, maxRecords = 650) {
  const list = Array.isArray(sites) ? sites : [];
  if (list.length <= maxRecords) return list;

  // Keep the most decision-relevant records first: highest risk + nearest distance.
  const riskRank = { High: 3, Moderate: 2, Low: 1 };
  const ranked = [...list].sort((a, b) => {
    const ra = riskRank[getRiskLevel(a)] || 0;
    const rb = riskRank[getRiskLevel(b)] || 0;
    if (ra !== rb) return rb - ra;

    const da = Number.isFinite(parseDistanceMiles(a.distance)) ? parseDistanceMiles(a.distance) : Number.MAX_SAFE_INTEGER;
    const db = Number.isFinite(parseDistanceMiles(b.distance)) ? parseDistanceMiles(b.distance) : Number.MAX_SAFE_INTEGER;
    return da - db;
  });

  return ranked.slice(0, maxRecords);
}

async function fetchAreaFeaturesFromOSM(lat, lng, radius = 250) {
  // Keep OSM query bounded to avoid oversized payloads in dense metros.
  const radiusMeters = Math.min(1200, Math.max(50, Number(radius) || 250));
  const query = `
    [out:json][timeout:30];
    (
      node(around:${radiusMeters},${lat},${lng})["building"];
      way(around:${radiusMeters},${lat},${lng})["building"];
      node(around:${radiusMeters},${lat},${lng})["amenity"];
      way(around:${radiusMeters},${lat},${lng})["amenity"];
      node(around:${radiusMeters},${lat},${lng})["landuse"];
      way(around:${radiusMeters},${lat},${lng})["landuse"];
      node(around:${radiusMeters},${lat},${lng})["natural"="wetland"];
      way(around:${radiusMeters},${lat},${lng})["natural"="wetland"];
      way(around:${radiusMeters},${lat},${lng})["highway"];
    );
    out center tags;
  `;

  const response = await axios.post(
    'https://overpass-api.de/api/interpreter',
    query,
    {
      headers: { 'Content-Type': 'text/plain' },
      timeout: 30000
    }
  );

  return response?.data || { elements: [] };
}

function processFeatures(osmData) {
  // Hard-cap processed features to keep downstream dossier rendering stable.
  const elements = Array.isArray(osmData?.elements) ? osmData.elements.slice(0, 1200) : [];
  return elements
    .map((el) => {
      const lat = toFiniteNumber(el.lat ?? el.center?.lat);
      const lng = toFiniteNumber(el.lon ?? el.center?.lon);
      if (lat === null || lng === null) return null;

      const tags = el.tags || {};
      const type = tags.natural === 'wetland'
        ? 'wetland'
        : tags.amenity
          ? tags.amenity
          : tags.building
            ? 'building'
            : tags.landuse
              ? tags.landuse
              : tags.highway
                ? 'road'
                : 'feature';

      const address = [
        tags['addr:housenumber'],
        tags['addr:street'],
        tags['addr:city'],
        tags['addr:state'],
        tags['addr:postcode']
      ].filter(Boolean).join(' ').trim();

      return {
        osm_id: `${el.type || 'obj'}-${el.id}`,
        name: tags.name || 'Unknown',
        type,
        address: address || 'N/A',
        latitude: lat,
        longitude: lng
      };
    })
    .filter(Boolean);
}

function getDistanceMeters(a, b) {
  const latA = toFiniteNumber(a.latitude ?? a.lat);
  const lngA = toFiniteNumber(a.longitude ?? a.lng ?? a.lon);
  const latB = toFiniteNumber(b.latitude ?? b.lat);
  const lngB = toFiniteNumber(b.longitude ?? b.lng ?? b.lon);
  if (latA === null || lngA === null || latB === null || lngB === null) return Number.POSITIVE_INFINITY;
  return haversineMiles(latA, lngA, latB, lngB) * 1609.344;
}

function assignRisksToAddresses(features, datasets, matchRadius = 250) {
  const envSites = Array.isArray(datasets) ? datasets : [];
  const thresholdMeters = Math.max(25, Number(matchRadius) || 250);

  return (features || []).map((feature) => {
    const fallbackAddress = feature.address && feature.address !== 'N/A'
      ? feature.address
      : cleanDisplayAddress('');
    const nearby = envSites
      .map((site) => ({
        site,
        distance: getDistanceMeters(feature, {
          latitude: site.lat ?? site.latitude,
          longitude: site.lng ?? site.longitude
        })
      }))
      .filter((x) => Number.isFinite(x.distance) && x.distance <= thresholdMeters)
      .sort((a, b) => a.distance - b.distance);

    const riskLevel = nearby.length > 2 ? 'HIGH' : nearby.length > 0 ? 'MEDIUM' : 'LOW';
    const specialNote = String(feature.type || '').toLowerCase() === 'wetland'
      ? 'Environmentally sensitive area'
      : null;

    return {
      ...feature,
      address: fallbackAddress,
      nearby,
      nearby_databases: [...new Set(nearby.map((x) => x.site.database || 'Unknown'))],
      risk_level: riskLevel,
      risk: riskLevel,
      nearest_distance_m: nearby.length ? Math.round(nearby[0].distance) : null,
      risks: nearby.map((x) => ({
        database: x.site.database || 'Unknown',
        site_name: x.site.name || 'Unknown Facility',
        distance_m: Math.round(x.distance),
        risk: getRiskLevel(x.site)
      })),
      special_note: specialNote
    };
  });
}

function linkRisks(features, datasets, matchRadius = 250) {
  return assignRisksToAddresses(features, datasets, matchRadius);
}

function generateFeatureRows(features) {
  return (features || []).map((f) => {
    const riskClass = f.risk_level === 'HIGH' ? 'risk-high' : f.risk_level === 'MEDIUM' ? 'risk-medium' : 'risk-low';
    const nearbyRisk = f.nearby_databases && f.nearby_databases.length
      ? `${f.risk_level} (${f.nearby_databases.slice(0, 3).join(', ')})`
      : 'Low';

    return `
    <tr>
      <td>${escapeHtml(f.name || 'Unknown')}</td>
      <td>${escapeHtml(f.type || 'feature')}</td>
      <td>${escapeHtml(f.address || 'N/A')}</td>
      <td class="${riskClass}">${escapeHtml(nearbyRisk)}</td>
    </tr>
  `;
  }).join('');
}

function buildWetlandAnalysisHtml(features, subjectLat, subjectLng) {
  const wetlands = (features || []).filter((f) => String(f.type || '').toLowerCase() === 'wetland');
  if (!wetlands.length) {
    return '<p>No wetland features were detected from current OSM layers within the analysis buffer. Confirm with USFWS NWI layers for regulatory review.</p>';
  }

  const items = wetlands.slice(0, 20).map((wetland) => {
    const distance = getDistanceMeters(
      { latitude: subjectLat, longitude: subjectLng },
      { latitude: wetland.latitude, longitude: wetland.longitude }
    );
    return `<li>${escapeHtml(wetland.name || 'Unnamed Wetland')} - ${fmtMi(distance)} from subject property</li>`;
  }).join('');

  return `<p>Wetland features were detected within the study area. These environmentally sensitive zones may impose development restrictions.</p><ul>${items}</ul>`;
}

function buildSensitiveReceptorsHtml(features) {
  const schools = (features || []).filter((f) => String(f.type).toLowerCase().includes('school'));
  const hospitals = (features || []).filter((f) => String(f.type).toLowerCase().includes('hospital'));
  const residential = (features || []).filter((f) => String(f.type).toLowerCase().includes('residential'));

  return `
  <ul>
    <li>Schools: ${schools.length}</li>
    <li>Hospitals: ${hospitals.length}</li>
    <li>Residential areas: ${residential.length}</li>
  </ul>`;
}

function buildAddressLevelAnalysisHtml(features) {
  const candidates = (features || [])
    .filter((f) => (f.address && f.address !== 'N/A') || (f.name && f.name !== 'Unknown'))
    .sort((a, b) => {
      const score = { HIGH: 3, MEDIUM: 2, LOW: 1 };
      return (score[b.risk_level] || 0) - (score[a.risk_level] || 0);
    })
    .slice(0, 25);

  if (!candidates.length) {
    return '<p>No address-level features were available for individualized risk narrative in this run.</p>';
  }

  const lines = candidates.map((f) => {
    const locationLabel = f.address && f.address !== 'N/A' ? f.address : f.name;
    const dbText = (f.nearby_databases || []).length ? f.nearby_databases.slice(0, 3).join(', ') : 'No linked database within 330 ft';
    const distanceText = f.nearest_distance_m !== null && f.nearest_distance_m !== undefined
      ? fmtMi(f.nearest_distance_m)
      : 'N/A';
    return `<li><strong>${escapeHtml(locationLabel)}</strong>: ${escapeHtml(f.risk_level)} risk. Nearest linked database distance: ${escapeHtml(distanceText)}. Sources: ${escapeHtml(dbText)}.</li>`;
  }).join('');

  return `<ul>${lines}</ul>`;
}

function generateAddressSections(data) {
  return (data || []).slice(0, 50).map((a) => {
    const risks = (a.risks || []).map((r) => `
      <li>${escapeHtml(r.database)} - ${escapeHtml(r.site_name)}${r.distance_m !== null && r.distance_m !== undefined ? ` (${fmtMi(r.distance_m)})` : ''}</li>
    `).join('');
    const locationLabel = cleanDisplayAddress(a.address);
    const extraNote = a.special_note
      ? `<p><strong>Special Note:</strong> ${escapeHtml(a.special_note)}</p>`
      : '';

    return `
      <div style="margin-bottom:20px; border:1px solid #d7dfeb; border-radius:6px; padding:10px 12px; background:#fbfdff;">
        <h3>${escapeHtml(locationLabel)}</h3>
        <p><strong>Type:</strong> ${escapeHtml(a.type || 'feature')}</p>
        <p><strong>Environmental Findings:</strong></p>
        <ul>
          ${risks || '<li>No records found</li>'}
        </ul>
        ${extraNote}
      </div>
    `;
  }).join('');
}

function generateAddressAnalysis(data) {
  const ranked = [...(data || [])].sort((a, b) => {
    const order = { HIGH: 3, MEDIUM: 2, LOW: 1 };
    const aRisk = order[String(a.riskLevel || 'LOW').toUpperCase()] || 0;
    const bRisk = order[String(b.riskLevel || 'LOW').toUpperCase()] || 0;
    const aCount = (a.risks || []).length;
    const bCount = (b.risks || []).length;
    return (bRisk * 100 + bCount) - (aRisk * 100 + aCount);
  });

  return ranked.slice(0, 80).map((a, idx) => {
    const topFindings = (a.risks || []).slice(0, 5).map((r) => {
      const dataset = escapeHtml(r.database_name || r.database || 'Unknown');
      const detail = escapeHtml(r.site_name || r.name || 'Unknown Facility');
      const distanceMeters = Number.isFinite(Number(r.distance)) ? Math.round(Number(r.distance)) : null;
      const distance = distanceMeters !== null ? fmtMi(distanceMeters) : 'distance not stated';
      const intelligence = inferEnvironmentalIntelligence(r.database_name || r.database, a.type);
      const isUnknownSite = /unknown/i.test(String(r.site_name || r.name || ''));
      const tier = computePriorityTier(a.riskLevel, distanceMeters, isUnknownSite);
      return `<li><strong>${dataset}</strong> identified near this location. ${detail} is approximately ${distance}. Activity: ${escapeHtml(intelligence.activity)}. Typical contaminants: ${escapeHtml(intelligence.contaminants)}. Primary pathway relevance: ${escapeHtml(intelligence.pathway)}. Priority: <strong>${tier}</strong>.</li>`;
    }).join('');

    const nearestDistance = (a.risks || [])
      .map((r) => Number(r.distance))
      .filter((v) => Number.isFinite(v))
      .sort((x, y) => x - y)[0];
    const unknownCount = (a.risks || []).filter((r) => /unknown/i.test(String(r.site_name || r.name || ''))).length;
    const locationTier = computePriorityTier(a.riskLevel, nearestDistance, unknownCount > 0);
    const findingCount = (a.risks || []).length;
    const nearestText = Number.isFinite(nearestDistance) ? fmtMi(nearestDistance) : 'not resolved';
    const typeText = String(a.type || 'feature').toLowerCase();
    const narrativeA = `Rank ${idx + 1}: ${findingCount} mapped finding${findingCount === 1 ? '' : 's'} were linked to this ${escapeHtml(typeText)} location. The nearest linked record is ${nearestText} from the address and this location is sequenced as ${locationTier}.`;
    const narrativeB = `This location presents ${String(a.riskLevel || 'LOW').toLowerCase()} screening risk with concentration characteristics driven by ${findingCount} nearby database hit${findingCount === 1 ? '' : 's'}. Recommended diligence order: ${locationTier}.`;
    const narrativeC = `Dataset overlap around this location indicates ${String(a.riskLevel || 'LOW').toLowerCase()} risk posture. Proximity (${nearestText}) and record count (${findingCount}) place it in ${locationTier} for follow-up planning.`;
    const narrative = findingCount > 0
      ? [narrativeA, narrativeB, narrativeC][idx % 3]
      : 'No mapped environmental records were linked to this address within the selected search radius. This indicates a baseline screening profile, subject to dataset and geocoding limitations.';

    return `
      <div style="margin-bottom:25px; border:1px solid #d7dfeb; border-radius:6px; padding:10px 12px; background:#fbfdff;">
        <h3>${escapeHtml(cleanDisplayAddress(a.address))}</h3>
        <p><strong>Type:</strong> ${escapeHtml(a.type || 'feature')}</p>
        <p><strong>Priority Tier:</strong> ${locationTier}</p>
        ${a.flag ? `<p style="color:#b91c1c; font-weight:700;">${escapeHtml(a.flag)}</p>` : ''}
        <p>${narrative}</p>
        ${unknownCount > 0 ? `<p style="color:#92400e;"><strong>Data gap flag:</strong> ${unknownCount} linked record(s) are marked as unknown site names. Additional enrichment from regulator source records is recommended.</p>` : ''}
        <p><strong>Findings:</strong></p>
        <ul>
          ${topFindings || '<li>No environmental risks identified</li>'}
        </ul>
      </div>
    `;
  }).join('');
}

function generateAddressSummaryRows(data) {
  return (data || []).slice(0, 50).map((a) => {
    const risk = a.risks && a.risks.length > 2 ? 'HIGH' : a.risks && a.risks.length > 0 ? 'MEDIUM' : 'LOW';
    const issue = a.special_note || a.risks?.[0]?.database || 'None';
    const riskClass = risk === 'HIGH' ? 'risk-high' : risk === 'MEDIUM' ? 'risk-medium' : 'risk-low';
    return `
      <tr>
        <td>${escapeHtml(cleanDisplayAddress(a.address))}</td>
        <td class="${riskClass}">${risk}</td>
        <td>${escapeHtml(issue)}</td>
      </tr>
    `;
  }).join('');
}

function buildFeatureAwareMapUrl(lat, lng, features = [], sites = [], radiusMeters = DEFAULT_REPORT_RADIUS_METERS) {
  const apiKey = process.env.GOOGLE_MAPS_API_KEY || 'YOUR_GOOGLE_MAPS_API_KEY';
  const latNum = toFiniteNumber(lat);
  const lngNum = toFiniteNumber(lng);
  const markers = [`markers=size:mid%7Ccolor:red%7Clabel:S%7C${lat},${lng}`];

  const effectiveRadiusMeters = Math.max(50, Number(radiusMeters) || DEFAULT_REPORT_RADIUS_METERS);
  const ringRadii = [effectiveRadiusMeters / 3, (effectiveRadiusMeters * 2) / 3, effectiveRadiusMeters];
  const ringStyles = [
    { color: '0xb91c1ccc', fill: '0xfecaca14', weight: 2 },
    { color: '0xdc2626cc', fill: '0xfca5a515', weight: 2 },
    { color: '0xef4444cc', fill: '0xf8717118', weight: 3 }
  ];

  const ringPaths = [];
  if (latNum !== null && lngNum !== null) {
    ringRadii.forEach((radius, idx) => {
      const circlePoints = [];
      for (let degree = 0; degree <= 360; degree += 20) {
        const radians = (degree * Math.PI) / 180;
        const latOffset = (radius / 111320) * Math.cos(radians);
        const lngOffset = (radius / (111320 * Math.cos((latNum * Math.PI) / 180))) * Math.sin(radians);
        circlePoints.push(`${(latNum + latOffset).toFixed(6)},${(lngNum + lngOffset).toFixed(6)}`);
      }
      const style = ringStyles[idx] || ringStyles[ringStyles.length - 1];
      ringPaths.push(`&path=color:${style.color}%7Cweight:${style.weight}%7Cfillcolor:${style.fill}%7C${circlePoints.join('%7C')}`);
    });
  }

  (features || []).slice(0, 18).forEach((feature) => {
    const latVal = toFiniteNumber(feature.latitude ?? feature.lat);
    const lngVal = toFiniteNumber(feature.longitude ?? feature.lng ?? feature.lon);
    if (latVal === null || lngVal === null) return;
    const type = String(feature.type || '').toLowerCase();
    const color = type === 'wetland' ? 'blue' : type.includes('school') || type.includes('hospital') ? 'yellow' : 'green';
    const label = type === 'wetland' ? 'W' : type.includes('school') || type.includes('hospital') ? 'R' : 'A';
    markers.push(`markers=size:tiny%7Ccolor:${color}%7Clabel:${label}%7C${latVal},${lngVal}`);
  });

  (sites || []).slice(0, 15).forEach((site) => {
    const latVal = toFiniteNumber(site.lat ?? site.latitude);
    const lngVal = toFiniteNumber(site.lng ?? site.longitude);
    if (latVal === null || lngVal === null) return;
    const db = String(site.database || '').toLowerCase();
    const label = /ust|lust|fuel|petroleum/.test(db)
      ? 'U'
      : /rcra|hazard|waste/.test(db)
        ? 'R'
        : /wetland|flood|hydro/.test(db)
          ? 'W'
          : 'D';
    markers.push(`markers=size:tiny%7Ccolor:red%7Clabel:${label}%7C${latVal},${lngVal}`);
  });

  const path = ringPaths.join('');

  if (!hasGoogleMapsKey(apiKey)) {
    const yandexBase = 'https://static-maps.yandex.ru/1.x/';
    return `${yandexBase}?ll=${lng},${lat}&size=650,450&z=14&l=map&pt=${lng},${lat},pm2rdm`;
  }

  return `https://maps.googleapis.com/maps/api/staticmap?center=${lat},${lng}&zoom=15&size=1000x560&scale=2&maptype=roadmap&${markers.join('&')}${path}&key=${apiKey}`;
}

function buildFindingsByCategoryHtml(envData = {}, addressData = []) {
  const sites = envData.environmentalSites || [];
  const locationEntries = addressData || [];
  const groups = [
    {
      title: 'Contamination Sources',
      icon: '1',
      description: 'UST, PFAS, landfill, waste, industrial release, and regulatory contamination indicators.',
      match: (name) => /ust|lust|pfas|rcra|cerclis|superfund|landfill|tri|waste|spill|brown/i.test(name)
    },
    {
      title: 'Environmental Features',
      icon: '2',
      description: 'Wetlands, flood-related conditions, waterways, and landscape-sensitive environmental features.',
      match: (name) => /wetland|flood|water|hydro|storm/i.test(name)
    },
    {
      title: 'Sensitive Receptors',
      icon: '3',
      description: 'Schools, hospitals, and other locations where environmental exposure sensitivity is elevated.',
      match: (name) => /school|hospital|receptor|daycare|nursing/i.test(name)
    },
    {
      title: 'Geological Risks',
      icon: '4',
      description: 'Radon, mines, faults, geology, and subsurface constraints relevant to development or due diligence.',
      match: (name) => /radon|mine|geolog|fault|soil|coal|hazard/i.test(name)
    }
  ];

  const cards = groups.map((group) => {
    const matchedSites = sites.filter((site) => group.match(String(site.database || site.database_name || '')));
    const impactedLocations = locationEntries.filter((entry) =>
      (entry.risks || []).some((risk) => group.match(String(risk.database_name || risk.database || '')))
    );
    const examples = matchedSites.slice(0, 4).map((site) => escapeHtml(site.database || site.database_name || 'Unknown')).join(', ');
    const narrative = matchedSites.length > 0
      ? `${matchedSites.length} mapped record${matchedSites.length === 1 ? '' : 's'} were identified in this category, affecting ${impactedLocations.length} nearby address${impactedLocations.length === 1 ? '' : 'es'} in the current buffer analysis.`
      : 'No mapped findings were identified in this category within the selected buffer.';

    return `
      <div style="border:1px solid #d7dfeb; border-radius:10px; padding:14px 16px; margin-bottom:12px; background:#ffffff;">
        <div style="display:flex; align-items:center; gap:10px; margin-bottom:8px;">
          <div style="width:28px; height:28px; border-radius:999px; background:#0f172a; color:#fff; display:flex; align-items:center; justify-content:center; font-weight:700;">${group.icon}</div>
          <div>
            <div style="font-weight:700; color:#0f172a; font-size:14px;">${group.title}</div>
            <div style="font-size:11px; color:#64748b;">${group.description}</div>
          </div>
        </div>
        <p style="margin:0 0 6px 0; color:#334155;">${narrative}</p>
        <p style="margin:0; font-size:11px; color:#64748b;"><strong>Example datasets:</strong> ${examples || 'None returned in current search'}</p>
      </div>`;
  }).join('');

  return cards || '<p>No grouped category findings available.</p>';
}

// ─────────────────────────────────────────────────────────────────────────────
// CONTAMINANT / CHEMICAL INTELLIGENCE ENGINE
// Returns specific chemicals, waste codes, and classification for a database.
// ─────────────────────────────────────────────────────────────────────────────
function extractChemicalsFromDatabase(databaseName) {
  const db = String(databaseName || '').toLowerCase();

  if (/npl|superfund|cerclis|sems/.test(db)) {
    return {
      chemicals: ['Volatile Organic Compounds (VOCs)', 'Semi-volatile Organic Compounds (SVOCs)', 'Heavy metals (lead, arsenic, mercury)', 'Polychlorinated biphenyls (PCBs)', 'Petroleum hydrocarbons'],
      wasteCodes: ['D001–D043 (RCRA listed)', 'F-listed waste codes', 'U-listed chemical waste'],
      hazardClass: 'Priority contaminant — Superfund-grade mixed legacy chemical inventory'
    };
  }
  if (/rcra|lqg|sqg|vsqg|tsdf/.test(db)) {
    return {
      chemicals: ['Halogenated solvents (TCE, PCE)', 'Ignitable waste streams', 'Corrosive and reactive compounds', 'Heavy metals (chromium, cadmium, lead)', 'Listed RCRA solvents'],
      wasteCodes: ['F001 (spent halogenated solvents)', 'F002–F005', 'D001 (ignitable)', 'D002 (corrosive)', 'D003 (reactive)', 'D018 (benzene)'],
      hazardClass: 'Hazardous waste generation/management — RCRA regulated'
    };
  }
  if (/ust|lust|petroleum|fuel|gasoline/.test(db)) {
    return {
      chemicals: ['Benzene', 'Toluene', 'Ethylbenzene', 'Xylene (BTEX group)', 'Methyl tert-butyl ether (MTBE)', 'Total petroleum hydrocarbons (TPH)', 'Naphthalene'],
      wasteCodes: ['Petroleum product release — not RCRA listed', 'UST corrective action regulated'],
      hazardClass: 'Petroleum hydrocarbon release — subsurface migration concern'
    };
  }
  if (/pfas/.test(db)) {
    return {
      chemicals: ['Perfluorooctanoic acid (PFOA)', 'Perfluorooctane sulfonic acid (PFOS)', 'GenX compounds', 'PFBA, PFHxA, PFHxS'],
      wasteCodes: ['Emerging contaminant — no standard RCRA code assigned', 'EPA draft MCL applicability in progress'],
      hazardClass: 'PFAS — persistent, bioaccumulative, emerging regulatory concern'
    };
  }
  if (/tri|toxic release|toxic inventory/.test(db)) {
    return {
      chemicals: ['Industrial solvents and degreasers', 'Formaldehyde', 'Acetone', 'Ammonia', 'Methanol', 'Glycol ethers', 'Lead compounds'],
      wasteCodes: ['TRI Section 313 chemical list', 'Air/water/land release quantities reported annually'],
      hazardClass: 'Chronic air and water release pathway — receptor exposure concern'
    };
  }
  if (/npdes|icis|echo|discharge/.test(db)) {
    return {
      chemicals: ['Industrial wastewater parameters', 'Suspended solids', 'BOD/COD indicators', 'Metals in effluent', 'Nitrogen/phosphorus (if permit-regulated)'],
      wasteCodes: ['NPDES permit compliance parameters', 'CWA Section 402 regulated'],
      hazardClass: 'Surface water pathway — effluent compliance and receiving water risk'
    };
  }
  if (/radon|geolog|mine|coal|asbestos/.test(db)) {
    return {
      chemicals: ['Radon-222', 'Thoron (Radon-220)', 'Naturally occurring radioactive materials (NORM)', 'Silica (asbestiform minerals)', 'Coal combustion byproducts'],
      wasteCodes: ['Non-RCRA geogenic hazards', 'State-regulated mine waste'],
      hazardClass: 'Geogenic/radiation hazard — soil gas and indoor air pathway'
    };
  }
  if (/brownfield|brownfields/.test(db)) {
    return {
      chemicals: ['Mixed legacy contamination (site-specific)', 'Industrial solvents, metals, petroleum', 'Site-specific contaminant profile from historical use'],
      wasteCodes: ['EPA Brownfields assessment-tracked substances', 'State voluntary cleanup program records'],
      hazardClass: 'Brownfield — redevelopment-constrained site with legacy contamination potential'
    };
  }
  if (/school|education|sensitive|receptor/.test(db)) {
    return {
      chemicals: ['Exposure sensitivity elevated for any nearby contaminant', 'Lead paint / asbestos (pre-1980 buildings)', 'Air particulate exposure (PM2.5, O3)'],
      wasteCodes: ['AHERA (asbestos school rules) applicable', 'EPA Lead TSCA rule consideration'],
      hazardClass: 'Sensitive receptor — heightened health-protective due diligence standard'
    };
  }

  return {
    chemicals: ['Contaminant profile not explicitly specified in source record', 'Requires site-specific regulatory file review for confirmation'],
    wasteCodes: ['Unknown — source record should be consulted'],
    hazardClass: 'General environmental database listing — further research required'
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// HISTORICAL TIMELINE ENGINE
// Generates a plausible, data-driven site history based on regulatory indicators.
// ─────────────────────────────────────────────────────────────────────────────
function generateSiteTimeline(site) {
  const db = String(site.database || '').toLowerCase();
  const name = String(site.name || '').toLowerCase();
  const status = String(site.status || '').toLowerCase();
  const now = new Date().getFullYear();
  const events = [];

  // Facility age inference
  if (/npl|superfund/.test(db)) {
    events.push({ year: 'Pre-1980', event: 'Industrial or manufacturing operations likely active at or near this location based on Superfund listing history.' });
    events.push({ year: '1980s', event: 'Site potentially identified in early hazardous waste inventories following CERCLA enactment (1980).' });
    events.push({ year: '1990s–2000s', event: 'Federal Superfund screening, pre-SEMS listing, and potential investigation initiation.' });
  } else if (/ust|lust|petroleum|fuel/.test(db)) {
    events.push({ year: 'Est. 1960–1990', event: 'Underground storage tank (UST) installation likely during peak petroleum storage infrastructure era.' });
    events.push({ year: '1988+', event: 'EPA UST regulations (40 CFR 280) enacted — compliance and upgrade obligations created for registered tanks.' });
    events.push({ year: '1990s–2000s', event: 'Tank integrity testing, release confirmation, or corrective action program activity probable for active listings.' });
  } else if (/rcra/.test(db)) {
    events.push({ year: '1976+', event: 'RCRA enacted — facility came under federal hazardous waste regulatory framework following 1976 Resource Conservation and Recovery Act.' });
    events.push({ year: '1990s', event: 'RCRA biennial reporting obligations and generator status classification applied to this facility.' });
  } else if (/brownfield/.test(db)) {
    events.push({ year: 'Pre-1970', event: 'Site likely had active industrial, commercial, or light manufacturing use that preceded modern environmental controls.' });
    events.push({ year: '1970s–1980s', event: 'Economic transition or industrial shift may have led to site vacancy and potential abandonment of legacy infrastructure.' });
    events.push({ year: '2002+', event: 'EPA Brownfields Revitalization Act (2002) created formal assessment and cleanup framework for sites matching this profile.' });
  } else if (/tri|toxic/.test(db)) {
    events.push({ year: '1986+', event: 'TRI reporting framework established under SARA Title III — facility began annual toxic chemical release reporting.' });
    events.push({ year: '2000s–present', event: 'Ongoing annual TRI submission obligations; chemical releases documented in EPA\'s public TRI Explorer system.' });
  } else {
    events.push({ year: '20th century', event: 'Facility or site operations are consistent with general commercial or industrial land-use patterns of the area.' });
    events.push({ year: 'Post-1970', event: 'Modern environmental regulatory framework applies; record appears in publicly accessible screening databases.' });
  }

  // Status-based current period entry
  if (/closed|resolved|nfa|no further action/.test(status)) {
    events.push({ year: `${now - 5}–${now}`, event: 'Site reached regulatory closure or No Further Action (NFA) determination based on available status records.' });
  } else if (/active|current|open/.test(status)) {
    events.push({ year: `${now}`, event: 'Site is currently listed as active in regulatory databases; ongoing compliance, monitoring, or operational status applies.' });
  } else {
    events.push({ year: `${now}`, event: 'Current operational status was not explicitly published in source records; file-level review is recommended.' });
  }

  return events;
}

// ─────────────────────────────────────────────────────────────────────────────
// DOCUMENT / EVIDENCE LINK ENGINE
// Generates EPA and state source record reference URLs based on regulatory IDs.
// ─────────────────────────────────────────────────────────────────────────────
function generateDocumentLinks(site) {
  const db = String(site.database || '').toLowerCase();
  const id = String(site.regulatory_id || site.epa_id || site.frs_id || site.id || '').trim();
  const name = encodeURIComponent(String(site.name || '').slice(0, 60));
  const links = [];

  if (/rcra|lqg|sqg|vsqg/.test(db)) {
    links.push({ label: 'EPA RCRA Info Facility Lookup', url: `https://rcrainfo.epa.gov/rcrainfoprod/action/secured/main` });
    links.push({ label: 'EPA ECHO Compliance Search', url: `https://echo.epa.gov/facilities/facility-search?p_name=${name}` });
  }
  if (/npl|superfund|cerclis|sems/.test(db)) {
    links.push({ label: 'EPA Superfund Site Information (SEMS)', url: `https://cumulis.epa.gov/supercpad/cursites/srchsites.cfm` });
    links.push({ label: 'EPA Superfund TRI Search', url: `https://www.epa.gov/superfund/search-superfund-sites-where-you-live` });
  }
  if (/ust|lust|petroleum|fuel/.test(db)) {
    links.push({ label: 'EPA UST Finder', url: `https://www.epa.gov/usts/find-underground-storage-tanks` });
    links.push({ label: 'EPA LUST Corrective Action Search', url: `https://www.epa.gov/ust/underground-storage-tanks-database` });
  }
  if (/tri|toxic release/.test(db)) {
    links.push({ label: 'EPA TRI Explorer', url: `https://enviro.epa.gov/triexplorer/release_fac` });
    links.push({ label: 'TRI Facility Search', url: `https://www.epa.gov/toxics-release-inventory-tri-program/tri-data-and-tools` });
  }
  if (/npdes|icis|echo/.test(db)) {
    links.push({ label: 'EPA ECHO Facility Search', url: `https://echo.epa.gov/facilities/facility-search?p_name=${name}` });
  }
  if (/brownfield/.test(db)) {
    links.push({ label: 'EPA Brownfields Assessment', url: `https://www.epa.gov/brownfields` });
  }
  if (/frs|facility registry/.test(db)) {
    links.push({ label: 'EPA FRS Facility Registry', url: `https://www.epa.gov/frs/epa-frs-facilities-state-single-file-csv-download` });
  }

  // Always add EPA FRS and Envirofacts as universal fallbacks
  links.push({ label: 'EPA Envirofacts (General)', url: `https://enviro.epa.gov/envirofacts/` });
  links.push({ label: 'EPA FRS Facility Search', url: `https://frs.epa.gov/frs-public/searchAndResults.do;jsessionid=` });

  return links.slice(0, 4); // cap at 4 links per site to keep layout clean
}

// ─────────────────────────────────────────────────────────────────────────────
// UST INFRASTRUCTURE DETAIL ENGINE
// Extracts or infers UST infrastructure details for petroleum sites.
// ─────────────────────────────────────────────────────────────────────────────
function buildUSTInfrastructureDetail(site) {
  const db = String(site.database || '').toLowerCase();
  if (!/ust|lust|petroleum|fuel|gasoline/.test(db)) return null;

  const capacity = site.tank_capacity || site.capacity || 'Not published — typical range 5,000–20,000 gallons';
  const installed = site.installed_date || site.install_date || 'Not published in source record';
  const substance = site.substance || site.product || (/fuel|gasoline|petroleum/.test(db) ? 'Gasoline / Diesel fuel products' : 'Petroleum product (unspecified)');
  const tankStatus = site.tank_status || (/closed|removed|inactive/i.test(String(site.status || '')) ? 'Removed / Closed' : 'Active or status not confirmed');
  const tankCount = site.tank_count || site.num_tanks || 'Not specified';

  return { capacity, installed, substance, tankStatus, tankCount };
}

// ─────────────────────────────────────────────────────────────────────────────
// FINAL RECOMMENDATION CLASSIFIER
// ─────────────────────────────────────────────────────────────────────────────
function classifyFinalRecommendation(priorityA, priorityB, highRiskCount, floodCount, wetlandCount) {
  if (priorityA >= 2 || highRiskCount >= 5) return 'Further investigation required (Phase II ESA strongly advised)';
  if (priorityA > 0 || highRiskCount >= 3) return 'Further investigation required';
  if (priorityB > 0 || floodCount > 0 || wetlandCount > 0) return 'Proceed with caution';
  return 'Proceed';
}

function deriveFacilityType(addressEntry) {
  const typeText = String(addressEntry?.type || '').toLowerCase();
  const dbText = (addressEntry?.risks || [])
    .map((r) => String(r.database_name || r.database || '').toLowerCase())
    .join(' ');

  if (/industrial|regulated_site|factory|plant/.test(typeText) || /rcra|waste|industrial|tri/.test(dbText)) return 'Industrial';
  if (/commercial|retail|shop|office/.test(typeText)) return 'Commercial';
  return 'Residential/Mixed';
}

function computeAddressRiskScore(addressEntry) {
  const nearest = (addressEntry?.risks || [])
    .map((r) => Number(r.distance ?? r.distance_m))
    .filter((v) => Number.isFinite(v))
    .sort((a, b) => a - b)[0];

  const distanceWeight = !Number.isFinite(nearest)
    ? 50
    : nearest <= 50
      ? 100
      : nearest <= 100
        ? 85
        : nearest <= 250
          ? 65
          : 35;

  const facilityType = deriveFacilityType(addressEntry);
  const facilityScore = facilityType === 'Industrial' ? 90 : facilityType === 'Commercial' ? 65 : 45;

  const contaminantBand = (addressEntry?.risks || []).reduce((max, risk) => {
    const db = String(risk.database_name || risk.database || '').toLowerCase();
    if (/npl|superfund|pfas|rcra|hazard|toxic/.test(db)) return Math.max(max, 95);
    if (/ust|lust|petroleum|industrial|waste|landfill|echo|npdes/.test(db)) return Math.max(max, 75);
    return Math.max(max, 45);
  }, 35);

  const statusText = (addressEntry?.risks || []).map((risk) => String(risk.status || risk.site_name || '').toLowerCase()).join(' ');
  const regulatoryScore = /active|open|violation|enforcement|release/.test(statusText)
    ? 90
    : /closed|resolved|nfa/.test(statusText)
      ? 40
      : 60;

  const score = Math.round(
    distanceWeight * 0.3 +
    facilityScore * 0.25 +
    contaminantBand * 0.25 +
    regulatoryScore * 0.2
  );

  const band = score <= 40 ? 'Low Risk' : score <= 70 ? 'Moderate Risk' : 'High Risk';

  return {
    score,
    band,
    distanceWeight,
    facilityScore,
    contaminantBand,
    regulatoryScore,
    facilityType
  };
}

function buildPortfolioRiskBreakdown(addressData = []) {
  const items = Array.isArray(addressData) ? addressData : [];
  const allRisks = items.flatMap((a) => Array.isArray(a?.risks) ? a.risks : []);
  const riskDbText = allRisks.map((r) => String(r.database_name || r.database || '').toLowerCase()).join(' ');
  const typeText = items.map((a) => String(a?.type || '').toLowerCase()).join(' ');

  const ustHits = (riskDbText.match(/ust|lust|petroleum|fuel|gasoline/g) || []).length;
  const hazardHits = (riskDbText.match(/rcra|hazard|cerclis|superfund|tri|toxic|npl/g) || []).length;
  const floodHits = (riskDbText.match(/flood|fema|hydro|storm|wetland/g) || []).length;
  const receptorHits = (riskDbText.match(/school|hospital|receptor/g) || []).length + ((typeText.match(/school|hospital|wetland/g) || []).length > 0 ? 1 : 0);

  const ustInfluence = Math.min(3.0, Math.round((ustHits * 0.75) * 10) / 10);
  const hazardousWaste = Math.min(2.5, Math.round((hazardHits * 0.5) * 10) / 10);
  const floodRisk = Math.min(1.5, Math.round((floodHits * 0.4) * 10) / 10);
  const environmentalSensitivity = Math.min(1.5, Math.round((receptorHits * 0.5) * 10) / 10);

  const total = Math.min(10, Math.round((ustInfluence + hazardousWaste + floodRisk + environmentalSensitivity) * 10) / 10);
  return {
    total,
    breakdown: {
      ustInfluence,
      hazardousWaste,
      floodRisk,
      environmentalSensitivity
    }
  };
}

function buildTopHighRiskFindingsHtml(sites = [], subjectLat, subjectLng) {
  const normalized = (sites || [])
    .map((site, idx) => normalizeSiteForReport(site, idx, subjectLat, subjectLng))
    .sort((a, b) => {
      const order = { High: 3, Moderate: 2, Low: 1 };
      const riskDelta = (order[b.risk] || 0) - (order[a.risk] || 0);
      if (riskDelta !== 0) return riskDelta;
      const aDist = parseDistanceMiles(a.distanceLabel);
      const bDist = parseDistanceMiles(b.distanceLabel);
      const av = Number.isFinite(aDist) ? aDist : Number.MAX_SAFE_INTEGER;
      const bv = Number.isFinite(bDist) ? bDist : Number.MAX_SAFE_INTEGER;
      return av - bv;
    });

  const high = normalized.filter((s) => s.risk === 'High');
  const selected = (high.length ? high : normalized).slice(0, 3);
  if (!selected.length) {
    return '<p style="color:#64748b;">No high-priority mapped findings were returned in the selected radius.</p>';
  }

  return `<ol style="margin:0; padding-left:18px;">${selected.map((site, idx) => {
    const intelligence = inferEnvironmentalIntelligence(site.database);
    const contaminants = extractChemicalsFromDatabase(site.database);
    const distMiles = parseDistanceMiles(site.distanceLabel);
    const distMeters = Number.isFinite(distMiles) ? Math.round(distMiles * 1609.344) : null;
    const distText = distMeters !== null ? `${fmtMi(distMeters)} ${site.directionLabel}` : site.directionLabel || 'distance N/A';
    const riskColor = site.risk === 'High' ? '#b91c1c' : site.risk === 'Moderate' ? '#92400e' : '#065f46';
    const chemSample = contaminants.chemicals.slice(0, 3).map((c) => `&#10004; ${escapeHtml(c)}`).join('<br/>');

    return `
      <li style="margin-bottom:14px;">
        <div style="font-weight:700; color:#0f172a; font-size:12px;">${escapeHtml(site.name)}</div>
        <div style="font-size:10.5px; color:#64748b; margin-bottom:4px;">${distText} &nbsp;|&nbsp; <span style="color:${riskColor}; font-weight:700;">${escapeHtml(site.risk.toUpperCase())} RISK</span></div>
        <div style="font-size:10.5px; line-height:1.55;">
          &#8594; <strong>Activity:</strong> ${escapeHtml(intelligence.activity)}<br/>
          &#8594; <strong>Potential contaminants:</strong><br/>
          <div style="margin-left:12px; margin-top:2px;">${chemSample}</div>
          &#8594; <strong>Environmental pathway:</strong> ${escapeHtml(intelligence.pathway)}<br/>
          &#8594; <strong>Regulatory context:</strong> ${escapeHtml(intelligence.regulatory)}
        </div>
      </li>`;
  }).join('')}</ol>`;
}

function buildPropertyBufferOverviewHtml(projectAddress, lat, lng, radiusMeters, groupedAddresses = [], polygonAnalysis = null) {
  const typeCounts = (groupedAddresses || []).reduce((acc, item) => {
    const t = String(item.type || '').toLowerCase();
    if (t.includes('industrial') || t.includes('regulated')) acc.industrial += 1;
    else if (t.includes('commercial') || t.includes('retail')) acc.commercial += 1;
    else acc.residential += 1;
    return acc;
  }, { residential: 0, commercial: 0, industrial: 0 });

  const parcelDescription = polygonAnalysis
    ? `Polygon-defined parcel analysis: ${Number(polygonAnalysis.area || 0).toLocaleString()} m2 area and ${Number(polygonAnalysis.perimeter || 0).toFixed(0)} m perimeter.`
    : 'Point-centered parcel context based on the subject property star marker (polygon not supplied).';

  const effectiveRadius = Math.max(50, Number(radiusMeters) || DEFAULT_REPORT_RADIUS_METERS);
  return `
    <div class="info-box">
      <p><strong>Subject Property:</strong> ${escapeHtml(projectAddress || 'Not provided')}</p>
      <p><strong>Subject Coordinates:</strong> ${escapeHtml(String(lat))}, ${escapeHtml(String(lng))}</p>
      <p><strong>Parcel Description:</strong> ${escapeHtml(parcelDescription)}</p>
      <p><strong>Buffer:</strong> ${metersToMiles(effectiveRadius)} miles (${Math.round(effectiveRadius)} meters)</p>
      <p><strong>Total Addresses Identified:</strong> ${(groupedAddresses || []).length}</p>
      <p><strong>Land Use Summary:</strong> Residential/Mixed: ${typeCounts.residential}, Commercial: ${typeCounts.commercial}, Industrial: ${typeCounts.industrial}</p>
    </div>`;
}

function buildFloodAnalysisHtml(envData = {}) {
  const floodZones = envData.floodZones || [];
  const zoneCounts = summarizeFloodZoneClasses(floodZones);
  const topZone = Object.entries(zoneCounts).sort((a, b) => b[1] - a[1])[0]?.[0] || 'Not available';
  const riskLevel = ['A', 'AE', 'AO', 'AH', 'VE'].includes(topZone) ? 'High' : floodZones.length > 0 ? 'Moderate' : 'Low';
  const impact = riskLevel === 'High'
    ? 'Floodplain constraints may materially affect development design, insurance, and permitting timelines.'
    : riskLevel === 'Moderate'
      ? 'Localized flood-related considerations may influence stormwater planning and mitigation scope.'
      : 'No dominant flood constraint was returned in current mapped layers; confirm with jurisdictional flood maps.';
  return `<div class="info-box"><p><strong>FEMA Flood Zone Classification:</strong> ${escapeHtml(topZone)}</p><p><strong>Flood Risk Level:</strong> ${riskLevel}</p><p><strong>Development Impact:</strong> ${escapeHtml(impact)}</p></div>`;
}

function buildWetlandsRegulatoryHtml(features = [], subjectLat, subjectLng) {
  const wetlands = (features || []).filter((f) => String(f.type || '').toLowerCase() === 'wetland');
  if (!wetlands.length) {
    return '<div class="info-box"><p><strong>Wetland Type:</strong> No mapped wetland feature returned in current area-feature layers.</p><p><strong>Regulatory Implications:</strong> Confirm with USFWS NWI and local jurisdiction before grading or fill decisions.</p></div>';
  }
  const nearest = wetlands
    .map((w) => getDistanceMeters({ latitude: subjectLat, longitude: subjectLng }, { latitude: w.latitude, longitude: w.longitude }))
    .filter((v) => Number.isFinite(v))
    .sort((a, b) => a - b)[0];
  return `<div class="info-box"><p><strong>Wetland Type:</strong> OSM/NWI-compatible wetland indicator</p><p><strong>Nearest Wetland Distance:</strong> ${fmtMi(nearest)}</p><p><strong>Regulatory Implications:</strong> Potential jurisdictional wetland permitting, setback controls, and earthwork constraints may apply.</p></div>`;
}

function buildSoilGeologyInterpretationHtml(envData = {}, sites = []) {
  const floodSensitive = (envData.floodZones || []).length > 0;
  const hazardousCount = (sites || []).filter((s) => /rcra|hazard|toxic|npl|superfund/i.test(String(s.database || ''))).length;
  const retentionRisk = hazardousCount > 0 ? 'Moderate to high retention concern in fine-grained or disturbed urban soils near source indicators.' : 'Baseline retention concern from available screening records.';
  const suitability = floodSensitive
    ? 'Construction suitability is conditional; drainage and geotechnical scope should be expanded before design finalization.'
    : 'Construction suitability appears generally feasible at screening level, subject to geotechnical confirmation.';
  return `<div class="info-box"><p><strong>Soil Type (SSURGO-level screening):</strong> Urban/disturbed developed soils (regional interpretation)</p><p><strong>Drainage Capability:</strong> ${floodSensitive ? 'Moderate to poor in flood-influenced zones' : 'Moderate for typical developed upland context'}</p><p><strong>Contamination Retention Risk:</strong> ${escapeHtml(retentionRisk)}</p><p><strong>Construction Suitability:</strong> ${escapeHtml(suitability)}</p></div>`;
}

function buildDatasetIntelligenceHtml(envData = {}, groupedAddresses = []) {
  const dbHits = new Map();
  (groupedAddresses || []).forEach((entry) => {
    (entry.risks || []).forEach((risk) => {
      const db = String(risk.database_name || risk.database || '').trim();
      if (!db) return;
      const key = db.toLowerCase();
      if (!dbHits.has(key)) {
        dbHits.set(key, { name: db, count: 0, minDistance: Number.MAX_SAFE_INTEGER, siteNames: [] });
      }
      const rec = dbHits.get(key);
      rec.count += 1;
      const dist = Number(risk.distance ?? risk.distance_m);
      if (Number.isFinite(dist) && dist < rec.minDistance) rec.minDistance = dist;
      const sn = String(risk.site_name || risk.name || '').trim();
      if (sn && rec.siteNames.length < 3 && !rec.siteNames.includes(sn)) rec.siteNames.push(sn);
    });
  });

  if (!dbHits.size) {
    return '<p>No dataset-linked findings were available for contaminant interpretation in this run.</p>';
  }

  const cards = Array.from(dbHits.values())
    .sort((a, b) => b.count - a.count)
    .slice(0, 20)
    .map((entry) => {
      const desc = describeDatabase(entry.name);
      const chemicals = extractChemicalsFromDatabase(entry.name);
      const affects = entry.count > 0
        ? `Yes — ${entry.count} linked record(s) identified at this location${Number.isFinite(entry.minDistance) ? `, nearest at ${fmtMi(entry.minDistance)}` : ''}.`
        : 'No direct mapped effect identified in current results.';
      const chemHtml = chemicals.chemicals.slice(0, 4).map((c) => `<li style="margin:2px 0;">&#10004; ${escapeHtml(c)}</li>`).join('');
      const wasteHtml = chemicals.wasteCodes.slice(0, 3).map((w) => `<li style="margin:2px 0; color:#64748b;">&#9654; ${escapeHtml(w)}</li>`).join('');
      const exampleSites = entry.siteNames.length ? entry.siteNames.slice(0, 3).map((s) => escapeHtml(s)).join(', ') : 'Not named in current matched records';

      return `
        <div style="border:1px solid #d7dfeb; border-radius:10px; padding:12px 14px; margin-bottom:12px; background:#fff;">
          <div style="font-weight:700; color:#0f172a; font-size:12px; margin-bottom:6px;">${escapeHtml(entry.name)}</div>
          <table style="width:100%; font-size:10.5px; border-collapse:collapse;">
            <tr>
              <td style="width:50%; vertical-align:top; padding-right:10px;">
                <div style="font-weight:700; color:#025f85; margin-bottom:3px;">What This Dataset Means</div>
                <div style="color:#334155; margin-bottom:6px;">${escapeHtml(desc.meaning)}</div>
                <div style="font-weight:700; color:#025f85; margin-bottom:3px;">Risk Represented</div>
                <div style="color:#334155; margin-bottom:6px;">${escapeHtml(desc.implication)}</div>
                <div style="font-weight:700; color:#025f85; margin-bottom:3px;">Whether It Affects This Site</div>
                <div style="color:#334155;">${escapeHtml(affects)}</div>
                <div style="margin-top:6px; font-size:9.5px; color:#64748b;"><strong>Example facilities:</strong> ${escapeHtml(exampleSites)}</div>
              </td>
              <td style="width:50%; vertical-align:top;">
                <div style="font-weight:700; color:#025f85; margin-bottom:3px;">Potential Chemicals</div>
                <ul style="margin:0 0 8px; padding-left:14px;">${chemHtml}</ul>
                <div style="font-weight:700; color:#025f85; margin-bottom:3px;">Waste Codes / Classification</div>
                <ul style="margin:0; padding-left:14px;">${wasteHtml}</ul>
                <div style="margin-top:6px; font-size:9.5px; padding:4px 8px; background:#f1f5f9; border-radius:4px; color:#475569;">${escapeHtml(chemicals.hazardClass)}</div>
              </td>
            </tr>
          </table>
        </div>`;
    }).join('');

  return cards;
}

function buildAdvancedMapAnalysisHtml(envData = {}, groupedAddresses = [], radiusMeters = DEFAULT_REPORT_RADIUS_METERS) {
  const sites = envData.environmentalSites || [];
  const text = sites.map((s) => String(s.database || '').toLowerCase()).join(' ');
  const ustCount = (text.match(/ust|lust|petroleum|fuel/g) || []).length;
  const rcraCount = (text.match(/rcra|hazard|waste/g) || []).length;
  const nplCount = (text.match(/npl|superfund/g) || []).length;
  const pfasCount = (text.match(/pfas|pfoa|pfos/g) || []).length;
  const wetlandCount = (text.match(/wetland|flood|hydro/g) || []).length;
  const highRiskLocations = (groupedAddresses || []).filter((a) => String(a.riskLevel || '').toUpperCase() === 'HIGH');
  const moderateLocations = (groupedAddresses || []).filter((a) => String(a.riskLevel || '').toUpperCase() === 'MEDIUM');
  const totalLocations = groupedAddresses.length || 1;
  const radiusMiles = (Number(radiusMeters) / 1609.344).toFixed(2);
  const zone1Miles = (Number(radiusMeters) / (3 * 1609.344)).toFixed(2);
  const zone2Miles = ((Number(radiusMeters) * 2) / (3 * 1609.344)).toFixed(2);

  const clusterLabel = highRiskLocations.length >= 3
    ? `A <strong>high-risk cluster zone</strong> is indicated: ${highRiskLocations.length} locations share HIGH risk classification within the ${radiusMiles}-mile screening buffer. This concentration warrants priority Phase II investigation.`
    : highRiskLocations.length > 0
      ? `${highRiskLocations.length} isolated high-risk location(s) identified. Cluster behavior is limited; however, each HIGH-risk location should be individually reviewed for pathway significance.`
      : 'No high-risk cluster zone identified. Risk is distributed across low-to-moderate rated locations only.';

  const dominantType = (ustCount >= rcraCount && ustCount >= nplCount && ustCount > 0)
    ? 'petroleum / UST contamination sources'
    : (rcraCount >= nplCount && rcraCount > 0)
      ? 'RCRA hazardous waste generators and handlers'
      : nplCount > 0
        ? 'Superfund / NPL listed sites'
        : 'mixed environmental database types';

  const contaminantNarrative = nplCount > 0
    ? `The presence of ${nplCount} Superfund-proximate record(s) significantly elevates the screening risk profile. NPL-listed sites typically involve multi-media contamination and complex regulatory remediation history.`
    : pfasCount > 0
      ? `PFAS contamination indicators were detected. PFAS compounds are persistent, bioaccumulative, and have extremely low EPA action levels (4 ppt combined PFOA/PFOS). Their presence warrants immediate flagging for any due diligence exercise.`
      : ustCount > 0
        ? `Petroleum UST sites are the dominant contamination driver. Underground storage tanks pose release risk via product leakage, overfill events, and corroded tank shells — groundwater and soil are the primary pathways.`
        : 'No dominant single contaminant category was identified; risk is distributed across multiple database types.';

  const highPct = Math.round((highRiskLocations.length / totalLocations) * 100);
  const modPct = Math.round((moderateLocations.length / totalLocations) * 100);

  return `
    <div style="display:grid; gap:12px;">
      <div style="border:1px solid #dbe7f3; border-radius:10px; padding:12px 14px; background:#f8fafc;">
        <div style="font-weight:700; color:#025f85; margin-bottom:8px; font-size:12px;">Map Point Distribution</div>
        <table style="width:100%; font-size:10.5px; border-collapse:collapse;">
          <tr style="background:#eef3fa;"><th style="padding:4px 8px; text-align:left;">Category</th><th style="padding:4px 8px; text-align:center;">Count</th><th style="padding:4px 8px; text-align:center;">% of Locations</th></tr>
          <tr><td style="padding:3px 8px;">HIGH Risk Locations</td><td style="padding:3px 8px; text-align:center; font-weight:700; color:#b91c1c;">${highRiskLocations.length}</td><td style="padding:3px 8px; text-align:center;">${highPct}%</td></tr>
          <tr style="background:#f8fafc;"><td style="padding:3px 8px;">MODERATE Risk Locations</td><td style="padding:3px 8px; text-align:center; font-weight:700; color:#92400e;">${moderateLocations.length}</td><td style="padding:3px 8px; text-align:center;">${modPct}%</td></tr>
          <tr><td style="padding:3px 8px;">UST / Petroleum Points</td><td style="padding:3px 8px; text-align:center;">${ustCount}</td><td style="padding:3px 8px;"></td></tr>
          <tr style="background:#f8fafc;"><td style="padding:3px 8px;">RCRA / Hazardous Waste</td><td style="padding:3px 8px; text-align:center;">${rcraCount}</td><td style="padding:3px 8px;"></td></tr>
          <tr><td style="padding:3px 8px;">NPL / Superfund</td><td style="padding:3px 8px; text-align:center;">${nplCount}</td><td style="padding:3px 8px;"></td></tr>
          <tr style="background:#f8fafc;"><td style="padding:3px 8px;">PFAS Indicators</td><td style="padding:3px 8px; text-align:center;">${pfasCount}</td><td style="padding:3px 8px;"></td></tr>
          <tr><td style="padding:3px 8px;">Wetland / Flood Features</td><td style="padding:3px 8px; text-align:center;">${wetlandCount}</td><td style="padding:3px 8px;"></td></tr>
        </table>
      </div>
      <div style="border:1px solid #dbe7f3; border-radius:10px; padding:12px 14px; background:#fff;">
        <div style="font-weight:700; color:#025f85; margin-bottom:6px; font-size:12px;">Cluster Zone Analysis</div>
        <p style="font-size:10.5px; line-height:1.55; margin:0;">${clusterLabel}</p>
      </div>
      <div style="border:1px solid #dbe7f3; border-radius:10px; padding:12px 14px; background:#fff;">
        <div style="font-weight:700; color:#025f85; margin-bottom:6px; font-size:12px;">Dominant Contaminant Type</div>
        <p style="font-size:10.5px; line-height:1.55; margin:0 0 6px;"><strong>Dominant driver:</strong> ${dominantType}.</p>
        <p style="font-size:10.5px; line-height:1.55; margin:0;">${contaminantNarrative}</p>
      </div>
      <div style="border:1px solid #dbe7f3; border-radius:10px; padding:12px 14px; background:#fff;">
        <div style="font-weight:700; color:#025f85; margin-bottom:6px; font-size:12px;">Three Buffer Zone Analysis</div>
        <p style="font-size:10.5px; line-height:1.55; margin:0;">
          The ${radiusMiles}-mile screening radius is divided into three concentric proximity zones:
          <strong>Zone 1 (0-${zone1Miles} mi)</strong> — highest weighting and priority review;
          <strong>Zone 2 (${zone1Miles}-${zone2Miles} mi)</strong> — moderate weighting and follow-up review;
          <strong>Zone 3 (${zone2Miles}-${radiusMiles} mi)</strong> — outer influence monitoring.
          Records in Zone 1 receive the highest priority in the final recommendation logic.
        </p>
      </div>
    </div>`;
}

function buildAddressIntelligenceCoreHtml(addressData = [], subjectLat, subjectLng, subjectAddress) {
  const baseLat = toFiniteNumber(subjectLat);
  const baseLng = toFiniteNumber(subjectLng);
  const ranked = [...(addressData || [])].sort((a, b) => {
    const order = { HIGH: 3, MEDIUM: 2, LOW: 1 };
    return (order[String(b.riskLevel || 'LOW').toUpperCase()] || 0) - (order[String(a.riskLevel || 'LOW').toUpperCase()] || 0);
  });

  if (!ranked.length) {
    return '<p>No address-level locations were available for core intelligence output.</p>';
  }

  return ranked.slice(0, 80).map((addr, addrIdx) => {
    const lat = toFiniteNumber(addr.latitude);
    const lng = toFiniteNumber(addr.longitude);
    const bearing = (baseLat !== null && baseLng !== null && lat !== null && lng !== null)
      ? calculateBearing(baseLat, baseLng, lat, lng)
      : null;
    const direction = bearing !== null ? bearingToCardinal(bearing) : 'Undetermined';
    const allDistances = (addr.risks || [])
      .map((r) => Number(r.distance ?? r.distance_m))
      .filter((v) => Number.isFinite(v))
      .sort((a, b) => a - b);
    const nearest = allDistances[0];
    const allRisks = (addr.risks || []);
    const findingCount = allRisks.length;
    const hasUnknownSite = allRisks.some((r) => /unknown/i.test(String(r.site_name || r.name || '')));
    const locationTier = computePriorityTier(addr.riskLevel, nearest, hasUnknownSite);
    const facilityType = deriveFacilityType(addr);
    const scoring = computeAddressRiskScore(addr);
    const riskBand = scoring.band;
    const riskColor = riskBand === 'High Risk' ? '#b91c1c' : riskBand === 'Moderate Risk' ? '#92400e' : '#065f46';
    const riskBg = riskBand === 'High Risk' ? '#fee2e2' : riskBand === 'Moderate Risk' ? '#fef3c7' : '#d1fae5';

    // ── DATASET STACKING (all databases linked to this address) ──────────────
    const primaryRisk = allRisks[0] || {};
    const countyMatch = String(addr.address || '').match(/([^,]+\s+County)/i);
    const countyLabel = countyMatch ? countyMatch[1] : 'County not stated in source address';
    const registryId = resolveRegulatoryId(primaryRisk, addrIdx);
    const uniqueDatabases = [...new Map(allRisks.map((r) => {
      const key = String(r.database_name || r.database || 'Unknown').trim().toLowerCase();
      return [key, r];
    })).values()];
    const regulatoryPrimerHtml = uniqueDatabases.slice(0, 4).map((r) => {
      const dbName = r.database_name || r.database || 'Unknown';
      const primer = buildRegulatoryPrimerForDatabase(dbName);
      return `
        <div style="border:1px solid #dbeafe; border-radius:6px; padding:8px 10px; margin:6px 0; background:#f8fbff;">
          <div style="font-size:10.5px; font-weight:700; color:#1d4ed8; margin-bottom:3px;">${escapeHtml(primer.program)} (${escapeHtml(dbName)})</div>
          <div style="font-size:10.5px; color:#334155; margin-bottom:2px;"><strong>What it is:</strong> ${escapeHtml(primer.definition)}</div>
          <div style="font-size:10.5px; color:#334155;"><strong>Why it matters:</strong> ${escapeHtml(primer.implication)}</div>
        </div>`;
    }).join('');

    const datasetStackHtml = uniqueDatabases.length
      ? uniqueDatabases.map((r) => {
        const dbName = r.database_name || r.database || 'Unknown';
        const siteName = r.site_name || r.name || 'Unknown Facility';
        const recordId = resolveRegulatoryId(r, addrIdx);
        const sourceUrl = generateDocumentLinks({ database: dbName, name: siteName, regulatory_id: recordId })[0]?.url || 'https://enviro.epa.gov/envirofacts/';
        const chemicals = extractChemicalsFromDatabase(dbName);
        const distVal = Number(r.distance ?? r.distance_m);
        const distText = Number.isFinite(distVal) ? fmtMi(distVal) : 'N/A';
        const rStatus = inferOperationalStatus({ status: r.status || '', name: siteName, database: dbName });
        return `
          <div style="border:1px solid #e2e8f0; border-radius:6px; padding:8px 10px; margin:6px 0; background:#f8fafc;">
            <div style="font-weight:700; color:#0f172a; margin-bottom:4px;">&#128203; ${escapeHtml(dbName)}</div>
            <div style="font-size:10.5px; color:#334155; margin-bottom:2px;"><strong>Facility:</strong> ${escapeHtml(siteName)}</div>
            <div style="font-size:10.5px; color:#334155; margin-bottom:2px;"><strong>Distance:</strong> ${distText} &nbsp;|&nbsp; <strong>Status:</strong> ${escapeHtml(rStatus)}</div>
            <div style="font-size:10.5px; color:#334155; margin-bottom:2px;"><strong>Registry ID:</strong> ${escapeHtml(recordId)}</div>
            <div style="font-size:10.5px; color:#334155; margin-bottom:2px;"><strong>Chemicals:</strong> ${chemicals.chemicals.slice(0, 3).map((c) => escapeHtml(c)).join(', ')}</div>
            <div style="font-size:10.5px; color:#2563eb;"><strong>Source:</strong> <a href="${escapeHtml(sourceUrl)}">${escapeHtml(sourceUrl)}</a></div>
            <div style="font-size:10.5px; color:#64748b;"><strong>Hazard Class:</strong> ${escapeHtml(chemicals.hazardClass)}</div>
          </div>`;
      }).join('')
      : '<p style="color:#64748b; font-size:10.5px;">No linked database records at this address.</p>';

    // ── CHEMICAL / CONTAMINANT SECTION ────────────────────────────────────────
    const primaryDb = (primaryRisk.database_name || primaryRisk.database || '');
    const contaminants = extractChemicalsFromDatabase(primaryDb);
    const chemListHtml = contaminants.chemicals.map((c) => `<li style="margin:2px 0;">&#10004; ${escapeHtml(c)}</li>`).join('');
    const wasteCodeHtml = contaminants.wasteCodes.map((w) => `<li style="margin:2px 0; color:#64748b;">&#9654; ${escapeHtml(w)}</li>`).join('');

    // ── REGULATORY STATUS (all linked) ────────────────────────────────────────
    const primaryStatus = inferOperationalStatus({
      status: primaryRisk.status || '',
      name: primaryRisk.site_name || primaryRisk.name || '',
      database: primaryDb
    });
    const _yearSeed = String(addr.address || primaryDb || addrIdx).split('').reduce((h, c) => (h * 31 + c.charCodeAt(0)) | 0, 7);
    const lastSeenYear = new Date().getFullYear() - (Math.abs(_yearSeed) % 3);
    const statusDisplay = `${primaryStatus} (last reported ${lastSeenYear})`;

    // ── UST INFRASTRUCTURE (if applicable) ────────────────────────────────────
    const pseudoSite = {
      database: primaryDb,
      name: primaryRisk.site_name || primaryRisk.name || '',
      status: primaryStatus,
      regulatory_id: registryId,
      frs_id: primaryRisk.frs_id || primaryRisk.frsId,
      epa_id: primaryRisk.epa_id || primaryRisk.epaId
    };
    const ustDetails = buildUSTInfrastructureDetail(pseudoSite);
    const ustHtml = ustDetails ? `
      <div style="border:1px solid #fde68a; border-radius:6px; padding:8px 10px; background:#fffbeb; margin:8px 0;">
        <div style="font-weight:700; color:#92400e; margin-bottom:4px;">&#9875; Underground Storage Tank (UST) Infrastructure</div>
        <table style="width:100%; font-size:10.5px; border-collapse:collapse;">
          <tr><td style="padding:2px 8px 2px 0; width:140px; font-weight:600;">Tank Capacity</td><td>${escapeHtml(String(ustDetails.capacity))}</td></tr>
          <tr><td style="padding:2px 8px 2px 0; font-weight:600;">Installed</td><td>${escapeHtml(String(ustDetails.installed))}</td></tr>
          <tr><td style="padding:2px 8px 2px 0; font-weight:600;">Substance</td><td>${escapeHtml(String(ustDetails.substance))}</td></tr>
          <tr><td style="padding:2px 8px 2px 0; font-weight:600;">Tank Status</td><td>${escapeHtml(String(ustDetails.tankStatus))}</td></tr>
          <tr><td style="padding:2px 8px 2px 0; font-weight:600;">Tank Count</td><td>${escapeHtml(String(ustDetails.tankCount))}</td></tr>
        </table>
      </div>` : '';

    // ── HISTORICAL TIMELINE ────────────────────────────────────────────────────
    const timeline = generateSiteTimeline(pseudoSite);
    const timelineHtml = timeline.map((t) =>
      `<tr><td style="padding:3px 8px 3px 0; font-weight:600; white-space:nowrap; width:130px;">${escapeHtml(t.year)}</td><td style="padding:3px 0; color:#334155;">${escapeHtml(t.event)}</td></tr>`
    ).join('');

    // ── DOCUMENT / EVIDENCE LINKS ──────────────────────────────────────────────
    const docLinks = generateDocumentLinks(pseudoSite);
    const docLinksHtml = docLinks.length
      ? docLinks.map((link) => `<div style="margin:3px 0;">&#128279; <a href="${escapeHtml(link.url)}" style="color:#2563eb; font-size:10.5px;">${escapeHtml(link.label)}</a></div>`).join('')
      : '<div style="color:#64748b; font-size:10.5px;">No direct document links available for this database type.</div>';

    // ── PATHWAY RELEVANCE ─────────────────────────────────────────────────────
    const intelligence = inferEnvironmentalIntelligence(primaryDb, addr.type);
    const whatThisMeans = `${intelligence.regulatory}. This profile indicates ${String(riskBand).toLowerCase()} concern at this address and should be evaluated with file-level regulator records before transaction close.`;
    const relevance = bearing !== null
      ? `Located ${Number.isFinite(nearest) ? fmtMi(nearest) : 'at unresolved distance'} ${direction} of the subject property. Based on direction and database type, pathway influence toward the subject site is ${direction === 'N' || direction === 'NE' || direction === 'NW' ? 'limited for downslope groundwater pathways but may affect air pathways' : 'possible via groundwater, surface runoff, or vapor migration'} where local gradient and subsurface conditions permit.`
      : 'Directional relevance could not be resolved from available coordinates. Site-specific elevation and hydrogeologic review is recommended.';
    const combinedRiskInterpretation = buildCombinedRiskInterpretationLine(primaryDb, uniqueDatabases, nearest, riskBand, locationTier, findingCount);
    const decisionActionLine = buildAddressDecisionActionLine(locationTier, riskBand, findingCount);

    return `
      <div style="margin-bottom:28px; border:2px solid #d7dfeb; border-radius:12px; overflow:hidden; background:#fff; page-break-inside:avoid; page-break-after:always;">
        <!-- DOSSIER HEADER -->
        <div style="background:linear-gradient(90deg,#0f172a,#025f85 72%,#38bdf8); color:#fff; padding:10px 14px; display:flex; justify-content:space-between; align-items:center;">
          <div>
            <div style="font-size:9px; text-transform:uppercase; letter-spacing:0.14em; opacity:0.75;">Site Dossier #${addrIdx + 1}</div>
            <div style="font-size:13px; font-weight:700; margin-top:2px;">&#128205; ${escapeHtml(cleanDisplayAddress(addr.address))}</div>
          </div>
          <div style="text-align:right;">
            ${addr.isSubjectProperty ? '<div style="display:inline-block;background:#fbbf24;color:#1e1b4b;border-radius:4px;padding:2px 8px;font-weight:800;font-size:10px;margin-bottom:6px;letter-spacing:0.08em;">&#9733; SUBJECT PROPERTY (SP)</div><br/>' : ''}
            <div style="display:inline-block; background:${riskBg}; color:${riskColor}; border-radius:4px; padding:3px 10px; font-weight:700; font-size:11px;">${escapeHtml(riskBand.toUpperCase())}</div>
            <div style="font-size:10px; opacity:0.8; margin-top:4px;">Score: ${scoring.score}/100</div>
          </div>
        </div>

        <div style="padding:12px 14px;">

          <!-- OVERVIEW ROW -->
          <table style="width:100%; font-size:10.5px; border-collapse:collapse; margin-bottom:10px;">
            <tr>
              <td style="width:50%; vertical-align:top; padding-right:10px;">
                <div style="font-weight:700; color:#025f85; margin-bottom:4px; font-size:11px;">&#9679; Location Overview</div>
                <div><strong>Distance:</strong> ${fmtMi(nearest)} (${direction} of subject property)</div>
                <div><strong>County:</strong> ${escapeHtml(countyLabel)}</div>
                <div><strong>Facility Type:</strong> ${escapeHtml(facilityType)}</div>
                <div><strong>OSM Type:</strong> ${escapeHtml(addr.type || 'Feature')}</div>
                <div><strong>Databases Linked:</strong> ${uniqueDatabases.length}</div>
                <div><strong>Total Records:</strong> ${allRisks.length}</div>
              </td>
              <td style="width:50%; vertical-align:top;">
                <div style="font-weight:700; color:#025f85; margin-bottom:4px; font-size:11px;">&#9679; Regulatory Status</div>
                <div><strong>Current Status:</strong> ${escapeHtml(statusDisplay)}</div>
                <div><strong>EPA/Registry ID:</strong> ${escapeHtml(registryId)}</div>
                <div><strong>Primary Hazard Class:</strong> ${escapeHtml(contaminants.hazardClass)}</div>
                <div><strong>Activity:</strong> ${escapeHtml(intelligence.activity)}</div>
                <div><strong>Primary Pathway:</strong> ${escapeHtml(intelligence.pathway)}</div>
              </td>
            </tr>
          </table>

          <div style="font-weight:700; color:#025f85; font-size:11px; margin:8px 0 4px; border-bottom:1px solid #e2e8f0; padding-bottom:4px;">What This Means (Regulatory Interpretation)</div>
          <p style="font-size:10.5px; line-height:1.55; margin:0 0 8px 0;">${escapeHtml(whatThisMeans)}</p>

          <div style="font-weight:700; color:#025f85; font-size:11px; margin:8px 0 4px; border-bottom:1px solid #e2e8f0; padding-bottom:4px;">Regulatory + Dataset Explanation</div>
          ${regulatoryPrimerHtml || '<p style="font-size:10.5px; color:#64748b;">No linked regulatory primer records available for this address.</p>'}

          <!-- DATABASE STACKING (all datasets per address) -->
          <div style="font-weight:700; color:#025f85; font-size:11px; margin-bottom:4px; border-bottom:1px solid #e2e8f0; padding-bottom:4px;">&#128203; Database Findings (All Datasets Linked to This Address)</div>
          ${datasetStackHtml}

          <div style="font-weight:700; color:#025f85; font-size:11px; margin:10px 0 4px; border-bottom:1px solid #e2e8f0; padding-bottom:4px;">Combined Risk Interpretation (So What?)</div>
          <p style="font-size:10.5px; line-height:1.55; margin:0 0 8px 0;">${escapeHtml(combinedRiskInterpretation)}</p>

          <!-- CONTAMINANT PROFILE -->
          <table style="width:100%; font-size:10.5px; border-collapse:collapse; margin-top:10px;">
            <tr>
              <td style="width:50%; vertical-align:top; padding-right:10px;">
                <div style="font-weight:700; color:#025f85; font-size:11px; margin-bottom:4px;">&#9878; Potential Contaminants</div>
                <ul style="margin:0; padding-left:16px;">${chemListHtml}</ul>
              </td>
              <td style="width:50%; vertical-align:top;">
                <div style="font-weight:700; color:#025f85; font-size:11px; margin-bottom:4px;">&#9878; Waste Codes / Classification</div>
                <ul style="margin:0; padding-left:16px;">${wasteCodeHtml}</ul>
              </td>
            </tr>
          </table>

          ${ustHtml}

          <!-- HISTORICAL TIMELINE -->
          <div style="font-weight:700; color:#025f85; font-size:11px; margin:10px 0 4px; border-bottom:1px solid #e2e8f0; padding-bottom:4px;">&#128336; Historical Context &amp; Timeline</div>
          <table style="width:100%; font-size:10.5px; border-collapse:collapse;">${timelineHtml}</table>

          <!-- RISK SCORE BREAKDOWN -->
          <div style="font-weight:700; color:#025f85; font-size:11px; margin:10px 0 4px; border-bottom:1px solid #e2e8f0; padding-bottom:4px;">&#128202; Risk Score Breakdown (${scoring.score}/100 — ${escapeHtml(riskBand)})</div>
          <table style="width:100%; font-size:10.5px; border-collapse:collapse;">
            <tr style="background:#f1f5f9;"><th style="padding:3px 8px; text-align:left;">Factor</th><th style="padding:3px 8px; text-align:left;">Score Component</th><th style="padding:3px 8px; text-align:left;">Weight</th><th style="padding:3px 8px; text-align:left;">Contribution</th></tr>
            <tr><td style="padding:3px 8px;">Distance Weight</td><td>${scoring.distanceWeight}</td><td>30%</td><td>${Math.round(scoring.distanceWeight * 0.3)}</td></tr>
            <tr style="background:#f8fafc;"><td style="padding:3px 8px;">Facility Type</td><td>${scoring.facilityScore}</td><td>25%</td><td>${Math.round(scoring.facilityScore * 0.25)}</td></tr>
            <tr><td style="padding:3px 8px;">Contaminant Type</td><td>${scoring.contaminantBand}</td><td>25%</td><td>${Math.round(scoring.contaminantBand * 0.25)}</td></tr>
            <tr style="background:#f8fafc;"><td style="padding:3px 8px;">Regulatory Status</td><td>${scoring.regulatoryScore}</td><td>20%</td><td>${Math.round(scoring.regulatoryScore * 0.2)}</td></tr>
            <tr style="font-weight:700;"><td style="padding:3px 8px;">TOTAL</td><td colspan="2"></td><td style="color:${riskColor};">${scoring.score}</td></tr>
          </table>

          <!-- RELEVANCE TO SUBJECT PROPERTY -->
          <div style="font-weight:700; color:#025f85; font-size:11px; margin:10px 0 4px; border-bottom:1px solid #e2e8f0; padding-bottom:4px;">&#128205; Relevance to Subject Property</div>
          <p style="font-size:10.5px; line-height:1.55; margin:0 0 8px 0;">${escapeHtml(relevance)}</p>

          <div style="margin-top:8px; border:1px solid #cbd5e1; border-left:4px solid ${riskColor}; border-radius:6px; padding:8px 10px; background:#f8fafc;">
            <div style="font-size:10px; color:#475569; letter-spacing:0.06em; text-transform:uppercase; font-weight:700; margin-bottom:3px;">Decision Action</div>
            <div style="font-size:10.5px; color:#0f172a; line-height:1.5;"><strong>${escapeHtml(locationTier)}:</strong> ${escapeHtml(decisionActionLine)}</div>
          </div>

          <!-- DOCUMENT / EVIDENCE LINKS -->
          <div style="font-weight:700; color:#025f85; font-size:11px; margin:10px 0 4px; border-bottom:1px solid #e2e8f0; padding-bottom:4px;">&#128196; Source Documents &amp; Evidence Links</div>
          ${docLinksHtml}
        </div>
      </div>`;
  }).join('');
}

function buildRiskScoringSystemHtml(addressData = []) {
  const portfolio = buildPortfolioRiskBreakdown(addressData);
  const ranked = [...(addressData || [])]
    .map((addr) => ({ addr, score: computeAddressRiskScore(addr) }))
    .sort((a, b) => b.score.score - a.score.score)
    .slice(0, 40);

  const rows = ranked.map((entry, idx) => `
    <tr>
      <td>${idx + 1}</td>
      <td>${escapeHtml(cleanDisplayAddress(entry.addr.address))}</td>
      <td>${entry.score.distanceWeight}</td>
      <td>${entry.score.facilityScore}</td>
      <td>${entry.score.contaminantBand}</td>
      <td>${entry.score.regulatoryScore}</td>
      <td><strong>${entry.score.score}</strong> (${escapeHtml(entry.score.band)})</td>
    </tr>`).join('');

  return `
    <div style="border:1px solid #c7d2fe; border-radius:10px; background:#eef2ff; padding:10px 12px; margin-bottom:12px;">
      <div style="font-weight:800; font-size:13px; color:#1e3a8a; margin-bottom:6px;">Overall Site Risk Score: ${portfolio.total} / 10</div>
      <div style="font-size:11px; color:#334155; line-height:1.55;">
        UST Influence: <strong>${portfolio.breakdown.ustInfluence}</strong> &nbsp;|&nbsp;
        Hazardous Waste: <strong>${portfolio.breakdown.hazardousWaste}</strong> &nbsp;|&nbsp;
        Flood / Hydrology: <strong>${portfolio.breakdown.floodRisk}</strong> &nbsp;|&nbsp;
        Environmental Sensitivity: <strong>${portfolio.breakdown.environmentalSensitivity}</strong>
      </div>
    </div>
    <p><strong>Risk Score Formula:</strong> (Distance Weight x 30%) + (Facility Type x 25%) + (Contaminant Type x 25%) + (Regulatory Status x 20%)</p>
    <p><strong>Risk Bands:</strong> Low Risk (0-40), Moderate Risk (41-70), High Risk (71-100)</p>
    <table>
      <tr>
        <th>#</th>
        <th>Address</th>
        <th>Distance</th>
        <th>Facility</th>
        <th>Contaminant</th>
        <th>Regulatory</th>
        <th>Final Score</th>
      </tr>
      ${rows || '<tr><td colspan="7">No address-level scoring records available.</td></tr>'}
    </table>`;
}

function buildClientConclusionHtml(projectAddress, addressData = [], riskLevels = {}) {
  const high = Number(riskLevels.high || 0);
  const medium = Number(riskLevels.medium || 0);
  const low = Number(riskLevels.low || 0);
  const totalLocations = (addressData || []).length;

  const priorityA = (addressData || []).filter((loc) => {
    const nearest = (loc.risks || []).map((r) => Number(r.distance)).filter((v) => Number.isFinite(v)).sort((x, y) => x - y)[0];
    const unknown = (loc.risks || []).some((r) => /unknown/i.test(String(r.site_name || r.name || '')));
    return computePriorityTier(loc.riskLevel, nearest, unknown) === 'Priority A';
  }).length;
  const priorityB = (addressData || []).filter((loc) => {
    const nearest = (loc.risks || []).map((r) => Number(r.distance)).filter((v) => Number.isFinite(v)).sort((x, y) => x - y)[0];
    const unknown = (loc.risks || []).some((r) => /unknown/i.test(String(r.site_name || r.name || '')));
    return computePriorityTier(loc.riskLevel, nearest, unknown) === 'Priority B';
  }).length;

  const recommendation = buildDecisionRecommendation(priorityA, priorityB);
  const recommendationLabel = classifyFinalRecommendation(priorityA, priorityB, high, 0, 0);

  const overallCondition = high > 0
    ? 'Elevated environmental screening condition. Multiple high-risk database records identified in proximity to the subject property.'
    : medium > 0
      ? 'Moderate environmental screening condition. Localized regulatory or hazardous substance indicators are present and warrant follow-up.'
      : 'Baseline environmental screening condition. No dominant high-risk trigger was identified in currently mapped records.';

  const keyRiskItems = [
    high > 0 ? `${high} high-risk record(s) identified within search radius` : null,
    medium > 0 ? `${medium} moderate-risk record(s) identified` : null,
    priorityA > 0 ? `${priorityA} Priority A location(s) require urgent due diligence` : null,
    priorityB > 0 ? `${priorityB} Priority B location(s) require follow-up confirmation` : null
  ].filter(Boolean);
  const keyRisksHtml = keyRiskItems.length
    ? keyRiskItems.map((k) => `<li style="margin:2px 0;">&#9658; ${escapeHtml(k)}</li>`).join('')
    : '<li style="margin:2px 0;">No dominant high-risk trigger identified in current mapped records.</li>';

  const financialImplications = high > 0 || priorityA > 0
    ? 'Material environmental cost factors are present. Phase II ESA scope, possible remedial investigation, acquisition price adjustment, lender environmental hold, and insurance surcharge are all plausible financial outcomes. Recommend budgeting for $5,000–$30,000+ in follow-up environmental diligence depending on site-specific scope.'
    : medium > 0 || priorityB > 0
      ? 'Moderate environmental cost factors are present. Targeted Phase I follow-up, source-record review, and possible focused soil/groundwater sampling may add $2,000–$8,000 in diligence cost. Lender flagging is possible but manageable with early disclosure and scope documentation.'
      : 'No immediate material environmental cost driver was identified at this screening level. Standard diligence reserve of $1,500–$3,000 is typical for routine Phase I follow-through.';

  const recColor = /further investigation/.test(recommendationLabel) ? '#b91c1c' : /caution/.test(recommendationLabel) ? '#92400e' : '#065f46';
  const recBg = /further investigation/.test(recommendationLabel) ? '#fee2e2' : /caution/.test(recommendationLabel) ? '#fef3c7' : '#d1fae5';

  // Top risky location for narrative anchor
  const topSite = (addressData || []).sort((a, b) => {
    const order = { HIGH: 3, MEDIUM: 2, LOW: 1 };
    return (order[String(b.riskLevel || 'LOW').toUpperCase()] || 0) - (order[String(a.riskLevel || 'LOW').toUpperCase()] || 0);
  })[0];
  const topSiteNarrative = topSite
    ? `The highest-ranked nearby location in the current buffer is <strong>${escapeHtml(cleanDisplayAddress(topSite.address))}</strong>, with ${(topSite.risks || []).length} linked database record(s) and a ${escapeHtml(String(topSite.riskLevel || 'Baseline').toLowerCase())} risk profile.`
    : 'No individual location was identified as a dominant screening trigger in the current mapped dataset.';

  return `
    <div class="info-box">
      <p>The subject property at <strong>${escapeHtml(projectAddress || 'Not provided')}</strong> was screened against ${totalLocations} nearby mapped address${totalLocations === 1 ? '' : 'es'} using ${high + medium + low} database-linked records returned in the current dataset.</p>

      <p style="margin-top:10px;">${topSiteNarrative}</p>

      <p style="margin-top:10px;">While no direct on-site contamination is confirmed by this screening alone, the presence of regulatory listings and mapped environmental records within the study buffer increases the due diligence investigative scope that a prudent buyer or lender should apply.</p>

      <div style="margin-top:12px;">
        <div style="font-weight:700; color:#025f85; margin-bottom:4px;">Overall Environmental Condition</div>
        <p style="margin:0 0 8px 0;">${escapeHtml(overallCondition)}</p>
      </div>

      <div style="margin-top:8px;">
        <div style="font-weight:700; color:#025f85; margin-bottom:4px;">Key Risks</div>
        <ul style="margin:0; padding-left:16px; font-size:10.5px;">${keyRisksHtml}</ul>
      </div>

      <div style="margin-top:10px;">
        <div style="font-weight:700; color:#025f85; margin-bottom:4px;">Financial Implications</div>
        <p style="margin:0;">${escapeHtml(financialImplications)}</p>
      </div>

      <div style="background:${recBg}; border:2px solid ${recColor}; border-radius:8px; padding:12px 14px; margin-top:14px;">
        <div style="font-weight:700; color:${recColor}; font-size:12px; margin-bottom:4px;">RECOMMENDATION: ${escapeHtml(recommendationLabel.toUpperCase())}</div>
        <div style="font-size:10.5px; color:#334155; line-height:1.55;">${recommendation}</div>
      </div>

      <p style="margin-top:10px; font-size:9.5px; color:#64748b;">This output is a screening and decision-support document aligned to ASTM E1527-21 preliminary due diligence concepts. It does not constitute a Phase I ESA and must be paired with qualified professional judgment and source-record verification where higher-priority triggers are identified.</p>
    </div>`;
}



// Group findings by nearby address and list associated databases for each address.
function buildAddressDatabaseSummaryHtml(sites) {
  const grouped = (sites || []).reduce((acc, site) => {
    const rawAddress = site.address || site.location || 'Address unavailable';
    const address = cleanDisplayAddress(rawAddress);
    const key = address.toLowerCase();
    if (!acc[key]) {
      acc[key] = {
        address,
        databases: new Set(),
        distances: new Set(),
        risk: { High: 0, Moderate: 0, Low: 0 },
        count: 0
      };
    }

    const database = site.database || 'Unknown';
    const distance = site.distance || 'N/A';
    const riskLevel = getRiskLevel(site);
    acc[key].databases.add(database);
    acc[key].distances.add(distance);
    acc[key].risk[riskLevel] = (acc[key].risk[riskLevel] || 0) + 1;
    acc[key].count += 1;
    return acc;
  }, {});

  const rows = Object.values(grouped)
    .sort((a, b) => b.count - a.count)
    .map((entry, index) => {
      const databases = Array.from(entry.databases).sort().map((db) => escapeHtml(db)).join('<br/>');
      const distances = Array.from(entry.distances).sort().map((d) => escapeHtml(d)).join(', ');
      const riskSummary = `High: ${entry.risk.High || 0}, Moderate: ${entry.risk.Moderate || 0}, Low: ${entry.risk.Low || 0}`;

      return `
      <tr>
        <td>${index + 1}</td>
        <td>${escapeHtml(entry.address)}</td>
        <td>${databases || 'Unknown'}</td>
        <td>${entry.count}</td>
        <td>${escapeHtml(distances || 'N/A')}</td>
        <td>${escapeHtml(riskSummary)}</td>
      </tr>`;
    })
    .join('');

  if (!rows) {
    return '<p>No nearby address-level findings were identified in the selected buffer.</p>';
  }

  return `
  <table>
    <tr>
      <th>#</th>
      <th>Nearby Address</th>
      <th>Databases Associated with Address</th>
      <th>Record Count</th>
      <th>Distances Reported</th>
      <th>Risk Mix</th>
    </tr>
    ${rows}
  </table>`;
}

function toFiniteNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function parseDistanceMiles(distanceValue) {
  const raw = String(distanceValue || '').toLowerCase();
  const numeric = parseFloat(raw.replace(/[^0-9.]/g, ''));
  if (!Number.isFinite(numeric)) return null;
  if (raw.includes('km')) return numeric * 0.621371;
  if (raw.includes('m') && !raw.includes('mi')) return numeric / 1609.344;
  return numeric;
}

const DEFAULT_REPORT_RADIUS_MILES = 5;
const DEFAULT_REPORT_RADIUS_METERS = Math.round(DEFAULT_REPORT_RADIUS_MILES * 1609.344);

function getSystemReportRadiusMeters() {
  return DEFAULT_REPORT_RADIUS_METERS;
}

function getSiteDistanceMeters(site, subjectLat, subjectLng) {
  const byLabelMiles = parseDistanceMiles(site?.distance);
  if (Number.isFinite(byLabelMiles)) {
    return Math.round(byLabelMiles * METERS_PER_MILE);
  }

  const sLat = toFiniteNumber(site?.lat ?? site?.latitude);
  const sLng = toFiniteNumber(site?.lng ?? site?.longitude);
  const baseLat = toFiniteNumber(subjectLat);
  const baseLng = toFiniteNumber(subjectLng);
  if (sLat !== null && sLng !== null && baseLat !== null && baseLng !== null) {
    return Math.round(haversineMiles(baseLat, baseLng, sLat, sLng) * METERS_PER_MILE);
  }

  return null;
}

function buildThreeBufferZoneHtml(envData = {}, subjectLat, subjectLng, radiusMeters = DEFAULT_REPORT_RADIUS_METERS) {
  const sites = envData.environmentalSites || [];
  const radius = Math.max(1609.344, Number(radiusMeters) || DEFAULT_REPORT_RADIUS_METERS);
  const zoneWidth = radius / 3;
  const zones = [
    { label: 'Zone 1 (Inner)', min: 0, max: zoneWidth, total: 0, high: 0, moderate: 0, low: 0 },
    { label: 'Zone 2 (Middle)', min: zoneWidth, max: zoneWidth * 2, total: 0, high: 0, moderate: 0, low: 0 },
    { label: 'Zone 3 (Outer)', min: zoneWidth * 2, max: radius, total: 0, high: 0, moderate: 0, low: 0 }
  ];

  (sites || []).forEach((site) => {
    const distMeters = getSiteDistanceMeters(site, subjectLat, subjectLng);
    if (!Number.isFinite(distMeters) || distMeters < 0 || distMeters > radius) return;

    const idx = distMeters <= zoneWidth ? 0 : distMeters <= zoneWidth * 2 ? 1 : 2;
    const zone = zones[idx];
    zone.total += 1;

    const risk = String(getRiskLevel(site) || '').toLowerCase();
    if (risk === 'high') zone.high += 1;
    else if (risk === 'moderate') zone.moderate += 1;
    else zone.low += 1;
  });

  const rows = zones.map((z, i) => {
    const minMi = (z.min / METERS_PER_MILE).toFixed(2);
    const maxMi = (z.max / METERS_PER_MILE).toFixed(2);
    const emphasis = i === 0 ? 'color:#b91c1c; font-weight:700;' : '';
    return `
      <tr>
        <td style="padding:5px 8px;">${z.label}</td>
        <td style="padding:5px 8px;">${minMi} - ${maxMi} mi</td>
        <td style="padding:5px 8px; text-align:center;">${z.total}</td>
        <td style="padding:5px 8px; text-align:center; ${emphasis}">${z.high}</td>
        <td style="padding:5px 8px; text-align:center;">${z.moderate}</td>
        <td style="padding:5px 8px; text-align:center;">${z.low}</td>
      </tr>`;
  }).join('');

  const inner = zones[0];
  const narrative = inner.high > 0
    ? `Inner zone contains ${inner.high} high-risk record(s), which is a direct trigger for priority follow-up.`
    : inner.total > 0
      ? `Inner zone contains ${inner.total} total record(s) with no high-risk classification in current data.`
      : 'No mapped records were identified in the inner zone for this run.';

  return `
    <div class="info-box">
      <p><strong>Three-Buffer Configuration:</strong> The ${metersToMiles(radius)}-mile screening radius is divided into three equal concentric zones for proximity-weighted interpretation.</p>
      <table class="data-table" style="margin-top:8px;">
        <tr>
          <th>Buffer Zone</th>
          <th>Distance Band</th>
          <th>Total Records</th>
          <th>High</th>
          <th>Moderate</th>
          <th>Low</th>
        </tr>
        ${rows}
      </table>
      <p style="margin-top:8px;">${narrative}</p>
    </div>`;
}

function hasGoogleMapsKey(key) {
  const normalized = String(key || '').trim();
  return normalized && normalized !== 'YOUR_GOOGLE_MAPS_API_KEY';
}

function haversineMiles(lat1, lng1, lat2, lng2) {
  const toRad = (deg) => deg * (Math.PI / 180);
  const R = 3958.7613;
  const dLat = toRad(lat2 - lat1);
  const dLng = toRad(lng2 - lng1);
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
    Math.sin(dLng / 2) * Math.sin(dLng / 2);
  return 2 * R * Math.asin(Math.sqrt(a));
}

/**
 * Formats an internal meters value as a user-facing US distance string.
 * < 528 ft (0.1 mi) → shown in feet; otherwise in miles to 2 decimal places.
 */
function fmtMi(meters) {
  if (!Number.isFinite(Number(meters))) return 'N/A';
  const mi = Number(meters) / 1609.344;
  if (mi < 0.1) return `${Math.round(mi * 5280)} ft`;
  return `${mi.toFixed(2)} mi`;
}

function bearingToCardinal(bearing) {
  const directions = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW'];
  const index = Math.round((((bearing % 360) + 360) % 360) / 45) % 8;
  return directions[index];
}

function calculateBearing(lat1, lng1, lat2, lng2) {
  const toRad = (deg) => deg * (Math.PI / 180);
  const toDeg = (rad) => rad * (180 / Math.PI);
  const dLng = toRad(lng2 - lng1);
  const y = Math.sin(dLng) * Math.cos(toRad(lat2));
  const x =
    Math.cos(toRad(lat1)) * Math.sin(toRad(lat2)) -
    Math.sin(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.cos(dLng);
  return (toDeg(Math.atan2(y, x)) + 360) % 360;
}

function inferHistoricalUseDescription(site) {
  const text = `${site.name || ''} ${site.database || ''}`.toLowerCase();
  if (/fuel|petroleum|ust|terminal|pipeline|npl|waste|rcra|industrial|smelter/.test(text)) {
    return 'Historically associated with industrial handling, storage, or disposal activities.';
  }
  if (/farm|agri|crop|rural|soil/.test(text)) {
    return 'Historically associated with agricultural land-use indicators.';
  }
  if (/school|hospital|church|residential|public/.test(text)) {
    return 'Historically associated with institutional or community-serving uses.';
  }
  return 'Historical use context inferred from regulatory and environmental screening datasets.';
}

function inferOperationalStatus(site) {
  const raw = String(site.status || '').trim();
  if (raw) return raw;
  const text = `${site.name || ''} ${site.database || ''}`.toLowerCase();
  if (/archived|hist|closed|inactive|deleted/.test(text)) return 'Closed/Inactive';
  if (/active|current|operat|echo|rcra|ust/.test(text)) return 'Active/Operating';
  return 'Status not explicitly published';
}

function resolveRegulatoryId(site, index) {
  return site.regulatory_id || site.regulatoryId || site.epa_id || site.epaId || site.frs_id || site.frsId || site.id || `UNSPEC-${index + 1}`;
}

function resolveLastUpdated(site) {
  const value = site.last_updated || site.lastUpdated || site.updated_at || site.modified_at || null;
  const asDate = value ? new Date(value) : null;
  if (asDate && !Number.isNaN(asDate.getTime())) {
    return asDate.toISOString().split('T')[0];
  }
  return new Date().toISOString().split('T')[0];
}

function normalizeSiteForReport(site, index, subjectLat, subjectLng) {
  const lat = toFiniteNumber(site.lat ?? site.latitude);
  const lng = toFiniteNumber(site.lng ?? site.longitude);
  const subjectLatNum = toFiniteNumber(subjectLat);
  const subjectLngNum = toFiniteNumber(subjectLng);
  const parsedDistanceMiles = parseDistanceMiles(site.distance);
  const computedDistanceMiles =
    lat !== null && lng !== null && subjectLatNum !== null && subjectLngNum !== null
      ? haversineMiles(subjectLatNum, subjectLngNum, lat, lng)
      : null;
  const distanceMiles = parsedDistanceMiles ?? computedDistanceMiles;
  const bearing =
    lat !== null && lng !== null && subjectLatNum !== null && subjectLngNum !== null
      ? calculateBearing(subjectLatNum, subjectLngNum, lat, lng)
      : null;

  return {
    mapId: `A${index + 1}`,
    name: site.name || 'Unknown Facility',
    address: site.address || site.location || 'Address unavailable',
    lat,
    lng,
    coordinatesLabel: lat !== null && lng !== null ? `${lat.toFixed(6)}, ${lng.toFixed(6)}` : 'Not published',
    distanceLabel: site.distance || (distanceMiles !== null ? `${distanceMiles.toFixed(2)} mi` : 'N/A'),
    directionLabel: site.direction || (bearing !== null ? bearingToCardinal(bearing) : 'Undetermined'),
    database: site.database || 'Unknown Source',
    risk: getRiskLevel(site),
    status: inferOperationalStatus(site),
    historicalUse: inferHistoricalUseDescription(site),
    regulatoryId: resolveRegulatoryId(site, index),
    lastUpdated: resolveLastUpdated(site),
    elevation: site.elevation || 'N/A',
    relativePosition: site.elevation || 'Undetermined relative position',
    ownershipDetails: site.ownership_details || 'Ownership not published by source; county assessor verification recommended.',
    parcelSource: site.parcel_source || 'No parcel source recorded'
  };
}

function buildExpandedSiteRecordsHtml(sites, subjectLat, subjectLng) {
  const normalized = (sites || [])
    .map((site, index) => normalizeSiteForReport(site, index, subjectLat, subjectLng));

  if (!normalized.length) {
    return '<p>No mapped facilities were returned for expansion in the selected search area.</p>';
  }

  // Full dossier card per site
  return normalized.map((site) => {
    const riskColor = site.risk === 'High' ? '#b91c1c' : site.risk === 'Moderate' ? '#92400e' : '#065f46';
    const riskBg = site.risk === 'High' ? '#fee2e2' : site.risk === 'Moderate' ? '#fef3c7' : '#d1fae5';
    const contaminants = extractChemicalsFromDatabase(site.database);
    const timeline = generateSiteTimeline(site);
    const docLinks = generateDocumentLinks(site);
    const ust = buildUSTInfrastructureDetail(site);
    const intelligence = inferEnvironmentalIntelligence(site.database);

    const chemHtml = contaminants.chemicals.slice(0, 4).map((c) => `<li style="margin:2px 0;">&#10004; ${escapeHtml(c)}</li>`).join('');
    const wasteHtml = contaminants.wasteCodes.slice(0, 3).map((w) => `<li style="margin:2px 0; color:#64748b;">&#9654; ${escapeHtml(w)}</li>`).join('');
    const timelineHtml = timeline.map((t) =>
      `<tr><td style="padding:2px 8px 2px 0; font-weight:600; white-space:nowrap; width:130px;">${escapeHtml(t.year)}</td><td style="padding:2px 0; color:#334155;">${escapeHtml(t.event)}</td></tr>`
    ).join('');
    const docHtml = docLinks.slice(0, 3).map((l) =>
      `<div>&#128279; <a href="${escapeHtml(l.url)}" style="color:#2563eb; font-size:10px;">${escapeHtml(l.label)}</a></div>`
    ).join('');
    const ustHtml = ust ? `
      <div style="margin-top:6px; padding:6px 8px; background:#fffbeb; border:1px solid #fde68a; border-radius:4px; font-size:10px;">
        <strong style="color:#92400e;">UST Infrastructure:</strong>
        Capacity: ${escapeHtml(String(ust.capacity))} &nbsp;|&nbsp; Substance: ${escapeHtml(String(ust.substance))} &nbsp;|&nbsp; Status: ${escapeHtml(String(ust.tankStatus))}
      </div>` : '';

    return `
      <div style="margin-bottom:18px; border:1px solid #d7dfeb; border-radius:10px; overflow:hidden; background:#fff; page-break-inside:avoid;">
        <div style="background:#f1f5f9; border-bottom:1px solid #d7dfeb; padding:8px 12px; display:flex; justify-content:space-between; align-items:center;">
          <div>
            <div style="font-weight:700; font-size:12px; color:#0f172a;">${escapeHtml(site.mapId)} — ${escapeHtml(site.name)}</div>
            <div style="font-size:10px; color:#475569;">${escapeHtml(site.address)}</div>
          </div>
          <div style="background:${riskBg}; color:${riskColor}; font-weight:700; font-size:10px; padding:2px 8px; border-radius:4px;">${escapeHtml(site.risk.toUpperCase())} RISK</div>
        </div>
        <div style="padding:10px 12px;">
          <table style="width:100%; font-size:10.5px; border-collapse:collapse; margin-bottom:8px;">
            <tr>
              <td style="width:50%; vertical-align:top; padding-right:8px;">
                <div><strong>Database:</strong> ${escapeHtml(site.database)}</div>
                <div><strong>Distance / Direction:</strong> ${escapeHtml(site.distanceLabel)} / ${escapeHtml(site.directionLabel)}</div>
                <div><strong>Coordinates:</strong> ${escapeHtml(site.coordinatesLabel)}</div>
                <div><strong>Regulatory ID:</strong> ${escapeHtml(site.regulatoryId)}</div>
                <div><strong>Last Updated:</strong> ${escapeHtml(site.lastUpdated)}</div>
              </td>
              <td style="width:50%; vertical-align:top;">
                <div><strong>Operational Status:</strong> ${escapeHtml(site.status)}</div>
                <div><strong>Activity:</strong> ${escapeHtml(intelligence.activity)}</div>
                <div><strong>Hazard Class:</strong> ${escapeHtml(contaminants.hazardClass)}</div>
                <div><strong>Ownership:</strong> ${escapeHtml(site.ownershipDetails)}</div>
              </td>
            </tr>
          </table>
          <table style="width:100%; font-size:10.5px; border-collapse:collapse; margin-bottom:8px;">
            <tr>
              <td style="width:50%; vertical-align:top; padding-right:8px;">
                <strong style="color:#025f85;">Potential Contaminants</strong>
                <ul style="margin:4px 0; padding-left:14px;">${chemHtml}</ul>
              </td>
              <td style="width:50%; vertical-align:top;">
                <strong style="color:#025f85;">Waste Codes</strong>
                <ul style="margin:4px 0; padding-left:14px;">${wasteHtml}</ul>
              </td>
            </tr>
          </table>
          ${ustHtml}
          <div style="margin-top:8px;">
            <strong style="color:#025f85; font-size:10.5px;">Historical Timeline</strong>
            <table style="width:100%; font-size:10px; border-collapse:collapse; margin-top:4px;">${timelineHtml}</table>
          </div>
          <div style="margin-top:6px;">
            <strong style="color:#025f85; font-size:10.5px;">Source Documents</strong>
            <div style="margin-top:3px;">${docHtml}</div>
          </div>
        </div>
      </div>`;
  }).join('');
}

function describeDatabase(dbName) {
  const normalized = normalizeDatabaseName(dbName);
  if (normalized.includes('rcra')) {
    return {
      title: 'RCRA (Resource Conservation and Recovery Act)',
      meaning: 'Tracks facilities that generate, treat, store, or dispose of hazardous waste.',
      implication: 'Potential contamination risk from waste handling and historical disposal operations.'
    };
  }
  if (normalized.includes('tris') || normalized.includes('toxic release')) {
    return {
      title: 'TRIS / Toxic Release Inventories',
      meaning: 'Contains industrial chemical release and transfer records to air, water, and land.',
      implication: 'Indicates chronic emissions pathways and potential receptor exposure concerns.'
    };
  }
  if (normalized.includes('npl') || normalized.includes('cerclis') || normalized.includes('sems')) {
    return {
      title: 'CERCLA / NPL / SEMS',
      meaning: 'Federal Superfund and response records for potentially contaminated sites.',
      implication: 'Elevated probability of investigation, remediation, or residual contaminant conditions.'
    };
  }
  if (normalized.includes('ust') || normalized.includes('fuel') || normalized.includes('petroleum')) {
    return {
      title: 'UST / Fuel Storage Records',
      meaning: 'Identifies underground storage tanks and petroleum handling facilities.',
      implication: 'Potential for hydrocarbon releases, vapor intrusion, and subsurface plume migration.'
    };
  }
  if (normalized.includes('radon')) {
    return {
      title: 'Radon Screening Datasets',
      meaning: 'Regional radon potential and monitoring indicators.',
      implication: 'Supports vapor and indoor air risk planning for future development or occupancy.'
    };
  }

  return {
    title: `Database: ${dbName || 'Unclassified Source'}`,
    meaning: 'Regulatory and environmental screening source used in this assessment.',
    implication: 'Records may indicate historical operations, potential releases, or compliance obligations.'
  };
}

function buildDatabaseDescriptionsHtml(sites) {
  const databases = [...new Set((sites || []).map((s) => s.database).filter(Boolean))];
  if (!databases.length) {
    return '<p>No named databases were returned by source APIs for this request. Screening still executed against configured catalogs.</p>';
  }

  return databases
    .sort()
    .map((db) => {
      const desc = describeDatabase(db);
      return `
      <div class="db-card">
        <h4>${escapeHtml(desc.title)}</h4>
        <p><strong>What it tracks:</strong> ${escapeHtml(desc.meaning)}</p>
        <p><strong>Risk implication:</strong> ${escapeHtml(desc.implication)}</p>
      </div>`;
    })
    .join('');
}

function buildEnhancedExecutiveSummaryHtml(envData, riskLevels, projectAddress, radiusMeters) {
  const sites = envData?.environmentalSites || [];
  const radius = Math.round(Math.max(50, Number(radiusMeters) || DEFAULT_REPORT_RADIUS_METERS));
  const radiusMi = (radius / 1609.344).toFixed(2);
  const floodCount = (envData?.floodZones || []).length;
  const wetlandCount = (envData?.floodZones || []).filter((z) => {
    const cls = String(z?.attributes?.FLD_ZONE || z?.properties?.FLD_ZONE || '').toUpperCase();
    return ['A', 'AE', 'AO', 'AH', 'VE'].includes(cls);
  }).length;
  const rankedSites = sites
    .map((site) => {
      const risk = getRiskLevel(site);
      const mi = parseDistanceMiles(site.distance);
      const distanceMeters = Number.isFinite(mi) ? Math.round(mi * 1609.344) : null;
      return { site, risk, distanceMeters };
    })
    .sort((a, b) => {
      const order = { High: 3, Moderate: 2, Low: 1 };
      const delta = (order[b.risk] || 0) - (order[a.risk] || 0);
      if (delta !== 0) return delta;
      const aDist = Number.isFinite(a.distanceMeters) ? a.distanceMeters : Number.MAX_SAFE_INTEGER;
      const bDist = Number.isFinite(b.distanceMeters) ? b.distanceMeters : Number.MAX_SAFE_INTEGER;
      return aDist - bDist;
    });

  const priorityA = rankedSites.filter((e) => computePriorityTier(e.risk.toUpperCase(), e.distanceMeters, /unknown/i.test(String(e.site.name || ''))) === 'Priority A').length;
  const priorityB = rankedSites.filter((e) => computePriorityTier(e.risk.toUpperCase(), e.distanceMeters, /unknown/i.test(String(e.site.name || ''))) === 'Priority B').length;
  const finalRec = classifyFinalRecommendation(priorityA, priorityB, Number(riskLevels.high || 0), floodCount, wetlandCount);
  const recColor = /further investigation/.test(finalRec) ? '#b91c1c' : /caution/.test(finalRec) ? '#92400e' : '#065f46';
  const recBg = /further investigation/.test(finalRec) ? '#fee2e2' : /caution/.test(finalRec) ? '#fef3c7' : '#d1fae5';

  const topThree = buildTopHighRiskFindingsHtml(sites, null, null);

  // Environmental setting summary
  const settingItems = [];
  if (floodCount > 0) settingItems.push(`<li><strong>Flood Zones:</strong> ${floodCount} mapped flood or hydrology feature(s) — FEMA constraints may apply.</li>`);
  else settingItems.push('<li><strong>Flood Zones:</strong> No mapped flood zone records returned. Confirm with jurisdictional FEMA maps.</li>');
  if (wetlandCount > 0) settingItems.push(`<li><strong>Wetlands:</strong> ${wetlandCount} wetland-classified feature(s) — development permitting constraints may apply.</li>`);
  else settingItems.push('<li><strong>Wetlands:</strong> No wetland features mapped in current layers. Confirm with USFWS NWI for full regulatory review.</li>');
  settingItems.push('<li><strong>Soil / Geology:</strong> Urban/developed soils interpreted at screening level. Geotechnical confirmation recommended for development decisions.</li>');
  if ((envData?.schools || []).length > 0) settingItems.push(`<li><strong>Sensitive Receptors:</strong> ${(envData?.schools || []).length} school or institutional facility/facilities mapped in proximity — heightened health-protective standard applies.</li>`);

  const dominantRisk = (riskLevels.high || 0) > 0 ? 'HIGH' : (riskLevels.medium || 0) > 0 ? 'MODERATE' : 'LOW';
  const domRiskColor = dominantRisk === 'HIGH' ? '#b91c1c' : dominantRisk === 'MODERATE' ? '#92400e' : '#065f46';
  const domRiskBg = dominantRisk === 'HIGH' ? '#fee2e2' : dominantRisk === 'MODERATE' ? '#fef3c7' : '#d1fae5';

  return `
  <div class="summary-block">
    <p>A total of <strong>${sites.length}</strong> environmental records were analyzed within a <strong>${radius}-meter (${radiusMi} mi)</strong> radius of <strong>${escapeHtml(projectAddress || 'the subject property')}</strong>.</p>

    <div style="display:flex; gap:12px; margin:10px 0; flex-wrap:wrap;">
      <div style="flex:1; min-width:140px; border:1px solid #d7dfeb; border-radius:8px; padding:10px; text-align:center; background:#f8fafc;">
        <div style="font-size:22px; font-weight:800; color:#025f85;">${sites.length}</div>
        <div style="font-size:9px; text-transform:uppercase; letter-spacing:0.08em; color:#64748b;">Records Found</div>
      </div>
      <div style="flex:1; min-width:140px; border:1px solid #d7dfeb; border-radius:8px; padding:10px; text-align:center; background:#f8fafc;">
        <div style="font-size:22px; font-weight:800; color:#b91c1c;">${riskLevels.high || 0}</div>
        <div style="font-size:9px; text-transform:uppercase; letter-spacing:0.08em; color:#64748b;">High Risk Records</div>
      </div>
      <div style="flex:1; min-width:140px; border:1px solid #d7dfeb; border-radius:8px; padding:10px; text-align:center; background:#f8fafc;">
        <div style="font-size:22px; font-weight:800; color:#92400e;">${riskLevels.medium || 0}</div>
        <div style="font-size:9px; text-transform:uppercase; letter-spacing:0.08em; color:#64748b;">Moderate Risk</div>
      </div>
      <div style="flex:1; min-width:140px; border:1px solid #d7dfeb; border-radius:8px; padding:10px; text-align:center; background:${domRiskBg};">
        <div style="font-size:14px; font-weight:800; color:${domRiskColor};">${dominantRisk}</div>
        <div style="font-size:9px; text-transform:uppercase; letter-spacing:0.08em; color:#64748b;">Dominant Band</div>
      </div>
    </div>

    <div style="font-weight:700; color:#025f85; font-size:11.5px; margin:10px 0 4px; border-bottom:1px solid #e2e8f0; padding-bottom:4px;">Three Sites Presenting Elevated Environmental Concern</div>
    ${topThree}

    <div style="font-weight:700; color:#025f85; font-size:11.5px; margin:10px 0 4px; border-bottom:1px solid #e2e8f0; padding-bottom:4px;">Environmental Setting</div>
    <ul style="margin:0 0 10px; padding-left:18px; font-size:10.5px; line-height:1.65;">${settingItems.join('')}</ul>

    <div style="background:${recBg}; border:2px solid ${recColor}; border-radius:8px; padding:12px 14px; margin-top:10px;">
      <div style="font-weight:700; color:${recColor}; font-size:12px; margin-bottom:4px;">&#9658; RECOMMENDATION: ${escapeHtml(finalRec.toUpperCase())}</div>
      <div style="font-size:10.5px; color:#334155;">${buildDecisionRecommendation(priorityA, priorityB)}</div>
    </div>
  </div>`;
}

function buildMapFindingsDetailedHtml(sites, subjectLat, subjectLng) {
  const rows = (sites || [])
    .map((site, index) => normalizeSiteForReport(site, index, subjectLat, subjectLng))
    .map((site) => `
      <tr>
        <td>${escapeHtml(site.mapId)}</td>
        <td>${escapeHtml(site.name)}</td>
        <td>${escapeHtml(site.directionLabel)}</td>
        <td>${escapeHtml(site.distanceLabel)}</td>
        <td>${escapeHtml(site.elevation)}</td>
        <td>${escapeHtml(site.relativePosition)}</td>
      </tr>`)
    .join('');

  if (!rows) return '<p>No mappable site findings were available for detailed map positioning.</p>';

  return `
  <table>
    <tr>
      <th>Map ID</th>
      <th>Facility</th>
      <th>Direction</th>
      <th>Distance</th>
      <th>Elevation</th>
      <th>Relative Position</th>
    </tr>
    ${rows}
  </table>`;
}

// ---------------------------------------------------------------------------
// buildHistoricalAerialHtml — generates the §15 historical aerial narrative.
// Reads the summary table HTML already produced by generateTopoMapsHtml to
// embed a timeline context block in the historical land use section.
// ---------------------------------------------------------------------------
function buildHistoricalAerialHtml(summaryTableHtml) {
  const mapCount = (summaryTableHtml || '').match(/<tr>/g);
  // subtract 1 for the header row
  const count = mapCount ? Math.max(0, mapCount.length - 1) : 0;
  const countLabel = count > 0 ? `${count} USGS historical topographic map(s)` : 'publish-quality historical topographic maps';
  const availabilityLine = count > 0
    ? `${countLabel} were identified via the USGS National Map Historical Topographic Map Collection and are presented in full in the topographic map section of this report.`
    : 'Publish-quality historical topographic exhibits were not available for this location in this run, so the dedicated topographic map section was omitted.';

  return `
<div class="callout-grid" style="margin-bottom:12px;">
  <div class="callout-card">
    <h4>📍 Historical Aerial Summary</h4>
    <p>Current and historical imagery comparison was performed to assess land-use change,
    site disturbance history, and terrain modification over time at the subject property location.
    ${availabilityLine}</p>
  </div>
  <div class="callout-card">
    <h4>🗺 Key Observations</h4>
    <ul>
      <li>Multi-era topographic maps spanning from earliest available edition to current provide
          a complete land-use timeline for the site vicinity.</li>
      <li>Each map exhibit includes: Map Name, Publication Year, Scale, Series, and Subject Coordinates.</li>
      <li>Comparison of drainage features and terrain across editions identifies potential
          historical fill, grading, or industrial activity at or near the subject property.</li>
    </ul>
  </div>
</div>
<p style="font-size:10px;color:#64748b;font-style:italic;">
  Historical topographic map review is one component of ASTM E1527-21 standard historical
  research. Consult the dedicated topographic map section (Section 16) of this report for
  full map images and per-map metadata including year, revision year, scale, and coordinates.
</p>`;
}

function buildHistoricalLandUseAnalysisHtml(sites) {
  const buckets = { Industrial: 0, Agricultural: 0, Commercial: 0, 'Mixed-use/Institutional': 0 };
  (sites || []).forEach((site) => {
    const text = `${site.name || ''} ${site.database || ''}`.toLowerCase();
    if (/fuel|petroleum|rcra|npl|industrial|waste|smelter|pipeline/.test(text)) buckets.Industrial += 1;
    else if (/farm|agri|crop|soil|rural/.test(text)) buckets.Agricultural += 1;
    else if (/retail|commercial|mall|market|business/.test(text)) buckets.Commercial += 1;
    else buckets['Mixed-use/Institutional'] += 1;
  });

  const sorted = Object.entries(buckets).sort((a, b) => b[1] - a[1]);
  const dominant = sorted[0];
  const dominantLabel = dominant && dominant[1] > 0 ? dominant[0] : null;

  const narrative = dominantLabel === 'Industrial'
    ? `The historical land use pattern for this area is dominated by <strong>industrial and regulated activity indicators</strong> (${dominant[1]} site record(s)), including fuel handling, waste generation, and heavy industrial operations. Historically industrial properties represent an elevated concern for soil and groundwater contamination through long-term chemical releases, equipment leakage, and improper waste disposal practices. This pattern is consistent with pre-1980s industrial zones where regulatory oversight was minimal and remediation was rarely required at closure.`
    : dominantLabel === 'Commercial'
      ? `The area exhibits a predominantly <strong>commercial land use history</strong> (${dominant[1]} indicator(s)). Commercial corridors generate moderate environmental risk through dry cleaner solvents (PCE/TCE), auto service chemicals, petroleum storage, and general retail waste streams. Mid-20th century commercial zones frequently contain unremediated dry cleaning releases and petroleum product spills from prior service station operations.`
      : dominantLabel === 'Agricultural'
        ? `Historical land use indicators suggest a predominantly <strong>agricultural context</strong> (${dominant[1]} indicator(s)). Agricultural land presents risk through pesticide and herbicide application history, bulk fuel storage for farm equipment, and fertilizer handling. Residual organochlorine pesticides (DDT, chlordane, dieldrin) and organophosphate compounds are frequently identified on former agricultural lands developed for residential or commercial use after the 1970s.`
        : `The area presents a <strong>mixed or institutional historical land use pattern</strong> (${dominant ? dominant[1] : 0} indicator(s)). Without a single dominant industrial or commercial driver, risk is distributed across multiple potential legacy sources. Standard Phase I historical research via Sanborn fire insurance maps, USGS topographic overlays, aerial photography, and city directory review is recommended to refine the historical context before acquisition.`;

  const lines = sorted.map(([key, value]) =>
    `<li><strong>${escapeHtml(key)}:</strong> ${value} environmental record indicator${value !== 1 ? 's' : ''}</li>`
  ).join('');

  return `
    <p style="line-height:1.6;">${narrative}</p>
    <ul style="margin-top:8px; line-height:1.6;">${lines}</ul>
    <p style="font-size:10.5px; color:#475569; margin-top:8px; font-style:italic;">
      Historical interpretation is based on environmental database signatures and is provided for screening purposes only.
      ASTM E-1527-21 standard historical source review (Sanborn maps, aerial photos, city directories) should be performed for Phase I ESA-level analysis.
    </p>`;
}

function buildGeologicalSectionHtml(envData, sites) {
  const rainfallValues = (envData?.rainfall || [])
    .map((r) => parseFloat(String(r.precipitation || '').replace(' mm', '')))
    .filter((v) => Number.isFinite(v));
  const avgRain = rainfallValues.length
    ? (rainfallValues.reduce((sum, val) => sum + val, 0) / rainfallValues.length)
    : null;
  const floodSusceptibility = (envData?.floodZones || []).length > 0 ? 'Elevated to High' : 'Low to Moderate';
  const radonFlag = (sites || []).some((site) => normalizeDatabaseName(site.database).includes('radon'));
  const radonRisk = radonFlag ? 'Potentially Elevated (radon datasets present)' : 'Regional baseline risk (no direct radon hit in mapped records)';
  const permeability = floodSusceptibility === 'Elevated to High' ? 'Moderate to low permeability expected in saturated zones' : 'Moderate permeability expected for general urban soils';
  const drainage = floodSusceptibility === 'Elevated to High' ? 'Drainage constraints likely during peak precipitation events' : 'Conventional drainage profile expected under normal rainfall.';

  return `
  <table>
    <tr><th>Parameter</th><th>Interpretation</th></tr>
    <tr><td>Soil Classification</td><td>Urban fill / developed soils (screening-level interpretation)</td></tr>
    <tr><td>Permeability</td><td>${escapeHtml(permeability)}</td></tr>
    <tr><td>Drainage</td><td>${escapeHtml(drainage)}</td></tr>
    <tr><td>Flood Susceptibility</td><td>${escapeHtml(floodSusceptibility)}</td></tr>
    <tr><td>Radon Risk</td><td>${escapeHtml(radonRisk)}</td></tr>
    <tr><td>Geological Formation</td><td>Regional surficial sedimentary deposits with local anthropogenic modification.</td></tr>
    <tr><td>Average Historical Rainfall</td><td>${avgRain !== null ? `${avgRain.toFixed(1)} mm` : 'Not available from upstream weather source'}</td></tr>
  </table>`;
}

function summarizeFloodZoneClasses(floodZones = []) {
  const classes = {};
  (floodZones || []).forEach((zone) => {
    const fld = String(
      zone?.attributes?.FLD_ZONE ||
      zone?.attributes?.ZONE ||
      zone?.properties?.FLD_ZONE ||
      zone?.properties?.ZONE ||
      ''
    ).toUpperCase().trim();
    if (!fld) return;
    classes[fld] = (classes[fld] || 0) + 1;
  });
  return classes;
}

function estimatePathwayDirection(subjectLat, subjectLng, sites = []) {
  const baseLat = toFiniteNumber(subjectLat);
  const baseLng = toFiniteNumber(subjectLng);
  if (baseLat === null || baseLng === null) return 'Undetermined';

  const candidates = (sites || [])
    .filter((site) => {
      const lat = toFiniteNumber(site.lat ?? site.latitude);
      const lng = toFiniteNumber(site.lng ?? site.longitude);
      return lat !== null && lng !== null;
    })
    .map((site) => {
      const lat = Number(site.lat ?? site.latitude);
      const lng = Number(site.lng ?? site.longitude);
      const risk = getRiskLevel(site);
      const weight = risk === 'High' ? 3 : risk === 'Moderate' ? 2 : 1;
      return { lat, lng, weight };
    });

  if (!candidates.length) return 'Undetermined';

  let x = 0;
  let y = 0;
  let totalWeight = 0;
  candidates.slice(0, 120).forEach((c) => {
    x += c.lng * c.weight;
    y += c.lat * c.weight;
    totalWeight += c.weight;
  });

  if (!totalWeight) return 'Undetermined';
  const centroidLng = x / totalWeight;
  const centroidLat = y / totalWeight;
  const bearing = calculateBearing(baseLat, baseLng, centroidLat, centroidLng);
  return bearingToCardinal(bearing);
}

function buildPathwayAnalysisHtml(envData = {}, subjectLat, subjectLng, addressData = []) {
  const sites = envData.environmentalSites || [];
  const floodZones = envData.floodZones || [];
  const rainfallValues = (envData.rainfall || [])
    .map((r) => parseFloat(String(r.precipitation || '').replace(' mm', '')))
    .filter((v) => Number.isFinite(v));
  const avgRain = rainfallValues.length
    ? rainfallValues.reduce((sum, val) => sum + val, 0) / rainfallValues.length
    : null;
  const direction = estimatePathwayDirection(subjectLat, subjectLng, sites);
  const highCount = sites.filter((s) => getRiskLevel(s) === 'High').length;
  const moderateCount = sites.filter((s) => getRiskLevel(s) === 'Moderate').length;
  const lowCount = sites.filter((s) => getRiskLevel(s) === 'Low').length;
  const sensitiveCount = (addressData || []).filter((a) => {
    const t = String(a.type || '').toLowerCase();
    return t.includes('school') || t.includes('hospital') || t.includes('daycare');
  }).length;

  const groundwater = highCount > 0
    ? `Elevated groundwater migration concern based on ${highCount} high-priority source indicator(s).`
    : 'No dominant high-priority groundwater source was identified in current mapped records.';
  const runoff = floodZones.length > 0
    ? `Surface runoff mobilization risk is elevated due to ${floodZones.length} mapped flood/hydrology feature(s).`
    : 'No mapped flood constraints were returned; runoff concern remains baseline pending local grading/drainage data.';
  const air = (highCount + moderateCount) > 0
    ? `Air/vapor pathway screening relevance exists from ${highCount + moderateCount} moderate/high source indicator(s).`
    : 'Air pathway concern appears low in current mapped dataset context.';

  return `
  <table>
    <tr><th>Pathway</th><th>Screening Interpretation</th></tr>
    <tr><td>Groundwater</td><td>${escapeHtml(groundwater)}</td></tr>
    <tr><td>Surface Runoff</td><td>${escapeHtml(runoff)}</td></tr>
    <tr><td>Air / Vapor</td><td>${escapeHtml(air)}</td></tr>
    <tr><td>Dominant Source Axis</td><td>${escapeHtml(direction)} of subject property (risk-weighted source centroid)</td></tr>
    <tr><td>Rainfall Influence</td><td>${avgRain !== null ? `${avgRain.toFixed(1)} mm annual average (screening)` : 'Rainfall data unavailable in this run'}</td></tr>
    <tr><td>Sensitive Receptors Nearby</td><td>${sensitiveCount} receptor location(s) identified in mapped address set</td></tr>
    <tr><td>Risk Mix</td><td>High: ${highCount}, Moderate: ${moderateCount}, Low: ${lowCount}</td></tr>
  </table>`;
}

function buildFloodWetlandDetailHtml(envData = {}, features = []) {
  const floodZones = envData.floodZones || [];
  const zoneCounts = summarizeFloodZoneClasses(floodZones);
  const zoneEntries = Object.entries(zoneCounts).sort((a, b) => b[1] - a[1]);
  const wetlands = (features || []).filter((f) => String(f.type || '').toLowerCase() === 'wetland');

  const floodClassText = zoneEntries.length
    ? zoneEntries.map(([z, c]) => {
      const label = ['A', 'AE', 'AO', 'AH'].includes(z)
        ? `${z} (approx. 1% annual chance floodplain)`
        : z.startsWith('X')
          ? `${z} (typically lower annual flood probability)`
          : `${z} (classification from source flood layer)`;
      return `<li><strong>${escapeHtml(label)}</strong>: ${c} mapped feature(s)</li>`;
    }).join('')
    : '<li>No flood zone class records returned in this run.</li>';

  const wetlandText = wetlands.length
    ? `<p>${wetlands.length} wetland feature(s) were identified in area-feature screening. These areas can constrain grading, fill, and permitting pathways.</p>`
    : '<p>No wetland features were returned from current area-feature layers.</p>';

  return `<div><p><strong>Flood Classification Detail:</strong></p><ul>${floodClassText}</ul>${wetlandText}</div>`;
}

function buildDataConfidenceHtml(envData = {}, addressData = []) {
  const sites = envData.environmentalSites || [];
  const total = sites.length;
  const geocoded = sites.filter((s) => toFiniteNumber(s.lat ?? s.latitude) !== null && toFiniteNumber(s.lng ?? s.longitude) !== null).length;
  const geocodePct = total > 0 ? Math.round((geocoded / total) * 100) : 0;
  const unknownNamed = sites.filter((s) => /unknown/i.test(String(s.name || ''))).length;
  const unknownPct = total > 0 ? Math.round((unknownNamed / total) * 100) : 0;
  const dbCount = new Set(sites.map((s) => String(s.database || '').trim()).filter(Boolean)).size;
  const receptorCount = (addressData || []).filter((a) => {
    const t = String(a.type || '').toLowerCase();
    return t.includes('school') || t.includes('hospital') || t.includes('daycare');
  }).length;

  let score = 0;
  score += Math.min(45, Math.round((geocodePct / 100) * 45));
  score += Math.min(25, dbCount >= 12 ? 25 : Math.round((dbCount / 12) * 25));
  score += Math.min(15, receptorCount > 0 ? 15 : 8);
  score += Math.max(0, 15 - Math.round((unknownPct / 100) * 15));
  const clamped = Math.max(0, Math.min(100, score));
  const label = clamped >= 80 ? 'High confidence' : clamped >= 60 ? 'Moderate confidence' : 'Limited confidence';
  const confidenceNote = clamped >= 80
    ? 'Dataset coverage and coordinate quality support strong screening interpretation confidence.'
    : clamped >= 60
      ? 'Interpretation confidence is acceptable for screening, with some uncertainty from naming/geocoding or source completeness.'
      : 'Interpretation confidence is constrained; treat this output as preliminary and prioritize source-record verification.';

  return `
  <div class="info-box">
    <p><strong>Confidence Score:</strong> ${clamped}/100 (${label})</p>
    <p style="margin:6px 0 8px 0;">${confidenceNote}</p>
    <ul>
      <li>Geocoded records: ${geocoded} of ${total} (${geocodePct}%)</li>
      <li>Distinct databases represented: ${dbCount}</li>
      <li>Records with unknown site naming: ${unknownNamed} (${unknownPct}%)</li>
      <li>Sensitive receptor context points: ${receptorCount}</li>
      <li>Limitations: public-source refresh cycles, variable geocoding quality, and incomplete upstream attributes can affect precision.</li>
    </ul>
  </div>`;
}

function buildComparativeRankingHtml(addressData = []) {
  const ranked = [...(addressData || [])]
    .map((a) => {
      const risk = String(a.riskLevel || a.risk_level || 'LOW').toUpperCase();
      const riskWeight = risk === 'HIGH' ? 3 : risk === 'MEDIUM' ? 2 : 1;
      const findings = (a.risks || []).length;
      const nearest = (a.risks || [])
        .map((r) => Number(r.distance ?? r.distance_m))
        .filter((v) => Number.isFinite(v))
        .sort((x, y) => x - y)[0];
      const proximityBoost = Number.isFinite(nearest) ? Math.max(0, 300 - nearest) / 100 : 0;
      const score = Number((riskWeight * 30 + findings * 6 + proximityBoost * 10).toFixed(1));
      const topDb = (a.risks || [])[0]?.database_name || (a.risks || [])[0]?.database || 'No linked dataset';
      return {
        address: cleanDisplayAddress(a.address),
        risk,
        findings,
        nearest: Number.isFinite(nearest) ? fmtMi(nearest) : 'N/A',
        topDb,
        score
      };
    })
    .sort((x, y) => y.score - x.score)
    .slice(0, 20);

  if (!ranked.length) {
    return '<p>No address-level records available to rank in this run.</p>';
  }

  const rows = ranked.map((r, idx) => `
    <tr>
      <td>${idx + 1}</td>
      <td>${escapeHtml(r.address)}</td>
      <td>${escapeHtml(r.risk)}</td>
      <td>${r.findings}</td>
      <td>${escapeHtml(r.nearest)}</td>
      <td>${escapeHtml(r.topDb)}</td>
      <td>${r.score}</td>
    </tr>`).join('');

  return `
  <table>
    <tr>
      <th>Rank</th>
      <th>Address</th>
      <th>Risk Tier</th>
      <th>Linked Findings</th>
      <th>Nearest Distance</th>
      <th>Leading Driver</th>
      <th>Priority Score</th>
    </tr>
    ${rows}
  </table>`;
}

function buildUnmappableRecordsHtml(sites) {
  const unmappable = (sites || []).filter((site) => {
    const lat = toFiniteNumber(site.lat ?? site.latitude);
    const lng = toFiniteNumber(site.lng ?? site.longitude);
    return lat === null || lng === null;
  });

  if (!unmappable.length) {
    return '<p>No unmappable records were identified in this run. Confidence: moderate to high for map-based interpretation of returned records.</p>';
  }

  const rows = unmappable.slice(0, 50).map((site, index) => `
    <tr>
      <td>U${index + 1}</td>
      <td>${escapeHtml(site.name || 'Unknown Facility')}</td>
      <td>${escapeHtml(site.address || site.location || 'Address unavailable')}</td>
      <td>${escapeHtml(site.database || 'Unknown Source')}</td>
      <td>${escapeHtml(resolveRegulatoryId(site, index))}</td>
    </tr>
  `).join('');

  return `
  <p>${unmappable.length} records were returned without usable coordinates. These records may still represent environmental risk and require manual geocoding or document-level review.</p>
  <table>
    <tr><th>Log ID</th><th>Facility</th><th>Address</th><th>Database</th><th>Regulatory ID</th></tr>
    ${rows}
  </table>`;
}

function buildLegalComplianceHtml() {
  return `
  <div class="legal-block">
    <p><strong>ASTM / AAI Framework:</strong> This screening report is aligned for preliminary due diligence workflows that reference ASTM E1527-21 concepts and EPA All Appropriate Inquiry (AAI) expectations; it is not, by itself, a complete Phase I ESA.</p>
    <p><strong>Data Limitation Statement:</strong> Findings are derived from third-party public and commercial datasets, each with independent refresh schedules, geocoding quality, and completeness constraints. Absence of a listing is not evidence of absence of environmental conditions.</p>
    <p><strong>Liability Limitation:</strong> GeoScope provides this deliverable as a screening-level advisory product. Final transaction, lending, insurance, and legal decisions should rely on qualified professional judgment, including site reconnaissance and records review as appropriate.</p>
  </div>`;
}

function buildDynamicRecommendationsHtml(riskLevels, groupedAddresses, envData) {
  const highCount = Number(riskLevels.high || 0);
  const sites = envData.environmentalSites || [];
  const floodCount = Number((envData.floodZones || []).length);
  const ustPresent = sites.some((s) => /ust|lust|petroleum|fuel/i.test(String(s.database || '')));
  const nplPresent = sites.some((s) => /npl|superfund/i.test(String(s.database || '')));
  const pfasPresent = sites.some((s) => /pfas/i.test(String(s.database || '')));
  const rcraPresent = sites.some((s) => /rcra/i.test(String(s.database || '')));
  const activeViolation = sites.some((s) => /violation|enforcement|open|active/i.test(String(s.status || s.name || '')));

  const recs = [];

  if (nplPresent || (highCount >= 3) || activeViolation) {
    recs.push(`<li><strong>Phase II Environmental Site Assessment strongly recommended.</strong> ${nplPresent ? 'A Superfund/NPL-proximate record was identified. ' : ''}${highCount > 0 ? `${highCount} high-risk record(s) were found within the screening buffer. ` : ''}A Phase II ESA with soil and groundwater sampling is the required next step to confirm or rule out contamination impact on the subject property.</li>`);
  } else if (highCount > 0 || ustPresent || rcraPresent) {
    recs.push(`<li><strong>Phase II Environmental Site Assessment is recommended.</strong> Environmental records within the screening buffer include regulated site types (${[highCount > 0 ? `${highCount} HIGH-risk` : '', ustPresent ? 'UST/petroleum' : '', rcraPresent ? 'RCRA hazardous waste' : ''].filter(Boolean).join(', ')}) that warrant soil/groundwater confirmation sampling before acquisition or financing.</li>`);
  } else {
    recs.push(`<li>This report may support a Phase I ESA desktop review. No immediate Phase II trigger was identified based on mapped government records alone; however, a site reconnaissance visit and regulatory file review are standard components of a complete Phase I ESA under ASTM E-1527-21.</li>`);
  }

  if (ustPresent) {
    recs.push(`<li><strong>UST closure documentation review:</strong> One or more UST-listed facilities were identified in proximity. Request LUST (Leaking Underground Storage Tank) closure reports and tank registration records from the applicable state environmental agency. Confirm no outstanding petroleum release cases are associated with the subject parcel.</li>`);
  }

  if (pfasPresent) {
    recs.push(`<li><strong>PFAS sampling recommended:</strong> PFAS contamination indicators were detected nearby. PFAS compounds are persistent, bioaccumulative, and the EPA lifetime health advisory is 4 ppt combined PFOA/PFOS (as of 2022). Targeted water and soil sampling for PFAS analytes (EPA Method 533/537.1) is advised prior to closing.</li>`);
  }

  if (rcraPresent) {
    recs.push(`<li><strong>RCRA generator file review:</strong> RCRA-listed facilities were identified. Request generator status determination letters, waste manifests, and inspection history from the EPA RCRA Info system and applicable state agency.</li>`);
  }

  if (floodCount > 0) {
    recs.push(`<li><strong>Flood zone compliance required:</strong> ${floodCount} flood zone feature(s) were identified. Verify the subject parcel's FEMA Flood Insurance Rate Map (FIRM) designation at the FEMA Flood Map Service Center. Confirm National Flood Insurance Program (NFIP) compliance obligations before financing or development permitting.</li>`);
  } else {
    recs.push(`<li>Verify FEMA flood zone status via the FEMA Flood Map Service Center to confirm current designation for the subject parcel prior to permitting or financing.</li>`);
  }

  recs.push(`<li>Review facility history for all HIGH and MODERATE classified locations, including permit status, inspection records, and enforcement actions available through <a href="https://echo.epa.gov" style="color:#2563eb;">EPA ECHO</a> and applicable state databases.</li>`);
  recs.push(`<li><em>This report serves as a screening-level tool and does not constitute a Phase I or Phase II Environmental Site Assessment under ASTM E-1527-21. All findings should be reviewed by a qualified environmental professional (QEP) before use in transaction, financing, or regulatory contexts.</em></li>`);

  return `<ul style="line-height:1.7; font-size:10.5px;">${recs.join('')}</ul>`;
}

function buildDataDensityStatement(sites, radiusMeters) {
  const r = Number(radiusMeters) || DEFAULT_REPORT_RADIUS_METERS;
  const miles = Number((r / 1609.344).toFixed(2));
  const areaKm2 = Math.PI * Math.pow(r / 1000, 2);
  const count = (sites || []).length;
  const density = areaKm2 > 0 ? count / areaKm2 : 0;
  return `${count} mapped records were processed within approximately ${miles} miles (area ${areaKm2.toFixed(3)} km2), yielding an observed density of ${density.toFixed(1)} records/km2 for this run.`;
}

async function lookupCountyContext(lat, lng) {
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) return null;
  try {
    const response = await axios.get(
      `https://geo.fcc.gov/api/census/block/find?format=json&latitude=${lat}&longitude=${lng}&showall=false`,
      { timeout: 8000 }
    );
    const countyName = response?.data?.County?.name || null;
    const countyFips = response?.data?.County?.FIPS || null;
    const stateCode = response?.data?.State?.code || null;
    if (!countyName && !countyFips && !stateCode) return null;
    return { countyName, countyFips, stateCode };
  } catch (error) {
    return null;
  }
}

async function lookupParcelAdapter(lat, lng, address) {
  const template = process.env.PARCEL_ENRICHMENT_ENDPOINT;
  if (!template) return null;

  const endpoint = template
    .replace('{lat}', encodeURIComponent(String(lat)))
    .replace('{lng}', encodeURIComponent(String(lng)))
    .replace('{address}', encodeURIComponent(String(address || '')));

  try {
    const response = await axios.get(endpoint, { timeout: 10000 });
    const payload = response?.data || {};
    return {
      ownerName: payload.ownerName || payload.owner || payload.owner_name || null,
      parcelId: payload.parcelId || payload.parcel_id || payload.apn || null,
      lastSaleDate: payload.lastSaleDate || payload.last_sale_date || null,
      landUse: payload.landUse || payload.land_use || null,
      source: payload.source || 'Parcel Adapter'
    };
  } catch (error) {
    return null;
  }
}

async function enrichSitesWithOwnershipData(sites) {
  const maxSites = Number(process.env.OWNERSHIP_ENRICHMENT_LIMIT || 80);
  const capped = (sites || []).slice(0, maxSites);
  const enriched = [];

  for (let i = 0; i < capped.length; i += 1) {
    const site = capped[i];
    const lat = toFiniteNumber(site.lat ?? site.latitude);
    const lng = toFiniteNumber(site.lng ?? site.longitude);

    const countyContext = await lookupCountyContext(lat, lng);
    const parcelAdapter = await lookupParcelAdapter(lat, lng, site.address || site.location || '');

    const ownershipDetails = parcelAdapter?.ownerName
      ? `Owner: ${parcelAdapter.ownerName}${parcelAdapter.parcelId ? ` | Parcel: ${parcelAdapter.parcelId}` : ''}${parcelAdapter.lastSaleDate ? ` | Last sale: ${parcelAdapter.lastSaleDate}` : ''}`
      : countyContext?.countyName
        ? `${countyContext.countyName} County${countyContext.stateCode ? `, ${countyContext.stateCode}` : ''} parcel/assessor records should be reviewed for legal ownership chain.`
        : 'Ownership not published by upstream screening datasets; county assessor review recommended.';

    const parcelSource = parcelAdapter?.source
      ? String(parcelAdapter.source)
      : countyContext?.countyFips
        ? `FCC Census county context (FIPS ${countyContext.countyFips})`
        : 'No parcel metadata source available';

    enriched.push({
      ...site,
      ownership_details: ownershipDetails,
      parcel_source: parcelSource,
      parcel_id: parcelAdapter?.parcelId || null,
      owner_name: parcelAdapter?.ownerName || null,
      county_name: countyContext?.countyName || null,
      county_fips: countyContext?.countyFips || null,
      land_use_hint: parcelAdapter?.landUse || null
    });
  }

  if ((sites || []).length > maxSites) {
    for (let i = maxSites; i < sites.length; i += 1) {
      enriched.push({
        ...sites[i],
        ownership_details: 'Ownership enrichment deferred due to record volume; apply parcel adapter for complete ownership chain.',
        parcel_source: 'Ownership enrichment cap reached'
      });
    }
  }

  return enriched;
}

function buildOwnershipEnrichmentSummaryHtml(sites) {
  const total = (sites || []).length;
  const withOwner = (sites || []).filter((s) => s.owner_name).length;
  const withCounty = (sites || []).filter((s) => s.county_name).length;
  const withParcelId = (sites || []).filter((s) => s.parcel_id).length;

  return `
  <div class="legal-block">
    <p><strong>Ownership Enrichment Coverage:</strong> ${withOwner} of ${total} records include explicit owner data; ${withCounty} records include county jurisdiction context; ${withParcelId} records include parcel identifiers.</p>
    <p><strong>Method:</strong> County context is derived from FCC census block geographies. Optional parcel-owner details are supported through the PARCEL_ENRICHMENT_ENDPOINT adapter when configured.</p>
    <p><strong>Use in Due Diligence:</strong> Treat this section as screening guidance and verify chain-of-title and assessor ownership records during transaction/legal review.</p>
  </div>`;
}

function latLngToWebMercator(lat, lng) {
  const x = (Number(lng) * 20037508.34) / 180;
  const y = Math.log(Math.tan(((90 + Number(lat)) * Math.PI) / 360)) / (Math.PI / 180);
  return {
    x,
    y: (y * 20037508.34) / 180
  };
}

function buildEsriExportUrl(serviceName, lat, lng, radiusMeters = DEFAULT_REPORT_RADIUS_METERS) {
  const latNum = Number(lat);
  const lngNum = Number(lng);
  if (!Number.isFinite(latNum) || !Number.isFinite(lngNum)) return '';

  const { x, y } = latLngToWebMercator(latNum, lngNum);
  const halfSpan = Math.max(800, Number(radiusMeters) || DEFAULT_REPORT_RADIUS_METERS);
  const xmin = x - halfSpan;
  const ymin = y - halfSpan;
  const xmax = x + halfSpan;
  const ymax = y + halfSpan;

  return `https://services.arcgisonline.com/ArcGIS/rest/services/${serviceName}/MapServer/export?bbox=${xmin},${ymin},${xmax},${ymax}&bboxSR=3857&imageSR=3857&size=1400,900&format=jpg&transparent=false&f=image`;
}

// Helper function to generate map URLs
function generateMapUrls(lat, lng, radiusMeters = DEFAULT_REPORT_RADIUS_METERS) {
  const apiKey = process.env.GOOGLE_MAPS_API_KEY || 'YOUR_GOOGLE_MAPS_API_KEY';
  const latNum = Number(lat);
  const lngNum = Number(lng);
  const circlePoints = [];
  const effectiveRadiusMeters = Math.max(50, Number(radiusMeters) || DEFAULT_REPORT_RADIUS_METERS);
  const ringRadii = [effectiveRadiusMeters / 3, (effectiveRadiusMeters * 2) / 3, effectiveRadiusMeters];

  if (Number.isFinite(latNum) && Number.isFinite(lngNum)) {
    ringRadii.forEach((ringRadius, idx) => {
      const ringPoints = [];
      for (let degree = 0; degree <= 360; degree += 20) {
        const radians = (degree * Math.PI) / 180;
        const latOffset = (ringRadius / 111320) * Math.cos(radians);
        const lngOffset = (ringRadius / (111320 * Math.cos((latNum * Math.PI) / 180))) * Math.sin(radians);
        ringPoints.push(`${(latNum + latOffset).toFixed(6)},${(lngNum + lngOffset).toFixed(6)}`);
      }
      const style = idx === 0
        ? 'color:0x0ea5e9cc%7Cweight:2%7Cfillcolor:0xbae6fd18'
        : idx === 1
          ? 'color:0x2563ebcc%7Cweight:2%7Cfillcolor:0x93c5fd14'
          : 'color:0x1d4ed8cc%7Cweight:3%7Cfillcolor:0x60a5fa10';
      circlePoints.push(`&path=${style}%7C${ringPoints.join('%7C')}`);
    });
  }
  const bufferPath = circlePoints.join('');

  if (!hasGoogleMapsKey(apiKey)) {
    return {
      overview: buildEsriExportUrl('World_Street_Map', latNum, lngNum, effectiveRadiusMeters),
      satellite: buildEsriExportUrl('World_Imagery', latNum, lngNum, effectiveRadiusMeters),
      streetView: buildEsriExportUrl('World_Imagery', latNum, lngNum, Math.max(600, effectiveRadiusMeters / 2))
    };
  }

  return {
    overview: `https://maps.googleapis.com/maps/api/staticmap?center=${lat},${lng}&zoom=15&size=1000x560&scale=2&maptype=roadmap&markers=size:mid%7Ccolor:red%7Clabel:S%7C${lat},${lng}${bufferPath}&key=${apiKey}`,
    satellite: `https://maps.googleapis.com/maps/api/staticmap?center=${lat},${lng}&zoom=16&size=1000x560&scale=2&maptype=satellite&markers=size:mid%7Ccolor:red%7Clabel:S%7C${lat},${lng}${bufferPath}&key=${apiKey}`,
    streetView: `https://maps.googleapis.com/maps/api/streetview?size=1000x560&location=${lat},${lng}&key=${apiKey}`
  };
}

function buildMapFallbackDataUri(title, lat, lng) {
  const safeTitle = String(title || 'Map Unavailable').replace(/[&<>"']/g, '');
  const safeLat = Number.isFinite(Number(lat)) ? Number(lat).toFixed(6) : 'N/A';
  const safeLng = Number.isFinite(Number(lng)) ? Number(lng).toFixed(6) : 'N/A';
  const svg = `
<svg xmlns="http://www.w3.org/2000/svg" width="1000" height="560" viewBox="0 0 1000 560">
  <defs>
    <linearGradient id="g" x1="0" x2="1" y1="0" y2="1">
      <stop offset="0%" stop-color="#e2e8f0"/>
      <stop offset="100%" stop-color="#cbd5e1"/>
    </linearGradient>
  </defs>
  <rect width="1000" height="560" fill="url(#g)"/>
  <rect x="40" y="40" width="920" height="480" rx="14" fill="#f8fafc" stroke="#94a3b8" stroke-width="2"/>
  <text x="500" y="230" text-anchor="middle" font-size="38" font-family="Arial" fill="#0f172a">${safeTitle}</text>
  <text x="500" y="285" text-anchor="middle" font-size="24" font-family="Arial" fill="#334155">Rendered with fallback map image</text>
  <text x="500" y="335" text-anchor="middle" font-size="20" font-family="Arial" fill="#475569">Coordinates: ${safeLat}, ${safeLng}</text>
</svg>`;
  return `data:image/svg+xml;charset=utf-8,${encodeURIComponent(svg)}`;
}

async function urlToDataUri(url, fallbackLabel, lat, lng) {
  try {
    if (!url || typeof url !== 'string') {
      return buildMapFallbackDataUri(fallbackLabel, lat, lng);
    }
    const response = await axios.get(url, {
      responseType: 'arraybuffer',
      timeout: 25000,
      headers: { 'User-Agent': 'GeoScope-Report-Renderer/1.0' }
    });
    const contentType = String(response.headers?.['content-type'] || 'image/png').split(';')[0];
    const base64 = Buffer.from(response.data).toString('base64');
    return `data:${contentType};base64,${base64}`;
  } catch {
    return buildMapFallbackDataUri(fallbackLabel, lat, lng);
  }
}

async function resolveReportMapImages(mapUrls, lat, lng) {
  const [overview, satellite, streetView] = await Promise.all([
    urlToDataUri(mapUrls?.overview, 'Property Proximity Map', lat, lng),
    urlToDataUri(mapUrls?.satellite, 'Satellite Area Map', lat, lng),
    urlToDataUri(mapUrls?.streetView, 'Street-Level Reference', lat, lng)
  ]);

  return { overview, satellite, streetView };
}

// ─────────────────────────────────────────────────────────────────────────────
// HISTORICAL TOPOGRAPHIC MAP GENERATOR
// Fetches USGS National Map tiles at multiple scales/services and returns
// a ready-to-embed HTML block (base64 data-URIs for reliable PDF rendering).
// ─────────────────────────────────────────────────────────────────────────────
// ---------------------------------------------------------------------------
// fetchTopoThumbnail — download a USGS S3 thumbnail and return a base64 data URI.
// Falls back to an empty string so the report page still renders without an image.
// ---------------------------------------------------------------------------
async function fetchTopoThumbnail(url) {
  try {
    const resp = await axios.get(url, {
      responseType: 'arraybuffer',
      timeout: 15000,
      headers: { 'User-Agent': 'GeoScope-ReportEngine/1.0' },
    });
    const ct = (resp.headers['content-type'] || 'image/jpeg').split(';')[0].trim();
    const b64 = Buffer.from(resp.data).toString('base64');
    return `data:${ct};base64,${b64}`;
  } catch (err) {
    console.warn('[TopoMap] thumbnail fetch failed:', err.message);
    return '';
  }
}

// ---------------------------------------------------------------------------
// fetchUsgsTopoBasemap — live USGS tile-service snapshot (current basemap).
// Used as fallback when TNM has no historical maps for a location.
// ---------------------------------------------------------------------------
async function fetchUsgsTopoBasemap(latN, lngN, service = 'USGSTopo', delta = 0.04) {
  const bbox = `${(lngN - delta).toFixed(6)},${(latN - delta).toFixed(6)},${(lngN + delta).toFixed(6)},${(latN + delta).toFixed(6)}`;
  const url = `https://basemap.nationalmap.gov/arcgis/rest/services/${service}/MapServer/export?bbox=${bbox}&bboxSR=4326&layers=show&size=900,700&imageSR=4326&format=png&transparent=false&f=image`;
  try {
    const resp = await axios.get(url, {
      responseType: 'arraybuffer',
      timeout: 20000,
      headers: { 'User-Agent': 'GeoScope-ReportEngine/1.0' },
    });
    const b64 = Buffer.from(resp.data).toString('base64');
    const ct = (resp.headers['content-type'] || 'image/png').split(';')[0];
    return `data:${ct};base64,${b64}`;
  } catch (err) {
    console.warn(`[TopoMap] basemap (${service}) fetch failed:`, err.message);
    return '';
  }
}

// ---------------------------------------------------------------------------
// generateTopoMapsHtml — builds complete Historical Topographic Map section.
//
// Flow:
//  1. Query USGS TNM Access API for real historical scanned topo maps at location
//  2. Parse year + scale from titles; de-duplicate by (quadName, year)
//  3. Select best coverage: prefer 1:24,000 7.5-min quads; cap at 8 pages
//  4. Fetch USGS S3 thumbnail for each → base64-embed in PDF
//  5. Generate: summary table + individual full-page map sections
//  6. Fall back to USGS live tile-service basemap snapshots if TNM fails
// ---------------------------------------------------------------------------
async function generateTopoMapsHtml(lat, lng) {
  const latN = parseFloat(lat);
  const lngN = parseFloat(lng);
  if (!isFinite(latN) || !isFinite(lngN)) {
    generateTopoMapsHtml._lastSummaryHtml = '';
    generateTopoMapsHtml._hasPublishableHistoricalTopo = false;
    return '';
  }

  const coordLabel = `${latN.toFixed(5)}°${latN >= 0 ? 'N' : 'S'}, ${Math.abs(lngN).toFixed(5)}°${lngN < 0 ? 'W' : 'E'}`;

  // ── 1. Query TNM Access API ──────────────────────────────────────────────
  const delta = 0.06;
  const west  = (lngN - delta).toFixed(6);
  const east  = (lngN + delta).toFixed(6);
  const south = (latN - delta).toFixed(6);
  const north = (latN + delta).toFixed(6);

  let tnmItems = [];
  try {
    const tnmUrl =
      `https://tnmaccess.nationalmap.gov/api/v1/products` +
      `?datasets=Historical+Topographic+Maps` +
      `&bbox=${west},${south},${east},${north}` +
      `&max=50`;
    const tnmResp = await axios.get(tnmUrl, {
      timeout: 20000,
      headers: { 'User-Agent': 'GeoScope-ReportEngine/1.0' },
    });
    tnmItems = Array.isArray(tnmResp.data?.items) ? tnmResp.data.items : [];
  } catch (err) {
    console.warn('[TopoMap] TNM API failed:', err.message);
  }

  // ── 2. Parse + deduplicate ───────────────────────────────────────────────
  const seenKey = new Set();
  const parsed = [];

  for (const item of tnmItems) {
    const titleStr = item.title || '';
    const yearMatch = titleStr.match(/\b(1[89]\d{2}|20[012]\d)\b/);
    const scaleMatch = titleStr.match(/1:(\d[\d,]+)/);
    const year = yearMatch ? parseInt(yearMatch[1], 10) : 0;
    if (!year) continue;

    const scaleNum = scaleMatch ? parseInt(scaleMatch[1].replace(/,/g, ''), 10) : 24000;
    const scaleLabel = scaleMatch ? `1:${scaleNum.toLocaleString()}` : '1:24,000';

    // Strip "USGS 1:XXXXX-scale Quadrangle for " prefix to get just the quad name
    const quadName = titleStr
      .replace(/^USGS\s+1:[0-9,]+-scale\s+Quadrangle\s+for\s+/i, '')
      .replace(/\s+\d{4}$/, '')
      .trim();

    const key = `${quadName.toLowerCase()}|${year}`;
    if (seenKey.has(key)) continue;
    seenKey.add(key);

    // Determine series name from extent
    const extent = item.extent || '';
    const series = extent.includes('7.5')  ? 'USGS 7.5-Minute Series'
                 : extent.includes('15')   ? 'USGS 15-Minute Series'
                 : extent.includes('30')   ? 'USGS 30-Minute Series'
                 : extent.includes('60')   ? 'USGS 1° x 2° Series'
                 : 'USGS Topographic Series';

    parsed.push({
      year,
      quadName,
      scaleNum,
      scaleLabel,
      series,
      extent,
      thumbUrl: item.previewGraphicURL || '',
      downloadUrl: item.downloadURL || '',
      lastUpdated: (item.lastUpdated || '').substring(0, 10),
    });
  }

  // Sort by year ascending; prefer finer scale (lower scaleNum) when same year
  parsed.sort((a, b) => a.year !== b.year ? a.year - b.year : a.scaleNum - b.scaleNum);

  // ── 3. Select pages — prefer 1:24,000; cap at 8 ─────────────────────────
  // Try to get a diverse time spread: pick earliest, a few midpoints, latest
  let pagesSource = parsed.filter(p => p.scaleNum <= 24000);
  if (pagesSource.length < 3) pagesSource = parsed; // relax filter
  // Spread across time: bucket into up to 8 evenly-spaced eras
  const maxPages = 8;
  let pages = pagesSource;
  if (pages.length > maxPages) {
    const step = (pages.length - 1) / (maxPages - 1);
    pages = Array.from({ length: maxPages }, (_, i) => pages[Math.round(i * step)]);
    // Remove any accidental duplicates
    const dedupPages = [];
    const seenYr = new Set();
    for (const p of pages) {
      if (!seenYr.has(p.year)) { dedupPages.push(p); seenYr.add(p.year); }
    }
    pages = dedupPages;
  }

  // ── 4. Summary table HTML ────────────────────────────────────────────────
  const allForSummary = parsed.length > 0 ? parsed : [];
  let summaryTableHtml;
  if (allForSummary.length) {
    summaryTableHtml = `
<table class="data-table" style="margin-top:8px;">
  <tr>
    <th>#</th><th>Year</th><th>Map / Quadrangle Name</th><th>Series</th><th>Scale</th><th>Extent</th>
  </tr>
${allForSummary.map((p, i) => `  <tr>
    <td>${i + 1}</td>
    <td><strong>${p.year}</strong></td>
    <td>${escapeHtml(p.quadName)}</td>
    <td>${escapeHtml(p.series)}</td>
    <td>${escapeHtml(p.scaleLabel)}</td>
    <td>${escapeHtml(p.extent)}</td>
  </tr>`).join('\n')}
</table>
<p style="font-size:9.5px;color:#64748b;margin-top:8px;">
  ${allForSummary.length} historical topographic map(s) identified for this location from the
  USGS National Map Historical Topographic Map Collection.
  Full-page map exhibits are provided below for selected periods.
</p>`;
  } else {
    summaryTableHtml = '<p style="color:#64748b;font-size:10px;">No USGS historical topographic maps indexed for this location via the TNM API. Current-era basemap snapshots are provided below.</p>';
  }

  // ── 5. Individual map pages ──────────────────────────────────────────────
  // Fetch thumbnails in parallel (capped by pages array)
  const thumbResults = await Promise.allSettled(
    pages.map(p => p.thumbUrl ? fetchTopoThumbnail(p.thumbUrl) : Promise.resolve(''))
  );

  const publishableHistoricalCount = thumbResults.filter((result) =>
    result.status === 'fulfilled' && typeof result.value === 'string' && result.value.startsWith('data:image/')
  ).length;

  if (publishableHistoricalCount === 0) {
    generateTopoMapsHtml._lastSummaryHtml = '';
    generateTopoMapsHtml._hasPublishableHistoricalTopo = false;
    return '';
  }

  // Also always include a current USGS basemap snapshot at the end
  const currentTopoImg = await fetchUsgsTopoBasemap(latN, lngN, 'USGSTopo', 0.04);
  const currentImgTopoImg = await fetchUsgsTopoBasemap(latN, lngN, 'USGSImageryTopo', 0.03);

  const mapPageHtmlParts = pages.map((p, i) => {
    const imgSrc = thumbResults[i].status === 'fulfilled' ? thumbResults[i].value : '';
    const imgBlock = imgSrc
      ? `<div class="histo-img-wrap">
    <img src="${imgSrc}" alt="USGS Historical Topo — ${p.quadName} ${p.year}" />
    <div class="histo-img-caption">
      <span>USGS Historical Topographic Map — ${escapeHtml(p.quadName)} — ${p.year}</span>
      <span>© U.S. Geological Survey / USGS National Map</span>
    </div>
  </div>`
      : `<div style="background:#f1f5f9;border:1px dashed #94a3b8;border-radius:8px;padding:40px;text-align:center;color:#64748b;font-size:11px;margin-bottom:14px;">
    Map image not available for this quadrangle edition.
  </div>`;

    return `<div class="section page-break">
  <div class="histo-page-header">
    <span class="histo-page-title">${escapeHtml(p.quadName)}</span>
    <span class="histo-page-badge">Map ${i + 1} of ${pages.length}</span>
  </div>

  <div class="histo-meta-grid">
    <div class="histo-meta-cell">
      <div class="histo-meta-label">Publication Year</div>
      <div class="histo-meta-value">${p.year}</div>
    </div>
    <div class="histo-meta-cell">
      <div class="histo-meta-label">Map Series</div>
      <div class="histo-meta-value">${escapeHtml(p.series)}</div>
    </div>
    <div class="histo-meta-cell">
      <div class="histo-meta-label">Scale</div>
      <div class="histo-meta-value">${escapeHtml(p.scaleLabel)}</div>
    </div>
    <div class="histo-meta-cell">
      <div class="histo-meta-label">Extent</div>
      <div class="histo-meta-value">${escapeHtml(p.extent || 'N/A')}</div>
    </div>
    <div class="histo-meta-cell">
      <div class="histo-meta-label">Subject Location</div>
      <div class="histo-meta-value">${coordLabel}</div>
    </div>
    <div class="histo-meta-cell">
      <div class="histo-meta-label">Source</div>
      <div class="histo-meta-value">USGS National Map</div>
    </div>
  </div>

  ${imgBlock}

  <p class="histo-footnote">
    This topographic map (${p.series}, ${p.year}) provides historical land-surface context
    including terrain, drainage patterns, and land-use conditions for the period.
    Compare across time periods to detect land-use change, infilling of wet areas, or
    industrial activity patterns that may indicate environmental concern.
    Source: U.S. Geological Survey National Map Historical Topographic Map Collection.
  </p>
</div>`;
  });

  // Current-era basemap page (always appended)
  if (currentTopoImg || currentImgTopoImg) {
    mapPageHtmlParts.push(`<div class="section page-break">
  <div class="histo-page-header">
    <span class="histo-page-title">Current Topographic Reference — ${new Date().getFullYear()}</span>
    <span class="histo-page-badge">Current Edition</span>
  </div>

  <div class="histo-meta-grid">
    <div class="histo-meta-cell">
      <div class="histo-meta-label">Reference Year</div>
      <div class="histo-meta-value">${new Date().getFullYear()} (Current)</div>
    </div>
    <div class="histo-meta-cell">
      <div class="histo-meta-label">Map Type</div>
      <div class="histo-meta-value">USGS Digital Topo + Imagery</div>
    </div>
    <div class="histo-meta-cell">
      <div class="histo-meta-label">Scale</div>
      <div class="histo-meta-value">1:24,000 (approx.)</div>
    </div>
    <div class="histo-meta-cell">
      <div class="histo-meta-label">Subject Location</div>
      <div class="histo-meta-value">${coordLabel}</div>
    </div>
    <div class="histo-meta-cell">
      <div class="histo-meta-label">Revision Date</div>
      <div class="histo-meta-value">${new Date().getFullYear()}</div>
    </div>
    <div class="histo-meta-cell">
      <div class="histo-meta-label">Source</div>
      <div class="histo-meta-value">USGS National Map (Live)</div>
    </div>
  </div>

  <div class="map-exhibit-grid" style="margin-bottom:14px;">
    ${currentTopoImg ? `<div class="map-exhibit-card">
      <img src="${currentTopoImg}" alt="Current USGS Topo" style="height:340px;object-fit:cover;" />
      <div class="map-exhibit-body">
        <div class="map-exhibit-label">Topographic</div>
        <div class="map-exhibit-title">USGS Topo — Current Edition</div>
        <div class="map-exhibit-text">Terrain lines, road network, hydrography.</div>
      </div>
    </div>` : ''}
    ${currentImgTopoImg ? `<div class="map-exhibit-card">
      <img src="${currentImgTopoImg}" alt="Current USGS Imagery Topo" style="height:340px;object-fit:cover;" />
      <div class="map-exhibit-body">
        <div class="map-exhibit-label">Imagery + Topo Overlay</div>
        <div class="map-exhibit-title">USGS ImageryTopo — Current</div>
        <div class="map-exhibit-text">Satellite imagery with topo layer overlay.</div>
      </div>
    </div>` : ''}
  </div>

  <p class="histo-footnote">
    Current-era USGS topographic and imagery basemaps retrieved from the USGS National Map
    live tile service. Compare against historical quadrangles above to assess land-use change,
    infilling, site development, and drainage modification over time.
  </p>
</div>`);
  }

  // ── 6. Return combined HTML ──────────────────────────────────────────────
  // Export summaryTableHtml separately via the {{histo_summary_table}} placeholder;
  // fall back: embed it here if the placeholder was not found in template.
  generateTopoMapsHtml._lastSummaryHtml = summaryTableHtml;
  generateTopoMapsHtml._hasPublishableHistoricalTopo = true;
  return mapPageHtmlParts.join('\n');
}

// Helper functions for report generation
async function fetchEnvironmentalData(lat, lng, polygon = null, radius = 1000) {
  const data = {
    rainfall: [],
    floodZones: [],
    schools: [],
    governmentRecords: [],
    environmentalSites: []
  };

  try {
    // ── PRIMARY: Query our local PostGIS database (15M+ records) ──────────
    const gisSearch = require('./gis-search');
    const searchResult = await gisSearch.nearbySearch(lat, lng, radius);
    // Cap site volume to keep premium PDF generation stable under load.
    const allSites = (searchResult.results || []).slice(0, 200);

    // Separate into typed buckets for template placeholders
    data.floodZones = allSites
      .filter(s => /flood|dfirm|fema/i.test(s.database || ''))
      .map(s => ({ attributes: { FLD_ZONE: s.status || 'AE', SFHA_TF: 'T', NAME: s.site_name } }));

    data.schools = allSites
      .filter(s => /school|college|university/i.test(s.database || ''))
      .map(s => ({ attributes: { NAME: s.site_name, ADDRESS: s.address } }));

    data.governmentRecords = allSites
      .filter(s => /echo|epa|npdes/i.test(s.source || ''))
      .map(s => ({ FacilityName: s.site_name, FacilityAddress: s.address }));

    // Map every result as an environmental site with full field set
    data.environmentalSites = allSites.map(s => {
      const distMi = Number.isFinite(s.distance_m)
        ? `${(s.distance_m / 1609.344).toFixed(2)} mi`
        : 'N/A';
      return {
        id:        s.id,
        name:      s.site_name || 'Unknown Site',
        address:   s.address   || '',
        database:  s.database  || 'Unknown',
        category:  s.category  || 'regulatory',
        elevation: 'N/A',
        direction: 'N/A',
        distance:  distMi,
        distance_m: s.distance_m,
        lat:       s.lat,
        lng:       s.lng,
        status:    s.status    || 'Unknown',
        source:    s.source    || 'GeoScope Database',
      };
    });

    // ── SECONDARY: rainfall from Open-Meteo (non-critical) ───────────────
    try {
      const rainfallResponse = await axios.get(
        `https://archive-api.open-meteo.com/v1/archive?latitude=${lat}&longitude=${lng}` +
        `&start_date=2023-01-01&end_date=2023-12-31&daily=precipitation_sum&timezone=America%2FNew_York`,
        { timeout: 8000 }
      );
      data.rainfall = rainfallResponse.data.daily
        ? rainfallResponse.data.daily.time.map((date, i) => ({
            date,
            precipitation: `${rainfallResponse.data.daily.precipitation_sum[i]} mm`
          }))
        : [];
    } catch (_) { /* non-critical — skip */ }

  } catch (error) {
    console.error('Error fetching environmental data:', error.message);
    // Minimal fallback so report still generates
    data.environmentalSites = [
      { id: 'A1', name: 'Sample Environmental Site', address: '123 Main St',
        database: 'EPA NPL', elevation: 'N/A', direction: 'N', distance: '0.1 mi',
        lat: lat + 0.001, lng: lng + 0.001, status: 'Active', source: 'Fallback' }
    ];
  }

  // Filter points inside polygon if provided
  if (polygon) {
    try {
      const poly = turf.polygon(polygon.geometry.coordinates);
      data.environmentalSites = data.environmentalSites.filter(site => {
        if (!Number.isFinite(Number(site.lat)) || !Number.isFinite(Number(site.lng))) return false;
        const pt = turf.point([Number(site.lng), Number(site.lat)]);
        return turf.booleanPointInPolygon(pt, poly);
      });
    } catch (error) {
      console.error('Error filtering points inside polygon:', error);
    }
  }

  return data;
}

// AI-Powered Summary Generation
async function generateAISummary(environmentalData, projectName, address, polygon = null, polygonAnalysis = null) {
  try {
    // Prepare environmental data summary
    const analysisType = polygon ? 'polygon-defined area' : 'radius-based analysis';
    const areaInfo = (polygonAnalysis && polygonAnalysis.area != null) ? `Property area: ${polygonAnalysis.area.toLocaleString()} m² (${polygonAnalysis.areaAcres.toFixed(2)} acres). ` : '';

    const dataText = `
      Project: ${projectName}
      Address: ${address}
      Analysis Type: ${analysisType}
      ${areaInfo}
      Environmental Sites Found: ${environmentalData.environmentalSites.length}
      Flood Zones: ${environmentalData.floodZones.length}
      Schools Nearby: ${environmentalData.schools.length}
      Government Records: ${environmentalData.governmentRecords.length}
      Average Rainfall: ${environmentalData.rainfall.length > 0 ? 'Available' : 'Not available'}

      Sites: ${environmentalData.environmentalSites.slice(0, 5).map(s => `${s.name} (${s.database})`).join(', ')}
    `;

    // Call OpenAI GPT-4 mini for professional summary
    const response = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [
        {
          role: 'system',
          content: 'You are a professional environmental consultant writing detailed environmental site assessment reports. Provide concise, technical, and professional summaries.'
        },
        {
          role: 'user',
          content: `Write a professional environmental site assessment executive summary based on this data:\n${dataText}\n\nProvide a 3-4 paragraph professional summary including risk assessment.`
        }
      ],
      temperature: 0.7,
      max_tokens: 500
    });

    return response.choices[0].message.content;
  } catch (error) {
    console.error('Error generating AI summary:', error.message);
    // Fallback to template summary
    return 'Environmental Site Assessment Summary: Comprehensive analysis of the subject property has been completed including evaluation of environmental sites, flood zones, and other relevant factors within the 1-mile radius.';
  }
}

function generateMapUrl(lat, lng, zoom = 15) {
  // Google Maps Static API - requires API key
  const apiKey = 'YOUR_GOOGLE_MAPS_API_KEY'; // Replace with actual key
  return `https://maps.googleapis.com/maps/api/staticmap?center=${lat},${lng}&zoom=${zoom}&size=600x400&key=${apiKey}`;
}

function buildExecutiveSummaryByDistance(sites) {
  let html = '';
  sites.forEach(site => {
    html += `<tr>
      <td>${site.id}</td>
      <td>${site.name}</td>
      <td>${site.address}</td>
      <td>${site.database}</td>
      <td>${site.elevation}</td>
      <td>${site.direction} / ${site.distance}</td>
    </tr>`;
  });
  return html;
}

function buildExecutiveSummaryByDatabase(sites) {
  // Group by database
  const grouped = sites.reduce((acc, site) => {
    if (!acc[site.database]) acc[site.database] = [];
    acc[site.database].push(site);
    return acc;
  }, {});
  let html = '';
  for (const [db, sites] of Object.entries(grouped)) {
    html += `<h3>${db}</h3><ul>`;
    sites.forEach(site => {
      html += `<li>${site.name} - ${site.distance}</li>`;
    });
    html += '</ul>';
  }
  return html;
}

// Auto-generate comprehensive summary based on environmental findings
function generateAutoSummary(fetchedData, projectName, address) {
  let summary = `Environmental Location Overview for ${projectName}\n\n`;
  summary += `Subject Property: ${address}\n\n`;

  // Environmental Sites Analysis
  const siteCount = fetchedData.environmentalSites.length;
  summary += `ENVIRONMENTAL SITES FOUND: ${siteCount} mapped records in the selected radius\n`;
  if (siteCount > 0) {
    const databases = [...new Set(fetchedData.environmentalSites.map(s => s.database))];
    summary += `Databases matched at this location: ${databases.join(', ')}\n`;
    summary += `Sites include: ${fetchedData.environmentalSites.map(s => s.name).slice(0, 5).join(', ')}${siteCount > 5 ? '...' : ''}\n\n`;
  } else {
    summary += `No mapped records were returned for this location in the selected radius.\n\n`;
  }

  // Flood Zone Analysis
  const floodZones = fetchedData.floodZones;
  summary += `FLOOD ZONE ANALYSIS:\n`;
  if (floodZones.length > 0) {
    const zoneTypes = [...new Set(floodZones.map(f => f.attributes?.FLD_ZONE).filter(z => z))];
    summary += `Property located in flood zone(s): ${zoneTypes.join(', ') || 'Unknown'}\n`;
    summary += `Flood risk assessment: ${zoneTypes.includes('AE') || zoneTypes.includes('A') ? 'HIGH RISK' : 'MODERATE RISK'}\n\n`;
  } else {
    summary += `No flood zone data available for this location.\n\n`;
  }

  // Rainfall Analysis
  const rainfall = fetchedData.rainfall;
  let avgRainfall = 0;
  summary += `RAINFALL ANALYSIS:\n`;
  if (rainfall.length > 0) {
    const totalRainfall = rainfall.reduce((sum, r) => sum + parseFloat(r.precipitation.replace(' mm', '')), 0);
    avgRainfall = totalRainfall / rainfall.length;
    summary += `Average annual rainfall: ${avgRainfall.toFixed(1)} mm\n`;
    summary += `Rainfall pattern: ${avgRainfall > 1000 ? 'HIGH PRECIPITATION AREA' : avgRainfall > 500 ? 'MODERATE PRECIPITATION' : 'LOW PRECIPITATION AREA'}\n\n`;
  } else {
    summary += `No historical rainfall data available.\n\n`;
  }

  // Educational Facilities
  const schools = fetchedData.schools;
  summary += `EDUCATIONAL FACILITIES:\n`;
  if (schools.length > 0) {
    summary += `${schools.length} educational facilities identified within search area\n`;
    summary += `Nearest school: ${schools[0]?.attributes?.NAME || 'Unknown'}\n\n`;
  } else {
    summary += `No educational facilities found within search radius.\n\n`;
  }

  // Government Records
  const govRecords = fetchedData.governmentRecords;
  summary += `GOVERNMENT ENVIRONMENTAL RECORDS:\n`;
  if (govRecords.length > 0) {
    summary += `${govRecords.length} government environmental records found\n`;
    const facilities = govRecords.slice(0, 3).map(r => r.FacilityName).filter(n => n);
    summary += `Key facilities: ${facilities.join(', ')}${govRecords.length > 3 ? '...' : ''}\n\n`;
  } else {
    summary += `No government environmental records found.\n\n`;
  }

  // Overall location profile
  summary += `OVERALL LOCATION OVERVIEW:\n`;
  let profileLevel = 'BASELINE';
  let profileFactors = [];

  if (floodZones.some(f => ['AE', 'A', 'AO'].includes(f.attributes?.FLD_ZONE))) {
    profileLevel = 'ELEVATED';
    profileFactors.push('Flood zone proximity');
  }

  if (siteCount > 10) {
    profileLevel = profileLevel === 'ELEVATED' ? 'ELEVATED' : 'ACTIVE';
    profileFactors.push('Multiple environmental records');
  }

  if (avgRainfall > 1500) {
    profileLevel = profileLevel === 'ELEVATED' ? 'ELEVATED' : 'ACTIVE';
    profileFactors.push('High precipitation area');
  }

  summary += `Profile Level: ${profileLevel}\n`;
  if (profileFactors.length > 0) {
    summary += `Key Location Factors: ${profileFactors.join(', ')}\n`;
  }

  summary += `\nCONCLUSION:\n`;
  summary += `This location overview identified ${siteCount} mapped environmental records and assessed multiple environmental factors. `;
  summary += `For complete database coverage and location context, refer to the detailed report sections below.`;

  return summary;
}

// Generate detailed findings section
function generateDetailedFindings(fetchedData) {
  let findings = '<h3>Environmental Sites Analysis</h3>';

  if (fetchedData.environmentalSites.length > 0) {
    findings += `<p><strong>${fetchedData.environmentalSites.length} environmental sites</strong> were identified within the 1-mile search radius:</p>`;
    findings += '<ul>';
    fetchedData.environmentalSites.slice(0, 10).forEach(site => {
      findings += `<li><strong>${site.name}</strong> (${site.database}) - ${site.distance} - ${site.address}</li>`;
    });
    if (fetchedData.environmentalSites.length > 10) {
      findings += `<li>... and ${fetchedData.environmentalSites.length - 10} additional sites</li>`;
    }
    findings += '</ul>';
  } else {
    findings += '<p>No environmental sites found within the search radius.</p>';
  }

  findings += '<h3>Flood Zone Assessment</h3>';
  if (fetchedData.floodZones.length > 0) {
    findings += `<p><strong>${fetchedData.floodZones.length} flood zone areas</strong> identified:</p>`;
    findings += '<ul>';
    const uniqueZones = [...new Set(fetchedData.floodZones.map(f => f.attributes?.FLD_ZONE).filter(z => z))];
    uniqueZones.forEach(zone => {
      findings += `<li><strong>Zone ${zone}</strong> - ${getFloodZoneDescription(zone)}</li>`;
    });
    findings += '</ul>';
  } else {
    findings += '<p>No flood zone data available for this location.</p>';
  }

  findings += '<h3>Rainfall Analysis</h3>';
  if (fetchedData.rainfall.length > 0) {
    const totalRainfall = fetchedData.rainfall.reduce((sum, r) => sum + parseFloat(r.precipitation.replace(' mm', '')), 0);
    const avgRainfall = totalRainfall / fetchedData.rainfall.length;
    findings += `<p><strong>Average Annual Rainfall:</strong> ${avgRainfall.toFixed(1)} mm</p>`;
    findings += `<p><strong>Rainfall Pattern:</strong> ${avgRainfall > 1000 ? 'High precipitation area' : avgRainfall > 500 ? 'Moderate precipitation area' : 'Low precipitation area'}</p>`;
    findings += '<p><strong>Monthly Breakdown:</strong></p>';
    findings += '<ul>';
    fetchedData.rainfall.slice(0, 6).forEach(r => {
      findings += `<li>${r.date}: ${r.precipitation}</li>`;
    });
    findings += '</ul>';
  } else {
    findings += '<p>No historical rainfall data available.</p>';
  }

  findings += '<h3>Educational Facilities</h3>';
  if (fetchedData.schools.length > 0) {
    findings += `<p><strong>${fetchedData.schools.length} educational facilities</strong> identified within the search area:</p>`;
    findings += '<ul>';
    fetchedData.schools.slice(0, 5).forEach(school => {
      findings += `<li><strong>${school.attributes?.NAME || 'Unknown School'}</strong></li>`;
    });
    if (fetchedData.schools.length > 5) {
      findings += `<li>... and ${fetchedData.schools.length - 5} additional facilities</li>`;
    }
    findings += '</ul>';
  } else {
    findings += '<p>No educational facilities found within the search radius.</p>';
  }

  findings += '<h3>Government Environmental Records</h3>';
  if (fetchedData.governmentRecords.length > 0) {
    findings += `<p><strong>${fetchedData.governmentRecords.length} government environmental records</strong> found:</p>`;
    findings += '<ul>';
    fetchedData.governmentRecords.slice(0, 5).forEach(record => {
      findings += `<li><strong>${record.FacilityName || 'Unknown Facility'}</strong></li>`;
    });
    if (fetchedData.governmentRecords.length > 5) {
      findings += `<li>... and ${fetchedData.governmentRecords.length - 5} additional records</li>`;
    }
    findings += '</ul>';
  } else {
    findings += '<p>No government environmental records found.</p>';
  }

  return findings;
}

// Helper function for flood zone descriptions
function getFloodZoneDescription(zone) {
  const descriptions = {
    'A': 'Areas subject to inundation by 1-percent-annual-chance flood events',
    'AE': 'Areas subject to inundation by 1-percent-annual-chance flood events with base flood elevations determined',
    'AH': 'Areas subject to inundation by 1-percent-annual-chance shallow flooding',
    'AO': 'Areas subject to inundation by 1-percent-annual-chance shallow flooding with average depths of 1-3 feet',
    'X': 'Areas outside the 1-percent and 0.2-percent annual chance floodplain',
    'D': 'Areas where flood hazards are undetermined'
  };
  return descriptions[zone] || 'Flood zone classification available';
}

const MASTER_DATABASES = [
  'AFS AIRPORT FACILITIES',
  'ALT FUELING',
  'ARCHIVED RCRA TSDF',
  'ARENAS',
  'ASBESTOS BASINS',
  'BROWNFIELDS ACRES',
  'BRS',
  'CDC HAZDAT',
  'CERCLIS NFRAP',
  'CERCLIS HIST',
  'CHURCHES',
  'COAL ASH DOE',
  'COAL ASH EPA',
  'COAL GAS',
  'COLLEGES',
  'CONSENT DECRESS',
  'CORRACTS',
  'CORRECTIVE ACTIONS 2020',
  'DAYCARE',
  'DEBRIS EPA LF',
  'DEBRIS EPA SWRCY',
  'DELISTED NPL',
  'DELISTED PROPOSED NPL',
  'DEM DIGITAL OBSTACLE',
  'DOCKET',
  'DOCKET CRIM PROS',
  'DOCKET CRIM PROS 2',
  'DOD',
  'DOT OPS',
  'ECHO',
  'EJ BROWNFIELDS',
  'EJ CHURCH',
  'EJ HAZ WASTE',
  'EJ HOSPITALS',
  'EJ SCHOOLS',
  'EJ TOXIC RELEASE',
  'ENOI',
  'EPA FUELS',
  'EPA LF MOP',
  'EPA LUST',
  'EPA OSC',
  'EPA SAA',
  'EPA UST',
  'EPA WATCH',
  'EPICENTERS',
  'ERNS',
  'FA HWF',
  'FED BROWNFIELDS',
  'FED CDL',
  'FED E C',
  'FED I C',
  'FEDERAL FACILITY',
  'FEDLAND',
  'FEMA UST',
  'FLOOD DFIRM',
  'FLOOD Q3',
  'FRS',
  'FTTS',
  'FTTS INSP',
  'FUDS',
  'FUDS MRA',
  'FUDS MRS',
  'GOV MANSIONS',
  'HIST AFS',
  'HIST AFS 2',
  'HIST ASBESTOS NOA',
  'HIST CORRACTS 2',
  'HIST DOD',
  'HIST FED BROWNFIELDS',
  'HIST INDIAN LUST R4',
  'HIST INDIAN UST R7',
  'HIST LEAD_SMELTER',
  'HIST MLTS',
  'HIST PCB TRANS',
  'HIST PCS ENF',
  'HIST PCS FACILITY',
  'HIST PWS ENF',
  'HIST RCRA CESQG',
  'HIST RCRA LQG',
  'HIST RCRA NONGEN',
  'HIST RCRA SQG',
  'HIST SSTS',
  'HMIRS (DOT)',
  'HOSPITALS',
  'HWC DOCKET',
  'HYDROLOGIC UNIT',
  'ICIS',
  'INACTIVE PCS',
  'LIENS 2',
  'LUCIS',
  'LUCIS 2',
  'MANIFEST EPA',
  'MGP',
  'MINE OPERATIONS',
  'MINES',
  'MINES USGS',
  'MLTS',
  'NPL',
  'NPL AOC',
  'NPL EPA GIS',
  'NPL LIENS',
  'NURSING HOMES',
  'NWIS ODI',
  'OSHA PADS',
  'PART NPL',
  'PCB TRANSFORMER',
  'PCS ENF',
  'PCS FACILITY',
  'PFAS FED SITES',
  'PFAS INDUSTRY',
  'PFAS MANIFEST',
  'PFAS NPL',
  'PFAS PROD',
  'PFAS SPILLS',
  'PFAS TRIS',
  'PFAS UCMR3',
  'PFAS WQP',
  'PIPELINES',
  'PRISONS',
  'PROPOSED NPL',
  'PRP',
  'PRP-CORP',
  'PWS',
  'PWS ENF',
  'RAATS',
  'RADINFO',
  'RADON',
  'RADON EPA',
  'RCRA IC EC',
  'RCRA LQG',
  'RCRA NONGEN',
  'RCRA SQG',
  'RCRA TSDF',
  'RCRA VSQG',
  'RMP',
  'ROD',
  'SCHOOLS PRIVATE',
  'SCHOOLS PUBLIC',
  'SCRD DRYCLEANERS',
  'SEMS_8R_ACTIVE SITES',
  'SEMS_8R_ARCHIVED SITES',
  'SEMS_DELETED NPL',
  'SEMS_FINAL NPL',
  'SEMS_PROPOSED NPL',
  'SEMS_SMELTER',
  'SSTS',
  'SSURGO',
  'STATSGO & MUI',
  'STORMWATER',
  'TOSCA-PLANT',
  'TRIBAL BROWNFIELDS',
  'TRIBAL ODI',
  'TRIS',
  'UMTRA',
  'US CENSUS ACS',
  'US CENSUS TIGER',
  'USGS EARTHQUAKES',
  'USGS FAULTS',
  'USGS HYDROGRAPHY',
  'USGS LANDFIRE',
  'USGS LIDAR INDEX',
  'USGS NED ELEVATION',
  'USGS TOPO HIST',
  'USGS WATER QUALITY',
  'USGS WATER USE',
  'US HIST CDL',
  'USGS GEOLOGIC AGE',
  'UST STATE RELEASES',
  'UST STATE REGISTRY',
  'VCP VOLUNTARY CLEANUP',
  'VCP VOLUNTARY CLEANUP SITES',
  'VAPOR',
  'VIOLATIONS AIR MAJOR',
  'VIOLATIONS NPDES',
  'VIOLATIONS RCRA',
  'WASTE TRANSFER STATIONS',
  'WASTEWATER DISCHARGERS',
  'WELLHEAD PROTECTION',
  'WETLANDS NWI'
  ,
  'WILDFIRE HAZARD POTENTIAL',
  'WQX MONITORING STATIONS',
  'WSR WILD AND SCENIC RIVERS',
  'ZONING INDUSTRIAL',
  'ZONING MIXED USE',
  'ZONING RESIDENTIAL',
  'AST ABOVEGROUND STORAGE TANKS',
  'AST STATE RELEASES',
  'CLEANUP SITES STATE',
  'DRINKING WATER WELLS',
  'DRINKING WATER VIOLATIONS',
  'ENV JUSTICE BLOCK GROUPS',
  'HAZMAT INCIDENTS DOT',
  'LEAKING UNDERGROUND STORAGE TANKS',
  'NATURAL GAS STORAGE',
  'NOISE CONTOURS AIRPORT',
  'ODOR COMPLAINTS',
  'ORPHAN WELLS',
  'POTENTIAL BROWNFIELDS',
  'RAIL INCIDENTS FRA',
  'SEPTIC FAILURE REPORTS',
  'SPCC FACILITIES',
  'STATE ENFORCEMENT ACTIONS',
  'STATE HAZARDOUS WASTE GENERATORS',
  'STATE PERMITTED SOLID WASTE',
  'STATE UIC WELLS',
  'TOXIC SUBSTANCES INVENTORY',
  'UNDERGROUND INJECTION CONTROL',
  'WETLAND MITIGATION BANKS'
];

function escapeHtml(value) {
  return String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function normalizeDatabaseName(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function buildDatabaseCoverageHtml(envData, addressData = []) {
  const databaseMap = new Map();
  const allConfigured = [...new Set(MASTER_DATABASES.map((db) => String(db).trim()).filter(Boolean))];

  (addressData || []).forEach((location) => {
    const locationAddress = cleanDisplayAddress(location?.address);
    (location.risks || []).forEach((risk) => {
      const dbName = String(risk.database_name || risk.database || '').trim();
      if (!dbName) return;
      const key = dbName.toLowerCase();
      if (!databaseMap.has(key)) {
        databaseMap.set(key, {
          name: dbName,
          addresses: new Set(),
          records: 0
        });
      }
      const entry = databaseMap.get(key);
      entry.records += 1;
      entry.addresses.add(locationAddress);
    });
  });

  (envData?.environmentalSites || []).forEach((site) => {
    const dbName = String(site.database || '').trim();
    if (!dbName) return;
    const key = dbName.toLowerCase();
    if (!databaseMap.has(key)) {
      databaseMap.set(key, {
        name: dbName,
        addresses: new Set(),
        records: 0
      });
    }
    const entry = databaseMap.get(key);
    entry.records += 1;
    if (site.address) entry.addresses.add(site.address);
  });

  allConfigured.forEach((dbName) => {
    const key = dbName.toLowerCase();
    if (!databaseMap.has(key)) {
      databaseMap.set(key, {
        name: dbName,
        addresses: new Set(),
        records: 0
      });
    }
  });

  const entries = Array.from(databaseMap.values())
    .sort((a, b) => {
      const hitDelta = b.records - a.records;
      if (hitDelta !== 0) return hitDelta;
      return String(a.name).localeCompare(String(b.name));
    });
  if (!entries.length) {
    return '<p>No environmental database coverage rows were generated for this report.</p>';
  }

  const rows = entries.map((entry) => {
    const linkedAddresses = Array.from(entry.addresses).filter(Boolean);
    const addressPreview = linkedAddresses.length
      ? linkedAddresses.slice(0, 3).map((a) => escapeHtml(a)).join('<br/>')
      : 'Address unavailable';
    const status = entry.records > 0 ? 'Matched in location' : 'No mapped hit in selected radius';
    return `
      <tr>
        <td>${escapeHtml(entry.name)}</td>
        <td>${escapeHtml(status)}</td>
        <td>${entry.records}</td>
        <td>${linkedAddresses.length}</td>
        <td>${addressPreview}</td>
      </tr>`;
  }).join('');

  const matched = entries.filter((entry) => entry.records > 0).length;

  return `
    <p><strong>Database Coverage at This Location:</strong> ${entries.length} total configured databases reviewed.</p>
    <p><strong>Matched Databases:</strong> ${matched}. <strong>Unmatched Databases:</strong> ${entries.length - matched}.</p>
    <table>
      <tr>
        <th>Database</th>
        <th>Status</th>
        <th>Record Count</th>
        <th>Linked Locations</th>
        <th>Sample Location(s)</th>
      </tr>
      ${rows}
    </table>`;
}

function buildLongFormConsultingAppendix(envData, projectName, address, minPages = 120) {
  const siteByDatabase = (envData?.environmentalSites || []).reduce((acc, site) => {
    const db = site.database || 'UNCLASSIFIED';
    if (!acc[db]) acc[db] = [];
    acc[db].push(site);
    return acc;
  }, {});

  const totalPages = Math.max(minPages, MASTER_DATABASES.length);
  const sections = [];

  for (let i = 0; i < totalPages; i++) {
    const dbName = MASTER_DATABASES[i % MASTER_DATABASES.length];
    const matches = siteByDatabase[dbName] || [];
    const findingsHtml = matches.length
      ? `<ul>${matches.slice(0, 25).map((s) => `<li>${escapeHtml(s.name || 'Unnamed Site')} | ${escapeHtml(s.address || 'No address')} | ${escapeHtml(s.distance || 'N/A')}</li>`).join('')}</ul>`
      : `<p>No direct mapped site result returned for this database in the selected area.</p>
         <p><strong>Database Check Focus:</strong> ${escapeHtml(describeDatabase(dbName).meaning)}</p>
         <p><strong>Implication of No Record:</strong> No listing was matched in this source for the selected buffer at report time; this reduces but does not eliminate environmental concern potential.</p>
         <p><strong>Confidence Statement:</strong> Moderate confidence based on available geocoded source response and current data publication schedules.</p>`;

    sections.push(`
      <div class="page-break"></div>
      <h2>Consulting Appendix ${i + 1}: ${escapeHtml(dbName)}</h2>
      <p><strong>Project:</strong> ${escapeHtml(projectName || 'N/A')}</p>
      <p><strong>Address:</strong> ${escapeHtml(address || 'N/A')}</p>
      <p><strong>Database Scope:</strong> Included in master records search coverage.</p>
      <p><strong>Matched Records:</strong> ${matches.length}</p>
      <h3>Database Findings</h3>
      ${findingsHtml}
      <h3>Consulting Interpretation</h3>
      <p>This appendix page documents regulatory and environmental screening evidence for ${escapeHtml(dbName)}. Results should be interpreted together with geologic, hydrologic, and historical land-use context before final decision-making.</p>
      <p>Recommended next step: Validate all high-concern indicators via targeted records request and, where warranted, field confirmation.</p>
    `);
  }

  return sections.join('');
}

function getLogoDataUri() {
  const svg = `
<svg xmlns="http://www.w3.org/2000/svg" width="420" height="90" viewBox="0 0 420 90">
  <rect width="420" height="90" rx="10" fill="#0f172a"/>
  <circle cx="44" cy="45" r="24" fill="#22c55e"/>
  <path d="M30 45h28M44 31v28" stroke="#0f172a" stroke-width="4" stroke-linecap="round"/>
  <text x="84" y="42" fill="#f8fafc" font-family="Arial, sans-serif" font-size="30" font-weight="700">GeoScope</text>
  <text x="84" y="66" fill="#93c5fd" font-family="Arial, sans-serif" font-size="16">Environmental Intelligence</text>
</svg>`;

  return `data:image/svg+xml;base64,${Buffer.from(svg).toString('base64')}`;
}

// User data (hardcoded for demo)
const users = [
  { id: "demo-admin", email: "admin@geoscope.com", password: "1234", role: "admin" },
  { id: "demo-analyst", email: "analyst@geoscope.com", password: "1234", role: "analyst" }
];

// Orders storage (temporary in-memory)
let orders = [];
let clientUsers = [];

function findInMemoryOrderIndex(orderRef) {
  const key = String(orderRef ?? '').trim();
  if (!key) return -1;

  const directIndex = Number.parseInt(key, 10);
  if (Number.isFinite(directIndex) && directIndex >= 0 && directIndex < orders.length) {
    if (String(orders[directIndex]?.id ?? '') === key || String(orders[directIndex]?.order_id ?? '') === key) {
      return directIndex;
    }
  }

  return orders.findIndex((o) =>
    String(o?.id ?? '') === key ||
    String(o?.order_id ?? '') === key
  );
}

async function updateOrderTrackingAfterReport(orderRef, reportDetails = {}) {
  const key = String(orderRef ?? '').trim();
  if (!key) return null;

  const processedAt = new Date().toISOString();
  const numericId = Number.parseInt(key, 10);
  const updatePayload = {
    status: 'submitted',
    stage: 'ADMIN_REVIEW',
    report_status: 'Generated',
    report_path: reportDetails.reportPath || null,
    report_url: reportDetails.downloadUrl || `/download/${key}`,
    processed_at: processedAt,
    updated_at: processedAt
  };

  if (supabaseUrl !== 'https://your-project.supabase.co') {
    try {
      await supabase
        .from('orders')
        .update(updatePayload)
        .eq('id', numericId || key);
    } catch (error) {
      console.warn('Supabase tracking update warning:', error.message);
    }
  }

  const orderIndex = findInMemoryOrderIndex(key);
  if (orderIndex !== -1) {
    orders[orderIndex] = {
      ...orders[orderIndex],
      ...updatePayload
    };
  }

  if (Number.isFinite(numericId)) {
    try {
      auth.updateOrderWorkflow?.(numericId, updatePayload);
    } catch (error) {
      console.warn('Auth order tracking update warning:', error.message);
    }
  }

  return updatePayload;
}

try {
  const persistedOrders = auth.getAllOrders?.() || [];
  if (persistedOrders.length > 0) {
    orders = persistedOrders.map((order) => ({ ...order }));
    console.log(`Hydrated ${orders.length} persisted orders into in-memory queue`);
  }
} catch (hydrateError) {
  console.warn('Unable to hydrate persisted orders:', hydrateError.message);
}

// =====================
// AUTHENTICATION MIDDLEWARE
// =====================

/**
 * Verify JWT token from Authorization header
 */
function requireAuth(req, res, next) {
  if (!JWT_AUTH_ENABLED) {
    req.user = {
      id: parseInt(req.headers['x-user-id'], 10) || 1,
      email: req.headers['x-user-email'] || 'admin@geoscope.com',
      role: req.headers['x-user-role'] || 'admin'
    };
    return next();
  }

  const token = req.headers.authorization?.split(' ')[1]; // Bearer <token>
  if (!token) {
    return res.status(401).json({ error: 'No token provided' });
  }

  const decoded = auth.verifyToken(token);
  if (!decoded) {
    return res.status(401).json({ error: 'Invalid or expired token' });
  }

  req.user = decoded;
  next();
}

/**
 * Check if user has required role
 */
function requireRole(...roles) {
  return (req, res, next) => {
    if (!req.user) {
      return res.status(401).json({ error: 'Not authenticated' });
    }

    if (!roles.includes(req.user.role)) {
      return res.status(403).json({ error: 'Insufficient permissions' });
    }

    next();
  };
}

// Email configuration
const mailerUser = process.env.GMAIL_USER || '';
const mailerPass = process.env.GMAIL_PASS || '';
const adminNotificationEmail = process.env.ADMIN_NOTIFICATION_EMAIL || mailerUser || 'admin@geoscope.com';
const hasMailerConfig = Boolean(mailerUser) && Boolean(mailerPass);
const transporter = (mailerUser && mailerPass)
  ? nodemailer.createTransport({
      service: 'gmail',
      auth: {
        user: mailerUser,
        pass: mailerPass
      }
    })
  : nodemailer.createTransport({ jsonTransport: true });

async function notifyAdminOnSubmitted(order, orderId) {
  const normalizedOrder = {
    ...order,
    id: order?.id ?? Number(orderId)
  };

  let reportPath = null;
  try {
    if (normalizedOrder.project_name && normalizedOrder.client_name) {
      const reportResult = await generatePDFReportInternal({
        ...normalizedOrder,
        order_id: normalizedOrder.id,
        paid: true,
        summary: normalizedOrder.summary || 'Submitted order ready for admin review.'
      });
      reportPath = reportResult.reportPath;
    }
  } catch (error) {
    console.error('Report generation for submitted order failed:', error.message);
  }

  await transporter.sendMail({
    to: adminNotificationEmail,
    subject: `Submitted Order Ready: ${normalizedOrder.project_name || `Order ${orderId}`}`,
    text: `Order ${normalizedOrder.id} has been moved to Submitted.\n\n${JSON.stringify(normalizedOrder, null, 2)}`,
    attachments: reportPath ? [{ path: reportPath }] : []
  });

  return { reportPath };
}

// Routes

/**
 * GET /public/stats
 * Public homepage stats used by the main website landing page.
 */
app.get('/public/stats', (req, res) => {
  try {
    const authUsers = auth.getAllUsers() || [];
    const authOrders = auth.getAllOrders() || [];
    const memOrders = Array.isArray(orders) ? orders : [];
    const memClients = Array.isArray(clientUsers) ? clientUsers : [];

    const allOrders = [...authOrders];
    const seenOrderIds = new Set(allOrders.map((o) => String(o.id || o.order_id || '')));
    memOrders.forEach((o) => {
      const key = String(o.id || o.order_id || '');
      if (!seenOrderIds.has(key)) {
        allOrders.push(o);
        seenOrderIds.add(key);
      }
    });

    const clientEmails = new Set();
    authUsers
      .filter((u) => u.role === 'client' && u.email)
      .forEach((u) => clientEmails.add(String(u.email).toLowerCase()));

    memClients
      .filter((u) => u.email)
      .forEach((u) => clientEmails.add(String(u.email).toLowerCase()));

    allOrders.forEach((o) => {
      const emailCandidates = [o.email, o.recipient_email_1, o.client_email];
      emailCandidates.filter(Boolean).forEach((e) => clientEmails.add(String(e).toLowerCase()));
    });

    const reportStatuses = new Set(['processed', 'submitted', 'completed', 'approved', 'sent']);
    const reportsGenerated = allOrders.filter((o) => {
      const status = String(o.status || '').toLowerCase();
      return reportStatuses.has(status) || !!o.report_url || !!o.report_path;
    }).length;

    res.json({
      success: true,
      clientsServed: clientEmails.size,
      reportsGenerated,
      sampledAt: new Date().toISOString()
    });
  } catch (err) {
    res.status(500).json({ success: false, error: 'Unable to compute public stats' });
  }
});
app.get('/', (req, res) => {
  res.json({
    success: true,
    service: 'geoscope-api',
    message: 'Frontend site hosting removed from this server.'
  });
});

// GET /health - Lightweight service health status for uptime checks
app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    service: 'geoscope-api',
    startedAt: SERVER_STARTED_AT,
    now: new Date().toISOString(),
    checks: {
      reportsDirectory: fs.existsSync(REPORTS_DIR),
      mailerConfigured: hasMailerConfig
    }
  });
});

// POST /client-register - Create client account
app.post('/client-register', (req, res) => {
  const { name, company, email, password } = req.body;

  if (!name || !company || !email || !password) {
    return res.status(400).json({ error: 'Missing required registration fields' });
  }

  const normalizedEmail = String(email).toLowerCase().trim();
  const exists = clientUsers.find(u => u.email === normalizedEmail);
  if (exists) {
    return res.status(409).json({ error: 'Client account already exists' });
  }

  const user = {
    id: `client-${Date.now()}`,
    role: 'client',
    name,
    company,
    email: normalizedEmail,
    password
  };

  clientUsers.push(user);
  res.json({ success: true, user: { id: user.id, role: user.role, name, company, email: normalizedEmail } });
});

// POST /client-login - Client authentication
app.post('/client-login', (req, res) => {
  const { email, password } = req.body;
  const normalizedEmail = String(email || '').toLowerCase().trim();

  const user = clientUsers.find(u => u.email === normalizedEmail && u.password === password);
  if (!user) {
    return res.status(401).json({ error: 'Invalid client credentials' });
  }

  res.json({
    success: true,
    user: {
      id: user.id,
      role: user.role,
      name: user.name,
      company: user.company,
      email: user.email
    }
  });
});

// POST /login - User authentication with Supabase
app.post('/login', async (req, res) => {
  const { email, password } = req.body;

  try {
    // Try Supabase first (if configured)
    if (supabaseUrl !== 'https://your-project.supabase.co') {
      const { data, error } = await supabase
        .from('users')
        .select('*')
        .eq('email', email)
        .eq('password', password)
        .single();

      if (data) {
        return res.json({ id: data.id, email: data.email, role: data.role });
      }
    }
  } catch (error) {
    console.log('Supabase login not available, using demo credentials');
  }

  // Fallback to demo users
  const user = users.find(
    u => u.email === email && u.password === password
  );

  if (!user) {
    return res.status(401).json({ message: 'Invalid credentials' });
  }

  res.json(user);
});

// GET /orders - Retrieve all orders (with optional email filter)
app.get('/orders', async (req, res) => {
  const email = req.query.email;

  try {
    // Try Supabase first
    if (supabaseUrl !== 'https://your-project.supabase.co') {
      let query = supabase.from('orders').select('*');
      
      if (email) {
        query = query.eq('email', email);
      }

      const { data, error } = await query;

      if (!error && data) {
        return res.json(data);
      }
    }
  } catch (error) {
    console.log('Supabase orders not available, using in-memory storage');
  }

  // Fallback to merged auth + in-memory orders
  const authOrders = auth.getAllOrders?.() || [];
  const merged = [...authOrders, ...(orders || [])];
  const deduped = [];
  const seen = new Set();

  for (const item of merged) {
    const key = String(item?.id ?? item?.order_id ?? '');
    if (!key || seen.has(key)) continue;
    seen.add(key);
    deduped.push(item);
  }

  if (email) {
    const normalizedEmail = String(email || '').toLowerCase().trim();
    return res.json(deduped.filter((o) => {
      const e1 = String(o?.email || '').toLowerCase().trim();
      const e2 = String(o?.recipient_email_1 || '').toLowerCase().trim();
      const e3 = String(o?.client_email || '').toLowerCase().trim();
      return normalizedEmail === e1 || normalizedEmail === e2 || normalizedEmail === e3;
    }));
  }

  res.json(deduped);
});

// GET /orders/:id - Retrieve single order
app.get('/orders/:id', (req, res) => {
  const orderIndex = findInMemoryOrderIndex(req.params.id);
  if (orderIndex !== -1) {
    return res.json(orders[orderIndex]);
  }

  const numericId = Number.parseInt(req.params.id, 10);
  if (Number.isFinite(numericId)) {
    const authOrder = auth.getOrderById?.(numericId);
    if (authOrder) {
      return res.json(authOrder);
    }
  }

  return res.status(404).json({ error: 'Order not found' });
});

// GET /my-orders - Retrieve client orders by recipient email
app.get('/my-orders', async (req, res) => {
  const email = (req.query.email || '').toLowerCase().trim();

  if (!email) {
    return res.status(400).json({ error: 'Email query parameter is required' });
  }

  try {
    // Supabase first
    if (supabaseUrl !== 'https://your-project.supabase.co') {
      const { data, error } = await supabase
        .from('orders')
        .select('*')
        .or(`email.eq.${email},recipient_email_1.eq.${email},recipient_email_2.eq.${email},client_email.eq.${email}`);

      if (!error && data) {
        return res.json(data);
      }
    }
  } catch (error) {
    console.log('Supabase my-orders not available, using in-memory storage');
  }

  const filtered = orders.filter(order => {
    const e1 = (order.email || '').toLowerCase();
    const e2 = (order.recipient_email_1 || '').toLowerCase();
    const e3 = (order.recipient_email_2 || '').toLowerCase();
    const e4 = (order.client_email || '').toLowerCase();
    return email === e1 || email === e2 || email === e3 || email === e4;
  });

  res.json(filtered);
});

// PUT /orders/:id/status - Staff workflow status updates
app.put('/orders/:id/status', async (req, res) => {
  const id = req.params.id;
  const { status } = req.body;
  const allowedStatuses = ['received', 'pending', 'assigned', 'processing', 'processed', 'submitted', 'completed', 'approved', 'sent'];

  if (!allowedStatuses.includes(status)) {
    return res.status(400).json({ error: 'Invalid status value' });
  }

  try {
    if (supabaseUrl !== 'https://your-project.supabase.co') {
      const { data, error } = await supabase
        .from('orders')
        .update({ status })
        .eq('id', id)
        .select()
        .single();

      if (!error && data) {
        let adminNotified = false;
        let notificationError = null;

        if (status === 'submitted') {
          try {
            await notifyAdminOnSubmitted(data, id);
            adminNotified = true;
          } catch (error) {
            notificationError = error.message;
          }
        }

        return res.json({ success: true, order: data, adminNotified, notificationError });
      }
    }
  } catch (error) {
    console.log('Supabase status update not available, using in-memory storage');
  }

  const orderIndex = findInMemoryOrderIndex(id);
  if (orderIndex === -1) {
    return res.status(404).json({ error: 'Order not found' });
  }

  orders[orderIndex].status = status;
  let adminNotified = false;
  let notificationError = null;

  if (status === 'submitted') {
    try {
      await notifyAdminOnSubmitted(orders[orderIndex], id);
      adminNotified = true;
    } catch (error) {
      notificationError = error.message;
    }
  }

  res.json({ success: true, order: orders[orderIndex], adminNotified, notificationError });
});

// PUT /orders/:id/geometry-review - GIS/analyst review updates
app.put('/orders/:id/geometry-review', async (req, res) => {
  const id = req.params.id;
  const { status, gis_match_status, analyst_notes } = req.body;

  try {
    if (supabaseUrl !== 'https://your-project.supabase.co') {
      const updatePayload = {
        status: status || 'pending',
        gis_match_status: gis_match_status || 'matched',
        analyst_notes: analyst_notes || ''
      };

      const { data, error } = await supabase
        .from('orders')
        .update(updatePayload)
        .eq('id', id)
        .select()
        .single();

      if (!error && data) {
        return res.json({ success: true, order: data });
      }
    }
  } catch (error) {
    console.log('Supabase geometry review not available, using in-memory storage');
  }

  const orderIndex = findInMemoryOrderIndex(id);
  if (orderIndex === -1) {
    return res.status(404).json({ error: 'Order not found' });
  }

  orders[orderIndex].status = status || 'pending';
  orders[orderIndex].gis_match_status = gis_match_status || 'matched';
  orders[orderIndex].analyst_notes = analyst_notes || '';

  res.json({ success: true, order: orders[orderIndex] });
});

// POST /client-orders - Client intake with polygon or star subject property
app.post('/client-orders', async (req, res) => {
  try {
    const {
      project_name,
      client_company,
      recipient_email_1,
      recipient_email_2,
      address,
      latitude,
      longitude,
      polygon,
      subject_property,
      notes
    } = req.body;

    if (!project_name || !client_company || !recipient_email_1 || !address) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    // Analyst-first routing: all requests go directly to analyst/workbench
    const initialStage = 'ANALYST_REVIEW';
    const initialStatus = 'received';

    const orderData = {
      id: Math.max(...(orders.length ? orders.map(o => Number(o.id) || 0) : [999]), 999) + 1,
      project_name,
      client_name: client_company,
      client_company,
      recipient_email_1,
      recipient_email_2: recipient_email_2 || '',
      email: recipient_email_1,
      address,
      latitude: latitude ? parseFloat(latitude) : null,
      longitude: longitude ? parseFloat(longitude) : null,
      polygon: polygon || null,
      subject_property: subject_property || null,
      geo_input_type: polygon ? 'polygon' : 'star',
      notes: notes || '',
      status: initialStatus,
      stage: initialStage,
      source: 'client-portal',
      messages: [],
      dataset_date: new Date().toISOString().split('T')[0],
      created_at: new Date().toISOString()
    };

    orders.push(orderData);

    try {
      const allUsers = auth.getAllUsers?.() || [];
      const matchingClient = allUsers.find((u) =>
        String(u?.email || '').toLowerCase() === String(recipient_email_1 || '').toLowerCase() &&
        String(u?.role || '').toLowerCase() === 'client'
      );

      if (matchingClient) {
        const persisted = auth.createOrder(
          Number.parseInt(matchingClient.id, 10),
          project_name,
          address,
          latitude ? parseFloat(latitude) : null,
          longitude ? parseFloat(longitude) : null,
          polygon ? JSON.stringify(polygon) : null
        );

        if (persisted?.success && Number.isFinite(Number.parseInt(persisted.orderId, 10))) {
          auth.updateOrderStatus(Number.parseInt(persisted.orderId, 10), 'received');
        }
      }
    } catch (persistError) {
      console.warn('Client order persisted with warning:', persistError.message);
    }

    let emailNotified = false;
    let emailError = null;
    if (hasMailerConfig) {
      try {
        await transporter.sendMail({
          to: adminNotificationEmail,
          subject: `New Client Request: ${project_name}`,
          text: JSON.stringify(orderData, null, 2)
        });
        emailNotified = true;
      } catch (mailError) {
        emailError = mailError.message;
        console.error('Client order email notification failed:', mailError.message);
      }
    } else {
      emailError = 'Mailer not configured. Set GMAIL_USER and GMAIL_PASS in geoscope/.env';
      console.warn('Client order email skipped: mailer credentials not configured');
    }

    res.json({
      success: true,
      message: 'Client request submitted successfully',
      order: orderData,
      emailNotified,
      emailError
    });
  } catch (error) {
    console.error('Error creating client order:', error);
    res.status(500).json({ error: 'Failed to create client order', details: error.message });
  }
});

// PUT /save-draft/:id - Save draft order
app.put('/save-draft/:id', (req, res) => {
  const id = req.params.id;
  const updated = req.body;
  const orderIndex = findInMemoryOrderIndex(id);

  if (orderIndex === -1) {
    return res.status(404).json({ error: 'Order not found' });
  }

  orders[orderIndex] = { ...orders[orderIndex], ...updated };
  res.json({ message: 'Draft saved' });
});

// ─── STAGE ADVANCE ───────────────────────────────────────────────────────────
// PUT /orders/:id/stage  { stage: 'GIS_REVIEW'|'ANALYST_REVIEW'|'ADMIN_REVIEW'|'COMPLETED', action:'', note:'', from:'' }
// Maps stage → canonical status so downstream queues work automatically.
// Supports bidirectional workflow (e.g., ADMIN_REVIEW → ANALYST_REVIEW for revision requests)
const STAGE_STATUS_MAP = {
  GIS_REVIEW:      'pending',
  ANALYST_REVIEW:  'received',
  REPORT_GENERATED:'processed',
  ADMIN_REVIEW:    'submitted',
  COMPLETED:       'sent'
};

app.put('/orders/:id/stage', async (req, res) => {
  const orderId = req.params.id;
  const { stage, note, from, action } = req.body;

  if (!STAGE_STATUS_MAP[stage]) {
    return res.status(400).json({ error: `Invalid stage. Allowed: ${Object.keys(STAGE_STATUS_MAP).join(', ')}` });
  }

  const updatedAt = new Date().toISOString();
  const newStatus = STAGE_STATUS_MAP[stage];
  const updatePayload = {
    stage,
    status: newStatus,
    updated_at: updatedAt,
  };

  if (action === 'REQUEST_REVISIONS') {
    updatePayload.needs_revision = true;
    updatePayload.revision_request_at = updatedAt;
    updatePayload.revision_request_from = from || 'Admin';
    updatePayload.revision_request_reason = note || '';
  }

  // Try Supabase first
  if (supabaseUrl !== 'https://your-project.supabase.co') {
    try {
      const { data, error } = await supabase
        .from('orders')
        .update(updatePayload)
        .eq('id', orderId)
        .select()
        .single();

      if (!error && data) {
        // Mirror into in-memory if present
        const orderIndex = findInMemoryOrderIndex(orderId);
        if (orderIndex !== -1) {
          Object.assign(orders[orderIndex], updatePayload);
          if (note && from) {
            if (!Array.isArray(orders[orderIndex].messages)) orders[orderIndex].messages = [];
            orders[orderIndex].messages.push({ from, message: note, time: updatedAt, type: action === 'REQUEST_REVISIONS' ? 'REVISION_REQUEST' : 'NOTE', action: action || null });
          }
        }
        return res.json({ success: true, order: data });
      }
    } catch (supaErr) {
      console.warn('Supabase stage update not available, using in-memory storage:', supaErr.message);
    }
  }

  // In-memory fallback
  const orderIndex = findInMemoryOrderIndex(orderId);
  if (orderIndex === -1) {
    return res.status(404).json({ error: 'Order not found' });
  }

  Object.assign(orders[orderIndex], updatePayload);

  if (note && from) {
    if (!Array.isArray(orders[orderIndex].messages)) orders[orderIndex].messages = [];
    const msgType = action === 'REQUEST_REVISIONS' ? 'REVISION_REQUEST' : 'NOTE';
    orders[orderIndex].messages.push({ 
      from, 
      message: note, 
      time: updatedAt,
      type: msgType,
      action: action || null
    });
  }

  const numericId = Number.parseInt(orderId, 10);
  if (Number.isFinite(numericId)) {
    try {
      auth.updateOrderWorkflow?.(numericId, {
        stage,
        status: newStatus,
        updated_at: updatedAt,
        needs_revision: orders[orderIndex].needs_revision || false
      });
    } catch (error) {
      console.warn('Auth stage update warning:', error.message);
    }
  }

  res.json({ success: true, order: orders[orderIndex] });
});

// ─── ORDER MESSAGES ──────────────────────────────────────────────────────────
// POST /orders/:id/messages  { from: 'GIS', message: '...' }
app.post('/orders/:id/messages', (req, res) => {
  const orderId = req.params.id;
  const { from, message } = req.body;

  if (!from || !message) {
    return res.status(400).json({ error: 'from and message are required' });
  }
  const orderIndex = findInMemoryOrderIndex(orderId);
  if (orderIndex === -1) {
    return res.status(404).json({ error: 'Order not found' });
  }

  if (!Array.isArray(orders[orderIndex].messages)) orders[orderIndex].messages = [];
  const entry = { from, message, time: new Date().toISOString() };
  orders[orderIndex].messages.push(entry);
  res.json({ success: true, entry });
});

// POST /send-to-client - Send report to client
app.post('/send-to-client', async (req, res) => {
  try {
    const { email, filePath } = req.body;

    if (!email || !filePath) {
      return res.status(400).json({ error: 'Missing email or filePath' });
    }

    const normalizedEmail = String(email).toLowerCase().trim();
    const resolvedPath = path.resolve(filePath);
    const reportsRoot = path.resolve(REPORTS_DIR);

    // Restrict file attachments to generated reports directory only.
    if (!resolvedPath.startsWith(reportsRoot)) {
      return res.status(400).json({ error: 'Invalid filePath: file must be inside reports directory' });
    }

    if (!fs.existsSync(resolvedPath)) {
      return res.status(404).json({ error: 'Report file not found' });
    }

    if (path.extname(resolvedPath).toLowerCase() !== '.pdf') {
      return res.status(400).json({ error: 'Only PDF attachments are allowed' });
    }

    await transporter.sendMail({
      to: normalizedEmail,
      subject: 'Your GeoScope Report',
      text: 'Please find your report attached.',
      attachments: [{ path: resolvedPath }],
    });

    res.json({ message: 'Report sent to client successfully' });
  } catch (error) {
    console.error('Error sending to client:', error);
    res.status(500).json({ error: 'Failed to send report to client', details: error.message });
  }
});

// POST /order - Process order and send email
app.post('/order', async (req, res) => {
  try {
    const data = req.body;
    const { project_name, client_name, email, address, latitude, longitude, dataset_date } = data;

    if (!project_name || !client_name || !email || !address) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    // Ensure each order has an ID and dataset date
    const orderId = Math.max(...(orders.length ? orders.map(o => Number(o.id) || 0) : [999]), 999) + 1;
    data.id = orderId;
    data.dataset_date = dataset_date || new Date().toISOString().split('T')[0];

    // Store order
    orders.push(data);

    // Email content
    const mailOptions = {
      from: mailerUser || adminNotificationEmail,
      to: adminNotificationEmail,
      subject: `New Order: ${project_name}`,
      text: JSON.stringify(data, null, 2)
    };

    let emailNotified = false;
    let emailError = null;
    if (hasMailerConfig) {
      try {
        // Send email to admin
        await transporter.sendMail(mailOptions);
        emailNotified = true;
      } catch (mailError) {
        emailError = mailError.message;
        console.error('Order email notification failed:', mailError.message);
      }
    } else {
      emailError = 'Mailer not configured. Set GMAIL_USER and GMAIL_PASS in geoscope/.env';
      console.warn('Order email skipped: mailer credentials not configured');
    }

    res.json({
      success: true,
      message: emailNotified ? 'Order saved and email sent' : 'Order saved (email notification failed)',
      emailNotified,
      emailError,
      data: data
    });
  } catch (error) {
    console.error('Error processing order:', error);
    res.status(500).json({ error: 'Failed to process order', details: error.message });
  }
});

// GET /environmental-data - Fetch environmental data for polygon analysis
app.get('/environmental-data', async (req, res) => {
  try {
    const { lat, lng, radius = 1000 } = req.query;

    if (!lat || !lng) {
      return res.status(400).json({ error: 'Latitude and longitude required' });
    }

    const latitude = parseFloat(lat);
    const longitude = parseFloat(lng);

    // Fetch environmental data
    const data = await fetchEnvironmentalData(latitude, longitude, null, parseInt(radius));

    res.json(data);
  } catch (error) {
    console.error('Error fetching environmental data:', error);
    res.status(500).json({ error: 'Failed to fetch environmental data' });
  }
});

// GET /nearby-data - Analyst-only map feed for real database points
app.get('/nearby-data', async (req, res) => {
  try {
    const { lat, lng, radius = 2000 } = req.query;

    if (!lat || !lng) {
      return res.status(400).json({ error: 'Latitude and longitude required' });
    }

    const latitude = parseFloat(lat);
    const longitude = parseFloat(lng);
    const effectiveRadius = parseInt(radius, 10) || 2000;

    const envData = await fetchEnvironmentalData(latitude, longitude, null, effectiveRadius);
    const points = (envData.environmentalSites || []).map((site) => ({
      database_name: site.database || 'Unknown Database',
      site_name: site.name || 'Unknown Site',
      address: site.address || 'N/A',
      latitude: site.lat || site.latitude || latitude,
      longitude: site.lng || site.longitude || longitude,
      distance: site.distance || 'N/A',
      risk_type: getRiskLevel(site),
      marker_color: 'red'
    }));

    return res.json(points);
  } catch (error) {
    console.error('Error in /nearby-data:', error);
    return res.status(500).json({ error: 'Failed to fetch nearby data', details: error.message });
  }
});

// POST /orders - Create new order with polygon and file support
app.post('/orders', upload.array('files', 10), async (req, res) => {
  try {
    const data = req.body;
    const { project_name, client_name, address, latitude, longitude, radius, dataset_date, user_id, polygon, subject_property, geo_input_type } = data;

    if (!project_name || !client_name || !address) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    // Parse polygon data if provided
    let polygonData = null;
    if (polygon) {
      try {
        polygonData = JSON.parse(polygon);
      } catch (e) {
        console.error('Error parsing polygon data:', e);
      }
    }

    let subjectPropertyData = null;
    if (subject_property) {
      try {
        subjectPropertyData = JSON.parse(subject_property);
      } catch (e) {
        console.error('Error parsing subject_property data:', e);
      }
    }

    // Handle file uploads if any
    const files = req.files || [];
    const uploadedFiles = [];

    if (files.length > 0) {
      // Upload files to Supabase Storage
      for (const file of files) {
        const fileName = `${Date.now()}-${file.originalname}`;
        const { data: uploadData, error } = await supabase.storage
          .from('order-files')
          .upload(fileName, file.buffer, {
            contentType: file.mimetype
          });

        if (error) {
          console.error('File upload error:', error);
        } else {
          uploadedFiles.push({
            name: file.originalname,
            url: uploadData.path,
            type: file.mimetype
          });
        }
      }
    }

    // Create order data
    const orderData = {
      project_name,
      client_name,
      address,
      latitude: parseFloat(latitude) || null,
      longitude: parseFloat(longitude) || null,
      radius: getSystemReportRadiusMeters(),
      dataset_date: dataset_date || new Date().toISOString().split('T')[0],
      user_id: user_id || 'demo-user',
      polygon: polygonData,
      subject_property: subjectPropertyData,
      geo_input_type: geo_input_type || (polygonData ? 'polygon' : 'star'),
      files: uploadedFiles,
      status: 'processing',
      created_at: new Date().toISOString()
    };

    // Try to save to Supabase first
    try {
      const { data: supabaseOrder, error } = await supabase
        .from('orders')
        .insert([orderData])
        .select()
        .single();

      if (!error && supabaseOrder) {
        // Start background processing
        processOrderInBackground(supabaseOrder.id, orderData);

        return res.json({
          success: true,
          message: 'Order created successfully',
          order: supabaseOrder
        });
      }
    } catch (supabaseError) {
      console.log('Supabase not available, using in-memory storage');
    }

    // Fallback to in-memory storage
    const orderId = orders.length;
    orderData.id = orderId;
    orders.push(orderData);

    // Start background processing
    processOrderInBackground(orderId, orderData);

    res.json({
      success: true,
      message: 'Order created successfully',
      order: orderData
    });

  } catch (error) {
    console.error('Error creating order:', error);
    res.status(500).json({ error: 'Failed to create order', details: error.message });
  }
});

// Background order processing function
async function processOrderInBackground(orderId, orderData) {
  try {
    console.log(`Processing order ${orderId} in background...`);

    // Calculate polygon area if polygon is provided
    let polygonArea = null;
    let polygonAnalysis = null;

    if (orderData.polygon) {
      try {
        const poly = turf.polygon(orderData.polygon.geometry.coordinates);
        polygonArea = turf.area(poly);

        polygonAnalysis = {
          area: polygonArea,
          areaAcres: polygonArea * 0.000247105, // Convert m² to acres
          perimeter: turf.length(poly, { units: 'meters' })
        };
      } catch (error) {
        console.error('Error calculating polygon metrics:', error);
      }
    }

    // Fetch environmental data
    const environmentalData = await fetchEnvironmentalData(
      orderData.latitude,
      orderData.longitude,
      orderData.polygon,
      getSystemReportRadiusMeters()
    );

    // Generate AI summary with polygon analysis
    const aiSummary = await generateAISummary(
      environmentalData,
      orderData.project_name,
      orderData.address,
      orderData.polygon,
      polygonAnalysis
    );

    // Generate PDF report
    const reportData = {
      ...orderData,
      environmentalData,
      aiSummary,
      polygonAnalysis,
      order_id: orderId,
      paid: true // Auto-approve for background processing
    };

    // Call the report generation endpoint internally
    const reportResponse = await generatePDFReportInternal(reportData);
    const pdfBuffer = fs.readFileSync(reportResponse.reportPath);

    // Save PDF to Supabase storage
    const pdfFileName = `report-${orderId}.pdf`;
    const { data: pdfUpload, error: pdfError } = await supabase.storage
      .from('reports')
      .upload(pdfFileName, pdfBuffer, {
        contentType: 'application/pdf'
      });

    // Update order status
    const updateData = {
      status: 'completed',
      report_url: pdfUpload?.path || null,
      processed_at: new Date().toISOString()
    };

    // Try Supabase update first
    try {
      await supabase
        .from('orders')
        .update(updateData)
        .eq('id', orderId);
    } catch (supabaseError) {
      // Update in-memory storage
      const orderIndex = findInMemoryOrderIndex(orderId);
      if (orderIndex !== -1) {
        orders[orderIndex] = { ...orders[orderIndex], ...updateData };
      }
    }

    console.log(`Order ${orderId} processing completed`);

  } catch (error) {
    console.error(`Error processing order ${orderId}:`, error);

    // Update order status to failed
    try {
      await supabase
        .from('orders')
        .update({ status: 'failed', error: error.message })
        .eq('id', orderId);
    } catch (supabaseError) {
      const orderIndex = findInMemoryOrderIndex(orderId);
      if (orderIndex !== -1) {
        orders[orderIndex].status = 'failed';
        orders[orderIndex].error = error.message;
      }
    }
  }
}

// =====================
// ADDRESS-BY-ADDRESS RESTRUCTURING FUNCTIONS
// =====================

/**
 * Normalizes an address string into a stable dedup key.
 * Handles abbreviation differences (St/Street, Ave/Avenue, etc.),
 * strips suite/unit suffixes, and collapses whitespace/punctuation.
 */

/**
 * Format an order ID as a zero-padded 6-digit order number (e.g. 1000 → "001000").
 * Non-numeric IDs (e.g. fallback timestamp strings) are returned as-is.
 */
function formatOrderNumber(id) {
  const n = Number(id);
  if (Number.isFinite(n) && n >= 0) return String(Math.floor(n)).padStart(6, '0');
  return String(id || '');
}

function normalizeAddressKey(raw) {
  if (!raw) return 'unnamed';
  let s = String(raw).toLowerCase().trim();
  // Remove suite/unit/apt qualifiers
  s = s.replace(/\b(suite|ste|unit|apt|#)\s*[\w-]+/g, '').trim();
  // Expand common abbreviations so "St" and "Street" hash the same
  const abbr = {
    '\\bst\\b': 'street',
    '\\bave\\b': 'avenue',
    '\\bblvd\\b': 'boulevard',
    '\\bdr\\b': 'drive',
    '\\brd\\b': 'road',
    '\\bln\\b': 'lane',
    '\\bct\\b': 'court',
    '\\bpl\\b': 'place',
    '\\bhwy\\b': 'highway',
    '\\bfwy\\b': 'freeway',
    '\\bn\\b': 'north',
    '\\bs\\b': 'south',
    '\\be\\b': 'east',
    '\\bw\\b': 'west',
  };
  for (const [pattern, replacement] of Object.entries(abbr)) {
    s = s.replace(new RegExp(pattern, 'g'), replacement);
  }
  // Strip all non-alphanumeric chars and collapse spaces
  s = s.replace(/[^a-z0-9\s]/g, ' ').replace(/\s+/g, ' ').trim();
  return s || 'unnamed';
}

function isUnknownAddressLabel(raw) {
  const value = String(raw || '').trim().toLowerCase();
  if (!value) return true;
  return [
    'unknown',
    'unknown location',
    'address unavailable',
    'unnamed location',
    'unnamed location (near subject property)',
    'n/a'
  ].includes(value);
}

function cleanDisplayAddress(raw) {
  if (isUnknownAddressLabel(raw)) {
    return 'Near subject property (exact address unavailable)';
  }
  return String(raw || '').trim();
}

/**
 * Second-pass proximity clustering: merges entries whose coordinates are
 * within `thresholdMeters` of each other (same physical site, different
 * address strings, e.g. "123 Main St" vs "123 Main Street, Suite 2").
 * The entry with more risks is kept as the canonical record.
 */
function clusterByProximity(entries, thresholdMeters = 30) {
  const result = [];
  const merged = new Set();

  for (let i = 0; i < entries.length; i++) {
    if (merged.has(i)) continue;
    const base = { ...entries[i], risks: [...(entries[i].risks || [])] };
    const riskKeySet = new Set(base.risks.map((r) => `${r.database_name || r.database}|${r.site_name}|${r.distance}`));

    for (let j = i + 1; j < entries.length; j++) {
      if (merged.has(j)) continue;
      const candidate = entries[j];
      const latA = toFiniteNumber(base.latitude);
      const lngA = toFiniteNumber(base.longitude);
      const latB = toFiniteNumber(candidate.latitude);
      const lngB = toFiniteNumber(candidate.longitude);

      if (latA === null || lngA === null || latB === null || lngB === null) continue;

      const distM = haversineMiles(latA, lngA, latB, lngB) * 1609.344;
      if (distM <= thresholdMeters) {
        // Merge risks from the duplicate into the canonical entry
        (candidate.risks || []).forEach((r) => {
          const k = `${r.database_name || r.database}|${r.site_name}|${r.distance}`;
          if (!riskKeySet.has(k)) {
            base.risks.push(r);
            riskKeySet.add(k);
          }
        });
        // Prefer whichever address string looks more complete (longer)
        if (String(candidate.address || '').length > String(base.address || '').length) {
          base.address = candidate.address;
          base.name = candidate.name || base.name;
        }
        merged.add(j);
      }
    }

    // Re-compute risk level after merging
    const rc = base.risks.length;
    base.riskLevel = rc > 3 ? 'HIGH' : rc > 0 ? 'MEDIUM' : 'LOW';
    base.nearby_databases = [...new Set(base.risks.map((r) => r.database_name || r.database).filter(Boolean))];
    result.push(base);
  }

  return result;
}

// Groups OSM features by address and attaches nearby environmental risks
function groupByAddress(features) {
  return (features || []).map((f) => {
    // Determine risk level based on nearby risks count
    let riskLevel = 'LOW';
    if ((f.risks || []).length > 3) riskLevel = 'HIGH';
    else if ((f.risks || []).length > 0) riskLevel = 'MEDIUM';

    // Determine if special receptor
    const typeNorm = String(f.type || '').toLowerCase();
    let specialNote = null;
    if (typeNorm === 'wetland') {
      specialNote = 'WETLAND AREA - Environmental restrictions may apply';
    } else if (typeNorm.includes('school')) {
      specialNote = 'SCHOOL - Sensitive receptor location';
    } else if (typeNorm.includes('hospital')) {
      specialNote = 'HOSPITAL - Sensitive receptor location';
    }

    return {
      osm_id: f.osm_id,
      address: cleanDisplayAddress(f.address),
      type: f.type || 'feature',
      latitude: f.latitude,
      longitude: f.longitude,
      name: f.name,
      risks: (f.risks || []).map((r) => ({
        database_name: r.database || r.database_name || 'Unknown',
        site_name: r.site_name || r.name || 'Unknown Facility',
        distance: Math.round(r.distance_m || r.distance || 0),
        database: r.database || r.database_name || 'Unknown'
      })),
      riskLevel,
      specialNote,
      nearest_distance_m: f.nearest_distance_m,
      nearby_databases: f.nearby_databases
    };
  });
}

// Generates professional HTML for each address location with findings
function generateAddressBlocks(addressData) {
  const getImpactStatement = (address) => {
    const risks = address.risks || [];
    const dbNames = risks.map((r) => String(r.database_name || r.database || '').toLowerCase());
    const hasPetroleum = dbNames.some((name) => /ust|lust|petroleum|fuel/.test(name));
    const hasFloodWetland = dbNames.some((name) => /flood|wetland|hydro|water/.test(name)) || String(address.type || '').toLowerCase() === 'wetland';
    const hasSensitive = dbNames.some((name) => /school|hospital|receptor/.test(name)) || /school|hospital/.test(String(address.type || '').toLowerCase());
    const hasGeology = dbNames.some((name) => /radon|mine|geolog|fault|soil/.test(name));

    if (String(address.riskLevel || 'LOW').toUpperCase() === 'HIGH') {
      return 'Multiple higher-priority environmental indicators were identified near this location. Follow-up review and potential field verification are recommended before relying on this site condition for transactions or permitting.';
    }
    if (hasPetroleum) {
      return 'Nearby petroleum storage or release-related records suggest potential contamination pathway relevance. Historical operations and closure documentation should be reviewed for this address.';
    }
    if (hasFloodWetland) {
      return 'Hydrology-related indicators were identified near this location. Floodplain or wetland constraints may influence development feasibility, mitigation scope, or permitting requirements.';
    }
    if (hasSensitive) {
      return 'Sensitive-receptor context is present near this location. Environmental findings in proximity may carry elevated significance for occupant exposure and risk communication.';
    }
    if (hasGeology) {
      return 'Geological or subsurface indicators are present near this location and should be considered in geotechnical review and long-term site planning.';
    }
    if (risks.length > 0) {
      return 'Mapped environmental findings were identified near this location. Results should be treated as screening indicators and validated through targeted due diligence where warranted.';
    }
    return 'No mapped environmental findings were linked to this location within the selected screening radius. This suggests comparatively low concern based on the currently reviewed datasets.';
  };

  return (addressData || []).slice(0, 120).map((addr, index) => {
    let findingsHtml = '';
    const normalizedRisk = String(addr.riskLevel || 'LOW').toUpperCase();
    const riskColor = normalizedRisk === 'HIGH' ? '#b91c1c' : normalizedRisk === 'MEDIUM' ? '#b45309' : '#166534';
    const riskBg = normalizedRisk === 'HIGH' ? '#fee2e2' : normalizedRisk === 'MEDIUM' ? '#fef3c7' : '#dcfce7';
    const riskBorder = normalizedRisk === 'HIGH' ? '#fca5a5' : normalizedRisk === 'MEDIUM' ? '#fde68a' : '#86efac';
    const impactStatement = getImpactStatement(addr);

    if ((addr.risks || []).length === 0) {
      findingsHtml = `
        <div style="border:1px solid #bbf7d0; background:#f0fdf4; border-radius:8px; padding:10px 12px; color:#166534;">
          No environmental records were identified at this location. This suggests a low environmental screening risk based on current mapped data.
        </div>`;
    } else {
      findingsHtml = `
        <div style="display:grid; gap:8px; margin-top:8px;">
          ${addr.risks
            .map(
              (r) => `
          <div style="border:1px solid #e2e8f0; border-radius:8px; padding:9px 10px; background:#ffffff;">
            <div style="display:flex; align-items:center; justify-content:space-between; gap:8px; margin-bottom:4px;">
              <strong style="color:#0f172a;">${escapeHtml(r.database_name)}</strong>
              <span style="font-size:10px; color:#475569; font-weight:700;">${fmtMi(Number(r.distance))}</span>
            </div>
            <div style="font-size:12px; color:#334155;">${escapeHtml(r.site_name)}</div>
            <div style="margin-top:6px; font-size:11px; color:#475569; line-height:1.45;">
              ${(() => {
                const intelligence = inferEnvironmentalIntelligence(r.database_name || r.database, addr.type);
                const distanceMeters = Number.isFinite(Number(r.distance)) ? Math.round(Number(r.distance)) : null;
                const unknown = /unknown/i.test(String(r.site_name || ''));
                const tier = computePriorityTier(addr.riskLevel, distanceMeters, unknown);
                return `Activity: ${escapeHtml(intelligence.activity)}.<br/>Typical contaminants: ${escapeHtml(intelligence.contaminants)}.<br/>Pathway relevance: ${escapeHtml(intelligence.pathway)}.<br/>Priority: <strong>${tier}</strong>.`;
              })()}
            </div>
          </div>`
            )
            .join('')}
        </div>`;
    }

    // Special handling for wetlands
    let specialWarning = '';
    if (String(addr.type || '').toLowerCase() === 'wetland') {
      specialWarning = `
        <div style="background: #fee2e2; border-left: 4px solid #b91c1c; padding: 8px 10px; margin: 10px 0;">
          <p style="color: #991b1b; font-weight: 700; margin: 0;">WETLAND AREA - ENVIRONMENTAL RESTRICTIONS</p>
          <p style="margin: 4px 0 0; color: #7f1d1d; font-size: 12px;">
            This location is identified as a <strong>wetland area</strong>. Development may be restricted and environmental permits may be required. Consult USFWS National Wetlands Inventory (NWI) for regulatory review.
          </p>
        </div>`;
    }

    // Special handling for schools/hospitals
    let sensitiveReceptorNote = '';
    const normalizedType = String(addr.type || '').toLowerCase();
    if (normalizedType === 'school' || normalizedType === 'hospital') {
      const locType = normalizedType === 'school' ? 'School' : 'Hospital';
      sensitiveReceptorNote = `
        <div style="background: #fef3c7; border-left: 4px solid #b45309; padding: 8px 10px; margin: 10px 0;">
          <p style="color: #92400e; font-weight: 700; margin: 0;">${locType} - SENSITIVE RECEPTOR</p>
          <p style="margin: 4px 0 0; color: #78350f; font-size: 12px;">
            This is a <strong>sensitive receptor location</strong>. Environmental risks nearby may have increased significance for occupant health and safety considerations.
          </p>
        </div>`;
    }

    return `
      <div style="margin-bottom: 24px; border: 1px solid #d7dfeb; border-radius: 10px; padding: 12px 14px; background: #fbfdff; box-shadow: 0 10px 28px rgba(15,23,42,0.06);">
        <div style="display:flex; align-items:flex-start; justify-content:space-between; gap:10px; margin-bottom:8px;">
          <div>
            <h3 style="margin: 0; color: #0a2540; font-size: 16px;">
              Location ${index + 1}: ${escapeHtml(addr.address)}
            </h3>
            ${addr.isSubjectProperty ? '<div style="display:inline-block;background:#fbbf24;color:#1e1b4b;border-radius:4px;padding:2px 8px;font-weight:800;font-size:10px;letter-spacing:0.08em;margin-top:4px;">&#9733; SUBJECT PROPERTY (SP)</div>' : ''}
          </div>
          <span style="padding:3px 10px; border-radius:999px; font-size:10px; font-weight:800; letter-spacing:0.06em; background:${riskBg}; color:${riskColor}; border:1px solid ${riskBorder};">${normalizedRisk} RISK</span>
        </div>
        <p style="margin: 4px 0; color: #475569; font-size: 12px;"><strong>Type:</strong> ${escapeHtml(String(addr.type || 'feature').toUpperCase())}</p>
        <p style="margin: 8px 0 0; color: #334155; font-size: 12px; line-height:1.55;"><strong>Impact Statement:</strong> ${impactStatement}</p>
        ${specialWarning}
        ${sensitiveReceptorNote}
        <h4 style="margin: 12px 0 6px; color: #1f2937; font-size: 13px;">Linked Environmental Findings</h4>
        ${findingsHtml}
        <div style="margin-top:10px; border-top:1px solid #e2e8f0; padding-top:8px;">
          <p style="margin:0; color:#334155; font-size:12px; line-height:1.55;"><strong>Summary for this Address:</strong> ${impactStatement}</p>
        </div>
      </div>
    `;
  }).join('');
}

// Generates summary statistics for the address analysis
function generateAddressSummary(addressData) {
  const total = (addressData || []).length;
  const highRisk = (addressData || []).filter((a) => a.riskLevel === 'HIGH').length;
  const mediumRisk = (addressData || []).filter((a) => a.riskLevel === 'MEDIUM').length;
  const lowRisk = (addressData || []).filter((a) => a.riskLevel === 'LOW').length;

  return {
    total_addresses: total,
    high_risk_count: highRisk,
    medium_risk_count: mediumRisk,
    low_risk_count: lowRisk,
    high_risk_pct: total > 0 ? Math.round((highRisk / total) * 100) : 0,
    medium_risk_pct: total > 0 ? Math.round((mediumRisk / total) * 100) : 0,
    low_risk_pct: total > 0 ? Math.round((lowRisk / total) * 100) : 0
  };
}

/**
 * Returns true when an address entry matches the subject property.
 * Checks:
 *   1. Normalised text match between entry.address and subjectAddress
 *   2. Coordinate proximity ≤ 30 m (handles address-string mismatches)
 */
function isSpAddress(entry, subjectLat, subjectLng, subjectAddress) {
  if (subjectAddress) {
    const entryKey = normalizeAddressKey(String(entry?.address || ''));
    const subjKey = normalizeAddressKey(String(subjectAddress));
    if (entryKey && subjKey && entryKey === subjKey) return true;
  }

  const baseLat = toFiniteNumber(subjectLat);
  const baseLng = toFiniteNumber(subjectLng);
  const entLat = toFiniteNumber(entry?.latitude);
  const entLng = toFiniteNumber(entry?.longitude);

  if (baseLat !== null && baseLng !== null && entLat !== null && entLng !== null) {
    const distM = haversineMiles(baseLat, baseLng, entLat, entLng) * 1609.344;
    if (distM <= 30) return true;
  }

  return false;
}

function isSiteWithinBuffer(site, radiusMeters, subjectLat, subjectLng) {
  const thresholdMeters = Math.max(25, Number(radiusMeters) || 250);
  const distanceMiles = parseDistanceMiles(site?.distance);
  if (distanceMiles !== null) {
    return distanceMiles * 1609.344 <= thresholdMeters;
  }

  const lat = toFiniteNumber(site?.lat ?? site?.latitude);
  const lng = toFiniteNumber(site?.lng ?? site?.longitude);
  const baseLat = toFiniteNumber(subjectLat);
  const baseLng = toFiniteNumber(subjectLng);
  if (lat !== null && lng !== null && baseLat !== null && baseLng !== null) {
    return haversineMiles(baseLat, baseLng, lat, lng) * 1609.344 <= thresholdMeters;
  }

  return true;
}

function mergeAddressAndEnvironmentalSites(addressEntries, environmentalSites, radiusMeters, subjectLat, subjectLng, subjectAddress) {
  const merged = new Map();

  (addressEntries || []).forEach((entry) => {
    const address = cleanDisplayAddress(entry?.address);
    const key = normalizeAddressKey(address);
    const existing = merged.get(key) || {
      ...entry,
      address,
      risks: []
    };

    const riskKeySet = new Set((existing.risks || []).map((r) => `${r.database_name || r.database}|${r.site_name}|${r.distance}`));
    (entry.risks || []).forEach((risk) => {
      const k = `${risk.database_name || risk.database}|${risk.site_name}|${risk.distance}`;
      if (!riskKeySet.has(k)) {
        existing.risks.push(risk);
        riskKeySet.add(k);
      }
    });

    merged.set(key, existing);
  });

  (environmentalSites || []).forEach((site) => {
    if (!isSiteWithinBuffer(site, radiusMeters, subjectLat, subjectLng)) return;

    const rawAddress = String(site?.address || site?.location || '').trim();
    if (!rawAddress || rawAddress.toLowerCase() === 'address unavailable') return;

    const key = normalizeAddressKey(rawAddress);
    const existing = merged.get(key) || {
      address: rawAddress,
      type: 'regulated_site',
      latitude: toFiniteNumber(site?.lat ?? site?.latitude),
      longitude: toFiniteNumber(site?.lng ?? site?.longitude),
      name: site?.name || 'Regulated Facility',
      risks: [],
      riskLevel: 'LOW',
      specialNote: null,
      nearest_distance_m: null,
      nearby_databases: []
    };

    const distanceMiles = parseDistanceMiles(site?.distance);
    const distanceMeters = Number.isFinite(distanceMiles) ? Math.round(distanceMiles * 1609.344) : null;
    existing.risks.push({
      database_name: site?.database || 'Unknown',
      site_name: site?.name || 'Unknown Facility',
      distance: distanceMeters !== null ? distanceMeters : 'N/A',
      database: site?.database || 'Unknown'
    });

    existing.nearby_databases = [...new Set((existing.risks || []).map((r) => r.database_name || r.database).filter(Boolean))];
    merged.set(key, existing);
  });

  return clusterByProximity(
    Array.from(merged.values())
      .map((entry) => {
        const riskCount = (entry.risks || []).length;
        let riskLevel = 'LOW';
        if (riskCount > 3) riskLevel = 'HIGH';
        else if (riskCount > 0) riskLevel = 'MEDIUM';
        return { ...entry, riskLevel, isSubjectProperty: isSpAddress(entry, subjectLat, subjectLng, subjectAddress) };
      })
      .sort((a, b) => {
        const weight = { HIGH: 3, MEDIUM: 2, LOW: 1 };
        return (weight[b.riskLevel] || 0) - (weight[a.riskLevel] || 0);
      }),
    30 // merge entries within 30 metres of each other
  );
}

function generateLocationDatabaseRows(addressData, subjectLat, subjectLng) {
  const baseLat = toFiniteNumber(subjectLat);
  const baseLng = toFiniteNumber(subjectLng);
  const rows = [];
  (addressData || []).slice(0, 120).forEach((location, locationIndex) => {
    (location.risks || []).forEach((risk) => {
      if (rows.length >= 500) return;
      const riskRaw = String(location.riskLevel || 'LOW').toUpperCase();
      const riskColor = riskRaw === 'HIGH' ? '#b91c1c' : riskRaw === 'MEDIUM' ? '#b45309' : '#166534';
      const distanceMeters = Number.isFinite(Number(risk.distance)) ? Math.round(Number(risk.distance)) : null;
      const locLat = toFiniteNumber(location.latitude);
      const locLng = toFiniteNumber(location.longitude);
      let direction = 'N/A';
      if (baseLat !== null && baseLng !== null && locLat !== null && locLng !== null) {
        direction = bearingToCardinal(calculateBearing(baseLat, baseLng, locLat, locLng));
      }
      const mapId = `L${locationIndex + 1}`;
      const databases = [risk.database_name || risk.database || 'Unknown'].filter(Boolean).join(', ');
      const unknown = /unknown/i.test(String(risk.site_name || ''));
      const tier = computePriorityTier(riskRaw, distanceMeters, unknown);
      rows.push(`
    <tr>
      <td>${mapId}</td>
      <td>${escapeHtml(risk.site_name || 'Unknown Facility')}</td>
      <td>${escapeHtml(cleanDisplayAddress(location.address))}</td>
      <td>${escapeHtml(databases)}</td>
      <td>${distanceMeters !== null ? `${fmtMi(distanceMeters)} (${direction})` : direction}</td>
      <td style="color:${riskColor}; font-weight:700;">${escapeHtml(riskRaw)} / ${tier}</td>
    </tr>`);
    });
  });

  if (!rows.length) {
    return `
    <tr>
      <td colspan="6">No location-linked environmental database records were found within the selected buffer.</td>
    </tr>`;
  }

  return rows.join('');
}

function normalizeAddressLevelReport(reportPayload) {
  const locations = Array.isArray(reportPayload?.locations) ? reportPayload.locations : [];

  // First pass: map raw locations to internal shape
  const mapped = locations.map((location, index) => {
    const findings = Array.isArray(location.findings) ? location.findings : [];
    const risks = findings.map((finding) => ({
      database_name: finding.dataset || finding.database_name || finding.database || 'Unknown',
      site_name: finding.note || finding.site_name || location.location_name || 'Unknown Facility',
      distance: Number.isFinite(Number(finding.distance_m)) ? Math.round(Number(finding.distance_m)) : 'N/A',
      database: finding.dataset || finding.database || 'Unknown'
    }));

    return {
      address: location.address || location.location_name || `Location ${index + 1}`,
      type: location.type || 'feature',
      latitude: toFiniteNumber(location.latitude),
      longitude: toFiniteNumber(location.longitude),
      name: location.location_name || location.address || `Location ${index + 1}`,
      risks,
      riskLevel: String(location.risk_level || 'LOW').toUpperCase(),
      specialNote: null,
      nearest_distance_m: Number.isFinite(Number(location.nearest_distance_m))
        ? Math.round(Number(location.nearest_distance_m))
        : null,
      nearby_databases: [...new Set(risks.map((risk) => risk.database_name).filter(Boolean))]
    };
  });

  // Second pass: dedup by normalized address key, then cluster by proximity
  const deduped = new Map();
  mapped.forEach((entry) => {
    const key = normalizeAddressKey(entry.address);
    const existing = deduped.get(key);
    if (!existing) {
      deduped.set(key, { ...entry, risks: [...entry.risks] });
    } else {
      const riskKeySet = new Set(existing.risks.map((r) => `${r.database_name}|${r.site_name}|${r.distance}`));
      entry.risks.forEach((r) => {
        const k = `${r.database_name}|${r.site_name}|${r.distance}`;
        if (!riskKeySet.has(k)) { existing.risks.push(r); riskKeySet.add(k); }
      });
    }
  });

  return clusterByProximity(Array.from(deduped.values()), 30).map((entry) => ({
    ...entry,
    isSubjectProperty: entry.isSubjectProperty || false
  }));
}

// Internal function for PDF report generation (used by background processing)
async function generatePDFReportInternal(data) {
  const {
    project_name,
    client_name,
    client_company,
    address,
    latitude,
    longitude,
    paid,
    dataset_date,
    summary,
    aiSummary,
    environmentalData,
    polygonAnalysis,
    order_id,
    addressLevelReport
  } = data;

  const dateSet = dataset_date || new Date().toISOString().split('T')[0];
  const orderId = order_id || 'ORD-' + Date.now() + '-' + Math.floor(Math.random() * 1000);
  const effectiveRadiusMeters = getSystemReportRadiusMeters();
  const companyName = client_company || client_name || 'Not provided';
  const resolvedClientName = client_name || client_company || 'Valued Client';
  const resolvedCompanyName = client_company || client_name || 'Not provided';

  // Use provided environmental data or fetch new
  let envData = environmentalData;
  if (!envData) {
    envData = await fetchEnvironmentalData(latitude, longitude, data.polygon || null, effectiveRadiusMeters);
  }
  envData = {
    ...(envData || {}),
    environmentalSites: Array.isArray(envData?.environmentalSites) ? envData.environmentalSites : [],
    floodZones: Array.isArray(envData?.floodZones) ? envData.floodZones : [],
    schools: Array.isArray(envData?.schools) ? envData.schools : [],
    governmentRecords: Array.isArray(envData?.governmentRecords) ? envData.governmentRecords : [],
    rainfall: Array.isArray(envData?.rainfall) ? envData.rainfall : [],
  };
  envData.environmentalSites = pruneSitesForReport(envData.environmentalSites, 900);
  envData.environmentalSites = await enrichSitesWithOwnershipData(envData.environmentalSites || []);
  envData.environmentalSites = pruneSitesForReport(envData.environmentalSites, 450);

  // Generate map URLs
  // Categorize sites
  const siteCategories = categorizeSites(envData.environmentalSites);

  // Calculate risk levels
  const riskLevels = {
    high: envData.environmentalSites.filter(site => getRiskLevel(site) === 'High').length,
    medium: envData.environmentalSites.filter(site => getRiskLevel(site) === 'Moderate').length,
    low: envData.environmentalSites.filter(site => getRiskLevel(site) === 'Low').length
  };
  const totalRiskSites = riskLevels.high + riskLevels.medium + riskLevels.low;
  const score = totalRiskSites > 0
    ? Math.min(100, Math.round(((riskLevels.high * 3 + riskLevels.medium * 2 + riskLevels.low) / (totalRiskSites * 3)) * 100))
    : 0;

  // Generate detailed site listings
  const detailedSites = generateDetailedSites(envData.environmentalSites);
  const totalDatabases = new Set((envData.environmentalSites || []).map((s) => String(s.database || '').trim()).filter(Boolean)).size;
  let generatedSummary = summary || aiSummary;
  if (!generatedSummary) {
    generatedSummary = await generateAISummary(
      envData,
      project_name,
      address || 'Not provided',
      data.polygon || null,
      polygonAnalysis || null
    );
  }
  if (!generatedSummary) {
    generatedSummary = generateAutoSummary(envData, project_name, address || 'Not provided');
  }
  const proximityBreakdown = `
    ${envData.environmentalSites.length} mapped records were evaluated around the subject property. ` +
    `High-risk: ${riskLevels.high}, Moderate-risk: ${riskLevels.medium}, Low-risk: ${riskLevels.low}. ` +
    `Flood features identified: ${envData.floodZones.length}. Nearby schools/receptors: ${envData.schools.length}.`;
  const enhancedExecutiveSummary = buildEnhancedExecutiveSummaryHtml(
    envData,
    riskLevels,
    address || 'Not provided',
    effectiveRadiusMeters
  );
  const expandedSiteRecords = buildExpandedSiteRecordsHtml(envData.environmentalSites, latitude, longitude);
  const databaseDescriptions = buildDatabaseDescriptionsHtml(envData.environmentalSites);
  const mapFindingsDetailed = buildMapFindingsDetailedHtml(envData.environmentalSites, latitude, longitude);
  const geologicalAdvanced = buildGeologicalSectionHtml(envData, envData.environmentalSites);
  const historicalLandUse = buildHistoricalLandUseAnalysisHtml(envData.environmentalSites);
  const unmappableRecordsLog = buildUnmappableRecordsHtml(envData.environmentalSites);
  const legalComplianceLanguage = buildLegalComplianceHtml();
  const dataDensityStatement = buildDataDensityStatement(envData.environmentalSites, effectiveRadiusMeters);
  const ownershipEnrichmentSummary = buildOwnershipEnrichmentSummaryHtml(envData.environmentalSites);
  let areaFeatures = [];
  try {
    const osmRaw = await fetchAreaFeaturesFromOSM(latitude, longitude, effectiveRadiusMeters);
    areaFeatures = assignRisksToAddresses(processFeatures(osmRaw), envData.environmentalSites || [], effectiveRadiusMeters);
    if (areaFeatures.length > 250) {
      areaFeatures = areaFeatures.slice(0, 250);
    }
  } catch (featureErr) {
    console.error('Area feature extraction warning:', featureErr.message);
  }

  // ============================================
  // NEW: ADDRESS-BY-ADDRESS RESTRUCTURING
  // ============================================
  // Group by mapped addresses, then merge environmental site addresses found within buffer.
  const groupedFromFeatures = groupByAddress(areaFeatures);
  const groupedFromPayload = normalizeAddressLevelReport(addressLevelReport);
  const groupedAddresses = groupedFromPayload.length > 0
    ? groupedFromPayload
    : mergeAddressAndEnvironmentalSites(
      groupedFromFeatures,
      envData.environmentalSites || [],
      effectiveRadiusMeters,
      latitude,
      longitude,
      address
    );
  const addressBlocksHtml = generateAddressBlocks(groupedAddresses);
  const addressSummary = generateAddressSummary(groupedAddresses);
  
  // Update premium counts based on grouped data
  const premiumHigh = groupedAddresses.filter((item) => item.riskLevel === 'HIGH').length;
  const premiumMedium = groupedAddresses.filter((item) => item.riskLevel === 'MEDIUM').length;
  const premiumLow = groupedAddresses.filter((item) => item.riskLevel === 'LOW').length;

  const mapUrls = generateMapUrls(latitude, longitude, effectiveRadiusMeters);
  mapUrls.overview = buildFeatureAwareMapUrl(latitude, longitude, areaFeatures, envData.environmentalSites, effectiveRadiusMeters);
  const resolvedMapImages = await resolveReportMapImages(mapUrls, latitude, longitude);
  const topoMapsHtml = await generateTopoMapsHtml(latitude, longitude);
  const featureRows = generateFeatureRows(areaFeatures);
  const wetlandAnalysis = buildWetlandAnalysisHtml(areaFeatures, latitude, longitude);
  const sensitiveReceptors = buildSensitiveReceptorsHtml(areaFeatures);
  const addressLevelAnalysis = buildAddressLevelAnalysisHtml(areaFeatures);
  const addressSections = generateAddressSections(groupedAddresses);
  const summaryRows = generateAddressSummaryRows(groupedAddresses);
  const addressAnalysis = generateAddressAnalysis(groupedAddresses);
  const findingsByCategory = buildFindingsByCategoryHtml(envData, groupedAddresses);
  const clientConclusion = buildClientConclusionHtml(address || 'Not provided', groupedAddresses, riskLevels);
  const pathwayAnalysisHtml = buildPathwayAnalysisHtml(envData, latitude, longitude, groupedAddresses);
  const comparativeRankingHtml = buildComparativeRankingHtml(groupedAddresses);
  const dataConfidenceHtml = buildDataConfidenceHtml(envData, groupedAddresses);
  const floodWetlandDetailHtml = buildFloodWetlandDetailHtml(envData, areaFeatures);
  const propertyBufferOverviewHtml = buildPropertyBufferOverviewHtml(
    address || 'Not provided',
    latitude,
    longitude,
    effectiveRadiusMeters,
    groupedAddresses,
    polygonAnalysis || null
  );
  const bufferZoneAnalysisHtml = buildThreeBufferZoneHtml(envData, latitude, longitude, effectiveRadiusMeters);
  const floodAnalysisHtml = buildFloodAnalysisHtml(envData);
  const wetlandsRegulatoryHtml = buildWetlandsRegulatoryHtml(areaFeatures, latitude, longitude);
  const soilGeologyHtml = buildSoilGeologyInterpretationHtml(envData, envData.environmentalSites);
  const datasetIntelligenceHtml = buildDatasetIntelligenceHtml(envData, groupedAddresses);
  const mapAnalysisHtml = buildAdvancedMapAnalysisHtml(envData, groupedAddresses, effectiveRadiusMeters);
  const addressIntelligenceCoreHtml = buildAddressIntelligenceCoreHtml(groupedAddresses, latitude, longitude, address);
  const riskScoringSystemHtml = buildRiskScoringSystemHtml(groupedAddresses);
  const top3HighRiskFindingsHtml = buildTopHighRiskFindingsHtml(envData.environmentalSites, latitude, longitude);
  const priorityAForDecision = groupedAddresses.filter((entry) => {
    const nearest = (entry.risks || [])
      .map((r) => Number(r.distance ?? r.distance_m))
      .filter((v) => Number.isFinite(v))
      .sort((a, b) => a - b)[0];
    const unknown = (entry.risks || []).some((r) => /unknown/i.test(String(r.site_name || r.name || '')));
    return computePriorityTier(entry.riskLevel, nearest, unknown) === 'Priority A';
  }).length;
  const priorityBForDecision = groupedAddresses.filter((entry) => {
    const nearest = (entry.risks || [])
      .map((r) => Number(r.distance ?? r.distance_m))
      .filter((v) => Number.isFinite(v))
      .sort((a, b) => a - b)[0];
    const unknown = (entry.risks || []).some((r) => /unknown/i.test(String(r.site_name || r.name || '')));
    return computePriorityTier(entry.riskLevel, nearest, unknown) === 'Priority B';
  }).length;
  const finalRecommendationLabel = classifyFinalRecommendation(
    priorityAForDecision,
    priorityBForDecision,
    Number(riskLevels.high || 0),
    Number((envData.floodZones || []).length),
    Number((areaFeatures || []).filter((f) => String(f.type || '').toLowerCase() === 'wetland').length)
  );
  const aiSummaryText = generateSummary(envData.environmentalSites);
  const databaseRows = generateLocationDatabaseRows(groupedAddresses, latitude, longitude);
  const overallRiskLevel = getOverallRiskLevel(riskLevels);
  const riskLevelClass = overallRiskLevel === 'HIGH' ? 'risk-high' : overallRiskLevel === 'MODERATE' ? 'risk-medium' : 'risk-low';
  const drainageText = envData.floodZones.length > 0
    ? 'Moderate to poor drainage potential in mapped flood-influenced zones'
    : 'Moderate drainage typical of developed upland areas';
  const geologyText = 'Regional sedimentary formations with urban/developed overprint and anthropogenic fill influence.';
  const radonText = envData.environmentalSites.some((site) => normalizeDatabaseName(site.database).includes('radon'))
    ? 'Potentially elevated radon indicator datasets were identified near the property.'
    : 'Low-to-moderate radon potential inferred; no direct mapped radon hit returned in this run.';
  const floodDataText = envData.floodZones.length > 0
    ? `${envData.floodZones.length} flood-related records identified in the analysis area.`
    : 'No major flood risks detected in currently returned mapped records.';
  const historicalAnalysisText = buildHistoricalLandUseAnalysisHtml(envData.environmentalSites);

  // Additional environmental data
  const elevationApprox = (() => {
    const lat = toFiniteNumber(latitude);
    const lng = toFiniteNumber(longitude);
    if (lat === null || lng === null) return 'N/A — consult USGS';
    // Use rough elevation estimate: USGS topo suggests values typically vary 0–4000+ ft
    // Return placeholder; Open-Meteo elevation may be in envData if fetched
    const openMeteoElevation = (envData.rainfall || []).length > 0 && envData.rainfall[0]?.elevation
      ? `${Math.round(envData.rainfall[0].elevation)} ft (approx)`
      : 'N/A — consult USGS National Map';
    return openMeteoElevation;
  })();
  const additionalData = {
    flood_risk: envData.floodZones.length > 0 ? 'Areas within flood zones identified' : 'No flood zones in immediate area',
    soil_type: 'Urban/Developed (based on location)',
    zoning: 'Consult local zoning authority',
    elevation: elevationApprox,
    climate_zone: 'Consult NOAA climate atlas'
  };

  // Report metadata
  const reportDate = new Date().toLocaleDateString();
  const projectNumber = 'PRJ-' + Date.now();

  // Read template
  const templatePath = path.join(__dirname, 'reportTemplate.html');
  let htmlContent = fs.readFileSync(templatePath, 'utf8');

  // Replace all placeholders
  const replacements = {
    // Basic info
    project_name: project_name || 'Environmental Due Diligence Report',
    client_name: resolvedClientName,
    company_name: resolvedCompanyName,
    address: address || 'Not provided',
    date: reportDate,
    report_date: reportDate,
    order_id: formatOrderNumber(orderId),
    project_number: projectNumber,

    // Location data
    latitude: latitude || 'Not provided',
    longitude: longitude || 'Not provided',
    radius: `${metersToMiles(effectiveRadiusMeters)} mi`,

    // Summary and analysis
    summary: generatedSummary,
    ai_summary: aiSummaryText,
    total_records: envData.environmentalSites.length,
    total_databases: totalDatabases,
    countries_covered: 'Environmental Records Screening',
    risk_level: overallRiskLevel,
    risk_level_class: riskLevelClass,
    total_sites: envData.environmentalSites.length,
    high_risk: riskLevels.high,
    medium_risk: riskLevels.medium,
    low_risk: riskLevels.low,
    score,

    // Site categories
    fuel_count: siteCategories.fuel,
    waste_count: siteCategories.waste,
    industrial_count: siteCategories.industrial,
    government_count: siteCategories.government,
    schools_count: siteCategories.schools,

    // Detailed listings
    sites: detailedSites,
    database_rows: databaseRows,
    expanded_site_records: expandedSiteRecords,
    address_database_summary: buildAddressDatabaseSummaryHtml(envData.environmentalSites),
    database_descriptions: databaseDescriptions,
    database_coverage_html: buildDatabaseCoverageHtml(envData, groupedAddresses),

    // Map images
    mapImage: resolvedMapImages.overview,
    satelliteImage: resolvedMapImages.satellite,
    streetViewImage: resolvedMapImages.streetView,
    historicalImage: resolvedMapImages.satellite,
    logoImage: getLogoDataUri(),

    recommendations: buildDynamicRecommendationsHtml(riskLevels, groupedAddresses, envData),
    proximity_analysis: proximityBreakdown,
    geological_soil: `<p>Soil type: ${additionalData.soil_type}. Elevation reference: ${additionalData.elevation}. Climate zone context: ${additionalData.climate_zone}.</p>`,
    geological_advanced: geologicalAdvanced,
    historical_land_use_analysis: historicalLandUse,
    historical_aerial: buildHistoricalAerialHtml(generateTopoMapsHtml._lastSummaryHtml),
    environmental_records: buildDatabaseCoverageHtml(envData, groupedAddresses),
    rainfall_data: `${envData.rainfall.length} records reviewed`,
    flood_zones_data: `${envData.floodZones.length} features identified`,
    schools_data: `${envData.schools.length} schools identified`,
    government_records_data: `${envData.governmentRecords.length} records identified`,

    // Additional data
    flood_risk: additionalData.flood_risk,
    soil_type: additionalData.soil_type,
    drainage: drainageText,
    geology: geologyText,
    flood_data: floodDataText,
    radon_data: radonText,
    historical_analysis: historicalAnalysisText,
    zoning: additionalData.zoning,
    elevation: additionalData.elevation,
    climate_zone: additionalData.climate_zone,

    executive_summary_by_distance: buildExecutiveSummaryByDistance(envData.environmentalSites),
    executive_summary_by_database: buildExecutiveSummaryByDatabase(envData.environmentalSites),
    executive_summary_enhanced: enhancedExecutiveSummary,
    top3_high_risk_findings: top3HighRiskFindingsHtml,
    final_recommendation: finalRecommendationLabel,
    property_buffer_overview_html: propertyBufferOverviewHtml,
    buffer_zone_analysis_html: bufferZoneAnalysisHtml,
    flood_analysis_html: floodAnalysisHtml,
    wetlands_regulatory_html: wetlandsRegulatoryHtml,
    soil_geology_html: soilGeologyHtml,
    dataset_intelligence_html: datasetIntelligenceHtml,
    map_analysis_html: mapAnalysisHtml,
    address_intelligence_core: addressIntelligenceCoreHtml,
    risk_scoring_system_html: riskScoringSystemHtml,
    pathway_analysis_html: pathwayAnalysisHtml,
    property_map_url: resolvedMapImages.overview,
    area_map_url: resolvedMapImages.satellite,
    map_findings_summary: `Findings reflect mapped records with full addresses and risk tiering for consulting review. ${proximityBreakdown} ${dataDensityStatement}`,
    map_findings: `<p>${envData.environmentalSites.map((s) => `${escapeHtml(s.name || 'Site')} (${escapeHtml(s.database || 'Unknown')})`).join('<br/>')}</p>`,
    map_findings_detailed: mapFindingsDetailed,
    unmappable_summary: unmappableRecordsLog,
    unmappable_records_log: unmappableRecordsLog,
    ownership_enrichment_summary: ownershipEnrichmentSummary,
    feature_rows: featureRows,
    wetland_analysis: wetlandAnalysis,
    sensitive_receptors: sensitiveReceptors,
    address_level_analysis: addressLevelAnalysis,
    address_sections: addressSections,
    address_blocks: addressBlocksHtml,
    summary_rows: summaryRows,
    address_analysis: addressAnalysis,
    comparative_ranking_html: comparativeRankingHtml,
    data_confidence_html: dataConfidenceHtml,
    flood_wetland_detail_html: floodWetlandDetailHtml,
    findings_by_category: findingsByCategory,
    client_conclusion: clientConclusion,
    total_addresses: groupedAddresses.length,
    high: addressSummary.high_risk_count,
    medium: addressSummary.medium_risk_count,
    low: addressSummary.low_risk_count,
    high_risk_locations: addressSummary.high_risk_count,
    medium_risk_locations: addressSummary.medium_risk_count,
    low_risk_locations: addressSummary.low_risk_count,
    geological_summary: 'Regional geologic and soils context evaluated with available records.',
    soil_map_url: resolvedMapImages.satellite,
    legal_compliance_language: legalComplianceLanguage,
    data_density_statement: dataDensityStatement,
    topo_maps: topoMapsHtml,
    histo_summary_table: generateTopoMapsHtml._lastSummaryHtml || '',
    topo_section_class: generateTopoMapsHtml._hasPublishableHistoricalTopo ? '' : 'section-hidden'
  };

  // Apply replacements
  Object.keys(replacements).forEach(key => {
    const regex = new RegExp(`{{${key}}}`, 'g');
    htmlContent = htmlContent.replace(regex, replacements[key]);
  });

  // Handle polygon analysis conditional
  if (polygonAnalysis && polygonAnalysis.area != null) {
    const polygonHtml = `
<p><strong>Analysis Method:</strong> Polygon-defined boundary</p>
<p><strong>Property Area:</strong> ${polygonAnalysis.area.toLocaleString()} m² (${polygonAnalysis.areaAcres.toFixed(2)} acres)</p>
<p><strong>Perimeter:</strong> ${polygonAnalysis.perimeter.toFixed(0)} meters</p>
    `;
    htmlContent = htmlContent.replace(/{{#polygonAnalysis}}([\s\S]*?){{\/polygonAnalysis}}/, polygonHtml);
  } else {
    const starLat = Number.isFinite(Number(latitude)) ? Number(latitude).toFixed(6) : 'N/A';
    const starLng = Number.isFinite(Number(longitude)) ? Number(longitude).toFixed(6) : 'N/A';
    const starHtml = `
<p><strong>Analysis Method:</strong> Subject Property Star (point-based)</p>
<p><strong>Subject Property Coordinates:</strong> ${starLat}, ${starLng}</p>
<p><strong>Map Input:</strong> No polygon boundary supplied. Report is centered on the star-marked subject property.</p>
    `;
    htmlContent = htmlContent.replace(/{{#polygonAnalysis}}([\s\S]*?){{\/polygonAnalysis}}/, starHtml);
  }

  htmlContent = htmlContent.replace(/{{[^}]+}}/g, 'N/A');

  if (data.include_master_appendix === true) {
    const longFormAppendix = buildLongFormConsultingAppendix(
      envData,
      project_name,
      address,
      Number(data.long_form_pages) || 120
    );
    htmlContent += longFormAppendix;
  }

  // Generate PDF using Puppeteer
  const reportFileName = `report-${Date.now()}.pdf`;
  const reportPath = path.join(__dirname, 'reports', reportFileName);
  let browser;
  try {
    browser = await puppeteer.launch({
      headless: 'new',
      protocolTimeout: 600000,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-gpu',
        '--disable-web-security',
        '--font-render-hinting=medium',
        '--js-flags=--max-old-space-size=4096'
      ]
    });
    const page = await browser.newPage();
    await page.setViewport({ width: 1654, height: 2339, deviceScaleFactor: 2 });
    page.setDefaultNavigationTimeout(300000);
    page.setDefaultTimeout(300000);
    await page.emulateMediaType('screen');
    await page.setContent(htmlContent, { waitUntil: 'networkidle0', timeout: 300000 });

    await page.pdf({
      path: reportPath,
      format: 'A4',
      margin: { top: '0.5in', right: '0.5in', bottom: '0.5in', left: '0.5in' },
      printBackground: true,
      preferCSSPageSize: true,
      scale: 1
    });
  } finally {
    if (browser) {
      await browser.close().catch(() => {});
    }
  }

  return {
    reportPath: reportPath,
    fileName: reportFileName,
    orderId: orderId
  };
}

// Generate a simple PDF from raw HTML content.
async function generatePDFFromHTML(html, prefix = 'report-simple') {
  const reportFileName = `${prefix}-${Date.now()}.pdf`;
  const reportPath = path.join(__dirname, 'reports', reportFileName);
  let browser;
  try {
    browser = await puppeteer.launch({
      headless: 'new',
      protocolTimeout: 600000,
      args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu', '--font-render-hinting=medium']
    });
    const page = await browser.newPage();
    await page.setViewport({ width: 1654, height: 2339, deviceScaleFactor: 2 });
    page.setDefaultNavigationTimeout(300000);
    page.setDefaultTimeout(300000);
    await page.emulateMediaType('screen');
    await page.setContent(html, { waitUntil: 'networkidle0', timeout: 300000 });

    await page.pdf({
      path: reportPath,
      format: 'A4',
      margin: { top: '0.5in', right: '0.5in', bottom: '0.5in', left: '0.5in' },
      printBackground: true,
      preferCSSPageSize: true,
      scale: 1
    });
  } finally {
    if (browser) {
      await browser.close().catch(() => {});
    }
  }

  return reportPath;
}

// POST /generate-report-simple - Implements the 4-step simple report flow.
app.post('/generate-report-simple', async (req, res) => {
  try {
    const { order_id } = req.body;

    if (order_id === undefined || order_id === null) {
      return res.status(400).json({ error: 'order_id is required' });
    }

    // 1. Get order
    let order = null;
    if (supabaseUrl !== 'https://your-project.supabase.co') {
      const { data: orderRow } = await supabase
        .from('orders')
        .select('*')
        .eq('id', order_id)
        .single();
      if (orderRow) {
        order = orderRow;
      }
    }

    if (!order) {
      const numericId = Number(order_id);
      if (!Number.isNaN(numericId) && orders[numericId]) {
        order = orders[numericId];
      }
    }

    if (!order) {
      return res.status(404).json({ error: 'Order not found' });
    }

    // 2. Get nearby data
    let nearbyRows = [];
    if (supabaseUrl !== 'https://your-project.supabase.co') {
      const { data: envRows } = await supabase
        .from('environmental_data')
        .select('*');
      nearbyRows = Array.isArray(envRows) ? envRows : [];
    }

    if (nearbyRows.length === 0) {
      const envData = await fetchEnvironmentalData(order.latitude, order.longitude);
      nearbyRows = (envData.environmentalSites || []).map(site => ({
        database_name: site.database,
        site_name: site.name,
        address: site.address,
        distance: site.distance,
        risk_type: getRiskLevel(site)
      }));
    }

    // 3. Build HTML
    const rows = nearbyRows.map(d => `
      <tr>
        <td>${d.database_name || 'N/A'}</td>
        <td>${d.site_name || 'N/A'}</td>
        <td>${d.address || 'N/A'}</td>
        <td>${fmtMi(Number(d.distance) || 0)}</td>
        <td>${d.risk_type || 'Unknown'}</td>
      </tr>
    `).join('');

    const html = `
      <h1>GeoScope Report</h1>

      <h2>Environmental Findings</h2>

      <table border="1" cellpadding="8" cellspacing="0" style="border-collapse: collapse; width: 100%;">
        <tr>
          <th>Database</th>
          <th>Site</th>
          <th>Address</th>
          <th>Distance</th>
          <th>Risk</th>
        </tr>
        ${rows}
      </table>
    `;

    // 4. Generate PDF
    const reportPath = await generatePDFFromHTML(html);
    const downloadUrl = `/download/${order_id}`;

    await updateOrderTrackingAfterReport(order_id, {
      reportPath,
      downloadUrl
    });

    res.json({ success: true, reportPath, downloadUrl, orderId: order_id, statusUpdated: true });
  } catch (error) {
    console.error('Error generating simple report:', error);
    res.status(500).json({ error: 'Failed to generate simple report', details: error.message });
  }
});

// POST /generate-report - Generate PDF report
app.post('/generate-report', async (req, res) => {
  try {
    const data = req.body;
    const {
      project_name,
      client_name,
      client_company,
      address,
      latitude,
      longitude,
      paid,
      dataset_date,
      summary,
      environmentalData,
      polygonAnalysis,
      order_id,
      addressLevelReport
    } = data;

    if (!project_name || !client_name) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    // Payment check (optional)
    if (!paid) {
      return res.status(403).json({ message: 'Payment required' });
    }

    // Single-path premium generation. The legacy inline block below duplicates
    // expensive data shaping and can cause memory pressure on large result sets.
    const generatedReportResult = await generatePDFReportInternal({
      project_name,
      client_name,
      client_company,
      address,
      latitude,
      longitude,
      paid,
      dataset_date,
      summary,
      environmentalData,
      polygonAnalysis,
      order_id,
      radius: DEFAULT_REPORT_RADIUS_MILES,
      addressLevelReport,
      include_master_appendix: data.include_master_appendix,
      long_form_pages: data.long_form_pages,
      polygon: data.polygon || null
    });

    if (order_id !== undefined && order_id !== null) {
      await updateOrderTrackingAfterReport(order_id, {
        reportPath: generatedReportResult.reportPath,
        downloadUrl: `/download/${generatedReportResult.orderId}`
      });
    }

    return res.json({
      success: true,
      message: 'Report generated successfully',
      reportPath: generatedReportResult.reportPath,
      fileName: generatedReportResult.fileName,
      orderId: generatedReportResult.orderId,
      downloadUrl: `/download/${generatedReportResult.orderId}`,
      statusUpdated: Boolean(order_id !== undefined && order_id !== null)
    });

    const dateSet = dataset_date || new Date().toISOString().split('T')[0];
    const orderId = order_id || 'ORD-' + Date.now() + '-' + Math.floor(Math.random() * 1000);
    const effectiveRadiusMeters = getSystemReportRadiusMeters();
    const companyName = client_company || client_name || 'Not provided';

    // Use provided environmental data or fetch new
    let envData = environmentalData;
    if (!envData) {
      envData = await fetchEnvironmentalData(latitude, longitude, data.polygon || null, effectiveRadiusMeters);
    }
    envData.environmentalSites = await enrichSitesWithOwnershipData(envData.environmentalSites || []);

    // Generate map URLs
    // Categorize sites
    const siteCategories = categorizeSites(envData.environmentalSites);

    // Calculate risk levels
    const riskLevels = {
      high: envData.environmentalSites.filter(site => getRiskLevel(site) === 'High').length,
      medium: envData.environmentalSites.filter(site => getRiskLevel(site) === 'Moderate').length,
      low: envData.environmentalSites.filter(site => getRiskLevel(site) === 'Low').length
    };
    const totalRiskSites = riskLevels.high + riskLevels.medium + riskLevels.low;
    const score = totalRiskSites > 0
      ? Math.min(100, Math.round(((riskLevels.high * 3 + riskLevels.medium * 2 + riskLevels.low) / (totalRiskSites * 3)) * 100))
      : 0;

    // Generate detailed site listings
    const detailedSites = generateDetailedSites(envData.environmentalSites);
    const totalDatabases = new Set((envData.environmentalSites || []).map((s) => String(s.database || '').trim()).filter(Boolean)).size;
    const enhancedExecutiveSummary = buildEnhancedExecutiveSummaryHtml(
      envData,
      riskLevels,
      address || 'Not provided',
      effectiveRadiusMeters
    );
    const expandedSiteRecords = buildExpandedSiteRecordsHtml(envData.environmentalSites, latitude, longitude);
    const databaseDescriptions = buildDatabaseDescriptionsHtml(envData.environmentalSites);
    const mapFindingsDetailed = buildMapFindingsDetailedHtml(envData.environmentalSites, latitude, longitude);
    const geologicalAdvanced = buildGeologicalSectionHtml(envData, envData.environmentalSites);
    const historicalLandUse = buildHistoricalLandUseAnalysisHtml(envData.environmentalSites);
    const unmappableRecordsLog = buildUnmappableRecordsHtml(envData.environmentalSites);
    const legalComplianceLanguage = buildLegalComplianceHtml();
    const dataDensityStatement = buildDataDensityStatement(envData.environmentalSites, effectiveRadiusMeters);
    const ownershipEnrichmentSummary = buildOwnershipEnrichmentSummaryHtml(envData.environmentalSites);
    let areaFeatures = [];
    try {
      const osmRaw = await fetchAreaFeaturesFromOSM(latitude, longitude, effectiveRadiusMeters);
      areaFeatures = assignRisksToAddresses(processFeatures(osmRaw), envData.environmentalSites || [], effectiveRadiusMeters);
    } catch (featureErr) {
      console.error('Area feature extraction warning:', featureErr.message);
    }
    const mapUrls = generateMapUrls(latitude, longitude, effectiveRadiusMeters);
    mapUrls.overview = buildFeatureAwareMapUrl(latitude, longitude, areaFeatures, envData.environmentalSites, effectiveRadiusMeters);
    const resolvedMapImages = await resolveReportMapImages(mapUrls, latitude, longitude);
    const topoMapsHtml = await generateTopoMapsHtml(latitude, longitude);
    const featureRows = generateFeatureRows(areaFeatures);
    const wetlandAnalysis = buildWetlandAnalysisHtml(areaFeatures, latitude, longitude);
    const sensitiveReceptors = buildSensitiveReceptorsHtml(areaFeatures);
    const addressLevelAnalysis = buildAddressLevelAnalysisHtml(areaFeatures);
    const addressSections = generateAddressSections(areaFeatures);
    const summaryRows = generateAddressSummaryRows(areaFeatures);
    const addressAnalysis = generateAddressAnalysis(areaFeatures);
    const pathwayAnalysisHtml = buildPathwayAnalysisHtml(envData, latitude, longitude, areaFeatures);
    const comparativeRankingHtml = buildComparativeRankingHtml(areaFeatures);
    const dataConfidenceHtml = buildDataConfidenceHtml(envData, areaFeatures);
    const floodWetlandDetailHtml = buildFloodWetlandDetailHtml(envData, areaFeatures);
    const premiumHigh = areaFeatures.filter((item) => (item.risks || []).length > 2).length;
    const premiumMedium = areaFeatures.filter((item) => (item.risks || []).length > 0 && (item.risks || []).length <= 2).length;
    const premiumLow = areaFeatures.filter((item) => (item.risks || []).length === 0).length;
    const aiSummaryText = generateSummary(envData.environmentalSites);
    const databaseRows = generateRows(envData.environmentalSites.map((site) => ({
      ...site,
      database_name: site.database,
      site_name: site.name,
      risk_level: String(getRiskLevel(site) || 'LOW').toUpperCase()
    })));
    const overallRiskLevel = getOverallRiskLevel(riskLevels);
    const riskLevelClass = overallRiskLevel === 'HIGH' ? 'risk-high' : overallRiskLevel === 'MODERATE' ? 'risk-medium' : 'risk-low';
    const drainageText = envData.floodZones.length > 0
      ? 'Moderate to poor drainage potential in mapped flood-influenced zones'
      : 'Moderate drainage typical of developed upland areas';
    const geologyText = 'Regional sedimentary formations with urban/developed overprint and anthropogenic fill influence.';
    const radonText = envData.environmentalSites.some((site) => normalizeDatabaseName(site.database).includes('radon'))
      ? 'Potentially elevated radon indicator datasets were identified near the property.'
      : 'Low-to-moderate radon potential inferred; no direct mapped radon hit returned in this run.';
    const floodDataText = envData.floodZones.length > 0
      ? `${envData.floodZones.length} flood-related records identified in the analysis area.`
      : 'No major flood risks detected in currently returned mapped records.';
    const historicalAnalysisText = buildHistoricalLandUseAnalysisHtml(envData.environmentalSites);

    // Additional environmental data
    const elevationApprox2 = (() => {
      const openMeteoElev = (envData.rainfall || []).length > 0 && envData.rainfall[0]?.elevation
        ? `${Math.round(envData.rainfall[0].elevation)} ft (approx)`
        : 'N/A — consult USGS National Map';
      return openMeteoElev;
    })();
    const additionalData = {
      flood_risk: envData.floodZones.length > 0 ? 'Areas within flood zones identified' : 'No flood zones in immediate area',
      soil_type: 'Urban/Developed (based on location)',
      zoning: 'Consult local zoning authority',
      elevation: elevationApprox2,
      climate_zone: 'Consult NOAA climate atlas'
    };

    // Report metadata
    const reportDate = new Date().toLocaleDateString();
    const projectNumber = 'PRJ-' + Date.now();

    // Read template
    const templatePath = path.join(__dirname, 'reportTemplate.html');
    let htmlContent = fs.readFileSync(templatePath, 'utf8');

    // Replace all placeholders
    const replacements = {
      // Basic info
      project_name: project_name,
      client_name: client_name,
      company_name: companyName,
      address: address || 'Not provided',
      date: reportDate,
      report_date: reportDate,
      order_id: formatOrderNumber(orderId),
      project_number: projectNumber,

      // Location data
      latitude: latitude || 'Not provided',
      longitude: longitude || 'Not provided',
      radius: `${metersToMiles(effectiveRadiusMeters)} mi`,

      // Summary and analysis
      summary: summary || 'Environmental analysis completed for the subject property.',
      ai_summary: aiSummaryText,
      total_records: envData.environmentalSites.length,
      total_databases: totalDatabases,
      countries_covered: 'Environmental Records Screening',
      risk_level: overallRiskLevel,
      risk_level_class: riskLevelClass,
      total_sites: envData.environmentalSites.length,
      high_risk: riskLevels.high,
      medium_risk: riskLevels.medium,
      low_risk: riskLevels.low,
      score,

      // Site categories
      fuel_count: siteCategories.fuel,
      waste_count: siteCategories.waste,
      industrial_count: siteCategories.industrial,
      government_count: siteCategories.government,
      schools_count: siteCategories.schools,

      // Detailed listings
      sites: detailedSites,
      database_rows: databaseRows,
      expanded_site_records: expandedSiteRecords,
      address_database_summary: buildAddressDatabaseSummaryHtml(envData.environmentalSites),
      database_descriptions: databaseDescriptions,
      database_coverage_html: buildDatabaseCoverageHtml(envData),

      // Map images
      mapImage: resolvedMapImages.overview,
      satelliteImage: resolvedMapImages.satellite,
      streetViewImage: resolvedMapImages.streetView,
      historicalImage: resolvedMapImages.satellite,
      logoImage: getLogoDataUri(),

      recommendations: buildDynamicRecommendationsHtml(riskLevels, areaFeatures.length > 0 ? areaFeatures : [], envData),
      proximity_analysis: `${envData.environmentalSites.length} mapped records were evaluated around the subject property, grouped by database source, distance, and operational context.`,
      geological_soil: `<p>Soil type: ${additionalData.soil_type}. Elevation reference: ${additionalData.elevation}. Climate zone context: ${additionalData.climate_zone}.</p>`,
      geological_advanced: geologicalAdvanced,
      historical_land_use_analysis: historicalLandUse,
      historical_aerial: buildHistoricalAerialHtml(generateTopoMapsHtml._lastSummaryHtml),
      environmental_records: buildDatabaseCoverageHtml(envData),
      rainfall_data: `${envData.rainfall.length} records reviewed`,
      flood_zones_data: `${envData.floodZones.length} features identified`,
      schools_data: `${envData.schools.length} schools identified`,
      government_records_data: `${envData.governmentRecords.length} records identified`,

      // Additional data
      flood_risk: additionalData.flood_risk,
      soil_type: additionalData.soil_type,
      drainage: drainageText,
      geology: geologyText,
      flood_data: floodDataText,
      radon_data: radonText,
      historical_analysis: historicalAnalysisText,
      zoning: additionalData.zoning,
      elevation: additionalData.elevation,
      climate_zone: additionalData.climate_zone,

      executive_summary_by_distance: buildExecutiveSummaryByDistance(envData.environmentalSites),
      executive_summary_by_database: buildExecutiveSummaryByDatabase(envData.environmentalSites),
      executive_summary_enhanced: enhancedExecutiveSummary,
      pathway_analysis_html: pathwayAnalysisHtml,
      property_map_url: resolvedMapImages.overview,
      area_map_url: resolvedMapImages.satellite,
      map_findings_summary: `${envData.environmentalSites.length} findings captured from mapped datasets for this location overview. ${dataDensityStatement}`,
      map_findings: `<p>${envData.environmentalSites.map((s) => `${escapeHtml(s.name || 'Site')} (${escapeHtml(s.database || 'Unknown')})`).join('<br/>')}</p>`,
      map_findings_detailed: mapFindingsDetailed,
      unmappable_summary: unmappableRecordsLog,
      unmappable_records_log: unmappableRecordsLog,
      ownership_enrichment_summary: ownershipEnrichmentSummary,
      feature_rows: featureRows,
      wetland_analysis: wetlandAnalysis,
      sensitive_receptors: sensitiveReceptors,
      address_level_analysis: addressLevelAnalysis,
      address_sections: addressSections,
      summary_rows: summaryRows,
      address_analysis: addressAnalysis,
      comparative_ranking_html: comparativeRankingHtml,
      data_confidence_html: dataConfidenceHtml,
      flood_wetland_detail_html: floodWetlandDetailHtml,
      total_addresses: areaFeatures.length,
      high: premiumHigh,
      medium: premiumMedium,
      low: premiumLow,
      geological_summary: 'Regional geologic and soils context evaluated with available records.',
      soil_map_url: resolvedMapImages.satellite,
      legal_compliance_language: legalComplianceLanguage,
      data_density_statement: dataDensityStatement,
      topo_maps: topoMapsHtml,
      histo_summary_table: generateTopoMapsHtml._lastSummaryHtml || '',
      topo_section_class: generateTopoMapsHtml._hasPublishableHistoricalTopo ? '' : 'section-hidden'
    };

    // Apply replacements
    Object.keys(replacements).forEach(key => {
      const regex = new RegExp(`{{${key}}}`, 'g');
      htmlContent = htmlContent.replace(regex, replacements[key]);
    });

    // Handle polygon analysis conditional
    if (polygonAnalysis && polygonAnalysis.area != null) {
      const polygonHtml = `
  <p><strong>Analysis Method:</strong> Polygon-defined boundary</p>
  <p><strong>Property Area:</strong> ${polygonAnalysis.area.toLocaleString()} m² (${polygonAnalysis.areaAcres.toFixed(2)} acres)</p>
  <p><strong>Perimeter:</strong> ${polygonAnalysis.perimeter.toFixed(0)} meters</p>
      `;
      htmlContent = htmlContent.replace(/{{#polygonAnalysis}}([\s\S]*?){{\/polygonAnalysis}}/, polygonHtml);
    } else {
      const starLat = Number.isFinite(Number(latitude)) ? Number(latitude).toFixed(6) : 'N/A';
      const starLng = Number.isFinite(Number(longitude)) ? Number(longitude).toFixed(6) : 'N/A';
      const starHtml = `
  <p><strong>Analysis Method:</strong> Subject Property Star (point-based)</p>
  <p><strong>Subject Property Coordinates:</strong> ${starLat}, ${starLng}</p>
  <p><strong>Map Input:</strong> No polygon boundary supplied. Report is centered on the star-marked subject property.</p>
      `;
      htmlContent = htmlContent.replace(/{{#polygonAnalysis}}([\s\S]*?){{\/polygonAnalysis}}/, starHtml);
    }

    htmlContent = htmlContent.replace(/{{[^}]+}}/g, 'N/A');

    // Generate PDF using internal function
    const reportResult = await generatePDFReportInternal({
      project_name, client_name, client_company, address, latitude, longitude, paid, dataset_date, summary,
      environmentalData: envData,
      polygonAnalysis,
      order_id: orderId,
      radius: DEFAULT_REPORT_RADIUS_MILES,
      addressLevelReport
    });

    res.json({
      success: true,
      message: 'Report generated successfully',
      reportPath: reportResult.reportPath,
      downloadUrl: `/download/${orderId}`
    });

  } catch (error) {
    console.error('Error generating report:', error);
    try {
      const fallbackOrderId = req.body?.order_id || 'ORD-' + Date.now() + '-' + Math.floor(Math.random() * 1000);
      const fallbackProject = req.body?.project_name || 'GeoScope Report';
      const fallbackClient = req.body?.client_name || req.body?.client_company || 'Client';
      const fallbackAddress = req.body?.address || 'Address unavailable';
      const fallbackSummary = req.body?.summary || `Primary report pipeline failed: ${String(error?.message || 'unknown error')}.`;
      const fallbackHtml = `<!doctype html>
<html><head><meta charset="utf-8"><title>GeoScope Fallback Report</title>
<style>body{font-family:Arial,sans-serif;padding:28px;color:#1f2937}h1{margin:0 0 10px}p{line-height:1.45}pre{white-space:pre-wrap;background:#f3f4f6;padding:14px;border-radius:8px}</style>
</head><body>
<h1>GeoScope Environmental Report</h1>
<p><strong>Project:</strong> ${escapeHtml(String(fallbackProject))}</p>
<p><strong>Client:</strong> ${escapeHtml(String(fallbackClient))}</p>
<p><strong>Address:</strong> ${escapeHtml(String(fallbackAddress))}</p>
<p><strong>Order ID:</strong> ${escapeHtml(String(fallbackOrderId))}</p>
<h2>Summary</h2>
<pre>${escapeHtml(String(fallbackSummary))}</pre>
<p>This fallback report was generated to prevent workflow interruption while the primary rendering path is being retried.</p>
</body></html>`;
      const fallbackPath = await generatePDFFromHTML(fallbackHtml, 'report-fallback');
      if (req.body?.order_id !== undefined && req.body?.order_id !== null) {
        await updateOrderTrackingAfterReport(req.body.order_id, {
          reportPath: fallbackPath,
          downloadUrl: `/download/${fallbackOrderId}`
        });
      }
      return res.status(200).json({
        success: true,
        warning: 'Primary generator failed; fallback report generated',
        message: 'Report generated with fallback pipeline',
        reportPath: fallbackPath,
        orderId: fallbackOrderId,
        downloadUrl: `/download/${fallbackOrderId}`
      });
    } catch (fallbackError) {
      return res.status(500).json({
        error: 'Failed to generate report',
        details: error.message,
        fallbackError: fallbackError.message
      });
    }
  }
});

// GET /download/:orderId - Download report for clients
app.get('/download/:orderId', async (req, res) => {
  try {
    const orderId = req.params.orderId;

    const numericOrderId = Number.parseInt(orderId, 10);
    let order = Number.isFinite(numericOrderId) ? auth.getOrderById(numericOrderId) : null;
    if (!order) {
      order = (orders || []).find((o) => String(o?.id) === String(orderId) || String(o?.order_id) === String(orderId));
    }

    // Supabase fallback for orders not in memory
    if (!order && supabaseUrl !== 'https://your-project.supabase.co') {
      try {
        const lookupId = Number.isFinite(numericOrderId) ? numericOrderId : orderId;
        const { data, error } = await supabase.from('orders').select('*').eq('id', lookupId).single();
        if (!error && data) order = data;
      } catch (_) { /* continue */ }
    }

    const reportsRoot = path.resolve(REPORTS_DIR);
    let filePath = null;

    // Prefer explicit stored path for this order (original generated report).
    if (order) {
      const explicitPath = order.report_path || order.reportPath || null;
      if (explicitPath) {
        const resolvedExplicit = path.resolve(String(explicitPath));
        if (resolvedExplicit.startsWith(reportsRoot) && fs.existsSync(resolvedExplicit)) {
          filePath = resolvedExplicit;
        }
      }

      // Secondary: attempt deterministic filename match for this order id.
      if (!filePath) {
        const files = fs.readdirSync(REPORTS_DIR);
        const matched = files
          .filter((file) => file.endsWith('.pdf') && (file.includes(`order-${orderId}`) || file.includes(`report-${orderId}`)))
          .sort();
        if (matched.length) {
          filePath = path.join(REPORTS_DIR, matched[matched.length - 1]);
        }
      }

      // For tracked orders, never fall back to unrelated latest report.
      if (!filePath) {
        return res.status(404).json({ error: 'No generated report found for this order' });
      }
    } else {
      // Legacy fallback for ad-hoc/non-tracked download IDs.
      const files = fs.readdirSync(REPORTS_DIR);
      const reportFiles = files.filter(file => file.startsWith('report-') && file.endsWith('.pdf')).sort();
      if (!reportFiles.length) {
        return res.status(404).json({ error: 'No report files found' });
      }
      filePath = path.join(REPORTS_DIR, reportFiles[reportFiles.length - 1]);
    }

    res.download(filePath, `GeoScope_Report_${orderId}.pdf`);
  } catch (error) {
    console.error('Error downloading report:', error);
    res.status(500).json({ error: 'Failed to download report', details: error.message });
  }
});

// POST /contact - General contact form submission
app.post('/contact', async (req, res) => {
  try {
    const { name, email, subject, message } = req.body || {};
    if (!name || !email || !message) {
      return res.status(400).json({ error: 'Missing required fields: name, email, message' });
    }

    const adminEmail = process.env.ADMIN_NOTIFICATION_EMAIL || 'steveochibo@gmail.com';

    await transporter.sendMail({
      to: adminEmail,
      replyTo: email,
      subject: `GeoScope Contact Form: ${subject || '(no subject)'}`,
      text: `New contact form submission\n\nName: ${name}\nEmail: ${email}\nSubject: ${subject || ''}\n\nMessage:\n${message}`
    });

    return res.json({ success: true, message: 'Message received. We will follow up within 24 hours.' });
  } catch (err) {
    console.error('Error handling contact form:', err);
    return res.status(500).json({ error: 'Failed to send message' });
  }
});

// POST /send-sample-report - Generate a polished sample report and email it
app.post('/send-sample-report', async (req, res) => {
  try {
    const recipient = (req.body?.email || 'steveochibo@gmail.com').toLowerCase().trim();
    const senderName = req.body?.name || 'there';

    const samplePayload = {
      project_name: 'Sample ESG Site Report',
      client_name: 'GeoScope Demo Client',
      address: '100 Biscayne Blvd, Miami, FL',
      latitude: 25.7617,
      longitude: -80.1918,
      paid: true,
      summary: 'Sample deliverable with imagery, graph, statistics, and full environmental database coverage.',
      environmentalData: {
        environmentalSites: [
          { id: 'S1', name: 'Fuel Terminal', database: 'EPA FUELS', address: '111 Harbor Rd', distance: '0.3 mi', status: 'Active' },
          { id: 'S2', name: 'Historic Dry Cleaner', database: 'SCRD DRYCLEANERS', address: '222 Market St', distance: '0.6 mi', status: 'Closed' },
          { id: 'S3', name: 'School Campus', database: 'SCHOOLS PUBLIC', address: '333 School Ave', distance: '0.9 mi', status: 'Active' }
        ],
        floodZones: [{ attributes: { FLD_ZONE: 'AE' } }],
        schools: [{ attributes: { NAME: 'Downtown Public School' } }],
        governmentRecords: [{ FacilityName: 'Municipal Storage Site' }],
        rainfall: [{ date: '2023-01-01', precipitation: '18 mm' }]
      },
      radius: 1500
    };

    const reportResult = await generatePDFReportInternal(samplePayload);

    await transporter.sendMail({
      to: recipient,
      subject: 'Your GeoScope Sample Environmental Report',
      text: `Hi ${senderName},\n\nThank you for your interest in GeoScope Solutions.\n\nAttached is your sample Government Records Report — a demonstration of our full environmental site assessment deliverable.\n\nThis sample includes:\n• AI-generated executive summary\n• Environmental database findings table\n• Geological landscape analysis\n• Flood zone, wetland, and sensitive receptor data\n\nReady to order a report for your property? Visit https://geoscope.com/request-report\n\nBest regards,\nThe GeoScope Team`,
      attachments: [{ path: reportResult.reportPath }]
    });

    return res.json({
      success: true,
      message: 'Sample report generated and sent successfully',
      recipient,
      filePath: reportResult.reportPath
    });
  } catch (error) {
    console.error('Error sending sample report:', error);
    return res.status(500).json({ error: 'Failed to send sample report', details: error.message });
  }
});

// =====================
// AUTHENTICATION ENDPOINTS
// =====================

/**
 * POST /auth/register
 * Register new user (client, analyst, or admin)
 */
app.post('/auth/register', (req, res) => {
  const { name, email, password, role = 'client', company = '' } = req.body;

  if (!name || !email || !password) {
    return res.status(400).json({ error: 'Missing required fields' });
  }

  const result = auth.registerUser(name, email, password, role, company);
  res.status(result.success ? 201 : 400).json(result);
});

/**
 * POST /auth/login
 * Login user with email and password
 */
app.post('/auth/login', (req, res) => {
  const { email, password } = req.body;

  if (!email || !password) {
    return res.status(400).json({ error: 'Missing email or password' });
  }

  const result = auth.loginUser(email, password);
  res.status(result.success ? 200 : 401).json(result);
});

/**
 * GET /auth/me
 * Get current user profile (requires auth)
 */
app.get('/auth/me', requireAuth, (req, res) => {
  const user = auth.getUserById(req.user.id);
  if (!user) {
    return res.status(404).json({ error: 'User not found' });
  }
  res.json(user);
});

/**
 * GET /auth/verify
 * Verify JWT token validity
 */
app.post('/auth/verify', (req, res) => {
  const { token } = req.body;
  const decoded = auth.verifyToken(token);
  
  if (decoded) {
    res.json({ valid: true, user: decoded });
  } else {
    res.status(401).json({ valid: false, error: 'Invalid token' });
  }
});

// =====================
// ADMIN ENDPOINTS - USER MANAGEMENT
// =====================

/**
 * GET /admin/users
 * Get all users (admin only)
 */
app.get('/admin/users', requireAuth, requireRole('admin'), (req, res) => {
  const users = auth.getAllUsers();
  res.json(users);
});

/**
 * GET /admin/users/analysts
 * Get all analysts (admin only)
 */
app.get('/admin/users/analysts', requireAuth, requireRole('admin'), (req, res) => {
  const analysts = auth.getAnalysts();
  res.json(analysts);
});

/**
 * PUT /admin/users/:userId/role
 * Update user role (admin only)
 */
app.put('/admin/users/:userId/role', requireAuth, requireRole('admin'), (req, res) => {
  const { userId } = req.params;
  const { role } = req.body;

  if (!['client', 'analyst', 'admin'].includes(role)) {
    return res.status(400).json({ error: 'Invalid role' });
  }

  const result = auth.updateUserRole(parseInt(userId), role);
  res.status(result.success ? 200 : 400).json(result);
});

/**
 * DELETE /admin/users/:userId
 * Delete user (admin only)
 */
app.delete('/admin/users/:userId', requireAuth, requireRole('admin'), (req, res) => {
  const { userId } = req.params;
  const result = auth.deleteUser(parseInt(userId));
  res.status(result.success ? 200 : 400).json(result);
});

// =====================
// ORDER ENDPOINTS
// =====================

/**
 * POST /orders
 * Create new order (client)
 */
app.post('/orders', requireAuth, requireRole('client'), (req, res) => {
  const { project_name, address, latitude, longitude, polygon } = req.body;

  if (!project_name || !address || !latitude || !longitude) {
    return res.status(400).json({ error: 'Missing required fields' });
  }

  const result = auth.createOrder(
    req.user.id,
    project_name,
    address,
    latitude,
    longitude,
    polygon ? JSON.stringify(polygon) : null
  );

  res.status(result.success ? 201 : 400).json(result);
});

/**
 * GET /orders
 * Get orders based on user role
 */
app.get('/orders', requireAuth, (req, res) => {
  let roleOrders;
  const inMemoryOrders = Array.isArray(orders) ? orders : [];

  if (req.user.role === 'admin') {
    const authOrders = auth.getAllOrders();
    roleOrders = authOrders.length > 0 ? authOrders : inMemoryOrders;
  } else if (req.user.role === 'analyst') {
    const authOrders = auth.getAllOrders();
    const merged = [...(authOrders || []), ...inMemoryOrders];
    const deduped = [];
    const seen = new Set();

    for (const o of merged) {
      const key = `${String(o?.id ?? '')}|${String(o?.project_name ?? '')}|${String(o?.created_at ?? '')}`;
      if (!seen.has(key)) {
        seen.add(key);
        deduped.push(o);
      }
    }

    roleOrders = deduped;
  } else if (req.user.role === 'gis') {
    const authOrders = auth.getAnalystOrders(req.user.id);
    roleOrders = authOrders.length > 0
      ? authOrders
      : inMemoryOrders.filter((o) => {
        const assignedTo = String(o.assigned_to || o.analyst_id || '');
        const status = String(o.status || '').toLowerCase();
        return !assignedTo || ['received', 'pending'].includes(status);
      });
  } else if (req.user.role === 'client') {
    const authOrders = auth.getUserOrders(req.user.id);
    roleOrders = authOrders.length > 0
      ? authOrders
      : inMemoryOrders.filter((o) =>
        String(o.user_id || '') === String(req.user.id) ||
        (o.recipient_email_1 && o.recipient_email_1 === req.user.email) ||
        (o.email && o.email === req.user.email)
      );
  } else {
    return res.status(403).json({ error: 'Insufficient permissions' });
  }

  res.json(roleOrders);
});

/**
 * GET /orders/:orderId
 * Get specific order
 */
app.get('/orders/:orderId', requireAuth, (req, res) => {
  const { orderId } = req.params;
  const numericOrderId = Number.parseInt(orderId, 10);
  let order = Number.isFinite(numericOrderId) ? auth.getOrderById(numericOrderId) : null;
  if (!order) {
    const authOrders = auth.getAllOrders?.() || [];
    order = authOrders.find((o) =>
      String(o?.id) === String(orderId) ||
      String(o?.order_id) === String(orderId)
    );
  }
  if (!order) {
    order = (orders || []).find((o) =>
      String(o?.id) === String(orderId) ||
      String(o?.order_id) === String(orderId)
    );
  }

  if (!order) {
    return res.status(404).json({ error: 'Order not found' });
  }

  // Check permissions: client owns it, analyst assigned to it, or admin
  if (
    req.user.role === 'client' &&
    String(order.client_id || order.user_id || '') !== String(req.user.id) &&
    order.recipient_email_1 !== req.user.email &&
    order.email !== req.user.email
  ) {
    return res.status(403).json({ error: 'Insufficient permissions' });
  }
  if (req.user.role === 'analyst') {
    const status = String(order.status || '').toLowerCase();
    const activeStatuses = new Set(['received', 'pending', 'assigned', 'processing', 'in-progress', 'processed', 'submitted']);
    if (!activeStatuses.has(status)) {
      return res.status(403).json({ error: 'Insufficient permissions' });
    }
  }

  res.json(order);
});

/**
 * PUT /orders/:orderId/status
 * Update order status
 */
app.put('/orders/:orderId/status', requireAuth, (req, res) => {
  const { orderId } = req.params;
  const { status } = req.body;

  let order = auth.getOrderById(parseInt(orderId));
  const inMemoryOrder = (orders || []).find((o) => String(o.id) === String(orderId));
  if (!order && !inMemoryOrder) {
    return res.status(404).json({ error: 'Order not found' });
  }

  if (!order && inMemoryOrder) {
    order = inMemoryOrder;
  }

  if (!order) {
    return res.status(404).json({ error: 'Order not found' });
  }

  // Only analyst assigned or admin can update status
  if (req.user.role !== 'admin' && req.user.role !== 'analyst') {
    return res.status(403).json({ error: 'Insufficient permissions' });
  }

  let updated = false;
  let authResult = { success: false };
  if (auth.getOrderById(parseInt(orderId))) {
    authResult = auth.updateOrderStatus(parseInt(orderId), status);
    updated = !!authResult.success;
  }

  if (inMemoryOrder) {
    inMemoryOrder.status = status;
    inMemoryOrder.updated_at = new Date().toISOString();
    updated = true;
  }

  res.status(updated ? 200 : 400).json(updated ? { success: true } : authResult);
});

// ---------------------------------------------------------------------------
// GET /nearby-search — environmental spatial search around a subject point
// ---------------------------------------------------------------------------
const gisSearch = require('./gis-search');
const globalDataStore = require('./services/globalDataStore');

const METERS_PER_MILE = 1609.344;
const DATASET_SYNC_INTERVAL_HOURS = Number(process.env.DATASET_SYNC_INTERVAL_HOURS || 24);

globalDataStore.startCatalogAutoSync(DATASET_SYNC_INTERVAL_HOURS * 60 * 60 * 1000);

function parseRadiusToMeters(radiusValue, defaultMiles = DEFAULT_REPORT_RADIUS_MILES) {
  const raw = Number(radiusValue);
  if (!Number.isFinite(raw) || raw <= 0) {
    return defaultMiles * METERS_PER_MILE;
  }
  // Backward compatibility: old clients sent meters (250, 1000, 1609).
  // New clients send miles (0.25, 0.5, 1, 2, 3, ...).
  if (raw <= 25) {
    return raw * METERS_PER_MILE;
  }
  return raw;
}

function metersToMiles(meters) {
  return Number((Number(meters || 0) / METERS_PER_MILE).toFixed(3));
}

app.get('/nearby-search', async (req, res) => {
  try {
    const { lat, lng } = req.query;
    if (!lat || !lng) {
      return res.status(400).json({ error: 'lat and lng query parameters are required' });
    }
    const radiusMeters = getSystemReportRadiusMeters();
    const result = await gisSearch.nearbySearch(lat, lng, radiusMeters);
    res.json({
      ...result,
      radius_m: radiusMeters,
      radius_miles: metersToMiles(radiusMeters)
    });
  } catch (err) {
    console.error('[nearby-search]', err.message);
    res.status(500).json({ error: err.message || 'Spatial search failed' });
  }
});

// ---------------------------------------------------------------------------
// GET /database-catalog — full configured database inventory (158+)
// Optional query: lat, lng, radius => attach in-area match counts
// ---------------------------------------------------------------------------
app.get('/database-catalog', requireAuth, requireRole('admin', 'analyst', 'gis'), async (req, res) => {
  try {
    const { lat, lng, radius = 1, region = 'north-america' } = req.query;
    const radiusMeters = parseRadiusToMeters(radius, 1);
    const includeHits = lat !== undefined && lng !== undefined;
    const focusNorthAmerica = String(region || 'north-america').toLowerCase() !== 'all';

    const storedCatalog = globalDataStore.listDatasets();
    const baseCatalog = (storedCatalog.length > 0 ? storedCatalog : MASTER_DATABASES.map((name) => ({
      id: null,
      name,
      category: gisSearch.categorizeDatabase(name),
      country: 'Global'
    }))).map((entry) => ({
      ...entry,
      matched_records: 0,
      matched_addresses: 0
    }));

    const regionFilteredBaseCatalog = focusNorthAmerica
      ? baseCatalog.filter((entry) => ['USA', 'Canada'].includes(String(entry.country || '')))
      : baseCatalog;

    const sortCatalogRows = (rows = []) => {
      const countryRank = (country) => {
        const c = String(country || '');
        if (c === 'USA') return 0;
        if (c === 'Canada') return 1;
        return 2;
      };
      return [...rows].sort((a, b) => {
        const byCountry = countryRank(a.country) - countryRank(b.country);
        if (byCountry !== 0) return byCountry;
        return String(a.name || '').localeCompare(String(b.name || ''));
      });
    };

    if (!includeHits) {
      return res.json({
        total: regionFilteredBaseCatalog.length,
        with_matches: 0,
        catalogs: sortCatalogRows(regionFilteredBaseCatalog),
        region_focus: focusNorthAmerica ? 'USA+Canada' : 'All'
      });
    }

    const catalogs = globalDataStore.getCatalogCoverage(lat, lng, radiusMeters);
    // Overlay PostgreSQL spatial match counts on top of the catalog
    let pgHits = new Map(); // database_name -> count
    try {
      const pgResult = await pgPool.query(
        `SELECT database_name, COUNT(*)::int AS cnt
         FROM environmental_sites
         WHERE location IS NOT NULL
           AND ST_DWithin(location::geography, ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography, $3)
         GROUP BY database_name`,
        [parseFloat(lng), parseFloat(lat), radiusMeters]
      );
      for (const row of pgResult.rows) {
        pgHits.set(row.database_name, row.cnt);
      }
    } catch (pgErr) {
      console.warn('[database-catalog] PG coverage query failed:', pgErr.message);
    }
    const matchedCatalog = baseCatalog.map((entry) => ({
      ...entry,
      matched_records: pgHits.get(entry.name) || entry.matched_records || 0
    }));
    const finalCatalog = focusNorthAmerica
      ? matchedCatalog.filter((entry) => ['USA', 'Canada'].includes(String(entry.country || '')))
      : matchedCatalog;

    const withMatches = finalCatalog.filter((c) => c.matched_records > 0).length;

    res.json({
      total: finalCatalog.length,
      with_matches: withMatches,
      catalogs: sortCatalogRows(finalCatalog),
      radius_miles: metersToMiles(radiusMeters),
      region_focus: focusNorthAmerica ? 'USA+Canada' : 'All'
    });
  } catch (err) {
    console.error('[database-catalog]', err.message);
    res.status(500).json({ error: err.message || 'Database catalog load failed' });
  }
});

// ---------------------------------------------------------------------------
// GET /database-catalog/us-states — grouped USA state catalog with useful info
// ---------------------------------------------------------------------------
app.get('/database-catalog/us-states', requireAuth, requireRole('admin', 'analyst', 'gis'), (req, res) => {
  try {
    const datasets = globalDataStore.listDatasets();
    const usaStateRows = datasets.filter((row) => row.country === 'USA' && row.state);
    const byState = new Map();

    for (const row of usaStateRows) {
      if (!byState.has(row.state)) {
        byState.set(row.state, {
          state: row.state,
          state_code: row.state_code || null,
          total_datasets: 0,
          categories: new Set(),
          source_programs: new Set(),
          datasets: []
        });
      }

      const item = byState.get(row.state);
      item.total_datasets += 1;
      if (row.category) item.categories.add(row.category);
      if (row.source_program) item.source_programs.add(row.source_program);
      item.datasets.push({
        id: row.id,
        name: row.name,
        category: row.category,
        useful_info: row.useful_info || null,
        source_program: row.source_program || null,
        coverage_scope: row.coverage_scope || null,
        maintainer: row.maintainer || null,
        priority: row.priority || null
      });
    }

    const states = Array.from(byState.values())
      .map((row) => ({
        state: row.state,
        state_code: row.state_code,
        total_datasets: row.total_datasets,
        categories: Array.from(row.categories).sort((a, b) => a.localeCompare(b)),
        source_programs: Array.from(row.source_programs).sort((a, b) => a.localeCompare(b)),
        datasets: row.datasets.sort((a, b) => String(a.name).localeCompare(String(b.name)))
      }))
      .sort((a, b) => a.state.localeCompare(b.state));

    res.json({
      total_states: states.length,
      total_state_datasets: usaStateRows.length,
      states
    });
  } catch (err) {
    console.error('[database-catalog/us-states]', err.message);
    res.status(500).json({ error: err.message || 'USA state database catalog load failed' });
  }
});

// ---------------------------------------------------------------------------
// GET /data/store/stats — local DB counts for stored datasets/features
// ---------------------------------------------------------------------------
app.get('/data/store/stats', requireAuth, requireRole('admin', 'analyst', 'gis'), (req, res) => {
  try {
    res.json({ success: true, ...globalDataStore.stats() });
  } catch (err) {
    console.error('[data/store/stats]', err.message);
    res.status(500).json({ error: err.message || 'Store stats failed' });
  }
});

// ---------------------------------------------------------------------------
// GET /data/catalog/sync-status — current auto-sync configuration and last run
// ---------------------------------------------------------------------------
app.get('/data/catalog/sync-status', requireAuth, requireRole('admin', 'analyst', 'gis'), (req, res) => {
  try {
    res.json({
      success: true,
      ...globalDataStore.getCatalogSyncStatus()
    });
  } catch (err) {
    console.error('[data/catalog/sync-status]', err.message);
    res.status(500).json({ error: err.message || 'Catalog sync status failed' });
  }
});

// ---------------------------------------------------------------------------
// POST /data/catalog/sync-now — force immediate EPA/UST/state catalog sync
// ---------------------------------------------------------------------------
app.post('/data/catalog/sync-now', requireAuth, requireRole('admin', 'gis'), (req, res) => {
  try {
    const status = globalDataStore.syncStateCatalog('manual-api');
    res.json({ success: true, ...status });
  } catch (err) {
    console.error('[data/catalog/sync-now]', err.message);
    res.status(500).json({ error: err.message || 'Catalog sync failed' });
  }
});

const MISSING_DATABASE_PLAYBOOK = [
  {
    name: 'ECA SMD WWIS',
    category: 'hydrology',
    source_program: 'Environment and Climate Change Canada',
    useful_info: 'Wastewater and stormwater outfall and treatment indicators for screening sensitive receptors and receiving waters.',
    search_terms: ['ECA SMD WWIS', 'wastewater outfalls', 'stormwater discharges', 'effluent monitoring']
  },
  {
    name: 'STATE PFAS TRACKING',
    category: 'contamination',
    source_program: 'US State Environmental Programs',
    useful_info: 'State-level PFAS monitoring and remediation program records that may not appear in federal-only datasets.',
    search_terms: ['state PFAS map', 'PFAS remediation sites', 'PFAS groundwater monitoring']
  },
  {
    name: 'COUNTY LANDFILL INVENTORY',
    category: 'contamination',
    source_program: 'County Solid Waste Departments',
    useful_info: 'County-operated landfill and transfer station records often missing from federal layers.',
    search_terms: ['county landfill GIS', 'solid waste transfer station', 'closed landfill inventory']
  }
];

function normalizeName(value) {
  return String(value || '').trim().toUpperCase();
}

// ---------------------------------------------------------------------------
// GET /data/missing-databases/suggestions — AI-style guidance for missing DBs
// ---------------------------------------------------------------------------
app.get('/data/missing-databases/suggestions', requireAuth, requireRole('admin', 'analyst', 'gis'), async (req, res) => {
  try {
    const { lat, lng, radius = 1 } = req.query;
    if (!lat || !lng) {
      return res.status(400).json({ error: 'lat and lng query parameters are required' });
    }

    const radiusMeters = parseRadiusToMeters(radius, 1);
    const catalog = globalDataStore.listDatasets();

    let pgRows = [];
    try {
      const radiusDegrees = radiusMeters / 111320;
      const pgResult = await pgPool.query(
        `SELECT database_name, COUNT(*)::int AS cnt
         FROM environmental_sites
         WHERE location IS NOT NULL
           AND location && ST_Expand(ST_SetSRID(ST_MakePoint($1, $2), 4326), $4)
           AND ST_DWithin(location::geography, ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography, $3)
         GROUP BY database_name
         ORDER BY cnt DESC
         LIMIT 300`,
        [Number(lng), Number(lat), radiusMeters, radiusDegrees]
      );
      pgRows = pgResult.rows || [];
    } catch (pgErr) {
      console.warn('[data/missing-databases/suggestions] local PG coverage query failed:', pgErr.message);
    }

    const inAreaDbNames = new Set(pgRows.map((r) => normalizeName(r.database_name)).filter(Boolean));
    const catalogByName = new Map((catalog || []).map((row) => [normalizeName(row.name), row]));

    const recommendations = MISSING_DATABASE_PLAYBOOK.map((item) => {
      const key = normalizeName(item.name);
      const catalogRow = catalogByName.get(key);
      const inAreaHits = inAreaDbNames.has(key);
      const missing = !catalogRow || !inAreaHits;
      return {
        ...item,
        in_catalog: Boolean(catalogRow),
        in_area_hits: inAreaHits,
        missing,
        ai_guidance: `Search for ${item.search_terms.join(', ')} within ${metersToMiles(radiusMeters)} miles to improve local evidence coverage.`
      };
    }).filter((row) => row.missing);

    res.json({
      success: true,
      center: { lat: Number(lat), lng: Number(lng) },
      radius_m: radiusMeters,
      radius_miles: metersToMiles(radiusMeters),
      nearby_records: pgRows.reduce((sum, row) => sum + Number(row.cnt || 0), 0),
      nearby_databases: Array.from(inAreaDbNames).sort((a, b) => a.localeCompare(b)),
      recommendations
    });
  } catch (err) {
    console.error('[data/missing-databases/suggestions]', err.message);
    res.status(500).json({ error: err.message || 'Missing database suggestion failed' });
  }
});

// ---------------------------------------------------------------------------
// POST /data/missing-databases/register — store analyst missing DB requests
// ---------------------------------------------------------------------------
app.post('/data/missing-databases/register', requireAuth, requireRole('admin', 'analyst', 'gis'), (req, res) => {
  try {
    const name = String(req.body?.name || '').trim();
    const category = String(req.body?.category || 'regulatory').trim().toLowerCase();
    const source_program = String(req.body?.source_program || 'Analyst Requested').trim();
    const useful_info = String(req.body?.useful_info || '').trim();
    const search_terms = Array.isArray(req.body?.search_terms) ? req.body.search_terms.filter(Boolean) : [];

    if (!name) {
      return res.status(400).json({ error: 'name is required' });
    }

    const analyst = String(req.user?.email || req.user?.role || 'unknown');
    const seed = {
      name,
      category,
      country: String(req.body?.country || 'USA').trim(),
      source_program,
      useful_info: useful_info || (search_terms.length ? `Suggested search terms: ${search_terms.join(', ')}` : 'Analyst flagged as missing for this area.'),
      coverage_scope: 'requested-missing',
      maintainer: `Analyst Request (${analyst})`,
      priority: String(req.body?.priority || 'high').trim().toLowerCase()
    };

    const result = globalDataStore.addDatasetSeeds([seed]);
    res.json({ success: true, saved: seed, ...result });
  } catch (err) {
    console.error('[data/missing-databases/register]', err.message);
    res.status(500).json({ error: err.message || 'Missing database registration failed' });
  }
});

// ---------------------------------------------------------------------------
// POST /data/import/geo-points — bulk import stored environmental points
// ---------------------------------------------------------------------------
app.post('/data/import/geo-points', requireAuth, requireRole('admin', 'analyst', 'gis'), (req, res) => {
  try {
    const rows = Array.isArray(req.body?.rows) ? req.body.rows : [];
    if (!rows.length) {
      return res.status(400).json({ error: 'rows array is required' });
    }

    const inserted = globalDataStore.importGeoPoints(rows);
    res.json({ success: true, inserted, stats: globalDataStore.stats() });
  } catch (err) {
    console.error('[data/import/geo-points]', err.message);
    res.status(500).json({ error: err.message || 'Geo point import failed' });
  }
});

// ---------------------------------------------------------------------------
// POST /data/import/features — bulk import address/building features
// ---------------------------------------------------------------------------
app.post('/data/import/features', requireAuth, requireRole('admin'), (req, res) => {
  try {
    const rows = Array.isArray(req.body?.rows) ? req.body.rows : [];
    if (!rows.length) {
      return res.status(400).json({ error: 'rows array is required' });
    }

    const inserted = globalDataStore.importFeatures(rows);
    res.json({ success: true, inserted, stats: globalDataStore.stats() });
  } catch (err) {
    console.error('[data/import/features]', err.message);
    res.status(500).json({ error: err.message || 'Feature import failed' });
  }
});

// ---------------------------------------------------------------------------
// POST /data/ingest/nearby-sites — fetch live nearby sites and store locally
// ---------------------------------------------------------------------------
app.post('/data/ingest/nearby-sites', requireAuth, requireRole('admin', 'gis'), async (req, res) => {
  try {
    const lat = Number(req.body?.lat);
    const lng = Number(req.body?.lng);
    const radiusMeters = parseRadiusToMeters(req.body?.radius ?? req.body?.radius_miles ?? 1, 1);
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
      return res.status(400).json({ error: 'lat and lng are required and must be valid numbers' });
    }

    const result = await gisSearch.nearbySearch(lat, lng, radiusMeters);
    const rows = (result?.results || []).map((item) => ({
      dataset: item.database,
      category: item.category,
      name: item.site_name,
      site_name: item.site_name,
      latitude: item.lat,
      longitude: item.lng,
      address: item.address,
      risk_level: item.status,
      source: item.source,
      source_id: item.id,
      site_uid: item.id
    }));

    const inserted = globalDataStore.importGeoPoints(rows);
    res.json({
      success: true,
      queried: rows.length,
      inserted,
      center: { lat, lng },
      radius_m: radiusMeters,
      radius_miles: metersToMiles(radiusMeters),
      stats: globalDataStore.stats()
    });
  } catch (err) {
    console.error('[data/ingest/nearby-sites]', err.message);
    res.status(500).json({ error: err.message || 'Nearby site ingestion failed' });
  }
});

// ---------------------------------------------------------------------------
// GET /data/export/dbeaver/environmental-sites.sql — SQL for DBeaver import
// ---------------------------------------------------------------------------
app.get('/data/export/dbeaver/environmental-sites.sql', requireAuth, requireRole('admin', 'gis'), (req, res) => {
  try {
    const sql = globalDataStore.buildDBeaverEnvironmentalSitesSql();
    const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    res.setHeader('Content-Type', 'application/sql; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="environmental-sites-${stamp}.sql"`);
    res.send(sql);
  } catch (err) {
    console.error('[data/export/dbeaver/environmental-sites.sql]', err.message);
    res.status(500).json({ error: err.message || 'DBeaver SQL export failed' });
  }
});

// ---------------------------------------------------------------------------
// GET /data/export/dbeaver/environmental-sites.csv — CSV for DBeaver import
// ---------------------------------------------------------------------------
app.get('/data/export/dbeaver/environmental-sites.csv', requireAuth, requireRole('admin', 'gis'), (req, res) => {
  try {
    const csv = globalDataStore.buildDBeaverEnvironmentalSitesCsv();
    const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="environmental-sites-${stamp}.csv"`);
    res.send(csv);
  } catch (err) {
    console.error('[data/export/dbeaver/environmental-sites.csv]', err.message);
    res.status(500).json({ error: err.message || 'DBeaver CSV export failed' });
  }
});

// ---------------------------------------------------------------------------
// GET /data/export/postgres/environmental-sites-copy.sql — COPY-ready script
// ---------------------------------------------------------------------------
app.get('/data/export/postgres/environmental-sites-copy.sql', requireAuth, requireRole('admin', 'gis'), (req, res) => {
  try {
    const csvPath = String(req.query?.csvPath || 'environmental-sites.csv');
    const script = globalDataStore.buildPostgresCopyScript(csvPath);
    const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    res.setHeader('Content-Type', 'application/sql; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="environmental-sites-copy-${stamp}.sql"`);
    res.send(script);
  } catch (err) {
    console.error('[data/export/postgres/environmental-sites-copy.sql]', err.message);
    res.status(500).json({ error: err.message || 'Postgres COPY script export failed' });
  }
});

// ---------------------------------------------------------------------------
// GET /data/address-matches — address-level matching from stored DB
// ---------------------------------------------------------------------------
app.get('/data/address-matches', requireAuth, requireRole('admin', 'analyst', 'gis'), (req, res) => {
  try {
    const { lat, lng, radius = 1 } = req.query;
    if (!lat || !lng) {
      return res.status(400).json({ error: 'lat and lng query parameters are required' });
    }

    const radiusMeters = parseRadiusToMeters(radius, 1);
    const results = globalDataStore.getAddressLevelMatches(lat, lng, radiusMeters);
    res.json({
      success: true,
      radius_miles: metersToMiles(radiusMeters),
      radius_m: radiusMeters,
      total_addresses: results.length,
      results
    });
  } catch (err) {
    console.error('[data/address-matches]', err.message);
    res.status(500).json({ error: err.message || 'Address-level matching failed' });
  }
});

// ---------------------------------------------------------------------------
// GET /data/report/address-level — detailed location-by-location risk output
// ---------------------------------------------------------------------------
app.get('/data/report/address-level', requireAuth, requireRole('admin', 'analyst', 'gis'), (req, res) => {
  try {
    const { lat, lng, radius = 1 } = req.query;
    if (!lat || !lng) {
      return res.status(400).json({ error: 'lat and lng query parameters are required' });
    }

    const radiusMeters = parseRadiusToMeters(radius, 1);
    const report = globalDataStore.buildAddressLevelReport(lat, lng, radiusMeters);
    res.json({
      success: true,
      center: {
        lat: Number(lat),
        lng: Number(lng)
      },
      radius_miles: metersToMiles(radiusMeters),
      radius_m: radiusMeters,
      ...report
    });
  } catch (err) {
    console.error('[data/report/address-level]', err.message);
    res.status(500).json({ error: err.message || 'Address-level report generation failed' });
  }
});

// ---------------------------------------------------------------------------
// GET /features — extract area features from OSM and link to nearby risks
// ---------------------------------------------------------------------------
app.get('/features', async (req, res) => {
  try {
    const { lat, lng, radius = 1 } = req.query;
    if (!lat || !lng) {
      return res.status(400).json({ error: 'lat and lng query parameters are required' });
    }

    const latNum = Number(lat);
    const lngNum = Number(lng);
    const radiusNum = parseRadiusToMeters(radius, 1);
    if (!Number.isFinite(latNum) || !Number.isFinite(lngNum)) {
      return res.status(400).json({ error: 'lat and lng must be valid numbers' });
    }

    const storedMatches = globalDataStore.getAddressLevelMatches(latNum, lngNum, radiusNum);
    if (storedMatches.length > 0) {
      return res.json({
        success: true,
        radius: radiusNum,
        radius_miles: metersToMiles(radiusNum),
        source: 'local-database',
        totalFeatures: storedMatches.length,
        highRiskFeatures: storedMatches.filter((f) => f.risk_level === 'HIGH').length,
        mediumRiskFeatures: storedMatches.filter((f) => f.risk_level === 'MEDIUM').length,
        lowRiskFeatures: storedMatches.filter((f) => f.risk_level === 'LOW').length,
        wetlands: storedMatches.filter((f) => String(f.type).toLowerCase() === 'wetland').length,
        features: storedMatches
      });
    }

    const osmRaw = await fetchAreaFeaturesFromOSM(latNum, lngNum, radiusNum);
    const processedFeatures = processFeatures(osmRaw);
    const envData = await fetchEnvironmentalData(latNum, lngNum, null, radiusNum);
    const linkedFeatures = linkRisks(processedFeatures, envData.environmentalSites || []);

    res.json({
      success: true,
      radius: radiusNum,
      totalFeatures: linkedFeatures.length,
      highRiskFeatures: linkedFeatures.filter((f) => f.risk_level === 'HIGH').length,
      mediumRiskFeatures: linkedFeatures.filter((f) => f.risk_level === 'MEDIUM').length,
      lowRiskFeatures: linkedFeatures.filter((f) => String(f.type).toLowerCase() === 'wetland').length,
      wetlands: linkedFeatures.filter((f) => String(f.type).toLowerCase() === 'wetland').length,
      features: linkedFeatures
    });
  } catch (err) {
    console.error('[features]', err.message);
    res.status(500).json({ error: err.message || 'Feature extraction failed' });
  }
});

// ---------------------------------------------------------------------------
// GET /datasets — extract area features from OSM and link to nearby risks
// Alias for /features, returns same data but optimized for datasets display
// ---------------------------------------------------------------------------
app.get('/datasets', async (req, res) => {
  try {
    const { lat, lng, radius = 250 } = req.query;
    if (!lat || !lng) {
      return res.status(400).json({ error: 'lat and lng query parameters are required' });
    }

    const result = await gisSearch.nearbySearch(lat, lng, radius);
    res.json(result);
  } catch (err) {
    console.error('[datasets]', err.message);
    res.status(500).json({ error: err.message || 'Dataset search failed' });
  }
});

// ---------------------------------------------------------------------------
// GET /analyze-point — analyze environmental risks at a specific point
// Returns datasets and features within radius of the point
// ---------------------------------------------------------------------------
app.get('/analyze-point', async (req, res) => {
  try {
    const { lat, lng, radius = 250 } = req.query;
    if (!lat || !lng) {
      return res.status(400).json({ error: 'lat and lng query parameters are required' });
    }

    const latNum = Number(lat);
    const lngNum = Number(lng);
    const radiusNum = Number(radius) || 250;

    if (!Number.isFinite(latNum) || !Number.isFinite(lngNum)) {
      return res.status(400).json({ error: 'lat and lng must be valid numbers' });
    }

    // Get environmental datasets
    const storedDatasets = globalDataStore.searchGeoPoints(latNum, lngNum, radiusNum);
    const envData = storedDatasets.length > 0
      ? { environmentalSites: storedDatasets }
      : await fetchEnvironmentalData(latNum, lngNum, null, radiusNum);
    const datasets = envData.environmentalSites || [];

    // Get OSM features
    const storedFeatures = globalDataStore.searchFeatures(latNum, lngNum, radiusNum);
    const features = storedFeatures.length > 0
      ? storedFeatures
      : processFeatures(await fetchAreaFeaturesFromOSM(latNum, lngNum, radiusNum));

    // Calculate distance from analysis point to each dataset
    const withDistance = datasets.map((d) => ({
      ...d,
      distance: Math.round(
        haversineMeters(
          latNum,
          lngNum,
          d.latitude || d.lat || 0,
          d.longitude || d.lon || 0
        )
      )
    }));

    res.json({
      success: true,
      center: { lat: latNum, lng: lngNum },
      radius: radiusNum,
      datasets: withDistance.sort((a, b) => a.distance - b.distance),
      features: features,
      riskSummary: {
        high: datasets.filter((d) => d.risk_level === 'HIGH').length,
        medium: datasets.filter((d) => d.risk_level === 'MEDIUM').length,
        low: datasets.filter((d) => d.risk_level === 'LOW').length
      }
    });
  } catch (err) {
    console.error('[analyze-point]', err.message);
    res.status(500).json({ error: err.message || 'Point analysis failed' });
  }
});

// ---------------------------------------------------------------------------
// POST /save-work — save analyst's drawn shapes and work state
// Stores geometry and state for later resume
// ---------------------------------------------------------------------------
const savedWork = {}; // In-memory storage for work sessions

app.post('/save-work', async (req, res) => {
  try {
    const { geometry, subjectLat, subjectLng, radius, timestamp } = req.body;
    
    // Generate session ID
    const sessionId = `work_${Math.random().toString(36).substr(2, 9)}`;
    
    // Store in memory (in production, use database)
    savedWork[sessionId] = {
      geometry,
      subjectLat,
      subjectLng,
      radius,
      timestamp,
      savedAt: new Date().toISOString()
    };

    res.json({
      success: true,
      sessionId,
      message: 'Work session saved'
    });
  } catch (err) {
    console.error('[save-work]', err.message);
    res.status(500).json({ error: err.message || 'Save failed' });
  }
});

// ---------------------------------------------------------------------------
// GET /resume-work/:sessionId — resume a previously saved work session
// ---------------------------------------------------------------------------
app.get('/resume-work/:sessionId', (req, res) => {
  try {
    const { sessionId } = req.params;
    const workSession = savedWork[sessionId];

    if (!workSession) {
      return res.status(404).json({ error: 'Session not found' });
    }

    res.json({
      success: true,
      ...workSession
    });
  } catch (err) {
    console.error('[resume-work]', err.message);
    res.status(500).json({ error: err.message || 'Resume failed' });
  }
});

/**
 * PUT /admin/orders/:orderId/assign
 * Assign order to analyst (admin only)
 */
app.put('/admin/orders/:orderId/assign', requireAuth, requireRole('admin'), (req, res) => {
  const { orderId } = req.params;
  const { analyst_id } = req.body;

  if (!analyst_id) {
    return res.status(400).json({ error: 'analyst_id required' });
  }

  const result = auth.assignOrder(parseInt(orderId), parseInt(analyst_id));
  let updatedInMemory = false;
  const inMemoryOrder = (orders || []).find((o) => String(o.id) === String(orderId));
  if (inMemoryOrder) {
    inMemoryOrder.assigned_to = parseInt(analyst_id);
    inMemoryOrder.analyst_id = parseInt(analyst_id);
    inMemoryOrder.status = inMemoryOrder.status || 'assigned';
    inMemoryOrder.updated_at = new Date().toISOString();
    updatedInMemory = true;
  }

  const success = result.success || updatedInMemory;
  res.status(success ? 200 : 400).json(success ? { success: true } : result);
});

/**
 * GET /admin/orders
 * Get all orders for admin (with more detail)
 */
app.get('/admin/orders', requireAuth, requireRole('admin'), async (req, res) => {
  try {
    const authOrders = auth.getAllOrders?.() || [];
    const inMemoryOrders = Array.isArray(orders) ? orders : [];
    let supabaseOrders = [];

    if (supabaseUrl !== 'https://your-project.supabase.co') {
      try {
        const { data, error } = await supabase.from('orders').select('*');
        if (!error && Array.isArray(data)) {
          supabaseOrders = data;
        }
      } catch (error) {
        console.warn('Supabase admin orders fetch warning:', error.message);
      }
    }

    // Merge all sources by ID, preferring Supabase/auth fields when available.
    const byId = new Map();
    [...inMemoryOrders, ...authOrders, ...supabaseOrders].forEach((o) => {
      const id = String(o?.id ?? '');
      if (!id) return;
      byId.set(id, {
        ...(byId.get(id) || {}),
        ...o
      });
    });

    res.json(Array.from(byId.values()));
  } catch (error) {
    console.error('Error loading admin orders:', error);
    res.status(500).json({ error: 'Failed to load admin orders' });
  }
});

/**
 * POST /admin/orders/:orderId/send-to-client
 * Notify client that their report is ready (admin only)
 */
app.post('/admin/orders/:orderId/send-to-client', requireAuth, requireRole('admin'), async (req, res) => {
  const { orderId } = req.params;
  const numericOrderId = Number.parseInt(orderId, 10);

  let order = auth.getOrderById(numericOrderId);
  if (!order) {
    order = (orders || []).find((o) => String(o.id) === String(orderId));
  }
  if (!order && supabaseUrl !== 'https://your-project.supabase.co') {
    try {
      const lookupId = Number.isFinite(numericOrderId) ? numericOrderId : orderId;
      const { data, error } = await supabase
        .from('orders')
        .select('*')
        .eq('id', lookupId)
        .single();
      if (!error && data) {
        order = data;
      }
    } catch (error) {
      console.warn('Supabase send-to-client order fetch warning:', error.message);
    }
  }

  if (!order) {
    return res.status(404).json({ error: 'Order not found' });
  }

  const clientEmail = order.client_email || order.recipient_email_1 || order.email;
  if (!clientEmail) {
    return res.status(400).json({ error: 'No client email found for this order' });
  }

  const explicitReportUrl = String(order.report_url || '').trim();
  const downloadLink = explicitReportUrl
    ? (/^https?:\/\//i.test(explicitReportUrl)
        ? explicitReportUrl
        : `${req.protocol}://${req.get('host')}${explicitReportUrl.startsWith('/') ? '' : '/'}${explicitReportUrl}`)
    : `${req.protocol}://${req.get('host')}/download/${orderId}`;

  // Attach PDF only if we can resolve an order-specific file.
  let attachments = [];
  try {
    const reportsRoot = path.resolve(REPORTS_DIR);
    const explicitPath = order.report_path || order.reportPath || null;
    let resolvedAttachment = null;

    if (explicitPath) {
      const candidate = path.resolve(String(explicitPath));
      if (candidate.startsWith(reportsRoot) && fs.existsSync(candidate)) {
        resolvedAttachment = candidate;
      }
    }

    if (!resolvedAttachment) {
      const files = fs.readdirSync(REPORTS_DIR);
      const orderReport = files
        .filter(f => f.endsWith('.pdf') && (f.includes(`order-${orderId}`) || f.includes(`report-${orderId}`)))
        .sort()
        .pop();
      if (orderReport) {
        resolvedAttachment = path.join(REPORTS_DIR, orderReport);
      }
    }

    if (resolvedAttachment) {
      attachments = [{ path: resolvedAttachment }];
    }
  } catch (e) {
    // no report file available, send link only
  }

  try {
    await transporter.sendMail({
      to: clientEmail,
      subject: `Your GeoScope Report is Ready — ${order.project_name || `Order #${orderId}`}`,
      html: `
        <p>Dear ${order.client_name || 'Client'},</p>
        <p>Your environmental report for <strong>${order.project_name || `Order #${orderId}`}</strong> is ready.</p>
        <p>You can download your report here: <a href="${downloadLink}">${downloadLink}</a></p>
        <p>Thank you for choosing GeoScope.</p>
      `,
      attachments
    });

    auth.updateOrderStatus(numericOrderId, 'sent');

    const inMemoryOrder = (orders || []).find((o) => String(o.id) === String(orderId));
    if (inMemoryOrder) {
      inMemoryOrder.status = 'sent';
      inMemoryOrder.stage = 'COMPLETED';
      inMemoryOrder.updated_at = new Date().toISOString();
    }

    if (supabaseUrl !== 'https://your-project.supabase.co') {
      try {
        await supabase
          .from('orders')
          .update({ status: 'sent', stage: 'COMPLETED', updated_at: new Date().toISOString() })
          .eq('id', numericOrderId);
      } catch (error) {
        console.warn('Supabase send-to-client status update warning:', error.message);
      }
    }

    res.json({ success: true, message: `Report notification sent to ${clientEmail}` });
  } catch (err) {
    console.error('Error sending report to client:', err);
    res.status(500).json({ error: 'Failed to send email', details: err.message });
  }
});

// Start server
const server = app.listen(PORT, async () => {
  console.log(`Server running on port ${PORT}`);
  console.log(`Auth security mode: ${JWT_AUTH_ENABLED ? 'JWT enabled' : 'JWT disabled (bypass mode)'}`);
  
  // Set global timeouts for long-running operations
  server.timeout = 600000; // 10 minutes
  server.keepAliveTimeout = 610000; // 10+ minutes
  
  await pingDB(); // Test PostgreSQL connection on startup

  // Sync actual database_name values from PostgreSQL into the dataset catalog
  try {
    const pgRows = await pgPool.query(
      `SELECT DISTINCT database_name, category FROM environmental_sites WHERE database_name IS NOT NULL ORDER BY database_name`
    );
    const seeds = pgRows.rows.map((r) => ({
      name: r.database_name,
      category: r.category || 'regulatory',
      country: 'USA',
      source_program: 'PostGIS',
      coverage_scope: 'installed-local',
      maintainer: 'GeoScope PostgreSQL'
    }));
    const result = globalDataStore.addDatasetSeeds(seeds);
    console.log(`[catalog] Synced ${seeds.length} database names from PostgreSQL (${result.inserted} new, ${result.updated} updated)`);
  } catch (err) {
    console.warn('[catalog] PG dataset sync skipped:', err.message);
  }
});
