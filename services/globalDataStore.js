'use strict';

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { buildUSStateDatasetSeeds, buildEPAAndUSTStateDatasetSeeds } = require('./usStateDatasets');
const METERS_PER_MILE = 1609.344;

const DATA_DIR = path.join(__dirname, '..', '.data');
const STORE_FILE = path.join(DATA_DIR, 'global-data-store.json');

const DATASET_SEEDS = [
  { name: 'CERCLIS', category: 'contamination', country: 'USA' },
  { name: 'RCRA', category: 'contamination', country: 'USA' },
  { name: 'UST', category: 'contamination', country: 'USA' },
  { name: 'LUST', category: 'contamination', country: 'USA' },
  { name: 'TRI', category: 'contamination', country: 'USA' },
  { name: 'EPA PFAS', category: 'pfas', country: 'USA' },
  { name: 'STATE PFAS TRACKING', category: 'pfas', country: 'USA' },
  { name: 'USGS MINES', category: 'mines', country: 'USA' },
  { name: 'USGS GEOLOGICAL UNITS', category: 'geology', country: 'USA' },
  { name: 'USGS RADON', category: 'radon', country: 'USA' },
  { name: 'USGS FAULT LINES', category: 'geology', country: 'USA' },
  { name: 'FEMA DFIRM', category: 'flood', country: 'USA' },
  { name: 'USFWS WETLANDS', category: 'wetlands', country: 'USA' },
  { name: 'PHMSA PIPELINES', category: 'pipelines', country: 'USA' },
  { name: 'EPA LANDFILLS', category: 'landfills', country: 'USA' },
  { name: 'ECHO INDUSTRIAL', category: 'industrial', country: 'USA' },
  { name: 'ECCC CONTAMINATED SITES', category: 'contamination', country: 'Canada' },
  { name: 'ECCC AIR POLLUTANTS', category: 'industrial', country: 'Canada' },
  { name: 'ECCC WASTE FACILITIES', category: 'landfills', country: 'Canada' },
  { name: 'NRCAN SOIL', category: 'geology', country: 'Canada' },
  { name: 'NRCAN GEOLOGICAL MAPS', category: 'geology', country: 'Canada' },
  { name: 'NRCAN MINES', category: 'mines', country: 'Canada' },
  { name: 'CANADIAN WATER QUALITY', category: 'water', country: 'Canada' },
  { name: 'EEA INDUSTRIAL SITES', category: 'industrial', country: 'Europe' },
  { name: 'EEA POLLUTION DATA', category: 'contamination', country: 'Europe' },
  { name: 'EEA LAND USE', category: 'landuse', country: 'Europe' },
  { name: 'E-PRTR', category: 'industrial', country: 'Europe' },
  { name: 'COPERNICUS FLOOD', category: 'flood', country: 'Europe' },
  { name: 'COPERNICUS WATER', category: 'water', country: 'Europe' },
  { name: 'OPENSTREETMAP BUILDINGS', category: 'features', country: 'Global' },
  { name: 'OPENSTREETMAP ADDRESSES', category: 'features', country: 'Global' },
  { name: 'HYDROSHEDS', category: 'water', country: 'Global' },
  { name: 'WORLDCLIM', category: 'climate', country: 'Global' },
  { name: 'NASA ELEVATION', category: 'geology', country: 'Global' },
  { name: 'NASA SATELLITE', category: 'imagery', country: 'Global' },
  { name: 'SCHOOLS', category: 'receptors', country: 'Global' },
  { name: 'HOSPITALS', category: 'receptors', country: 'Global' },
  { name: 'WATER SOURCES', category: 'water', country: 'Global' },
  { name: 'GEOLOGICAL HAZARDS', category: 'geology', country: 'Global' }
];

const EXTRA_DATASET_SEEDS = [
  { name: 'EPA CERCLIS ARCHIVE', category: 'contamination', country: 'USA' },
  { name: 'EPA SUPERFUND ENTERPRISE', category: 'contamination', country: 'USA' },
  { name: 'EPA RCRA TSD', category: 'contamination', country: 'USA' },
  { name: 'EPA RCRA LARGE QUANTITY GENERATORS', category: 'contamination', country: 'USA' },
  { name: 'EPA RCRA SMALL QUANTITY GENERATORS', category: 'contamination', country: 'USA' },
  { name: 'EPA UST NATIONAL REGISTER', category: 'contamination', country: 'USA' },
  { name: 'EPA LUST CORRECTIVE ACTION', category: 'contamination', country: 'USA' },
  { name: 'EPA TRI ACTIVE FACILITIES', category: 'industrial', country: 'USA' },
  { name: 'EPA TRI HISTORICAL', category: 'industrial', country: 'USA' },
  { name: 'EPA BROWNSFIELDS', category: 'contamination', country: 'USA' },
  { name: 'EPA ECHO ENFORCEMENT', category: 'industrial', country: 'USA' },
  { name: 'EPA ECHO AIR', category: 'industrial', country: 'USA' },
  { name: 'EPA ECHO WATER', category: 'water', country: 'USA' },
  { name: 'EPA PFAS MONITORING', category: 'pfas', country: 'USA' },
  { name: 'EPA PFAS DISCHARGE TRACKER', category: 'pfas', country: 'USA' },
  { name: 'STATE PFAS NY', category: 'pfas', country: 'USA' },
  { name: 'STATE PFAS CA', category: 'pfas', country: 'USA' },
  { name: 'STATE PFAS MI', category: 'pfas', country: 'USA' },
  { name: 'USGS ABANDONED MINES', category: 'mines', country: 'USA' },
  { name: 'USGS ACTIVE MINES', category: 'mines', country: 'USA' },
  { name: 'USGS COAL RESOURCES', category: 'geology', country: 'USA' },
  { name: 'USGS GEOLOGIC FAULTS', category: 'geology', country: 'USA' },
  { name: 'USGS LANDSLIDE SUSCEPTIBILITY', category: 'geology', country: 'USA' },
  { name: 'USGS SEISMIC HAZARD', category: 'geology', country: 'USA' },
  { name: 'USGS NATIONAL HYDROGRAPHY', category: 'water', country: 'USA' },
  { name: 'USGS WATER QUALITY PORTAL', category: 'water', country: 'USA' },
  { name: 'FEMA FLOODWAY', category: 'flood', country: 'USA' },
  { name: 'FEMA BASE FLOOD ELEVATION', category: 'flood', country: 'USA' },
  { name: 'FEMA COASTAL FLOOD', category: 'flood', country: 'USA' },
  { name: 'USFWS CRITICAL HABITAT', category: 'wetlands', country: 'USA' },
  { name: 'USFWS NWI ENHANCED', category: 'wetlands', country: 'USA' },
  { name: 'PHMSA HAZARDOUS LIQUID PIPELINES', category: 'pipelines', country: 'USA' },
  { name: 'PHMSA NATURAL GAS TRANSMISSION', category: 'pipelines', country: 'USA' },
  { name: 'EPA LANDFILL GHG', category: 'landfills', country: 'USA' },
  { name: 'STATE SOLID WASTE PERMITS', category: 'landfills', country: 'USA' },
  { name: 'NRCAN CONTAMINATED SITES', category: 'contamination', country: 'Canada' },
  { name: 'NRCAN ABANDONED MINES', category: 'mines', country: 'Canada' },
  { name: 'NRCAN ACTIVE MINES', category: 'mines', country: 'Canada' },
  { name: 'NRCAN GEOCHEMISTRY', category: 'geology', country: 'Canada' },
  { name: 'NRCAN RADON POTENTIAL', category: 'radon', country: 'Canada' },
  { name: 'ECCC NPRI FACILITIES', category: 'industrial', country: 'Canada' },
  { name: 'ECCC CONTAMINATED FEDERAL SITES', category: 'contamination', country: 'Canada' },
  { name: 'ECCC WATER MONITORING', category: 'water', country: 'Canada' },
  { name: 'ECCC FLOOD HAZARD', category: 'flood', country: 'Canada' },
  { name: 'CANADIAN DRINKING WATER ADVISORIES', category: 'water', country: 'Canada' },
  { name: 'PROVINCIAL LANDFILLS ON', category: 'landfills', country: 'Canada' },
  { name: 'PROVINCIAL LANDFILLS BC', category: 'landfills', country: 'Canada' },
  { name: 'PROVINCIAL PFAS QUEBEC', category: 'pfas', country: 'Canada' },
  { name: 'PROVINCIAL PFAS ALBERTA', category: 'pfas', country: 'Canada' },
  { name: 'EEA INDUSTRIAL EMISSIONS', category: 'industrial', country: 'Europe' },
  { name: 'EEA CONTAMINATED LAND', category: 'contamination', country: 'Europe' },
  { name: 'EEA AIR QUALITY STATIONS', category: 'industrial', country: 'Europe' },
  { name: 'EEA WATERBASE', category: 'water', country: 'Europe' },
  { name: 'EEA BATHING WATER', category: 'water', country: 'Europe' },
  { name: 'EEA FLOOD HAZARD MAPS', category: 'flood', country: 'Europe' },
  { name: 'COPERNICUS LAND MONITORING', category: 'landuse', country: 'Europe' },
  { name: 'COPERNICUS EMERGENCY FLOOD', category: 'flood', country: 'Europe' },
  { name: 'E-PRTR FACILITY RELEASES', category: 'industrial', country: 'Europe' },
  { name: 'E-PRTR OFF-SITE TRANSFERS', category: 'industrial', country: 'Europe' },
  { name: 'EUROGEOSURVEYS MINERAL RESOURCES', category: 'mines', country: 'Europe' },
  { name: 'EU SOIL CONTAMINATION INDICATORS', category: 'geology', country: 'Europe' },
  { name: 'EU RADON MAP', category: 'radon', country: 'Europe' },
  { name: 'EU PIPELINE NETWORK', category: 'pipelines', country: 'Europe' },
  { name: 'EU HOSPITAL LOCATIONS', category: 'receptors', country: 'Europe' },
  { name: 'EU SCHOOL LOCATIONS', category: 'receptors', country: 'Europe' },
  { name: 'OPENSTREETMAP HOSPITALS', category: 'receptors', country: 'Global' },
  { name: 'OPENSTREETMAP SCHOOLS', category: 'receptors', country: 'Global' },
  { name: 'OPENSTREETMAP INDUSTRIAL LANDUSE', category: 'industrial', country: 'Global' },
  { name: 'OPENSTREETMAP WASTE FACILITIES', category: 'landfills', country: 'Global' },
  { name: 'OPENSTREETMAP WATERWAYS', category: 'water', country: 'Global' },
  { name: 'OPENSTREETMAP PIPELINES', category: 'pipelines', country: 'Global' },
  { name: 'HYDROSHEDS RIVER NETWORK', category: 'water', country: 'Global' },
  { name: 'HYDROSHEDS BASINS', category: 'water', country: 'Global' },
  { name: 'WORLDCLIM TEMPERATURE', category: 'climate', country: 'Global' },
  { name: 'WORLDCLIM PRECIPITATION', category: 'climate', country: 'Global' },
  { name: 'NASA SRTM ELEVATION', category: 'geology', country: 'Global' },
  { name: 'NASA ASTER ELEVATION', category: 'geology', country: 'Global' },
  { name: 'NASA LANDSAT', category: 'imagery', country: 'Global' },
  { name: 'NASA MODIS', category: 'imagery', country: 'Global' },
  { name: 'GLOBAL VOLCANIC HAZARDS', category: 'geology', country: 'Global' },
  { name: 'GLOBAL EARTHQUAKE CATALOG', category: 'geology', country: 'Global' },
  { name: 'GLOBAL TSUNAMI HAZARD', category: 'flood', country: 'Global' },
  { name: 'GLOBAL GROUNDWATER', category: 'water', country: 'Global' },
  { name: 'GLOBAL SURFACE WATER', category: 'water', country: 'Global' },
  { name: 'GLOBAL INDUSTRIAL COMPLEXES', category: 'industrial', country: 'Global' },
  { name: 'GLOBAL CONTAMINATED SITES', category: 'contamination', country: 'Global' },
  { name: 'GLOBAL PFAS SCREENING', category: 'pfas', country: 'Global' },
  { name: 'GLOBAL LANDFILL INVENTORY', category: 'landfills', country: 'Global' },
  { name: 'GLOBAL MINING PROJECTS', category: 'mines', country: 'Global' },
  { name: 'GLOBAL SENSITIVE RECEPTORS', category: 'receptors', country: 'Global' }
];

const US_STATE_DATASET_SEEDS = buildUSStateDatasetSeeds();
const EPA_UST_STATE_DATASET_SEEDS = buildEPAAndUSTStateDatasetSeeds();

const CATALOG_SYNC_FIELDS = [
  'category',
  'country',
  'state',
  'state_code',
  'source_program',
  'useful_info',
  'coverage_scope',
  'maintainer',
  'priority'
];

let catalogAutoSyncTimer = null;
let catalogSyncStatus = {
  last_sync_at: null,
  last_reason: null,
  last_result: null,
  interval_ms: null,
  auto_sync_running: false
};

function normalizeDatasetName(value) {
  return String(value || '').trim().toUpperCase();
}

function upsertDatasetSeeds(store, seeds = []) {
  let inserted = 0;
  let updated = 0;

  const byName = new Map((store.datasets_master || []).map((item) => [normalizeDatasetName(item.name), item]));

  for (const seed of seeds) {
    const key = normalizeDatasetName(seed.name);
    if (!key) continue;

    const existing = byName.get(key);
    if (!existing) {
      const created = { id: store.nextDatasetId++, ...seed };
      store.datasets_master.push(created);
      byName.set(key, created);
      inserted += 1;
      continue;
    }

    let rowChanged = false;
    for (const field of CATALOG_SYNC_FIELDS) {
      const nextValue = seed[field] ?? null;
      if ((existing[field] ?? null) !== nextValue) {
        existing[field] = nextValue;
        rowChanged = true;
      }
    }

    if (rowChanged) {
      updated += 1;
    }
  }

  return { inserted, updated };
}

function readStore() {
  return JSON.parse(fs.readFileSync(STORE_FILE, 'utf8'));
}

function writeStore(store) {
  fs.writeFileSync(STORE_FILE, JSON.stringify(store, null, 2));
}

function ensureStore() {
  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
  }

  if (!fs.existsSync(STORE_FILE)) {
    writeStore({
      nextDatasetId: 1,
      nextGeoPointId: 1,
      nextFeatureId: 1,
      datasets_master: [],
      geo_points: [],
      features: []
    });
  }

  const store = readStore();
  let changed = false;

  if (!Array.isArray(store.geo_points)) {
    store.geo_points = [];
    changed = true;
  }

  const seedSync = upsertDatasetSeeds(store, [
    ...DATASET_SEEDS,
    ...EXTRA_DATASET_SEEDS,
    ...US_STATE_DATASET_SEEDS,
    ...EPA_UST_STATE_DATASET_SEEDS
  ]);
  if (seedSync.inserted > 0 || seedSync.updated > 0) {
    changed = true;
  }

  const datasetsById = new Map((store.datasets_master || []).map((item) => [item.id, item]));
  const usedIds = new Set();
  for (const point of store.geo_points) {
    if (point.site_uid) usedIds.add(sanitizeSiteUid(point.site_uid));
  }
  for (const point of store.geo_points) {
    const datasetName = datasetsById.get(point.dataset_id)?.name || 'DATASET';
    if (!point.site_uid) {
      point.site_uid = ensureUniqueSiteUid(usedIds, deriveSiteUid(point, datasetName));
      changed = true;
    }
  }

  if (changed) {
    writeStore(store);
  }

  return {
    changed,
    inserted: seedSync.inserted,
    updated: seedSync.updated,
    total_datasets: store.datasets_master.length
  };
}

ensureStore();

function syncStateCatalog(reason = 'manual') {
  const result = ensureStore();
  catalogSyncStatus = {
    ...catalogSyncStatus,
    last_sync_at: new Date().toISOString(),
    last_reason: reason,
    last_result: result
  };
  return {
    ...catalogSyncStatus
  };
}

function startCatalogAutoSync(intervalMs = 24 * 60 * 60 * 1000) {
  const parsed = Number(intervalMs);
  const safeInterval = Number.isFinite(parsed) && parsed >= 60 * 1000 ? parsed : 24 * 60 * 60 * 1000;

  if (catalogAutoSyncTimer) {
    clearInterval(catalogAutoSyncTimer);
    catalogAutoSyncTimer = null;
  }

  catalogAutoSyncTimer = setInterval(() => {
    try {
      syncStateCatalog('auto-interval');
    } catch (err) {
      console.error('[globalDataStore] catalog auto-sync failed:', err.message);
    }
  }, safeInterval);

  if (typeof catalogAutoSyncTimer.unref === 'function') {
    catalogAutoSyncTimer.unref();
  }

  catalogSyncStatus = {
    ...catalogSyncStatus,
    interval_ms: safeInterval,
    auto_sync_running: true
  };

  syncStateCatalog('startup');
  return {
    ...catalogSyncStatus
  };
}

function getCatalogSyncStatus() {
  return {
    ...catalogSyncStatus
  };
}

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function haversineMeters(lat1, lng1, lat2, lng2) {
  const R = 6371000;
  const dLat = ((lat2 - lat1) * Math.PI) / 180;
  const dLng = ((lng2 - lng1) * Math.PI) / 180;
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos((lat1 * Math.PI) / 180) *
      Math.cos((lat2 * Math.PI) / 180) *
      Math.sin(dLng / 2) * Math.sin(dLng / 2);
  return 2 * R * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function normalizeRiskLevel(value) {
  const risk = String(value || 'LOW').trim().toUpperCase();
  if (['HIGH', 'MEDIUM', 'LOW'].includes(risk)) return risk;
  return 'LOW';
}

function sanitizeSiteUid(value) {
  return String(value || '')
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 40);
}

function deriveSiteUid(row, datasetName) {
  const explicit = row.site_uid || row.site_id || row.source_id || row.registry_id;
  const normalizedExplicit = sanitizeSiteUid(explicit);
  if (normalizedExplicit) return normalizedExplicit;

  const latitude = toNumber(row.latitude ?? row.lat);
  const longitude = toNumber(row.longitude ?? row.lng ?? row.lon);
  const basis = [
    datasetName,
    row.name || row.site_name || '',
    latitude !== null ? latitude.toFixed(6) : '',
    longitude !== null ? longitude.toFixed(6) : '',
    row.address || ''
  ].join('|');
  const digest = crypto.createHash('sha1').update(basis).digest('hex').slice(0, 12).toUpperCase();
  return `SITE-${digest}`;
}

function ensureUniqueSiteUid(usedIds, candidate) {
  let uid = sanitizeSiteUid(candidate) || `SITE-${crypto.randomUUID().slice(0, 8).toUpperCase()}`;
  if (!usedIds.has(uid)) {
    usedIds.add(uid);
    return uid;
  }

  let i = 2;
  while (usedIds.has(`${uid}-${i}`)) {
    i += 1;
  }
  uid = `${uid}-${i}`;
  usedIds.add(uid);
  return uid;
}

function getOrCreateDataset(name, category = 'other', country = 'Global') {
  ensureStore();
  const normalizedName = String(name || '').trim();
  if (!normalizedName) {
    throw new Error('Dataset name is required');
  }

  const store = readStore();
  const found = store.datasets_master.find((item) => item.name === normalizedName);
  if (found) return found;

  const created = {
    id: store.nextDatasetId++,
    name: normalizedName,
    category,
    country
  };
  store.datasets_master.push(created);
  writeStore(store);
  return created;
}

function importGeoPoints(rows = []) {
  ensureStore();
  const store = readStore();
  let inserted = 0;
  const usedSiteUids = new Set(
    (store.geo_points || [])
      .map((item) => sanitizeSiteUid(item.site_uid))
      .filter(Boolean)
  );

  for (const row of rows) {
    const latitude = toNumber(row.latitude ?? row.lat);
    const longitude = toNumber(row.longitude ?? row.lng ?? row.lon);
    if (latitude === null || longitude === null) continue;

    const datasetName = String(row.dataset_name || row.dataset || row.name_of_dataset || 'Unnamed Dataset').trim();
    let dataset = store.datasets_master.find((item) => item.name === datasetName);
    if (!dataset) {
      dataset = {
        id: store.nextDatasetId++,
        name: datasetName,
        category: row.category || 'other',
        country: row.country || 'Global'
      };
      store.datasets_master.push(dataset);
    }

    const siteUid = ensureUniqueSiteUid(usedSiteUids, deriveSiteUid(row, datasetName));

    store.geo_points.push({
      id: store.nextGeoPointId++,
      site_uid: siteUid,
      dataset_id: dataset.id,
      name: row.name || row.site_name || dataset.name,
      latitude,
      longitude,
      address: row.address || '',
      risk_level: normalizeRiskLevel(row.risk_level),
      source: row.source || dataset.country || 'import',
      source_id: row.source_id || siteUid
    });
    inserted += 1;
  }

  writeStore(store);
  return inserted;
}

function importFeatures(rows = []) {
  ensureStore();
  const store = readStore();
  let inserted = 0;

  for (const row of rows) {
    const latitude = toNumber(row.latitude ?? row.lat);
    const longitude = toNumber(row.longitude ?? row.lng ?? row.lon);
    if (latitude === null || longitude === null) continue;

    store.features.push({
      id: store.nextFeatureId++,
      name: row.name || 'Unnamed feature',
      address: row.address || '',
      latitude,
      longitude,
      type: row.type || 'feature'
    });
    inserted += 1;
  }

  writeStore(store);
  return inserted;
}

function listDatasets() {
  ensureStore();
  const store = readStore();
  return [...store.datasets_master].sort((a, b) =>
    `${a.country}:${a.state || ''}:${a.category}:${a.name}`.localeCompare(`${b.country}:${b.state || ''}:${b.category}:${b.name}`)
  );
}

function searchGeoPoints(lat, lng, radius = 250) {
  ensureStore();
  const latNum = toNumber(lat);
  const lngNum = toNumber(lng);
  const radiusNum = Math.max(25, Number(radius) || 250);
  if (latNum === null || lngNum === null) return [];

  const store = readStore();
  const datasetsById = new Map(store.datasets_master.map((item) => [item.id, item]));

  return store.geo_points
    .map((row) => ({
      id: row.id,
      site_uid: row.site_uid,
      source_id: row.source_id || row.site_uid,
      database: datasetsById.get(row.dataset_id)?.name,
      database_name: datasetsById.get(row.dataset_id)?.name,
      dataset_id: row.dataset_id,
      category: datasetsById.get(row.dataset_id)?.category,
      country: datasetsById.get(row.dataset_id)?.country,
      site_name: row.name,
      name: row.name,
      latitude: row.latitude,
      longitude: row.longitude,
      lat: row.latitude,
      lng: row.longitude,
      address: row.address,
      risk_level: row.risk_level,
      source: row.source,
      distance_m: haversineMeters(latNum, lngNum, row.latitude, row.longitude),
      status: row.risk_level
    }))
    .filter((row) => row.distance_m <= radiusNum)
    .sort((a, b) => a.distance_m - b.distance_m);
}

function searchFeatures(lat, lng, radius = 250) {
  ensureStore();
  const latNum = toNumber(lat);
  const lngNum = toNumber(lng);
  const radiusNum = Math.max(25, Number(radius) || 250);
  if (latNum === null || lngNum === null) return [];

  const store = readStore();

  return store.features
    .map((row) => ({
      ...row,
      distance_m: haversineMeters(latNum, lngNum, row.latitude, row.longitude)
    }))
    .filter((row) => row.distance_m <= radiusNum)
    .sort((a, b) => a.distance_m - b.distance_m);
}

function getAddressLevelMatches(lat, lng, radius = 250) {
  const radiusNum = Math.max(25, Number(radius) || 250);
  const features = searchFeatures(lat, lng, radius);
  const datasets = searchGeoPoints(lat, lng, radius);

  return features.map((feature) => {
    const risks = datasets.filter((dataset) =>
      haversineMeters(feature.latitude, feature.longitude, dataset.latitude, dataset.longitude) <= radiusNum
    );

    return {
      ...feature,
      risks,
      nearby_databases: [...new Set(risks.map((risk) => risk.database_name))],
      risk_level: risks.length > 2 ? 'HIGH' : risks.length > 0 ? 'MEDIUM' : 'LOW'
    };
  });
}

function buildAddressLevelReport(lat, lng, radius = 250) {
  const radiusNum = Math.max(25, Number(radius) || 250);
  const matches = getAddressLevelMatches(lat, lng, radiusNum);

  const locations = matches.map((feature, index) => {
    const findings = feature.risks.slice(0, 10).map((risk) => ({
      dataset: risk.database_name || risk.database || 'Unknown dataset',
      category: risk.category || 'other',
      source: risk.source || 'Unknown source',
      distance_m: Math.round(Number(risk.distance_m) || 0),
      distance_miles: Number((Number(risk.distance_m || 0) / METERS_PER_MILE).toFixed(3)),
      risk_level: risk.risk_level || 'LOW',
      note: risk.address || risk.site_name || 'Potential environmental concern'
    }));

    return {
      location_number: index + 1,
      location_name: feature.name || `Location ${index + 1}`,
      address: feature.address || 'Address unavailable',
      type: feature.type || 'feature',
      latitude: feature.latitude,
      longitude: feature.longitude,
      risk_level: feature.risk_level,
      finding_count: feature.risks.length,
      nearest_distance_m: feature.nearest_distance_m,
      nearest_distance_miles: Number.isFinite(Number(feature.nearest_distance_m))
        ? Number((Number(feature.nearest_distance_m) / METERS_PER_MILE).toFixed(3))
        : null,
      findings
    };
  });

  const summary = {
    scanned_radius_m: radiusNum,
    scanned_radius_miles: Number((radiusNum / METERS_PER_MILE).toFixed(3)),
    total_locations: locations.length,
    high_risk_locations: locations.filter((x) => x.risk_level === 'HIGH').length,
    medium_risk_locations: locations.filter((x) => x.risk_level === 'MEDIUM').length,
    low_risk_locations: locations.filter((x) => x.risk_level === 'LOW').length,
    total_findings: locations.reduce((sum, x) => sum + x.finding_count, 0)
  };

  return { summary, locations };
}

function getCatalogCoverage(lat, lng, radius = 250) {
  const datasets = listDatasets();
  const matches = searchGeoPoints(lat, lng, radius);
  const grouped = new Map();

  for (const match of matches) {
    const key = `${match.dataset_id}`;
    if (!grouped.has(key)) {
      grouped.set(key, {
        matched_records: 0,
        addresses: new Set()
      });
    }
    const item = grouped.get(key);
    item.matched_records += 1;
    if (match.address) item.addresses.add(match.address);
  }

  return datasets.map((dataset) => {
    const hit = grouped.get(String(dataset.id));
    return {
      ...dataset,
      matched_records: hit?.matched_records || 0,
      matched_addresses: hit?.addresses?.size || 0
    };
  });
}

function stats() {
  ensureStore();
  const store = readStore();
  return {
    datasetCount: store.datasets_master.length,
    geoPointCount: store.geo_points.length,
    featureCount: store.features.length,
    siteUidCount: store.geo_points.filter((x) => x.site_uid).length,
    dbPath: STORE_FILE
  };
}

function sqlText(value) {
  if (value === null || value === undefined || value === '') return 'NULL';
  return `'${String(value).replace(/'/g, "''")}'`;
}

function buildDBeaverEnvironmentalSitesSql() {
  ensureStore();
  const store = readStore();
  const datasetsById = new Map(store.datasets_master.map((item) => [item.id, item]));

  const rows = store.geo_points
    .filter((row) => Number.isFinite(row.latitude) && Number.isFinite(row.longitude))
    .map((row) => {
      const dataset = datasetsById.get(row.dataset_id) || {};
      return {
        database_name: dataset.name || 'UNKNOWN DATASET',
        category: dataset.category || 'other',
        source_org: dataset.country || 'Global',
        site_name: row.name || dataset.name || 'Unnamed Site',
        address: row.address || '',
        status: row.risk_level || 'LOW',
        source_id: row.site_uid || row.source_id,
        source: row.source || 'import',
        latitude: row.latitude,
        longitude: row.longitude,
        attributes: JSON.stringify({
          dataset_id: row.dataset_id,
          local_id: row.id,
          site_uid: row.site_uid || null
        })
      };
    });

  const lines = [];
  lines.push('-- GeoScope export for DBeaver');
  lines.push('-- Target table: environmental_sites (PostGIS)');
  lines.push(`-- Exported rows: ${rows.length}`);
  lines.push('BEGIN;');

  for (const row of rows) {
    lines.push(
      'INSERT INTO environmental_sites (location, database_name, category, source_org, site_name, address, status, source_id, source, attributes) VALUES (' +
      `ST_SetSRID(ST_MakePoint(${Number(row.longitude)}, ${Number(row.latitude)}), 4326), ` +
      `${sqlText(row.database_name)}, ${sqlText(row.category)}, ${sqlText(row.source_org)}, ` +
      `${sqlText(row.site_name)}, ${sqlText(row.address)}, ${sqlText(row.status)}, ` +
      `${sqlText(row.source_id)}, ${sqlText(row.source)}, ${sqlText(row.attributes)}::jsonb` +
      ');'
    );
  }

  lines.push('COMMIT;');
  lines.push('');
  return lines.join('\n');
}

function csvField(value) {
  const s = String(value ?? '');
  if (/[",\n\r]/.test(s)) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

function buildDBeaverEnvironmentalSitesCsv() {
  ensureStore();
  const store = readStore();
  const datasetsById = new Map(store.datasets_master.map((item) => [item.id, item]));

  const header = [
    'source_id',
    'database_name',
    'category',
    'source_org',
    'site_name',
    'address',
    'status',
    'source',
    'latitude',
    'longitude',
    'dataset_id',
    'local_id',
    'site_uid'
  ];

  const lines = [header.join(',')];

  for (const row of store.geo_points) {
    if (!Number.isFinite(row.latitude) || !Number.isFinite(row.longitude)) continue;
    const dataset = datasetsById.get(row.dataset_id) || {};
    const sourceId = row.site_uid || row.source_id || '';
    const csvRow = [
      sourceId,
      dataset.name || 'UNKNOWN DATASET',
      dataset.category || 'other',
      dataset.country || 'Global',
      row.name || dataset.name || 'Unnamed Site',
      row.address || '',
      row.risk_level || 'LOW',
      row.source || 'import',
      row.latitude,
      row.longitude,
      row.dataset_id,
      row.id,
      row.site_uid || ''
    ].map(csvField).join(',');
    lines.push(csvRow);
  }

  return lines.join('\n') + '\n';
}

function buildPostgresCopyScript(csvPath = 'environmental-sites.csv') {
  const safePath = String(csvPath || 'environmental-sites.csv').replace(/'/g, "''");
  return [
    '-- GeoScope PostgreSQL COPY import script',
    '-- 1) Export CSV from: /data/export/dbeaver/environmental-sites.csv',
    '-- 2) Save the CSV locally and update the file path below if needed',
    'BEGIN;',
    '',
    'CREATE TEMP TABLE geoscope_env_sites_stage (',
    '  source_id TEXT,',
    '  database_name TEXT,',
    '  category TEXT,',
    '  source_org TEXT,',
    '  site_name TEXT,',
    '  address TEXT,',
    '  status TEXT,',
    '  source TEXT,',
    '  latitude DOUBLE PRECISION,',
    '  longitude DOUBLE PRECISION,',
    '  dataset_id INT,',
    '  local_id BIGINT,',
    '  site_uid TEXT',
    ');',
    '',
    `COPY geoscope_env_sites_stage (source_id, database_name, category, source_org, site_name, address, status, source, latitude, longitude, dataset_id, local_id, site_uid) FROM '${safePath}' WITH (FORMAT csv, HEADER true);`,
    '',
    'INSERT INTO environmental_sites (location, database_name, category, source_org, site_name, address, status, source_id, source, attributes)',
    'SELECT',
    '  ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) AS location,',
    '  database_name,',
    '  category,',
    '  source_org,',
    '  site_name,',
    '  address,',
    '  status,',
    '  source_id,',
    '  source,',
    '  jsonb_build_object(',
    "    'dataset_id', dataset_id,",
    "    'local_id', local_id,",
    "    'site_uid', site_uid",
    '  ) AS attributes',
    'FROM geoscope_env_sites_stage',
    'WHERE latitude IS NOT NULL',
    '  AND longitude IS NOT NULL',
    '  AND source_id IS NOT NULL',
    'ON CONFLICT (source_id) DO UPDATE SET',
    '  location = EXCLUDED.location,',
    '  database_name = EXCLUDED.database_name,',
    '  category = EXCLUDED.category,',
    '  source_org = EXCLUDED.source_org,',
    '  site_name = EXCLUDED.site_name,',
    '  address = EXCLUDED.address,',
    '  status = EXCLUDED.status,',
    '  source = EXCLUDED.source,',
    '  attributes = EXCLUDED.attributes,',
    '  updated_at = NOW();',
    '',
    'COMMIT;',
    ''
  ].join('\n');
}

function addDatasetSeeds(seeds = []) {
  const store = readStore();
  const { inserted, updated } = upsertDatasetSeeds(store, seeds);
  if (inserted > 0 || updated > 0) writeStore(store);
  return { inserted, updated };
}

module.exports = {
  importFeatures,
  addDatasetSeeds,
  importGeoPoints,
  getAddressLevelMatches,
  getCatalogCoverage,
  getOrCreateDataset,
  buildAddressLevelReport,
  listDatasets,
  searchFeatures,
  searchGeoPoints,
  stats,
  haversineMeters,
  buildDBeaverEnvironmentalSitesSql,
  buildDBeaverEnvironmentalSitesCsv,
  buildPostgresCopyScript,
  syncStateCatalog,
  startCatalogAutoSync,
  getCatalogSyncStatus
};