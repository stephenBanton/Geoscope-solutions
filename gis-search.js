/**
 * GIS Spatial Search Module
 *
 * Queries live public APIs to find environmental records within a given
 * radius of a subject property:
 *   • EPA ECHO  — RCRA, TRI, NPDES, AIR, GHG facilities
 *   • FEMA NFHL — Flood zone DFIRM at the subject point
 *   • USFWS NWI — National Wetlands Inventory polygons
 *   • USGS MRDS — Mine / mineral-resource sites
 *   • EPA Radon  — Zone estimate (Zone 1-3) by latitude
 *
 * When a PostGIS database is populated (see schema.sql), replace each
 * fetch function with a ST_DWithin query against the local tables for
 * near-instant results and full 150+ database coverage.
 */

'use strict';

const axios = require('axios');
const https = require('https');
const { pool: pgPool } = require('./db');
const globalDataStore = require('./services/globalDataStore');

// Allow self-signed / older TLS certs from government ArcGIS servers
const GIS_AGENT = new https.Agent({ rejectUnauthorized: false });

// ---------------------------------------------------------------------------
// Distance helpers
// ---------------------------------------------------------------------------

function haversineMeters(lat1, lng1, lat2, lng2) {
  const R = 6371000;
  const φ1 = (lat1 * Math.PI) / 180;
  const φ2 = (lat2 * Math.PI) / 180;
  const Δφ = ((lat2 - lat1) * Math.PI) / 180;
  const Δλ = ((lng2 - lng1) * Math.PI) / 180;
  const a =
    Math.sin(Δφ / 2) ** 2 +
    Math.cos(φ1) * Math.cos(φ2) * Math.sin(Δλ / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

/** Compute a bounding box in degrees around a lat/lng point. */
function bboxFromCenter(lat, lng, radius_m) {
  const latDeg = radius_m / 111320;
  const lngDeg = radius_m / (111320 * Math.cos((lat * Math.PI) / 180));
  return {
    west:  lng - lngDeg,
    east:  lng + lngDeg,
    south: lat - latDeg,
    north: lat + latDeg,
  };
}

/** Map a database name string to one of five display categories. */
function categorizeDatabase(db) {
  const d = (db || '').toLowerCase();
  if (/farm|farmland|farmyard|agri|agric|orchard|vineyard|meadow|greenhouse/.test(d))
    return 'agriculture';
  if (/rcra|cerclis|npl|pfas|ust|lust|corracts|tris|brownfield|superfund|manifest|docket|hazard|pcb|transformer|spill/.test(d))
    return 'contamination';
  if (/flood|wetland|storm|nwis|basin|watershed/.test(d))
    return 'hydrology';
  if (/mine|radon|soil|ssurgo|geolog|coal|mgp|asbestos|umtra|vapor/.test(d))
    return 'geology';
  if (/school|hospital|daycare|prison|church|college|nursing|arena|daycare/.test(d))
    return 'receptors';
  // echo, npdes, air, dod, federal, rmp, icis, etc.
  return 'regulatory';
}

function safeGet(url, timeoutMs = 12000, agent = null) {
  const opts = { timeout: timeoutMs };
  if (agent) opts.httpsAgent = agent;
  return axios.get(url, opts).then((r) => r.data);
}

function normalizeLegacyStoreRecord(row, lat, lng) {
  const rowLat = Number(row.lat ?? row.latitude);
  const rowLng = Number(row.lng ?? row.longitude);
  return {
    id: row.id || row.source_id || row.local_id || null,
    database: row.database || row.database_name || 'Unknown',
    category: row.category || categorizeDatabase(row.database || row.database_name || ''),
    site_name: row.site_name || row.name || 'Unknown Site',
    address: row.address || '',
    lat: rowLat,
    lng: rowLng,
    distance_m: Number.isFinite(row.distance_m)
      ? Number(row.distance_m)
      : haversineMeters(lat, lng, rowLat, rowLng),
    status: row.status || row.risk_level || 'Unknown',
    source: row.source || 'Local Store',
  };
}

async function fetchPostgresSites(lat, lng, radius_m) {
  const sql = `
    SELECT
      id,
      database_name,
      category,
      site_name,
      address,
      status,
      source_org,
      source_id,
      ST_Y(location::geometry) AS lat,
      ST_X(location::geometry) AS lng,
      ST_Distance(
        location::geography,
        ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography
      ) AS distance_m
    FROM environmental_sites
    WHERE ST_DWithin(
      location::geography,
      ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography,
      $3
    )
    ORDER BY distance_m ASC
    LIMIT $4
  `;

  const maxResults = Math.max(100, Number(process.env.GIS_MAX_RESULTS || 3000));

  try {
    const query = await pgPool.query(sql, [lng, lat, radius_m, maxResults]);
    return query.rows.map((r) => ({
      id: r.source_id || `pg-${r.id}`,
      database: r.database_name,
      category: r.category || categorizeDatabase(r.database_name),
      site_name: r.site_name || 'Unknown Site',
      address: r.address || '',
      lat: Number(r.lat),
      lng: Number(r.lng),
      distance_m: Number(r.distance_m),
      status: r.status || 'Unknown',
      source: r.source_org ? `PostgreSQL / ${r.source_org}` : 'PostgreSQL / PostGIS',
    }));
  } catch (err) {
    console.warn('[GIS] PostgreSQL search unavailable:', err.message);
    return [];
  }
}

function dedupeResults(records) {
  const seen = new Set();
  const unique = [];

  for (const r of records) {
    if (!Number.isFinite(r.lat) || !Number.isFinite(r.lng)) continue;
    const siteKey = (r.site_name || '').toLowerCase().trim();
    const dbKey = (r.database || '').toLowerCase().trim();
    const srcId = (r.id || '').toString().toLowerCase().trim();
    const key = srcId
      ? `id:${srcId}`
      : `${dbKey}|${siteKey}|${r.lat.toFixed(5)}|${r.lng.toFixed(5)}`;

    if (seen.has(key)) continue;
    seen.add(key);
    unique.push(r);
  }

  return unique;
}

function buildSummary(results) {
  const by_category = {};
  const by_database = {};
  for (const r of results) {
    by_category[r.category] = (by_category[r.category] || 0) + 1;
    by_database[r.database] = (by_database[r.database] || 0) + 1;
  }
  return { total: results.length, by_category, by_database };
}

// ---------------------------------------------------------------------------
// EPA ECHO — facility search via program-specific REST endpoints
// (facility_search.json was deprecated; using CWA, RCRA, and AIR services)
// ---------------------------------------------------------------------------

function echoFacilitiesToRecords(facilities, lat, lng, radius_m, database, nameKey, streetKey, cityKey, stateKey, statusKey, sourceLabel) {
  return (facilities || [])
    .map((f, i) => {
      const fLat = parseFloat(f.FacLat);
      const fLng = parseFloat(f.FacLong);
      return {
        id: `${database.toLowerCase().replace(/\s/g, '-')}-${i}-${f.SourceID || f.RCRAIDs || f.AIRIDs || Math.random().toString(36).slice(2)}`,
        database,
        category: categorizeDatabase(database),
        site_name: f[nameKey] || f.CWPName || f.RCRAName || f.AIRName || 'Unknown Facility',
        address: [f[streetKey], f[cityKey], f[stateKey]].filter(Boolean).join(', '),
        lat: fLat,
        lng: fLng,
        distance_m: haversineMeters(lat, lng, fLat, fLng),
        status: f[statusKey] || 'Regulated',
        source: sourceLabel,
      };
    })
    .filter((f) => Number.isFinite(f.lat) && Number.isFinite(f.lng) && f.distance_m <= radius_m);
}

async function fetchEchoFacilities(lat, lng, radius_m) {
  const radiusMiles = (radius_m / 1609.344).toFixed(4);
  const base = `https://echodata.epa.gov/echo`;
  const qs   = `?output=json&p_lat=${lat}&p_long=${lng}&p_radius=${radiusMiles}`;

  const [cwaR, rcraR, airR, sdwR] = await Promise.allSettled([
    safeGet(`${base}/cwa_rest_services.get_facility_info${qs}`, 15000),
    safeGet(`${base}/rcra_rest_services.get_facility_info${qs}`, 15000),
    safeGet(`${base}/air_rest_services.get_facility_info${qs}`, 15000),
    safeGet(`${base}/sdw_rest_services.get_facility_info${qs}`, 15000),
  ]);

  const results = [];

  if (cwaR.status === 'fulfilled') {
    const facs = cwaR.value?.Results?.Facilities || cwaR.value?.Results?.facilities || [];
    results.push(...echoFacilitiesToRecords(facs, lat, lng, radius_m, 'NPDES', 'CWPName', 'CWPStreet', 'CWPCity', 'CWPState', 'CWPPermitStatusDesc', 'EPA ECHO / CWA'));
  } else {
    console.error('[GIS] EPA ECHO CWA failed:', cwaR.reason?.message);
  }

  if (rcraR.status === 'fulfilled') {
    const facs = rcraR.value?.Results?.Facilities || rcraR.value?.Results?.facilities || [];
    results.push(...echoFacilitiesToRecords(facs, lat, lng, radius_m, 'RCRA', 'RCRAName', 'RCRAStreet', 'RCRACity', 'RCRAState', 'RCRAComplStatus', 'EPA ECHO / RCRA'));
  } else {
    console.error('[GIS] EPA ECHO RCRA failed:', rcraR.reason?.message);
  }

  if (airR.status === 'fulfilled') {
    const facs = airR.value?.Results?.Facilities || airR.value?.Results?.facilities || [];
    results.push(...echoFacilitiesToRecords(facs, lat, lng, radius_m, 'AIR FACILITY', 'AIRName', 'AIRStreet', 'AIRCity', 'AIRState', 'AIRComplStatus', 'EPA ECHO / CAA'));
  } else {
    console.error('[GIS] EPA ECHO AIR failed:', airR.reason?.message);
  }

  if (sdwR.status === 'fulfilled') {
    const facs = sdwR.value?.Results?.Facilities || sdwR.value?.Results?.facilities || [];
    results.push(...echoFacilitiesToRecords(facs, lat, lng, radius_m, 'PWS', 'SDWName', 'SDWStreet', 'SDWCity', 'SDWState', 'SDWPermStatus', 'EPA ECHO / SDWA'));
  } else {
    console.warn('[GIS] EPA ECHO SDW skipped:', sdwR.reason?.message);
  }

  return results;
}

// ---------------------------------------------------------------------------
// FEMA NFHL — Flood zone at the subject point
// ---------------------------------------------------------------------------

async function fetchFloodZones(lat, lng) {
  const url =
    `https://hazards.fema.gov/gis/nfhl/rest/services/public/NFHL/MapServer/28/query` +
    `?where=1%3D1&geometry=${lng},${lat}` +
    `&geometryType=esriGeometryPoint&spatialRel=esriSpatialRelIntersects` +
    `&outFields=FLD_ZONE,SFHA_TF,ZONE_SUBTY&inSR=4326&outSR=4326&f=json`;
  try {
    const data = await safeGet(url, 12000, GIS_AGENT);
    return (data?.features || []).map((f, i) => ({
      id: `fema-${i}`,
      database: 'FLOOD DFIRM',
      category: 'hydrology',
      site_name: `Flood Zone ${f.attributes?.FLD_ZONE || 'Unknown'}`,
      address: (f.attributes?.ZONE_SUBTY || '').trim(),
      lat,
      lng,
      distance_m: 0,
      status: f.attributes?.SFHA_TF === 'T' ? 'High Risk – SFHA' : 'Flood Zone',
      source: 'FEMA NFHL',
    }));
  } catch (e) {
    console.error('[GIS] FEMA flood failed:', e.message);
    return [];
  }
}

// ---------------------------------------------------------------------------
// USFWS NWI — National Wetlands Inventory (bbox query)
// ---------------------------------------------------------------------------

async function fetchWetlands(lat, lng, radius_m) {
  const { west, east, south, north } = bboxFromCenter(lat, lng, radius_m);
  const url =
    `https://fwsprimary.wim.usgs.gov/server/rest/services/Wetlands/MapServer/0/query` +
    `?where=1%3D1&geometry=${west},${south},${east},${north}` +
    `&geometryType=esriGeometryEnvelope&spatialRel=esriSpatialRelIntersects` +
    `&outFields=WETLAND_TYPE,ACRES&inSR=4326&outSR=4326` +
    `&returnGeometry=true&f=json`;
  try {
    const data = await safeGet(url, 12000, GIS_AGENT);
    return (data?.features || []).slice(0, 15).map((f, i) => {
      const { lat: cLat, lng: cLng } = ringCentroid(f.geometry?.rings?.[0]);
      const fLat = cLat ?? lat;
      const fLng = cLng ?? lng;
      return {
        id: `nwi-${i}`,
        database: 'WETLANDS NWI',
        category: 'hydrology',
        site_name: f.attributes?.WETLAND_TYPE || 'Wetland',
        address: f.attributes?.ACRES
          ? `${parseFloat(f.attributes.ACRES).toFixed(2)} acres`
          : '',
        lat: fLat,
        lng: fLng,
        distance_m: haversineMeters(lat, lng, fLat, fLng),
        status: 'Protected',
        source: 'USFWS NWI',
      };
    });
  } catch (e) {
    console.error('[GIS] USFWS wetlands failed:', e.message);
    return [];
  }
}

function ringCentroid(ring) {
  if (!Array.isArray(ring) || ring.length === 0) return { lat: null, lng: null };
  const lng = ring.reduce((s, p) => s + p[0], 0) / ring.length;
  const lat = ring.reduce((s, p) => s + p[1], 0) / ring.length;
  return { lat, lng };
}

// ---------------------------------------------------------------------------
// USGS MRDS — Mineral Resources Data System (mines, mineral sites)
// ---------------------------------------------------------------------------

async function fetchMines(lat, lng, radius_m) {
  const { west, east, south, north } = bboxFromCenter(lat, lng, radius_m);
  const url =
    `https://mrdata.usgs.gov/api/v1/feature` +
    `?bbox=${west},${south},${east},${north}&dataset=mrds&limit=30&format=json`;
  try {
    const data = await safeGet(url, 10000, GIS_AGENT);
    const features = data?.features || data?.result || [];
    return features
      .map((f, i) => {
        const coords = f.geometry?.coordinates || [lng, lat];
        const fLng = coords[0];
        const fLat = coords[1];
        return {
          id: `mine-${i}`,
          database: 'MINES',
          category: 'geology',
          site_name:
            f.properties?.name ||
            f.properties?.SITE_NAME ||
            'Mine Site',
          address: [f.properties?.state, f.properties?.county]
            .filter(Boolean)
            .join(', '),
          lat: fLat,
          lng: fLng,
          distance_m: haversineMeters(lat, lng, fLat, fLng),
          status: f.properties?.OPER_STAT || 'Unknown',
          source: 'USGS MRDS',
        };
      })
      .filter(
        (f) =>
          Number.isFinite(f.lat) &&
          Number.isFinite(f.lng) &&
          f.distance_m <= radius_m
      );
  } catch (e) {
    console.error('[GIS] USGS mines failed:', e.message);
    return [];
  }
}

// ---------------------------------------------------------------------------
// EPA Radon Zone (simplified — production: use EPA/USGS radon shapefile)
// ---------------------------------------------------------------------------

function estimateRadonZone(lat) {
  // Approximate EPA radon zones 1-3 by latitude band.
  // Zone 1 = predicted avg >4 pCi/L  (high)
  // Zone 2 = predicted avg 2-4 pCi/L (moderate)
  // Zone 3 = predicted avg <2 pCi/L  (low)
  if (lat >= 42) return { zone: 1, label: 'Zone 1 — Predicted Avg >4 pCi/L (High)' };
  if (lat >= 36) return { zone: 2, label: 'Zone 2 — Predicted Avg 2–4 pCi/L (Moderate)' };
  return { zone: 3, label: 'Zone 3 — Predicted Avg <2 pCi/L (Low)' };
}

function buildRadonRecord(lat, lng) {
  const { zone, label } = estimateRadonZone(lat);
  return [
    {
      id: 'radon-zone',
      database: 'RADON EPA',
      category: 'geology',
      site_name: label,
      address: '',
      lat,
      lng,
      distance_m: 0,
      status: zone === 1 ? 'High Risk' : zone === 2 ? 'Moderate' : 'Low',
      source: 'EPA Radon Zone Map',
    },
  ];
}

// ---------------------------------------------------------------------------
// OSM Farm/Agriculture landuse features
// ---------------------------------------------------------------------------

async function fetchFarmLanduse(lat, lng, radius_m) {
  const query = `
    [out:json][timeout:25];
    (
      node(around:${Math.round(radius_m)},${lat},${lng})["landuse"~"^(farmland|farmyard|orchard|vineyard|meadow|greenhouse_horticulture)$"];
      way(around:${Math.round(radius_m)},${lat},${lng})["landuse"~"^(farmland|farmyard|orchard|vineyard|meadow|greenhouse_horticulture)$"];
      relation(around:${Math.round(radius_m)},${lat},${lng})["landuse"~"^(farmland|farmyard|orchard|vineyard|meadow|greenhouse_horticulture)$"];
      node(around:${Math.round(radius_m)},${lat},${lng})["place"="farm"];
      way(around:${Math.round(radius_m)},${lat},${lng})["place"="farm"];
      relation(around:${Math.round(radius_m)},${lat},${lng})["place"="farm"];
    );
    out center tags;
  `;

  let data;
  try {
    data = await axios.post('https://overpass-api.de/api/interpreter', query, {
      headers: { 'Content-Type': 'text/plain' },
      timeout: 30000,
      httpsAgent: GIS_AGENT,
    });
  } catch (err) {
    console.warn('[GIS] OSM farm landuse fetch failed:', err.message);
    return [];
  }

  const elements = Array.isArray(data?.data?.elements) ? data.data.elements : [];
  return elements
    .map((el, idx) => {
      const fLat = Number(el.lat ?? el.center?.lat);
      const fLng = Number(el.lon ?? el.center?.lon);
      if (!Number.isFinite(fLat) || !Number.isFinite(fLng)) return null;

      const tags = el.tags || {};
      const landuse = String(tags.landuse || tags.place || 'farm').toLowerCase();
      const siteName = tags.name || `OSM ${landuse}`;
      const address = [tags['addr:housenumber'], tags['addr:street'], tags['addr:city'], tags['addr:state']]
        .filter(Boolean)
        .join(', ');

      return {
        id: `osm-farm-${el.type || 'obj'}-${el.id || idx}`,
        database: `OSM Farm Landuse (${landuse})`,
        category: 'agriculture',
        site_name: siteName,
        address: address || '',
        lat: fLat,
        lng: fLng,
        distance_m: haversineMeters(lat, lng, fLat, fLng),
        status: 'Mapped landuse',
        source: 'OpenStreetMap / Overpass',
      };
    })
    .filter((r) => r && Number.isFinite(r.distance_m) && r.distance_m <= radius_m);
}

// ---------------------------------------------------------------------------
// EPA TRI via Envirofacts
// Primary: state + county query from FCC reverse geocode
// Fallback: state-only query when county lookup/query is unavailable
// ---------------------------------------------------------------------------
async function fetchTRIFacilities(lat, lng, radius_m) {
  try {
    const reverse = await safeGet(
      `https://geo.fcc.gov/api/census/block/find?format=json&latitude=${lat}&longitude=${lng}&showall=true`,
      12000
    );
    const stateCode = String(reverse?.State?.code || '').trim().toUpperCase();
    const rawCounty = String(reverse?.County?.name || '').trim();
    if (!stateCode) return [];

    const countyName = rawCounty
      .replace(/\s+County$/i, '')
      .replace(/\s+Parish$/i, '')
      .trim()
      .toUpperCase();

    let facilities = [];
    if (countyName) {
      const triCountyUrl =
        `https://data.epa.gov/efservice/TRI_FACILITY` +
        `/STATE_ABBR/=/${encodeURIComponent(stateCode)}` +
        `/COUNTY_NAME/=/${encodeURIComponent(countyName)}` +
        `/JSON`;
      const countyData = await safeGet(triCountyUrl, 22000, GIS_AGENT).catch(() => []);
      facilities = Array.isArray(countyData) ? countyData : [];
    }

    if (!facilities.length) {
      const triStateUrl =
        `https://data.epa.gov/efservice/TRI_FACILITY` +
        `/STATE_ABBR/=/${encodeURIComponent(stateCode)}` +
        `/ROWS/0:5000/JSON`;
      const stateData = await safeGet(triStateUrl, 26000, GIS_AGENT).catch(() => []);
      facilities = Array.isArray(stateData) ? stateData : [];
    }

    return facilities
      .map((f, i) => {
        const fLat = parseFloat(f.pref_latitude);
        const rawLng = parseFloat(f.pref_longitude);
        const fLng = Number.isFinite(rawLng) ? (rawLng > 0 ? -rawLng : rawLng) : NaN;
        if (!Number.isFinite(fLat) || !Number.isFinite(fLng)) return null;

        return {
          id: `tri-${f.tri_facility_id || f.epa_registry_id || i}`,
          database: 'TRIS',
          category: 'contamination',
          site_name: f.facility_name || 'Unknown TRI Facility',
          address: [f.street_address, f.city_name, f.state_abbr].filter(Boolean).join(', '),
          lat: fLat,
          lng: fLng,
          distance_m: haversineMeters(lat, lng, fLat, fLng),
          status: 'TRI Reporter',
          source: 'EPA Envirofacts / TRI',
        };
      })
      .filter((f) => f && Number.isFinite(f.distance_m) && f.distance_m <= radius_m);
  } catch (e) {
    console.error('[GIS] TRI facilities failed:', e.message);
    return [];
  }
}

// ---------------------------------------------------------------------------
// EPA SEMS — Superfund / NPL sites via Envirofacts
// ---------------------------------------------------------------------------
async function fetchSuperfundSites(lat, lng, radius_m) {
  return [];
}

// ---------------------------------------------------------------------------
// EPA RMP — Risk Management Plan facilities via Envirofacts
// ---------------------------------------------------------------------------
async function fetchRMPFacilities(lat, lng, radius_m) {
  return [];
}

// ---------------------------------------------------------------------------
// EPA FRS — Facility Registry Service (all-programs bbox query)
// Returns facilities registered in ANY EPA program (RCRA, TRI, Superfund,
// RMP, AIR, NPDES, SDWA, etc.) that are not already captured by ECHO.
// ---------------------------------------------------------------------------
async function fetchFRSFacilities(lat, lng, radius_m) {
  return [];
}

// ---------------------------------------------------------------------------
// Main export
// ---------------------------------------------------------------------------

async function nearbySearch(lat, lng, radius_m = 250) {
  lat = parseFloat(lat);
  lng = parseFloat(lng);
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
    throw new Error('Invalid lat/lng — must be numeric');
  }
  // Allow up to DEFAULT_REPORT_RADIUS_MILES (5 mi = ~8047 m) plus a buffer.
  // Never silently cap below what the caller requests for report purposes.
  radius_m = Math.min(Math.max(Number(radius_m) || 250, 50), 24140); // 15 mile hard cap

  const [pgR, echoR, floodR, wetlandsR, minesR, farmsR, triR, superfundR, rmpR, frsR] = await Promise.allSettled([
    fetchPostgresSites(lat, lng, radius_m),
    fetchEchoFacilities(lat, lng, radius_m),
    fetchFloodZones(lat, lng),
    fetchWetlands(lat, lng, radius_m),
    fetchMines(lat, lng, radius_m),
    fetchFarmLanduse(lat, lng, radius_m),
    fetchTRIFacilities(lat, lng, radius_m),
    fetchSuperfundSites(lat, lng, radius_m),
    fetchRMPFacilities(lat, lng, radius_m),
    fetchFRSFacilities(lat, lng, radius_m),
  ]);

  const storedResults = globalDataStore
    .searchGeoPoints(lat, lng, radius_m)
    .map((row) => normalizeLegacyStoreRecord(row, lat, lng));

  const merged = [
    ...(pgR.status        === 'fulfilled' ? pgR.value        : []),
    ...storedResults,
    ...(echoR.status      === 'fulfilled' ? echoR.value      : []),
    ...(floodR.status     === 'fulfilled' ? floodR.value     : []),
    ...(wetlandsR.status  === 'fulfilled' ? wetlandsR.value  : []),
    ...(minesR.status     === 'fulfilled' ? minesR.value     : []),
    ...(farmsR.status     === 'fulfilled' ? farmsR.value     : []),
    ...(triR.status       === 'fulfilled' ? triR.value       : []),
    ...(superfundR.status === 'fulfilled' ? superfundR.value : []),
    ...(rmpR.status       === 'fulfilled' ? rmpR.value       : []),
    ...(frsR.status       === 'fulfilled' ? frsR.value       : []),
    ...buildRadonRecord(lat, lng),
  ];

  const results = dedupeResults(merged).sort((a, b) => a.distance_m - b.distance_m);
  const summary = buildSummary(results);

  return {
    subject: { lat, lng },
    radius_m,
    source: 'hybrid-postgresql-live-apis',
    results,
    summary,
  };
}

module.exports = { nearbySearch, haversineMeters, categorizeDatabase };
