const axios = require('axios');
const globalDataStore = require('./globalDataStore');

async function getFeatures(lat, lng, radius = 250) {
  const stored = globalDataStore.searchFeatures(lat, lng, radius);
  if (stored.length > 0) {
    return stored.map((item) => ({
      name: item.name || 'Unnamed',
      address: item.address || 'Unknown location',
      type: item.type || 'feature',
      latitude: item.latitude,
      longitude: item.longitude
    }));
  }

  const query = `
    [out:json][timeout:30];
    (
      node(around:${radius},${lat},${lng});
      way(around:${radius},${lat},${lng});
    );
    out center tags;
  `;

  const url = 'https://overpass-api.de/api/interpreter';
  const res = await axios.post(url, query, {
    headers: { 'Content-Type': 'text/plain' },
    timeout: 30000
  });

  return (res.data?.elements || [])
    .map((el) => ({
      name: el.tags?.name || 'Unnamed',
      address: [el.tags?.['addr:housenumber'], el.tags?.['addr:street']].filter(Boolean).join(' ') || 'Unknown location',
      type: el.tags?.building || el.tags?.amenity || el.tags?.natural || el.tags?.landuse || 'feature',
      latitude: el.lat || el.center?.lat,
      longitude: el.lon || el.center?.lon
    }))
    .filter((el) => Number.isFinite(Number(el.latitude)) && Number.isFinite(Number(el.longitude)));
}

module.exports = { getFeatures };
