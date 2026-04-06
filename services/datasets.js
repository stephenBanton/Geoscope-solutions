const gisSearch = require('../gis-search');
const globalDataStore = require('./globalDataStore');

async function getDatasets(lat, lng, radius = 250) {
  const stored = globalDataStore.searchGeoPoints(lat, lng, radius);
  if (stored.length > 0) {
    return stored.map((item) => ({
      ...item,
      database_name: item.database_name || item.database,
      site_name: item.site_name || item.name,
      latitude: item.latitude ?? item.lat,
      longitude: item.longitude ?? item.lng
    }));
  }

  const result = await gisSearch.nearbySearch(lat, lng, radius);
  return (result?.results || []).map((item) => ({
    ...item,
    database_name: item.database,
    site_name: item.site_name,
    latitude: item.lat,
    longitude: item.lng
  }));
}

module.exports = { getDatasets };
