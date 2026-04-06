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

function getDistance(a, b) {
  const latA = toNumber(a.latitude ?? a.lat);
  const lngA = toNumber(a.longitude ?? a.lng ?? a.lon);
  const latB = toNumber(b.latitude ?? b.lat);
  const lngB = toNumber(b.longitude ?? b.lng ?? b.lon);
  if (latA === null || lngA === null || latB === null || lngB === null) {
    return Number.POSITIVE_INFINITY;
  }
  return haversineMeters(latA, lngA, latB, lngB);
}

function assignRisksToAddresses(features, datasets, radius = 250) {
  return (features || []).map((f) => {
    const matched = (datasets || []).filter((d) => getDistance(f, d) <= radius);
    const normalizedAddress = f.address && f.address !== 'Unknown location'
      ? f.address
      : 'Unnamed Location (Near subject property)';
    const feature = {
      ...f,
      address: normalizedAddress,
      risks: matched,
      flag: String(f.type || '').toLowerCase() === 'wetland' ? '⚠ Wetland Area' : ''
    };
    return feature;
  });
}

module.exports = {
  getDistance,
  assignRisksToAddresses
};
