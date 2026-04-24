# Geoscope 120+ Dataset API — Usage Guide

## Quick Start

### Live Endpoint
```
https://api.geoscopesolutions.com/api/nearby-search?lat=LAT&lng=LNG&radius_m=RADIUS
```

### Example Query (Boston, MA)
```bash
curl "https://api.geoscopesolutions.com/api/nearby-search?lat=42.3601&lng=-71.0589&radius_m=3000"
```

### Response Format
```json
{
  "subject": {
    "lat": 42.3601,
    "lng": -71.0589
  },
  "radius_m": 3000,
  "source": "hybrid-postgresql-live-apis",
  "summary": {
    "total": 1800,
    "by_category": {
      "contamination": 450,
      "hydrology": 200,
      "geology": 1050,
      "receptors": 80,
      "regulatory": 20
    },
    "by_database": {
      "SSURGO": 1000,
      "RADON EPA": 50,
      "RCRA": 150,
      "AIR FACILITY": 100,
      "TRIS": 50,
      "NPDES": 30,
      "SCHOOLS": 45,
      "HOSPITALS": 15,
      ... (more databases)
    }
  },
  "results": [
    {
      "id": "ssurgo-104355",
      "database": "SSURGO",
      "category": "geology",
      "site_name": "Urban land, 0 to 15 percent slopes",
      "address": "Map unit symbol: URB",
      "lat": 42.3601,
      "lng": -71.0589,
      "distance_m": 0,
      "status": "Soil Map Unit",
      "source": "USDA NRCS SSURGO"
    },
    {
      "id": "radon-zone",
      "database": "RADON EPA",
      "category": "geology",
      "site_name": "Zone 1 — Predicted Avg >4 pCi/L (High)",
      "address": "",
      "lat": 42.3601,
      "lng": -71.0589,
      "distance_m": 0,
      "status": "High Risk",
      "source": "EPA Radon Zone Map"
    },
    {
      "id": "rcra-2890",
      "database": "RCRA",
      "category": "contamination",
      "site_name": "Boston Hazardous Waste Facility",
      "address": "123 Main Street, Boston, MA",
      "lat": 42.3598,
      "lng": -71.0581,
      "distance_m": 125,
      "status": "Large Quantity Generator",
      "source": "EPA Envirofacts / RCRA"
    },
    {
      "id": "npdes-4521",
      "database": "NPDES",
      "category": "contamination",
      "site_name": "Charles River Outfall",
      "address": "Cambridge, MA",
      "lat": 42.3550,
      "lng": -71.0655,
      "distance_m": 890,
      "status": "Active Permit",
      "source": "EPA ECHO / CWA"
    },
    {
      "id": "schools-osm-node-12345",
      "database": "SCHOOLS",
      "category": "receptors",
      "site_name": "Boston Latin School",
      "address": "78 Avenue Louis Pasteur, Boston, MA",
      "lat": 42.3465,
      "lng": -71.0888,
      "distance_m": 2780,
      "status": "Mapped Facility",
      "source": "OpenStreetMap / Overpass"
    },
    ... (up to 3000 records)
  ]
}
```

---

## Query Parameters

### Required Parameters
| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `lat` | number | Latitude (WGS84) | 42.3601 |
| `lng` | number | Longitude (WGS84) | -71.0589 |

### Optional Parameters
| Parameter | Type | Default | Range | Description |
|-----------|------|---------|-------|-------------|
| `radius_m` | number | 250 | 50–24,140 | Search radius in meters (50m–15 miles) |

### Examples

#### 1km Radius (Tight Search)
```
https://api.geoscopesolutions.com/api/nearby-search?lat=40.7128&lng=-74.0060&radius_m=1000
```

#### 5km Radius (Typical Report)
```
https://api.geoscopesolutions.com/api/nearby-search?lat=40.7128&lng=-74.0060&radius_m=5000
```

#### 15 Mile Radius (Maximum)
```
https://api.geoscopesolutions.com/api/nearby-search?lat=40.7128&lng=-74.0060&radius_m=24140
```

---

## Database Categories

### 1. **Contamination** (50+ datasets)
Hazardous waste, chemical spills, regulatory sites
- RCRA (Resource Conservation and Recovery Act) — hazardous waste
- NPDES (National Pollutant Discharge Elimination System) — water discharges
- CERCLIS — comprehensive environmental response sites
- NPL (National Priorities List) — Superfund sites (final, proposed, delisted)
- TRI (Toxic Release Inventory) — chemical releases
- PCS (Permit Compliance System) — water permits
- UST/LUST — underground storage tanks (active/leaking)
- Brownfields — industrial/commercial sites
- Hazmat Manifest — hazardous waste tracking
- AIRS — air pollution facilities

### 2. **Hydrology** (15+ datasets)
Water resources, wetlands, flood hazards
- NWIS (National Water Information System) — monitoring stations
- USFWS NWI (National Wetlands Inventory) — wetland areas
- FEMA NFHL (National Flood Hazard Layer) — flood zones
- Streams/rivers — OSM water features
- Hydrologic units — watersheds/basins

### 3. **Geology** (10+ datasets)
Soils, minerals, seismic hazards, radon
- SSURGO (Soil Survey Geographic) — detailed soil maps
- STATSGO (State Soil Geographic) — regional soil data
- MRDS (Mineral Resources Data System) — mine sites
- Earthquakes — seismic events (1 year + historical)
- Radon — estimated EPA radon zones

### 4. **Receptors** (15+ datasets)
Sensitive populations, infrastructure
- Schools (public/private)
- Hospitals & medical facilities
- Daycare centers
- Colleges & universities
- Churches & religious sites
- Prisons & detention
- Nursing homes
- Arenas & sports facilities
- Government buildings
- Airports

### 5. **Agriculture** (5+ datasets)
Farmland, orchards, crop data
- Farm landuse — farmland, orchards, vineyards
- Pasture/meadow areas
- Greenhouses
- Crop Data Layer (CDL)

### 6. **Regulatory** (20+ datasets)
Compliance, permits, enforcement
- Air quality permits (CAA)
- Water quality permits (CWA)
- Drinking water systems (SDWA)
- Compliance records (ICIS)
- Enforcement actions
- Consent decrees
- Multi-program facilities (FRS)

---

## Use Cases

### Environmental Impact Assessment
Query all contamination + hydrology + geology databases within 3km radius to assess property risk.

```bash
curl "https://api.geoscopesolutions.com/api/nearby-search?lat=LAT&lng=LNG&radius_m=3000" \
  | jq '.results[] | select(.category == "contamination" or .category == "hydrology" or .category == "geology")'
```

### Receptor Screening
Find sensitive populations (schools, hospitals, daycare) near industrial sites.

```bash
curl "https://api.geoscopesolutions.com/api/nearby-search?lat=LAT&lng=LNG&radius_m=3000" \
  | jq '.results[] | select(.category == "receptors")'
```

### Hazmat Incident Analysis
Identify all hazardous waste, chemical spill, and manifest sites in area.

```bash
curl "https://api.geoscopesolutions.com/api/nearby-search?lat=LAT&lng=LNG&radius_m=3000" \
  | jq '.results[] | select(.database | test("(RCRA|TRI|HAZMAT|MANIFEST|UST|LUST)"))'
```

### Flood/Wetland Analysis
Find all flood zones and wetland areas for water resource planning.

```bash
curl "https://api.geoscopesolutions.com/api/nearby-search?lat=LAT&lng=LNG&radius_m=3000" \
  | jq '.results[] | select(.category == "hydrology")'
```

### Soil & Geology Report
Get soil type, radon zone, and earthquake history for property.

```bash
curl "https://api.geoscopesolutions.com/api/nearby-search?lat=LAT&lng=LNG&radius_m=500" \
  | jq '.results[] | select(.category == "geology")'
```

---

## Data Quality Notes

### Complete Coverage (95-99% accuracy)
- ✅ EPA ECHO (RCRA, NPDES, Air, SDWA)
- ✅ USDA SSURGO soil
- ✅ EPA Envirofacts tables (CERCLIS, NPL, PCS, PWS, ICIS, UST, LUST, Manifest, AIRS)
- ✅ USGS Earthquakes (1 year historical, magnitude 2+)
- ✅ FEMA NFHL flood zones (DFIRM)
- ✅ USFWS NWI wetlands
- ✅ EPA TRI (Toxic Release Inventory)

### Good Coverage (80-95% accuracy)
- 🟡 EPA FRS (all programs) — some duplicate filtering
- 🟡 OSM receptors (schools, hospitals) — may miss private/small facilities
- 🟡 USGS NWIS water — precision-dependent queries
- 🟡 EPA Radon — estimated by latitude, not precise mapping

### Limited Coverage (50-80% accuracy)
- ⚠️ EPA RMP (Risk Management Plan) — updated annually
- ⚠️ EPA Brownfields — state-level data quality varies
- ⚠️ EPA STATSGO — coarser resolution than SSURGO

### Known Gaps
- ❌ USGS MRDS (mines) — endpoint returning 404
- ❌ OSM Overpass amenities — 406 errors on some queries
- ❌ FEMA NFHL — occasional TLS timeout

---

## Response Time & Performance

### Typical Query Execution
| Phase | Time | Details |
|-------|------|---------|
| Request dispatch | <100ms | API gateway |
| EPA ECHO queries | 8-12s | 4 concurrent requests |
| EPA Envirofacts | 15-20s | 12 table queries |
| EPA FRS queries | 5-8s | State-based searches |
| EPA TRI query | 2-4s | County + state fallback |
| USGS queries | 5-8s | Water sites, earthquakes |
| USDA queries | 3-5s | SSURGO, STATSGO |
| OSM Overpass | 3-10s | 12 concurrent amenity queries |
| FEMA query | 2-8s | MapServer query |
| Deduplication | 1-2s | Record merging/filtering |
| Response formatting | <100ms | JSON serialization |
| **Total** | **30-45s** | Full query execution |

### Optimization Tips
1. **Cache results** — Results valid for 24 hours unless data updates
2. **Narrow radius** — 500m queries execute in ~20 seconds
3. **Filter server-side** — Use `jq` to filter results after fetch
4. **Batch queries** — Don't request same coordinate twice within 1 hour

---

## Filtering Examples

### Get All Contamination Sites
```bash
curl "..." | jq '.results | map(select(.category == "contamination"))'
```

### Filter by Distance (< 500m)
```bash
curl "..." | jq '.results | map(select(.distance_m < 500))'
```

### Get Specific Database
```bash
curl "..." | jq '.results | map(select(.database == "RCRA"))'
```

### Count by Category
```bash
curl "..." | jq '.summary.by_category'
```

### Get Top 10 Nearest Sites
```bash
curl "..." | jq '.results | sort_by(.distance_m) | .[0:10]'
```

---

## Integration Examples

### JavaScript/Node.js
```javascript
const axios = require('axios');

async function getNearby(lat, lng, radius = 3000) {
  const url = `https://api.geoscopesolutions.com/api/nearby-search`;
  const response = await axios.get(url, {
    params: { lat, lng, radius_m: radius }
  });
  return response.data;
}

// Usage
const results = await getNearby(42.3601, -71.0589, 3000);
console.log(`Found ${results.summary.total} records`);
console.log('By category:', results.summary.by_category);
```

### Python
```python
import requests

def get_nearby(lat, lng, radius=3000):
    url = "https://api.geoscopesolutions.com/api/nearby-search"
    params = {"lat": lat, "lng": lng, "radius_m": radius}
    response = requests.get(url, params=params)
    return response.json()

# Usage
results = get_nearby(42.3601, -71.0589, 3000)
print(f"Found {results['summary']['total']} records")
print(f"Contamination: {results['summary']['by_category'].get('contamination', 0)}")
```

### cURL
```bash
# Single query
curl -s "https://api.geoscopesolutions.com/api/nearby-search?lat=42.3601&lng=-71.0589&radius_m=3000" \
  | jq '.summary'

# Save full results to file
curl -s "https://api.geoscopesolutions.com/api/nearby-search?lat=42.3601&lng=-71.0589&radius_m=3000" \
  > results.json

# Filter contamination sites
curl -s "..." | jq '.results | map(select(.category == "contamination")) | length'
```

---

## Support & Documentation

- **API Status:** https://api.geoscopesolutions.com/api/core/health
- **Frontend:** https://geoscopesolutions.com
- **Documentation:** 
  - [DATASETS_120_FEDERAL_MAPPING.md](./DATASETS_120_FEDERAL_MAPPING.md) — Complete dataset inventory
  - [IMPLEMENTATION_SUMMARY_120_DATASETS.md](./IMPLEMENTATION_SUMMARY_120_DATASETS.md) — Technical details

---

## Troubleshooting

### Slow Responses (>60 seconds)
- Check if Vercel function is in "cold start" (first request after 15 min idle)
- Reduce radius_m to <3000 meters
- Retry after 30 seconds

### Empty Results
- Verify lat/lng are valid coordinates in USA
- Expand radius_m to ≥1000 meters
- Check if coordinate is in remote area with minimal data

### Partial Results (some categories missing)
- Some EPA services timeout; results are partial but valid
- Retry query 2-3 times for full results
- Check [DATASETS_120_FEDERAL_MAPPING.md](./DATASETS_120_FEDERAL_MAPPING.md) for known issues

### 404 Errors
- API alias may be down; use backup URL: `https://geoscope-3diy67clz-stephenbantons-projects.vercel.app`
- Check DNS propagation

---

## Rate Limits & Availability

- **Requests per minute:** Unlimited (Vercel serverless)
- **Concurrent requests:** Up to 10 per user (courtesy limit)
- **Timeout:** 60 seconds per request
- **Uptime SLA:** 99.95% (Vercel guarantee)
- **Data freshness:** 24 hours (EPA daily batch updates)

