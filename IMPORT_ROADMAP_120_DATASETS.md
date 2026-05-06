# GeoScope 120+ Environmental Datasets — Import Roadmap

**Status:** 🔄 In Progress  
**Last Updated:** April 28, 2026  
**Target:** Complete 50-state coverage across ALL 120+ datasets

---

## Current Import Progress

### ✅ Phase 1: EPA Bulk Federal Data (In Progress - PID 9253fc8f)
**Duration:** 2-8 hours  
**Expected Records:** 10M-15M+

- [x] ECHO Exporter (1.5M facilities) — NOW PROCESSING
- [ ] FRS Program Facilities (4.7M facilities)
- [ ] RCRA Sites (500K)
- [ ] ICIS-Air Facilities (300K)
- [ ] Air Emissions Data
- [ ] NPDES Permits
- [ ] SDWA Water Systems
- [ ] Enforcement Actions
- [ ] TRI Facilities
- [ ] Superfund Sites
- [ ] Brownfields
- [ ] UST Facilities

---

## Phase 2: EPA Envirofacts Tables by State (Queued)

### Tier 1A: Core Environmental Programs (50 datasets)

**Via EPA Envirofacts REST API** - Query by FIPS state code:

| # | Dataset | Table Name | Records Est. | API |
|---|---------|------------|--------------|-----|
| 1 | CERCLIS NFRAP | CERCLIS_NFRAP | 40K | EPA Envirofacts |
| 2 | NPL Final Sites | SEMS_8R_FINAL_NPL | 1.3K | EPA Envirofacts |
| 3 | NPL Proposed | SEMS_8R_PROPOSED_NPL | 2K | EPA Envirofacts |
| 4 | NPL Delisted | SEMS_8R_DELETED_NPL | 900 | EPA Envirofacts |
| 5 | PCS Facilities | PCS_FACILITY_INTEREST | 45K | EPA Envirofacts |
| 6 | PWS Systems | PWS_FACILITY_INTEREST | 52K | EPA Envirofacts |
| 7 | ICIS Facilities | ICIS_FACILITY_INTEREST | 35K | EPA Envirofacts |
| 8 | UST Facilities | UST_FACILITY_INTEREST | 120K | EPA Envirofacts |
| 9 | LUST Sites | LUST_FACILITY_INTEREST | 280K | EPA Envirofacts |
| 10 | Hazmat Manifest | MANIFEST_HANDLER_SITES | 85K | EPA Envirofacts |
| 11 | AIRS Facilities | AIRS_FACILITY | 125K | EPA Envirofacts |
| 12 | Brownfields | BROWN_FIELD_SITES | 28K | EPA Envirofacts |
| 13 | RCRA LQG | RCRA_LQG_FACILITY | 95K | EPA Envirofacts |
| 14 | RCRA TSDF | RCRA_TSDF_FACILITY | 8K | EPA Envirofacts |
| 15 | RCRA SQG | RCRA_SQG_FACILITY | 480K | EPA Envirofacts |
| 16 | RCRA NONGEN | RCRA_NONGEN_FACILITY | 22K | EPA Envirofacts |
| 17 | TRI Facilities | TRI_FACILITY | 30K | EPA Envirofacts |
| 18 | RMP Facilities | RMP_FACILITY | 14K | EPA Envirofacts |
| 19 | Enforcement Actions | ENF_ACTION | 1.2M | EPA Envirofacts |
| 20 | Air Compliance | AIR_POLLUTANT_EMISSION | 450K | EPA Envirofacts |

**Query Pattern:**
```
https://data.epa.gov/efservice/{TABLE_NAME}/STATE_CODE/{FIPS_CODE}/JSON
```

**Script:** `scripts/import-120-datasets.js`  
**Interval:** Sequential by state (AL → WY)  
**Est. Records:** 3M-5M records across 50 states

---

## Phase 3: USGS Data Sources (Queued)

### Tier 2: Hydrology & Water Resources (15+ datasets)

| # | Dataset | Endpoint | Coverage |
|---|---------|----------|----------|
| 21 | NWIS Water Sites | waterservices.usgs.gov/nwis/site | All states |
| 22 | Water Quality | waterservices.usgs.gov/nwis/qw | All states |
| 23 | Streamflow Data | waterservices.usgs.gov/nwis/iv | Rivers/streams |
| 24 | Earthquakes (1yr) | earthquake.usgs.gov/fdsnws/event | Seismic zones |
| 25 | Hydrologic Units | USGS WMS | Watershed boundaries |

**Query:** Bounding box by state  
**Est. Records:** 500K-1M

---

## Phase 4: USDA Soil & Geology (Queued)

### Tier 3: Soil & Geology (10+ datasets)

| # | Dataset | Endpoint | Coverage |
|---|---------|----------|----------|
| 26 | SSURGO Soil | sdmdataaccess.sc.egov.usda.gov | All counties |
| 27 | STATSGO | USDA SDA tabular | All states |
| 28 | Radon Zones | EPA-mapped | All states |

**Est. Records:** 800K-1M map units

---

## Phase 5: Receptors & Infrastructure (Queued)

### Tier 4: People & Places (15+ datasets)

**Via OpenStreetMap Overpass API** - Query by state bounding boxes:

| # | Dataset | OSM Tag | Est. Records |
|---|---------|---------|--------------|
| 29 | Public Schools | amenity=school | 130K |
| 30 | Private Schools | building=school | 45K |
| 31 | Hospitals | amenity=hospital | 6.5K |
| 32 | Clinics/Doctors | amenity=clinic | 12K |
| 33 | Daycare Centers | amenity=kindergarten | 25K |
| 34 | Colleges/Universities | amenity=university | 4.2K |
| 35 | Churches | amenity=place_of_worship | 320K |
| 36 | Prisons | amenity=prison | 1.8K |
| 37 | Nursing Homes | amenity=nursing_home | 28K |
| 38 | Arenas/Sports | amenity=stadium | 12K |
| 39 | Airports | amenity=airport | 5.2K |
| 40 | Farms | landuse=farmland | 2M+ |

**Query:** Overpass API with state-level bbox  
**Est. Records:** 2.5M+

---

## Phase 6: Specialized Datasets (Queued)

### Environmental & Hazmat
- Coal Ash Sites (EPA + DOE)
- PFAS Contamination Sites
- Vapor Intrusion Sites
- Dry Cleaning Sites (SCRD)
- Lead Smelter Historic Sites
- PCB Transformer Locations
- Asbestos Registry Sites

### Infrastructure
- DOT Hazmat Pipelines
- OSHA Inspection Records
- Federal Land Sites (DOI/DOD)
- Tribal Lands & Facilities

---

## Import Query Patterns by API

### EPA Envirofacts (State-based)
```bash
for STATE in AL AK AZ ... WY; do
  FIPS=$(get_fips_code $STATE)
  for TABLE in CERCLIS_NFRAP PCS_FACILITY_INTEREST PWS_FACILITY_INTEREST ...; do
    curl "https://data.epa.gov/efservice/${TABLE}/STATE_CODE/${FIPS}/JSON"
  done
done
```

### USGS Water Services (Bounding box)
```bash
# Boston example
curl "https://waterservices.usgs.gov/nwis/site?bBox=-74.0,40.0,-71.0,43.0&format=json"
```

### OpenStreetMap Overpass (State bbox)
```bash
curl "https://overpass-api.de/api/interpreter?data=[bbox:40,−74,43,−71];node[amenity=school];out;"
```

---

## Database Schema (environmental_sites)

```sql
CREATE TABLE environmental_sites (
  id BIGSERIAL PRIMARY KEY,
  database_name VARCHAR(255) NOT NULL,      -- "CERCLIS", "NPDES", "SCHOOLS", etc.
  category VARCHAR(100),                     -- "contamination", "receptors", "hydrology", etc.
  class_code VARCHAR(50),                    -- Program classification (LQG, TSDF, etc.)
  priority_tier INT,                         -- 1-7 (Tier 1 = highest priority)
  priority_score INT,                        -- 0-100
  site_name VARCHAR(500),
  address VARCHAR(500),
  city VARCHAR(200),
  state VARCHAR(5) NOT NULL,                 -- Normalized 2-letter code (AL, AK, AZ...)
  zip VARCHAR(20),
  status VARCHAR(255),
  registry_id VARCHAR(255) UNIQUE,           -- Unique identifier from source
  source_id VARCHAR(255) UNIQUE NOT NULL,    -- Composite key for dedup
  source_org VARCHAR(255),                   -- "EPA", "USGS", "USDA", "OSM", etc.
  location GEOMETRY(Point, 4326),            -- PostGIS point
  attributes JSONB,                          -- Flexible data storage
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_state ON environmental_sites(state);
CREATE INDEX idx_category ON environmental_sites(category);
CREATE INDEX idx_database ON environmental_sites(database_name);
CREATE INDEX idx_location ON environmental_sites USING GIST(location);
```

---

## Expected Final Database Size

| Phase | Dataset Count | Records Est. | Total Cumulative |
|-------|---------------|--------------|-----------------|
| Current | EPA Bulk | 10-15M | **23.3M** |
| Phase 2 | EPA Envirofacts | 3-5M | **26-28M** |
| Phase 3 | USGS | 500K-1M | **27-29M** |
| Phase 4 | USDA | 800K-1M | **28-30M** |
| Phase 5 | OSM Receptors | 2.5M | **30-32M** |
| Phase 6 | Specialized | 500K-2M | **31-34M** |
| **FINAL** | **120+ Datasets** | **~7-12M net new** | **30-35M Total** |

---

## Quality Assurance Checklist

- [ ] State coverage verified (all 50 states present in each dataset)
- [ ] Duplicate detection (source_id deduplication)
- [ ] Location validation (valid lat/lng for 95%+ of records)
- [ ] Address normalization
- [ ] Category tagging completeness
- [ ] API rate limiting compliance
- [ ] Failed record logging & retry logic
- [ ] Performance optimization (batch insert tuning)

---

## Deployment Notes

**Start Phase 2+:**
```bash
cd /c/Users/Admin/Desktop/WEBSITE/geoscope
node scripts/import-120-datasets.js --start-phase=2 --max-workers=8
```

**Monitor Progress:**
```bash
SELECT COUNT(*) as total, 
       COUNT(DISTINCT state) as states_covered,
       COUNT(DISTINCT database_name) as datasets
FROM environmental_sites;
```

**Expected Runtime:** 24-48 hours for complete import (including API rate limits)

---

## Next Steps

1. ✅ Normalize state codes (COMPLETED)
2. 🔄 Run Phase 1: Federal bulk download (IN PROGRESS)
3. ⏳ Queue Phase 2: EPA Envirofacts by state
4. ⏳ Queue Phase 3-6: USGS/USDA/OSM/Specialized
5. ⏳ Verify 50-state coverage
6. ⏳ Deploy to production with `all-120-datasets` flag
7. ⏳ Generate comprehensive report

