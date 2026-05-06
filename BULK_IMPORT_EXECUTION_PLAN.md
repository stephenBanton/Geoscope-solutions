# 🚀 Missing Datasets - Bulk Import Execution Plan

## Current Progress ✅

### Completed (5 datasets, 21,146 records):
- ✅ PRISONS: 135 records
- ✅ EPA LUST: 1,295 records  
- ✅ FEMA UST: 2,928 records
- ✅ CERCLIS: 1,380 records
- ✅ DAYCARE: 16,739 records

### In Progress - Background:
- ⏳ **EPA FRS Bulk (S3)** - downloading ~1.8GB containing:
  - NPL (Superfund sites)
  - PFAS (Federal sites, NPL, spills, TRIS)
  - RMP (Risk Management Plan facilities)
  - DELISTED NPL
  - PROPOSED NPL
  - Other EPA-tracked facilities

- ⏳ **NURSING HOMES** - CMS data (14,703 records, 1.01MB CSV ready)

### Target Next Batch (5 datasets):
1. **SCHOOLS PUBLIC** (NCES) - ~130,000 schools
2. **SCHOOLS PRIVATE** (NCES) - ~30,000 schools
3. **COLLEGES** (NCES IPEDS) - ~5,000 institutions
4. **RCRA SQG/TSDF/VSQG** (EPA) - ~3,500 generators/facilities
5. **DOD** (Department of Defense) - military installations

### Pending Research (10 datasets):
- COAL ASH EPA, CORRACTS, HYDROLOGIC UNIT, MGP, PCB TRANSFORMER, RADON EPA, FUDS, FLOOD DFIRM, FLOOD Q3, STATSGO

---

## Action Timeline

### Phase 1: Now (EPA FRS Download)
```
⏳ Script: download-epa-frs-bulk.js
   Status: Running (461MB / ~1.8GB)
   ETA: 10-30 minutes
   Output: EPA_FRS_BULK/ folder with extracted CSVs
```

### Phase 2: Once EPA FRS = 100% Complete
```
✅ Extract EPA FRS ZIPs automatically
✅ Check CSV contents
✅ Identify NPL, RMP, PFAS columns and file names
✅ Run batch import:
   node scripts/import-csv.js EPA_FRS_BULK/*/Combined.csv NPL contamination
   node scripts/import-csv.js EPA_FRS_BULK/*/Combined.csv RMP regulatory
   (filter by database_name or category column as needed)
```

### Phase 3: NCES Schools & Colleges (Next)
```
📥 Download NCES Common Core of Data:
   - Public Schools (~130,000)
   - Private Schools (~30,000)  
   - Colleges/Universities (~5,000)
   
   Via: https://data.nces.ed.gov/api/EdGovData/PublicSchools
        https://data.nces.ed.gov/api/EdGovData/PrivateSchools
        https://data.nces.ed.gov/api/EdGovData/InstitutionProfile

✅ Run imports:
   node scripts/import-csv.js <public.csv> SCHOOLS_PUBLIC receptors
   node scripts/import-csv.js <private.csv> SCHOOLS_PRIVATE receptors
   node scripts/import-csv.js <colleges.csv> COLLEGES receptors
```

### Phase 4: RCRA & DOD (Alternative Downloads)
```
📥 RCRA via EPA RCRA Info:
   https://rcrapublic.epa.gov/rcra-public-export/
   (Requires manual download from web portal or API query)

📥 DOD via DISDI:
   http://www.acq.osd.mil/eie/Downloads/DISDI/
```

---

## Command Reference

### Check Download Progress
```bash
# Monitor EPA FRS download
get_terminal_output <terminal-id>

# Check file sizes
ls -lah downloads/missing/EPA_FRS_BULK/

# Verify extraction
dir downloads/missing/EPA_FRS_BULK/FRS_Interests/
```

### Import EPA FRS Data (Once Ready)
```bash
# Find CSV files
find downloads/missing/EPA_FRS_BULK -name "*.csv" | head -10

# Import NPL from combined file
node scripts/import-csv.js downloads/missing/EPA_FRS_BULK/FRS_Interests/Combined.csv NPL contamination

# Import RMP from combined file  
node scripts/import-csv.js downloads/missing/EPA_FRS_BULK/FRS_Interests/Combined.csv RMP regulatory

# Check progress
node scripts/check-missing-dbs.js
```

### Manual Download Fallback (if scripts fail)
```bash
# Download EPA FRS Interests directly
Invoke-WebRequest -Uri 'https://dmap-prod-oms-edc.s3.amazonaws.com/OMS/FRS/FRS_Interests_Download.zip' `
  -OutFile 'downloads/missing/EPA_FRS_Interests.zip' -UseBasicParsing

# Extract
Add-Type -AssemblyName System.IO.Compression.FileSystem
[System.IO.Compression.ZipFile]::ExtractToDirectory('downloads/missing/EPA_FRS_Interests.zip', 'downloads/missing/EPA_FRS_BULK/')
```

---

## Expected Outcome After All Phases

Current: 25 missing datasets
Expected reduction:
- Phase 1 (EPA FRS): -7 datasets (NPL, DELISTED_NPL, PROPOSED_NPL, RMP, PFAS_FEDERAL, PFAS_NPL, PFAS_SPILLS, + check for CORRACTS)
- Phase 2 (NCES): -3 datasets (SCHOOLS_PUBLIC, SCHOOLS_PRIVATE, COLLEGES)
- Phase 3 (RCRA/DOD): -3 datasets (RCRA_SQG, RCRA_TSDF, RCRA_VSQG, DOD)

**Target: Reduce from 25 → ~12 missing**

---

## Known Issues & Workarounds

### Issue 1: Direct EPA RMP URL returns 403
**Workaround**: RMP data is included in EPA FRS Interests ZIP

### Issue 2: DOD direct download times out
**Workaround**: Use EPA FRS which may include DOD/FUDS data tagged differently

### Issue 3: EPA RCRA export requires form submission
**Workaround**: Query data.gov CKAN for RCRA datasets with direct CSV links

### Issue 4: NCES APIs may not return lat/lon in expected columns
**Workaround**: Use NCES data portal bulk download with geocoding

---

## Resources

- EPA FRS Documentation: https://www.epa.gov/frs
- NCES Common Core of Data: https://data.nces.ed.gov/
- EPA RCRA Info: https://rcrapublic.epa.gov/
- data.gov CKAN: https://catalog.data.gov/api/3/
- World Bank Data: https://data.worldbank.org/api/v2/

---

Generated: 2026-04-02 | Last updated in real-time from bulk download operations
