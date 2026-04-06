# 📊 Missing Datasets - Bulk Download Status

## 🚀 Current Operations (April 2, 2026 - Real Time)

### Active Downloads

#### 1. **EPA FRS Bulk (Primary Operation)**
- **Status**: ⏳ IN PROGRESS
- **Progress**: 686MB of ~1.8GB (37%)
- **Elapsed**: 218 seconds (~3.6 minutes)
- **ETA**: ~3-4 more minutes
- **Terminal**: aad14fd1-c601-430d-ac38-4a1255bea942
- **Contains**:
  - NPL (Superfund sites)
  - DELISTED NPL
  - PROPOSED NPL
  - RMP (Risk Management Plan facilities)
  - PFAS (Federal sites + NPL + spills contamination)
  - CORRACTS (Corrective actions)
  - Other EPA-tracked facilities with lat/lon

#### 2. **Nursing Homes (CMS)**
- **Status**: ✅ DOWNLOAD COMPLETE
- **Records**: ~14,703 facilities
- **File**: cms_nh_facilities.csv (1.01MB)
- **Location**: downloads/missing/NURSING_HOMES/
- **Next**: Ready for import

---

## 📋 Action Plan - Once EPA FRS Download = 100%

```bash
# Step 1: Auto-extract and identify datasets
node scripts/import-epa-frs-bulk.js

# Step 2: Check database status
node scripts/check-missing-dbs.js

# Step 3: Start NCES schools downloads
node scripts/fetch-nces-schools.js public
node scripts/fetch-nces-schools.js private
```

---

## 📈 Expected Impact

### Datasets Covered by EPA FRS Interests ZIP:

| Database | Status | Records | Source |
|----------|--------|---------|--------|
| NPL | Will import | ~1,300+ | EPA Superfund |
| DELISTED NPL | Will import | Included | EPA Superfund |
| PROPOSED NPL | Will import | Included | EPA Superfund |
| RMP | Will import | ~12,000+ | EPA Risk Mgmt |
| PFAS FEDERAL SITES | Will import | Included | EPA PFAS registry |
| PFAS NPL | Will import | Included | EPA Superfund |
| PFAS SPILLS | Will import | Included | EPA spills/releases |
| CORRACTS | Will import | Included | EPA RCRA |
| (Others) | Possible | See file | EPA FRS interests |

**Expected Reduction**: 25 missing → ~15-18 missing after EPA FRS imports

---

## 🎯 Secondary Batch (Starting After EPA FRS)

### NCES Schools & Colleges
- **Source**: https://data.nces.ed.gov/ (Common Core of Data)
- **Datasets**:
  - SCHOOLS PUBLIC (~130,000 schools)
  - SCHOOLS PRIVATE (~30,000 schools)
  - COLLEGES (~5,000 institutions)
- **Status**: Scripts ready, will start after EPA FRS
- **Impact**: Reduce missing by 3 more

### RCRA (SQG/TSDF/VSQG)
- **Source**: https://rcrapublic.epa.gov/rcra-public-export/
- **Datasets**: RCRA SQG, RCRA TSDF, RCRA VSQG
- **Status**: API endpoints identified, download ready
- **Impact**: Reduce missing by 3 more

---

## 📊 Cumulative Progress Tracking

### Current (Baseline):
- Started at: **30 missing**
- Current: **25 missing** (5 already imported)
  - ✅ PRISONS: 135
  - ✅ EPA LUST: 1,295
  - ✅ FEMA UST: 2,928
  - ✅ CERCLIS: 1,380
  - ✅ DAYCARE: 16,739

### After EPA FRS (~8 datasets):
- **Expected: 17-20 missing**

### After NCES + RCRA (~6 datasets):
- **Expected: 11-14 missing**

### Remaining (Harder sources):
- Coal Ash EPA, DOD, FLOOD DFIRM/Q3, FUDS, HYDROLOGIC UNIT, MGP, PCB TRANSFORMER, RADON EPA, STATSGO

---

## ⏱️ Timeline

| Phase | Duration | Datasets | Start | Status |
|-------|----------|----------|-------|--------|
| **Phase 1** | 5-10 min | EPA FRS (8 DBs) | Now | ⏳ EPA download 37% done |
| **Phase 2** | 10-15 min | Extract + Import | ~5 min | 📋 Ready |
| **Phase 3** | 10-20 min | NCES (3 DBs) | ~20 min | 📋 Ready |
| **Phase 4** | 5-10 min | RCRA (3 DBs) | ~35 min | 📋 Prepared |
| **Phase 5** | ~30 min | Remaining (5 DBs) | ~45 min | 📋 In research |

**Total estimated time to 80% complete: ~45-60 minutes**

---

## 🔧 Commands to Monitor

### Live Progress
```bash
# Check EPA FRS download
get_terminal_output aad14fd1-c601-430d-ac38-4a1255bea942

# Other operations (if spawned)
get_terminal_output <terminal-id>
```

### Once Download Complete
```bash
# Verify extraction
dir /s downloads/missing/EPA_FRS_BULK/

# Import EPA data
node scripts/import-epa-frs-bulk.js

# Check improved count
node scripts/check-missing-dbs.js
```

### Manually kickstart next phases
```bash
# NCES Schools
node scripts/fetch-nces-schools.js public
node scripts/fetch-nces-schools.js private

# RCRA data (if CMS NHS already imported)
# ... (check for manual submission support)
```

---

## 📝 Notes

1. **EPA FRS ZIP** is the "hub" - contains many datasets tagged internally
2. **Column matching** in import scripts will auto-identify which records map to which database
3. **Lat/Lon coverage**: ~95%+ for most EPA data
4. **Deduplication**: PostgreSQL ON CONFLICT on `source_id` prevents duplicates
5. **World Bank**: Confirmed NOT useful (country-level macro data only)

---

## ✅ Checklist

- [x] Identified high-quality federal data sources
- [x] Started EPA FRS bulk download (1.8GB)
- [x] Nursing homes CSV downloaded and verified
- [x] Created auto-import scripts for EPA FRS
- [x] Created NCES download scripts
- [x] Documented execution plan
- [ ] EPA FRS download 100%
- [ ] EPA FRS extraction complete
- [ ] EPA FRS imports run
- [ ] NCES schools downloaded
- [ ] NCES imports run
- [ ] Final missing count check

---

**Status Updated**: April 2, 2026 - 18:45 UTC
**Last Checked**: EPA FRS at 37% (686MB of 1.8GB)
**Estimated Completion**: April 2, 2026 - 18:50-19:15 UTC
