// Missing database source configuration.
// Add trusted download URLs under each database key.
// urls can point to .csv, .zip, or .gz files that contain CSV.

module.exports = {
  'ASBESTOS NOA': {
    category: 'geology',
    localRegex: ['asbestos'],
    urls: [],
  },
  'BROWNFIELDS': {
    category: 'contamination',
    localRegex: ['acres', 'brownfield'],
    urls: [],
  },
  'CERCLIS': {
    category: 'contamination',
    localRegex: ['cerclis', 'superfund'],
    urls: [],
  },
  'COAL ASH EPA': {
    category: 'geology',
    localRegex: ['coal.*ash'],
    urls: [],
  },
  'CORRACTS': {
    category: 'contamination',
    localRegex: ['corracts'],
    urls: [],
  },
  'DELISTED NPL': {
    category: 'contamination',
    localRegex: ['delist.*npl', 'npl.*delist'],
    urls: [],
  },
  'ECHO': {
    category: 'regulatory',
    localRegex: ['echo'],
    urls: [],
  },
  'EPA LUST': {
    category: 'contamination',
    localRegex: ['lust'],
    urls: [],
  },
  'EPA UST': {
    category: 'contamination',
    localRegex: ['\\bust\\b'],
    urls: [],
  },
  'FEDERAL FACILITY': {
    category: 'regulatory',
    localRegex: ['federal.*facilit', 'frs'],
    urls: [],
  },
  'FLOOD DFIRM': {
    category: 'hydrology',
    localRegex: ['dfirm', 'flood'],
    urls: [],
  },
  'FLOOD Q3': {
    category: 'hydrology',
    localRegex: ['q3.*flood', 'flood.*q3'],
    urls: [],
  },
  'ICIS': {
    category: 'regulatory',
    localRegex: ['icis'],
    urls: [],
  },
  'MINE OPERATIONS': {
    category: 'geology',
    localRegex: ['mine.*oper', 'oper.*mine', 'mineplant'],
    urls: [],
  },
  'NPL': {
    category: 'contamination',
    localRegex: ['\\bnpl\\b', 'superfund', 'ef_npl'],
    urls: [],
  },
  'PFAS FEDERAL SITES': {
    category: 'contamination',
    localRegex: ['pfas.*federal', 'federal.*pfas'],
    urls: [],
  },
  'PFAS NPL': {
    category: 'contamination',
    localRegex: ['pfas.*npl', 'npl.*pfas'],
    urls: [],
  },
  'PFAS SPILLS': {
    category: 'contamination',
    localRegex: ['pfas.*spill', 'spill.*pfas'],
    urls: [],
  },
  'PFAS TRIS': {
    category: 'contamination',
    localRegex: ['pfas.*tri', 'tri.*pfas'],
    urls: [],
  },
  'PROPOSED NPL': {
    category: 'contamination',
    localRegex: ['proposed.*npl', 'npl.*proposed'],
    urls: [],
  },
  'RCRA SQG': {
    category: 'contamination',
    localRegex: ['rcra.*sqg', 'sqg'],
    urls: [],
  },
  'RCRA TSDF': {
    category: 'contamination',
    localRegex: ['rcra.*tsdf', 'tsdf'],
    urls: [],
  },
  'RCRA VSQG': {
    category: 'contamination',
    localRegex: ['rcra.*vsqg', 'vsqg', 'rcra.*cesqg'],
    urls: [],
  },
  'RMP': {
    category: 'regulatory',
    localRegex: ['\\brmp\\b'],
    urls: [],
  },
  'SSURGO': {
    category: 'geology',
    localRegex: ['ssurgo', 'soil'],
    urls: [],
  },
  'STATSGO': {
    category: 'geology',
    localRegex: ['statsgo'],
    urls: [],
  },
  'STORMWATER': {
    category: 'hydrology',
    localRegex: ['stormwater', 'hydrowaste'],
    urls: [],
  },
  'TRIS': {
    category: 'contamination',
    localRegex: ['\\btri\\b', 'tris', 'ef_tri'],
    urls: [],
  },
  'WETLANDS NWI': {
    category: 'hydrology',
    localRegex: ['wetland', '\\bnwi\\b'],
    urls: [],
  },
};
