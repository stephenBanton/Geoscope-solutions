// Missing database source configuration.
// Add trusted download URLs under each database key.
// urls can point to .csv, .zip, or .gz files that contain CSV.

module.exports = {
  'AFS AIRPORT FACILITIES': {
    category: 'regulatory',
    localRegex: ['afs', 'air.*facility', 'airport.*facilit', 'eis'],
    urls: ['https://www.epa.gov/air-emissions-inventories'],
  },
  'AIR FACILITY': {
    category: 'regulatory',
    localRegex: ['air.*facility', 'afs', 'eis', 'air.*emissions'],
    urls: ['https://www.epa.gov/air-emissions-inventories'],
  },
  'ASBESTOS NOA': {
    category: 'geology',
    localRegex: ['asbestos'],
    urls: [],
  },
  'BROWNFIELDS': {
    category: 'contamination',
    localRegex: ['acres', 'brownfield'],
    urls: ['https://www.epa.gov/brownfields'],
  },
  'BROWNFIELDS ACRES': {
    category: 'contamination',
    localRegex: ['acres', 'brownfield', 'brownfields'],
    urls: ['https://www.epa.gov/brownfields'],
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
    urls: ['https://www.epa.gov/superfund'],
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
  'FED BROWNFIELDS': {
    category: 'contamination',
    localRegex: ['fed.*brownfield', 'brownfield'],
    urls: ['https://www.epa.gov/brownfields'],
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
  'HAZMAT INCIDENTS DOT': {
    category: 'regulatory',
    localRegex: ['hmirs', 'hazmat', 'dot.*incident'],
    urls: ['https://hazmatonline.phmsa.dot.gov/'],
  },
  'HIST AFS': {
    category: 'regulatory',
    localRegex: ['hist.*afs', 'afs', 'air.*facility', 'eis'],
    urls: ['https://www.epa.gov/air-emissions-inventories'],
  },
  'HIST AFS 2': {
    category: 'regulatory',
    localRegex: ['hist.*afs', 'afs', 'air.*facility', 'eis'],
    urls: ['https://www.epa.gov/air-emissions-inventories'],
  },
  'HMIRS (DOT)': {
    category: 'regulatory',
    localRegex: ['hmirs', 'hazmat', 'hazardous.*material.*incident'],
    urls: ['https://hazmatonline.phmsa.dot.gov/'],
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
    urls: [
      'https://www.epa.gov/superfund',
      'https://services.arcgis.com/cJ9YHowT8TU7DUyn/arcgis/rest/services/FAC_Superfund_Site_Boundaries_EPA_/FeatureServer/0/query?where=1%3D1&outFields=*&f=json'
    ],
  },
  'PFAS FEDERAL SITES': {
    category: 'contamination',
    localRegex: ['pfas.*federal', 'federal.*pfas'],
    urls: ['https://www.epa.gov/pfas'],
  },
  'PFAS INDUSTRY': {
    category: 'contamination',
    localRegex: ['pfas.*industry', 'industry.*pfas'],
    urls: ['https://www.epa.gov/pfas'],
  },
  'PFAS MANIFEST': {
    category: 'contamination',
    localRegex: ['pfas.*manifest', 'manifest.*pfas'],
    urls: ['https://www.epa.gov/pfas'],
  },
  'PFAS NPL': {
    category: 'contamination',
    localRegex: ['pfas.*npl', 'npl.*pfas'],
    urls: ['https://www.epa.gov/pfas'],
  },
  'PFAS PROD': {
    category: 'contamination',
    localRegex: ['pfas.*prod', 'production.*pfas'],
    urls: ['https://www.epa.gov/pfas'],
  },
  'PFAS SPILLS': {
    category: 'contamination',
    localRegex: ['pfas.*spill', 'spill.*pfas'],
    urls: ['https://www.epa.gov/pfas'],
  },
  'PFAS TRIS': {
    category: 'contamination',
    localRegex: ['pfas.*tri', 'tri.*pfas'],
    urls: ['https://www.epa.gov/pfas'],
  },
  'PFAS WQP': {
    category: 'contamination',
    localRegex: ['pfas.*wqp', 'water.*quality.*pfas'],
    urls: ['https://www.epa.gov/pfas'],
  },
  'PROPOSED NPL': {
    category: 'contamination',
    localRegex: ['proposed.*npl', 'npl.*proposed'],
    urls: ['https://www.epa.gov/superfund'],
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
    urls: ['https://websoilsurvey.nrcs.usda.gov/'],
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
