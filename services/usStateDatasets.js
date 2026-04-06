'use strict';

const US_STATES = [
  { code: 'AL', name: 'Alabama' },
  { code: 'AK', name: 'Alaska' },
  { code: 'AZ', name: 'Arizona' },
  { code: 'AR', name: 'Arkansas' },
  { code: 'CA', name: 'California' },
  { code: 'CO', name: 'Colorado' },
  { code: 'CT', name: 'Connecticut' },
  { code: 'DE', name: 'Delaware' },
  { code: 'FL', name: 'Florida' },
  { code: 'GA', name: 'Georgia' },
  { code: 'HI', name: 'Hawaii' },
  { code: 'ID', name: 'Idaho' },
  { code: 'IL', name: 'Illinois' },
  { code: 'IN', name: 'Indiana' },
  { code: 'IA', name: 'Iowa' },
  { code: 'KS', name: 'Kansas' },
  { code: 'KY', name: 'Kentucky' },
  { code: 'LA', name: 'Louisiana' },
  { code: 'ME', name: 'Maine' },
  { code: 'MD', name: 'Maryland' },
  { code: 'MA', name: 'Massachusetts' },
  { code: 'MI', name: 'Michigan' },
  { code: 'MN', name: 'Minnesota' },
  { code: 'MS', name: 'Mississippi' },
  { code: 'MO', name: 'Missouri' },
  { code: 'MT', name: 'Montana' },
  { code: 'NE', name: 'Nebraska' },
  { code: 'NV', name: 'Nevada' },
  { code: 'NH', name: 'New Hampshire' },
  { code: 'NJ', name: 'New Jersey' },
  { code: 'NM', name: 'New Mexico' },
  { code: 'NY', name: 'New York' },
  { code: 'NC', name: 'North Carolina' },
  { code: 'ND', name: 'North Dakota' },
  { code: 'OH', name: 'Ohio' },
  { code: 'OK', name: 'Oklahoma' },
  { code: 'OR', name: 'Oregon' },
  { code: 'PA', name: 'Pennsylvania' },
  { code: 'RI', name: 'Rhode Island' },
  { code: 'SC', name: 'South Carolina' },
  { code: 'SD', name: 'South Dakota' },
  { code: 'TN', name: 'Tennessee' },
  { code: 'TX', name: 'Texas' },
  { code: 'UT', name: 'Utah' },
  { code: 'VT', name: 'Vermont' },
  { code: 'VA', name: 'Virginia' },
  { code: 'WA', name: 'Washington' },
  { code: 'WV', name: 'West Virginia' },
  { code: 'WI', name: 'Wisconsin' },
  { code: 'WY', name: 'Wyoming' }
];

const STATE_DATASET_BLUEPRINTS = [
  {
    suffix: 'UST / LUST RELEASE SITES',
    category: 'contamination',
    useful_info: 'Underground storage tank facilities and petroleum release records with corrective action status.',
    source_program: 'State leaking underground storage tank program'
  },
  {
    suffix: 'BROWNFIELDS & VOLUNTARY CLEANUP',
    category: 'contamination',
    useful_info: 'Brownfield inventories and voluntary cleanup participation sites with reuse constraints.',
    source_program: 'State brownfields or voluntary cleanup program'
  },
  {
    suffix: 'SOLID WASTE / LANDFILL FACILITIES',
    category: 'landfills',
    useful_info: 'Permitted landfills, transfer stations, and waste handling facilities.',
    source_program: 'State solid waste permit registry'
  },
  {
    suffix: 'AIR PERMITS & MAJOR SOURCES',
    category: 'industrial',
    useful_info: 'Air permits, major emissions sources, and compliance tracking facilities.',
    source_program: 'State air quality permitting and compliance'
  },
  {
    suffix: 'NPDES / WASTEWATER PERMITS',
    category: 'water',
    useful_info: 'Discharge permits, wastewater facilities, and permit status details.',
    source_program: 'State delegated clean water permit program'
  },
  {
    suffix: 'IMPAIRED WATERS (303d/305b)',
    category: 'water',
    useful_info: 'Impaired surface waters, listed causes of impairment, and restoration planning.',
    source_program: 'State integrated water quality assessment'
  },
  {
    suffix: 'DRINKING WATER VIOLATIONS',
    category: 'water',
    useful_info: 'Public water system compliance actions and exceedance/violation records.',
    source_program: 'State drinking water compliance program'
  },
  {
    suffix: 'PFAS / EMERGING CONTAMINANTS',
    category: 'pfas',
    useful_info: 'PFAS sampling results, advisories, and known impacted facilities where published.',
    source_program: 'State PFAS response and monitoring program'
  },
  {
    suffix: 'ENFORCEMENT & COMPLIANCE ACTIONS',
    category: 'regulatory',
    useful_info: 'State enforcement actions, consent orders, penalties, and compliance milestones.',
    source_program: 'State environmental enforcement docket'
  },
  {
    suffix: 'INSTITUTIONAL / LAND USE CONTROLS',
    category: 'regulatory',
    useful_info: 'Recorded environmental covenants and use restrictions tied to remediation projects.',
    source_program: 'State remediation oversight and land records'
  }
];

const EPA_UST_STATE_DATASET_BLUEPRINTS = [
  {
    suffix: 'EPA ECHO AIR FACILITIES',
    category: 'industrial',
    useful_info: 'EPA ECHO Clean Air Act-regulated facilities mapped for statewide visibility.',
    source_program: 'EPA ECHO / CAA'
  },
  {
    suffix: 'EPA ECHO WATER FACILITIES',
    category: 'water',
    useful_info: 'EPA ECHO Clean Water Act and NPDES-regulated facilities mapped for statewide visibility.',
    source_program: 'EPA ECHO / CWA'
  },
  {
    suffix: 'EPA ECHO RCRA FACILITIES',
    category: 'contamination',
    useful_info: 'EPA ECHO RCRA-regulated hazardous waste facilities mapped for statewide visibility.',
    source_program: 'EPA ECHO / RCRA'
  },
  {
    suffix: 'EPA UST REGISTRY',
    category: 'contamination',
    useful_info: 'Underground storage tank facility registry records aligned to EPA/state program publication.',
    source_program: 'EPA / State UST program'
  },
  {
    suffix: 'EPA LUST CORRECTIVE ACTION',
    category: 'contamination',
    useful_info: 'Leaking underground storage tank release and corrective action records aligned to EPA/state program publication.',
    source_program: 'EPA / State LUST program'
  }
];

function buildUSStateDatasetSeeds() {
  const seeds = [];

  for (const state of US_STATES) {
    for (const blueprint of STATE_DATASET_BLUEPRINTS) {
      seeds.push({
        name: `${state.name} ${blueprint.suffix}`,
        category: blueprint.category,
        country: 'USA',
        state: state.name,
        state_code: state.code,
        source_program: blueprint.source_program,
        useful_info: blueprint.useful_info,
        coverage_scope: 'Statewide',
        maintainer: `${state.name} environmental agency`,
        priority: 'high'
      });
    }
  }

  return seeds;
}

function buildEPAAndUSTStateDatasetSeeds() {
  const seeds = [];

  for (const state of US_STATES) {
    for (const blueprint of EPA_UST_STATE_DATASET_BLUEPRINTS) {
      seeds.push({
        name: `${state.name} ${blueprint.suffix}`,
        category: blueprint.category,
        country: 'USA',
        state: state.name,
        state_code: state.code,
        source_program: blueprint.source_program,
        useful_info: blueprint.useful_info,
        coverage_scope: 'Statewide',
        maintainer: 'EPA and state environmental agencies',
        priority: 'high'
      });
    }
  }

  return seeds;
}

module.exports = {
  US_STATES,
  STATE_DATASET_BLUEPRINTS,
  EPA_UST_STATE_DATASET_BLUEPRINTS,
  buildUSStateDatasetSeeds,
  buildEPAAndUSTStateDatasetSeeds
};
