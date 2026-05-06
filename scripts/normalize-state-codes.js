const { Pool } = require('pg');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });

const pool = new Pool({
  host: process.env.PG_HOST,
  port: process.env.PG_PORT,
  database: process.env.PG_DATABASE,
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
});

// FIPS code to state abbreviation mapping
const FIPS_TO_STATE = {
  '01': 'AL', '02': 'AK', '04': 'AZ', '05': 'AR', '06': 'CA',
  '08': 'CO', '09': 'CT', '10': 'DE', '12': 'FL', '13': 'GA',
  '15': 'HI', '16': 'ID', '17': 'IL', '18': 'IN', '19': 'IA',
  '20': 'KS', '21': 'KY', '22': 'LA', '23': 'ME', '24': 'MD',
  '25': 'MA', '26': 'MI', '27': 'MN', '28': 'MS', '29': 'MO',
  '30': 'MT', '31': 'NE', '32': 'NV', '33': 'NH', '34': 'NJ',
  '35': 'NM', '36': 'NY', '37': 'NC', '38': 'ND', '39': 'OH',
  '40': 'OK', '41': 'OR', '42': 'PA', '44': 'RI', '45': 'SC',
  '46': 'SD', '47': 'TN', '48': 'TX', '49': 'UT', '50': 'VT',
  '51': 'VA', '53': 'WA', '54': 'WV', '55': 'WI', '56': 'WY',
  '11': 'DC'
};

// Valid US state abbreviations
const VALID_STATES = new Set(Object.values(FIPS_TO_STATE));

function normalizeState(state) {
  if (!state) return null;
  
  state = state.trim().toUpperCase();
  
  // Already valid 2-letter code
  if (VALID_STATES.has(state)) return state;
  
  // FIPS code
  if (FIPS_TO_STATE[state]) return FIPS_TO_STATE[state];
  
  // Lowercase variant
  if (VALID_STATES.has(state.toUpperCase())) return state.toUpperCase();
  
  return null; // Invalid, will be set to NULL
}

(async () => {
  try {
    console.log('=== STATE CODE NORMALIZATION ===\n');
    
    // Get current state distribution
    const currentResult = await pool.query(`
      SELECT state, COUNT(*) as count 
      FROM environmental_sites 
      WHERE state IS NOT NULL 
      GROUP BY state 
      ORDER BY state
    `);
    
    console.log(`Current unique state codes: ${currentResult.rows.length}\n`);
    
    // Analyze current codes
    let validCount = 0, invalidCount = 0;
    const updates = {};
    
    for (const row of currentResult.rows) {
      const normalized = normalizeState(row.state);
      if (normalized) {
        validCount += row.count;
        if (!updates[normalized]) updates[normalized] = 0;
        updates[normalized] += row.count;
        
        if (normalized !== row.state) {
          console.log(`  ${row.state.padEnd(8)} → ${normalized} (${row.count} records)`);
        }
      } else {
        invalidCount += row.count;
        console.log(`  ${row.state.padEnd(8)} → REMOVE (${row.count} records) [INVALID]`);
      }
    }
    
    console.log(`\nValid records to normalize: ${validCount}`);
    console.log(`Invalid records to remove: ${invalidCount}\n`);
    
    // Apply normalization
    console.log('Applying normalization...');
    let totalUpdated = 0;
    
    for (const [newState, count] of Object.entries(updates)) {
      const oldStates = currentResult.rows
        .filter(r => normalizeState(r.state) === newState && r.state !== newState)
        .map(r => r.state);
      
      if (oldStates.length > 0) {
        const updateResult = await pool.query(
          `UPDATE environmental_sites 
           SET state = $1 
           WHERE state = ANY($2)`,
          [newState, oldStates]
        );
        totalUpdated += updateResult.rowCount;
      }
    }
    
    // Remove invalid state codes
    const invalidResult = await pool.query(`
      SELECT DISTINCT state 
      FROM environmental_sites 
      WHERE state IS NOT NULL 
        AND state NOT IN (${Array.from(VALID_STATES).map(s => `'${s}'`).join(',')})
    `);
    
    if (invalidResult.rows.length > 0) {
      const invalidStates = invalidResult.rows.map(r => r.state);
      const deleteResult = await pool.query(
        `UPDATE environmental_sites 
         SET state = NULL 
         WHERE state = ANY($1)`,
        [invalidStates]
      );
      totalUpdated += deleteResult.rowCount;
      console.log(`  Cleared ${deleteResult.rowCount} records with invalid state codes`);
    }
    
    console.log(`  Updated ${totalUpdated} total records\n`);
    
    // Verify final state coverage
    const finalResult = await pool.query(`
      SELECT state, COUNT(*) as count 
      FROM environmental_sites 
      WHERE state IS NOT NULL 
      GROUP BY state 
      ORDER BY state
    `);
    
    const statesInDb = new Set(finalResult.rows.map(r => r.state));
    const allStateAbbrs = Array.from(VALID_STATES).sort();
    
    console.log('=== FINAL STATE COVERAGE ===\n');
    console.log(`States with data: ${statesInDb.size}/${allStateAbbrs.length}\n`);
    
    finalResult.rows.forEach(r => {
      console.log(`  ${r.state.padEnd(5)} | ${r.count.toLocaleString().padStart(10)} records`);
    });
    
    const missing = allStateAbbrs.filter(s => !statesInDb.has(s));
    if (missing.length > 0) {
      console.log(`\n⚠ MISSING STATES (${missing.length}):`);
      missing.forEach(s => console.log(`  ${s}`));
    } else {
      console.log('\n✓ ALL 50 STATES COVERED!');
    }
    
    const totalRecords = finalResult.rows.reduce((sum, r) => sum + r.count, 0);
    console.log(`\nTotal records with valid state codes: ${totalRecords.toLocaleString()}`);
    
    process.exit(0);
  } catch (err) {
    console.error('Error:', err);
    process.exit(1);
  }
})();
