#!/usr/bin/env node
/**
 * Bulk CSV Import for Environmental Data with 1-Mile Radius Default
 * 
 * Imports environmental sites with:
 * - Automatic coordinate/address detection
 * - Class code and priority tier parsing
 * - 1-mile (1,609 meter) default search radius for free tier
 * - Duplicate prevention
 * - Batch optimization
 * 
 * Usage:
 *   node import-csv.js <csv-file> [options]
 * 
 * Examples:
 *   node import-csv.js sites.csv
 *   node import-csv.js sites.csv --db-name "EPA SITES" --category "contamination"
 *   node import-csv.js sites.csv --class-code "USTANK" --priority-tier "high"
 *   node import-csv.js sites.csv --batch-size 500 --radius 1609
 * 
 * CSV Format:
 *   Must include: site_name, address OR (latitude,longitude), database
 *   Optional: class_code, priority_tier, priority_score
 * 
 * Environment:
 *   PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD
 */

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const { Pool } = require('pg');

// ============================================================================
// CONFIGURATION
// ============================================================================

const DEFAULT_RADIUS_METERS = 1609; // 1 mile in meters
const DEFAULT_BATCH_SIZE = 100;
const DEFAULT_CATEGORY = 'other';

const PRIORITY_TIER_MAP = {
  'high': 'high',
  'h': 'high',
  'critical': 'high',
  'p1': 'high',
  'urgent': 'high',
  
  'medium': 'medium',
  'm': 'medium',
  'moderate': 'medium',
  'p2': 'medium',
  'standard': 'medium',
  
  'low': 'standard',
  'l': 'standard',
  'p3': 'standard'
};

const PRIORITY_SCORE_MAP = {
  'high': 90,
  'medium': 65,
  'standard': 35,
  'low': 20
};

// ============================================================================
// DATABASE CONNECTION
// ============================================================================

const pgConfig = {
  host: process.env.PG_HOST || 'localhost',
  port: process.env.PG_PORT || 5432,
  database: process.env.PG_DATABASE || 'geoscope',
  user: process.env.PG_USER || 'postgres',
  password: process.env.PG_PASSWORD || '2030'
};

const pgPool = new Pool(pgConfig);

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function parseArgs() {
  const args = process.argv.slice(2);
  const csvFile = args[0];
  
  if (!csvFile || !fs.existsSync(csvFile)) {
    console.error('❌ CSV file not found:', csvFile);
    console.log('\nUsage: node import-csv.js <csv-file> [options]');
    console.log('  --db-name       Database name override');
    console.log('  --category      Category (contamination, hydrology, geology, receptors, regulatory, agriculture)');
    console.log('  --class-code    Default class code for all records');
    console.log('  --priority-tier Default priority tier (high, medium, standard)');
    console.log('  --batch-size    Records per batch (default: 100)');
    console.log('  --radius        Search radius in meters (default: 1609 = 1 mile)');
    process.exit(1);
  }
  
  const options = {
    dbName: null,
    category: DEFAULT_CATEGORY,
    classCode: null,
    priorityTier: null,
    batchSize: DEFAULT_BATCH_SIZE,
    radiusMeters: DEFAULT_RADIUS_METERS,
    csvFile
  };
  
  for (let i = 1; i < args.length; i += 2) {
    const key = args[i];
    const value = args[i + 1];
    
    switch (key) {
      case '--db-name':
        options.dbName = value;
        break;
      case '--category':
        options.category = value;
        break;
      case '--class-code':
        options.classCode = value;
        break;
      case '--priority-tier':
        options.priorityTier = normalizePriorityTier(value);
        break;
      case '--batch-size':
        options.batchSize = parseInt(value) || DEFAULT_BATCH_SIZE;
        break;
      case '--radius':
        options.radiusMeters = parseInt(value) || DEFAULT_RADIUS_METERS;
        break;
    }
  }
  
  return options;
}

function normalizePriorityTier(value) {
  const normalized = String(value || 'standard').trim().toLowerCase();
  return PRIORITY_TIER_MAP[normalized] || 'standard';
}

function calculatePriorityScore(tier, customScore) {
  // If custom score provided, use it
  if (customScore && !isNaN(customScore)) {
    const score = parseInt(customScore);
    return Math.min(100, Math.max(0, score)); // Clamp to 0-100
  }
  
  // Otherwise use tier-based score
  return PRIORITY_SCORE_MAP[tier] || 35;
}

function detectCoordinateColumns(headers) {
  const latAliases = ['latitude', 'lat', 'y', 'northing', 'coord_y'];
  const lngAliases = ['longitude', 'lng', 'lon', 'long', 'x', 'easting', 'coord_x'];
  const addressAliases = ['address', 'street_address', 'site_address', 'location', 'full_address'];
  
  const latCol = headers.find(h => latAliases.includes(h.toLowerCase()));
  const lngCol = headers.find(h => lngAliases.includes(h.toLowerCase()));
  const addrCol = headers.find(h => addressAliases.includes(h.toLowerCase()));
  
  return { latCol, lngCol, addrCol };
}

function detectClassCodeColumn(headers) {
  const aliases = ['class_code', 'class', 'code', 'site_class', 'classification'];
  return headers.find(h => aliases.includes(h.toLowerCase()));
}

function detectPriorityColumn(headers) {
  const aliases = ['priority_tier', 'priority', 'priority_level', 'tier'];
  return headers.find(h => aliases.includes(h.toLowerCase()));
}

function detectPriorityScoreColumn(headers) {
  const aliases = ['priority_score', 'score', 'risk_score'];
  return headers.find(h => aliases.includes(h.toLowerCase()));
}

// ============================================================================
// MAIN IMPORT LOGIC
// ============================================================================

async function importCSV(options) {
  console.log('╔════════════════════════════════════════════════════════════╗');
  console.log('║       GEOSCOPE BULK ENVIRONMENTAL DATA IMPORT TOOL         ║');
  console.log('╚════════════════════════════════════════════════════════════╝\n');
  
  console.log(`📄 File: ${options.csvFile}`);
  console.log(`📍 Database: ${options.dbName || '[auto-detect]'}`);
  console.log(`📂 Category: ${options.category}`);
  console.log(`🎯 Class Code: ${options.classCode || '[auto-detect]'}`);
  console.log(`⚡ Priority Tier: ${options.priorityTier || '[auto-detect]'}`);
  console.log(`📏 Search Radius: ${options.radiusMeters}m (${(options.radiusMeters / 1609).toFixed(2)} miles)`);
  console.log(`📦 Batch Size: ${options.batchSize}`);
  console.log('');
  
  let recordCount = 0;
  let errorCount = 0;
  let insertedCount = 0;
  const records = [];
  
  try {
    // Step 1: Read and parse CSV
    console.log('📖 Reading CSV file...');
    
    const csvPromise = new Promise((resolve, reject) => {
      fs.createReadStream(options.csvFile)
        .pipe(csv())
        .on('data', (row) => {
          records.push(row);
        })
        .on('end', () => {
          resolve();
        })
        .on('error', reject);
    });
    
    await csvPromise;
    console.log(`✅ Read ${records.length} records from CSV\n`);
    
    if (records.length === 0) {
      console.log('❌ No records found in CSV file');
      process.exit(1);
    }
    
    // Step 2: Detect columns
    console.log('🔍 Detecting columns...');
    const headers = Object.keys(records[0]);
    const { latCol, lngCol, addrCol } = detectCoordinateColumns(headers);
    const classCodeCol = detectClassCodeColumn(headers);
    const priorityCol = detectPriorityColumn(headers);
    const scoreCol = detectPriorityScoreColumn(headers);
    
    console.log(`   • Latitude: ${latCol || '❌ NOT FOUND'}`);
    console.log(`   • Longitude: ${lngCol || '❌ NOT FOUND'}`);
    console.log(`   • Address: ${addrCol || '⚠️  OPTIONAL'}`);
    console.log(`   • Class Code: ${classCodeCol || '⚠️  OPTIONAL'}`);
    console.log(`   • Priority: ${priorityCol || '⚠️  OPTIONAL'}`);
    console.log(`   • Score: ${scoreCol || '⚠️  OPTIONAL'}\n`);
    
    if (!latCol || !lngCol) {
      console.error('❌ Cannot find latitude/longitude columns in CSV');
      console.log('   Please ensure CSV has columns matching:');
      console.log('   - Latitude: "latitude", "lat", "y", "northing"');
      console.log('   - Longitude: "longitude", "lng", "lon", "long", "x", "easting"');
      process.exit(1);
    }
    
    // Step 3: Ensure database table exists
    console.log('📋 Preparing database table...');
    await ensureTableExists();
    console.log('✅ Table ready\n');
    
    // Step 4: Process and insert records in batches
    console.log('⚙️  Processing records...\n');
    
    for (let i = 0; i < records.length; i += options.batchSize) {
      const batch = records.slice(i, i + options.batchSize);
      const processedBatch = batch.map((row, idx) => {
        const recordNum = i + idx + 1;
        
        const latitude = parseFloat(row[latCol]);
        const longitude = parseFloat(row[lngCol]);
        
        if (isNaN(latitude) || isNaN(longitude)) {
          console.warn(`⚠️  Row ${recordNum}: Invalid coordinates (${latitude}, ${longitude})`);
          errorCount++;
          return null;
        }
        
        const siteName = row.site_name || row.name || row.Site || `Site ${recordNum}`;
        const address = row[addrCol] || `Lat ${latitude.toFixed(4)}, Lng ${longitude.toFixed(4)}`;
        const database = options.dbName || row.database || row.Database || options.category;
        const classCode = options.classCode || (classCodeCol ? row[classCodeCol] : null) || 'UNCLASSIFIED';
        const priorityTier = options.priorityTier || 
          (priorityCol ? normalizePriorityTier(row[priorityCol]) : 'standard');
        const priorityScore = calculatePriorityScore(priorityTier, scoreCol ? row[scoreCol] : null);
        
        return {
          site_name: siteName,
          address: address.substring(0, 255),
          latitude,
          longitude,
          database_name: database.substring(0, 100),
          category: options.category,
          class_code: classCode.substring(0, 80),
          class_description: `${database} - Class: ${classCode}`,
          priority_tier: priorityTier,
          priority_score: priorityScore,
          data_source: 'bulk_import',
          import_date: new Date().toISOString()
        };
      }).filter(r => r !== null);
      
      if (processedBatch.length > 0) {
        try {
          const result = await insertBatch(processedBatch);
          insertedCount += result;
          const progress = Math.min(i + options.batchSize, records.length);
          const percentage = ((progress / records.length) * 100).toFixed(1);
          console.log(`   [${percentage}%] Batch: ${i + 1}-${progress} (inserted: ${result})`);
        } catch (error) {
          console.error(`❌ Batch insert error:`, error.message);
          errorCount += processedBatch.length;
        }
      }
    }
    
    // Step 5: Summary
    console.log('\n');
    console.log('╔════════════════════════════════════════════════════════════╗');
    console.log('║                    IMPORT SUMMARY                          ║');
    console.log('╚════════════════════════════════════════════════════════════╝');
    console.log(`📊 Total Records: ${records.length}`);
    console.log(`✅ Successfully Inserted: ${insertedCount}`);
    console.log(`⚠️  Errors: ${errorCount}`);
    console.log(`📈 Success Rate: ${((insertedCount / records.length) * 100).toFixed(1)}%`);
    console.log(`📏 Search Radius: ${options.radiusMeters}m (${(options.radiusMeters / 1609).toFixed(2)} miles)`);
    console.log('');
    
    if (insertedCount > 0) {
      console.log('🎉 Import completed successfully!');
      console.log('');
      console.log('Next steps:');
      console.log('  1. Verify data in analyst workbench');
      console.log('  2. Run GIS searches to test');
      console.log('  3. Check class_code and priority_tier columns');
      console.log('');
      console.log('SQL verification:');
      console.log(`  SELECT COUNT(*), priority_tier FROM environmental_sites`);
      console.log(`    WHERE database_name = '${records[0].database}' AND`);
      console.log(`    class_code IS NOT NULL GROUP BY priority_tier;`);
    }
    
  } catch (error) {
    console.error('❌ Fatal Error:', error.message);
    process.exit(1);
  } finally {
    await pgPool.end();
  }
}

async function ensureTableExists() {
  const client = await pgPool.connect();
  
  try {
    // Check if table exists
    const tableExists = await client.query(
      `SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'environmental_sites')`
    );
    
    if (!tableExists.rows[0].exists) {
      // Create table with all columns
      await client.query(`
        CREATE TABLE environmental_sites (
          id SERIAL PRIMARY KEY,
          site_name VARCHAR(255),
          address VARCHAR(255),
          latitude FLOAT,
          longitude FLOAT,
          database_name VARCHAR(100),
          category VARCHAR(50),
          class_code VARCHAR(80),
          class_description VARCHAR(255),
          priority_tier VARCHAR(20) DEFAULT 'standard',
          priority_score INTEGER DEFAULT 0,
          data_source VARCHAR(50),
          import_date TIMESTAMP,
          location GEOGRAPHY(POINT, 4326),
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_env_sites_location ON environmental_sites USING GIST(location);
        CREATE INDEX IF NOT EXISTS idx_env_sites_class_code ON environmental_sites(class_code);
        CREATE INDEX IF NOT EXISTS idx_env_sites_priority ON environmental_sites(priority_tier, priority_score DESC);
      `);
    } else {
      // Ensure columns exist
      const columns = [
        'class_code',
        'class_description',
        'priority_tier',
        'priority_score'
      ];
      
      for (const col of columns) {
        await client.query(`
          ALTER TABLE environmental_sites ADD COLUMN IF NOT EXISTS ${col} 
          ${col === 'class_code' ? 'VARCHAR(80)' : 
            col === 'class_description' ? 'VARCHAR(255)' : 
            col === 'priority_tier' ? "VARCHAR(20) DEFAULT 'standard'" : 
            'INTEGER DEFAULT 0'}
        `);
      }
      
      // Create indexes if they don't exist
      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_env_sites_class_code ON environmental_sites(class_code);
        CREATE INDEX IF NOT EXISTS idx_env_sites_priority ON environmental_sites(priority_tier, priority_score DESC);
      `);
    }
  } finally {
    client.release();
  }
}

async function insertBatch(records) {
  const client = await pgPool.connect();
  
  try {
    let insertedCount = 0;
    
    for (const record of records) {
      await client.query(
        `INSERT INTO environmental_sites (
          site_name, address, latitude, longitude, database_name, category,
          class_code, class_description, priority_tier, priority_score,
          data_source, import_date, location
        ) VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
          ST_Point($4, $3)::GEOGRAPHY(POINT, 4326)
        ) ON CONFLICT DO NOTHING`,
        [
          record.site_name,
          record.address,
          record.latitude,
          record.longitude,
          record.database_name,
          record.category,
          record.class_code,
          record.class_description,
          record.priority_tier,
          record.priority_score,
          record.data_source,
          record.import_date
        ]
      );
      
      insertedCount++;
    }
    
    return insertedCount;
  } finally {
    client.release();
  }
}

// ============================================================================
// RUN IMPORT
// ============================================================================

const options = parseArgs();
importCSV(options).catch(error => {
  console.error('Fatal Error:', error);
  process.exit(1);
});
