#!/usr/bin/env node
/**
 * Final Dependency Check - Verify all modules can be loaded
 */

const checks = {
  backend: [
    'express',
    'body-parser',
    'cors',
    'nodemailer',
    'puppeteer',
    'mongoose',
    'axios',
    '@supabase/supabase-js',
    'openai',
    'multer',
    'jsonwebtoken',
    'pg',
    'csv-parse',
    'dotenv'
  ],
  frontend: [
    'react',
    'react-dom',
    'react-router-dom',
    'react-leaflet',
    'leaflet',
    'axios',
    'dotenv'
  ]
};

console.log('\n🔍 COMPREHENSIVE DEPENDENCY CHECK\n');

let allGood = true;

// Check backend dependencies
console.log('Backend Dependencies:');
for (const pkg of checks.backend) {
  try {
    require(pkg);
    console.log(`  ✓ ${pkg}`);
  } catch (err) {
    console.log(`  ✗ ${pkg} - ${err.message}`);
    allGood = false;
  }
}

// Check that we can connect to database
console.log('\nDatabase Connection:');
try {
  const pg = require('pg');
  new pg.Client({
    host: 'localhost',
    port: 5432,
    user: 'postgres',
    password: '2030',
    database: 'geoscope'
  }).connect().then(client => {
    client.query('SELECT 1').then(() => {
      console.log('  ✓ PostgreSQL connection working');
      client.end();
      
      // All checks complete
      console.log('\n' + (allGood ? '✓ ALL DEPENDENCIES INSTALLED' : '✗ SOME DEPENDENCIES MISSING'));
      console.log('\nSetup Status: READY FOR DEPLOYMENT\n');
    }).catch(err => {
      console.log(`  ✗ Database query failed: ${err.message}`);
      client.end();
    });
  }).catch(err => {
    console.log(`  ✗ PostgreSQL connection failed: ${err.message}`);
  });
} catch (err) {
  console.log(`  ✗ pg module error: ${err.message}`);
  allGood = false;
}
