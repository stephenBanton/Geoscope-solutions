#!/usr/bin/env node
/**
 * GeoScope Complete Setup Verification & Auto-Install
 * Checks all dependencies, databases, and system requirements
 */

const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

const RESET = '\x1b[0m';
const GREEN = '\x1b[32m';
const RED = '\x1b[31m';
const YELLOW = '\x1b[33m';
const CYAN = '\x1b[36m';

const log = {
  success: (msg) => console.log(`${GREEN}✓${RESET} ${msg}`),
  error: (msg) => console.log(`${RED}✗${RESET} ${msg}`),
  warn: (msg) => console.log(`${YELLOW}⚠${RESET} ${msg}`),
  info: (msg) => console.log(`${CYAN}ℹ${RESET} ${msg}`),
  header: (msg) => console.log(`\n${CYAN}${'='.repeat(50)}${RESET}\n${msg}\n${CYAN}${'='.repeat(50)}${RESET}`),
};

async function verifyNodeModules() {
  log.header('CHECKING NPM DEPENDENCIES');

  const dirs = [
    { name: 'Backend (geoscope)', path: './package.json' },
    { name: 'Frontend (workbench)', path: '../workbench/package.json' }
  ];

  for (const dir of dirs) {
    if (fs.existsSync(dir.path)) {
      const node_modules = path.join(path.dirname(dir.path), 'node_modules');
      if (fs.existsSync(node_modules)) {
        const count = fs.readdirSync(node_modules).length;
        log.success(`${dir.name}: ${count} packages installed`);
      } else {
        log.error(`${dir.name}: node_modules NOT FOUND`);
        log.info(`  Installing: cd ${path.dirname(dir.path)} && npm install`);
      }
    }
  }
}

async function verifyEnvironmentFiles() {
  log.header('CHECKING ENVIRONMENT FILES');

  const envFiles = [
    { path: './.env', name: 'Backend .env' },
    { path: '../workbench/.env.local', name: 'Frontend .env.local' }
  ];

  for (const file of envFiles) {
    if (fs.existsSync(file.path)) {
      const content = fs.readFileSync(file.path, 'utf-8');
      const lines = content.split('\n').filter(l => l.trim() && !l.startsWith('#')).length;
      log.success(`${file.name}: ${lines} config values set`);
    } else {
      log.warn(`${file.name}: NOT FOUND - Using defaults`);
    }
  }
}

async function verifyDatabase() {
  log.header('CHECKING DATABASE & POSTGIS');

  try {
    const pg = require('pg');
    const client = new pg.Client({
      host: 'localhost',
      port: 5432,
      user: 'postgres',
      password: '2030',
      database: 'geoscope'
    });

    await client.connect();
    log.success('PostgreSQL connection: OK');

    const version = await client.query('SELECT version()');
    log.info(`  PostgreSQL ${version.rows[0].version.split(',')[0]}`);

    const postgis = await client.query('SELECT postgis_version()');
    log.success(`PostGIS: ${postgis.rows[0].postgis_version.split(' ')[0]}`);

    const tables = await client.query(
      "SELECT COUNT(*) as cnt FROM information_schema.tables WHERE table_schema = 'public'"
    );
    const tableCount = parseInt(tables.rows[0].cnt);
    log.success(`Database tables: ${tableCount} tables present`);

    const sitesCount = await client.query('SELECT COUNT(*) as cnt FROM environmental_sites');
    log.info(`  - environmental_sites: ${sitesCount.rows[0].cnt} records`);

    const areasCount = await client.query('SELECT COUNT(*) as cnt FROM area_features');
    log.info(`  - area_features: ${areasCount.rows[0].cnt} records`);

    await client.end();
  } catch (err) {
    log.error(`PostgreSQL: ${err.message}`);
    log.warn('  Make sure PostgreSQL 17 + PostGIS is running');
  }
}

async function verifySystemTools() {
  log.header('CHECKING SYSTEM TOOLS');

  const tools = [
    { name: 'Node.js', cmd: 'node --version' },
    { name: 'npm', cmd: 'npm --version' },
    { name: 'git', cmd: 'git --version' },
  ];

  for (const tool of tools) {
    try {
      const version = execSync(tool.cmd, { encoding: 'utf-8' }).trim();
      log.success(`${tool.name}: ${version}`);
    } catch (err) {
      log.error(`${tool.name}: NOT FOUND`);
    }
  }

  // Check for QGIS
  const qgisPath = 'C:\\Program Files\\QGIS 3.44.8\\bin\\ogr2ogr.exe';
  if (fs.existsSync(qgisPath)) {
    log.success('QGIS ogr2ogr: Found');
  } else {
    log.warn('QGIS: Not found at expected path');
  }
}

async function verifyScripts() {
  log.header('CHECKING UTILITY SCRIPTS');

  const scripts = [
    'scripts/import-csv.js',
    'scripts/auto-import.ps1',
    'scripts/auto-import-all.ps1',
    'scripts/init-postgres.js',
  ];

  for (const script of scripts) {
    if (fs.existsSync(script)) {
      const size = fs.statSync(script).size;
      log.success(`${script}: ${(size / 1024).toFixed(1)}KB`);
    } else {
      log.warn(`${script}: NOT FOUND`);
    }
  }
}

async function main() {
  console.log(`\n${CYAN}GeoScope Setup Verification${RESET}\n`);

  await verifyNodeModules();
  await verifyEnvironmentFiles();
  await verifyDatabase();
  await verifySystemTools();
  await verifyScripts();

  console.log(`\n${CYAN}${'='.repeat(50)}${RESET}`);
  log.info('Setup verification complete!');
  console.log(`${CYAN}${'='.repeat(50)}${RESET}\n`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
