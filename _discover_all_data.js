#!/usr/bin/env node
/**
 * Comprehensive Data Discovery - H:\databae all folders
 * Scans recursively and catalogs all importable data files
 */

const fs = require('fs');
const path = require('path');

const RESET = '\x1b[0m';
const GREEN = '\x1b[32m';
const YELLOW = '\x1b[33m';
const CYAN = '\x1b[36m';

const supportedFormats = ['.csv', '.shp', '.geojson', '.json', '.kml', '.kmz', '.gpkg', '.sqlite', '.gdb'];
const rootPath = 'H:\\databae';

const results = {
  csv: [],
  shapefiles: [],
  geodatabases: [],
  geojson: [],
  kml: [],
  other: [],
  total: 0
};

function walkDir(dir, maxDepth = 10, currentDepth = 0) {
  if (currentDepth > maxDepth) return;
  
  try {
    const items = fs.readdirSync(dir);
    
    for (const item of items) {
      const fullPath = path.join(dir, item);
      const relativePath = path.relative(rootPath, fullPath);
      
      try {
        const stat = fs.statSync(fullPath);
        
        if (stat.isDirectory()) {
          // Recurse into subdirectories
          walkDir(fullPath, maxDepth, currentDepth + 1);
        } else if (stat.isFile()) {
          const ext = path.extname(item).toLowerCase();
          const size = (stat.size / (1024 * 1024)).toFixed(2); // MB
          
          const fileInfo = { path: relativePath, size, name: item };
          
          if (ext === '.csv') {
            results.csv.push(fileInfo);
          } else if (ext === '.shp') {
            results.shapefiles.push(fileInfo);
          } else if (ext === '.gdb') {
            results.geodatabases.push(fileInfo);
          } else if (ext === '.geojson' || ext === '.json') {
            results.geojson.push(fileInfo);
          } else if (ext === '.kml' || ext === '.kmz') {
            results.kml.push(fileInfo);
          } else if (['.gpkg', '.sqlite', '.db'].includes(ext)) {
            results.other.push(fileInfo);
          }
          
          results.total++;
        }
      } catch (err) {
        // Skip files we can't access
      }
    }
  } catch (err) {
    console.error(`Error reading ${dir}:`, err.message);
  }
}

console.log(`${CYAN}=== COMPREHENSIVE DATA DISCOVERY ===${RESET}\n`);
console.log(`Scanning: ${rootPath}\n`);

walkDir(rootPath, 15);

console.log(`${YELLOW}CSV Files (${results.csv.length}):${RESET}`);
results.csv.slice(0, 30).forEach(f => console.log(`  ${f.size}MB  ${f.path}`));
if (results.csv.length > 30) console.log(`  ... and ${results.csv.length - 30} more`);

console.log(`\n${YELLOW}Shapefiles (${results.shapefiles.length}):${RESET}`);
results.shapefiles.slice(0, 20).forEach(f => console.log(`  ${f.size}MB  ${f.path}`));
if (results.shapefiles.length > 20) console.log(`  ... and ${results.shapefiles.length - 20} more`);

console.log(`\n${YELLOW}GeoJSON Files (${results.geojson.length}):${RESET}`);
results.geojson.slice(0, 20).forEach(f => console.log(`  ${f.size}MB  ${f.path}`));
if (results.geojson.length > 20) console.log(`  ... and ${results.geojson.length - 20} more`);

console.log(`\n${YELLOW}File Geodatabases (${results.geodatabases.length}):${RESET}`);
results.geodatabases.forEach(f => console.log(`  ${f.size}MB  ${f.path}`));

console.log(`\n${YELLOW}KML/KMZ Files (${results.kml.length}):${RESET}`);
results.kml.forEach(f => console.log(`  ${f.size}MB  ${f.path}`));

console.log(`\n${YELLOW}Other Formats (${results.other.length}):${RESET}`);
results.other.slice(0, 10).forEach(f => console.log(`  ${f.size}MB  ${f.path}`));

console.log(`\n${CYAN}=== SUMMARY ===${RESET}`);
console.log(`Total files scanned: ${results.total}`);
console.log(`Total importable: ${results.csv.length + results.shapefiles.length + results.geojson.length + results.geodatabases.length + results.kml.length + results.other.length}`);
console.log(`\n${GREEN}Ready for batch import!${RESET}\n`);

// Export summary
const summary = {
  csv_count: results.csv.length,
  shp_count: results.shapefiles.length,
  geojson_count: results.geojson.length,
  gdb_count: results.geodatabases.length,
  kml_count: results.kml.length,
  other_count: results.other.length,
  csv_files: results.csv,
  shp_files: results.shapefiles,
  gdb_files: results.geodatabases,
  geojson_files: results.geojson,
  kml_files: results.kml
};

fs.writeFileSync('_data_discovery_report.json', JSON.stringify(summary, null, 2));
console.log('Detailed report saved to: _data_discovery_report.json\n');
