#!/usr/bin/env node
require('dotenv').config({ path: require('path').join(__dirname, '../.env') });

const fs = require('fs');
const path = require('path');
const http = require('http');
const https = require('https');
const { Pool } = require('pg');

const argv = process.argv.slice(2);
const shouldDownload = argv.includes('--download');
const sourceFile = getArgValue('--source-file') || path.join(__dirname, 'dataset-master-request.txt');
const maxDownloads = Number(getArgValue('--max-downloads') || 12);

const OUTPUT_DIR = path.join(__dirname, '..', 'downloads', 'missing', '_normalized_queue');

const DIRECT_SOURCES = {
  ECHO_CORE: {
    label: 'EPA ECHO Exporter',
    urls: ['https://echo.epa.gov/files/echodownloads/echo_exporter.zip']
  },
  ECHO_FRS: {
    label: 'EPA FRS Facilities and Linkages',
    urls: ['https://echo.epa.gov/files/echodownloads/frs_downloads.zip']
  },
  ECHO_RCRA: {
    label: 'EPA RCRAInfo National Download',
    urls: ['https://echo.epa.gov/files/echodownloads/rcra_downloads.zip']
  },
  ECHO_AIR: {
    label: 'EPA ICIS-Air National Download',
    urls: ['https://echo.epa.gov/files/echodownloads/icis-air_downloads.zip']
  },
  ECHO_AIR_EMISSIONS: {
    label: 'EPA Air Emissions Download',
    urls: ['https://echo.epa.gov/files/echodownloads/air-emissions_downloads.zip']
  },
  ECHO_NPDES: {
    label: 'EPA ICIS-NPDES National Download',
    urls: ['https://echo.epa.gov/files/echodownloads/ICIS-NPDES_downloads.zip']
  },
  ECHO_SDWA: {
    label: 'EPA SDWA National Download',
    urls: ['https://echo.epa.gov/files/echodownloads/SDWA_downloads.zip']
  },
  ECHO_ENFORCEMENT: {
    label: 'EPA ICIS FE&C Download',
    urls: ['https://echo.epa.gov/files/echodownloads/ICIS_FEandC_downloads.zip']
  },
  EPA_RMP: {
    label: 'EPA Risk Management Plan Export',
    urls: ['https://www.epa.gov/sites/default/files/2021-03/rmp_search_export_0.csv']
  },
  EPA_FRS_LEGACY: {
    label: 'EPA National Combined FRS Legacy ZIP',
    urls: ['https://www3.epa.gov/enviro/html/fii/downloads/state_files/national_combined.zip']
  },
  DOD_INSTALLATIONS: {
    label: 'DOD Military Installations ZIP',
    urls: ['http://www.acq.osd.mil/eie/Downloads/DISDI/installations_ranges.zip']
  },
  FEMA_FLOOD_CLAIMS_API: {
    label: 'OpenFEMA NFIP Claims API',
    urls: ['https://www.fema.gov/api/open/v2/FimaNfipClaims?$top=10000']
  },
  FEMA_FLOOD_POLICIES_API: {
    label: 'OpenFEMA NFIP Policies API',
    urls: ['https://www.fema.gov/api/open/v2/FimaNfipPolicies?$top=10000']
  }
};

const FAMILY_RULES = [
  { re: /(RCRA|CORRACTS|HAZ\s*WASTE|TSDF|SQG|VSQG|NONGEN|BRS|MANIFEST)/i, sourceKey: 'ECHO_RCRA' },
  { re: /(NPL|CERCLIS|SEMS|PRP|ROD|DELISTED|PROPOSED\s+NPL|AOC|LIENS)/i, sourceKey: 'ECHO_FRS' },
  { re: /(AIR|AFS|EIS|ALT\s*FUEL|SAA|EMISSIONS)/i, sourceKey: 'ECHO_AIR' },
  { re: /(NPDES|STORMWATER|ENOI|PCS|PWS\s+ENF|INACTIVE\s+PCS)/i, sourceKey: 'ECHO_NPDES' },
  { re: /(SDWA|PWS|DRINKING\s+WATER)/i, sourceKey: 'ECHO_SDWA' },
  { re: /(UST|LUST|ERNS|EPA\s+WATCH|DOCKET|FED\s+E\s*C|FED\s+I\s*C|ICIS|FTTS|HMIRS|DOT\s+OPS)/i, sourceKey: 'ECHO_ENFORCEMENT' },
  { re: /(TRI|TRIS|PFAS)/i, sourceKey: 'ECHO_AIR_EMISSIONS' },
  { re: /(RMP)/i, sourceKey: 'EPA_RMP' },
  { re: /(DOD|FUDS|US\s+HIST\s+CDL|FEDLAND|FED\s+CDL)/i, sourceKey: 'DOD_INSTALLATIONS' },
  { re: /(FLOOD|DFIRM|Q3)/i, sourceKey: 'FEMA_FLOOD_CLAIMS_API' }
];

function getArgValue(name) {
  const match = argv.find((a) => a.startsWith(name + '='));
  if (match) return match.split('=')[1];
  const idx = argv.indexOf(name);
  if (idx >= 0) return argv[idx + 1];
  return null;
}

function normalizeName(s) {
  return String(s || '')
    .toUpperCase()
    .replace(/&/g, ' AND ')
    .replace(/[-_/]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function guessSourceKey(name) {
  for (const rule of FAMILY_RULES) {
    if (rule.re.test(name)) return rule.sourceKey;
  }
  return null;
}

function makeSafeName(s) {
  return String(s || '').replace(/[^a-zA-Z0-9._-]+/g, '_');
}

function downloadFile(url, targetPath) {
  const protocol = url.startsWith('https') ? https : http;
  return new Promise((resolve, reject) => {
    const tmp = targetPath + '.tmp';
    const out = fs.createWriteStream(tmp);

    const req = protocol.get(url, { timeout: 180000, headers: { 'User-Agent': 'GeoScope-Normalizer/1.0' } }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        out.destroy();
        return resolve(downloadFile(res.headers.location, targetPath));
      }
      if (res.statusCode !== 200) {
        out.destroy();
        return reject(new Error(`HTTP ${res.statusCode} ${url}`));
      }
      res.pipe(out);
      out.on('finish', () => {
        out.close(() => {
          fs.renameSync(tmp, targetPath);
          resolve(targetPath);
        });
      });
    });

    req.on('error', (err) => {
      if (fs.existsSync(tmp)) fs.unlinkSync(tmp);
      reject(err);
    });
    req.on('timeout', () => {
      req.destroy(new Error('timeout'));
    });
  });
}

async function main() {
  if (!fs.existsSync(sourceFile)) {
    console.error('Source file not found:', sourceFile);
    process.exit(1);
  }

  const raw = fs.readFileSync(sourceFile, 'utf8');
  const requested = [...new Set(raw.split(/\r?\n/).map(normalizeName).filter(Boolean))];

  const pool = new Pool({
    host: process.env.PG_HOST,
    port: Number(process.env.PG_PORT || 5432),
    database: process.env.PG_DATABASE,
    user: process.env.PG_USER,
    password: process.env.PG_PASSWORD
  });

  const [loadedRes, catalogRes] = await Promise.all([
    pool.query(`SELECT DISTINCT UPPER(TRIM(database_name)) AS name FROM environmental_sites WHERE database_name IS NOT NULL AND TRIM(database_name) <> ''`),
    pool.query(`SELECT DISTINCT UPPER(TRIM(name)) AS name FROM database_catalog WHERE name IS NOT NULL AND TRIM(name) <> ''`)
  ]);

  const loaded = new Set(loadedRes.rows.map((r) => r.name));
  const catalog = new Set(catalogRes.rows.map((r) => r.name));

  const normalized = requested.map((name) => {
    const sourceKey = guessSourceKey(name);
    const source = sourceKey ? DIRECT_SOURCES[sourceKey] : null;
    const urls = source ? source.urls : [];
    const directDownloadable = urls.some((u) => /\.zip$|\.csv$|\$top=|\.json$/i.test(u));
    return {
      requested_name: name,
      loaded: loaded.has(name),
      in_catalog: catalog.has(name),
      source_key: sourceKey,
      source_label: source ? source.label : null,
      direct_urls: urls,
      installable_now: Boolean(source && directDownloadable)
    };
  });

  const summary = {
    requested_count: normalized.length,
    loaded_count: normalized.filter((x) => x.loaded).length,
    in_catalog_count: normalized.filter((x) => x.in_catalog).length,
    installable_now_count: normalized.filter((x) => x.installable_now).length,
    missing_and_installable_count: normalized.filter((x) => !x.loaded && x.installable_now).length,
    missing_without_source_count: normalized.filter((x) => !x.loaded && !x.installable_now).length
  };

  fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  const reportPath = path.join(OUTPUT_DIR, 'requested-datasets-normalized-report.json');
  fs.writeFileSync(reportPath, JSON.stringify({ summary, datasets: normalized }, null, 2));

  const csvLines = [
    'requested_name,loaded,in_catalog,source_key,source_label,installable_now,direct_urls'
  ];
  for (const d of normalized) {
    csvLines.push([
      d.requested_name,
      d.loaded,
      d.in_catalog,
      d.source_key || '',
      d.source_label || '',
      d.installable_now,
      '"' + (d.direct_urls || []).join(' | ').replace(/"/g, '""') + '"'
    ].join(','));
  }
  const csvPath = path.join(OUTPUT_DIR, 'requested-datasets-normalized-report.csv');
  fs.writeFileSync(csvPath, csvLines.join('\n'));

  // Build deduplicated download queue
  const needed = normalized.filter((x) => !x.loaded && x.installable_now);
  const queueByUrl = new Map();
  for (const item of needed) {
    for (const u of item.direct_urls) {
      if (!queueByUrl.has(u)) {
        queueByUrl.set(u, { url: u, source_key: item.source_key, source_label: item.source_label, datasets: [item.requested_name] });
      } else {
        queueByUrl.get(u).datasets.push(item.requested_name);
      }
    }
  }
  const queue = Array.from(queueByUrl.values());
  fs.writeFileSync(path.join(OUTPUT_DIR, 'direct-download-queue.json'), JSON.stringify(queue, null, 2));

  console.log('Normalization summary:', summary);
  console.log('Report JSON:', reportPath);
  console.log('Report CSV :', csvPath);
  console.log('Queue file :', path.join(OUTPUT_DIR, 'direct-download-queue.json'));

  if (shouldDownload) {
    const toDownload = queue.slice(0, Math.max(0, maxDownloads));
    console.log(`Starting direct downloads: ${toDownload.length} item(s)`);

    for (const item of toDownload) {
      const parsed = new URL(item.url);
      let filename = path.basename(parsed.pathname) || makeSafeName(item.source_key || 'download');
      if (!filename.includes('.')) {
        if (/\$top=/i.test(item.url)) filename += '.json';
        else filename += '.dat';
      }
      const targetDir = path.join(OUTPUT_DIR, 'files', makeSafeName(item.source_key || 'misc'));
      fs.mkdirSync(targetDir, { recursive: true });
      const target = path.join(targetDir, filename);

      if (fs.existsSync(target)) {
        console.log('Skip existing:', target);
        continue;
      }

      try {
        console.log('Downloading:', item.url);
        await downloadFile(item.url, target);
        console.log('Saved:', target);
      } catch (err) {
        console.log('Download failed:', item.url, '->', err.message);
      }
    }
  }

  await pool.end();
}

main().catch((err) => {
  console.error(err.message);
  process.exit(1);
});
