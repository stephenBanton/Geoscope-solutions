import puppeteer from './node_modules/puppeteer-core/lib/esm/puppeteer/puppeteer-core.js';
import { readFileSync, existsSync, writeFileSync } from 'fs';
import https from 'https';

const CHROME = 'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe';
const SAT_CACHE = 'satellite-bg-cache.png';

// Download satellite image once and cache locally — never changes after first download
async function getSatelliteDataUri() {
  if (!existsSync(SAT_CACHE)) {
    console.log('Downloading satellite image (one-time)...');
    const url = 'https://services.arcgisonline.com/arcgis/rest/services/World_Imagery/MapServer/export?bbox=-80.32,25.62,-80.02,25.86&bboxSR=4326&size=900,1100&format=png32&transparent=false&f=image';
    await new Promise((resolve, reject) => {
      https.get(url, res => {
        const chunks = [];
        res.on('data', c => chunks.push(c));
        res.on('end', () => { writeFileSync(SAT_CACHE, Buffer.concat(chunks)); resolve(); });
        res.on('error', reject);
      }).on('error', reject);
    });
    console.log('Satellite image cached to', SAT_CACHE);
  }
  const b64 = readFileSync(SAT_CACHE).toString('base64');
  return `data:image/png;base64,${b64}`;
}

// Logo SVG — same as server.js getLogoDataUri()
const logoSvg = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 220 260" width="220" height="260">
  <defs>
    <linearGradient id="pinGrad" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0%" stop-color="#7ec445"/>
      <stop offset="100%" stop-color="#3d9b35"/>
    </linearGradient>
    <linearGradient id="lensGrad" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0%" stop-color="#33b2b5"/>
      <stop offset="100%" stop-color="#175b96"/>
    </linearGradient>
  </defs>
  <ellipse cx="110" cy="242" rx="42" ry="10" fill="rgba(15,58,97,0.10)"/>
  <path d="M110 234C110 234 188 148 188 90C188 42 153 10 110 10C67 10 32 42 32 90C32 148 110 234 110 234Z" fill="url(#pinGrad)"/>
  <path d="M110 234C110 234 188 148 188 90C188 42 153 10 110 10C67 10 32 42 32 90C32 148 110 234 110 234Z" stroke="#ffffff" stroke-width="8"/>
  <circle cx="106" cy="84" r="46" fill="#ffffff"/>
  <g transform="translate(61 39)">
    <ellipse cx="45" cy="45" rx="38" ry="14" fill="none" stroke="#86c74e" stroke-width="3"/>
    <ellipse cx="45" cy="45" rx="38" ry="25" fill="none" stroke="#86c74e" stroke-width="3"/>
    <path d="M7 45H83" stroke="#86c74e" stroke-width="3"/>
    <path d="M45 7V83" stroke="#86c74e" stroke-width="3"/>
    <circle cx="57" cy="57" r="16" fill="none" stroke="url(#lensGrad)" stroke-width="6"/>
    <path d="M69 69L82 82" stroke="url(#lensGrad)" stroke-width="7" stroke-linecap="round"/>
  </g>
</svg>`;
const logoDataUri = `data:image/svg+xml;base64,${Buffer.from(logoSvg).toString('base64')}`;

const satelliteDataUri = await getSatelliteDataUri();
const template = readFileSync('reportTemplate.html', 'utf8');
const browser = await puppeteer.launch({
  executablePath: CHROME,
  args: ['--no-sandbox','--disable-setuid-sandbox','--disable-gpu'],
  headless: 'new'
});

let html = template
  .replace(/\{\{cover_style\}\}/g, 'premium')
  .replace(/\{\{cover_style_label\}\}/g, 'Premium Corporate')
  .replace(/\{\{project_name\}\}/g, 'GeoScope Solutions Portfolio Review')
  .replace(/\{\{client_name\}\}/g, 'Summit Capital Partners')
  .replace(/\{\{report_date\}\}/g, 'April 28, 2026')
  .replace(/\{\{address\}\}/g, '69th Road, Suwannee County, Florida, United States')
  .replace(/\{\{latitude\}\}/g, '25.7617')
  .replace(/\{\{longitude\}\}/g, '-80.1918')
  .replace(/\{\{radius\}\}/g, '1 mile')
  .replace(/\{\{order_id\}\}/g, '9131')
  .replace(/\{\{project_number\}\}/g, 'PRJ-1777059969692')
  .replace(/\{\{mapImage\}\}/g, satelliteDataUri)
  .replace(/\{\{logoImage\}\}/g, logoDataUri)
  .replace(/\{\{[^}]+\}\}/g, '—');

// A4 at 150dpi: 1240 × 1754 px
const page = await browser.newPage();
await page.setViewport({ width: 1240, height: 1754, deviceScaleFactor: 2 });
await page.setContent(html, { waitUntil: 'networkidle0' });

const cover = await page.$('.cover');
if (!cover) {
  throw new Error('Cover element not found');
}

await cover.screenshot({ path: 'cover-premium-preview.png', type: 'png' });
console.log('Saved cover-premium-preview.png');
await page.close();
await browser.close();
console.log('DONE');
