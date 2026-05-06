const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });

const pool = new Pool({
  host: process.env.PG_HOST,
  port: process.env.PG_PORT,
  database: process.env.PG_DATABASE,
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  max: 3,
});

const LOG_DIR = path.join(__dirname, '..');
const INTERVAL_MS = 10 * 60 * 1000;

function latestFederalLog() {
  const files = fs.readdirSync(LOG_DIR)
    .filter((f) => (f.startsWith('federal-import-') || f.startsWith('phase2-envirofacts-')) && f.endsWith('.log'))
    .map((f) => {
      const full = path.join(LOG_DIR, f);
      const st = fs.statSync(full);
      return { name: f, full, mtime: st.mtime, size: st.size };
    })
    .sort((a, b) => b.mtime - a.mtime);
  return files[0] || null;
}

async function snapshot() {
  const now = new Date();
  const result = await pool.query('SELECT COUNT(*)::bigint AS c FROM environmental_sites');
  const count = Number(result.rows[0].c || 0);
  const log = latestFederalLog();

  const lines = [
    '------------------------------------------------------------',
    `Progress Check: ${now.toISOString()}`,
    `DB Total Records: ${count.toLocaleString()}`,
    log
      ? `Latest Import Log: ${log.name} | Size ${log.size.toLocaleString()} bytes | Updated ${log.mtime.toISOString()}`
      : 'Latest Import Log: none found',
    '------------------------------------------------------------',
  ];

  console.log(lines.join('\n'));
}

(async () => {
  try {
    console.log('10-minute progress monitor started.');
    await snapshot();
    setInterval(async () => {
      try {
        await snapshot();
      } catch (err) {
        console.error('Monitor snapshot failed:', err.message);
      }
    }, INTERVAL_MS);
  } catch (err) {
    console.error('Monitor startup failed:', err.message);
    process.exit(1);
  }
})();
